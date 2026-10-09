#![cfg(feature = "dynamodb")]

use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aws_sdk_dynamodb::config::{
  AsyncSleep, BehaviorVersion, Credentials, Region, Sleep, StalledStreamProtectionConfig,
};
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::batch_get_item::BatchGetItemError;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client;
use aws_smithy_runtime_api::client::http::{
  http_client_fn, HttpClient, HttpConnector, HttpConnectorFuture, SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::HttpRequest;
use aws_smithy_types::retry::RetryConfig;
use aws_smithy_types::timeout::TimeoutConfig;
use aws_smithy_types::{body::SdkBody, byte_stream::ByteStream};
use chrono::DateTime;
use event_store_adapter_rs::next::aggregate_id::AggregateId;
use event_store_adapter_rs::next::dynamodb::{DynamoDbOptions, DynamoDbTables, EventStoreForDynamoDB};
use event_store_adapter_rs::next::error::{ContractRule, EventStoreError, SerializationPhase, StorageOperation};
use event_store_adapter_rs::next::event_envelope::{EventEnvelope, SnapshotEnvelope, SnapshotRead};
use event_store_adapter_rs::next::seq_nr::SeqNr;
use event_store_adapter_rs::next::serializer::{JsonEventSerializer, SnapshotSerializer};
use event_store_adapter_test_utils_rs::{docker, dynamodb};
use serde_json::{json, Value};
use testcontainers::{ContainerAsync, GenericImage};

type Item = HashMap<String, AttributeValue>;
type Traces = Arc<Mutex<Vec<Value>>>;
type JsonStore = EventStoreForDynamoDB<Id, Value, Value>;
type BytesStore = EventStoreForDynamoDB<Id, Bytes, Value>;

#[derive(Debug, Clone)]
struct Id {
  type_name: String,
  value: String,
  type_calls: Arc<AtomicUsize>,
  value_calls: Arc<AtomicUsize>,
}

impl Id {
  fn new(type_name: &str, value: &str) -> Self {
    Self {
      type_name: type_name.into(),
      value: value.into(),
      type_calls: Arc::new(AtomicUsize::new(0)),
      value_calls: Arc::new(AtomicUsize::new(0)),
    }
  }
}

impl AggregateId for Id {
  fn type_name(&self) -> String {
    self.type_calls.fetch_add(1, Ordering::SeqCst);
    self.type_name.clone()
  }

  fn value(&self) -> String {
    self.value_calls.fetch_add(1, Ordering::SeqCst);
    self.value.clone()
  }
}

impl std::fmt::Display for Id {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    formatter.write_str("caller-defined-decoy")
  }
}

// 非serde・非Clone・非Debugの集約状態を任意serializerの公開入口で使う。
struct Bytes(Vec<u8>);

#[derive(Debug, thiserror::Error)]
#[error("controlled snapshot restoration failure")]
struct RestoreCause {
  token: usize,
}

#[derive(Debug, Default)]
struct BytesSerializer {
  inputs: Mutex<Vec<Vec<u8>>>,
  fail: AtomicBool,
  failures: AtomicUsize,
}

impl SnapshotSerializer<Bytes> for BytesSerializer {
  fn serialize(&self, aggregate: &Bytes) -> Result<Vec<u8>, EventStoreError> {
    Ok(aggregate.0.clone())
  }

  fn deserialize(&self, data: &[u8]) -> Result<Bytes, EventStoreError> {
    self.inputs.lock().unwrap().push(data.to_vec());
    if self.fail.load(Ordering::SeqCst) {
      self.failures.fetch_add(1, Ordering::SeqCst);
      return Err(EventStoreError::Serialization {
        phase: SerializationPhase::DeserializeSnapshot,
        source: Box::new(RestoreCause { token: 37 }),
      });
    }
    Ok(Bytes(data.to_vec()))
  }
}

#[derive(Debug, Clone, Default)]
struct RecordedSleep(Arc<Mutex<Vec<Duration>>>);

impl AsyncSleep for RecordedSleep {
  fn sleep(&self, delay: Duration) -> Sleep {
    let recorded = self.clone();
    Sleep::new(async move {
      recorded.0.lock().unwrap().push(delay);
    })
  }
}

#[derive(Debug)]
struct InterleavedAppend {
  store: JsonStore,
  observer: Client,
  observer_traces: Traces,
  head_table: String,
  aid: String,
  event: EventEnvelope<Id, Value>,
  snapshot: SnapshotEnvelope<Value>,
}

#[derive(Debug)]
enum Injection {
  Unprocessed(Vec<String>),
  Attribute {
    table: String,
    name: String,
    replacement: Option<Value>,
  },
  Interleave(Box<InterleavedAppend>),
}

#[derive(Debug)]
struct Plan {
  pending: VecDeque<Injection>,
  declared: usize,
  applied: usize,
}

#[derive(Debug)]
struct ObservedConnector {
  upstream: SharedHttpConnector,
  traces: Traces,
  plan: Arc<Mutex<Option<Plan>>>,
}

impl HttpConnector for ObservedConnector {
  fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
    let api = request
      .headers()
      .get("x-amz-target")
      .unwrap()
      .rsplit('.')
      .next()
      .unwrap()
      .to_string();
    let input: Value = serde_json::from_slice(request.body().bytes().unwrap()).unwrap();
    let is_snapshot = api == "BatchGetItem"
      && input["RequestItems"]
        .as_object()
        .unwrap()
        .values()
        .all(|attributes| attributes["Keys"][0]["aid"]["S"] != "__config__");
    let injection = if is_snapshot {
      self
        .plan
        .lock()
        .unwrap()
        .as_mut()
        .and_then(|plan| plan.pending.pop_front())
    } else {
      None
    };
    let index = {
      let mut traces = self.traces.lock().unwrap();
      let index = traces.len();
      traces.push(
        json!({"api": api, "input": input, "upstream_body": null, "upstream_status": null,
        "delivered_body": null, "delivered_status": null, "injection": null, "upstream_error": null}),
      );
      index
    };
    let traces = self.traces.clone();
    let upstream = self.upstream.clone();
    let plan = self.plan.clone();
    HttpConnectorFuture::new(async move {
      let old_head = if let Some(Injection::Interleave(append)) = &injection {
        append
          .observer
          .get_item()
          .table_name(&append.head_table)
          .key("aid", AttributeValue::S(append.aid.clone()))
          .consistent_read(true)
          .send()
          .await
          .unwrap()
          .item
          .unwrap();
        let captured_trace = append.observer_traces.lock().unwrap().last().unwrap().clone();
        assert_eq!(captured_trace["api"], "GetItem");
        assert_eq!(captured_trace["input"]["TableName"], append.head_table);
        let captured_body: Value = serde_json::from_str(captured_trace["upstream_body"].as_str().unwrap()).unwrap();
        let old_head = captured_body["Item"].clone();
        assert!(old_head.is_object());
        append
          .store
          .persist_event_and_snapshot(append.event.clone(), append.snapshot.clone())
          .await
          .unwrap();
        Some(old_head)
      } else {
        None
      };
      let mut response = match upstream.call(request).await {
        Ok(response) => response,
        Err(error) => {
          traces.lock().unwrap()[index]["upstream_error"] = json!(format!("{error:?}"));
          return Err(error);
        }
      };
      let body = std::mem::replace(response.body_mut(), SdkBody::taken());
      let bytes = ByteStream::new(body).collect().await.unwrap().into_bytes();
      let original = String::from_utf8(bytes.to_vec()).unwrap();
      let status = response.status().as_u16();
      let (delivered, applied) = if let Some(injection) = injection {
        assert_eq!(status, 200);
        let mut altered: Value = serde_json::from_str(&original).unwrap();
        let applied = match injection {
          Injection::Unprocessed(tables) => {
            let mut pending = serde_json::Map::new();
            for table in &tables {
              let attributes = input["RequestItems"].get(table).unwrap();
              altered["Responses"].as_object_mut().unwrap().remove(table).unwrap();
              pending.insert(
                table.clone(),
                json!({"Keys": attributes["Keys"], "ConsistentRead": true}),
              );
            }
            altered["UnprocessedKeys"] = Value::Object(pending);
            json!({"kind": "replace-response", "unprocessed_tables": tables, "applied": 1})
          }
          Injection::Attribute {
            table,
            name,
            replacement,
          } => {
            let item = altered["Responses"][&table][0].as_object_mut().unwrap();
            match &replacement {
              Some(value) => {
                item.insert(name.clone(), value.clone());
              }
              None => {
                item.remove(&name).unwrap();
              }
            }
            json!({"kind": "replace-response", "table": table, "attribute": name,
              "replacement": replacement, "applied": 1})
          }
          Injection::Interleave(append) => {
            let captured = old_head.unwrap();
            let item = altered["Responses"][&append.head_table][0].as_object_mut().unwrap();
            *item = captured.as_object().unwrap().clone();
            json!({"kind": "read-interleave", "captured_head": captured, "applied": 1})
          }
        };
        plan.lock().unwrap().as_mut().unwrap().applied += 1;
        (altered.to_string(), applied)
      } else {
        (original.clone(), Value::Null)
      };
      *response.body_mut() = SdkBody::from(delivered.clone());
      traces.lock().unwrap()[index] = json!({"api": api, "input": input, "upstream_body": original,
        "upstream_status": status, "delivered_body": delivered, "delivered_status": response.status().as_u16(),
        "injection": applied, "upstream_error": null});
      Ok(response)
    })
  }
}

struct Observed {
  client: Client,
  traces: Traces,
  plan: Arc<Mutex<Option<Plan>>>,
  sleeper: RecordedSleep,
}

impl Observed {
  fn new(endpoint: &str) -> Self {
    let traces: Traces = Arc::new(Mutex::new(Vec::new()));
    let plan = Arc::new(Mutex::new(None));
    let sleeper = RecordedSleep::default();
    let records = traces.clone();
    let control = plan.clone();
    let upstream = aws_smithy_http_client::Builder::new().build_http();
    let http = http_client_fn(move |settings, components| {
      SharedHttpConnector::new(ObservedConnector {
        upstream: upstream.http_connector(settings, components),
        traces: records.clone(),
        plan: control.clone(),
      })
    });
    let config = aws_sdk_dynamodb::Config::builder()
      .behavior_version(BehaviorVersion::latest())
      .region(Region::new("us-west-1"))
      .credentials_provider(Credentials::new("x", "x", None, None, "local-latest-snapshot-test"))
      .endpoint_url(endpoint)
      .http_client(http)
      .sleep_impl(sleeper.clone())
      .retry_config(RetryConfig::disabled())
      .timeout_config(TimeoutConfig::disabled())
      .stalled_stream_protection(StalledStreamProtectionConfig::disabled())
      .build();
    Self {
      client: Client::from_conf(config),
      traces,
      plan,
      sleeper,
    }
  }

  fn install(&self, injections: Vec<Injection>) {
    let mut slot = self.plan.lock().unwrap();
    assert!(slot.is_none());
    *slot = Some(Plan {
      declared: injections.len(),
      applied: 0,
      pending: injections.into(),
    });
  }

  fn finish(&self) -> Result<Value, Value> {
    let plan = self.plan.lock().unwrap().take().unwrap();
    let report = json!({"declared": plan.declared, "applied": plan.applied, "remaining": plan.pending.len()});
    if plan.declared == plan.applied && plan.pending.is_empty() {
      Ok(report)
    } else {
      Err(report)
    }
  }

  fn take(&self) -> Value {
    let waits = std::mem::take(&mut *self.sleeper.0.lock().unwrap());
    json!({"traces": std::mem::take(&mut *self.traces.lock().unwrap()),
      "waits_ms": waits.iter().map(Duration::as_millis).collect::<Vec<_>>()})
  }
}

struct Fixture {
  container: ContainerAsync<GenericImage>,
  raw: Observed,
  endpoint: String,
  tables: DynamoDbTables,
  record_number: AtomicUsize,
}

impl Fixture {
  async fn new() -> Self {
    let container = docker::dynamodb_local().await.unwrap();
    let port = container.get_host_port_ipv4(docker::DYNAMODB_LOCAL_PORT).await.unwrap();
    let endpoint = format!("http://127.0.0.1:{port}");
    let raw = Observed::new(&endpoint);
    let names = dynamodb::TableNames {
      journal: "latest-first".into(),
      snapshot: "latest-second".into(),
      head: "latest-third".into(),
      snapshot_history_index: "latest-history".into(),
    };
    dynamodb::create_tables(&raw.client, &names, false).await.unwrap();
    let fixture = Self {
      container,
      raw,
      endpoint,
      record_number: AtomicUsize::new(0),
      tables: DynamoDbTables {
        journal_table_name: names.journal,
        snapshot_table_name: names.snapshot,
        head_table_name: names.head,
        snapshot_history_index_name: names.snapshot_history_index,
      },
    };
    fixture.record("create-tables", &Value::Null, &Value::Null);
    fixture
  }

  async fn open_json(&self, options: DynamoDbOptions) -> (JsonStore, Observed) {
    let observed = Observed::new(&self.endpoint);
    let store = JsonStore::open(observed.client.clone(), self.tables.clone(), options)
      .await
      .unwrap();
    self.record("open-json", &observed.take(), &Value::Null);
    (store, observed)
  }

  async fn open_bytes(&self, serializer: Arc<BytesSerializer>) -> (BytesStore, Observed) {
    let observed = Observed::new(&self.endpoint);
    let store = BytesStore::open_with_serializers(
      observed.client.clone(),
      self.tables.clone(),
      DynamoDbOptions::default(),
      Arc::new(JsonEventSerializer::new()),
      serializer,
    )
    .await
    .unwrap();
    self.record("open-bytes", &observed.take(), &Value::Null);
    (store, observed)
  }

  async fn get(&self, table: &str, key: Item) -> Option<Item> {
    self
      .raw
      .client
      .get_item()
      .table_name(table)
      .set_key(Some(key))
      .consistent_read(true)
      .send()
      .await
      .unwrap()
      .item
  }

  async fn current(&self, aid: &str) -> Option<Item> {
    let mut key = head_key(aid);
    key.insert("skey".into(), AttributeValue::N("0".into()));
    self.get(&self.tables.snapshot_table_name, key).await
  }

  async fn head(&self, aid: &str) -> Option<Item> {
    self.get(&self.tables.head_table_name, head_key(aid)).await
  }

  async fn put(&self, table: &str, item: Item) {
    self
      .raw
      .client
      .put_item()
      .table_name(table)
      .set_item(Some(item))
      .send()
      .await
      .unwrap();
  }

  fn record(&self, name: &str, observation: &Value, extra: &Value) {
    let observer = self.raw.take();
    if let Some(directory) = std::env::var_os("DYNAMODB_SNAPSHOT_READ_EVIDENCE_DIR") {
      let directory = std::path::PathBuf::from(directory);
      std::fs::create_dir_all(&directory).unwrap();
      let record = json!({"container_id": self.container.id(), "tables": {"journal": self.tables.journal_table_name,
        "snapshot": self.tables.snapshot_table_name, "head": self.tables.head_table_name},
        "operation": observation, "observer": observer, "extra": extra});
      let number = self.record_number.fetch_add(1, Ordering::SeqCst);
      let port = self.endpoint.rsplit(':').next().unwrap();
      std::fs::write(
        directory.join(format!("{name}-{port}-{number}.json")),
        serde_json::to_vec_pretty(&record).unwrap(),
      )
      .unwrap();
    }
  }

  async fn close(self) {
    self.container.rm().await.unwrap();
  }
}

fn head_key(aid: &str) -> Item {
  Item::from([("aid".into(), AttributeValue::S(aid.into()))])
}

fn event(value: &str, seq_nr: SeqNr) -> EventEnvelope<Id, Value> {
  EventEnvelope::new(
    Id::new("Account", value),
    seq_nr,
    DateTime::from_timestamp_nanos(123456789),
    json!({"event": seq_nr}),
  )
}

async fn persist_pair(
  fixture: &Fixture,
  store: &JsonStore,
  observed: &Observed,
  value: &str,
  seq_nr: SeqNr,
  aggregate: Value,
  manifest: &str,
) {
  store
    .persist_event_and_snapshot(
      event(value, seq_nr),
      SnapshotEnvelope::new(aggregate.clone(), seq_nr).with_manifest(manifest),
    )
    .await
    .unwrap();
  let aid = format!("Account-{value}");
  let head = fixture.head(&aid).await.unwrap();
  let current = fixture.current(&aid).await.unwrap();
  assert_eq!(head["seq_nr"].as_n().unwrap(), &seq_nr.to_string());
  assert_eq!(current["seq_nr"].as_n().unwrap(), &seq_nr.to_string());
  assert_eq!(current["manifest"].as_s().unwrap(), manifest);
  assert_eq!(
    serde_json::from_slice::<Value>(current["payload"].as_b().unwrap().as_ref()).unwrap(),
    aggregate
  );
  let observation = observed.take();
  assert_eq!(observation["traces"].as_array().unwrap().len(), 1);
  assert_eq!(observation["traces"][0]["api"], "TransactWriteItems");
  assert_eq!(observation["traces"][0]["upstream_status"], 200);
  fixture.record(
    "pair-write",
    &observation,
    &json!({"aid": aid, "seq_nr": seq_nr, "aggregate": aggregate, "manifest": manifest}),
  );
}

async fn persist_event(fixture: &Fixture, store: &JsonStore, observed: &Observed, value: &str, seq_nr: SeqNr) {
  store.persist_event(event(value, seq_nr)).await.unwrap();
  let head = fixture.head(&format!("Account-{value}")).await.unwrap();
  assert_eq!(head["seq_nr"].as_n().unwrap(), &seq_nr.to_string());
  let observation = observed.take();
  assert_eq!(observation["traces"].as_array().unwrap().len(), 1);
  assert_eq!(observation["traces"][0]["api"], "TransactWriteItems");
  assert_eq!(observation["traces"][0]["upstream_status"], 200);
  fixture.record("event-only-write", &observation, &json!({"seq_nr": seq_nr}));
}

fn response_body(trace: &Value, name: &str) -> Value {
  serde_json::from_str(trace[name].as_str().unwrap()).unwrap()
}

fn assert_requests(fixture: &Fixture, observation: &Value, aid: &str) {
  let traces = observation["traces"]
    .as_array()
    .unwrap()
    .iter()
    .filter(|trace| trace["api"] == "BatchGetItem")
    .collect::<Vec<_>>();
  assert!(!traces.is_empty());
  for (index, trace) in traces.iter().enumerate() {
    let request = trace["input"]["RequestItems"].as_object().unwrap();
    for attributes in request.values() {
      assert_eq!(attributes["ConsistentRead"], true);
      assert!(attributes.get("ProjectionExpression").is_none());
      assert!(attributes.get("AttributesToGet").is_none());
    }
    if index == 0 {
      assert_eq!(request.len(), 2);
      assert_eq!(
        request[&fixture.tables.head_table_name]["Keys"],
        json!([{ "aid": {"S": aid} }])
      );
      assert_eq!(
        request[&fixture.tables.snapshot_table_name]["Keys"],
        json!([{ "aid": {"S": aid}, "skey": {"N": "0"} }])
      );
    } else {
      let previous = response_body(traces[index - 1], "delivered_body");
      let pending = previous["UnprocessedKeys"].as_object().unwrap();
      assert_eq!(request.len(), pending.len());
      for (table, attributes) in request {
        assert_eq!(attributes["Keys"], pending[table]["Keys"]);
      }
    }
  }
}

fn assert_id_fixed(id: &Id) {
  assert_eq!(id.type_calls.load(Ordering::SeqCst), 1);
  assert_eq!(id.value_calls.load(Ordering::SeqCst), 1);
}

fn result_json(read: &SnapshotRead<Value>) -> Value {
  json!({"head_seq_nr": read.head_seq_nr(), "snapshot": read.snapshot().map(|snapshot| json!({"seq_nr": snapshot.seq_nr(),
    "manifest": snapshot.manifest(), "aggregate": snapshot.aggregate()}))})
}

fn failure<A>(result: &Result<Option<SnapshotRead<A>>, EventStoreError>) -> &EventStoreError {
  match result {
    Err(error) => error,
    Ok(_) => panic!("expected the snapshot read to fail"),
  }
}

fn assert_storage(error: &EventStoreError) {
  assert!(matches!(
    error,
    EventStoreError::Storage {
      operation: StorageOperation::LoadSnapshot,
      ..
    }
  ));
  assert!(error.source().is_some());
}

fn error_json(error: &EventStoreError) -> Value {
  match error {
    EventStoreError::ContractViolation { rule, .. } => json!({"error": "contract-violation", "rule": rule.to_string()}),
    EventStoreError::Storage { operation, source } => {
      json!({"error": "storage", "operation": operation.to_string(), "source": format!("{source:?}")})
    }
    EventStoreError::Serialization { phase, source } => {
      json!({"error": "serialization", "phase": phase.to_string(), "source": format!("{source:?}")})
    }
    other => panic!("unexpected snapshot read error: {other:?}"),
  }
}

#[tokio::test]
async fn should_read_absence_head_only_and_pair_written_snapshots_through_public_open() {
  let fixture = Fixture::new().await;
  let (store, observed) = fixture.open_json(DynamoDbOptions::default()).await;
  let id = Id::new("Account", "連続-with-dash");
  assert!(store.get_latest_snapshot_by_id(&id).await.unwrap().is_none());
  assert_id_fixed(&id);
  let observation = observed.take();
  assert_eq!(observation["traces"].as_array().unwrap().len(), 1);
  assert_requests(&fixture, &observation, "Account-連続-with-dash");
  fixture.record("absent", &observation, &json!({"result": null}));

  persist_event(&fixture, &store, &observed, "連続-with-dash", 1).await;
  assert!(fixture.current("Account-連続-with-dash").await.is_none());
  let id = Id::new("Account", "連続-with-dash");
  let read = store.get_latest_snapshot_by_id(&id).await.unwrap().unwrap();
  assert_id_fixed(&id);
  assert_eq!(read.head_seq_nr(), 1);
  assert!(read.snapshot().is_none());
  let observation = observed.take();
  assert_eq!(observation["traces"].as_array().unwrap().len(), 1);
  assert_requests(&fixture, &observation, "Account-連続-with-dash");
  fixture.record("head-only", &observation, &result_json(&read));

  let aggregate = json!({"text": "e\u{301}🙂", "values": [null, false, 42]});
  persist_pair(
    &fixture,
    &store,
    &observed,
    "連続-with-dash",
    2,
    aggregate.clone(),
    "free-form:2",
  )
  .await;
  let id = Id::new("Account", "連続-with-dash");
  let read = store.get_latest_snapshot_by_id(&id).await.unwrap().unwrap();
  assert_id_fixed(&id);
  assert_eq!(read.head_seq_nr(), 2);
  let snapshot = read.snapshot().unwrap();
  assert_eq!(snapshot.seq_nr(), 2);
  assert_eq!(snapshot.manifest(), "free-form:2");
  assert_eq!(snapshot.aggregate(), &aggregate);
  let observation = observed.take();
  assert_eq!(observation["traces"].as_array().unwrap().len(), 1);
  assert_requests(&fixture, &observation, "Account-連続-with-dash");
  assert_eq!(
    observation["traces"][0]["upstream_body"],
    observation["traces"][0]["delivered_body"]
  );
  assert_eq!(observation["waits_ms"], json!([]));
  fixture.record("pair-read", &observation, &result_json(&read));
  fixture.close().await;
}

#[tokio::test]
async fn should_ignore_an_actual_orphan_current_without_deserializing_it() {
  let fixture = Fixture::new().await;
  let serializer = Arc::new(BytesSerializer::default());
  let (store, observed) = fixture.open_bytes(serializer.clone()).await;
  store
    .persist_event_and_snapshot(event("orphan", 1), SnapshotEnvelope::new(Bytes(vec![0, 255]), 1))
    .await
    .unwrap();
  let saved = fixture.current("Account-orphan").await.unwrap();
  fixture.record("orphan-pair-write", &observed.take(), &Value::Null);
  fixture
    .raw
    .client
    .delete_item()
    .table_name(&fixture.tables.head_table_name)
    .set_key(Some(head_key("Account-orphan")))
    .send()
    .await
    .unwrap();
  assert!(fixture.head("Account-orphan").await.is_none());
  assert_eq!(fixture.current("Account-orphan").await.unwrap(), saved);
  serializer.fail.store(true, Ordering::SeqCst);
  let id = Id::new("Account", "orphan");
  assert!(store.get_latest_snapshot_by_id(&id).await.unwrap().is_none());
  assert_id_fixed(&id);
  assert!(serializer.inputs.lock().unwrap().is_empty());
  assert_eq!(serializer.failures.load(Ordering::SeqCst), 0);
  let observation = observed.take();
  assert_eq!(observation["traces"].as_array().unwrap().len(), 1);
  assert_requests(&fixture, &observation, "Account-orphan");
  let body = response_body(&observation["traces"][0], "upstream_body");
  assert_eq!(
    body["Responses"][&fixture.tables.snapshot_table_name]
      .as_array()
      .unwrap()
      .len(),
    1
  );
  assert!(body["Responses"][&fixture.tables.head_table_name]
    .as_array()
    .unwrap()
    .is_empty());
  fixture.record(
    "orphan-read",
    &observation,
    &json!({"result": null, "serializer_calls": 0}),
  );
  fixture.close().await;
}

#[tokio::test]
async fn should_restore_non_serde_binary_aggregates_with_independent_metadata_and_owned_bytes() {
  let fixture = Fixture::new().await;
  let serializer = Arc::new(BytesSerializer::default());
  let (store, observed) = fixture.open_bytes(serializer.clone()).await;
  let payload = vec![0, 255, 128, 5];
  for (value, manifest) in [("binary-free", "e\u{301}🙂:opaque"), ("binary-empty", "")] {
    store
      .persist_event_and_snapshot(
        event(value, 1),
        SnapshotEnvelope::new(Bytes(payload.clone()), 1).with_manifest(manifest),
      )
      .await
      .unwrap();
    let aid = format!("Account-{value}");
    let saved = fixture.current(&aid).await.unwrap();
    assert_eq!(saved["payload"].as_b().unwrap().as_ref(), payload);
    assert_eq!(saved["manifest"].as_s().unwrap(), manifest);
    fixture.record(
      "binary-pair-write",
      &observed.take(),
      &json!({"aid": aid, "bytes": payload}),
    );
    for mutate in [true, false] {
      let id = Id::new("Account", value);
      let read = store.get_latest_snapshot_by_id(&id).await.unwrap().unwrap();
      assert_id_fixed(&id);
      assert_eq!(read.head_seq_nr(), 1);
      let snapshot = read.snapshot().unwrap();
      assert_eq!(snapshot.seq_nr(), 1);
      assert_eq!(snapshot.manifest(), manifest);
      assert_eq!(snapshot.aggregate().0, payload);
      let observation = observed.take();
      assert_eq!(observation["traces"].as_array().unwrap().len(), 1);
      assert_requests(&fixture, &observation, &aid);
      let (snapshot, _) = read.into_parts();
      let mut aggregate = snapshot.unwrap().into_aggregate();
      if mutate {
        aggregate.0[0] = 99;
        aggregate.0.push(7);
      }
      assert_eq!(fixture.current(&aid).await.unwrap(), saved);
      fixture.record("binary-read", &observation, &json!({"aid": aid, "head_seq_nr": 1, "seq_nr": 1, "manifest": manifest,
        "restored_bytes": payload, "returned_aggregate_modified": mutate, "serializer_inputs": *serializer.inputs.lock().unwrap()}));
    }
  }
  assert_eq!(*serializer.inputs.lock().unwrap(), vec![payload; 4]);
  fixture.close().await;
}

#[tokio::test]
async fn should_retry_only_partial_or_all_unprocessed_keys_and_keep_the_checked_aid() {
  let fixture = Fixture::new().await;
  let (store, observed) = fixture.open_json(DynamoDbOptions::default()).await;
  let aggregate = json!({"saved": true});
  persist_pair(
    &fixture,
    &store,
    &observed,
    "retry",
    1,
    aggregate.clone(),
    "retry:manifest",
  )
  .await;
  for (name, pending) in [
    ("unprocessed-head", vec![fixture.tables.head_table_name.clone()]),
    ("unprocessed-current", vec![fixture.tables.snapshot_table_name.clone()]),
    (
      "unprocessed-all",
      vec![
        fixture.tables.head_table_name.clone(),
        fixture.tables.snapshot_table_name.clone(),
      ],
    ),
  ] {
    observed.install(vec![Injection::Unprocessed(pending)]);
    let id = Id::new("Account", "retry");
    let read = store.get_latest_snapshot_by_id(&id).await.unwrap().unwrap();
    assert_id_fixed(&id);
    assert_eq!(read.head_seq_nr(), 1);
    assert_eq!(read.snapshot().unwrap().aggregate(), &aggregate);
    assert_eq!(read.snapshot().unwrap().seq_nr(), 1);
    assert_eq!(read.snapshot().unwrap().manifest(), "retry:manifest");
    let plan = observed.finish().unwrap();
    assert_eq!(plan, json!({"declared": 1, "applied": 1, "remaining": 0}));
    let observation = observed.take();
    assert_eq!(observation["traces"].as_array().unwrap().len(), 2);
    assert_eq!(observation["waits_ms"], json!([50]));
    assert_requests(&fixture, &observation, "Account-retry");
    assert_eq!(observation["traces"][0]["injection"]["applied"], 1);
    assert!(observation["traces"][1]["injection"].is_null());
    assert_eq!(
      observation["traces"][1]["upstream_body"],
      observation["traces"][1]["delivered_body"]
    );
    fixture.record(
      name,
      &observation,
      &json!({"result": result_json(&read), "plan": plan,
      "type_name_calls": id.type_calls.load(Ordering::SeqCst), "value_calls": id.value_calls.load(Ordering::SeqCst)}),
    );
  }
  fixture.close().await;
}

#[tokio::test]
async fn should_complete_unprocessed_reads_before_deciding_absence() {
  let fixture = Fixture::new().await;
  let (store, observed) = fixture.open_json(DynamoDbOptions::default()).await;
  observed.install(vec![Injection::Unprocessed(vec![
    fixture.tables.head_table_name.clone(),
    fixture.tables.snapshot_table_name.clone(),
  ])]);
  let id = Id::new("Account", "absent-retry");
  assert!(store.get_latest_snapshot_by_id(&id).await.unwrap().is_none());
  assert_id_fixed(&id);
  let plan = observed.finish().unwrap();
  assert_eq!(plan, json!({"declared": 1, "applied": 1, "remaining": 0}));
  let observation = observed.take();
  assert_eq!(observation["traces"].as_array().unwrap().len(), 2);
  assert_eq!(observation["waits_ms"], json!([50]));
  assert_requests(&fixture, &observation, "Account-absent-retry");
  let completed = response_body(&observation["traces"][1], "upstream_body");
  assert!(completed["Responses"][&fixture.tables.head_table_name]
    .as_array()
    .unwrap()
    .is_empty());
  assert!(completed["Responses"][&fixture.tables.snapshot_table_name]
    .as_array()
    .unwrap()
    .is_empty());
  fixture.record(
    "unprocessed-then-absent",
    &observation,
    &json!({"result": null, "plan": plan}),
  );

  persist_event(&fixture, &store, &observed, "head-only-retry", 1).await;
  assert!(fixture.current("Account-head-only-retry").await.is_none());
  observed.install(vec![Injection::Unprocessed(vec![fixture
    .tables
    .snapshot_table_name
    .clone()])]);
  let id = Id::new("Account", "head-only-retry");
  let read = store.get_latest_snapshot_by_id(&id).await.unwrap().unwrap();
  assert_id_fixed(&id);
  assert_eq!(read.head_seq_nr(), 1);
  assert!(read.snapshot().is_none());
  let plan = observed.finish().unwrap();
  assert_eq!(plan, json!({"declared": 1, "applied": 1, "remaining": 0}));
  let observation = observed.take();
  assert_eq!(observation["traces"].as_array().unwrap().len(), 2);
  assert_eq!(observation["waits_ms"], json!([50]));
  assert_requests(&fixture, &observation, "Account-head-only-retry");
  let completed = response_body(&observation["traces"][1], "upstream_body");
  assert!(completed["Responses"][&fixture.tables.snapshot_table_name]
    .as_array()
    .unwrap()
    .is_empty());
  fixture.record(
    "unprocessed-then-head-only",
    &observation,
    &json!({"result": result_json(&read), "plan": plan}),
  );
  fixture.close().await;
}

#[tokio::test]
async fn should_stop_at_zero_or_positive_retry_limits_with_bounded_exponential_waits() {
  let fixture = Fixture::new().await;
  let (writer, writes) = fixture.open_json(DynamoDbOptions::default()).await;
  persist_pair(
    &fixture,
    &writer,
    &writes,
    "bounded",
    1,
    json!({"saved": "bounded"}),
    "bounded",
  )
  .await;
  for (limit, waits) in [
    (0, vec![]),
    (1, vec![50]),
    (8, vec![50, 100, 200, 400, 800, 1600, 2000, 2000]),
  ] {
    let options = DynamoDbOptions {
      unprocessed_retry_limit: limit,
      ..DynamoDbOptions::default()
    };
    let (store, observed) = fixture.open_json(options).await;
    observed.install(
      (0..=limit)
        .map(|_| {
          Injection::Unprocessed(vec![
            fixture.tables.head_table_name.clone(),
            fixture.tables.snapshot_table_name.clone(),
          ])
        })
        .collect(),
    );
    let id = Id::new("Account", "bounded");
    let result = store.get_latest_snapshot_by_id(&id).await;
    assert_id_fixed(&id);
    let error = failure(&result);
    assert_storage(error);
    assert_eq!(
      error.source().unwrap().downcast_ref::<std::io::Error>().unwrap().kind(),
      std::io::ErrorKind::Other
    );
    let plan = observed.finish().unwrap();
    assert_eq!(
      plan,
      json!({"declared": limit + 1, "applied": limit + 1, "remaining": 0})
    );
    let observation = observed.take();
    assert_eq!(observation["traces"].as_array().unwrap().len(), limit as usize + 1);
    assert_eq!(observation["waits_ms"], json!(waits));
    assert_requests(&fixture, &observation, "Account-bounded");
    let last = observation["traces"].as_array().unwrap().last().unwrap();
    assert_eq!(
      response_body(last, "delivered_body")["UnprocessedKeys"]
        .as_object()
        .unwrap()
        .len(),
      2
    );
    fixture.record(
      "unprocessed-limit",
      &observation,
      &json!({"retry_limit": limit, "plan": plan, "result": error_json(error)}),
    );
  }
  fixture.close().await;
}

#[tokio::test]
async fn should_return_ahead_and_behind_combinations_from_real_public_writes() {
  let fixture = Fixture::new().await;
  let (store, observed) = fixture.open_json(DynamoDbOptions::default()).await;
  persist_pair(&fixture, &store, &observed, "mixed", 1, json!({"position": 1}), "old").await;
  observed.install(vec![Injection::Interleave(Box::new(InterleavedAppend {
    store: store.clone(),
    observer: fixture.raw.client.clone(),
    observer_traces: fixture.raw.traces.clone(),
    head_table: fixture.tables.head_table_name.clone(),
    aid: "Account-mixed".into(),
    event: event("mixed", 2),
    snapshot: SnapshotEnvelope::new(json!({"position": 2}), 2).with_manifest("new"),
  }))]);
  let id = Id::new("Account", "mixed");
  let read = store.get_latest_snapshot_by_id(&id).await.unwrap().unwrap();
  assert_id_fixed(&id);
  assert_eq!(read.head_seq_nr(), 1);
  assert_eq!(read.snapshot().unwrap().seq_nr(), 2);
  assert_eq!(read.snapshot().unwrap().manifest(), "new");
  assert_eq!(read.snapshot().unwrap().aggregate(), &json!({"position": 2}));
  let plan = observed.finish().unwrap();
  assert_eq!(plan, json!({"declared": 1, "applied": 1, "remaining": 0}));
  let observation = observed.take();
  assert_eq!(observation["traces"].as_array().unwrap().len(), 2);
  assert_eq!(observation["traces"][0]["api"], "BatchGetItem");
  assert_eq!(observation["traces"][1]["api"], "TransactWriteItems");
  assert_eq!(observation["traces"][1]["upstream_status"], 200);
  assert!(observation["waits_ms"].as_array().unwrap().is_empty());
  assert_requests(&fixture, &observation, "Account-mixed");
  let physical_head = fixture.head("Account-mixed").await.unwrap();
  let physical_current = fixture.current("Account-mixed").await.unwrap();
  assert_eq!(physical_head["seq_nr"].as_n().unwrap(), "2");
  assert_eq!(physical_current["seq_nr"].as_n().unwrap(), "2");
  let upstream = response_body(&observation["traces"][0], "upstream_body");
  let delivered = response_body(&observation["traces"][0], "delivered_body");
  assert_eq!(
    upstream["Responses"][&fixture.tables.head_table_name][0]["seq_nr"]["N"],
    "2"
  );
  assert_eq!(
    delivered["Responses"][&fixture.tables.head_table_name][0]["seq_nr"]["N"],
    "1"
  );
  assert_eq!(
    upstream["Responses"][&fixture.tables.snapshot_table_name],
    delivered["Responses"][&fixture.tables.snapshot_table_name]
  );
  fixture.record("snapshot-ahead-of-head", &observation, &json!({"result": result_json(&read), "plan": plan,
    "physical_head_seq_nr": physical_head["seq_nr"].as_n().unwrap(), "physical_current_seq_nr": physical_current["seq_nr"].as_n().unwrap()}));

  persist_event(&fixture, &store, &observed, "mixed", 3).await;
  let id = Id::new("Account", "mixed");
  let read = store.get_latest_snapshot_by_id(&id).await.unwrap().unwrap();
  assert_id_fixed(&id);
  assert_eq!(read.head_seq_nr(), 3);
  assert_eq!(read.snapshot().unwrap().seq_nr(), 2);
  assert_eq!(read.snapshot().unwrap().manifest(), "new");
  assert_eq!(read.snapshot().unwrap().aggregate(), &json!({"position": 2}));
  let observation = observed.take();
  assert_eq!(observation["traces"].as_array().unwrap().len(), 1);
  assert_requests(&fixture, &observation, "Account-mixed");
  assert_eq!(
    observation["traces"][0]["upstream_body"],
    observation["traces"][0]["delivered_body"]
  );
  fixture.record(
    "snapshot-behind-head",
    &observation,
    &json!({"result": result_json(&read)}),
  );
  fixture.close().await;
}

#[tokio::test]
async fn should_reject_real_missing_or_wrongly_typed_non_key_attributes_as_storage() {
  let fixture = Fixture::new().await;
  let serializer = Arc::new(BytesSerializer::default());
  let (store, observed) = fixture.open_bytes(serializer.clone()).await;
  store
    .persist_event_and_snapshot(
      event("attributes", 1),
      SnapshotEnvelope::new(Bytes(vec![0, 255]), 1).with_manifest("saved"),
    )
    .await
    .unwrap();
  let aid = "Account-attributes";
  let saved_head = fixture.head(aid).await.unwrap();
  let saved_current = fixture.current(aid).await.unwrap();
  fixture.record("attributes-pair-write", &observed.take(), &json!({"aid": aid}));
  for (table, original, names) in [
    (&fixture.tables.head_table_name, &saved_head, vec!["seq_nr"]),
    (
      &fixture.tables.snapshot_table_name,
      &saved_current,
      vec!["seq_nr", "manifest", "payload"],
    ),
  ] {
    for name in names {
      for missing in [true, false] {
        let mut corrupt = original.clone();
        if missing {
          corrupt.remove(name).unwrap();
        } else {
          corrupt.insert(name.into(), AttributeValue::Bool(true));
        }
        fixture.put(table, corrupt.clone()).await;
        let key = if table == &fixture.tables.head_table_name {
          head_key(aid)
        } else {
          let mut key = head_key(aid);
          key.insert("skey".into(), AttributeValue::N("0".into()));
          key
        };
        assert_eq!(fixture.get(table, key).await.unwrap(), corrupt);
        let id = Id::new("Account", "attributes");
        let result = store.get_latest_snapshot_by_id(&id).await;
        assert_id_fixed(&id);
        let error = failure(&result);
        assert_storage(error);
        assert_eq!(
          error.source().unwrap().downcast_ref::<std::io::Error>().unwrap().kind(),
          std::io::ErrorKind::InvalidData
        );
        assert!(serializer.inputs.lock().unwrap().is_empty());
        let observation = observed.take();
        assert_eq!(observation["traces"].as_array().unwrap().len(), 1);
        assert_requests(&fixture, &observation, aid);
        assert!(observation["traces"][0]["injection"].is_null());
        assert_eq!(
          observation["traces"][0]["upstream_body"],
          observation["traces"][0]["delivered_body"]
        );
        fixture.record("saved-attribute-failure", &observation, &json!({"table": table, "attribute": name,
          "missing": missing, "result": error_json(error), "serializer_calls": serializer.inputs.lock().unwrap().len()}));
        fixture.put(table, original.clone()).await;
      }
    }
    for number in ["-1", "1.5", "9007199254740992"] {
      let mut corrupt = original.clone();
      corrupt.insert("seq_nr".into(), AttributeValue::N(number.into()));
      fixture.put(table, corrupt.clone()).await;
      let saved = if table == &fixture.tables.head_table_name {
        fixture.head(aid).await
      } else {
        fixture.current(aid).await
      };
      assert_eq!(saved.unwrap(), corrupt);
      let result = store.get_latest_snapshot_by_id(&Id::new("Account", "attributes")).await;
      let error = failure(&result);
      assert_storage(error);
      assert!(serializer.inputs.lock().unwrap().is_empty());
      let observation = observed.take();
      assert_eq!(observation["traces"].as_array().unwrap().len(), 1);
      assert_requests(&fixture, &observation, aid);
      assert_eq!(
        observation["traces"][0]["upstream_body"],
        observation["traces"][0]["delivered_body"]
      );
      fixture.record(
        "saved-number-failure",
        &observation,
        &json!({"table": table, "saved_number": number, "result": error_json(error)}),
      );
      fixture.put(table, original.clone()).await;
    }
  }
  fixture.close().await;
}

#[tokio::test]
async fn should_reject_response_key_attribute_corruptions_and_aid_mismatches() {
  let fixture = Fixture::new().await;
  let (store, observed) = fixture.open_json(DynamoDbOptions::default()).await;
  persist_pair(&fixture, &store, &observed, "keys", 1, json!({"key": "saved"}), "saved").await;
  let aid = "Account-keys";
  let saved_head = fixture.head(aid).await.unwrap();
  let saved_current = fixture.current(aid).await.unwrap();
  for (table, name, replacement) in [
    (fixture.tables.head_table_name.clone(), "aid", None),
    (
      fixture.tables.head_table_name.clone(),
      "aid",
      Some(json!({"BOOL": true})),
    ),
    (
      fixture.tables.head_table_name.clone(),
      "aid",
      Some(json!({"S": "Account-other"})),
    ),
    (fixture.tables.snapshot_table_name.clone(), "aid", None),
    (
      fixture.tables.snapshot_table_name.clone(),
      "aid",
      Some(json!({"BOOL": true})),
    ),
    (
      fixture.tables.snapshot_table_name.clone(),
      "aid",
      Some(json!({"S": "Account-other"})),
    ),
    (fixture.tables.snapshot_table_name.clone(), "skey", None),
    (
      fixture.tables.snapshot_table_name.clone(),
      "skey",
      Some(json!({"BOOL": true})),
    ),
    (
      fixture.tables.snapshot_table_name.clone(),
      "skey",
      Some(json!({"N": "7"})),
    ),
  ] {
    observed.install(vec![Injection::Attribute {
      table: table.clone(),
      name: name.into(),
      replacement: replacement.clone(),
    }]);
    let id = Id::new("Account", "keys");
    let result = store.get_latest_snapshot_by_id(&id).await;
    assert_id_fixed(&id);
    let error = failure(&result);
    assert_storage(error);
    let plan = observed.finish().unwrap();
    assert_eq!(plan, json!({"declared": 1, "applied": 1, "remaining": 0}));
    let observation = observed.take();
    assert_eq!(observation["traces"].as_array().unwrap().len(), 1);
    assert_requests(&fixture, &observation, aid);
    assert_eq!(observation["traces"][0]["upstream_status"], 200);
    assert_eq!(observation["traces"][0]["delivered_status"], 200);
    assert_eq!(observation["traces"][0]["injection"]["applied"], 1);
    assert_ne!(
      observation["traces"][0]["upstream_body"],
      observation["traces"][0]["delivered_body"]
    );
    assert_eq!(fixture.head(aid).await.unwrap(), saved_head);
    assert_eq!(fixture.current(aid).await.unwrap(), saved_current);
    fixture.record(
      "response-key-failure",
      &observation,
      &json!({"table": table, "attribute": name,
      "replacement": replacement, "plan": plan, "result": error_json(error)}),
    );
  }
  fixture.close().await;
}

#[tokio::test]
async fn should_preserve_the_original_snapshot_serializer_cause() {
  let fixture = Fixture::new().await;
  let serializer = Arc::new(BytesSerializer::default());
  let (store, observed) = fixture.open_bytes(serializer.clone()).await;
  let bytes = vec![0, 255, 128, 5];
  store
    .persist_event_and_snapshot(
      event("restore-failure", 1),
      SnapshotEnvelope::new(Bytes(bytes.clone()), 1).with_manifest("failure"),
    )
    .await
    .unwrap();
  let saved = fixture.current("Account-restore-failure").await.unwrap();
  assert_eq!(saved["payload"].as_b().unwrap().as_ref(), bytes);
  fixture.record(
    "restore-failure-pair-write",
    &observed.take(),
    &json!({"saved_bytes": bytes}),
  );
  serializer.fail.store(true, Ordering::SeqCst);
  let id = Id::new("Account", "restore-failure");
  let result = store.get_latest_snapshot_by_id(&id).await;
  assert_id_fixed(&id);
  let error = failure(&result);
  assert!(matches!(
    error,
    EventStoreError::Serialization {
      phase: SerializationPhase::DeserializeSnapshot,
      ..
    }
  ));
  assert_eq!(
    error.source().unwrap().downcast_ref::<RestoreCause>().unwrap().token,
    37
  );
  assert_eq!(*serializer.inputs.lock().unwrap(), vec![bytes]);
  assert_eq!(serializer.failures.load(Ordering::SeqCst), 1);
  let observation = observed.take();
  assert_eq!(observation["traces"].as_array().unwrap().len(), 1);
  assert_requests(&fixture, &observation, "Account-restore-failure");
  assert_eq!(
    observation["traces"][0]["upstream_body"],
    observation["traces"][0]["delivered_body"]
  );
  fixture.record("snapshot-serializer-failure", &observation, &json!({"result": error_json(error), "cause_token": 37,
    "serializer_inputs": *serializer.inputs.lock().unwrap(), "declared": 1, "applied": serializer.failures.load(Ordering::SeqCst)}));
  fixture.close().await;
}

#[tokio::test]
async fn should_preserve_the_original_sdk_cause_after_real_communication_stops() {
  let fixture = Fixture::new().await;
  let (store, observed) = fixture.open_json(DynamoDbOptions::default()).await;
  persist_pair(
    &fixture,
    &store,
    &observed,
    "communication",
    1,
    json!({"saved": true}),
    "communication",
  )
  .await;
  let read = store
    .get_latest_snapshot_by_id(&Id::new("Account", "communication"))
    .await
    .unwrap()
    .unwrap();
  assert_eq!(read.head_seq_nr(), 1);
  assert!(read.snapshot().is_some());
  fixture.record(
    "before-communication-failure",
    &observed.take(),
    &json!({"result": result_json(&read)}),
  );
  fixture.container.stop().await.unwrap();
  let id = Id::new("Account", "communication");
  let result = store.get_latest_snapshot_by_id(&id).await;
  assert_id_fixed(&id);
  let error = failure(&result);
  assert_storage(error);
  let sdk = error
    .source()
    .unwrap()
    .downcast_ref::<SdkError<BatchGetItemError>>()
    .unwrap();
  let SdkError::DispatchFailure(dispatch) = sdk else {
    panic!("expected SDK dispatch failure: {sdk:?}")
  };
  assert!(dispatch.as_connector_error().unwrap().source().is_some());
  let observation = observed.take();
  assert_eq!(observation["traces"].as_array().unwrap().len(), 1);
  assert_requests(&fixture, &observation, "Account-communication");
  let trace = &observation["traces"][0];
  assert!(trace["upstream_body"].is_null());
  assert!(trace["delivered_body"].is_null());
  assert!(trace["injection"].is_null());
  assert!(!trace["upstream_error"].is_null());
  assert!(observation["waits_ms"].as_array().unwrap().is_empty());
  fixture.record(
    "real-communication-failure",
    &observation,
    &json!({"container_stopped": true,
    "same_open_store": true, "result": error_json(error)}),
  );
  fixture.close().await;
}

#[tokio::test]
async fn should_detect_unfired_and_partially_applied_plans() {
  let fixture = Fixture::new().await;
  let (store, observed) = fixture.open_json(DynamoDbOptions::default()).await;
  persist_pair(
    &fixture,
    &store,
    &observed,
    "plans",
    1,
    json!({"saved": "plans"}),
    "saved",
  )
  .await;
  observed.install(vec![Injection::Unprocessed(vec![
    fixture.tables.head_table_name.clone(),
    fixture.tables.snapshot_table_name.clone(),
  ])]);
  let result = store.get_latest_snapshot_by_id(&Id::new("Bad-Type", "plans")).await;
  let error = failure(&result);
  assert!(matches!(
    error,
    EventStoreError::ContractViolation {
      rule: ContractRule::T11,
      ..
    }
  ));
  let plan = observed.finish().unwrap_err();
  assert_eq!(plan, json!({"declared": 1, "applied": 0, "remaining": 1}));
  let observation = observed.take();
  assert!(observation["traces"].as_array().unwrap().is_empty());
  assert!(observation["waits_ms"].as_array().unwrap().is_empty());
  fixture.record(
    "unfired-plan-detected",
    &observation,
    &json!({"plan": plan, "plan_completion_failed": true, "result": error_json(error)}),
  );

  observed.install(
    (0..2)
      .map(|_| Injection::Attribute {
        table: fixture.tables.snapshot_table_name.clone(),
        name: "manifest".into(),
        replacement: Some(json!({"BOOL": true})),
      })
      .collect(),
  );
  let result = store.get_latest_snapshot_by_id(&Id::new("Account", "plans")).await;
  let error = failure(&result);
  assert_storage(error);
  let plan = observed.finish().unwrap_err();
  assert_eq!(plan, json!({"declared": 2, "applied": 1, "remaining": 1}));
  let observation = observed.take();
  assert_eq!(observation["traces"].as_array().unwrap().len(), 1);
  assert_requests(&fixture, &observation, "Account-plans");
  assert_eq!(observation["traces"][0]["injection"]["applied"], 1);
  fixture.record(
    "partially-applied-plan-detected",
    &observation,
    &json!({"plan": plan,
    "plan_completion_failed": true, "result": error_json(error)}),
  );

  let read = store
    .get_latest_snapshot_by_id(&Id::new("Account", "plans"))
    .await
    .unwrap()
    .unwrap();
  assert_eq!(read.snapshot().unwrap().manifest(), "saved");
  let observation = observed.take();
  assert_eq!(observation["traces"].as_array().unwrap().len(), 1);
  assert!(observation["traces"][0]["injection"].is_null());
  assert_eq!(
    observation["traces"][0]["upstream_body"],
    observation["traces"][0]["delivered_body"]
  );
  fixture.record(
    "plan-removed-before-next-operation",
    &observation,
    &json!({"result": result_json(&read)}),
  );
  fixture.close().await;
}

#[tokio::test]
async fn should_reject_invalid_ids_before_any_send_and_accept_the_utf8_boundary() {
  let fixture = Fixture::new().await;
  let serializer = Arc::new(BytesSerializer::default());
  let (store, observed) = fixture.open_bytes(serializer.clone()).await;
  for (id, rule) in [
    (Id::new("Bad-Type", "input"), ContractRule::T11),
    (Id::new("Account", &"界".repeat(339)), ContractRule::T12),
  ] {
    let result = store.get_latest_snapshot_by_id(&id).await;
    assert_id_fixed(&id);
    assert!(matches!(failure(&result), EventStoreError::ContractViolation { rule: actual, .. } if actual == &rule));
    assert!(serializer.inputs.lock().unwrap().is_empty());
    let observation = observed.take();
    assert!(observation["traces"].as_array().unwrap().is_empty());
    assert!(observation["waits_ms"].as_array().unwrap().is_empty());
    fixture.record("invalid-input-send-zero", &observation, &json!({"type_name": id.type_name, "value": id.value,
      "type_name_calls": id.type_calls.load(Ordering::SeqCst), "value_calls": id.value_calls.load(Ordering::SeqCst), "result": error_json(failure(&result))}));
  }
  let value = format!("{}ab", "界".repeat(338));
  let aid = format!("Account-{value}");
  assert_eq!(aid.len(), 1024);
  let id = Id::new("Account", &value);
  assert!(store.get_latest_snapshot_by_id(&id).await.unwrap().is_none());
  assert_id_fixed(&id);
  let observation = observed.take();
  assert_eq!(observation["traces"].as_array().unwrap().len(), 1);
  assert_requests(&fixture, &observation, &aid);
  fixture.record(
    "input-utf8-boundary",
    &observation,
    &json!({"aid_bytes": aid.len(), "result": null}),
  );
  fixture.close().await;
}
