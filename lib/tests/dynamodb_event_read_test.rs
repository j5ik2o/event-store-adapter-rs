#![cfg(feature = "dynamodb")]

use std::collections::HashMap;
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aws_sdk_dynamodb::config::{
  AsyncSleep, BehaviorVersion, Credentials, Region, Sleep, StalledStreamProtectionConfig,
};
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::query::QueryError;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client;
use aws_smithy_runtime_api::client::http::{
  http_client_fn, HttpClient, HttpConnector, HttpConnectorFuture, SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::HttpRequest;
use aws_smithy_runtime_api::http::StatusCode;
use aws_smithy_types::retry::RetryConfig;
use aws_smithy_types::timeout::TimeoutConfig;
use aws_smithy_types::{body::SdkBody, byte_stream::ByteStream};
use chrono::DateTime;
use event_store_adapter_rs::AggregateId;
use event_store_adapter_rs::EventEnvelope;
use event_store_adapter_rs::{ContractRule, EventStoreError, SerializationPhase, StorageOperation};
use event_store_adapter_rs::{DynamoDbOptions, DynamoDbTables, EventStoreForDynamoDB};
use event_store_adapter_rs::{EventSerializer, SnapshotSerializer};
use event_store_adapter_rs::{SeqNr, SEQ_NR_MAX};
use event_store_adapter_test_utils_rs::{docker, dynamodb};
use serde_json::{json, Value};
use testcontainers::{ContainerAsync, GenericImage};

type Item = HashMap<String, AttributeValue>;
type Traces = Arc<Mutex<Vec<Value>>>;
type JsonStore = EventStoreForDynamoDB<Id, Value, Value>;
type BytesStore = EventStoreForDynamoDB<Id, Bytes, Bytes>;

#[derive(Debug, Clone)]
struct Id {
  type_name: String,
  value: String,
  caller: String,
  type_calls: Arc<AtomicUsize>,
  value_calls: Arc<AtomicUsize>,
}

impl Id {
  fn new(type_name: &str, value: &str, caller: &str) -> Self {
    Self {
      type_name: type_name.into(),
      value: value.into(),
      caller: caller.into(),
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

// 非serde・非Clone・非Debug型を公開生成入口から書込み・読取に渡す。
struct Bytes(Vec<u8>);

#[derive(Debug, thiserror::Error)]
#[error("controlled payload restoration failure")]
struct RestoreCause {
  token: usize,
}

#[derive(Debug, Default)]
struct BytesSerializer {
  inputs: Mutex<Vec<Vec<u8>>>,
  fail_at: AtomicUsize,
}

impl EventSerializer<Bytes> for BytesSerializer {
  fn serialize(&self, payload: &Bytes) -> Result<Vec<u8>, EventStoreError> {
    Ok(payload.0.clone())
  }

  fn deserialize(&self, data: &[u8]) -> Result<Bytes, EventStoreError> {
    let mut inputs = self.inputs.lock().unwrap();
    inputs.push(data.to_vec());
    if inputs.len() == self.fail_at.load(Ordering::SeqCst) {
      return Err(EventStoreError::Serialization {
        phase: SerializationPhase::DeserializeEvent,
        source: Box::new(RestoreCause { token: 37 }),
      });
    }
    Ok(Bytes(data.to_vec()))
  }
}

#[derive(Debug)]
struct NoSnapshot;

impl SnapshotSerializer<Bytes> for NoSnapshot {
  fn serialize(&self, _: &Bytes) -> Result<Vec<u8>, EventStoreError> {
    panic!("イベント単独の操作ではsnapshot serializerを呼ばない")
  }

  fn deserialize(&self, _: &[u8]) -> Result<Bytes, EventStoreError> {
    panic!("イベント読取ではsnapshot serializerを呼ばない")
  }
}

#[derive(Debug)]
struct NoWait;

impl AsyncSleep for NoWait {
  fn sleep(&self, _: Duration) -> Sleep {
    Sleep::new(async {})
  }
}

#[derive(Debug)]
enum Injection {
  Attribute { name: String, replacement: Option<Value> },
  LaterPageError,
}

#[derive(Debug)]
struct ObservedConnector {
  upstream: SharedHttpConnector,
  traces: Traces,
  injection: Arc<Mutex<Option<Injection>>>,
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
    let injection = if api == "Query" {
      let mut pending = self.injection.lock().unwrap();
      match pending.as_ref() {
        Some(Injection::LaterPageError) if input.get("ExclusiveStartKey").is_none() => None,
        _ => pending.take(),
      }
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
    HttpConnectorFuture::new(async move {
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
      let (delivered, applied) = match injection {
        Some(Injection::Attribute { name, replacement }) => {
          assert_eq!(status, 200);
          let mut altered: Value = serde_json::from_str(&original).unwrap();
          let item = altered["Items"][1].as_object_mut().unwrap();
          match &replacement {
            Some(value) => {
              item.insert(name.clone(), value.clone());
            }
            None => {
              item.remove(&name).unwrap();
            }
          }
          (
            altered.to_string(),
            json!({"kind": "replace-response", "attribute": name,
            "replacement": replacement, "item_index": 1, "applied": 1}),
          )
        }
        Some(Injection::LaterPageError) => {
          assert_eq!(status, 200);
          *response.status_mut() = StatusCode::try_from(500).unwrap();
          response.headers_mut().insert("x-amzn-errortype", "InternalServerError");
          (
            json!({"__type": "InternalServerError", "Message": "controlled later page failure"}).to_string(),
            json!({"kind": "replace-response", "error": "InternalServerError", "applied": 1}),
          )
        }
        None => (original.clone(), Value::Null),
      };
      *response.body_mut() = SdkBody::from(delivered.clone());
      traces.lock().unwrap()[index] = json!({"api": api, "input": input,
        "upstream_body": original, "upstream_status": status, "upstream_error": null,
        "delivered_body": delivered, "delivered_status": response.status().as_u16(), "injection": applied});
      Ok(response)
    })
  }
}

struct Observed {
  client: Client,
  traces: Traces,
  injection: Arc<Mutex<Option<Injection>>>,
}

impl Observed {
  fn new(endpoint: &str) -> Self {
    let traces: Traces = Arc::new(Mutex::new(Vec::new()));
    let injection = Arc::new(Mutex::new(None));
    let records = traces.clone();
    let control = injection.clone();
    let upstream = aws_smithy_http_client::Builder::new().build_http();
    let http = http_client_fn(move |settings, components| {
      SharedHttpConnector::new(ObservedConnector {
        upstream: upstream.http_connector(settings, components),
        traces: records.clone(),
        injection: control.clone(),
      })
    });
    let config = aws_sdk_dynamodb::Config::builder()
      .behavior_version(BehaviorVersion::latest())
      .region(Region::new("us-west-1"))
      .credentials_provider(Credentials::new("x", "x", None, None, "local-event-read-test"))
      .endpoint_url(endpoint)
      .http_client(http)
      .sleep_impl(NoWait)
      .retry_config(RetryConfig::disabled())
      .timeout_config(TimeoutConfig::disabled())
      .stalled_stream_protection(StalledStreamProtectionConfig::disabled())
      .build();
    Self {
      client: Client::from_conf(config),
      traces,
      injection,
    }
  }

  fn take(&self) -> Vec<Value> {
    std::mem::take(&mut *self.traces.lock().unwrap())
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
      journal: "read-first".into(),
      snapshot: "read-second".into(),
      head: "read-third".into(),
      snapshot_history_index: "read-history".into(),
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
    fixture.record("create-tables", &[], &Value::Null);
    fixture
  }

  async fn open_json(&self) -> (JsonStore, Observed) {
    let observed = Observed::new(&self.endpoint);
    let store = JsonStore::open(observed.client.clone(), self.tables.clone(), DynamoDbOptions::default())
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
      serializer,
      Arc::new(NoSnapshot),
    )
    .await
    .unwrap();
    self.record("open-bytes", &observed.take(), &Value::Null);
    (store, observed)
  }

  async fn get(&self, aid: &str, seq_nr: SeqNr) -> Item {
    self
      .raw
      .client
      .get_item()
      .table_name(&self.tables.journal_table_name)
      .key("aid", AttributeValue::S(aid.into()))
      .key("seq_nr", AttributeValue::N(seq_nr.to_string()))
      .consistent_read(true)
      .send()
      .await
      .unwrap()
      .item
      .unwrap()
  }

  async fn put(&self, item: Item) {
    self
      .raw
      .client
      .put_item()
      .table_name(&self.tables.journal_table_name)
      .set_item(Some(item))
      .send()
      .await
      .unwrap();
  }

  fn record(&self, name: &str, traces: &[Value], extra: &Value) {
    let observer = self.raw.take();
    if let Some(directory) = std::env::var_os("DYNAMODB_EVENT_READ_EVIDENCE_DIR") {
      let directory = std::path::PathBuf::from(directory);
      std::fs::create_dir_all(&directory).unwrap();
      let port = self.endpoint.rsplit(':').next().unwrap();
      let record = json!({"tables": {"journal": self.tables.journal_table_name,
        "snapshot": self.tables.snapshot_table_name, "head": self.tables.head_table_name},
        "traces": traces, "observer_traces": observer, "extra": extra});
      let number = self.record_number.fetch_add(1, Ordering::SeqCst);
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

fn metadata(seq_nr: SeqNr) -> (i64, &'static str) {
  match seq_nr {
    1 => (i64::MIN, ""),
    2 => (-876543211, "e\u{301}🙂"),
    3 => (i64::MAX, "free-form:manifest"),
    _ => panic!("短受入のイベントは1〜3"),
  }
}

fn assert_saved(item: &Item, aid: &str, seq_nr: SeqNr, nanos: i64, manifest: &str, bytes: &[u8]) {
  assert_eq!(item["aid"].as_s().unwrap(), aid);
  assert_eq!(item["seq_nr"].as_n().unwrap(), &seq_nr.to_string());
  assert_eq!(item["occurred_at"].as_n().unwrap(), &nanos.to_string());
  assert_eq!(item["manifest"].as_s().unwrap(), manifest);
  assert_eq!(item["payload"].as_b().unwrap().as_ref(), bytes);
}

async fn persist_json_events(fixture: &Fixture, store: &JsonStore, observed: &Observed, value: &str) -> Vec<Value> {
  let payloads = vec![
    json!({"created": true, "text": "e\u{301}🙂"}),
    json!([null, false, 42]),
    json!("last"),
  ];
  let aid = format!("Account-{value}");
  for seq_nr in 1..=3 {
    let (nanos, manifest) = metadata(seq_nr);
    let payload = &payloads[(seq_nr - 1) as usize];
    store
      .persist_event(
        EventEnvelope::new(
          Id::new("Account", value, "writer"),
          seq_nr,
          DateTime::from_timestamp_nanos(nanos),
          payload.clone(),
        )
        .with_manifest(manifest),
      )
      .await
      .unwrap();
    assert_saved(
      &fixture.get(&aid, seq_nr).await,
      &aid,
      seq_nr,
      nanos,
      manifest,
      &serde_json::to_vec(payload).unwrap(),
    );
  }
  let writes = observed.take();
  assert_eq!(writes.len(), 3);
  assert!(writes
    .iter()
    .all(|trace| trace["api"] == "TransactWriteItems" && trace["upstream_status"] == 200));
  fixture.record("persist-json", &writes, &json!({"aid": aid, "seq_nrs": [1, 2, 3]}));
  payloads
}

async fn persist_large_events(fixture: &Fixture, store: &BytesStore, observed: &Observed) -> Vec<Vec<u8>> {
  let manifest = "m".repeat(200_000);
  let mut saved_bytes = 0;
  let mut saved_manifest_bytes = 0;
  let mut payloads = Vec::new();
  for seq_nr in 1..=6 {
    let bytes = (0..200_000)
      .map(|position| ((position + seq_nr) % 256) as u8)
      .collect::<Vec<_>>();
    store
      .persist_event(
        EventEnvelope::new(
          Id::new("Account", "pages", "writer"),
          seq_nr,
          DateTime::from_timestamp_nanos(seq_nr as i64),
          Bytes(bytes.clone()),
        )
        .with_manifest(&manifest),
      )
      .await
      .unwrap();
    let saved = fixture.get("Account-pages", seq_nr).await;
    assert_saved(&saved, "Account-pages", seq_nr, seq_nr as i64, &manifest, &bytes);
    saved_bytes += saved["payload"].as_b().unwrap().as_ref().len();
    saved_manifest_bytes += saved["manifest"].as_s().unwrap().len();
    payloads.push(bytes);
  }
  assert!(saved_bytes > 1_048_576);
  let writes = observed.take();
  assert_eq!(writes.len(), 6);
  assert!(writes
    .iter()
    .all(|trace| trace["api"] == "TransactWriteItems" && trace["upstream_status"] == 200));
  fixture.record(
    "large-persist",
    &writes,
    &json!({"independent_saved_payload_bytes": saved_bytes, "independent_saved_manifest_bytes": saved_manifest_bytes,
    "seq_nrs": [1, 2, 3, 4, 5, 6]}),
  );
  payloads
}

fn assert_queries(fixture: &Fixture, traces: &[Value], aid: &str, seq_nr: SeqNr) {
  assert!(!traces.is_empty());
  for trace in traces {
    assert_eq!(trace["api"], "Query");
    let input = &trace["input"];
    assert_eq!(input["TableName"], fixture.tables.journal_table_name);
    assert_eq!(input["ConsistentRead"], true);
    assert_eq!(input["ScanIndexForward"], true);
    for field in ["IndexName", "FilterExpression", "ProjectionExpression", "Limit"] {
      assert!(input.get(field).is_none());
    }
    let mut conditions = HashMap::new();
    for term in input["KeyConditionExpression"].as_str().unwrap().split("AND") {
      let (left, right, operator) = if let Some((left, right)) = term.split_once(">=") {
        (left, right, ">=")
      } else {
        let (left, right) = term.split_once('=').unwrap();
        (left, right, "=")
      };
      let attribute = input["ExpressionAttributeNames"]
        .get(left.trim())
        .and_then(Value::as_str)
        .unwrap_or(left.trim());
      assert!(conditions
        .insert(
          attribute,
          (operator, input["ExpressionAttributeValues"][right.trim()].clone())
        )
        .is_none());
    }
    assert_eq!(
      conditions,
      HashMap::from([
        ("aid", ("=", json!({"S": aid}))),
        ("seq_nr", (">=", json!({"N": seq_nr.to_string()}))),
      ])
    );
  }
}

fn original_body(trace: &Value) -> Value {
  serde_json::from_str(trace["upstream_body"].as_str().unwrap()).unwrap()
}

fn assert_caller<P>(events: &[EventEnvelope<Id, P>], id: &Id) {
  for event in events {
    assert_eq!(event.aggregate_id().type_name, id.type_name);
    assert_eq!(event.aggregate_id().value, id.value);
    assert_eq!(event.aggregate_id().caller, id.caller);
  }
}

fn failure<P>(result: &Result<Vec<EventEnvelope<Id, P>>, EventStoreError>) -> &EventStoreError {
  match result {
    Err(error) => error,
    Ok(_) => panic!("expected the whole read to fail"),
  }
}

fn error_json(error: &EventStoreError) -> Value {
  match error {
    EventStoreError::ContractViolation { rule, seq_nr, .. } => {
      json!({"error": "contract-violation", "rule": rule.to_string(), "seq_nr": seq_nr})
    }
    EventStoreError::Storage { operation, source } => {
      json!({"error": "storage", "operation": operation.to_string(), "source": format!("{source:?}")})
    }
    EventStoreError::Serialization { phase, source } => {
      json!({"error": "serialization", "phase": phase.to_string(), "source": format!("{source:?}")})
    }
    other => panic!("unexpected read error: {other:?}"),
  }
}

#[tokio::test]
async fn should_read_exact_id_since_zero_two_or_past_tail_through_public_open() {
  let fixture = Fixture::new().await;
  let (writer, writes) = fixture.open_json().await;
  let payloads = persist_json_events(&fixture, &writer, &writes, "連続-with-dash").await;
  for id in [
    Id::new("Account", "連続-with-dash-extra", "other-writer"),
    Id::new("OtherAccount", "連続-with-dash", "other-writer"),
  ] {
    writer
      .persist_event(EventEnvelope::new(
        id,
        1,
        DateTime::from_timestamp_nanos(0),
        json!("other"),
      ))
      .await
      .unwrap();
  }
  fixture.record("persist-other-ids", &writes.take(), &Value::Null);
  let (reader, observed) = fixture.open_json().await;
  for (name, value, start, expected) in [
    ("zero", "連続-with-dash", 0, vec![1, 2, 3]),
    ("two", "連続-with-dash", 2, vec![2, 3]),
    ("past-tail", "連続-with-dash", 4, vec![]),
    ("absent", "missing", 0, vec![]),
    ("upper-bound", "連続-with-dash", SEQ_NR_MAX, vec![]),
  ] {
    let id = Id::new("Account", value, "reader-context");
    let events = reader.get_events_by_id_since_seq_nr(&id, start).await.unwrap();
    assert_eq!(events.iter().map(EventEnvelope::seq_nr).collect::<Vec<_>>(), expected);
    assert_caller(&events, &id);
    assert_eq!(id.type_calls.load(Ordering::SeqCst), 1);
    assert_eq!(id.value_calls.load(Ordering::SeqCst), 1);
    for event in &events {
      let (nanos, manifest) = metadata(event.seq_nr());
      assert_eq!(event.occurred_at().timestamp_nanos_opt(), Some(nanos));
      assert_eq!(event.manifest(), manifest);
      assert_eq!(event.payload(), &payloads[(event.seq_nr() - 1) as usize]);
    }
    let traces = observed.take();
    assert_eq!(traces.len(), 1);
    assert_queries(&fixture, &traces, &format!("Account-{value}"), start);
    assert!(traces[0]["injection"].is_null());
    assert_eq!(traces[0]["upstream_body"], traces[0]["delivered_body"]);
    fixture.record(
      &format!("read-{name}"),
      &traces,
      &json!({"seq_nrs": expected, "caller": id.caller}),
    );
  }
  fixture.close().await;
}

#[tokio::test]
async fn should_restore_arbitrary_binary_payloads_without_serde_clone_or_debug() {
  let fixture = Fixture::new().await;
  let serializer = Arc::new(BytesSerializer::default());
  let (store, observed) = fixture.open_bytes(serializer.clone()).await;
  let payloads = [vec![0, 255, 128], vec![1, 0, 254], vec![]];
  for seq_nr in 1..=3 {
    let (nanos, manifest) = metadata(seq_nr);
    let bytes = &payloads[(seq_nr - 1) as usize];
    store
      .persist_event(
        EventEnvelope::new(
          Id::new("Account", "binary", "writer"),
          seq_nr,
          DateTime::from_timestamp_nanos(nanos),
          Bytes(bytes.clone()),
        )
        .with_manifest(manifest),
      )
      .await
      .unwrap();
    assert_saved(
      &fixture.get("Account-binary", seq_nr).await,
      "Account-binary",
      seq_nr,
      nanos,
      manifest,
      bytes,
    );
  }
  fixture.record("binary-persist", &observed.take(), &json!({"bytes": payloads}));
  let id = Id::new("Account", "binary", "binary-reader");
  let events = store.get_events_by_id_since_seq_nr(&id, 0).await.unwrap();
  assert_eq!(events.len(), 3);
  assert_caller(&events, &id);
  for (index, event) in events.iter().enumerate() {
    assert_eq!(event.seq_nr(), index as SeqNr + 1);
    assert_eq!(event.payload().0, payloads[index]);
    let (nanos, manifest) = metadata(event.seq_nr());
    assert_eq!(event.occurred_at().timestamp_nanos_opt(), Some(nanos));
    assert_eq!(event.manifest(), manifest);
  }
  assert_eq!(*serializer.inputs.lock().unwrap(), payloads);
  let traces = observed.take();
  assert_queries(&fixture, &traces, "Account-binary", 0);
  fixture.record("binary-read", &traces, &json!({"bytes": payloads, "caller": id.caller}));
  fixture.close().await;
}

#[tokio::test]
async fn should_follow_actual_last_evaluated_keys_for_saved_events_over_one_megabyte() {
  let fixture = Fixture::new().await;
  let serializer = Arc::new(BytesSerializer::default());
  let (store, observed) = fixture.open_bytes(serializer.clone()).await;
  let payloads = persist_large_events(&fixture, &store, &observed).await;
  let id = Id::new("Account", "pages", "page-reader");
  let events = store.get_events_by_id_since_seq_nr(&id, 0).await.unwrap();
  assert_eq!(events.len(), 6);
  assert_caller(&events, &id);
  assert_eq!(id.type_calls.load(Ordering::SeqCst), 1);
  assert_eq!(id.value_calls.load(Ordering::SeqCst), 1);
  for (index, event) in events.iter().enumerate() {
    assert_eq!(event.seq_nr(), index as SeqNr + 1);
    assert_eq!(event.occurred_at().timestamp_nanos_opt(), Some(index as i64 + 1));
    assert_eq!(event.manifest(), "m".repeat(200_000));
    assert_eq!(event.payload().0, payloads[index]);
  }
  assert_eq!(*serializer.inputs.lock().unwrap(), payloads);
  let traces = observed.take();
  fixture.record(
    "actual-page-responses",
    &traces,
    &json!({"returned_count": events.len()}),
  );
  assert_queries(&fixture, &traces, "Account-pages", 0);
  assert!(traces.len() > 1);
  assert!(traces[0]["input"].get("ExclusiveStartKey").is_none());
  let mut response_seq_nrs = Vec::new();
  for (index, trace) in traces.iter().enumerate() {
    assert_eq!(trace["upstream_status"], 200);
    assert_eq!(trace["upstream_body"], trace["delivered_body"]);
    assert!(trace["injection"].is_null());
    let body = original_body(trace);
    response_seq_nrs.extend(
      body["Items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["seq_nr"]["N"].as_str().unwrap().parse::<SeqNr>().unwrap()),
    );
    if let Some(next) = traces.get(index + 1) {
      let last_key = body["LastEvaluatedKey"].as_object().unwrap();
      assert!(!last_key.is_empty());
      assert_eq!(next["input"]["ExclusiveStartKey"], body["LastEvaluatedKey"]);
    } else {
      assert!(body
        .get("LastEvaluatedKey")
        .is_none_or(|key| key.as_object().unwrap().is_empty()));
    }
  }
  assert_eq!(response_seq_nrs, [1, 2, 3, 4, 5, 6]);
  fixture.record(
    "actual-all-pages",
    &traces,
    &json!({"seq_nrs": response_seq_nrs, "returned_count": events.len(),
    "type_name_calls": 1, "value_calls": 1, "caller": id.caller}),
  );
  fixture.close().await;
}

#[tokio::test]
async fn should_reject_invalid_aid_or_start_number_without_sdk_requests() {
  let fixture = Fixture::new().await;
  let serializer = Arc::new(BytesSerializer::default());
  let (store, observed) = fixture.open_bytes(serializer.clone()).await;
  for (name, id, seq_nr, expected) in [
    ("type", Id::new("Bad-Type", "input", "reader"), 0, ContractRule::T11),
    (
      "utf8-length",
      Id::new("Account", &"界".repeat(339), "reader"),
      0,
      ContractRule::T12,
    ),
    (
      "seq-limit",
      Id::new("Account", "input", "reader"),
      SEQ_NR_MAX + 1,
      ContractRule::T9,
    ),
    (
      "aid-before-seq",
      Id::new("Bad-Type", "input", "reader"),
      SEQ_NR_MAX + 1,
      ContractRule::T11,
    ),
  ] {
    let result = store.get_events_by_id_since_seq_nr(&id, seq_nr).await;
    let error = failure(&result);
    assert!(matches!(error, EventStoreError::ContractViolation { rule, .. } if *rule == expected));
    assert!(serializer.inputs.lock().unwrap().is_empty());
    let traces = observed.take();
    assert!(traces.is_empty());
    fixture.record(
      &format!("reject-{name}"),
      &traces,
      &json!({"result": error_json(error), "sdk_requests": 0}),
    );
  }
  fixture.close().await;
}

#[tokio::test]
async fn should_reject_real_missing_or_wrongly_typed_non_key_attributes_as_storage() {
  let fixture = Fixture::new().await;
  let (store, observed) = fixture.open_json().await;
  persist_json_events(&fixture, &store, &observed, "attributes").await;
  let original = fixture.get("Account-attributes", 2).await;
  fixture.record("original-saved-item", &[], &Value::Null);
  for name in ["occurred_at", "manifest", "payload"] {
    for missing in [true, false] {
      let mut altered = original.clone();
      if missing {
        altered.remove(name);
      } else {
        altered.insert(name.into(), AttributeValue::Bool(true));
      }
      fixture.put(altered.clone()).await;
      assert_eq!(fixture.get("Account-attributes", 2).await, altered);
      let result = store
        .get_events_by_id_since_seq_nr(&Id::new("Account", "attributes", "reader"), 0)
        .await;
      let error = failure(&result);
      assert!(matches!(
        error,
        EventStoreError::Storage {
          operation: StorageOperation::LoadEvents,
          ..
        }
      ));
      let traces = observed.take();
      assert_eq!(traces.len(), 1);
      assert_queries(&fixture, &traces, "Account-attributes", 0);
      assert!(traces[0]["injection"].is_null());
      assert_eq!(traces[0]["upstream_body"], traces[0]["delivered_body"]);
      let body = original_body(&traces[0]);
      if missing {
        assert!(body["Items"][1].get(name).is_none());
      } else {
        assert_eq!(body["Items"][1][name], json!({"BOOL": true}));
      }
      fixture.record(
        &format!("saved-{name}-{}", if missing { "missing" } else { "wrong-type" }),
        &traces,
        &json!({"storage_modified": true, "response_injected": false, "result": error_json(error)}),
      );
    }
  }
  fixture.put(original).await;
  fixture.close().await;
}

#[tokio::test]
async fn should_reject_controlled_key_attribute_corruptions_without_partial_success() {
  let fixture = Fixture::new().await;
  let (store, observed) = fixture.open_json().await;
  persist_json_events(&fixture, &store, &observed, "keys").await;
  let saved = fixture.get("Account-keys", 2).await;
  fixture.record("original-saved-keys", &[], &Value::Null);
  // Localが保存時に拒否するキー欠落・型不一致は、実Query応答を差し替えて区別する。
  for (label, name, replacement) in [
    ("missing-aid", "aid", None),
    ("wrong-aid-type", "aid", Some(json!({"BOOL": true}))),
    ("missing-seq", "seq_nr", None),
    ("wrong-seq-type", "seq_nr", Some(json!({"BOOL": true}))),
    ("different-aid", "aid", Some(json!({"S": "Account-other"}))),
  ] {
    *observed.injection.lock().unwrap() = Some(Injection::Attribute {
      name: name.into(),
      replacement,
    });
    let result = store
      .get_events_by_id_since_seq_nr(&Id::new("Account", "keys", "reader"), 0)
      .await;
    let error = failure(&result);
    assert!(matches!(
      error,
      EventStoreError::Storage {
        operation: StorageOperation::LoadEvents,
        ..
      }
    ));
    let traces = observed.take();
    assert_eq!(traces.len(), 1);
    assert_queries(&fixture, &traces, "Account-keys", 0);
    assert_eq!(traces[0]["upstream_status"], 200);
    assert_eq!(traces[0]["delivered_status"], 200);
    assert_ne!(traces[0]["upstream_body"], traces[0]["delivered_body"]);
    assert_eq!(traces[0]["injection"]["applied"], 1);
    assert!(observed.injection.lock().unwrap().is_none());
    assert_eq!(
      original_body(&traces[0])["Items"][1]["aid"],
      json!({"S": "Account-keys"})
    );
    assert_eq!(fixture.get("Account-keys", 2).await, saved);
    fixture.record(
      &format!("injected-{label}"),
      &traces,
      &json!({"storage_modified": false, "result": error_json(error)}),
    );
  }
  fixture.close().await;
}

#[tokio::test]
async fn should_preserve_the_original_sdk_cause_on_real_communication_failure() {
  let fixture = Fixture::new().await;
  let (store, observed) = fixture.open_json().await;
  persist_json_events(&fixture, &store, &observed, "communication").await;
  let id = Id::new("Account", "communication", "reader");
  let events = store.get_events_by_id_since_seq_nr(&id, 0).await.unwrap();
  assert_eq!(events.len(), 3);
  fixture.record(
    "before-communication-failure",
    &observed.take(),
    &json!({"returned_count": 3}),
  );
  fixture.container.stop().await.unwrap();
  // 同じopen済みストアとClientを保持して、実サーバ停止後の送信を観測する。
  let result = store.get_events_by_id_since_seq_nr(&id, 0).await;
  let error = failure(&result);
  assert!(matches!(
    error,
    EventStoreError::Storage {
      operation: StorageOperation::LoadEvents,
      ..
    }
  ));
  let sdk = error.source().unwrap().downcast_ref::<SdkError<QueryError>>().unwrap();
  let SdkError::DispatchFailure(dispatch) = sdk else {
    panic!("expected an SDK dispatch failure: {sdk:?}")
  };
  assert!(dispatch.as_connector_error().unwrap().source().is_some());
  let traces = observed.take();
  assert_eq!(traces.len(), 1);
  assert_queries(&fixture, &traces, "Account-communication", 0);
  assert!(traces[0]["upstream_body"].is_null());
  assert!(traces[0]["delivered_body"].is_null());
  assert!(traces[0]["injection"].is_null());
  assert!(!traces[0]["upstream_error"].is_null());
  fixture.record(
    "real-communication-failure",
    &traces,
    &json!({"container_stopped": true, "same_open_store": true, "result": error_json(error)}),
  );
  fixture.close().await;
}

#[tokio::test]
async fn should_fail_the_whole_read_when_a_later_real_page_response_is_replaced_by_an_sdk_error() {
  let fixture = Fixture::new().await;
  let serializer = Arc::new(BytesSerializer::default());
  let (store, observed) = fixture.open_bytes(serializer.clone()).await;
  let payloads = persist_large_events(&fixture, &store, &observed).await;
  *observed.injection.lock().unwrap() = Some(Injection::LaterPageError);
  let result = store
    .get_events_by_id_since_seq_nr(&Id::new("Account", "pages", "reader"), 0)
    .await;
  let traces = observed.take();
  fixture.record(
    "later-page-responses",
    &traces,
    &json!({"returned_count": result.as_ref().ok().map(Vec::len), "error": result.as_ref().err().map(error_json)}),
  );
  let error = failure(&result);
  assert!(matches!(
    error,
    EventStoreError::Storage {
      operation: StorageOperation::LoadEvents,
      ..
    }
  ));
  let sdk = error.source().unwrap().downcast_ref::<SdkError<QueryError>>().unwrap();
  assert!(matches!(
    sdk.as_service_error(),
    Some(QueryError::InternalServerError(_))
  ));
  assert_eq!(traces.len(), 2);
  assert_queries(&fixture, &traces, "Account-pages", 0);
  let first = original_body(&traces[0]);
  let first_count = first["Items"].as_array().unwrap().len();
  assert!(first_count > 0 && first_count < payloads.len());
  assert!(!first["LastEvaluatedKey"].as_object().unwrap().is_empty());
  assert_eq!(traces[1]["input"]["ExclusiveStartKey"], first["LastEvaluatedKey"]);
  assert_eq!(traces[0]["upstream_body"], traces[0]["delivered_body"]);
  assert!(traces[0]["injection"].is_null());
  assert_eq!(traces[1]["upstream_status"], 200);
  assert_eq!(traces[1]["delivered_status"], 500);
  assert_eq!(traces[1]["injection"]["applied"], 1);
  assert!(observed.injection.lock().unwrap().is_none());
  assert_eq!(serializer.inputs.lock().unwrap().as_slice(), &payloads[..first_count]);
  assert_eq!(
    sdk.raw_response().unwrap().body().bytes().unwrap(),
    traces[1]["delivered_body"].as_str().unwrap().as_bytes()
  );
  fixture.record(
    "injected-later-page-error",
    &traces,
    &json!({"restored_before_failure": first_count, "result": error_json(error)}),
  );
  fixture.close().await;
}

#[tokio::test]
async fn should_propagate_serializer_failure_after_an_earlier_event_without_partial_success() {
  let fixture = Fixture::new().await;
  let serializer = Arc::new(BytesSerializer::default());
  let (store, observed) = fixture.open_bytes(serializer.clone()).await;
  for seq_nr in 1..=3 {
    store
      .persist_event(EventEnvelope::new(
        Id::new("Account", "restore", "writer"),
        seq_nr,
        DateTime::from_timestamp_nanos(0),
        Bytes(vec![seq_nr as u8]),
      ))
      .await
      .unwrap();
    assert_saved(
      &fixture.get("Account-restore", seq_nr).await,
      "Account-restore",
      seq_nr,
      0,
      "",
      &[seq_nr as u8],
    );
  }
  fixture.record("restore-persist", &observed.take(), &Value::Null);
  serializer.fail_at.store(2, Ordering::SeqCst);
  let result = store
    .get_events_by_id_since_seq_nr(&Id::new("Account", "restore", "reader"), 0)
    .await;
  let error = failure(&result);
  assert!(matches!(
    error,
    EventStoreError::Serialization {
      phase: SerializationPhase::DeserializeEvent,
      ..
    }
  ));
  assert_eq!(
    error.source().unwrap().downcast_ref::<RestoreCause>().unwrap().token,
    37
  );
  assert_eq!(*serializer.inputs.lock().unwrap(), [vec![1], vec![2]]);
  let traces = observed.take();
  assert_eq!(traces.len(), 1);
  assert_queries(&fixture, &traces, "Account-restore", 0);
  assert_eq!(original_body(&traces[0])["Items"].as_array().unwrap().len(), 3);
  assert_eq!(traces[0]["upstream_body"], traces[0]["delivered_body"]);
  assert!(traces[0]["injection"].is_null());
  fixture.record(
    "injected-deserialization-error",
    &traces,
    &json!({"serializer_injection": {"phase": "deserialize-event",
    "call": 2, "applied": 1}, "restored_before_failure": 1, "result": error_json(error)}),
  );
  fixture.close().await;
}
