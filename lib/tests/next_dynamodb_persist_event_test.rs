#![cfg(feature = "dynamodb")]

use std::collections::HashMap;
use std::error::Error;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aws_sdk_dynamodb::config::{
  AsyncSleep, BehaviorVersion, Credentials, Region, Sleep, StalledStreamProtectionConfig,
};
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client;
use aws_smithy_runtime_api::client::http::{
  http_client_fn, HttpClient, HttpConnector, HttpConnectorFuture, SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::HttpRequest;
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::http::{Response, StatusCode};
use aws_smithy_types::retry::RetryConfig;
use aws_smithy_types::timeout::TimeoutConfig;
use aws_smithy_types::{body::SdkBody, byte_stream::ByteStream};
use chrono::{DateTime, Utc};
use event_store_adapter_rs::next::aggregate_id::AggregateId;
use event_store_adapter_rs::next::dynamodb::{DynamoDbOptions, DynamoDbTables, EventStoreForDynamoDB};
use event_store_adapter_rs::next::error::{ContractRule, EventStoreError, SerializationPhase, StorageOperation};
use event_store_adapter_rs::next::event_envelope::EventEnvelope;
use event_store_adapter_rs::next::retention::{RetentionMode, RetentionSettings};
use event_store_adapter_rs::next::seq_nr::SEQ_NR_MAX;
use event_store_adapter_rs::next::serializer::{EventSerializer, SnapshotSerializer};
use event_store_adapter_test_utils_rs::{docker, dynamodb};
use serde_json::{json, Value};
use testcontainers::{ContainerAsync, GenericImage};

type Item = HashMap<String, AttributeValue>;
type Traces = Arc<Mutex<Vec<Value>>>;
type JsonStore = EventStoreForDynamoDB<Id, Value, Value>;
type OpaqueStore = EventStoreForDynamoDB<Id, Opaque, Opaque>;

#[derive(Debug, Clone)]
struct Id {
  type_name: String,
  value: String,
  type_calls: Arc<AtomicUsize>,
}

impl Id {
  fn new(type_name: &str, value: &str) -> Self {
    Self {
      type_name: type_name.into(),
      value: value.into(),
      type_calls: Arc::new(AtomicUsize::new(0)),
    }
  }
}

impl AggregateId for Id {
  fn type_name(&self) -> String {
    self.type_calls.fetch_add(1, Ordering::SeqCst);
    self.type_name.clone()
  }

  fn value(&self) -> String {
    self.value.clone()
  }
}

// 非serde・非Clone・非Debug型を公開serializer入口に渡す。
struct Opaque(Vec<u8>);

#[derive(Debug, Default)]
struct BytesSerializer {
  calls: AtomicUsize,
  fail: AtomicBool,
}

impl EventSerializer<Opaque> for BytesSerializer {
  fn serialize(&self, payload: &Opaque) -> Result<Vec<u8>, EventStoreError> {
    self.calls.fetch_add(1, Ordering::SeqCst);
    if self.fail.load(Ordering::SeqCst) {
      return Err(EventStoreError::Serialization {
        phase: SerializationPhase::SerializeEvent,
        source: Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, "SERIALIZER_CAUSE")),
      });
    }
    Ok(payload.0.clone())
  }

  fn deserialize(&self, data: &[u8]) -> Result<Opaque, EventStoreError> {
    Ok(Opaque(data.to_vec()))
  }
}

#[derive(Debug)]
struct NoSnapshot;

impl SnapshotSerializer<Opaque> for NoSnapshot {
  fn serialize(&self, _: &Opaque) -> Result<Vec<u8>, EventStoreError> {
    panic!("イベント単独ではsnapshot serializerを呼ばない")
  }

  fn deserialize(&self, _: &[u8]) -> Result<Opaque, EventStoreError> {
    panic!("イベント単独ではsnapshotを復元しない")
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
  Cancellation {
    template: Value,
    codes: Vec<(String, String)>,
    old_head: Option<Value>,
  },
  Response(Value),
  Communication,
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
    let injection = if api == "TransactWriteItems" {
      self.injection.lock().unwrap().take()
    } else {
      None
    };
    let index = {
      let mut traces = self.traces.lock().unwrap();
      let index = traces.len();
      traces.push(json!({"api": api, "input": input}));
      index
    };
    let upstream = self.upstream.clone();
    let traces = self.traces.clone();
    HttpConnectorFuture::new(async move {
      let injected = match injection {
        Some(Injection::Cancellation {
          mut template,
          codes,
          old_head,
        }) => {
          let writes = input["TransactItems"].as_array().unwrap();
          // 表名と実操作から実要求の位置を解く。期待された位置は使わない。
          let reasons = writes
            .iter()
            .map(|write| {
              assert_eq!(write.as_object().unwrap().len(), 1);
              let action = write.get("Put").or_else(|| write.get("Update")).unwrap();
              let table = action["TableName"].as_str().unwrap();
              let code = codes
                .iter()
                .find(|(name, _)| name == table)
                .map(|(_, code)| code.as_str())
                .unwrap_or("None");
              let mut reason = json!({"Code": code});
              if code == "ConditionalCheckFailed" && action["ReturnValuesOnConditionCheckFailure"] == "ALL_OLD" {
                if let Some(item) = &old_head {
                  reason["Item"] = item.clone();
                }
              }
              reason
            })
            .collect::<Vec<_>>();
          for (table, _) in &codes {
            assert!(writes
              .iter()
              .any(|write| { write.get("Put").or_else(|| write.get("Update")).unwrap()["TableName"] == *table }));
          }
          template["CancellationReasons"] = json!(reasons);
          template["Message"] = json!("TransactionConflict ConditionalCheckFailed SDK_SENTINEL");
          Some(template)
        }
        Some(Injection::Response(response)) => Some(response),
        Some(Injection::Communication) => {
          traces.lock().unwrap()[index] = json!({
            "api": api, "input": input, "upstream_body": null, "upstream_status": null,
            "injected_communication": "COMMUNICATION_CAUSE", "delivered_body": null,
          });
          return Err(ConnectorError::io(Box::new(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "COMMUNICATION_CAUSE",
          ))));
        }
        None => None,
      };
      let (response, upstream_body, upstream_status) = if let Some(body) = &injected {
        let mut response = Response::new(StatusCode::try_from(400).unwrap(), SdkBody::from(body.to_string()));
        response
          .headers_mut()
          .insert("content-type", "application/x-amz-json-1.0");
        response
          .headers_mut()
          .try_insert("x-amzn-errortype", body["__type"].as_str().unwrap().to_string())
          .unwrap();
        (response, None, None)
      } else {
        let mut response = upstream.call(request).await?;
        let body = std::mem::replace(response.body_mut(), SdkBody::taken());
        let bytes = ByteStream::new(body).collect().await.unwrap().into_bytes();
        let original = String::from_utf8(bytes.to_vec()).unwrap();
        let status = response.status().as_u16();
        *response.body_mut() = SdkBody::from(bytes);
        (response, Some(original), Some(status))
      };
      traces.lock().unwrap()[index] = json!({
        "api": api, "input": input, "upstream_body": upstream_body, "upstream_status": upstream_status,
        "delivered_body": std::str::from_utf8(response.body().bytes().unwrap()).unwrap(),
        "delivered_status": response.status().as_u16(), "injected_response": injected,
      });
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
    let upstream = aws_smithy_http_client::Builder::new().build_http();
    let records = traces.clone();
    let control = injection.clone();
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
      .credentials_provider(Credentials::new("x", "x", None, None, "local-test"))
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
}

impl Fixture {
  async fn new() -> Self {
    let container = docker::dynamodb_local().await.unwrap();
    let port = container.get_host_port_ipv4(docker::DYNAMODB_LOCAL_PORT).await.unwrap();
    let endpoint = format!("http://127.0.0.1:{port}");
    let raw = Observed::new(&endpoint);
    let names = dynamodb::TableNames {
      journal: "persist-first".into(),
      snapshot: "persist-second".into(),
      head: "persist-third".into(),
      snapshot_history_index: "persist-history".into(),
    };
    dynamodb::create_tables(&raw.client, &names, true).await.unwrap();
    raw.take();
    Self {
      container,
      raw,
      endpoint,
      tables: DynamoDbTables {
        journal_table_name: names.journal,
        snapshot_table_name: names.snapshot,
        head_table_name: names.head,
        snapshot_history_index_name: names.snapshot_history_index,
      },
    }
  }

  async fn open_json(&self) -> (JsonStore, Observed) {
    let observed = Observed::new(&self.endpoint);
    let store = JsonStore::open(observed.client.clone(), self.tables.clone(), options())
      .await
      .unwrap();
    self.record("open-json", &observed.take(), &json!(null));
    (store, observed)
  }

  async fn open_opaque(&self, serializer: Arc<BytesSerializer>) -> (OpaqueStore, Observed) {
    let observed = Observed::new(&self.endpoint);
    let store = OpaqueStore::open_with_serializers(
      observed.client.clone(),
      self.tables.clone(),
      options(),
      serializer,
      Arc::new(NoSnapshot),
    )
    .await
    .unwrap();
    self.record("open-opaque", &observed.take(), &json!(null));
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

  async fn query(&self, table: &str, aid: &str) -> Vec<Item> {
    self
      .raw
      .client
      .query()
      .table_name(table)
      .key_condition_expression("aid = :aid")
      .expression_attribute_values(":aid", AttributeValue::S(aid.into()))
      .consistent_read(true)
      .scan_index_forward(true)
      .send()
      .await
      .unwrap()
      .items
      .unwrap_or_default()
  }

  async fn state(&self, aid: &str) -> Value {
    let journal = self.query(&self.tables.journal_table_name, aid).await;
    let head = self.get(&self.tables.head_table_name, key(aid, None)).await;
    let snapshot = self.query(&self.tables.snapshot_table_name, aid).await;
    let mut config = serde_json::Map::new();
    for (table, sort) in [
      (&self.tables.journal_table_name, Some(("seq_nr", 0))),
      (&self.tables.snapshot_table_name, Some(("skey", 0))),
      (&self.tables.head_table_name, None),
    ] {
      config.insert(
        table.clone(),
        self
          .get(table, key("__config__", sort))
          .await
          .map(item_json)
          .unwrap_or(Value::Null),
      );
    }
    json!({
      "journal": journal.into_iter().map(item_json).collect::<Vec<_>>(),
      "head": head.map(item_json), "snapshot": snapshot.into_iter().map(item_json).collect::<Vec<_>>(),
      "configuration": config,
    })
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

  fn record(&self, name: &str, traces: &[Value], extra: &Value) {
    let observer = self.raw.take();
    if let Some(directory) = std::env::var_os("DYNAMODB_PERSIST_EVENT_EVIDENCE_DIR") {
      let directory = std::path::PathBuf::from(directory);
      std::fs::create_dir_all(&directory).unwrap();
      let value = json!({"tables": {
        "journal": self.tables.journal_table_name, "snapshot": self.tables.snapshot_table_name,
        "head": self.tables.head_table_name,
      }, "traces": traces, "observer_traces": observer, "extra": extra});
      // 同名のopen記録を別fixtureで上書きしない。
      let port = self.endpoint.rsplit(':').next().unwrap();
      std::fs::write(
        directory.join(format!("{name}-{port}.json")),
        serde_json::to_vec_pretty(&value).unwrap(),
      )
      .unwrap();
    }
  }

  async fn close(self) {
    for table in [
      &self.tables.journal_table_name,
      &self.tables.snapshot_table_name,
      &self.tables.head_table_name,
    ] {
      self.raw.client.delete_table().table_name(table).send().await.unwrap();
    }
    self.container.rm().await.unwrap();
  }
}

fn options() -> DynamoDbOptions {
  DynamoDbOptions {
    retention: RetentionSettings::keep_latest(1).with_mode(RetentionMode::Ttl { grace_seconds: 60 }),
    ..DynamoDbOptions::default()
  }
}

fn key(aid: &str, sort: Option<(&str, u64)>) -> Item {
  let mut item = HashMap::from([("aid".into(), AttributeValue::S(aid.into()))]);
  if let Some((name, number)) = sort {
    item.insert(name.into(), AttributeValue::N(number.to_string()));
  }
  item
}

fn attribute_json(value: AttributeValue) -> Value {
  match value {
    AttributeValue::S(value) => json!({"S": value}),
    AttributeValue::N(value) => json!({"N": value}),
    AttributeValue::B(value) => json!({"B": value.as_ref()}),
    AttributeValue::L(values) => json!({"L": values.into_iter().map(attribute_json).collect::<Vec<_>>()}),
    AttributeValue::M(values) => json!({"M": item_json(values)}),
    other => panic!("unexpected saved attribute: {other:?}"),
  }
}

fn item_json(item: Item) -> Value {
  Value::Object(
    item
      .into_iter()
      .map(|(name, value)| (name, attribute_json(value)))
      .collect(),
  )
}

fn event<P>(id: Id, seq_nr: u64, payload: P) -> EventEnvelope<Id, P> {
  EventEnvelope::new(
    id,
    seq_nr,
    DateTime::<Utc>::from_timestamp(-1, 123456789).unwrap(),
    payload,
  )
}

fn result_json(result: &Result<(), EventStoreError>) -> Value {
  match result {
    Ok(()) => json!({"success": true}),
    Err(EventStoreError::OptimisticLock {
      aid,
      seq_nr,
      head_seq_nr,
    }) => json!({"error": "optimistic-lock", "aid": aid, "seq_nr": seq_nr, "head_seq_nr": head_seq_nr}),
    Err(EventStoreError::ContractViolation { rule, seq_nr, .. }) => {
      json!({"error": "contract-violation", "rule": rule.to_string(), "seq_nr": seq_nr})
    }
    Err(EventStoreError::Serialization { phase, source }) => {
      json!({"error": "serialization", "phase": phase.to_string(), "source": source.to_string()})
    }
    Err(EventStoreError::Storage { operation, source }) => {
      json!({"error": "storage", "operation": operation.to_string(), "source": format!("{source:?}")})
    }
    Err(other) => panic!("unexpected persist result: {other:?}"),
  }
}

fn actions<'a>(fixture: &Fixture, trace: &'a Value) -> (&'a Value, &'a Value) {
  assert_eq!(trace["api"], "TransactWriteItems");
  let writes = trace["input"]["TransactItems"].as_array().unwrap();
  assert_eq!(writes.len(), 2);
  let mut journal = None;
  let mut head = None;
  for write in writes {
    assert_eq!(write.as_object().unwrap().len(), 1);
    let action = write.get("Put").or_else(|| write.get("Update")).unwrap();
    let table = action["TableName"].as_str().unwrap();
    if table == fixture.tables.journal_table_name {
      assert!(write.get("Put").is_some());
      assert_eq!(action["ConditionExpression"], "attribute_not_exists(aid)");
      assert!(journal.replace(action).is_none());
    } else {
      assert_eq!(table, fixture.tables.head_table_name);
      assert_eq!(action["ReturnValuesOnConditionCheckFailure"], "ALL_OLD");
      assert!(head.replace(action).is_none());
    }
  }
  (journal.unwrap(), head.unwrap())
}

fn assert_saved_event(item: &Value, aid: &str, seq_nr: u64, nanos: i64, manifest: &str, bytes: &[u8]) {
  assert_eq!(item.as_object().unwrap().len(), 5);
  assert_eq!(item["aid"], json!({"S": aid}));
  assert_eq!(item["seq_nr"], json!({"N": seq_nr.to_string()}));
  assert_eq!(item["occurred_at"], json!({"N": nanos.to_string()}));
  assert_eq!(item["manifest"], json!({"S": manifest}));
  assert_eq!(item["payload"], json!({"B": bytes}));
}

fn assert_head_matches_journal(state: &Value, type_name: &str) {
  let head = &state["head"];
  let journal = state["journal"].as_array().unwrap().last().unwrap();
  assert_eq!(head.as_object().unwrap().len(), 4);
  assert_eq!(head["aid"], journal["aid"]);
  assert_eq!(head["type_name"], json!({"S": type_name}));
  assert_eq!(head["seq_nr"], journal["seq_nr"]);
  let events = head["events"]["L"].as_array().unwrap();
  assert_eq!(events.len(), 1);
  let metadata = &events[0]["M"];
  assert_eq!(metadata.as_object().unwrap().len(), 4);
  for name in ["seq_nr", "occurred_at", "manifest", "payload"] {
    assert_eq!(metadata[name], journal[name]);
  }
}

#[tokio::test]
async fn should_commit_first_then_next_event_through_public_open() {
  let fixture = Fixture::new().await;
  let (store, observed) = fixture.open_json().await;
  let aid = "Account-連続-with-dash";
  // snapshotが先に存在してもイベント単独操作はsnapshotを照合・更新・保持しない。
  for number in [0, 1, 2] {
    let mut snapshot = key(aid, Some(("skey", number)));
    snapshot.insert("payload".into(), AttributeValue::B(Blob::new(vec![9, number as u8])));
    if number > 0 {
      snapshot.insert("active_history_seq_nr".into(), AttributeValue::N(number.to_string()));
    }
    fixture.put(&fixture.tables.snapshot_table_name, snapshot).await;
  }
  let before = fixture.state(aid).await;
  let id = Id::new("Account", "連続-with-dash");
  let calls = id.type_calls.clone();
  let payload1 = json!({"created": true, "text": "e\u{301}🙂"});
  let result1 = store.persist_event(event(id, 1, payload1.clone())).await;
  assert!(result1.is_ok(), "{result1:?}");
  assert_eq!(calls.load(Ordering::SeqCst), 1);
  let first = fixture.state(aid).await;
  assert_eq!(first["journal"].as_array().unwrap().len(), 1);
  assert_saved_event(
    &first["journal"][0],
    aid,
    1,
    -876543211,
    "",
    &serde_json::to_vec(&payload1).unwrap(),
  );
  assert_head_matches_journal(&first, "Account");
  let trace1 = observed.take();
  assert_eq!(trace1.len(), 1);
  let (journal1, head1) = actions(&fixture, &trace1[0]);
  assert_eq!(head1["ConditionExpression"], "attribute_not_exists(aid)");
  assert_eq!(head1["Item"]["events"]["L"].as_array().unwrap().len(), 1);
  assert_eq!(
    journal1["Item"]["payload"],
    head1["Item"]["events"]["L"][0]["M"]["payload"]
  );
  assert_eq!(trace1[0]["upstream_status"], 200);
  assert_eq!(trace1[0]["upstream_body"], trace1[0]["delivered_body"]);
  // 別Clientの実GetItem応答と送信時の項目を、バイナリ表現も含めて照合する。
  let saved1 = fixture
    .get(&fixture.tables.journal_table_name, key(aid, Some(("seq_nr", 1))))
    .await
    .unwrap();
  assert_eq!(item_json(saved1), first["journal"][0]);
  let raw_records = fixture.raw.traces.lock().unwrap().clone();
  let raw_journal = raw_records
    .iter()
    .find(|trace| {
      trace["api"] == "GetItem"
        && trace["input"]["TableName"] == fixture.tables.journal_table_name
        && trace["input"]["Key"]["seq_nr"]["N"] == "1"
    })
    .unwrap();
  let raw_body: Value = serde_json::from_str(raw_journal["upstream_body"].as_str().unwrap()).unwrap();
  assert_eq!(raw_body["Item"], journal1["Item"]);
  let raw_head = raw_records
    .iter()
    .rev()
    .find(|trace| {
      trace["api"] == "GetItem"
        && trace["input"]["TableName"] == fixture.tables.head_table_name
        && trace["input"]["Key"]["aid"]["S"] == aid
    })
    .unwrap();
  let raw_body: Value = serde_json::from_str(raw_head["upstream_body"].as_str().unwrap()).unwrap();
  assert_eq!(raw_body["Item"], head1["Item"]);
  fixture.record(
    "event1",
    &trace1,
    &json!({"before": before, "after": first, "result": result_json(&result1)}),
  );

  let payload2 = json!({"changed": [null, false, 42]});
  let nanos = i64::MAX;
  let event2 = EventEnvelope::new(
    Id::new("Account", "連続-with-dash"),
    2,
    DateTime::from_timestamp_nanos(nanos),
    payload2.clone(),
  )
  .with_manifest("manifest:e\u{301}🙂");
  let result2 = store.persist_event(event2).await;
  assert!(result2.is_ok(), "{result2:?}");
  let second = fixture.state(aid).await;
  assert_eq!(second["journal"].as_array().unwrap().len(), 2);
  assert_eq!(second["journal"][0], first["journal"][0]);
  assert_saved_event(
    &second["journal"][1],
    aid,
    2,
    nanos,
    "manifest:e\u{301}🙂",
    &serde_json::to_vec(&payload2).unwrap(),
  );
  assert_head_matches_journal(&second, "Account");
  assert_eq!(second["configuration"], before["configuration"]);
  assert_eq!(second["snapshot"], before["snapshot"]);
  let traces = observed.take();
  assert_eq!(traces.len(), 1);
  let (journal2, head2) = actions(&fixture, &traces[0]);
  assert_eq!(head2["ExpressionAttributeValues"][":prev"], json!({"N": "1"}));
  assert_eq!(head2["ExpressionAttributeValues"][":next"], json!({"N": "2"}));
  let values = &head2["ExpressionAttributeValues"];
  let (attribute, binding) = head2["ConditionExpression"].as_str().unwrap().split_once('=').unwrap();
  assert_eq!(attribute.trim(), "seq_nr");
  assert_eq!(values[binding.trim()], json!({"N": "1"}));
  let assignments = head2["UpdateExpression"]
    .as_str()
    .unwrap()
    .trim()
    .strip_prefix("SET ")
    .unwrap()
    .split(',')
    .map(|assignment| {
      let (attribute, binding) = assignment.split_once('=').unwrap();
      (attribute.trim(), &values[binding.trim()])
    })
    .collect::<HashMap<_, _>>();
  assert_eq!(assignments.len(), 2);
  assert_eq!(assignments["seq_nr"], &journal2["Item"]["seq_nr"]);
  assert_eq!(assignments["events"], &values[":events"]);
  assert_eq!(values[":events"]["L"].as_array().unwrap().len(), 1);
  assert_eq!(values[":events"]["L"][0]["M"]["payload"], journal2["Item"]["payload"]);
  assert_eq!(traces[0]["upstream_status"], 200);
  assert_eq!(traces[0]["upstream_body"], traces[0]["delivered_body"]);
  let saved2 = fixture
    .get(&fixture.tables.journal_table_name, key(aid, Some(("seq_nr", 2))))
    .await
    .unwrap();
  assert_eq!(item_json(saved2), second["journal"][1]);
  let raw_records = fixture.raw.traces.lock().unwrap().clone();
  let raw_journal = raw_records
    .iter()
    .rev()
    .find(|trace| {
      trace["api"] == "GetItem"
        && trace["input"]["TableName"] == fixture.tables.journal_table_name
        && trace["input"]["Key"]["seq_nr"]["N"] == "2"
    })
    .unwrap();
  let raw_body: Value = serde_json::from_str(raw_journal["upstream_body"].as_str().unwrap()).unwrap();
  assert_eq!(raw_body["Item"], journal2["Item"]);
  let raw_head = raw_records
    .iter()
    .rev()
    .find(|trace| {
      trace["api"] == "GetItem"
        && trace["input"]["TableName"] == fixture.tables.head_table_name
        && trace["input"]["Key"]["aid"]["S"] == aid
    })
    .unwrap();
  let raw_body: Value = serde_json::from_str(raw_head["upstream_body"].as_str().unwrap()).unwrap();
  assert_eq!(raw_body["Item"]["events"], values[":events"]);
  assert_eq!(raw_body["Item"]["seq_nr"], values[":next"]);
  assert_eq!(raw_body["Item"]["aid"], head2["Key"]["aid"]);
  fixture.record(
    "event2",
    &traces,
    &json!({"before": first, "after": second, "result": result_json(&result2)}),
  );
  fixture.close().await;
}

#[tokio::test]
async fn should_store_non_serde_non_clone_payload_with_its_serializer() {
  let fixture = Fixture::new().await;
  let serializer = Arc::new(BytesSerializer::default());
  let (store, observed) = fixture.open_opaque(serializer.clone()).await;
  let store = store.clone();
  let before = fixture.state("Opaque-1").await;
  for seq_nr in [1, 2] {
    let bytes = vec![0, 255, seq_nr as u8, 128];
    let result = store
      .persist_event(event(Id::new("Opaque", "1"), seq_nr, Opaque(bytes.clone())).with_manifest("opaque"))
      .await;
    assert!(result.is_ok(), "{result:?}");
    let saved = fixture.state("Opaque-1").await;
    assert_saved_event(
      &saved["journal"][(seq_nr - 1) as usize],
      "Opaque-1",
      seq_nr,
      -876543211,
      "opaque",
      &bytes,
    );
    assert_head_matches_journal(&saved, "Opaque");
    assert_eq!(saved["snapshot"], before["snapshot"]);
    assert_eq!(saved["configuration"], before["configuration"]);
    let traces = observed.take();
    assert_eq!(traces.len(), 1);
    actions(&fixture, &traces[0]);
    fixture.record(
      &format!("opaque-{seq_nr}"),
      &traces,
      &json!({"saved": saved, "result": result_json(&result)}),
    );
  }
  assert_eq!(serializer.calls.load(Ordering::SeqCst), 2);
  fixture.close().await;
}

#[tokio::test]
async fn should_reject_invalid_inputs_before_serialization_or_requests() {
  let fixture = Fixture::new().await;
  let serializer = Arc::new(BytesSerializer::default());
  let (store, observed) = fixture.open_opaque(serializer.clone()).await;
  let before = fixture.state("Account-input").await;
  let cases = [
    (
      "type",
      Id::new("Bad-Type", "input"),
      1,
      DateTime::from_timestamp_nanos(0),
      ContractRule::T11,
    ),
    (
      "aid-bytes",
      Id::new("Account", &"界".repeat(339)),
      1,
      DateTime::from_timestamp_nanos(0),
      ContractRule::T12,
    ),
    (
      "seq-limit",
      Id::new("Account", "input"),
      SEQ_NR_MAX + 1,
      DateTime::from_timestamp_nanos(0),
      ContractRule::T9,
    ),
    (
      "seq-zero",
      Id::new("Account", "input"),
      0,
      DateTime::from_timestamp_nanos(0),
      ContractRule::W6,
    ),
    (
      "time",
      Id::new("Account", "input"),
      1,
      DateTime::from_timestamp(9223372037, 0).unwrap(),
      ContractRule::T13,
    ),
  ];
  for (name, id, number, time, rule) in cases {
    let result = store
      .persist_event(EventEnvelope::new(id, number, time, Opaque(vec![1])))
      .await;
    assert!(matches!(&result, Err(EventStoreError::ContractViolation { rule: actual, .. }) if *actual == rule));
    assert_eq!(serializer.calls.load(Ordering::SeqCst), 0);
    let traces = observed.take();
    assert!(traces.is_empty());
    let after = fixture.state("Account-input").await;
    assert_eq!(after, before);
    fixture.record(
      &format!("reject-{name}"),
      &traces,
      &json!({"before": before, "after": after, "serializer_calls": 0, "result": result_json(&result)}),
    );
  }
  fixture.close().await;
}

#[tokio::test]
async fn should_keep_serializer_failure_cause_and_existing_storage_unchanged() {
  let fixture = Fixture::new().await;
  let serializer = Arc::new(BytesSerializer::default());
  let (store, observed) = fixture.open_opaque(serializer.clone()).await;
  store
    .persist_event(event(Id::new("Account", "serialization"), 1, Opaque(vec![1])))
    .await
    .unwrap();
  observed.take();
  let before = fixture.state("Account-serialization").await;
  serializer.fail.store(true, Ordering::SeqCst);
  let result = store
    .persist_event(event(Id::new("Account", "serialization"), 2, Opaque(vec![2])))
    .await;
  let error = result.as_ref().unwrap_err();
  assert!(matches!(
    error,
    EventStoreError::Serialization {
      phase: SerializationPhase::SerializeEvent,
      ..
    }
  ));
  assert!(!error.to_string().contains("SERIALIZER_CAUSE"));
  assert_eq!(
    error.source().unwrap().downcast_ref::<std::io::Error>().unwrap().kind(),
    std::io::ErrorKind::InvalidData
  );
  assert_eq!(serializer.calls.load(Ordering::SeqCst), 2);
  let after = fixture.state("Account-serialization").await;
  assert_eq!(after, before);
  let traces = observed.take();
  assert!(traces.is_empty());
  fixture.record(
    "reject-serialization",
    &traces,
    &json!({"before": before, "after": after, "result": result_json(&result)}),
  );
  fixture.close().await;
}

#[tokio::test]
async fn should_reject_journal_or_head_size_overflow_without_requests() {
  let fixture = Fixture::new().await;
  let (store, observed) = fixture.open_json().await;
  for seq_nr in [1, 2] {
    for (name, id, manifest, payload) in [
      ("payload", Id::new("Account", "size"), String::new(), "x".repeat(409600)),
      (
        "manifest",
        Id::new("Account", "size"),
        "界".repeat(136534),
        String::new(),
      ),
      (
        "head",
        Id::new(&"x".repeat(1022), ""),
        String::new(),
        "x".repeat(408000),
      ),
    ] {
      let aid = format!("{}-{}", id.type_name, id.value);
      let before = fixture.state(&aid).await;
      let result = store
        .persist_event(event(id, seq_nr, json!(payload)).with_manifest(manifest))
        .await;
      assert!(
        matches!(&result, Err(EventStoreError::ContractViolation { rule: ContractRule::ItemSizeLimit, seq_nr: Some(n), .. }) if *n == seq_nr)
      );
      let traces = observed.take();
      assert!(traces.is_empty());
      let after = fixture.state(&aid).await;
      assert_eq!(after, before);
      fixture.record(
        &format!("reject-size-{name}-{seq_nr}"),
        &traces,
        &json!({"before": before, "after": after, "result": result_json(&result)}),
      );
    }
  }
  fixture.close().await;
}

#[tokio::test]
async fn should_classify_real_head_and_journal_cancellations_without_reading() {
  let fixture = Fixture::new().await;
  let (store, observed) = fixture.open_json().await;
  for seq_nr in [1, 2, 3] {
    store
      .persist_event(event(Id::new("Account", "cancel"), seq_nr, json!(seq_nr)))
      .await
      .unwrap();
  }
  observed.take();
  for (name, value, seq_nr, expected) in [
    (
      "new-head",
      "cancel",
      1,
      json!({"error": "optimistic-lock", "aid": "Account-cancel", "seq_nr": 1, "head_seq_nr": null}),
    ),
    (
      "duplicate",
      "cancel",
      3,
      json!({"error": "optimistic-lock", "aid": "Account-cancel", "seq_nr": 3, "head_seq_nr": 3}),
    ),
    (
      "older",
      "cancel",
      2,
      json!({"error": "optimistic-lock", "aid": "Account-cancel", "seq_nr": 2, "head_seq_nr": 3}),
    ),
    (
      "gap",
      "cancel",
      5,
      json!({"error": "contract-violation", "rule": "W-8", "seq_nr": 5}),
    ),
    (
      "absent-head",
      "absent",
      2,
      json!({"error": "contract-violation", "rule": "W-8", "seq_nr": 2}),
    ),
  ] {
    let aid = format!("Account-{value}");
    let before = fixture.state(&aid).await;
    let result = store
      .persist_event(event(Id::new("Account", value), seq_nr, json!("loser")))
      .await;
    assert_eq!(result_json(&result), expected);
    let traces = observed.take();
    assert_eq!(traces.len(), 1);
    actions(&fixture, &traces[0]);
    assert_eq!(traces[0]["upstream_status"], 400);
    assert_eq!(traces[0]["upstream_body"], traces[0]["delivered_body"]);
    assert!(traces[0]["injected_response"].is_null());
    let body: Value = serde_json::from_str(traces[0]["upstream_body"].as_str().unwrap()).unwrap();
    let writes = traces[0]["input"]["TransactItems"].as_array().unwrap();
    let head_position = writes
      .iter()
      .position(|write| {
        write.get("Put").or_else(|| write.get("Update")).unwrap()["TableName"] == fixture.tables.head_table_name
      })
      .unwrap();
    assert_eq!(
      body["CancellationReasons"][head_position]["Code"],
      "ConditionalCheckFailed"
    );
    if value == "cancel" {
      assert_eq!(
        body["CancellationReasons"][head_position]["Item"]["seq_nr"],
        json!({"N": "3"})
      );
    } else {
      assert!(body["CancellationReasons"][head_position]["Item"].is_null());
    }
    let after = fixture.state(&aid).await;
    assert_eq!(after, before);
    fixture.record(
      &format!("real-{name}"),
      &traces,
      &json!({"before": before, "after": after, "result": result_json(&result)}),
    );
  }
  // head条件が成立する状態でjournalだけを重複させる。
  let mut duplicate = key("Account-cancel", Some(("seq_nr", 4)));
  duplicate.insert("payload".into(), AttributeValue::B(Blob::new(vec![77])));
  fixture.put(&fixture.tables.journal_table_name, duplicate).await;
  let before = fixture.state("Account-cancel").await;
  let result = store
    .persist_event(event(Id::new("Account", "cancel"), 4, json!("loser")))
    .await;
  assert!(matches!(
    result,
    Err(EventStoreError::OptimisticLock { head_seq_nr: None, .. })
  ));
  let traces = observed.take();
  assert_eq!(traces.len(), 1);
  actions(&fixture, &traces[0]);
  let body: Value = serde_json::from_str(traces[0]["upstream_body"].as_str().unwrap()).unwrap();
  let writes = traces[0]["input"]["TransactItems"].as_array().unwrap();
  for (position, write) in writes.iter().enumerate() {
    let table = &write.get("Put").or_else(|| write.get("Update")).unwrap()["TableName"];
    assert_eq!(
      body["CancellationReasons"][position]["Code"],
      if table == &fixture.tables.journal_table_name {
        "ConditionalCheckFailed"
      } else {
        "None"
      }
    );
  }
  let after = fixture.state("Account-cancel").await;
  assert_eq!(after, before);
  fixture.record(
    "real-journal",
    &traces,
    &json!({"before": before, "after": after, "result": result_json(&result)}),
  );
  fixture.close().await;
}

#[tokio::test]
async fn should_classify_injected_typed_reasons_and_keep_original_failure_causes() {
  let fixture = Fixture::new().await;
  let (store, observed) = fixture.open_json().await;
  store
    .persist_event(event(Id::new("Account", "injection"), 1, json!("committed")))
    .await
    .unwrap();
  observed.take();
  let actual_cancel = store
    .persist_event(event(Id::new("Account", "injection"), 1, json!("duplicate")))
    .await;
  assert!(matches!(actual_cancel, Err(EventStoreError::OptimisticLock { .. })));
  let traces = observed.take();
  let template: Value = serde_json::from_str(traces[0]["upstream_body"].as_str().unwrap()).unwrap();
  fixture.record(
    "injection-template-real-cancellation",
    &traces,
    &json!({"result": result_json(&actual_cancel)}),
  );
  let state = fixture.state("Account-injection").await;
  let head = fixture
    .get(&fixture.tables.head_table_name, key("Account-injection", None))
    .await
    .unwrap();
  let raw_head = fixture
    .raw
    .traces
    .lock()
    .unwrap()
    .iter()
    .rev()
    .find(|trace| trace["api"] == "GetItem" && trace["input"]["Key"]["aid"]["S"] == "Account-injection")
    .unwrap()
    .clone();
  let wire_head = serde_json::from_str::<Value>(raw_head["upstream_body"].as_str().unwrap()).unwrap()["Item"].clone();
  assert_eq!(item_json(head), state["head"]);
  let cases = [
    (
      "conflict-journal",
      "TransactionConflict",
      "ConditionalCheckFailed",
      3,
      Some(wire_head.clone()),
      "optimistic-lock",
    ),
    (
      "conflict-head",
      "ConditionalCheckFailed",
      "TransactionConflict",
      3,
      Some(wire_head.clone()),
      "optimistic-lock",
    ),
    (
      "head-before-journal",
      "ConditionalCheckFailed",
      "ConditionalCheckFailed",
      3,
      Some(wire_head.clone()),
      "contract-violation",
    ),
    (
      "head-before-other",
      "ThrottlingError",
      "ConditionalCheckFailed",
      3,
      Some(wire_head.clone()),
      "contract-violation",
    ),
    (
      "journal-before-other",
      "ConditionalCheckFailed",
      "ThrottlingError",
      2,
      None,
      "optimistic-lock",
    ),
    (
      "unexpected-previous",
      "None",
      "ConditionalCheckFailed",
      2,
      Some(wire_head.clone()),
      "storage",
    ),
    (
      "unexpected-before-journal",
      "ConditionalCheckFailed",
      "ConditionalCheckFailed",
      2,
      Some(wire_head.clone()),
      "storage",
    ),
    (
      "invalid-old-number",
      "None",
      "ConditionalCheckFailed",
      2,
      Some(json!({"seq_nr": {"S": "1"}})),
      "storage",
    ),
    (
      "missing-old-number",
      "None",
      "ConditionalCheckFailed",
      2,
      Some(json!({"aid": {"S": "Account-injection"}})),
      "storage",
    ),
    (
      "other-journal",
      "ProvisionedThroughputExceeded",
      "None",
      2,
      None,
      "storage",
    ),
    ("other-head", "None", "ThrottlingError", 2, None, "storage"),
  ];
  for (name, journal, head, seq_nr, old, expected) in cases {
    *observed.injection.lock().unwrap() = Some(Injection::Cancellation {
      template: template.clone(),
      codes: vec![
        (fixture.tables.journal_table_name.clone(), journal.into()),
        (fixture.tables.head_table_name.clone(), head.into()),
      ],
      old_head: old,
    });
    let result = store
      .persist_event(event(Id::new("Account", "injection"), seq_nr, json!("never-committed")))
      .await;
    assert_eq!(result_json(&result)["error"], expected);
    let error = result.as_ref().unwrap_err();
    assert!(!error.to_string().contains("SDK_SENTINEL"));
    let traces = observed.take();
    assert_eq!(traces.len(), 1);
    actions(&fixture, &traces[0]);
    assert!(traces[0]["upstream_body"].is_null());
    assert!(!traces[0]["injected_response"].is_null());
    if expected == "storage" {
      assert!(matches!(
        error,
        EventStoreError::Storage {
          operation: StorageOperation::Append,
          ..
        }
      ));
      let sdk = error
        .source()
        .unwrap()
        .downcast_ref::<SdkError<TransactWriteItemsError>>()
        .unwrap();
      let Some(TransactWriteItemsError::TransactionCanceledException(canceled)) = sdk.as_service_error() else {
        panic!("{sdk:?}")
      };
      let delivered: Value = serde_json::from_str(traces[0]["delivered_body"].as_str().unwrap()).unwrap();
      for (position, reason) in canceled.cancellation_reasons().iter().enumerate() {
        assert_eq!(
          reason.code(),
          delivered["CancellationReasons"][position]["Code"].as_str()
        );
      }
      assert_eq!(
        sdk.raw_response().unwrap().body().bytes().unwrap(),
        traces[0]["delivered_body"].as_str().unwrap().as_bytes()
      );
    } else {
      assert!(error.source().is_none());
    }
    let after = fixture.state("Account-injection").await;
    assert_eq!(after, state);
    assert!(observed.injection.lock().unwrap().is_none());
    fixture.record(
      &format!("injected-{name}"),
      &traces,
      &json!({"before": state, "after": after, "result": result_json(&result)}),
    );
  }
  for (name, injection) in [
    (
      "missing-reasons",
      Injection::Response(
        json!({"__type": template["__type"], "Message": "TransactionConflict ConditionalCheckFailed SDK_SENTINEL"}),
      ),
    ),
    (
      "other-service",
      Injection::Response(
        json!({"__type": "InternalServerError", "Message": "TransactionConflict ConditionalCheckFailed SDK_SENTINEL"}),
      ),
    ),
    ("communication", Injection::Communication),
  ] {
    *observed.injection.lock().unwrap() = Some(injection);
    let result = store
      .persist_event(event(Id::new("Account", "injection"), 2, json!("never-committed")))
      .await;
    let error = result.as_ref().unwrap_err();
    assert!(matches!(
      error,
      EventStoreError::Storage {
        operation: StorageOperation::Append,
        ..
      }
    ));
    let sdk = error
      .source()
      .unwrap()
      .downcast_ref::<SdkError<TransactWriteItemsError>>()
      .unwrap();
    if name == "communication" {
      let SdkError::DispatchFailure(failure) = sdk else {
        panic!("{sdk:?}")
      };
      let cause = failure
        .as_connector_error()
        .unwrap()
        .source()
        .unwrap()
        .downcast_ref::<std::io::Error>()
        .unwrap();
      assert_eq!(cause.kind(), std::io::ErrorKind::ConnectionReset);
      assert_eq!(cause.to_string(), "COMMUNICATION_CAUSE");
    } else {
      match (name, sdk.as_service_error().unwrap()) {
        ("missing-reasons", TransactWriteItemsError::TransactionCanceledException(canceled)) => {
          assert!(canceled.cancellation_reasons().is_empty());
        }
        ("other-service", TransactWriteItemsError::InternalServerError(_)) => {}
        _ => panic!("{sdk:?}"),
      }
    }
    assert!(!error.to_string().contains("SDK_SENTINEL"));
    let traces = observed.take();
    assert_eq!(traces.len(), 1);
    actions(&fixture, &traces[0]);
    assert!(traces[0]["upstream_body"].is_null());
    if name != "communication" {
      assert_eq!(
        sdk.raw_response().unwrap().body().bytes().unwrap(),
        traces[0]["delivered_body"].as_str().unwrap().as_bytes()
      );
    }
    let after = fixture.state("Account-injection").await;
    assert_eq!(after, state);
    fixture.record(
      &format!("injected-{name}"),
      &traces,
      &json!({"before": state, "after": after, "result": result_json(&result)}),
    );
  }
  fixture.close().await;
}

#[tokio::test]
async fn should_commit_exactly_one_parallel_event_for_both_new_and_existing_head() {
  let fixture = Fixture::new().await;
  let (store, observed) = fixture.open_json().await;
  let mut previous = fixture.state("Account-parallel").await;
  for seq_nr in [1, 2] {
    let payloads = [
      json!({"candidate": "left", "seq": seq_nr}),
      json!({"candidate": "right", "seq": seq_nr}),
    ];
    let (left, right) = tokio::join!(
      store.persist_event(event(Id::new("Account", "parallel"), seq_nr, payloads[0].clone())),
      store.persist_event(event(Id::new("Account", "parallel"), seq_nr, payloads[1].clone())),
    );
    let results = [left, right];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    let winner = results.iter().position(Result::is_ok).unwrap();
    let loser = results[1 - winner].as_ref().unwrap_err();
    assert!(matches!(loser, EventStoreError::OptimisticLock { .. }));
    let after = fixture.state("Account-parallel").await;
    assert_eq!(after["journal"].as_array().unwrap().len(), seq_nr as usize);
    assert_saved_event(
      &after["journal"][(seq_nr - 1) as usize],
      "Account-parallel",
      seq_nr,
      -876543211,
      "",
      &serde_json::to_vec(&payloads[winner]).unwrap(),
    );
    assert_head_matches_journal(&after, "Account");
    if seq_nr == 2 {
      assert_eq!(after["journal"][0], previous["journal"][0]);
    }
    assert_eq!(after["snapshot"], previous["snapshot"]);
    assert_eq!(after["configuration"], previous["configuration"]);
    let traces = observed.take();
    assert_eq!(traces.len(), 2);
    assert_eq!(traces.iter().filter(|trace| trace["upstream_status"] == 200).count(), 1);
    assert_eq!(traces.iter().filter(|trace| trace["upstream_status"] == 400).count(), 1);
    for trace in &traces {
      actions(&fixture, trace);
      assert!(trace["injected_response"].is_null());
      assert_eq!(trace["upstream_body"], trace["delivered_body"]);
    }
    fixture.record(
      &format!("parallel-{seq_nr}"),
      &traces,
      &json!({"before": previous, "after": after, "results": results.iter().map(result_json).collect::<Vec<_>>() }),
    );
    previous = after;
  }
  fixture.close().await;
}
