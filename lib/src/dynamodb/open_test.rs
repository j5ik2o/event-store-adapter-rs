//! 公開生成のSDK要求・元応答・保存属性を固定DynamoDB Localで観測する。

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aws_sdk_dynamodb::config::{
  AsyncSleep, BehaviorVersion, Credentials, Region, Sleep, StalledStreamProtectionConfig,
};
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client;
use aws_smithy_runtime_api::client::http::{
  http_client_fn, HttpClient, HttpConnector, HttpConnectorFuture, SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::HttpRequest;
use aws_smithy_runtime_api::http::{Response, StatusCode};
use aws_smithy_types::retry::RetryConfig;
use aws_smithy_types::timeout::TimeoutConfig;
use aws_smithy_types::{body::SdkBody, byte_stream::ByteStream};
use event_store_adapter_test_utils_rs::{docker, dynamodb};
use serde_json::{json, Value};
use testcontainers::{ContainerAsync, GenericImage};
use tokio::sync::Notify;

use super::{DynamoDbOptions, DynamoDbTables, EventStoreForDynamoDB};
use crate::aggregate_id::AggregateId;
use crate::error::{ConfigurationReason, EventStoreError, StorageOperation};
use crate::retention::RetentionSettings;
use crate::serializer::{EventSerializer, SnapshotSerializer};

type Item = HashMap<String, AttributeValue>;
type Traces = Arc<Mutex<Vec<Value>>>;

#[derive(Debug, Clone)]
struct Id;

impl AggregateId for Id {
  fn type_name(&self) -> String {
    "Test".into()
  }

  fn value(&self) -> String {
    "1".into()
  }
}

// serde・Debug・Cloneを実装しない型で任意serializer入口の境界を確認する。
struct Opaque(Vec<u8>);

#[derive(Debug)]
struct BytesSerializer;

impl EventSerializer<Opaque> for BytesSerializer {
  fn serialize(&self, payload: &Opaque) -> Result<Vec<u8>, EventStoreError> {
    Ok(payload.0.clone())
  }

  fn deserialize(&self, data: &[u8]) -> Result<Opaque, EventStoreError> {
    Ok(Opaque(data.to_vec()))
  }
}

impl SnapshotSerializer<Opaque> for BytesSerializer {
  fn serialize(&self, aggregate: &Opaque) -> Result<Vec<u8>, EventStoreError> {
    Ok(aggregate.0.clone())
  }

  fn deserialize(&self, data: &[u8]) -> Result<Opaque, EventStoreError> {
    Ok(Opaque(data.to_vec()))
  }
}

#[derive(Debug, Clone, Copy)]
enum Entry {
  Json,
  Custom,
}

impl Entry {
  async fn open(
    self,
    client: Client,
    tables: DynamoDbTables,
    options: DynamoDbOptions,
  ) -> Result<String, EventStoreError> {
    match self {
      Self::Json => {
        let store = EventStoreForDynamoDB::<Id, Value, Value>::open(client, tables, options).await?;
        let payload = json!({"value": 1});
        let bytes = store.event_serializer.serialize(&payload).unwrap();
        assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap(), payload);
        assert_eq!(store.snapshot_serializer.deserialize(&bytes).unwrap(), payload);
        Ok(store.store_id)
      }
      Self::Custom => {
        let store = EventStoreForDynamoDB::<Id, Opaque, Opaque>::open_with_serializers(
          client,
          tables,
          options,
          Arc::new(BytesSerializer),
          Arc::new(BytesSerializer),
        )
        .await?;
        let cloned = store.clone();
        let payload = Opaque(vec![0, 255, 1]);
        let bytes = cloned.event_serializer.serialize(&payload).unwrap();
        assert_eq!(bytes, payload.0);
        assert_eq!(cloned.snapshot_serializer.deserialize(&bytes).unwrap().0, payload.0);
        assert!(format!("{cloned:?}").contains(&store.store_id));
        Ok(store.store_id)
      }
    }
  }
}

#[derive(Debug, Clone, Default)]
struct Waits(Arc<Mutex<Vec<Duration>>>);

impl AsyncSleep for Waits {
  fn sleep(&self, duration: Duration) -> Sleep {
    let waits = self.clone();
    Sleep::new(async move {
      waits.0.lock().unwrap().push(duration);
    })
  }
}

#[derive(Debug, Default)]
struct Gate {
  reached: Notify,
  resume: Notify,
}

#[derive(Debug, Default)]
struct Control {
  gate: Option<Arc<Gate>>,
  create_error: Option<Value>,
  unprocessed_reads: u32,
  pending_tables: Vec<String>,
}

#[derive(Debug)]
struct ObservedConnector {
  upstream: SharedHttpConnector,
  control: Arc<Mutex<Control>>,
  traces: Traces,
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
    let (gate, error, pending) = {
      let mut control = self.control.lock().unwrap();
      if api == "TransactWriteItems" {
        (control.gate.take(), control.create_error.take(), Vec::new())
      } else if api == "BatchGetItem" && control.unprocessed_reads > 0 {
        control.unprocessed_reads -= 1;
        (None, None, control.pending_tables.clone())
      } else {
        (None, None, Vec::new())
      }
    };
    let traces = self.traces.clone();
    let upstream = self.upstream.clone();
    HttpConnectorFuture::new(async move {
      if let Some(gate) = gate {
        gate.reached.notify_one();
        gate.resume.notified().await;
      }
      let (mut response, original_body, original_status) = if let Some(error) = &error {
        let mut response = Response::new(StatusCode::try_from(400).unwrap(), SdkBody::from(error.to_string()));
        response
          .headers_mut()
          .insert("content-type", "application/x-amz-json-1.0");
        response
          .headers_mut()
          .try_insert("x-amzn-errortype", error["__type"].as_str().unwrap().to_string())
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
      if !pending.is_empty() {
        assert!(response.status().is_success());
        // 実上流応答の属性を使い、指定した実要求キーだけを未処理へ移す。
        let mut delivered: Value = serde_json::from_str(original_body.as_ref().unwrap()).unwrap();
        assert!(delivered["UnprocessedKeys"]
          .as_object()
          .is_none_or(|keys| keys.is_empty()));
        let mut unprocessed = serde_json::Map::new();
        for table in pending {
          let keys = input["RequestItems"]
            .get(&table)
            .expect("実際に送った表だけを対象にする");
          delivered["Responses"].as_object_mut().unwrap().remove(&table);
          unprocessed.insert(table, keys.clone());
        }
        delivered["UnprocessedKeys"] = Value::Object(unprocessed);
        *response.body_mut() = SdkBody::from(delivered.to_string());
      }
      traces.lock().unwrap().push(json!({
        "api": api, "input": input,
        "upstream_body": original_body, "upstream_status": original_status,
        "delivered_body": std::str::from_utf8(response.body().bytes().unwrap()).unwrap(),
        "delivered_status": response.status().as_u16(),
        "injected_create_error": error,
      }));
      Ok(response)
    })
  }
}

struct Fixture {
  container: ContainerAsync<GenericImage>,
  raw: Client,
  endpoint: String,
  tables: DynamoDbTables,
}

impl Fixture {
  async fn new() -> Self {
    let container = docker::dynamodb_local().await.unwrap();
    let port = container.get_host_port_ipv4(docker::DYNAMODB_LOCAL_PORT).await.unwrap();
    let raw = dynamodb::create_dynamodb_local_client(port);
    let names = dynamodb::TableNames {
      journal: "open-first".into(),
      snapshot: "open-second".into(),
      head: "open-third".into(),
      snapshot_history_index: "open-history".into(),
    };
    dynamodb::create_tables(&raw, &names, false).await.unwrap();
    Self {
      container,
      raw,
      endpoint: format!("http://127.0.0.1:{port}"),
      tables: DynamoDbTables {
        journal_table_name: names.journal,
        snapshot_table_name: names.snapshot,
        head_table_name: names.head,
        snapshot_history_index_name: names.snapshot_history_index,
      },
    }
  }

  fn names(&self) -> [&str; 3] {
    [
      &self.tables.journal_table_name,
      &self.tables.snapshot_table_name,
      &self.tables.head_table_name,
    ]
  }

  fn observed(&self, control: Control, sleeper: Option<Waits>) -> (Client, Traces) {
    let traces: Traces = Arc::new(Mutex::new(Vec::new()));
    let control = Arc::new(Mutex::new(control));
    let upstream = aws_smithy_http_client::Builder::new().build_http();
    let records = traces.clone();
    let http = http_client_fn(move |settings, components| {
      SharedHttpConnector::new(ObservedConnector {
        upstream: upstream.http_connector(settings, components),
        control: control.clone(),
        traces: records.clone(),
      })
    });
    let mut builder = aws_sdk_dynamodb::Config::builder()
      .behavior_version(BehaviorVersion::latest())
      .region(Region::new("us-west-1"))
      .credentials_provider(Credentials::new("x", "x", None, None, "local-test"))
      .endpoint_url(&self.endpoint)
      .http_client(http)
      .retry_config(RetryConfig::disabled())
      .timeout_config(TimeoutConfig::disabled())
      .stalled_stream_protection(StalledStreamProtectionConfig::disabled());
    if let Some(sleeper) = sleeper {
      builder = builder.sleep_impl(sleeper);
    }
    (Client::from_conf(builder.build()), traces)
  }

  async fn seed(&self, items: [Option<Item>; 3]) {
    for (index, (table, item)) in self.names().into_iter().zip(items).enumerate() {
      self
        .raw
        .delete_item()
        .table_name(table)
        .set_key(Some(key(index)))
        .send()
        .await
        .unwrap();
      if let Some(item) = item {
        self
          .raw
          .put_item()
          .table_name(table)
          .set_item(Some(item))
          .send()
          .await
          .unwrap();
      }
    }
  }

  async fn saved(&self) -> Value {
    let mut saved = serde_json::Map::new();
    for (index, table) in self.names().into_iter().enumerate() {
      let output = self
        .raw
        .get_item()
        .table_name(table)
        .set_key(Some(key(index)))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
      let item = output.item.map(|item| {
        Value::Object(
          item
            .into_iter()
            .map(|(name, value)| {
              let attribute = match value {
                AttributeValue::S(value) => json!({"S": value}),
                AttributeValue::N(value) => json!({"N": value}),
                other => panic!("unexpected configuration attribute: {other:?}"),
              };
              (name, attribute)
            })
            .collect(),
        )
      });
      saved.insert(table.into(), item.unwrap_or(Value::Null));
    }
    Value::Object(saved)
  }

  async fn record(
    &self,
    name: &str,
    entry: Entry,
    traces: &Traces,
    result: &Result<String, EventStoreError>,
    extra: Value,
  ) {
    if let Some(directory) = std::env::var_os("DYNAMODB_OPEN_EVIDENCE_DIR") {
      let result = match result {
        Ok(store_id) => json!({"store_id": store_id}),
        Err(EventStoreError::Configuration { reason }) => {
          json!({"error": "configuration", "reason": reason.to_string()})
        }
        Err(EventStoreError::Storage { operation, source }) => {
          json!({"error": "storage", "operation": operation.to_string(), "source": source.to_string()})
        }
        Err(other) => panic!("unexpected open result: {other}"),
      };
      let saved = self.saved().await;
      let records = traces.lock().unwrap().clone();
      let evidence = json!({
        "entry": format!("{entry:?}"), "result": result, "traces": records,
        "saved": saved, "extra": extra,
      });
      let directory = std::path::PathBuf::from(directory);
      std::fs::create_dir_all(&directory).unwrap();
      std::fs::write(
        directory.join(format!("{name}-{entry:?}.json")),
        serde_json::to_vec_pretty(&evidence).unwrap(),
      )
      .unwrap();
    }
  }

  async fn close(self) {
    for table in self.names() {
      self.raw.delete_table().table_name(table).send().await.unwrap();
    }
    self.container.rm().await.unwrap();
  }
}

fn key(index: usize) -> Item {
  let mut item = Item::from([("aid".into(), AttributeValue::S("__config__".into()))]);
  if index < 2 {
    item.insert(
      if index == 0 { "seq_nr" } else { "skey" }.into(),
      AttributeValue::N("0".into()),
    );
  }
  item
}

fn config(index: usize, id: &str, version: &str) -> Item {
  let mut item = key(index);
  item.insert("store_id".into(), AttributeValue::S(id.into()));
  item.insert("layout_version".into(), AttributeValue::N(version.into()));
  item
}

fn cancellation(code: &str, position: usize) -> Value {
  let mut reasons = vec![json!({"Code": "None"}); 3];
  reasons[position] = json!({"Code": code});
  json!({"__type": "TransactionCanceledException", "Message": "SDK_SENTINEL http://SECRET_ENDPOINT", "CancellationReasons": reasons})
}

fn assert_read(fixture: &Fixture, trace: &Value, indices: &[usize]) {
  assert_eq!(trace["api"], "BatchGetItem");
  let sent = trace["input"]["RequestItems"].as_object().unwrap();
  assert_eq!(sent.len(), indices.len());
  for index in indices {
    let attributes = &sent[fixture.names()[*index]];
    assert_eq!(attributes["ConsistentRead"], true);
    let mut expected = json!({"aid": {"S": "__config__"}});
    if *index < 2 {
      expected[if *index == 0 { "seq_nr" } else { "skey" }] = json!({"N": "0"});
    }
    assert_eq!(attributes["Keys"], json!([expected]));
  }
}

fn assert_create(fixture: &Fixture, trace: &Value) -> String {
  assert_eq!(trace["api"], "TransactWriteItems");
  let writes = trace["input"]["TransactItems"].as_array().unwrap();
  assert_eq!(writes.len(), 3);
  let mut tables = HashSet::new();
  let mut id = None;
  for write in writes {
    assert_eq!(write.as_object().unwrap().len(), 1, "独立ConditionCheckなどを含めない");
    let put = write.get("Put").unwrap();
    let table = put["TableName"].as_str().unwrap();
    let index = fixture.names().iter().position(|name| *name == table).unwrap();
    assert!(tables.insert(table));
    assert_eq!(put["ConditionExpression"], "attribute_not_exists(aid)");
    let item = put["Item"].as_object().unwrap();
    assert_eq!(item.len(), if index < 2 { 4 } else { 3 });
    assert_eq!(item["aid"], json!({"S": "__config__"}));
    assert_eq!(item["layout_version"], json!({"N": "1"}));
    if index < 2 {
      assert_eq!(item[if index == 0 { "seq_nr" } else { "skey" }], json!({"N": "0"}));
    }
    assert!(!item.contains_key("active_history_seq_nr"));
    let current_id = item["store_id"]["S"].as_str().unwrap();
    assert!(!current_id.is_empty());
    if let Some(previous) = id {
      assert_eq!(current_id, previous);
    }
    id = Some(current_id);
  }
  id.unwrap().to_string()
}

fn options() -> DynamoDbOptions {
  DynamoDbOptions {
    unprocessed_retry_limit: 1,
    unprocessed_retry_initial_delay: Duration::from_millis(7),
    unprocessed_retry_max_delay: Duration::from_millis(20),
    ..DynamoDbOptions::default()
  }
}

#[tokio::test]
async fn should_open_existing_configuration_through_both_public_entries() {
  let fixture = Fixture::new().await;
  fixture
    .seed(std::array::from_fn(|index| Some(config(index, "stored-id", "1"))))
    .await;
  let saved = fixture.saved().await;
  for entry in [Entry::Json, Entry::Custom] {
    let (client, traces) = fixture.observed(Control::default(), Some(Waits::default()));
    let result = entry.open(client, fixture.tables.clone(), options()).await;
    assert_eq!(result.as_ref().unwrap(), "stored-id");
    let observed = traces.lock().unwrap().clone();
    assert_eq!(observed.len(), 1);
    assert_read(&fixture, &observed[0], &[0, 1, 2]);
    assert_eq!(observed[0]["upstream_body"], observed[0]["delivered_body"]);
    assert_eq!(fixture.saved().await, saved);
    fixture.record("existing", entry, &traces, &result, Value::Null).await;
  }
  fixture.close().await;
}

#[tokio::test]
async fn should_create_three_configurations_atomically_through_both_public_entries() {
  let mut ids = HashSet::new();
  for entry in [Entry::Json, Entry::Custom] {
    let fixture = Fixture::new().await;
    let (client, traces) = fixture.observed(Control::default(), Some(Waits::default()));
    let result = entry.open(client, fixture.tables.clone(), options()).await;
    let observed = traces.lock().unwrap().clone();
    assert_eq!(observed.len(), 2);
    assert_read(&fixture, &observed[0], &[0, 1, 2]);
    let id = assert_create(&fixture, &observed[1]);
    assert_eq!(result.as_ref().unwrap(), &id);
    assert!(ids.insert(id));
    assert_eq!(observed[1]["upstream_status"], 200);
    let saved = fixture.saved().await;
    for write in observed[1]["input"]["TransactItems"].as_array().unwrap() {
      let put = &write["Put"];
      assert_eq!(saved[put["TableName"].as_str().unwrap()], put["Item"]);
    }
    fixture.record("new", entry, &traces, &result, Value::Null).await;
    fixture.close().await;
  }
}

async fn complete_race(entry: Entry, injected: Option<Value>, name: &str) {
  let fixture = Fixture::new().await;
  let gate = Arc::new(Gate::default());
  let (client, traces) = fixture.observed(
    Control {
      gate: Some(gate.clone()),
      create_error: injected,
      ..Control::default()
    },
    Some(Waits::default()),
  );
  let tables = fixture.tables.clone();
  let loser = tokio::spawn(async move { entry.open(client, tables, options()).await });
  tokio::time::timeout(Duration::from_secs(10), gate.reached.notified())
    .await
    .unwrap();
  let (client, winner_traces) = fixture.observed(Control::default(), Some(Waits::default()));
  let winner_entry = match entry {
    Entry::Json => Entry::Custom,
    Entry::Custom => Entry::Json,
  };
  let winner = winner_entry.open(client, fixture.tables.clone(), options()).await;
  assert!(winner.is_ok());
  gate.resume.notify_one();
  let result = tokio::time::timeout(Duration::from_secs(10), loser)
    .await
    .unwrap()
    .unwrap();
  assert_eq!(result.as_ref().unwrap(), winner.as_ref().unwrap());
  let observed = traces.lock().unwrap().clone();
  assert_eq!(observed.len(), 3);
  assert_read(&fixture, &observed[0], &[0, 1, 2]);
  let candidate = assert_create(&fixture, &observed[1]);
  assert_ne!(&candidate, winner.as_ref().unwrap());
  assert_read(&fixture, &observed[2], &[0, 1, 2]);
  if observed[1]["injected_create_error"].is_null() {
    assert_eq!(observed[1]["upstream_status"], 400);
    let actual: Value = serde_json::from_str(observed[1]["upstream_body"].as_str().unwrap()).unwrap();
    assert!(actual["CancellationReasons"]
      .as_array()
      .unwrap()
      .iter()
      .any(|reason| reason["Code"] == "ConditionalCheckFailed"));
    assert_eq!(observed[1]["upstream_body"], observed[1]["delivered_body"]);
  } else {
    assert!(observed[1]["upstream_body"].is_null());
  }
  let saved = fixture.saved().await;
  for table in fixture.names() {
    assert_eq!(saved[table]["store_id"]["S"], *winner.as_ref().unwrap());
  }
  let winner_records = winner_traces.lock().unwrap().clone();
  fixture
    .record(
      name,
      entry,
      &traces,
      &result,
      json!({"winner_traces": winner_records, "winner_store_id": winner.unwrap()}),
    )
    .await;
  fixture.close().await;
}

#[tokio::test]
async fn should_converge_after_a_real_creation_race() {
  for entry in [Entry::Json, Entry::Custom] {
    complete_race(entry, None, "real-race").await;
  }
}

#[tokio::test]
async fn should_converge_after_transaction_conflict_at_every_position() {
  for entry in [Entry::Json, Entry::Custom] {
    for position in 0..3 {
      complete_race(
        entry,
        Some(cancellation("TransactionConflict", position)),
        &format!("transaction-conflict-{position}"),
      )
      .await;
    }
  }
}

#[tokio::test]
async fn should_stop_with_storage_after_cancellation_and_an_absent_reread() {
  let fixture = Fixture::new().await;
  for entry in [Entry::Json, Entry::Custom] {
    for code in ["ConditionalCheckFailed", "TransactionConflict"] {
      for position in 0..3 {
        let (client, traces) = fixture.observed(
          Control {
            create_error: Some(cancellation(code, position)),
            ..Control::default()
          },
          Some(Waits::default()),
        );
        let result = entry.open(client, fixture.tables.clone(), options()).await;
        let error = result.as_ref().unwrap_err();
        assert!(!error.to_string().contains("SDK_SENTINEL"));
        let EventStoreError::Storage { operation, source } = error else {
          panic!("{error:?}")
        };
        assert_eq!(*operation, StorageOperation::CreateConfiguration);
        let sdk = source.downcast_ref::<SdkError<TransactWriteItemsError>>().unwrap();
        let Some(TransactWriteItemsError::TransactionCanceledException(canceled)) = sdk.as_service_error() else {
          panic!("{sdk:?}")
        };
        assert_eq!(canceled.cancellation_reasons()[position].code(), Some(code));
        let observed = traces.lock().unwrap().clone();
        assert_eq!(observed.len(), 3);
        assert_read(&fixture, &observed[0], &[0, 1, 2]);
        assert_create(&fixture, &observed[1]);
        assert_read(&fixture, &observed[2], &[0, 1, 2]);
        assert!(fixture.saved().await.as_object().unwrap().values().all(Value::is_null));
        fixture
          .record(
            &format!("absent-{code}-{position}"),
            entry,
            &traces,
            &result,
            Value::Null,
          )
          .await;
      }
    }
  }
  fixture.close().await;
}

#[tokio::test]
async fn should_propagate_configuration_failure_after_a_creation_conflict() {
  let fixture = Fixture::new().await;
  for entry in [Entry::Json, Entry::Custom] {
    fixture.seed([None, None, None]).await;
    let gate = Arc::new(Gate::default());
    let (client, traces) = fixture.observed(
      Control {
        gate: Some(gate.clone()),
        create_error: Some(cancellation("TransactionConflict", 1)),
        ..Control::default()
      },
      Some(Waits::default()),
    );
    let tables = fixture.tables.clone();
    let open = tokio::spawn(async move { entry.open(client, tables, options()).await });
    tokio::time::timeout(Duration::from_secs(10), gate.reached.notified())
      .await
      .unwrap();
    fixture.seed([Some(config(0, "partial-winner", "1")), None, None]).await;
    gate.resume.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(10), open)
      .await
      .unwrap()
      .unwrap();
    assert!(matches!(
      result,
      Err(EventStoreError::Configuration {
        reason: ConfigurationReason::PartialDynamoDbConfiguration,
      })
    ));
    let observed = traces.lock().unwrap().clone();
    assert_eq!(observed.len(), 3);
    assert_read(&fixture, &observed[0], &[0, 1, 2]);
    assert_create(&fixture, &observed[1]);
    assert_read(&fixture, &observed[2], &[0, 1, 2]);
    fixture
      .record("conflict-partial", entry, &traces, &result, Value::Null)
      .await;
  }
  fixture.close().await;
}

#[tokio::test]
async fn should_propagate_existing_configuration_failures_through_both_entries() {
  let fixture = Fixture::new().await;
  let cases = [
    (
      [Some(config(0, "same", "1")), None, None],
      ConfigurationReason::PartialDynamoDbConfiguration,
    ),
    (
      [
        Some(config(0, "first", "1")),
        Some(config(1, "second", "1")),
        Some(config(2, "first", "1")),
      ],
      ConfigurationReason::DynamoDbStoreIdMismatch,
    ),
    (
      std::array::from_fn(|index| Some(config(index, "same", "2"))),
      ConfigurationReason::UnsupportedDynamoDbLayoutVersion,
    ),
  ];
  for (index, (items, expected)) in cases.into_iter().enumerate() {
    fixture.seed(items).await;
    let saved = fixture.saved().await;
    for entry in [Entry::Json, Entry::Custom] {
      let (client, traces) = fixture.observed(Control::default(), Some(Waits::default()));
      let result = entry.open(client, fixture.tables.clone(), options()).await;
      assert!(matches!(&result, Err(EventStoreError::Configuration { reason }) if *reason == expected));
      assert_eq!(traces.lock().unwrap().len(), 1);
      assert_eq!(fixture.saved().await, saved);
      fixture
        .record(
          &format!("configuration-error-{index}"),
          entry,
          &traces,
          &result,
          Value::Null,
        )
        .await;
    }
  }
  fixture.close().await;
}

#[tokio::test]
async fn should_reject_invalid_generation_settings_before_sending_requests() {
  let fixture = Fixture::new().await;
  for entry in [Entry::Json, Entry::Custom] {
    for (left, right) in [(0, 1), (0, 2), (1, 2)] {
      let mut tables = fixture.tables.clone();
      let name = fixture.names()[left].to_string();
      if right == 1 {
        tables.snapshot_table_name = name;
      } else {
        tables.head_table_name = name;
      }
      let (client, traces) = fixture.observed(Control::default(), Some(Waits::default()));
      let result = entry.open(client, tables, options()).await;
      assert!(matches!(
        result,
        Err(EventStoreError::Configuration {
          reason: ConfigurationReason::DuplicateDynamoDbTableNames
        })
      ));
      assert!(traces.lock().unwrap().is_empty());
      fixture
        .record(
          &format!("duplicate-{left}-{right}"),
          entry,
          &traces,
          &result,
          Value::Null,
        )
        .await;
    }
    let (client, traces) = fixture.observed(Control::default(), None);
    assert!(client.config().sleep_impl().is_none());
    let result = entry.open(client, fixture.tables.clone(), options()).await;
    assert!(matches!(
      result,
      Err(EventStoreError::Configuration {
        reason: ConfigurationReason::MissingRetrySleeper
      })
    ));
    assert!(traces.lock().unwrap().is_empty());
    fixture
      .record("missing-sleeper", entry, &traces, &result, Value::Null)
      .await;
    let (client, traces) = fixture.observed(Control::default(), Some(Waits::default()));
    let result = entry
      .open(
        client,
        fixture.tables.clone(),
        DynamoDbOptions {
          retention: RetentionSettings::keep_latest(0),
          ..options()
        },
      )
      .await;
    assert!(matches!(
      result,
      Err(EventStoreError::Configuration {
        reason: ConfigurationReason::KeepSnapshotCountZero
      })
    ));
    assert!(traces.lock().unwrap().is_empty());
    fixture
      .record("invalid-retention", entry, &traces, &result, Value::Null)
      .await;
  }
  fixture.close().await;
}

#[tokio::test]
async fn should_retry_only_unprocessed_keys_before_accepting_existing_settings() {
  let fixture = Fixture::new().await;
  fixture
    .seed(std::array::from_fn(|index| Some(config(index, "stored-id", "1"))))
    .await;
  for entry in [Entry::Json, Entry::Custom] {
    let waits = Waits::default();
    let (client, traces) = fixture.observed(
      Control {
        unprocessed_reads: 1,
        pending_tables: fixture.names()[1..].iter().map(|table| table.to_string()).collect(),
        ..Control::default()
      },
      Some(waits.clone()),
    );
    let result = entry.open(client, fixture.tables.clone(), options()).await;
    assert_eq!(result.as_ref().unwrap(), "stored-id");
    let observed = traces.lock().unwrap().clone();
    assert_eq!(observed.len(), 2);
    assert_read(&fixture, &observed[0], &[0, 1, 2]);
    assert_read(&fixture, &observed[1], &[1, 2]);
    let original: Value = serde_json::from_str(observed[0]["upstream_body"].as_str().unwrap()).unwrap();
    let delivered: Value = serde_json::from_str(observed[0]["delivered_body"].as_str().unwrap()).unwrap();
    assert_eq!(
      delivered["Responses"][fixture.names()[0]],
      original["Responses"][fixture.names()[0]]
    );
    assert_eq!(waits.0.lock().unwrap().as_slice(), &[Duration::from_millis(7)]);
    fixture
      .record("unprocessed-resolved", entry, &traces, &result, Value::Null)
      .await;
  }
  fixture.close().await;
}

#[tokio::test]
async fn should_not_create_while_reads_remain_unprocessed() {
  let fixture = Fixture::new().await;
  for entry in [Entry::Json, Entry::Custom] {
    let waits = Waits::default();
    let (client, traces) = fixture.observed(
      Control {
        unprocessed_reads: 2,
        pending_tables: fixture.names().iter().map(|table| table.to_string()).collect(),
        ..Control::default()
      },
      Some(waits.clone()),
    );
    let result = entry.open(client, fixture.tables.clone(), options()).await;
    assert!(matches!(
      result,
      Err(EventStoreError::Storage {
        operation: StorageOperation::ReadConfiguration,
        ..
      })
    ));
    let observed = traces.lock().unwrap().clone();
    assert_eq!(observed.len(), 2);
    for trace in &observed {
      assert_read(&fixture, trace, &[0, 1, 2]);
    }
    assert_eq!(waits.0.lock().unwrap().as_slice(), &[Duration::from_millis(7)]);
    assert!(fixture.saved().await.as_object().unwrap().values().all(Value::is_null));
    fixture
      .record("unprocessed-exhausted", entry, &traces, &result, Value::Null)
      .await;
  }
  fixture.close().await;
}

#[tokio::test]
async fn should_return_storage_for_other_creation_errors_without_rereading() {
  let fixture = Fixture::new().await;
  for entry in [Entry::Json, Entry::Custom] {
    for (index, error) in [
      json!({"__type": "InternalServerError", "Message": "TransactionConflict ConditionalCheckFailed SDK_SENTINEL"}),
      cancellation("ThrottlingError", 1),
      json!({"__type": "TransactionCanceledException", "Message": "TransactionConflict SDK_SENTINEL"}),
    ]
    .into_iter()
    .enumerate()
    {
      let (client, traces) = fixture.observed(
        Control {
          create_error: Some(error),
          ..Control::default()
        },
        Some(Waits::default()),
      );
      let result = entry.open(client, fixture.tables.clone(), options()).await;
      assert!(matches!(
        result,
        Err(EventStoreError::Storage {
          operation: StorageOperation::CreateConfiguration,
          ..
        })
      ));
      let observed = traces.lock().unwrap().clone();
      assert_eq!(observed.len(), 2);
      assert_read(&fixture, &observed[0], &[0, 1, 2]);
      assert_create(&fixture, &observed[1]);
      assert!(fixture.saved().await.as_object().unwrap().values().all(Value::is_null));
      fixture
        .record(
          &format!("other-create-error-{index}"),
          entry,
          &traces,
          &result,
          Value::Null,
        )
        .await;
    }
  }
  fixture.close().await;
}
