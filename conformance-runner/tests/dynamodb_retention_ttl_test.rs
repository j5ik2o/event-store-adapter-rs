//! TTL の接続だけを、配布入力・実公開操作・固定 Local の要求応答と物理項目で照合する。

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_sdk_dynamodb::config::{AsyncSleep, BehaviorVersion, Credentials, Region, Sleep};
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client;
use chrono::{DateTime, Utc};
use event_store_adapter_conformance_rs::compare::json_equal;
use event_store_adapter_conformance_rs::fault::{FaultPlan, Phase};
use event_store_adapter_conformance_rs::target_dynamodb::{FaultTransport, OperationReport, RequestLayout};
use event_store_adapter_rs::next::aggregate_id::AggregateId;
use event_store_adapter_rs::next::dynamodb::{Clock, DynamoDbOptions, DynamoDbTables, EventStoreForDynamoDB};
use event_store_adapter_rs::next::event_envelope::{EventEnvelope, SnapshotEnvelope};
use event_store_adapter_rs::next::retention::{RetentionMode, RetentionSettings};
use event_store_adapter_rs::next::serializer::{JsonEventSerializer, JsonSnapshotSerializer};
use event_store_adapter_test_utils_rs::{docker, dynamodb};
use serde_json::{json, Value};
use testcontainers::{ContainerAsync, GenericImage};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id};
use tracing::{Event, Instrument, Subscriber};
use tracing_subscriber::{layer::Context, prelude::*, registry::LookupSpan, Layer};

type Item = HashMap<String, AttributeValue>;
type Store = EventStoreForDynamoDB<TestId, Value, Value>;

#[derive(Debug, Clone)]
struct TestId {
  type_name: String,
  value: String,
}

impl AggregateId for TestId {
  fn type_name(&self) -> String {
    self.type_name.clone()
  }

  fn value(&self) -> String {
    self.value.clone()
  }
}

#[derive(Debug)]
struct FixedClock(AtomicU64);

impl Clock for FixedClock {
  fn now_epoch_seconds(&self) -> u64 {
    self.0.load(Ordering::SeqCst)
  }
}

#[derive(Debug)]
struct SdkSleep;

impl AsyncSleep for SdkSleep {
  fn sleep(&self, duration: Duration) -> Sleep {
    Sleep::new(tokio::time::sleep(duration))
  }
}

#[derive(Default)]
struct Fields(serde_json::Map<String, Value>);

impl Visit for Fields {
  fn record_u64(&mut self, field: &Field, value: u64) {
    self.0.insert(field.name().into(), json!(value));
  }

  fn record_str(&mut self, field: &Field, value: &str) {
    self.0.insert(field.name().into(), json!(value));
  }

  fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
    self.0.insert(field.name().into(), json!(format!("{value:?}")));
  }
}

struct Capture(Arc<Mutex<Vec<Value>>>);
struct Identity {
  case_id: String,
  operation: u64,
}

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Capture {
  fn on_new_span(&self, attributes: &Attributes<'_>, id: &Id, context: Context<'_, S>) {
    if attributes.metadata().target() != "ttl_direct_test::operation" {
      return;
    }
    let mut fields = Fields::default();
    attributes.record(&mut fields);
    context.span(id).unwrap().extensions_mut().insert(Identity {
      case_id: fields.0["case_id"].as_str().unwrap().into(),
      operation: fields.0["operation"].as_u64().unwrap(),
    });
  }

  fn on_event(&self, event: &Event<'_>, context: Context<'_, S>) {
    if event.metadata().target() != "event_store_adapter::retention" {
      return;
    }
    let mut fields = Fields::default();
    event.record(&mut fields);
    if let Some(scope) = context.event_scope(event) {
      for span in scope {
        if let Some(identity) = span.extensions().get::<Identity>() {
          fields.0.insert("case_id".into(), json!(identity.case_id));
          fields.0.insert("operation".into(), json!(identity.operation));
          fields.0.insert("target".into(), json!(event.metadata().target()));
          fields
            .0
            .insert("level".into(), json!(event.metadata().level().as_str()));
          self.0.lock().unwrap().push(Value::Object(fields.0));
          break;
        }
      }
    }
  }
}

fn notifications(case_id: &str, operation: u32) -> Vec<Value> {
  static CAPTURED: OnceLock<Arc<Mutex<Vec<Value>>>> = OnceLock::new();
  let captured = CAPTURED.get_or_init(|| {
    let captured = Arc::new(Mutex::new(Vec::new()));
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(Capture(captured.clone()))).unwrap();
    captured
  });
  let mut captured = captured.lock().unwrap();
  let matches = |notice: &Value| notice["case_id"] == case_id && notice["operation"] == operation;
  let found = captured.iter().filter(|notice| matches(notice)).cloned().collect();
  captured.retain(|notice| !matches(notice));
  found
}

fn operation_span(case_id: &str, operation: u32) -> tracing::Span {
  notifications(case_id, operation);
  tracing::info_span!(target: "ttl_direct_test::operation", "operation", case_id, operation = u64::from(operation))
}

fn load_case(id: &str) -> Value {
  for name in ["retention.json", "item-shapes.json"] {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
      .join("../conformance/dynamodb")
      .join(name);
    let document: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    if let Some(case) = document["cases"]
      .as_array()
      .unwrap()
      .iter()
      .find(|case| case["id"] == id)
    {
      return case.clone();
    }
  }
  panic!("配布ケースがない: {id}");
}

fn test_id(value: &Value) -> TestId {
  TestId {
    type_name: value["type_name"].as_str().unwrap().into(),
    value: value["value"].as_str().unwrap().into(),
  }
}

fn event(value: &Value) -> EventEnvelope<TestId, Value> {
  EventEnvelope::new(
    test_id(&value["aggregate_id"]),
    value["seq_nr"].as_u64().unwrap(),
    DateTime::parse_from_rfc3339(value["occurred_at"].as_str().unwrap())
      .unwrap()
      .with_timezone(&Utc),
    value["payload"].clone(),
  )
  .with_manifest(value["manifest"].as_str().unwrap())
}

fn snapshot(value: &Value) -> SnapshotEnvelope<Value> {
  SnapshotEnvelope::new(value["aggregate"].clone(), value["seq_nr"].as_u64().unwrap())
    .with_manifest(value["manifest"].as_str().unwrap())
}

fn event_json(event: &EventEnvelope<TestId, Value>) -> Value {
  json!({"aggregate_id": {"type_name": event.aggregate_id().type_name, "value": event.aggregate_id().value},
    "seq_nr": event.seq_nr(), "occurred_at": event.occurred_at().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
    "manifest": event.manifest(), "payload": event.payload()})
}

fn snapshot_json(snapshot: &SnapshotEnvelope<Value>) -> Value {
  json!({"aggregate": snapshot.aggregate(), "seq_nr": snapshot.seq_nr(), "manifest": snapshot.manifest()})
}

fn declared_map(declaration: &Value, types: &Value, values: &Value, prefix: &str) -> Item {
  types
    .as_object()
    .unwrap()
    .iter()
    .map(|(name, kind)| {
      let path = if prefix.is_empty() {
        name.clone()
      } else {
        format!("{prefix}.{name}")
      };
      let attribute = match kind.as_str().unwrap() {
        "S" => AttributeValue::S(values[name].as_str().unwrap().into()),
        "N" => AttributeValue::N(values[name].as_str().unwrap().into()),
        "B" => AttributeValue::B(Blob::new(
          serde_json::to_vec(&declaration["binary_json"][&path]).unwrap(),
        )),
        "L" => AttributeValue::L(
          values[name]
            .as_array()
            .unwrap()
            .iter()
            .enumerate()
            .map(|(index, value)| {
              let nested_path = format!("{path}[{index}]");
              AttributeValue::M(declared_map(
                declaration,
                &declaration["nested_attributes"][&nested_path],
                value,
                &nested_path,
              ))
            })
            .collect(),
        ),
        other => panic!("今回の入力にはない型: {other}"),
      };
      (name.clone(), attribute)
    })
    .collect()
}

fn attribute_json(attribute: &AttributeValue) -> Value {
  match attribute {
    AttributeValue::S(value) => json!({"S": value}),
    AttributeValue::N(value) => json!({"N": value}),
    AttributeValue::B(value) => json!({"B": value.as_ref()}),
    AttributeValue::L(values) => json!({"L": values.iter().map(attribute_json).collect::<Vec<_>>()}),
    AttributeValue::M(values) => json!({"M": item_json(values)}),
    other => panic!("実項目の未対応属性: {other:?}"),
  }
}

fn item_json(item: &Item) -> Value {
  Value::Object(
    item
      .iter()
      .map(|(name, attribute)| (name.clone(), attribute_json(attribute)))
      .collect(),
  )
}

fn assert_item(declaration: &Value, item: &Item) {
  let expected = declared_map(declaration, &declaration["attributes"], &declaration["values"], "");
  assert_eq!(
    item.keys().collect::<BTreeSet<_>>(),
    expected.keys().collect::<BTreeSet<_>>()
  );
  fn compare(expected: &AttributeValue, actual: &AttributeValue) {
    match (expected, actual) {
      (AttributeValue::N(expected), AttributeValue::N(actual)) => {
        assert_eq!(actual.parse::<i128>().unwrap(), expected.parse::<i128>().unwrap())
      }
      (AttributeValue::B(expected), AttributeValue::B(actual)) => assert!(json_equal(
        &serde_json::from_slice::<Value>(expected.as_ref()).unwrap(),
        &serde_json::from_slice::<Value>(actual.as_ref()).unwrap()
      )),
      (AttributeValue::L(expected), AttributeValue::L(actual)) => {
        assert_eq!(actual.len(), expected.len());
        for (expected, actual) in expected.iter().zip(actual) {
          compare(expected, actual);
        }
      }
      (AttributeValue::M(expected), AttributeValue::M(actual)) => {
        assert_eq!(
          actual.keys().collect::<BTreeSet<_>>(),
          expected.keys().collect::<BTreeSet<_>>()
        );
        for (name, expected) in expected {
          compare(expected, &actual[name]);
        }
      }
      _ => assert_eq!(actual, expected),
    }
  }
  for (name, expected) in expected {
    compare(&expected, &item[&name]);
  }
}

struct Fixture {
  container: ContainerAsync<GenericImage>,
  raw: Client,
  client: Client,
  transport: FaultTransport,
  tables: DynamoDbTables,
  case_id: String,
}

impl Fixture {
  async fn new(case_id: &str) -> Self {
    let container = docker::dynamodb_local().await.unwrap();
    let port = container.get_host_port_ipv4(docker::DYNAMODB_LOCAL_PORT).await.unwrap();
    let raw = dynamodb::create_dynamodb_local_client(port);
    let names = dynamodb::TableNames {
      journal: "ttl-first".into(),
      snapshot: "ttl-second".into(),
      head: "ttl-third".into(),
      snapshot_history_index: "ttl-history".into(),
    };
    dynamodb::create_tables(&raw, &names, true).await.unwrap();
    let tables = DynamoDbTables {
      journal_table_name: names.journal,
      snapshot_table_name: names.snapshot,
      head_table_name: names.head,
      snapshot_history_index_name: names.snapshot_history_index,
    };
    let transport = FaultTransport::new_with_history_client(
      RequestLayout::new(
        &tables.journal_table_name,
        &tables.snapshot_table_name,
        &tables.head_table_name,
        &tables.snapshot_history_index_name,
      )
      .unwrap(),
      raw.clone(),
    );
    let upstream = aws_smithy_http_client::Builder::new().build_http();
    let client = transport.client(
      aws_sdk_dynamodb::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-west-1"))
        .credentials_provider(Credentials::new("x", "x", None, None, "ttl-test"))
        .endpoint_url(format!("http://127.0.0.1:{port}"))
        .sleep_impl(SdkSleep),
      upstream,
    );
    let fixture = Self {
      container,
      raw,
      client,
      transport,
      tables,
      case_id: case_id.into(),
    };
    fixture.check_layout().await;
    fixture
  }

  fn table(&self, symbol: &str) -> &str {
    match symbol {
      "journal" => &self.tables.journal_table_name,
      "snapshot" => &self.tables.snapshot_table_name,
      "head" => &self.tables.head_table_name,
      other => panic!("表名がない: {other}"),
    }
  }

  fn record(&self, suffix: &str, evidence: Value) {
    if let Some(directory) = std::env::var_os("DYNAMODB_TTL_EVIDENCE_DIR") {
      let directory = std::path::PathBuf::from(directory);
      std::fs::create_dir_all(&directory).unwrap();
      std::fs::write(
        directory.join(format!("{}-{suffix}.json", self.case_id)),
        serde_json::to_vec_pretty(&evidence).unwrap(),
      )
      .unwrap();
    }
  }

  async fn check_layout(&self) {
    let declaration: Value = serde_json::from_slice(
      &std::fs::read(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance/dynamodb/layout.json"))
        .unwrap(),
    )
    .unwrap();
    let mut observations = Vec::new();
    for expected in declaration["cases"][0]["tables"].as_array().unwrap() {
      let symbol = expected["name"].as_str().unwrap();
      let description = self
        .raw
        .describe_table()
        .table_name(self.table(symbol))
        .send()
        .await
        .unwrap();
      let ttl = self
        .raw
        .describe_time_to_live()
        .table_name(self.table(symbol))
        .send()
        .await
        .unwrap();
      let table = description.table().unwrap();
      let keys = table
        .key_schema()
        .iter()
        .map(|key| {
          let name = key.attribute_name();
          let kind = table
            .attribute_definitions()
            .iter()
            .find(|attribute| attribute.attribute_name() == name)
            .unwrap();
          (
            key.key_type().as_str(),
            json!({"name": name, "type": kind.attribute_type().as_str()}),
          )
        })
        .collect::<HashMap<_, _>>();
      assert_eq!(keys["HASH"], expected["partition_key"]);
      assert_eq!(keys.get("RANGE").cloned().unwrap_or(Value::Null), expected["sort_key"]);
      assert_eq!(
        table.global_secondary_indexes().len(),
        expected["gsi"].as_array().unwrap().len()
      );
      if symbol == "snapshot" {
        let index = &table.global_secondary_indexes()[0];
        assert_eq!(
          index.index_name(),
          Some(self.tables.snapshot_history_index_name.as_str())
        );
        assert_eq!(
          index.projection().unwrap().projection_type().unwrap().as_str(),
          "KEYS_ONLY"
        );
        assert_eq!(
          index
            .key_schema()
            .iter()
            .map(|key| (key.attribute_name(), key.key_type().as_str()))
            .collect::<BTreeSet<_>>(),
          BTreeSet::from([("aid", "HASH"), ("active_history_seq_nr", "RANGE")])
        );
      }
      let streams_enabled = table
        .stream_specification()
        .is_some_and(|streams| streams.stream_enabled());
      assert_eq!(streams_enabled, expected["streams"]["enabled"].as_bool().unwrap());
      if streams_enabled {
        assert_eq!(
          table
            .stream_specification()
            .unwrap()
            .stream_view_type()
            .unwrap()
            .as_str(),
          expected["streams"]["view_type"].as_str().unwrap()
        );
      }
      let ttl_description = ttl.time_to_live_description().unwrap();
      assert_eq!(
        ttl_description.time_to_live_status().unwrap().as_str(),
        if symbol == "snapshot" { "ENABLED" } else { "DISABLED" }
      );
      assert_eq!(
        ttl_description.attribute_name(),
        if symbol == "snapshot" { Some("ttl") } else { None }
      );
      observations.push(json!({"table": symbol, "DescribeTable": format!("{description:#?}"), "DescribeTimeToLive": format!("{ttl:#?}")}));
    }
    self.record(
      "layout",
      json!({"container_id": self.container.id(), "declaration": declaration, "observations": observations}),
    );
  }

  async fn seed(&self, case: &Value) {
    if let Some(items) = case.pointer("/seed/items").and_then(Value::as_array) {
      for declaration in items {
        self
          .raw
          .put_item()
          .table_name(self.table(declaration["table"].as_str().unwrap()))
          .set_item(Some(declared_map(
            declaration,
            &declaration["attributes"],
            &declaration["values"],
            "",
          )))
          .send()
          .await
          .unwrap();
      }
    }
  }

  async fn open(&self, options: DynamoDbOptions, clock: Option<Arc<FixedClock>>) -> Store {
    let store = Store::open(self.client.clone(), self.tables.clone(), options)
      .await
      .unwrap();
    match clock {
      Some(clock) => store.with_clock_for_test(clock),
      None => store,
    }
  }

  async fn state(&self, aid: &str) -> Value {
    let mut state = serde_json::Map::new();
    for symbol in ["journal", "snapshot"] {
      let result = self
        .raw
        .query()
        .table_name(self.table(symbol))
        .key_condition_expression("aid = :aid")
        .expression_attribute_values(":aid", AttributeValue::S(aid.into()))
        .consistent_read(true)
        .scan_index_forward(true)
        .send()
        .await
        .unwrap();
      assert!(result.last_evaluated_key().is_none());
      state.insert(
        symbol.into(),
        json!(result.items().iter().map(item_json).collect::<Vec<_>>()),
      );
    }
    let head = self
      .raw
      .get_item()
      .table_name(self.table("head"))
      .key("aid", AttributeValue::S(aid.into()))
      .consistent_read(true)
      .send()
      .await
      .unwrap();
    state.insert("head".into(), head.item().map(item_json).unwrap_or(Value::Null));
    let mut configuration = Vec::new();
    for symbol in ["journal", "snapshot", "head"] {
      let mut key = HashMap::from([("aid".into(), AttributeValue::S("__config__".into()))]);
      if symbol != "head" {
        key.insert(
          if symbol == "journal" { "seq_nr" } else { "skey" }.into(),
          AttributeValue::N("0".into()),
        );
      }
      let item = self
        .raw
        .get_item()
        .table_name(self.table(symbol))
        .set_key(Some(key))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
      configuration.push(item_json(item.item().unwrap()));
    }
    state.insert("configuration".into(), json!(configuration));
    Value::Object(state)
  }

  async fn assert_items(&self, declarations: &Value) -> Vec<Value> {
    let mut observed = Vec::new();
    for declaration in declarations.as_array().unwrap() {
      let symbol = declaration["table"].as_str().unwrap();
      let mut key = HashMap::from([(
        "aid".into(),
        AttributeValue::S(declaration["values"]["aid"].as_str().unwrap().into()),
      )]);
      if symbol != "head" {
        let sort = if symbol == "journal" { "seq_nr" } else { "skey" };
        key.insert(
          sort.into(),
          AttributeValue::N(declaration["values"][sort].as_str().unwrap().into()),
        );
      }
      let result = self
        .raw
        .get_item()
        .table_name(self.table(symbol))
        .set_key(Some(key))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
      let item = result.item().unwrap();
      assert_item(declaration, item);
      observed.push(json!({"table": symbol, "item": item_json(item), "sdk_response": format!("{result:#?}")}));
    }
    observed
  }

  async fn close(self) {
    for symbol in ["journal", "snapshot", "head"] {
      self
        .raw
        .delete_table()
        .table_name(self.table(symbol))
        .send()
        .await
        .unwrap();
    }
    self.container.rm().await.unwrap();
  }
}

fn history(state: &Value) -> Value {
  let mut active = Vec::new();
  let mut marked = Vec::new();
  for item in state["snapshot"].as_array().unwrap() {
    let number = item["skey"]["N"].as_str().unwrap().parse::<u64>().unwrap();
    if number == 0 {
      assert!(item.get("ttl").is_none());
      assert!(item.get("active_history_seq_nr").is_none());
      continue;
    }
    assert_eq!(number.to_string(), item["seq_nr"]["N"].as_str().unwrap());
    if let Some(active_number) = item.get("active_history_seq_nr") {
      assert_eq!(active_number["N"].as_str().unwrap().parse::<u64>().unwrap(), number);
      assert!(item.get("ttl").is_none());
      active.push(number);
    } else {
      marked.push(json!({"seq_nr": number, "ttl": item["ttl"]["N"].as_str().unwrap().parse::<u64>().unwrap()}));
    }
  }
  for item in state["journal"]
    .as_array()
    .unwrap()
    .iter()
    .chain(std::iter::once(&state["head"]))
    .chain(state["configuration"].as_array().unwrap())
  {
    assert!(item.get("ttl").is_none());
    assert!(item.get("active_history_seq_nr").is_none());
  }
  json!({"active": active, "marked": marked, "absent": []})
}

fn assert_requests(report: &OperationReport, expected: &Value) {
  let mut position = 0;
  for declaration in expected.as_array().unwrap() {
    let phase = Phase::parse(declaration["phase"].as_str().unwrap()).unwrap();
    let relative = report.requests[position..]
      .iter()
      .position(|request| request.api == declaration["api"] && request.phase == Some(phase))
      .unwrap();
    position += relative + 1;
    let body = &report.requests[position - 1].body;
    for (name, expected) in declaration["constraints"].as_object().unwrap() {
      match name.as_str() {
        "expression_attribute_names" => assert_eq!(&body["ExpressionAttributeNames"], expected),
        "expires" => assert_eq!(
          body["ExpressionAttributeValues"][":expires"]["N"]
            .as_str()
            .unwrap()
            .parse::<u128>()
            .unwrap(),
          u128::from(expected.as_u64().unwrap())
        ),
        "target_seq_nrs" => {
          let mut actual: Vec<u64> = report
            .requests
            .iter()
            .filter(|request| request.phase == Some(phase))
            .map(|request| request.body["Key"]["skey"]["N"].as_str().unwrap().parse().unwrap())
            .collect();
          actual.sort_unstable();
          assert_eq!(json!(actual), *expected);
        }
        "condition" => {
          let name = body["ConditionExpression"]
            .as_str()
            .unwrap()
            .trim()
            .strip_prefix("attribute_exists(")
            .unwrap()
            .strip_suffix(')')
            .unwrap()
            .trim();
          let resolved = body["ExpressionAttributeNames"]
            .get(name)
            .and_then(Value::as_str)
            .unwrap_or(name);
          assert_eq!(resolved, expected["attribute_exists"].as_str().unwrap());
        }
        "update" => {
          let (set, remove) = body["UpdateExpression"].as_str().unwrap().split_once("REMOVE").unwrap();
          let resolve = |name: &str| {
            body["ExpressionAttributeNames"]
              .get(name)
              .and_then(Value::as_str)
              .unwrap_or(name)
              .to_owned()
          };
          let assignments: HashMap<_, _> = set
            .trim()
            .strip_prefix("SET")
            .unwrap()
            .split(',')
            .map(|assignment| {
              let (name, binding) = assignment.split_once('=').unwrap();
              (
                resolve(name.trim()),
                body["ExpressionAttributeValues"][binding.trim()].clone(),
              )
            })
            .collect();
          assert_eq!(
            assignments.keys().cloned().collect::<BTreeSet<_>>(),
            expected["set"].as_object().unwrap().keys().cloned().collect()
          );
          assert_eq!(assignments["ttl"], body["ExpressionAttributeValues"][":expires"]);
          assert_eq!(expected["set"]["ttl"]["value_binding"], "expires");
          assert_eq!(
            remove
              .split(',')
              .map(|name| resolve(name.trim()))
              .collect::<BTreeSet<_>>(),
            expected["remove"]
              .as_array()
              .unwrap()
              .iter()
              .map(|name| name.as_str().unwrap().to_owned())
              .collect()
          );
        }
        other => panic!("今回の配布ケースにない要求条件: {other}"),
      }
    }
  }
}

async fn execute(store: &Store, case: &Value, step: &Value) -> Value {
  let arguments = &step["arguments"];
  match step["op"].as_str().unwrap() {
    "persistEventAndSnapshot" => {
      store
        .persist_event_and_snapshot(
          event(&case["fixtures"]["events"][arguments["event"].as_str().unwrap()]),
          snapshot(&case["fixtures"]["snapshots"][arguments["snapshot"].as_str().unwrap()]),
        )
        .await
        .unwrap();
      json!({"result": "success"})
    }
    "getLatestSnapshotById" => {
      let read = store
        .get_latest_snapshot_by_id(&test_id(&arguments["aggregate_id"]))
        .await
        .unwrap()
        .unwrap();
      json!({"result": "snapshot", "head_seq_nr": read.head_seq_nr(), "snapshot": snapshot_json(read.snapshot().unwrap())})
    }
    "getEventsByIdSinceSeqNr" => {
      let events = store
        .get_events_by_id_since_seq_nr(
          &test_id(&arguments["aggregate_id"]),
          arguments["seq_nr"].as_u64().unwrap(),
        )
        .await
        .unwrap();
      json!({"result": "events", "events": events.iter().map(event_json).collect::<Vec<_>>()})
    }
    other => panic!("今回の配布ケースにない操作: {other}"),
  }
}

async fn run_distributed(id: &str) {
  let case = load_case(id);
  let fixture = Fixture::new(id).await;
  fixture.seed(&case).await;
  let clock = Arc::new(FixedClock(AtomicU64::new(
    case["clock"]["epoch_seconds"].as_u64().unwrap(),
  )));
  let options = DynamoDbOptions {
    retention: RetentionSettings::keep_latest(case["store"]["retention_count"].as_u64().unwrap() as usize).with_mode(
      RetentionMode::Ttl {
        grace_seconds: case["store"]["ttl_grace_seconds"].as_u64().unwrap(),
      },
    ),
    ..Default::default()
  };
  let plan = FaultPlan::register(&case).unwrap();
  let guard = fixture.transport.begin_operation(&plan, 0).unwrap();
  let store = fixture.open(options, Some(clock.clone())).await;
  let opened = guard.finish();
  assert!(opened.unfired.is_empty());
  fixture.record("input-open", json!({"input": case, "requests": opened.requests.iter().map(|request| json!({"api": request.api, "phase": request.phase, "body": request.body})).collect::<Vec<_>>(), "responses": opened.responses}));
  let initial = fixture.state("Order-9").await;
  for (index, step) in case["steps"].as_array().unwrap().iter().enumerate() {
    let operation = index as u32 + 1;
    if let Some(now) = step.get("clock_epoch_seconds") {
      clock.0.store(now.as_u64().unwrap(), Ordering::SeqCst);
    }
    let guard = fixture.transport.begin_operation(&plan, operation).unwrap();
    let actual = execute(&store, &case, step)
      .instrument(operation_span(id, operation))
      .await;
    let report = guard.finish();
    let notices = notifications(id, operation);
    let mut expected = step["expect"].clone();
    if expected["result"] == "snapshot" {
      expected["snapshot"] = case["fixtures"]["snapshots"][expected["snapshot"].as_str().unwrap()].clone();
    }
    if expected["result"] == "events" {
      expected["events"] = json!(expected["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|name| case["fixtures"]["events"][name.as_str().unwrap()].clone())
        .collect::<Vec<_>>());
    }
    let state = fixture.state("Order-9").await;
    let actual_history = history(&state);
    let items = if let Some(declarations) = step.pointer("/observe/items") {
      fixture.assert_items(declarations).await
    } else {
      Vec::new()
    };
    fixture.record(&format!("operation-{operation}"), json!({"step": step, "actual": actual, "state": state,
      "history": actual_history, "items": items, "notifications": notices,
      "requests": report.requests.iter().map(|request| json!({"api": request.api, "phase": request.phase, "body": request.body})).collect::<Vec<_>>(),
      "responses": report.responses, "unfired_faults": report.unfired}));
    assert!(json_equal(&expected, &actual), "{id}, operation {operation}: {actual}");
    assert!(
      report.unfired.is_empty(),
      "{id}, operation {operation}: {:?}",
      report.unfired
    );
    assert_eq!(state["configuration"], initial["configuration"]);
    if let Some(expected) = step.pointer("/observe/history") {
      assert_eq!(&actual_history, expected);
    }
    let categories: Vec<&Value> = notices.iter().map(|notice| &notice["category"]).collect();
    assert_eq!(
      json!(categories),
      step
        .pointer("/observe/notifications")
        .cloned()
        .unwrap_or_else(|| json!([]))
    );
    if let Some(expected) = step.pointer("/observe/requests") {
      assert_requests(&report, expected);
    }
    if id == "dynamodb-retention-ttl-stale-marked" {
      let marks: Vec<_> = report
        .responses
        .iter()
        .filter(|response| response["phase"] == "retention-mark")
        .collect();
      assert_eq!(marks.len(), 2);
      for mark in marks {
        assert_eq!(mark["upstream"]["status"], 400);
        assert_eq!(mark["upstream"], mark["delivered"]);
        assert!(mark["upstream"]["body"]
          .as_str()
          .unwrap()
          .contains("ConditionalCheckFailedException"));
      }
    }
    if id == "dynamodb-retention-failure-ttl" && operation == 2 {
      assert_eq!(notices.len(), 1);
      assert_eq!(notices[0]["phase"], "retention-mark");
      assert_eq!(notices[0]["level"], "WARN");
      assert!(notices[0]["error"]
        .as_str()
        .unwrap()
        .contains("ProvisionedThroughputExceededException"));
      assert!(notices[0]["error"].as_str().unwrap().contains("RETENTION_FAILURE"));
      let mark = report
        .responses
        .iter()
        .find(|response| response["phase"] == "retention-mark")
        .unwrap();
      assert!(mark["upstream"].is_null());
      assert_eq!(mark["delivered"]["status"], 400);
    }
  }
  fixture.close().await;
}

#[tokio::test]
async fn should_match_distributed_ttl_once_on_the_same_store_and_clock() {
  run_distributed("dynamodb-retention-ttl-once").await;
}

#[tokio::test]
async fn should_match_distributed_stale_marked_using_real_condition_failures() {
  run_distributed("dynamodb-retention-ttl-stale-marked").await;
}

#[tokio::test]
async fn should_match_distributed_ttl_failure_and_subsequent_reads_and_recovery() {
  run_distributed("dynamodb-retention-failure-ttl").await;
}

#[tokio::test]
async fn should_match_distributed_written_item_shapes_including_binary_and_nested_values() {
  run_distributed("dynamodb-written-item-shapes").await;
}

fn numbered_event(number: u64, value: &str) -> EventEnvelope<TestId, Value> {
  EventEnvelope::new(
    TestId {
      type_name: "Account".into(),
      value: value.into(),
    },
    number,
    DateTime::from_timestamp(0, 123_000_000).unwrap(),
    json!({"number": number}),
  )
  .with_manifest("event")
}

fn numbered_snapshot(number: u64) -> SnapshotEnvelope<Value> {
  SnapshotEnvelope::new(json!({"total": number}), number).with_manifest("snapshot")
}

fn retention_options(count: usize, grace_seconds: u64) -> DynamoDbOptions {
  DynamoDbOptions {
    retention: RetentionSettings::keep_latest(count).with_mode(RetentionMode::Ttl { grace_seconds }),
    ..Default::default()
  }
}

fn transport_evidence(report: &OperationReport) -> Value {
  json!({"requests": report.requests.iter().map(|request| json!({"api": request.api, "phase": request.phase, "body": request.body})).collect::<Vec<_>>(),
    "responses": report.responses, "unfired_faults": report.unfired})
}

fn history_item(state: &Value, number: u64) -> &Value {
  state["snapshot"]
    .as_array()
    .unwrap()
    .iter()
    .find(|item| item["skey"]["N"].as_str().unwrap().parse::<u64>().unwrap() == number)
    .unwrap()
}

#[tokio::test]
async fn should_keep_partial_marks_and_recover_from_the_real_gsi_on_the_same_store() {
  let id = "ttl-partial-recovery";
  let fixture = Fixture::new(id).await;
  let seeder = fixture
    .open(
      DynamoDbOptions {
        retention: RetentionSettings::keep_latest(6),
        ..Default::default()
      },
      None,
    )
    .await;
  for number in 1..=4 {
    seeder
      .persist_event_and_snapshot(numbered_event(number, "recovery"), numbered_snapshot(number))
      .await
      .unwrap();
  }
  let clock = Arc::new(FixedClock(AtomicU64::new(4_102_444_800)));
  let store = fixture.open(retention_options(1, 60), Some(clock.clone())).await;
  let plan = FaultPlan::register(&json!({"steps": [{}, {}, {}], "faults": [
    {"operation": 1, "phase": "retention-query", "kind": "sdk-response", "injection": "replace-response",
      "repeat": {"mode": "count", "count": 1}, "details": {"history_pages": [[4, 3], [2, 1]]}},
    {"operation": 1, "phase": "retention-mark", "kind": "sdk-error", "injection": "replace-response",
      "repeat": {"mode": "count", "count": 1}, "details": {"code": "ConditionalCheckFailedException", "message": "FIRST_MARK_RESPONSE"}},
    {"operation": 1, "phase": "retention-mark", "kind": "sdk-error", "injection": "replace-request",
      "repeat": {"mode": "count", "count": 1}, "details": {"code": "ProvisionedThroughputExceededException", "message": "PARTIAL_MARK_CAUSE"}}
  ]})).unwrap();
  let before = fixture.state("Account-recovery").await;
  let guard = fixture.transport.begin_operation(&plan, 1).unwrap();
  let result = store
    .persist_event_and_snapshot(numbered_event(5, "recovery"), numbered_snapshot(5))
    .instrument(operation_span(id, 1))
    .await;
  let report = guard.finish();
  let notices = notifications(id, 1);
  let partial = fixture.state("Account-recovery").await;
  fixture.record(
    "partial",
    json!({"public_result": format!("{result:?}"), "before": before, "after": partial,
    "transport": transport_evidence(&report), "notifications": notices}),
  );
  result.unwrap();
  assert!(report.unfired.is_empty());
  let queries: Vec<_> = report
    .requests
    .iter()
    .filter(|request| request.phase == Some(Phase::RetentionQuery))
    .collect();
  assert_eq!(queries.len(), 2);
  let delivered_page: Value = serde_json::from_str(
    report
      .responses
      .iter()
      .find(|response| response["phase"] == "retention-query")
      .unwrap()["delivered"]["body"]
      .as_str()
      .unwrap(),
  )
  .unwrap();
  assert_eq!(queries[1].body["ExclusiveStartKey"], delivered_page["LastEvaluatedKey"]);
  let targets: Vec<_> = report
    .requests
    .iter()
    .filter(|request| request.phase == Some(Phase::RetentionMark))
    .map(|request| request.body["Key"]["skey"]["N"].clone())
    .collect();
  assert_eq!(targets, [json!("4"), json!("3")]);
  let marks: Vec<_> = report
    .responses
    .iter()
    .filter(|response| response["phase"] == "retention-mark")
    .collect();
  assert_eq!(marks[0]["upstream"]["status"], 200);
  assert_eq!(marks[0]["delivered"]["status"], 400);
  assert!(marks[1]["upstream"].is_null());
  assert_eq!(marks[1]["delivered"]["status"], 400);
  assert_eq!(
    history(&partial),
    json!({"active": [1, 2, 3, 5], "marked": [{"seq_nr": 4, "ttl": 4102444860_u64}], "absent": []})
  );
  let mut expected_mark = history_item(&before, 4).clone();
  expected_mark.as_object_mut().unwrap().remove("active_history_seq_nr");
  expected_mark["ttl"] = json!({"N": "4102444860"});
  assert_eq!(*history_item(&partial, 4), expected_mark);
  for number in 1..=3 {
    assert_eq!(history_item(&partial, number), history_item(&before, number));
  }
  assert_eq!(notices.len(), 1);
  assert_eq!(notices[0]["phase"], "retention-mark");
  assert_eq!(notices[0]["level"], "WARN");
  assert_eq!(notices[0]["aid"], "Account-recovery");
  assert_eq!(notices[0]["seq_nr"], 5);
  assert!(notices[0]["error"]
    .as_str()
    .unwrap()
    .contains("ProvisionedThroughputExceededException"));
  assert!(notices[0]["error"].as_str().unwrap().contains("PARTIAL_MARK_CAUSE"));

  let guard = fixture.transport.begin_operation(&plan, 2).unwrap();
  store
    .persist_event(numbered_event(6, "recovery"))
    .instrument(operation_span(id, 2))
    .await
    .unwrap();
  let report = guard.finish();
  let notices = notifications(id, 2);
  let event_only = fixture.state("Account-recovery").await;
  fixture.record(
    "event-only",
    json!({"state": event_only, "transport": transport_evidence(&report), "notifications": notices}),
  );
  assert_eq!(report.requests.len(), 1);
  assert_eq!(report.requests[0].api, "TransactWriteItems");
  assert_eq!(report.requests[0].phase, Some(Phase::Commit));
  assert!(notices.is_empty());
  assert_eq!(event_only["snapshot"], partial["snapshot"]);

  clock.0.store(4_102_445_800, Ordering::SeqCst);
  let guard = fixture.transport.begin_operation(&plan, 3).unwrap();
  store
    .persist_event_and_snapshot(numbered_event(7, "recovery"), numbered_snapshot(7))
    .instrument(operation_span(id, 3))
    .await
    .unwrap();
  let report = guard.finish();
  let notices = notifications(id, 3);
  let recovered = fixture.state("Account-recovery").await;
  fixture.record(
    "recovery",
    json!({"state": recovered, "transport": transport_evidence(&report), "notifications": notices}),
  );
  assert!(notices.is_empty());
  assert!(report.unfired.is_empty());
  for response in &report.responses {
    assert!(response.get("fault_index").is_none());
    assert_eq!(response["upstream"], response["delivered"]);
  }
  let queries: Vec<_> = report
    .responses
    .iter()
    .filter(|response| response["phase"] == "retention-query")
    .collect();
  assert!(!queries.is_empty());
  let mut visible = Vec::new();
  for query in queries {
    let upstream: Value = serde_json::from_str(query["upstream"]["body"].as_str().unwrap()).unwrap();
    visible.extend(
      upstream["Items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["skey"]["N"].as_str().unwrap().parse::<u64>().unwrap()),
    );
  }
  assert!(visible.contains(&1) && visible.contains(&2) && visible.contains(&3) && visible.contains(&5));
  assert!(!visible.contains(&4));
  let targets: Vec<_> = report
    .requests
    .iter()
    .filter(|request| request.phase == Some(Phase::RetentionMark))
    .map(|request| request.body["Key"]["skey"]["N"].clone())
    .collect();
  assert_eq!(targets, [json!("5"), json!("3"), json!("2"), json!("1")]);
  assert_eq!(
    history(&recovered),
    json!({"active": [7], "marked": [
    {"seq_nr": 1, "ttl": 4102445860_u64}, {"seq_nr": 2, "ttl": 4102445860_u64}, {"seq_nr": 3, "ttl": 4102445860_u64},
    {"seq_nr": 4, "ttl": 4102444860_u64}, {"seq_nr": 5, "ttl": 4102445860_u64}], "absent": []})
  );
  assert_eq!(history_item(&recovered, 4), history_item(&partial, 4));
  for number in [1, 2, 3, 5] {
    let mut expected = history_item(&partial, number).clone();
    expected.as_object_mut().unwrap().remove("active_history_seq_nr");
    expected["ttl"] = json!({"N": "4102445860"});
    assert_eq!(history_item(&recovered, number), &expected);
  }
  assert_eq!(recovered["configuration"], before["configuration"]);
  assert_eq!(recovered["journal"].as_array().unwrap().len(), 7);
  assert_eq!(recovered["head"]["seq_nr"]["N"], "7");
  assert_eq!(history_item(&recovered, 0)["seq_nr"]["N"], "7");
  fixture.close().await;
}

#[tokio::test]
async fn should_use_the_real_default_clock_through_both_public_open_paths() {
  let fixture = Fixture::new("ttl-default-clock").await;
  let plan = FaultPlan::register(&json!({})).unwrap();
  for explicit_serializers in [false, true] {
    let store = if explicit_serializers {
      Store::open_with_serializers(
        fixture.client.clone(),
        fixture.tables.clone(),
        retention_options(1, 86_400),
        Arc::new(JsonEventSerializer::new()),
        Arc::new(JsonSnapshotSerializer::new()),
      )
      .await
      .unwrap()
    } else {
      fixture.open(retention_options(1, 86_400), None).await
    };
    let value = if explicit_serializers { "serializers" } else { "default" };
    let before = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let guard = fixture.transport.begin_operation(&plan, 1).unwrap();
    for number in 1..=2 {
      store
        .persist_event_and_snapshot(numbered_event(number, value), numbered_snapshot(number))
        .await
        .unwrap();
    }
    let report = guard.finish();
    let after = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let state = fixture.state(&format!("Account-{value}")).await;
    fixture.record(value, json!({"before_seconds": before, "after_seconds": after, "state": state, "transport": transport_evidence(&report)}));
    let marked = history_item(&state, 1);
    let expires: u64 = marked["ttl"]["N"].as_str().unwrap().parse().unwrap();
    assert!((before + 86_400..=after + 86_400).contains(&expires));
    assert!(!marked.as_object().unwrap().contains_key("active_history_seq_nr"));
    assert_eq!(history_item(&state, 2)["active_history_seq_nr"]["N"], "2");
    let mark = report
      .requests
      .iter()
      .find(|request| request.phase == Some(Phase::RetentionMark))
      .unwrap();
    assert_eq!(
      mark.body["ExpressionAttributeValues"][":expires"]["N"],
      expires.to_string()
    );
    assert!(report.unfired.is_empty());
    history(&state);
  }
  fixture.close().await;
}
