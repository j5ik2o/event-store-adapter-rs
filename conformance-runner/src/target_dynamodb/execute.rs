use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::sync::{
  atomic::{AtomicU64, Ordering},
  Arc, Mutex,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_sdk_dynamodb::{
  config::{AsyncSleep, BehaviorVersion, Credentials, Region, Sleep, StalledStreamProtectionConfig},
  types::AttributeValue,
  Client,
};
use aws_smithy_types::timeout::TimeoutConfig;
use event_store_adapter_rs::{
  aggregate_id::AidString,
  dynamodb::{Clock, DynamoDbOptions, DynamoDbTables, EventStoreForDynamoDB},
  error::EventStoreError,
  serializer::{EventSerializer, JsonEventSerializer, JsonSnapshotSerializer, SnapshotSerializer},
};
use event_store_adapter_test_utils_rs::{docker, dynamodb};
use serde_json::{json, Value};
use testcontainers::{ContainerAsync, GenericImage};
use tracing::Instrument;

use super::{
  check_requests::{self, RequestContext},
  items::{self, Item},
  layout, FaultTransport, OperationReport, RequestLayout,
};
use crate::{
  case::{self, CaseId, ComparisonFailure},
  compare::json_equal,
  data::{Case, CaseKind},
  fault::Phase,
  notifications::OperationNotifications,
  report::CaseOutcome,
  runner::PreparedCase,
};

type Store = EventStoreForDynamoDB<CaseId, Value, Value>;

struct ObservationContext<'a> {
  case: &'a Case,
  id: Option<&'a CaseId>,
  seq_nr: Option<u64>,
  layout: &'a RequestLayout,
  tables: &'a Value,
  report: &'a OperationReport,
  notifications: &'a [String],
  waits: &'a Waits,
}

#[derive(serde::Serialize)]
struct Observation {
  actual: Value,
  errors: Vec<String>,
}

#[derive(Debug)]
struct MutableClock(AtomicU64);
impl Clock for MutableClock {
  fn now_epoch_seconds(&self) -> u64 {
    self.0.load(Ordering::SeqCst)
  }
}

#[derive(Debug, Default, Clone)]
struct Waits(Arc<Mutex<Vec<u128>>>);
impl AsyncSleep for Waits {
  fn sleep(&self, delay: Duration) -> Sleep {
    let waits = self.clone();
    Sleep::new(async move {
      waits.0.lock().expect("待ち記録のロック").push(delay.as_millis());
    })
  }
}

#[derive(Debug)]
struct FaultSerializer(FaultTransport);
impl EventSerializer<Value> for FaultSerializer {
  fn serialize(&self, value: &Value) -> Result<Vec<u8>, EventStoreError> {
    self.0.inject_serialization(Phase::SerializeEvent)?;
    JsonEventSerializer::new().serialize(value)
  }

  fn deserialize(&self, bytes: &[u8]) -> Result<Value, EventStoreError> {
    self.0.inject_serialization(Phase::DeserializeEvent)?;
    JsonEventSerializer::new().deserialize(bytes)
  }
}
impl SnapshotSerializer<Value> for FaultSerializer {
  fn serialize(&self, value: &Value) -> Result<Vec<u8>, EventStoreError> {
    self.0.inject_serialization(Phase::SerializeSnapshot)?;
    JsonSnapshotSerializer::new().serialize(value)
  }

  fn deserialize(&self, bytes: &[u8]) -> Result<Value, EventStoreError> {
    self.0.inject_serialization(Phase::DeserializeSnapshot)?;
    JsonSnapshotSerializer::new().deserialize(bytes)
  }
}

/// 実行全体のLocalとruntimeを保持し、各ケースへ独立した3表を割り当てる。
pub struct Execution {
  runtime: tokio::runtime::Runtime,
  container: Option<ContainerAsync<GenericImage>>,
  raw: Client,
  endpoint: String,
  run_id: String,
  next_case: Cell<u32>,
  observations: RefCell<BTreeMap<String, Vec<Value>>>,
  shapes: RefCell<BTreeMap<String, (String, Item)>>,
  table_observations: RefCell<Vec<(RequestLayout, bool, Value)>>,
}

impl Execution {
  /// 固定Local3.3.1を既起動ヘルパーで開始する。
  pub fn start() -> Result<Self, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
      .enable_all()
      .build()
      .map_err(|e| e.to_string())?;
    let run_id = format!(
      "{}-{}",
      std::process::id(),
      SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| e.to_string())?
        .as_nanos()
    );
    let (container, port) = runtime.block_on(async {
      let container = docker::dynamodb_local().await.map_err(|e| e.to_string())?;
      let port = container
        .get_host_port_ipv4(docker::DYNAMODB_LOCAL_PORT)
        .await
        .map_err(|e| e.to_string())?;
      Ok::<_, String>((container, port))
    })?;
    Ok(Self {
      runtime,
      container: Some(container),
      raw: dynamodb::create_dynamodb_local_client(port),
      endpoint: format!("http://127.0.0.1:{port}"),
      run_id,
      next_case: Cell::new(0),
      observations: RefCell::new(BTreeMap::new()),
      shapes: RefCell::new(BTreeMap::new()),
      table_observations: RefCell::new(Vec::new()),
    })
  }

  /// ケースと操作単位で保存した実観測を返す。
  pub fn observations(&self) -> BTreeMap<String, Vec<Value>> {
    self.observations.borrow().clone()
  }

  /// 実環境の固定イメージと独立した実行識別子を返す。
  pub fn environment(&self) -> Value {
    json!({"dynamodb_local":"3.3.1","image_digest":"sha256:ff89bd48ff32cd8d9be5fee8873b65b8854dc408f1afe881be6eb00247bc0dab",
      "run_id":self.run_id,"region":"us-west-1"})
  }

  /// 実公開生成・4操作を使い、全expect・observeと障害の終了時計数を照合する。
  pub fn run_case(&self, case: &Case, prepared: PreparedCase) -> CaseOutcome {
    if case.kind == CaseKind::Layout {
      return self.run_layout(case, &prepared.body);
    }
    let index = self.next_case.get() + 1;
    self.next_case.set(index);
    let prefix = format!("cf-{}-{index}", self.run_id);
    let names = dynamodb::TableNames {
      journal: format!("{prefix}-journal"),
      snapshot: format!("{prefix}-snapshot"),
      head: format!("{prefix}-head"),
      snapshot_history_index: format!("{prefix}-history"),
    };
    let result = self.runtime.block_on(self.execute(case, prepared, &names));
    let cleanup = self.runtime.block_on(async {
      let mut errors = Vec::new();
      for table in [&names.journal, &names.snapshot, &names.head] {
        if let Err(error) = self.raw.delete_table().table_name(table).send().await {
          errors.push(format!("{table}: {error}"));
        }
      }
      errors
    });
    if !cleanup.is_empty() {
      self.record(&case.id, json!({"cleanup_errors":cleanup}));
    }
    result.unwrap_or_else(|e| case::failed(None, e))
  }

  fn record(&self, id: &str, value: Value) {
    self.observations.borrow_mut().entry(id.into()).or_default().push(value);
  }

  fn run_layout(&self, case: &Case, body: &Value) -> CaseOutcome {
    let result = (|| -> Result<(), String> {
      if self.table_observations.borrow().is_empty() {
        return Err("実配置の取得がない".into());
      }
      let tables = self.table_observations.borrow();
      if !tables.iter().any(|(_, ttl, _)| *ttl) || !tables.iter().any(|(_, ttl, _)| !*ttl) {
        return Err("TTL有効・無効の実配置が揃っていない".into());
      }
      for (names, ttl, actual) in tables.iter() {
        layout::check_tables(case::required(body, "tables")?, actual, names, *ttl)?;
      }
      layout::check_items(case::required(body, "items")?, &self.shapes.borrow())
    })();
    self.record(&case.id,json!({"tables":self.table_observations.borrow().iter().map(|(_,ttl,v)| json!({"ttl_mode":ttl,"actual":v})).collect::<Vec<_>>(),
      "items":self.shapes.borrow().iter().map(|(kind,(table,item))| json!({"kind":kind,"table":table,"attributes":items::wire_item(item)})).collect::<Vec<_>>() }));
    match result {
      Ok(()) => CaseOutcome::Passed { values: None },
      Err(e) => case::failed(None, e),
    }
  }

  async fn execute(
    &self,
    case: &Case,
    prepared: PreparedCase,
    names: &dynamodb::TableNames,
  ) -> Result<CaseOutcome, String> {
    let body = &prepared.body;
    let settings = if case.kind == CaseKind::ValueTable {
      event_store_adapter_rs::retention::RetentionSettings::current_only()
    } else {
      case::settings(body)?
    };
    let ttl = body.pointer("/store/retention_mode").and_then(Value::as_str) == Some("ttl");
    dynamodb::create_tables(&self.raw, names, ttl)
      .await
      .map_err(|e| e.to_string())?;
    let tables = DynamoDbTables {
      journal_table_name: names.journal.clone(),
      snapshot_table_name: names.snapshot.clone(),
      head_table_name: names.head.clone(),
      snapshot_history_index_name: names.snapshot_history_index.clone(),
    };
    let names = RequestLayout::new(
      &names.journal,
      &names.snapshot,
      &names.head,
      &names.snapshot_history_index,
    )
    .map_err(|e| e.to_string())?;
    let table_observation = layout::describe(&self.raw, &names).await?;
    self.record(&case.id, json!({"tables":table_observation,"ttl_mode":ttl}));
    self
      .table_observations
      .borrow_mut()
      .push((names.clone(), ttl, table_observation.clone()));
    if let Some(seed) = body.pointer("/seed/items").and_then(Value::as_array) {
      items::seed(&self.raw, &names, seed).await?;
    }
    let transport = FaultTransport::new_with_history_client(names.clone(), self.raw.clone());
    let waits = Waits::default();
    let client = transport.client(
      aws_sdk_dynamodb::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-west-1"))
        .credentials_provider(Credentials::new("x", "x", None, None, "conformance-local"))
        .endpoint_url(&self.endpoint)
        .timeout_config(TimeoutConfig::disabled())
        .stalled_stream_protection(StalledStreamProtectionConfig::disabled())
        .sleep_impl(waits.clone()),
      aws_smithy_http_client::Builder::new().build_http(),
    );
    let options = DynamoDbOptions {
      retention: settings,
      unprocessed_retry_limit: body
        .pointer("/store/retry_limit")
        .map(case::seq)
        .transpose()?
        .map(u32::try_from)
        .transpose()
        .map_err(|e| e.to_string())?
        .unwrap_or(10),
      ..DynamoDbOptions::default()
    };
    let clock = Arc::new(MutableClock(AtomicU64::new(
      body
        .pointer("/clock/epoch_seconds")
        .map(case::seq)
        .transpose()?
        .unwrap_or(0),
    )));
    let guard = match transport.begin_operation(&prepared.faults, 0) {
      Ok(v) => v,
      Err(e) => return Ok(case::unverified(&e.to_string())),
    };
    let notices = OperationNotifications::begin(&case.id, 0)?;
    let serializers = prepared.faults.faults().iter().any(|f| {
      matches!(
        f.phase,
        Phase::SerializeEvent | Phase::SerializeSnapshot | Phase::DeserializeEvent | Phase::DeserializeSnapshot
      )
    });
    let store = async {
      if serializers {
        let serializer = Arc::new(FaultSerializer(transport.clone()));
        Store::open_with_serializers(client, tables, options, serializer.clone(), serializer).await
      } else {
        Store::open(client, tables, options).await
      }
    }
    .instrument(notices.span())
    .await;
    let notices = notices.finish();
    let report = guard.finish();
    let actual = match &store {
      Ok(_) => json!({"result":"success"}),
      Err(e) => case::error_value(e),
    };
    let initialization = body
      .get("initialization")
      .cloned()
      .unwrap_or_else(|| json!({"expect":{"result":"success"}}));
    let comparison = match &store {
      Ok(_) => case::compare_result(
        case::required(&initialization, "expect")?,
        Ok(json!({"result":"success"})),
      ),
      Err(e) => case::compare_error(case::required(&initialization, "expect")?, e),
    };
    let observed = self
      .observe(
        &initialization,
        &ObservationContext {
          case,
          id: None,
          seq_nr: None,
          layout: &names,
          tables: &table_observation,
          report: &report,
          notifications: &notices,
          waits: &waits,
        },
      )
      .await;
    self.record(&case.id,json!({"operation":0,"actual":actual,"requests":report.requests,"responses":report.responses,"fault_applications":report.applications,
      "unfired_faults":report.unfired,"notifications":notices,"waits_ms":*waits.0.lock().expect("待ち記録"),"expect":initialization["expect"],"observe":initialization.get("observe"),"observed":observed}));
    let mut failure = comparison.err().map(|v| case::failed(Some(0), v));
    if !report.unfired.is_empty() {
      failure = Some(unfired(0, &report, actual.clone()));
    }
    if !observed.errors.is_empty() && failure.is_none() {
      failure = Some(case::failed(
        Some(0),
        ComparisonFailure {
          detail: observed.errors.join("; "),
          expected: initialization.get("observe").cloned(),
          actual: Some(observed.actual),
        },
      ));
    }
    let store = match store {
      Err(_) => return Ok(failure.unwrap_or(CaseOutcome::Passed { values: None })),
      Ok(v) => v.with_clock_for_test(clock.clone()),
    };
    if case.kind == CaseKind::ValueTable {
      let guard = transport
        .begin_operation(&prepared.faults, 1)
        .map_err(|e| e.to_string())?;
      let mut actual = Vec::new();
      let result = case::run_value(case, &store, &mut actual).await;
      let report = guard.finish();
      self.record(&case.id,json!({"operation":1,"requests":report.requests,"responses":report.responses,
        "fault_applications":report.applications,"actual":actual,"expect":body["expect"],"failure":result.as_ref().err().map(|v| &v.detail)}));
      return Ok(failure.unwrap_or_else(|| match result {
        Ok(values) => CaseOutcome::Passed { values },
        Err(e) => case::failed(None, e),
      }));
    }
    for (index, input) in case::required(body, "steps")?
      .as_array()
      .ok_or("stepsが配列ではない")?
      .iter()
      .enumerate()
    {
      let operation = u32::try_from(index + 1).map_err(|e| e.to_string())?;
      waits.0.lock().expect("待ち記録").clear();
      if let Some(time) = input.get("clock_epoch_seconds") {
        clock.0.store(case::seq(time)?, Ordering::SeqCst);
      }
      let guard = match transport.begin_operation(&prepared.faults, operation) {
        Ok(v) => v,
        Err(e) => return Ok(case::unverified(&e.to_string())),
      };
      if prepared
        .faults
        .faults()
        .iter()
        .any(|f| f.operation == operation && f.kind == crate::fault::FaultKind::ReadInterleave)
      {
        let write_store = store.clone();
        let write_body = body.clone();
        transport
          .interleaved_write(move |input| {
            let store = write_store.clone();
            let body = write_body.clone();
            async move {
              let result = case::write(&store, &body, &input).await?;
              result.map_err(|e| e.to_string())
            }
          })
          .map_err(|e| e.to_string())?;
      }
      let notices = OperationNotifications::begin(&case.id, operation)?;
      let result = case::step(&store, body, input).instrument(notices.span()).await;
      let notices = notices.finish();
      let report = guard.finish();
      let (id, seq) = case::operation_context(body, input)?;
      let observed = self
        .observe(
          input,
          &ObservationContext {
            case,
            id: id.as_ref(),
            seq_nr: seq,
            layout: &names,
            tables: &table_observation,
            report: &report,
            notifications: &notices,
            waits: &waits,
          },
        )
        .await;
      let actual = match &result {
        Ok(value) => Some(value.clone()),
        Err(e) => e.actual.clone(),
      };
      self.record(&case.id,json!({"operation":operation,"clock_epoch_seconds":clock.now_epoch_seconds(),"actual":actual,
        "comparison_error":result.as_ref().err().map(|v| &v.detail),"requests":report.requests,"responses":report.responses,"fault_applications":report.applications,
        "unfired_faults":report.unfired,"notifications":notices,"waits_ms":*waits.0.lock().expect("待ち記録"),
        "expect":input["expect"],"observe":input.get("observe"),"observed":observed}));
      if failure.is_none() {
        failure = if !report.unfired.is_empty() {
          Some(unfired(operation, &report, actual.clone().unwrap_or(Value::Null)))
        } else if let Err(e) = result {
          Some(case::failed(Some(operation), e))
        } else if !observed.errors.is_empty() {
          Some(case::failed(
            Some(operation),
            ComparisonFailure {
              detail: observed.errors.join("; "),
              expected: input.get("observe").cloned(),
              actual: Some(observed.actual),
            },
          ))
        } else {
          None
        };
      }
    }
    if case.id == "dynamodb-events-over-one-megabyte" {
      let mut physical = Vec::new();
      for fixture in case::required(case::required(body, "fixtures")?, "events")?
        .as_object()
        .ok_or("eventsが辞書ではない")?
        .values()
      {
        let aid = AidString::from_aggregate_id(&case::id(case::required(fixture, "aggregate_id")?)?)
          .map_err(|e| e.to_string())?;
        let key = Item::from([
          ("aid".into(), AttributeValue::S(aid.as_str().into())),
          (
            "seq_nr".into(),
            AttributeValue::N(case::seq(case::required(fixture, "seq_nr")?)?.to_string()),
          ),
        ]);
        let result = self
          .raw
          .get_item()
          .table_name(&names.journal)
          .set_key(Some(key.clone()))
          .consistent_read(true)
          .send()
          .await
          .map_err(|e| e.to_string())?;
        let item = result.item.ok_or("正本4件の実journal項目がない")?;
        let payload_bytes = item
          .get("payload")
          .and_then(|v| v.as_b().ok())
          .map(|v| v.as_ref().len());
        physical.push(json!({"observer":{"api":"GetItem","table":names.journal,"key":items::wire_item(&key),"consistent_read":true},"attributes":items::wire_item(&item),"payload_bytes":payload_bytes}));
      }
      self.record(&case.id, json!({"physical_items":physical}));
    }
    Ok(failure.unwrap_or(CaseOutcome::Passed { values: None }))
  }

  async fn observe(&self, input: &Value, context: &ObservationContext<'_>) -> Observation {
    let mut actual = serde_json::Map::new();
    let mut errors = Vec::new();
    let empty = json!({});
    let observe = input.get("observe").unwrap_or(&empty);
    let aid = context.id.and_then(|v| AidString::from_aggregate_id(v).ok());
    if let Some(expected) = observe.get("notifications") {
      let notices = json!(context.notifications);
      if !json_equal(expected, &notices) {
        errors.push("保持失敗通知が一致しない".into());
      }
      actual.insert("notifications".into(), notices);
    }
    if let Some(expected) = observe.get("history") {
      let observed = match &aid {
        Some(aid) => history(&self.raw, context.layout, aid.as_str()).await,
        None => Err("履歴を観測する有効な集約がない".into()),
      };
      match observed {
        Ok(history) => {
          let mut expected = expected.clone();
          let absent = expected["absent"].take();
          expected.as_object_mut().expect("history").remove("absent");
          let absent_matches = absent.as_array().is_some_and(|v| {
            v.iter().all(|v| {
              !history["active"].as_array().expect("active").contains(v)
                && !history["marked"]
                  .as_array()
                  .expect("marked")
                  .iter()
                  .any(|m| m["seq_nr"] == *v)
            })
          });
          if !json_equal(&expected, &history) || !absent_matches {
            errors.push("実保存履歴が一致しない".into());
          }
          actual.insert("history".into(), history);
        }
        Err(e) => errors.push(e),
      }
    }
    if let Some(declared) = observe.get("items").and_then(Value::as_array) {
      let mut bindings = BTreeMap::new();
      let mut observed = Vec::new();
      for declared in declared {
        match items::get(&self.raw, context.layout, declared).await {
          Ok(item) => {
            let table = declared["table"].as_str().expect("Schemaで検査した表");
            if context.case.id == "dynamodb-config-new" || context.case.id == "dynamodb-written-item-shapes" {
              match layout::item_kind(table, &item) {
                Ok(kind) => {
                  self.shapes.borrow_mut().insert(kind, (table.into(), item.clone()));
                }
                Err(e) => errors.push(e),
              }
            }
            let check = items::compare_item(declared, &item, &mut bindings);
            observed.push(
              json!({"table":table,"attributes":items::wire_item(&item),"comparison_error":check.as_ref().err()}),
            );
            if let Err(e) = check {
              errors.push(e);
            }
          }
          Err(e) => {
            observed.push(json!({"table":declared["table"],"error":e}));
            errors.push(e);
          }
        }
      }
      actual.insert("items".into(), json!(observed));
    }
    let projection = context
      .tables
      .as_array()
      .and_then(|v| v.iter().find(|v| v["name"] == "snapshot"))
      .and_then(|v| v.pointer("/gsi/0/projection"))
      .and_then(Value::as_str);
    let waits = context.waits.0.lock().expect("待ち記録").clone();
    let checks = check_requests::check(
      observe,
      context.report,
      &RequestContext {
        layout: context.layout,
        aid: aid.as_ref().map(AidString::as_str),
        seq_nr: context.seq_nr,
        history: actual.get("history"),
        projection,
        waits: &waits,
      },
    );
    errors.extend(checks.errors.iter().cloned());
    actual.insert("requests".into(), json!(checks.entries));
    Observation {
      actual: Value::Object(actual),
      errors,
    }
  }
}

impl Drop for Execution {
  fn drop(&mut self) {
    let _entered = self.runtime.enter();
    drop(self.container.take());
  }
}

fn unfired(operation: u32, report: &OperationReport, actual: Value) -> CaseOutcome {
  CaseOutcome::Failed {
    failed_operation: Some(operation),
    detail: "障害の適用回数が宣言を満たさない".into(),
    expected: None,
    actual: Some(actual),
    unfired_faults: report.unfired.clone(),
  }
}

async fn history(raw: &Client, names: &RequestLayout, aid: &str) -> Result<Value, String> {
  let mut active = Vec::new();
  let mut marked = Vec::new();
  let mut continuation = None;
  loop {
    let response = raw
      .query()
      .table_name(&names.snapshot)
      .key_condition_expression("aid = :aid")
      .expression_attribute_values(":aid", AttributeValue::S(aid.into()))
      .consistent_read(true)
      .scan_index_forward(true)
      .set_exclusive_start_key(continuation)
      .send()
      .await
      .map_err(|e| e.to_string())?;
    for item in response.items() {
      let number = |name| {
        item
          .get(name)
          .and_then(|v| v.as_n().ok())
          .and_then(|v| v.parse::<u64>().ok())
          .ok_or_else(|| format!("履歴の{name}が整数ではない"))
      };
      let skey = number("skey")?;
      if skey == 0 {
        continue;
      }
      let seq = number("seq_nr")?;
      if item.contains_key("active_history_seq_nr") {
        active.push(seq);
      } else if item.contains_key("ttl") {
        marked.push(json!({"seq_nr":seq,"ttl":number("ttl")?}));
      } else {
        return Err("履歴がactiveでもmarkedでもない".into());
      }
    }
    continuation = response.last_evaluated_key.filter(|v| !v.is_empty());
    if continuation.is_none() {
      break;
    }
  }
  active.sort_unstable();
  marked.sort_by_key(|v| v["seq_nr"].as_u64());
  Ok(json!({"active":active,"marked":marked}))
}

#[cfg(test)]
#[path = "execute_test.rs"]
mod tests;
