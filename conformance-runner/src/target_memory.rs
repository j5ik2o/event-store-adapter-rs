//! 新契約メモリの公開操作を実行し、保存後の封筒と障害消費を検証する。

use std::sync::{Arc, Mutex};

use chrono::{DateTime, SecondsFormat, Utc};
use event_store_adapter_rs::next::{
  aggregate_id::{AggregateId, AidString},
  error::{EventStoreError, SerializationPhase, StorageOperation},
  event_envelope::{EventEnvelope, SnapshotEnvelope},
  event_store::EventStore,
  memory::{EventStoreForMemory, MemoryStorage, MemoryTestHooks},
  retention::{RetentionMode, RetentionSettings},
  serializer::{EventSerializer, JsonEventSerializer, JsonSnapshotSerializer, SnapshotSerializer},
};
use serde_json::{json, Value};

use crate::{
  compare::json_equal,
  data::{Case, CaseKind},
  fault::{FaultKind, Injection, OperationFaults, Phase},
  number::to_integer,
  report::{CaseOutcome, UnverifiedReason},
  runner::{PreparedCase, Target},
};

/// メモリの保存先を表す。
///
/// 任意の能力を提供しない（期限切れ方式の TTL は提供しない。MEM-12）。DynamoDB の 3 テーブルの配置もない。
pub const TARGET: Target = Target {
  name: "memory",
  capabilities: &[],
  has_layout: false,
};

#[derive(Debug, Clone)]
struct CaseId {
  type_name: String,
  value: String,
}

impl AggregateId for CaseId {
  fn type_name(&self) -> String {
    self.type_name.clone()
  }

  fn value(&self) -> String {
    self.value.clone()
  }
}

type Store = EventStoreForMemory<CaseId, Value, Value>;

#[derive(Debug)]
struct Callbacks {
  faults: Mutex<OperationFaults>,
}

impl Callbacks {
  fn inject(&self, phase: Phase) -> Result<(), EventStoreError> {
    let mut faults = self.faults.lock().expect("障害消費のロック");
    let Some(fault) = faults.start_application(phase) else {
      return Ok(());
    };
    let source = Box::new(std::io::Error::other(
      fault
        .details
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("injected fault")
        .to_owned(),
    ));
    Err(match phase {
      Phase::SerializeEvent => EventStoreError::Serialization {
        phase: SerializationPhase::SerializeEvent,
        source,
      },
      Phase::SerializeSnapshot => EventStoreError::Serialization {
        phase: SerializationPhase::SerializeSnapshot,
        source,
      },
      Phase::DeserializeEvent => EventStoreError::Serialization {
        phase: SerializationPhase::DeserializeEvent,
        source,
      },
      Phase::DeserializeSnapshot => EventStoreError::Serialization {
        phase: SerializationPhase::DeserializeSnapshot,
        source,
      },
      Phase::Commit => EventStoreError::Storage {
        operation: StorageOperation::Append,
        source,
      },
      Phase::ReadEvents => EventStoreError::Storage {
        operation: StorageOperation::LoadEvents,
        source,
      },
      Phase::ReadSnapshot => EventStoreError::Storage {
        operation: StorageOperation::LoadSnapshot,
        source,
      },
      _ => unreachable!("対応する段階は実行前に検査する"),
    })
  }
}

impl MemoryTestHooks for Callbacks {
  fn before_commit(&self, _: &AidString, _: u64) -> Result<(), EventStoreError> {
    self.inject(Phase::Commit)
  }

  fn read_events(&self, _: &AidString) -> Result<(), EventStoreError> {
    self.inject(Phase::ReadEvents)
  }

  fn read_snapshot(&self, _: &AidString) -> Result<(), EventStoreError> {
    self.inject(Phase::ReadSnapshot)
  }
}

impl EventSerializer<Value> for Callbacks {
  fn serialize(&self, payload: &Value) -> Result<Vec<u8>, EventStoreError> {
    self.inject(Phase::SerializeEvent)?;
    JsonEventSerializer::new().serialize(payload)
  }

  fn deserialize(&self, data: &[u8]) -> Result<Value, EventStoreError> {
    self.inject(Phase::DeserializeEvent)?;
    JsonEventSerializer::new().deserialize(data)
  }
}

impl SnapshotSerializer<Value> for Callbacks {
  fn serialize(&self, aggregate: &Value) -> Result<Vec<u8>, EventStoreError> {
    self.inject(Phase::SerializeSnapshot)?;
    JsonSnapshotSerializer::new().serialize(aggregate)
  }

  fn deserialize(&self, data: &[u8]) -> Result<Value, EventStoreError> {
    self.inject(Phase::DeserializeSnapshot)?;
    JsonSnapshotSerializer::new().deserialize(data)
  }
}

fn required<'a>(value: &'a Value, key: &str) -> Result<&'a Value, String> {
  value.get(key).ok_or_else(|| format!("必須要素 {key} がない"))
}

fn string(value: &Value) -> Result<&str, String> {
  value.as_str().ok_or_else(|| "文字列ではない".to_owned())
}

fn seq(value: &Value) -> Result<u64, String> {
  to_integer(value)
    .and_then(|v| u64::try_from(v).ok())
    .ok_or_else(|| format!("u64 で表せる整数ではない: {value}"))
}

fn id(value: &Value) -> Result<CaseId, String> {
  Ok(CaseId {
    type_name: string(required(value, "type_name")?)?.to_owned(),
    value: string(required(value, "value")?)?.to_owned(),
  })
}

fn time(value: &Value) -> Result<DateTime<Utc>, String> {
  DateTime::parse_from_rfc3339(string(value)?)
    .map(|v| v.with_timezone(&Utc))
    .map_err(|e| e.to_string())
}

fn manifest(value: &Value) -> Result<&str, String> {
  match value.get("manifest") {
    Some(value) => string(value),
    None => Ok(""),
  }
}

fn event(value: &Value) -> Result<EventEnvelope<CaseId, Value>, String> {
  Ok(
    EventEnvelope::new(
      id(required(value, "aggregate_id")?)?,
      seq(required(value, "seq_nr")?)?,
      time(required(value, "occurred_at")?)?,
      required(value, "payload")?.clone(),
    )
    .with_manifest(manifest(value)?),
  )
}

fn snapshot(value: &Value) -> Result<SnapshotEnvelope<Value>, String> {
  Ok(
    SnapshotEnvelope::new(required(value, "aggregate")?.clone(), seq(required(value, "seq_nr")?)?)
      .with_manifest(manifest(value)?),
  )
}

fn event_value(event: &EventEnvelope<CaseId, Value>) -> Value {
  json!({
    "aggregate_id": {"type_name": event.aggregate_id().type_name(), "value": event.aggregate_id().value()},
    "seq_nr": event.seq_nr(), "occurred_at": event.occurred_at().to_rfc3339_opts(SecondsFormat::Nanos, true),
    "manifest": event.manifest(), "payload": event.payload()
  })
}

fn snapshot_value(snapshot: &SnapshotEnvelope<Value>) -> Value {
  json!({"seq_nr":snapshot.seq_nr(), "manifest":snapshot.manifest(), "aggregate":snapshot.aggregate()})
}

fn fixture<'a>(body: &'a Value, kind: &str, name: &Value) -> Result<&'a Value, String> {
  required(required(required(body, "fixtures")?, kind)?, string(name)?)
}

fn error_matches(expected: &Value, error: &EventStoreError) -> Result<(), String> {
  let category = match error {
    EventStoreError::OptimisticLock { .. } => "optimistic-lock",
    EventStoreError::ContractViolation { .. } => "contract-violation",
    EventStoreError::Serialization { .. } => "serialization",
    EventStoreError::Configuration { .. } => "configuration",
    EventStoreError::Storage { .. } => "storage",
    _ => return Err(format!("未知のエラー分類: {error}")),
  };
  if required(expected, "category")?.as_str() != Some(category) {
    return Err(format!("エラー分類が一致しない: {error}"));
  }
  if let Some(rule) = expected.get("rule") {
    match error {
      EventStoreError::ContractViolation { rule: actual, .. } if string(rule)? == actual.to_string() => {}
      _ => return Err(format!("規則が一致しない: {error}")),
    }
  }
  let text = error.to_string();
  if let Some(message) = expected.get("message") {
    for (key, must_contain) in [("must_contain", true), ("must_not_contain", false)] {
      if let Some(needles) = message.get(key) {
        for needle in needles.as_array().ok_or("メッセージ条件が配列ではない")? {
          if text.contains(string(needle)?) != must_contain {
            return Err(format!("メッセージ条件 {key} を満たさない: {text}"));
          }
        }
      }
    }
  }
  Ok(())
}

struct ComparisonFailure {
  detail: String,
  expected: Option<Value>,
  actual: Option<Value>,
}

impl From<String> for ComparisonFailure {
  fn from(detail: String) -> Self {
    Self {
      detail,
      expected: None,
      actual: None,
    }
  }
}

impl From<&str> for ComparisonFailure {
  fn from(detail: &str) -> Self {
    detail.to_owned().into()
  }
}

fn compare_result(expected: &Value, actual: Result<Value, EventStoreError>) -> Result<Value, ComparisonFailure> {
  match actual {
    Err(error) => {
      let actual = json!({"error": error.to_string()});
      required(expected, "error")
        .and_then(|expected| error_matches(expected, &error))
        .map_err(|detail| ComparisonFailure {
          detail,
          expected: Some(expected.clone()),
          actual: Some(actual.clone()),
        })?;
      Ok(actual)
    }
    Ok(actual) => {
      if !json_equal(expected, &actual) {
        return Err(ComparisonFailure {
          detail: "期待値と実結果が一致しない".to_owned(),
          expected: Some(expected.clone()),
          actual: Some(actual),
        });
      }
      Ok(actual)
    }
  }
}

fn failed(operation: Option<u32>, failure: impl Into<ComparisonFailure>) -> CaseOutcome {
  let failure = failure.into();
  CaseOutcome::Failed {
    failed_operation: operation,
    detail: failure.detail,
    expected: failure.expected,
    actual: failure.actual,
    unfired_faults: Vec::new(),
  }
}

fn unverified(detail: &str) -> CaseOutcome {
  CaseOutcome::Unverified {
    reason: UnverifiedReason::NotExecuted {
      detail: detail.to_owned(),
    },
  }
}

fn settings(body: &Value) -> Result<RetentionSettings, String> {
  let store = required(body, "store")?;
  let count = required(store, "retention_count")?;
  let settings = if count.is_null() {
    RetentionSettings::current_only()
  } else {
    RetentionSettings::keep_latest(usize::try_from(seq(count)?).map_err(|e| e.to_string())?)
  };
  match string(required(store, "retention_mode")?)? {
    "delete" => Ok(settings),
    "ttl" => Ok(settings.with_mode(RetentionMode::Ttl {
      grace_seconds: seq(required(store, "ttl_grace_seconds")?)?,
    })),
    _ => Err("未知の保持方式".to_owned()),
  }
}

async fn step(store: &Store, body: &Value, step: &Value) -> Result<(Value, Option<CaseId>), ComparisonFailure> {
  let args = required(step, "arguments")?;
  let expect = required(step, "expect")?;
  let (actual, expected, observed_id) = match string(required(step, "op")?)? {
    "persistEvent" | "persistEventAndSnapshot" => {
      let event = event(fixture(body, "events", required(args, "event")?)?)?;
      let observed_id = event.aggregate_id().clone();
      let result = if step["op"] == "persistEvent" {
        store.persist_event(event).await
      } else {
        let snapshot = snapshot(fixture(body, "snapshots", required(args, "snapshot")?)?)?;
        store.persist_event_and_snapshot(event, snapshot).await
      };
      (
        result.map(|()| json!({"result":"success"})),
        expect.clone(),
        Some(observed_id),
      )
    }
    "getEventsByIdSinceSeqNr" => {
      let id = id(required(args, "aggregate_id")?)?;
      let result = store
        .get_events_by_id_since_seq_nr(&id, seq(required(args, "seq_nr")?)?)
        .await;
      let expected = if expect.get("error").is_some() {
        expect.clone()
      } else {
        let events = required(expect, "events")?
          .as_array()
          .ok_or("events が配列ではない")?
          .iter()
          .map(|name| event(fixture(body, "events", name)?).map(|v| event_value(&v)))
          .collect::<Result<Vec<_>, _>>()?;
        let mut expected = expect.clone();
        expected["events"] = json!(events);
        expected
      };
      (
        result.map(|events| json!({"result":"events", "events": events.iter().map(event_value).collect::<Vec<_>>()})),
        expected,
        Some(id),
      )
    }
    "getLatestSnapshotById" => {
      let id = id(required(args, "aggregate_id")?)?;
      let result = store.get_latest_snapshot_by_id(&id).await.map(|read| match read {
        None => json!({"result":"none"}),
        Some(read) => {
          json!({"result":"snapshot", "head_seq_nr":read.head_seq_nr(), "snapshot":read.snapshot().map(snapshot_value)})
        }
      });
      let mut expected = expect.clone();
      if let Some(name) = expect.get("snapshot").filter(|name| !name.is_null()) {
        expected["snapshot"] = snapshot_value(&snapshot(fixture(body, "snapshots", name)?)?);
      }
      (result, expected, Some(id))
    }
    op => return Err(format!("未知の操作: {op}").into()),
  };
  compare_result(&expected, actual).map(|actual| (actual, observed_id))
}

fn observe(storage: &MemoryStorage, step: &Value, id: Option<&CaseId>) -> Result<(), ComparisonFailure> {
  let Some(observe) = step.get("observe") else {
    return Ok(());
  };
  for (key, value) in observe.as_object().ok_or("observe がオブジェクトではない")? {
    if key != "history" {
      return Err(format!("未対応の観測: {key}").into());
    }
    let aid = AidString::from_aggregate_id(id.ok_or("観測する集約 ID がない")?).map_err(|e| e.to_string())?;
    let history = storage.history_view(&aid).map_err(|e| e.to_string())?;
    let active = required(value, "active")?
      .as_array()
      .ok_or("active が配列ではない")?
      .iter()
      .map(seq)
      .collect::<Result<Vec<_>, _>>()?;
    let marked = required(value, "marked")?.as_array().ok_or("marked が配列ではない")?;
    let absent = required(value, "absent")?
      .as_array()
      .ok_or("absent が配列ではない")?
      .iter()
      .map(seq)
      .collect::<Result<Vec<_>, _>>()?;
    let mut expected = active;
    expected.sort_unstable();
    if history != expected || !marked.is_empty() || absent.iter().any(|n| history.contains(n)) {
      return Err(ComparisonFailure {
        detail: "履歴が一致しない".to_owned(),
        expected: Some(value.clone()),
        actual: Some(json!({"active": history, "marked": []})),
      });
    }
  }
  Ok(())
}

/// ケースごとに独立した保存先を作り、公開操作を順に実行する。
pub(crate) fn run_case(case: &Case, prepared: PreparedCase) -> CaseOutcome {
  let body = &prepared.body;
  if matches!(case.kind, CaseKind::Scenario)
    && body
      .pointer("/store/retention_count")
      .is_some_and(|v| !v.is_null() && to_integer(v).is_some_and(|n| n > 0))
  {
    return unverified("保持の間引きと失敗通知は次工程で実装するため、未検証");
  }
  for fault in prepared.faults.faults() {
    let supported = match fault.phase {
      Phase::SerializeEvent | Phase::SerializeSnapshot | Phase::DeserializeEvent | Phase::DeserializeSnapshot => {
        fault.kind == FaultKind::SerializationError
      }
      Phase::Commit | Phase::ReadEvents | Phase::ReadSnapshot => fault.kind == FaultKind::StorageError,
      _ => false,
    };
    if !supported || fault.injection != Injection::ReplaceRequest {
      return unverified("宣言された障害の段階・種類・方式へ未接続");
    }
  }
  let callbacks = Arc::new(Callbacks {
    faults: Mutex::new(prepared.faults.begin_operation(0)),
  });
  let settings = if matches!(case.kind, CaseKind::ValueTable) {
    Ok(RetentionSettings::current_only())
  } else {
    settings(body)
  };
  let settings = match settings {
    Ok(v) => v,
    Err(e) => return failed(Some(0), e),
  };
  let storage = MemoryStorage::new_with_hooks(settings, callbacks.clone());
  let faults = std::mem::replace(
    &mut *callbacks.faults.lock().expect("障害消費のロック"),
    prepared.faults.begin_operation(0),
  );
  if let Err(unfired_faults) = faults.finish() {
    return CaseOutcome::Failed {
      failed_operation: Some(0),
      detail: "生成時の障害の適用回数が宣言を満たさない".to_owned(),
      expected: body.pointer("/initialization/expect").cloned(),
      actual: None,
      unfired_faults,
    };
  }
  let initialization = body.pointer("/initialization/expect");
  let storage = match storage {
    Err(error) => {
      return match initialization {
        Some(expected) => match compare_result(expected, Err(error)) {
          Ok(_) => CaseOutcome::Passed,
          Err(failure) => failed(Some(0), failure),
        },
        None => failed(
          Some(0),
          ComparisonFailure {
            detail: error.to_string(),
            expected: None,
            actual: Some(json!({"error": error.to_string()})),
          },
        ),
      }
    }
    Ok(storage) => storage,
  };
  if let Some(expected) = initialization {
    if let Err(e) = compare_result(expected, Ok(json!({"result":"success"}))) {
      return failed(Some(0), e);
    }
  }
  let store = Store::with_serializers(storage.clone(), callbacks.clone(), callbacks.clone());
  let runtime = match tokio::runtime::Builder::new_current_thread().build() {
    Ok(runtime) => runtime,
    Err(e) => return failed(None, e.to_string()),
  };
  if matches!(case.kind, CaseKind::ValueTable) {
    return match runtime.block_on(run_value(case, &store)) {
      Ok(()) => CaseOutcome::Passed,
      Err(e) => failed(None, e),
    };
  }
  let Some(steps) = body.get("steps").and_then(Value::as_array) else {
    return failed(None, "steps がない".to_owned());
  };
  for (index, input) in steps.iter().enumerate() {
    let operation = match u32::try_from(index + 1) {
      Ok(v) => v,
      Err(e) => return failed(None, e.to_string()),
    };
    *callbacks.faults.lock().expect("障害消費のロック") = prepared.faults.begin_operation(operation);
    let result = runtime.block_on(step(&store, body, input));
    let faults = std::mem::replace(
      &mut *callbacks.faults.lock().expect("障害消費のロック"),
      prepared.faults.begin_operation(0),
    );
    if let Err(unfired_faults) = faults.finish() {
      return CaseOutcome::Failed {
        failed_operation: Some(operation),
        detail: "障害の適用回数が宣言を満たさない".to_owned(),
        expected: input.get("expect").cloned(),
        actual: match &result {
          Ok((value, _)) => Some(value.clone()),
          Err(failure) => failure.actual.clone(),
        },
        unfired_faults,
      };
    }
    match result {
      Ok((_, id)) => {
        if let Err(e) = observe(&storage, input, id.as_ref()) {
          return failed(Some(operation), e);
        }
      }
      Err(e) => return failed(Some(operation), e),
    }
  }
  CaseOutcome::Passed
}

async fn run_value(case: &Case, store: &Store) -> Result<(), ComparisonFailure> {
  let input = required(&case.body, "input")?;
  let expected = required(&case.body, "expect")?;
  let id = CaseId {
    type_name: "ConformanceTime".to_owned(),
    value: case.id.clone(),
  };
  let default_time = time(&json!("1970-01-01T00:00:00.123000000Z"))?;
  match string(required(&case.body, "operation")?)? {
    "validateSeqNr" => {
      let seq = seq(required(input, "seq_nr")?)?;
      let result = match string(required(input, "context")?)? {
        "value" => store
          .get_events_by_id_since_seq_nr(&id, seq)
          .await
          .map(|_| json!({"value": seq})),
        "event" => store
          .persist_event(EventEnvelope::new(id, seq, default_time, json!({})))
          .await
          .map(|()| json!({"value": seq})),
        _ => return Err("未知の番号の文脈".into()),
      };
      compare_result(expected, result)?;
    }
    "validateOccurredAt" => {
      let seq = seq(required(input, "event_seq_nr")?)?;
      for prior in 1..seq {
        store
          .persist_event(EventEnvelope::new(id.clone(), prior, default_time, json!({})))
          .await
          .map_err(|e| e.to_string())?;
      }
      let occurred_at = time(required(input, "iso8601")?)?;
      let input_nanos = string(required(input, "epoch_nanoseconds")?)?
        .parse::<i128>()
        .map_err(|e| e.to_string())?;
      let converted_nanos =
        i128::from(occurred_at.timestamp()) * 1_000_000_000 + i128::from(occurred_at.timestamp_subsec_nanos());
      if input_nanos != converted_nanos {
        return Err("入力の ISO 時刻とエポックナノ秒が一致しない".into());
      }
      let result = store
        .persist_event(EventEnvelope::new(id.clone(), seq, occurred_at, json!({})))
        .await;
      match result {
        Err(error) => {
          compare_result(expected, Err(error))?;
        }
        Ok(()) => {
          let events = store
            .get_events_by_id_since_seq_nr(&id, seq)
            .await
            .map_err(|e| e.to_string())?;
          let [event] = events.as_slice() else {
            return Err("書き込んだ時刻のイベントが1件ではない".into());
          };
          let actual = event
            .occurred_at()
            .timestamp_nanos_opt()
            .ok_or("復元時刻が範囲外")?
            .to_string();
          if expected.get("error").is_some()
            || required(expected, "value")?.as_str() != Some(actual.as_str())
            || event.occurred_at() != &occurred_at
          {
            return Err(ComparisonFailure {
              detail: "復元時刻が一致しない".to_owned(),
              expected: Some(expected.clone()),
              actual: Some(json!({"value":actual})),
            });
          }
        }
      }
    }
    operation => return Err(format!("未対応の値の表の操作: {operation}").into()),
  }
  Ok(())
}
