//! 両保存先が使うfixture変換、公開操作、native結果の比較。

use crate::{
  compare::json_equal,
  data::Case,
  number::to_integer,
  report::{CaseOutcome, ObservedValues, UnverifiedReason},
};
use chrono::{DateTime, SecondsFormat, Utc};
use event_store_adapter_rs::{
  aggregate_id::AggregateId,
  error::EventStoreError,
  event_envelope::{EventEnvelope, SnapshotEnvelope},
  event_store::EventStore,
  retention::{RetentionMode, RetentionSettings},
};
use serde_json::{json, Value};

#[derive(Debug, Clone)]
pub(crate) struct CaseId {
  pub(crate) type_name: String,
  pub(crate) value: String,
}

impl AggregateId for CaseId {
  fn type_name(&self) -> String {
    self.type_name.clone()
  }

  fn value(&self) -> String {
    self.value.clone()
  }
}

pub(crate) fn required<'a>(value: &'a Value, key: &str) -> Result<&'a Value, String> {
  value.get(key).ok_or_else(|| format!("必須要素 {key} がない"))
}

pub(crate) fn string(value: &Value) -> Result<&str, String> {
  value.as_str().ok_or_else(|| "文字列ではない".to_owned())
}

pub(crate) fn seq(value: &Value) -> Result<u64, String> {
  to_integer(value)
    .and_then(|v| u64::try_from(v).ok())
    .ok_or_else(|| format!("u64 で表せる整数ではない: {value}"))
}

pub(crate) fn id(value: &Value) -> Result<CaseId, String> {
  Ok(CaseId {
    type_name: string(required(value, "type_name")?)?.to_owned(),
    value: string(required(value, "value")?)?.to_owned(),
  })
}

pub(crate) fn time(value: &Value) -> Result<DateTime<Utc>, String> {
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

pub(crate) fn event(value: &Value) -> Result<EventEnvelope<CaseId, Value>, String> {
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

pub(crate) fn snapshot(value: &Value) -> Result<SnapshotEnvelope<Value>, String> {
  Ok(
    SnapshotEnvelope::new(required(value, "aggregate")?.clone(), seq(required(value, "seq_nr")?)?)
      .with_manifest(manifest(value)?),
  )
}

pub(crate) fn event_value(event: &EventEnvelope<CaseId, Value>) -> Value {
  json!({
    "aggregate_id": {"type_name": event.aggregate_id().type_name(), "value": event.aggregate_id().value()},
    "seq_nr": event.seq_nr(), "occurred_at": event.occurred_at().to_rfc3339_opts(SecondsFormat::Nanos, true),
    "manifest": event.manifest(), "payload": event.payload()
  })
}

pub(crate) fn snapshot_value(snapshot: &SnapshotEnvelope<Value>) -> Value {
  json!({"seq_nr":snapshot.seq_nr(), "manifest":snapshot.manifest(), "aggregate":snapshot.aggregate()})
}

pub(crate) fn fixture<'a>(body: &'a Value, kind: &str, name: &Value) -> Result<&'a Value, String> {
  required(required(required(body, "fixtures")?, kind)?, string(name)?)
}

pub(crate) fn error_matches(expected: &Value, error: &EventStoreError) -> Result<(), String> {
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

pub(crate) fn error_value(error: &EventStoreError) -> Value {
  let mut value = json!({"error":error.to_string()});
  let fields = match error {
    EventStoreError::OptimisticLock {
      aid,
      seq_nr,
      head_seq_nr,
    } => json!({"category":"optimistic-lock","aid":aid,"seq_nr":seq_nr,"head_seq_nr":head_seq_nr}),
    EventStoreError::ContractViolation {
      rule,
      seq_nr,
      snapshot_seq_nr,
    } => {
      json!({"category":"contract-violation","rule":rule.to_string(),"seq_nr":seq_nr,"snapshot_seq_nr":snapshot_seq_nr})
    }
    EventStoreError::Serialization { phase, .. } => json!({"category":"serialization","phase":phase.to_string()}),
    EventStoreError::Configuration { reason } => json!({"category":"configuration","reason":reason.to_string()}),
    EventStoreError::Storage { operation, .. } => json!({"category":"storage","operation":operation.to_string()}),
    _ => json!({"category":"unknown"}),
  };
  value
    .as_object_mut()
    .expect("native診断")
    .extend(fields.as_object().expect("native項目").clone());
  value
}

pub(crate) fn compare_error(expected: &Value, error: &EventStoreError) -> Result<Value, ComparisonFailure> {
  let actual = error_value(error);
  required(expected, "error")
    .and_then(|v| error_matches(v, error))
    .map_err(|detail| ComparisonFailure {
      detail,
      expected: Some(expected.clone()),
      actual: Some(actual.clone()),
    })?;
  Ok(actual)
}

pub(crate) struct ComparisonFailure {
  pub(crate) detail: String,
  pub(crate) expected: Option<Value>,
  pub(crate) actual: Option<Value>,
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

pub(crate) fn compare_result(
  expected: &Value,
  actual: Result<Value, EventStoreError>,
) -> Result<Value, ComparisonFailure> {
  match actual {
    Err(error) => compare_error(expected, &error),
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

pub(crate) fn failed(operation: Option<u32>, failure: impl Into<ComparisonFailure>) -> CaseOutcome {
  let failure = failure.into();
  CaseOutcome::Failed {
    failed_operation: operation,
    detail: failure.detail,
    expected: failure.expected,
    actual: failure.actual,
    unfired_faults: Vec::new(),
  }
}

pub(crate) fn unverified(detail: &str) -> CaseOutcome {
  CaseOutcome::Unverified {
    reason: UnverifiedReason::NotExecuted {
      detail: detail.to_owned(),
    },
  }
}

pub(crate) fn settings(body: &Value) -> Result<RetentionSettings, String> {
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

pub(crate) async fn write<S: EventStore<AID = CaseId, A = Value, P = Value>>(
  store: &S,
  body: &Value,
  input: &Value,
) -> Result<Result<(), EventStoreError>, String> {
  let args = required(input, "arguments")?;
  let event = event(fixture(body, "events", required(args, "event")?)?)?;
  let result = match string(required(input, "op")?)? {
    "persistEvent" => store.persist_event(event).await,
    "persistEventAndSnapshot" => {
      store
        .persist_event_and_snapshot(
          event,
          snapshot(fixture(body, "snapshots", required(args, "snapshot")?)?)?,
        )
        .await
    }
    op => return Err(format!("追記操作ではない: {op}")),
  };
  Ok(result)
}

pub(crate) fn operation_context(body: &Value, input: &Value) -> Result<(Option<CaseId>, Option<u64>), String> {
  let args = required(input, "arguments")?;
  if let Some(name) = args.get("event") {
    let event = fixture(body, "events", name)?;
    Ok((
      Some(id(required(event, "aggregate_id")?)?),
      Some(seq(required(event, "seq_nr")?)?),
    ))
  } else {
    Ok((
      args.get("aggregate_id").map(id).transpose()?,
      args.get("seq_nr").map(seq).transpose()?,
    ))
  }
}

pub(crate) async fn step<S: EventStore<AID = CaseId, A = Value, P = Value>>(
  store: &S,
  body: &Value,
  step: &Value,
) -> Result<Value, ComparisonFailure> {
  let args = required(step, "arguments")?;
  let expect = required(step, "expect")?;
  let (actual, expected) = match string(required(step, "op")?)? {
    "persistEvent" | "persistEventAndSnapshot" => {
      let result = write(store, body, step).await?;
      (result.map(|()| json!({"result":"success"})), expect.clone())
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
      (result, expected)
    }
    op => return Err(format!("未知の操作: {op}").into()),
  };
  compare_result(&expected, actual)
}

#[cfg(test)]
#[path = "case_test.rs"]
mod tests;

pub(crate) async fn run_value<S: EventStore<AID = CaseId, A = Value, P = Value>>(
  case: &Case,
  store: &S,
  observations: &mut Vec<Value>,
) -> Result<Option<ObservedValues>, ComparisonFailure> {
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
      observations.push(json!({"actual":match &result {Ok(v)=>v.clone(),Err(e)=>error_value(e)}}));
      compare_result(expected, result)?;
    }
    "validateOccurredAt" => {
      let seq = seq(required(input, "event_seq_nr")?)?;
      for prior in 1..seq {
        let result = store
          .persist_event(EventEnvelope::new(id.clone(), prior, default_time, json!({})))
          .await;
        let actual = match &result {
          Ok(()) => json!({"result":"success"}),
          Err(e) => error_value(e),
        };
        observations.push(json!({"op":"persistEvent","seq_nr":prior,"actual":actual}));
        result.map_err(|e| ComparisonFailure {
          detail: e.to_string(),
          expected: None,
          actual: Some(actual),
        })?;
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
      observations.push(json!({"op":"persistEvent","seq_nr":seq,"actual":match &result {Ok(())=>json!({"result":"success"}),Err(e)=>error_value(e)}}));
      match result {
        Err(error) => {
          compare_result(expected, Err(error))?;
        }
        Ok(()) => {
          let result = store.get_events_by_id_since_seq_nr(&id, seq).await;
          let actual = match &result {
            Ok(events) => json!({"result":"events","events":events.iter().map(event_value).collect::<Vec<_>>()}),
            Err(e) => error_value(e),
          };
          observations.push(json!({"op":"getEventsByIdSinceSeqNr","actual":actual}));
          let events = result.map_err(|e| ComparisonFailure {
            detail: e.to_string(),
            expected: None,
            actual: Some(actual),
          })?;
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
          return Ok(Some(ObservedValues {
            expected: json!({"value": converted_nanos.to_string()}),
            actual: json!({"value": actual}),
          }));
        }
      }
    }
    operation => return Err(format!("未対応の値の表の操作: {operation}").into()),
  }
  Ok(None)
}
