//! 新契約メモリの公開操作を実行し、保存後の封筒と障害消費を検証する。

use std::sync::{Arc, Mutex};

use event_store_adapter_rs::next::{
  aggregate_id::AidString,
  error::{EventStoreError, SerializationPhase, StorageOperation},
  memory::{EventStoreForMemory, MemoryStorage, MemoryTestHooks},
  retention::RetentionSettings,
  serializer::{EventSerializer, JsonEventSerializer, JsonSnapshotSerializer, SnapshotSerializer},
};
use serde_json::{json, Value};
use tracing::Instrument;

use crate::{
  case::{compare_result, failed, required, run_value, seq, settings, step, unverified, CaseId, ComparisonFailure},
  compare::json_equal,
  data::{Case, CaseKind},
  fault::{FaultKind, Injection, OperationFaults, Phase},
  notifications::OperationNotifications,
  report::CaseOutcome,
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

type Store = EventStoreForMemory<CaseId, Value, Value>;

#[derive(Debug)]
struct Callbacks {
  faults: Mutex<OperationFaults>,
  invalid_response: Mutex<Option<String>>,
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
      Phase::Commit | Phase::RetentionDelete => EventStoreError::Storage {
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

  fn retention_visible_history(
    &self,
    _: &AidString,
    history: &[u64],
    just_written: Option<u64>,
  ) -> Result<Vec<u64>, EventStoreError> {
    let fault = self
      .faults
      .lock()
      .expect("障害消費のロック")
      .start_application(Phase::RetentionQuery)
      .cloned();
    let Some(fault) = fault else {
      return Ok(history.to_vec());
    };
    let result = if fault.kind == FaultKind::SdkResponse {
      visible_history(&fault.details, history, just_written)
    } else {
      return Err(EventStoreError::Storage {
        operation: StorageOperation::Append,
        source: Box::new(std::io::Error::other(
          fault.details["message"].as_str().unwrap_or("injected fault").to_owned(),
        )),
      });
    };
    result.map_err(|message| {
      *self.invalid_response.lock().expect("応答検査のロック") = Some(message.clone());
      EventStoreError::Storage {
        operation: StorageOperation::Append,
        source: Box::new(std::io::Error::other(message)),
      }
    })
  }

  fn retention_delete(&self, _: &AidString, _: &[u64]) -> Result<(), EventStoreError> {
    self.inject(Phase::RetentionDelete)
  }
}

fn visible_history(details: &Value, history: &[u64], just_written: Option<u64>) -> Result<Vec<u64>, String> {
  let mut visible = Vec::new();
  for page in required(details, "history_pages")?
    .as_array()
    .ok_or("history_pages が配列ではない")?
  {
    for value in page.as_array().ok_or("履歴ページが配列ではない")? {
      let seq_nr = seq(value)?;
      if !history.contains(&seq_nr) {
        return Err(format!("応答計画の履歴 {seq_nr} が対象集約の保存済み履歴にない"));
      }
      visible.push(seq_nr);
    }
  }
  if details.get("omit_just_written_history").and_then(Value::as_bool) == Some(true)
    && just_written.is_some_and(|seq_nr| visible.contains(&seq_nr))
  {
    return Err("省略指定の応答計画に今書いた履歴が含まれる".to_owned());
  }
  Ok(visible)
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

fn observe(
  storage: &MemoryStorage,
  step: &Value,
  id: Option<&CaseId>,
  notifications: &[String],
) -> Result<Value, ComparisonFailure> {
  let mut observed = serde_json::Map::new();
  let Some(observe) = step.get("observe") else {
    return Ok(Value::Object(observed));
  };
  for (key, value) in observe.as_object().ok_or("observe がオブジェクトではない")? {
    if key == "notifications" {
      let actual = json!(notifications);
      observed.insert("notifications".into(), actual.clone());
      if !json_equal(value, &actual) {
        return Err(ComparisonFailure {
          detail: "通知が一致しない".to_owned(),
          expected: Some(value.clone()),
          actual: Some(actual),
        });
      }
      continue;
    }
    if key != "history" {
      return Err(format!("未対応の観測: {key}").into());
    }
    let aid = AidString::from_aggregate_id(id.ok_or("観測する集約 ID がない")?).map_err(|e| e.to_string())?;
    let history = storage.history_view(&aid).map_err(|e| e.to_string())?;
    observed.insert("history".into(), json!({"active":history,"marked":[]}));
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
  Ok(Value::Object(observed))
}

/// ケースごとに独立した保存先を作り、公開操作を順に実行する。
pub fn run_case(case: &Case, prepared: PreparedCase) -> CaseOutcome {
  run_case_observed(case, prepared, &mut Vec::new())
}

/// 同じ実行経路の公開結果・障害計数・保持履歴と通知を操作単位で保存する。
pub fn run_case_observed(case: &Case, prepared: PreparedCase, observations: &mut Vec<Value>) -> CaseOutcome {
  let body = &prepared.body;
  for fault in prepared.faults.faults() {
    let supported = match fault.phase {
      Phase::SerializeEvent | Phase::SerializeSnapshot | Phase::DeserializeEvent | Phase::DeserializeSnapshot => {
        fault.kind == FaultKind::SerializationError && fault.injection == Injection::ReplaceRequest
      }
      Phase::Commit | Phase::ReadEvents | Phase::ReadSnapshot | Phase::RetentionDelete => {
        fault.kind == FaultKind::StorageError && fault.injection == Injection::ReplaceRequest
      }
      Phase::RetentionQuery => {
        (fault.kind == FaultKind::StorageError && fault.injection == Injection::ReplaceRequest)
          || (fault.kind == FaultKind::SdkResponse
            && fault.injection == Injection::ReplaceResponse
            && fault.details.get("history_pages").is_some())
      }
      _ => false,
    };
    if fault.operation == 0 || !supported {
      return unverified("宣言された障害の段階・種類・方式へ未接続");
    }
  }
  let callbacks = Arc::new(Callbacks {
    faults: Mutex::new(prepared.faults.begin_operation(0)),
    invalid_response: Mutex::new(None),
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
  let applications = faults.applications();
  observations.push(json!({"operation":0,"actual":match &storage {Ok(_)=>json!({"result":"success"}),Err(error)=>crate::case::error_value(error)},"expect":body.pointer("/initialization/expect"),"fault_applications":applications}));
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
          Ok(_) => CaseOutcome::Passed { values: None },
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
    let mut actual = Vec::new();
    let result = runtime.block_on(run_value(case, &store, &mut actual));
    observations.push(
      json!({"operation":1,"actual":actual,"expect":body["expect"],"failure":result.as_ref().err().map(|v| &v.detail)}),
    );
    return match result {
      Ok(values) => CaseOutcome::Passed { values },
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
    let notifications = match OperationNotifications::begin(&case.id, operation) {
      Ok(notifications) => notifications,
      Err(error) => return unverified(&error),
    };
    let result = runtime.block_on(step(&store, body, input).instrument(notifications.span()));
    let notifications = notifications.finish();
    let faults = std::mem::replace(
      &mut *callbacks.faults.lock().expect("障害消費のロック"),
      prepared.faults.begin_operation(0),
    );
    let applications = faults.applications();
    let fault_result = faults.finish();
    let actual = match &result {
      Ok(value) => Some(value.clone()),
      Err(failure) => failure.actual.clone(),
    };
    let observed = crate::case::operation_context(body, input)
      .map_err(ComparisonFailure::from)
      .and_then(|(id, _)| observe(&storage, input, id.as_ref(), &notifications));
    observations.push(json!({"operation":operation,"actual":actual,"expect":input["expect"],"observe":input.get("observe"),
      "observed":match &observed {Ok(v)=>Some(v.clone()),Err(e)=>e.actual.clone()},"observation_error":observed.as_ref().err().map(|v| &v.detail),
      "notifications":notifications,"fault_applications":applications,"unfired_faults":fault_result.as_ref().err()}));
    if let Err(unfired_faults) = fault_result {
      return CaseOutcome::Failed {
        failed_operation: Some(operation),
        detail: "障害の適用回数が宣言を満たさない".to_owned(),
        expected: input.get("expect").cloned(),
        actual: match &result {
          Ok(value) => Some(value.clone()),
          Err(failure) => failure.actual.clone(),
        },
        unfired_faults,
      };
    }
    if let Some(error) = callbacks.invalid_response.lock().expect("応答検査のロック").take() {
      return failed(Some(operation), error);
    }
    match result {
      Ok(_) => {
        if let Err(e) = observed {
          return failed(Some(operation), e);
        }
      }
      Err(e) => return failed(Some(operation), e),
    }
  }
  CaseOutcome::Passed { values: None }
}
