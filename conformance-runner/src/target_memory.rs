//! 新契約メモリの公開操作を実行し、保存後の封筒と障害消費を検証する。

mod stores;

use std::sync::Arc;

use event_store_adapter_rs::{
  aggregate_id::AidString,
  error::EventStoreError,
  memory::MemoryTestHooks,
  retention::RetentionSettings,
  serializer::{EventSerializer, JsonEventSerializer, JsonSnapshotSerializer, SnapshotSerializer},
};
use serde_json::{json, Value};
use stores::{LibraryStores, PayloadHandle, StoreFactory, Stores};
use tracing::Instrument;

use crate::{
  case::{compare_result, failed, required, run_value, settings, step, string, unverified, ComparisonFailure},
  data::{Case, CaseKind},
  fault::Phase,
  hook_faults::{self, HookFaults},
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
  extra_store_settings: &[],
  extra_seed_items: &[],
  represents_change_feed: false,
};

#[derive(Debug)]
struct Callbacks {
  faults: HookFaults,
}

impl MemoryTestHooks for Callbacks {
  fn before_commit(&self, _: &AidString, _: u64) -> Result<(), EventStoreError> {
    self.faults.inject(Phase::Commit)
  }

  fn read_events(&self, _: &AidString) -> Result<(), EventStoreError> {
    self.faults.inject(Phase::ReadEvents)
  }

  fn read_snapshot(&self, _: &AidString) -> Result<(), EventStoreError> {
    self.faults.inject(Phase::ReadSnapshot)
  }

  fn retention_visible_history(
    &self,
    _: &AidString,
    history: &[u64],
    just_written: Option<u64>,
  ) -> Result<Vec<u64>, EventStoreError> {
    self.faults.retention_visible_history(history, just_written)
  }

  fn retention_delete(&self, _: &AidString, _: &[u64]) -> Result<(), EventStoreError> {
    self.faults.inject(Phase::RetentionDelete)
  }
}

impl EventSerializer<PayloadHandle> for Callbacks {
  fn serialize(&self, payload: &PayloadHandle) -> Result<Vec<u8>, EventStoreError> {
    self.faults.inject(Phase::SerializeEvent)?;
    JsonEventSerializer::<Value>::new().serialize(&payload.value())
  }

  fn deserialize(&self, data: &[u8]) -> Result<PayloadHandle, EventStoreError> {
    self.faults.inject(Phase::DeserializeEvent)?;
    JsonEventSerializer::<Value>::new()
      .deserialize(data)
      .map(PayloadHandle::new)
  }
}

impl SnapshotSerializer<Value> for Callbacks {
  fn serialize(&self, aggregate: &Value) -> Result<Vec<u8>, EventStoreError> {
    self.faults.inject(Phase::SerializeSnapshot)?;
    JsonSnapshotSerializer::new().serialize(aggregate)
  }

  fn deserialize(&self, data: &[u8]) -> Result<Value, EventStoreError> {
    self.faults.inject(Phase::DeserializeSnapshot)?;
    JsonSnapshotSerializer::new().deserialize(data)
  }
}

/// ケースごとに独立した保存先を作り、公開操作を順に実行する。
pub fn run_case(case: &Case, prepared: PreparedCase) -> CaseOutcome {
  run_case_observed(case, prepared, &mut Vec::new())
}

/// 同じ実行経路の公開結果・障害計数・保持履歴と通知を操作単位で保存する。
pub fn run_case_observed(case: &Case, prepared: PreparedCase, observations: &mut Vec<Value>) -> CaseOutcome {
  run_with(case, prepared, observations, &LibraryStores)
}

/// 保存先とストアの作り方を `factory` で差し替えて、場面を実行する。
fn run_with<F: StoreFactory>(
  case: &Case,
  prepared: PreparedCase,
  observations: &mut Vec<Value>,
  factory: &F,
) -> CaseOutcome {
  let body = &prepared.body;
  if !hook_faults::supports_all(&prepared.faults) {
    return unverified("宣言された障害の段階・種類・方式へ未接続");
  }
  let callbacks = Arc::new(Callbacks {
    faults: HookFaults::new(prepared.faults.begin_operation(0)),
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
  let storage = factory.create_storage(settings.clone(), callbacks.clone());
  let faults = callbacks.faults.replace(prepared.faults.begin_operation(0));
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
  let mut stores = Stores::new(
    factory,
    settings,
    callbacks.clone(),
    callbacks.clone(),
    callbacks.clone(),
    storage,
  );
  let runtime = match tokio::runtime::Builder::new_current_thread().build() {
    Ok(runtime) => runtime,
    Err(e) => return failed(None, e.to_string()),
  };
  if matches!(case.kind, CaseKind::ValueTable) {
    let mut actual = Vec::new();
    let result = runtime.block_on(run_value(case, &stores.registry.primary().store, &mut actual));
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
    callbacks.faults.replace(prepared.faults.begin_operation(operation));
    stores.begin(operation);
    let notifications = match OperationNotifications::begin(&case.id, operation) {
      Ok(notifications) => notifications,
      Err(error) => return unverified(&error),
    };
    let result = execute_step(&mut stores, &runtime, body, input, notifications.span());
    let notifications = notifications.finish();
    let faults = callbacks.faults.replace(prepared.faults.begin_operation(0));
    let applications = faults.applications();
    let fault_result = faults.finish();
    let actual = match &result {
      Ok(value) => Some(value.clone()),
      Err(failure) => failure.actual.clone(),
    };
    let observed = crate::case::operation_context(body, input)
      .map_err(ComparisonFailure::from)
      .and_then(|(id, _)| {
        let named = stores.registry.select(input)?;
        hook_faults::observe(input, id.as_ref(), &notifications, |aid| {
          named.storage.history_view(aid)
        })
      });
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
    if let Some(error) = callbacks.faults.take_invalid_response() {
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

/// 1 つの操作を、`store` が指すストアか、ストアの表そのものに対して実行し、期待値と比べる。
///
/// データの誤り（生成前のストア名、予約名の再利用、ペイロードにない位置など）は、期待値を載せた比較の失敗にする。
fn execute_step<F: StoreFactory>(
  stores: &mut Stores<'_, F>,
  runtime: &tokio::runtime::Runtime,
  body: &Value,
  input: &Value,
  span: tracing::Span,
) -> Result<Value, ComparisonFailure> {
  let expect = required(input, "expect")?;
  let data_error = |detail: String| ComparisonFailure {
    detail,
    expected: Some(expect.clone()),
    actual: None,
  };
  match string(required(input, "op")?)? {
    "createStore" => {
      let created = stores.create(required(input, "arguments")?).map_err(data_error)?;
      compare_result(expect, created.map(|()| json!({"result":"success"})))
    }
    "mutateWrittenPayload" => {
      stores
        .mutate_written(required(input, "arguments")?)
        .map_err(data_error)?;
      compare_result(expect, Ok(json!({"result":"success"})))
    }
    "mutateReadPayload" => {
      stores
        .mutate_read(body, required(input, "arguments")?)
        .map_err(data_error)?;
      compare_result(expect, Ok(json!({"result":"success"})))
    }
    _ => {
      let named = stores.registry.select(input).map_err(data_error)?;
      runtime.block_on(step(&named.store, body, input).instrument(span))
    }
  }
}

#[cfg(test)]
#[path = "target_memory_test.rs"]
mod tests;
