//! SQLite の公開生成と公開操作を実行し、障害の消費・保持履歴・通知を検証する。
//!
//! ストアの生成は、公開の初期化関数、`seed.configuration` の記録（`test-hooks` の経路）、ストアを開く操作の順に行う。
//! 生成（初期化と開く）の結果は、場面の `initialization` の期待と比べる。

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use event_store_adapter_rs::{
  aggregate_id::AidString,
  error::EventStoreError,
  initialize_sqlite_store,
  retention::RetentionSettings,
  serializer::{EventSerializer, JsonEventSerializer, JsonSnapshotSerializer, SnapshotSerializer},
  sqlite::{overwrite_sqlite_configuration_for_test, SqliteConfigurationRecord, SqliteTestHooks},
  EventStoreForSqlite, SqliteOptions,
};
use serde_json::{json, Value};
use tracing::Instrument;

use super::stores::{ScenarioFile, Store, Stores};
use crate::{
  case::{compare_result, failed, required, run_value, seq, settings, step, string, ComparisonFailure},
  data::{Case, CaseKind},
  fault::Phase,
  hook_faults::{self, HookFaults},
  notifications::OperationNotifications,
  report::CaseOutcome,
  runner::PreparedCase,
};

#[derive(Debug)]
struct Callbacks {
  faults: HookFaults,
}

impl SqliteTestHooks for Callbacks {
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

impl EventSerializer<Value> for Callbacks {
  fn serialize(&self, payload: &Value) -> Result<Vec<u8>, EventStoreError> {
    self.faults.inject(Phase::SerializeEvent)?;
    JsonEventSerializer::<Value>::new().serialize(payload)
  }

  fn deserialize(&self, data: &[u8]) -> Result<Value, EventStoreError> {
    self.faults.inject(Phase::DeserializeEvent)?;
    JsonEventSerializer::<Value>::new().deserialize(data)
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

/// 場面の `store` の設定から、ストアの設定を作る。
///
/// 値の表は `store` を持たないので、既定の設定を使う。場面は保持・変更フィード・ロックの待ち時間
/// （`lock_wait_milliseconds`、省略時は既定）を読む。
fn options(case: &Case) -> Result<SqliteOptions, String> {
  if matches!(case.kind, CaseKind::ValueTable) {
    return Ok(SqliteOptions {
      retention: RetentionSettings::current_only(),
      ..SqliteOptions::default()
    });
  }
  let mut options = SqliteOptions {
    retention: settings(&case.body)?,
    ..SqliteOptions::default()
  };
  if let Some(change_feed) = case.body.pointer("/store/change_feed") {
    options.change_feed = change_feed.as_bool().ok_or("store.change_feed が真偽値ではない")?;
  }
  if let Some(milliseconds) = case.body.pointer("/store/lock_wait_milliseconds") {
    options.lock_wait = Duration::from_millis(seq(milliseconds)?);
  }
  Ok(options)
}

/// `seed.configuration` を、記録する config の行にする。なければ `None` を返す。
fn seed_record(body: &Value) -> Result<Option<SqliteConfigurationRecord>, String> {
  let Some(configuration) = body.pointer("/seed/configuration") else {
    return Ok(None);
  };
  let integer = |key: &str| -> Result<i64, String> {
    i64::try_from(seq(required(configuration, key)?)?).map_err(|error| format!("{key}: {error}"))
  };
  let keep_snapshot_count = match required(configuration, "keep_snapshot_count")? {
    Value::Null => None,
    value => Some(i64::try_from(seq(value)?).map_err(|error| format!("keep_snapshot_count: {error}"))?),
  };
  Ok(Some(SqliteConfigurationRecord {
    store_id: string(required(configuration, "store_id")?)?.to_owned(),
    layout_version: integer("layout_version")?,
    keep_snapshot_count,
    retention_mode: string(required(configuration, "retention_mode")?)?.to_owned(),
    change_feed_enabled: integer("change_feed_enabled")?,
  }))
}

/// 書き込みのロックの操作が、データとして正しく使われているかを、実行の前に検査する。
///
/// 対象は、`holdWriteLock`・`releaseWriteLock` を使う場面。保存先が `sqlite` だけで、規則に SQ-7 を持ち、
/// `store.lock_wait_milliseconds` を指定する。操作は引数が空で、`store` を持たず、期待は成功だけ。
/// 持っていないロックを放す、すでに持っているロックをもう一度持つ、放さないまま終わる、
/// ロックを持つ間に保存先エラーを期待する追記がない、場面は拒否する。
fn check_write_lock_usage(case: &Case) -> Result<(), String> {
  let body = &case.body;
  let steps = body
    .get("steps")
    .and_then(Value::as_array)
    .map(Vec::as_slice)
    .unwrap_or(&[]);
  let operation = |input: &Value| input.get("op").and_then(Value::as_str).unwrap_or_default().to_owned();
  if !steps
    .iter()
    .any(|input| matches!(operation(input).as_str(), "holdWriteLock" | "releaseWriteLock"))
  {
    return Ok(());
  }
  let backends: Vec<&str> = body
    .get("backends")
    .and_then(Value::as_array)
    .map(|backends| backends.iter().filter_map(Value::as_str).collect())
    .unwrap_or_default();
  if backends != ["sqlite"] {
    return Err("書き込みのロックの操作は、backends が sqlite だけの場面でしか使えない".to_owned());
  }
  if !case.rules.iter().any(|rule| rule == "SQ-7") {
    return Err("書き込みのロックの操作を使う場面は、規則に SQ-7 を持たなければならない".to_owned());
  }
  if body.pointer("/store/lock_wait_milliseconds").is_none() {
    return Err("書き込みのロックの操作を使う場面は、store.lock_wait_milliseconds を指定しなければならない".to_owned());
  }
  let mut holding = false;
  let mut failure_expected_while_holding = false;
  for input in steps {
    let name = operation(input);
    if matches!(name.as_str(), "holdWriteLock" | "releaseWriteLock") {
      if input.get("arguments") != Some(&json!({})) || input.get("store").is_some() {
        return Err(format!("{name} の arguments は空で、store を持たない"));
      }
      if input.get("expect") != Some(&json!({"result": "success"})) {
        return Err(format!("{name} の期待は result = success だけ"));
      }
    }
    match name.as_str() {
      "holdWriteLock" => {
        if holding {
          return Err("すでに持っている書き込みのロックを、もう一度持とうとしている".to_owned());
        }
        holding = true;
        failure_expected_while_holding = false;
      }
      "releaseWriteLock" => {
        if !holding {
          return Err("持っていない書き込みのロックを放そうとしている".to_owned());
        }
        if !failure_expected_while_holding {
          return Err("ロックを持つ間に、保存先エラーを期待する追記がない".to_owned());
        }
        holding = false;
      }
      "persistEvent" | "persistEventAndSnapshot"
        if holding && input.pointer("/expect/error/category").and_then(Value::as_str) == Some("storage") =>
      {
        failure_expected_while_holding = true;
      }
      _ => {}
    }
  }
  if holding {
    return Err("書き込みのロックを放さないまま場面が終わる".to_owned());
  }
  Ok(())
}

/// 初期化済みのファイルを、障害のフック付きのストアとして開く。
fn open_store(path: &Path, options: &SqliteOptions, callbacks: &Arc<Callbacks>) -> Result<Store, EventStoreError> {
  EventStoreForSqlite::open_with_hooks(
    path,
    options.clone(),
    callbacks.clone(),
    callbacks.clone(),
    callbacks.clone(),
  )
}

/// 公開の初期化、`seed.configuration` の記録、ストアを開く操作を順に行う。誤りは、その生成の結果として返す。
fn create_primary(
  path: &Path,
  options: &SqliteOptions,
  seed: Option<&SqliteConfigurationRecord>,
  callbacks: &Arc<Callbacks>,
) -> Result<Store, EventStoreError> {
  initialize_sqlite_store(path, options)?;
  if let Some(record) = seed {
    overwrite_sqlite_configuration_for_test(path, options, record)?;
  }
  open_store(path, options, callbacks)
}

/// ケースごとに新しい一時ファイルのストアを作り、公開操作を順に実行する。
pub fn run_case(case: &Case, prepared: PreparedCase) -> CaseOutcome {
  let body = &prepared.body;
  if !hook_faults::supports_all(&prepared.faults) {
    return crate::case::unverified("宣言された障害の段階・種類・方式へ未接続");
  }
  if let Err(detail) = check_write_lock_usage(case) {
    return failed(None, detail);
  }
  let options = match options(case) {
    Ok(options) => options,
    Err(detail) => return failed(Some(0), detail),
  };
  let seed = match seed_record(body) {
    Ok(seed) => seed,
    Err(detail) => return failed(Some(0), detail),
  };
  let file = match ScenarioFile::new() {
    Ok(file) => file,
    Err(detail) => return failed(None, detail),
  };
  let callbacks = Arc::new(Callbacks {
    faults: HookFaults::new(prepared.faults.begin_operation(0)),
  });
  let created = create_primary(file.path(), &options, seed.as_ref(), &callbacks);
  callbacks.faults.replace(prepared.faults.begin_operation(0));
  let initialization = body.pointer("/initialization/expect");
  let primary = match created {
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
    Ok(primary) => primary,
  };
  if let Some(expected) = initialization {
    if let Err(failure) = compare_result(expected, Ok(json!({"result": "success"}))) {
      return failed(Some(0), failure);
    }
  }
  let reopen = || open_store(file.path(), &options, &callbacks);
  let mut stores = Stores::new(file.path(), &reopen, primary);
  let runtime = match tokio::runtime::Builder::new_current_thread().build() {
    Ok(runtime) => runtime,
    Err(error) => return failed(None, error.to_string()),
  };
  if matches!(case.kind, CaseKind::ValueTable) {
    return match runtime.block_on(run_value(case, stores.registry.primary(), &mut Vec::new())) {
      Ok(values) => CaseOutcome::Passed { values },
      Err(failure) => failed(None, failure),
    };
  }
  let Some(steps) = body.get("steps").and_then(Value::as_array) else {
    return failed(None, "steps がない".to_owned());
  };
  for (index, input) in steps.iter().enumerate() {
    let operation = match u32::try_from(index + 1) {
      Ok(operation) => operation,
      Err(error) => return failed(None, error.to_string()),
    };
    callbacks.faults.replace(prepared.faults.begin_operation(operation));
    let notifications = match OperationNotifications::begin(&case.id, operation) {
      Ok(notifications) => notifications,
      Err(error) => return crate::case::unverified(&error),
    };
    let result = execute_step(&mut stores, &runtime, body, input, notifications.span());
    let notifications = notifications.finish();
    let faults = callbacks.faults.replace(prepared.faults.begin_operation(0));
    let fault_result = faults.finish();
    let observed = crate::case::operation_context(body, input)
      .map_err(ComparisonFailure::from)
      .and_then(|(id, _)| {
        let named = stores.registry.select(input)?;
        hook_faults::observe(input, id.as_ref(), &notifications, |aid| named.history_view(aid))
      });
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
        if let Err(failure) = observed {
          return failed(Some(operation), failure);
        }
      }
      Err(failure) => return failed(Some(operation), failure),
    }
  }
  CaseOutcome::Passed { values: None }
}

/// 1 つの操作を、`store` が指すストアか、ファイル全体の書き込みのロックに対して実行し、期待値と比べる。
///
/// データの誤り（生成前のストア名、予約名の再利用、`storage = new` など）は、期待値を載せた比較の失敗にする。
fn execute_step(
  stores: &mut Stores<'_>,
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
      compare_result(expect, created.map(|()| json!({"result": "success"})))
    }
    "holdWriteLock" => {
      stores.hold_write_lock().map_err(data_error)?;
      compare_result(expect, Ok(json!({"result": "success"})))
    }
    "releaseWriteLock" => {
      stores.release_write_lock().map_err(data_error)?;
      compare_result(expect, Ok(json!({"result": "success"})))
    }
    _ => {
      let named = stores.registry.select(input).map_err(data_error)?;
      runtime.block_on(step(named, body, input).instrument(span))
    }
  }
}
