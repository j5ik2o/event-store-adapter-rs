//! Bigtable の公開生成と公開操作を実行し、障害の消費・保持履歴・通知を検証する。
//!
//! 場面ごとに新しい journal・head のテーブルの組を作る。ストアの生成は、公開の初期化関数、`seed.configuration` の記録
//! （`test-hooks` の経路）、ストアを開く操作の順に行う。生成（初期化と開く）の結果は、場面の `initialization` の期待と比べる。

use std::sync::Arc;

use event_store_adapter_rs::{
  aggregate_id::AidString,
  bigtable::{overwrite_bigtable_configuration_for_test, BigtableConfigurationRecord, BigtableTestHooks},
  error::EventStoreError,
  initialize_bigtable_store,
  serializer::{EventSerializer, JsonEventSerializer, JsonSnapshotSerializer, SnapshotSerializer},
  BigtableOptions,
};
use serde_json::{json, Value};
use tracing::Instrument;

use super::{
  emulator, envelope,
  scene::{Scene, Store},
};
use crate::{
  case::{
    compare_result, failed, required, run_value, seq, settings_with_default_grace, step, string, ComparisonFailure,
  },
  data::{Case, CaseKind},
  fault::Phase,
  hook_faults::{self, HookFaults},
  notifications::OperationNotifications,
  report::CaseOutcome,
  runner::PreparedCase,
  store_registry::{CreateRequest, StorageRequest, StoreRegistry},
};

#[derive(Debug)]
struct Callbacks {
  faults: HookFaults,
}

impl BigtableTestHooks for Callbacks {
  fn before_commit(&self, _: &AidString, _: u64) -> Result<(), EventStoreError> {
    self.faults.inject(Phase::Commit)
  }

  fn after_commit(&self, _: &AidString, _: u64) -> Result<(), EventStoreError> {
    self.faults.inject_response(Phase::Commit)
  }

  fn journal_write_previous(&self, _: &AidString, _: u64) -> Result<(), EventStoreError> {
    self.faults.inject(Phase::JournalWritePrevious)
  }

  fn journal_write_tail(&self, _: &AidString, _: u64) -> Result<(), EventStoreError> {
    self.faults.inject(Phase::JournalWriteTail)
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
/// 値の表は `store` を持たないので、既定の設定を使う。場面は保持・変更フィード・封筒の上限
/// （`envelope_limit_bytes`、省略時は既定）を読む。TTL の場面は猶予を持たない（TTL は提供しない保存先なので、
/// 設定エラーになることを確かめる）。
fn options(case: &Case) -> Result<BigtableOptions, String> {
  if matches!(case.kind, CaseKind::ValueTable) {
    return Ok(BigtableOptions::default());
  }
  let mut options = BigtableOptions {
    retention: settings_with_default_grace(&case.body, Some(0))?,
    ..BigtableOptions::default()
  };
  if let Some(change_feed) = case.body.pointer("/store/change_feed") {
    options.change_feed = change_feed.as_bool().ok_or("store.change_feed が真偽値ではない")?;
  }
  if let Some(limit) = case.body.pointer("/store/envelope_limit_bytes") {
    options.envelope_limit_bytes = seq(limit)?;
  }
  Ok(options)
}

/// 設定行 1 つの `seed.configuration.journal`・`head` を、記録する行にする。
fn seed_record(configuration: &Value) -> Result<BigtableConfigurationRecord, String> {
  let keep_snapshot_count = match required(configuration, "keep_snapshot_count")? {
    Value::Null => None,
    value => Some(seq(value)?),
  };
  Ok(BigtableConfigurationRecord {
    store_id: string(required(configuration, "store_id")?)?.to_owned(),
    layout_version: i64::try_from(seq(required(configuration, "layout_version")?)?)
      .map_err(|error| format!("layout_version: {error}"))?,
    keep_snapshot_count,
    retention_mode: string(required(configuration, "retention_mode")?)?.to_owned(),
    envelope_limit_bytes: seq(required(configuration, "envelope_limit_bytes")?)?,
  })
}

/// `seed.configuration` を、journal と head の設定行にする。なければ `None` を返す。
fn seed_records(body: &Value) -> Result<Option<(BigtableConfigurationRecord, BigtableConfigurationRecord)>, String> {
  let Some(configuration) = body.pointer("/seed/configuration") else {
    return Ok(None);
  };
  Ok(Some((
    seed_record(required(configuration, "journal")?)?,
    seed_record(required(configuration, "head")?)?,
  )))
}

/// 公開の初期化、`seed.configuration` の記録、ストアを開く操作を順に行う。誤りは、その生成の結果として返す。
async fn create_primary(
  scene: &Scene,
  options: &BigtableOptions,
  seed: Option<&(BigtableConfigurationRecord, BigtableConfigurationRecord)>,
  callbacks: &Arc<Callbacks>,
) -> Result<Store, EventStoreError> {
  initialize_bigtable_store(&scene.connection, &scene.tables, options).await?;
  if let Some((journal, head)) = seed {
    overwrite_bigtable_configuration_for_test(&scene.connection, &scene.tables, options, journal, head).await?;
  }
  scene.open(options, callbacks).await
}

/// ケースごとに新しいテーブルの組のストアを作り、公開操作を順に実行する。
pub fn run_case(case: &Case, prepared: PreparedCase) -> CaseOutcome {
  let body = &prepared.body;
  if matches!(case.kind, CaseKind::ValueTable) && envelope::is_envelope_operation(body) {
    return envelope::run(body);
  }
  if !hook_faults::supports_all_bigtable(&prepared.faults) {
    return crate::case::unverified("宣言された障害の段階・種類・方式へ未接続");
  }
  let options = match options(case) {
    Ok(options) => options,
    Err(detail) => return failed(Some(0), detail),
  };
  let seed = match seed_records(body) {
    Ok(seed) => seed,
    Err(detail) => return failed(Some(0), detail),
  };
  let port = match emulator::port() {
    Ok(port) => port,
    Err(detail) => return crate::case::unverified(&format!("Bigtable エミュレーターを起動できない: {detail}")),
  };
  // 接続は、この実行基盤の中で作る。ストアは、この実行基盤より先に落とす。
  let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
    Ok(runtime) => runtime,
    Err(error) => return failed(None, error.to_string()),
  };
  let scene = match runtime.block_on(Scene::provision(port)) {
    Ok(scene) => scene,
    Err(detail) => return failed(None, detail),
  };
  let callbacks = Arc::new(Callbacks {
    faults: HookFaults::new(prepared.faults.begin_operation(0)),
  });
  let created = runtime.block_on(create_primary(&scene, &options, seed.as_ref(), &callbacks));
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
  let mut registry = StoreRegistry::new(primary);
  if matches!(case.kind, CaseKind::ValueTable) {
    return match runtime.block_on(run_value(case, registry.primary(), &mut Vec::new())) {
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
    let result = execute_step(
      &mut registry,
      &Session {
        scene: &scene,
        options: &options,
        callbacks: &callbacks,
        runtime: &runtime,
      },
      body,
      input,
      notifications.span(),
    );
    let notifications = notifications.finish();
    let faults = callbacks.faults.replace(prepared.faults.begin_operation(0));
    let fault_result = faults.finish();
    let observed = crate::case::operation_context(body, input)
      .map_err(ComparisonFailure::from)
      .and_then(|(id, _)| {
        let named = registry.select(input)?;
        hook_faults::observe(input, id.as_ref(), &notifications, |aid| {
          runtime.block_on(named.history_view(aid))
        })
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

/// 操作の実行が使う、場面のテーブルの組・設定・フック・実行基盤。
struct Session<'a> {
  scene: &'a Scene,
  options: &'a BigtableOptions,
  callbacks: &'a Arc<Callbacks>,
  runtime: &'a tokio::runtime::Runtime,
}

/// 1 つの操作を `store` が指すストアに対して実行し、期待値と比べる。
///
/// データの誤り（生成前のストア名、予約名の再利用、`storage = new` など）は、期待値を載せた比較の失敗にする。
fn execute_step(
  registry: &mut StoreRegistry<Store>,
  session: &Session<'_>,
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
      let CreateRequest { name, storage } = registry
        .parse_create(required(input, "arguments")?)
        .map_err(data_error)?;
      match storage {
        StorageRequest::New => Err(data_error(
          "storage = new は Bigtable の場面では使えない（共有する場合は shared を使う）".to_owned(),
        )),
        // 共有するのは同じテーブルの組で、設定は記録済みの設定行と照合される。
        StorageRequest::Shared(_) => {
          let opened = session
            .runtime
            .block_on(session.scene.open(session.options, session.callbacks));
          match opened {
            Ok(store) => {
              registry.insert(name, store);
              compare_result(expect, Ok(json!({"result": "success"})))
            }
            Err(error) => compare_result(expect, Err(error)),
          }
        }
      }
    }
    _ => {
      let named = registry.select(input).map_err(data_error)?;
      session.runtime.block_on(step(named, body, input).instrument(span))
    }
  }
}
