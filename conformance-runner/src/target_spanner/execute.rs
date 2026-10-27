//! Spanner の公開生成と公開操作を実行し、障害の消費・保持履歴・通知を検証する。
//!
//! 場面ごとに新しいデータベース（5 テーブル）を作る。ストアの生成は、`seed.configuration` があればその設定の行を
//! 生の DML で書いてからストアを開く操作、なければ公開の初期化関数とストアを開く操作の順に行う。
//! 生成（初期化と開く）の結果は、場面の `initialization` の期待と比べる。

use std::sync::Arc;

use event_store_adapter_rs::{
  aggregate_id::AidString,
  error::EventStoreError,
  seq_nr::SeqNr,
  serializer::{EventSerializer, JsonEventSerializer, JsonSnapshotSerializer, SnapshotSerializer},
  spanner::{fnv1a64_for_test, SpannerCommitFault, SpannerTestHooks},
  SpannerOptions, SpannerRetention,
};
use event_store_adapter_test_utils_rs::spanner::SeedConfiguration;
use serde_json::{json, Value};
use tracing::Instrument;

use super::{
  emulator,
  scene::{Scene, Store},
};
use crate::{
  case::{compare_result, failed, required, run_value, seq, step, string, ComparisonFailure},
  data::{Case, CaseKind},
  fault::Phase,
  hash_values,
  hook_faults::{self, CommitInjection, HookFaults},
  notifications::OperationNotifications,
  report::CaseOutcome,
  runner::PreparedCase,
  store_registry::{CreateRequest, StorageRequest, StoreRegistry},
};

#[derive(Debug)]
struct Callbacks {
  faults: HookFaults,
}

impl SpannerTestHooks for Callbacks {
  fn before_commit(&self, _: &AidString, _: SeqNr) -> Result<(), SpannerCommitFault> {
    match self.faults.inject_commit_request() {
      CommitInjection::Proceed => Ok(()),
      CommitInjection::Aborted => Err(SpannerCommitFault::Aborted),
      CommitInjection::Failed(error) => Err(SpannerCommitFault::Failed(error)),
    }
  }

  fn after_commit(&self, _: &AidString, _: SeqNr) -> Result<(), EventStoreError> {
    self.faults.inject_response(Phase::Commit)
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
    history: &[SeqNr],
    just_written: Option<SeqNr>,
  ) -> Result<Vec<SeqNr>, EventStoreError> {
    self.faults.retention_visible_history(history, just_written)
  }

  fn retention_delete(&self, _: &AidString, _: &[SeqNr]) -> Result<(), EventStoreError> {
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
/// 値の表は `store` を持たないので、既定の設定を使う。場面は保持（件数・方式・猶予の日数）・変更フィード・
/// 中断の再試行の上限（`transaction_retry_limit`、省略時は既定）を読む。
fn options(case: &Case) -> Result<SpannerOptions, String> {
  if matches!(case.kind, CaseKind::ValueTable) {
    return Ok(SpannerOptions::default());
  }
  let store = required(&case.body, "store")?;
  let count = match required(store, "retention_count")? {
    Value::Null => None,
    value => Some(usize::try_from(seq(value)?).map_err(|error| format!("retention_count: {error}"))?),
  };
  let retention = match (string(required(store, "retention_mode")?)?, count) {
    ("delete", None) => SpannerRetention::CurrentOnly,
    ("delete", Some(count)) => SpannerRetention::KeepLatest { count },
    ("ttl", Some(count)) => SpannerRetention::KeepLatestWithTtl {
      count,
      grace_days: seq(required(store, "ttl_grace_days").map_err(|_| "必須要素 ttl_grace_days がない")?)?,
    },
    ("ttl", None) => return Err("ttl の保持方式には retention_count が必要".to_owned()),
    _ => return Err("未知の保持方式".to_owned()),
  };
  let mut options = SpannerOptions {
    retention,
    ..SpannerOptions::default()
  };
  if let Some(change_feed) = store.get("change_feed") {
    options.change_feed = change_feed.as_bool().ok_or("store.change_feed が真偽値ではない")?;
  }
  if let Some(limit) = store.get("transaction_retry_limit") {
    options.transaction_retry_limit =
      u32::try_from(seq(limit)?).map_err(|error| format!("store.transaction_retry_limit: {error}"))?;
  }
  Ok(options)
}

/// 整数か `null` の項目を読む。
fn optional_integer(value: &Value, name: &str) -> Result<Option<i64>, String> {
  match value {
    Value::Null => Ok(None),
    value => i64::try_from(seq(value)?)
      .map(Some)
      .map_err(|error| format!("{name}: {error}")),
  }
}

/// `seed.configuration` を、書く設定の行にする。なければ `None` を返す。
fn seed_configuration(body: &Value) -> Result<Option<SeedConfiguration>, String> {
  let Some(configuration) = body.pointer("/seed/configuration") else {
    return Ok(None);
  };
  Ok(Some(SeedConfiguration {
    store_id: string(required(configuration, "store_id")?)?.to_owned(),
    layout_version: optional_integer(required(configuration, "layout_version")?, "layout_version")?
      .ok_or("layout_version が null")?,
    keep_snapshot_count: optional_integer(required(configuration, "keep_snapshot_count")?, "keep_snapshot_count")?,
    retention_mode: string(required(configuration, "retention_mode")?)?.to_owned(),
    ttl_grace_days: optional_integer(required(configuration, "ttl_grace_days")?, "ttl_grace_days")?,
    change_feed_enabled: required(configuration, "change_feed_enabled")?
      .as_bool()
      .ok_or("change_feed_enabled が真偽値ではない")?,
  }))
}

/// データベースの snapshot_history に付ける行削除ポリシーの日数を決める。
///
/// 場面が `seed` を宣言するときは `seed.row_deletion_policy_days`（`null` と省略は、ポリシーなし）に従う。宣言しないときは、
/// 期限切れ方式の猶予の日数をそのままポリシーにする（0 以下は、ポリシーを作れないので付けず、生成が設定エラーになる）。
fn row_deletion_policy_days(body: &Value, options: &SpannerOptions) -> Result<Option<i64>, String> {
  if body.get("seed").is_some() {
    return match body.pointer("/seed/row_deletion_policy_days") {
      Some(days) => optional_integer(days, "seed.row_deletion_policy_days"),
      None => Ok(None),
    };
  }
  Ok(match options.retention {
    SpannerRetention::KeepLatestWithTtl { grace_days, .. } if grace_days >= 1 => i64::try_from(grace_days).ok(),
    _ => None,
  })
}

/// 設定の行（`seed.configuration` または公開の初期化関数）を用意してから、ストアを開く。誤りは、その生成の結果として返す。
async fn create_primary(
  scene: &Scene,
  options: &SpannerOptions,
  seeded: bool,
  callbacks: &Arc<Callbacks>,
) -> Result<Store, EventStoreError> {
  if !seeded {
    scene.initialize(options).await?;
  }
  scene.open(options, callbacks).await
}

/// ケースごとに新しいデータベースのストアを作り、公開操作を順に実行する。
pub fn run_case(case: &Case, prepared: PreparedCase) -> CaseOutcome {
  let body = &prepared.body;
  if matches!(case.kind, CaseKind::ValueTable) && hash_values::is_hash_operation(body) {
    return hash_values::run_with(body, fnv1a64_for_test);
  }
  if !hook_faults::supports_all_spanner(&prepared.faults) {
    return crate::case::unverified("宣言された障害の段階・種類・方式へ未接続");
  }
  let options = match options(case) {
    Ok(options) => options,
    Err(detail) => return failed(Some(0), detail),
  };
  let seed = match seed_configuration(body) {
    Ok(seed) => seed,
    Err(detail) => return failed(Some(0), detail),
  };
  let policy_days = match row_deletion_policy_days(body, &options) {
    Ok(days) => days,
    Err(detail) => return failed(Some(0), detail),
  };
  let port = match emulator::port() {
    Ok(port) => port,
    Err(detail) => return crate::case::unverified(&format!("Spanner エミュレーターを起動できない: {detail}")),
  };
  // 接続は、この実行基盤の中で作る。ストアは、この実行基盤より先に落とす。
  let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
    Ok(runtime) => runtime,
    Err(error) => return failed(None, error.to_string()),
  };
  let scene = match runtime.block_on(Scene::provision(port, policy_days)) {
    Ok(scene) => scene,
    Err(detail) => return failed(None, detail),
  };
  let outcome = run_in_scene(
    case,
    &prepared,
    &Context {
      runtime: &runtime,
      scene: &scene,
      options: &options,
      seed: seed.as_ref(),
    },
  );
  // ストアは `run_in_scene` の中で落とし済み。データベースは、実行基盤が生きている間に削除する。
  runtime.block_on(scene.discard());
  outcome
}

/// 場面の実行が使う、実行基盤・データベース・設定・`seed.configuration`。
struct Context<'a> {
  runtime: &'a tokio::runtime::Runtime,
  scene: &'a Scene,
  options: &'a SpannerOptions,
  seed: Option<&'a SeedConfiguration>,
}

/// 場面のデータベースにストアを作り、公開操作を順に実行する。
fn run_in_scene(case: &Case, prepared: &PreparedCase, context: &Context<'_>) -> CaseOutcome {
  let body = &prepared.body;
  let Context {
    runtime,
    scene,
    options,
    seed,
  } = *context;
  if let Some(configuration) = seed {
    if let Err(detail) = runtime.block_on(scene.seed_configuration(configuration)) {
      return failed(None, detail);
    }
  }
  let callbacks = Arc::new(Callbacks {
    faults: HookFaults::new(prepared.faults.begin_operation(0)),
  });
  let created = runtime.block_on(create_primary(scene, options, seed.is_some(), &callbacks));
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
        scene,
        options,
        callbacks: &callbacks,
        runtime,
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

/// 操作の実行が使う、場面のデータベース・設定・フック・実行基盤。
struct Session<'a> {
  scene: &'a Scene,
  options: &'a SpannerOptions,
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
          "storage = new は Spanner の場面では使えない（共有する場合は shared を使う）".to_owned(),
        )),
        // 共有するのは同じデータベースで、設定は記録済みの設定行と照合される。
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
