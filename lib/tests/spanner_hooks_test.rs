#![cfg(all(feature = "spanner", feature = "test-hooks"))]
//! Spanner の保存先の結合試験のうち、障害の差し込みや、読み取りの途中に別の書き手を動かす試験（エミュレーター）。
//!
//! 前提にしているフック（`event_store_adapter_rs::spanner::SpannerTestHooks`）の呼ばれ方:
//!
//! - `before_begin_transaction(&TransactionOptions)`: トランザクションを始める直前（読み書き・読み取り専用のどちらも）。
//! - `before_attempt(aid, seq_nr, attempt)`: 追記の 1 回の試行を始める前。トランザクションを開く前なので、
//!   エミュレーターが重なる読み書きトランザクションを中断しないように、別の書き手はここで動かす。
//!   `attempt` は 0 から始まる（0 が初回、1 以降が再試行）。
//! - `before_commit(aid, seq_nr)`: Commit を送る直前。`Err(SpannerCommitFault::Aborted)` は Commit が ABORTED になった
//!   ことを、`Err(SpannerCommitFault::Failed(e))` は Commit が ABORTED 以外で失敗したことを表す（送らない）。
//! - `after_commit(aid, seq_nr)`: Commit の応答を受け取った後。`Err` は応答の消失を表す（確定した後の失敗）。
//! - `read_events(aid)` / `read_snapshot(aid)`: 読み取り専用トランザクションの中で head を読んだ直後。
//! - `retention_visible_history(aid, history, just_written)`: 保持の読み書きトランザクションが、印のない履歴を降順に
//!   読んだ後。`Err` は retention-query の失敗になる。
//! - `retention_delete(aid, seq_nrs)` / `retention_mark(aid, seq_nrs)`: 削除・印付けの DML の直前。`Err` はそれぞれ
//!   retention-delete・retention-mark の失敗になり、保持のトランザクションは確定しない。
//! - `before_retention_commit(aid, seq_nr)`: 保持のトランザクションの Commit を送る直前（DML の後）。
//!   `Err(SpannerCommitFault::Aborted)` は保持の Commit が ABORTED になったことを表し、保持は候補を選び直して再試行する。
//! - `aggregate_hash(aid)`: `Some` を返すと、FNV-1a 64 の代わりにその値を hash_prefix に使う。

mod spanner_support;
#[path = "bigtable_capture/mod.rs"]
mod tracing_capture;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use event_store_adapter_rs::spanner::{SpannerCommitFault, SpannerTestHooks};
use event_store_adapter_rs::{
  AidString, EventStore, EventStoreError, JsonEventSerializer, JsonSnapshotSerializer, SeqNr, SpannerOptions,
  StorageOperation,
};
use event_store_adapter_test_utils_rs::spanner::Param;
use googleapis_tonic_google_spanner_v1::google::spanner::v1::transaction_options::{
  read_only::TimestampBound, IsolationLevel, Mode,
};
use googleapis_tonic_google_spanner_v1::google::spanner::v1::TransactionOptions;
use serde_json::json;
use spanner_support::{
  event, event_with_payload, hash_prefix, options_keeping, run_on_another_runtime, snapshot, ttl_options, Env, Fixture,
  Id, Store, PREFIX,
};
use tracing_capture::captured_events;

type Action = Box<dyn FnOnce() + Send>;

fn failure(operation: StorageOperation) -> EventStoreError {
  EventStoreError::Storage {
    operation,
    source: Box::new(std::io::Error::other("INJECTED")),
  }
}

/// 残りの回数が 1 以上なら 1 つ減らして真を返す（その回数だけ障害を差し込む）。
fn take_one(remaining: &AtomicUsize) -> bool {
  let mut current = remaining.load(Ordering::SeqCst);
  loop {
    let Some(next) = current.checked_sub(1) else {
      return false;
    };
    match remaining.compare_exchange(current, next, Ordering::SeqCst, Ordering::SeqCst) {
      Ok(_) => return true,
      Err(actual) => current = actual,
    }
  }
}

/// 障害を差し込み、各フックの呼び出しを記録する。試行の前や読み取りの途中には、別の書き手を動かせる。
#[derive(Default)]
struct Hooks {
  aborts: AtomicUsize,
  commit_failures: AtomicUsize,
  lost_responses: AtomicUsize,
  query_failures: AtomicUsize,
  delete_failures: AtomicUsize,
  mark_failures: AtomicUsize,
  retention_aborts: AtomicUsize,
  retention_commit_calls: AtomicUsize,
  commit_calls: AtomicUsize,
  begun: Mutex<Vec<TransactionOptions>>,
  attempts: Mutex<Vec<(String, u64, u32)>>,
  delete_batches: Mutex<Vec<Vec<SeqNr>>>,
  mark_batches: Mutex<Vec<Vec<SeqNr>>>,
  fixed_hash: Mutex<Option<u64>>,
  on_retry: Mutex<Option<Action>>,
  on_read_events: Mutex<Option<Action>>,
  on_read_snapshot: Mutex<Option<Action>>,
}

impl std::fmt::Debug for Hooks {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    formatter.debug_struct("Hooks").finish()
  }
}

impl Hooks {
  fn arm(counter: &AtomicUsize, times: usize) {
    counter.store(times, Ordering::SeqCst);
  }

  fn calls(counter: &AtomicUsize) -> usize {
    counter.load(Ordering::SeqCst)
  }

  fn run(slot: &Mutex<Option<Action>>) {
    let action = slot.lock().unwrap().take();
    if let Some(action) = action {
      action();
    }
  }

  fn attempts(&self) -> Vec<(String, u64, u32)> {
    self.attempts.lock().unwrap().clone()
  }

  fn begun(&self) -> Vec<TransactionOptions> {
    self.begun.lock().unwrap().clone()
  }

  fn forget_setup(&self) {
    self.begun.lock().unwrap().clear();
    self.attempts.lock().unwrap().clear();
    self.commit_calls.store(0, Ordering::SeqCst);
    self.retention_commit_calls.store(0, Ordering::SeqCst);
    self.delete_batches.lock().unwrap().clear();
    self.mark_batches.lock().unwrap().clear();
  }
}

impl SpannerTestHooks for Hooks {
  fn before_begin_transaction(&self, options: &TransactionOptions) {
    self.begun.lock().unwrap().push(options.clone());
  }

  fn before_attempt(&self, aid: &AidString, seq_nr: SeqNr, attempt: u32) {
    self
      .attempts
      .lock()
      .unwrap()
      .push((aid.as_str().to_string(), seq_nr, attempt));
    if attempt >= 1 {
      Hooks::run(&self.on_retry);
    }
  }

  fn before_commit(&self, _: &AidString, _: SeqNr) -> Result<(), SpannerCommitFault> {
    self.commit_calls.fetch_add(1, Ordering::SeqCst);
    if take_one(&self.aborts) {
      Err(SpannerCommitFault::Aborted)
    } else if take_one(&self.commit_failures) {
      Err(SpannerCommitFault::Failed(failure(StorageOperation::Append)))
    } else {
      Ok(())
    }
  }

  fn after_commit(&self, _: &AidString, _: SeqNr) -> Result<(), EventStoreError> {
    if take_one(&self.lost_responses) {
      Err(failure(StorageOperation::Append))
    } else {
      Ok(())
    }
  }

  fn read_events(&self, _: &AidString) -> Result<(), EventStoreError> {
    Hooks::run(&self.on_read_events);
    Ok(())
  }

  fn read_snapshot(&self, _: &AidString) -> Result<(), EventStoreError> {
    Hooks::run(&self.on_read_snapshot);
    Ok(())
  }

  fn retention_visible_history(
    &self,
    _: &AidString,
    history: &[SeqNr],
    _: Option<SeqNr>,
  ) -> Result<Vec<SeqNr>, EventStoreError> {
    if take_one(&self.query_failures) {
      Err(failure(StorageOperation::Append))
    } else {
      Ok(history.to_vec())
    }
  }

  fn retention_delete(&self, _: &AidString, seq_nrs: &[SeqNr]) -> Result<(), EventStoreError> {
    self.delete_batches.lock().unwrap().push(seq_nrs.to_vec());
    if take_one(&self.delete_failures) {
      Err(failure(StorageOperation::Append))
    } else {
      Ok(())
    }
  }

  fn retention_mark(&self, _: &AidString, seq_nrs: &[SeqNr]) -> Result<(), EventStoreError> {
    self.mark_batches.lock().unwrap().push(seq_nrs.to_vec());
    if take_one(&self.mark_failures) {
      Err(failure(StorageOperation::Append))
    } else {
      Ok(())
    }
  }

  fn before_retention_commit(&self, _: &AidString, _: SeqNr) -> Result<(), SpannerCommitFault> {
    self.retention_commit_calls.fetch_add(1, Ordering::SeqCst);
    if take_one(&self.retention_aborts) {
      Err(SpannerCommitFault::Aborted)
    } else {
      Ok(())
    }
  }

  fn aggregate_hash(&self, _: &AidString) -> Option<u64> {
    *self.fixed_hash.lock().unwrap()
  }
}

async fn open_with_hooks(env: &Env, options: SpannerOptions, hooks: Arc<Hooks>) -> Store {
  Store::open_with_hooks(
    env.connection().await,
    env.tables.clone(),
    options,
    Arc::new(JsonEventSerializer::new()),
    Arc::new(JsonSnapshotSerializer::new()),
    hooks,
  )
  .await
  .expect("フック付きのストアを開ける")
}

/// 初期化した新しいデータベースを、フック付きのストアとして開く。生成で記録したフックの呼び出しは忘れる。
async fn setup(options: SpannerOptions) -> (Fixture, Arc<Hooks>, Store) {
  // 知らせを出しうる追記より前に、tracing の収集器を付け終える（付けるのはプロセスで 1 度だけ）。
  captured_events();
  let fixture = Fixture::initialized(&options).await;
  let hooks = Arc::new(Hooks::default());
  let store = open_with_hooks(&fixture, options, hooks.clone()).await;
  hooks.forget_setup();
  (fixture, hooks, store)
}

/// 別の実行基盤で、フックなしの別の書き手のストアを開き、`work` を実行する処理を返す。
fn as_another_writer<Fut: std::future::Future<Output = ()> + 'static>(
  env: &Env,
  options: SpannerOptions,
  work: impl FnOnce(Store) -> Fut + Send + 'static,
) -> Action {
  let env = env.clone();
  Box::new(move || {
    run_on_another_runtime(move || async move {
      let writer = env.open(options).await.expect("別の書き手のストアを開ける");
      work(writer).await;
    })
  })
}

// ---------------------------------------------------------------------------
// ハッシュの衝突（SPN-4・SP-2）
// ---------------------------------------------------------------------------

const COLLIDING_HASH: u64 = 0x0123_4567_89ab_cdef;

#[tokio::test]
async fn should_spanner_hash_collision_not_mix_the_event_ranges_of_different_aids() {
  let (fixture, hooks, store) = setup(SpannerOptions::default()).await;
  *hooks.fixed_hash.lock().unwrap() = Some(COLLIDING_HASH);
  let order_1 = Id::new("Order", "1");
  let order_12 = Id::new("Order", "12");
  store.persist_event(event(&order_1, 1)).await.unwrap();
  store.persist_event(event(&order_1, 2)).await.unwrap();
  store.persist_event(event(&order_12, 1)).await.unwrap();

  let events = store.get_events_by_id_since_seq_nr(&order_1, 1).await.unwrap();
  let latest = store.get_latest_snapshot_by_id(&order_12).await.unwrap().unwrap();

  assert_eq!(
    events,
    vec![event(&order_1, 1), event(&order_1, 2)],
    "Order-12 の封筒を含まない"
  );
  assert_eq!(latest.head_seq_nr(), 1, "別の aid の head を読まない");
  let heads = fixture.head_rows().await;
  assert_eq!(heads.len(), 2, "同じハッシュでも、aid が違えば主キーが違う行になる");
  assert!(
    heads
      .iter()
      .all(|row| row.hash_prefix == COLLIDING_HASH.to_be_bytes().to_vec()),
    "差し替えたハッシュが hash_prefix に使われている: {heads:?}"
  );
}

#[tokio::test]
async fn should_spanner_hash_collision_not_retire_the_history_of_other_aids() {
  let (fixture, hooks, store) = setup(options_keeping(1)).await;
  *hooks.fixed_hash.lock().unwrap() = Some(COLLIDING_HASH);
  let with_nul = Id::new("Order", "1\0");
  let japanese = Id::new("注文", "1");
  let target = Id::new("Order", "1");
  store
    .persist_event_and_snapshot(event(&with_nul, 1), snapshot(1))
    .await
    .unwrap();
  store
    .persist_event_and_snapshot(event(&japanese, 1), snapshot(1))
    .await
    .unwrap();

  store
    .persist_event_and_snapshot(event(&target, 1), snapshot(1))
    .await
    .unwrap();
  store
    .persist_event_and_snapshot(event(&target, 2), snapshot(2))
    .await
    .unwrap();

  let history: Vec<(String, i64)> = fixture
    .history_rows()
    .await
    .into_iter()
    .map(|row| (row.row.aid, row.row.seq_nr))
    .collect();
  assert!(
    history.contains(&(with_nul.aid(), 1)),
    "ほかの aid の履歴を巻き込まない: {history:?}"
  );
  assert!(
    history.contains(&(japanese.aid(), 1)),
    "ほかの aid の履歴を巻き込まない: {history:?}"
  );
  assert!(history.contains(&("Order-1".to_string(), 2)));
  assert!(
    !history.contains(&("Order-1".to_string(), 1)),
    "対象の aid の古い履歴だけを退ける"
  );
}

#[tokio::test]
async fn should_spanner_default_hash_be_the_fnv_prefix_of_the_aid_when_no_hook_overrides_it() {
  let (fixture, _hooks, store) = setup(SpannerOptions::default()).await;
  let id = Id::order();

  store.persist_event(event(&id, 1)).await.unwrap();

  assert_eq!(fixture.head_rows().await[0].hash_prefix, hash_prefix("Order-1"));
}

// ---------------------------------------------------------------------------
// トランザクションの種類（SPN-1・SPN-9・SPN-10・SPN-11）
// ---------------------------------------------------------------------------

fn is_read_write_serializable(options: &TransactionOptions) -> bool {
  matches!(options.mode, Some(Mode::ReadWrite(_))) && options.isolation_level == IsolationLevel::Serializable as i32
}

fn is_read_only_strong(options: &TransactionOptions) -> bool {
  matches!(
    &options.mode,
    Some(Mode::ReadOnly(read_only)) if matches!(read_only.timestamp_bound, Some(TimestampBound::Strong(true)))
  )
}

#[tokio::test]
async fn should_spanner_transaction_options_of_an_event_append_be_one_read_write_serializable_transaction() {
  let (_fixture, hooks, store) = setup(SpannerOptions::default()).await;

  store.persist_event(event(&Id::order(), 1)).await.unwrap();

  let begun = hooks.begun();
  assert_eq!(
    begun.len(),
    1,
    "イベントだけの追記は、1 つのトランザクション: {begun:?}"
  );
  assert!(
    is_read_write_serializable(&begun[0]),
    "SERIALIZABLE を明示する: {:?}",
    begun[0]
  );
}

#[tokio::test]
async fn should_spanner_transaction_options_of_a_snapshot_append_be_serializable_for_the_append_and_for_the_retention()
{
  let (_fixture, hooks, store) = setup(options_keeping(1)).await;

  store
    .persist_event_and_snapshot(event(&Id::order(), 1), snapshot(1))
    .await
    .unwrap();

  let begun = hooks.begun();
  assert_eq!(begun.len(), 2, "追記と保持は別のトランザクション: {begun:?}");
  assert!(
    begun.iter().all(is_read_write_serializable),
    "どちらも SERIALIZABLE: {begun:?}"
  );
}

#[tokio::test]
async fn should_spanner_transaction_options_of_reads_be_one_read_only_strong_transaction_each() {
  let (_fixture, hooks, store) = setup(SpannerOptions::default()).await;
  let id = Id::order();
  store
    .persist_event_and_snapshot(event(&id, 1), snapshot(1))
    .await
    .unwrap();
  hooks.forget_setup();

  store.get_events_by_id_since_seq_nr(&id, 1).await.unwrap();
  let after_events = hooks.begun();
  hooks.forget_setup();
  store.get_latest_snapshot_by_id(&id).await.unwrap();
  let after_snapshot = hooks.begun();

  assert_eq!(
    after_events.len(),
    1,
    "head と journal は同じトランザクション: {after_events:?}"
  );
  assert!(
    is_read_only_strong(&after_events[0]),
    "strong の読み取り専用: {:?}",
    after_events[0]
  );
  assert_eq!(
    after_snapshot.len(),
    1,
    "head と snapshot_current は 1 つのトランザクション: {after_snapshot:?}"
  );
  assert!(
    is_read_only_strong(&after_snapshot[0]),
    "strong の読み取り専用: {:?}",
    after_snapshot[0]
  );
}

// ---------------------------------------------------------------------------
// 中断の再試行と上限（SPN-7）
// ---------------------------------------------------------------------------

fn retry_options(limit: u32) -> SpannerOptions {
  SpannerOptions {
    transaction_retry_limit: limit,
    ..SpannerOptions::default()
  }
}

#[tokio::test]
async fn should_spanner_aborted_commit_within_the_limit_be_retried_with_the_same_envelope_and_commit_once() {
  let (fixture, hooks, store) = setup(retry_options(3)).await;
  let id = Id::order();
  Hooks::arm(&hooks.aborts, 2);

  let appended = store.persist_event(event(&id, 1)).await;

  assert!(appended.is_ok(), "上限以内なら成功する: {appended:?}");
  assert_eq!(
    hooks.attempts(),
    vec![
      ("Order-1".to_string(), 1, 0),
      ("Order-1".to_string(), 1, 1),
      ("Order-1".to_string(), 1, 2)
    ],
    "初回と 2 回の再試行。どの試行も同じ aid・同じ seq_nr で、採番し直さない"
  );
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&id, 1).await.unwrap(),
    vec![event(&id, 1)]
  );
  assert_eq!(fixture.journal_rows().await.len(), 1, "確定は 1 件だけ");
}

#[tokio::test]
async fn should_spanner_aborted_commit_beyond_the_limit_be_a_storage_error_and_commit_nothing() {
  let (fixture, hooks, store) = setup(retry_options(2)).await;
  let id = Id::order();
  Hooks::arm(&hooks.aborts, 3);

  let appended = store.persist_event(event(&id, 1)).await;

  assert!(
    matches!(
      appended,
      Err(EventStoreError::Storage {
        operation: StorageOperation::Append,
        ..
      })
    ),
    "上限（初回を数えず 2 回）を超えたら保存先エラー: {appended:?}"
  );
  assert_eq!(hooks.attempts().len(), 3, "初回 + 再試行 2 回で止める");
  assert_eq!(
    fixture.row_counts().await,
    (0, 0, 0, 0, 1),
    "何も確定しない（設定の行だけ）"
  );
  store.persist_event(event(&id, 1)).await.unwrap();
  assert_eq!(fixture.journal_rows().await.len(), 1, "同じ追記を送り直すと成功する");
}

#[tokio::test]
async fn should_spanner_aborted_is_the_only_failure_that_is_retried() {
  let (fixture, hooks, store) = setup(retry_options(5)).await;
  let id = Id::order();
  Hooks::arm(&hooks.commit_failures, 1);

  let appended = store.persist_event(event(&id, 1)).await;

  assert!(
    matches!(
      appended,
      Err(EventStoreError::Storage {
        operation: StorageOperation::Append,
        ..
      })
    ),
    "{appended:?}"
  );
  assert_eq!(hooks.attempts().len(), 1, "ABORTED 以外の失敗は再試行しない");
  assert_eq!(Hooks::calls(&hooks.commit_calls), 1);
  assert_eq!(
    fixture.row_counts().await,
    (0, 0, 0, 0, 1),
    "コミット前の失敗は何も見えない"
  );
}

#[tokio::test]
async fn should_spanner_aborted_append_that_lost_to_an_earlier_writer_be_an_optimistic_lock_without_renumbering() {
  let (fixture, hooks, store) = setup(retry_options(3)).await;
  let id = Id::order();
  Hooks::arm(&hooks.aborts, 1);
  *hooks.on_retry.lock().unwrap() = Some(as_another_writer(&fixture, retry_options(3), {
    let id = id.clone();
    move |writer: Store| async move {
      writer
        .persist_event(event_with_payload(&id, 1, json!({ "number": 99 })))
        .await
        .unwrap()
    }
  }));

  let appended = store.persist_event(event(&id, 1)).await;

  assert!(
    matches!(
      &appended,
      Err(EventStoreError::OptimisticLock {
        seq_nr: 1,
        head_seq_nr: Some(1),
        ..
      })
    ),
    "再試行で先に確定した追記を見つけたら楽観ロック（採番し直さない）: {appended:?}"
  );
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&id, 1).await.unwrap(),
    vec![event_with_payload(&id, 1, json!({ "number": 99 }))],
    "先に確定した書き手の封筒だけが残る"
  );
  assert_eq!(
    hooks.attempts(),
    vec![("Order-1".to_string(), 1, 0), ("Order-1".to_string(), 1, 1)],
    "再試行も同じ seq_nr"
  );
}

// ---------------------------------------------------------------------------
// コミットの応答の消失（SPN-8・W-7）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_spanner_lost_commit_response_be_an_unknown_outcome_storage_error_without_retry_or_compensation() {
  let (fixture, hooks, store) = setup(retry_options(5)).await;
  let id = Id::order();
  Hooks::arm(&hooks.lost_responses, 1);

  let lost = store.persist_event(event(&id, 1)).await;

  assert!(
    matches!(
      lost,
      Err(EventStoreError::Storage {
        operation: StorageOperation::Append,
        ..
      })
    ),
    "{lost:?}"
  );
  assert_eq!(Hooks::calls(&hooks.commit_calls), 1, "応答の消失で再送しない");
  assert_eq!(hooks.attempts().len(), 1, "再試行しない");
  assert_eq!(
    fixture.journal_rows().await.len(),
    1,
    "確定していたイベントを削除して補償しない"
  );
  let resent = store.persist_event(event(&id, 1)).await;
  assert!(
    matches!(&resent, Err(EventStoreError::OptimisticLock { seq_nr: 1, .. })),
    "更新は確定していたので、同じ封筒の送り直しは楽観ロックになる: {resent:?}"
  );
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&id, 1).await.unwrap(),
    vec![event(&id, 1)]
  );
}

// ---------------------------------------------------------------------------
// 同じ読み取りの時点（SPN-9・SPN-10・R-8）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_spanner_latest_snapshot_read_the_head_and_the_current_snapshot_at_the_same_read_point() {
  let (fixture, hooks, store) = setup(SpannerOptions::default()).await;
  let id = Id::order();
  store
    .persist_event_and_snapshot(event(&id, 1), snapshot(1))
    .await
    .unwrap();
  *hooks.on_read_snapshot.lock().unwrap() = Some(as_another_writer(&fixture, SpannerOptions::default(), {
    let id = id.clone();
    move |writer: Store| async move {
      writer
        .persist_event_and_snapshot(event(&id, 2), snapshot(2))
        .await
        .unwrap()
    }
  }));

  let during = store.get_latest_snapshot_by_id(&id).await.unwrap().unwrap();

  assert_eq!(during.head_seq_nr(), 1, "読み取りの時点のヘッド");
  assert_eq!(
    during.snapshot().unwrap().seq_nr(),
    1,
    "ヘッドと同じ時点のスナップショット"
  );
  let after = store.get_latest_snapshot_by_id(&id).await.unwrap().unwrap();
  assert_eq!(
    after.head_seq_nr(),
    2,
    "別の書き手の追記は確定していて、次の読み取りには見える"
  );
  assert_eq!(after.snapshot().unwrap().seq_nr(), 2);
}

#[tokio::test]
async fn should_spanner_read_events_return_the_rows_up_to_the_head_that_was_read_at_the_same_read_point() {
  let (fixture, hooks, store) = setup(SpannerOptions::default()).await;
  let id = Id::order();
  store.persist_event(event(&id, 1)).await.unwrap();
  store.persist_event(event(&id, 2)).await.unwrap();
  *hooks.on_read_events.lock().unwrap() = Some(as_another_writer(&fixture, SpannerOptions::default(), {
    let id = id.clone();
    move |writer: Store| async move { writer.persist_event(event(&id, 3)).await.unwrap() }
  }));

  let during = store.get_events_by_id_since_seq_nr(&id, 1).await.unwrap();

  assert_eq!(
    during,
    vec![event(&id, 1), event(&id, 2)],
    "最初に取得した h までを返し、途中で確定した 3 を混ぜない"
  );
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&id, 1).await.unwrap(),
    vec![event(&id, 1), event(&id, 2), event(&id, 3)],
    "次の読み取りには見える"
  );
}

// ---------------------------------------------------------------------------
// 保持の失敗は確定済みの追記を変えない（SPN-13・S-4）
// ---------------------------------------------------------------------------

async fn history_of(fixture: &Fixture) -> Vec<(i64, bool)> {
  fixture
    .history_rows()
    .await
    .into_iter()
    .map(|row| (row.row.seq_nr, row.expired_at.is_some()))
    .collect()
}

#[tokio::test]
async fn should_spanner_retention_delete_failure_keep_the_append_successful_and_retry_on_the_next_snapshot() {
  let captured = captured_events();
  let (fixture, hooks, store) = setup(options_keeping(1)).await;
  let id = Id::new("Order", "retention-delete-failure");
  store
    .persist_event_and_snapshot(event(&id, 1), snapshot(1))
    .await
    .unwrap();
  Hooks::arm(&hooks.delete_failures, 1);

  let appended = store.persist_event_and_snapshot(event(&id, 2), snapshot(2)).await;

  assert!(appended.is_ok(), "追記を失敗として返さない: {appended:?}");
  assert_eq!(
    captured.categories_of("event_store_adapter::retention", &id.aid()),
    vec!["retention-failure".to_string()],
    "保持の失敗を知らせる"
  );
  assert_eq!(
    history_of(&fixture).await,
    vec![(1, false), (2, false)],
    "削除に失敗したので履歴は残る"
  );
  assert_eq!(fixture.head_rows().await[0].seq_nr, 2, "追記は確定している");
  assert_eq!(fixture.current_rows().await[0].seq_nr, 2);
  store
    .persist_event_and_snapshot(event(&id, 3), snapshot(3))
    .await
    .unwrap();
  assert_eq!(
    history_of(&fixture).await,
    vec![(3, false)],
    "次のスナップショットの書き込みで片付く"
  );
}

#[tokio::test]
async fn should_spanner_retention_query_failure_keep_the_append_successful() {
  let (fixture, hooks, store) = setup(options_keeping(1)).await;
  let id = Id::order();
  store
    .persist_event_and_snapshot(event(&id, 1), snapshot(1))
    .await
    .unwrap();
  Hooks::arm(&hooks.query_failures, 1);

  let appended = store.persist_event_and_snapshot(event(&id, 2), snapshot(2)).await;

  assert!(appended.is_ok(), "追記を失敗として返さない: {appended:?}");
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&id, 1).await.unwrap(),
    vec![event(&id, 1), event(&id, 2)]
  );
  assert_eq!(history_of(&fixture).await, vec![(1, false), (2, false)]);
}

#[tokio::test]
async fn should_spanner_retention_mark_failure_keep_the_append_successful_and_mark_on_the_next_snapshot() {
  let captured = captured_events();
  let (fixture, hooks, store) = setup(ttl_options(1, 7)).await;
  let id = Id::new("Order", "retention-mark-failure");
  store
    .persist_event_and_snapshot(event(&id, 1), snapshot(1))
    .await
    .unwrap();
  Hooks::arm(&hooks.mark_failures, 1);

  let appended = store.persist_event_and_snapshot(event(&id, 2), snapshot(2)).await;

  assert!(appended.is_ok(), "追記を失敗として返さない: {appended:?}");
  assert_eq!(
    captured.categories_of("event_store_adapter::retention", &id.aid()),
    vec!["retention-failure".to_string()]
  );
  assert_eq!(
    history_of(&fixture).await,
    vec![(1, false), (2, false)],
    "印付けに失敗したので、印は付かない"
  );
  store
    .persist_event_and_snapshot(event(&id, 3), snapshot(3))
    .await
    .unwrap();
  assert_eq!(
    history_of(&fixture).await,
    vec![(1, true), (2, true), (3, false)],
    "次のスナップショットの書き込みで、古い行だけに印を付ける"
  );
}

// ---------------------------------------------------------------------------
// 保持のトランザクションの中断（SPN-11・SPN-13・SPN-7）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_spanner_retention_aborted_commit_within_the_limit_be_retried_with_the_candidates_selected_again() {
  let captured = captured_events();
  let (fixture, hooks, store) = setup(options_keeping(1)).await;
  let id = Id::new("Order", "retention-aborted-within-limit");
  store
    .persist_event_and_snapshot(event(&id, 1), snapshot(1))
    .await
    .unwrap();
  Hooks::arm(&hooks.retention_aborts, 2);

  let appended = store.persist_event_and_snapshot(event(&id, 2), snapshot(2)).await;

  assert!(appended.is_ok(), "{appended:?}");
  assert_eq!(
    Hooks::calls(&hooks.retention_commit_calls),
    3,
    "中断された 2 回の後の 3 回目の Commit で確定する（上限は既定の 5 回）"
  );
  assert_eq!(
    *hooks.delete_batches.lock().unwrap(),
    vec![vec![1], vec![1], vec![1]],
    "試行のたびに候補を選び直す。中断された試行の削除は確定しない"
  );
  assert_eq!(
    history_of(&fixture).await,
    vec![(2, false)],
    "最後の試行で古い履歴が消える"
  );
  assert!(
    captured
      .categories_of("event_store_adapter::retention", &id.aid())
      .is_empty(),
    "上限以内なので、保持の失敗を知らせない"
  );
}

#[tokio::test]
async fn should_spanner_retention_aborted_commit_beyond_the_limit_keep_the_append_successful_and_retry_on_the_next_snapshot(
) {
  let captured = captured_events();
  let options = SpannerOptions {
    transaction_retry_limit: 2,
    ..options_keeping(1)
  };
  let (fixture, hooks, store) = setup(options).await;
  let id = Id::new("Order", "retention-aborted-beyond-limit");
  store
    .persist_event_and_snapshot(event(&id, 1), snapshot(1))
    .await
    .unwrap();
  Hooks::arm(&hooks.retention_aborts, 3);

  let appended = store.persist_event_and_snapshot(event(&id, 2), snapshot(2)).await;

  assert!(
    appended.is_ok(),
    "保持の中断が上限を超えても、確定した追記は成功のまま: {appended:?}"
  );
  assert_eq!(
    Hooks::calls(&hooks.retention_commit_calls),
    3,
    "上限 2 は最初の試行を数えないので、3 回目の中断で諦める"
  );
  assert_eq!(
    captured.categories_and_phases_of("event_store_adapter::retention", &id.aid()),
    vec![("retention-failure".to_string(), Some("retention-delete".to_string()))],
    "保持の失敗を知らせる。段階は、最後に中断した削除（問い合わせではない）"
  );
  assert_eq!(
    history_of(&fixture).await,
    vec![(1, false), (2, false)],
    "中断された試行は何も確定しないので、履歴は残る"
  );
  assert_eq!(fixture.head_rows().await[0].seq_nr, 2, "追記は確定している");
  store
    .persist_event_and_snapshot(event(&id, 3), snapshot(3))
    .await
    .unwrap();
  assert_eq!(
    history_of(&fixture).await,
    vec![(3, false)],
    "次のスナップショットの書き込みで片付く"
  );
}

// ---------------------------------------------------------------------------
// 1000 件を超える整理は分割し、毎回選び直す（SPN-13）
// ---------------------------------------------------------------------------

/// 履歴の seq_nr が 1 から `through` まで続く集約を作る。`seq_nr = 1` だけストアで書き、2 から `through` までの履歴と
/// head の位置は、生の DML で入れる（1000 件を超える履歴を、1000 回の追記なしで用意するため）。
async fn aggregate_with_history_through(fixture: &Fixture, store: &Store, id: &Id, through: i64) {
  store
    .persist_event_and_snapshot(event(id, 1), snapshot(1))
    .await
    .unwrap();
  let key = || {
    vec![
      ("hp", Param::Bytes(hash_prefix(&id.aid()))),
      ("aid", Param::String(id.aid())),
    ]
  };
  let mut insert = key();
  insert.push(("last", Param::Int64(through)));
  let inserted = fixture
    .raw_execute(
      &format!(
        "INSERT INTO {PREFIX}_snapshot_history (hash_prefix, aid, seq_nr, manifest, payload, expired_at) \
         SELECT @hp, @aid, n, '', b'', CAST(NULL AS TIMESTAMP) FROM UNNEST(GENERATE_ARRAY(2, @last)) AS n"
      ),
      &insert,
    )
    .await;
  assert_eq!(inserted, through - 1, "2 から {through} までの履歴を入れた");
  let mut update = key();
  update.push(("last", Param::Int64(through)));
  let updated = fixture
    .raw_execute(
      &format!("UPDATE {PREFIX}_head SET seq_nr = @last WHERE hash_prefix = @hp AND aid = @aid"),
      &update,
    )
    .await;
  assert_eq!(updated, 1, "head の位置を {through} に進めた");
}

fn range(first: SeqNr, last: SeqNr) -> Vec<SeqNr> {
  (first..=last).collect()
}

#[tokio::test]
async fn should_spanner_retention_split_more_than_one_thousand_candidates_and_never_remove_the_newest_rows() {
  let (fixture, hooks, store) = setup(options_keeping(2)).await;
  let id = Id::new("Order", "retention-split-delete");
  aggregate_with_history_through(&fixture, &store, &id, 1004).await;
  hooks.forget_setup();

  let appended = store.persist_event_and_snapshot(event(&id, 1005), snapshot(1005)).await;

  assert!(appended.is_ok(), "{appended:?}");
  assert_eq!(
    *hooks.delete_batches.lock().unwrap(),
    vec![range(1, 1000), range(1001, 1003)],
    "古い順に 1000 件、選び直した残りの 3 件に分ける。新しい 2 件（1004・1005）は含まない"
  );
  assert_eq!(
    Hooks::calls(&hooks.retention_commit_calls),
    2,
    "分けた数だけトランザクションを確定する"
  );
  assert_eq!(
    history_of(&fixture).await,
    vec![(1004, false), (1005, false)],
    "残るのは新しい 2 件だけ"
  );
  assert_eq!(fixture.head_rows().await[0].seq_nr, 1005);
  assert_eq!(fixture.current_rows().await[0].seq_nr, 1005);
}

#[tokio::test]
async fn should_spanner_ttl_retention_split_more_than_one_thousand_candidates_and_mark_only_the_old_rows() {
  let (fixture, hooks, store) = setup(ttl_options(2, 7)).await;
  let id = Id::new("Order", "retention-split-mark");
  aggregate_with_history_through(&fixture, &store, &id, 1004).await;
  hooks.forget_setup();

  let appended = store.persist_event_and_snapshot(event(&id, 1005), snapshot(1005)).await;

  assert!(appended.is_ok(), "{appended:?}");
  assert_eq!(
    *hooks.mark_batches.lock().unwrap(),
    vec![range(1, 1000), range(1001, 1003)],
    "印付け済みの行は選び直しに現れないので、2 回目は残りの 3 件だけを印付けする"
  );
  assert_eq!(Hooks::calls(&hooks.retention_commit_calls), 2);
  let history = history_of(&fixture).await;
  assert_eq!(history.len(), 1005, "TTL 方式では行を消さない");
  let (marked, unmarked): (Vec<_>, Vec<_>) = history.into_iter().partition(|(_, marked)| *marked);
  assert_eq!(
    marked.iter().map(|(seq_nr, _)| *seq_nr).collect::<Vec<_>>(),
    range(1, 1003)
      .into_iter()
      .map(|seq_nr| seq_nr as i64)
      .collect::<Vec<_>>(),
    "古い 1003 件に印が付く"
  );
  assert_eq!(
    unmarked,
    vec![(1004, false), (1005, false)],
    "新しい 2 件は印のないまま残る"
  );
}
