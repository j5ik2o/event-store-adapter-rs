#![cfg(all(feature = "bigtable", feature = "test-hooks"))]
//! Bigtable の保存先の結合試験のうち、障害の差し込みや、読み取りの途中に別の書き手を動かす試験（エミュレーター）。

mod bigtable_support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bigtable_support::capture::captured_events;
use bigtable_support::{
  cells_in, event, event_with_payload, expected_envelope_of, expected_snapshot_envelope_of, journal_key, only_value,
  options_keeping, row_prefix, run_on_another_runtime, snapshot, Env, Fixture, Id, Store, TableKind, RECORDED_STORE_ID,
};
use event_store_adapter_rs::bigtable::{
  overwrite_bigtable_configuration_for_test, BigtableConfigurationRecord, BigtableTestHooks,
};
use event_store_adapter_rs::{
  AidString, BigtableOptions, EventStore, EventStoreError, JsonEventSerializer, JsonSnapshotSerializer, SeqNr,
  StorageOperation,
};
use serde_json::json;

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

/// 障害を差し込み、各フックの呼び出しを数える。読み取りや書き出しのフックの中では、別の書き手を動かせる。
#[derive(Default)]
struct Hooks {
  previous_failures: AtomicUsize,
  tail_failures: AtomicUsize,
  commit_failures: AtomicUsize,
  lost_responses: AtomicUsize,
  query_failures: AtomicUsize,
  delete_failures: AtomicUsize,
  previous_calls: AtomicUsize,
  tail_calls: AtomicUsize,
  commit_calls: AtomicUsize,
  fixed_hash: Mutex<Option<u64>>,
  on_previous: Mutex<Option<Action>>,
  on_commit: Mutex<Option<Action>>,
  on_read_events: Mutex<Option<Action>>,
  on_read_snapshot: Mutex<Option<Action>>,
}

impl std::fmt::Debug for Hooks {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    formatter.debug_struct("Hooks").finish()
  }
}

impl Hooks {
  fn arm(counter: &AtomicUsize) {
    counter.store(1, Ordering::SeqCst);
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
}

impl BigtableTestHooks for Hooks {
  fn before_commit(&self, _: &AidString, _: SeqNr) -> Result<(), EventStoreError> {
    self.commit_calls.fetch_add(1, Ordering::SeqCst);
    Hooks::run(&self.on_commit);
    if take_one(&self.commit_failures) {
      Err(failure(StorageOperation::Append))
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

  fn journal_write_previous(&self, _: &AidString, _: SeqNr) -> Result<(), EventStoreError> {
    self.previous_calls.fetch_add(1, Ordering::SeqCst);
    Hooks::run(&self.on_previous);
    if take_one(&self.previous_failures) {
      Err(failure(StorageOperation::Append))
    } else {
      Ok(())
    }
  }

  fn journal_write_tail(&self, _: &AidString, _: SeqNr) -> Result<(), EventStoreError> {
    self.tail_calls.fetch_add(1, Ordering::SeqCst);
    if take_one(&self.tail_failures) {
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

  fn retention_delete(&self, _: &AidString, _: &[SeqNr]) -> Result<(), EventStoreError> {
    if take_one(&self.delete_failures) {
      Err(failure(StorageOperation::Append))
    } else {
      Ok(())
    }
  }

  fn aggregate_hash(&self, _: &AidString) -> Option<u64> {
    *self.fixed_hash.lock().unwrap()
  }
}

async fn open_with_hooks(env: &Env, options: BigtableOptions, hooks: Arc<Hooks>) -> Store {
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

/// 初期化した新しいテーブルを、フック付きのストアとして開く。
async fn setup(options: BigtableOptions) -> (Fixture, Arc<Hooks>, Store) {
  // 知らせを出しうる追記より前に、tracing の収集器を付け終える（付けるのはプロセスで 1 度だけ）。
  captured_events();
  let fixture = Fixture::initialized(&options).await;
  let hooks = Arc::new(Hooks::default());
  let store = open_with_hooks(&fixture, options, hooks.clone()).await;
  (fixture, hooks, store)
}

/// 別の実行基盤で、フックなしの別の書き手のストアを開き、`work` を実行する処理を返す。
fn as_another_writer<Fut: std::future::Future<Output = ()> + 'static>(
  env: &Env,
  options: BigtableOptions,
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
// ハッシュの衝突（BT-3・BT-4）
// ---------------------------------------------------------------------------

const COLLIDING_HASH: u64 = 0x0123_4567_89ab_cdef;

#[tokio::test]
async fn should_bigtable_colliding_hash_not_mix_the_event_ranges_of_different_aids() {
  let (_fixture, hooks, store) = setup(BigtableOptions::default()).await;
  *hooks.fixed_hash.lock().unwrap() = Some(COLLIDING_HASH);
  let order_1 = Id::new("Order", "1");
  let order_12 = Id::new("Order", "12");
  store.persist_event(event(&order_1, 1)).await.unwrap();
  store.persist_event(event(&order_1, 2)).await.unwrap();
  store.persist_event(event(&order_12, 1)).await.unwrap();

  let events = store.get_events_by_id_since_seq_nr(&order_1, 1).await.unwrap();

  assert_eq!(
    events,
    vec![event(&order_1, 1), event(&order_1, 2)],
    "Order-12 の封筒を含まない"
  );
}

#[tokio::test]
async fn should_bigtable_colliding_hash_not_retire_the_history_of_other_aids() {
  let (_fixture, hooks, store) = setup(options_keeping(1)).await;
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

  assert_eq!(store.history_view(&with_nul.aid_string()).await.unwrap(), vec![1]);
  assert_eq!(store.history_view(&japanese.aid_string()).await.unwrap(), vec![1]);
}

// ---------------------------------------------------------------------------
// 封筒の上限（BT-16）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_bigtable_envelope_over_the_limit_be_rejected_before_any_write_hook_runs() {
  let options = BigtableOptions {
    envelope_limit_bytes: 1024,
    ..BigtableOptions::default()
  };
  let (_fixture, hooks, store) = setup(options).await;
  let id = Id::new("Order", "9");
  store.persist_event(event(&id, 1)).await.unwrap();
  let previous = Hooks::calls(&hooks.previous_calls);
  let commit = Hooks::calls(&hooks.commit_calls);
  let tail = Hooks::calls(&hooks.tail_calls);

  let result = store
    .persist_event(event_with_payload(&id, 2, json!("x".repeat(2048))))
    .await;

  assert!(result.is_err());
  assert_eq!(
    Hooks::calls(&hooks.previous_calls),
    previous,
    "直前のイベントの書き出しの前に拒否する"
  );
  assert_eq!(Hooks::calls(&hooks.commit_calls), commit, "head の更新の前に拒否する");
  assert_eq!(Hooks::calls(&hooks.tail_calls), tail, "確定の後の検査ではない");
}

// ---------------------------------------------------------------------------
// journal の行の作成と照合（BT-5）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_bigtable_journal_row_that_already_holds_the_same_envelope_be_verified_and_accepted() {
  let (_fixture, hooks, store) = setup(BigtableOptions::default()).await;
  let id = Id::order();
  store.persist_event(event(&id, 1)).await.unwrap();
  Hooks::arm(&hooks.commit_failures);

  let failed = store.persist_event(event(&id, 2)).await;
  let retried = store.persist_event(event(&id, 2)).await;

  assert!(failed.is_err(), "確定の前の失敗: {failed:?}");
  assert!(
    retried.is_ok(),
    "行 1 は既にある。同じ封筒なので照合して成功する: {retried:?}"
  );
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&id, 1).await.unwrap(),
    vec![event(&id, 1), event(&id, 2)]
  );
}

#[tokio::test]
async fn should_bigtable_journal_row_with_a_different_envelope_be_a_storage_error_and_stay_untouched() {
  let (fixture, _hooks, store) = setup(BigtableOptions::default()).await;
  let id = Id::order();
  store.persist_event(event(&id, 1)).await.unwrap();
  let prefix = row_prefix(&id.aid(), None);
  let row_key = journal_key(&prefix, 1);
  fixture
    .raw_put(TableKind::Journal, &row_key, "event", "envelope", 1_000, b"tampered")
    .await;

  let result = store.persist_event(event(&id, 2)).await;

  assert!(
    matches!(result, Err(EventStoreError::Storage { .. })),
    "照合しないで成功にしない: {result:?}"
  );
  let row = fixture.raw_row(TableKind::Journal, &row_key).await;
  assert_eq!(
    cells_in(&row, "event", "envelope")[0].value,
    b"tampered".to_vec(),
    "既存の行を上書きしない"
  );
  assert_eq!(
    store
      .get_latest_snapshot_by_id(&id)
      .await
      .unwrap()
      .unwrap()
      .head_seq_nr(),
    1,
    "ヘッドは進まない"
  );
}

// ---------------------------------------------------------------------------
// h の必須の書き出しの失敗（BT-7・BT-D7）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_bigtable_previous_event_write_failure_keep_the_head_and_be_a_storage_error() {
  let (fixture, hooks, store) = setup(BigtableOptions::default()).await;
  let id = Id::order();
  store.persist_event(event(&id, 1)).await.unwrap();
  Hooks::arm(&hooks.previous_failures);

  let failed = store.persist_event(event(&id, 2)).await;

  assert!(
    matches!(
      failed,
      Err(EventStoreError::Storage {
        operation: StorageOperation::Append,
        ..
      })
    ),
    "{failed:?}"
  );
  assert_eq!(
    Hooks::calls(&hooks.commit_calls),
    1,
    "失敗した追記は head の更新に進まない（最初の追記の分だけ）"
  );
  let prefix = row_prefix(&id.aid(), None);
  let head = fixture.raw_row(TableKind::Head, &prefix).await;
  assert_eq!(
    only_value(&head, "head", "seq_nr"),
    Some(bigtable_support::head_seq_nr_text(1)),
    "head は h のまま"
  );
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&id, 1).await.unwrap(),
    vec![event(&id, 1)]
  );
  store
    .persist_event(event(&id, 2))
    .await
    .expect("障害がなくなれば、同じ seq_nr で追記できる");
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&id, 1).await.unwrap(),
    vec![event(&id, 1), event(&id, 2)]
  );
}

// ---------------------------------------------------------------------------
// 条件付き更新の応答の消失（BT-7）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_bigtable_lost_commit_response_be_an_unknown_outcome_storage_error_without_resending() {
  let (_fixture, hooks, store) = setup(BigtableOptions::default()).await;
  let id = Id::order();
  Hooks::arm(&hooks.lost_responses);

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
// 条件付き更新の競合（H-2）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_bigtable_creation_lost_to_a_concurrent_writer_be_an_optimistic_lock() {
  let (fixture, hooks, store) = setup(BigtableOptions::default()).await;
  let id = Id::order();
  *hooks.on_commit.lock().unwrap() = Some(as_another_writer(&fixture, BigtableOptions::default(), {
    let id = id.clone();
    move |writer: Store| async move { writer.persist_event(event(&id, 1)).await.unwrap() }
  }));

  let result = store
    .persist_event(event_with_payload(&id, 1, json!({ "number": 99 })))
    .await;

  assert!(
    matches!(&result, Err(EventStoreError::OptimisticLock { seq_nr: 1, .. })),
    "契約違反や保存先エラーにしない: {result:?}"
  );
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&id, 1).await.unwrap(),
    vec![event(&id, 1)],
    "先に確定した書き手の封筒だけが残る"
  );
}

#[tokio::test]
async fn should_bigtable_update_lost_to_a_concurrent_writer_be_an_optimistic_lock() {
  let (fixture, hooks, store) = setup(BigtableOptions::default()).await;
  let id = Id::order();
  store.persist_event(event(&id, 1)).await.unwrap();
  *hooks.on_commit.lock().unwrap() = Some(as_another_writer(&fixture, BigtableOptions::default(), {
    let id = id.clone();
    move |writer: Store| async move { writer.persist_event(event(&id, 2)).await.unwrap() }
  }));

  let result = store
    .persist_event(event_with_payload(&id, 2, json!({ "number": 99 })))
    .await;

  assert!(
    matches!(
      &result,
      Err(EventStoreError::OptimisticLock {
        seq_nr: 2,
        head_seq_nr: None,
        ..
      })
    ),
    "条件付き更新の不成立は、古いと分かっている head の seq_nr を含めない楽観ロックにする: {result:?}"
  );
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&id, 1).await.unwrap(),
    vec![event(&id, 1), event(&id, 2)],
    "次の CAS の勝者だけが確定する"
  );
}

// ---------------------------------------------------------------------------
// 確定後の末尾の書き出しの失敗（BT-7・BT-13・S-4）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_bigtable_new_tail_write_failure_keep_the_append_successful_and_report_journal_write_failure() {
  let captured = captured_events();
  let (fixture, hooks, store) = setup(BigtableOptions::default()).await;
  let id = Id::new("Order", "tail-write-failure");
  store.persist_event(event(&id, 1)).await.unwrap();
  Hooks::arm(&hooks.tail_failures);

  let appended = store.persist_event(event(&id, 2)).await;

  assert!(appended.is_ok(), "確定した追記を失敗として返さない: {appended:?}");
  assert_eq!(
    captured.categories_of("event_store_adapter::journal", &id.aid()),
    vec!["journal-write-failure".to_string()],
    "失敗は別の経路で知らせる"
  );
  let prefix = row_prefix(&id.aid(), None);
  assert!(
    fixture
      .raw_row(TableKind::Journal, &journal_key(&prefix, 2))
      .await
      .is_empty(),
    "末尾の行はまだ書かれていない"
  );
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&id, 1).await.unwrap(),
    vec![event(&id, 1), event(&id, 2)],
    "head が末尾を持つので全件読める"
  );
  store.persist_event(event(&id, 3)).await.unwrap();
  assert!(
    !fixture
      .raw_row(TableKind::Journal, &journal_key(&prefix, 2))
      .await
      .is_empty(),
    "次の書き手が journal の行 2 を完成させる"
  );
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&id, 1).await.unwrap(),
    vec![event(&id, 1), event(&id, 2), event(&id, 3)]
  );
}

/// 保持件数 2 で、スナップショット付きの追記 2 の末尾の書き出しだけを失敗させた状態を作る。
/// 追記 1 は成功し、追記 2 は成功を返すが、journal の行 2 はまだない。
async fn store_with_a_failed_tail_write_of_a_snapshot_append() -> (Fixture, Store, Id) {
  let (fixture, hooks, store) = setup(options_keeping(2)).await;
  let id = Id::order();
  store
    .persist_event_and_snapshot(event(&id, 1), snapshot(1))
    .await
    .unwrap();
  Hooks::arm(&hooks.tail_failures);

  let appended = store.persist_event_and_snapshot(event(&id, 2), snapshot(2)).await;

  assert!(appended.is_ok(), "確定した追記を失敗として返さない: {appended:?}");
  let prefix = row_prefix(&id.aid(), None);
  assert!(
    fixture
      .raw_row(TableKind::Journal, &journal_key(&prefix, 2))
      .await
      .is_empty(),
    "末尾の行 2 はまだ書かれていない"
  );
  (fixture, store, id)
}

// 末尾の書き出しに失敗した直後は、head の末尾のスナップショットを、履歴に数える（BT-11）。
#[tokio::test]
async fn should_bigtable_snapshot_of_a_failed_tail_write_still_count_as_history_and_stay_the_latest_snapshot() {
  let (_fixture, store, id) = store_with_a_failed_tail_write_of_a_snapshot_append().await;

  assert_eq!(
    store.history_view(&id.aid_string()).await.unwrap(),
    vec![1, 2],
    "行 2 がなくても、head の末尾のスナップショット 2 を履歴に数える"
  );
  let latest = store
    .get_latest_snapshot_by_id(&id)
    .await
    .unwrap()
    .expect("スナップショットがある");
  assert_eq!(latest.head_seq_nr(), 2);
  assert_eq!(latest.snapshot().map(|snapshot| snapshot.seq_nr()), Some(2));
}

// 次の追記（イベントだけ）が、直前の行 2 を、履歴付きで作る（BT-7 手順 3）。head の current が上書きされても、
// 履歴のスナップショット 2 は失われない。
#[tokio::test]
async fn should_bigtable_next_event_append_create_the_missing_row_with_the_snapshot_history() {
  let (fixture, store, id) = store_with_a_failed_tail_write_of_a_snapshot_append().await;

  store.persist_event(event(&id, 3)).await.unwrap();

  let prefix = row_prefix(&id.aid(), None);
  let row_2 = fixture.raw_row(TableKind::Journal, &journal_key(&prefix, 2)).await;
  assert_eq!(
    only_value(&row_2, "event", "envelope"),
    Some(expected_envelope_of(&id, 2)),
    "行 2 のイベント"
  );
  assert_eq!(only_value(&row_2, "event", "has_snapshot"), Some(b"1".to_vec()));
  assert_eq!(only_value(&row_2, "history", "state"), Some(b"active".to_vec()));
  assert_eq!(
    only_value(&row_2, "history", "envelope"),
    Some(expected_snapshot_envelope_of(2)),
    "行 2 の履歴は、失敗した追記のスナップショット 2"
  );
  assert_eq!(store.history_view(&id.aid_string()).await.unwrap(), vec![1, 2]);
  let latest = store
    .get_latest_snapshot_by_id(&id)
    .await
    .unwrap()
    .expect("スナップショットがある");
  assert_eq!(latest.head_seq_nr(), 3);
  assert_eq!(
    latest.snapshot().map(|snapshot| snapshot.seq_nr()),
    Some(2),
    "最新のスナップショットは 2 のまま"
  );
}

// 次の追記（スナップショット付き）でも、行 2 を履歴付きで作る。保持件数 2 なので、古い行 1 だけが retired になる。
#[tokio::test]
async fn should_bigtable_next_snapshot_append_create_the_missing_row_and_retire_only_the_oldest_history() {
  let (fixture, store, id) = store_with_a_failed_tail_write_of_a_snapshot_append().await;

  store
    .persist_event_and_snapshot(event(&id, 3), snapshot(3))
    .await
    .unwrap();

  let prefix = row_prefix(&id.aid(), None);
  let row_1 = fixture.raw_row(TableKind::Journal, &journal_key(&prefix, 1)).await;
  assert_eq!(only_value(&row_1, "history", "state"), Some(b"retired".to_vec()));
  let row_2 = fixture.raw_row(TableKind::Journal, &journal_key(&prefix, 2)).await;
  assert_eq!(only_value(&row_2, "history", "state"), Some(b"active".to_vec()));
  assert_eq!(
    only_value(&row_2, "history", "envelope"),
    Some(expected_snapshot_envelope_of(2)),
    "履歴のスナップショット 2 は、新しいスナップショット 3 を追記しても失われない"
  );
  let row_3 = fixture.raw_row(TableKind::Journal, &journal_key(&prefix, 3)).await;
  assert_eq!(only_value(&row_3, "history", "state"), Some(b"active".to_vec()));
  assert_eq!(store.history_view(&id.aid_string()).await.unwrap(), vec![2, 3]);
}

// ---------------------------------------------------------------------------
// 読み取りの途中に別の書き手が head を進める（BT-8・BT-9）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_bigtable_latest_snapshot_read_the_head_and_the_current_snapshot_as_one_point() {
  let (fixture, hooks, store) = setup(BigtableOptions::default()).await;
  let id = Id::order();
  store
    .persist_event_and_snapshot(event(&id, 1), snapshot(1))
    .await
    .unwrap();
  *hooks.on_read_snapshot.lock().unwrap() = Some(as_another_writer(&fixture, BigtableOptions::default(), {
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
async fn should_bigtable_read_events_return_the_rows_up_to_the_head_that_was_read_first() {
  let (fixture, hooks, store) = setup(BigtableOptions::default()).await;
  let id = Id::order();
  store.persist_event(event(&id, 1)).await.unwrap();
  store.persist_event(event(&id, 2)).await.unwrap();
  *hooks.on_read_events.lock().unwrap() = Some(as_another_writer(&fixture, BigtableOptions::default(), {
    let id = id.clone();
    move |writer: Store| async move { writer.persist_event(event(&id, 3)).await.unwrap() }
  }));

  let during = store.get_events_by_id_since_seq_nr(&id, 1).await.unwrap();

  assert_eq!(
    during,
    vec![event(&id, 1), event(&id, 2)],
    "最初に取得した h までを返し、途中で head を読み直さず、h 以上の物理行を混ぜない"
  );
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&id, 1).await.unwrap(),
    vec![event(&id, 1), event(&id, 2), event(&id, 3)],
    "次の読み取りには見える"
  );
}

// ---------------------------------------------------------------------------
// 遅れた書き手が retired の履歴を復活させない（BT-5・BT-12）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_bigtable_late_writer_not_resurrect_a_retired_history() {
  let (fixture, hooks, late) = setup(options_keeping(1)).await;
  let id = Id::order();
  late
    .persist_event_and_snapshot(event(&id, 1), snapshot(1))
    .await
    .unwrap();
  *hooks.on_previous.lock().unwrap() = Some(as_another_writer(&fixture, options_keeping(1), {
    let id = id.clone();
    move |writer: Store| async move {
      writer
        .persist_event_and_snapshot(event(&id, 2), snapshot(2))
        .await
        .unwrap();
      writer
        .persist_event_and_snapshot(event(&id, 3), snapshot(3))
        .await
        .unwrap();
    }
  }));

  let result = late
    .persist_event_and_snapshot(event_with_payload(&id, 2, json!({ "number": 99 })), snapshot(2))
    .await;

  assert!(
    matches!(&result, Err(EventStoreError::OptimisticLock { seq_nr: 2, .. })),
    "遅れた書き手の更新は競合で負ける: {result:?}"
  );
  let prefix = row_prefix(&id.aid(), None);
  let row_1 = fixture.raw_row(TableKind::Journal, &journal_key(&prefix, 1)).await;
  assert_eq!(
    only_value(&row_1, "history", "state"),
    Some(b"retired".to_vec()),
    "行 1 は retired のまま"
  );
  assert!(
    cells_in(&row_1, "history", "envelope").is_empty(),
    "遅れた書き出しは履歴を作り直さない"
  );
  assert_eq!(late.history_view(&id.aid_string()).await.unwrap(), vec![3]);
}

// ---------------------------------------------------------------------------
// 保持の失敗は追記の結果を変えない（BT-13・S-4）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_bigtable_retention_delete_failure_keep_the_append_successful_and_retry_on_the_next_snapshot() {
  let captured = captured_events();
  let (fixture, hooks, store) = setup(options_keeping(1)).await;
  let id = Id::new("Order", "retention-delete-failure");
  store
    .persist_event_and_snapshot(event(&id, 1), snapshot(1))
    .await
    .unwrap();
  Hooks::arm(&hooks.delete_failures);

  let appended = store.persist_event_and_snapshot(event(&id, 2), snapshot(2)).await;

  assert!(appended.is_ok(), "追記を失敗として返さない: {appended:?}");
  assert_eq!(
    captured.categories_of("event_store_adapter::retention", &id.aid()),
    vec!["retention-failure".to_string()],
    "保持の失敗を知らせる"
  );
  let prefix = row_prefix(&id.aid(), None);
  let row_1 = fixture.raw_row(TableKind::Journal, &journal_key(&prefix, 1)).await;
  assert_eq!(
    only_value(&row_1, "history", "state"),
    Some(b"active".to_vec()),
    "削除に失敗したので行 1 は active のまま"
  );
  store
    .persist_event_and_snapshot(event(&id, 3), snapshot(3))
    .await
    .unwrap();
  let row_1 = fixture.raw_row(TableKind::Journal, &journal_key(&prefix, 1)).await;
  assert_eq!(
    only_value(&row_1, "history", "state"),
    Some(b"retired".to_vec()),
    "次のスナップショットの書き込みで片付く"
  );
}

#[tokio::test]
async fn should_bigtable_retention_query_failure_keep_the_append_successful() {
  let (fixture, hooks, store) = setup(options_keeping(1)).await;
  let id = Id::order();
  store
    .persist_event_and_snapshot(event(&id, 1), snapshot(1))
    .await
    .unwrap();
  Hooks::arm(&hooks.query_failures);

  let appended = store.persist_event_and_snapshot(event(&id, 2), snapshot(2)).await;

  assert!(appended.is_ok(), "追記を失敗として返さない: {appended:?}");
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&id, 1).await.unwrap(),
    vec![event(&id, 1), event(&id, 2)]
  );
  let prefix = row_prefix(&id.aid(), None);
  let row_1 = fixture.raw_row(TableKind::Journal, &journal_key(&prefix, 1)).await;
  assert_eq!(only_value(&row_1, "history", "state"), Some(b"active".to_vec()));
}

// ---------------------------------------------------------------------------
// 記録した設定の食い違い（2 章・BT-16・E-1）
// ---------------------------------------------------------------------------

fn record(keep_snapshot_count: Option<u64>, envelope_limit_bytes: u64) -> BigtableConfigurationRecord {
  BigtableConfigurationRecord {
    store_id: RECORDED_STORE_ID.to_string(),
    layout_version: 1,
    keep_snapshot_count,
    retention_mode: "delete".to_string(),
    envelope_limit_bytes,
  }
}

/// 初期化の後、生成の前に、両テーブルの設定行を書き換えて、クライアントの設定で開く。
async fn open_with_seeded_configuration(
  options: BigtableOptions,
  journal: BigtableConfigurationRecord,
  head: BigtableConfigurationRecord,
) -> Result<Store, EventStoreError> {
  let fixture = Fixture::initialized(&options).await;
  overwrite_bigtable_configuration_for_test(&fixture.connection().await, &fixture.tables, &options, &journal, &head)
    .await
    .expect("設定行を書き換えられる");
  fixture.open(options).await
}

#[tokio::test]
async fn should_bigtable_configuration_that_matches_in_both_tables_and_the_client_open() {
  let result = open_with_seeded_configuration(
    options_keeping(1),
    record(Some(1), 1_048_576),
    record(Some(1), 1_048_576),
  )
  .await;

  assert!(result.is_ok(), "{:?}", result.err());
}

#[tokio::test]
async fn should_bigtable_envelope_limit_that_differs_between_the_tables_or_from_the_client_be_a_configuration_error() {
  let cases: [(&str, BigtableConfigurationRecord, BigtableConfigurationRecord); 3] = [
    ("journal だけが違う", record(None, 2_097_152), record(None, 1_048_576)),
    ("head だけが違う", record(None, 1_048_576), record(None, 2_097_152)),
    (
      "両テーブルが一致してもクライアントと違う",
      record(None, 2_097_152),
      record(None, 2_097_152),
    ),
  ];

  for (label, journal, head) in cases {
    let result = open_with_seeded_configuration(BigtableOptions::default(), journal, head).await;

    assert!(
      matches!(result, Err(EventStoreError::Configuration { .. })),
      "{label}: {:?}",
      result.err()
    );
  }
}

#[tokio::test]
async fn should_bigtable_configuration_that_differs_between_the_tables_or_from_the_client_be_a_configuration_error() {
  let other_store_id = BigtableConfigurationRecord {
    store_id: "7a1d4c2e-9b3f-4e5a-8c6d-0f1e2d3c4b5a".to_string(),
    ..record(None, 1_048_576)
  };
  let layout_version_2 = BigtableConfigurationRecord {
    layout_version: 2,
    ..record(None, 1_048_576)
  };
  let ttl_mode = BigtableConfigurationRecord {
    retention_mode: "ttl".to_string(),
    ..record(None, 1_048_576)
  };
  let cases: Vec<(
    &str,
    BigtableOptions,
    BigtableConfigurationRecord,
    BigtableConfigurationRecord,
  )> = vec![
    (
      "store_id が違う",
      BigtableOptions::default(),
      record(None, 1_048_576),
      other_store_id,
    ),
    (
      "保持件数がテーブル間で違う",
      options_keeping(1),
      record(Some(1), 1_048_576),
      record(None, 1_048_576),
    ),
    (
      "記録した保持件数がクライアントと違う",
      options_keeping(1),
      record(Some(2), 1_048_576),
      record(Some(2), 1_048_576),
    ),
    (
      "配置の版が 1 でない",
      BigtableOptions::default(),
      layout_version_2,
      record(None, 1_048_576),
    ),
    (
      "保持方式が delete でない",
      BigtableOptions::default(),
      ttl_mode,
      record(None, 1_048_576),
    ),
  ];

  for (label, options, journal, head) in cases {
    let result = open_with_seeded_configuration(options, journal, head).await;

    assert!(
      matches!(result, Err(EventStoreError::Configuration { .. })),
      "{label}: {:?}",
      result.err()
    );
  }
}
