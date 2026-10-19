#![cfg(all(feature = "sqlite", feature = "test-hooks"))]

mod sqlite_support;

use std::sync::{
  atomic::{AtomicBool, AtomicUsize, Ordering},
  Arc, Mutex,
};

use chrono::{DateTime, Utc};
use event_store_adapter_rs::sqlite::{
  overwrite_sqlite_configuration_for_test, SqliteConfigurationRecord, SqliteTestHooks,
};
use event_store_adapter_rs::{
  initialize_sqlite_store, AggregateId, AidString, EventEnvelope, EventStore, EventStoreError, EventStoreForSqlite,
  JsonEventSerializer, JsonSnapshotSerializer, RetentionSettings, SeqNr, SnapshotEnvelope, SqliteOptions,
  StorageOperation,
};
use rusqlite::Connection;
use sqlite_support::TempDatabase;

#[derive(Debug, Clone, PartialEq)]
struct Id;
impl AggregateId for Id {
  fn type_name(&self) -> String {
    "Account".into()
  }

  fn value(&self) -> String {
    "1".into()
  }
}
type Store = EventStoreForSqlite<Id, u64, u64>;

const RECORDED_STORE_ID: &str = "5f0c8b8e-3c1a-4d7e-9a4b-2f6d1c0e7a11";

fn event(seq: u64) -> EventEnvelope<Id, u64> {
  EventEnvelope::new(Id, seq, DateTime::<Utc>::from_timestamp(0, 0).unwrap(), seq)
}

fn aid() -> AidString {
  AidString::from_aggregate_id(&Id).unwrap()
}

fn failure(operation: StorageOperation) -> EventStoreError {
  EventStoreError::Storage {
    operation,
    source: Box::new(std::io::Error::other("INJECTED")),
  }
}

/// 障害を差し込み、各フックの呼び出しを数える。読み取りのフックの中では、別の接続のストアに追記させられる。
#[derive(Debug, Default)]
struct Hooks {
  commit: AtomicBool,
  events: AtomicBool,
  snapshot: AtomicBool,
  query: AtomicBool,
  delete: AtomicBool,
  commit_calls: AtomicUsize,
  query_calls: AtomicUsize,
  delete_calls: AtomicUsize,
  /// 次の読み取りのフックの中で、1 度だけ追記する別の接続のストアと、その seq_nr。
  concurrent_writer: Mutex<Option<(Store, SeqNr)>>,
}

impl Hooks {
  fn write_from_another_connection(&self) {
    let writer = self.concurrent_writer.lock().unwrap().take();
    if let Some((store, seq)) = writer {
      std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
        runtime.block_on(store.persist_event_and_snapshot(event(seq), SnapshotEnvelope::new(seq, seq)))
      })
      .join()
      .unwrap()
      .expect("別の接続の追記は、読み取りトランザクションに妨げられず確定する");
    }
  }
}

impl SqliteTestHooks for Hooks {
  fn before_commit(&self, _: &AidString, _: SeqNr) -> Result<(), EventStoreError> {
    self.commit_calls.fetch_add(1, Ordering::SeqCst);
    if self.commit.load(Ordering::SeqCst) {
      Err(failure(StorageOperation::Append))
    } else {
      Ok(())
    }
  }

  fn read_events(&self, _: &AidString) -> Result<(), EventStoreError> {
    self.write_from_another_connection();
    if self.events.load(Ordering::SeqCst) {
      Err(failure(StorageOperation::LoadEvents))
    } else {
      Ok(())
    }
  }

  fn read_snapshot(&self, _: &AidString) -> Result<(), EventStoreError> {
    self.write_from_another_connection();
    if self.snapshot.load(Ordering::SeqCst) {
      Err(failure(StorageOperation::LoadSnapshot))
    } else {
      Ok(())
    }
  }

  fn retention_visible_history(
    &self,
    _: &AidString,
    history: &[SeqNr],
    _: Option<SeqNr>,
  ) -> Result<Vec<SeqNr>, EventStoreError> {
    self.query_calls.fetch_add(1, Ordering::SeqCst);
    if self.query.load(Ordering::SeqCst) {
      Err(failure(StorageOperation::Append))
    } else {
      Ok(history.to_vec())
    }
  }

  fn retention_delete(&self, _: &AidString, _: &[SeqNr]) -> Result<(), EventStoreError> {
    self.delete_calls.fetch_add(1, Ordering::SeqCst);
    if self.delete.load(Ordering::SeqCst) {
      Err(failure(StorageOperation::Append))
    } else {
      Ok(())
    }
  }
}

fn options_keeping(count: usize) -> SqliteOptions {
  SqliteOptions {
    retention: RetentionSettings::keep_latest(count),
    ..SqliteOptions::default()
  }
}

/// 初期化した新しいファイルを、フック付きのストアとして開く。
fn setup(database: &TempDatabase, options: SqliteOptions) -> (Arc<Hooks>, Store) {
  initialize_sqlite_store(database.path(), &options).unwrap();
  let hooks = Arc::new(Hooks::default());
  let store = open_with_hooks(database, options, hooks.clone());
  (hooks, store)
}

fn open_with_hooks(database: &TempDatabase, options: SqliteOptions, hooks: Arc<Hooks>) -> Store {
  Store::open_with_hooks(
    database.path(),
    options,
    Arc::new(JsonEventSerializer::new()),
    Arc::new(JsonSnapshotSerializer::new()),
    hooks,
  )
  .unwrap()
}

fn count_rows(connection: &Connection, table: &str) -> i64 {
  connection
    .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| row.get(0))
    .unwrap_or_else(|error| panic!("{table} の行数を数えられる: {error}"))
}

fn row_counts(database: &TempDatabase) -> Vec<(&'static str, i64)> {
  let connection = Connection::open(database.path()).unwrap();
  ["journal", "head", "snapshot_current", "snapshot_history"]
    .into_iter()
    .map(|table| (table, count_rows(&connection, table)))
    .collect()
}

// ---------------------------------------------------------------------------
// 確定の前の失敗（APPEND-ROLLBACK）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_sqlite_failure_before_commit_leave_no_journal_head_or_snapshot() {
  let database = TempDatabase::new();
  let (hooks, store) = setup(&database, options_keeping(1));
  hooks.commit.store(true, Ordering::SeqCst);

  let result = store
    .persist_event_and_snapshot(event(1), SnapshotEnvelope::new(1, 1))
    .await;

  assert!(
    matches!(
      result,
      Err(EventStoreError::Storage {
        operation: StorageOperation::Append,
        ..
      })
    ),
    "{result:?}"
  );
  assert_eq!(
    hooks.commit_calls.load(Ordering::SeqCst),
    1,
    "確定の前のフックまで進んだ"
  );
  assert_eq!(
    row_counts(&database),
    vec![
      ("journal", 0),
      ("head", 0),
      ("snapshot_current", 0),
      ("snapshot_history", 0)
    ],
    "別の接続から、どの記録も見えない"
  );
  assert!(store.get_latest_snapshot_by_id(&Id).await.unwrap().is_none());
  assert!(store.get_events_by_id_since_seq_nr(&Id, 0).await.unwrap().is_empty());

  hooks.commit.store(false, Ordering::SeqCst);
  store
    .persist_event_and_snapshot(event(1), SnapshotEnvelope::new(1, 1))
    .await
    .expect("失敗した追記の後、別の seq_nr を振り直さず、同じ seq_nr 1 で確定できる");
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&Id, 0).await.unwrap(),
    vec![event(1)]
  );
}

#[tokio::test]
async fn should_sqlite_failure_before_commit_leave_no_journal_head_or_snapshot_after_existing_commit() {
  let database = TempDatabase::new();
  let (hooks, store) = setup(&database, options_keeping(5));
  store
    .persist_event_and_snapshot(event(1), SnapshotEnvelope::new(1, 1))
    .await
    .unwrap();
  let previous = store.get_latest_snapshot_by_id(&Id).await.unwrap();
  let history = store.history_view(&aid()).unwrap();
  let counts = row_counts(&database);
  hooks.commit.store(true, Ordering::SeqCst);
  let query_calls = hooks.query_calls.load(Ordering::SeqCst);
  let delete_calls = hooks.delete_calls.load(Ordering::SeqCst);

  let result = store
    .persist_event_and_snapshot(event(2), SnapshotEnvelope::new(2, 2))
    .await;

  assert!(
    matches!(
      result,
      Err(EventStoreError::Storage {
        operation: StorageOperation::Append,
        ..
      })
    ),
    "{result:?}"
  );
  assert_eq!(store.get_latest_snapshot_by_id(&Id).await.unwrap(), previous);
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&Id, 0).await.unwrap(),
    vec![event(1)]
  );
  assert_eq!(store.history_view(&aid()).unwrap(), history);
  assert_eq!(row_counts(&database), counts);
  assert_eq!(
    hooks.query_calls.load(Ordering::SeqCst),
    query_calls,
    "失敗した追記は保持処理を始めない"
  );
  assert_eq!(hooks.delete_calls.load(Ordering::SeqCst), delete_calls);
}

// ---------------------------------------------------------------------------
// 同じ読み取りトランザクションでの読み取り（READ-ONE-POINT）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_sqlite_reads_keep_one_read_point_while_another_connection_writes_snapshot() {
  let database = TempDatabase::new();
  let (hooks, reader) = setup(&database, SqliteOptions::default());
  let writer = Store::open(database.path(), SqliteOptions::default()).unwrap();
  writer
    .persist_event_and_snapshot(event(1), SnapshotEnvelope::new(1, 1))
    .await
    .unwrap();
  let in_hook_writer = Store::open(database.path(), SqliteOptions::default()).unwrap();
  *hooks.concurrent_writer.lock().unwrap() = Some((in_hook_writer, 2));

  let during = reader.get_latest_snapshot_by_id(&Id).await.unwrap().unwrap();

  assert_eq!(during.head_seq_nr(), 1, "読み取りの時点のヘッド");
  assert_eq!(
    during.snapshot().unwrap().seq_nr(),
    1,
    "ヘッドと同じ時点のスナップショット"
  );
  assert_eq!(during.snapshot().unwrap().aggregate(), &1);
  let after = reader.get_latest_snapshot_by_id(&Id).await.unwrap().unwrap();
  assert_eq!(
    after.head_seq_nr(),
    2,
    "別の接続の追記は確定していて、次の読み取りには見える"
  );
  assert_eq!(after.snapshot().unwrap().seq_nr(), 2);
}

#[tokio::test]
async fn should_sqlite_reads_keep_one_read_point_while_another_connection_writes_events() {
  let database = TempDatabase::new();
  let (hooks, reader) = setup(&database, SqliteOptions::default());
  let writer = Store::open(database.path(), SqliteOptions::default()).unwrap();
  writer
    .persist_event_and_snapshot(event(1), SnapshotEnvelope::new(1, 1))
    .await
    .unwrap();
  let in_hook_writer = Store::open(database.path(), SqliteOptions::default()).unwrap();
  *hooks.concurrent_writer.lock().unwrap() = Some((in_hook_writer, 2));

  let during = reader.get_events_by_id_since_seq_nr(&Id, 1).await.unwrap();

  assert_eq!(during, vec![event(1)], "読み取りの時点のヘッドまでのイベントだけを返す");
  assert_eq!(
    reader.get_events_by_id_since_seq_nr(&Id, 1).await.unwrap(),
    vec![event(1), event(2)],
    "別の接続の追記は確定していて、次の読み取りには見える"
  );
}

// ---------------------------------------------------------------------------
// 確定の後の保持（RETENTION-AFTER-COMMIT・RETENTION-FAILURE）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_sqlite_retention_keep_the_newest_history_and_leave_current_head_and_journal_after_commit() {
  let database = TempDatabase::new();
  let (hooks, store) = setup(&database, options_keeping(2));

  for seq in 1..=4 {
    store
      .persist_event_and_snapshot(event(seq), SnapshotEnvelope::new(seq, seq))
      .await
      .unwrap();
  }

  assert_eq!(store.history_view(&aid()).unwrap(), vec![3, 4], "新しい 2 件だけを残す");
  assert_eq!(
    hooks.query_calls.load(Ordering::SeqCst),
    4,
    "スナップショットを伴う追記ごとに確定の後で履歴を調べる"
  );
  assert_eq!(
    row_counts(&database),
    vec![
      ("journal", 4),
      ("head", 1),
      ("snapshot_current", 1),
      ("snapshot_history", 2)
    ],
    "journal・head・current は消さない"
  );
  let read = store.get_latest_snapshot_by_id(&Id).await.unwrap().unwrap();
  assert_eq!(read.head_seq_nr(), 4);
  assert_eq!(read.snapshot().unwrap().aggregate(), &4);
}

#[tokio::test]
async fn should_sqlite_retention_query_failure_keep_append_success_and_retry_on_the_next_snapshot_append() {
  retry_retention_failure(true).await;
}

#[tokio::test]
async fn should_sqlite_retention_delete_failure_keep_append_success_and_retry_on_the_next_snapshot_append() {
  retry_retention_failure(false).await;
}

async fn retry_retention_failure(query: bool) {
  let database = TempDatabase::new();
  let (hooks, store) = setup(&database, options_keeping(1));
  store
    .persist_event_and_snapshot(event(1), SnapshotEnvelope::new(1, 1))
    .await
    .unwrap();
  assert_eq!(store.history_view(&aid()).unwrap(), vec![1]);
  let fault = if query { &hooks.query } else { &hooks.delete };
  fault.store(true, Ordering::SeqCst);

  store
    .persist_event_and_snapshot(event(2), SnapshotEnvelope::new(2, 2))
    .await
    .expect("保持の失敗は追記の成功を変えない");

  assert_eq!(
    store.history_view(&aid()).unwrap(),
    vec![1, 2],
    "古い履歴が消えずに残る"
  );
  assert_eq!(hooks.delete_calls.load(Ordering::SeqCst), usize::from(!query));
  let read = store.get_latest_snapshot_by_id(&Id).await.unwrap().unwrap();
  assert_eq!(read.head_seq_nr(), 2);
  assert_eq!(read.snapshot().unwrap().aggregate(), &2);
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&Id, 0).await.unwrap(),
    vec![event(1), event(2)]
  );

  fault.store(false, Ordering::SeqCst);
  store
    .persist_event_and_snapshot(event(3), SnapshotEnvelope::new(3, 3))
    .await
    .unwrap();

  assert_eq!(
    store.history_view(&aid()).unwrap(),
    vec![3],
    "次のスナップショットを伴う追記で整理し直す"
  );
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&Id, 0).await.unwrap(),
    vec![event(1), event(2), event(3)]
  );
}

// ---------------------------------------------------------------------------
// 試験用の config の書き換え（RUNNER-SEED が使う経路）
// ---------------------------------------------------------------------------

#[test]
fn should_sqlite_overwrite_configuration_make_open_fail_with_configuration_error() {
  let database = TempDatabase::new();
  let options = options_keeping(1);
  initialize_sqlite_store(database.path(), &options).unwrap();
  let record = SqliteConfigurationRecord {
    store_id: RECORDED_STORE_ID.to_owned(),
    layout_version: 1,
    keep_snapshot_count: Some(2),
    retention_mode: "delete".to_owned(),
    change_feed_enabled: 0,
  };

  overwrite_sqlite_configuration_for_test(database.path(), &options, &record).unwrap();

  let result = Store::open(database.path(), options);
  assert!(
    matches!(result, Err(EventStoreError::Configuration { .. })),
    "記録した保持件数 2 とクライアントの 1 は食い違う: {result:?}"
  );
}

#[test]
fn should_sqlite_overwrite_configuration_record_the_given_values_that_a_matching_open_accepts() {
  let database = TempDatabase::new();
  let options = options_keeping(1);
  initialize_sqlite_store(database.path(), &options).unwrap();
  let record = SqliteConfigurationRecord {
    store_id: RECORDED_STORE_ID.to_owned(),
    layout_version: 1,
    keep_snapshot_count: Some(1),
    retention_mode: "delete".to_owned(),
    change_feed_enabled: 0,
  };

  overwrite_sqlite_configuration_for_test(database.path(), &options, &record).unwrap();

  let recorded: (String, i64, Option<i64>, String, i64) = Connection::open(database.path())
    .unwrap()
    .query_row(
      "SELECT store_id, layout_version, keep_snapshot_count, retention_mode, change_feed_enabled FROM config",
      [],
      |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
    )
    .unwrap();
  assert_eq!(
    recorded,
    (RECORDED_STORE_ID.to_owned(), 1, Some(1), "delete".to_owned(), 0)
  );
  let result = Store::open(database.path(), options);
  assert!(
    result.is_ok(),
    "書き換えた値がクライアントと一致すれば開ける: {result:?}"
  );
}
