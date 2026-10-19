#![cfg(feature = "sqlite")]

mod sqlite_support;

use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use event_store_adapter_rs::{
  initialize_sqlite_store, AggregateId, ConfigurationReason, EventEnvelope, EventStore, EventStoreError,
  EventStoreForSqlite, RetentionMode, RetentionSettings, SnapshotEnvelope, SqliteOptions, StorageOperation,
};
use rusqlite::Connection;
use serde_json::{json, Value};
use sqlite_support::TempDatabase;

#[derive(Debug, Clone, PartialEq)]
struct Id(&'static str);
impl AggregateId for Id {
  fn type_name(&self) -> String {
    "Order".into()
  }

  fn value(&self) -> String {
    self.0.into()
  }
}
type Store = EventStoreForSqlite<Id, Value, Value>;

const RECORDED_STORE_ID: &str = "5f0c8b8e-3c1a-4d7e-9a4b-2f6d1c0e7a11";

fn options_keeping(count: usize) -> SqliteOptions {
  SqliteOptions {
    retention: RetentionSettings::keep_latest(count),
    ..SqliteOptions::default()
  }
}

fn event_of(id: &'static str, seq: u64) -> EventEnvelope<Id, Value> {
  EventEnvelope::new(
    Id(id),
    seq,
    DateTime::<Utc>::from_timestamp(1, 123).unwrap(),
    json!({"id": id, "seq": seq}),
  )
  .with_manifest("event")
}

fn event(seq: u64) -> EventEnvelope<Id, Value> {
  event_of("1", seq)
}

fn snapshot_of(id: &'static str, seq: u64) -> SnapshotEnvelope<Value> {
  SnapshotEnvelope::new(json!({"id": id, "seq": seq}), seq).with_manifest("snapshot")
}

fn snapshot(seq: u64) -> SnapshotEnvelope<Value> {
  snapshot_of("1", seq)
}

/// 初期化した新しいファイルを、既定の設定で開いたストアを返す。
fn initialized_store(database: &TempDatabase, options: SqliteOptions) -> Store {
  initialize_sqlite_store(database.path(), &options).expect("初期化できる");
  Store::open(database.path(), options).expect("開ける")
}

/// ストアとは別の接続を、同じファイルに開く。
fn raw_connection(database: &TempDatabase) -> Connection {
  Connection::open(database.path()).expect("別の接続を開ける")
}

fn count_rows(connection: &Connection, table: &str) -> i64 {
  connection
    .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| row.get(0))
    .unwrap_or_else(|error| panic!("{table} の行数を数えられる: {error}"))
}

/// 検査を無効にして、記録済みの config の行を書き換える。
fn rewrite_config(database: &TempDatabase, assignments: &str) {
  raw_connection(database)
    .execute_batch(&format!(
      "PRAGMA ignore_check_constraints = ON; UPDATE config SET {assignments} WHERE singleton = 1;"
    ))
    .expect("config を書き換えられる");
}

fn is_lowercase_uuid(text: &str) -> bool {
  text.len() == 36
    && text.char_indices().all(|(index, character)| match index {
      8 | 13 | 18 | 23 => character == '-',
      _ => character.is_ascii_digit() || ('a'..='f').contains(&character),
    })
}

fn configuration_reason<T: std::fmt::Debug>(result: Result<T, EventStoreError>) -> ConfigurationReason {
  match result {
    Err(EventStoreError::Configuration { reason }) => reason,
    other => panic!("設定エラーになる: {other:?}"),
  }
}

// ---------------------------------------------------------------------------
// 初期化と配置（LAYOUT-CREATE・INIT-SEPARATE）
// ---------------------------------------------------------------------------

#[test]
fn should_sqlite_initialize_create_strict_tables_and_config() {
  let database = TempDatabase::new();
  assert!(!database.path().exists(), "試験の前提: ファイルはまだない");

  initialize_sqlite_store(database.path(), &SqliteOptions::default()).unwrap();

  let connection = raw_connection(&database);
  let mut statement = connection
    .prepare(
      "SELECT name, \"strict\" FROM pragma_table_list WHERE schema = 'main' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )
    .unwrap();
  let tables: Vec<(String, i64)> = statement
    .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
    .unwrap()
    .collect::<Result<_, _>>()
    .unwrap();
  assert_eq!(
    tables,
    vec![
      ("config".to_owned(), 1),
      ("head".to_owned(), 1),
      ("journal".to_owned(), 1),
      ("snapshot_current".to_owned(), 1),
      ("snapshot_history".to_owned(), 1),
    ],
    "5 つのテーブルがすべて STRICT で、head_transition はない"
  );
  assert_eq!(count_rows(&connection, "config"), 1);
  let (store_id, layout_version, keep_snapshot_count, retention_mode, change_feed_enabled): (
    String,
    i64,
    Option<i64>,
    String,
    i64,
  ) = connection
    .query_row(
      "SELECT store_id, layout_version, keep_snapshot_count, retention_mode, change_feed_enabled FROM config WHERE singleton = 1",
      [],
      |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
    )
    .unwrap();
  assert!(is_lowercase_uuid(&store_id), "{store_id}");
  assert_eq!(layout_version, 1);
  assert_eq!(keep_snapshot_count, None);
  assert_eq!(retention_mode, "delete");
  assert_eq!(change_feed_enabled, 0);
}

#[test]
fn should_sqlite_initialize_record_the_requested_keep_snapshot_count() {
  let database = TempDatabase::new();

  initialize_sqlite_store(database.path(), &options_keeping(3)).unwrap();

  let recorded: Option<i64> = raw_connection(&database)
    .query_row("SELECT keep_snapshot_count FROM config", [], |row| row.get(0))
    .unwrap();
  assert_eq!(recorded, Some(3));
}

#[test]
fn should_sqlite_initialize_enforce_seq_nr_range_strict_types_and_not_null_columns() {
  let database = TempDatabase::new();
  initialize_sqlite_store(database.path(), &SqliteOptions::default()).unwrap();
  let connection = raw_connection(&database);
  let insert = |seq_nr: &str, occurred_at_ns: &str, manifest: &str| {
    connection.execute(
      &format!(
        "INSERT INTO journal (aid, seq_nr, occurred_at_ns, manifest, payload) VALUES ('Order-1', {seq_nr}, {occurred_at_ns}, {manifest}, x'7b7d')"
      ),
      [],
    )
  };

  assert!(insert("1", "0", "'event'").is_ok(), "試験の前提: 正しい行は入る");

  assert!(insert("0", "0", "'event'").is_err(), "seq_nr の下限 1 の CHECK");
  assert!(
    insert("9007199254740992", "0", "'event'").is_err(),
    "seq_nr の上限 2^53-1 の CHECK"
  );
  assert!(
    insert("9007199254740991", "0", "'event'").is_ok(),
    "seq_nr の上限ちょうどは入る"
  );
  assert!(
    insert("2", "'text'", "'event'").is_err(),
    "STRICT は INTEGER の列に文字列を入れない"
  );
  assert!(
    insert("3", "1.5", "'event'").is_err(),
    "STRICT は INTEGER の列に損失のある REAL を入れない"
  );
  assert!(
    connection
      .execute(
        "INSERT INTO journal (aid, seq_nr, occurred_at_ns, manifest, payload) VALUES (NULL, 5, 0, 'event', x'7b7d')",
        [],
      )
      .is_err(),
    "キーの列は NOT NULL"
  );
}

#[test]
fn should_sqlite_reject_memory_only_database() {
  let initialized = initialize_sqlite_store(":memory:", &SqliteOptions::default());
  let opened = Store::open(":memory:", SqliteOptions::default());

  assert_eq!(
    configuration_reason(initialized),
    ConfigurationReason::SqliteConnectionSettingMismatch
  );
  assert_eq!(
    configuration_reason(opened),
    ConfigurationReason::SqliteConnectionSettingMismatch
  );
}

#[test]
fn should_sqlite_reject_uri_filenames_instead_of_enabling_shared_cache() {
  let database = TempDatabase::new();
  // 同梱の SQLite は URI の解釈が既定で有効なので、"file:" で始まる名前は、フラグを足さなくても URI として解釈され得る。
  let uri = format!("file:{}?cache=shared", database.path().display());

  let result = initialize_sqlite_store(&uri, &SqliteOptions::default());

  assert!(
    result.is_err(),
    "URI の引数で shared cache を有効にしたストアを開かない: {result:?}"
  );
  assert!(
    !database.path().exists(),
    "名前を URI として解釈して、指す場所のファイルを作っていない"
  );
}

#[test]
fn should_sqlite_initialize_reject_file_with_application_objects() {
  let database = TempDatabase::new();
  raw_connection(&database)
    .execute_batch("CREATE VIEW v_app AS SELECT 1 AS value;")
    .unwrap();

  let result = initialize_sqlite_store(database.path(), &SqliteOptions::default());

  assert_eq!(
    configuration_reason(result),
    ConfigurationReason::SqliteDatabaseNotEmpty
  );
  let connection = raw_connection(&database);
  let tables: i64 = connection
    .query_row("SELECT count(*) FROM sqlite_schema WHERE type = 'table'", [], |row| {
      row.get(0)
    })
    .unwrap();
  assert_eq!(tables, 0, "journal・head・config のテーブルは作られない");
}

#[test]
fn should_sqlite_initialize_reject_an_already_initialized_database_without_changing_it() {
  let database = TempDatabase::new();
  initialize_sqlite_store(database.path(), &options_keeping(2)).unwrap();
  let before: String = raw_connection(&database)
    .query_row("SELECT store_id FROM config", [], |row| row.get(0))
    .unwrap();

  let result = initialize_sqlite_store(database.path(), &SqliteOptions::default());

  assert_eq!(
    configuration_reason(result),
    ConfigurationReason::SqliteDatabaseNotEmpty
  );
  let (after, keep): (String, Option<i64>) = raw_connection(&database)
    .query_row("SELECT store_id, keep_snapshot_count FROM config", [], |row| {
      Ok((row.get(0)?, row.get(1)?))
    })
    .unwrap();
  assert_eq!(after, before, "記録した store_id を作り直さない");
  assert_eq!(keep, Some(2), "記録した保持件数を書き換えない");
}

#[test]
fn should_sqlite_open_not_create_a_missing_file() {
  let database = TempDatabase::new();

  let result = Store::open(database.path(), SqliteOptions::default());

  assert!(result.is_err(), "存在しないストアを開く操作は失敗する: {result:?}");
  assert!(!database.path().exists(), "開く操作はファイルを作らない");
}

#[test]
fn should_sqlite_open_reject_uninitialized_existing_file_without_creating_tables() {
  let database = TempDatabase::new();
  std::fs::File::create(database.path()).unwrap();

  let result = Store::open(database.path(), SqliteOptions::default());

  assert!(
    matches!(result, Err(EventStoreError::Configuration { .. })),
    "テーブルの無い既存のファイルは定義の欠損として設定エラー: {result:?}"
  );
  let tables: i64 = raw_connection(&database)
    .query_row(
      "SELECT count(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
      [],
      |row| row.get(0),
    )
    .unwrap();
  assert_eq!(tables, 0, "開く操作は欠けた定義を補わない");
}

#[test]
fn should_sqlite_layout_reject_a_missing_table() {
  let database = TempDatabase::new();
  initialize_sqlite_store(database.path(), &SqliteOptions::default()).unwrap();
  raw_connection(&database)
    .execute_batch("DROP TABLE snapshot_current;")
    .unwrap();

  let result = Store::open(database.path(), SqliteOptions::default());

  assert_eq!(configuration_reason(result), ConfigurationReason::SqliteLayoutMismatch);
  let restored: i64 = raw_connection(&database)
    .query_row(
      "SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = 'snapshot_current'",
      [],
      |row| row.get(0),
    )
    .unwrap();
  assert_eq!(restored, 0, "開く操作は欠けたテーブルを補わない");
}

#[test]
fn should_sqlite_layout_reject_a_table_that_is_not_strict() {
  let database = TempDatabase::new();
  initialize_sqlite_store(database.path(), &SqliteOptions::default()).unwrap();
  raw_connection(&database)
    .execute_batch(
      "DROP TABLE snapshot_history;
       CREATE TABLE snapshot_history (
         aid TEXT COLLATE BINARY NOT NULL,
         seq_nr INTEGER NOT NULL CHECK (seq_nr BETWEEN 1 AND 9007199254740991),
         manifest TEXT NOT NULL,
         payload BLOB NOT NULL,
         PRIMARY KEY (aid, seq_nr)
       );",
    )
    .unwrap();

  let result = Store::open(database.path(), SqliteOptions::default());

  assert_eq!(configuration_reason(result), ConfigurationReason::SqliteLayoutMismatch);
}

#[test]
fn should_sqlite_layout_reject_a_different_primary_key() {
  let database = TempDatabase::new();
  initialize_sqlite_store(database.path(), &SqliteOptions::default()).unwrap();
  raw_connection(&database)
    .execute_batch(
      "DROP TABLE journal;
       CREATE TABLE journal (
         aid TEXT COLLATE BINARY NOT NULL,
         seq_nr INTEGER NOT NULL CHECK (seq_nr BETWEEN 1 AND 9007199254740991),
         occurred_at_ns INTEGER NOT NULL,
         manifest TEXT NOT NULL,
         payload BLOB NOT NULL,
         PRIMARY KEY (aid)
       ) STRICT;",
    )
    .unwrap();

  let result = Store::open(database.path(), SqliteOptions::default());

  assert_eq!(configuration_reason(result), ConfigurationReason::SqliteLayoutMismatch);
}

#[test]
fn should_sqlite_layout_reject_an_aid_column_whose_collation_is_not_binary() {
  let database = TempDatabase::new();
  initialize_sqlite_store(database.path(), &SqliteOptions::default()).unwrap();
  raw_connection(&database)
    .execute_batch(
      "DROP TABLE journal;
       CREATE TABLE journal (
         aid TEXT COLLATE NOCASE NOT NULL,
         seq_nr INTEGER NOT NULL CHECK (seq_nr BETWEEN 1 AND 9007199254740991),
         occurred_at_ns INTEGER NOT NULL,
         manifest TEXT NOT NULL,
         payload BLOB NOT NULL,
         PRIMARY KEY (aid COLLATE BINARY, seq_nr)
       ) STRICT;",
    )
    .unwrap();

  let result = Store::open(database.path(), SqliteOptions::default());

  assert_eq!(configuration_reason(result), ConfigurationReason::SqliteLayoutMismatch);
}

#[test]
fn should_sqlite_layout_reject_a_primary_key_whose_aid_collation_is_not_binary() {
  let database = TempDatabase::new();
  initialize_sqlite_store(database.path(), &SqliteOptions::default()).unwrap();
  raw_connection(&database)
    .execute_batch(
      "DROP TABLE journal;
       CREATE TABLE journal (
         aid TEXT COLLATE BINARY NOT NULL,
         seq_nr INTEGER NOT NULL CHECK (seq_nr BETWEEN 1 AND 9007199254740991),
         occurred_at_ns INTEGER NOT NULL,
         manifest TEXT NOT NULL,
         payload BLOB NOT NULL,
         PRIMARY KEY (aid COLLATE NOCASE, seq_nr)
       ) STRICT;",
    )
    .unwrap();

  let result = Store::open(database.path(), SqliteOptions::default());

  assert_eq!(configuration_reason(result), ConfigurationReason::SqliteLayoutMismatch);
}

// ---------------------------------------------------------------------------
// 記録した config との照合（CONFIG-MATCH・CONFIG-FEED-RECORDED）
// ---------------------------------------------------------------------------

#[test]
fn should_sqlite_open_accept_a_matching_recorded_configuration() {
  let database = TempDatabase::new();
  initialize_sqlite_store(database.path(), &options_keeping(1)).unwrap();
  rewrite_config(&database, &format!("store_id = '{RECORDED_STORE_ID}'"));

  let result = Store::open(database.path(), options_keeping(1));

  assert!(
    result.is_ok(),
    "小文字の UUID の store_id で、ほかも一致すれば開ける: {result:?}"
  );
}

#[test]
fn should_sqlite_open_reject_recorded_configuration_mismatch_of_keep_snapshot_count() {
  // (記録した保持件数, クライアントの設定)
  let combinations = [
    (Some(2), options_keeping(1)),
    (None, options_keeping(1)),
    (Some(1), SqliteOptions::default()),
  ];
  for (recorded, options) in combinations {
    let database = TempDatabase::new();
    initialize_sqlite_store(database.path(), &SqliteOptions::default()).unwrap();
    let assignment = recorded.map_or("keep_snapshot_count = NULL".to_owned(), |count| {
      format!("keep_snapshot_count = {count}")
    });
    rewrite_config(&database, &assignment);

    let result = Store::open(database.path(), options.clone());

    assert!(
      matches!(result, Err(EventStoreError::Configuration { .. })),
      "記録 {recorded:?}・設定 {:?}: {result:?}",
      options.retention.keep_snapshot_count()
    );
  }
}

#[test]
fn should_sqlite_open_reject_recorded_configuration_mismatch_of_layout_version() {
  let database = TempDatabase::new();
  initialize_sqlite_store(database.path(), &SqliteOptions::default()).unwrap();
  rewrite_config(&database, "layout_version = 2");

  let result = Store::open(database.path(), SqliteOptions::default());

  assert!(
    matches!(result, Err(EventStoreError::Configuration { .. })),
    "{result:?}"
  );
}

#[test]
fn should_sqlite_open_reject_recorded_configuration_mismatch_of_retention_mode() {
  let database = TempDatabase::new();
  initialize_sqlite_store(database.path(), &SqliteOptions::default()).unwrap();
  rewrite_config(&database, "retention_mode = 'ttl'");

  let result = Store::open(database.path(), SqliteOptions::default());

  assert!(
    matches!(result, Err(EventStoreError::Configuration { .. })),
    "{result:?}"
  );
}

#[test]
fn should_sqlite_open_reject_recorded_configuration_mismatch_of_store_id_that_is_not_lowercase_uuid() {
  for store_id in [
    "5F0C8B8E-3C1A-4D7E-9A4B-2F6D1C0E7A11",
    "5f0c8b8e3c1a4d7e9a4b2f6d1c0e7a11",
  ] {
    let database = TempDatabase::new();
    initialize_sqlite_store(database.path(), &SqliteOptions::default()).unwrap();
    rewrite_config(&database, &format!("store_id = '{store_id}'"));

    let result = Store::open(database.path(), SqliteOptions::default());

    assert!(
      matches!(result, Err(EventStoreError::Configuration { .. })),
      "{store_id}: {result:?}"
    );
  }
}

#[test]
fn should_sqlite_open_reject_recorded_configuration_mismatch_of_a_missing_configuration_row() {
  let database = TempDatabase::new();
  initialize_sqlite_store(database.path(), &SqliteOptions::default()).unwrap();
  raw_connection(&database).execute_batch("DELETE FROM config;").unwrap();

  let result = Store::open(database.path(), SqliteOptions::default());

  assert!(
    matches!(result, Err(EventStoreError::Configuration { .. })),
    "{result:?}"
  );
}

#[test]
fn should_sqlite_open_reject_recorded_change_feed_enabled() {
  let database = TempDatabase::new();
  initialize_sqlite_store(database.path(), &SqliteOptions::default()).unwrap();
  rewrite_config(&database, "change_feed_enabled = 1");

  let result = Store::open(database.path(), SqliteOptions::default());

  assert_eq!(
    configuration_reason(result),
    ConfigurationReason::SqliteChangeFeedUnsupported
  );
}

// ---------------------------------------------------------------------------
// 提供しない能力と範囲外の設定（OPTIONS-TTL・OPTIONS-FEED・OPTIONS-RANGE）
// ---------------------------------------------------------------------------

#[test]
fn should_sqlite_reject_ttl_setting() {
  for retention in [
    RetentionSettings::keep_latest(1).with_mode(RetentionMode::Ttl { grace_seconds: 60 }),
    RetentionSettings::current_only().with_mode(RetentionMode::Ttl { grace_seconds: 60 }),
  ] {
    let database = TempDatabase::new();
    let ttl = SqliteOptions {
      retention,
      ..SqliteOptions::default()
    };

    let initialized = initialize_sqlite_store(database.path(), &ttl);
    assert_eq!(
      configuration_reason(initialized),
      ConfigurationReason::SqliteTtlUnsupported
    );
    initialize_sqlite_store(database.path(), &SqliteOptions::default())
      .expect("失敗した初期化の後でも、正しい設定で初期化できる");
    let opened = Store::open(database.path(), ttl);
    assert_eq!(configuration_reason(opened), ConfigurationReason::SqliteTtlUnsupported);
  }
}

#[test]
fn should_sqlite_reject_change_feed_setting() {
  let database = TempDatabase::new();
  let feed = SqliteOptions {
    change_feed: true,
    ..SqliteOptions::default()
  };

  let initialized = initialize_sqlite_store(database.path(), &feed);
  assert_eq!(
    configuration_reason(initialized),
    ConfigurationReason::SqliteChangeFeedUnsupported
  );
  initialize_sqlite_store(database.path(), &SqliteOptions::default())
    .expect("失敗した初期化の後でも、正しい設定で初期化できる");
  let opened = Store::open(database.path(), feed);
  assert_eq!(
    configuration_reason(opened),
    ConfigurationReason::SqliteChangeFeedUnsupported
  );
  let recorded: i64 = raw_connection(&database)
    .query_row("SELECT change_feed_enabled FROM config", [], |row| row.get(0))
    .unwrap();
  assert_eq!(recorded, 0, "変更フィードの有無は 0 に固定して記録する");
}

#[test]
fn should_sqlite_reject_out_of_range_settings_for_a_zero_keep_snapshot_count() {
  let database = TempDatabase::new();

  let initialized = initialize_sqlite_store(database.path(), &options_keeping(0));
  assert_eq!(
    configuration_reason(initialized),
    ConfigurationReason::KeepSnapshotCountZero
  );
  initialize_sqlite_store(database.path(), &SqliteOptions::default())
    .expect("失敗した初期化の後でも、正しい設定で初期化できる");
  let opened = Store::open(database.path(), options_keeping(0));
  assert_eq!(configuration_reason(opened), ConfigurationReason::KeepSnapshotCountZero);
}

#[cfg(target_pointer_width = "64")]
#[test]
fn should_sqlite_reject_out_of_range_settings_for_a_keep_snapshot_count_beyond_signed_64_bits() {
  let database = TempDatabase::new();
  let too_large = options_keeping(usize::MAX);

  let initialized = initialize_sqlite_store(database.path(), &too_large);
  assert_eq!(
    configuration_reason(initialized),
    ConfigurationReason::SqliteKeepSnapshotCountOutOfRange
  );
  initialize_sqlite_store(database.path(), &SqliteOptions::default())
    .expect("失敗した初期化の後でも、正しい設定で初期化できる");
  let opened = Store::open(database.path(), too_large);
  assert_eq!(
    configuration_reason(opened),
    ConfigurationReason::SqliteKeepSnapshotCountOutOfRange
  );
}

#[test]
fn should_sqlite_reject_out_of_range_settings_for_a_lock_wait_beyond_32_bit_milliseconds() {
  let database = TempDatabase::new();
  for lock_wait in [Duration::from_millis(i32::MAX as u64 + 1), Duration::MAX] {
    let out_of_range = SqliteOptions {
      lock_wait,
      ..SqliteOptions::default()
    };

    let initialized = initialize_sqlite_store(database.path(), &out_of_range);
    assert_eq!(
      configuration_reason(initialized),
      ConfigurationReason::SqliteLockWaitOutOfRange,
      "{lock_wait:?}"
    );
  }
  initialize_sqlite_store(database.path(), &SqliteOptions::default())
    .expect("失敗した初期化の後でも、正しい設定で初期化できる");
  let opened = Store::open(
    database.path(),
    SqliteOptions {
      lock_wait: Duration::MAX,
      ..SqliteOptions::default()
    },
  );
  assert_eq!(
    configuration_reason(opened),
    ConfigurationReason::SqliteLockWaitOutOfRange
  );
}

#[test]
fn should_sqlite_accept_a_lock_wait_of_exactly_the_largest_32_bit_milliseconds() {
  let database = TempDatabase::new();
  let options = SqliteOptions {
    lock_wait: Duration::from_millis(i32::MAX as u64),
    ..SqliteOptions::default()
  };

  initialize_sqlite_store(database.path(), &options).unwrap();
  let store = Store::open(database.path(), options);

  assert!(store.is_ok(), "{store:?}");
}

// ---------------------------------------------------------------------------
// 追記（APPEND-LOCK-WAIT・APPEND-KEY-CONFLICT）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_sqlite_lock_wait_timeout_return_storage_error_not_optimistic_lock() {
  let database = TempDatabase::new();
  let store = initialized_store(
    &database,
    SqliteOptions {
      lock_wait: Duration::from_millis(50),
      ..SqliteOptions::default()
    },
  );
  store.persist_event(event(1)).await.unwrap();
  let holder = raw_connection(&database);
  holder.execute_batch("BEGIN IMMEDIATE").unwrap();

  let started = Instant::now();
  let result = store.persist_event(event(2)).await;
  let waited = started.elapsed();

  assert!(
    matches!(
      result,
      Err(EventStoreError::Storage {
        operation: StorageOperation::Append,
        ..
      })
    ),
    "ロック待ちの時間切れは楽観ロックではなく保存先エラー: {result:?}"
  );
  assert!(
    waited >= Duration::from_millis(40),
    "待ち時間 50 ミリ秒の設定まで待ってから失敗する: {waited:?}"
  );

  holder.execute_batch("ROLLBACK").unwrap();
  store
    .persist_event(event(2))
    .await
    .expect("ロックを放した後は、同じストアで同じ追記が成功する");
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&Id("1"), 1).await.unwrap(),
    vec![event(1), event(2)],
    "失敗した追記は何も確定せず、成功した追記は 1 件だけ読める"
  );
}

#[tokio::test]
async fn should_sqlite_concurrent_stores_commit_one_and_reject_others_with_optimistic_lock() {
  let database = TempDatabase::new();
  initialize_sqlite_store(database.path(), &SqliteOptions::default()).unwrap();
  let barrier = Arc::new(Barrier::new(4));
  let threads: Vec<_> = (0..4)
    .map(|_| {
      let store = Store::open(database.path(), SqliteOptions::default()).unwrap();
      let barrier = barrier.clone();
      std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
        barrier.wait();
        runtime.block_on(store.persist_event(event(1)))
      })
    })
    .collect();

  let results: Vec<_> = threads.into_iter().map(|thread| thread.join().unwrap()).collect();

  assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1, "{results:?}");
  assert_eq!(
    results
      .iter()
      .filter(|result| matches!(result, Err(EventStoreError::OptimisticLock { .. })))
      .count(),
    3,
    "{results:?}"
  );
  let reader = Store::open(database.path(), SqliteOptions::default()).unwrap();
  assert_eq!(
    reader.get_events_by_id_since_seq_nr(&Id("1"), 0).await.unwrap(),
    vec![event(1)]
  );
}

#[tokio::test]
async fn should_sqlite_journal_primary_key_conflict_after_the_head_check_return_optimistic_lock() {
  let database = TempDatabase::new();
  let store = initialized_store(&database, SqliteOptions::default());
  store.persist_event(event(1)).await.unwrap();
  // ヘッドは 1 のまま、journal にだけ seq_nr = 2 の行がある状態を作る。ヘッドの照合は通り、journal の挿入で主キーが重複する。
  raw_connection(&database)
    .execute_batch(
      "INSERT INTO journal (aid, seq_nr, occurred_at_ns, manifest, payload)
       SELECT aid, 2, occurred_at_ns, manifest, payload FROM journal WHERE aid = 'Order-1' AND seq_nr = 1;",
    )
    .unwrap();

  let result = store.persist_event(event(2)).await;

  assert!(
    matches!(
      &result,
      Err(EventStoreError::OptimisticLock {
        seq_nr: 2,
        head_seq_nr: Some(1),
        ..
      })
    ),
    "journal の主キーの重複は楽観ロック: {result:?}"
  );
  let connection = raw_connection(&database);
  let head: i64 = connection
    .query_row("SELECT seq_nr FROM head WHERE aid = 'Order-1'", [], |row| row.get(0))
    .unwrap();
  assert_eq!(head, 1, "失敗した追記はヘッドを進めない");
  assert_eq!(
    count_rows(&connection, "journal"),
    2,
    "失敗した追記は journal に何も足さない"
  );
}

#[tokio::test]
async fn should_sqlite_other_constraint_violation_on_append_return_storage_error_not_optimistic_lock() {
  let database = TempDatabase::new();
  let store = initialized_store(&database, SqliteOptions::default());
  store.persist_event(event(1)).await.unwrap();
  // 主キー以外の制約違反（トリガーによる中止）を、journal の挿入で起こす。
  raw_connection(&database)
    .execute_batch("CREATE TRIGGER reject_journal BEFORE INSERT ON journal BEGIN SELECT RAISE(ABORT, 'rejected'); END;")
    .unwrap();

  let result = store.persist_event(event(2)).await;

  assert!(
    matches!(
      &result,
      Err(EventStoreError::Storage {
        operation: StorageOperation::Append,
        ..
      })
    ),
    "主キー以外の制約違反は、楽観ロックにせず保存先エラー: {result:?}"
  );
  raw_connection(&database)
    .execute_batch("DROP TRIGGER reject_journal;")
    .unwrap();
  store
    .persist_event(event(2))
    .await
    .expect("失敗した追記は何も確定せず、原因を除けば同じ追記が成功する");
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&Id("1"), 1).await.unwrap(),
    vec![event(1), event(2)]
  );
}

// ---------------------------------------------------------------------------
// 読み取り（READ-EVENTS-RANGE・AID-EXACT・TIME-INTEGER・API-COMMON）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_sqlite_read_events_return_storage_error_on_journal_gap() {
  let database = TempDatabase::new();
  let store = initialized_store(&database, SqliteOptions::default());
  for seq in 1..=3 {
    store.persist_event(event(seq)).await.unwrap();
  }
  raw_connection(&database)
    .execute_batch("DELETE FROM journal WHERE aid = 'Order-1' AND seq_nr = 2;")
    .unwrap();

  let result = store.get_events_by_id_since_seq_nr(&Id("1"), 0).await;

  assert!(
    matches!(
      result,
      Err(EventStoreError::Storage {
        operation: StorageOperation::LoadEvents,
        ..
      })
    ),
    "取得範囲の欠番は、短い列を成功として返さず保存先エラー: {result:?}"
  );
}

#[tokio::test]
async fn should_sqlite_read_events_not_return_rows_beyond_the_head() {
  let database = TempDatabase::new();
  let store = initialized_store(&database, SqliteOptions::default());
  for seq in 1..=3 {
    store.persist_event(event(seq)).await.unwrap();
  }
  raw_connection(&database)
    .execute_batch(
      "INSERT INTO journal (aid, seq_nr, occurred_at_ns, manifest, payload)
       SELECT aid, 4, occurred_at_ns, manifest, payload FROM journal WHERE aid = 'Order-1' AND seq_nr = 3;",
    )
    .unwrap();

  let events = store.get_events_by_id_since_seq_nr(&Id("1"), 0).await.unwrap();

  assert_eq!(
    events,
    vec![event(1), event(2), event(3)],
    "ヘッドの h より先の行は返さない"
  );
}

#[tokio::test]
async fn should_sqlite_isolate_aggregates_with_like_wildcards_and_return_only_the_exact_value() {
  let database = TempDatabase::new();
  let store = initialized_store(&database, SqliteOptions::default());
  store.persist_event(event_of("9", 1)).await.unwrap();
  store.persist_event(event_of("90", 1)).await.unwrap();

  let events = store.get_events_by_id_since_seq_nr(&Id("9"), 1).await.unwrap();

  assert_eq!(events, vec![event_of("9", 1)], "値 9 の集約のイベントだけが返る");
}

#[tokio::test]
async fn should_sqlite_isolate_aggregates_with_like_wildcards_in_the_requested_value() {
  let database = TempDatabase::new();
  let store = initialized_store(&database, SqliteOptions::default());
  store
    .persist_event_and_snapshot(event_of("9", 1), snapshot_of("9", 1))
    .await
    .unwrap();
  store
    .persist_event_and_snapshot(event_of("90", 1), snapshot_of("90", 1))
    .await
    .unwrap();

  let events = store.get_events_by_id_since_seq_nr(&Id("9%"), 1).await.unwrap();
  let snapshot = store.get_latest_snapshot_by_id(&Id("9_")).await.unwrap();

  assert!(events.is_empty(), "LIKE の % で前方一致しない: {events:?}");
  assert!(snapshot.is_none(), "LIKE の _ で 1 文字一致しない: {snapshot:?}");
}

#[tokio::test]
async fn should_sqlite_store_occurred_at_as_integer_nanoseconds() {
  let database = TempDatabase::new();
  let store = initialized_store(&database, SqliteOptions::default());
  let at = |nanos: i64| DateTime::<Utc>::from_timestamp_nanos(nanos);
  let first = EventEnvelope::new(Id("1"), 1, at(i64::MAX), json!(1)).with_manifest("event");
  let second = EventEnvelope::new(Id("1"), 2, at(i64::MIN), json!(2)).with_manifest("event");
  store.persist_event(first.clone()).await.unwrap();
  store.persist_event(second.clone()).await.unwrap();

  let stored: Vec<(String, i64)> = raw_connection(&database)
    .prepare("SELECT typeof(occurred_at_ns), occurred_at_ns FROM journal ORDER BY seq_nr")
    .unwrap()
    .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
    .unwrap()
    .collect::<Result<_, _>>()
    .unwrap();
  let head_type: String = raw_connection(&database)
    .query_row("SELECT typeof(event_occurred_at_ns) FROM head", [], |row| row.get(0))
    .unwrap();

  assert_eq!(
    stored,
    vec![("integer".to_owned(), i64::MAX), ("integer".to_owned(), i64::MIN)],
    "REAL に丸めず、符号付き 64bit の整数のナノ秒で保存する"
  );
  assert_eq!(head_type, "integer");
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&Id("1"), 1).await.unwrap(),
    vec![first, second],
    "読むときも整数のナノ秒のまま復元する"
  );
}

async fn append_and_read_through_the_common_trait<S>(store: &S)
where
  S: EventStore<AID = Id, A = Value, P = Value>, {
  store.persist_event_and_snapshot(event(1), snapshot(1)).await.unwrap();
  store.persist_event(event(2)).await.unwrap();
  store.persist_event(event(3)).await.unwrap();

  assert_eq!(
    store.get_events_by_id_since_seq_nr(&Id("1"), 2).await.unwrap(),
    vec![event(2), event(3)]
  );
  let read = store.get_latest_snapshot_by_id(&Id("1")).await.unwrap().unwrap();
  assert_eq!(read.head_seq_nr(), 3);
  assert_eq!(read.snapshot(), Some(&snapshot(1)));
}

#[tokio::test]
async fn should_sqlite_round_trip_events_and_snapshot() {
  let database = TempDatabase::new();
  let store = initialized_store(&database, SqliteOptions::default());

  append_and_read_through_the_common_trait(&store).await;

  let reopened = Store::open(database.path(), SqliteOptions::default()).unwrap();
  assert_eq!(
    reopened.get_events_by_id_since_seq_nr(&Id("1"), 1).await.unwrap(),
    vec![event(1), event(2), event(3)],
    "開き直したストアが確定済みの記録を読める"
  );
}
