//! v3 の SQLite の配置のファイルを試験の中で作り、移行の結果を公開 API と生の SQL で確かめる補助。
//!
//! ライブラリの試験（`lib/tests/sqlite_migration_test.rs`）と移行の道具の試験
//! （`migration-cli/tests/sqlite_cli_test.rs`）が `#[path]` で共有する。
//! どちらの試験のクレートも、ここにある関数をすべて使う。

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use event_store_adapter_rs::{
  initialize_sqlite_store, AggregateId, EventSerializer, EventStore, EventStoreError, EventStoreForSqlite,
  RetentionSettings, SnapshotSerializer, SqliteOptions,
};
use rusqlite::types::Value;
use rusqlite::{params, Connection};

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// 破棄時にディレクトリごと消える、試験用のファイルの置き場を表す。ファイルそのものは作らない。
pub struct TempDirectory {
  directory: PathBuf,
}

impl TempDirectory {
  /// プロセスの番号と連番で名前が決まる空のディレクトリを作る。
  pub fn new() -> Self {
    let directory = std::env::temp_dir().join(format!(
      "event-store-adapter-sqlite-migration-{}-{}",
      std::process::id(),
      SEQUENCE.fetch_add(1, Ordering::SeqCst)
    ));
    if directory.exists() {
      fs::remove_dir_all(&directory).expect("前回の一時ディレクトリを消せる");
    }
    fs::create_dir_all(&directory).expect("一時ディレクトリを作れる");
    Self { directory }
  }

  /// 置き場の下の `name` を返す。`name` は `./old.sqlite3` のように書いてよい。
  pub fn file(&self, name: &str) -> PathBuf {
    self.directory.join(name)
  }
}

impl Drop for TempDirectory {
  fn drop(&mut self) {
    // 後片付けの失敗は試験の結果を変えないので、握りつぶす。
    let _ = fs::remove_dir_all(&self.directory);
  }
}

/// タグ `v3.0.6` の `lib/src/event_store_for_sqlite.rs` の `CREATE_SCHEMA_SQL` の写し。
const V3_CREATE_SCHEMA_SQL: &str = "\
CREATE TABLE IF NOT EXISTS journal (
  pkey TEXT NOT NULL,
  skey TEXT NOT NULL,
  aid TEXT NOT NULL,
  seq_nr INTEGER NOT NULL,
  payload BLOB NOT NULL,
  occurred_at INTEGER NOT NULL,
  manifest TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (pkey, skey)
);
CREATE UNIQUE INDEX IF NOT EXISTS journal_aid_seq_nr_idx ON journal (aid, seq_nr);
CREATE TABLE IF NOT EXISTS snapshot (
  pkey TEXT NOT NULL,
  skey TEXT NOT NULL,
  aid TEXT NOT NULL,
  seq_nr INTEGER NOT NULL,
  version INTEGER NOT NULL,
  payload BLOB NOT NULL,
  last_updated_at INTEGER NOT NULL,
  PRIMARY KEY (pkey, skey)
);
CREATE INDEX IF NOT EXISTS snapshot_aid_seq_nr_idx ON snapshot (aid, seq_nr);
";

/// v3.0.6 の `INSERT_JOURNAL_SQL` の写し。
const V3_INSERT_JOURNAL_SQL: &str =
  "INSERT INTO journal (pkey, skey, aid, seq_nr, payload, occurred_at, manifest) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)";

/// v3.0.6 の `INSERT_SNAPSHOT_SQL` の写し。
const V3_INSERT_SNAPSHOT_SQL: &str =
  "INSERT INTO snapshot (pkey, skey, aid, seq_nr, version, payload, last_updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)";

/// v3 の journal の 1 行。`aid` 列は v3 が `Display` で作る利用者の文字列で、型名と値の復元には使えない。
#[derive(Debug, Clone)]
pub struct LegacyEvent {
  pub pkey: String,
  pub skey: String,
  aid_column: String,
  seq_nr: i64,
  payload: Vec<u8>,
  occurred_at_ns: i64,
  manifest: String,
}

impl LegacyEvent {
  /// 既定の発生時刻（-876543211 ナノ秒）、空の manifest、`seq_nr` 1 バイトの payload を持つ行を作る。
  /// `aid` 列は (aid, seq_nr) の一意索引に反しないよう、行ごとに違う値にする。
  pub fn new(pkey: &str, skey: &str, seq_nr: i64) -> Self {
    Self {
      pkey: pkey.into(),
      skey: skey.into(),
      aid_column: format!("caller representation {pkey}/{skey}"),
      seq_nr,
      payload: vec![seq_nr as u8],
      occurred_at_ns: -876543211,
      manifest: String::new(),
    }
  }

  pub fn with_aid_column(mut self, aid_column: &str) -> Self {
    self.aid_column = aid_column.into();
    self
  }

  pub fn with_payload(mut self, payload: &[u8]) -> Self {
    self.payload = payload.to_vec();
    self
  }

  pub fn with_occurred_at_ns(mut self, occurred_at_ns: i64) -> Self {
    self.occurred_at_ns = occurred_at_ns;
    self
  }

  pub fn with_manifest(mut self, manifest: &str) -> Self {
    self.manifest = manifest.into();
    self
  }
}

/// v3 の snapshot の 1 行。`skey` の末尾が 0 の行が今のスナップショットで、それ以外が保持の履歴である。
#[derive(Debug, Clone)]
pub struct LegacySnapshot {
  pkey: String,
  skey: String,
  aid_column: String,
  seq_nr: i64,
  version: i64,
  payload: Vec<u8>,
}

impl LegacySnapshot {
  /// 版 1、`seq_nr` 1 バイトの payload を持つ行を作る。`last_updated_at` は常に -877 にする。
  pub fn new(pkey: &str, skey: &str, seq_nr: i64) -> Self {
    Self {
      pkey: pkey.into(),
      skey: skey.into(),
      aid_column: format!("caller representation {pkey}/{skey}"),
      seq_nr,
      version: 1,
      payload: vec![seq_nr as u8],
    }
  }

  pub fn with_version(mut self, version: i64) -> Self {
    self.version = version;
    self
  }

  pub fn with_payload(mut self, payload: &[u8]) -> Self {
    self.payload = payload.to_vec();
    self
  }
}

/// v3 の配置のファイルを `path` に作り、行を入れる。v3 のクレートには依存しない。
pub fn create_v3_file(path: &Path, events: &[LegacyEvent], snapshots: &[LegacySnapshot]) {
  let connection = Connection::open(path).expect("v3 のファイルを作れる");
  connection
    .execute_batch(V3_CREATE_SCHEMA_SQL)
    .expect("v3 のスキーマを作れる");
  for event in events {
    connection
      .execute(
        V3_INSERT_JOURNAL_SQL,
        params![
          event.pkey,
          event.skey,
          event.aid_column,
          event.seq_nr,
          event.payload,
          event.occurred_at_ns,
          event.manifest
        ],
      )
      .expect("v3 の journal に行を入れられる");
  }
  for snapshot in snapshots {
    connection
      .execute(
        V3_INSERT_SNAPSHOT_SQL,
        params![
          snapshot.pkey,
          snapshot.skey,
          snapshot.aid_column,
          snapshot.seq_nr,
          snapshot.version,
          snapshot.payload,
          -877_i64
        ],
      )
      .expect("v3 の snapshot に行を入れられる");
  }
}

/// 試験ごとの一時ディレクトリと、その下の旧ファイル・新ファイル。
pub struct Files {
  pub old: PathBuf,
  pub new: PathBuf,
  _directory: TempDirectory,
}

/// 旧ファイルを v3 の配置で作り、新ファイルを `initialize_sqlite_store` で初期化して返す。
pub fn prepare_files(events: &[LegacyEvent], snapshots: &[LegacySnapshot], keep: Option<usize>) -> Files {
  let directory = TempDirectory::new();
  let old = directory.file("old.sqlite3");
  let new = directory.file("new.sqlite3");
  create_v3_file(&old, events, snapshots);
  initialize_new(&new, keep);
  Files {
    old,
    new,
    _directory: directory,
  }
}

/// 旧ファイルの中身のバイト列を返す。
pub fn old_bytes(path: &Path) -> Vec<u8> {
  fs::read(path).expect("旧ファイルを読める")
}

/// 新ファイルの保持件数の設定。`None` は今のスナップショットだけを保持する。
fn store_options(keep: Option<usize>) -> SqliteOptions {
  SqliteOptions {
    retention: match keep {
      Some(count) => RetentionSettings::keep_latest(count),
      None => RetentionSettings::current_only(),
    },
    ..SqliteOptions::default()
  }
}

/// 利用者が移行の前に行う操作として、新ファイルを `initialize_sqlite_store` で初期化する。
fn initialize_new(path: &Path, keep: Option<usize>) {
  initialize_sqlite_store(path, &store_options(keep)).expect("新ファイルを初期化できる");
}

/// 新ファイルの 4 つの表の全行。移行が何も書かなかったことの確認に使う。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct NewRows {
  pub journal: Vec<Vec<Value>>,
  pub head: Vec<Vec<Value>>,
  pub snapshot_current: Vec<Vec<Value>>,
  pub snapshot_history: Vec<Vec<Value>>,
}

/// 移行が行を書く、新ファイルの 4 つの表の名前。
pub const NEW_TABLES: [&str; 4] = ["journal", "head", "snapshot_current", "snapshot_history"];

fn table_rows(connection: &Connection, table: &str) -> Vec<Vec<Value>> {
  let mut statement = connection
    .prepare(&format!("SELECT * FROM {table} ORDER BY 1, 2"))
    .expect("表を読める");
  let columns = statement.column_count();
  statement
    .query_map([], |row| (0..columns).map(|index| row.get::<_, Value>(index)).collect())
    .expect("表を読める")
    .collect::<Result<_, _>>()
    .expect("行を読める")
}

/// 新ファイルの 4 つの表の全行を返す。
pub fn new_rows(path: &Path) -> NewRows {
  let connection = Connection::open(path).expect("新ファイルを開ける");
  NewRows {
    journal: table_rows(&connection, "journal"),
    head: table_rows(&connection, "head"),
    snapshot_current: table_rows(&connection, "snapshot_current"),
    snapshot_history: table_rows(&connection, "snapshot_history"),
  }
}

/// 移行が新ファイルに何も書かず、旧ファイルも変えなかったことを確かめる。
pub fn assert_nothing_written(files: &Files, old_before: &[u8]) {
  assert_eq!(new_rows(&files.new), NewRows::default());
  assert_eq!(old_bytes(&files.old), old_before);
}

/// 初期化済みの新ファイルの `table` に、利用者が先に入れた行を 1 つ入れる。
pub fn insert_new_row(path: &Path, table: &str) {
  let sql = match table {
    "journal" => {
      "INSERT INTO journal (aid, seq_nr, occurred_at_ns, manifest, payload) VALUES ('Occupied-a', 1, 0, '', x'00')"
    }
    "head" => {
      "INSERT INTO head (aid, type_name, seq_nr, event_occurred_at_ns, event_manifest, event_payload)
       VALUES ('Occupied-a', 'Occupied', 1, 0, '', x'00')"
    }
    "snapshot_current" => {
      "INSERT INTO snapshot_current (aid, seq_nr, manifest, payload) VALUES ('Occupied-a', 1, '', x'00')"
    }
    "snapshot_history" => {
      "INSERT INTO snapshot_history (aid, seq_nr, manifest, payload) VALUES ('Occupied-a', 1, '', x'00')"
    }
    other => panic!("未対応の表: {other}"),
  };
  Connection::open(path)
    .expect("新ファイルを開ける")
    .execute(sql, [])
    .expect("行を入れられる");
}

/// 公開 API で読み戻すための集約 ID。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Id(pub String, pub String);

impl AggregateId for Id {
  fn type_name(&self) -> String {
    self.0.clone()
  }

  fn value(&self) -> String {
    self.1.clone()
  }
}

/// 保存済みのバイト列をそのまま返す直列化。読み戻しでは直列化しない。
#[derive(Debug)]
struct BytesSerializer;

impl EventSerializer<Vec<u8>> for BytesSerializer {
  fn serialize(&self, _: &Vec<u8>) -> Result<Vec<u8>, EventStoreError> {
    panic!("公開の読み取りの確認で直列化を呼ばない")
  }

  fn deserialize(&self, data: &[u8]) -> Result<Vec<u8>, EventStoreError> {
    Ok(data.to_vec())
  }
}

impl SnapshotSerializer<Vec<u8>> for BytesSerializer {
  fn serialize(&self, _: &Vec<u8>) -> Result<Vec<u8>, EventStoreError> {
    panic!("公開の読み取りの確認で直列化を呼ばない")
  }

  fn deserialize(&self, data: &[u8]) -> Result<Vec<u8>, EventStoreError> {
    Ok(data.to_vec())
  }
}

/// 移行した新ファイルを、v5 の公開 API で開いたストア。
pub type BytesStore = EventStoreForSqlite<Id, Vec<u8>, Vec<u8>>;

/// 初期化のときと同じ保持件数で、新ファイルを公開の `open_with_serializers` で開く。
pub fn open_bytes_store(path: &Path, keep: Option<usize>) -> BytesStore {
  BytesStore::open_with_serializers(
    path,
    store_options(keep),
    Arc::new(BytesSerializer),
    Arc::new(BytesSerializer),
  )
  .expect("移行した新ファイルを開ける")
}

/// 型名 `Old-Account` を `Account` に置き換える対応表。
pub fn mapping() -> HashMap<String, String> {
  HashMap::from([("Old-Account".into(), "Account".into())])
}

/// 1 つのイベントだけを持つ、移行が成功する最小の v3 の行。型名 `Old-Account` は対応表で置き換える。
pub fn minimal_events() -> Vec<LegacyEvent> {
  vec![LegacyEvent::new("Old-Account-7", "Old-Account-a-1", 1)]
}

/// 移行が成功する v3 のデータと、その対応表。
pub struct SuccessFixture {
  pub events: Vec<LegacyEvent>,
  pub snapshots: Vec<LegacySnapshot>,
  pub mapping: HashMap<String, String>,
}

/// 集約 A（型名 `Old-Account`、値 `value-with-hyphen`）と集約 B（型名 `Other`、値 `second`）。
///
/// - A: イベント 1〜3、今のスナップショット（seq_nr 2、版 7）、履歴 seq_nr 1 と 2。
/// - B: イベント 1、今のスナップショット（seq_nr 1）。履歴はない。
pub fn success_fixture() -> SuccessFixture {
  let skey = |number: i64| format!("Old-Account-value-with-hyphen-{number}");
  let event = |seq: i64| LegacyEvent::new("Old-Account-7", &skey(seq), seq);
  let snapshot = |skey_number: i64, seq: i64| LegacySnapshot::new("Old-Account-7", &skey(skey_number), seq);
  SuccessFixture {
    events: vec![
      event(1).with_payload(&[255, 0, 128]),
      event(2)
        .with_payload(&[1, 254])
        .with_occurred_at_ns(i64::MIN)
        .with_manifest("e\u{301}🙂"),
      event(3)
        .with_payload(&[3, 0])
        .with_occurred_at_ns(i64::MAX)
        .with_manifest("last-event"),
      LegacyEvent::new("Other-2", "Other-second-1", 1)
        .with_aid_column("caller representation")
        .with_payload(&[9, 255]),
    ],
    snapshots: vec![
      snapshot(0, 2).with_version(7).with_payload(&[255, 22]),
      snapshot(1, 1).with_version(5).with_payload(&[255, 11]),
      snapshot(2, 2).with_version(7).with_payload(&[255, 22]),
      LegacySnapshot::new("Other-2", "Other-second-0", 1).with_payload(&[128, 255]),
    ],
    mapping: mapping(),
  }
}

/// `success_fixture` を移行した新ファイルを、公開 API で読み戻して旧データと照合する。
///
/// 封筒の列（集約 ID・seq_nr・payload・manifest・発生時刻のナノ秒）、head の seq_nr、最新のスナップショットを確かめる。
pub async fn assert_success_fixture_readable(path: &Path, keep: Option<usize>) {
  let store = open_bytes_store(path, keep);
  let account = Id("Account".into(), "value-with-hyphen".into());
  let events = store.get_events_by_id_since_seq_nr(&account, 0).await.unwrap();
  let expected: [(u64, &[u8], &str, i64); 3] = [
    (1, &[255, 0, 128], "", -876543211),
    (2, &[1, 254], "e\u{301}🙂", i64::MIN),
    (3, &[3, 0], "last-event", i64::MAX),
  ];
  assert_eq!(events.len(), 3);
  for (event, (seq_nr, payload, manifest, nanos)) in events.iter().zip(expected) {
    assert_eq!(event.aggregate_id(), &account);
    assert_eq!(event.seq_nr(), seq_nr);
    assert_eq!(event.payload(), &payload.to_vec());
    assert_eq!(event.manifest(), manifest);
    assert_eq!(event.occurred_at().timestamp_nanos_opt(), Some(nanos));
  }
  let read = store.get_latest_snapshot_by_id(&account).await.unwrap().unwrap();
  assert_eq!(read.head_seq_nr(), 3);
  let snapshot = read.snapshot().unwrap();
  assert_eq!(snapshot.seq_nr(), 2);
  assert_eq!(snapshot.aggregate(), &vec![255, 22]);
  assert_eq!(snapshot.manifest(), "");

  let other = Id("Other".into(), "second".into());
  let events = store.get_events_by_id_since_seq_nr(&other, 0).await.unwrap();
  assert_eq!(events.len(), 1);
  assert_eq!(events[0].seq_nr(), 1);
  assert_eq!(events[0].payload(), &vec![9, 255]);
  assert_eq!(events[0].manifest(), "");
  assert_eq!(events[0].occurred_at().timestamp_nanos_opt(), Some(-876543211));
  let read = store.get_latest_snapshot_by_id(&other).await.unwrap().unwrap();
  assert_eq!(read.head_seq_nr(), 1);
  let snapshot = read.snapshot().unwrap();
  assert_eq!(snapshot.seq_nr(), 1);
  assert_eq!(snapshot.aggregate(), &vec![128, 255]);
}

/// 移行が拒む旧データ 1 つ分。拒否される行の前に、検査を通る集約 `Good` の行を必ず入れる。
pub struct RejectionCase {
  pub name: &'static str,
  pub events: Vec<LegacyEvent>,
  pub snapshots: Vec<LegacySnapshot>,
  pub mapping: HashMap<String, String>,
  /// 拒否の理由に含まれるべき文言。
  pub expected_reason: &'static str,
}

/// 検査を通る集約。これを先に書く実装だと、拒否の場合にも新ファイルへ行が残る。
fn good_aggregate() -> (Vec<LegacyEvent>, Vec<LegacySnapshot>) {
  (
    vec![
      LegacyEvent::new("Good-1", "Good-a-1", 1),
      LegacyEvent::new("Good-1", "Good-a-2", 2),
    ],
    vec![LegacySnapshot::new("Good-1", "Good-a-0", 1)],
  )
}

fn rejection_case(
  name: &'static str,
  expected_reason: &'static str,
  mapping: HashMap<String, String>,
  events: Vec<LegacyEvent>,
  snapshots: Vec<LegacySnapshot>,
) -> RejectionCase {
  let (mut all_events, mut all_snapshots) = good_aggregate();
  all_events.extend(events);
  all_snapshots.extend(snapshots);
  RejectionCase {
    name,
    events: all_events,
    snapshots: all_snapshots,
    mapping,
    expected_reason,
  }
}

/// 既定でない配置の 6 つの変種。どの行も `P-22` で拒む。
pub fn non_default_layout_events() -> Vec<LegacyEvent> {
  vec![
    // pkey にシャード番号がない。
    LegacyEvent::new("Account", "Account-a-1", 1),
    // pkey の末尾が数ではない。
    LegacyEvent::new("Account-x", "Account-a-1", 1),
    // skey の末尾が既定の整数表記ではない。
    LegacyEvent::new("Account-7", "Account-a-01", 1),
    // skey の型名が pkey と一致しない。
    LegacyEvent::new("Account-7", "Wrong-a-1", 1),
    // skey に番号がない。
    LegacyEvent::new("Account-7", "Account", 1),
    // skey の末尾の番号と seq_nr 列が違う。
    LegacyEvent::new("Account-7", "Account-a-2", 1),
  ]
}

/// 旧ファイルの中身を理由にして拒む場合の一覧。どれも新ファイルに何も書いてはならない。
pub fn rejection_cases() -> Vec<RejectionCase> {
  let none = HashMap::new;
  let long_skey = format!("Long-{}-1", "あ".repeat(340));
  vec![
    rejection_case(
      "seq_nr の欠番",
      "P-36",
      none(),
      vec![
        LegacyEvent::new("Gap-2", "Gap-a-1", 1),
        LegacyEvent::new("Gap-2", "Gap-a-3", 3),
      ],
      vec![],
    ),
    rejection_case(
      "対応表のない - の型名",
      "P-23",
      none(),
      vec![LegacyEvent::new("Mapped-Old-2", "Mapped-Old-a-1", 1)],
      vec![],
    ),
    rejection_case(
      "対応後にも - が残る型名",
      "T-11",
      HashMap::from([("Old-Account".into(), "Still-Hyphen".into())]),
      vec![LegacyEvent::new("Old-Account-7", "Old-Account-a-1", 1)],
      vec![],
    ),
    rejection_case(
      "対応後の aid の衝突",
      "複数の旧パーティション",
      mapping(),
      vec![
        LegacyEvent::new("Account-1", "Account-a-1", 1),
        LegacyEvent::new("Old-Account-7", "Old-Account-a-1", 1),
      ],
      vec![],
    ),
    rejection_case(
      "1024 バイトを超える aid",
      "T-12",
      none(),
      vec![LegacyEvent::new("Long-1", &long_skey, 1)],
      vec![],
    ),
    rejection_case(
      "最大の seq_nr を超えるスナップショット",
      "未来snapshot",
      none(),
      (1..=3)
        .map(|seq| LegacyEvent::new("Future-3", &format!("Future-a-{seq}"), seq))
        .collect(),
      vec![LegacySnapshot::new("Future-3", "Future-a-0", 4)],
    ),
    rejection_case(
      "イベントのない集約のスナップショット",
      "孤立snapshot",
      none(),
      vec![],
      vec![LegacySnapshot::new("Orphan-9", "Orphan-a-0", 1)],
    ),
    rejection_case(
      "既定でない配置のキー",
      "P-22",
      none(),
      non_default_layout_events(),
      vec![],
    ),
  ]
}
