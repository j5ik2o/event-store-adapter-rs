//! Spanner の結合試験が共有する補助。
//!
//! - エミュレーターは、試験のプロセスの中で共有する（`event_store_adapter_test_utils_rs::spanner::shared_emulator`）。
//!   使っている試験が 1 つもなくなったときにコンテナを削除する。
//! - 試験ごとに新しいデータベースを作る（エミュレーターはデータベースごとに読み書きトランザクションを直列にする。
//!   別の試験の書き込みが重なって中断されるのを避けるため）。5 テーブルは接頭辞 `esa` から導いた名前で作る。
//! - 行の生の内容は、ライブラリを通さず、生の SQL で直接読み書きする。期待値の符号化（キーのハッシュ・JSON の
//!   payload・ナノ秒）もライブラリとは独立に、プロファイル（`storage/spanner.md` 3 章）の規則から組み立てる。

#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use event_store_adapter_rs::{
  AggregateId, EventEnvelope, EventStoreError, EventStoreForSpanner, SnapshotEnvelope, SpannerConnection,
  SpannerOptions, SpannerRetention, SpannerTables,
};
use event_store_adapter_test_utils_rs::spanner::{self, Cell, EmulatorHost, Param, RawSql, SeedConfiguration};
use serde_json::{json, Value};
use tonic::transport::{Channel, Endpoint};

/// 5 テーブルの名前を導く接頭辞。
pub const PREFIX: &str = "esa";
/// 設定の行が持つ store_id の例（小文字の UUID）。
pub const RECORDED_STORE_ID: &str = "5f0c8b8e-3c1a-4d7e-9a4b-2f6d1c0e7a11";

static DATABASE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// 試験の間、エミュレーターを保持し、試験専用のデータベース（5 テーブル）を持つ。
pub struct Fixture {
  _emulator: Arc<EmulatorHost>,
  env: Env,
}

/// 試験の本体や、別の実行基盤のスレッドへ渡せる、接続先の情報。接続は呼び出した実行基盤の中で作る。
#[derive(Debug, Clone)]
pub struct Env {
  pub port: u16,
  pub database_id: String,
  pub tables: SpannerTables,
}

pub type Store = EventStoreForSpanner<Id, Value, Value>;

fn next_database_id() -> String {
  format!(
    "db{}x{}",
    std::process::id(),
    DATABASE_SEQUENCE.fetch_add(1, Ordering::SeqCst)
  )
}

impl Fixture {
  /// `statements`（DDL）を適用した新しいデータベースを作る。設定の行は作らない（初期化の前の状態）。
  /// `table_prefix` は、`Env::tables` に入れる接頭辞。
  pub async fn with_statements(statements: Vec<String>, table_prefix: &str) -> Self {
    let emulator = spanner::shared_emulator().expect("エミュレーターを起動できる");
    let database_id = next_database_id();
    let channel = spanner::connect(emulator.port())
      .await
      .expect("エミュレーターへ接続できる");
    spanner::create_database(&channel, &database_id, statements)
      .await
      .expect("データベースを作れる");
    let env = Env {
      port: emulator.port(),
      tables: SpannerTables {
        project_id: spanner::PROJECT.to_string(),
        instance_id: spanner::INSTANCE.to_string(),
        database_id: database_id.clone(),
        table_prefix: table_prefix.to_string(),
      },
      database_id,
    };
    Self {
      _emulator: emulator,
      env,
    }
  }

  /// プロファイルどおりの 5 テーブルを持つ新しいデータベース。行削除ポリシーはない。設定の行は作らない。
  pub async fn new() -> Self {
    Self::with_statements(spanner::store_ddl(PREFIX, None), PREFIX).await
  }

  /// snapshot_history に `OLDER_THAN(expired_at, INTERVAL <days> DAY)` の行削除ポリシーを付けた新しいデータベース。
  pub async fn with_policy_days(days: Option<i64>) -> Self {
    Self::with_statements(spanner::store_ddl(PREFIX, days), PREFIX).await
  }

  /// `options` に合うデータベースを作り、`options` で初期化する（設定の行を書く）。
  pub async fn initialized(options: &SpannerOptions) -> Self {
    let fixture = Self::with_policy_days(policy_days_of(options)).await;
    fixture.env.initialize(options).await.expect("初期化できる");
    fixture
  }

  pub fn env(&self) -> Env {
    self.env.clone()
  }
}

impl std::ops::Deref for Fixture {
  type Target = Env;

  fn deref(&self) -> &Env {
    &self.env
  }
}

/// 期限切れ方式の設定から、データベースに付ける行削除ポリシーの日数を導く。
pub fn policy_days_of(options: &SpannerOptions) -> Option<i64> {
  match options.retention {
    SpannerRetention::KeepLatestWithTtl { grace_days, .. } => Some(grace_days as i64),
    _ => None,
  }
}

impl Env {
  pub async fn channel(&self) -> Channel {
    spanner::connect(self.port).await.expect("エミュレーターへ接続できる")
  }

  /// 呼び出した実行基盤の中で新しい接続を作る。
  pub async fn connection(&self) -> SpannerConnection {
    SpannerConnection::new(self.channel().await)
  }

  pub async fn initialize(&self, options: &SpannerOptions) -> Result<(), EventStoreError> {
    event_store_adapter_rs::initialize_spanner_store(&self.connection().await, &self.tables, options).await
  }

  pub async fn open(&self, options: SpannerOptions) -> Result<Store, EventStoreError> {
    Store::open(self.connection().await, self.tables.clone(), options).await
  }

  pub async fn raw(&self) -> RawSql {
    RawSql::new(self.channel().await, &self.database_id)
  }

  fn table(&self, logical: &str) -> String {
    format!("{}_{logical}", self.tables.table_prefix)
  }

  /// 設定の行を、ライブラリを通さず書く（初期化の手順を経ない状態を作る）。
  pub async fn seed_configuration(&self, configuration: &SeedConfiguration) {
    spanner::insert_configuration(&self.raw().await, &self.tables.table_prefix, configuration)
      .await
      .expect("設定の行を書ける");
  }

  /// 生の SQL を 1 つ実行する（試験用の改ざんにも使う）。
  pub async fn raw_execute(&self, sql: &str, params: &[(&str, Param)]) -> i64 {
    self.raw().await.execute(sql, params).await.expect("DML を実行できる")
  }

  /// journal の全ての行を、(aid, seq_nr) の順に返す。
  pub async fn journal_rows(&self) -> Vec<JournalRow> {
    let sql = format!(
      "SELECT hash_prefix, aid, seq_nr, occurred_at_ns, manifest, payload FROM {} ORDER BY aid, seq_nr",
      self.table("journal")
    );
    self
      .rows(&sql)
      .await
      .into_iter()
      .map(|row| JournalRow {
        hash_prefix: row[0].bytes().to_vec(),
        aid: row[1].string().to_string(),
        seq_nr: row[2].int64(),
        occurred_at_ns: row[3].int64(),
        manifest: row[4].string().to_string(),
        payload: row[5].bytes().to_vec(),
      })
      .collect()
  }

  pub async fn head_rows(&self) -> Vec<HeadRow> {
    let sql = format!(
      "SELECT hash_prefix, aid, type_name, seq_nr, event_occurred_at_ns, event_manifest, event_payload FROM {} ORDER BY aid",
      self.table("head")
    );
    self
      .rows(&sql)
      .await
      .into_iter()
      .map(|row| HeadRow {
        hash_prefix: row[0].bytes().to_vec(),
        aid: row[1].string().to_string(),
        type_name: row[2].string().to_string(),
        seq_nr: row[3].int64(),
        event_occurred_at_ns: row[4].int64(),
        event_manifest: row[5].string().to_string(),
        event_payload: row[6].bytes().to_vec(),
      })
      .collect()
  }

  pub async fn current_rows(&self) -> Vec<SnapshotRow> {
    let sql = format!(
      "SELECT hash_prefix, aid, seq_nr, manifest, payload FROM {} ORDER BY aid",
      self.table("snapshot_current")
    );
    self.snapshot_rows(&sql).await
  }

  /// snapshot_history の全ての行を、(aid, seq_nr) の順に返す。`expired_at` は RFC 3339 の文字列。
  pub async fn history_rows(&self) -> Vec<HistoryRow> {
    let sql = format!(
      "SELECT hash_prefix, aid, seq_nr, manifest, payload, expired_at FROM {} ORDER BY aid, seq_nr",
      self.table("snapshot_history")
    );
    self
      .rows(&sql)
      .await
      .into_iter()
      .map(|row| HistoryRow {
        row: SnapshotRow {
          hash_prefix: row[0].bytes().to_vec(),
          aid: row[1].string().to_string(),
          seq_nr: row[2].int64(),
          manifest: row[3].string().to_string(),
          payload: row[4].bytes().to_vec(),
        },
        expired_at: match &row[5] {
          Cell::Null => None,
          Cell::Timestamp(text) => Some(text.clone()),
          other => panic!("expired_at は TIMESTAMP のはず: {other:?}"),
        },
      })
      .collect()
  }

  /// config の行（0 行か 1 行）。
  pub async fn config_rows(&self) -> Vec<ConfigRow> {
    let sql = format!(
      "SELECT singleton, store_id, layout_version, keep_snapshot_count, retention_mode, ttl_grace_days, change_feed_enabled FROM {}",
      self.table("config")
    );
    self
      .rows(&sql)
      .await
      .into_iter()
      .map(|row| ConfigRow {
        singleton: row[0].int64(),
        store_id: row[1].string().to_string(),
        layout_version: row[2].int64(),
        keep_snapshot_count: (!row[3].is_null()).then(|| row[3].int64()),
        retention_mode: row[4].string().to_string(),
        ttl_grace_days: (!row[5].is_null()).then(|| row[5].int64()),
        change_feed_enabled: matches!(row[6], Cell::Bool(true)),
      })
      .collect()
  }

  /// 5 テーブルの全ての行の数。(journal, head, current, history, config)。
  pub async fn row_counts(&self) -> (usize, usize, usize, usize, usize) {
    (
      self.journal_rows().await.len(),
      self.head_rows().await.len(),
      self.current_rows().await.len(),
      self.history_rows().await.len(),
      self.config_rows().await.len(),
    )
  }

  async fn snapshot_rows(&self, sql: &str) -> Vec<SnapshotRow> {
    self
      .rows(sql)
      .await
      .into_iter()
      .map(|row| SnapshotRow {
        hash_prefix: row[0].bytes().to_vec(),
        aid: row[1].string().to_string(),
        seq_nr: row[2].int64(),
        manifest: row[3].string().to_string(),
        payload: row[4].bytes().to_vec(),
      })
      .collect()
  }

  async fn rows(&self, sql: &str) -> Vec<Vec<Cell>> {
    self.raw().await.query(sql, &[]).await.expect("行を読める")
  }

  /// JavaScript 版が追記で実行する SQL（`spanner-persist-event.ts`）と同じ文を、1 つのトランザクションで実行する。
  /// ライブラリを通さずに、JavaScript 版が書いた行を作る。`snapshot` は `(seq_nr, manifest, payload)`。
  /// `with_history` が真なら snapshot_history へも `expired_at = NULL` で入れる。
  pub async fn write_like_javascript(&self, event: &JavascriptEvent<'_>, with_history: bool) {
    let aid = event.aid.to_string();
    let hash_prefix = Param::Bytes(hash_prefix(&aid));
    let key = |extra: Vec<(&'static str, Param)>| {
      let mut params = vec![("hp", hash_prefix.clone()), ("aid", Param::String(aid.clone()))];
      params.extend(extra);
      params
    };
    let event_params = key(vec![
      ("seq", Param::Int64(event.seq_nr)),
      ("manifest", Param::String(event.manifest.to_string())),
      ("payload", Param::Bytes(event.payload.to_vec())),
      ("nanos", Param::Int64(event.nanos)),
      ("type_name", Param::String(event.type_name.to_string())),
    ]);
    let journal = format!(
      "INSERT INTO {} (hash_prefix, aid, seq_nr, occurred_at_ns, manifest, payload) VALUES (@hp, @aid, @seq, @nanos, @manifest, @payload)",
      self.table("journal")
    );
    let head = if event.seq_nr == 1 {
      format!(
        "INSERT INTO {} (hash_prefix, aid, type_name, seq_nr, event_occurred_at_ns, event_manifest, event_payload) VALUES (@hp, @aid, @type_name, @seq, @nanos, @manifest, @payload)",
        self.table("head")
      )
    } else {
      format!(
        "UPDATE {} SET seq_nr = @seq, event_occurred_at_ns = @nanos, event_manifest = @manifest, event_payload = @payload WHERE hash_prefix = @hp AND aid = @aid",
        self.table("head")
      )
    };
    let mut statements: Vec<(String, Vec<(&str, Param)>)> = vec![(journal, event_params.clone()), (head, event_params)];
    if let Some((seq_nr, manifest, payload)) = &event.snapshot {
      let snapshot_params = key(vec![
        ("seq", Param::Int64(*seq_nr)),
        ("manifest", Param::String(manifest.to_string())),
        ("payload", Param::Bytes(payload.to_vec())),
      ]);
      statements.push((
        format!(
          "INSERT OR UPDATE {} (hash_prefix, aid, seq_nr, manifest, payload) VALUES (@hp, @aid, @seq, @manifest, @payload)",
          self.table("snapshot_current")
        ),
        snapshot_params.clone(),
      ));
      if with_history {
        statements.push((
          format!(
            "INSERT INTO {} (hash_prefix, aid, seq_nr, manifest, payload, expired_at) VALUES (@hp, @aid, @seq, @manifest, @payload, NULL)",
            self.table("snapshot_history")
          ),
          snapshot_params,
        ));
      }
    }
    let borrowed: Vec<(&str, &[(&str, Param)])> = statements
      .iter()
      .map(|(sql, params)| (sql.as_str(), params.as_slice()))
      .collect();
    self
      .raw()
      .await
      .execute_all(&borrowed)
      .await
      .expect("JavaScript 版と同じ SQL を実行できる");
  }
}

/// JavaScript 版の追記が書く 1 件分の値。
pub struct JavascriptEvent<'a> {
  pub type_name: &'a str,
  pub aid: &'a str,
  pub seq_nr: i64,
  pub nanos: i64,
  pub manifest: &'a str,
  pub payload: &'a [u8],
  pub snapshot: Option<(i64, &'a str, &'a [u8])>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JournalRow {
  pub hash_prefix: Vec<u8>,
  pub aid: String,
  pub seq_nr: i64,
  pub occurred_at_ns: i64,
  pub manifest: String,
  pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HeadRow {
  pub hash_prefix: Vec<u8>,
  pub aid: String,
  pub type_name: String,
  pub seq_nr: i64,
  pub event_occurred_at_ns: i64,
  pub event_manifest: String,
  pub event_payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotRow {
  pub hash_prefix: Vec<u8>,
  pub aid: String,
  pub seq_nr: i64,
  pub manifest: String,
  pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HistoryRow {
  pub row: SnapshotRow,
  pub expired_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigRow {
  pub singleton: i64,
  pub store_id: String,
  pub layout_version: i64,
  pub keep_snapshot_count: Option<i64>,
  pub retention_mode: String,
  pub ttl_grace_days: Option<i64>,
  pub change_feed_enabled: bool,
}

/// 到達できない接続先（閉じたポート）を指す、接続を張らない `Channel`。設定の検査が通信の前に終わることを確かめるのに使う。
pub fn unreachable_connection() -> SpannerConnection {
  SpannerConnection::new(Endpoint::from_static("http://127.0.0.1:1").connect_lazy())
}

/// 到達できない接続先を指す表で、5 テーブルを接頭辞 `prefix` から導く。
pub fn tables_with_prefix(prefix: &str) -> SpannerTables {
  SpannerTables {
    project_id: spanner::PROJECT.to_string(),
    instance_id: spanner::INSTANCE.to_string(),
    database_id: "unreachable".to_string(),
    table_prefix: prefix.to_string(),
  }
}

/// 別の実行基盤を持つスレッドで `work` を実行して結果を返す。同期のフックの中から、別の書き手を動かすのに使う。
pub fn run_on_another_runtime<T: Send + 'static, Fut: std::future::Future<Output = T>>(
  work: impl FnOnce() -> Fut + Send + 'static,
) -> T {
  std::thread::spawn(move || {
    let runtime = tokio::runtime::Builder::new_current_thread()
      .enable_all()
      .build()
      .expect("別の実行基盤を作れる");
    runtime.block_on(work())
  })
  .join()
  .expect("別の実行基盤のスレッドが終了する")
}

// ---------------------------------------------------------------------------
// 集約・イベント・スナップショット
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct Id {
  type_name: String,
  value: String,
}

impl Id {
  pub fn new(type_name: &str, value: &str) -> Self {
    Self {
      type_name: type_name.to_string(),
      value: value.to_string(),
    }
  }

  /// 型名 `Order`・値 `1` の集約。aid は `Order-1`。
  pub fn order() -> Self {
    Self::new("Order", "1")
  }

  /// ライブラリが組み立てる aid 文字列（`型名-値`）。
  pub fn aid(&self) -> String {
    format!("{}-{}", self.type_name, self.value)
  }

  pub fn aid_string(&self) -> event_store_adapter_rs::AidString {
    event_store_adapter_rs::AidString::from_aggregate_id(self).expect("aid を組み立てられる")
  }
}

impl AggregateId for Id {
  fn type_name(&self) -> String {
    self.type_name.clone()
  }

  fn value(&self) -> String {
    self.value.clone()
  }
}

pub fn occurred_at(seq_nr: u64) -> DateTime<Utc> {
  DateTime::<Utc>::from_timestamp(1_700_000_000 + seq_nr as i64, 123_456_789).expect("時刻を作れる")
}

pub fn event(id: &Id, seq_nr: u64) -> EventEnvelope<Id, Value> {
  EventEnvelope::new(id.clone(), seq_nr, occurred_at(seq_nr), json!({ "number": seq_nr }))
}

pub fn event_with_payload(id: &Id, seq_nr: u64, payload: Value) -> EventEnvelope<Id, Value> {
  EventEnvelope::new(id.clone(), seq_nr, occurred_at(seq_nr), payload)
}

pub fn event_at(id: &Id, seq_nr: u64, occurred_at: DateTime<Utc>) -> EventEnvelope<Id, Value> {
  EventEnvelope::new(id.clone(), seq_nr, occurred_at, json!({ "number": seq_nr }))
}

pub fn snapshot(seq_nr: u64) -> SnapshotEnvelope<Value> {
  SnapshotEnvelope::new(json!({ "state": seq_nr }), seq_nr)
}

/// 即時削除方式で、保持件数 `count` の設定。
pub fn options_keeping(count: usize) -> SpannerOptions {
  SpannerOptions {
    retention: SpannerRetention::KeepLatest { count },
    ..SpannerOptions::default()
  }
}

/// 期限切れ方式で、保持件数 `count`・猶予 `grace_days` 日の設定。
pub fn ttl_options(count: usize, grace_days: u64) -> SpannerOptions {
  SpannerOptions {
    retention: SpannerRetention::KeepLatestWithTtl { count, grace_days },
    ..SpannerOptions::default()
  }
}

// ---------------------------------------------------------------------------
// 期待値の符号化（プロファイル 3 章から、ライブラリとは独立に組み立てる）
// ---------------------------------------------------------------------------

/// FNV-1a 64（hash.md の K-1）。
pub fn fnv1a64(input: &[u8]) -> u64 {
  let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
  for byte in input {
    hash ^= u64::from(*byte);
    hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
  }
  hash
}

/// `H(aid)`: FNV-1a 64 の 8 バイトのビッグエンディアン（SPN-3 の表）。
pub fn hash_prefix(aid: &str) -> Vec<u8> {
  fnv1a64(aid.as_bytes()).to_be_bytes().to_vec()
}

/// 直列化した payload（JSON のバイト列）。
pub fn payload_bytes(value: &Value) -> Vec<u8> {
  serde_json::to_vec(value).expect("payload を直列化できる")
}

/// 時刻の、Unix エポックからの符号付き 64 ビットのナノ秒。
pub fn nanos_of(time: DateTime<Utc>) -> i64 {
  time.timestamp_nanos_opt().expect("ナノ秒に収まる")
}
