//! Bigtable の結合試験が共有する補助。
//!
//! - エミュレーターは、試験のプロセスの中で共有する。使っている試験が 1 つもなくなったときにコンテナを削除する。
//! - 試験ごとに新しい journal・head のテーブルの組（プロファイル 2 章の配置）を作る。設定行は作らない。
//! - 行の生の内容は、ライブラリを通さず、データ API で直接読み書きする。期待値の符号化（行キー・封筒）も
//!   ライブラリとは独立に、プロファイルの規則から組み立てる。

#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex, Weak};
use std::thread::JoinHandle;

use chrono::{DateTime, Utc};
use event_store_adapter_rs::{
  AggregateId, BigtableConnection, BigtableOptions, BigtableTables, EventEnvelope, EventStoreError,
  EventStoreForBigtable, SnapshotEnvelope,
};
use event_store_adapter_test_utils_rs::{bigtable, docker};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::bigtable_client::BigtableClient;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::{DeleteFromRow, SetCell};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::read_rows_response::cell_chunk::RowStatus;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_range::{EndKey, StartKey};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
  MutateRowRequest, Mutation, ReadRowsRequest, RowRange, RowSet,
};
use serde_json::{json, Value};

pub const PROJECT: &str = "project";
pub const INSTANCE: &str = "instance";
/// 全ての試験の設定行が持つ store_id の例（小文字の UUID）。
pub const RECORDED_STORE_ID: &str = "5f0c8b8e-3c1a-4d7e-9a4b-2f6d1c0e7a11";

// ---------------------------------------------------------------------------
// エミュレーター（プロセスの中で共有する）
// ---------------------------------------------------------------------------

/// 専用のスレッドと実行基盤でコンテナを持つ。落とすとコンテナを削除する。
struct EmulatorHost {
  port: u16,
  stop: Option<Sender<()>>,
  thread: Option<JoinHandle<()>>,
}

impl EmulatorHost {
  fn start() -> Self {
    let (port_sender, port_receiver) = channel::<u16>();
    let (stop, stop_receiver) = channel::<()>();
    let thread = std::thread::spawn(move || {
      let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("エミュレーター用の実行基盤を作れる");
      runtime.block_on(async move {
        let container = docker::bigtable_emulator().await.expect("エミュレーターを起動できる");
        let port = container
          .get_host_port_ipv4(docker::BIGTABLE_EMULATOR_PORT)
          .await
          .expect("公開ポートを得られる");
        port_sender.send(port).expect("ポートを渡せる");
        // 使う側が全ていなくなる（送信側が落ちる）まで、コンテナを持ち続ける。
        let _ = tokio::task::spawn_blocking(move || stop_receiver.recv()).await;
        drop(container);
      });
    });
    let port = port_receiver.recv().expect("エミュレーターのポートを受け取れる");
    Self {
      port,
      stop: Some(stop),
      thread: Some(thread),
    }
  }
}

impl Drop for EmulatorHost {
  fn drop(&mut self) {
    drop(self.stop.take());
    if let Some(thread) = self.thread.take() {
      let _ = thread.join();
    }
  }
}

static SHARED_EMULATOR: Mutex<Weak<EmulatorHost>> = Mutex::new(Weak::new());

fn shared_emulator() -> Arc<EmulatorHost> {
  let mut slot = SHARED_EMULATOR.lock().expect("エミュレーターの共有のロック");
  if let Some(host) = slot.upgrade() {
    return host;
  }
  let host = Arc::new(EmulatorHost::start());
  *slot = Arc::downgrade(&host);
  host
}

static TABLE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// 試験の間、エミュレーターを保持し、試験専用の journal・head のテーブルの組を持つ。
pub struct Fixture {
  _emulator: Arc<EmulatorHost>,
  env: Env,
}

/// 試験の本体や、別の実行基盤のスレッドへ渡せる、接続先の情報。接続は呼び出した実行基盤の中で作る。
#[derive(Debug, Clone)]
pub struct Env {
  pub port: u16,
  pub tables: BigtableTables,
}

impl Fixture {
  /// 新しい journal・head のテーブルの組を作る。設定行は作らない（初期化の前の状態）。
  pub async fn new() -> Self {
    let emulator = shared_emulator();
    let sequence = TABLE_SEQUENCE.fetch_add(1, Ordering::SeqCst);
    let tables = BigtableTables {
      project_id: PROJECT.to_string(),
      instance_id: INSTANCE.to_string(),
      journal_table_id: format!("journal-{}-{sequence}", std::process::id()),
      head_table_id: format!("head-{}-{sequence}", std::process::id()),
    };
    let env = Env {
      port: emulator.port,
      tables,
    };
    let channel = bigtable::connect(env.port).await.expect("エミュレーターへ接続できる");
    bigtable::create_tables(
      &channel,
      PROJECT,
      INSTANCE,
      &env.tables.journal_table_id,
      &env.tables.head_table_id,
    )
    .await
    .expect("テーブルを作れる");
    Self {
      _emulator: emulator,
      env,
    }
  }

  /// 新しいテーブルの組を作り、`options` で初期化する（設定行を書く）。
  pub async fn initialized(options: &BigtableOptions) -> Self {
    let fixture = Self::new().await;
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

impl Env {
  /// 呼び出した実行基盤の中で新しい接続を作る。データ API と管理 API は同じエミュレーターを指す。
  pub async fn connection(&self) -> BigtableConnection {
    let channel = bigtable::connect(self.port).await.expect("エミュレーターへ接続できる");
    BigtableConnection {
      data: channel.clone(),
      admin: channel,
    }
  }

  pub async fn initialize(&self, options: &BigtableOptions) -> Result<(), EventStoreError> {
    event_store_adapter_rs::initialize_bigtable_store(&self.connection().await, &self.tables, options).await
  }

  pub async fn open(&self, options: BigtableOptions) -> Result<Store, EventStoreError> {
    Store::open(self.connection().await, self.tables.clone(), options).await
  }

  pub fn table_path(&self, table: TableKind) -> String {
    let table_id = match table {
      TableKind::Journal => &self.tables.journal_table_id,
      TableKind::Head => &self.tables.head_table_id,
    };
    format!("projects/{}/instances/{}/tables/{table_id}", PROJECT, INSTANCE)
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

pub type Store = EventStoreForBigtable<Id, Value, Value>;

pub fn occurred_at(seq_nr: u64) -> DateTime<Utc> {
  DateTime::<Utc>::from_timestamp(1_700_000_000 + seq_nr as i64, 123_456_789).expect("時刻を作れる")
}

pub fn event(id: &Id, seq_nr: u64) -> EventEnvelope<Id, Value> {
  EventEnvelope::new(id.clone(), seq_nr, occurred_at(seq_nr), json!({ "number": seq_nr }))
}

pub fn event_with_payload(id: &Id, seq_nr: u64, payload: Value) -> EventEnvelope<Id, Value> {
  EventEnvelope::new(id.clone(), seq_nr, occurred_at(seq_nr), payload)
}

pub fn snapshot(seq_nr: u64) -> SnapshotEnvelope<Value> {
  SnapshotEnvelope::new(json!({ "state": seq_nr }), seq_nr)
}

/// 保持件数 `count` の設定。
pub fn options_keeping(count: usize) -> BigtableOptions {
  BigtableOptions {
    retention: event_store_adapter_rs::RetentionSettings::keep_latest(count),
    ..BigtableOptions::default()
  }
}

// ---------------------------------------------------------------------------
// 期待値の符号化（プロファイル 3 章・4 章から、ライブラリとは独立に組み立てる）
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

/// `H ‖ L16 ‖ A`。`hash` を省略すると aid の FNV-1a 64 を使う。
pub fn row_prefix(aid: &str, hash: Option<u64>) -> Vec<u8> {
  let hash = hash.unwrap_or_else(|| fnv1a64(aid.as_bytes()));
  let mut key = hash.to_be_bytes().to_vec();
  key.extend_from_slice(&(aid.len() as u16).to_be_bytes());
  key.extend_from_slice(aid.as_bytes());
  key
}

/// `P ‖ U64BE(seq_nr)`。
pub fn journal_key(prefix: &[u8], seq_nr: u64) -> Vec<u8> {
  [prefix, &seq_nr.to_be_bytes()].concat()
}

pub fn expected_event_envelope(aid: &str, seq_nr: u64, occurred_at_ns: i64, manifest: &str, payload: &[u8]) -> Vec<u8> {
  let mut bytes = vec![0x01];
  bytes.extend_from_slice(&(aid.len() as u16).to_be_bytes());
  bytes.extend_from_slice(aid.as_bytes());
  bytes.extend_from_slice(&seq_nr.to_be_bytes());
  bytes.extend_from_slice(&occurred_at_ns.to_be_bytes());
  bytes.extend_from_slice(&(manifest.len() as u32).to_be_bytes());
  bytes.extend_from_slice(manifest.as_bytes());
  bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
  bytes.extend_from_slice(payload);
  bytes
}

pub fn expected_snapshot_envelope(seq_nr: u64, manifest: &str, payload: &[u8]) -> Vec<u8> {
  let mut bytes = vec![0x01];
  bytes.extend_from_slice(&seq_nr.to_be_bytes());
  bytes.extend_from_slice(&(manifest.len() as u32).to_be_bytes());
  bytes.extend_from_slice(manifest.as_bytes());
  bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
  bytes.extend_from_slice(payload);
  bytes
}

/// `event(id, seq_nr)` を追記したときに保存される、イベント封筒のバイト列。
pub fn expected_envelope_of(id: &Id, seq_nr: u64) -> Vec<u8> {
  let event = event(id, seq_nr);
  expected_event_envelope(
    &id.aid(),
    seq_nr,
    event.occurred_at().timestamp_nanos_opt().expect("ナノ秒に収まる"),
    "",
    &serde_json::to_vec(event.payload()).expect("payload を直列化できる"),
  )
}

/// `snapshot(seq_nr)` を追記したときに保存される、スナップショット封筒のバイト列。
pub fn expected_snapshot_envelope_of(seq_nr: u64) -> Vec<u8> {
  expected_snapshot_envelope(
    seq_nr,
    "",
    &serde_json::to_vec(snapshot(seq_nr).aggregate()).expect("集約を直列化できる"),
  )
}

/// head の `head:seq_nr` に保存される、20 桁のゼロ詰めの ASCII 数字。
pub fn head_seq_nr_text(seq_nr: u64) -> Vec<u8> {
  format!("{seq_nr:020}").into_bytes()
}

// ---------------------------------------------------------------------------
// 生の行の読み書き（ライブラリを通さない）
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableKind {
  Journal,
  Head,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawCell {
  pub family: String,
  pub qualifier: Vec<u8>,
  pub timestamp_micros: i64,
  pub value: Vec<u8>,
}

/// 行 1 つ分の、全てのセル（全ての版）。
pub type RawRow = Vec<RawCell>;

/// 指定した列の全ての版（新しい順）。
pub fn cells_in<'a>(row: &'a [RawCell], family: &str, qualifier: &str) -> Vec<&'a RawCell> {
  row
    .iter()
    .filter(|cell| cell.family == family && cell.qualifier == qualifier.as_bytes())
    .collect()
}

/// 指定した列がちょうど 1 つのセルを持つときの値。0 個か 2 個以上なら `None`。
pub fn only_value(row: &[RawCell], family: &str, qualifier: &str) -> Option<Vec<u8>> {
  match cells_in(row, family, qualifier).as_slice() {
    [cell] => Some(cell.value.clone()),
    _ => None,
  }
}

fn prefix_successor(prefix: &[u8]) -> Vec<u8> {
  let mut end = prefix.to_vec();
  while let Some(last) = end.pop() {
    if last < 0xff {
      end.push(last + 1);
      return end;
    }
  }
  panic!("接頭辞の後続を作れない");
}

impl Env {
  async fn raw_client(&self) -> BigtableClient<tonic::transport::Channel> {
    let channel = bigtable::connect(self.port).await.expect("エミュレーターへ接続できる");
    BigtableClient::new(channel)
  }

  /// 行キーの範囲 `[start, end)` にある行を、全てのセルの全ての版つきで返す。フィルターは付けない。
  async fn raw_scan(&self, table: TableKind, start: Vec<u8>, end: Vec<u8>) -> Vec<(Vec<u8>, RawRow)> {
    let mut client = self.raw_client().await;
    let mut stream = client
      .read_rows(ReadRowsRequest {
        table_name: self.table_path(table),
        rows: Some(RowSet {
          row_keys: vec![],
          row_ranges: vec![RowRange {
            start_key: Some(StartKey::StartKeyClosed(start)),
            end_key: Some(EndKey::EndKeyOpen(end)),
          }],
        }),
        ..Default::default()
      })
      .await
      .expect("ReadRows を送れる")
      .into_inner();
    let mut rows: Vec<(Vec<u8>, RawRow)> = vec![];
    let mut current: Option<(Vec<u8>, RawRow)> = None;
    let mut family = String::new();
    let mut qualifier: Vec<u8> = vec![];
    let mut pending: Option<RawCell> = None;
    while let Some(response) = stream.message().await.expect("応答を受け取れる") {
      for chunk in response.chunks {
        // エミュレーターは、同じ行の 2 つ目以降のチャンクにも行キーを付ける。行キーが変わったときだけ新しい行にする。
        let is_new_row =
          !chunk.row_key.is_empty() && current.as_ref().is_none_or(|(row_key, _)| *row_key != chunk.row_key);
        if is_new_row {
          current = Some((chunk.row_key.clone(), vec![]));
        }
        if let Some(name) = chunk.family_name.clone() {
          family = name;
        }
        if let Some(name) = chunk.qualifier.clone() {
          qualifier = name;
        }
        match pending.as_mut() {
          Some(cell) => cell.value.extend_from_slice(&chunk.value),
          None => {
            pending = Some(RawCell {
              family: family.clone(),
              qualifier: qualifier.clone(),
              timestamp_micros: chunk.timestamp_micros,
              value: chunk.value.clone(),
            })
          }
        }
        if chunk.value_size == 0 {
          let cell = pending.take().expect("セルを組み立て中");
          current.as_mut().expect("行の途中").1.push(cell);
        }
        match chunk.row_status {
          Some(RowStatus::CommitRow(true)) => rows.push(current.take().expect("行の途中")),
          Some(RowStatus::ResetRow(true)) => {
            current = None;
            pending = None;
          }
          _ => {}
        }
      }
    }
    rows
  }

  /// 1 行の全てのセル。行がなければ空。
  pub async fn raw_row(&self, table: TableKind, key: &[u8]) -> RawRow {
    let mut end = key.to_vec();
    end.push(0x00);
    self
      .raw_scan(table, key.to_vec(), end)
      .await
      .into_iter()
      .next()
      .map(|(_, row)| row)
      .unwrap_or_default()
  }

  /// 接頭辞が一致する全ての行（行キーの順）。
  pub async fn raw_rows_with_prefix(&self, table: TableKind, prefix: &[u8]) -> Vec<(Vec<u8>, RawRow)> {
    self.raw_scan(table, prefix.to_vec(), prefix_successor(prefix)).await
  }

  /// 設定行（行キー `0x00`）の `config:metadata` の値。行がなければ `None`。
  pub async fn raw_configuration(&self, table: TableKind) -> Option<Vec<u8>> {
    let row = self.raw_row(table, &[0x00]).await;
    cells_in(&row, "config", "metadata")
      .first()
      .map(|cell| cell.value.clone())
  }

  /// 1 セルを書く（ライブラリを通さない、試験用の改ざんにも使う）。
  pub async fn raw_put(
    &self,
    table: TableKind,
    key: &[u8],
    family: &str,
    qualifier: &str,
    timestamp_micros: i64,
    value: &[u8],
  ) {
    self
      .raw_client()
      .await
      .mutate_row(MutateRowRequest {
        table_name: self.table_path(table),
        row_key: key.to_vec(),
        mutations: vec![Mutation {
          mutation: Some(
            googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::Mutation::SetCell(SetCell {
              family_name: family.to_string(),
              column_qualifier: qualifier.as_bytes().to_vec(),
              timestamp_micros,
              value: value.to_vec(),
            }),
          ),
        }],
        ..Default::default()
      })
      .await
      .expect("MutateRow を送れる");
  }

  /// 1 行を丸ごと消す（欠番を作る試験用）。
  pub async fn raw_delete_row(&self, table: TableKind, key: &[u8]) {
    self
      .raw_client()
      .await
      .mutate_row(MutateRowRequest {
        table_name: self.table_path(table),
        row_key: key.to_vec(),
        mutations: vec![Mutation {
          mutation: Some(
            googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::Mutation::DeleteFromRow(
              DeleteFromRow {},
            ),
          ),
        }],
        ..Default::default()
      })
      .await
      .expect("MutateRow を送れる");
  }
}
