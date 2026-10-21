//! v3.0.6 の Bigtable の配置の 2 テーブルを試験の中で作り、移行の結果を公開 API と生の行で確かめる補助。
//!
//! ライブラリの試験（`lib/tests/bigtable_migration_test.rs`）と移行の道具の試験
//! （`migration-cli/tests/bigtable_cli_test.rs`）が `#[path]` で共有する。
//! どちらの試験のクレートも、ここにある関数をすべて使う。片方だけが使う補助は、その試験ファイルに置く。
//! 呼び出し側は、根に `mod bigtable_support;`（`lib/tests/bigtable_support/mod.rs`）を置く。
//!
//! v3 の行は、タグ `v3.0.6` の `event_store_for_bigtable.rs` と `key_resolver.rs` に合わせて直接書く。
//! v3 のクレートには依存しない。
//!
//! - journal の行キー: `{パーティションキー}#{型名}#{値}#{seq_nr を 20 桁にゼロ詰め}`。列ファミリ `event` に
//!   `payload`・`aggregate_id`（値だけ）・`seq_nr`（10 進）・`occurred_at`（`to_rfc3339_opts(Nanos, true)`）・`manifest`。
//! - snapshot の今の行キー: `{パーティションキー}#{型名}#{値}`。列ファミリ `snapshot` に
//!   `payload`・`version`・`seq_nr`・`last_updated_at`（ミリ秒の 10 進）。
//! - snapshot の履歴の行キー: 今の行キーの後ろに `#{seq_nr を 20 桁にゼロ詰め}`。
//! - どのセルも、時刻は `-1`（サーバーの時刻）で書く。

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use chrono::{DateTime, SecondsFormat, Utc};
use event_store_adapter_rs::{
  BigtableOptions, EventSerializer, EventStore, EventStoreError, EventStoreForBigtable, LegacyBigtableTables,
  MigrationReport, SnapshotSerializer,
};
use event_store_adapter_test_utils_rs::bigtable::{self, FamilySpec, GarbageCollection};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::bigtable_client::BigtableClient;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::{Mutation as MutationKind, SetCell};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::read_rows_response::cell_chunk::RowStatus;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{MutateRowRequest, Mutation, ReadRowsRequest};
use tonic::transport::Channel;

use crate::bigtable_support::{occurred_at, Env, Fixture, Id, RawCell, RawRow, TableKind, INSTANCE, PROJECT};

// ---------------------------------------------------------------------------
// v3 の行
// ---------------------------------------------------------------------------

/// 時刻を、v3.0.6 の `format_occurred_at`（`to_rfc3339_opts(SecondsFormat::Nanos, true)`）と同じ形の文字列にする。
pub fn rfc3339_of_nanos(nanos: i64) -> String {
  DateTime::<Utc>::from_timestamp_nanos(nanos).to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn default_nanos(seq_nr: u64) -> i64 {
  occurred_at(seq_nr).timestamp_nanos_opt().expect("ナノ秒に収まる")
}

/// v3 の journal の 1 行。フィールドは行キーと 5 つのセルの値で、試験が構造体の更新記法で差し替える。
#[derive(Debug, Clone)]
pub struct V3Event {
  pub row_key: Vec<u8>,
  pub aggregate_id: Vec<u8>,
  pub seq_nr: Vec<u8>,
  pub occurred_at: Vec<u8>,
  pub manifest: Vec<u8>,
  pub payload: Vec<u8>,
}

impl V3Event {
  /// 既定の配置の行。`aggregate_id` のセルは値だけ、`seq_nr` のセルは 10 進。
  /// 発生時刻は秒より細かい桁を持つ値、manifest は空、payload は `seq_nr` 1 バイトにする。
  pub fn new(pkey: &str, type_name: &str, value: &str, seq_nr: u64) -> Self {
    Self {
      row_key: format!("{pkey}#{type_name}#{value}#{seq_nr:020}").into_bytes(),
      aggregate_id: value.as_bytes().to_vec(),
      seq_nr: seq_nr.to_string().into_bytes(),
      occurred_at: rfc3339_of_nanos(default_nanos(seq_nr)).into_bytes(),
      manifest: Vec::new(),
      payload: vec![seq_nr as u8],
    }
  }
}

/// v3 の snapshot の 1 行（今の行か履歴の行）。`version` と `last_updated_at` は移行しないので、`seq_nr` と
/// 取り違えない値にしてある。
#[derive(Debug, Clone)]
pub struct V3Snapshot {
  pub row_key: Vec<u8>,
  pub payload: Vec<u8>,
  pub version: Vec<u8>,
  pub seq_nr: Vec<u8>,
  pub last_updated_at: Vec<u8>,
}

impl V3Snapshot {
  fn at(row_key: String, seq_nr: u64) -> Self {
    Self {
      row_key: row_key.into_bytes(),
      payload: vec![255, seq_nr as u8],
      version: (seq_nr + 100).to_string().into_bytes(),
      seq_nr: seq_nr.to_string().into_bytes(),
      last_updated_at: occurred_at(seq_nr).timestamp_millis().to_string().into_bytes(),
    }
  }

  /// 今のスナップショットの行（行キーは `{パーティションキー}#{型名}#{値}`）。`seq_nr` はその位置。
  pub fn current(pkey: &str, type_name: &str, value: &str, seq_nr: u64) -> Self {
    Self::at(format!("{pkey}#{type_name}#{value}"), seq_nr)
  }

  /// 履歴の行（行キーは今の行キーの後ろに `#{seq_nr を 20 桁}`）。置き換えられる前の像を、その位置で書いたもの。
  pub fn history(pkey: &str, type_name: &str, value: &str, seq_nr: u64) -> Self {
    Self::at(format!("{pkey}#{type_name}#{value}#{seq_nr:020}"), seq_nr)
  }
}

fn table_path(legacy: &LegacyBigtableTables, table_id: &str) -> String {
  format!(
    "projects/{}/instances/{}/tables/{table_id}",
    legacy.project_id, legacy.instance_id
  )
}

/// 1 行に複数のセルを書く（時刻は v3 と同じ `-1`）。
///
/// 接続は呼び出し側が持ち、行ごとに作り直さない。1000 行を超える旧データを書く試験が、行ごとに TCP の接続を作ると、
/// 使える一時的なポートを使い切るためである。
async fn put_row(channel: &Channel, table_name: &str, row_key: &[u8], cells: &[(&str, &str, &[u8])]) {
  let mutations = cells
    .iter()
    .map(|(family, qualifier, value)| Mutation {
      mutation: Some(MutationKind::SetCell(SetCell {
        family_name: (*family).to_string(),
        column_qualifier: qualifier.as_bytes().to_vec(),
        timestamp_micros: -1,
        value: value.to_vec(),
      })),
    })
    .collect();
  BigtableClient::new(channel.clone())
    .mutate_row(MutateRowRequest {
      table_name: table_name.to_string(),
      row_key: row_key.to_vec(),
      mutations,
      ..Default::default()
    })
    .await
    .expect("MutateRow を送れる");
}

static LEGACY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// v3 の 2 テーブル（journal は列ファミリ `event`、snapshot は列ファミリ `snapshot`）を `instance` に作る。
/// v3 のテーブルには GC の規則がない。名前は呼び出しごとに新しくする。
pub async fn create_legacy_tables(port: u16, instance: &str) -> LegacyBigtableTables {
  let sequence = LEGACY_SEQUENCE.fetch_add(1, Ordering::SeqCst);
  let legacy = LegacyBigtableTables {
    project_id: PROJECT.to_string(),
    instance_id: instance.to_string(),
    journal_table_id: format!("v3-journal-{}-{sequence}", std::process::id()),
    snapshot_table_id: format!("v3-snapshot-{}-{sequence}", std::process::id()),
  };
  create_legacy_tables_named(port, &legacy).await;
  legacy
}

/// `legacy` が指す v3 の 2 テーブルを作る。テーブルの名前を指定したい試験（新しいテーブルと同じ名前を別のインスタンスに
/// 置く試験）が使う。
pub async fn create_legacy_tables_named(port: u16, legacy: &LegacyBigtableTables) {
  let channel = bigtable::connect(port).await.expect("エミュレーターへ接続できる");
  let family = |name: &'static str| FamilySpec {
    name,
    garbage_collection: GarbageCollection::None,
  };
  bigtable::create_table(
    &channel,
    &legacy.project_id,
    &legacy.instance_id,
    &legacy.journal_table_id,
    &[family("event")],
  )
  .await
  .expect("v3 の journal のテーブルを作れる");
  bigtable::create_table(
    &channel,
    &legacy.project_id,
    &legacy.instance_id,
    &legacy.snapshot_table_id,
    &[family("snapshot")],
  )
  .await
  .expect("v3 の snapshot のテーブルを作れる");
}

/// v3 の行を、v3.0.6 の形（列ファミリ・列名）で書く。
pub async fn write_legacy_rows(port: u16, legacy: &LegacyBigtableTables, events: &[V3Event], snapshots: &[V3Snapshot]) {
  let channel = bigtable::connect(port).await.expect("エミュレーターへ接続できる");
  let journal = table_path(legacy, &legacy.journal_table_id);
  for event in events {
    put_row(
      &channel,
      &journal,
      &event.row_key,
      &[
        ("event", "payload", &event.payload),
        ("event", "aggregate_id", &event.aggregate_id),
        ("event", "seq_nr", &event.seq_nr),
        ("event", "occurred_at", &event.occurred_at),
        ("event", "manifest", &event.manifest),
      ],
    )
    .await;
  }
  let snapshot_table = table_path(legacy, &legacy.snapshot_table_id);
  for snapshot in snapshots {
    put_row(
      &channel,
      &snapshot_table,
      &snapshot.row_key,
      &[
        ("snapshot", "payload", &snapshot.payload),
        ("snapshot", "version", &snapshot.version),
        ("snapshot", "seq_nr", &snapshot.seq_nr),
        ("snapshot", "last_updated_at", &snapshot.last_updated_at),
      ],
    )
    .await;
  }
}

// ---------------------------------------------------------------------------
// テーブルの生の内容（ライブラリを通さない）
// ---------------------------------------------------------------------------

/// テーブル全体の生の内容。行キーの順に、各行の全てのセル（全ての版・時刻つき）を持つ。
pub type Dump = Vec<(Vec<u8>, RawRow)>;

/// `table_name` の全ての行を、フィルターなし・終了キーなしで読む。
async fn dump(port: u16, table_name: &str) -> Dump {
  let channel = bigtable::connect(port).await.expect("エミュレーターへ接続できる");
  let mut stream = BigtableClient::new(channel)
    .read_rows(ReadRowsRequest {
      table_name: table_name.to_string(),
      ..Default::default()
    })
    .await
    .expect("ReadRows を送れる")
    .into_inner();
  let mut rows: Dump = vec![];
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

/// 旧の 2 テーブルと新しい 2 テーブルの、ある時点の生の内容。
#[derive(Debug, Clone, PartialEq)]
pub struct State {
  pub old_journal: Dump,
  pub old_snapshot: Dump,
  pub new_journal: Dump,
  pub new_head: Dump,
}

/// 試験 1 つ分の舞台。初期化済み（または試験が選んだ状態）の新しい 2 テーブルと、v3 の行を入れた旧の 2 テーブルを持つ。
pub struct Scene {
  _base: Fixture,
  pub env: Env,
  pub legacy: LegacyBigtableTables,
  /// 移行の関数に渡す新しいストアの設定。新しいテーブルの初期化に使った設定と同じとは限らない。
  pub options: BigtableOptions,
}

impl Scene {
  /// 新しい 2 テーブルを `options` で初期化し、旧の 2 テーブルを作って v3 の行を入れる。
  pub async fn new(events: &[V3Event], snapshots: &[V3Snapshot], options: BigtableOptions) -> Self {
    let base = Fixture::initialized(&options).await;
    Self::over(base, options, events, snapshots).await
  }

  /// 試験が用意した新しい 2 テーブル（`base`）の上に、旧の 2 テーブルを作って v3 の行を入れる。
  pub async fn over(base: Fixture, options: BigtableOptions, events: &[V3Event], snapshots: &[V3Snapshot]) -> Self {
    let env = base.env();
    Self::over_env(base, env, options, events, snapshots).await
  }

  /// `env` が指す新しい 2 テーブル（`base` が保持するエミュレーターの上にある）の上に、旧の 2 テーブルを作る。
  pub async fn over_env(
    base: Fixture,
    env: Env,
    options: BigtableOptions,
    events: &[V3Event],
    snapshots: &[V3Snapshot],
  ) -> Self {
    let legacy = create_legacy_tables(env.port, INSTANCE).await;
    write_legacy_rows(env.port, &legacy, events, snapshots).await;
    Self {
      _base: base,
      env,
      legacy,
      options,
    }
  }

  /// 旧の 2 テーブルと新しい 2 テーブルの全ての行・全てのセル・全ての版・セルの時刻を読む。
  pub async fn state(&self) -> State {
    let port = self.env.port;
    State {
      old_journal: dump(port, &table_path(&self.legacy, &self.legacy.journal_table_id)).await,
      old_snapshot: dump(port, &table_path(&self.legacy, &self.legacy.snapshot_table_id)).await,
      new_journal: dump(port, &self.env.table_path(TableKind::Journal)).await,
      new_head: dump(port, &self.env.table_path(TableKind::Head)).await,
    }
  }
}

/// 旧の 2 テーブルの行・セル・版・時刻が、`before` から変わっていないことを確かめる。
pub fn assert_old_tables_unchanged(before: &State, after: &State) {
  assert_eq!(
    after.old_journal, before.old_journal,
    "旧 journal のテーブルは行・セル・版・時刻とも変わらない"
  );
  assert_eq!(
    after.old_snapshot, before.old_snapshot,
    "旧 snapshot のテーブルは行・セル・版・時刻とも変わらない"
  );
}

/// 新しい 2 テーブルが、設定行（行キー `0x00`）だけを持つことを確かめる。
pub fn assert_only_configuration_rows(state: &State) {
  for (name, rows) in [("journal", &state.new_journal), ("head", &state.new_head)] {
    let keys: Vec<&[u8]> = rows.iter().map(|(key, _)| key.as_slice()).collect();
    assert_eq!(keys, vec![[0x00_u8].as_slice()], "新しい {name} は設定行だけを持つ");
  }
}

/// 移行が何も書かず、旧の 2 テーブルも変えなかったことを確かめる。新しい 2 テーブルは設定行だけである。
pub async fn assert_nothing_written(scene: &Scene, before: &State) {
  let after = scene.state().await;
  assert_eq!(&after, before, "旧 2 テーブルも新 2 テーブルも変わらない");
  assert_only_configuration_rows(&after);
}

/// 拒否の報告の共通の確認: 件数が (0, 0, 0) で、何も書かれていない。
pub async fn assert_rejected_and_nothing_written(scene: &Scene, before: &State, report: &MigrationReport) {
  assert_eq!(
    (report.aggregates, report.events, report.snapshots),
    (0, 0, 0),
    "{report:?}"
  );
  assert_nothing_written(scene, before).await;
}

/// `fragments` のそれぞれが、拒否の理由のどれかに含まれることを確かめる。
pub fn assert_reasons_contain(report: &MigrationReport, fragments: &[&str], context: &str) {
  for fragment in fragments {
    assert!(
      report
        .reasons
        .iter()
        .any(|rejection| rejection.reason.contains(fragment)),
      "{context}: 理由に {fragment} がない: {report:?}"
    );
  }
}

// ---------------------------------------------------------------------------
// 公開 API で読み戻す
// ---------------------------------------------------------------------------

/// 保存済みのバイト列をそのまま返す直列化。移行は直列化をやり直さないので、読み戻しでも変換しない。
#[derive(Debug)]
struct BytesSerializer;

impl EventSerializer<Vec<u8>> for BytesSerializer {
  fn serialize(&self, payload: &Vec<u8>) -> Result<Vec<u8>, EventStoreError> {
    Ok(payload.clone())
  }

  fn deserialize(&self, data: &[u8]) -> Result<Vec<u8>, EventStoreError> {
    Ok(data.to_vec())
  }
}

impl SnapshotSerializer<Vec<u8>> for BytesSerializer {
  fn serialize(&self, aggregate: &Vec<u8>) -> Result<Vec<u8>, EventStoreError> {
    Ok(aggregate.clone())
  }

  fn deserialize(&self, data: &[u8]) -> Result<Vec<u8>, EventStoreError> {
    Ok(data.to_vec())
  }
}

/// 移行した新しい 2 テーブルを、v5 の公開 API で開いたストア。payload はバイト列のまま扱う。
pub type BytesStore = EventStoreForBigtable<Id, Vec<u8>, Vec<u8>>;

/// 移行した新しい 2 テーブルを、`scene.options` で公開の `open_with_serializers` から開く。
pub async fn open_bytes_store(scene: &Scene) -> BytesStore {
  BytesStore::open_with_serializers(
    scene.env.connection().await,
    scene.env.tables.clone(),
    scene.options.clone(),
    Arc::new(BytesSerializer),
    Arc::new(BytesSerializer),
  )
  .await
  .expect("移行した新しいテーブルを公開の open_with_serializers で開ける")
}

// ---------------------------------------------------------------------------
// 移行が成功する旧データ
// ---------------------------------------------------------------------------

/// 旧型名 `Old-Account` を `Account` に置き換える対応表。
pub fn mapping() -> HashMap<String, String> {
  HashMap::from([("Old-Account".into(), "Account".into())])
}

/// 移行後に読めるはずのイベント 1 件。
#[derive(Debug, Clone)]
pub struct ExpectedEvent {
  pub seq_nr: u64,
  pub payload: Vec<u8>,
  pub manifest: &'static str,
  pub occurred_at_ns: i64,
}

/// 移行後に読めるはずのスナップショット 1 件（manifest は常に空）。
#[derive(Debug, Clone)]
pub struct ExpectedSnapshot {
  pub seq_nr: u64,
  pub payload: Vec<u8>,
}

/// 旧データ 1 集約分と、移行後に公開 API で読めるはずの内容。
#[derive(Debug, Clone)]
pub struct ExpectedAggregate {
  pub old_pkey: &'static str,
  pub old_type: &'static str,
  pub new_type: &'static str,
  pub value: &'static str,
  pub events: Vec<ExpectedEvent>,
  /// 今のスナップショット。
  pub current: Option<ExpectedSnapshot>,
  /// 旧の履歴の行（今の行は含まない）。
  pub history: Vec<ExpectedSnapshot>,
}

impl ExpectedAggregate {
  pub fn id(&self) -> Id {
    Id::new(self.new_type, self.value)
  }

  pub fn head_seq_nr(&self) -> u64 {
    self.events.last().expect("イベントがある").seq_nr
  }
}

fn expected_event(seq_nr: u64, payload: &[u8], manifest: &'static str, occurred_at_ns: i64) -> ExpectedEvent {
  ExpectedEvent {
    seq_nr,
    payload: payload.to_vec(),
    manifest,
    occurred_at_ns,
  }
}

fn expected_snapshot(seq_nr: u64, payload: &[u8]) -> ExpectedSnapshot {
  ExpectedSnapshot {
    seq_nr,
    payload: payload.to_vec(),
  }
}

/// 移行が成功する 3 つの集約。
///
/// - A（旧型名 `Old-Account`、値 `a-b`、移行後は `Account-a-b`）: イベント 1〜4（manifest と発生時刻のナノ秒が
///   それぞれ違い、時刻は符号付き 64 ビットの両端を含む）、今のスナップショット（位置 3）、履歴の行（位置 1 と 2）。
///   今のスナップショットはイベント 4 より古い。
/// - B（型名 `Other`、値 `second`）: イベント 1 と、その位置の今のスナップショット。履歴の行はない。
/// - C（型名 `Plain`、値 `third`）: イベント 1 と 2 だけ。スナップショットはない。
pub fn success_aggregates() -> Vec<ExpectedAggregate> {
  vec![
    ExpectedAggregate {
      old_pkey: "Old-Account-3",
      old_type: "Old-Account",
      new_type: "Account",
      value: "a-b",
      events: vec![
        expected_event(1, &[255, 0, 128], "", -876_543_211),
        expected_event(2, &[1, 254], "e\u{301}🙂", i64::MIN),
        expected_event(3, &[3, 0], "m3", i64::MAX),
        expected_event(4, &[4], "head-event", 1_700_000_004_123_456_789),
      ],
      current: Some(expected_snapshot(3, &[255, 33])),
      history: vec![expected_snapshot(1, &[255, 11]), expected_snapshot(2, &[255, 22])],
    },
    ExpectedAggregate {
      old_pkey: "Other-2",
      old_type: "Other",
      new_type: "Other",
      value: "second",
      events: vec![expected_event(1, &[9, 255], "", 1_700_000_001_123_456_789)],
      current: Some(expected_snapshot(1, &[128, 255])),
      history: vec![],
    },
    ExpectedAggregate {
      old_pkey: "Plain-0",
      old_type: "Plain",
      new_type: "Plain",
      value: "third",
      events: vec![
        expected_event(1, &[7], "x", 1_700_000_001_000_000_001),
        expected_event(2, &[8, 8], "", 1_700_000_002_999_999_999),
      ],
      current: None,
      history: vec![],
    },
  ]
}

/// 移行が成功する旧データと、その対応表。
pub struct SuccessFixture {
  pub events: Vec<V3Event>,
  pub snapshots: Vec<V3Snapshot>,
  pub mapping: HashMap<String, String>,
}

/// `success_aggregates` を v3 の行にしたもの。
pub fn success_fixture() -> SuccessFixture {
  let mut events = vec![];
  let mut snapshots = vec![];
  for aggregate in success_aggregates() {
    for event in &aggregate.events {
      events.push(V3Event {
        payload: event.payload.clone(),
        manifest: event.manifest.as_bytes().to_vec(),
        occurred_at: rfc3339_of_nanos(event.occurred_at_ns).into_bytes(),
        ..V3Event::new(aggregate.old_pkey, aggregate.old_type, aggregate.value, event.seq_nr)
      });
    }
    if let Some(current) = &aggregate.current {
      snapshots.push(V3Snapshot {
        payload: current.payload.clone(),
        ..V3Snapshot::current(aggregate.old_pkey, aggregate.old_type, aggregate.value, current.seq_nr)
      });
    }
    for history in &aggregate.history {
      snapshots.push(V3Snapshot {
        payload: history.payload.clone(),
        ..V3Snapshot::history(aggregate.old_pkey, aggregate.old_type, aggregate.value, history.seq_nr)
      });
    }
  }
  SuccessFixture {
    events,
    snapshots,
    mapping: mapping(),
  }
}

/// `success_fixture` を移行した新しい 2 テーブルを、v5 の公開 API で読み戻して旧データと照合する。
///
/// 封筒の列（集約 ID・seq_nr・payload・manifest・発生時刻のナノ秒）、head の seq_nr、最新のスナップショットを確かめる。
pub async fn assert_success_fixture_readable(scene: &Scene) {
  let store = open_bytes_store(scene).await;
  for aggregate in success_aggregates() {
    let id = aggregate.id();
    let events = store
      .get_events_by_id_since_seq_nr(&id, 0)
      .await
      .expect("移行したイベントを読める");
    assert_eq!(events.len(), aggregate.events.len(), "{}", id.aid());
    for (event, expected) in events.iter().zip(&aggregate.events) {
      assert_eq!(event.aggregate_id(), &id);
      assert_eq!(event.seq_nr(), expected.seq_nr, "{}", id.aid());
      assert_eq!(event.payload(), &expected.payload, "{} / {}", id.aid(), expected.seq_nr);
      assert_eq!(
        event.manifest(),
        expected.manifest,
        "{} / {}",
        id.aid(),
        expected.seq_nr
      );
      assert_eq!(
        event.occurred_at().timestamp_nanos_opt(),
        Some(expected.occurred_at_ns),
        "{} / {}",
        id.aid(),
        expected.seq_nr
      );
    }
    let read = store
      .get_latest_snapshot_by_id(&id)
      .await
      .expect("移行した head を読める")
      .unwrap_or_else(|| panic!("{} の head がある", id.aid()));
    assert_eq!(read.head_seq_nr(), aggregate.head_seq_nr(), "{}", id.aid());
    match &aggregate.current {
      Some(expected) => {
        let snapshot = read
          .snapshot()
          .unwrap_or_else(|| panic!("{} の最新のスナップショットがある", id.aid()));
        assert_eq!(snapshot.seq_nr(), expected.seq_nr, "{}", id.aid());
        assert_eq!(snapshot.aggregate(), &expected.payload, "{}", id.aid());
        assert_eq!(
          snapshot.manifest(),
          "",
          "{}: スナップショットに manifest はない",
          id.aid()
        );
      }
      None => assert!(read.snapshot().is_none(), "{} にスナップショットはない", id.aid()),
    }
  }
}

// ---------------------------------------------------------------------------
// 移行が拒む旧データ
// ---------------------------------------------------------------------------

/// 検査を通る集約を 2 つ足す。拒否の前に書く実装や、拒否した集約だけを飛ばす実装だと、新しいテーブルに行が残る。
/// 型名の先頭を変えて、行キーの順で拒否される行の前と後ろの両方に並ぶようにしてある。
pub fn with_good_aggregates(
  mut events: Vec<V3Event>,
  mut snapshots: Vec<V3Snapshot>,
) -> (Vec<V3Event>, Vec<V3Snapshot>) {
  for type_name in ["0Good", "zGood"] {
    let pkey = format!("{type_name}-0");
    events.push(V3Event::new(&pkey, type_name, "a", 1));
    events.push(V3Event::new(&pkey, type_name, "a", 2));
    snapshots.push(V3Snapshot::current(&pkey, type_name, "a", 1));
  }
  (events, snapshots)
}

/// 移行が拒む旧データ 1 つ分。検査を通る集約を 2 つ含む。
pub struct RejectionCase {
  pub name: &'static str,
  pub events: Vec<V3Event>,
  pub snapshots: Vec<V3Snapshot>,
  pub mapping: HashMap<String, String>,
  /// 拒否の理由のどれかに、それぞれ含まれるべき文言。
  pub expected_reasons: Vec<&'static str>,
  /// 新しいストアの保持件数。履歴を移さない設定でも拒むべき検査は、その設定も並べる。
  pub keeps: Vec<Option<usize>>,
}

fn rejection_case(
  name: &'static str,
  expected_reasons: &[&'static str],
  keeps: &[Option<usize>],
  events: Vec<V3Event>,
  snapshots: Vec<V3Snapshot>,
) -> RejectionCase {
  let (events, snapshots) = with_good_aggregates(events, snapshots);
  RejectionCase {
    name,
    events,
    snapshots,
    mapping: HashMap::new(),
    expected_reasons: expected_reasons.to_vec(),
    keeps: keeps.to_vec(),
  }
}

/// イベント 1〜3 を持つ集約 `type_name`（値 `a`）。
fn events_up_to_three(type_name: &str) -> Vec<V3Event> {
  let pkey = format!("{type_name}-0");
  (1..=3).map(|seq| V3Event::new(&pkey, type_name, "a", seq)).collect()
}

/// 旧データの中身を理由にして拒む場合の一覧（シナリオとして個別に試験する場合を除く）。
/// どれも新しいテーブルに何も書いてはならない。
pub fn rejection_cases() -> Vec<RejectionCase> {
  let positions = [None, Some(1)];
  let once = [Some(1)];
  vec![
    rejection_case(
      "seq_nr の欠番",
      &["P-36", "連続"],
      &once,
      vec![
        V3Event::new("Gap-0", "Gap", "a", 1),
        V3Event::new("Gap-0", "Gap", "a", 3),
      ],
      vec![],
    ),
    rejection_case(
      "別の shard にある同じ seq_nr（重複）",
      &["P-36", "重複"],
      &once,
      vec![
        V3Event::new("Dup-0", "Dup", "a", 1),
        V3Event::new("Dup-1", "Dup", "a", 1),
      ],
      vec![],
    ),
    rejection_case(
      "重複と欠番が同時にあり、行の数が最大の seq_nr に一致する",
      &["P-36"],
      &once,
      vec![
        V3Event::new("Mix-0", "Mix", "a", 1),
        V3Event::new("Mix-1", "Mix", "a", 1),
        V3Event::new("Mix-0", "Mix", "a", 3),
      ],
      vec![],
    ),
    rejection_case(
      "同じ aid に別のパーティションキーの行がある",
      &["P-22", "複数の旧パーティション"],
      &once,
      vec![
        V3Event::new("Part-0", "Part", "a", 1),
        V3Event::new("Part-1", "Part", "a", 2),
      ],
      vec![],
    ),
    rejection_case(
      "最大の seq_nr を超える今のスナップショット",
      &["未来snapshot"],
      &positions,
      events_up_to_three("Future"),
      vec![V3Snapshot::current("Future-0", "Future", "a", 4)],
    ),
    rejection_case(
      "対応するイベントのない今のスナップショット（seq_nr 0）",
      &["snapshotに対応するイベントがありません"],
      &positions,
      events_up_to_three("NoEvent"),
      vec![V3Snapshot::current("NoEvent-0", "NoEvent", "a", 0)],
    ),
    rejection_case(
      "イベントのない集約のスナップショット",
      &["孤立snapshot"],
      &positions,
      vec![],
      vec![V3Snapshot::current("Orphan-0", "Orphan", "a", 1)],
    ),
    rejection_case(
      "今のスナップショットがない集約の履歴",
      &["今のスナップショットがないのに履歴の行があります"],
      &positions,
      events_up_to_three("NoCurrent"),
      vec![V3Snapshot::history("NoCurrent-0", "NoCurrent", "a", 1)],
    ),
    rejection_case(
      "今のスナップショットと同じ位置の履歴",
      &["履歴のseq_nrが今のスナップショット以上です"],
      &positions,
      events_up_to_three("Same"),
      vec![
        V3Snapshot::current("Same-0", "Same", "a", 2),
        V3Snapshot::history("Same-0", "Same", "a", 2),
      ],
    ),
    rejection_case(
      "今のスナップショットより新しい位置の履歴",
      &["履歴のseq_nrが今のスナップショット以上です"],
      &positions,
      events_up_to_three("Newer"),
      vec![
        V3Snapshot::current("Newer-0", "Newer", "a", 2),
        V3Snapshot::history("Newer-0", "Newer", "a", 3),
      ],
    ),
    rejection_case(
      "aggregate_id のセルが行キーの値と違う",
      &["aggregate_idのセルが行キーの値と一致しません"],
      &once,
      vec![V3Event {
        aggregate_id: b"other".to_vec(),
        ..V3Event::new("Cell-0", "Cell", "a", 1)
      }],
      vec![],
    ),
    rejection_case(
      "読めない発生時刻",
      &["T-13"],
      &once,
      vec![V3Event {
        occurred_at: b"not-a-time".to_vec(),
        ..V3Event::new("Time-0", "Time", "a", 1)
      }],
      vec![],
    ),
    rejection_case(
      "符号付き 64 ビットのナノ秒に収まらない発生時刻",
      &["T-13"],
      &once,
      vec![V3Event {
        occurred_at: b"2300-01-01T00:00:00Z".to_vec(),
        ..V3Event::new("Range-0", "Range", "a", 1)
      }],
      vec![],
    ),
    rejection_case(
      "履歴の行キーの番号と seq_nr のセルが違う",
      &["P-22: 履歴キー番号とsnapshotのseq_nrが一致しません"],
      &positions,
      events_up_to_three("HistKey"),
      vec![
        V3Snapshot::current("HistKey-0", "HistKey", "a", 3),
        // 行キーの末尾は 1 だが、seq_nr のセルは 2。
        V3Snapshot {
          seq_nr: b"2".to_vec(),
          ..V3Snapshot::history("HistKey-0", "HistKey", "a", 1)
        },
      ],
    ),
  ]
}
