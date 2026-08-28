use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::bigtable_client::BigtableClient;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::read_rows_response::cell_chunk::RowStatus;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::read_rows_response::CellChunk;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
  mutation, row_filter,
  row_range::{EndKey, StartKey},
  value_range::{EndValue, StartValue},
  CheckAndMutateRowRequest, MutateRowRequest, Mutation, ReadRowsRequest, RowFilter, RowRange, RowSet, ValueRange,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tonic::transport::Channel;
use tonic::Status;

use crate::event_envelope::{EventEnvelope, SnapshotEnvelope};
use crate::event_store_backend::{SnapshotMaintenance, StorageBackend};
use crate::generic_event_store::GenericEventStore;
use crate::key_resolver::{DefaultKeyResolver, KeyResolver};
use crate::serializer::{EventSerializer, SnapshotSerializer};
use crate::types::{
  format_optimistic_lock_message, AggregateId, EventStore, EventStoreReadError, EventStoreWriteError,
};

// FR6.1 / FR6.2 / FR6.3: Bigtable バックエンドの v3 封筒化。封筒⇔セルの 1:1 マッピング
// （AC2.1.1 — 1 封筒 = journal 1 行）、CheckAndMutateRow による原子的 CAS（BR1.1 / BR1.2）、
// 履歴行 + 剪定フックによる保持ポリシー（BR3.1）を StorageBackend + GenericEventStore の
// 2 層委譲構造の上に実装する。

const EVENT_FAMILY: &str = "event";
const SNAPSHOT_FAMILY: &str = "snapshot";

const QUALIFIER_PAYLOAD: &[u8] = b"payload";
const QUALIFIER_AGGREGATE_ID: &[u8] = b"aggregate_id";
const QUALIFIER_SEQ_NR: &[u8] = b"seq_nr";
const QUALIFIER_OCCURRED_AT: &[u8] = b"occurred_at";
const QUALIFIER_MANIFEST: &[u8] = b"manifest";
const QUALIFIER_VERSION: &[u8] = b"version";
const QUALIFIER_LAST_UPDATED_AT: &[u8] = b"last_updated_at";

/// Event Store for Google Cloud Bigtable
pub struct EventStoreForBigtable<AID, A, P>
where
  AID: AggregateId, {
  inner: GenericEventStore<AID, A, P, BigtableBackend<AID, A, P>>,
}

// P2(U1): derive は型パラメータ（A / P）へ Debug / Clone 境界を課すため手動 impl とし、
// payload への要求を実フィールド由来のものに限定する（BR1.6 の最小境界維持）
impl<AID: AggregateId, A, P> Debug for EventStoreForBigtable<AID, A, P> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("EventStoreForBigtable").finish()
  }
}

impl<AID: AggregateId, A, P> Clone for EventStoreForBigtable<AID, A, P> {
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}

impl<AID, A, P> EventStoreForBigtable<AID, A, P>
where
  AID: AggregateId,
  A: Serialize + DeserializeOwned + Send + Sync + 'static,
  P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
  pub fn new(
    client: BigtableClient<Channel>,
    project_id: String,
    instance_id: String,
    journal_table_name: String,
    snapshot_table_name: String,
    shard_count: u64,
  ) -> Self {
    let backend = BigtableBackend::new(
      client,
      project_id,
      instance_id,
      journal_table_name,
      snapshot_table_name,
      shard_count,
    );
    Self {
      inner: GenericEventStore::new(backend),
    }
  }

  pub fn with_key_resolver(mut self, key_resolver: Arc<dyn KeyResolver<ID = AID>>) -> Self {
    self.inner.backend_mut().set_key_resolver(key_resolver);
    self
  }

  pub fn with_event_serializer(mut self, serializer: Arc<dyn EventSerializer<P>>) -> Self {
    self.inner.backend_mut().set_event_serializer(serializer);
    self
  }

  pub fn with_snapshot_serializer(mut self, serializer: Arc<dyn SnapshotSerializer<A>>) -> Self {
    self.inner.backend_mut().set_snapshot_serializer(serializer);
    self
  }

  /// 保持するスナップショット履歴数を設定する。
  ///
  /// BR4.1 / P3: 検証（`Some(0)` の拒否）は `GenericEventStore` 側の 1 箇所で行い、
  /// このラッパーは Result を素通しする。
  pub fn with_keep_snapshot_count(mut self, keep_snapshot_count: Option<usize>) -> Result<Self, EventStoreWriteError> {
    self.inner = self.inner.with_keep_snapshot_count(keep_snapshot_count)?;
    Ok(self)
  }

  /// 履歴スナップショットの保持期限を設定する。
  ///
  /// `with_keep_snapshot_count` と併用時のみ効果を持つ（`SnapshotMaintenance` の現行契約）。
  pub fn with_delete_ttl(mut self, delete_ttl: Option<Duration>) -> Self {
    self.inner = self.inner.with_delete_ttl(delete_ttl);
    self
  }

  pub fn maintenance(&self) -> &SnapshotMaintenance {
    self.inner.maintenance()
  }
}

#[async_trait]
impl<AID, A, P> EventStore for EventStoreForBigtable<AID, A, P>
where
  AID: AggregateId,
  A: Serialize + DeserializeOwned + Send + Sync + 'static,
  P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
  type A = A;
  type AID = AID;
  type P = P;

  async fn persist_event(
    &mut self,
    event: EventEnvelope<Self::AID, Self::P>,
    expected_version: usize,
  ) -> Result<(), EventStoreWriteError> {
    self.inner.persist_event(event, expected_version).await
  }

  async fn persist_event_and_snapshot(
    &mut self,
    event: EventEnvelope<Self::AID, Self::P>,
    aggregate: Self::A,
    expected_version: usize,
  ) -> Result<(), EventStoreWriteError> {
    self
      .inner
      .persist_event_and_snapshot(event, aggregate, expected_version)
      .await
  }

  async fn get_latest_snapshot_by_id(
    &self,
    aid: &Self::AID,
  ) -> Result<Option<SnapshotEnvelope<Self::A>>, EventStoreReadError> {
    self.inner.get_latest_snapshot_by_id(aid).await
  }

  async fn get_events_by_id_since_seq_nr(
    &self,
    aid: &Self::AID,
    seq_nr: usize,
  ) -> Result<Vec<EventEnvelope<Self::AID, Self::P>>, EventStoreReadError> {
    self.inner.get_events_by_id_since_seq_nr(aid, seq_nr).await
  }
}

struct BigtableBackend<AID, A, P>
where
  AID: AggregateId, {
  client: BigtableClient<Channel>,
  project_id: String,
  instance_id: String,
  journal_table_name: String,
  snapshot_table_name: String,
  shard_count: u64,
  key_resolver: Arc<dyn KeyResolver<ID = AID>>,
  event_serializer: Arc<dyn EventSerializer<P>>,
  snapshot_serializer: Arc<dyn SnapshotSerializer<A>>,
}

// P2(U1): derive は A / P へ Debug 境界を課すため手動 impl とする（内部構成は表示しない）
impl<AID: AggregateId, A, P> Debug for BigtableBackend<AID, A, P> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("BigtableBackend").finish()
  }
}

impl<AID: AggregateId, A, P> Clone for BigtableBackend<AID, A, P> {
  fn clone(&self) -> Self {
    Self {
      client: self.client.clone(),
      project_id: self.project_id.clone(),
      instance_id: self.instance_id.clone(),
      journal_table_name: self.journal_table_name.clone(),
      snapshot_table_name: self.snapshot_table_name.clone(),
      shard_count: self.shard_count,
      key_resolver: Arc::clone(&self.key_resolver),
      event_serializer: Arc::clone(&self.event_serializer),
      snapshot_serializer: Arc::clone(&self.snapshot_serializer),
    }
  }
}

impl<AID, A, P> BigtableBackend<AID, A, P>
where
  AID: AggregateId,
  A: Serialize + DeserializeOwned + Send + Sync + 'static,
  P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
  fn new(
    client: BigtableClient<Channel>,
    project_id: String,
    instance_id: String,
    journal_table_name: String,
    snapshot_table_name: String,
    shard_count: u64,
  ) -> Self {
    Self {
      client,
      project_id,
      instance_id,
      journal_table_name,
      snapshot_table_name,
      shard_count,
      key_resolver: Arc::new(DefaultKeyResolver::default()),
      event_serializer: Arc::new(crate::serializer::JsonEventSerializer::default()),
      snapshot_serializer: Arc::new(crate::serializer::JsonSnapshotSerializer::default()),
    }
  }

  fn set_key_resolver(&mut self, key_resolver: Arc<dyn KeyResolver<ID = AID>>) {
    self.key_resolver = key_resolver;
  }

  fn set_event_serializer(&mut self, serializer: Arc<dyn EventSerializer<P>>) {
    self.event_serializer = serializer;
  }

  fn set_snapshot_serializer(&mut self, serializer: Arc<dyn SnapshotSerializer<A>>) {
    self.snapshot_serializer = serializer;
  }

  fn table_path(&self, table: &str) -> String {
    format!(
      "projects/{}/instances/{}/tables/{}",
      self.project_id, self.instance_id, table
    )
  }

  fn snapshot_row_key(&self, aid: &AID) -> Vec<u8> {
    format!(
      "{}#{}#{}",
      self.key_resolver.resolve_partition_key(aid, self.shard_count),
      aid.type_name(),
      aid.value()
    )
    .into_bytes()
  }

  fn event_row_prefix(&self, aid: &AID) -> Vec<u8> {
    self.snapshot_row_key(aid)
  }

  fn event_row_key(&self, aid: &AID, seq_nr: usize) -> Vec<u8> {
    let mut key = self.event_row_prefix(aid);
    key.push(b'#');
    key.extend_from_slice(format!("{:020}", seq_nr).as_bytes());
    key
  }

  fn history_row_prefix(&self, aid: &AID) -> Vec<u8> {
    let mut key = self.snapshot_row_key(aid);
    key.push(b'#');
    key
  }

  // BR3.1 / FR6.3: 履歴行キーは snapshot 行キー + ゼロ詰め seq_nr 修飾（現行行と同一テーブル・
  // 同一キー接頭辞で範囲走査可能な形）。key_resolver の既存 API 出力への修飾のみで構成し、
  // key_resolver 自体は変更しない（U3 境界）
  fn history_row_key(&self, aid: &AID, seq_nr: usize) -> Vec<u8> {
    let mut key = self.history_row_prefix(aid);
    key.extend_from_slice(format!("{:020}", seq_nr).as_bytes());
    key
  }

  async fn fetch_rows(&self, request: ReadRowsRequest) -> Result<Vec<RowData>, EventStoreReadError> {
    let mut client = self.client.clone();
    let mut stream = client
      .read_rows(request)
      .await
      .map_err(status_to_read_error)?
      .into_inner();
    let mut rows = Vec::new();
    let mut acc = RowAccumulator::default();
    while let Some(response) = stream.message().await.map_err(status_to_read_error)? {
      for chunk in response.chunks {
        Self::process_chunk(&mut acc, chunk, &mut rows);
      }
    }
    if let Some(row) = acc.finish_row() {
      rows.push(row);
    }
    Ok(rows)
  }

  fn process_chunk(acc: &mut RowAccumulator, chunk: CellChunk, rows: &mut Vec<RowData>) {
    if matches!(chunk.row_status, Some(RowStatus::ResetRow(true))) {
      acc.reset();
      return;
    }

    if !chunk.row_key.is_empty() && acc.key != chunk.row_key {
      if let Some(row) = acc.finish_row() {
        rows.push(row);
      }
      acc.key = chunk.row_key.clone();
    }

    if chunk.family_name.is_some() || chunk.qualifier.is_some() {
      acc.start_cell();
    }

    if let Some(family) = chunk.family_name {
      acc.current_family = Some(family);
    }
    if let Some(ref qualifier) = chunk.qualifier {
      acc.current_qualifier = Some(qualifier.clone());
    }

    if !chunk.value.is_empty() {
      acc.current_value.extend_from_slice(&chunk.value);
    }

    if matches!(chunk.row_status, Some(RowStatus::CommitRow(true))) {
      if let Some(row) = acc.finish_row() {
        rows.push(row);
      }
    }
  }

  /// journal へ封筒 1 件を 1 行として書き込む。
  ///
  /// BR2.1 / FR6.1 / AC2.1.1: 封筒メタデータ 4 点 + payload を 5 セルへ 1:1 で書く。
  /// manifest セルは v3 新設で、省略時は空文字列がそのまま格納される
  /// （U1 BR1.2 のラウンドトリップ — AC5.1.1）。payload の分解は封筒の公開アクセサ
  /// のみで行う（NFR2.3）。
  async fn write_event(&self, event: &EventEnvelope<AID, P>) -> Result<(), EventStoreWriteError> {
    let row_key = self.event_row_key(event.aggregate_id(), event.seq_nr());
    let payload = self.event_serializer.serialize(event.payload())?;
    let mutations = vec![
      set_cell(EVENT_FAMILY, QUALIFIER_PAYLOAD, payload),
      set_cell(
        EVENT_FAMILY,
        QUALIFIER_AGGREGATE_ID,
        event.aggregate_id().value().into_bytes(),
      ),
      set_cell(EVENT_FAMILY, QUALIFIER_SEQ_NR, event.seq_nr().to_string().into_bytes()),
      set_cell(
        EVENT_FAMILY,
        QUALIFIER_OCCURRED_AT,
        format_occurred_at(event.occurred_at()).into_bytes(),
      ),
      set_cell(EVENT_FAMILY, QUALIFIER_MANIFEST, event.manifest().as_bytes().to_vec()),
    ];
    self
      .mutate_row(self.table_path(&self.journal_table_name), row_key, mutations)
      .await
  }

  async fn mutate_row(
    &self,
    table_name: String,
    row_key: Vec<u8>,
    mutations: Vec<Mutation>,
  ) -> Result<(), EventStoreWriteError> {
    let mut client = self.client.clone();
    client
      .mutate_row(MutateRowRequest {
        table_name,
        row_key,
        mutations,
        ..Default::default()
      })
      .await
      .map_err(status_to_write_error)?;
    Ok(())
  }

  /// snapshot 現行行へ CheckAndMutateRow を発行し、述語の成否を返す。
  ///
  /// BR1.1 / BR1.2 / NFR4.2: 述語評価とミューテーション適用は Bigtable の単一行原子性
  /// により原子的で、read→check→write の TOCTOU 窓は構造的に存在しない。
  /// BR5.1 / NFR5.2: CheckAndMutateRow の単一行原子性は Bigtable API 契約であり、
  /// エミュレータ（cloud-sdk）と本番でこの意味論は同一である（性能特性の差は検証範囲外）。
  async fn check_and_mutate_snapshot_row(
    &self,
    row_key: Vec<u8>,
    predicate_filter: RowFilter,
    true_mutations: Vec<Mutation>,
    false_mutations: Vec<Mutation>,
  ) -> Result<bool, EventStoreWriteError> {
    let mut client = self.client.clone();
    let response = client
      .check_and_mutate_row(CheckAndMutateRowRequest {
        table_name: self.table_path(&self.snapshot_table_name),
        row_key,
        predicate_filter: Some(predicate_filter),
        true_mutations,
        false_mutations,
        ..Default::default()
      })
      .await
      .map_err(status_to_write_error)?;
    Ok(response.into_inner().predicate_matched)
  }

  /// snapshot 現行行を読み取り、生セル + 解釈済みメタデータを返す。行不在は `None`。
  ///
  /// 封筒構築（fetch_latest_snapshot）とプレイメージ複製（履歴行書込）の両方が使う。
  async fn read_snapshot_row(&self, aid: &AID) -> Result<Option<SnapshotRowImage>, EventStoreReadError> {
    let row_key = self.snapshot_row_key(aid);
    let request = ReadRowsRequest {
      table_name: self.table_path(&self.snapshot_table_name),
      rows: Some(RowSet {
        row_keys: vec![row_key],
        row_ranges: vec![],
      }),
      filter: Some(latest_cells_of_family_filter(SNAPSHOT_FAMILY)),
      rows_limit: 1,
      ..Default::default()
    };
    let mut rows = self.fetch_rows(request).await?;
    let row = match rows.pop() {
      Some(row) => row,
      None => return Ok(None),
    };
    let payload = required_cell(&row, SNAPSHOT_FAMILY, QUALIFIER_PAYLOAD, "snapshot")?.clone();
    let version = parse_usize(required_cell(&row, SNAPSHOT_FAMILY, QUALIFIER_VERSION, "snapshot")?)?;
    let seq_nr = parse_usize(required_cell(&row, SNAPSHOT_FAMILY, QUALIFIER_SEQ_NR, "snapshot")?)?;
    let last_updated_at = required_cell(&row, SNAPSHOT_FAMILY, QUALIFIER_LAST_UPDATED_AT, "snapshot")?.clone();
    Ok(Some(SnapshotRowImage {
      payload,
      version,
      seq_nr,
      last_updated_at,
    }))
  }

  /// 履歴行を行キー昇順（= seq_nr 昇順 = 旧い順）で読み取る。
  async fn read_history_rows(&self, aid: &AID) -> Result<Vec<RowData>, EventStoreReadError> {
    let prefix = self.history_row_prefix(aid);
    let mut end_key = prefix.clone();
    end_key.push(0xFF);
    let request = ReadRowsRequest {
      table_name: self.table_path(&self.snapshot_table_name),
      rows: Some(RowSet {
        row_keys: vec![],
        row_ranges: vec![RowRange {
          start_key: Some(StartKey::StartKeyClosed(prefix)),
          end_key: Some(EndKey::EndKeyOpen(end_key)),
        }],
      }),
      filter: Some(latest_cells_of_family_filter(SNAPSHOT_FAMILY)),
      ..Default::default()
    };
    let mut rows = self.fetch_rows(request).await?;
    rows.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(rows)
  }

  /// journal 行の全セルからイベント封筒を再構成する。
  ///
  /// BR2.2 / AC4.1.1: aggregate_id / seq_nr / occurred_at / manifest / payload の全セルを
  /// 使い、セル欠落は破損データとして読取エラーにする（payload セルのみ読取の廃止 — FR5.1）。
  /// NFR2.3: 封筒の構築は `EventEnvelope::new` + `with_manifest` の公開ビルダーのみで行う。
  fn event_envelope_from_row(&self, aid: &AID, row: &RowData) -> Result<EventEnvelope<AID, P>, EventStoreReadError> {
    let stored_aid = required_cell(row, EVENT_FAMILY, QUALIFIER_AGGREGATE_ID, "journal")?;
    if stored_aid.as_slice() != aid.value().as_bytes() {
      return Err(EventStoreReadError::OtherError(format!(
        "journal aggregate_id cell does not match the requested aggregate: aid={}",
        aid
      )));
    }
    let seq_nr = parse_usize(required_cell(row, EVENT_FAMILY, QUALIFIER_SEQ_NR, "journal")?)?;
    let occurred_at = parse_occurred_at(required_cell(row, EVENT_FAMILY, QUALIFIER_OCCURRED_AT, "journal")?)?;
    let manifest = String::from_utf8(required_cell(row, EVENT_FAMILY, QUALIFIER_MANIFEST, "journal")?.clone())
      .map_err(|err| EventStoreReadError::OtherError(err.to_string()))?;
    let payload = self
      .event_serializer
      .deserialize(required_cell(row, EVENT_FAMILY, QUALIFIER_PAYLOAD, "journal")?)?;
    Ok(EventEnvelope::new(aid.clone(), seq_nr, occurred_at, payload).with_manifest(manifest))
  }
}

#[async_trait]
impl<AID, A, P> StorageBackend<AID, A, P> for BigtableBackend<AID, A, P>
where
  AID: AggregateId,
  A: Serialize + DeserializeOwned + Send + Sync + 'static,
  P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
  async fn fetch_latest_snapshot(&self, aid: &AID) -> Result<Option<SnapshotEnvelope<A>>, EventStoreReadError> {
    // FR4.3 / U1 BR2.5: 読取後の set_version 補正は存在しない — version / seq_nr は
    // 列（セル）側の値をそのまま封筒に載せて返す
    match self.read_snapshot_row(aid).await? {
      None => Ok(None),
      Some(image) => {
        let aggregate = self.snapshot_serializer.deserialize(&image.payload)?;
        Ok(Some(SnapshotEnvelope::new(aggregate, image.seq_nr, image.version)))
      }
    }
  }

  async fn fetch_events_since(
    &self,
    aid: &AID,
    seq_nr: usize,
  ) -> Result<Vec<EventEnvelope<AID, P>>, EventStoreReadError> {
    let prefix = self.event_row_prefix(aid);
    let mut end_key = prefix.clone();
    end_key.push(0xFF);
    let start_key = self.event_row_key(aid, seq_nr);

    let request = ReadRowsRequest {
      table_name: self.table_path(&self.journal_table_name),
      rows: Some(RowSet {
        row_keys: vec![],
        row_ranges: vec![RowRange {
          start_key: Some(StartKey::StartKeyClosed(start_key)),
          end_key: Some(EndKey::EndKeyOpen(end_key)),
        }],
      }),
      filter: Some(latest_cells_of_family_filter(EVENT_FAMILY)),
      ..Default::default()
    };

    let rows = self.fetch_rows(request).await?;
    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
      // BR2.2: 各行の全セルから封筒を再構成する（セル欠落は読取エラー）
      events.push(self.event_envelope_from_row(aid, &row)?);
    }
    Ok(events)
  }

  async fn create_event_and_snapshot(
    &self,
    event: &EventEnvelope<AID, P>,
    aggregate: &A,
    _maintenance: &SnapshotMaintenance,
  ) -> Result<(), EventStoreWriteError> {
    // BR2.3 / AC2.3.3: snapshot payload は純ドメイン内容のみ（version 注入の廃止 — FR4.2）
    let payload = self.snapshot_serializer.serialize(aggregate)?;

    // BR1.2 / FR6.2 / P4: 新規作成は CheckAndMutateRow の述語不成立側（version セル不在）で
    // 原子的に初期化する。false ミューテーションで version=1 / seq_nr=event.seq_nr() /
    // last_updated_at=event.occurred_at() / payload を設定する
    let false_mutations = vec![
      set_cell(SNAPSHOT_FAMILY, QUALIFIER_PAYLOAD, payload),
      set_cell(SNAPSHOT_FAMILY, QUALIFIER_VERSION, 1_usize.to_string().into_bytes()),
      set_cell(
        SNAPSHOT_FAMILY,
        QUALIFIER_SEQ_NR,
        event.seq_nr().to_string().into_bytes(),
      ),
      set_cell(
        SNAPSHOT_FAMILY,
        QUALIFIER_LAST_UPDATED_AT,
        event.occurred_at().timestamp_millis().to_string().into_bytes(),
      ),
    ];
    let predicate_matched = self
      .check_and_mutate_snapshot_row(
        self.snapshot_row_key(event.aggregate_id()),
        version_exists_predicate(),
        vec![],
        false_mutations,
      )
      .await?;
    if predicate_matched {
      // 既存集約への create は楽観ロックエラー（統一書式・actual なし。
      // 新規作成の expected_version は 0 — U1 の W1 規約）
      return Err(EventStoreWriteError::OptimisticLockError(
        format_optimistic_lock_message(&event.aggregate_id().to_string(), 0, None),
      ));
    }

    // BR2.1: journal へ封筒 5 セルを書込（CAS 成功後の別行書込 — 現行と同じ順序・非トランザクション性）
    self.write_event(event).await
  }

  async fn update_event_and_snapshot(
    &self,
    event: &EventEnvelope<AID, P>,
    aggregate: Option<&A>,
    expected_version: usize,
    maintenance: &SnapshotMaintenance,
  ) -> Result<(), EventStoreWriteError> {
    let aid = event.aggregate_id();

    // BR3.1 / P5: プレイメージ読取は keep_snapshot_count = Some(n) かつスナップショット付き
    // 更新の場合のみ行う。読取 version ≠ expected_version なら履歴コピーはしない（version は
    // 単調増加のため後続 CAS は必ず失敗する — 競合判定はあくまで述語側）。読取失敗は
    // ベストエフォートとして warn 記録のみで継続する（競合・接続不能の実エラーは直後の CAS が返す）
    let pre_image = if maintenance.keep_snapshot_count.is_some() && aggregate.is_some() {
      match self.read_snapshot_row(aid).await {
        Ok(Some(image)) if image.version == expected_version => Some(image),
        Ok(_) => None,
        Err(err) => {
          tracing::warn!(
            "snapshot pre-image read failed: aid={}, kind={}",
            aid,
            read_error_kind(&err)
          );
          None
        }
      }
    } else {
      None
    };

    let mut true_mutations = vec![
      set_cell(
        SNAPSHOT_FAMILY,
        QUALIFIER_VERSION,
        (expected_version + 1).to_string().into_bytes(),
      ),
      set_cell(
        SNAPSHOT_FAMILY,
        QUALIFIER_LAST_UPDATED_AT,
        event.occurred_at().timestamp_millis().to_string().into_bytes(),
      ),
    ];
    if let Some(aggregate) = aggregate {
      // BR2.3 / AC2.3.3: payload は純ドメイン内容のみ（version 注入の廃止 — FR4.2）。
      // seq_nr セルは event.seq_nr() を採用する（U1 SPI 契約）
      let payload = self.snapshot_serializer.serialize(aggregate)?;
      true_mutations.push(set_cell(SNAPSHOT_FAMILY, QUALIFIER_PAYLOAD, payload));
      true_mutations.push(set_cell(
        SNAPSHOT_FAMILY,
        QUALIFIER_SEQ_NR,
        event.seq_nr().to_string().into_bytes(),
      ));
    }

    // BR1.1 / FR6.2 / NFR4.2 / AC2.2.1: read→check→write の非トランザクション実装を
    // CheckAndMutateRow による単一行原子性 CAS で置換する
    let predicate_matched = self
      .check_and_mutate_snapshot_row(
        self.snapshot_row_key(aid),
        version_matches_predicate(expected_version),
        true_mutations,
        vec![],
      )
      .await?;
    if !predicate_matched {
      // 追補読取で actual_version を取得できる場合のみ付与する（統一書式の許可キー内）。
      // 行不在（不在集約への更新）は actual なし — U1 リファレンス意味論との対称性
      let actual_version = match self.read_snapshot_row(aid).await {
        Ok(Some(image)) => Some(image.version),
        _ => None,
      };
      return Err(EventStoreWriteError::OptimisticLockError(
        format_optimistic_lock_message(&aid.to_string(), expected_version, actual_version),
      ));
    }

    // BR3.1 / P5 / NFR3.5: CAS 勝者のみがプレイメージを別呼び出しの mutate_row で履歴行へ書く
    // （CheckAndMutateRow は単一行にしか作用しないため同一 CAS には束ねられない。敗者は上で
    // 復帰済みなので二重書込は発生しない）。失敗はエラーを返さず warn で記録して続行する
    // （履歴は剪定対象の付随データであり、CAS 成功後のエラー返却は呼出し側の再試行を偽競合に
    // する。warn には aid + エラー種別のみを載せ、payload は載せない）
    if let Some(image) = pre_image {
      let history_key = self.history_row_key(aid, image.seq_nr);
      let mutations = vec![
        set_cell(SNAPSHOT_FAMILY, QUALIFIER_PAYLOAD, image.payload),
        set_cell(
          SNAPSHOT_FAMILY,
          QUALIFIER_VERSION,
          image.version.to_string().into_bytes(),
        ),
        set_cell(SNAPSHOT_FAMILY, QUALIFIER_SEQ_NR, image.seq_nr.to_string().into_bytes()),
        set_cell(SNAPSHOT_FAMILY, QUALIFIER_LAST_UPDATED_AT, image.last_updated_at),
      ];
      if let Err(err) = self
        .mutate_row(self.table_path(&self.snapshot_table_name), history_key, mutations)
        .await
      {
        tracing::warn!(
          "snapshot history write failed: aid={}, kind={}",
          aid,
          write_error_kind(&err)
        );
      }
    }

    // BR2.1: journal へ封筒 5 セルを書込
    self.write_event(event).await
  }

  async fn on_event_persisted(&self, aid: &AID, maintenance: &SnapshotMaintenance) -> Result<(), EventStoreWriteError> {
    // BR3.1 / FR6.3 / AC2.3.4: 保持ポリシーの実行点。keep_snapshot_count = None は
    // 現行互換の no-op（履歴行も書かれないため剪定対象が存在しない）
    let keep = match maintenance.keep_snapshot_count {
      Some(keep) => keep,
      None => return Ok(()),
    };
    let rows = self.read_history_rows(aid).await.map_err(read_error_to_write_error)?;
    // 行キーは snapshot 行キー + ゼロ詰め seq_nr 修飾のため、キー昇順 = 旧い順。
    // 新しい順に keep 件残し、先頭（旧い側）の超過分を DeleteFromRow で剪定する
    let excess = rows.len().saturating_sub(keep);
    // delete_ttl 併用時は、保持数内でも期限超過の履歴行を削除する（BR3.1）
    let cutoff_millis = maintenance.delete_ttl.map(|ttl| (Utc::now() - ttl).timestamp_millis());
    for (index, row) in rows.into_iter().enumerate() {
      let expired = cutoff_millis.is_some_and(|cutoff| {
        row
          .cells
          .get(&(SNAPSHOT_FAMILY.to_string(), QUALIFIER_LAST_UPDATED_AT.to_vec()))
          .and_then(|bytes| parse_millis(bytes))
          .is_some_and(|millis| millis < cutoff)
      });
      if index < excess || expired {
        self
          .mutate_row(
            self.table_path(&self.snapshot_table_name),
            row.key,
            vec![delete_from_row()],
          )
          .await?;
      }
    }
    Ok(())
  }
}

#[derive(Default)]
struct RowAccumulator {
  key: Vec<u8>,
  current_family: Option<String>,
  current_qualifier: Option<Vec<u8>>,
  current_value: Vec<u8>,
  cells: HashMap<(String, Vec<u8>), Vec<u8>>,
}

impl RowAccumulator {
  fn start_cell(&mut self) {
    if let (Some(family), Some(qualifier)) = (&self.current_family, &self.current_qualifier) {
      let value = std::mem::take(&mut self.current_value);
      self.cells.entry((family.clone(), qualifier.clone())).or_insert(value);
    } else {
      self.current_value.clear();
    }
    self.current_family = None;
    self.current_qualifier = None;
  }

  fn finish_row(&mut self) -> Option<RowData> {
    if self.key.is_empty() {
      return None;
    }
    self.start_cell();
    let key = std::mem::take(&mut self.key);
    if self.cells.is_empty() {
      return None;
    }
    let cells = std::mem::take(&mut self.cells);
    Some(RowData { key, cells })
  }

  fn reset(&mut self) {
    self.key.clear();
    self.cells.clear();
    self.current_family = None;
    self.current_qualifier = None;
    self.current_value.clear();
  }
}

struct RowData {
  key: Vec<u8>,
  cells: HashMap<(String, Vec<u8>), Vec<u8>>,
}

/// snapshot 現行行の読取結果（生セル + 解釈済みメタデータ）。
///
/// payload / last_updated_at は生バイト列のまま保持し、プレイメージの履歴行複製で
/// payload を解釈せずに転記できるようにする（NFR2.3 — payload の中身を覗く分岐を持ち込まない）。
struct SnapshotRowImage {
  payload: Vec<u8>,
  version: usize,
  seq_nr: usize,
  last_updated_at: Vec<u8>,
}

fn set_cell(family: &str, qualifier: &[u8], value: Vec<u8>) -> Mutation {
  Mutation {
    mutation: Some(mutation::Mutation::SetCell(mutation::SetCell {
      family_name: family.to_string(),
      column_qualifier: qualifier.to_vec(),
      timestamp_micros: -1,
      value,
    })),
  }
}

fn delete_from_row() -> Mutation {
  Mutation {
    mutation: Some(mutation::Mutation::DeleteFromRow(mutation::DeleteFromRow {})),
  }
}

fn chain_filter(filters: Vec<row_filter::Filter>) -> RowFilter {
  RowFilter {
    filter: Some(row_filter::Filter::Chain(row_filter::Chain {
      filters: filters
        .into_iter()
        .map(|filter| RowFilter { filter: Some(filter) })
        .collect(),
    })),
  }
}

// 読取は対象ファミリの各列の最新セルのみを対象にする（set_cell が残す旧セルバージョンの
// 誤読防止 — BR1.1 の最新セル限定と同根）
fn latest_cells_of_family_filter(family: &str) -> RowFilter {
  chain_filter(vec![
    row_filter::Filter::FamilyNameRegexFilter(family.to_string()),
    row_filter::Filter::CellsPerColumnLimitFilter(1),
  ])
}

// BR1.2 / P4: 新規作成パスの述語 — version セルの存在チェック（2 フィルタ）。
// 新規作成には比較対象の expected_version が存在しないため値一致フィルタを持たない
// （列スコープは更新パスと同じ理由で必須）
fn version_exists_predicate() -> RowFilter {
  chain_filter(vec![
    row_filter::Filter::FamilyNameRegexFilter(SNAPSHOT_FAMILY.to_string()),
    row_filter::Filter::ColumnQualifierRegexFilter(QUALIFIER_VERSION.to_vec()),
  ])
}

// BR1.1 / P4 / NFR4.2: 更新パスの述語 — 列スコープ + 最新セル限定 + expected_version 完全一致の
// 4 フィルタチェーン。列スコープなしの ValueRangeFilter は行内全セル（seq_nr 含む）に一致する
// ため禁止。最新セル限定（CellsPerColumnLimitFilter(1)）は set_cell が残す旧セルバージョンとの
// 誤一致を防ぐため必須
fn version_matches_predicate(expected_version: usize) -> RowFilter {
  let expected = expected_version.to_string().into_bytes();
  chain_filter(vec![
    row_filter::Filter::FamilyNameRegexFilter(SNAPSHOT_FAMILY.to_string()),
    row_filter::Filter::ColumnQualifierRegexFilter(QUALIFIER_VERSION.to_vec()),
    row_filter::Filter::CellsPerColumnLimitFilter(1),
    row_filter::Filter::ValueRangeFilter(ValueRange {
      start_value: Some(StartValue::StartValueClosed(expected.clone())),
      end_value: Some(EndValue::EndValueClosed(expected)),
    }),
  ])
}

/// gRPC ステータスの中立表現。
///
/// BR4.1 / NFR3.4 / P6: gRPC の失敗はこの写像点 1 箇所で IOError 系へ包み、保持するのは
/// ステータスコードのみとする。生ステータスのメッセージ（テーブルパス・プロジェクト ID・
/// 接続先が混入しうる）は展開しない。
#[derive(Debug)]
struct BigtableStatusError {
  code: tonic::Code,
}

impl std::fmt::Display for BigtableStatusError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "grpc status: {:?}", self.code)
  }
}

impl std::error::Error for BigtableStatusError {}

fn status_to_read_error(status: Status) -> EventStoreReadError {
  EventStoreReadError::IOError(Box::new(BigtableStatusError { code: status.code() }))
}

fn status_to_write_error(status: Status) -> EventStoreWriteError {
  EventStoreWriteError::IOError(Box::new(BigtableStatusError { code: status.code() }))
}

// on_event_persisted（書込フック）内の読取失敗を、種別を保存したまま書込エラーへ写像する（BR4.1）
fn read_error_to_write_error(err: EventStoreReadError) -> EventStoreWriteError {
  match err {
    EventStoreReadError::IOError(source) => EventStoreWriteError::IOError(source),
    EventStoreReadError::DeserializationError(source) => EventStoreWriteError::OtherError(source.to_string()),
    EventStoreReadError::OtherError(message) => EventStoreWriteError::OtherError(message),
  }
}

// NFR3.5 / P5: warn ログに載せるのはエラー種別のみ（payload・接続情報・生ステータスを展開しない）
fn read_error_kind(err: &EventStoreReadError) -> &'static str {
  match err {
    EventStoreReadError::DeserializationError(_) => "DeserializationError",
    EventStoreReadError::IOError(_) => "IOError",
    EventStoreReadError::OtherError(_) => "OtherError",
  }
}

fn write_error_kind(err: &EventStoreWriteError) -> &'static str {
  match err {
    EventStoreWriteError::SerializationError(_) => "SerializationError",
    EventStoreWriteError::OptimisticLockError(_) => "OptimisticLockError",
    EventStoreWriteError::ContractViolation(_) => "ContractViolation",
    EventStoreWriteError::IOError(_) => "IOError",
    EventStoreWriteError::OtherError(_) => "OtherError",
  }
}

// FR1.4 / U1 BR1.3: occurred_at はドメイン供給値のまま保存・読出しする。epoch millis 格納では
// サブミリ秒精度が失われ供給値の完全往復（共有シナリオの occurred_at 同値 assert — C3）を
// 満たせないため、ナノ秒精度の RFC 3339 文字列でセルへ格納する
fn format_occurred_at(occurred_at: &DateTime<Utc>) -> String {
  occurred_at.to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn parse_occurred_at(bytes: &[u8]) -> Result<DateTime<Utc>, EventStoreReadError> {
  let text = std::str::from_utf8(bytes).map_err(|err| EventStoreReadError::OtherError(err.to_string()))?;
  DateTime::parse_from_rfc3339(text)
    .map(|occurred_at| occurred_at.with_timezone(&Utc))
    .map_err(|err| EventStoreReadError::OtherError(err.to_string()))
}

// BR2.2: セル欠落は破損データとして読取エラーにする（メッセージは行種別とファミリ・列名のみ）
fn required_cell<'a>(
  row: &'a RowData,
  family: &str,
  qualifier: &[u8],
  row_kind: &str,
) -> Result<&'a Vec<u8>, EventStoreReadError> {
  row.cells.get(&(family.to_string(), qualifier.to_vec())).ok_or_else(|| {
    EventStoreReadError::OtherError(format!(
      "{} row cell is missing: {}:{}",
      row_kind,
      family,
      String::from_utf8_lossy(qualifier)
    ))
  })
}

fn parse_usize(bytes: &[u8]) -> Result<usize, EventStoreReadError> {
  let s = std::str::from_utf8(bytes).map_err(|err| EventStoreReadError::OtherError(err.to_string()))?;
  s.parse::<usize>()
    .map_err(|err| EventStoreReadError::OtherError(err.to_string()))
}

fn parse_millis(bytes: &[u8]) -> Option<i64> {
  std::str::from_utf8(bytes).ok()?.parse::<i64>().ok()
}
