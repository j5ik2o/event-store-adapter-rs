use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::transact_write_items::{TransactWriteItemsError, TransactWriteItemsOutput};
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{AttributeValue, DeleteRequest, Put, Select, TransactWriteItem, Update, WriteRequest};
use aws_sdk_dynamodb::Client;
use chrono::{DateTime, Duration, Utc};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tracing::Instrument;

use crate::event_envelope::{EventEnvelope, SnapshotEnvelope};
use crate::event_store_backend::{SnapshotMaintenance, StorageBackend};
use crate::generic_event_store::GenericEventStore;
use crate::key_resolver::{DefaultKeyResolver, KeyResolver};
use crate::serializer::{EventSerializer, SnapshotSerializer};
use crate::types::{
  format_optimistic_lock_message, AggregateId, EventStore, EventStoreReadError, EventStoreWriteError,
};

// FR6.1 / FR6.5: DynamoDB バックエンドの v3 封筒化。封筒⇔属性の 1:1 マッピング（AC2.1.1）、
// TransactWriteItems + 条件式による原子的 CAS の現行維持（BR1.1 / P7 / NFR4.3）、条件付き
// 履歴 Put + 剪定フックによる保持ポリシー（BR3.1）を StorageBackend + GenericEventStore の
// 2 層委譲構造の上に実装する。

// BR2.4: current スナップショット項目の skey は seq_nr=0 マーカーで解決する（キー設計の現行維持）。
// このマーカーは物理キーの解決だけに使い、seq_nr 属性の実値（event.seq_nr()）とは混用しない。
const CURRENT_SNAPSHOT_SKEY_MARKER: usize = 0;

/// Event Store for Amazon DynamoDB
pub struct EventStoreForDynamoDB<AID, A, P>
where
  AID: AggregateId, {
  inner: GenericEventStore<AID, A, P, DynamoDbBackend<AID, A, P>>,
}

// P2(U1): derive は型パラメータ（A / P）へ Debug / Clone 境界を課すため手動 impl とし、
// payload への要求を実フィールド由来のものに限定する（BR1.6 の最小境界維持）
impl<AID: AggregateId, A, P> Debug for EventStoreForDynamoDB<AID, A, P> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("EventStoreForDynamoDB").finish()
  }
}

impl<AID: AggregateId, A, P> Clone for EventStoreForDynamoDB<AID, A, P> {
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}

impl<AID, A, P> EventStoreForDynamoDB<AID, A, P>
where
  AID: AggregateId,
  A: Serialize + DeserializeOwned + Send + Sync + 'static,
  P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
  pub fn new(
    client: Client,
    journal_table_name: String,
    journal_aid_index_name: String,
    snapshot_table_name: String,
    snapshot_aid_index_name: String,
    shard_count: u64,
  ) -> Self {
    let backend = DynamoDbBackend::new(
      client,
      journal_table_name,
      journal_aid_index_name,
      snapshot_table_name,
      snapshot_aid_index_name,
      shard_count,
    );
    Self {
      inner: GenericEventStore::new(backend),
    }
  }

  /// 保持するスナップショット履歴数を設定する。
  ///
  /// BR4.1(U1) / P3: 検証（`Some(0)` の拒否）は `GenericEventStore` 側の 1 箇所で行い、
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

  pub fn maintenance(&self) -> &SnapshotMaintenance {
    self.inner.maintenance()
  }
}

#[async_trait]
impl<AID, A, P> EventStore for EventStoreForDynamoDB<AID, A, P>
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

struct DynamoDbBackend<AID, A, P>
where
  AID: AggregateId, {
  client: Client,
  journal_table_name: String,
  journal_aid_index_name: String,
  snapshot_table_name: String,
  snapshot_aid_index_name: String,
  shard_count: u64,
  key_resolver: Arc<dyn KeyResolver<ID = AID>>,
  event_serializer: Arc<dyn EventSerializer<P>>,
  snapshot_serializer: Arc<dyn SnapshotSerializer<A>>,
}

// P2(U1): derive は A / P へ Debug 境界を課すため手動 impl とする（内部構成は表示しない）
impl<AID: AggregateId, A, P> Debug for DynamoDbBackend<AID, A, P> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("DynamoDbBackend").finish()
  }
}

impl<AID: AggregateId, A, P> Clone for DynamoDbBackend<AID, A, P> {
  fn clone(&self) -> Self {
    Self {
      client: self.client.clone(),
      journal_table_name: self.journal_table_name.clone(),
      journal_aid_index_name: self.journal_aid_index_name.clone(),
      snapshot_table_name: self.snapshot_table_name.clone(),
      snapshot_aid_index_name: self.snapshot_aid_index_name.clone(),
      shard_count: self.shard_count,
      key_resolver: Arc::clone(&self.key_resolver),
      event_serializer: Arc::clone(&self.event_serializer),
      snapshot_serializer: Arc::clone(&self.snapshot_serializer),
    }
  }
}

impl<AID, A, P> DynamoDbBackend<AID, A, P>
where
  AID: AggregateId,
  A: Serialize + DeserializeOwned + Send + Sync + 'static,
  P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
  fn new(
    client: Client,
    journal_table_name: String,
    journal_aid_index_name: String,
    snapshot_table_name: String,
    snapshot_aid_index_name: String,
    shard_count: u64,
  ) -> Self {
    Self {
      client,
      journal_table_name,
      journal_aid_index_name,
      snapshot_table_name,
      snapshot_aid_index_name,
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

  fn resolve_pkey(&self, id: &AID) -> String {
    self.key_resolver.resolve_partition_key(id, self.shard_count)
  }

  fn resolve_skey(&self, id: &AID, seq_nr: usize) -> String {
    self.key_resolver.resolve_sort_key(id, seq_nr)
  }

  fn current_snapshot_skey(&self, id: &AID) -> String {
    self.resolve_skey(id, CURRENT_SNAPSHOT_SKEY_MARKER)
  }

  /// スナップショット項目の Put を構築する。
  ///
  /// BR2.4 / FR3.1 / FR3.2: version は明示値（create = 1、update = expected_version + 1）で
  /// 受け取り、seq_nr 属性には封筒の実値（`event.seq_nr()`）を書く。`skey_seq_nr` は物理キーの
  /// 解決のみに使い（current = マーカー 0、履歴 = event.seq_nr() 由来）、属性値には流用しない
  /// （旧実装の skey 用引数の属性流用と `aggregate.version()` 導出の廃止）。
  /// NFR2.4: 封筒の分解は公開アクセサのみで行う。
  fn put_snapshot(
    &self,
    event: &EventEnvelope<AID, P>,
    skey_seq_nr: usize,
    version: usize,
    aggregate: &A,
  ) -> Result<Put, EventStoreWriteError> {
    let pkey = self.resolve_pkey(event.aggregate_id());
    let skey = self.resolve_skey(event.aggregate_id(), skey_seq_nr);
    // BR2.3 / FR4.2 / AC2.3.3: payload は純ドメイン内容のみ（メタデータ注入つき直列化の廃止）
    let payload = self.snapshot_serializer.serialize(aggregate)?;
    Put::builder()
      .table_name(self.snapshot_table_name.clone())
      .item("pkey", AttributeValue::S(pkey))
      .item("skey", AttributeValue::S(skey))
      .item("payload", AttributeValue::B(Blob::new(payload)))
      .item("aid", AttributeValue::S(event.aggregate_id().to_string()))
      .item("seq_nr", AttributeValue::N(event.seq_nr().to_string()))
      .item("version", AttributeValue::N(version.to_string()))
      .item("ttl", AttributeValue::N("0".to_string()))
      .item(
        "last_updated_at",
        AttributeValue::N(event.occurred_at().timestamp_millis().to_string()),
      )
      .condition_expression("attribute_not_exists(pkey) AND attribute_not_exists(skey)")
      .build()
      .map_err(|err| EventStoreWriteError::IOError(err.into()))
  }

  /// current スナップショット項目の条件付き Update を構築する。
  ///
  /// BR1.1 / P7: 条件式 `#version=:before_version` が唯一の競合判定点（現行維持）。
  /// BR2.4: version（expected_version + 1 の明示値）と last_updated_at は常時更新し、
  /// seq_nr / payload はスナップショット付き更新（aggregate あり）のときだけ実値
  /// （`event.seq_nr()`）で更新する。イベントのみ更新（aggregate なし）では集約状態が
  /// 変わらないため据え置く。
  fn update_snapshot(
    &self,
    event: &EventEnvelope<AID, P>,
    expected_version: usize,
    aggregate: Option<&A>,
  ) -> Result<Update, EventStoreWriteError> {
    let pkey = self.resolve_pkey(event.aggregate_id());
    let skey = self.current_snapshot_skey(event.aggregate_id());
    let mut update_snapshot = Update::builder()
      .table_name(self.snapshot_table_name.clone())
      .update_expression("SET #version=:after_version, #last_updated_at=:last_updated_at")
      .key("pkey", AttributeValue::S(pkey))
      .key("skey", AttributeValue::S(skey))
      .expression_attribute_names("#version", "version")
      .expression_attribute_names("#last_updated_at", "last_updated_at")
      .expression_attribute_values(":before_version", AttributeValue::N(expected_version.to_string()))
      .expression_attribute_values(":after_version", AttributeValue::N((expected_version + 1).to_string()))
      .expression_attribute_values(
        ":last_updated_at",
        AttributeValue::N(event.occurred_at().timestamp_millis().to_string()),
      )
      .condition_expression("#version=:before_version");
    if let Some(aggregate) = aggregate {
      // BR2.3 / AC2.3.3: payload は純ドメイン内容のみ
      let payload = self.snapshot_serializer.serialize(aggregate)?;
      update_snapshot = update_snapshot
        .update_expression(
          "SET #payload=:payload, #seq_nr=:seq_nr, #version=:after_version, #last_updated_at=:last_updated_at",
        )
        .expression_attribute_names("#seq_nr", "seq_nr")
        .expression_attribute_names("#payload", "payload")
        .expression_attribute_values(":seq_nr", AttributeValue::N(event.seq_nr().to_string()))
        .expression_attribute_values(":payload", AttributeValue::B(Blob::new(payload)));
    }
    update_snapshot
      .build()
      .map_err(|err| EventStoreWriteError::IOError(err.into()))
  }

  /// journal へ封筒 1 件を 1 アイテムとして書く Put を構築する。
  ///
  /// BR2.1 / FR6.1 / AC2.1.1: 封筒メタデータ 4 点（aid / seq_nr / occurred_at / manifest）+
  /// payload を属性へ 1:1 で書く。manifest 属性は v3 新設で、省略時は空文字列がそのまま
  /// 格納される（U1 BR1.2 のラウンドトリップ — AC5.1.1）。payload の直列化対象は
  /// `event.payload()` のみ（BR2.3 — イベント丸ごと直列化の廃止）。
  fn put_journal(&self, event: &EventEnvelope<AID, P>) -> Result<Put, EventStoreWriteError> {
    let pkey = self.resolve_pkey(event.aggregate_id());
    let skey = self.resolve_skey(event.aggregate_id(), event.seq_nr());
    let payload = self.event_serializer.serialize(event.payload())?;
    let occurred_at = format_occurred_at(event.occurred_at())?;
    Put::builder()
      .table_name(self.journal_table_name.clone())
      .item("pkey", AttributeValue::S(pkey))
      .item("skey", AttributeValue::S(skey))
      .item("aid", AttributeValue::S(event.aggregate_id().to_string()))
      .item("seq_nr", AttributeValue::N(event.seq_nr().to_string()))
      .item("payload", AttributeValue::B(Blob::new(payload)))
      .item("occurred_at", AttributeValue::N(occurred_at))
      .item("manifest", AttributeValue::S(event.manifest().to_string()))
      .build()
      .map_err(|err| EventStoreWriteError::IOError(err.into()))
  }

  /// journal アイテムの属性群からイベント封筒を再構成する。
  ///
  /// BR2.2 / AC4.1.1: aid / seq_nr / occurred_at / manifest / payload の全属性を使い、
  /// 属性欠落は破損データとして読取エラーにする（payload 単独 deserialize の廃止 — FR5.1）。
  /// NFR2.4: 封筒の構築は `EventEnvelope::new` + `with_manifest` の公開ビルダーのみで行う。
  fn event_envelope_from_item(
    &self,
    aid: &AID,
    item: &HashMap<String, AttributeValue>,
  ) -> Result<EventEnvelope<AID, P>, EventStoreReadError> {
    let stored_aid = required_string(item, "aid", "journal")?;
    if stored_aid != aid.to_string() {
      return Err(EventStoreReadError::OtherError(format!(
        "journal aid attribute does not match the requested aggregate: aid={}",
        aid
      )));
    }
    let seq_nr = parse_usize_attr(item, "seq_nr", "journal")?;
    let occurred_at = parse_occurred_at(required_number(item, "occurred_at", "journal")?)?;
    let manifest = required_string(item, "manifest", "journal")?.to_string();
    let payload = self
      .event_serializer
      .deserialize(required_blob(item, "payload", "journal")?)?;
    Ok(EventEnvelope::new(aid.clone(), seq_nr, occurred_at, payload).with_manifest(manifest))
  }

  async fn maintain_snapshots(
    &self,
    aggregate_id: &AID,
    maintenance: &SnapshotMaintenance,
  ) -> Result<(), EventStoreWriteError> {
    // BR3.1 / FR6.3: keep_snapshot_count = None は剪定しない（履歴 Put も行われないため
    // 対象が存在しない）。Some(0) は U1 BR4.1 のビルダー拒否によりここへ到達しない
    let keep_snapshot_count = match maintenance.keep_snapshot_count {
      Some(count) if count > 0 => count,
      _ => return Ok(()),
    };
    if let Some(delete_ttl) = maintenance.delete_ttl {
      self
        .update_ttl_of_excess_snapshots(aggregate_id, keep_snapshot_count, delete_ttl)
        .await
    } else {
      self.delete_excess_snapshots(aggregate_id, keep_snapshot_count).await
    }
  }

  async fn delete_excess_snapshots(
    &self,
    aggregate_id: &AID,
    keep_snapshot_count: usize,
  ) -> Result<(), EventStoreWriteError> {
    // BR3.1: excess = 件数 −（keep + 1）の現行意味論を維持する（current 1 + 履歴 n 件）
    let snapshot_count = self
      .get_snapshot_count(aggregate_id)
      .await
      .map_err(|err| EventStoreWriteError::OtherError(err.to_string()))?;
    let excess_count = snapshot_count.saturating_sub(keep_snapshot_count + 1);
    if excess_count == 0 {
      return Ok(());
    }
    let keys = self
      .get_last_snapshot_keys(aggregate_id, excess_count)
      .await
      .map_err(|err| EventStoreWriteError::OtherError(err.to_string()))?;
    if keys.is_empty() {
      return Ok(());
    }
    let request_items = keys
      .into_iter()
      .map(|(pkey, skey)| {
        // BR4.1 / FR6.5 / P8: DeleteRequest ビルダーの build() Result もエラー写像し、
        // panic 経路を残さない（同ファイルの Put ビルダーの map_err と同形）
        let delete_request = DeleteRequest::builder()
          .key("pkey", AttributeValue::S(pkey))
          .key("skey", AttributeValue::S(skey))
          .build()
          .map_err(|err| EventStoreWriteError::IOError(err.into()))?;
        Ok(WriteRequest::builder().delete_request(delete_request).build())
      })
      .collect::<Result<Vec<_>, EventStoreWriteError>>()?;
    let result = self
      .client
      .batch_write_item()
      .request_items(self.snapshot_table_name.clone(), request_items)
      .send()
      .instrument(tracing::debug_span!("batch_write_item"))
      .await;
    match result {
      Ok(_) => Ok(()),
      Err(err) => Err(EventStoreWriteError::IOError(Box::new(err.into_service_error()))),
    }
  }

  async fn update_ttl_of_excess_snapshots(
    &self,
    aggregate_id: &AID,
    keep_snapshot_count: usize,
    delete_ttl: Duration,
  ) -> Result<(), EventStoreWriteError> {
    let snapshot_count = self
      .get_snapshot_count(aggregate_id)
      .await
      .map_err(|err| EventStoreWriteError::OtherError(err.to_string()))?;
    let excess_count = snapshot_count.saturating_sub(keep_snapshot_count + 1);
    if excess_count == 0 {
      return Ok(());
    }
    let keys = self
      .get_last_snapshot_keys(aggregate_id, excess_count)
      .await
      .map_err(|err| EventStoreWriteError::OtherError(err.to_string()))?;
    if keys.is_empty() {
      return Ok(());
    }
    let ttl = (Utc::now() + delete_ttl).timestamp();
    for (pkey, skey) in keys {
      let result = self
        .client
        .update_item()
        .table_name(self.snapshot_table_name.clone())
        .key("pkey", AttributeValue::S(pkey))
        .key("skey", AttributeValue::S(skey))
        .update_expression("SET #ttl=:ttl")
        .expression_attribute_names("#ttl", "ttl")
        .expression_attribute_values(":ttl", AttributeValue::N(ttl.to_string()))
        .send()
        .await;
      if let Err(err) = result {
        return Err(EventStoreWriteError::IOError(err.into()));
      }
    }
    Ok(())
  }

  async fn get_snapshot_count(&self, aid: &AID) -> Result<usize, EventStoreReadError> {
    let response = self
      .client
      .query()
      .table_name(self.snapshot_table_name.clone())
      .index_name(self.snapshot_aid_index_name.clone())
      .key_condition_expression("#aid = :aid")
      .expression_attribute_names("#aid", "aid")
      .expression_attribute_values(":aid", AttributeValue::S(aid.to_string()))
      .select(Select::Count)
      .send()
      .await;
    match response {
      Err(err) => Err(EventStoreReadError::IOError(Box::new(err.into_service_error()))),
      Ok(response) => Ok(response.count as usize),
    }
  }

  /// 剪定対象候補のスナップショット項目キーを seq_nr 降順で `limit` 件返す。
  ///
  /// BR2.4 で current 項目の seq_nr 属性が実値化されたため、旧実装の「seq_nr > 0」条件では
  /// current 項目を候補から除外できない。候補を 1 件余分に取得し、current 項目
  /// （skey マーカー 0）をコードで除外してから limit 件に切り詰める（走査順は現行維持 —
  /// seq_nr 降順。BR3.1 の件数意味論は不変）。
  async fn get_last_snapshot_keys(
    &self,
    aid: &AID,
    limit: usize,
  ) -> Result<Vec<(String, String)>, EventStoreReadError> {
    let current_skey = self.current_snapshot_skey(aid);
    let response = self
      .client
      .query()
      .table_name(self.snapshot_table_name.clone())
      .index_name(self.snapshot_aid_index_name.clone())
      .key_condition_expression("#aid = :aid")
      .expression_attribute_names("#aid", "aid")
      .expression_attribute_values(":aid", AttributeValue::S(aid.to_string()))
      .limit((limit + 1) as i32)
      .scan_index_forward(false)
      .send()
      .await;
    match response {
      Err(err) => Err(EventStoreReadError::IOError(Box::new(err.into_service_error()))),
      Ok(response) => {
        let mut keys = Vec::new();
        if let Some(items) = response.items {
          for item in items {
            let pkey = item.get("pkey").and_then(|v| v.as_s().ok()).cloned();
            let skey = item.get("skey").and_then(|v| v.as_s().ok()).cloned();
            if let (Some(pkey), Some(skey)) = (pkey, skey) {
              if skey != current_skey {
                keys.push((pkey, skey));
              }
            }
          }
        }
        keys.truncate(limit);
        Ok(keys)
      }
    }
  }
}

#[async_trait]
impl<AID, A, P> StorageBackend<AID, A, P> for DynamoDbBackend<AID, A, P>
where
  AID: AggregateId,
  A: Serialize + DeserializeOwned + Send + Sync + 'static,
  P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
  async fn fetch_latest_snapshot(&self, aid: &AID) -> Result<Option<SnapshotEnvelope<A>>, EventStoreReadError> {
    // BR2.4 の帰結: current 項目の seq_nr 属性が実値化されたため、旧実装の「aid インデックスへの
    // seq_nr = 0 Query」では current 項目を特定できない。主キー（pkey + skey マーカー 0）の
    // GetItem で取得する。読取済み version を次回書込の expected_version に使うため（W4）、
    // 自身の書込を読み戻せる強整合読取にする（GSI Query では選択不可能だった）
    let pkey = self.resolve_pkey(aid);
    let skey = self.current_snapshot_skey(aid);
    let response = self
      .client
      .get_item()
      .table_name(self.snapshot_table_name.clone())
      .key("pkey", AttributeValue::S(pkey))
      .key("skey", AttributeValue::S(skey))
      .consistent_read(true)
      .send()
      .await;
    let item = match response {
      Err(err) => return Err(EventStoreReadError::IOError(err.into())),
      Ok(response) => match response.item {
        None => return Ok(None),
        Some(item) => item,
      },
    };
    // FR4.3 / U1 BR2.5: 読取後の set_version 補正は存在しない — version / seq_nr は
    // 属性値をそのまま封筒に載せて返す（属性欠落は破損データとして読取エラー）
    let version = parse_usize_attr(&item, "version", "snapshot")?;
    let seq_nr = parse_usize_attr(&item, "seq_nr", "snapshot")?;
    let aggregate = self
      .snapshot_serializer
      .deserialize(required_blob(&item, "payload", "snapshot")?)?;
    Ok(Some(SnapshotEnvelope::new(aggregate, seq_nr, version)))
  }

  async fn fetch_events_since(
    &self,
    aid: &AID,
    seq_nr: usize,
  ) -> Result<Vec<EventEnvelope<AID, P>>, EventStoreReadError> {
    let response = self
      .client
      .query()
      .table_name(self.journal_table_name.clone())
      .index_name(self.journal_aid_index_name.clone())
      .key_condition_expression("#aid = :aid AND #seq_nr >= :seq_nr")
      .expression_attribute_names("#aid", "aid")
      .expression_attribute_names("#seq_nr", "seq_nr")
      .expression_attribute_values(":aid", AttributeValue::S(aid.to_string()))
      .expression_attribute_values(":seq_nr", AttributeValue::N(seq_nr.to_string()))
      .send()
      .await;
    match response {
      Err(err) => Err(EventStoreReadError::IOError(err.into())),
      Ok(response) => {
        let items = response.items.unwrap_or_default();
        let mut events = Vec::with_capacity(items.len());
        for item in items {
          // BR2.2: 各アイテムの属性群から封筒を再構成する（属性欠落は読取エラー）
          events.push(self.event_envelope_from_item(aid, &item)?);
        }
        Ok(events)
      }
    }
  }

  async fn create_event_and_snapshot(
    &self,
    event: &EventEnvelope<AID, P>,
    aggregate: &A,
    maintenance: &SnapshotMaintenance,
  ) -> Result<(), EventStoreWriteError> {
    // BR1.1 / P7 / NFR4.3: journal Put + current Put（+ 条件付き履歴 Put）を単一の
    // TransactWriteItems に束ね、条件式 attribute_not_exists(pkey) AND attribute_not_exists(skey)
    // を唯一の競合判定点にする（現行維持）
    let mut builder = self
      .client
      .transact_write_items()
      .transact_items(
        TransactWriteItem::builder()
          // W1: current 項目は version = 1（固定値）、seq_nr 属性 = event.seq_nr()（実値 — BR2.4）
          .put(self.put_snapshot(event, CURRENT_SNAPSHOT_SKEY_MARKER, 1, aggregate)?)
          .build(),
      )
      .transact_items(TransactWriteItem::builder().put(self.put_journal(event)?).build());
    if maintenance.keep_snapshot_count.is_some() {
      // BR3.1: keep_snapshot_count = Some(n) のときのみ履歴項目（skey / seq_nr は
      // event.seq_nr() 由来）を同一トランザクションに含める（現行の条件付き 2 本目 Put の維持）
      builder = builder.transact_items(
        TransactWriteItem::builder()
          .put(self.put_snapshot(event, event.seq_nr(), 1, aggregate)?)
          .build(),
      );
    }
    let result = builder.send().await;
    // 新規作成の expected_version は 0（U1 の W1 規約）
    write_error_handling(result, &event.aggregate_id().to_string(), 0)
  }

  async fn update_event_and_snapshot(
    &self,
    event: &EventEnvelope<AID, P>,
    aggregate: Option<&A>,
    expected_version: usize,
    maintenance: &SnapshotMaintenance,
  ) -> Result<(), EventStoreWriteError> {
    // BR1.1 / P7: journal Put + current Update（条件式 #version=:before_version）を単一の
    // TransactWriteItems に束ねる（現行維持）。不在集約への更新も同じ条件式の不成立として
    // OptimisticLockError（actual なし — U1 リファレンス意味論）になる（AC2.2.1）
    let mut builder = self
      .client
      .transact_write_items()
      .transact_items(
        TransactWriteItem::builder()
          .update(self.update_snapshot(event, expected_version, aggregate)?)
          .build(),
      )
      .transact_items(TransactWriteItem::builder().put(self.put_journal(event)?).build());
    if let (Some(aggregate), Some(_)) = (aggregate, maintenance.keep_snapshot_count) {
      // BR3.1 / BR2.4: 履歴項目は keep_snapshot_count = Some(n) かつスナップショット付き更新の
      // ときのみ。skey / seq_nr 属性は event.seq_nr()、version は expected_version + 1 の明示値
      builder = builder.transact_items(
        TransactWriteItem::builder()
          .put(self.put_snapshot(event, event.seq_nr(), expected_version + 1, aggregate)?)
          .build(),
      );
    }
    let result = builder.send().await;
    write_error_handling(result, &event.aggregate_id().to_string(), expected_version)
  }

  async fn on_event_persisted(&self, aid: &AID, maintenance: &SnapshotMaintenance) -> Result<(), EventStoreWriteError> {
    // BR3.1 / FR6.3 / AC2.3.4: 保持ポリシーの実行点（ttl 更新 / 削除の 2 モード — 現行維持）
    self.maintain_snapshots(aid, maintenance).await
  }
}

// BR1.1 / BR5.1 / P9: TransactWriteItems の失敗写像点。条件不成立（TransactionCanceledException）
// のみ OptimisticLockError（統一書式 — 現行維持）とし、その他の SDK エラーは書式化展開ゼロで
// IOError のソースとして保持する（資格情報・エンドポイント・テーブル名を展開しない — NFR3.7）
fn write_error_handling(
  result: Result<TransactWriteItemsOutput, SdkError<TransactWriteItemsError>>,
  aid: &str,
  expected_version: usize,
) -> Result<(), EventStoreWriteError> {
  match result {
    Ok(_) => Ok(()),
    Err(e) => match e.into_service_error() {
      TransactWriteItemsError::TransactionCanceledException(e) => {
        if !e.cancellation_reasons().is_empty() {
          // 実バージョンはSDKエラーから判明しないため付加しない（BR1.2の判明分のみ規約）
          Err(EventStoreWriteError::OptimisticLockError(
            format_optimistic_lock_message(aid, expected_version, None),
          ))
        } else {
          Err(EventStoreWriteError::IOError(e.into()))
        }
      }
      error => Err(EventStoreWriteError::IOError(error.into())),
    },
  }
}

// FR1.4 / U1 BR1.3 / C3: occurred_at はドメイン供給値のまま保存・読出しする。現行の epoch millis
// 格納ではサブミリ秒精度が失われ供給値の完全往復（共有シナリオの occurred_at 同値 assert）を
// 満たせないため、N 属性の epoch nanos で格納する（U3 申し送りの精度要件。entities.md の
// 論理型 N は維持し、精度のみ millis → nanos に置換）。i64 nanos の表現範囲外
// （およそ西暦 1677〜2262 年の外）は黙って切り詰めずエラーで拒否する（NFR3.6 — fail fast）
fn format_occurred_at(occurred_at: &DateTime<Utc>) -> Result<String, EventStoreWriteError> {
  occurred_at
    .timestamp_nanos_opt()
    .map(|nanos| nanos.to_string())
    .ok_or_else(|| EventStoreWriteError::OtherError("occurred_at is out of the epoch-nanos range".to_string()))
}

fn parse_occurred_at(text: &str) -> Result<DateTime<Utc>, EventStoreReadError> {
  text
    .parse::<i64>()
    .map(DateTime::from_timestamp_nanos)
    .map_err(|err| EventStoreReadError::OtherError(err.to_string()))
}

// BR2.2 / P9: 属性欠落・型不一致は破損データとして読取エラーにする
// （メッセージはアイテム種別と属性名のみで、値・テーブル名は展開しない）
fn required_attr<'a>(
  item: &'a HashMap<String, AttributeValue>,
  name: &'static str,
  item_kind: &'static str,
) -> Result<&'a AttributeValue, EventStoreReadError> {
  item
    .get(name)
    .ok_or_else(|| EventStoreReadError::OtherError(format!("{} item attribute is missing: {}", item_kind, name)))
}

fn attr_type_error(name: &str, item_kind: &str) -> EventStoreReadError {
  EventStoreReadError::OtherError(format!("{} item attribute has an unexpected type: {}", item_kind, name))
}

fn required_string<'a>(
  item: &'a HashMap<String, AttributeValue>,
  name: &'static str,
  item_kind: &'static str,
) -> Result<&'a str, EventStoreReadError> {
  required_attr(item, name, item_kind)?
    .as_s()
    .map(|s| s.as_str())
    .map_err(|_| attr_type_error(name, item_kind))
}

fn required_blob<'a>(
  item: &'a HashMap<String, AttributeValue>,
  name: &'static str,
  item_kind: &'static str,
) -> Result<&'a [u8], EventStoreReadError> {
  required_attr(item, name, item_kind)?
    .as_b()
    .map(|b| b.as_ref())
    .map_err(|_| attr_type_error(name, item_kind))
}

fn required_number<'a>(
  item: &'a HashMap<String, AttributeValue>,
  name: &'static str,
  item_kind: &'static str,
) -> Result<&'a str, EventStoreReadError> {
  required_attr(item, name, item_kind)?
    .as_n()
    .map(|s| s.as_str())
    .map_err(|_| attr_type_error(name, item_kind))
}

fn parse_usize_attr(
  item: &HashMap<String, AttributeValue>,
  name: &'static str,
  item_kind: &'static str,
) -> Result<usize, EventStoreReadError> {
  required_number(item, name, item_kind)?
    .parse::<usize>()
    .map_err(|err| EventStoreReadError::OtherError(err.to_string()))
}
