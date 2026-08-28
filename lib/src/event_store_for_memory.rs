use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use chrono::Duration;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tracing::instrument;

use crate::event_envelope::{EventEnvelope, SnapshotEnvelope};
use crate::event_store_backend::{SnapshotMaintenance, StorageBackend};
use crate::generic_event_store::GenericEventStore;
use crate::types::{
  format_optimistic_lock_message, AggregateId, EventStore, EventStoreReadError, EventStoreWriteError,
};

// FR6.1: Memory バックエンドの封筒保持への構造化。読取時の `set_version` 書き戻しは
// 廃止し、version の正は SnapshotEnvelope（列相当）側に一本化する（FR4.3 / BR2.5）。
// BR1.6: この impl のみ payload（A / P）へ追加で `Clone` を要求する（直列化なしで
// 所有権付き返却するため — 文書化済みの非対称）。

/// Event Store for On-Memory
pub struct EventStoreForMemory<AID, A, P> {
  inner: GenericEventStore<AID, A, P, InMemoryBackend<AID, A, P>>,
}

// P2: derive は型パラメータ（AID / A / P）へ Debug / Clone 境界を課すため手動 impl とし、
// payload への要求を実フィールド由来のものに限定する
impl<AID, A, P> Debug for EventStoreForMemory<AID, A, P> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("EventStoreForMemory").finish()
  }
}

impl<AID, A, P> Clone for EventStoreForMemory<AID, A, P> {
  // クローン間で同一ストアを共有する（Arcの共有クローン — Clone時の状態分岐を作らない）
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}

impl<AID, A, P> EventStoreForMemory<AID, A, P> {
  pub fn new() -> Self {
    Self {
      inner: GenericEventStore::new(InMemoryBackend::new()),
    }
  }

  /// 保持するスナップショット履歴数を設定する。
  ///
  /// BR4.1 / P3: 検証（`Some(0)` の拒否）は `GenericEventStore` 側の 1 箇所で行い、
  /// このラッパーは Result を素通しする。
  pub fn with_keep_snapshot_count(mut self, keep_snapshot_count: Option<usize>) -> Result<Self, EventStoreWriteError> {
    self.inner = self.inner.with_keep_snapshot_count(keep_snapshot_count)?;
    Ok(self)
  }

  pub fn with_delete_ttl(mut self, delete_ttl: Option<Duration>) -> Self {
    self.inner = self.inner.with_delete_ttl(delete_ttl);
    self
  }

  pub fn maintenance(&self) -> &SnapshotMaintenance {
    self.inner.maintenance()
  }
}

impl<AID, A, P> Default for EventStoreForMemory<AID, A, P> {
  fn default() -> Self {
    Self::new()
  }
}

#[async_trait]
impl<AID, A, P> EventStore for EventStoreForMemory<AID, A, P>
where
  AID: AggregateId,
  A: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
  P: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
  type A = A;
  type AID = AID;
  type P = P;

  #[instrument(skip_all)]
  async fn persist_event(
    &mut self,
    event: EventEnvelope<Self::AID, Self::P>,
    expected_version: usize,
  ) -> Result<(), EventStoreWriteError> {
    self.inner.persist_event(event, expected_version).await
  }

  #[instrument(skip_all)]
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

  #[instrument(skip_all)]
  async fn get_latest_snapshot_by_id(
    &self,
    aid: &Self::AID,
  ) -> Result<Option<SnapshotEnvelope<Self::A>>, EventStoreReadError> {
    self.inner.get_latest_snapshot_by_id(aid).await
  }

  #[instrument(skip_all)]
  async fn get_events_by_id_since_seq_nr(
    &self,
    aid: &Self::AID,
    seq_nr: usize,
  ) -> Result<Vec<EventEnvelope<Self::AID, Self::P>>, EventStoreReadError> {
    self.inner.get_events_by_id_since_seq_nr(aid, seq_nr).await
  }
}

/// Memoryバックエンドの内部状態（キー=集約ID文字列。公開APIへ露出しない）
struct InMemoryStoreState<AID, A, P> {
  events: Vec<EventEnvelope<AID, P>>,
  snapshots: Vec<SnapshotEnvelope<A>>,
}

impl<AID, A, P> InMemoryStoreState<AID, A, P> {
  fn new() -> Self {
    Self {
      events: Vec::new(),
      snapshots: Vec::new(),
    }
  }
}

type StoreStateMap<AID, A, P> = HashMap<String, InMemoryStoreState<AID, A, P>>;

// fnポインタ経由の型マーカー — Send/Sync自動導出を阻害しない
type TypeMarker<AID, A, P> = fn() -> (AID, A, P);

struct InMemoryBackend<AID, A, P> {
  state: Arc<Mutex<StoreStateMap<AID, A, P>>>,
  _marker: PhantomData<TypeMarker<AID, A, P>>,
}

// P2: derive は AID / A / P へ Debug 境界を課すため手動 impl とする（内部状態は表示しない）
impl<AID, A, P> Debug for InMemoryBackend<AID, A, P> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("InMemoryBackend").finish()
  }
}

impl<AID, A, P> InMemoryBackend<AID, A, P> {
  fn new() -> Self {
    Self {
      state: Arc::new(Mutex::new(HashMap::new())),
      _marker: PhantomData,
    }
  }

  // ロックは各メソッド内で取得・解放し、ガード保持中に `.await` しない。
  // ポイズニングはpanicさせず文字列化してエラーへ写像する（BR5.1 — ガード起因の機密混入なし）。
  fn lock_state(&self) -> Result<MutexGuard<'_, StoreStateMap<AID, A, P>>, String> {
    self
      .state
      .lock()
      .map_err(|_| "memory store mutex is poisoned".to_string())
  }
}

impl<AID, A, P> Clone for InMemoryBackend<AID, A, P> {
  // Arcの共有クローン — deriveはAID/A/PへのClone境界を要求するため手動実装とする
  fn clone(&self) -> Self {
    Self {
      state: Arc::clone(&self.state),
      _marker: PhantomData,
    }
  }
}

#[async_trait]
impl<AID, A, P> StorageBackend<AID, A, P> for InMemoryBackend<AID, A, P>
where
  AID: AggregateId,
  A: Clone + Send + Sync + 'static,
  P: Clone + Send + Sync + 'static,
{
  async fn fetch_latest_snapshot(&self, aid: &AID) -> Result<Option<SnapshotEnvelope<A>>, EventStoreReadError> {
    let state = self.lock_state().map_err(EventStoreReadError::OtherError)?;
    // BR2.5: version / seq_nr は保存済み封筒（列相当）の値をそのまま返す — payload からの補正はない
    Ok(
      state
        .get(&aid.to_string())
        .and_then(|entry| entry.snapshots.last().cloned()),
    )
  }

  async fn fetch_events_since(
    &self,
    aid: &AID,
    seq_nr: usize,
  ) -> Result<Vec<EventEnvelope<AID, P>>, EventStoreReadError> {
    let state = self.lock_state().map_err(EventStoreReadError::OtherError)?;
    // BR3.2: 保存時の封筒（メタデータ込み）を seq_nr 昇順（挿入順）でそのまま返す
    Ok(match state.get(&aid.to_string()) {
      Some(entry) => entry
        .events
        .iter()
        .filter(|event| event.seq_nr() >= seq_nr)
        .cloned()
        .collect(),
      None => Vec::new(),
    })
  }

  async fn create_event_and_snapshot(
    &self,
    event: &EventEnvelope<AID, P>,
    aggregate: &A,
    _maintenance: &SnapshotMaintenance,
  ) -> Result<(), EventStoreWriteError> {
    let aid = event.aggregate_id().to_string();
    let mut state = self.lock_state().map_err(EventStoreWriteError::OtherError)?;
    let entry = state.entry(aid.clone()).or_insert_with(InMemoryStoreState::new);
    if let Some(latest) = entry.snapshots.last() {
      // 既存集約への新規作成は競合（W1）。新規作成の expected_version は 0（BR2.6 で保証済み）
      return Err(EventStoreWriteError::OptimisticLockError(
        format_optimistic_lock_message(&aid, 0, Some(latest.version())),
      ));
    }
    // W1: snapshot は version = 1、seq_nr = event.seq_nr() で作成する
    entry
      .snapshots
      .push(SnapshotEnvelope::new(aggregate.clone(), event.seq_nr(), 1));
    entry.events.push(event.clone());
    Ok(())
  }

  async fn update_event_and_snapshot(
    &self,
    event: &EventEnvelope<AID, P>,
    aggregate: Option<&A>,
    expected_version: usize,
    _maintenance: &SnapshotMaintenance,
  ) -> Result<(), EventStoreWriteError> {
    let aid = event.aggregate_id().to_string();
    let mut state = self.lock_state().map_err(EventStoreWriteError::OtherError)?;
    let entry = state
      .get_mut(&aid)
      .ok_or_else(|| EventStoreWriteError::OtherError(format!("snapshot not found for aggregate {}", aid)))?;
    let latest = entry
      .snapshots
      .last_mut()
      .ok_or_else(|| EventStoreWriteError::OtherError(format!("snapshot not found for aggregate {}", aid)))?;
    // BR2.3: version CAS。競合時は統一書式の楽観ロックエラー（NFR3.2 — 許可キーのみ）
    if latest.version() != expected_version {
      return Err(EventStoreWriteError::OptimisticLockError(
        format_optimistic_lock_message(&aid, expected_version, Some(latest.version())),
      ));
    }
    // FR4.3 / BR2.5: version 加算は列（封筒）側で行う。`set_version` 相当の
    // payload への書き戻しは存在しない
    let new_version = expected_version + 1;
    *latest = match aggregate {
      // W3: スナップショット付き更新 — aggregate / seq_nr / version を新値へ置換
      Some(aggregate) => SnapshotEnvelope::new(aggregate.clone(), event.seq_nr(), new_version),
      // W2: イベントのみ更新 — aggregate / seq_nr は据え置き、version のみ加算
      None => SnapshotEnvelope::new(latest.aggregate().clone(), latest.seq_nr(), new_version),
    };
    entry.events.push(event.clone());
    Ok(())
  }
}
