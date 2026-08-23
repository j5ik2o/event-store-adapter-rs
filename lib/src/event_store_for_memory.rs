use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use chrono::Duration;
use tracing::instrument;

use crate::event_store_backend::{SnapshotEnvelope, SnapshotMaintenance, StorageBackend};
use crate::generic_event_store::GenericEventStore;
use crate::types::{
  format_optimistic_lock_message, Aggregate, AggregateId, Event, EventStore, EventStoreReadError, EventStoreWriteError,
};

/// Event Store for On-Memory
#[derive(Debug)]
pub struct EventStoreForMemory<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>, {
  inner: GenericEventStore<AID, A, E, InMemoryBackend<AID, A, E>>,
}

impl<AID, A, E> EventStoreForMemory<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>,
{
  pub fn new() -> Self {
    Self {
      inner: GenericEventStore::new(InMemoryBackend::new()),
    }
  }

  pub fn with_keep_snapshot_count(mut self, keep_snapshot_count: Option<usize>) -> Self {
    self.inner = self.inner.with_keep_snapshot_count(keep_snapshot_count);
    self
  }

  pub fn with_delete_ttl(mut self, delete_ttl: Option<Duration>) -> Self {
    self.inner = self.inner.with_delete_ttl(delete_ttl);
    self
  }

  pub fn maintenance(&self) -> &SnapshotMaintenance {
    self.inner.maintenance()
  }
}

impl<AID, A, E> Default for EventStoreForMemory<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>,
{
  fn default() -> Self {
    Self::new()
  }
}

impl<AID, A, E> Clone for EventStoreForMemory<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>,
{
  // クローン間で同一ストアを共有する（Arcの共有クローン — Clone時の状態分岐を作らない）
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}

#[async_trait]
impl<AID, A, E> EventStore for EventStoreForMemory<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>,
{
  type AG = A;
  type AID = AID;
  type EV = E;

  #[instrument]
  async fn persist_event(&mut self, event: &Self::EV, version: usize) -> Result<(), EventStoreWriteError> {
    self.inner.persist_event(event, version).await
  }

  #[instrument]
  async fn persist_event_and_snapshot(
    &mut self,
    event: &Self::EV,
    aggregate: &Self::AG,
  ) -> Result<(), EventStoreWriteError> {
    self.inner.persist_event_and_snapshot(event, aggregate).await
  }

  #[instrument]
  async fn get_latest_snapshot_by_id(&self, aid: &Self::AID) -> Result<Option<Self::AG>, EventStoreReadError> {
    self.inner.get_latest_snapshot_by_id(aid).await
  }

  #[instrument]
  async fn get_events_by_id_since_seq_nr(
    &self,
    aid: &Self::AID,
    seq_nr: usize,
  ) -> Result<Vec<Self::EV>, EventStoreReadError> {
    self.inner.get_events_by_id_since_seq_nr(aid, seq_nr).await
  }
}

/// Memoryバックエンドの内部状態（キー=集約ID文字列。公開APIへ露出しない）
#[derive(Debug)]
struct InMemoryStoreState<A, E> {
  events: Vec<E>,
  snapshots: Vec<SnapshotEnvelope<A>>,
}

impl<A, E> InMemoryStoreState<A, E> {
  fn new() -> Self {
    Self {
      events: Vec::new(),
      snapshots: Vec::new(),
    }
  }
}

type StoreStateMap<A, E> = HashMap<String, InMemoryStoreState<A, E>>;

// fnポインタ経由の型マーカー — Send/Sync自動導出を阻害しない
type TypeMarker<AID, A, E> = fn() -> (AID, A, E);

#[derive(Debug)]
struct InMemoryBackend<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>, {
  state: Arc<Mutex<StoreStateMap<A, E>>>,
  _marker: PhantomData<TypeMarker<AID, A, E>>,
}

impl<AID, A, E> InMemoryBackend<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>,
{
  fn new() -> Self {
    Self {
      state: Arc::new(Mutex::new(HashMap::new())),
      _marker: PhantomData,
    }
  }

  // ロックは各メソッド内で取得・解放し、ガード保持中に `.await` しない。
  // ポイズニングはpanicさせず文字列化してエラーへ写像する（ガード起因の機密混入なし）。
  fn lock_state(&self) -> Result<MutexGuard<'_, StoreStateMap<A, E>>, String> {
    self
      .state
      .lock()
      .map_err(|_| "memory store mutex is poisoned".to_string())
  }
}

impl<AID, A, E> Clone for InMemoryBackend<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>,
{
  // Arcの共有クローン — deriveはAID/A/EへのClone境界を要求するため手動実装とする
  fn clone(&self) -> Self {
    Self {
      state: Arc::clone(&self.state),
      _marker: PhantomData,
    }
  }
}

#[async_trait]
impl<AID, A, E> StorageBackend<AID, A, E> for InMemoryBackend<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>,
{
  async fn fetch_latest_snapshot(&self, aid: &AID) -> Result<Option<SnapshotEnvelope<A>>, EventStoreReadError> {
    let state = self.lock_state().map_err(EventStoreReadError::OtherError)?;
    Ok(
      state
        .get(&aid.to_string())
        .and_then(|entry| entry.snapshots.last().cloned()),
    )
  }

  async fn fetch_events_since(&self, aid: &AID, seq_nr: usize) -> Result<Vec<E>, EventStoreReadError> {
    let state = self.lock_state().map_err(EventStoreReadError::OtherError)?;
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
    event: &E,
    aggregate: &A,
    _maintenance: &SnapshotMaintenance,
  ) -> Result<(), EventStoreWriteError> {
    let aid = event.aggregate_id().to_string();
    let mut state = self.lock_state().map_err(EventStoreWriteError::OtherError)?;
    let entry = state.entry(aid.clone()).or_insert_with(InMemoryStoreState::new);
    if let Some(latest) = entry.snapshots.last() {
      return Err(EventStoreWriteError::OptimisticLockError(
        format_optimistic_lock_message(&aid, aggregate.version(), Some(latest.version)),
      ));
    }
    entry.snapshots.push(SnapshotEnvelope {
      aggregate: aggregate.clone(),
      seq_nr: aggregate.seq_nr(),
      version: aggregate.version(),
    });
    entry.events.push(event.clone());
    Ok(())
  }

  async fn update_event_and_snapshot(
    &self,
    event: &E,
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
    if latest.version != expected_version {
      return Err(EventStoreWriteError::OptimisticLockError(
        format_optimistic_lock_message(&aid, expected_version, Some(latest.version)),
      ));
    }
    let new_version = expected_version + 1;
    latest.version = new_version;
    if let Some(aggregate) = aggregate {
      let mut stored = aggregate.clone();
      stored.set_version(new_version);
      latest.seq_nr = aggregate.seq_nr();
      latest.aggregate = stored;
    } else {
      latest.aggregate.set_version(new_version);
    }
    entry.events.push(event.clone());
    Ok(())
  }
}
