//! 新契約のインメモリ保存先。
mod backend;
#[cfg(test)]
mod retention_test;
mod storage;
use crate::{
  aggregate_id::AggregateId,
  error::EventStoreError,
  event_envelope::{EventEnvelope, SnapshotEnvelope, SnapshotRead},
  event_store::EventStore,
  generic_event_store::GenericEventStore,
  seq_nr::SeqNr,
  serializer::{EventSerializer, JsonEventSerializer, JsonSnapshotSerializer, SnapshotSerializer},
};
use async_trait::async_trait;
use backend::MemoryBackend;
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
pub use storage::MemoryStorage;
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub trait MemoryTestHooks: std::fmt::Debug + Send + Sync {
  fn before_commit(&self, aid: &crate::aggregate_id::AidString, seq_nr: SeqNr) -> Result<(), EventStoreError>;
  fn read_events(&self, aid: &crate::aggregate_id::AidString) -> Result<(), EventStoreError>;
  fn read_snapshot(&self, aid: &crate::aggregate_id::AidString) -> Result<(), EventStoreError>;
  fn retention_visible_history(
    &self,
    _aid: &crate::aggregate_id::AidString,
    history: &[SeqNr],
    _just_written: Option<SeqNr>,
  ) -> Result<Vec<SeqNr>, EventStoreError> {
    Ok(history.to_vec())
  }
  fn retention_delete(&self, _aid: &crate::aggregate_id::AidString, _seq_nrs: &[SeqNr]) -> Result<(), EventStoreError> {
    Ok(())
  }
}
pub struct EventStoreForMemory<AID, A, P> {
  inner: GenericEventStore<AID, A, P, MemoryBackend<A, P>>,
}
impl<AID, A, P> Clone for EventStoreForMemory<AID, A, P> {
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}
impl<AID, A, P> std::fmt::Debug for EventStoreForMemory<AID, A, P> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("EventStoreForMemory")
      .field("inner", &self.inner)
      .finish()
  }
}
impl<AID, A, P> EventStoreForMemory<AID, A, P> {
  pub fn with_serializers(
    storage: MemoryStorage,
    event_serializer: Arc<dyn EventSerializer<P>>,
    snapshot_serializer: Arc<dyn SnapshotSerializer<A>>,
  ) -> Self {
    Self {
      inner: GenericEventStore::new(MemoryBackend {
        storage,
        events: event_serializer,
        snapshots: snapshot_serializer,
      }),
    }
  }
}
impl<AID, A, P> EventStoreForMemory<AID, A, P>
where
  A: Serialize + DeserializeOwned + Send + Sync + 'static,
  P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
  pub fn new(storage: MemoryStorage) -> Self {
    Self::with_serializers(
      storage,
      Arc::new(JsonEventSerializer::new()),
      Arc::new(JsonSnapshotSerializer::new()),
    )
  }
}
#[async_trait]
impl<AID: AggregateId, A: Send + Sync + 'static, P: Send + Sync + 'static> EventStore
  for EventStoreForMemory<AID, A, P>
{
  type A = A;
  type AID = AID;
  type P = P;

  async fn persist_event(&self, event: EventEnvelope<AID, P>) -> Result<(), EventStoreError> {
    self.inner.persist_event(event).await
  }

  async fn persist_event_and_snapshot(
    &self,
    event: EventEnvelope<AID, P>,
    snapshot: SnapshotEnvelope<A>,
  ) -> Result<(), EventStoreError> {
    self.inner.persist_event_and_snapshot(event, snapshot).await
  }

  async fn get_latest_snapshot_by_id(&self, aid: &AID) -> Result<Option<SnapshotRead<A>>, EventStoreError> {
    self.inner.get_latest_snapshot_by_id(aid).await
  }

  async fn get_events_by_id_since_seq_nr(
    &self,
    aid: &AID,
    seq_nr: SeqNr,
  ) -> Result<Vec<EventEnvelope<AID, P>>, EventStoreError> {
    self.inner.get_events_by_id_since_seq_nr(aid, seq_nr).await
  }
}
