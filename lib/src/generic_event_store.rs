use std::marker::PhantomData;

use async_trait::async_trait;
use chrono::Duration;

use crate::event_store_backend::{SnapshotMaintenance, StorageBackend};
use crate::types::{Aggregate, AggregateId, Event, EventStore, EventStoreReadError, EventStoreWriteError};

#[derive(Debug, Clone)]
pub struct GenericEventStore<AID, A, E, B>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>,
  B: StorageBackend<AID, A, E>, {
  backend: B,
  maintenance: SnapshotMaintenance,
  _phantom: PhantomData<(AID, A, E)>,
}

impl<AID, A, E, B> GenericEventStore<AID, A, E, B>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>,
  B: StorageBackend<AID, A, E>,
{
  pub fn new(backend: B) -> Self {
    Self {
      backend,
      maintenance: SnapshotMaintenance::default(),
      _phantom: PhantomData,
    }
  }

  pub fn with_keep_snapshot_count(mut self, keep_snapshot_count: Option<usize>) -> Self {
    self.maintenance.keep_snapshot_count = keep_snapshot_count;
    self
  }

  pub fn with_delete_ttl(mut self, delete_ttl: Option<Duration>) -> Self {
    self.maintenance.delete_ttl = delete_ttl;
    self
  }

  pub fn backend_mut(&mut self) -> &mut B {
    &mut self.backend
  }

  pub fn maintenance(&self) -> &SnapshotMaintenance {
    &self.maintenance
  }
}

#[async_trait]
impl<AID, A, E, B> EventStore for GenericEventStore<AID, A, E, B>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>,
  B: StorageBackend<AID, A, E>,
{
  type AG = A;
  type AID = AID;
  type EV = E;

  async fn persist_event(&mut self, event: &Self::EV, version: usize) -> Result<(), EventStoreWriteError> {
    if event.is_created() {
      return Err(EventStoreWriteError::OtherError(
        "persist_event cannot accept creation events".to_string(),
      ));
    }
    self
      .backend
      .update_event_and_snapshot(event, None, version, &self.maintenance)
      .await?;
    self
      .backend
      .on_event_persisted(event.aggregate_id(), &self.maintenance)
      .await
  }

  async fn persist_event_and_snapshot(
    &mut self,
    event: &Self::EV,
    aggregate: &Self::AG,
  ) -> Result<(), EventStoreWriteError> {
    if event.is_created() {
      self
        .backend
        .create_event_and_snapshot(event, aggregate, &self.maintenance)
        .await?;
    } else {
      self
        .backend
        .update_event_and_snapshot(event, Some(aggregate), aggregate.version(), &self.maintenance)
        .await?;
    }
    self
      .backend
      .on_event_persisted(event.aggregate_id(), &self.maintenance)
      .await
  }

  async fn get_latest_snapshot_by_id(&self, aid: &Self::AID) -> Result<Option<Self::AG>, EventStoreReadError> {
    Ok(
      self
        .backend
        .fetch_latest_snapshot(aid)
        .await?
        .map(|snapshot| snapshot.aggregate),
    )
  }

  async fn get_events_by_id_since_seq_nr(
    &self,
    aid: &Self::AID,
    seq_nr: usize,
  ) -> Result<Vec<Self::EV>, EventStoreReadError> {
    self.backend.fetch_events_since(aid, seq_nr).await
  }
}
