use std::fmt::Debug;

use crate::types::{Aggregate, AggregateId, Event, EventStoreReadError, EventStoreWriteError};
use async_trait::async_trait;
use chrono::Duration;

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct SnapshotEnvelope<A> {
  pub aggregate: A,
  pub seq_nr: usize,
  pub version: usize,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct SnapshotMaintenance {
  pub keep_snapshot_count: Option<usize>,
  pub delete_ttl: Option<Duration>,
}

#[async_trait]
pub trait StorageBackend<AID, A, E>: Send + Sync + Clone + Debug + 'static
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>, {
  async fn fetch_latest_snapshot(&self, aid: &AID) -> Result<Option<SnapshotEnvelope<A>>, EventStoreReadError>;

  async fn fetch_events_since(&self, aid: &AID, seq_nr: usize) -> Result<Vec<E>, EventStoreReadError>;

  async fn create_event_and_snapshot(
    &self,
    event: &E,
    aggregate: &A,
    maintenance: &SnapshotMaintenance,
  ) -> Result<(), EventStoreWriteError>;

  async fn update_event_and_snapshot(
    &self,
    event: &E,
    aggregate: Option<&A>,
    expected_version: usize,
    maintenance: &SnapshotMaintenance,
  ) -> Result<(), EventStoreWriteError>;

  async fn on_event_persisted(
    &self,
    _aid: &AID,
    _maintenance: &SnapshotMaintenance,
  ) -> Result<(), EventStoreWriteError> {
    Ok(())
  }
}
