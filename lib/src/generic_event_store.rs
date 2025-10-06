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

#[cfg(test)]
mod tests {
  use super::*;
  use crate::event_store_backend::SnapshotEnvelope;
  use async_trait::async_trait;
  use chrono::{Duration as ChronoDuration, Utc};
  use serde::{Deserialize, Serialize};
  use std::fmt;
  use std::sync::{Arc, Mutex};

  #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
  struct TestAggregateId(u64);

  impl fmt::Display for TestAggregateId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
      write!(f, "{}", self.0)
    }
  }

  impl AggregateId for TestAggregateId {
    fn type_name(&self) -> String {
      "TestAggregate".to_string()
    }

    fn value(&self) -> String {
      self.0.to_string()
    }
  }

  #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
  struct TestAggregate {
    id: TestAggregateId,
    seq_nr: usize,
    version: usize,
    last_updated_at: chrono::DateTime<Utc>,
  }

  impl TestAggregate {
    fn new(id: u64) -> (Self, TestEvent) {
      let aggregate = TestAggregate {
        id: TestAggregateId(id),
        seq_nr: 1,
        version: 1,
        last_updated_at: Utc::now(),
      };
      let event = TestEvent::new(id, 1, true);
      (aggregate, event)
    }
  }

  impl Aggregate for TestAggregate {
    type ID = TestAggregateId;

    fn id(&self) -> &Self::ID {
      &self.id
    }

    fn seq_nr(&self) -> usize {
      self.seq_nr
    }

    fn version(&self) -> usize {
      self.version
    }

    fn set_version(&mut self, version: usize) {
      self.version = version;
    }

    fn last_updated_at(&self) -> &chrono::DateTime<Utc> {
      &self.last_updated_at
    }
  }

  #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
  struct TestEvent {
    id: u64,
    aggregate_id: TestAggregateId,
    seq_nr: usize,
    occurred_at: chrono::DateTime<Utc>,
    is_created: bool,
  }

  impl TestEvent {
    fn new(id: u64, seq_nr: usize, is_created: bool) -> Self {
      Self {
        id,
        aggregate_id: TestAggregateId(id),
        seq_nr,
        occurred_at: Utc::now(),
        is_created,
      }
    }

    fn rename(id: u64, seq_nr: usize) -> Self {
      Self::new(id, seq_nr, false)
    }
  }

  impl Event for TestEvent {
    type AggregateID = TestAggregateId;
    type ID = u64;

    fn id(&self) -> &Self::ID {
      &self.id
    }

    fn aggregate_id(&self) -> &Self::AggregateID {
      &self.aggregate_id
    }

    fn seq_nr(&self) -> usize {
      self.seq_nr
    }

    fn occurred_at(&self) -> &chrono::DateTime<Utc> {
      &self.occurred_at
    }

    fn is_created(&self) -> bool {
      self.is_created
    }
  }

  #[derive(Clone)]
  struct TestBackend {
    state: Arc<Mutex<TestBackendState>>,
  }

  #[derive(Default)]
  struct TestBackendState {
    snapshot: Option<SnapshotEnvelope<TestAggregate>>,
    events: Vec<TestEvent>,
    last_maintenance: Option<SnapshotMaintenance>,
    calls: Vec<String>,
  }

  #[derive(Clone, Debug, PartialEq)]
  struct TestBackendSnapshot {
    snapshot: Option<SnapshotEnvelope<TestAggregate>>,
    events: Vec<TestEvent>,
    last_maintenance: Option<SnapshotMaintenance>,
    calls: Vec<String>,
  }

  impl fmt::Debug for TestBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
      f.debug_struct("TestBackend").finish()
    }
  }

  impl TestBackend {
    fn new() -> Self {
      Self {
        state: Arc::new(Mutex::new(TestBackendState::default())),
      }
    }

    fn snapshot_state(&self) -> TestBackendSnapshot {
      let state = self.state.lock().unwrap();
      TestBackendSnapshot {
        snapshot: state.snapshot.clone(),
        events: state.events.clone(),
        last_maintenance: state.last_maintenance.clone(),
        calls: state.calls.clone(),
      }
    }
  }

  #[async_trait]
  impl StorageBackend<TestAggregateId, TestAggregate, TestEvent> for TestBackend {
    async fn fetch_latest_snapshot(
      &self,
      _aid: &TestAggregateId,
    ) -> Result<Option<SnapshotEnvelope<TestAggregate>>, EventStoreReadError> {
      let state = self.state.lock().unwrap();
      Ok(state.snapshot.clone())
    }

    async fn fetch_events_since(
      &self,
      _aid: &TestAggregateId,
      seq_nr: usize,
    ) -> Result<Vec<TestEvent>, EventStoreReadError> {
      let state = self.state.lock().unwrap();
      Ok(
        state
          .events
          .iter()
          .cloned()
          .filter(|event| event.seq_nr() >= seq_nr)
          .collect(),
      )
    }

    async fn create_event_and_snapshot(
      &self,
      event: &TestEvent,
      aggregate: &TestAggregate,
      _maintenance: &SnapshotMaintenance,
    ) -> Result<(), EventStoreWriteError> {
      let mut state = self.state.lock().unwrap();
      state.calls.push("create".to_string());
      state.snapshot = Some(SnapshotEnvelope {
        aggregate: aggregate.clone(),
        seq_nr: aggregate.seq_nr(),
        version: aggregate.version(),
      });
      state.events.push(event.clone());
      Ok(())
    }

    async fn update_event_and_snapshot(
      &self,
      event: &TestEvent,
      aggregate: Option<&TestAggregate>,
      expected_version: usize,
      _maintenance: &SnapshotMaintenance,
    ) -> Result<(), EventStoreWriteError> {
      let mut state = self.state.lock().unwrap();
      state.calls.push("update".to_string());
      let snapshot = state
        .snapshot
        .as_mut()
        .ok_or_else(|| EventStoreWriteError::OtherError("snapshot not found".to_string()))?;
      if snapshot.version != expected_version {
        return Err(EventStoreWriteError::OtherError("version mismatch".to_string()));
      }
      snapshot.version += 1;
      snapshot.seq_nr = event.seq_nr();
      if let Some(aggregate) = aggregate {
        snapshot.aggregate = aggregate.clone();
      }
      state.events.push(event.clone());
      Ok(())
    }

    async fn on_event_persisted(
      &self,
      _aid: &TestAggregateId,
      maintenance: &SnapshotMaintenance,
    ) -> Result<(), EventStoreWriteError> {
      let mut state = self.state.lock().unwrap();
      state.calls.push("on_event".to_string());
      state.last_maintenance = Some(maintenance.clone());
      Ok(())
    }
  }

  #[tokio::test]
  async fn persist_creation_stores_snapshot_and_event() {
    let backend = TestBackend::new();
    let mut store = GenericEventStore::new(backend.clone());
    let (aggregate, event) = TestAggregate::new(1);

    store
      .persist_event_and_snapshot(&event, &aggregate)
      .await
      .expect("creation should succeed");

    let state = backend.snapshot_state();
    assert_eq!(state.calls, vec!["create", "on_event"]);
    assert_eq!(state.events.len(), 1);
    let snapshot = state.snapshot.expect("snapshot stored");
    assert_eq!(snapshot.version, 1);
    assert_eq!(snapshot.seq_nr, 1);
    assert_eq!(snapshot.aggregate, aggregate);
  }

  #[tokio::test]
  async fn persist_event_updates_snapshot_version() {
    let backend = TestBackend::new();
    let mut store = GenericEventStore::new(backend.clone());
    let (aggregate, event) = TestAggregate::new(1);
    store.persist_event_and_snapshot(&event, &aggregate).await.unwrap();

    let update_event = TestEvent::rename(1, 2);
    store.persist_event(&update_event, 1).await.unwrap();

    let state = backend.snapshot_state();
    assert_eq!(state.calls, vec!["create", "on_event", "update", "on_event"]);
    let snapshot = state.snapshot.expect("snapshot stored");
    assert_eq!(snapshot.version, 2);
    assert_eq!(snapshot.seq_nr, 2);
    assert_eq!(state.events.len(), 2);
  }

  #[tokio::test]
  async fn persist_event_rejects_creation_event() {
    let backend = TestBackend::new();
    let mut store = GenericEventStore::new(backend.clone());
    let creation_event = TestEvent::new(1, 1, true);
    let result = store.persist_event(&creation_event, 0).await;
    assert!(result.is_err());
  }

  #[tokio::test]
  async fn maintenance_configuration_is_passed_to_backend() {
    let backend = TestBackend::new();
    let mut store = GenericEventStore::new(backend.clone())
      .with_keep_snapshot_count(Some(2))
      .with_delete_ttl(Some(ChronoDuration::seconds(30)));
    let (aggregate, event) = TestAggregate::new(42);

    store.persist_event_and_snapshot(&event, &aggregate).await.unwrap();

    let state = backend.snapshot_state();
    assert_eq!(
      state.last_maintenance,
      Some(SnapshotMaintenance {
        keep_snapshot_count: Some(2),
        delete_ttl: Some(ChronoDuration::seconds(30)),
      })
    );
  }

  #[tokio::test]
  async fn update_with_snapshot_replaces_aggregate_when_provided() {
    let backend = TestBackend::new();
    let mut store = GenericEventStore::new(backend.clone());
    let (aggregate, event) = TestAggregate::new(7);
    store.persist_event_and_snapshot(&event, &aggregate).await.unwrap();

    let mut updated_aggregate = aggregate.clone();
    updated_aggregate.seq_nr = 2;
    updated_aggregate.version = 1;
    let update_event = TestEvent::rename(7, 2);
    store
      .persist_event_and_snapshot(&update_event, &updated_aggregate)
      .await
      .unwrap();

    let state = backend.snapshot_state();
    let snapshot = state.snapshot.expect("snapshot stored");
    assert_eq!(snapshot.version, 2);
    assert_eq!(snapshot.seq_nr, 2);
    assert_eq!(snapshot.aggregate, updated_aggregate);
    assert_eq!(state.events.len(), 2);
  }
}
