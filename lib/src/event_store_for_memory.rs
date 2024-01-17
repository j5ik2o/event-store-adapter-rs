use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;

use crate::types::{
  Aggregate, AggregateId, Event, EventStore, EventStoreReadError, EventStoreWriteError,
  TransactionCanceledExceptionWrapper,
};

/// Event Store for On-Memory
#[derive(Debug, Clone)]
pub struct EventStoreForMemory<AID: AggregateId, A: Aggregate, E: Event> {
  events: HashMap<String, Vec<E>>,
  snapshots: HashMap<String, A>,
  _p: PhantomData<AID>,
}

unsafe impl<AID: AggregateId, A: Aggregate, E: Event> Sync for EventStoreForMemory<AID, A, E> {}

unsafe impl<AID: AggregateId, A: Aggregate, E: Event> Send for EventStoreForMemory<AID, A, E> {}

#[async_trait]
impl<AID: AggregateId, A: Aggregate<ID = AID>, E: Event<AggregateID = AID>> EventStore
  for EventStoreForMemory<AID, A, E>
{
  type AG = A;
  type AID = AID;
  type EV = E;

  async fn persist_event(&mut self, event: &Self::EV, version: usize) -> Result<(), EventStoreWriteError> {
    if event.is_created() {
      panic!("EventStoreForOnMemory does not support create event.")
    }
    let aid = event.aggregate_id().to_string();
    let aggregate = self.snapshots.get_mut(&aid).unwrap();
    if aggregate.version() != version {
      return Err(EventStoreWriteError::OptimisticLockError(
        TransactionCanceledExceptionWrapper(None),
      ));
    }
    let new_version = aggregate.version() + 1;
    self.events.entry(aid.clone()).or_insert(vec![]).push(event.clone());
    aggregate.set_version(new_version);
    return Ok(());
  }

  async fn persist_event_and_snapshot(
    &mut self,
    event: &Self::EV,
    aggregate: &Self::AG,
  ) -> Result<(), EventStoreWriteError> {
    let aid = event.aggregate_id().to_string();
    let mut new_version = 1;
    if !event.is_created() {
      let snapshot = self.snapshots.get(&aid).unwrap();
      let version = snapshot.version();
      if version != aggregate.version() {
        return Err(EventStoreWriteError::OptimisticLockError(
          TransactionCanceledExceptionWrapper(None),
        ));
      }
      new_version = snapshot.version() + 1;
    }
    self.events.entry(aid.clone()).or_insert(vec![]).push(event.clone());
    let mut ar = aggregate.clone();
    ar.set_version(new_version);
    self.snapshots.insert(aid, ar);
    return Ok(());
  }

  async fn get_latest_snapshot_by_id(&self, aid: &Self::AID) -> Result<Option<Self::AG>, EventStoreReadError> {
    match self.snapshots.get(&aid.to_string()) {
      Some(aggregate) => Ok(Some(aggregate.clone())),
      None => Ok(None),
    }
  }

  async fn get_events_by_id_since_seq_nr(
    &self,
    aid: &Self::AID,
    seq_nr: usize,
  ) -> Result<Vec<Self::EV>, EventStoreReadError> {
    match self.events.get(&aid.to_string()) {
      Some(events) => {
        let mut result = vec![];
        for event in events {
          if event.seq_nr() >= seq_nr {
            result.push(event.clone());
          }
        }
        Ok(result)
      }
      None => Ok(vec![]),
    }
  }
}

impl<AID: AggregateId, A: Aggregate, E: Event> EventStoreForMemory<AID, A, E> {
  pub fn new() -> Self {
    Self {
      events: HashMap::new(),
      snapshots: HashMap::new(),
      _p: PhantomData,
    }
  }
}
