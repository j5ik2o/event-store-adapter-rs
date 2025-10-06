use crate::types::{Aggregate, Event, EventStoreReadError, EventStoreWriteError};
use std::fmt::Debug;

pub trait EventSerializer<E: Event>: Debug + Send + Sync + 'static {
  fn serialize(&self, event: &E) -> Result<Vec<u8>, EventStoreWriteError>;
  fn deserialize(&self, data: &[u8]) -> Result<Box<E>, EventStoreReadError>;
}

#[derive(Debug)]
pub struct JsonEventSerializer<E: Event> {
  _phantom: std::marker::PhantomData<E>,
}

impl<E: Event> Default for JsonEventSerializer<E> {
  fn default() -> Self {
    JsonEventSerializer {
      _phantom: std::marker::PhantomData,
    }
  }
}

impl<E: Event> EventSerializer<E> for JsonEventSerializer<E> {
  fn serialize(&self, event: &E) -> Result<Vec<u8>, EventStoreWriteError> {
    serde_json::to_vec(event).map_err(|e| EventStoreWriteError::SerializationError(e.into()))
  }

  fn deserialize(&self, data: &[u8]) -> Result<Box<E>, EventStoreReadError> {
    serde_json::from_slice(data)
      .map_err(|e| EventStoreReadError::DeserializationError(e.into()))
      .map(Box::new)
  }
}

pub trait SnapshotSerializer<A: Aggregate>: Debug + Send + Sync + 'static {
  fn serialize(&self, aggregate: &A) -> Result<Vec<u8>, EventStoreWriteError>;
  fn deserialize(&self, data: &[u8]) -> Result<Box<A>, EventStoreReadError>;
}

#[derive(Debug)]
pub struct JsonSnapshotSerializer<A: Aggregate> {
  _phantom: std::marker::PhantomData<A>,
}

impl<A: Aggregate> Default for JsonSnapshotSerializer<A> {
  fn default() -> Self {
    JsonSnapshotSerializer {
      _phantom: std::marker::PhantomData,
    }
  }
}

impl<A: Aggregate> SnapshotSerializer<A> for JsonSnapshotSerializer<A> {
  fn serialize(&self, aggregate: &A) -> Result<Vec<u8>, EventStoreWriteError> {
    serde_json::to_vec(aggregate).map_err(|e| EventStoreWriteError::SerializationError(e.into()))
  }

  fn deserialize(&self, data: &[u8]) -> Result<Box<A>, EventStoreReadError> {
    serde_json::from_slice(data)
      .map_err(|e| EventStoreReadError::DeserializationError(e.into()))
      .map(Box::new)
  }
}
