use crate::types::{Aggregate, Event};
use anyhow::Result;
use std::fmt::Debug;

pub trait EventSerializer<E: Event>: Debug + 'static {
  fn serialize(&self, event: &E) -> Result<Vec<u8>>;
  fn deserialize(&self, data: &[u8]) -> Result<Box<E>>;
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
  fn serialize(&self, event: &E) -> Result<Vec<u8>> {
    serde_json::to_vec(event).map_err(|e| e.into())
  }

  fn deserialize(&self, data: &[u8]) -> Result<Box<E>> {
    let event: E = serde_json::from_slice(data)?;
    Ok(Box::new(event))
  }
}

pub trait SnapshotSerializer<A: Aggregate>: Debug + 'static {
  fn serialize(&self, aggregate: &A) -> Result<Vec<u8>>;
  fn deserialize(&self, data: &[u8]) -> Result<Box<A>>;
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
  fn serialize(&self, aggregate: &A) -> Result<Vec<u8>> {
    serde_json::to_vec(aggregate).map_err(|e| e.into())
  }

  fn deserialize(&self, data: &[u8]) -> Result<Box<A>> {
    let aggregate: A = serde_json::from_slice(data)?;
    Ok(Box::new(aggregate))
  }
}
