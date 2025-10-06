use crate::types::AggregateId;
use std::collections::hash_map::DefaultHasher;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};

pub trait KeyResolver: Debug + Send + Sync + 'static {
  type ID: AggregateId;

  fn resolve_partition_key(&self, id: &Self::ID, shard_count: u64) -> String;
  fn resolve_sort_key(&self, id: &Self::ID, seq_nr: usize) -> String;
}

#[derive(Debug, Clone)]
pub struct DefaultKeyResolver<AID> {
  _phantom: std::marker::PhantomData<AID>,
}

impl<AID> Default for DefaultKeyResolver<AID> {
  fn default() -> Self {
    DefaultKeyResolver {
      _phantom: std::marker::PhantomData,
    }
  }
}

impl<AID: AggregateId> KeyResolver for DefaultKeyResolver<AID> {
  type ID = AID;

  fn resolve_partition_key(&self, id: &Self::ID, shard_count: u64) -> String {
    let mut hasher = DefaultHasher::new();
    id.value().hash(&mut hasher);
    let hash_value = hasher.finish();
    let remainder = hash_value % shard_count;
    format!("{}-{}", id.type_name(), remainder)
  }

  fn resolve_sort_key(&self, id: &Self::ID, seq_nr: usize) -> String {
    format!("{}-{}-{}", id.type_name(), id.value(), seq_nr)
  }
}
