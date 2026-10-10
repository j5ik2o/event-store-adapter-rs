use crate::{
  aggregate_id::AidString,
  error::{EventStoreError, StorageOperation},
  retention::RetentionSettings,
  seq_nr::SeqNr,
};
use chrono::{DateTime, Utc};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Clone)]
pub struct MemoryStorage {
  pub(super) inner: Arc<SharedStorage>,
}
impl std::fmt::Debug for MemoryStorage {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("MemoryStorage")
      .field("retention", &self.inner.retention)
      .finish_non_exhaustive()
  }
}
#[derive(Debug)]
pub(super) struct SharedStorage {
  pub retention: RetentionSettings,
  pub records: Mutex<HashMap<AidString, Records>>,
  #[cfg(feature = "test-hooks")]
  pub hooks: Option<Arc<dyn super::MemoryTestHooks>>,
}
#[derive(Debug, Default)]
pub(super) struct Records {
  pub events: BTreeMap<SeqNr, StoredEvent>,
  pub snapshot: Option<StoredSnapshot>,
  pub history: BTreeMap<SeqNr, StoredSnapshot>,
}
#[derive(Debug, Clone)]
pub(super) struct StoredEvent {
  pub seq_nr: SeqNr,
  pub occurred_at: DateTime<Utc>,
  pub manifest: String,
  pub payload: Vec<u8>,
}
#[derive(Debug, Clone)]
pub(super) struct StoredSnapshot {
  pub seq_nr: SeqNr,
  pub manifest: String,
  pub aggregate: Vec<u8>,
}
impl MemoryStorage {
  pub fn new(retention: RetentionSettings) -> Result<Self, EventStoreError> {
    retention.validate_for_memory()?;
    Ok(Self {
      inner: Arc::new(SharedStorage {
        retention,
        records: Mutex::new(HashMap::new()),
        #[cfg(feature = "test-hooks")]
        hooks: None,
      }),
    })
  }

  #[cfg(feature = "test-hooks")]
  #[doc(hidden)]
  pub fn new_with_hooks(
    retention: RetentionSettings,
    hooks: Arc<dyn super::MemoryTestHooks>,
  ) -> Result<Self, EventStoreError> {
    retention.validate_for_memory()?;
    Ok(Self {
      inner: Arc::new(SharedStorage {
        retention,
        records: Mutex::new(HashMap::new()),
        hooks: Some(hooks),
      }),
    })
  }

  pub(super) fn lock(
    &self,
    operation: StorageOperation,
  ) -> Result<MutexGuard<'_, HashMap<AidString, Records>>, EventStoreError> {
    self.inner.records.lock().map_err(|_| EventStoreError::Storage {
      operation,
      source: Box::new(std::io::Error::other("memory storage lock poisoned")),
    })
  }

  #[cfg(feature = "test-hooks")]
  #[doc(hidden)]
  pub fn history_view(&self, aid: &AidString) -> Result<Vec<SeqNr>, EventStoreError> {
    Ok(
      self
        .lock(StorageOperation::LoadSnapshot)?
        .get(aid)
        .map(|r| r.history.keys().copied().collect())
        .unwrap_or_default(),
    )
  }
}
