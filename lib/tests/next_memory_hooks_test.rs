#![cfg(feature = "test-hooks")]

use chrono::{DateTime, Utc};
use event_store_adapter_rs::next::aggregate_id::{AggregateId, AidString};
use event_store_adapter_rs::next::error::{EventStoreError, StorageOperation};
use event_store_adapter_rs::next::event_envelope::{EventEnvelope, SnapshotEnvelope};
use event_store_adapter_rs::next::event_store::EventStore;
use event_store_adapter_rs::next::memory::{EventStoreForMemory, MemoryStorage, MemoryTestHooks};
use event_store_adapter_rs::next::retention::RetentionSettings;
use event_store_adapter_rs::next::seq_nr::SeqNr;
use std::sync::{
  atomic::{AtomicBool, Ordering},
  Arc,
};

#[derive(Debug, Clone, PartialEq)]
struct Id;
impl AggregateId for Id {
  fn type_name(&self) -> String {
    "Account".into()
  }

  fn value(&self) -> String {
    "1".into()
  }
}
#[derive(Debug, Default)]
struct Hooks {
  commit: AtomicBool,
  events: AtomicBool,
  snapshot: AtomicBool,
}
fn failure(operation: StorageOperation) -> EventStoreError {
  EventStoreError::Storage {
    operation,
    source: Box::new(std::io::Error::other("INJECTED")),
  }
}
impl MemoryTestHooks for Hooks {
  fn before_commit(&self, _: &AidString, _: SeqNr) -> Result<(), EventStoreError> {
    if self.commit.load(Ordering::SeqCst) {
      Err(failure(StorageOperation::Append))
    } else {
      Ok(())
    }
  }

  fn read_events(&self, _: &AidString) -> Result<(), EventStoreError> {
    if self.events.load(Ordering::SeqCst) {
      Err(failure(StorageOperation::LoadEvents))
    } else {
      Ok(())
    }
  }

  fn read_snapshot(&self, _: &AidString) -> Result<(), EventStoreError> {
    if self.snapshot.load(Ordering::SeqCst) {
      Err(failure(StorageOperation::LoadSnapshot))
    } else {
      Ok(())
    }
  }
}
type Store = EventStoreForMemory<Id, u64, u64>;
fn event(seq: u64) -> EventEnvelope<Id, u64> {
  EventEnvelope::new(Id, seq, DateTime::<Utc>::from_timestamp(0, 0).unwrap(), seq)
}
fn setup(retention: RetentionSettings) -> (Arc<Hooks>, MemoryStorage, Store) {
  let hooks = Arc::new(Hooks::default());
  let storage = MemoryStorage::new_with_hooks(retention, hooks.clone()).unwrap();
  let store = Store::new(storage.clone());
  (hooks, storage, store)
}
#[tokio::test]
async fn should_debug_exclude_hook_details() {
  let (hooks, storage, store) = setup(RetentionSettings::current_only());
  store
    .persist_event_and_snapshot(event(1), SnapshotEnvelope::new(1, 1))
    .await
    .unwrap();
  let hook_details = format!("{hooks:?}");
  let output = format!("storage={storage:?}\nstore={store:?}");
  assert!(!output.contains(&hook_details), "Debug exposed hook details: {output}");
}
#[tokio::test]
async fn should_hook_before_commit_allow_atomic_success() {
  let (_, _, store) = setup(RetentionSettings::current_only());
  store
    .persist_event_and_snapshot(event(1), SnapshotEnvelope::new(1, 1))
    .await
    .unwrap();
  let read = store.get_latest_snapshot_by_id(&Id).await.unwrap().unwrap();
  assert_eq!(read.head_seq_nr(), 1);
  assert_eq!(read.snapshot().unwrap().aggregate(), &1);
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&Id, 0).await.unwrap(),
    vec![event(1)]
  );
}
#[tokio::test]
async fn should_hook_before_commit_leave_all_records_unchanged_on_failure() {
  let (hooks, storage, store) = setup(RetentionSettings::keep_latest(5));
  store
    .persist_event_and_snapshot(event(1), SnapshotEnvelope::new(1, 1))
    .await
    .unwrap();
  let aid = AidString::from_aggregate_id(&Id).unwrap();
  let history = storage.history_view(&aid).unwrap();
  let previous = store.get_latest_snapshot_by_id(&Id).await.unwrap();
  hooks.commit.store(true, Ordering::SeqCst);
  assert!(matches!(
    store
      .persist_event_and_snapshot(event(2), SnapshotEnvelope::new(2, 2))
      .await,
    Err(EventStoreError::Storage {
      operation: StorageOperation::Append,
      ..
    })
  ));
  assert_eq!(store.get_latest_snapshot_by_id(&Id).await.unwrap(), previous);
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&Id, 0).await.unwrap(),
    vec![event(1)]
  );
  assert_eq!(storage.history_view(&aid).unwrap(), history);
}
#[tokio::test]
async fn should_hook_read_allow_event_and_snapshot_reads() {
  let (_, _, store) = setup(RetentionSettings::current_only());
  store
    .persist_event_and_snapshot(event(1), SnapshotEnvelope::new(1, 1))
    .await
    .unwrap();
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&Id, 0).await.unwrap(),
    vec![event(1)]
  );
  assert_eq!(
    store
      .get_latest_snapshot_by_id(&Id)
      .await
      .unwrap()
      .unwrap()
      .head_seq_nr(),
    1
  );
}
#[tokio::test]
async fn should_hook_read_return_storage_errors_instead_of_empty_results() {
  let (hooks, _, store) = setup(RetentionSettings::current_only());
  store.persist_event(event(1)).await.unwrap();
  hooks.events.store(true, Ordering::SeqCst);
  hooks.snapshot.store(true, Ordering::SeqCst);
  assert!(matches!(
    store.get_events_by_id_since_seq_nr(&Id, 0).await,
    Err(EventStoreError::Storage {
      operation: StorageOperation::LoadEvents,
      ..
    })
  ));
  assert!(matches!(
    store.get_latest_snapshot_by_id(&Id).await,
    Err(EventStoreError::Storage {
      operation: StorageOperation::LoadSnapshot,
      ..
    })
  ));
}
#[tokio::test]
async fn should_current_only_keep_latest_snapshot_with_empty_history() {
  let (_, storage, store) = setup(RetentionSettings::current_only());
  for seq in 1..=3 {
    store
      .persist_event_and_snapshot(event(seq), SnapshotEnvelope::new(seq, seq))
      .await
      .unwrap();
  }
  assert_eq!(
    store
      .get_latest_snapshot_by_id(&Id)
      .await
      .unwrap()
      .unwrap()
      .snapshot()
      .unwrap()
      .aggregate(),
    &3
  );
  assert!(storage
    .history_view(&AidString::from_aggregate_id(&Id).unwrap())
    .unwrap()
    .is_empty());
}
#[tokio::test]
async fn should_current_only_history_view_distinguish_actual_history_settings() {
  let (_, storage, store) = setup(RetentionSettings::keep_latest(5));
  store
    .persist_event_and_snapshot(event(1), SnapshotEnvelope::new(1, 1))
    .await
    .unwrap();
  let mut history = storage
    .history_view(&AidString::from_aggregate_id(&Id).unwrap())
    .unwrap();
  history.sort_unstable();
  assert_eq!(history, vec![1]);
}
