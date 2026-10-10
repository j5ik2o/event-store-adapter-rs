#![cfg(feature = "test-hooks")]

use chrono::{DateTime, Utc};
use event_store_adapter_rs::memory::{EventStoreForMemory, MemoryStorage, MemoryTestHooks};
use event_store_adapter_rs::EventStore;
use event_store_adapter_rs::RetentionSettings;
use event_store_adapter_rs::SeqNr;
use event_store_adapter_rs::{AggregateId, AidString};
use event_store_adapter_rs::{EventEnvelope, SnapshotEnvelope};
use event_store_adapter_rs::{EventStoreError, StorageOperation};
use std::sync::{
  atomic::{AtomicBool, AtomicUsize, Ordering},
  Arc, Mutex,
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
  query: AtomicBool,
  delete: AtomicBool,
  duplicate_history: AtomicBool,
  just_written: Mutex<Option<SeqNr>>,
  query_calls: AtomicUsize,
  delete_calls: AtomicUsize,
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

  fn retention_visible_history(
    &self,
    _: &AidString,
    history: &[SeqNr],
    just_written: Option<SeqNr>,
  ) -> Result<Vec<SeqNr>, EventStoreError> {
    self.query_calls.fetch_add(1, Ordering::SeqCst);
    *self.just_written.lock().unwrap() = just_written;
    if self.query.load(Ordering::SeqCst) {
      Err(failure(StorageOperation::Append))
    } else if self.duplicate_history.load(Ordering::SeqCst) {
      Ok(history.iter().chain(history).copied().collect())
    } else {
      Ok(history.to_vec())
    }
  }

  fn retention_delete(&self, _: &AidString, _: &[SeqNr]) -> Result<(), EventStoreError> {
    self.delete_calls.fetch_add(1, Ordering::SeqCst);
    if self.delete.load(Ordering::SeqCst) {
      Err(failure(StorageOperation::Append))
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
  let query_calls = hooks.query_calls.load(Ordering::SeqCst);
  let delete_calls = hooks.delete_calls.load(Ordering::SeqCst);
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
  assert_eq!(hooks.query_calls.load(Ordering::SeqCst), query_calls);
  assert_eq!(hooks.delete_calls.load(Ordering::SeqCst), delete_calls);
}

#[tokio::test]
async fn should_retry_query_failure_on_next_event_only_append_in_same_storage() {
  retry_retention_failure(true, false).await;
}

#[tokio::test]
async fn should_retry_delete_failure_on_next_event_only_append_in_same_storage() {
  retry_retention_failure(false, false).await;
}

#[tokio::test]
async fn should_retry_query_failure_with_duplicate_history_on_event_only_append_in_same_storage() {
  retry_retention_failure(true, true).await;
}

#[tokio::test]
async fn should_retry_delete_failure_with_duplicate_history_on_event_only_append_in_same_storage() {
  retry_retention_failure(false, true).await;
}

async fn retry_retention_failure(query: bool, duplicate_history: bool) {
  let (hooks, storage, store) = setup(RetentionSettings::keep_latest(1));
  let aid = AidString::from_aggregate_id(&Id).unwrap();
  store
    .persist_event_and_snapshot(event(1), SnapshotEnvelope::new(1, 1))
    .await
    .unwrap();
  assert_eq!(storage.history_view(&aid).unwrap(), vec![1]);
  let fault = if query { &hooks.query } else { &hooks.delete };
  fault.store(true, Ordering::SeqCst);
  store
    .persist_event_and_snapshot(event(2), SnapshotEnvelope::new(2, 2))
    .await
    .unwrap();
  assert_eq!(storage.history_view(&aid).unwrap(), vec![1, 2]);
  let read = store.get_latest_snapshot_by_id(&Id).await.unwrap().unwrap();
  assert_eq!(read.head_seq_nr(), 2);
  assert_eq!(read.snapshot().unwrap().aggregate(), &2);
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&Id, 0).await.unwrap(),
    vec![event(1), event(2)]
  );
  assert_eq!(hooks.delete_calls.load(Ordering::SeqCst), usize::from(!query));
  fault.store(false, Ordering::SeqCst);
  hooks.duplicate_history.store(duplicate_history, Ordering::SeqCst);
  store.persist_event(event(3)).await.unwrap();
  assert_eq!(*hooks.just_written.lock().unwrap(), None);
  assert_eq!(storage.history_view(&aid).unwrap(), vec![2]);
  let read = store.get_latest_snapshot_by_id(&Id).await.unwrap().unwrap();
  assert_eq!(read.head_seq_nr(), 3);
  assert_eq!(read.snapshot().unwrap().seq_nr(), 2);
  assert_eq!(read.snapshot().unwrap().aggregate(), &2);
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&Id, 0).await.unwrap(),
    vec![event(1), event(2), event(3)]
  );
}

#[tokio::test]
async fn should_keep_latest_history_with_duplicate_query_results() {
  for keep in [1, 2] {
    let (hooks, storage, store) = setup(RetentionSettings::keep_latest(keep));
    let aid = AidString::from_aggregate_id(&Id).unwrap();
    hooks.duplicate_history.store(true, Ordering::SeqCst);
    for seq in 1..=4 {
      store
        .persist_event_and_snapshot(event(seq), SnapshotEnvelope::new(seq, seq))
        .await
        .unwrap();
      let first = seq.saturating_sub(keep as u64 - 1).max(1);
      assert_eq!(storage.history_view(&aid).unwrap(), (first..=seq).collect::<Vec<_>>());
      assert_eq!(*hooks.just_written.lock().unwrap(), Some(seq));
      let read = store.get_latest_snapshot_by_id(&Id).await.unwrap().unwrap();
      assert_eq!(read.head_seq_nr(), seq);
      assert_eq!(read.snapshot().unwrap().seq_nr(), seq);
      assert_eq!(read.snapshot().unwrap().aggregate(), &seq);
      assert_eq!(
        store.get_events_by_id_since_seq_nr(&Id, 0).await.unwrap(),
        (1..=seq).map(event).collect::<Vec<_>>()
      );
    }
  }
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
