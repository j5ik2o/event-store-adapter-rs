use super::{EventStoreForMemory, MemoryStorage};
use crate::next::{
  aggregate_id::{AggregateId, AidString},
  event_envelope::{EventEnvelope, SnapshotEnvelope},
  event_store::EventStore,
  retention::RetentionSettings,
};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
struct Id(&'static str);
impl AggregateId for Id {
  fn type_name(&self) -> String {
    "Account".into()
  }

  fn value(&self) -> String {
    self.0.into()
  }
}
type Store = EventStoreForMemory<Id, u64, u64>;
fn event(id: Id, seq: u64) -> EventEnvelope<Id, u64> {
  EventEnvelope::new(id, seq, DateTime::<Utc>::from_timestamp(0, 0).unwrap(), seq)
}
fn history(storage: &MemoryStorage, id: &Id) -> Vec<u64> {
  storage
    .inner
    .records
    .lock()
    .unwrap()
    .get(&AidString::from_aggregate_id(id).unwrap())
    .map(|record| record.history.keys().copied().collect())
    .unwrap_or_default()
}

#[tokio::test]
async fn should_retain_newest_history_without_hooks_and_share_retention_settings() {
  for keep in [1, 2] {
    let storage = MemoryStorage::new(RetentionSettings::keep_latest(keep)).unwrap();
    let creator = Store::new(storage.clone());
    let writer = Store::new(storage.clone());
    for seq in 1..=4 {
      let store = if seq % 2 == 0 { &writer } else { &creator };
      store
        .persist_event_and_snapshot(event(Id("1"), seq), SnapshotEnvelope::new(seq, seq))
        .await
        .unwrap();
    }
    assert_eq!(history(&storage, &Id("1")), (5 - keep as u64..=4).collect::<Vec<_>>());
    assert_eq!(
      creator.get_events_by_id_since_seq_nr(&Id("1"), 0).await.unwrap().len(),
      4
    );
    let read = writer.get_latest_snapshot_by_id(&Id("1")).await.unwrap().unwrap();
    assert_eq!(read.head_seq_nr(), 4);
    assert_eq!(read.snapshot().unwrap().aggregate(), &4);
  }
}

#[tokio::test]
async fn should_not_remove_other_aggregate_history_during_retention() {
  let storage = MemoryStorage::new(RetentionSettings::keep_latest(2)).unwrap();
  let store = Store::new(storage.clone());
  for seq in 1..=4 {
    for id in [Id("1"), Id("10")] {
      if id.0 == "10" && seq > 2 {
        continue;
      }
      store
        .persist_event_and_snapshot(event(id, seq), SnapshotEnvelope::new(seq, seq))
        .await
        .unwrap();
    }
  }
  assert_eq!(history(&storage, &Id("1")), vec![3, 4]);
  assert_eq!(history(&storage, &Id("10")), vec![1, 2]);
  let read = store.get_latest_snapshot_by_id(&Id("10")).await.unwrap().unwrap();
  assert_eq!(read.head_seq_nr(), 2);
  assert_eq!(read.snapshot().unwrap().aggregate(), &2);
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&Id("10"), 0).await.unwrap().len(),
    2
  );
}

#[cfg(feature = "test-hooks")]
mod notifications {
  use super::*;
  use crate::next::{
    error::{EventStoreError, StorageOperation},
    memory::MemoryTestHooks,
  };
  use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
  };
  use tracing::{
    field::{Field, Visit},
    Event, Subscriber,
  };
  use tracing_subscriber::{layer::Context, prelude::*, Layer};

  #[derive(Debug, Default)]
  struct Hooks {
    commit: AtomicBool,
    query: AtomicBool,
    delete: AtomicBool,
  }
  fn failure() -> EventStoreError {
    EventStoreError::Storage {
      operation: StorageOperation::Append,
      source: Box::new(std::io::Error::other("RETENTION_FAILURE")),
    }
  }
  impl MemoryTestHooks for Hooks {
    fn before_commit(&self, _: &AidString, _: u64) -> Result<(), EventStoreError> {
      if self.commit.load(Ordering::SeqCst) {
        Err(failure())
      } else {
        Ok(())
      }
    }

    fn read_events(&self, _: &AidString) -> Result<(), EventStoreError> {
      Ok(())
    }

    fn read_snapshot(&self, _: &AidString) -> Result<(), EventStoreError> {
      Ok(())
    }

    fn retention_visible_history(
      &self,
      _: &AidString,
      history: &[u64],
      _: Option<u64>,
    ) -> Result<Vec<u64>, EventStoreError> {
      if self.query.load(Ordering::SeqCst) {
        Err(failure())
      } else {
        Ok(history.to_vec())
      }
    }

    fn retention_delete(&self, _: &AidString, _: &[u64]) -> Result<(), EventStoreError> {
      if self.delete.load(Ordering::SeqCst) {
        Err(failure())
      } else {
        Ok(())
      }
    }
  }
  #[derive(Debug, Default)]
  struct Observed {
    fields: std::collections::HashMap<String, String>,
    committed_head: Option<u64>,
    committed_snapshot: Option<u64>,
  }
  impl Visit for Observed {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
      self.fields.insert(field.name().into(), format!("{value:?}"));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
      self.fields.insert(field.name().into(), value.into());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
      self.fields.insert(field.name().into(), value.to_string());
    }
  }
  struct Capture {
    storage: MemoryStorage,
    seen: Arc<Mutex<Vec<Observed>>>,
    panic: bool,
  }
  impl<S: Subscriber> Layer<S> for Capture {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
      if event.metadata().target() != "event_store_adapter::retention" {
        return;
      }
      let mut observed = Observed::default();
      event.record(&mut observed);
      if let Ok(records) = self.storage.inner.records.try_lock() {
        if let Some(record) = records.get(&AidString::from_aggregate_id(&Id("1")).unwrap()) {
          observed.committed_head = record.events.last_key_value().map(|(seq, _)| *seq);
          observed.committed_snapshot = record.snapshot.as_ref().map(|snapshot| snapshot.seq_nr);
        }
      }
      self.seen.lock().unwrap().push(observed);
      if self.panic {
        panic!("notification subscriber failed");
      }
    }
  }
  fn setup() -> (Arc<Hooks>, MemoryStorage, Store) {
    let hooks = Arc::new(Hooks::default());
    let storage = MemoryStorage::new_with_hooks(RetentionSettings::keep_latest(1), hooks.clone()).unwrap();
    let store = Store::new(storage.clone());
    (hooks, storage, store)
  }
  fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread().build().unwrap()
  }

  #[test]
  fn should_notify_writer_after_unlock_for_both_append_entries() {
    let (hooks, storage, _) = setup();
    let creator_seen = Arc::new(Mutex::new(Vec::new()));
    let creator_subscriber = tracing_subscriber::registry().with(Capture {
      storage: storage.clone(),
      seen: creator_seen.clone(),
      panic: false,
    });
    let creator = tracing::subscriber::with_default(creator_subscriber, || Store::new(storage.clone()));
    let writer = Store::new(storage.clone());
    runtime()
      .block_on(creator.persist_event_and_snapshot(event(Id("1"), 1), SnapshotEnvelope::new(1, 1)))
      .unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(Capture {
      storage: storage.clone(),
      seen: seen.clone(),
      panic: false,
    });
    hooks.query.store(true, Ordering::SeqCst);
    tracing::subscriber::with_default(subscriber, || {
      let runtime = runtime();
      runtime
        .block_on(writer.persist_event_and_snapshot(event(Id("1"), 2), SnapshotEnvelope::new(2, 2)))
        .unwrap();
      runtime.block_on(writer.persist_event(event(Id("1"), 3))).unwrap();
    });
    assert!(creator_seen.lock().unwrap().is_empty());
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    for (observed, seq) in seen.iter().zip([2, 3]) {
      assert_eq!(
        observed.committed_head,
        Some(seq),
        "notification held the storage lock: {observed:?}"
      );
      assert_eq!(observed.committed_snapshot, Some(2));
      assert_eq!(observed.fields["category"], "retention-failure");
      assert_eq!(observed.fields["aid"], "Account-1");
      assert_eq!(observed.fields["seq_nr"], seq.to_string());
      assert_eq!(observed.fields["phase"], "retention-query");
      assert!(!observed.fields["error"].is_empty());
      assert_eq!(
        observed.fields["message"],
        "snapshot retention failed; the append was committed"
      );
    }
  }

  #[test]
  fn should_keep_committed_append_success_when_notification_subscriber_panics() {
    let (hooks, storage, store) = setup();
    hooks.query.store(true, Ordering::SeqCst);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(Capture {
      storage,
      seen: seen.clone(),
      panic: true,
    });
    let result = tracing::subscriber::with_default(subscriber, || {
      runtime().block_on(store.persist_event(event(Id("1"), 1)))
    });
    assert!(result.is_ok());
    assert_eq!(seen.lock().unwrap().len(), 1);
    assert_eq!(
      runtime()
        .block_on(store.get_events_by_id_since_seq_nr(&Id("1"), 0))
        .unwrap()
        .len(),
      1
    );
  }

  #[test]
  fn should_not_notify_retention_failure_when_commit_is_rejected() {
    let (hooks, storage, store) = setup();
    hooks.commit.store(true, Ordering::SeqCst);
    hooks.query.store(true, Ordering::SeqCst);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(Capture {
      storage: storage.clone(),
      seen: seen.clone(),
      panic: false,
    });
    let result = tracing::subscriber::with_default(subscriber, || {
      runtime().block_on(store.persist_event_and_snapshot(event(Id("1"), 1), SnapshotEnvelope::new(1, 1)))
    });
    assert!(matches!(result, Err(EventStoreError::Storage { .. })));
    assert!(seen.lock().unwrap().is_empty());
    assert!(history(&storage, &Id("1")).is_empty());
    assert!(runtime()
      .block_on(store.get_latest_snapshot_by_id(&Id("1")))
      .unwrap()
      .is_none());
    assert!(runtime()
      .block_on(store.get_events_by_id_since_seq_nr(&Id("1"), 0))
      .unwrap()
      .is_empty());
  }
}
