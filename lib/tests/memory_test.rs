use std::sync::{Arc, Barrier, Mutex};

use chrono::{DateTime, Utc};
use event_store_adapter_rs::AggregateId;
use event_store_adapter_rs::EventStore;
use event_store_adapter_rs::{ConfigurationReason, ContractRule, EventStoreError, SerializationPhase};
use event_store_adapter_rs::{EventEnvelope, SnapshotEnvelope};
use event_store_adapter_rs::{EventSerializer, SnapshotSerializer};
use event_store_adapter_rs::{EventStoreForMemory, MemoryStorage};
use event_store_adapter_rs::{RetentionMode, RetentionSettings};

#[derive(Debug, Clone, PartialEq)]
struct Id(&'static str);
impl AggregateId for Id {
  fn type_name(&self) -> String {
    "Account".into()
  }

  fn value(&self) -> String {
    self.0.into()
  }
}
type Store = EventStoreForMemory<Id, serde_json::Value, serde_json::Value>;
fn store(storage: MemoryStorage) -> Store {
  Store::new(storage)
}
fn fresh() -> Store {
  store(MemoryStorage::new(RetentionSettings::current_only()).unwrap())
}
fn event(seq: u64) -> EventEnvelope<Id, serde_json::Value> {
  EventEnvelope::new(
    Id("1"),
    seq,
    DateTime::<Utc>::from_timestamp(1, 123).unwrap(),
    serde_json::json!({"seq": seq}),
  )
  .with_manifest("event")
}
fn snapshot(seq: u64) -> SnapshotEnvelope<serde_json::Value> {
  SnapshotEnvelope::new(serde_json::json!({"seq": seq}), seq).with_manifest("snapshot")
}

#[tokio::test]
async fn should_debug_exclude_stored_identifiers_and_serialized_payloads() {
  let storage = MemoryStorage::new(RetentionSettings::current_only()).unwrap();
  let writer = store(storage.clone());
  let id = Id("private-aggregate-marker");
  let payload = serde_json::json!({"secret": "private-event-marker"});
  let aggregate = serde_json::json!({"secret": "private-snapshot-marker"});
  writer
    .persist_event_and_snapshot(
      EventEnvelope::new(
        id.clone(),
        1,
        DateTime::<Utc>::from_timestamp(0, 0).unwrap(),
        payload.clone(),
      ),
      SnapshotEnvelope::new(aggregate.clone(), 1),
    )
    .await
    .unwrap();
  assert_eq!(
    writer.get_events_by_id_since_seq_nr(&id, 0).await.unwrap()[0].payload(),
    &payload
  );
  assert_eq!(
    writer
      .get_latest_snapshot_by_id(&id)
      .await
      .unwrap()
      .unwrap()
      .snapshot()
      .unwrap()
      .aggregate(),
    &aggregate
  );

  let output = format!("storage={storage:?}\nstore={writer:?}");
  let forbidden = [
    id.0.to_owned(),
    format!("{:?}", serde_json::to_vec(&payload).unwrap()),
    format!("{:?}", serde_json::to_vec(&aggregate).unwrap()),
  ];
  let leaked: Vec<_> = forbidden
    .iter()
    .filter(|value| output.contains(value.as_str()))
    .collect();
  assert!(leaked.is_empty(), "Debug exposed {leaked:?}: {output}");
}
#[tokio::test]
async fn should_storage_sharing_expose_clone_writes() {
  let storage = MemoryStorage::new(RetentionSettings::current_only()).unwrap();
  let writer = store(storage.clone());
  let reader = store(storage);
  writer.persist_event(event(1)).await.unwrap();
  assert_eq!(
    reader.get_events_by_id_since_seq_nr(&Id("1"), 0).await.unwrap(),
    vec![event(1)]
  );
}
#[tokio::test]
async fn should_storage_sharing_keep_separate_new_independent() {
  let writer = fresh();
  let reader = fresh();
  writer.persist_event(event(1)).await.unwrap();
  assert!(reader
    .get_events_by_id_since_seq_nr(&Id("1"), 0)
    .await
    .unwrap()
    .is_empty());
  assert!(reader.get_latest_snapshot_by_id(&Id("1")).await.unwrap().is_none());
}
#[test]
fn should_storage_settings_accept_current_only_ttl() {
  assert!(
    MemoryStorage::new(RetentionSettings::current_only().with_mode(RetentionMode::Ttl { grace_seconds: 0 })).is_ok()
  );
  assert!(MemoryStorage::new(RetentionSettings::keep_latest(1)).is_ok());
}
#[test]
fn should_storage_settings_reject_zero_and_history_ttl() {
  assert!(matches!(
    MemoryStorage::new(RetentionSettings::keep_latest(0)),
    Err(EventStoreError::Configuration {
      reason: ConfigurationReason::KeepSnapshotCountZero
    })
  ));
  assert!(matches!(
    MemoryStorage::new(RetentionSettings::keep_latest(1).with_mode(RetentionMode::Ttl { grace_seconds: 1 })),
    Err(EventStoreError::Configuration {
      reason: ConfigurationReason::TtlWithKeepSnapshotCount
    })
  ));
}
#[tokio::test]
async fn should_append_concurrently_commit_one_and_read_one() {
  let storage = MemoryStorage::new(RetentionSettings::current_only()).unwrap();
  let barrier = Arc::new(Barrier::new(4));
  let threads: Vec<_> = (0..4)
    .map(|_| {
      let store = store(storage.clone());
      let barrier = barrier.clone();
      std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
        barrier.wait();
        runtime.block_on(store.persist_event(event(1)))
      })
    })
    .collect();
  let results: Vec<_> = threads.into_iter().map(|thread| thread.join().unwrap()).collect();
  assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
  assert_eq!(
    results
      .iter()
      .filter(|result| matches!(result, Err(EventStoreError::OptimisticLock { .. })))
      .count(),
    3
  );
  assert_eq!(
    store(storage).get_events_by_id_since_seq_nr(&Id("1"), 0).await.unwrap(),
    vec![event(1)]
  );
}
#[tokio::test]
async fn should_append_sequence_use_head_after_event_only_append() {
  let store = fresh();
  store.persist_event_and_snapshot(event(1), snapshot(1)).await.unwrap();
  store.persist_event(event(2)).await.unwrap();
  store.persist_event(event(3)).await.unwrap();
  let read = store.get_latest_snapshot_by_id(&Id("1")).await.unwrap().unwrap();
  assert_eq!(read.head_seq_nr(), 3);
  assert_eq!(read.snapshot(), Some(&snapshot(1)));
}
#[tokio::test]
async fn should_append_sequence_reject_duplicates_and_gaps_without_changes() {
  let store = fresh();
  store.persist_event(event(1)).await.unwrap();
  assert!(matches!(
    store.persist_event(event(1)).await,
    Err(EventStoreError::OptimisticLock {
      head_seq_nr: Some(1),
      ..
    })
  ));
  assert!(matches!(
    store.persist_event(event(3)).await,
    Err(EventStoreError::ContractViolation {
      rule: ContractRule::W8Gap,
      ..
    })
  ));
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&Id("1"), 0).await.unwrap(),
    vec![event(1)]
  );
}
#[tokio::test]
async fn should_append_sequence_reject_initial_gap_without_creating_records() {
  let store = fresh();

  let result = store.persist_event(event(2)).await;

  assert!(matches!(
    result,
    Err(EventStoreError::ContractViolation {
      rule: ContractRule::W8Gap,
      seq_nr: Some(2),
      ..
    })
  ));
  assert!(store.get_latest_snapshot_by_id(&Id("1")).await.unwrap().is_none());
  assert!(store
    .get_events_by_id_since_seq_nr(&Id("1"), 0)
    .await
    .unwrap()
    .is_empty());
}
#[tokio::test]
async fn should_read_snapshot_return_head_without_snapshot() {
  let store = fresh();
  assert!(store.get_latest_snapshot_by_id(&Id("1")).await.unwrap().is_none());
  store.persist_event(event(1)).await.unwrap();
  let read = store.get_latest_snapshot_by_id(&Id("1")).await.unwrap().unwrap();
  assert_eq!(read.head_seq_nr(), 1);
  assert!(read.snapshot().is_none());
}
#[tokio::test]
async fn should_read_snapshot_never_mix_head_and_snapshot_during_append() {
  let store = fresh();
  store.persist_event_and_snapshot(event(1), snapshot(1)).await.unwrap();
  let barrier = Arc::new(Barrier::new(2));
  let writer = store.clone();
  let writer_barrier = barrier.clone();
  let thread = std::thread::spawn(move || {
    let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
    writer_barrier.wait();
    runtime.block_on(async {
      for seq in 2..=200 {
        writer
          .persist_event_and_snapshot(event(seq), snapshot(seq))
          .await
          .unwrap();
      }
    });
  });
  barrier.wait();
  for _ in 0..200 {
    let read = store.get_latest_snapshot_by_id(&Id("1")).await.unwrap().unwrap();
    assert_eq!(read.snapshot().unwrap().seq_nr(), read.head_seq_nr());
  }
  thread.join().unwrap();
  assert_eq!(
    store
      .get_latest_snapshot_by_id(&Id("1"))
      .await
      .unwrap()
      .unwrap()
      .head_seq_nr(),
    200
  );
}
#[tokio::test]
async fn should_read_events_return_inclusive_sorted_complete_envelopes() {
  let store = fresh();
  for seq in 1..=3 {
    store.persist_event(event(seq)).await.unwrap();
  }
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&Id("1"), 2).await.unwrap(),
    vec![event(2), event(3)]
  );
}
#[tokio::test]
async fn should_read_events_not_return_other_aggregates_or_below_lower_bound() {
  let store = fresh();
  store.persist_event(event(1)).await.unwrap();
  assert!(store
    .get_events_by_id_since_seq_nr(&Id("1"), 2)
    .await
    .unwrap()
    .is_empty());
  assert!(store
    .get_events_by_id_since_seq_nr(&Id("2"), 0)
    .await
    .unwrap()
    .is_empty());
}
#[tokio::test]
async fn should_public_entry_accept_null_payload_and_empty_identifier() {
  let store = fresh();
  let input = EventEnvelope::new(
    Id(""),
    1,
    DateTime::from_timestamp(0, 0).unwrap(),
    serde_json::Value::Null,
  );
  store
    .persist_event_and_snapshot(input.clone(), SnapshotEnvelope::new(serde_json::Value::Null, 1))
    .await
    .unwrap();
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&Id(""), 0).await.unwrap(),
    vec![input]
  );
  assert_eq!(
    store
      .get_latest_snapshot_by_id(&Id(""))
      .await
      .unwrap()
      .unwrap()
      .snapshot()
      .unwrap()
      .aggregate(),
    &serde_json::Value::Null
  );
}
#[tokio::test]
async fn should_public_entry_reject_invalid_sequence_and_snapshot_mismatch() {
  let store = fresh();
  assert!(matches!(
    store.persist_event(event(0)).await,
    Err(EventStoreError::ContractViolation {
      rule: ContractRule::W6,
      ..
    })
  ));
  assert!(matches!(
    store.persist_event_and_snapshot(event(1), snapshot(2)).await,
    Err(EventStoreError::ContractViolation {
      rule: ContractRule::W9,
      ..
    })
  ));
  assert!(store.get_latest_snapshot_by_id(&Id("1")).await.unwrap().is_none());
}

// Intentionally has no Clone, Debug, Serialize or Deserialize implementation.
struct Payload(Arc<Mutex<Vec<u8>>>);
#[derive(Debug)]
struct Bytes;
impl EventSerializer<Payload> for Bytes {
  fn serialize(&self, payload: &Payload) -> Result<Vec<u8>, EventStoreError> {
    Ok(payload.0.lock().unwrap().clone())
  }

  fn deserialize(&self, bytes: &[u8]) -> Result<Payload, EventStoreError> {
    Ok(Payload(Arc::new(Mutex::new(bytes.to_vec()))))
  }
}
impl SnapshotSerializer<Payload> for Bytes {
  fn serialize(&self, payload: &Payload) -> Result<Vec<u8>, EventStoreError> {
    EventSerializer::serialize(self, payload)
  }

  fn deserialize(&self, bytes: &[u8]) -> Result<Payload, EventStoreError> {
    EventSerializer::deserialize(self, bytes)
  }
}
fn bytes_store(storage: MemoryStorage) -> EventStoreForMemory<Id, Payload, Payload> {
  EventStoreForMemory::with_serializers(storage, Arc::new(Bytes), Arc::new(Bytes))
}
#[tokio::test]
async fn should_serialization_isolate_input_and_returned_payloads() {
  let store = bytes_store(MemoryStorage::new(RetentionSettings::current_only()).unwrap());
  let input = Arc::new(Mutex::new(vec![1]));
  store
    .persist_event_and_snapshot(
      EventEnvelope::new(
        Id("1"),
        1,
        DateTime::from_timestamp(0, 0).unwrap(),
        Payload(input.clone()),
      ),
      SnapshotEnvelope::new(Payload(input.clone()), 1),
    )
    .await
    .unwrap();
  input.lock().unwrap().push(2);
  store
    .persist_event(EventEnvelope::new(
      Id("1"),
      2,
      DateTime::from_timestamp(0, 1).unwrap(),
      Payload(Arc::new(Mutex::new(vec![5]))),
    ))
    .await
    .unwrap();
  let events = store.get_events_by_id_since_seq_nr(&Id("1"), 0).await.unwrap();
  assert_eq!(events.len(), 2);
  assert_eq!(*events[1].payload().0.lock().unwrap(), vec![5]);
  assert_eq!(*events[0].payload().0.lock().unwrap(), vec![1]);
  events[0].payload().0.lock().unwrap().push(3);
  let read = store.get_latest_snapshot_by_id(&Id("1")).await.unwrap().unwrap();
  assert_eq!(read.head_seq_nr(), 2);
  assert_eq!(*read.snapshot().unwrap().aggregate().0.lock().unwrap(), vec![1]);
  read.snapshot().unwrap().aggregate().0.lock().unwrap().push(4);
  assert_eq!(
    *store.get_events_by_id_since_seq_nr(&Id("1"), 0).await.unwrap()[0]
      .payload()
      .0
      .lock()
      .unwrap(),
    vec![1]
  );
  assert_eq!(
    *store
      .get_latest_snapshot_by_id(&Id("1"))
      .await
      .unwrap()
      .unwrap()
      .snapshot()
      .unwrap()
      .aggregate()
      .0
      .lock()
      .unwrap(),
    vec![1]
  );
}
#[tokio::test]
async fn should_serialization_report_incompatible_shared_storage_deserialization() {
  let storage = MemoryStorage::new(RetentionSettings::current_only()).unwrap();
  let writer = bytes_store(storage.clone());
  writer
    .persist_event_and_snapshot(
      EventEnvelope::new(
        Id("1"),
        1,
        DateTime::from_timestamp(0, 0).unwrap(),
        Payload(Arc::new(Mutex::new(vec![255]))),
      ),
      SnapshotEnvelope::new(Payload(Arc::new(Mutex::new(vec![255]))), 1),
    )
    .await
    .unwrap();
  let reader = store(storage);
  assert!(matches!(
    reader.get_events_by_id_since_seq_nr(&Id("1"), 0).await,
    Err(EventStoreError::Serialization {
      phase: SerializationPhase::DeserializeEvent,
      ..
    })
  ));
  assert!(matches!(
    reader.get_latest_snapshot_by_id(&Id("1")).await,
    Err(EventStoreError::Serialization {
      phase: SerializationPhase::DeserializeSnapshot,
      ..
    })
  ));
}
#[tokio::test]
async fn should_construct_without_hooks_or_payload_trait_bounds() {
  let store = bytes_store(MemoryStorage::new(RetentionSettings::current_only()).unwrap());
  let clone = store.clone();
  assert!(clone.get_latest_snapshot_by_id(&Id("1")).await.unwrap().is_none());
}

#[derive(Debug)]
struct RejectSnapshot;
impl SnapshotSerializer<serde_json::Value> for RejectSnapshot {
  fn serialize(&self, _: &serde_json::Value) -> Result<Vec<u8>, EventStoreError> {
    Err(EventStoreError::Serialization {
      phase: SerializationPhase::SerializeSnapshot,
      source: Box::new(std::io::Error::other("INJECTED")),
    })
  }

  fn deserialize(&self, bytes: &[u8]) -> Result<serde_json::Value, EventStoreError> {
    serde_json::from_slice(bytes).map_err(|source| EventStoreError::Serialization {
      phase: SerializationPhase::DeserializeSnapshot,
      source: Box::new(source),
    })
  }
}
#[tokio::test]
async fn should_serialization_not_commit_event_when_snapshot_serialization_fails() {
  use event_store_adapter_rs::JsonEventSerializer;
  let storage = MemoryStorage::new(RetentionSettings::current_only()).unwrap();
  let writer: Store = EventStoreForMemory::with_serializers(
    storage.clone(),
    Arc::new(JsonEventSerializer::new()),
    Arc::new(RejectSnapshot),
  );
  assert!(matches!(
    writer.persist_event_and_snapshot(event(1), snapshot(1)).await,
    Err(EventStoreError::Serialization {
      phase: SerializationPhase::SerializeSnapshot,
      ..
    })
  ));
  let reader = store(storage);
  assert!(reader.get_latest_snapshot_by_id(&Id("1")).await.unwrap().is_none());
  assert!(reader
    .get_events_by_id_since_seq_nr(&Id("1"), 0)
    .await
    .unwrap()
    .is_empty());
}

#[tokio::test]
async fn should_append_concurrently_not_overwrite_winning_event() {
  let store = fresh();
  store.persist_event(event(1)).await.unwrap();
  let barrier = Arc::new(Barrier::new(2));
  let threads: Vec<_> = (0..2)
    .map(|_| {
      let store = store.clone();
      let barrier = barrier.clone();
      std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
        barrier.wait();
        runtime.block_on(store.persist_event(event(1)))
      })
    })
    .collect();
  for thread in threads {
    assert!(matches!(
      thread.join().unwrap(),
      Err(EventStoreError::OptimisticLock { .. })
    ));
  }
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&Id("1"), 0).await.unwrap(),
    vec![event(1)]
  );
}
#[tokio::test]
async fn should_construct_normal_store_without_hooks() {
  let store = fresh();
  store.persist_event(event(1)).await.unwrap();
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&Id("1"), 0).await.unwrap(),
    vec![event(1)]
  );
}
