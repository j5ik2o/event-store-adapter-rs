use std::sync::Mutex;

use aws_sdk_dynamodb::primitives::Blob;

use super::*;
use crate::next::error::SerializationPhase;

#[derive(Debug, Clone)]
struct Id;

impl AggregateId for Id {
  fn type_name(&self) -> String {
    "Account".into()
  }

  fn value(&self) -> String {
    "1".into()
  }
}

struct Aggregate(Vec<u8>);

#[derive(Debug, Default)]
struct Bytes {
  inputs: Mutex<Vec<Vec<u8>>>,
}

impl SnapshotSerializer<Aggregate> for Bytes {
  fn serialize(&self, aggregate: &Aggregate) -> Result<Vec<u8>, EventStoreError> {
    Ok(aggregate.0.clone())
  }

  fn deserialize(&self, data: &[u8]) -> Result<Aggregate, EventStoreError> {
    self.inputs.lock().unwrap().push(data.to_vec());
    Ok(Aggregate(data.to_vec()))
  }
}

fn tables() -> DynamoDbTables {
  DynamoDbTables {
    journal_table_name: "journal".into(),
    snapshot_table_name: "current".into(),
    head_table_name: "head".into(),
    snapshot_history_index_name: "history".into(),
  }
}

fn head() -> Item {
  Item::from([
    ("aid".into(), AttributeValue::S("Account-1".into())),
    ("seq_nr".into(), AttributeValue::N("2".into())),
  ])
}

fn current() -> Item {
  Item::from([
    ("aid".into(), AttributeValue::S("Account-1".into())),
    ("skey".into(), AttributeValue::N("0".into())),
    ("seq_nr".into(), AttributeValue::N("3".into())),
    ("manifest".into(), AttributeValue::S("e\u{301}🙂:free-form".into())),
    ("payload".into(), AttributeValue::B(Blob::new(vec![0, 255, 128]))),
  ])
}

fn read<A: 'static>(
  items: HashMap<String, Vec<Item>>,
  serializer: &dyn SnapshotSerializer<A>,
) -> Result<Option<SnapshotRead<A>>, EventStoreError> {
  let tables = tables();
  let aid = AidString::from_aggregate_id(&Id).unwrap();
  let requested = initial_request(&tables, &aid).unwrap();
  let mut accumulated = HashMap::new();
  let pending = accumulate(
    &mut accumulated,
    &requested,
    BatchGetItemOutput::builder().set_responses(Some(items)).build(),
  )?;
  assert!(pending.is_empty());
  restore_read(&tables, &accumulated, serializer)
}

fn assert_storage<T>(result: Result<T, EventStoreError>) {
  let Err(EventStoreError::Storage { operation, source }) = result else {
    panic!("expected a storage failure")
  };
  assert_eq!(operation, StorageOperation::LoadSnapshot);
  assert!(source.is::<std::io::Error>() || source.is::<std::num::ParseIntError>());
}

#[test]
fn should_request_only_head_and_current_with_strong_consistency() {
  let aid = AidString::from_aggregate_id(&Id).unwrap();
  let requested = initial_request(&tables(), &aid).unwrap();
  assert_eq!(requested.len(), 2);
  assert_eq!(requested["head"].consistent_read(), Some(true));
  assert_eq!(requested["current"].consistent_read(), Some(true));
  assert_eq!(
    requested["head"].keys(),
    &[Item::from([("aid".into(), AttributeValue::S("Account-1".into()))])]
  );
  assert_eq!(
    requested["current"].keys(),
    &[Item::from([
      ("aid".into(), AttributeValue::S("Account-1".into())),
      ("skey".into(), AttributeValue::N("0".into())),
    ])]
  );
}

#[test]
fn should_return_none_without_head_and_never_restore_an_orphan_current() {
  let bytes = Bytes::default();
  assert!(read(HashMap::new(), &bytes).unwrap().is_none());
  let mut orphan = current();
  orphan.remove("payload");
  assert!(read(HashMap::from([("current".into(), vec![orphan])]), &bytes)
    .unwrap()
    .is_none());
  assert!(bytes.inputs.lock().unwrap().is_empty());
}

#[test]
fn should_return_head_without_a_snapshot() {
  let bytes = Bytes::default();
  let restored = read(HashMap::from([("head".into(), vec![head()])]), &bytes)
    .unwrap()
    .unwrap();
  assert_eq!(restored.head_seq_nr(), 2);
  assert!(restored.snapshot().is_none());
  assert!(bytes.inputs.lock().unwrap().is_empty());
}

#[test]
fn should_restore_independent_metadata_and_bytes_in_both_legal_number_orders() {
  for (head_seq_nr, snapshot_seq_nr, manifest) in [(2, 3, "e\u{301}🙂:free-form"), (3, 2, "")] {
    let bytes = Bytes::default();
    let mut head = head();
    let mut current = current();
    head.insert("seq_nr".into(), AttributeValue::N(head_seq_nr.to_string()));
    current.insert("seq_nr".into(), AttributeValue::N(snapshot_seq_nr.to_string()));
    current.insert("manifest".into(), AttributeValue::S(manifest.into()));
    let restored = read(
      HashMap::from([("current".into(), vec![current]), ("head".into(), vec![head])]),
      &bytes,
    )
    .unwrap()
    .unwrap();
    let snapshot = restored.snapshot().unwrap();
    assert_eq!(restored.head_seq_nr(), head_seq_nr);
    assert_eq!(snapshot.seq_nr(), snapshot_seq_nr);
    assert_eq!(snapshot.manifest(), manifest);
    assert_eq!(snapshot.aggregate().0, [0, 255, 128]);
    assert_eq!(*bytes.inputs.lock().unwrap(), [vec![0, 255, 128]]);
  }
}

#[test]
fn should_reject_each_missing_or_wrongly_typed_consumed_attribute() {
  for (table, names) in [
    ("head", vec!["aid", "seq_nr"]),
    ("current", vec!["aid", "skey", "seq_nr", "manifest", "payload"]),
  ] {
    for name in names {
      for replacement in [None, Some(AttributeValue::Bool(true))] {
        let bytes = Bytes::default();
        let mut items = HashMap::from([("head".into(), vec![head()]), ("current".into(), vec![current()])]);
        let item = &mut items.get_mut(table).unwrap()[0];
        match replacement {
          Some(value) => {
            item.insert(name.into(), value);
          }
          None => {
            item.remove(name);
          }
        }
        assert_storage(read(items, &bytes));
        assert!(bytes.inputs.lock().unwrap().is_empty());
      }
    }
  }
}

#[test]
fn should_reject_a_different_aid_or_non_current_key() {
  for table in ["head", "current"] {
    let mut items = HashMap::from([("head".into(), vec![head()]), ("current".into(), vec![current()])]);
    items.get_mut(table).unwrap()[0].insert("aid".into(), AttributeValue::S("Account-other".into()));
    assert_storage(read(items, &Bytes::default()));
  }
  let mut current = current();
  current.insert("skey".into(), AttributeValue::N("3".into()));
  assert_storage(read(
    HashMap::from([("head".into(), vec![head()]), ("current".into(), vec![current])]),
    &Bytes::default(),
  ));
}

#[test]
fn should_reject_unrestorable_or_out_of_range_saved_numbers() {
  for table in ["head", "current"] {
    for number in ["-1", "1.5", "18446744073709551616", "9007199254740992"] {
      let mut items = HashMap::from([("head".into(), vec![head()]), ("current".into(), vec![current()])]);
      items.get_mut(table).unwrap()[0].insert("seq_nr".into(), AttributeValue::N(number.into()));
      assert_storage(read(items, &Bytes::default()));
    }
  }
}

#[test]
fn should_accumulate_either_partial_response_and_retry_only_the_remaining_strong_key() {
  let requested = initial_request(&tables(), &AidString::from_aggregate_id(&Id).unwrap()).unwrap();
  for (processed, pending_table, item, next_item) in [
    ("head", "current", head(), current()),
    ("current", "head", current(), head()),
  ] {
    let mut accumulated = HashMap::new();
    let output = BatchGetItemOutput::builder()
      .responses(processed, vec![item])
      .unprocessed_keys(
        pending_table,
        KeysAndAttributes::builder()
          .set_keys(Some(requested[pending_table].keys().to_vec()))
          .consistent_read(false)
          .build()
          .unwrap(),
      )
      .build();
    let pending = accumulate(&mut accumulated, &requested, output).unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[pending_table].keys(), requested[pending_table].keys());
    assert_eq!(pending[pending_table].consistent_read(), Some(true));
    assert_eq!(accumulated.len(), 1);
    let next = BatchGetItemOutput::builder()
      .responses(pending_table, vec![next_item])
      .build();
    assert!(accumulate(&mut accumulated, &pending, next).unwrap().is_empty());
    assert_eq!(accumulated.len(), 2);
  }
}

#[test]
fn should_distinguish_all_unprocessed_keys_from_a_completed_empty_response() {
  let requested = initial_request(&tables(), &AidString::from_aggregate_id(&Id).unwrap()).unwrap();
  let mut accumulated = HashMap::new();
  let output = BatchGetItemOutput::builder()
    .set_unprocessed_keys(Some(requested.clone()))
    .build();
  let pending = accumulate(&mut accumulated, &requested, output).unwrap();
  assert_eq!(pending.len(), 2);
  assert!(accumulated.is_empty());
  assert!(
    accumulate(&mut accumulated, &pending, BatchGetItemOutput::builder().build())
      .unwrap()
      .is_empty()
  );
  assert!(accumulated.is_empty());
}

#[test]
fn should_reject_unrequested_duplicate_or_already_processed_response_keys() {
  let requested = initial_request(&tables(), &AidString::from_aggregate_id(&Id).unwrap()).unwrap();
  for output in [
    BatchGetItemOutput::builder().responses("other", vec![head()]).build(),
    BatchGetItemOutput::builder()
      .responses("head", vec![head(), head()])
      .build(),
    BatchGetItemOutput::builder()
      .responses("head", vec![head()])
      .unprocessed_keys("head", requested["head"].clone())
      .build(),
    BatchGetItemOutput::builder()
      .unprocessed_keys("other", requested["head"].clone())
      .build(),
    BatchGetItemOutput::builder()
      .unprocessed_keys("head", KeysAndAttributes::builder().keys(current()).build().unwrap())
      .build(),
  ] {
    assert_storage(accumulate(&mut HashMap::new(), &requested, output));
  }
}

#[derive(Debug, thiserror::Error)]
#[error("snapshot restoration failed")]
struct RestoreCause;

#[derive(Debug)]
struct FailingBytes;

impl SnapshotSerializer<Aggregate> for FailingBytes {
  fn serialize(&self, aggregate: &Aggregate) -> Result<Vec<u8>, EventStoreError> {
    Bytes::default().serialize(aggregate)
  }

  fn deserialize(&self, _: &[u8]) -> Result<Aggregate, EventStoreError> {
    Err(EventStoreError::Serialization {
      phase: SerializationPhase::DeserializeSnapshot,
      source: Box::new(RestoreCause),
    })
  }
}

#[test]
fn should_preserve_the_original_snapshot_deserialization_cause() {
  let result = read(
    HashMap::from([("head".into(), vec![head()]), ("current".into(), vec![current()])]),
    &FailingBytes,
  );
  let Err(EventStoreError::Serialization { phase, source }) = result else {
    panic!("expected a Serialization failure")
  };
  assert_eq!(phase, SerializationPhase::DeserializeSnapshot);
  assert!(source.is::<RestoreCause>());
}
