use aws_sdk_dynamodb::primitives::Blob;

use super::*;
use crate::next::error::SerializationPhase;

#[derive(Debug, Clone, PartialEq)]
struct Id {
  caller: String,
}

impl AggregateId for Id {
  fn type_name(&self) -> String {
    "Account".into()
  }

  fn value(&self) -> String {
    "1".into()
  }
}

struct Payload(Vec<u8>);

#[derive(Debug)]
struct Bytes;

impl EventSerializer<Payload> for Bytes {
  fn serialize(&self, payload: &Payload) -> Result<Vec<u8>, EventStoreError> {
    Ok(payload.0.clone())
  }

  fn deserialize(&self, data: &[u8]) -> Result<Payload, EventStoreError> {
    Ok(Payload(data.to_vec()))
  }
}

fn item() -> HashMap<String, AttributeValue> {
  HashMap::from([
    ("aid".into(), AttributeValue::S("Account-1".into())),
    ("seq_nr".into(), AttributeValue::N("2".into())),
    ("occurred_at".into(), AttributeValue::N("-876543211".into())),
    ("manifest".into(), AttributeValue::S("e\u{301}🙂".into())),
    ("payload".into(), AttributeValue::B(Blob::new(vec![0, 255, 128]))),
  ])
}

fn assert_storage(item: &HashMap<String, AttributeValue>) {
  let id = Id {
    caller: "reader".into(),
  };
  let aid = AidString::from_aggregate_id(&id).unwrap();
  assert!(matches!(
    restore_event(item, &id, &aid, &Bytes),
    Err(EventStoreError::Storage {
      operation: StorageOperation::LoadEvents,
      ..
    })
  ));
}

#[test]
fn should_restore_saved_metadata_bytes_and_caller_id() {
  let id = Id {
    caller: "reader".into(),
  };
  let aid = AidString::from_aggregate_id(&id).unwrap();
  let restored = restore_event(&item(), &id, &aid, &Bytes).unwrap();
  assert_eq!(restored.aggregate_id(), &id);
  assert_eq!(restored.seq_nr(), 2);
  assert_eq!(restored.occurred_at().timestamp_nanos_opt(), Some(-876543211));
  assert_eq!(restored.manifest(), "e\u{301}🙂");
  assert_eq!(restored.payload().0, [0, 255, 128]);
}

#[test]
fn should_restore_empty_manifest_and_signed_nanosecond_boundaries() {
  let id = Id {
    caller: "reader".into(),
  };
  let aid = AidString::from_aggregate_id(&id).unwrap();
  for nanos in [i64::MIN, 0, i64::MAX] {
    let mut stored = item();
    stored.insert("occurred_at".into(), AttributeValue::N(nanos.to_string()));
    stored.insert("manifest".into(), AttributeValue::S(String::new()));
    let restored = restore_event(&stored, &id, &aid, &Bytes).unwrap();
    assert_eq!(restored.occurred_at().timestamp_nanos_opt(), Some(nanos));
    assert_eq!(restored.manifest(), "");
  }
}

#[test]
fn should_reject_each_missing_or_wrongly_typed_saved_attribute() {
  for name in ["aid", "seq_nr", "occurred_at", "manifest", "payload"] {
    let mut stored = item();
    stored.remove(name);
    assert_storage(&stored);
    stored.insert(name.into(), AttributeValue::Bool(true));
    assert_storage(&stored);
  }
}

#[test]
fn should_reject_an_aid_that_differs_from_the_checked_request() {
  let mut stored = item();
  stored.insert("aid".into(), AttributeValue::S("Account-10".into()));
  assert_storage(&stored);
}

#[test]
fn should_reject_numbers_that_cannot_restore_seq_nr_or_nanoseconds() {
  for (name, numbers) in [
    ("seq_nr", ["-1", "1.5", "18446744073709551616"]),
    ("occurred_at", ["-9223372036854775809", "1.5", "9223372036854775808"]),
  ] {
    for number in numbers {
      let mut stored = item();
      stored.insert(name.into(), AttributeValue::N(number.into()));
      assert_storage(&stored);
    }
  }
}

#[derive(Debug, thiserror::Error)]
#[error("payload restoration failed")]
struct DeserializeCause;

#[derive(Debug)]
struct FailingBytes;

impl EventSerializer<Payload> for FailingBytes {
  fn serialize(&self, payload: &Payload) -> Result<Vec<u8>, EventStoreError> {
    Bytes.serialize(payload)
  }

  fn deserialize(&self, _: &[u8]) -> Result<Payload, EventStoreError> {
    Err(EventStoreError::Serialization {
      phase: SerializationPhase::DeserializeEvent,
      source: Box::new(DeserializeCause),
    })
  }
}

#[test]
fn should_propagate_the_original_deserialization_error() {
  let id = Id {
    caller: "reader".into(),
  };
  let aid = AidString::from_aggregate_id(&id).unwrap();
  let result = restore_event(&item(), &id, &aid, &FailingBytes);
  let Err(EventStoreError::Serialization { phase, source }) = result else {
    panic!("expected a Serialization failure")
  };
  assert_eq!(phase, SerializationPhase::DeserializeEvent);
  assert!(source.is::<DeserializeCause>());
}
