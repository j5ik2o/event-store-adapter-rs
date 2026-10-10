use super::*;
use crate::next::aggregate_id::AggregateId;

#[derive(Debug, Clone)]
struct Id;
impl AggregateId for Id {
  fn type_name(&self) -> String {
    "Account".into()
  }

  fn value(&self) -> String {
    "a-b".into()
  }
}

#[test]
fn should_generate_journal_and_head_from_stored_bytes_and_metadata() {
  let aid = AidString::from_aggregate_id(&Id).unwrap();
  let event = StoredEvent {
    seq_nr: 9,
    occurred_at: "-876543211".into(),
    manifest: "e\u{301}🙂".into(),
    payload: Blob::new([0, 255, 128]),
  };
  let journal = event.journal(&aid);
  assert_eq!(journal.len(), 5);
  assert_eq!(journal["occurred_at"], AttributeValue::N("-876543211".into()));
  assert_eq!(journal["payload"], AttributeValue::B(Blob::new([0, 255, 128])));
  let head = event.head(&aid);
  assert_eq!(head.len(), 4);
  assert_eq!(head["type_name"], AttributeValue::S("Account".into()));
  let events = head["events"].as_l().unwrap();
  assert_eq!(events.len(), 1);
  let metadata = events[0].as_m().unwrap();
  assert_eq!(metadata.len(), 4);
  for name in ["seq_nr", "occurred_at", "manifest", "payload"] {
    assert_eq!(metadata[name], journal[name]);
  }
}

#[test]
fn should_generate_current_active_history_and_marked_history_attributes() {
  let aid = AidString::from_aggregate_id(&Id).unwrap();
  let snapshot = StoredSnapshot {
    seq_nr: 3,
    manifest: String::new(),
    payload: Blob::new([255]),
    last_updated_at: "-877".into(),
  };
  let current = snapshot.item(&aid, SnapshotKind::Current);
  assert_eq!(current.len(), 6);
  assert_eq!(current["skey"], AttributeValue::N("0".into()));
  assert_eq!(current["seq_nr"], AttributeValue::N("3".into()));
  let history = snapshot.item(&aid, SnapshotKind::History { ttl: None });
  assert_eq!(history.len(), 7);
  assert_eq!(history["active_history_seq_nr"], AttributeValue::N("3".into()));
  assert!(!history.contains_key("ttl"));
  let marked = snapshot.item(
    &aid,
    SnapshotKind::History {
      ttl: Some("4102444860".into()),
    },
  );
  assert_eq!(marked.len(), 7);
  assert_eq!(marked["ttl"], AttributeValue::N("4102444860".into()));
  assert!(!marked.contains_key("active_history_seq_nr"));
}
