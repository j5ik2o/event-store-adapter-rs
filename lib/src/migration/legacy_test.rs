use super::super::test_support::raw_event;
use super::*;
use aws_sdk_dynamodb::types::AttributeValue as A;

fn mapping() -> HashMap<String, String> {
  HashMap::from([("Old-Account".into(), "Account".into())])
}

#[test]
fn should_parse_stored_keys_and_preserve_binary_and_nanoseconds() {
  let raw = raw_event();
  let event = event(&raw, &mapping()).unwrap();
  assert_eq!(event.key.aid.as_str(), "Account-value-with-hyphen");
  assert_eq!(event.stored.occurred_at, "-876543211");
  assert_eq!(event.stored.manifest, "");
  assert_eq!(event.stored.payload.as_ref(), &[255, 0, 128]);
}

#[test]
fn should_reject_missing_and_invalid_mapping_and_multibyte_aid_overflow() {
  let raw = raw_event();
  assert!(event(&raw, &HashMap::new()).err().unwrap().contains("P-23"));
  let bad = HashMap::from([("Old-Account".into(), "New-Account".into())]);
  assert!(event(&raw, &bad).err().unwrap().contains("T-11"));
  let mut raw = raw;
  raw.insert("skey".into(), A::S(format!("Old-Account-{}-1", "あ".repeat(341))));
  assert!(event(&raw, &mapping()).err().unwrap().contains("T-12"));
}

#[test]
fn should_reject_nondefault_keys_and_required_attribute_errors() {
  for (name, value) in [
    ("pkey", A::S("Old-Account-x".into())),
    ("skey", A::S("Wrong-value-1".into())),
    ("skey", A::S("Old-Account-value-01".into())),
    ("seq_nr", A::N("0".into())),
    ("seq_nr", A::N("2".into())),
    ("seq_nr", A::N("9007199254740992".into())),
    ("occurred_at", A::N("9223372036854775808".into())),
    ("manifest", A::N("1".into())),
    ("payload", A::S("not bytes".into())),
  ] {
    let mut raw = raw_event();
    raw.insert(name.into(), value);
    assert!(event(&raw, &mapping()).is_err(), "{name}");
  }
  let mut raw = raw_event();
  raw.remove("payload");
  assert!(event(&raw, &mapping()).is_err());
}

#[test]
fn should_accept_empty_type_and_value_and_the_aid_boundary() {
  let mut raw = raw_event();
  raw.insert("pkey".into(), A::S("-0".into()));
  raw.insert("skey".into(), A::S("--1".into()));
  assert_eq!(event(&raw, &HashMap::new()).unwrap().key.aid.as_str(), "-");
  raw.insert("skey".into(), A::S(format!("-{}-1", "あ".repeat(341))));
  assert_eq!(event(&raw, &HashMap::new()).unwrap().key.aid.as_str().len(), 1024);
}

#[test]
fn should_convert_current_active_and_marked_snapshot_without_old_attributes() {
  let mut raw = raw_event();
  raw.remove("occurred_at");
  raw.insert("skey".into(), A::S("Old-Account-value-with-hyphen-0".into()));
  raw.insert("last_updated_at".into(), A::N("-877".into()));
  raw.insert("version".into(), A::N("42".into()));
  raw.insert("ttl".into(), A::N("0".into()));
  let current = snapshot(&raw, &mapping()).unwrap().item();
  assert_eq!(current.len(), 6);
  assert_eq!(current["skey"], A::N("0".into()));
  assert_eq!(current["seq_nr"], A::N("1".into()));
  assert_eq!(current["manifest"], A::S("".into()));
  raw.insert("skey".into(), A::S("Old-Account-value-with-hyphen-1".into()));
  let history = snapshot(&raw, &mapping()).unwrap().item();
  assert_eq!(history["active_history_seq_nr"], A::N("1".into()));
  raw.insert("ttl".into(), A::N("4102444860".into()));
  let marked = snapshot(&raw, &mapping()).unwrap().item();
  assert_eq!(marked["ttl"], A::N("4102444860".into()));
  assert!(!marked.contains_key("active_history_seq_nr"));
  raw.insert("seq_nr".into(), A::N("2".into()));
  assert!(snapshot(&raw, &mapping()).is_err());
}
