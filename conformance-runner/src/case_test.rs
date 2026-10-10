use super::*;
use event_store_adapter_rs::next::{
  error::{ContractRule, SerializationPhase},
  memory::EventStoreForMemory,
};

fn body() -> Value {
  json!({"store":{"retention_count":1,"retention_mode":"delete"},"fixtures":{
    "events":{"e1":{"aggregate_id":{"type_name":"Order","value":"9"},"seq_nr":1,
      "occurred_at":"2026-10-05T00:00:00.123456789Z","manifest":"event-kind","payload":{"number":1}},
      "e2":{"aggregate_id":{"type_name":"Order","value":"9"},"seq_nr":2,
      "occurred_at":"2026-10-05T00:00:00.123456789Z","payload":{"number":2}}},
    "snapshots":{"s2":{"seq_nr":2,"manifest":"aggregate-kind","aggregate":{"total":2}}}}})
}

#[tokio::test]
async fn should_call_all_four_public_operations_and_compare_all_envelope_fields() {
  let body = body();
  let store = EventStoreForMemory::<CaseId, Value, Value>::new(
    event_store_adapter_rs::next::memory::MemoryStorage::new(RetentionSettings::current_only()).unwrap(),
  );
  let first = json!({"op":"persistEvent","arguments":{"event":"e1"},"expect":{"result":"success"}});
  assert!(step(&store, &body, &first).await.is_ok());
  let pair =
    json!({"op":"persistEventAndSnapshot","arguments":{"event":"e2","snapshot":"s2"},"expect":{"result":"success"}});
  assert!(step(&store, &body, &pair).await.is_ok());
  let read = json!({"op":"getEventsByIdSinceSeqNr","arguments":{"aggregate_id":{"type_name":"Order","value":"9"},"seq_nr":1},
    "expect":{"result":"events","events":["e1","e2"]}});
  let result = step(&store, &body, &read).await.ok().expect("全封筒");
  assert_eq!(result["events"][0]["manifest"], "event-kind");
  assert_eq!(result["events"][0]["occurred_at"], "2026-10-05T00:00:00.123456789Z");
  let latest = json!({"op":"getLatestSnapshotById","arguments":{"aggregate_id":{"type_name":"Order","value":"9"}},
    "expect":{"result":"snapshot","head_seq_nr":2,"snapshot":"s2"}});
  assert!(step(&store, &body, &latest).await.is_ok());
  let mut changed = body.clone();
  changed["fixtures"]["events"]["e1"]["manifest"] = json!("other");
  let failure = step(&store, &changed, &read).await.expect_err("metadata差分");
  assert_eq!(failure.actual.unwrap()["events"][0]["manifest"], "event-kind");
  assert!(write(&store, &body, &read).await.is_err());
}

#[test]
fn should_compare_native_category_rule_and_required_diagnostics() {
  let error = EventStoreError::ContractViolation {
    rule: ContractRule::T9,
    seq_nr: Some(0),
    snapshot_seq_nr: None,
  };
  let expect = json!({"error":{"category":"contract-violation","rule":"T-9","message":{"must_contain":["seq_nr","0"],"must_not_contain":["PRIVATE"]}}});
  assert!(compare_error(&expect, &error).is_ok());
  let mut different = expect.clone();
  different["error"]["rule"] = json!("T-4");
  assert!(compare_error(&different, &error).is_err());
  different = expect.clone();
  different["error"]["message"]["must_contain"] = json!(["PRIVATE"]);
  assert!(compare_error(&different, &error).is_err());
  let serializer = EventStoreError::Serialization {
    phase: SerializationPhase::SerializeEvent,
    source: Box::new(std::io::Error::other("PRIVATE")),
  };
  assert_eq!(error_value(&serializer)["phase"], "serialize-event");
  assert!(compare_error(&expect, &serializer).is_err());
  assert!(matches!(
    failed(Some(3), "failure"),
    CaseOutcome::Failed {
      failed_operation: Some(3),
      ..
    }
  ));
  assert!(matches!(unverified("unavailable"), CaseOutcome::Unverified { .. }));
}

#[test]
fn should_preserve_integer_and_time_precision_and_validate_fixture_inputs() {
  assert_eq!(
    seq(&serde_json::from_str("9007199254740991.0").unwrap()).unwrap(),
    9_007_199_254_740_991
  );
  assert!(seq(&json!(-1)).is_err());
  assert!(seq(&json!(1.5)).is_err());
  assert!(id(&json!({"type_name":"Order"})).is_err());
  assert!(time(&json!("not-a-time")).is_err());
  assert!(fixture(&body(), "events", &json!("absent")).is_err());
  assert!(settings(&json!({"store":{"retention_count":1,"retention_mode":"invalid"}})).is_err());
  let ttl = settings(&json!({"store":{"retention_count":1,"retention_mode":"ttl","ttl_grace_seconds":30}})).unwrap();
  assert_eq!(ttl.mode(), &RetentionMode::Ttl { grace_seconds: 30 });
}

#[tokio::test]
async fn should_record_native_value_errors_and_full_time_round_trip_results() {
  let data = crate::data::load(&std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance")).unwrap();
  for (case_id, rule, seq_nr) in [
    ("seq-zero-event", "W-6", 0u64),
    ("seq-above-max-value", "T-9", 9_007_199_254_740_992),
    ("occurred-at-below-min", "T-13", 7),
  ] {
    let case = data.cases.iter().find(|v| v.id == case_id).unwrap();
    let store = EventStoreForMemory::<CaseId, Value, Value>::new(
      event_store_adapter_rs::next::memory::MemoryStorage::new(RetentionSettings::current_only()).unwrap(),
    );
    let mut observed = Vec::new();
    assert!(run_value(case, &store, &mut observed).await.is_ok());
    let actual = &observed.last().unwrap()["actual"];
    assert_eq!(actual["category"], "contract-violation");
    assert_eq!(actual["rule"], rule);
    assert_eq!(actual["seq_nr"], seq_nr);
  }
  let case = data.cases.iter().find(|v| v.id == "occurred-at-before-epoch").unwrap();
  let store = EventStoreForMemory::<CaseId, Value, Value>::new(
    event_store_adapter_rs::next::memory::MemoryStorage::new(RetentionSettings::current_only()).unwrap(),
  );
  let mut observed = Vec::new();
  let values = run_value(case, &store, &mut observed).await.ok().unwrap().unwrap();
  assert_eq!(values.actual["value"], "-1");
  assert_eq!(observed[0]["actual"]["result"], "success");
  assert_eq!(
    observed[1]["actual"]["events"][0]["occurred_at"],
    "1969-12-31T23:59:59.999999999Z"
  );
}
