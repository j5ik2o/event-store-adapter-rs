use event_store_adapter_conformance_rs::data::{load, Case, CaseKind, Coverage};
use event_store_adapter_conformance_rs::report::{CaseOutcome, NotApplicableReason, RepresentationGap};
use event_store_adapter_conformance_rs::runner::{run, run_case};
use event_store_adapter_conformance_rs::{target_dynamodb, target_memory};
use serde_json::json;
use std::path::Path;

fn scenario() -> Case {
  let event = json!({"aggregate_id":{"type_name":"Account","value":"1"},"seq_nr":1,"occurred_at":"1970-01-01T00:00:00.123456789Z","manifest":"event","payload":null});
  Case {
    id: "synthetic-memory".into(),
    rules: vec!["R-6".into()],
    kind: CaseKind::Scenario,
    file: "synthetic.json".into(),
    body: json!({
      "id":"synthetic-memory","rules":["R-6"],"backends":["memory"],
      "store":{"retention_count":null,"retention_mode":"delete"},
      "fixtures":{"events":{"e1":event.clone()},"snapshots":{"s1":{"aggregate":null,"seq_nr":1,"manifest":"snapshot"}}},
      "steps":[
        {"op":"persistEventAndSnapshot","arguments":{"event":"e1","snapshot":"s1"},"expect":{"result":"success"}},
        {"op":"getEventsByIdSinceSeqNr","arguments":{"aggregate_id":{"type_name":"Account","value":"1"},"seq_nr":0},"expect":{"result":"events","events":["e1"]}},
        {"op":"getLatestSnapshotById","arguments":{"aggregate_id":{"type_name":"Account","value":"1"}},"expect":{"result":"snapshot","snapshot":"s1","head_seq_nr":1}}
      ]
    }),
  }
}
fn execute(case: &Case) -> CaseOutcome {
  run_case(
    case,
    &target_memory::TARGET,
    &Coverage {
      required_rules: vec![],
      exclusions: vec![],
    },
    target_memory::run_case,
  )
}
#[test]
fn should_memory_execute_real_operations_with_null_payload() {
  let case = scenario();
  assert_eq!(execute(&case), CaseOutcome::Passed { values: None });
}
#[test]
fn should_memory_not_report_missing_payload_or_aggregate_as_success() {
  for (fixture, key) in [("events", "payload"), ("snapshots", "aggregate")] {
    let mut case = scenario();
    let name = if fixture == "events" { "e1" } else { "s1" };
    case.body["fixtures"][fixture][name]
      .as_object_mut()
      .unwrap()
      .remove(key);

    let outcome = execute(&case);

    let rejected = matches!(&outcome, CaseOutcome::Failed { .. })
      || matches!(&outcome, CaseOutcome::NotApplicable {
        reason: NotApplicableReason::Representation {
          representation: RepresentationGap::Unrepresentable, detail
        }
      } if !detail.is_empty());
    assert!(
      rejected,
      "必須キー {key} の欠落を成功や未接続として扱わない: {outcome:?}"
    );
  }
}
#[test]
fn should_memory_fail_when_expected_envelope_differs_from_saved_value() {
  let mut case = scenario();
  let mut wrong = case.body["fixtures"]["events"]["e1"].clone();
  wrong["manifest"] = json!("wrong");
  case.body["fixtures"]["events"]["wrong"] = wrong;
  case.body["steps"][1]["expect"]["events"][0] = json!("wrong");
  let outcome = execute(&case);
  let CaseOutcome::Failed {
    failed_operation,
    expected: Some(expected),
    actual: Some(actual),
    ..
  } = outcome
  else {
    panic!("比較失敗に期待値と実結果を載せる: {outcome:?}");
  };
  assert_eq!(failed_operation, Some(2));
  assert_eq!(expected["events"][0]["manifest"], "wrong");
  assert_eq!(actual["events"][0], case.body["fixtures"]["events"]["e1"]);
}
#[test]
fn should_faults_fire_through_real_commit_hook() {
  let mut case = scenario();
  case.body["steps"] = json!([
    {"op":"persistEventAndSnapshot","arguments":{"event":"e1","snapshot":"s1"},"expect":{"error":{"category":"storage","message":{"must_contain":["storage error", "append"],"must_not_contain":["INJECTED"]}}}},
    {"op":"persistEventAndSnapshot","arguments":{"event":"e1","snapshot":"s1"},"expect":{"result":"success"}},
    {"op":"getEventsByIdSinceSeqNr","arguments":{"aggregate_id":{"type_name":"Account","value":"1"},"seq_nr":0},"expect":{"result":"events","events":["e1"]}}
  ]);
  case.body["faults"] = json!([{"operation":1,"phase":"commit","kind":"storage-error","injection":"replace-request","repeat":{"mode":"count","count":1},"details":{"message":"INJECTED"}}]);
  assert_eq!(execute(&case), CaseOutcome::Passed { values: None });
}
#[test]
fn should_faults_fail_when_commit_hook_consumes_less_than_declared_count() {
  let mut case = scenario();
  case.body["steps"] = json!([{"op":"persistEventAndSnapshot","arguments":{"event":"e1","snapshot":"s1"},"expect":{"error":{"category":"storage"}}}]);
  case.body["faults"] = json!([{"operation":1,"phase":"commit","kind":"storage-error","injection":"replace-request","repeat":{"mode":"count","count":2},"details":{"message":"INJECTED"}}]);

  let outcome = execute(&case);

  assert!(
    matches!(&outcome, CaseOutcome::Failed { unfired_faults, .. }
      if unfired_faults.len() == 1 && unfired_faults[0].operation == 1
        && unfired_faults[0].applied == 1
        && unfired_faults[0].declared == event_store_adapter_conformance_rs::fault::Repeat::Count { count: 2 }),
    "{outcome:?}"
  );
}
#[test]
fn should_faults_fail_when_declared_phase_is_never_called() {
  let mut case = scenario();
  case.body["faults"] = json!([{"operation":2,"phase":"commit","kind":"storage-error","injection":"replace-request","repeat":{"mode":"count","count":1},"details":{"message":"INJECTED"}}]);
  assert!(matches!(execute(&case), CaseOutcome::Failed { unfired_faults, .. } if !unfired_faults.is_empty()));
}
#[test]
fn should_retention_cases_remain_unverified() {
  let data = load(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance")).unwrap();
  let reports = run(&data, &target_memory::TARGET, target_memory::run_case);
  for id in [
    "core-retention-delete-1",
    "core-retention-delete-2",
    "core-retention-failure-after-commit",
    "core-retention-query-failure",
  ] {
    assert!(
      matches!(
        reports.iter().find(|report| report.id == id).unwrap().outcome,
        CaseOutcome::Unverified { .. }
      ),
      "{id}"
    );
  }
}
#[test]
fn should_dynamodb_not_inherit_memory_passed_cases() {
  let data = load(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance")).unwrap();
  let reports = run(&data, &target_dynamodb::TARGET, target_dynamodb::run_case);
  for report in reports
    .iter()
    .filter(|report| matches!(report.outcome, CaseOutcome::Passed { .. }))
  {
    let case = data.cases.iter().find(|case| case.id == report.id).unwrap();
    assert_eq!(case.body["operation"], "buildAid");
  }
  assert_eq!(
    reports
      .iter()
      .filter(|report| matches!(report.outcome, CaseOutcome::Passed { .. }))
      .count(),
    9
  );
}
