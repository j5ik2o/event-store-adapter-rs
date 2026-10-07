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
fn should_generation_faults_without_connected_hooks_remain_unverified() {
  for (phase, kind) in [
    ("commit", "storage-error"),
    ("read-events", "storage-error"),
    ("read-snapshot", "storage-error"),
    ("serialize-event", "serialization-error"),
    ("serialize-snapshot", "serialization-error"),
    ("deserialize-event", "serialization-error"),
    ("deserialize-snapshot", "serialization-error"),
    ("retention-query", "storage-error"),
    ("retention-delete", "storage-error"),
    ("retention-query", "sdk-response"),
  ] {
    let mut case = scenario();
    let (injection, details) = if kind == "sdk-response" {
      ("replace-response", json!({"history_pages": [[1]]}))
    } else {
      ("replace-request", json!({"message": "INJECTED"}))
    };
    case.body["faults"] = json!([{
      "operation": 0, "phase": phase, "kind": kind, "injection": injection,
      "repeat": {"mode": "count", "count": 1}, "details": details
    }]);
    let outcome = execute(&case);
    assert!(
      matches!(outcome, CaseOutcome::Unverified { .. }),
      "{phase}: {outcome:?}"
    );
  }
}
#[test]
fn should_required_retention_cases_pass_real_operations() {
  let data = load(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance")).unwrap();
  let reports = run(&data, &target_memory::TARGET, target_memory::run_case);
  const REQUIRED_MEMORY_RETENTION_CASES: &[&str] = &[
    "core-retention-delete-1",
    "core-retention-delete-2",
    "core-retention-failure-after-commit",
    "core-retention-query-failure",
  ];
  for &id in REQUIRED_MEMORY_RETENTION_CASES {
    let report = reports
      .iter()
      .find(|report| report.id == id)
      .expect("必須保持ケースが存在する");
    assert_eq!(report.outcome, CaseOutcome::Passed { values: None }, "{id}");
  }
}

fn retention_case(id: &str) -> Case {
  load(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance"))
    .unwrap()
    .cases
    .into_iter()
    .find(|case| case.id == id)
    .unwrap()
}

#[test]
fn should_report_actual_history_when_retention_expectation_is_wrong() {
  let mut case = retention_case("core-retention-delete-1");
  case.body["steps"][1]["observe"]["history"]["active"] = json!([1, 2]);
  let outcome = execute(&case);
  assert!(
    matches!(outcome, CaseOutcome::Failed { failed_operation: Some(2), actual: Some(ref actual), .. }
    if actual["active"] == json!([2])),
    "{outcome:?}"
  );
}

#[test]
fn should_report_actual_notification_when_retention_expectation_is_wrong() {
  let mut case = retention_case("core-retention-failure-after-commit");
  case.body["steps"][1]["observe"]["notifications"] = json!([]);
  let outcome = execute(&case);
  assert!(
    matches!(outcome, CaseOutcome::Failed { failed_operation: Some(2), actual: Some(ref actual), .. }
    if actual == &json!(["retention-failure"])),
    "{outcome:?}"
  );
}

#[test]
fn should_not_generate_notifications_from_expectations_without_a_real_failure() {
  let mut case = retention_case("core-retention-delete-1");
  case.body["steps"][0]["observe"]["notifications"] = json!(["retention-failure"]);
  let outcome = execute(&case);
  assert!(
    matches!(outcome, CaseOutcome::Failed { failed_operation: Some(1), actual: Some(ref actual), .. }
    if actual == &json!([])),
    "{outcome:?}"
  );
}

#[test]
fn should_retention_history_pages_count_one_application_and_add_missing_new_history() {
  let mut case = retention_case("core-retention-delete-2");
  case.body["faults"][2]["details"] = json!({"history_pages":[[1],[2]], "omit_just_written_history":true});
  assert_eq!(execute(&case), CaseOutcome::Passed { values: None });
  case.body["faults"][2]["repeat"]["count"] = json!(2);
  let outcome = execute(&case);
  assert!(
    matches!(outcome, CaseOutcome::Failed { unfired_faults, .. } if unfired_faults.len() == 1 && unfired_faults[0].applied == 1)
  );
}

#[test]
fn should_keep_first_history_with_duplicate_numbers_in_one_page() {
  let mut case = retention_case("core-retention-delete-1");
  case.body["faults"][0]["details"]["history_pages"] = json!([[1, 1]]);
  assert_eq!(execute(&case), CaseOutcome::Passed { values: None });
}

#[test]
fn should_keep_first_history_with_duplicate_numbers_across_pages() {
  let mut case = retention_case("core-retention-delete-1");
  case.body["faults"][0]["details"]["history_pages"] = json!([[1], [1]]);
  assert_eq!(execute(&case), CaseOutcome::Passed { values: None });
}

#[test]
fn should_keep_second_history_with_duplicate_numbers_in_one_page() {
  let mut case = retention_case("core-retention-delete-1");
  case.body["faults"][1]["details"]["history_pages"] = json!([[2, 2, 1]]);
  assert_eq!(execute(&case), CaseOutcome::Passed { values: None });
}

#[test]
fn should_keep_second_history_with_duplicate_numbers_across_pages() {
  let mut case = retention_case("core-retention-delete-1");
  case.body["faults"][1]["details"]["history_pages"] = json!([[2], [2, 1]]);
  assert_eq!(execute(&case), CaseOutcome::Passed { values: None });
}

#[test]
fn should_keep_two_latest_histories_with_duplicate_unsorted_numbers_across_pages() {
  let mut case = retention_case("core-retention-delete-2");
  case.body["faults"][2]["details"]["history_pages"] = json!([[3], [2, 3, 1, 2]]);
  assert_eq!(execute(&case), CaseOutcome::Passed { values: None });
}

#[test]
fn should_reject_history_response_using_unsaved_history_or_violating_omission() {
  for details in [
    json!({"history_pages":[[99]]}),
    json!({"history_pages":[[1]], "omit_just_written_history":true}),
  ] {
    let mut case = retention_case("core-retention-delete-1");
    case.body["faults"][0]["details"] = details;
    assert!(matches!(
      execute(&case),
      CaseOutcome::Failed {
        failed_operation: Some(1),
        ..
      }
    ));
  }
}

#[test]
fn should_retention_faults_fail_when_unfired_or_not_fully_applied() {
  for phase in ["retention-query", "retention-delete"] {
    let mut case = retention_case("core-retention-failure-after-commit");
    let fault = case.body["faults"]
      .as_array()
      .unwrap()
      .iter()
      .find(|fault| fault["phase"] == phase)
      .unwrap()
      .clone();
    case.body["faults"] = json!([fault]);
    case.body["faults"][0]["repeat"]["count"] = json!(2);
    let outcome = execute(&case);
    assert!(
      matches!(outcome, CaseOutcome::Failed { ref unfired_faults, .. } if unfired_faults.len() == 1 && unfired_faults[0].applied == 1),
      "{outcome:?}"
    );
    case.body["faults"][0]["repeat"]["count"] = json!(1);
    case.body["faults"][0]["operation"] = json!(3);
    case.body["steps"][1].as_object_mut().unwrap().remove("observe");
    let outcome = execute(&case);
    assert!(
      matches!(outcome, CaseOutcome::Failed { ref unfired_faults, .. } if unfired_faults.len() == 1 && unfired_faults[0].applied == 0),
      "{outcome:?}"
    );
  }
}

#[test]
fn should_not_connect_unsupported_retention_fault_methods() {
  for (phase, kind, injection, details) in [
    (
      "retention-query",
      "storage-error",
      "replace-response",
      json!({"message":"FAIL"}),
    ),
    (
      "retention-delete",
      "storage-error",
      "replace-response",
      json!({"message":"FAIL"}),
    ),
    (
      "retention-query",
      "sdk-response",
      "replace-request",
      json!({"history_pages":[[1]]}),
    ),
    (
      "retention-delete",
      "sdk-response",
      "replace-request",
      json!({"unprocessed_first_n":1}),
    ),
  ] {
    let mut case = retention_case("core-retention-delete-1");
    case.body["faults"] = json!([{"operation":1,"phase":phase,"kind":kind,"injection":injection,"repeat":{"mode":"count","count":1},"details":details}]);
    assert!(matches!(execute(&case), CaseOutcome::Unverified { .. }));
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
