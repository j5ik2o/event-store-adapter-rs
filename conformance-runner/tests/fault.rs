//! 障害の登録と、発火の数え方（適用の回数）の試験。

use event_store_adapter_conformance_rs::data::load;
use event_store_adapter_conformance_rs::fault::{FaultError, FaultPlan, Phase, Repeat, UnfiredFault};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

fn conformance_dir() -> PathBuf {
  Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance")
}

fn count(times: u32) -> Value {
  json!({"mode": "count", "count": times})
}

fn until_operation_finishes() -> Value {
  json!({"mode": "until-operation-finishes"})
}

fn fault(operation: u32, phase: &str, repeat: Value) -> Value {
  json!({
    "operation": operation,
    "phase": phase,
    "kind": "storage-error",
    "injection": "replace-request",
    "repeat": repeat,
    "details": {"message": "INJECTED"}
  })
}

/// 手順を `steps` 個持ち、`faults` を宣言した場面を作る。
fn case(steps: usize, faults: Vec<Value>) -> Value {
  json!({"steps": vec![json!({}); steps], "faults": faults})
}

fn register(case: &Value) -> FaultPlan {
  FaultPlan::register(case).expect("宣言は正しい")
}

fn unfired(index: usize, operation: u32, phase: Phase, declared: Repeat, applied: u32) -> UnfiredFault {
  UnfiredFault {
    index,
    operation,
    phase,
    declared,
    applied,
  }
}

/// 宣言を登録できないとき、その誤りを返す。登録できてしまったら試験を失敗にする。
fn register_error(case: &Value) -> FaultError {
  match FaultPlan::register(case) {
    Ok(_) => panic!("不正な宣言を登録できてはならない: {case}"),
    Err(error) => error,
  }
}

#[test]
fn test_register_rejects_count_of_zero_so_that_an_unapplied_fault_is_never_counted_as_fired() {
  let error = register_error(&case(
    1,
    vec![fault(1, "commit", count(1)), fault(1, "commit", count(0))],
  ));

  assert_eq!(error.index, 1);
}

#[test]
fn test_register_rejects_operation_beyond_the_number_of_steps() {
  let error = register_error(&case(1, vec![fault(2, "commit", count(1))]));

  assert_eq!(error.index, 0);
}

#[test]
fn test_register_rejects_unknown_phase() {
  let error = register_error(&case(
    1,
    vec![fault(1, "commit", count(1)), fault(1, "no-such-phase", count(1))],
  ));

  assert_eq!(error.index, 1);
}

#[test]
fn test_register_rejects_details_that_is_not_an_object() {
  let mut declared = fault(1, "commit", count(1));
  declared["details"] = json!("INJECTED");

  let error = register_error(&case(1, vec![declared]));

  assert_eq!(error.index, 0);
}

#[test]
fn test_count_one_fault_fires_after_one_application() {
  let plan = register(&case(1, vec![fault(1, "commit", count(1))]));
  let mut operation = plan.begin_operation(1);

  let applied = operation.start_application(Phase::Commit);

  assert_eq!(applied.map(|fault| fault.index), Some(0));
  assert_eq!(operation.finish(), Ok(()));
}

#[test]
fn test_count_two_fault_does_not_fire_after_one_application() {
  let plan = register(&case(1, vec![fault(1, "commit", count(2))]));
  let mut operation = plan.begin_operation(1);

  operation.start_application(Phase::Commit);

  assert_eq!(
    operation.finish(),
    Err(vec![unfired(0, 1, Phase::Commit, Repeat::Count { count: 2 }, 1)])
  );
}

#[test]
fn test_count_two_fault_fires_after_two_applications() {
  let plan = register(&case(1, vec![fault(1, "commit", count(2))]));
  let mut operation = plan.begin_operation(1);

  operation.start_application(Phase::Commit);
  operation.start_application(Phase::Commit);

  assert_eq!(operation.finish(), Ok(()));
}

#[test]
fn test_fault_without_any_application_is_unfired() {
  let plan = register(&case(1, vec![fault(1, "commit", count(1))]));
  let operation = plan.begin_operation(1);

  assert_eq!(
    operation.finish(),
    Err(vec![unfired(0, 1, Phase::Commit, Repeat::Count { count: 1 }, 0)])
  );
}

#[test]
fn test_unfired_fault_serializes_declared_and_applied_counts() {
  let unfired_fault = unfired(0, 1, Phase::Commit, Repeat::Count { count: 2 }, 1);

  let value = serde_json::to_value(&unfired_fault).expect("直列化できる");

  assert_eq!(value["declared"], json!({"mode": "count", "count": 2}));
  assert_eq!(value["applied"], json!(1));
}

#[test]
fn test_until_operation_finishes_fault_fires_after_one_application() {
  let plan = register(&case(1, vec![fault(1, "retention-delete", until_operation_finishes())]));
  let mut operation = plan.begin_operation(1);

  operation.start_application(Phase::RetentionDelete);

  assert_eq!(operation.finish(), Ok(()));
}

#[test]
fn test_until_operation_finishes_fault_applies_to_every_request_of_the_operation() {
  let plan = register(&case(1, vec![fault(1, "retention-delete", until_operation_finishes())]));
  let mut operation = plan.begin_operation(1);

  let applications: Vec<Option<usize>> = (0..3)
    .map(|_| {
      operation
        .start_application(Phase::RetentionDelete)
        .map(|fault| fault.index)
    })
    .collect();

  assert_eq!(applications, vec![Some(0), Some(0), Some(0)]);
  assert_eq!(operation.finish(), Ok(()));
}

#[test]
fn test_until_operation_finishes_fault_without_any_application_is_unfired() {
  let plan = register(&case(1, vec![fault(1, "retention-delete", until_operation_finishes())]));
  let operation = plan.begin_operation(1);

  assert_eq!(
    operation.finish(),
    Err(vec![unfired(
      0,
      1,
      Phase::RetentionDelete,
      Repeat::UntilOperationFinishes,
      0
    )])
  );
}

#[test]
fn test_same_phase_faults_are_consumed_in_array_order_after_exhausting_each_count() {
  let plan = register(&case(
    1,
    vec![fault(1, "commit", count(2)), fault(1, "commit", count(1))],
  ));
  let mut operation = plan.begin_operation(1);

  let applications: Vec<Option<usize>> = (0..4)
    .map(|_| operation.start_application(Phase::Commit).map(|fault| fault.index))
    .collect();

  assert_eq!(applications, vec![Some(0), Some(0), Some(1), None]);
  assert_eq!(operation.finish(), Ok(()));
}

#[test]
fn test_faults_of_different_phases_are_all_registered_and_consumed_independently() {
  let plan = register(&case(
    1,
    vec![fault(1, "commit", count(1)), fault(1, "retention-delete", count(1))],
  ));
  let mut operation = plan.begin_operation(1);

  // 宣言の順とは逆に、後ろの段階から適用する。
  let retention = operation
    .start_application(Phase::RetentionDelete)
    .map(|fault| fault.index);
  let commit = operation.start_application(Phase::Commit).map(|fault| fault.index);
  let other = operation.start_application(Phase::ReadEvents).map(|fault| fault.index);

  assert_eq!((retention, commit, other), (Some(1), Some(0), None));
  assert_eq!(operation.finish(), Ok(()));
}

#[test]
fn test_application_of_one_phase_does_not_consume_fault_of_another_phase() {
  let plan = register(&case(
    1,
    vec![fault(1, "commit", count(1)), fault(1, "retention-delete", count(1))],
  ));
  let mut operation = plan.begin_operation(1);

  operation.start_application(Phase::RetentionDelete);

  assert_eq!(
    operation.finish(),
    Err(vec![unfired(0, 1, Phase::Commit, Repeat::Count { count: 1 }, 0)])
  );
}

#[test]
fn test_fault_of_one_operation_is_not_applied_in_another_operation() {
  let plan = register(&case(2, vec![fault(1, "commit", count(1))]));
  let mut other_operation = plan.begin_operation(2);

  let applied = other_operation.start_application(Phase::Commit);

  assert!(applied.is_none());
  assert_eq!(other_operation.finish(), Ok(()));
}

#[test]
fn test_application_counts_are_not_carried_over_to_the_next_operation() {
  let plan = register(&case(
    2,
    vec![fault(1, "commit", count(2)), fault(2, "commit", count(2))],
  ));
  let mut first = plan.begin_operation(1);
  first.start_application(Phase::Commit);
  first.start_application(Phase::Commit);
  assert_eq!(first.finish(), Ok(()));
  let mut second = plan.begin_operation(2);

  let applied = second.start_application(Phase::Commit).map(|fault| fault.index);

  assert_eq!(applied, Some(1));
  assert_eq!(
    second.finish(),
    Err(vec![unfired(1, 2, Phase::Commit, Repeat::Count { count: 2 }, 1)])
  );
}

#[test]
fn test_operation_zero_fault_for_store_creation_can_be_registered_and_applied() {
  let plan = register(&case(0, vec![fault(0, "configuration-create", count(1))]));
  let mut creation = plan.begin_operation(0);

  let applied = creation
    .start_application(Phase::ConfigurationCreate)
    .map(|fault| fault.index);

  assert_eq!(plan.faults().len(), 1);
  assert_eq!(plan.faults()[0].operation, 0);
  assert_eq!(applied, Some(0));
  assert_eq!(creation.finish(), Ok(()));
}

#[test]
fn test_register_reads_faults_of_real_retention_failure_case() {
  let data = load(&conformance_dir()).expect("実データを読める");
  let body = &data
    .cases
    .iter()
    .find(|case| case.id == "core-retention-failure-after-commit")
    .expect("core-retention-failure-after-commit がある")
    .body;

  let plan = register(body);

  let registered: Vec<(u32, Phase)> = plan
    .faults()
    .iter()
    .map(|fault| (fault.operation, fault.phase))
    .collect();
  assert_eq!(
    registered,
    vec![
      (2, Phase::RetentionDelete),
      (2, Phase::RetentionQuery),
      (5, Phase::RetentionQuery)
    ]
  );
}

#[test]
fn test_real_retention_failure_case_consumes_faults_per_operation() {
  let data = load(&conformance_dir()).expect("実データを読める");
  let body = &data
    .cases
    .iter()
    .find(|case| case.id == "core-retention-failure-after-commit")
    .expect("core-retention-failure-after-commit がある")
    .body;
  let plan = register(body);

  let mut second_operation = plan.begin_operation(2);
  let delete = second_operation
    .start_application(Phase::RetentionDelete)
    .map(|fault| fault.index);
  let query = second_operation
    .start_application(Phase::RetentionQuery)
    .map(|fault| fault.index);
  let query_again = second_operation
    .start_application(Phase::RetentionQuery)
    .map(|fault| fault.index);
  let mut fifth_operation = plan.begin_operation(5);
  let fifth_query = fifth_operation
    .start_application(Phase::RetentionQuery)
    .map(|fault| fault.index);

  assert_eq!((delete, query, query_again), (Some(0), Some(1), None));
  assert_eq!(fifth_query, Some(2));
  assert_eq!(second_operation.finish(), Ok(()));
  assert_eq!(fifth_operation.finish(), Ok(()));
}

// ---------------------------------------------------------------------------
// 整数の書き方（operation・repeat.count）。値は JSON の文字列から作る。
// ---------------------------------------------------------------------------

/// `operation` と `repeat` を、JSON の文字列のまま差し込んだ障害 1 つを持つ、手順が 1 つの場面を作る。
fn case_with_spelled_fault(operation: &str, repeat: &str) -> Value {
  serde_json::from_str(&format!(
    r#"{{
      "steps": [{{}}],
      "faults": [{{
        "operation": {operation},
        "phase": "commit",
        "kind": "storage-error",
        "injection": "replace-request",
        "repeat": {repeat},
        "details": {{"message": "INJECTED"}}
      }}]
    }}"#
  ))
  .expect("JSON")
}

#[test]
fn test_register_accepts_operation_and_count_written_as_whole_numbers_in_any_spelling() {
  for (operation, count) in [
    ("1", "2"),
    ("1.0", "2.0"),
    ("1e0", "2e0"),
    ("10e-1", "20e-1"),
    ("0.1e1", "0.2e1"),
  ] {
    let plan = register(&case_with_spelled_fault(
      operation,
      &format!(r#"{{"mode": "count", "count": {count}}}"#),
    ));

    let fault = &plan.faults()[0];
    assert_eq!(fault.operation, 1, "operation: {operation}");
    assert_eq!(fault.repeat, Repeat::Count { count: 2 }, "count: {count}");
  }
}

#[test]
fn test_register_accepts_operation_zero_written_with_a_fraction() {
  let plan = register(&case_with_spelled_fault(
    "0.0",
    r#"{"mode": "until-operation-finishes"}"#,
  ));

  assert_eq!(plan.faults()[0].operation, 0);
}

#[test]
fn test_register_rejects_operation_with_a_non_zero_fraction() {
  for operation in ["1.5", "0.5", "1e-1"] {
    let error = register_error(&case_with_spelled_fault(
      operation,
      r#"{"mode": "until-operation-finishes"}"#,
    ));

    assert_eq!(error.index, 0, "operation: {operation}");
    assert!(error.message.contains("operation"), "{}", error.message);
  }
}

#[test]
fn test_register_rejects_count_with_a_non_zero_fraction_or_below_one() {
  for count in ["1.5", "2.5", "1e-1", "0.0", "0e3", "-1.0"] {
    let error = register_error(&case_with_spelled_fault(
      "1",
      &format!(r#"{{"mode": "count", "count": {count}}}"#),
    ));

    assert_eq!(error.index, 0, "count: {count}");
    assert!(error.message.contains("repeat.count"), "{}", error.message);
  }
}

#[test]
fn test_register_still_rejects_operation_beyond_the_number_of_steps_when_written_with_a_fraction() {
  let error = register_error(&case_with_spelled_fault(
    "2.0",
    r#"{"mode": "until-operation-finishes"}"#,
  ));

  assert_eq!(error.index, 0);
}
