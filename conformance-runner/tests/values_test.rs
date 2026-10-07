use event_store_adapter_conformance_rs::data::load;
use event_store_adapter_conformance_rs::report::{CaseOutcome, CaseReport};
use event_store_adapter_conformance_rs::runner::run_case;
use event_store_adapter_conformance_rs::target_memory;
use std::path::Path;

fn check(operation: &str, errors: bool, expected_count: usize) {
  let data = load(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance")).unwrap();
  let cases: Vec<_> = data
    .cases
    .iter()
    .filter(|case| {
      case.body["operation"] == operation
        && case.body.pointer("/representation/signed_seq_nr") != Some(&serde_json::json!(true))
        && case.body.pointer("/representation/time_precision") != Some(&serde_json::json!("milliseconds"))
        && case.body.pointer("/expect/error").is_some() == errors
    })
    .collect();
  assert_eq!(cases.len(), expected_count);
  for case in cases {
    let outcome = run_case(case, &target_memory::TARGET, &data.coverage, target_memory::run_case);
    assert!(
      matches!(outcome, CaseOutcome::Passed { .. }),
      "{}: {outcome:?}",
      case.id
    );
  }
}
#[test]
fn should_seq_validate_supported_values_through_public_operations() {
  check("validateSeqNr", false, 3);
}
#[test]
fn should_seq_reject_invalid_values_through_public_operations() {
  check("validateSeqNr", true, 2);
}
#[test]
fn should_time_round_trip_valid_nanoseconds() {
  check("validateOccurredAt", false, 5);
}

#[test]
fn should_time_report_converted_and_read_nanoseconds_for_successful_cases() {
  let data = load(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance")).unwrap();
  for (id, nanos) in [
    ("occurred-at-min", "-9223372036854775808"),
    ("occurred-at-max", "9223372036854775807"),
    ("occurred-at-nanoseconds", "123456789"),
    ("occurred-at-before-epoch", "-1"),
    ("occurred-at-epoch", "0"),
  ] {
    let case = data.cases.iter().find(|case| case.id == id).unwrap();
    let outcome = run_case(case, &target_memory::TARGET, &data.coverage, target_memory::run_case);
    let report = serde_json::to_value(CaseReport {
      id: case.id.clone(),
      rules: case.rules.clone(),
      outcome,
    })
    .unwrap();

    assert_eq!(report["status"], "passed", "{id}: {report}");
    assert_eq!(report["expected"]["value"], nanos, "{id}: {report}");
    assert_eq!(report["actual"]["value"], nanos, "{id}: {report}");
  }
}
#[test]
fn should_time_reject_outside_signed_nanosecond_range() {
  check("validateOccurredAt", true, 2);
}
