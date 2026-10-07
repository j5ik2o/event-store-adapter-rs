use event_store_adapter_conformance_rs::{data::load, report::CaseOutcome, runner::prepare, target_memory};
use std::path::Path;

#[test]
fn should_not_pass_notification_observation_when_global_subscriber_cannot_be_registered() {
  tracing::subscriber::set_global_default(tracing_subscriber::registry()).unwrap();
  let data = load(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance")).unwrap();
  let case = data
    .cases
    .iter()
    .find(|case| case.id == "core-retention-failure-after-commit")
    .unwrap();
  for _ in 0..2 {
    let outcome = target_memory::run_case(case, prepare(case).unwrap());
    assert!(matches!(outcome, CaseOutcome::Unverified { .. }), "{outcome:?}");
  }
}
