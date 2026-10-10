use std::path::Path;

use event_store_adapter_conformance_rs::{
  data,
  report::CaseOutcome,
  runner,
  target_dynamodb::{Execution, TARGET},
};
use serde_json::json;

#[test]
fn should_reuse_the_accepted_executor_for_the_changed_item_generation() {
  let data = data::load(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance")).unwrap();
  assert!(data.manifest.passed());
  let case = data
    .cases
    .iter()
    .find(|case| case.id == "dynamodb-written-item-shapes")
    .unwrap();
  let execution = Execution::start().unwrap();
  let result = runner::run_case(case, &TARGET, &data.coverage, |case, prepared| {
    execution.run_case(case, prepared)
  });
  if let Some(dir) = std::env::var_os("MIGRATION_EVIDENCE_DIR") {
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(Path::new(&dir).join("migration-conformance.json"),serde_json::to_vec_pretty(&json!({"case":case.id,"environment":execution.environment(),"result":result,"observations":execution.observations()})).unwrap()).unwrap();
  }
  assert!(matches!(result, CaseOutcome::Passed { .. }), "{result:?}");
}
