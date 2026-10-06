//! ケースの分類（対象外・未検証・失敗）と、保存先に依存しない準備の試験。
//! 保存先にはまだつながないので、成功（`passed`）は 1 件も出ない。

use std::path::{Path, PathBuf};

use event_store_adapter_conformance_rs::data::{load, Case, CaseKind, Coverage, CoverageExclusion, DataSet};
use event_store_adapter_conformance_rs::report::{
  CaseOutcome, CaseReport, NotApplicableReason, RepresentationGap, UnverifiedReason,
};
use event_store_adapter_conformance_rs::runner::{run, run_case, Target};
use event_store_adapter_conformance_rs::{target_dynamodb, target_memory};
use serde_json::{json, Value};

fn conformance_dir() -> PathBuf {
  Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance")
}

fn real_data() -> DataSet {
  load(&conformance_dir()).expect("実データを読める")
}

fn outcome_of<'a>(reports: &'a [CaseReport], id: &str) -> &'a CaseOutcome {
  &reports
    .iter()
    .find(|report| report.id == id)
    .unwrap_or_else(|| panic!("{id} の報告がある"))
    .outcome
}

fn counts(reports: &[CaseReport]) -> (usize, usize, usize, usize) {
  let count = |wanted: fn(&CaseOutcome) -> bool| reports.iter().filter(|report| wanted(&report.outcome)).count();
  (
    count(|outcome| matches!(outcome, CaseOutcome::Passed)),
    count(|outcome| matches!(outcome, CaseOutcome::Failed { .. })),
    count(|outcome| matches!(outcome, CaseOutcome::NotApplicable { .. })),
    count(|outcome| matches!(outcome, CaseOutcome::Unverified { .. })),
  )
}

fn is_backend_not_targeted(outcome: &CaseOutcome) -> bool {
  matches!(
    outcome,
    CaseOutcome::NotApplicable {
      reason: NotApplicableReason::BackendNotTargeted { .. }
    }
  )
}

fn is_not_executed(outcome: &CaseOutcome) -> bool {
  matches!(
    outcome,
    CaseOutcome::Unverified {
      reason: UnverifiedReason::NotExecuted { .. }
    }
  )
}

fn is_representation(outcome: &CaseOutcome, expected: RepresentationGap) -> bool {
  matches!(
    outcome,
    CaseOutcome::NotApplicable {
      reason: NotApplicableReason::Representation { representation, .. }
    } if *representation == expected
  )
}

fn is_fnv1a64_decision(outcome: &CaseOutcome) -> bool {
  matches!(
    outcome,
    CaseOutcome::NotApplicable {
      reason: NotApplicableReason::Fnv1a64Decision { .. }
    }
  )
}

fn is_coverage_exclusion(outcome: &CaseOutcome) -> bool {
  matches!(
    outcome,
    CaseOutcome::NotApplicable {
      reason: NotApplicableReason::CoverageExclusion { .. }
    }
  )
}

// ---------------------------------------------------------------------------
// 合成したケースの分類
// ---------------------------------------------------------------------------

fn no_exclusions() -> Coverage {
  Coverage {
    required_rules: vec![],
    exclusions: vec![],
  }
}

fn scenario(rules: &[&str], body: Value) -> Case {
  Case {
    id: "synthetic".to_string(),
    rules: rules.iter().map(|rule| rule.to_string()).collect(),
    kind: CaseKind::Scenario,
    file: "synthetic.json".to_string(),
    body,
  }
}

/// 手順を 1 つだけ持つ、検査の要求のない場面の本体を作る。
fn scenario_body(backends: &[&str]) -> Value {
  json!({
    "id": "synthetic",
    "rules": ["T-1"],
    "backends": backends,
    "store": {"retention_count": null, "retention_mode": "delete"},
    "fixtures": {"events": {}, "snapshots": {}},
    "steps": [{"op": "getLatestSnapshotById", "arguments": {}, "expect": {"result": "none"}}]
  })
}

#[test]
fn should_run_case_marks_scenario_not_targeting_the_backend_as_not_applicable() {
  let case = scenario(&["T-1"], scenario_body(&["dynamodb"]));

  let outcome = run_case(&case, &target_memory::TARGET, &no_exclusions());

  assert!(is_backend_not_targeted(&outcome), "{outcome:?}");
}

#[test]
fn should_run_case_leaves_scenario_targeting_the_backend_unverified_when_nothing_ran() {
  let case = scenario(&["T-1"], scenario_body(&["memory", "dynamodb"]));

  let on_memory = run_case(&case, &target_memory::TARGET, &no_exclusions());
  let on_dynamodb = run_case(&case, &target_dynamodb::TARGET, &no_exclusions());

  assert!(is_not_executed(&on_memory), "{on_memory:?}");
  assert!(is_not_executed(&on_dynamodb), "{on_dynamodb:?}");
}

#[test]
fn should_run_case_marks_case_requiring_ttl_as_not_applicable_on_memory() {
  let mut body = scenario_body(&["memory", "dynamodb"]);
  body["requires"] = json!(["ttl"]);
  let case = scenario(&["T-1"], body);

  let outcome = run_case(&case, &target_memory::TARGET, &no_exclusions());

  assert!(is_backend_not_targeted(&outcome), "{outcome:?}");
}

#[test]
fn should_run_case_leaves_case_requiring_ttl_unverified_on_dynamodb() {
  let mut body = scenario_body(&["memory", "dynamodb"]);
  body["requires"] = json!(["ttl"]);
  let case = scenario(&["T-1"], body);

  let outcome = run_case(&case, &target_dynamodb::TARGET, &no_exclusions());

  assert!(is_not_executed(&outcome), "{outcome:?}");
}

fn excluding_w5() -> Coverage {
  Coverage {
    required_rules: vec!["W-5".to_string(), "T-1".to_string()],
    exclusions: vec![CoverageExclusion {
      rule: "W-5".to_string(),
      status: "deleted".to_string(),
      reason: "version の廃止で削除済み".to_string(),
    }],
  }
}

#[test]
fn should_run_case_marks_case_whose_rules_are_all_excluded_as_coverage_exclusion() {
  let case = scenario(&["W-5"], scenario_body(&["memory"]));

  let outcome = run_case(&case, &target_memory::TARGET, &excluding_w5());

  assert!(is_coverage_exclusion(&outcome), "{outcome:?}");
}

#[test]
fn should_run_case_keeps_case_with_a_non_excluded_rule_out_of_coverage_exclusion() {
  let case = scenario(&["W-5", "T-1"], scenario_body(&["memory"]));

  let outcome = run_case(&case, &target_memory::TARGET, &excluding_w5());

  assert!(is_not_executed(&outcome), "{outcome:?}");
}

#[test]
fn should_run_case_fails_case_whose_generator_cannot_be_expanded() {
  let mut body = scenario_body(&["memory"]);
  body["fixtures"] = json!({"events": {"e1": {"payload": "already set"}}, "snapshots": {}});
  body["generators"] = json!([{"target": "/fixtures/events/e1/payload", "character": "x", "byte_length": 3}]);
  let case = scenario(&["T-1"], body);

  let outcome = run_case(&case, &target_memory::TARGET, &no_exclusions());

  assert!(
    matches!(&outcome, CaseOutcome::Failed { detail, .. } if !detail.is_empty()),
    "{outcome:?}"
  );
}

#[test]
fn should_run_case_fails_case_whose_fault_declaration_cannot_be_registered() {
  let mut body = scenario_body(&["memory"]);
  body["faults"] = json!([{
    "operation": 1,
    "phase": "commit",
    "kind": "storage-error",
    "injection": "replace-request",
    "repeat": {"mode": "count", "count": 0},
    "details": {"message": "INJECTED"}
  }]);
  let case = scenario(&["T-1"], body);

  let outcome = run_case(&case, &target_memory::TARGET, &no_exclusions());

  assert!(
    matches!(&outcome, CaseOutcome::Failed { detail, .. } if !detail.is_empty()),
    "{outcome:?}"
  );
}

#[test]
fn should_run_case_reports_unimplemented_constraint_words_as_unverified() {
  let mut body = scenario_body(&["dynamodb"]);
  body["steps"][0]["observe"] = json!({"requests": [{
    "api": "BatchGetItem",
    "phase": "read-snapshot",
    "constraints": {"keys": ["journal:__config__:0"], "consistent_read_all_tables": true}
  }]});
  let case = scenario(&["DY-8"], body);

  let outcome = run_case(&case, &target_dynamodb::TARGET, &no_exclusions());

  assert_eq!(
    outcome,
    CaseOutcome::Unverified {
      reason: UnverifiedReason::UnimplementedConstraintWords {
        words: vec!["consistent_read_all_tables".to_string(), "keys".to_string()]
      }
    }
  );
}

#[test]
fn should_run_case_does_not_count_same_names_outside_request_constraints_as_words() {
  let mut body = scenario_body(&["dynamodb"]);
  body["store"]["layout_version"] = json!(1);
  body["steps"][0]["observe"] = json!({"items": [{"table": "journal", "attributes": {"aid": "S"}}]});
  body["faults"] = json!([{
    "operation": 1,
    "phase": "read-snapshot",
    "kind": "sdk-response",
    "injection": "replace-response",
    "repeat": {"mode": "count", "count": 1},
    "details": {"unprocessed_keys": ["journal:__config__:0"]}
  }]);
  let case = scenario(&["DY-8"], body);

  let outcome = run_case(&case, &target_dynamodb::TARGET, &no_exclusions());

  assert!(is_not_executed(&outcome), "{outcome:?}");
}

// ---------------------------------------------------------------------------
// 保存先を知らない分類（渡された値だけで決まる）
// ---------------------------------------------------------------------------

fn layout_case() -> Case {
  Case {
    id: "synthetic-layout".to_string(),
    rules: vec!["DY-1".to_string()],
    kind: CaseKind::Layout,
    file: "dynamodb/layout.json".to_string(),
    body: json!({"id": "synthetic-layout", "rules": ["DY-1"]}),
  }
}

fn requiring_ttl(backends: &[&str]) -> Case {
  let mut body = scenario_body(backends);
  body["requires"] = json!(["ttl"]);
  scenario(&["T-1"], body)
}

#[test]
fn should_target_constants_carry_the_capabilities_and_layout_of_each_backend() {
  assert_eq!(
    target_memory::TARGET,
    Target {
      name: "memory",
      capabilities: &[],
      has_layout: false
    }
  );
  assert_eq!(
    target_dynamodb::TARGET,
    Target {
      name: "dynamodb",
      capabilities: &["ttl"],
      has_layout: true
    }
  );
}

#[test]
fn should_run_case_decides_capability_from_the_given_target_not_from_its_name() {
  let case = requiring_ttl(&["memory", "dynamodb"]);
  let memory_with_ttl = Target {
    name: "memory",
    capabilities: &["ttl"],
    has_layout: false,
  };
  let dynamodb_without_ttl = Target {
    name: "dynamodb",
    capabilities: &[],
    has_layout: true,
  };

  let with_ttl = run_case(&case, &memory_with_ttl, &no_exclusions());
  let without_ttl = run_case(&case, &dynamodb_without_ttl, &no_exclusions());

  assert!(
    is_not_executed(&with_ttl),
    "名前が memory でも、ttl を提供するなら対象: {with_ttl:?}"
  );
  assert!(
    is_backend_not_targeted(&without_ttl),
    "名前が dynamodb でも、ttl を提供しないなら対象外: {without_ttl:?}"
  );
}

#[test]
fn should_run_case_requires_every_capability_word_to_be_provided() {
  let mut body = scenario_body(&["x"]);
  body["requires"] = json!(["ttl", "other"]);
  let case = scenario(&["T-1"], body);
  let only_ttl = Target {
    name: "x",
    capabilities: &["ttl"],
    has_layout: false,
  };
  let both = Target {
    name: "x",
    capabilities: &["other", "ttl"],
    has_layout: false,
  };

  assert!(is_backend_not_targeted(&run_case(&case, &only_ttl, &no_exclusions())));
  assert!(is_not_executed(&run_case(&case, &both, &no_exclusions())));
}

#[test]
fn should_run_case_decides_layout_from_has_layout_not_from_the_name() {
  let case = layout_case();
  let memory_with_layout = Target {
    name: "memory",
    capabilities: &[],
    has_layout: true,
  };
  let dynamodb_without_layout = Target {
    name: "dynamodb",
    capabilities: &["ttl"],
    has_layout: false,
  };

  let with_layout = run_case(&case, &memory_with_layout, &no_exclusions());
  let without_layout = run_case(&case, &dynamodb_without_layout, &no_exclusions());

  assert!(is_not_executed(&with_layout), "{with_layout:?}");
  assert!(is_backend_not_targeted(&without_layout), "{without_layout:?}");
}

#[test]
fn should_run_case_classifies_a_target_the_runner_has_never_heard_of() {
  let unknown = Target {
    name: "sqlite",
    capabilities: &[],
    has_layout: false,
  };

  let listed = run_case(
    &scenario(&["T-1"], scenario_body(&["sqlite"])),
    &unknown,
    &no_exclusions(),
  );
  let not_listed = run_case(
    &scenario(&["T-1"], scenario_body(&["memory"])),
    &unknown,
    &no_exclusions(),
  );

  assert!(is_not_executed(&listed), "{listed:?}");
  assert!(is_backend_not_targeted(&not_listed), "{not_listed:?}");
}

#[test]
fn should_run_case_names_the_given_target_in_the_not_targeted_detail() {
  let outcome = run_case(
    &scenario(&["T-1"], scenario_body(&["dynamodb"])),
    &target_memory::TARGET,
    &no_exclusions(),
  );

  assert!(
    matches!(
      &outcome,
      CaseOutcome::NotApplicable { reason: NotApplicableReason::BackendNotTargeted { detail } } if detail.contains("memory")
    ),
    "{outcome:?}"
  );
}

// ---------------------------------------------------------------------------
// 実データの分類
// ---------------------------------------------------------------------------

const TARGETS: [&Target; 2] = [&target_memory::TARGET, &target_dynamodb::TARGET];

#[test]
fn should_run_marks_fnv1a64_cases_as_fnv1a64_decision_on_every_backend() {
  let data = real_data();
  let ids: Vec<&str> = data
    .cases
    .iter()
    .filter(|case| case.body.get("operation") == Some(&json!("fnv1a64")))
    .map(|case| case.id.as_str())
    .collect();
  assert_eq!(ids.len(), 4);

  for target in TARGETS {
    let reports = run(&data, target);
    for id in &ids {
      let outcome = outcome_of(&reports, id);
      assert!(is_fnv1a64_decision(outcome), "{} {id}: {outcome:?}", target.name);
    }
  }
}

#[test]
fn should_run_marks_signed_seq_nr_cases_as_unrepresentable_on_every_backend() {
  let data = real_data();
  let mut ids: Vec<&str> = data
    .cases
    .iter()
    .filter(|case| case.body.pointer("/representation/signed_seq_nr") == Some(&json!(true)))
    .map(|case| case.id.as_str())
    .collect();
  ids.sort_unstable();
  assert_eq!(ids, vec!["core-seq-negative", "seq-negative-value"]);

  for target in TARGETS {
    let reports = run(&data, target);
    for id in &ids {
      let outcome = outcome_of(&reports, id);
      assert!(
        is_representation(outcome, RepresentationGap::Unrepresentable),
        "{} {id}: {outcome:?}",
        target.name
      );
    }
  }
}

#[test]
fn should_run_does_not_mark_seq_case_without_signed_flag_as_unrepresentable() {
  let data = real_data();

  for target in TARGETS {
    let reports = run(&data, target);
    let outcome = outcome_of(&reports, "core-seq-above-max");
    assert!(is_not_executed(outcome), "{}: {outcome:?}", target.name);
  }
}

#[test]
fn should_run_marks_millisecond_precision_cases_as_time_precision_on_every_backend() {
  let data = real_data();
  let ids: Vec<&str> = data
    .cases
    .iter()
    .filter(|case| case.body.pointer("/representation/time_precision") == Some(&json!("milliseconds")))
    .map(|case| case.id.as_str())
    .collect();
  assert_eq!(ids.len(), 8);

  for target in TARGETS {
    let reports = run(&data, target);
    for id in &ids {
      let outcome = outcome_of(&reports, id);
      assert!(
        is_representation(outcome, RepresentationGap::TimePrecision),
        "{} {id}: {outcome:?}",
        target.name
      );
    }
  }
}

#[test]
fn should_run_marks_dynamodb_only_cases_and_layout_as_not_applicable_on_memory() {
  let data = real_data();
  let reports = run(&data, &target_memory::TARGET);
  let dynamodb_only: Vec<&str> = data
    .cases
    .iter()
    .filter(|case| case.body.get("backends") == Some(&json!(["dynamodb"])) || matches!(case.kind, CaseKind::Layout))
    .map(|case| case.id.as_str())
    .collect();
  assert_eq!(dynamodb_only.len(), 44, "DynamoDB だけが対象の場面 43 件と、配置 1 件");

  for id in dynamodb_only {
    let outcome = outcome_of(&reports, id);
    assert!(is_backend_not_targeted(outcome), "{id}: {outcome:?}");
  }
}

#[test]
fn should_run_does_not_mark_any_case_as_not_targeted_on_dynamodb() {
  let data = real_data();

  let reports = run(&data, &target_dynamodb::TARGET);

  let not_targeted: Vec<&str> = reports
    .iter()
    .filter(|report| is_backend_not_targeted(&report.outcome))
    .map(|report| report.id.as_str())
    .collect();
  assert!(not_targeted.is_empty(), "{not_targeted:?}");
}

#[test]
fn should_run_reports_no_success_for_memory_and_leaves_the_rest_unverified() {
  let data = real_data();

  let reports = run(&data, &target_memory::TARGET);

  // (passed, failed, not-applicable, unverified)
  assert_eq!(counts(&reports), (0, 0, 58, 58));
}

#[test]
fn should_run_reports_no_success_for_dynamodb_and_leaves_the_rest_unverified() {
  let data = real_data();

  let reports = run(&data, &target_dynamodb::TARGET);

  // (passed, failed, not-applicable, unverified)
  assert_eq!(counts(&reports), (0, 0, 14, 102));
}

#[test]
fn should_run_reports_configuration_case_per_backend() {
  let data = real_data();

  let on_memory = run(&data, &target_memory::TARGET);
  let on_dynamodb = run(&data, &target_dynamodb::TARGET);

  assert!(is_backend_not_targeted(outcome_of(&on_memory, "dynamodb-config-new")));
  assert!(
    matches!(
      outcome_of(&on_dynamodb, "dynamodb-config-new"),
      CaseOutcome::Unverified { reason: UnverifiedReason::UnimplementedConstraintWords { words } }
        if words.contains(&"keys".to_string())
    ),
    "{:?}",
    outcome_of(&on_dynamodb, "dynamodb-config-new")
  );
}

#[test]
fn should_run_reports_layout_case_per_backend() {
  let data = real_data();

  let on_memory = run(&data, &target_memory::TARGET);
  let on_dynamodb = run(&data, &target_dynamodb::TARGET);

  assert!(is_backend_not_targeted(outcome_of(&on_memory, "dynamodb-layout-v1")));
  assert!(is_not_executed(outcome_of(&on_dynamodb, "dynamodb-layout-v1")));
}

#[test]
fn should_run_keeps_the_id_and_all_rules_of_each_case() {
  let data = real_data();
  let layout = data
    .cases
    .iter()
    .find(|case| case.id == "dynamodb-layout-v1")
    .expect("配置のケースがある");

  let reports = run(&data, &target_dynamodb::TARGET);

  let report = reports
    .iter()
    .find(|report| report.id == "dynamodb-layout-v1")
    .expect("報告がある");
  assert_eq!(reports.len(), data.cases.len());
  assert_eq!(report.rules, layout.rules);
  assert_eq!(report.rules.len(), 12);
}
