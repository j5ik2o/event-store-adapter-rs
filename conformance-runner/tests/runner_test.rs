//! ケースの分類（対象外・未検証・失敗・成功）と、保存先に依存しない準備の試験。
//! Memoryは実操作、DynamoDBの分類試験は明示した未実行のtest doubleを使う。実Localの検証はCLI試験で行う。

use std::cell::{Cell, RefCell};
use std::path::{Path, PathBuf};

use event_store_adapter_conformance_rs::data::{load, Case, CaseKind, Coverage, CoverageExclusion, DataSet};
use event_store_adapter_conformance_rs::fault::Phase;
use event_store_adapter_conformance_rs::report::{
  CaseOutcome, CaseReport, NotApplicableReason, ObservedValues, RepresentationGap, UnverifiedReason,
};
use event_store_adapter_conformance_rs::runner::{run, run_case, CaseExecutor, PreparedCase, Target};
use event_store_adapter_conformance_rs::{target_bigtable, target_dynamodb, target_memory, target_sqlite};
use serde_json::{json, Value};

fn classification_executor(_: &Case, _: PreparedCase) -> CaseOutcome {
  CaseOutcome::Unverified {
    reason: UnverifiedReason::NotExecuted {
      detail: "分類だけを検査するtest doubleで、実操作を行わない".into(),
    },
  }
}

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
    count(|outcome| matches!(outcome, CaseOutcome::Passed { .. })),
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
    "steps": [{"op": "getLatestSnapshotById", "arguments": {"aggregate_id": {"type_name": "Account", "value": "1"}}, "expect": {"result": "none"}}]
  })
}

#[test]
fn should_run_delegates_prepared_cases_and_preserves_executor_outcomes() {
  let target = Target {
    name: "custom",
    capabilities: &[],
    has_layout: false,
    extra_store_settings: &[],
    extra_seed_items: &[],
    represents_change_feed: false,
  };
  let outcomes = [
    CaseOutcome::Passed {
      values: Some(ObservedValues {
        expected: json!({"value": "123456789"}),
        actual: json!({"value": "123456789"}),
      }),
    },
    CaseOutcome::Failed {
      failed_operation: Some(1),
      detail: "実行先の比較失敗".into(),
      expected: Some(json!({"value": "expected"})),
      actual: Some(json!({"value": "actual"})),
      unfired_faults: vec![],
    },
    CaseOutcome::Unverified {
      reason: UnverifiedReason::NotExecuted {
        detail: "実行先が未接続".into(),
      },
    },
    CaseOutcome::NotApplicable {
      reason: NotApplicableReason::Representation {
        representation: RepresentationGap::Unrepresentable,
        detail: "実行先の表現能力".into(),
      },
    },
  ];
  let mut data = real_data();
  data.coverage = no_exclusions();
  data.cases = (0..outcomes.len())
    .map(|index| {
      let mut body = scenario_body(&["custom"]);
      body["id"] = json!(format!("delegated-{index}"));
      body["fixtures"]["events"] = json!({"e1": {"payload": ""}});
      body["generators"] = json!([{"target": "/fixtures/events/e1/payload", "character": "あ", "byte_length": 6}]);
      body["faults"] = json!([{
        "operation": 1, "phase": "read-snapshot", "kind": "storage-error", "injection": "replace-request",
        "repeat": {"mode": "count", "count": 1}, "details": {"message": "delegated fault"}
      }]);
      let mut case = scenario(&["T-1"], body);
      case.id = format!("delegated-{index}");
      case
    })
    .collect();
  let visited = RefCell::new(Vec::new());

  let reports = run(&data, &target, |case, prepared| {
    let index = data.cases.iter().position(|input| input.id == case.id).unwrap();
    visited.borrow_mut().push(case.id.clone());
    assert_eq!(prepared.body["fixtures"]["events"]["e1"]["payload"], "ああ");
    let mut faults = prepared.faults.begin_operation(1);
    let fault = faults
      .start_application(Phase::ReadSnapshot)
      .expect("登録済み障害を渡す");
    assert_eq!(fault.details["message"], "delegated fault");
    assert!(faults.finish().is_ok());
    outcomes[index].clone()
  });

  assert_eq!(
    *visited.borrow(),
    data.cases.iter().map(|case| case.id.clone()).collect::<Vec<_>>()
  );
  for ((report, case), expected) in reports.iter().zip(&data.cases).zip(&outcomes) {
    assert_eq!(report.id, case.id);
    assert_eq!(report.rules, case.rules);
    assert_eq!(&report.outcome, expected);
    assert_eq!(
      case.body["fixtures"]["events"]["e1"]["payload"], "",
      "元の入力を変更しない"
    );
  }
}

#[test]
fn should_run_case_marks_scenario_not_targeting_the_backend_as_not_applicable() {
  let case = scenario(&["T-1"], scenario_body(&["dynamodb"]));

  let outcome = run_case(&case, &target_memory::TARGET, &no_exclusions(), target_memory::run_case);

  assert!(is_backend_not_targeted(&outcome), "{outcome:?}");
}

#[test]
fn should_run_case_executes_memory_and_leaves_dynamodb_unverified() {
  let case = scenario(&["T-1"], scenario_body(&["memory", "dynamodb"]));

  let on_memory = run_case(&case, &target_memory::TARGET, &no_exclusions(), target_memory::run_case);
  let on_dynamodb = run_case(
    &case,
    &target_dynamodb::TARGET,
    &no_exclusions(),
    classification_executor,
  );

  assert_eq!(on_memory, CaseOutcome::Passed { values: None });
  assert!(is_not_executed(&on_dynamodb), "{on_dynamodb:?}");
}

#[test]
fn should_run_case_uses_the_selected_executor_for_memory_metadata() {
  let case = scenario(&["T-1"], scenario_body(&["memory"]));
  let expected = CaseOutcome::Unverified {
    reason: UnverifiedReason::NotExecuted {
      detail: "入口が選んだ実行先".into(),
    },
  };
  let calls = Cell::new(0);

  let outcome = run_case(&case, &target_memory::TARGET, &no_exclusions(), |_, _| {
    calls.set(calls.get() + 1);
    expected.clone()
  });

  assert_eq!(calls.get(), 1);
  assert_eq!(outcome, expected);
}

#[test]
fn should_run_case_preserves_classification_order_without_calling_the_executor() {
  let mut body = scenario_body(&["memory"]);
  body["fixtures"]["events"] = json!({"e1": {"payload": "not empty"}});
  body["generators"] = json!([{"target": "/fixtures/events/e1/payload", "character": "x", "byte_length": 3}]);
  body["steps"][0]["observe"] =
    json!({"requests": [{"api": "BatchGetItem", "phase": "read-snapshot", "constraints": {"unknown_word": true}}]});
  body["representation"] = json!({"signed_seq_nr": true, "time_precision": "milliseconds"});

  let mut not_targeted = scenario(&["W-5"], body.clone());
  not_targeted.body["backends"] = json!(["dynamodb"]);
  let signed = scenario(&["W-5"], body.clone());
  body["representation"].as_object_mut().unwrap().remove("signed_seq_nr");
  let mut precision = scenario(&["W-5"], body.clone());
  precision.kind = CaseKind::ValueTable;
  precision.body["operation"] = json!("fnv1a64");
  let mut fnv = scenario(&["W-5"], body.clone());
  fnv.kind = CaseKind::ValueTable;
  fnv.body["operation"] = json!("fnv1a64");
  fnv.body.as_object_mut().unwrap().remove("representation");
  body.as_object_mut().unwrap().remove("representation");
  body["operation"] = json!("buildAid");
  body["input"] = json!({"aggregate_id": {"type_name": "Account", "value": "1"}});
  body["expect"] = json!({"value": "Account-1"});
  let mut excluded = scenario(&["W-5"], body.clone());
  excluded.kind = CaseKind::ValueTable;
  let mut invalid = scenario(&["T-1"], body.clone());
  invalid.kind = CaseKind::ValueTable;
  body["fixtures"]["events"]["e1"]["payload"] = json!("");
  let mut constraints = scenario(&["T-1"], body);
  constraints.kind = CaseKind::ValueTable;

  for (case, status, reason, representation) in [
    (not_targeted, "not-applicable", Some("backend-not-targeted"), None),
    (
      signed,
      "not-applicable",
      Some("representation"),
      Some("unrepresentable"),
    ),
    (
      precision,
      "not-applicable",
      Some("representation"),
      Some("time-precision"),
    ),
    (fnv, "not-applicable", Some("fnv1a64-decision"), None),
    (excluded, "not-applicable", Some("coverage-exclusion"), None),
    (invalid, "failed", None, None),
    (constraints, "unverified", Some("unimplemented-constraint-words"), None),
  ] {
    let outcome = run_case(&case, &target_memory::TARGET, &excluding_w5(), |_, _| {
      panic!("共通分類で止まるケースを委譲しない")
    });
    let report = serde_json::to_value(outcome).unwrap();
    assert_eq!(report["status"], status, "{report}");
    if let Some(reason) = reason {
      assert_eq!(report["reason"]["kind"], reason, "{report}");
    }
    if let Some(representation) = representation {
      assert_eq!(report["reason"]["representation"], representation, "{report}");
    }
  }
}

#[test]
fn should_run_build_aid_without_calling_the_target_executor() {
  let data = real_data();
  let cases: Vec<_> = data
    .cases
    .iter()
    .filter(|case| case.body["operation"] == "buildAid")
    .collect();
  assert_eq!(cases.len(), 9);

  for (target, _) in TARGETS {
    for case in &cases {
      let outcome = run_case(case, target, &data.coverage, |_, _| {
        panic!("buildAidは共通処理で実行する")
      });
      let CaseOutcome::Passed { values: Some(values) } = outcome else {
        panic!("{} {}: {outcome:?}", target.name, case.id);
      };
      assert_eq!(values.expected, case.body["expect"]);
      if case.body["expect"].get("error").is_some() {
        assert_eq!(values.actual["category"], "contract-violation");
        assert_eq!(values.actual["rule"], case.body["expect"]["error"]["rule"]);
      } else {
        assert_eq!(values.actual["value"], case.body["expect"]["value"]);
      }
    }
  }
}

#[test]
fn should_run_case_marks_case_requiring_ttl_as_not_applicable_on_memory() {
  let mut body = scenario_body(&["memory", "dynamodb"]);
  body["requires"] = json!(["ttl"]);
  let case = scenario(&["T-1"], body);

  let outcome = run_case(&case, &target_memory::TARGET, &no_exclusions(), target_memory::run_case);

  assert!(is_backend_not_targeted(&outcome), "{outcome:?}");
}

#[test]
fn should_run_case_leaves_case_requiring_ttl_unverified_on_dynamodb() {
  let mut body = scenario_body(&["memory", "dynamodb"]);
  body["requires"] = json!(["ttl"]);
  let case = scenario(&["T-1"], body);

  let outcome = run_case(
    &case,
    &target_dynamodb::TARGET,
    &no_exclusions(),
    classification_executor,
  );

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

  let outcome = run_case(&case, &target_memory::TARGET, &excluding_w5(), target_memory::run_case);

  assert!(is_coverage_exclusion(&outcome), "{outcome:?}");
}

#[test]
fn should_run_case_keeps_case_with_a_non_excluded_rule_out_of_coverage_exclusion() {
  let case = scenario(&["W-5", "T-1"], scenario_body(&["memory"]));

  let outcome = run_case(&case, &target_memory::TARGET, &excluding_w5(), target_memory::run_case);

  assert_eq!(outcome, CaseOutcome::Passed { values: None });
}

#[test]
fn should_run_case_fails_case_whose_generator_cannot_be_expanded() {
  let mut body = scenario_body(&["memory"]);
  body["fixtures"] = json!({"events": {"e1": {"payload": "already set"}}, "snapshots": {}});
  body["generators"] = json!([{"target": "/fixtures/events/e1/payload", "character": "x", "byte_length": 3}]);
  let case = scenario(&["T-1"], body);

  let outcome = run_case(&case, &target_memory::TARGET, &no_exclusions(), target_memory::run_case);

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

  let outcome = run_case(&case, &target_memory::TARGET, &no_exclusions(), target_memory::run_case);

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
    "constraints": {"unknown_word": true}
  }]});
  let case = scenario(&["DY-8"], body);

  let outcome = run_case(
    &case,
    &target_dynamodb::TARGET,
    &no_exclusions(),
    classification_executor,
  );

  assert_eq!(
    outcome,
    CaseOutcome::Unverified {
      reason: UnverifiedReason::UnimplementedConstraintWords {
        words: vec!["unknown_word".to_string()]
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

  let outcome = run_case(
    &case,
    &target_dynamodb::TARGET,
    &no_exclusions(),
    classification_executor,
  );

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
      has_layout: false,
      extra_store_settings: &[],
      extra_seed_items: &[],
      represents_change_feed: false,
    }
  );
  assert_eq!(
    target_dynamodb::TARGET,
    Target {
      name: "dynamodb",
      capabilities: &["ttl"],
      has_layout: true,
      extra_store_settings: &[],
      extra_seed_items: &[],
      represents_change_feed: false,
    }
  );
  assert_eq!(
    target_sqlite::TARGET,
    Target {
      name: "sqlite",
      capabilities: &[],
      has_layout: false,
      extra_store_settings: &["lock_wait_milliseconds"],
      extra_seed_items: &["configuration"],
      represents_change_feed: true,
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
    extra_store_settings: &[],
    extra_seed_items: &[],
    represents_change_feed: false,
  };
  let dynamodb_without_ttl = Target {
    name: "dynamodb",
    capabilities: &[],
    has_layout: true,
    extra_store_settings: &[],
    extra_seed_items: &[],
    represents_change_feed: false,
  };

  let with_ttl = run_case(&case, &memory_with_ttl, &no_exclusions(), classification_executor);
  let without_ttl = run_case(&case, &dynamodb_without_ttl, &no_exclusions(), classification_executor);

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
    extra_store_settings: &[],
    extra_seed_items: &[],
    represents_change_feed: false,
  };
  let both = Target {
    name: "x",
    capabilities: &["other", "ttl"],
    has_layout: false,
    extra_store_settings: &[],
    extra_seed_items: &[],
    represents_change_feed: false,
  };

  assert!(is_backend_not_targeted(&run_case(
    &case,
    &only_ttl,
    &no_exclusions(),
    classification_executor
  )));
  assert!(is_not_executed(&run_case(
    &case,
    &both,
    &no_exclusions(),
    classification_executor
  )));
}

#[test]
fn should_run_case_decides_layout_from_has_layout_not_from_the_name() {
  let case = layout_case();
  let memory_with_layout = Target {
    name: "memory",
    capabilities: &[],
    has_layout: true,
    extra_store_settings: &[],
    extra_seed_items: &[],
    represents_change_feed: false,
  };
  let dynamodb_without_layout = Target {
    name: "dynamodb",
    capabilities: &["ttl"],
    has_layout: false,
    extra_store_settings: &[],
    extra_seed_items: &[],
    represents_change_feed: false,
  };

  let with_layout = run_case(&case, &memory_with_layout, &no_exclusions(), classification_executor);
  let without_layout = run_case(
    &case,
    &dynamodb_without_layout,
    &no_exclusions(),
    classification_executor,
  );

  assert!(is_not_executed(&with_layout), "{with_layout:?}");
  assert!(is_backend_not_targeted(&without_layout), "{without_layout:?}");
}

#[test]
fn should_run_case_classifies_a_target_the_runner_has_never_heard_of() {
  let unknown = Target {
    name: "custom",
    capabilities: &[],
    has_layout: false,
    extra_store_settings: &[],
    extra_seed_items: &[],
    represents_change_feed: false,
  };

  let listed = run_case(
    &scenario(&["T-1"], scenario_body(&["custom"])),
    &unknown,
    &no_exclusions(),
    classification_executor,
  );
  let not_listed = run_case(
    &scenario(&["T-1"], scenario_body(&["memory"])),
    &unknown,
    &no_exclusions(),
    classification_executor,
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
    target_memory::run_case,
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

const TARGETS: [(&Target, CaseExecutor); 2] = [
  (&target_memory::TARGET, target_memory::run_case),
  (&target_dynamodb::TARGET, classification_executor),
];

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

  for (target, execute) in TARGETS {
    let reports = run(&data, target, execute);
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

  for (target, execute) in TARGETS {
    let reports = run(&data, target, execute);
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

  for (target, execute) in TARGETS {
    let reports = run(&data, target, execute);
    let outcome = outcome_of(&reports, "core-seq-above-max");
    if target.name == "memory" {
      assert_eq!(outcome, &CaseOutcome::Passed { values: None });
    } else {
      assert!(is_not_executed(outcome), "{}: {outcome:?}", target.name);
    }
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

  for (target, execute) in TARGETS {
    let reports = run(&data, target, execute);
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
  let reports = run(&data, &target_memory::TARGET, target_memory::run_case);
  let dynamodb_only: Vec<&str> = data
    .cases
    .iter()
    .filter(|case| case.body.get("backends") == Some(&json!(["dynamodb"])) || matches!(case.kind, CaseKind::Layout))
    .map(|case| case.id.as_str())
    .collect();
  assert_eq!(dynamodb_only.len(), 46, "DynamoDB だけが対象の場面 45 件と、配置 1 件");

  for id in dynamodb_only {
    let outcome = outcome_of(&reports, id);
    assert!(is_backend_not_targeted(outcome), "{id}: {outcome:?}");
  }
}

#[test]
fn should_run_marks_only_memory_only_sqlite_only_and_bigtable_cases_as_not_targeted_on_dynamodb() {
  let data = real_data();

  let reports = run(&data, &target_dynamodb::TARGET, classification_executor);

  let mut not_targeted: Vec<&str> = reports
    .iter()
    .filter(|report| is_backend_not_targeted(&report.outcome))
    .map(|report| report.id.as_str())
    .collect();
  not_targeted.sort_unstable();
  // SQLite・Bigtable を提供するので、SQLite だけが対象の場面 8 件、Bigtable の場面 12 件と封筒の値の表 10 件も
  // 「提供しない」でなく「今回の保存先でない」になる。
  let mut expected = vec![
    "bigtable-cas-response-lost",
    "bigtable-change-feed-unsupported",
    "bigtable-envelope-event-bytes",
    "bigtable-envelope-event-empty",
    "bigtable-envelope-event-length-mismatch",
    "bigtable-envelope-event-occurred-at-max",
    "bigtable-envelope-event-occurred-at-min",
    "bigtable-envelope-event-trailing-bytes",
    "bigtable-envelope-event-unknown-version",
    "bigtable-envelope-event-utf8",
    "bigtable-envelope-limit-above-maximum",
    "bigtable-envelope-limit-below-minimum",
    "bigtable-envelope-over-limit",
    "bigtable-envelope-snapshot-empty",
    "bigtable-envelope-snapshot-utf8",
    "bigtable-new-tail-write-failure",
    "bigtable-previous-event-write-failure",
    "bigtable-seeded-configuration-match",
    "bigtable-seeded-envelope-limit-mismatch",
    "bigtable-seeded-keep-count-mismatch",
    "bigtable-seeded-table-envelope-limit-mismatch",
    "bigtable-ttl-unsupported",
    "memory-change-feed-unsupported",
    "memory-payload-read-mutation",
    "memory-payload-written-mutation",
    "memory-store-new-independent",
    "memory-store-shared",
    "memory-store-shared-configuration",
    "memory-ttl-unsupported",
    "sqlite-change-feed-unsupported",
    "sqlite-seeded-change-feed-enabled",
    "sqlite-seeded-configuration-match",
    "sqlite-seeded-keep-count-mismatch",
    "sqlite-seeded-layout-version-mismatch",
    "sqlite-ttl-unsupported",
    "sqlite-write-lock-busy",
    "store-shared-persistent",
  ];
  expected.sort_unstable();
  assert_eq!(not_targeted, expected);
}

#[test]
fn should_run_reports_memory_writes_reads_retention_and_notifications() {
  let data = real_data();

  let reports = run(&data, &target_memory::TARGET, target_memory::run_case);

  // (passed, failed, not-applicable, unverified)。保持・通知の4件と、期限切れ方式の設定エラーに加え、
  // 2 つ目のストアの 3 件とペイロードの隔離の 2 件も実操作で成功する。対象外 105 のうち 14 は、
  // この言語が提供しない保存先（Spanner）の場面で、22 は Bigtable の場面 12 件と封筒の値の表 10 件（今回の保存先でない）。
  assert_eq!(counts(&reports), (64, 0, 105, 0));
}

#[test]
fn should_run_classification_preserves_explicit_not_executed_dynamodb_results() {
  let data = real_data();

  let reports = run(&data, &target_dynamodb::TARGET, classification_executor);

  // (passed, failed, not-applicable, unverified)。passed 9 は値の表の buildAid。対象外 65 のうち 7 は、
  // メモリだけが対象の場面、8 は SQLite だけが対象の場面、14 は、この言語が提供しない保存先（Spanner）の場面、
  // 22 は Bigtable の場面と封筒の値の表。
  assert_eq!(counts(&reports), (9, 0, 65, 95));
}

#[test]
fn should_run_classifies_the_sqlite_leg_without_running_a_store() {
  let data = real_data();

  let reports = run(&data, &target_sqlite::TARGET, classification_executor);

  // (passed, failed, not-applicable, unverified)。passed 9 は値の表の buildAid。未検証 57 は、SQLite が対象で
  // 保存先の実行に回る場面 45 と値の表 12 で、SQLite だけの場面の `store`・`seed` の要素を未知の要素にしない。
  // 対象外 103 のうち 14 は、この言語が提供しない保存先（Spanner）の場面、22 は Bigtable の場面と封筒の値の表。
  assert_eq!(counts(&reports), (9, 0, 103, 57));
}

#[test]
fn should_run_classifies_the_bigtable_leg_without_running_a_store() {
  let data = real_data();

  let reports = run(&data, &target_bigtable::TARGET, classification_executor);

  // (passed, failed, not-applicable, unverified)。passed 9 は値の表の buildAid。未検証 72 は、Bigtable が対象で
  // 保存先の実行に回る場面と値の表。対象外 88 のうち、この言語が提供しない保存先（Spanner）の場面 14 件は「提供しない」、
  // ほかの保存先だけが対象の場面と配置の 60 件は「今回の保存先でない」。
  assert_eq!(counts(&reports), (9, 0, 88, 72));
  let not_provided: Vec<&str> = reports
    .iter()
    .filter(|report| is_backend_not_provided(&report.outcome))
    .map(|report| report.id.as_str())
    .collect();
  assert_eq!(not_provided.len(), 14, "{not_provided:?}");
  assert!(
    not_provided.iter().all(|id| id.starts_with("spanner-")),
    "提供しないのは Spanner の場面だけ: {not_provided:?}"
  );
}

#[test]
fn should_run_hands_every_bigtable_case_to_the_executor_on_the_bigtable_leg_instead_of_marking_it_not_provided() {
  let data = real_data();
  let cases = bigtable_only_cases(&data);
  assert_eq!(cases.len(), 22, "Bigtable だけの場面 12 件と、封筒の値の表 10 件");

  for case in cases {
    let outcome = run_case(case, &target_bigtable::TARGET, &data.coverage, classification_executor);

    assert!(
      is_not_executed(&outcome),
      "提供しない・今回の保存先でないと分類せず、保存先実行関数へ委譲する: {}: {outcome:?}",
      case.id
    );
  }
}

#[test]
fn should_run_case_accepts_the_bigtable_store_setting_and_seed_item_but_still_fails_other_backends_elements() {
  let mut known = scenario_body(&["bigtable"]);
  known["store"] = json!({"retention_count": null, "retention_mode": "delete", "envelope_limit_bytes": 1024});
  known["seed"] = json!({"configuration": {"journal": {}, "head": {}}});
  let mut unknown = scenario_body(&["bigtable"]);
  unknown["store"] = json!({"retention_count": null, "retention_mode": "delete", "lock_wait_milliseconds": 100});

  let accepted = run_case(
    &scenario(&["T-1"], known),
    &target_bigtable::TARGET,
    &no_exclusions(),
    classification_executor,
  );
  let rejected = run_case(
    &scenario(&["T-1"], unknown),
    &target_bigtable::TARGET,
    &no_exclusions(),
    classification_executor,
  );

  assert!(
    is_not_executed(&accepted),
    "Bigtable の要素は知らない要素にしない: {accepted:?}"
  );
  assert!(
    matches!(&rejected, CaseOutcome::Failed { detail, .. } if detail.contains("store.lock_wait_milliseconds")),
    "SQLite だけの要素は Bigtable の leg でも知らない要素になる: {rejected:?}"
  );
}

#[test]
fn should_run_reports_new_memory_configuration_cases_on_memory() {
  let data = real_data();

  let reports = run(&data, &target_memory::TARGET, target_memory::run_case);

  assert_eq!(
    outcome_of(&reports, "memory-ttl-unsupported"),
    &CaseOutcome::Passed { values: None }
  );
  let change_feed = outcome_of(&reports, "memory-change-feed-unsupported");
  assert!(
    matches!(change_feed, CaseOutcome::NotApplicable {
      reason: NotApplicableReason::Representation { representation: RepresentationGap::Unrepresentable, detail }
    } if !detail.is_empty()),
    "{change_feed:?}"
  );
}

#[test]
fn should_run_leaves_new_dynamodb_request_scope_cases_not_executed_on_dynamodb_and_not_targeted_on_memory() {
  let data = real_data();
  let on_memory = run(&data, &target_memory::TARGET, target_memory::run_case);
  let on_dynamodb = run(&data, &target_dynamodb::TARGET, classification_executor);

  for id in ["dynamodb-single-aggregate-delete", "dynamodb-single-aggregate-ttl"] {
    assert!(is_backend_not_targeted(outcome_of(&on_memory, id)), "memory {id}");
    assert!(is_not_executed(outcome_of(&on_dynamodb, id)), "dynamodb {id}");
  }
}

fn change_feed_case(backends: &[&str], change_feed: Option<bool>) -> Case {
  let mut body = scenario_body(backends);
  if let Some(change_feed) = change_feed {
    body["store"]["change_feed"] = json!(change_feed);
  }
  scenario(&["T-1"], body)
}

#[test]
fn should_run_case_marks_a_case_requesting_the_change_feed_as_unrepresentable_on_every_backend() {
  let case = change_feed_case(&["memory", "dynamodb"], Some(true));

  for (target, _) in TARGETS {
    let outcome = run_case(&case, target, &no_exclusions(), |_, _| {
      panic!("表現不能のケースを保存先実行関数へ委譲しない")
    });

    assert!(
      matches!(&outcome, CaseOutcome::NotApplicable {
        reason: NotApplicableReason::Representation { representation: RepresentationGap::Unrepresentable, detail }
      } if !detail.is_empty()),
      "{}: {outcome:?}",
      target.name
    );
  }
}

#[test]
fn should_run_case_executes_a_case_with_change_feed_false_or_omitted() {
  for change_feed in [Some(false), None] {
    let case = change_feed_case(&["memory", "dynamodb"], change_feed);

    for (target, _) in TARGETS {
      let calls = Cell::new(0);

      let outcome = run_case(&case, target, &no_exclusions(), |_, _| {
        calls.set(calls.get() + 1);
        CaseOutcome::Passed { values: None }
      });

      assert_eq!(calls.get(), 1, "{} {change_feed:?}", target.name);
      assert_eq!(
        outcome,
        CaseOutcome::Passed { values: None },
        "{} {change_feed:?}",
        target.name
      );
    }
  }
}

#[test]
fn should_run_case_executes_a_case_requesting_the_change_feed_on_a_target_that_represents_it() {
  let case = change_feed_case(&["sqlite"], Some(true));
  let calls = Cell::new(0);

  let outcome = run_case(&case, &target_sqlite::TARGET, &no_exclusions(), |_, _| {
    calls.set(calls.get() + 1);
    CaseOutcome::Passed { values: None }
  });

  assert_eq!(calls.get(), 1, "変更フィードを求める設定を表せる保存先へは委譲する");
  assert_eq!(outcome, CaseOutcome::Passed { values: None });
}

#[test]
fn should_run_case_executes_the_sqlite_change_feed_scene_while_the_memory_one_stays_unrepresentable() {
  let data = real_data();
  let sqlite = case_of_real_data(&data, "sqlite-change-feed-unsupported");
  assert_eq!(sqlite.body["store"]["change_feed"], json!(true));
  let memory = case_of_real_data(&data, "memory-change-feed-unsupported");

  let on_sqlite = run_case(sqlite, &target_sqlite::TARGET, &data.coverage, classification_executor);
  let on_memory = run_case(memory, &target_memory::TARGET, &data.coverage, target_memory::run_case);

  assert!(
    is_not_executed(&on_sqlite),
    "SQLite は設定エラーとして実行する。表現不能にしない: {on_sqlite:?}"
  );
  assert!(
    is_representation(&on_memory, RepresentationGap::Unrepresentable),
    "{on_memory:?}"
  );
}

// buildAid の 9 件が両保存先で成功する。
#[test]
fn should_run_marks_build_aid_cases_as_passed_on_every_backend() {
  let data = real_data();
  let ids: Vec<&str> = data
    .cases
    .iter()
    .filter(|case| case.body.get("operation") == Some(&json!("buildAid")))
    .map(|case| case.id.as_str())
    .collect();
  assert_eq!(ids.len(), 9);

  for (target, execute) in TARGETS {
    let reports = run(&data, target, execute);
    for id in &ids {
      assert!(
        matches!(outcome_of(&reports, id), CaseOutcome::Passed { values: Some(_) }),
        "{} {id}",
        target.name
      );
    }
  }
}

// buildAid の契約違反は分類と規則を伴い、規則はデータの期待と一致する。
#[test]
fn should_run_reports_build_aid_contract_violation_with_rule_and_message() {
  let data = real_data();
  let reports = run(&data, &target_memory::TARGET, target_memory::run_case);

  let case = data
    .cases
    .iter()
    .find(|case| case.id == "aid-hyphen-type")
    .expect("aid-hyphen-type がある");
  let expected_rule = case
    .body
    .pointer("/expect/error/rule")
    .and_then(Value::as_str)
    .expect("期待する規則がある");
  assert_eq!(expected_rule, "T-11");

  let CaseOutcome::Passed { values: Some(values) } = outcome_of(&reports, "aid-hyphen-type") else {
    panic!("native診断の保存");
  };
  assert_eq!(values.actual["rule"], expected_rule);
  assert!(values.actual["error"].as_str().unwrap().contains(expected_rule));
}

// 実行器は型名と値から aid を組み立て、利用者の文字列化（user_string）に依存しない。
#[test]
fn should_run_build_aid_uses_type_name_and_value_not_user_string() {
  let data = real_data();
  let case = data
    .cases
    .iter()
    .find(|case| case.id == "aid-library-format")
    .expect("aid-library-format がある");
  let user_string = case
    .body
    .pointer("/input/user_string")
    .and_then(Value::as_str)
    .expect("user_string がある");
  let expected = case
    .body
    .pointer("/expect/value")
    .and_then(Value::as_str)
    .expect("期待する aid がある");
  assert!(!expected.contains(user_string), "期待する aid は user_string と異なる");

  let reports = run(&data, &target_memory::TARGET, target_memory::run_case);

  let CaseOutcome::Passed { values: Some(values) } = outcome_of(&reports, "aid-library-format") else {
    panic!("公開Aidの結果保存");
  };
  assert_eq!(values.actual["value"], expected);
}

#[test]
fn should_run_reports_configuration_case_per_backend() {
  let data = real_data();

  let on_memory = run(&data, &target_memory::TARGET, target_memory::run_case);
  let on_dynamodb = run(&data, &target_dynamodb::TARGET, classification_executor);

  assert!(is_backend_not_targeted(outcome_of(&on_memory, "dynamodb-config-new")));
  assert!(
    matches!(
      outcome_of(&on_dynamodb, "dynamodb-config-new"),
      CaseOutcome::Unverified {
        reason: UnverifiedReason::NotExecuted { .. }
      }
    ),
    "{:?}",
    outcome_of(&on_dynamodb, "dynamodb-config-new")
  );
}

#[test]
fn should_run_reports_layout_case_per_backend() {
  let data = real_data();

  let on_memory = run(&data, &target_memory::TARGET, target_memory::run_case);
  let on_dynamodb = run(&data, &target_dynamodb::TARGET, classification_executor);

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

  let reports = run(&data, &target_dynamodb::TARGET, classification_executor);

  let report = reports
    .iter()
    .find(|report| report.id == "dynamodb-layout-v1")
    .expect("報告がある");
  assert_eq!(reports.len(), data.cases.len());
  assert_eq!(report.rules, layout.rules);
  assert_eq!(report.rules.len(), 12);
}

// ---------------------------------------------------------------------------
// この言語が提供しない保存先（Spanner・Bigtable）のケース
// ---------------------------------------------------------------------------

/// Bigtable の封筒の値の表の操作名。
const BIGTABLE_ENVELOPE_OPERATIONS: [&str; 3] = [
  "encodeBigtableEventEnvelope",
  "encodeBigtableSnapshotEnvelope",
  "decodeBigtableEnvelope",
];

/// 報告の JSON と同じ形にした、ケースの結果を返す。
fn report_of_outcome(outcome: &CaseOutcome) -> Value {
  serde_json::to_value(outcome).expect("結果を直列化できる")
}

/// 結果が、理由の種類 `kind` の対象外なら真。
fn is_not_applicable_of_kind(outcome: &CaseOutcome, kind: &str) -> bool {
  let report = report_of_outcome(outcome);
  report["status"] == "not-applicable" && report["reason"]["kind"] == kind
}

fn is_backend_not_provided(outcome: &CaseOutcome) -> bool {
  is_not_applicable_of_kind(outcome, "backend-not-provided")
}

fn is_failed(outcome: &CaseOutcome) -> bool {
  matches!(outcome, CaseOutcome::Failed { .. })
}

/// 実データのケースを ID で取り出す。
fn case_of_real_data<'a>(data: &'a DataSet, id: &str) -> &'a Case {
  data
    .cases
    .iter()
    .find(|case| case.id == id)
    .unwrap_or_else(|| panic!("実データにケース {id} がある"))
}

/// 実データのうち、この言語が提供しない保存先のケースを返す。
///
/// 場面は `backends` にメモリも DynamoDB も SQLite も Bigtable もないもの。値の表と配置のケースにはない。
fn cases_of_backends_not_provided(data: &DataSet) -> Vec<&Case> {
  data
    .cases
    .iter()
    .filter(|case| match case.kind {
      CaseKind::Scenario => case.body["backends"].as_array().is_some_and(|backends| {
        !backends.is_empty()
          && backends
            .iter()
            .all(|name| !matches!(name.as_str(), Some("memory" | "dynamodb" | "sqlite" | "bigtable")))
      }),
      CaseKind::ValueTable | CaseKind::Layout => false,
    })
    .collect()
}

/// 実データのうち、Bigtable が対象のケース（Bigtable だけの場面 12 件と封筒の値の表 10 件）を返す。
fn bigtable_only_cases(data: &DataSet) -> Vec<&Case> {
  data
    .cases
    .iter()
    .filter(|case| match case.kind {
      CaseKind::Scenario => case.body["backends"] == json!(["bigtable"]),
      CaseKind::ValueTable => case.body["operation"]
        .as_str()
        .is_some_and(|operation| BIGTABLE_ENVELOPE_OPERATIONS.contains(&operation)),
      CaseKind::Layout => false,
    })
    .collect()
}

#[test]
fn should_run_marks_the_14_cases_of_backends_not_provided_as_not_provided_on_every_backend() {
  let data = real_data();
  let cases = cases_of_backends_not_provided(&data);
  assert_eq!(cases.len(), 14, "Spanner の場面 14 件");

  for (target, _) in TARGETS {
    for case in &cases {
      let outcome = run_case(case, target, &data.coverage, |_, _| {
        panic!("提供しない保存先のケースを保存先実行関数へ委譲しない")
      });

      assert!(
        is_backend_not_provided(&outcome),
        "{} {}: {outcome:?}",
        target.name,
        case.id
      );
    }
  }
}

#[test]
fn should_run_case_marks_a_sqlite_only_scene_as_not_targeted_instead_of_not_provided() {
  let data = real_data();
  let case = case_of_real_data(&data, "sqlite-ttl-unsupported");
  assert_eq!(case.body["backends"], json!(["sqlite"]));
  assert!(case.body["store"].get("change_feed").is_none());

  for (target, _) in TARGETS {
    let outcome = run_case(case, target, &data.coverage, |_, _| {
      panic!("今回の保存先でない場面を保存先実行関数へ委譲しない")
    });

    let report = report_of_outcome(&outcome);
    assert_eq!(report["status"], "not-applicable", "{}: {report}", target.name);
    assert_eq!(
      report["reason"]["kind"], "backend-not-targeted",
      "SQLite は提供する保存先なので、「提供しない」にしない: {}: {report}",
      target.name
    );
    let detail = report["reason"]["detail"].as_str().expect("detail は文字列");
    assert!(detail.contains(target.name), "{}: {detail}", target.name);
  }
}

#[test]
fn should_run_case_marks_a_spanner_only_scene_as_not_provided_and_names_the_backend() {
  let data = real_data();
  let case = case_of_real_data(&data, "spanner-change-feed-unsupported");
  assert_eq!(case.body["backends"], json!(["spanner"]));

  for target in [&target_memory::TARGET, &target_dynamodb::TARGET, &target_sqlite::TARGET] {
    let outcome = run_case(case, target, &data.coverage, |_, _| {
      panic!("提供しない保存先のケースを保存先実行関数へ委譲しない")
    });

    let report = report_of_outcome(&outcome);
    assert_eq!(report["status"], "not-applicable", "{}: {report}", target.name);
    assert_eq!(
      report["reason"]["kind"], "backend-not-provided",
      "{}: {report}",
      target.name
    );
    let detail = report["reason"]["detail"].as_str().expect("detail は文字列");
    assert!(
      detail.contains("提供しない") && detail.contains("spanner"),
      "{}: {detail}",
      target.name
    );
  }
}

#[test]
fn should_run_case_does_not_mark_a_dynamodb_only_scene_or_a_common_scene_as_not_provided() {
  let data = real_data();
  let dynamodb_only = case_of_real_data(&data, "dynamodb-config-new");
  let common = case_of_real_data(&data, "core-retention-delete-1");
  assert_eq!(
    common.body["backends"],
    json!(["memory", "dynamodb", "sqlite", "spanner", "bigtable"])
  );

  let dynamodb_only_on_memory = run_case(
    dynamodb_only,
    &target_memory::TARGET,
    &data.coverage,
    target_memory::run_case,
  );
  let common_on_memory = run_case(common, &target_memory::TARGET, &data.coverage, target_memory::run_case);
  let common_on_dynamodb = run_case(
    common,
    &target_dynamodb::TARGET,
    &data.coverage,
    classification_executor,
  );

  assert!(
    is_not_applicable_of_kind(&dynamodb_only_on_memory, "backend-not-targeted"),
    "{dynamodb_only_on_memory:?}"
  );
  assert_eq!(common_on_memory, CaseOutcome::Passed { values: None });
  assert!(is_not_executed(&common_on_dynamodb), "{common_on_dynamodb:?}");
}

#[test]
fn should_run_case_keeps_a_scene_listing_a_provided_backend_not_targeted_instead_of_marking_it_not_provided() {
  let case = scenario(&["T-1"], scenario_body(&["dynamodb", "sqlite"]));

  let on_memory = run_case(&case, &target_memory::TARGET, &no_exclusions(), |_, _| {
    panic!("メモリが対象でない場面を保存先実行関数へ委譲しない")
  });
  let on_dynamodb = run_case(
    &case,
    &target_dynamodb::TARGET,
    &no_exclusions(),
    classification_executor,
  );

  assert!(
    is_not_applicable_of_kind(&on_memory, "backend-not-targeted"),
    "この言語が提供する DynamoDB を含む場面は、メモリでは提供しないのではなく対象でない: {on_memory:?}"
  );
  assert!(is_not_executed(&on_dynamodb), "{on_dynamodb:?}");
}

#[test]
fn should_run_case_marks_the_bigtable_envelope_value_table_as_not_targeted_without_calling_the_executor() {
  let data = real_data();
  let case = case_of_real_data(&data, "bigtable-envelope-event-empty");
  assert!(matches!(case.kind, CaseKind::ValueTable));
  assert_eq!(case.body["operation"], "encodeBigtableEventEnvelope");

  for (target, _) in TARGETS {
    let outcome = run_case(case, target, &data.coverage, |_, _| {
      panic!("今回の保存先でない値の表を保存先実行関数へ委譲しない")
    });

    assert!(
      is_not_applicable_of_kind(&outcome, "backend-not-targeted"),
      "Bigtable を提供するので、提供しないのではなく今回の保存先でない: {}: {outcome:?}",
      target.name
    );
  }
}

#[test]
fn should_run_does_not_mark_fnv1a64_or_build_aid_value_tables_as_not_provided() {
  let data = real_data();
  let first_of = |operation: &str| {
    data
      .cases
      .iter()
      .find(|case| case.body.get("operation") == Some(&json!(operation)))
      .unwrap_or_else(|| panic!("operation が {operation} のケースがある"))
  };
  let fnv1a64 = first_of("fnv1a64");
  let build_aid = first_of("buildAid");

  for (target, _) in TARGETS {
    let fnv1a64_outcome = run_case(fnv1a64, target, &data.coverage, |_, _| {
      panic!("FNV-1a 64 の決定は共通処理で止まる")
    });
    let build_aid_outcome = run_case(build_aid, target, &data.coverage, |_, _| {
      panic!("buildAid は共通処理で実行する")
    });

    assert!(
      is_fnv1a64_decision(&fnv1a64_outcome),
      "{}: {fnv1a64_outcome:?}",
      target.name
    );
    assert!(
      matches!(build_aid_outcome, CaseOutcome::Passed { values: Some(_) }),
      "{}: {build_aid_outcome:?}",
      target.name
    );
  }
}

#[test]
fn should_run_case_hands_the_cases_of_the_given_backend_to_the_executor_instead_of_marking_them_not_provided() {
  let bigtable = Target {
    name: "bigtable",
    capabilities: &[],
    has_layout: false,
    extra_store_settings: &[],
    extra_seed_items: &[],
    represents_change_feed: false,
  };
  let data = real_data();
  let envelope = case_of_real_data(&data, "bigtable-envelope-event-empty");
  let scene = scenario(&["T-1"], scenario_body(&["bigtable"]));

  for case in [envelope, &scene] {
    let calls = Cell::new(0);

    let outcome = run_case(case, &bigtable, &no_exclusions(), |_, _| {
      calls.set(calls.get() + 1);
      CaseOutcome::Passed { values: None }
    });

    assert_eq!(calls.get(), 1, "{}", case.id);
    assert_eq!(outcome, CaseOutcome::Passed { values: None }, "{}", case.id);
  }
}

// ---------------------------------------------------------------------------
// 知らない要素（対象外と分類してから中身を解釈する）
// ---------------------------------------------------------------------------

/// SQLite・Spanner・Bigtable の場面だけが使う要素（`store` の設定、シード、操作、障害の種類と段階）を全部持つ場面を作る。
/// メモリ・DynamoDB の場面では、SQLite の要素も含めて知らない要素になる。
fn scene_with_elements_of_other_backends(backends: &[&str]) -> Case {
  let mut body = scenario_body(backends);
  body["store"]["lock_wait_milliseconds"] = json!(100);
  body["seed"] = json!({"configuration": {"store_id": "5f0c8b8e-3c1a-4d7e-9a4b-2f6d1c0e7a11", "layout_version": 1}});
  body["steps"] = json!([
    {"op": "holdWriteLock", "arguments": {}, "expect": {"result": "success"}},
    {"op": "releaseWriteLock", "arguments": {}, "expect": {"result": "success"}}
  ]);
  body["faults"] = json!([{
    "operation": 1, "phase": "journal-write-previous", "kind": "transaction-aborted",
    "injection": "replace-request", "repeat": {"mode": "count", "count": 1}, "details": {"message": "INJECTED"}
  }]);
  scenario(&["T-1"], body)
}

#[test]
fn should_run_case_fails_a_provided_backend_scene_with_an_unknown_store_setting_as_unknown_element() {
  for (target, execute) in TARGETS {
    let mut body = scenario_body(&[target.name]);
    body["store"] = json!({"retention_count": null, "retention_mode": "delete", "lock_wait_milliseconds": 100});
    let case = scenario(&["T-1"], body);

    let outcome = run_case(&case, target, &no_exclusions(), execute);

    assert!(
      matches!(&outcome, CaseOutcome::Failed { detail, .. } if detail.contains("store.lock_wait_milliseconds")),
      "{}: {outcome:?}",
      target.name
    );
  }
}

#[test]
fn should_run_case_fails_each_remaining_unknown_store_setting_and_seed_item_as_unknown_element() {
  let elements = [
    ("store", "transaction_retry_limit", json!(3)),
    ("store", "ttl_grace_days", json!(1)),
    ("store", "envelope_limit_bytes", json!(1_048_576)),
    ("seed", "configuration", json!({"layout_version": 1})),
    ("seed", "row_deletion_policy_days", json!(1)),
  ];

  for (target, execute) in TARGETS {
    for (section, key, value) in &elements {
      let mut body = scenario_body(&[target.name]);
      if *section == "seed" {
        body["seed"] = json!({});
      }
      body[*section][*key] = value.clone();
      let case = scenario(&["T-1"], body);

      let outcome = run_case(&case, target, &no_exclusions(), execute);

      assert!(
        matches!(&outcome, CaseOutcome::Failed { detail, .. } if detail.contains(&format!("{section}.{key}"))),
        "{} {section}.{key}: {outcome:?}",
        target.name
      );
    }
  }
}

#[test]
fn should_run_case_does_not_treat_known_store_settings_or_seed_items_as_unknown_element() {
  let mut memory_body = scenario_body(&["memory"]);
  memory_body["store"] = json!({"retention_count": null, "retention_mode": "delete", "change_feed": false});
  let mut dynamodb_body = scenario_body(&["dynamodb"]);
  dynamodb_body["store"] = json!({
    "retention_count": 1, "retention_mode": "ttl", "ttl_grace_seconds": 60,
    "layout_version": 1, "retry_limit": 3, "change_feed": false
  });
  dynamodb_body["seed"] = json!({"items": []});

  let on_memory = run_case(
    &scenario(&["T-1"], memory_body),
    &target_memory::TARGET,
    &no_exclusions(),
    target_memory::run_case,
  );
  let on_dynamodb = run_case(
    &scenario(&["T-1"], dynamodb_body),
    &target_dynamodb::TARGET,
    &no_exclusions(),
    classification_executor,
  );

  assert_eq!(on_memory, CaseOutcome::Passed { values: None });
  assert!(is_not_executed(&on_dynamodb), "{on_dynamodb:?}");
}

#[test]
fn should_run_case_does_not_treat_sqlite_store_settings_and_seed_items_as_unknown_element() {
  let mut body = scenario_body(&["sqlite"]);
  body["store"] = json!({"retention_count": null, "retention_mode": "delete", "lock_wait_milliseconds": 100});
  body["seed"] = json!({"configuration": {
    "store_id": "5f0c8b8e-3c1a-4d7e-9a4b-2f6d1c0e7a11", "layout_version": 1, "keep_snapshot_count": null,
    "retention_mode": "delete", "change_feed_enabled": 0
  }});

  let outcome = run_case(
    &scenario(&["T-1"], body),
    &target_sqlite::TARGET,
    &no_exclusions(),
    classification_executor,
  );

  assert!(is_not_executed(&outcome), "{outcome:?}");
}

#[test]
fn should_run_case_fails_the_elements_of_other_backends_on_sqlite_as_unknown_element() {
  let elements = [
    ("store", "transaction_retry_limit", json!(3)),
    ("store", "ttl_grace_days", json!(1)),
    ("store", "envelope_limit_bytes", json!(1_048_576)),
    ("seed", "row_deletion_policy_days", json!(1)),
  ];

  for (section, key, value) in &elements {
    let mut body = scenario_body(&["sqlite"]);
    if *section == "seed" {
      body["seed"] = json!({});
    }
    body[*section][*key] = value.clone();

    let outcome = run_case(
      &scenario(&["T-1"], body),
      &target_sqlite::TARGET,
      &no_exclusions(),
      classification_executor,
    );

    assert!(
      matches!(&outcome, CaseOutcome::Failed { detail, .. } if detail.contains(&format!("{section}.{key}"))),
      "{section}.{key}: {outcome:?}"
    );
  }
}

#[test]
fn should_run_case_marks_a_not_provided_scene_before_interpreting_any_unknown_element() {
  let case = scene_with_elements_of_other_backends(&["spanner"]);

  for (target, _) in TARGETS {
    let outcome = run_case(&case, target, &no_exclusions(), |_, _| {
      panic!("提供しない保存先の場面を保存先実行関数へ委譲しない")
    });

    assert!(is_backend_not_provided(&outcome), "{}: {outcome:?}", target.name);
  }
}

#[test]
fn should_run_case_fails_the_same_elements_as_unknown_element_in_a_scene_of_a_provided_backend() {
  for (target, execute) in TARGETS {
    let case = scene_with_elements_of_other_backends(&[target.name]);

    let outcome = run_case(&case, target, &no_exclusions(), execute);

    assert!(is_failed(&outcome), "{}: {outcome:?}", target.name);
  }
}

#[test]
fn should_run_case_fails_unknown_operations_fault_declarations_and_value_operations_as_unknown_element() {
  for operation in ["holdWriteLock", "releaseWriteLock"] {
    let mut body = scenario_body(&["memory"]);
    body["steps"] = json!([{"op": operation, "arguments": {}, "expect": {"result": "success"}}]);

    let outcome = run_case(
      &scenario(&["T-1"], body),
      &target_memory::TARGET,
      &no_exclusions(),
      target_memory::run_case,
    );

    assert!(is_failed(&outcome), "{operation}: {outcome:?}");
  }

  for (phase, kind) in [
    ("commit", "transaction-aborted"),
    ("journal-write-previous", "storage-error"),
    ("journal-write-tail", "storage-error"),
  ] {
    for (target, execute) in TARGETS {
      let mut body = scenario_body(&[target.name]);
      body["faults"] = json!([{
        "operation": 1, "phase": phase, "kind": kind, "injection": "replace-request",
        "repeat": {"mode": "count", "count": 1}, "details": {"message": "INJECTED"}
      }]);

      let outcome = run_case(&scenario(&["T-1"], body), target, &no_exclusions(), execute);

      assert!(is_failed(&outcome), "{} {phase} {kind}: {outcome:?}", target.name);
    }
  }

  let mut value_table = scenario(&["T-1"], json!({"id": "synthetic", "rules": ["T-1"]}));
  value_table.kind = CaseKind::ValueTable;
  value_table.body["operation"] = json!("unknownOperation");
  value_table.body["input"] = json!({});
  value_table.body["expect"] = json!({});
  let outcome = run_case(
    &value_table,
    &target_memory::TARGET,
    &no_exclusions(),
    target_memory::run_case,
  );
  assert!(is_failed(&outcome), "{outcome:?}");
}
