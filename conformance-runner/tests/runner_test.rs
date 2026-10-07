//! ケースの分類（対象外・未検証・失敗・成功）と、保存先に依存しない準備の試験。
//! メモリは実操作に接続して54件が成功する。DynamoDBは未接続で、値の表の `buildAid` の9件だけが成功する。

use std::cell::{Cell, RefCell};
use std::path::{Path, PathBuf};

use event_store_adapter_conformance_rs::data::{load, Case, CaseKind, Coverage, CoverageExclusion, DataSet};
use event_store_adapter_conformance_rs::fault::Phase;
use event_store_adapter_conformance_rs::report::{
  CaseOutcome, CaseReport, NotApplicableReason, ObservedValues, RepresentationGap, UnverifiedReason,
};
use event_store_adapter_conformance_rs::runner::{run, run_case, CaseExecutor, Target};
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
    target_dynamodb::run_case,
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
    json!({"requests": [{"api": "BatchGetItem", "phase": "read-snapshot", "constraints": {"keys": []}}]});
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
      assert_eq!(
        outcome,
        CaseOutcome::Passed { values: None },
        "{} {}",
        target.name,
        case.id
      );
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
    target_dynamodb::run_case,
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
    "constraints": {"keys": ["journal:__config__:0"], "consistent_read_all_tables": true}
  }]});
  let case = scenario(&["DY-8"], body);

  let outcome = run_case(
    &case,
    &target_dynamodb::TARGET,
    &no_exclusions(),
    target_dynamodb::run_case,
  );

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

  let outcome = run_case(
    &case,
    &target_dynamodb::TARGET,
    &no_exclusions(),
    target_dynamodb::run_case,
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

  let with_ttl = run_case(&case, &memory_with_ttl, &no_exclusions(), target_dynamodb::run_case);
  let without_ttl = run_case(
    &case,
    &dynamodb_without_ttl,
    &no_exclusions(),
    target_dynamodb::run_case,
  );

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

  assert!(is_backend_not_targeted(&run_case(
    &case,
    &only_ttl,
    &no_exclusions(),
    target_dynamodb::run_case
  )));
  assert!(is_not_executed(&run_case(
    &case,
    &both,
    &no_exclusions(),
    target_dynamodb::run_case
  )));
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

  let with_layout = run_case(&case, &memory_with_layout, &no_exclusions(), target_dynamodb::run_case);
  let without_layout = run_case(
    &case,
    &dynamodb_without_layout,
    &no_exclusions(),
    target_dynamodb::run_case,
  );

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
    target_dynamodb::run_case,
  );
  let not_listed = run_case(
    &scenario(&["T-1"], scenario_body(&["memory"])),
    &unknown,
    &no_exclusions(),
    target_dynamodb::run_case,
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
  (&target_dynamodb::TARGET, target_dynamodb::run_case),
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
  assert_eq!(dynamodb_only.len(), 44, "DynamoDB だけが対象の場面 43 件と、配置 1 件");

  for id in dynamodb_only {
    let outcome = outcome_of(&reports, id);
    assert!(is_backend_not_targeted(outcome), "{id}: {outcome:?}");
  }
}

#[test]
fn should_run_does_not_mark_any_case_as_not_targeted_on_dynamodb() {
  let data = real_data();

  let reports = run(&data, &target_dynamodb::TARGET, target_dynamodb::run_case);

  let not_targeted: Vec<&str> = reports
    .iter()
    .filter(|report| is_backend_not_targeted(&report.outcome))
    .map(|report| report.id.as_str())
    .collect();
  assert!(not_targeted.is_empty(), "{not_targeted:?}");
}

#[test]
fn should_run_reports_memory_writes_and_reads_and_leaves_retention_unverified() {
  let data = real_data();

  let reports = run(&data, &target_memory::TARGET, target_memory::run_case);

  // (passed, failed, not-applicable, unverified)。保持・通知の4件を未検証に残す。
  assert_eq!(counts(&reports), (54, 0, 58, 4));
}

#[test]
fn should_run_reports_the_build_aid_success_for_dynamodb_and_leaves_the_rest_unverified() {
  let data = real_data();

  let reports = run(&data, &target_dynamodb::TARGET, target_dynamodb::run_case);

  // (passed, failed, not-applicable, unverified)。passed 9 は値の表の buildAid。
  assert_eq!(counts(&reports), (9, 0, 14, 93));
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
      assert_eq!(
        outcome_of(&reports, id),
        &CaseOutcome::Passed { values: None },
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

  assert_eq!(
    outcome_of(&reports, "aid-hyphen-type"),
    &CaseOutcome::Passed { values: None }
  );
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

  assert_eq!(
    outcome_of(&reports, "aid-library-format"),
    &CaseOutcome::Passed { values: None }
  );
}

#[test]
fn should_run_reports_configuration_case_per_backend() {
  let data = real_data();

  let on_memory = run(&data, &target_memory::TARGET, target_memory::run_case);
  let on_dynamodb = run(&data, &target_dynamodb::TARGET, target_dynamodb::run_case);

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

  let on_memory = run(&data, &target_memory::TARGET, target_memory::run_case);
  let on_dynamodb = run(&data, &target_dynamodb::TARGET, target_dynamodb::run_case);

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

  let reports = run(&data, &target_dynamodb::TARGET, target_dynamodb::run_case);

  let report = reports
    .iter()
    .find(|report| report.id == "dynamodb-layout-v1")
    .expect("報告がある");
  assert_eq!(reports.len(), data.cases.len());
  assert_eq!(report.rules, layout.rules);
  assert_eq!(report.rules.len(), 12);
}
