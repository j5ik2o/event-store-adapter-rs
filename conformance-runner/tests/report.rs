//! 報告の内容・規則ごとの集計・終了の判定の試験。

use std::path::{Path, PathBuf};

use event_store_adapter_conformance_rs::data::{
  load, Coverage, CoverageExclusion, DataSet, ManifestProblem, ManifestVerification,
};
use event_store_adapter_conformance_rs::fault::{Phase, Repeat, UnfiredFault};
use event_store_adapter_conformance_rs::report::{
  parse_package_version, resolve_revision, CaseOutcome, CaseReport, Implementation, NotApplicableReason, Report,
  RepresentationGap, StatusCounts, UnverifiedReason,
};
use event_store_adapter_conformance_rs::runner::run;
use event_store_adapter_conformance_rs::target_memory;
use serde_json::{json, Value};

const FIXED_MANIFEST_SHA256: &str = "61c26614dbbfba88eebce72cc1d2b0220218839e74dcfb64c19268f7ee2302ce";

fn conformance_dir() -> PathBuf {
  Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance")
}

fn data_set(problems: Vec<ManifestProblem>, required: &[&str], exclusions: Vec<CoverageExclusion>) -> DataSet {
  DataSet {
    manifest_version: Some("1.0.0".to_string()),
    manifest_files: 22,
    manifest_sha256: FIXED_MANIFEST_SHA256.to_string(),
    manifest: ManifestVerification { problems },
    coverage: Coverage {
      required_rules: required.iter().map(|rule| rule.to_string()).collect(),
      exclusions,
    },
    cases: vec![],
  }
}

fn implementation() -> Implementation {
  Implementation {
    language: "rust",
    crate_name: "event-store-adapter-rs",
    version: Some("4.0.0-alpha.0".to_string()),
    revision: None,
  }
}

fn case_report(id: &str, rules: &[&str], outcome: CaseOutcome) -> CaseReport {
  CaseReport {
    id: id.to_string(),
    rules: rules.iter().map(|rule| rule.to_string()).collect(),
    outcome,
  }
}

fn not_applicable() -> CaseOutcome {
  CaseOutcome::NotApplicable {
    reason: NotApplicableReason::BackendNotTargeted {
      detail: "ケースの backends に実行する保存先がない".to_string(),
    },
  }
}

fn unverified() -> CaseOutcome {
  CaseOutcome::Unverified {
    reason: UnverifiedReason::NotExecuted {
      detail: "保存先が未接続".to_string(),
    },
  }
}

fn failed() -> CaseOutcome {
  CaseOutcome::Failed {
    failed_operation: Some(1),
    detail: "期待と異なる".to_string(),
    expected: None,
    actual: None,
    unfired_faults: vec![],
  }
}

/// 規則 `T-1` だけを持つケースを、与えた結果の数だけ並べた報告を作る。
fn report_of(problems: Vec<ManifestProblem>, outcomes: Vec<CaseOutcome>) -> Report {
  let cases = outcomes
    .into_iter()
    .enumerate()
    .map(|(index, outcome)| case_report(&format!("case-{index}"), &["T-1"], outcome))
    .collect();
  Report::build(&data_set(problems, &["T-1"], vec![]), "memory", cases, implementation())
}

fn to_json(report: &Report) -> Value {
  serde_json::to_value(report).expect("報告を直列化できる")
}

fn rule_row<'a>(report: &'a Value, rule: &str) -> &'a Value {
  report["rules"]
    .as_array()
    .expect("rules は配列")
    .iter()
    .find(|row| row["rule"] == rule)
    .unwrap_or_else(|| panic!("規則 {rule} の行がある"))
}

fn counts_of(row: &Value) -> (u64, u64, u64, u64) {
  (
    row["passed"].as_u64().expect("passed は整数"),
    row["failed"].as_u64().expect("failed は整数"),
    row["not_applicable"].as_u64().expect("not_applicable は整数"),
    row["unverified"].as_u64().expect("unverified は整数"),
  )
}

// ---------------------------------------------------------------------------
// 報告の内容
// ---------------------------------------------------------------------------

#[test]
fn test_report_exposes_data_version_and_manifest_verification_result() {
  let report = Report::build(&data_set(vec![], &[], vec![]), "memory", vec![], implementation());

  let json = to_json(&report);

  assert_eq!(json["data"]["version"], "1.0.0");
  assert_eq!(json["data"]["manifest"]["sha256"], FIXED_MANIFEST_SHA256);
  assert_eq!(json["data"]["manifest"]["verification"], "passed");
}

#[test]
fn test_report_exposes_the_manifest_version_that_was_actually_read() {
  let mut data = data_set(vec![ManifestProblem::Version("\"9.9.9\"".to_string())], &[], vec![]);
  data.manifest_version = Some("9.9.9".to_string());

  let json = to_json(&Report::build(&data, "memory", vec![], implementation()));

  assert_eq!(json["data"]["version"], "9.9.9", "期待する版ではなく、読んだ版を書く");
  assert_eq!(
    json["data"]["manifest"]["verification"], "failed",
    "期待する版との比較は報告の版とは別に行う"
  );
}

#[test]
fn test_report_exposes_null_data_version_when_manifest_has_no_string_version() {
  let mut data = data_set(vec![], &[], vec![]);
  data.manifest_version = None;

  let json = to_json(&Report::build(&data, "memory", vec![], implementation()));

  assert!(json["data"]["version"].is_null());
}

#[test]
fn test_report_exposes_failed_manifest_verification_with_its_problems() {
  let problems = vec![ManifestProblem::Modified("values/aid.json".to_string())];
  let report = Report::build(&data_set(problems, &[], vec![]), "memory", vec![], implementation());

  let json = to_json(&report);

  assert_eq!(json["data"]["manifest"]["verification"], "failed");
  let listed = json["data"]["manifest"]["problems"]
    .as_array()
    .expect("problems は配列");
  assert_eq!(listed.len(), 1);
  assert!(listed[0].as_str().expect("問題は文字列").contains("values/aid.json"));
}

#[test]
fn test_report_exposes_implementation_and_backend() {
  let report = Report::build(&data_set(vec![], &[], vec![]), "dynamodb", vec![], implementation());

  let json = to_json(&report);

  assert_eq!(json["backend"], "dynamodb");
  assert_eq!(json["implementation"]["language"], "rust");
  assert_eq!(json["implementation"]["crate"], "event-store-adapter-rs");
  assert_eq!(json["implementation"]["version"], "4.0.0-alpha.0");
  assert!(json["implementation"]["revision"].is_null());
}

#[test]
fn test_report_case_entry_of_passed_case_carries_id_rules_and_status() {
  let entry = serde_json::to_value(case_report("c1", &["T-1", "T-3"], CaseOutcome::Passed)).unwrap();

  assert_eq!(entry, json!({"id": "c1", "rules": ["T-1", "T-3"], "status": "passed"}));
}

#[test]
fn test_report_case_entry_of_failed_case_carries_operation_values_and_unfired_faults() {
  let outcome = CaseOutcome::Failed {
    failed_operation: Some(3),
    detail: "期待と異なる".to_string(),
    expected: Some(json!({"a": 1})),
    actual: Some(json!({"a": 2})),
    unfired_faults: vec![UnfiredFault {
      index: 0,
      operation: 3,
      phase: Phase::Commit,
      declared: Repeat::Count { count: 2 },
      applied: 1,
    }],
  };

  let entry = serde_json::to_value(case_report("c1", &["T-1"], outcome)).unwrap();

  assert_eq!(entry["status"], "failed");
  assert_eq!(entry["failed_operation"], 3);
  assert_eq!(entry["expected"], json!({"a": 1}));
  assert_eq!(entry["actual"], json!({"a": 2}));
  assert_eq!(
    entry["unfired_faults"][0]["declared"],
    json!({"mode": "count", "count": 2})
  );
  assert_eq!(entry["unfired_faults"][0]["applied"], 1);
}

/// 対象外の理由の種類名。理由が 4 つだけであることを、網羅的な `match` で型に固定する。
fn kind_name(reason: &NotApplicableReason) -> &'static str {
  match reason {
    NotApplicableReason::BackendNotTargeted { .. } => "backend-not-targeted",
    NotApplicableReason::Representation { .. } => "representation",
    NotApplicableReason::Fnv1a64Decision { .. } => "fnv1a64-decision",
    NotApplicableReason::CoverageExclusion { .. } => "coverage-exclusion",
  }
}

#[test]
fn test_report_case_entry_of_not_applicable_case_carries_one_of_the_four_reasons() {
  let reasons = vec![
    NotApplicableReason::BackendNotTargeted {
      detail: "保存先が対象外".to_string(),
    },
    NotApplicableReason::Representation {
      representation: RepresentationGap::Unrepresentable,
      detail: "SeqNr は u64。負数を表せない".to_string(),
    },
    NotApplicableReason::Fnv1a64Decision {
      detail: "最初のメジャーにハッシュを使う保存先がない".to_string(),
    },
    NotApplicableReason::CoverageExclusion {
      detail: "deleted: 削除済み".to_string(),
    },
  ];

  for reason in reasons {
    let expected_kind = kind_name(&reason);
    let outcome = CaseOutcome::NotApplicable { reason };

    let entry = serde_json::to_value(case_report("c1", &["T-1"], outcome)).unwrap();

    assert_eq!(entry["status"], "not-applicable");
    assert_eq!(entry["reason"]["kind"], expected_kind);
    assert!(!entry["reason"]["detail"].as_str().expect("detail は文字列").is_empty());
  }
}

#[test]
fn test_report_case_entry_of_time_precision_choice_is_a_representation_reason() {
  let outcome = CaseOutcome::NotApplicable {
    reason: NotApplicableReason::Representation {
      representation: RepresentationGap::TimePrecision,
      detail: "標準時刻型の精度に合わない選択".to_string(),
    },
  };

  let entry = serde_json::to_value(case_report("c1", &["T-13"], outcome)).unwrap();

  assert_eq!(entry["reason"]["kind"], "representation");
  assert_eq!(entry["reason"]["representation"], "time-precision");
}

#[test]
fn test_report_case_entry_of_unverified_case_lists_unimplemented_constraint_words() {
  let outcome = CaseOutcome::Unverified {
    reason: UnverifiedReason::UnimplementedConstraintWords {
      words: vec!["consistent_read_all_tables".to_string(), "keys".to_string()],
    },
  };

  let entry = serde_json::to_value(case_report("c1", &["DY-8"], outcome)).unwrap();

  assert_eq!(entry["status"], "unverified");
  assert_eq!(
    entry["reason"],
    json!({"kind": "unimplemented-constraint-words", "words": ["consistent_read_all_tables", "keys"]})
  );
}

#[test]
fn test_report_case_entry_of_unverified_case_explains_why_it_was_not_executed() {
  let entry = serde_json::to_value(case_report("c1", &["T-1"], unverified())).unwrap();

  assert_eq!(entry["status"], "unverified");
  assert_eq!(entry["reason"]["kind"], "not-executed");
  assert!(!entry["reason"]["detail"].as_str().expect("detail は文字列").is_empty());
}

// ---------------------------------------------------------------------------
// 規則ごとの集計
// ---------------------------------------------------------------------------

#[test]
fn test_report_counts_each_case_in_every_rule_it_names() {
  let cases = vec![
    case_report("c1", &["T-1", "T-3"], CaseOutcome::Passed),
    case_report("c2", &["T-1"], failed()),
    case_report("c3", &["T-3", "T-9"], not_applicable()),
    case_report("c4", &["T-9"], unverified()),
  ];
  let report = Report::build(
    &data_set(vec![], &["T-1", "T-3", "T-9"], vec![]),
    "memory",
    cases,
    implementation(),
  );

  let json = to_json(&report);

  // (passed, failed, not_applicable, unverified)
  assert_eq!(counts_of(rule_row(&json, "T-1")), (1, 1, 0, 0));
  assert_eq!(counts_of(rule_row(&json, "T-3")), (1, 0, 1, 0));
  assert_eq!(counts_of(rule_row(&json, "T-9")), (0, 0, 1, 1));
}

#[test]
fn test_report_lists_required_rules_without_cases_with_zero_counts() {
  let report = Report::build(
    &data_set(vec![], &["T-1", "X-9"], vec![]),
    "memory",
    vec![],
    implementation(),
  );

  let json = to_json(&report);

  assert_eq!(counts_of(rule_row(&json, "T-1")), (0, 0, 0, 0));
  assert_eq!(counts_of(rule_row(&json, "X-9")), (0, 0, 0, 0));
}

#[test]
fn test_report_records_coverage_exclusion_reason_on_excluded_rules_only() {
  let exclusions = vec![
    CoverageExclusion {
      rule: "W-5".to_string(),
      status: "deleted".to_string(),
      reason: "version の廃止で削除済み".to_string(),
    },
    CoverageExclusion {
      rule: "R-7".to_string(),
      status: "caller-obligation".to_string(),
      reason: "呼び出し側の推奨事項".to_string(),
    },
  ];
  let report = Report::build(
    &data_set(vec![], &["T-1", "W-5", "R-7"], exclusions),
    "memory",
    vec![],
    implementation(),
  );

  let json = to_json(&report);

  let w5 = &rule_row(&json, "W-5")["reason"];
  assert_eq!(w5["kind"], "coverage-exclusion");
  let w5_detail = w5["detail"].as_str().expect("detail は文字列");
  assert!(
    w5_detail.contains("deleted") && w5_detail.contains("version の廃止で削除済み"),
    "{w5_detail}"
  );
  let r7_detail = rule_row(&json, "R-7")["reason"]["detail"]
    .as_str()
    .expect("detail は文字列");
  assert!(
    r7_detail.contains("caller-obligation") && r7_detail.contains("呼び出し側の推奨事項"),
    "{r7_detail}"
  );
  assert!(rule_row(&json, "T-1")["reason"].is_null());
}

#[test]
fn test_report_of_real_data_counts_every_case_in_every_rule_and_lists_each_case_once() {
  let data = load(&conformance_dir()).expect("実データを読める");
  let case_reports = run(&data, &target_memory::TARGET);
  let memberships: usize = case_reports.iter().map(|case| case.rules.len()).sum();

  let report = Report::build(&data, "memory", case_reports, implementation());

  let json = to_json(&report);
  let counted: u64 = json["rules"]
    .as_array()
    .expect("rules は配列")
    .iter()
    .map(|row| {
      let (passed, failed, not_applicable, unverified) = counts_of(row);
      passed + failed + not_applicable + unverified
    })
    .sum();
  assert_eq!(json["cases"].as_array().expect("cases は配列").len(), 116);
  assert_eq!(
    counted as usize, memberships,
    "複数の規則を持つケースを、最初の規則だけで数えない"
  );
  assert!(json["rules"].as_array().unwrap().len() >= 46);
  assert_eq!(json["data"]["manifest"]["verification"], "passed");
  assert_eq!(json["data"]["manifest"]["sha256"], FIXED_MANIFEST_SHA256);
  assert!(rule_row(&json, "W-5")["reason"]["detail"]
    .as_str()
    .unwrap()
    .contains("deleted"));
  assert!(rule_row(&json, "R-7")["reason"]["detail"]
    .as_str()
    .unwrap()
    .contains("caller-obligation"));
}

#[test]
fn test_status_counts_totals_each_state() {
  let outcomes = vec![
    CaseOutcome::Passed,
    failed(),
    failed(),
    not_applicable(),
    not_applicable(),
    not_applicable(),
    unverified(),
    unverified(),
    unverified(),
    unverified(),
  ];
  let report = report_of(vec![], outcomes);

  assert_eq!(
    report.status_counts(),
    StatusCounts {
      passed: 1,
      failed: 2,
      not_applicable: 3,
      unverified: 4
    }
  );
}

// ---------------------------------------------------------------------------
// 終了の判定
// ---------------------------------------------------------------------------

#[test]
fn test_should_fail_is_false_for_unverified_cases_without_require_all() {
  let report = report_of(vec![], vec![unverified(), not_applicable()]);

  assert!(!report.should_fail(false));
}

#[test]
fn test_should_fail_is_true_for_unverified_cases_with_require_all() {
  let report = report_of(vec![], vec![unverified(), not_applicable()]);

  assert!(report.should_fail(true));
}

#[test]
fn test_should_fail_is_true_for_failed_case_regardless_of_require_all() {
  let report = report_of(vec![], vec![CaseOutcome::Passed, failed()]);

  assert!(report.should_fail(false));
  assert!(report.should_fail(true));
}

#[test]
fn test_should_fail_is_true_when_manifest_verification_failed_without_require_all() {
  let report = report_of(
    vec![ManifestProblem::Missing("values/aid.json".to_string())],
    vec![not_applicable()],
  );

  assert!(report.should_fail(false));
}

#[test]
fn test_should_fail_is_false_for_passed_and_not_applicable_cases_even_with_require_all() {
  let report = report_of(vec![], vec![CaseOutcome::Passed, not_applicable()]);

  assert!(!report.should_fail(false));
  assert!(!report.should_fail(true));
}

// ---------------------------------------------------------------------------
// 実装の版
// ---------------------------------------------------------------------------

#[test]
fn test_parse_package_version_reads_version_of_package_section_only() {
  let manifest = "[dependencies]\nserde = { version = \"1.0.0\" }\n\n[package]\nname = \"sample\"\nversion = \"4.0.0-alpha.0\"\n\n[dev-dependencies]\nversion = \"9.9.9\"\n";

  assert_eq!(parse_package_version(manifest), Some("4.0.0-alpha.0".to_string()));
}

#[test]
fn test_parse_package_version_ignores_version_outside_package_section() {
  let manifest = "[package]\nname = \"sample\"\n\n[dependencies]\nversion = \"9.9.9\"\n";

  assert_eq!(parse_package_version(manifest), None);
}

#[test]
fn test_parse_package_version_finds_a_version_in_real_library_manifest() {
  let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../lib/Cargo.toml");
  let manifest = std::fs::read_to_string(path).expect("lib/Cargo.toml を読める");

  let version = parse_package_version(&manifest).expect("[package] の version がある");

  assert!(version.starts_with(|c: char| c.is_ascii_digit()), "{version}");
  assert!(manifest.contains(&format!("version = \"{version}\"")));
}

// ---------------------------------------------------------------------------
// 実装のコミットの識別子
// ---------------------------------------------------------------------------

const GIT_HEAD: &str = "1111111111111111111111111111111111111111";
const CI_SHA: &str = "2222222222222222222222222222222222222222";

#[test]
fn test_resolve_revision_prefers_git_head_over_github_sha() {
  let revision = resolve_revision(Some(GIT_HEAD.to_string()), Some(CI_SHA.to_string()));

  assert_eq!(revision, Some(GIT_HEAD.to_string()));
}

#[test]
fn test_resolve_revision_falls_back_to_github_sha_when_git_is_unavailable() {
  let revision = resolve_revision(None, Some(CI_SHA.to_string()));

  assert_eq!(revision, Some(CI_SHA.to_string()));
}

#[test]
fn test_resolve_revision_ignores_blank_values_and_trims_whitespace() {
  assert_eq!(
    resolve_revision(Some("\n".to_string()), Some(format!("{CI_SHA}\n"))),
    Some(CI_SHA.to_string())
  );
  assert_eq!(
    resolve_revision(Some(format!("{GIT_HEAD}\n")), None),
    Some(GIT_HEAD.to_string())
  );
  assert_eq!(resolve_revision(Some(String::new()), Some("  ".to_string())), None);
}

#[test]
fn test_resolve_revision_is_none_without_any_source() {
  assert_eq!(resolve_revision(None, None), None);
}
