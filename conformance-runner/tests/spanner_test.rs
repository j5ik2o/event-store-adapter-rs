#![cfg(feature = "spanner")]
//! Spanner の実保存先（エミュレーター）に対する適合ケースの実行の試験。
//! 各場面は新しいデータベース（5 テーブル）で実行し、ライブラリの公開操作と `test-hooks` の経路だけを使う。

use std::path::{Path, PathBuf};

use event_store_adapter_conformance_rs::data::{load, Case, CaseKind, DataSet};
use event_store_adapter_conformance_rs::report::{CaseOutcome, CaseReport, Implementation, Report};
use event_store_adapter_conformance_rs::runner::{run, run_case};
use event_store_adapter_conformance_rs::target_spanner;
use serde_json::{json, Value};

/// Spanner の場面だけが使うケース。
const SPANNER_ONLY_CASES: [&str; 14] = [
  "spanner-aborted-retry-within-limit",
  "spanner-aborted-retry-limit-exceeded",
  "spanner-commit-response-lost",
  "spanner-change-feed-unsupported",
  "spanner-seeded-layout-version-mismatch",
  "spanner-seeded-keep-count-mismatch",
  "spanner-seeded-retention-mode-mismatch",
  "spanner-seeded-ttl-grace-config-mismatch",
  "spanner-seeded-ttl-grace-policy-mismatch",
  "spanner-seeded-ttl-grace-client-mismatch",
  "spanner-seeded-change-feed-enabled",
  "spanner-ttl-grace-days-not-positive",
  "spanner-seeded-configuration-match",
  "spanner-seeded-ttl-configuration-match",
];

/// 行キーの接頭辞のハッシュ（FNV-1a 64）を確かめる値の表のケース。
const FNV1A64_CASES: [&str; 4] = ["hash-fnv1a64-1", "hash-fnv1a64-2", "hash-fnv1a64-3", "hash-fnv1a64-4"];

/// ナノ秒の時刻型と符号なしの seq_nr では表せないので、理由付きの対象外になる場面。
const UNREPRESENTABLE_CASES: [&str; 5] = [
  "core-time-millisecond-below-min",
  "core-time-millisecond-above-max",
  "core-time-roundtrip-millisecond-min",
  "core-time-roundtrip-millisecond-max",
  "core-seq-negative",
];

fn conformance_dir() -> PathBuf {
  Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance")
}

fn real_data() -> DataSet {
  load(&conformance_dir()).expect("実データを読める")
}

fn run_spanner(data: &DataSet) -> Vec<CaseReport> {
  run(data, &target_spanner::TARGET, target_spanner::run_case)
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

fn is_passed(outcome: &CaseOutcome) -> bool {
  matches!(outcome, CaseOutcome::Passed { .. })
}

fn is_failed_with_detail(outcome: &CaseOutcome) -> bool {
  matches!(outcome, CaseOutcome::Failed { detail, .. } if !detail.is_empty())
}

fn kind_of_not_applicable(outcome: &CaseOutcome) -> Option<String> {
  let report = serde_json::to_value(outcome).expect("結果を直列化できる");
  (report["status"] == "not-applicable").then(|| report["reason"]["kind"].as_str().unwrap_or_default().to_owned())
}

fn case_of<'a>(data: &'a DataSet, id: &str) -> &'a Case {
  data
    .cases
    .iter()
    .find(|case| case.id == id)
    .unwrap_or_else(|| panic!("実データにケース {id} がある"))
}

fn copy_of(case: &Case) -> Case {
  Case {
    id: case.id.clone(),
    rules: case.rules.clone(),
    kind: case.kind,
    file: case.file.clone(),
    body: case.body.clone(),
  }
}

fn execute(data: &DataSet, case: &Case) -> CaseOutcome {
  run_case(case, &target_spanner::TARGET, &data.coverage, target_spanner::run_case)
}

fn implementation() -> Implementation {
  Implementation {
    language: "rust",
    crate_name: "event-store-adapter-rs",
    version: None,
    revision: None,
  }
}

fn rule_counts(report: &Value, rule: &str) -> (u64, u64, u64, u64) {
  let row = report["rules"]
    .as_array()
    .expect("rules は配列")
    .iter()
    .find(|row| row["rule"] == rule)
    .unwrap_or_else(|| panic!("規則 {rule} の行がある"));
  (
    row["passed"].as_u64().expect("passed は整数"),
    row["failed"].as_u64().expect("failed は整数"),
    row["not_applicable"].as_u64().expect("not_applicable は整数"),
    row["unverified"].as_u64().expect("unverified は整数"),
  )
}

fn targets_spanner(case: &Case) -> bool {
  case.kind == CaseKind::Scenario
    && case.body["backends"]
      .as_array()
      .is_some_and(|backends| backends.iter().any(|name| name == "spanner"))
}

// ---------------------------------------------------------------------------
// 実データ全体
// ---------------------------------------------------------------------------

#[test]
fn should_run_reports_every_spanner_targeted_case_as_passed() {
  let data = real_data();

  let reports = run_spanner(&data);

  // (passed, failed, not-applicable, unverified)。成功 77 は、値の表の buildAid 9 件と、Spanner が対象の場面と値の表の
  // 実行 68 件（場面 57 から表現できない 5 件を除いた 52 と、FNV-1a 64・validateSeqNr・validateOccurredAt の値の表
  // 21 から表現できない 5 件を除いた 16）。対象外 92 の内訳は、runner_test の分類の試験が持つ。
  assert_eq!(counts(&reports), (77, 0, 92, 0));
  let mut targeted = 0;
  for case in data.cases.iter().filter(|case| targets_spanner(case)) {
    targeted += 1;
    let outcome = outcome_of(&reports, &case.id);
    if UNREPRESENTABLE_CASES.contains(&case.id.as_str()) {
      assert_eq!(
        kind_of_not_applicable(outcome).as_deref(),
        Some("representation"),
        "{}: {outcome:?}",
        case.id
      );
    } else {
      assert!(is_passed(outcome), "{}: {outcome:?}", case.id);
    }
  }
  assert_eq!(targeted, 57, "Spanner が対象の場面は共通 42・spanner/ 14・共有 1");
}

#[test]
fn should_run_spanner_pass_the_fourteen_cases_only_spanner_targets_and_the_shared_scene() {
  let reports = run_spanner(&real_data());

  for id in SPANNER_ONLY_CASES.iter().chain(["store-shared-persistent"].iter()) {
    assert!(
      is_passed(outcome_of(&reports, id)),
      "{id}: {:?}",
      outcome_of(&reports, id)
    );
  }
}

#[test]
fn should_run_spanner_report_the_spn_rules_as_passed_without_failures() {
  let data = real_data();
  let reports = run_spanner(&data);

  let report = Report::build(&data, "spanner", reports, implementation());

  assert!(!report.should_fail(true), "--require-all でも成功する");
  let json = serde_json::to_value(&report).expect("報告を直列化できる");
  assert_eq!(json["backend"], "spanner");
  // (passed, failed, not-applicable, unverified)
  assert_eq!(rule_counts(&json, "SPN-2"), (5, 0, 0, 0));
  assert_eq!(rule_counts(&json, "SPN-7"), (2, 0, 0, 0));
  assert_eq!(rule_counts(&json, "SPN-8"), (1, 0, 0, 0));
  assert_eq!(rule_counts(&json, "SPN-16"), (5, 0, 0, 0));
  assert_eq!(rule_counts(&json, "SPN-17"), (2, 0, 0, 0));
  assert_eq!(rule_counts(&json, "K-1"), (4, 0, 0, 0));
}

#[test]
fn should_run_spanner_pass_the_fnv1a64_cases_with_the_hash_the_row_keys_use() {
  let data = real_data();

  for id in FNV1A64_CASES {
    let case = case_of(&data, id);
    assert_eq!(case.body["operation"], "fnv1a64", "{id}");

    let outcome = execute(&data, case);

    match outcome {
      CaseOutcome::Passed { values: Some(values) } => {
        assert_eq!(values.expected, case.body["expect"], "{id}");
        assert_eq!(
          values.actual, case.body["expect"],
          "{id}: 実際の値を期待値と同じ表記で報告する"
        );
      }
      other => panic!("{id}: 成功のはずが {other:?}"),
    }
  }
}

#[test]
fn should_run_spanner_fail_an_fnv1a64_case_whose_expected_value_differs_from_the_row_key_hash() {
  let data = real_data();

  for id in FNV1A64_CASES {
    let mut altered = copy_of(case_of(&data, id));
    altered.body["expect"]["value"] = json!("0x0000000000000000");

    let outcome = execute(&data, &altered);

    assert!(
      is_failed_with_detail(&outcome),
      "{id}: 期待値と違う値を成功にしない: {outcome:?}"
    );
  }
}

// ---------------------------------------------------------------------------
// 障害の差し込み
// ---------------------------------------------------------------------------

#[test]
fn should_run_spanner_inject_every_fault_of_the_scenes_without_leaving_unfired_faults() {
  let data = real_data();
  let reports = run_spanner(&data);

  let with_faults: Vec<&Case> = data
    .cases
    .iter()
    .filter(|case| targets_spanner(case) && case.body["faults"].as_array().is_some_and(|faults| !faults.is_empty()))
    .collect();

  assert_eq!(with_faults.len(), 14, "障害を持つ場面は共通 11 件と spanner/ の 3 件");
  for case in with_faults {
    let outcome = outcome_of(&reports, &case.id);
    assert!(
      is_passed(outcome),
      "発火しなかった障害は失敗になる: {}: {outcome:?}",
      case.id
    );
  }
}

// 場面の宣言を変えると、実際にその設定で保存先が動いて、期待と食い違えば失敗になる（成功と決め打ちしていない）。
#[test]
fn should_run_spanner_use_the_declared_retry_limit_and_fail_when_the_expectation_no_longer_holds() {
  let data = real_data();
  let declared = case_of(&data, "spanner-aborted-retry-limit-exceeded");
  assert!(
    is_passed(&execute(&data, declared)),
    "宣言どおりの上限 2 では、3 回の中断の後に保存先エラーになる場面が成功する（比較の基準）"
  );
  let mut raised = copy_of(declared);
  raised.body["store"]["transaction_retry_limit"] = json!(5);

  let outcome = execute(&data, &raised);

  assert!(
    is_failed_with_detail(&outcome),
    "上限を 5 に上げると 3 回の中断の後に成功するので、保存先エラーを期待する場面は失敗になる: {outcome:?}"
  );
}

#[test]
fn should_run_spanner_apply_the_declared_number_of_aborts_and_fail_when_it_exceeds_the_limit() {
  let data = real_data();
  let declared = case_of(&data, "spanner-aborted-retry-within-limit");
  assert!(
    is_passed(&execute(&data, declared)),
    "宣言どおりの中断の回数（2 回、上限 3）では、再試行の後に追記が成功する場面が成功する（比較の基準）"
  );
  let mut exceeded = copy_of(declared);
  exceeded.body["faults"][0]["repeat"]["count"] = json!(4);

  let outcome = execute(&data, &exceeded);

  assert!(
    is_failed_with_detail(&outcome),
    "上限 3 を超える 4 回の中断では追記が成功しないので、成功を期待する場面は失敗になる: {outcome:?}"
  );
}

#[test]
fn should_run_spanner_check_the_seeded_configuration_against_the_declared_store_settings() {
  let data = real_data();
  let declared = case_of(&data, "spanner-seeded-keep-count-mismatch");
  assert!(
    is_passed(&execute(&data, declared)),
    "宣言どおりの保持件数では、記録した設定との食い違いで設定エラーになる場面が成功する（比較の基準）"
  );
  let mut matching = copy_of(declared);
  matching.body["store"]["retention_count"] = json!(2);

  let outcome = execute(&data, &matching);

  assert!(
    is_failed_with_detail(&outcome),
    "保持件数を記録した 2 に合わせると生成が成功するので、設定エラーを期待する場面は失敗になる: {outcome:?}"
  );
}

#[test]
fn should_run_spanner_create_the_row_deletion_policy_from_the_declared_days() {
  let data = real_data();
  let declared = case_of(&data, "spanner-seeded-ttl-configuration-match");
  assert!(
    is_passed(&execute(&data, declared)),
    "宣言どおりの日数では、生成が成功する場面が成功する（比較の基準）"
  );
  let mut mismatching = copy_of(declared);
  mismatching.body["seed"]["row_deletion_policy_days"] = json!(8);

  let outcome = execute(&data, &mismatching);

  assert!(
    is_failed_with_detail(&outcome),
    "行削除ポリシーの日数を 8 にすると生成が設定エラーになるので、成功を期待する場面は失敗になる: {outcome:?}"
  );
}
