#![cfg(feature = "bigtable")]
//! Bigtable の実保存先（エミュレーター）に対する適合ケースの実行の試験。
//! 各場面は新しい journal・head のテーブルで実行し、ライブラリの公開操作と `test-hooks` の経路だけを使う。

use std::path::{Path, PathBuf};

use event_store_adapter_conformance_rs::data::{load, Case, CaseKind, DataSet};
use event_store_adapter_conformance_rs::report::{CaseOutcome, CaseReport, Implementation, Report};
use event_store_adapter_conformance_rs::runner::{run, run_case};
use event_store_adapter_conformance_rs::target_bigtable;
use serde_json::{json, Value};

/// Bigtable の場面だけが使うケース。
const BIGTABLE_ONLY_CASES: [&str; 12] = [
  "bigtable-ttl-unsupported",
  "bigtable-change-feed-unsupported",
  "bigtable-seeded-table-envelope-limit-mismatch",
  "bigtable-seeded-keep-count-mismatch",
  "bigtable-seeded-envelope-limit-mismatch",
  "bigtable-seeded-configuration-match",
  "bigtable-envelope-limit-above-maximum",
  "bigtable-envelope-limit-below-minimum",
  "bigtable-envelope-over-limit",
  "bigtable-previous-event-write-failure",
  "bigtable-new-tail-write-failure",
  "bigtable-cas-response-lost",
];

/// 封筒の符号化と復元を確かめる値の表のケース。
const ENVELOPE_VALUE_CASES: [&str; 10] = [
  "bigtable-envelope-event-empty",
  "bigtable-envelope-event-bytes",
  "bigtable-envelope-event-utf8",
  "bigtable-envelope-event-occurred-at-min",
  "bigtable-envelope-event-occurred-at-max",
  "bigtable-envelope-snapshot-empty",
  "bigtable-envelope-snapshot-utf8",
  "bigtable-envelope-event-trailing-bytes",
  "bigtable-envelope-event-length-mismatch",
  "bigtable-envelope-event-unknown-version",
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

fn run_bigtable(data: &DataSet) -> Vec<CaseReport> {
  run(data, &target_bigtable::TARGET, target_bigtable::run_case)
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
  run_case(
    case,
    &target_bigtable::TARGET,
    &data.coverage,
    target_bigtable::run_case,
  )
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

fn targets_bigtable(case: &Case) -> bool {
  case.kind == CaseKind::Scenario
    && case.body["backends"]
      .as_array()
      .is_some_and(|backends| backends.iter().any(|name| name == "bigtable"))
}

// ---------------------------------------------------------------------------
// 実データ全体
// ---------------------------------------------------------------------------

#[test]
fn should_run_reports_every_bigtable_targeted_case_as_passed() {
  let data = real_data();

  let reports = run_bigtable(&data);

  // (passed, failed, not-applicable, unverified)。成功 85 は、Bigtable が対象の場面 50 と値の表 35
  // （封筒の 10 件と、FNV-1a 64 の 4 件と、全ての保存先で実行する値の表 21）。対象外 84 の内訳は、runner_test の
  // 分類の試験が持つ。
  assert_eq!(counts(&reports), (85, 0, 84, 0));
  let mut targeted = 0;
  for case in data.cases.iter().filter(|case| targets_bigtable(case)) {
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
  assert_eq!(targeted, 55, "Bigtable が対象の場面は共通 42・bigtable/ 12・共有 1");
}

#[test]
fn should_run_bigtable_pass_the_twelve_cases_only_bigtable_targets_and_the_shared_scene() {
  let reports = run_bigtable(&real_data());

  for id in BIGTABLE_ONLY_CASES.iter().chain(["store-shared-persistent"].iter()) {
    assert!(
      is_passed(outcome_of(&reports, id)),
      "{id}: {:?}",
      outcome_of(&reports, id)
    );
  }
}

#[test]
fn should_run_bigtable_report_the_bt_rules_as_passed_without_failures() {
  let data = real_data();
  let reports = run_bigtable(&data);

  let report = Report::build(&data, "bigtable", reports, implementation());

  assert!(!report.should_fail(true), "--require-all でも成功する");
  let json = serde_json::to_value(&report).expect("報告を直列化できる");
  assert_eq!(json["backend"], "bigtable");
  // (passed, failed, not-applicable, unverified)
  assert_eq!(rule_counts(&json, "BT-7"), (3, 0, 0, 0));
  assert_eq!(rule_counts(&json, "BT-13"), (1, 0, 0, 0));
  assert_eq!(rule_counts(&json, "BT-16"), (6, 0, 0, 0));
  assert_eq!(rule_counts(&json, "BT-17"), (1, 0, 0, 0));
  assert_eq!(rule_counts(&json, "BT-D6a"), (10, 0, 0, 0));
  assert_eq!(rule_counts(&json, "K-1"), (4, 0, 0, 0));
}

#[test]
fn should_run_bigtable_pass_the_fnv1a64_cases_with_the_hash_the_row_keys_use() {
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
fn should_run_bigtable_fail_an_fnv1a64_case_whose_expected_value_differs_from_the_row_key_hash() {
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

#[test]
fn should_run_bigtable_keep_spanner_cases_as_not_provided() {
  let data = real_data();
  let reports = run_bigtable(&data);

  let not_provided: Vec<&CaseReport> = reports
    .iter()
    .filter(|report| kind_of_not_applicable(&report.outcome).as_deref() == Some("backend-not-provided"))
    .collect();

  assert_eq!(
    not_provided.len(),
    14,
    "{:?}",
    not_provided.iter().map(|r| &r.id).collect::<Vec<_>>()
  );
  assert!(not_provided.iter().all(|report| report.id.starts_with("spanner-")));
}

// ---------------------------------------------------------------------------
// 障害・通知・保持
// ---------------------------------------------------------------------------

#[test]
fn should_run_bigtable_inject_every_fault_of_the_common_scenes_without_leaving_unfired_faults() {
  let data = real_data();
  let reports = run_bigtable(&data);

  let with_faults: Vec<&Case> = data
    .cases
    .iter()
    .filter(|case| targets_bigtable(case) && case.body["faults"].as_array().is_some_and(|faults| !faults.is_empty()))
    .collect();

  assert_eq!(with_faults.len(), 14, "障害を持つ場面は共通 11 件と bigtable/ の 3 件");
  for case in with_faults {
    let outcome = outcome_of(&reports, &case.id);
    assert!(
      is_passed(outcome),
      "発火しなかった障害は失敗になる: {}: {outcome:?}",
      case.id
    );
  }
}

#[test]
fn should_run_bigtable_pass_the_journal_write_and_response_loss_cases() {
  let reports = run_bigtable(&real_data());

  for id in [
    "bigtable-previous-event-write-failure",
    "bigtable-new-tail-write-failure",
    "bigtable-cas-response-lost",
  ] {
    assert!(
      is_passed(outcome_of(&reports, id)),
      "{id}: {:?}",
      outcome_of(&reports, id)
    );
  }
}

#[test]
fn should_run_bigtable_observe_the_journal_write_failure_notification_of_the_new_tail_case() {
  let data = real_data();
  let case = case_of(&data, "bigtable-new-tail-write-failure");
  assert_eq!(
    case.body["steps"][1]["observe"]["notifications"],
    json!(["journal-write-failure"]),
    "試験の前提: 操作 2 が通知を観測する"
  );
  let mut silent = copy_of(case);
  silent.body["steps"][1]["observe"]["notifications"] = json!([]);

  let observed = execute(&data, case);
  let mismatched = execute(&data, &silent);

  assert!(is_passed(&observed), "{observed:?}");
  assert!(
    is_failed_with_detail(&mismatched),
    "通知が出ているのに空を期待するデータは失敗になる: {mismatched:?}"
  );
}

#[test]
fn should_run_bigtable_pass_the_retention_cases_without_changing_the_append_result() {
  let data = real_data();
  let reports = run_bigtable(&data);

  let retention: Vec<&CaseReport> = reports
    .iter()
    .filter(|report| report.id.starts_with("core-retention-"))
    .collect();

  assert_eq!(retention.len(), 6);
  for report in retention {
    assert!(is_passed(&report.outcome), "{}: {:?}", report.id, report.outcome);
  }
}

#[test]
fn should_run_bigtable_reject_journal_write_faults_in_a_scene_shared_with_another_backend() {
  let data = real_data();
  let original = case_of(&data, "bigtable-previous-event-write-failure");
  let mut case = copy_of(original);
  case.body["backends"] = json!(["bigtable", "memory"]);

  let outcome = execute(&data, &case);

  assert!(
    is_failed_with_detail(&outcome),
    "データの誤りとして拒否する: {outcome:?}"
  );
}

#[test]
fn should_run_bigtable_treat_a_response_loss_differently_from_a_request_replacement() {
  let data = real_data();
  let original = case_of(&data, "bigtable-cas-response-lost");
  assert_eq!(original.body["faults"][0]["injection"], "replace-response");
  let mut case = copy_of(original);
  case.body["faults"][0]["injection"] = json!("replace-request");

  let outcome = execute(&data, &case);

  assert!(
    is_failed_with_detail(&outcome),
    "要求の置き換えなら更新は確定せず、送り直しは楽観ロックにならない: {outcome:?}"
  );
}

// ---------------------------------------------------------------------------
// 設定・テーブル・共有
// ---------------------------------------------------------------------------

#[test]
fn should_run_bigtable_execute_unsupported_settings_cases_as_configuration_errors() {
  let reports = run_bigtable(&real_data());

  for id in ["bigtable-ttl-unsupported", "bigtable-change-feed-unsupported"] {
    assert!(
      is_passed(outcome_of(&reports, id)),
      "表現不能にせず、設定エラーとして実行して成功する: {id}: {:?}",
      outcome_of(&reports, id)
    );
  }
}

#[test]
fn should_run_bigtable_record_seed_configuration_after_initialization_and_before_the_store_is_opened() {
  let reports = run_bigtable(&real_data());

  for id in [
    "bigtable-seeded-table-envelope-limit-mismatch",
    "bigtable-seeded-keep-count-mismatch",
    "bigtable-seeded-envelope-limit-mismatch",
    "bigtable-seeded-configuration-match",
  ] {
    assert!(
      is_passed(outcome_of(&reports, id)),
      "{id}: {:?}",
      outcome_of(&reports, id)
    );
  }
}

#[test]
fn should_run_bigtable_apply_the_declared_envelope_limit() {
  let reports = run_bigtable(&real_data());

  for id in [
    "bigtable-envelope-over-limit",
    "bigtable-envelope-limit-above-maximum",
    "bigtable-envelope-limit-below-minimum",
  ] {
    assert!(
      is_passed(outcome_of(&reports, id)),
      "{id}: {:?}",
      outcome_of(&reports, id)
    );
  }
}

#[test]
fn should_run_bigtable_use_a_new_table_pair_for_every_scene() {
  let data = real_data();
  let case = case_of(&data, "bigtable-envelope-over-limit");

  let first = execute(&data, case);
  let second = execute(&data, case);

  assert!(is_passed(&first), "{first:?}");
  assert!(
    is_passed(&second),
    "同じ場面を続けて実行しても、前の場面の行が残らず成功する（テーブルを使い回さない）: {second:?}"
  );
}

#[test]
fn should_run_bigtable_share_one_table_pair_between_the_primary_and_the_second_store() {
  let data = real_data();

  let outcome = execute(&data, case_of(&data, "store-shared-persistent"));

  assert!(is_passed(&outcome), "{outcome:?}");
}

#[test]
fn should_run_bigtable_rejects_new_storage() {
  let data = real_data();
  let independent = case_of(&data, "memory-store-new-independent");
  assert_eq!(independent.body["steps"][1]["arguments"]["storage"], "new");
  let mut case = copy_of(independent);
  case.body["backends"] = json!(["bigtable"]);

  let outcome = execute(&data, &case);

  assert!(
    is_failed_with_detail(&outcome),
    "storage = new を成功にしない。データの誤りとして拒否する: {outcome:?}"
  );
}

// ---------------------------------------------------------------------------
// 封筒の値の表
// ---------------------------------------------------------------------------

#[test]
fn should_run_bigtable_pass_the_ten_envelope_value_cases() {
  let data = real_data();

  for id in ENVELOPE_VALUE_CASES {
    let outcome = execute(&data, case_of(&data, id));

    assert!(is_passed(&outcome), "{id}: {outcome:?}");
  }
}

#[test]
fn should_run_bigtable_reject_a_decode_case_whose_envelope_is_actually_valid() {
  let data = real_data();
  let original = case_of(&data, "bigtable-envelope-event-trailing-bytes");
  assert_eq!(original.body["operation"], "decodeBigtableEnvelope");
  let mut case = copy_of(original);
  case.body["input"]["bytes_hex"] = json!("0100074f726465722d39000000000000000100000000000000000000000000000000");

  let outcome = execute(&data, &case);

  assert!(
    is_failed_with_detail(&outcome),
    "復元に成功した封筒を、復元の失敗を期待するデータの誤りとして失敗にする: {outcome:?}"
  );
}

#[test]
fn should_run_bigtable_fail_an_encode_case_whose_expected_bytes_differ_from_the_library_output() {
  let data = real_data();
  let original = case_of(&data, "bigtable-envelope-event-bytes");
  assert_eq!(original.body["operation"], "encodeBigtableEventEnvelope");
  let mut case = copy_of(original);
  case.body["expect"]["value"] =
    json!("0100074f726465722d390000000000000002000000003b9aca000000000276310000000600010203feee");

  let outcome = execute(&data, &case);

  assert!(
    is_failed_with_detail(&outcome),
    "実行器の中の独自の符号化ではなく、ライブラリの出力と比べる: {outcome:?}"
  );
}
