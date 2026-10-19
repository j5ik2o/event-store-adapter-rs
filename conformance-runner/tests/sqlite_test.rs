#![cfg(feature = "sqlite")]
//! SQLite の実保存先に対する適合ケースの実行の試験。
//! 各場面は新しい一時ファイルで実行し、ライブラリの公開操作と `test-hooks` の経路だけを使う。

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use event_store_adapter_conformance_rs::data::{load, Case, CaseKind, DataSet};
use event_store_adapter_conformance_rs::report::{CaseOutcome, CaseReport, Implementation, Report};
use event_store_adapter_conformance_rs::runner::{run, run_case};
use event_store_adapter_conformance_rs::target_sqlite;
use serde_json::{json, Value};

/// SQLite の場面だけが使うケース。
const SQLITE_ONLY_CASES: [&str; 8] = [
  "sqlite-ttl-unsupported",
  "sqlite-change-feed-unsupported",
  "sqlite-seeded-keep-count-mismatch",
  "sqlite-seeded-layout-version-mismatch",
  "sqlite-seeded-change-feed-enabled",
  "sqlite-seeded-configuration-match",
  "sqlite-write-lock-busy",
  "store-shared-persistent",
];

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

fn run_sqlite(data: &DataSet) -> Vec<CaseReport> {
  run(data, &target_sqlite::TARGET, target_sqlite::run_case)
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
  run_case(case, &target_sqlite::TARGET, &data.coverage, target_sqlite::run_case)
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

// ---------------------------------------------------------------------------
// 実データ全体（RUNNER-ALL・RUNNER-FAULTS・RUNNER-FEED-SETTING・RUNNER-SEED・RUNNER-SHARED・RUNNER-LOCK-WAIT）
// ---------------------------------------------------------------------------

#[test]
fn should_run_reports_every_sqlite_targeted_case_as_passed() {
  let data = real_data();

  let reports = run_sqlite(&data);

  // (passed, failed, not-applicable, unverified)。成功 66 は、SQLite が対象の場面 45 と値の表 21。
  assert_eq!(counts(&reports), (66, 0, 103, 0));
  let mut targeted = 0;
  for case in data.cases.iter().filter(|case| {
    case.kind == CaseKind::Scenario
      && case.body["backends"]
        .as_array()
        .is_some_and(|backends| backends.iter().any(|name| name == "sqlite"))
  }) {
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
  assert_eq!(targeted, 50, "SQLite が対象の場面は共通 42・sqlite/ 7・共有 1");
}

#[test]
fn should_run_sqlite_pass_the_eight_cases_only_sqlite_targets() {
  let reports = run_sqlite(&real_data());

  for id in SQLITE_ONLY_CASES {
    assert!(
      is_passed(outcome_of(&reports, id)),
      "{id}: {:?}",
      outcome_of(&reports, id)
    );
  }
}

#[test]
fn should_run_sqlite_report_the_sq_rules_as_passed_without_failures() {
  let data = real_data();
  let reports = run_sqlite(&data);

  let report = Report::build(&data, "sqlite", reports, implementation());

  assert!(!report.should_fail(true), "--require-all でも成功する");
  let json = serde_json::to_value(&report).expect("報告を直列化できる");
  assert_eq!(json["backend"], "sqlite");
  // (passed, failed, not-applicable, unverified)
  assert_eq!(rule_counts(&json, "SQ-2"), (3, 0, 0, 0));
  assert_eq!(rule_counts(&json, "SQ-7"), (1, 0, 0, 0));
  assert_eq!(rule_counts(&json, "SQ-11"), (1, 0, 0, 0));
  assert_eq!(rule_counts(&json, "SQ-19"), (2, 0, 0, 0));
  assert_eq!(rule_counts(&json, "SQ-20"), (2, 0, 0, 0));
}

#[test]
fn should_run_sqlite_keep_spanner_and_bigtable_cases_as_not_provided() {
  let data = real_data();
  let reports = run_sqlite(&data);

  let not_provided: Vec<&CaseReport> = reports
    .iter()
    .filter(|report| kind_of_not_applicable(&report.outcome).as_deref() == Some("backend-not-provided"))
    .collect();

  // Spanner の場面 14・Bigtable の場面 12・Bigtable の封筒の値の表 10。
  assert_eq!(
    not_provided.len(),
    36,
    "{:?}",
    not_provided.iter().map(|r| &r.id).collect::<Vec<_>>()
  );
  for report in not_provided {
    assert!(
      report.id.starts_with("spanner-") || report.id.starts_with("bigtable-"),
      "Spanner・Bigtable 以外が提供しない扱いになっている: {}",
      report.id
    );
  }
}

#[test]
fn should_run_sqlite_inject_every_fault_of_the_common_scenes_without_leaving_unfired_faults() {
  let data = real_data();
  let reports = run_sqlite(&data);

  let with_faults: Vec<&Case> = data
    .cases
    .iter()
    .filter(|case| {
      case.kind == CaseKind::Scenario
        && case.body["backends"]
          .as_array()
          .is_some_and(|backends| backends.iter().any(|name| name == "sqlite"))
        && case.body["faults"].as_array().is_some_and(|faults| !faults.is_empty())
    })
    .collect();

  assert_eq!(with_faults.len(), 11, "障害を持つ共通の場面は 11 件");
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
fn should_run_sqlite_execute_unsupported_settings_cases_as_configuration_errors() {
  let reports = run_sqlite(&real_data());

  for id in ["sqlite-ttl-unsupported", "sqlite-change-feed-unsupported"] {
    assert!(
      is_passed(outcome_of(&reports, id)),
      "表現不能にせず、設定エラーとして実行して成功する: {id}: {:?}",
      outcome_of(&reports, id)
    );
  }
}

#[test]
fn should_run_sqlite_record_seed_configuration_after_initialization_and_before_the_store_is_opened() {
  let reports = run_sqlite(&real_data());

  for id in [
    "sqlite-seeded-keep-count-mismatch",
    "sqlite-seeded-layout-version-mismatch",
    "sqlite-seeded-change-feed-enabled",
    "sqlite-seeded-configuration-match",
  ] {
    assert!(
      is_passed(outcome_of(&reports, id)),
      "{id}: {:?}",
      outcome_of(&reports, id)
    );
  }
}

#[test]
fn should_run_sqlite_pass_the_write_lock_case_within_the_declared_lock_wait() {
  let data = real_data();
  let case = case_of(&data, "sqlite-write-lock-busy");
  assert_eq!(case.body["store"]["lock_wait_milliseconds"], 100);

  let started = Instant::now();
  let outcome = execute(&data, case);
  let elapsed = started.elapsed();

  assert!(is_passed(&outcome), "{outcome:?}");
  assert!(
    elapsed < Duration::from_secs(3),
    "宣言した 100 ミリ秒の待ち時間で失敗する。既定の待ち時間で待っていない: {elapsed:?}"
  );
}

#[test]
fn should_run_sqlite_share_one_file_between_the_primary_and_the_second_store() {
  let data = real_data();

  let outcome = execute(&data, case_of(&data, "store-shared-persistent"));

  assert!(is_passed(&outcome), "{outcome:?}");
}

// ---------------------------------------------------------------------------
// データの誤りの拒否（RUNNER-WRITE-LOCK・RUNNER-SHARED）
// ---------------------------------------------------------------------------

#[test]
fn should_run_sqlite_rejects_write_lock_data_errors() {
  let data = real_data();
  let original = case_of(&data, "sqlite-write-lock-busy");
  let steps = original.body["steps"].as_array().expect("steps は配列").clone();
  // 元の手順: 0 追記 e1、1 ロックを持つ、2 追記 e2（保存先エラー）、3 ロックを放す、4 追記 e2、5 読み取り。
  assert_eq!(steps[1]["op"], "holdWriteLock");
  assert_eq!(steps[3]["op"], "releaseWriteLock");
  let pick = |indexes: &[usize]| -> Value { Value::Array(indexes.iter().map(|&index| steps[index].clone()).collect()) };
  let variants: [(&str, Option<Value>, Value); 5] = [
    ("持っていないロックを放す", None, pick(&[0, 3])),
    (
      "すでに持っているロックをもう一度持つ",
      None,
      pick(&[0, 1, 1, 2, 3, 4, 5]),
    ),
    ("ロックを放さないまま終わる", None, pick(&[0, 1, 2])),
    (
      "ロックを持つ間に保存先エラーを期待する追記がない",
      None,
      pick(&[0, 1, 3]),
    ),
    (
      "backends が sqlite だけでない場面で使う",
      Some(json!(["sqlite", "memory"])),
      pick(&[0, 1, 2, 3, 4, 5]),
    ),
  ];

  assert!(is_passed(&execute(&data, original)), "試験の前提: 元のケースは成功する");
  for (label, backends, steps) in variants {
    let mut case = copy_of(original);
    case.body["steps"] = steps;
    if let Some(backends) = backends {
      case.body["backends"] = backends;
    }

    let outcome = execute(&data, &case);

    assert!(
      matches!(&outcome, CaseOutcome::Failed { detail, .. } if !detail.is_empty()),
      "データの誤りとして拒否する: {label}: {outcome:?}"
    );
  }
}

#[test]
fn should_run_sqlite_rejects_new_storage() {
  let data = real_data();
  let independent = case_of(&data, "memory-store-new-independent");
  assert_eq!(independent.body["steps"][1]["arguments"]["storage"], "new");
  let mut case = copy_of(independent);
  case.body["backends"] = json!(["sqlite"]);

  let outcome = execute(&data, &case);

  assert!(
    matches!(&outcome, CaseOutcome::Failed { detail, .. } if !detail.is_empty()),
    "storage = new を成功にしない。データの誤りとして拒否する: {outcome:?}"
  );
}
