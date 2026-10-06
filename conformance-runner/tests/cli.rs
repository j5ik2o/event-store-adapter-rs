//! コマンドの入口（引数・報告の書き出し・終了コード）の試験。
//! 実行ファイルを起動して、利用者から見える結果だけを確かめる。保存先には接続しない。

mod support;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::Value;
use support::{conformance_dir, copy_dir_all, TempDir};

const FIXED_MANIFEST_SHA256: &str = "61c26614dbbfba88eebce72cc1d2b0220218839e74dcfb64c19268f7ee2302ce";

/// 実行器を、与えた引数のまま起動する。
fn launch(args: &[&str]) -> Output {
  Command::new(env!("CARGO_BIN_EXE_event-store-adapter-conformance-rs"))
    .args(args)
    .output()
    .expect("実行器を起動できる")
}

/// 実行ファイルの起動結果と、書き出された（はずの）報告の場所を持つ。
struct Execution {
  output: Output,
  report_path: PathBuf,
  _directory: TempDir,
}

impl Execution {
  fn code(&self) -> Option<i32> {
    self.output.status.code()
  }

  fn report(&self) -> Value {
    let text = fs::read_to_string(&self.report_path).expect("報告が書かれている");
    serde_json::from_str(&text).expect("報告は JSON")
  }

  fn report_exists(&self) -> bool {
    self.report_path.exists()
  }
}

fn execute(label: &str, data: &Path, extra_args: &[&str]) -> Execution {
  let directory = TempDir::new(label);
  let report_path = directory.path().join("report.json");
  let data = data.to_string_lossy();
  let report = report_path.to_string_lossy();
  let mut args = vec!["--data", &data, "--report", &report];
  args.extend_from_slice(extra_args);
  Execution {
    output: launch(&args),
    report_path,
    _directory: directory,
  }
}

fn execute_on_real_data(label: &str, extra_args: &[&str]) -> Execution {
  execute(label, &conformance_dir(), extra_args)
}

fn count_status(report: &Value, status: &str) -> usize {
  report["cases"]
    .as_array()
    .expect("cases は配列")
    .iter()
    .filter(|case| case["status"] == status)
    .count()
}

// ---------------------------------------------------------------------------
// 実データを全ケース未検証か対象外で報告する
// ---------------------------------------------------------------------------

#[test]
fn test_cli_exits_zero_and_passes_manifest_verification_for_memory_backend() {
  let execution = execute_on_real_data("cli-memory-exit", &["--backend", "memory"]);

  assert_eq!(
    execution.code(),
    Some(0),
    "stderr: {}",
    String::from_utf8_lossy(&execution.output.stderr)
  );
  assert_eq!(execution.report()["data"]["manifest"]["verification"], "passed");
}

#[test]
fn test_cli_reports_every_case_of_memory_backend_without_any_success() {
  let execution = execute_on_real_data("cli-memory-counts", &["--backend", "memory"]);

  let report = execution.report();
  assert_eq!(report["cases"].as_array().expect("cases は配列").len(), 116);
  assert_eq!(count_status(&report, "passed"), 0);
  assert_eq!(count_status(&report, "failed"), 0);
  assert_eq!(
    count_status(&report, "not-applicable") + count_status(&report, "unverified"),
    116
  );
}

#[test]
fn test_cli_gives_every_not_applicable_and_unverified_case_a_reason_of_a_known_kind() {
  let execution = execute_on_real_data("cli-memory-reasons", &["--backend", "memory"]);

  let report = execution.report();
  for case in report["cases"].as_array().expect("cases は配列") {
    let kind = case["reason"]["kind"]
      .as_str()
      .unwrap_or_else(|| panic!("理由がある: {case}"));
    match case["status"].as_str().expect("status は文字列") {
      "not-applicable" => assert!(
        [
          "backend-not-targeted",
          "representation",
          "fnv1a64-decision",
          "coverage-exclusion"
        ]
        .contains(&kind),
        "{case}"
      ),
      "unverified" => assert!(
        ["unimplemented-constraint-words", "not-executed"].contains(&kind),
        "{case}"
      ),
      other => panic!("成功にも失敗にもならない: {other} {case}"),
    }
  }
}

#[test]
fn test_cli_report_names_backend_data_version_and_implementation() {
  let execution = execute_on_real_data("cli-memory-identity", &["--backend", "memory"]);

  let report = execution.report();
  assert_eq!(report["backend"], "memory");
  assert_eq!(report["data"]["version"], "1.0.0");
  assert_eq!(report["data"]["manifest"]["sha256"], FIXED_MANIFEST_SHA256);
  assert_eq!(report["implementation"]["language"], "rust");
  assert_eq!(report["implementation"]["crate"], "event-store-adapter-rs");
  assert!(report["implementation"]["version"].is_string());
}

#[test]
fn test_cli_accepts_dynamodb_backend_and_reports_without_connecting() {
  let execution = execute_on_real_data("cli-dynamodb", &["--backend", "dynamodb"]);

  assert_eq!(
    execution.code(),
    Some(0),
    "stderr: {}",
    String::from_utf8_lossy(&execution.output.stderr)
  );
  let report = execution.report();
  assert_eq!(report["backend"], "dynamodb");
  assert_eq!(count_status(&report, "passed"), 0);
  assert_eq!(
    count_status(&report, "not-applicable") + count_status(&report, "unverified"),
    116
  );
}

// ---------------------------------------------------------------------------
// 終了コード
// ---------------------------------------------------------------------------

#[test]
fn test_cli_with_require_all_exits_with_failure_because_cases_are_unverified() {
  let execution = execute_on_real_data("cli-require-all", &["--backend", "memory", "--require-all"]);

  assert_eq!(execution.code(), Some(1));
  assert_eq!(count_status(&execution.report(), "passed"), 0);
}

#[test]
fn test_cli_exits_with_failure_when_manifest_verification_fails() {
  let copy = TempDir::new("cli-modified-data");
  copy_dir_all(&conformance_dir(), copy.path());
  let target = copy.path().join("values/aid.json");
  let mut bytes = fs::read(&target).expect("写した values/aid.json を読める");
  bytes.push(b' ');
  fs::write(&target, bytes).expect("1 バイト足せる");

  let execution = execute("cli-modified-run", copy.path(), &["--backend", "memory"]);

  assert_eq!(execution.code(), Some(1));
  let report = execution.report();
  assert_eq!(report["data"]["manifest"]["verification"], "failed");
  assert!(report["data"]["manifest"]["problems"]
    .to_string()
    .contains("values/aid.json"));
}

#[test]
fn test_cli_fails_when_report_cannot_be_written() {
  let directory = TempDir::new("cli-unwritable-report");
  let report_path = directory.path().join("no-such-directory").join("report.json");

  let data = conformance_dir();

  let output = launch(&[
    "--backend",
    "memory",
    "--data",
    &data.to_string_lossy(),
    "--report",
    &report_path.to_string_lossy(),
  ]);

  assert_eq!(output.status.code(), Some(2));
}

#[test]
fn test_cli_fails_without_writing_report_when_data_cannot_be_read() {
  let missing = TempDir::new("cli-missing-data");

  let execution = execute(
    "cli-missing-data-run",
    &missing.path().join("not-there"),
    &["--backend", "memory"],
  );

  assert_eq!(execution.code(), Some(2));
  assert!(!execution.report_exists());
}

// ---------------------------------------------------------------------------
// 引数の誤り
// ---------------------------------------------------------------------------

#[test]
fn test_cli_rejects_missing_backend() {
  let data = conformance_dir();
  let directory = TempDir::new("cli-no-backend");
  let report = directory.path().join("report.json");

  let output = launch(&["--data", &data.to_string_lossy(), "--report", &report.to_string_lossy()]);

  assert_eq!(output.status.code(), Some(2));
  assert!(!report.exists());
}

#[test]
fn test_cli_rejects_missing_data() {
  let directory = TempDir::new("cli-no-data");
  let report = directory.path().join("report.json");

  let output = launch(&["--backend", "memory", "--report", &report.to_string_lossy()]);

  assert_eq!(output.status.code(), Some(2));
  assert!(!report.exists());
}

#[test]
fn test_cli_rejects_missing_report() {
  let output = launch(&["--backend", "memory", "--data", &conformance_dir().to_string_lossy()]);

  assert_eq!(output.status.code(), Some(2));
}

#[test]
fn test_cli_rejects_option_without_value() {
  let output = launch(&[
    "--backend",
    "memory",
    "--data",
    &conformance_dir().to_string_lossy(),
    "--report",
  ]);

  assert_eq!(output.status.code(), Some(2));
}

#[test]
fn test_cli_rejects_unknown_argument() {
  let execution = execute_on_real_data("cli-unknown-argument", &["--backend", "memory", "--unknown-option"]);

  assert_eq!(execution.code(), Some(2));
  assert!(!execution.report_exists(), "不明な引数のまま実行しない");
}

#[test]
fn test_cli_rejects_unknown_backend() {
  let execution = execute_on_real_data("cli-unknown-backend", &["--backend", "sqlite"]);

  assert_eq!(execution.code(), Some(2));
  assert!(!execution.report_exists());
}
