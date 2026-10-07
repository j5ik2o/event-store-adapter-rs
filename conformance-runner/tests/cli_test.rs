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

/// 実行器の起動時に渡す環境変数と作業フォルダーの指定を表す。
#[derive(Default)]
struct Environment<'a> {
  set: &'a [(&'a str, &'a str)],
  removed: &'a [&'a str],
  /// 実行器を起動するときの作業フォルダー。なければ、この試験を起動したフォルダーのまま。
  current_dir: Option<&'a Path>,
}

fn execute(label: &str, data: &Path, extra_args: &[&str]) -> Execution {
  execute_with_environment(label, data, extra_args, &Environment::default())
}

fn execute_with_environment(label: &str, data: &Path, extra_args: &[&str], environment: &Environment) -> Execution {
  let directory = TempDir::new(label);
  let report_path = directory.path().join("report.json");
  let data = data.to_string_lossy();
  let report = report_path.to_string_lossy();
  let mut args = vec!["--data", &data, "--report", &report];
  args.extend_from_slice(extra_args);
  let mut command = Command::new(env!("CARGO_BIN_EXE_event-store-adapter-conformance-rs"));
  command.args(&args);
  for (name, value) in environment.set {
    command.env(name, value);
  }
  for name in environment.removed {
    command.env_remove(name);
  }
  if let Some(directory) = environment.current_dir {
    command.current_dir(directory);
  }
  Execution {
    output: command.output().expect("実行器を起動できる"),
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
// 実データを、値の表の buildAid の成功と、ほかのケースの未検証か対象外で報告する
// ---------------------------------------------------------------------------

#[test]
fn should_cli_exits_zero_and_passes_manifest_verification_for_memory_backend() {
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
fn should_cli_reports_every_case_of_memory_backend_with_the_build_aid_success() {
  let execution = execute_on_real_data("cli-memory-counts", &["--backend", "memory"]);

  let report = execution.report();
  assert_eq!(report["cases"].as_array().expect("cases は配列").len(), 116);
  assert_eq!(count_status(&report, "passed"), 9);
  assert_eq!(count_status(&report, "failed"), 0);
  assert_eq!(
    count_status(&report, "passed") + count_status(&report, "not-applicable") + count_status(&report, "unverified"),
    116
  );
}

#[test]
fn should_cli_gives_every_not_applicable_and_unverified_case_a_reason_of_a_known_kind() {
  let execution = execute_on_real_data("cli-memory-reasons", &["--backend", "memory"]);

  let report = execution.report();
  for case in report["cases"].as_array().expect("cases は配列") {
    match case["status"].as_str().expect("status は文字列") {
      // 値の表の buildAid は成功するので、理由を持たない。
      "passed" => continue,
      "not-applicable" => {
        let kind = case["reason"]["kind"]
          .as_str()
          .unwrap_or_else(|| panic!("理由がある: {case}"));
        assert!(
          [
            "backend-not-targeted",
            "representation",
            "fnv1a64-decision",
            "coverage-exclusion"
          ]
          .contains(&kind),
          "{case}"
        );
      }
      "unverified" => {
        let kind = case["reason"]["kind"]
          .as_str()
          .unwrap_or_else(|| panic!("理由がある: {case}"));
        assert!(
          ["unimplemented-constraint-words", "not-executed"].contains(&kind),
          "{case}"
        );
      }
      other => panic!("成功にも失敗にもならない: {other} {case}"),
    }
  }
}

#[test]
fn should_cli_report_names_backend_data_version_and_implementation() {
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
fn should_cli_accepts_dynamodb_backend_and_reports_without_connecting() {
  let execution = execute_on_real_data("cli-dynamodb", &["--backend", "dynamodb"]);

  assert_eq!(
    execution.code(),
    Some(0),
    "stderr: {}",
    String::from_utf8_lossy(&execution.output.stderr)
  );
  let report = execution.report();
  assert_eq!(report["backend"], "dynamodb");
  assert_eq!(count_status(&report, "passed"), 9);
  assert_eq!(
    count_status(&report, "passed") + count_status(&report, "not-applicable") + count_status(&report, "unverified"),
    116
  );
}

// ---------------------------------------------------------------------------
// 終了コード
// ---------------------------------------------------------------------------

#[test]
fn should_cli_with_require_all_exits_with_failure_because_cases_are_unverified() {
  let execution = execute_on_real_data("cli-require-all", &["--backend", "memory", "--require-all"]);

  assert_eq!(execution.code(), Some(1));
  assert_eq!(count_status(&execution.report(), "passed"), 9);
}

#[test]
fn should_cli_exits_with_failure_when_manifest_verification_fails() {
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
fn should_cli_fails_when_report_cannot_be_written() {
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
fn should_cli_fails_without_writing_report_when_data_cannot_be_read() {
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
fn should_cli_rejects_missing_backend() {
  let data = conformance_dir();
  let directory = TempDir::new("cli-no-backend");
  let report = directory.path().join("report.json");

  let output = launch(&["--data", &data.to_string_lossy(), "--report", &report.to_string_lossy()]);

  assert_eq!(output.status.code(), Some(2));
  assert!(!report.exists());
}

#[test]
fn should_cli_rejects_missing_data() {
  let directory = TempDir::new("cli-no-data");
  let report = directory.path().join("report.json");

  let output = launch(&["--backend", "memory", "--report", &report.to_string_lossy()]);

  assert_eq!(output.status.code(), Some(2));
  assert!(!report.exists());
}

#[test]
fn should_cli_rejects_missing_report() {
  let output = launch(&["--backend", "memory", "--data", &conformance_dir().to_string_lossy()]);

  assert_eq!(output.status.code(), Some(2));
}

#[test]
fn should_cli_rejects_option_without_value() {
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
fn should_cli_rejects_unknown_argument() {
  let execution = execute_on_real_data("cli-unknown-argument", &["--backend", "memory", "--unknown-option"]);

  assert_eq!(execution.code(), Some(2));
  assert!(!execution.report_exists(), "不明な引数のまま実行しない");
}

#[test]
fn should_cli_rejects_unknown_backend() {
  let execution = execute_on_real_data("cli-unknown-backend", &["--backend", "sqlite"]);

  assert_eq!(execution.code(), Some(2));
  assert!(!execution.report_exists());
}

// ---------------------------------------------------------------------------
// スキーマの違反は実行器のエラー（終了コード 2・報告なし）
// ---------------------------------------------------------------------------

#[test]
fn should_cli_fails_without_writing_report_when_a_data_file_violates_its_schema() {
  let copy = TempDir::new("cli-schema-violation");
  copy_dir_all(&conformance_dir(), copy.path());
  let target = copy.path().join("coverage.json");
  let text = fs::read_to_string(&target).expect("写した coverage.json を読める");
  let mut value: Value = serde_json::from_str(&text).expect("JSON");
  value.as_object_mut().expect("オブジェクト").remove("notes");
  fs::write(&target, value.to_string()).expect("書き戻せる");

  let execution = execute("cli-schema-violation-run", copy.path(), &["--backend", "memory"]);

  assert_eq!(execution.code(), Some(2));
  assert!(!execution.report_exists(), "スキーマの違反では報告を書かない");
  let stderr = String::from_utf8_lossy(&execution.output.stderr);
  assert!(
    stderr.contains("coverage.json") && stderr.contains("スキーマ"),
    "stderr: {stderr}"
  );
}

// ---------------------------------------------------------------------------
// manifest の版は読んだ値を報告し、期待する版との比較は別に行う
// ---------------------------------------------------------------------------

/// 配布データの写しの `manifest.json` を書き換える。
fn copy_with_manifest(label: &str, edit: impl FnOnce(&mut Value)) -> TempDir {
  let copy = TempDir::new(label);
  copy_dir_all(&conformance_dir(), copy.path());
  let target = copy.path().join("manifest.json");
  let text = fs::read_to_string(&target).expect("写した manifest.json を読める");
  let mut value: Value = serde_json::from_str(&text).expect("JSON");
  edit(&mut value);
  fs::write(&target, serde_json::to_string_pretty(&value).expect("書き戻せる")).expect("書き込める");
  copy
}

#[test]
fn should_cli_reports_the_manifest_version_it_read_and_fails_the_comparison_separately() {
  let copy = copy_with_manifest("cli-manifest-version", |manifest| {
    manifest["version"] = Value::from("9.9.9")
  });

  let execution = execute("cli-manifest-version-run", copy.path(), &["--backend", "memory"]);

  assert_eq!(execution.code(), Some(1), "照合の失敗は報告を書いて終了コード 1");
  let report = execution.report();
  assert_eq!(report["data"]["version"], "9.9.9");
  assert_eq!(report["data"]["manifest"]["verification"], "failed");
  assert!(report["data"]["manifest"]["problems"].to_string().contains("9.9.9"));
}

#[test]
fn should_cli_reports_unknown_manifest_field_as_a_failed_verification() {
  let copy = copy_with_manifest("cli-manifest-unknown-field", |manifest| {
    manifest["extra"] = Value::from(true);
  });

  let execution = execute("cli-manifest-unknown-field-run", copy.path(), &["--backend", "memory"]);

  assert_eq!(execution.code(), Some(1), "照合の失敗は報告を書いて終了コード 1");
  let report = execution.report();
  assert_eq!(report["data"]["version"], "1.0.0");
  assert_eq!(report["data"]["manifest"]["verification"], "failed");
  let problems = report["data"]["manifest"]["problems"].to_string();
  assert!(
    problems.contains("未知のフィールド") && problems.contains("extra"),
    "{problems}"
  );
  assert_eq!(report["cases"].as_array().expect("cases は配列").len(), 116);
}

// ---------------------------------------------------------------------------
// 一覧に正しい形で載っていないファイルは、読まずに照合の不一致として報告する
// ---------------------------------------------------------------------------

/// 報告が書かれ、照合が失敗し、`problem_path` が問題に載り、116 ケースのうち値の表の `buildAid` の 9 件だけが成功していることを確かめる。
fn assert_reported_as_failed_verification(execution: &Execution, problem_path: &str) {
  let stderr = String::from_utf8_lossy(&execution.output.stderr);
  assert_eq!(
    execution.code(),
    Some(1),
    "照合の失敗は報告を書いて終了コード 1: {stderr}"
  );
  assert!(execution.report_exists(), "報告を書く: {stderr}");
  let report = execution.report();
  assert_eq!(report["data"]["manifest"]["verification"], "failed");
  let problems = report["data"]["manifest"]["problems"].to_string();
  assert!(problems.contains(problem_path), "{problems}");
  assert_eq!(report["cases"].as_array().expect("cases は配列").len(), 116);
  assert_eq!(count_status(&report, "passed"), 9);
}

#[test]
fn should_cli_reports_an_unlisted_json_in_the_schema_directory_as_a_failed_verification() {
  let copy = TempDir::new("cli-unlisted-schema");
  copy_dir_all(&conformance_dir(), copy.path());
  // `$id` がない。スキーマとして登録していれば、実行器のエラーになって報告が出ない。
  fs::write(copy.path().join("schema").join("note.json"), r#"{"title": "no id"}"#).expect("一覧にないスキーマを書ける");

  let execution = execute("cli-unlisted-schema-run", copy.path(), &["--backend", "memory"]);

  assert_reported_as_failed_verification(&execution, "schema/note.json");
}

#[test]
fn should_cli_reports_an_entry_without_a_string_sha256_as_a_failed_verification() {
  let copy = copy_with_manifest("cli-manifest-non-string-sha256", |manifest| {
    manifest["files"]
      .as_array_mut()
      .expect("files は配列")
      .push(serde_json::json!({"path": "values/bad.json", "sha256": 42}));
  });
  // 壊れた JSON。ケースとして読んでいれば、実行器のエラーになって報告が出ない。
  fs::write(copy.path().join("values").join("bad.json"), "{").expect("壊れたファイルを書ける");

  let execution = execute(
    "cli-manifest-non-string-sha256-run",
    copy.path(),
    &["--backend", "memory"],
  );

  assert_reported_as_failed_verification(&execution, "values/bad.json");
}

// ---------------------------------------------------------------------------
// 実装のコミット（git が先、なければ GITHUB_SHA）
// ---------------------------------------------------------------------------

const CI_SHA: &str = "cafebabecafebabecafebabecafebabecafebabe";

/// 実装（この crate）の作業ツリーの `git rev-parse HEAD` を返す。
///
/// 試験を起動したフォルダーに左右されないよう、この crate のフォルダーで動かす。
fn git_head() -> String {
  git_head_of(Path::new(env!("CARGO_MANIFEST_DIR")))
}

/// `directory` で動かした `git rev-parse HEAD` を返す。
fn git_head_of(directory: &Path) -> String {
  let output = Command::new("git")
    .current_dir(directory)
    .args(["rev-parse", "HEAD"])
    .output()
    .expect("git を起動できる");
  assert!(output.status.success(), "{} は git の作業ツリー", directory.display());
  String::from_utf8_lossy(&output.stdout).trim().to_string()
}

/// `directory` に、空のコミットを 1 つ持つ別の git リポジトリを作る。
fn init_repository_with_one_commit(directory: &Path) {
  let run = |args: &[&str]| {
    let status = Command::new("git")
      .current_dir(directory)
      .args(args)
      // 試験を起動した git の環境が、作ったリポジトリの操作へ入り込まないようにする。
      .env_remove("GIT_DIR")
      .env_remove("GIT_WORK_TREE")
      .env_remove("GIT_INDEX_FILE")
      .status()
      .expect("git を起動できる");
    assert!(status.success(), "git {args:?}");
  };
  run(&["init", "-q"]);
  run(&[
    "-c",
    "user.name=conformance-runner-test",
    "-c",
    "user.email=conformance-runner-test@example.invalid",
    "-c",
    "commit.gpgsign=false",
    "commit",
    "-q",
    "--allow-empty",
    "--no-verify",
    "-m",
    "a commit of a repository that is not the implementation",
  ]);
}

fn revision_of(execution: &Execution) -> Value {
  execution.report()["implementation"]["revision"].clone()
}

#[test]
fn should_cli_reports_the_git_commit_as_the_revision() {
  let environment = Environment {
    removed: &["GITHUB_SHA"],
    ..Environment::default()
  };

  let execution = execute_with_environment(
    "cli-revision-git",
    &conformance_dir(),
    &["--backend", "memory"],
    &environment,
  );

  let head = git_head();
  assert_eq!(head.len(), 40);
  assert!(head.bytes().all(|byte| byte.is_ascii_hexdigit()));
  assert_eq!(revision_of(&execution), Value::from(head));
}

#[test]
fn should_cli_prefers_the_git_commit_over_github_sha() {
  let environment = Environment {
    set: &[("GITHUB_SHA", CI_SHA)],
    ..Environment::default()
  };

  let execution = execute_with_environment(
    "cli-revision-git-first",
    &conformance_dir(),
    &["--backend", "memory"],
    &environment,
  );

  assert_eq!(revision_of(&execution), Value::from(git_head()));
}

#[test]
fn should_cli_falls_back_to_github_sha_when_git_gives_no_commit() {
  let not_a_repository = TempDir::new("cli-revision-no-git-dir");
  let git_dir = not_a_repository.path().to_string_lossy().into_owned();
  let environment = Environment {
    set: &[("GIT_DIR", &git_dir), ("GITHUB_SHA", CI_SHA)],
    ..Environment::default()
  };

  let execution = execute_with_environment(
    "cli-revision-github-sha",
    &conformance_dir(),
    &["--backend", "memory"],
    &environment,
  );

  assert_eq!(execution.code(), Some(0));
  assert_eq!(revision_of(&execution), Value::from(CI_SHA));
}

#[test]
fn should_cli_leaves_the_revision_null_without_git_and_github_sha() {
  let not_a_repository = TempDir::new("cli-revision-none-git-dir");
  let git_dir = not_a_repository.path().to_string_lossy().into_owned();
  let environment = Environment {
    set: &[("GIT_DIR", &git_dir)],
    removed: &["GITHUB_SHA"],
    ..Environment::default()
  };

  let execution = execute_with_environment(
    "cli-revision-none",
    &conformance_dir(),
    &["--backend", "memory"],
    &environment,
  );

  assert_eq!(execution.code(), Some(0));
  assert!(revision_of(&execution).is_null());
}

#[test]
fn should_cli_reports_the_implementation_commit_when_launched_inside_another_git_repository() {
  let other_repository = TempDir::new("cli-revision-other-repository");
  init_repository_with_one_commit(other_repository.path());
  let other_head = git_head_of(other_repository.path());
  let environment = Environment {
    removed: &["GITHUB_SHA"],
    current_dir: Some(other_repository.path()),
    ..Environment::default()
  };

  let execution = execute_with_environment(
    "cli-revision-other-repository-run",
    &conformance_dir(),
    &["--backend", "memory"],
    &environment,
  );

  assert_eq!(execution.code(), Some(0));
  assert_ne!(git_head(), other_head, "試験の前提: 2 つのリポジトリの HEAD は異なる");
  assert_ne!(revision_of(&execution), Value::from(other_head));
  assert_eq!(revision_of(&execution), Value::from(git_head()));
}
