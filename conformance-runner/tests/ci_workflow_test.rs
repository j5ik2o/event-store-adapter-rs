//! CI ワークフロー（`.github/workflows/ci.yml`）の、緑のまま通ってしまう退行を防ぐ試験。
//! 適合試験の各枠が `--require-all` と feature を実行器へ渡すか、実行器と移行の実行ファイルの試験のジョブがあるか、
//! `CI Success` がそのジョブの失敗で失敗するかを、ワークフローから読み取った実効の値で検査する。
//! 文法とジョブの参照の整合は、`release-control` ジョブの `actionlint` が検査する。

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::process::{Command, Output};

const RUNNER_CARGO_RUN: &str = "cargo run -p event-store-adapter-conformance-rs";
const RUNNER_AND_MIGRATION_TEST_JOB: &str = "test-conformance-migration";

// ---------------------------------------------------------------------------
// ワークフローの読み取り（YAML の解析器に依存せず、行の字下げで構造を読む）
// ---------------------------------------------------------------------------

fn workflow() -> String {
  let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../.github/workflows/ci.yml");
  fs::read_to_string(path).expect("ci.yml を読める")
}

fn indentation(line: &str) -> usize {
  line.len() - line.trim_start().len()
}

/// ジョブ `id` の本体の行（`id:` の行は含まない）を返す。
fn job_body(id: &str) -> Vec<String> {
  let text = workflow();
  let header = format!("  {id}:");
  let mut lines = text.lines().skip_while(|line| line.trim_end() != header);
  assert!(lines.next().is_some(), "ジョブ {id} が ci.yml にある");
  lines
    .take_while(|line| line.trim().is_empty() || indentation(line) > 2)
    .map(str::to_string)
    .collect()
}

/// ジョブの本体にある `run:` の値を、書かれた順に返す。ブロックは字下げを除いた複数行の文字列になる。
fn run_commands(body: &[String]) -> Vec<String> {
  let mut commands = Vec::new();
  let mut index = 0;
  while index < body.len() {
    let line = &body[index];
    index += 1;
    let stripped = line.trim_start();
    let (key_indentation, entry) = match stripped.strip_prefix("- ") {
      Some(rest) => (indentation(line) + 2, rest.trim_start()),
      None => (indentation(line), stripped),
    };
    let Some(value) = entry.strip_prefix("run:") else {
      continue;
    };
    let value = value.trim();
    if value.starts_with('|') || value.starts_with('>') {
      let block: Vec<&str> = body[index..]
        .iter()
        .map(String::as_str)
        .take_while(|line| line.trim().is_empty() || indentation(line) > key_indentation)
        .collect();
      index += block.len();
      let margin = block
        .iter()
        .filter(|line| !line.trim().is_empty())
        .map(|line| indentation(line))
        .min()
        .unwrap_or(0);
      let script: Vec<&str> = block
        .iter()
        .map(|line| if line.len() >= margin { &line[margin..] } else { "" })
        .collect();
      commands.push(script.join("\n"));
    } else {
      commands.push(value.to_string());
    }
  }
  commands
}

fn unquote(value: &str) -> String {
  for quote in ['\'', '"'] {
    if let Some(inner) = value.strip_prefix(quote).and_then(|rest| rest.strip_suffix(quote)) {
      return inner.to_string();
    }
  }
  value.to_string()
}

/// 適合試験の 1 つの枠の matrix の値。
type Leg = BTreeMap<String, String>;

/// conformance ジョブの `matrix.include` が表す枠を返す。
fn matrix_legs(body: &[String]) -> Vec<Leg> {
  let position = body
    .iter()
    .position(|line| line.trim() == "include:")
    .expect("conformance の matrix に include がある");
  let include_indentation = indentation(&body[position]);
  let mut legs: Vec<Leg> = Vec::new();
  for line in body[position + 1..]
    .iter()
    .take_while(|line| line.trim().is_empty() || indentation(line) > include_indentation)
  {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
      continue;
    }
    let (entry, starts_leg) = match trimmed.strip_prefix("- ") {
      Some(rest) => (rest, true),
      None => (trimmed, false),
    };
    let (key, value) = entry
      .split_once(':')
      .unwrap_or_else(|| panic!("matrix の行は `key: value`: {trimmed}"));
    if starts_leg {
      legs.push(Leg::new());
    }
    legs
      .last_mut()
      .expect("include の最初の行は `- ` で始まる")
      .insert(key.trim().to_string(), unquote(value.trim()));
  }
  legs
}

/// `${{ matrix.<key> }}` を、その枠の値に置き換える。matrix 以外の式はそのまま残す。
fn expand_matrix(command: &str, leg: &Leg) -> String {
  let mut expanded = String::new();
  let mut rest = command;
  while let Some(start) = rest.find("${{") {
    expanded.push_str(&rest[..start]);
    let after = &rest[start + 3..];
    let end = after.find("}}").expect("式が閉じている");
    match after[..end].trim().strip_prefix("matrix.") {
      Some(key) => expanded.push_str(
        leg
          .get(key)
          .unwrap_or_else(|| panic!("matrix に {key} がない: {command}")),
      ),
      None => expanded.push_str(&rest[start..start + 3 + end + 2]),
    }
    rest = &after[end + 2..];
  }
  expanded.push_str(rest);
  expanded
}

/// 枠が起動する実行器のコマンドを、`cargo run` の引数と、`--` の後ろの実行器の引数に分けたもの。
struct RunnerInvocation {
  cargo_args: Vec<String>,
  runner_args: Vec<String>,
}

fn runner_invocation(backend: &str) -> RunnerInvocation {
  let body = job_body("conformance");
  let legs = matrix_legs(&body);
  let leg = legs
    .iter()
    .find(|leg| leg.get("backend").map(String::as_str) == Some(backend))
    .unwrap_or_else(|| panic!("{backend} の枠が conformance の matrix にある: {legs:?}"));
  let commands: Vec<String> = run_commands(&body)
    .into_iter()
    .filter(|command| command.contains(RUNNER_CARGO_RUN))
    .collect();
  assert_eq!(commands.len(), 1, "実行器を起動する手順は 1 つ: {commands:?}");
  let tokens: Vec<String> = expand_matrix(&commands[0], leg)
    .split_whitespace()
    .map(str::to_string)
    .collect();
  let separator = tokens
    .iter()
    .position(|token| token == "--")
    .unwrap_or_else(|| panic!("`cargo run` の引数と実行器の引数を分ける `--` がある: {tokens:?}"));
  RunnerInvocation {
    cargo_args: tokens[..separator].to_vec(),
    runner_args: tokens[separator + 1..].to_vec(),
  }
}

/// `--name value` か `--name=value` の形で、引数の中にオプションがあるか。
fn has_option(args: &[String], name: &str, value: &str) -> bool {
  args.windows(2).any(|pair| pair[0] == name && pair[1] == value)
    || args.iter().any(|arg| *arg == format!("{name}={value}"))
}

// ---------------------------------------------------------------------------
// 適合試験の各枠: DynamoDB の枠は feature 付きで実行し、どの枠も未検証 1 件で失敗する
// ---------------------------------------------------------------------------

#[test]
fn should_ci_dynamodb_leg_run_the_runner_with_the_dynamodb_feature() {
  let invocation = runner_invocation("dynamodb");

  assert!(
    has_option(&invocation.cargo_args, "--features", "dynamodb"),
    "DynamoDB の枠は feature なしだと全ケースが未検証になる: {:?}",
    invocation.cargo_args
  );
  assert!(
    has_option(&invocation.runner_args, "--backend", "dynamodb"),
    "{:?}",
    invocation.runner_args
  );
}

#[test]
fn should_ci_dynamodb_leg_require_all_cases_to_be_verified() {
  let invocation = runner_invocation("dynamodb");

  assert!(
    invocation.runner_args.iter().any(|arg| arg == "--require-all"),
    "`--require-all` は `--` の後ろの実行器の引数に置く: cargo={:?} runner={:?}",
    invocation.cargo_args,
    invocation.runner_args
  );
}

#[test]
fn should_ci_memory_leg_require_all_cases_to_be_verified() {
  let invocation = runner_invocation("memory");

  assert!(
    has_option(&invocation.runner_args, "--backend", "memory"),
    "{:?}",
    invocation.runner_args
  );
  assert!(
    invocation.runner_args.iter().any(|arg| arg == "--require-all"),
    "`--require-all` は `--` の後ろの実行器の引数に置く: cargo={:?} runner={:?}",
    invocation.cargo_args,
    invocation.runner_args
  );
}

// ---------------------------------------------------------------------------
// 実行器と移行の実行ファイルの試験のジョブ
// ---------------------------------------------------------------------------

#[test]
fn should_ci_run_the_runner_and_migration_tests_with_all_features_in_their_own_job() {
  let commands = run_commands(&job_body(RUNNER_AND_MIGRATION_TEST_JOB));

  let runs_both_packages_with_all_features = commands.iter().any(|command| {
    let tokens: Vec<String> = command.split_whitespace().map(str::to_string).collect();
    command.starts_with("cargo test ")
      && has_option(&tokens, "-p", "event-store-adapter-conformance-rs")
      && has_option(&tokens, "-p", "event-store-adapter-migration-rs")
      && tokens.iter().any(|token| token == "--all-features")
  });
  assert!(
    runs_both_packages_with_all_features,
    "{RUNNER_AND_MIGRATION_TEST_JOB} が実行器と移行の実行ファイルの試験を --all-features で走らせる: {commands:?}"
  );
}

// ---------------------------------------------------------------------------
// `CI Success` は、そのジョブの失敗で失敗する
// ---------------------------------------------------------------------------

/// `CI Success` ジョブの、待つジョブ・結果を受け取る環境変数・判定の手順。
struct CiSuccess {
  needs: Vec<String>,
  /// （環境変数の名前, その環境変数が結果を受け取るジョブ）
  results: Vec<(String, String)>,
  script: String,
}

fn ci_success() -> CiSuccess {
  let body = job_body("ci-success");

  let needs_position = body
    .iter()
    .position(|line| indentation(line) == 4 && line.trim().starts_with("needs:"))
    .expect("ci-success に needs がある");
  let inline = body[needs_position].trim().trim_start_matches("needs:").trim();
  let needs: Vec<String> = match inline.strip_prefix('[') {
    Some(list) => list
      .trim_end_matches(']')
      .split(',')
      .map(|job| job.trim().to_string())
      .filter(|job| !job.is_empty())
      .collect(),
    None => body[needs_position + 1..]
      .iter()
      .take_while(|line| line.trim().starts_with("- "))
      .map(|line| line.trim().trim_start_matches("- ").trim().to_string())
      .collect(),
  };

  let env_position = body
    .iter()
    .position(|line| line.trim() == "env:")
    .expect("ci-success の手順に env がある");
  let env_indentation = indentation(&body[env_position]);
  let results: Vec<(String, String)> = body[env_position + 1..]
    .iter()
    .take_while(|line| line.trim().is_empty() || indentation(line) > env_indentation)
    .filter_map(|line| {
      let (name, value) = line.trim().split_once(':')?;
      let job = value.split("needs.").nth(1)?.split('.').next()?;
      Some((name.trim().to_string(), job.to_string()))
    })
    .collect();

  let commands = run_commands(&body);
  assert_eq!(commands.len(), 1, "ci-success の判定の手順は 1 つ: {commands:?}");
  CiSuccess {
    needs,
    results,
    script: commands[0].clone(),
  }
}

/// 判定の手順を GitHub Actions の既定の bash と同じ指定で実行する。`failing` のジョブだけ failure、ほかは success にする。
fn run_ci_success(ci: &CiSuccess, failing: Option<&str>) -> Output {
  let mut command = Command::new("bash");
  command
    .args(["--noprofile", "--norc", "-eo", "pipefail", "-c", &ci.script])
    .env_clear();
  for (name, job) in &ci.results {
    command.env(
      name,
      if Some(job.as_str()) == failing {
        "failure"
      } else {
        "success"
      },
    );
  }
  command.output().expect("bash を起動できる")
}

fn describe(output: &Output) -> String {
  format!(
    "code={:?} stdout={} stderr={}",
    output.status.code(),
    String::from_utf8_lossy(&output.stdout),
    String::from_utf8_lossy(&output.stderr)
  )
}

#[test]
fn should_ci_success_step_fail_when_the_runner_and_migration_job_fails() {
  let ci = ci_success();

  assert!(
    ci.needs.iter().any(|job| job == RUNNER_AND_MIGRATION_TEST_JOB),
    "ci-success の needs に {RUNNER_AND_MIGRATION_TEST_JOB} がある: {:?}",
    ci.needs
  );
  assert!(
    ci.results.iter().any(|(_, job)| job == RUNNER_AND_MIGRATION_TEST_JOB),
    "{RUNNER_AND_MIGRATION_TEST_JOB} の結果を環境変数で受け取る: {:?}",
    ci.results
  );
  let all_succeeded = run_ci_success(&ci, None);
  assert_eq!(all_succeeded.status.code(), Some(0), "{}", describe(&all_succeeded));
  let migration_failed = run_ci_success(&ci, Some(RUNNER_AND_MIGRATION_TEST_JOB));
  assert_eq!(
    migration_failed.status.code(),
    Some(1),
    "{}",
    describe(&migration_failed)
  );
}
