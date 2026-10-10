//! 適合テストデータの実行器のコマンド。
//!
//! 使い方: `event-store-adapter-conformance-rs --backend <memory|dynamodb> --data <パス> --report <パス> [--require-all]`

use std::path::PathBuf;
use std::process::ExitCode;

use event_store_adapter_conformance_rs::data;
use event_store_adapter_conformance_rs::report::{Implementation, Report};
use event_store_adapter_conformance_rs::runner::{self, Target};
use event_store_adapter_conformance_rs::{target_dynamodb, target_memory};

const USAGE: &str =
  "使い方: event-store-adapter-conformance-rs --backend <memory|dynamodb> --data <conformance のパス> --report <報告のパス> [--require-all]";

/// コマンドの引数を解析した結果を表す。
#[derive(Debug)]
struct Args {
  backend: Target,
  data: PathBuf,
  report: PathBuf,
  require_all: bool,
}

/// コマンドの引数から、保存先の分類情報を選ぶ。知らない文字列は `None` を返す。
fn parse_target(value: &str) -> Option<Target> {
  match value {
    "memory" => Some(target_memory::TARGET),
    "dynamodb" => Some(target_dynamodb::TARGET),
    _ => None,
  }
}

/// 引数を解析する。必須の欠落、値の欠落、不明な引数、不明な保存先は誤りにする。
fn parse_args<I: IntoIterator<Item = String>>(args: I) -> Result<Args, String> {
  let mut args = args.into_iter();
  let mut backend = None;
  let mut data = None;
  let mut report = None;
  let mut require_all = false;
  while let Some(argument) = args.next() {
    match argument.as_str() {
      "--backend" => {
        let value = args.next().ok_or("--backend に値がない")?;
        backend = Some(parse_target(&value).ok_or_else(|| format!("不明な保存先: {value}"))?);
      }
      "--data" => data = Some(PathBuf::from(args.next().ok_or("--data に値がない")?)),
      "--report" => report = Some(PathBuf::from(args.next().ok_or("--report に値がない")?)),
      "--require-all" => require_all = true,
      other => return Err(format!("不明な引数: {other}")),
    }
  }
  Ok(Args {
    backend: backend.ok_or("--backend が必要")?,
    data: data.ok_or("--data が必要")?,
    report: report.ok_or("--report が必要")?,
    require_all,
  })
}

fn main() -> ExitCode {
  let args = match parse_args(std::env::args().skip(1)) {
    Ok(args) => args,
    Err(message) => {
      eprintln!("{message}\n{USAGE}");
      return ExitCode::from(2);
    }
  };
  let data = match data::load(&args.data) {
    Ok(data) => data,
    Err(error) => {
      eprintln!("データを読み込めない: {error}");
      return ExitCode::from(2);
    }
  };
  let target = args.backend;
  #[cfg(feature = "dynamodb")]
  let execution = if target == target_dynamodb::TARGET {
    Some(target_dynamodb::Execution::start())
  } else {
    None
  };
  let memory_observations = std::cell::RefCell::new(std::collections::BTreeMap::new());
  let cases = runner::run(&data, &target, |case, prepared| {
    if target == target_memory::TARGET {
      let mut observations = Vec::new();
      let outcome = target_memory::run_case_observed(case, prepared, &mut observations);
      memory_observations.borrow_mut().insert(case.id.clone(), observations);
      return outcome;
    }
    #[cfg(feature = "dynamodb")]
    {
      match execution.as_ref().expect("parse_targetが選んだDynamoDBの生成") {
        Ok(execution) => execution.run_case(case, prepared),
        Err(e) => event_store_adapter_conformance_rs::report::CaseOutcome::Unverified {
          reason: event_store_adapter_conformance_rs::report::UnverifiedReason::NotExecuted { detail: e.clone() },
        },
      }
    }
    #[cfg(not(feature = "dynamodb"))]
    target_dynamodb::run_case(case, prepared)
  });
  let mut report = Report::build(&data, target.name, cases, Implementation::detect());
  report.observations = memory_observations.into_inner();
  #[cfg(feature = "dynamodb")]
  if let Some(Ok(execution)) = &execution {
    report.environment = Some(execution.environment());
    report.observations = execution.observations();
  }
  let mut text = match serde_json::to_string_pretty(&report) {
    Ok(text) => text,
    Err(error) => {
      eprintln!("報告を JSON にできない: {error}");
      return ExitCode::from(2);
    }
  };
  text.push('\n');
  if let Err(error) = std::fs::write(&args.report, text) {
    eprintln!("報告を書き出せない（{}）: {error}", args.report.display());
    return ExitCode::from(2);
  }
  let counts = report.status_counts();
  println!(
    "manifest: {} ({} files), cases: {}, passed={} failed={} not-applicable={} unverified={}",
    report.data.manifest.verification,
    data.manifest_files,
    report.cases.len(),
    counts.passed,
    counts.failed,
    counts.not_applicable,
    counts.unverified,
  );
  if report.should_fail(args.require_all) {
    ExitCode::FAILURE
  } else {
    ExitCode::SUCCESS
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn arguments(list: &[&str]) -> Vec<String> {
    list.iter().map(|argument| argument.to_string()).collect()
  }

  #[test]
  fn test_parse_args_reads_all_arguments() {
    let args = parse_args(arguments(&[
      "--backend",
      "dynamodb",
      "--data",
      "conformance",
      "--report",
      "report.json",
      "--require-all",
    ]))
    .expect("正しい引数");

    assert_eq!(args.backend, target_dynamodb::TARGET);
    assert_eq!(args.data, PathBuf::from("conformance"));
    assert_eq!(args.report, PathBuf::from("report.json"));
    assert!(args.require_all);
  }

  #[test]
  fn test_parse_args_defaults_require_all_to_false() {
    let args = parse_args(arguments(&[
      "--backend",
      "memory",
      "--data",
      "conformance",
      "--report",
      "report.json",
    ]))
    .expect("正しい引数");

    assert_eq!(args.backend, target_memory::TARGET);
    assert!(!args.require_all);
  }

  #[test]
  fn test_parse_args_rejects_missing_required_arguments() {
    let complete = [
      "--backend",
      "memory",
      "--data",
      "conformance",
      "--report",
      "report.json",
    ];
    for omitted in ["--backend", "--data", "--report"] {
      let remaining: Vec<&str> = complete
        .chunks(2)
        .filter(|pair| pair[0] != omitted)
        .flatten()
        .copied()
        .collect();

      assert!(
        parse_args(arguments(&remaining)).is_err(),
        "{omitted} がなくても受け付けた"
      );
    }
  }

  #[test]
  fn test_parse_args_rejects_option_without_value() {
    assert!(parse_args(arguments(&["--backend"])).is_err());
    assert!(parse_args(arguments(&["--backend", "memory", "--data"])).is_err());
    assert!(parse_args(arguments(&["--backend", "memory", "--data", "conformance", "--report"])).is_err());
  }

  #[test]
  fn test_parse_args_rejects_unknown_argument() {
    let result = parse_args(arguments(&[
      "--backend",
      "memory",
      "--data",
      "conformance",
      "--report",
      "report.json",
      "--unknown",
    ]));

    assert!(result.is_err());
  }

  #[test]
  fn test_parse_args_rejects_unknown_backend() {
    let result = parse_args(arguments(&[
      "--backend",
      "sqlite",
      "--data",
      "conformance",
      "--report",
      "report.json",
    ]));

    assert!(result.is_err());
  }
}
