//! 報告の型、規則ごとの集計、状態ごとの件数、終了の判定。

use std::collections::BTreeMap;
use std::path::Path;

use serde_json::Value;

use crate::data::DataSet;
use crate::fault::UnfiredFault;

/// 表現能力の違いの、2 つの場合を表す。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RepresentationGap {
  /// 値を表せない（`representation.signed_seq_nr = true` のケースだけ）。
  Unrepresentable,
  /// 時刻の精度の選択（`representation.time_precision = milliseconds` のケース）。
  TimePrecision,
}

/// 対象外として認める理由を表す。理由は、この 4 つだけ。
///
/// 対象外は型で必ず理由を持つので、理由のない対象外は作れない。
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum NotApplicableReason {
  /// ケースの `backends` に、実行する保存先がない。
  BackendNotTargeted { detail: String },
  /// 表現能力の違い。
  Representation {
    representation: RepresentationGap,
    detail: String,
  },
  /// 最初のメジャーにハッシュを使う保存先がない（オーナーの決定）。
  Fnv1a64Decision { detail: String },
  /// `coverage.json` の規則の単位の除外。
  CoverageExclusion { detail: String },
}

/// 未検証（実行できなかった）の理由を表す。
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum UnverifiedReason {
  /// 実行器が実装していない条件の語がある。
  UnimplementedConstraintWords { words: Vec<String> },
  /// 保存先につながっていないので、実行していない。
  NotExecuted { detail: String },
}

/// ケース 1 つの結果を表す。
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(tag = "status", rename_all = "kebab-case")]
pub enum CaseOutcome {
  Passed,
  Failed {
    #[serde(skip_serializing_if = "Option::is_none")]
    failed_operation: Option<u32>,
    detail: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    expected: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    actual: Option<Value>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    unfired_faults: Vec<UnfiredFault>,
  },
  NotApplicable {
    reason: NotApplicableReason,
  },
  Unverified {
    reason: UnverifiedReason,
  },
}

/// 報告の `cases[]` の要素を表す。
#[derive(serde::Serialize)]
pub struct CaseReport {
  pub id: String,
  pub rules: Vec<String>,
  #[serde(flatten)]
  pub outcome: CaseOutcome,
}

/// 報告の `rules[]` の要素を表す。規則ごとの 4 状態の件数を持つ。
#[derive(serde::Serialize)]
pub struct RuleReport {
  pub rule: String,
  pub passed: u32,
  pub failed: u32,
  pub not_applicable: u32,
  pub unverified: u32,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub reason: Option<NotApplicableReason>,
}

/// 状態ごとのケースの件数を表す。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StatusCounts {
  pub passed: u32,
  pub failed: u32,
  pub not_applicable: u32,
  pub unverified: u32,
}

/// 実行した実装を表す。
#[derive(serde::Serialize)]
pub struct Implementation {
  pub language: &'static str,
  #[serde(rename = "crate")]
  pub crate_name: &'static str,
  pub version: Option<String>,
  pub revision: Option<String>,
}

impl Implementation {
  /// 試験の対象の実装（`event-store-adapter-rs`）の版と、コミットの識別子を調べる。
  ///
  /// 版は `lib/Cargo.toml` の `[package]` から取る。識別子は、この crate のフォルダー（ビルド時の
  /// `CARGO_MANIFEST_DIR`）で `git rev-parse HEAD` を動かして取る（`git_head_in`）。起動したフォルダーは
  /// 使わないので、別の git リポジトリの中から起動しても、その HEAD を報告に載せない。取れなければ
  /// 環境変数 `GITHUB_SHA`（CI のコミット）を使う（`resolve_revision`）。
  pub fn detect() -> Implementation {
    Implementation {
      language: "rust",
      crate_name: "event-store-adapter-rs",
      version: parse_package_version(include_str!("../../lib/Cargo.toml")),
      revision: resolve_revision(
        git_head_in(Path::new(env!("CARGO_MANIFEST_DIR"))),
        std::env::var("GITHUB_SHA").ok(),
      ),
    }
  }
}

/// `directory` を作業フォルダーにして `git rev-parse HEAD` を動かし、標準出力を返す。
///
/// フォルダーがない、git を起動できない、git が失敗した（git のリポジトリではないなど）ときは `None` を
/// 返す。前後の空白は除かない（`resolve_revision` が除く）。
pub fn git_head_in(directory: &Path) -> Option<String> {
  std::process::Command::new("git")
    .current_dir(directory)
    .args(["rev-parse", "HEAD"])
    .output()
    .ok()
    .filter(|output| output.status.success())
    .map(|output| String::from_utf8_lossy(&output.stdout).into_owned())
}

/// 報告の `implementation.revision` に載せるコミットの識別子を決める。
///
/// 作業ツリーの git の識別子を先に使い、なければ CI の `GITHUB_SHA` を使う。前後の空白を除いて空になる
/// 値は使わない。どちらもなければ `None` を返す。
pub fn resolve_revision(git_head: Option<String>, github_sha: Option<String>) -> Option<String> {
  [git_head, github_sha]
    .into_iter()
    .flatten()
    .map(|revision| revision.trim().to_string())
    .find(|revision| !revision.is_empty())
}

/// `Cargo.toml` の `[package]` の節にある `version` の値を返す。ほかの節の `version` は読まない。
pub fn parse_package_version(cargo_toml: &str) -> Option<String> {
  let mut in_package = false;
  for line in cargo_toml.lines() {
    let line = line.trim();
    if line.starts_with('[') {
      in_package = line == "[package]";
      continue;
    }
    if !in_package {
      continue;
    }
    if let Some((key, value)) = line.split_once('=') {
      if key.trim() == "version" {
        return Some(value.trim().trim_matches('"').to_string());
      }
    }
  }
  None
}

/// 報告の `data.manifest` を表す。
#[derive(serde::Serialize)]
pub struct ManifestReport {
  pub sha256: String,
  pub verification: &'static str,
  #[serde(skip_serializing_if = "Vec::is_empty")]
  pub problems: Vec<String>,
}

/// 報告の `data` を表す。
#[derive(serde::Serialize)]
pub struct DataReport {
  /// 実際に読んだ `manifest.json` の `version`。文字列でなければ `null`。
  pub version: Option<String>,
  pub manifest: ManifestReport,
}

/// 保存先ごとに 1 つ出す報告を表す。
#[derive(serde::Serialize)]
pub struct Report {
  pub data: DataReport,
  pub implementation: Implementation,
  pub backend: &'static str,
  pub cases: Vec<CaseReport>,
  pub rules: Vec<RuleReport>,
}

fn empty_rule_report(rule: &str) -> RuleReport {
  RuleReport {
    rule: rule.to_string(),
    passed: 0,
    failed: 0,
    not_applicable: 0,
    unverified: 0,
    reason: None,
  }
}

impl Report {
  /// データの照合結果とケースの結果から、報告を組み立てる。
  ///
  /// 規則の行は、必須の規則、除外の規則、全ケースの規則を入れる。各ケースの状態は、そのケースの全規則へ
  /// 数える。除外の規則の行には、除外の理由を載せる。
  pub fn build(
    data: &DataSet,
    backend: &'static str,
    cases: Vec<CaseReport>,
    implementation: Implementation,
  ) -> Report {
    let mut rules: BTreeMap<String, RuleReport> = BTreeMap::new();
    for rule in &data.coverage.required_rules {
      rules.entry(rule.clone()).or_insert_with(|| empty_rule_report(rule));
    }
    for exclusion in &data.coverage.exclusions {
      let row = rules
        .entry(exclusion.rule.clone())
        .or_insert_with(|| empty_rule_report(&exclusion.rule));
      row.reason = Some(NotApplicableReason::CoverageExclusion {
        detail: format!("{}: {}", exclusion.status, exclusion.reason),
      });
    }
    for case in &cases {
      for rule in &case.rules {
        let row = rules.entry(rule.clone()).or_insert_with(|| empty_rule_report(rule));
        match case.outcome {
          CaseOutcome::Passed => row.passed += 1,
          CaseOutcome::Failed { .. } => row.failed += 1,
          CaseOutcome::NotApplicable { .. } => row.not_applicable += 1,
          CaseOutcome::Unverified { .. } => row.unverified += 1,
        }
      }
    }
    Report {
      data: DataReport {
        version: data.manifest_version.clone(),
        manifest: ManifestReport {
          sha256: data.manifest_sha256.clone(),
          verification: if data.manifest.passed() { "passed" } else { "failed" },
          problems: data.manifest.problems.iter().map(ToString::to_string).collect(),
        },
      },
      implementation,
      backend,
      cases,
      rules: rules.into_values().collect(),
    }
  }

  /// 状態ごとのケースの件数を数える。
  pub fn status_counts(&self) -> StatusCounts {
    let mut counts = StatusCounts::default();
    for case in &self.cases {
      match case.outcome {
        CaseOutcome::Passed => counts.passed += 1,
        CaseOutcome::Failed { .. } => counts.failed += 1,
        CaseOutcome::NotApplicable { .. } => counts.not_applicable += 1,
        CaseOutcome::Unverified { .. } => counts.unverified += 1,
      }
    }
    counts
  }

  /// 終了コードを失敗にすべきなら真を返す。
  ///
  /// `manifest` の照合の失敗か、`failed` が 1 件でもあれば失敗。`require_all` のときは、`unverified` が
  /// 1 件でもあっても失敗にする。対象外は型で必ず理由を持つので、理由のない対象外の検査は要らない。
  pub fn should_fail(&self, require_all: bool) -> bool {
    let counts = self.status_counts();
    self.data.manifest.verification != "passed" || counts.failed > 0 || (require_all && counts.unverified > 0)
  }
}
