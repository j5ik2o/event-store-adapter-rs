//! ケースの分類と、実装済みの公開操作への接続。

use serde_json::Value;

use crate::data::{expand_generators, Case, CaseKind, Coverage, DataSet};
use crate::fault::FaultPlan;
use crate::observe::unimplemented_constraint_words;
use crate::report::{CaseOutcome, CaseReport, NotApplicableReason, RepresentationGap, UnverifiedReason};

/// 実行する保存先を、ケースの分類に必要な値だけで表す。
///
/// 保存先ごとの差は `target_*` に閉じる。入口（`main.rs`）が `target_*` の定数から選んで渡すので、
/// このモジュールは保存先の種類を知らない。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Target {
  /// 報告と引数で使う保存先の名前。ケースの `backends` の要素と比べる。
  pub name: &'static str,
  /// 保存先が提供する任意の能力の語。ケースの `requires` の語と比べる。
  pub capabilities: &'static [&'static str],
  /// 3 テーブルの配置のケースの対象なら真。
  pub has_layout: bool,
}

/// 準備（generators の展開と障害の登録）を通したケースを表す。
pub struct PreparedCase {
  pub body: Value,
  pub faults: FaultPlan,
}

/// 入口が選ぶ保存先のケース実行関数を表す。
pub type CaseExecutor = fn(&Case, PreparedCase) -> CaseOutcome;

/// ケースの本体を複製し、generators を展開して障害を登録する。誤りは、その説明を返す。
pub fn prepare(case: &Case) -> Result<PreparedCase, String> {
  let mut body = case.body.clone();
  expand_generators(&mut body).map_err(|error| error.to_string())?;
  let faults = FaultPlan::register(&body).map_err(|error| error.to_string())?;
  Ok(PreparedCase { body, faults })
}

fn not_applicable(reason: NotApplicableReason) -> CaseOutcome {
  CaseOutcome::NotApplicable { reason }
}

fn targets_backend(case: &Case, target: &Target) -> bool {
  match case.kind {
    CaseKind::Layout => target.has_layout,
    CaseKind::Scenario => {
      let listed = case
        .body
        .get("backends")
        .and_then(Value::as_array)
        .is_some_and(|backends| backends.iter().any(|name| name.as_str() == Some(target.name)));
      let provided = case
        .body
        .get("requires")
        .and_then(Value::as_array)
        .is_none_or(|capabilities| {
          capabilities.iter().all(|capability| {
            capability
              .as_str()
              .is_some_and(|name| target.capabilities.contains(&name))
          })
        });
      listed && provided
    }
    CaseKind::ValueTable => true,
  }
}

/// ケース 1 つを分類し、実装済みの保存先へ渡す。
///
/// 判定の順は、保存先の対象、表現不能、精度の選択、FNV-1a 64 の決定、`coverage.json` の除外、準備の失敗、
/// 実装していない条件の語、値の表の `buildAid` の実行、渡された保存先実行関数への委譲、の順。
pub fn run_case(
  case: &Case,
  target: &Target,
  coverage: &Coverage,
  execute: impl Fn(&Case, PreparedCase) -> CaseOutcome,
) -> CaseOutcome {
  if !targets_backend(case, target) {
    return not_applicable(NotApplicableReason::BackendNotTargeted {
      detail: format!(
        "ケースの backends・requires に、保存先 {} で実行できるものがない",
        target.name
      ),
    });
  }
  if case.body.pointer("/representation/signed_seq_nr") == Some(&Value::Bool(true)) {
    return not_applicable(NotApplicableReason::Representation {
      representation: RepresentationGap::Unrepresentable,
      detail: "SeqNr は u64 で、負数を表せない（signed_seq_nr）".to_string(),
    });
  }
  if case.body.pointer("/representation/time_precision") == Some(&Value::String("milliseconds".to_string())) {
    return not_applicable(NotApplicableReason::Representation {
      representation: RepresentationGap::TimePrecision,
      detail: "ナノ秒を表せる型を使うので、ミリ秒の精度を選ぶケースは実行しない".to_string(),
    });
  }
  if matches!(case.kind, CaseKind::ValueTable)
    && case.body.get("operation") == Some(&Value::String("fnv1a64".to_string()))
  {
    return not_applicable(NotApplicableReason::Fnv1a64Decision {
      detail: "最初のメジャーにハッシュを使う保存先がない（オーナーの決定、2026-10-06）".to_string(),
    });
  }
  let all_rules_excluded = !case.rules.is_empty()
    && case
      .rules
      .iter()
      .all(|rule| coverage.exclusions.iter().any(|e| &e.rule == rule));
  if all_rules_excluded {
    return not_applicable(NotApplicableReason::CoverageExclusion {
      detail: "ケースの全規則が coverage.json の除外の対象".to_string(),
    });
  }
  let prepared = match prepare(case) {
    Ok(prepared) => prepared,
    Err(detail) => {
      return CaseOutcome::Failed {
        failed_operation: None,
        detail,
        expected: None,
        actual: None,
        unfired_faults: Vec::new(),
      }
    }
  };
  let words = unimplemented_constraint_words(&prepared.body);
  if !words.is_empty() {
    return CaseOutcome::Unverified {
      reason: UnverifiedReason::UnimplementedConstraintWords {
        words: words.into_iter().collect(),
      },
    };
  }
  if crate::aid::is_build_aid(case) {
    return crate::aid::run_build_aid(&prepared.body);
  }
  execute(case, prepared)
}

/// 全ケースを分類して、ケースごとの報告を返す。
pub fn run(data: &DataSet, target: &Target, execute: impl Fn(&Case, PreparedCase) -> CaseOutcome) -> Vec<CaseReport> {
  data
    .cases
    .iter()
    .map(|case| CaseReport {
      id: case.id.clone(),
      rules: case.rules.clone(),
      outcome: run_case(case, target, &data.coverage, &execute),
    })
    .collect()
}
