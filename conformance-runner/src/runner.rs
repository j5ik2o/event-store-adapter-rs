//! ケースの分類（対象外の判定、準備、未検証の理由）。保存先にはまだつながない。

use serde_json::Value;

use crate::data::{expand_generators, Case, CaseKind, Coverage, DataSet};
use crate::fault::FaultPlan;
use crate::observe::unimplemented_constraint_words;
use crate::report::{CaseOutcome, CaseReport, NotApplicableReason, RepresentationGap, UnverifiedReason};

/// 実行する保存先を表す。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
  Memory,
  DynamoDb,
}

impl Backend {
  /// コマンドの引数の文字列から保存先を作る。知らない文字列は `None` を返す。
  pub fn parse(value: &str) -> Option<Backend> {
    match value {
      "memory" => Some(Backend::Memory),
      "dynamodb" => Some(Backend::DynamoDb),
      _ => None,
    }
  }

  /// 報告と引数で使う保存先の名前を返す。
  pub fn as_str(&self) -> &'static str {
    match self {
      Backend::Memory => "memory",
      Backend::DynamoDb => "dynamodb",
    }
  }

  /// 保存先が任意の能力 `capability` を提供するなら真を返す。`ttl` は DynamoDB だけが提供する。
  pub fn provides(&self, capability: &str) -> bool {
    match self {
      Backend::Memory => false,
      Backend::DynamoDb => capability == "ttl",
    }
  }
}

/// 準備（generators の展開と障害の登録）を通したケースを表す。
pub struct PreparedCase {
  pub body: Value,
  pub faults: FaultPlan,
}

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

fn targets_backend(case: &Case, backend: Backend) -> bool {
  match case.kind {
    // 配置は DynamoDB の 3 テーブルの表。メモリには配置がない。
    CaseKind::Layout => backend == Backend::DynamoDb,
    CaseKind::Scenario => {
      let listed = case
        .body
        .get("backends")
        .and_then(Value::as_array)
        .is_some_and(|backends| backends.iter().any(|name| name.as_str() == Some(backend.as_str())));
      let provided = case
        .body
        .get("requires")
        .and_then(Value::as_array)
        .is_none_or(|capabilities| {
          capabilities
            .iter()
            .all(|capability| capability.as_str().is_some_and(|name| backend.provides(name)))
        });
      listed && provided
    }
    CaseKind::ValueTable => true,
  }
}

/// ケース 1 つを分類する。保存先につながないので、`Passed` は返さない。
///
/// 判定の順は、保存先の対象、表現不能、精度の選択、FNV-1a 64 の決定、`coverage.json` の除外、準備の失敗、
/// 実装していない条件の語、実行していない、の順。
pub fn run_case(case: &Case, backend: Backend, coverage: &Coverage) -> CaseOutcome {
  if !targets_backend(case, backend) {
    return not_applicable(NotApplicableReason::BackendNotTargeted {
      detail: format!(
        "ケースの backends・requires に、保存先 {} で実行できるものがない",
        backend.as_str()
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
  CaseOutcome::Unverified {
    reason: UnverifiedReason::NotExecuted {
      detail: "保存先が未接続（実行器の骨格）なので、実行していない".to_string(),
    },
  }
}

/// 全ケースを分類して、ケースごとの報告を返す。
pub fn run(data: &DataSet, backend: Backend) -> Vec<CaseReport> {
  data
    .cases
    .iter()
    .map(|case| CaseReport {
      id: case.id.clone(),
      rules: case.rules.clone(),
      outcome: run_case(case, backend, &data.coverage),
    })
    .collect()
}
