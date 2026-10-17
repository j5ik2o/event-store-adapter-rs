//! ケースの分類と、実装済みの公開操作への接続。

use serde_json::Value;

use crate::data::{expand_generators, Case, CaseKind, Coverage, DataSet};
use crate::fault::FaultPlan;
use crate::observe::unimplemented_constraint_words;
use crate::report::{CaseOutcome, CaseReport, NotApplicableReason, RepresentationGap, UnverifiedReason};

/// 実行する保存先を、ケースの分類に必要な値だけで表す。
///
/// 保存先ごとの差は `target_*` に閉じる。入口（`main.rs`）が `target_*` の定数から選んで渡すので、
/// このモジュールは、実行する保存先の種類では分岐しない。この言語が提供する保存先の一覧
/// （[`PROVIDED_BACKENDS`]）は、対象でないケースの理由（提供しない／今回の保存先でない）の選択にだけ使う。
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

/// この言語が提供する保存先。
pub const PROVIDED_BACKENDS: [&str; 2] = ["memory", "dynamodb"];

/// 特定の保存先に属する値の表の操作と、その保存先。
const VALUE_OPERATION_BACKENDS: [(&str, &str); 3] = [
  ("encodeBigtableEventEnvelope", "bigtable"),
  ("encodeBigtableSnapshotEnvelope", "bigtable"),
  ("decodeBigtableEnvelope", "bigtable"),
];

/// 実行器が解釈する `store` の設定。
const KNOWN_STORE_SETTINGS: [&str; 6] = [
  "retention_count",
  "retention_mode",
  "ttl_grace_seconds",
  "layout_version",
  "retry_limit",
  "change_feed",
];

/// 実行器が解釈する `seed` の項目。
const KNOWN_SEED_ITEMS: [&str; 1] = ["items"];

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

/// ケースが対象とする保存先を返す。場面は `backends`、値の表は操作が属する保存先。
///
/// どの保存先でも実行する値の表と、配置のケースは `None` を返す。
fn declared_backends(case: &Case) -> Option<Vec<&str>> {
  match case.kind {
    CaseKind::Layout => None,
    CaseKind::Scenario => Some(
      case
        .body
        .get("backends")
        .and_then(Value::as_array)
        .map(|backends| backends.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default(),
    ),
    CaseKind::ValueTable => {
      let operation = case.body.get("operation").and_then(Value::as_str)?;
      VALUE_OPERATION_BACKENDS
        .iter()
        .find(|(name, _)| *name == operation)
        .map(|(_, backend)| vec![*backend])
    }
  }
}

/// `store`・`seed` のうち、実行器が知らない項目を `store.<名前>`・`seed.<名前>` の形で返す。
fn unknown_elements(body: &Value) -> Vec<String> {
  let unknown_in = |section: &str, known: &[&str]| -> Vec<String> {
    body
      .get(section)
      .and_then(Value::as_object)
      .into_iter()
      .flat_map(|items| items.keys())
      .filter(|key| !known.contains(&key.as_str()))
      .map(|key| format!("{section}.{key}"))
      .collect()
  };
  let mut unknown = unknown_in("store", &KNOWN_STORE_SETTINGS);
  unknown.extend(unknown_in("seed", &KNOWN_SEED_ITEMS));
  unknown
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
    CaseKind::ValueTable => declared_backends(case).is_none_or(|backends| backends.contains(&target.name)),
  }
}

/// 実行する保存先が対象にしないケースの理由を選ぶ。
///
/// ケースの対象の保存先が、実行する保存先も、この言語が提供する保存先も含まないときだけ「提供しない」にする。
/// 実行する保存先が `backends` にあるのに対象でないのは、能力（`requires`）が足りないためなので、
/// 「提供しない」にしない。
fn not_targeted_reason(case: &Case, target: &Target) -> NotApplicableReason {
  if let Some(backends) = declared_backends(case) {
    let provides_any = backends.iter().any(|backend| PROVIDED_BACKENDS.contains(backend));
    if !backends.is_empty() && !backends.contains(&target.name) && !provides_any {
      return NotApplicableReason::BackendNotProvided {
        detail: format!("この言語は保存先 {} を提供しない", backends.join("・")),
      };
    }
  }
  NotApplicableReason::BackendNotTargeted {
    detail: format!(
      "ケースの backends・requires に、保存先 {} で実行できるものがない",
      target.name
    ),
  }
}

/// `store.change_feed` を読む。省略時は変更フィードを求めない（false）。
fn requests_change_feed(body: &Value) -> bool {
  body
    .pointer("/store/change_feed")
    .and_then(Value::as_bool)
    .unwrap_or(false)
}

/// ケース 1 つを分類し、実装済みの保存先へ渡す。
///
/// 判定の順は、保存先の対象（対象でなければ、提供しない保存先か今回の保存先でないかの理由）、
/// 表現不能（`signed_seq_nr`・`store.change_feed`）、精度の選択、FNV-1a 64 の決定、
/// `coverage.json` の除外、実行器が知らない `store`・`seed` の要素、準備の失敗、実装していない条件の語、
/// 値の表の `buildAid` の実行、渡された保存先実行関数への委譲、の順。
/// 対象外のケースは、中身を解釈する前に分類するので、知らない要素があっても誤りにならない。
pub fn run_case(
  case: &Case,
  target: &Target,
  coverage: &Coverage,
  execute: impl Fn(&Case, PreparedCase) -> CaseOutcome,
) -> CaseOutcome {
  if !targets_backend(case, target) {
    return not_applicable(not_targeted_reason(case, target));
  }
  if case.body.pointer("/representation/signed_seq_nr") == Some(&Value::Bool(true)) {
    return not_applicable(NotApplicableReason::Representation {
      representation: RepresentationGap::Unrepresentable,
      detail: "SeqNr は u64 で、負数を表せない（signed_seq_nr）".to_string(),
    });
  }
  if requests_change_feed(&case.body) {
    return not_applicable(NotApplicableReason::Representation {
      representation: RepresentationGap::Unrepresentable,
      detail: "設定の型（RetentionSettings・DynamoDbOptions）に変更フィードの項目がなく、change_feed = true を表せない"
        .to_string(),
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
  let unknown = unknown_elements(&case.body);
  if !unknown.is_empty() {
    return CaseOutcome::Failed {
      failed_operation: None,
      detail: format!("実行器が解釈しない要素がある: {}", unknown.join("、")),
      expected: None,
      actual: None,
      unfired_faults: Vec::new(),
    };
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
