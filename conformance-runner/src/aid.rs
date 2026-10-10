//! 値の表の `buildAid` の実行（設計 5.2.1）。
//!
//! 実行器は、`AggregateId` を実装した試験用の型を持ち、`type_name()`・`value()` は入力の型名・値を返す。
//! `user_string` は別の表現（`Display`）として持つ。`AidString::from_aggregate_id` を呼び、`as_str()` を
//! 比較する。実行器の事前検査でライブラリの検査を代替しない。

use std::fmt::{Display, Formatter};

use event_store_adapter_rs::aggregate_id::{AggregateId, AidString};
use event_store_adapter_rs::error::EventStoreError;
use serde_json::{json, Value};

use crate::report::{CaseOutcome, ObservedValues};

/// 値の表の試験用の集約 ID。`user_string` は `Display` の別表現として持ち、結果に影響しない。
struct CaseAggregateId {
  type_name: String,
  value: String,
  user_string: String,
}

impl std::fmt::Debug for CaseAggregateId {
  fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
    formatter
      .debug_struct("CaseAggregateId")
      .field("type_name", &self.type_name)
      .field("value", &self.value)
      .finish()
  }
}

impl Clone for CaseAggregateId {
  fn clone(&self) -> Self {
    Self {
      type_name: self.type_name.clone(),
      value: self.value.clone(),
      user_string: self.user_string.clone(),
    }
  }
}

impl Display for CaseAggregateId {
  fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
    formatter.write_str(&self.user_string)
  }
}

impl AggregateId for CaseAggregateId {
  fn type_name(&self) -> String {
    self.type_name.clone()
  }

  fn value(&self) -> String {
    self.value.clone()
  }
}

fn failed(detail: String, expected: Option<Value>, actual: Option<Value>) -> CaseOutcome {
  CaseOutcome::Failed {
    failed_operation: None,
    detail,
    expected,
    actual,
    unfired_faults: Vec::new(),
  }
}

/// ケースの `expect.error` を、`EventStoreError` の実際の失敗と比較する。
fn matches_expected_error(expected: &Value, error: &EventStoreError) -> Result<(), String> {
  let category = expected
    .get("category")
    .and_then(Value::as_str)
    .ok_or_else(|| "expect.error.category がない".to_string())?;
  let EventStoreError::ContractViolation { rule, .. } = error else {
    return Err(format!("分類が contract-violation ではない: {error}"));
  };
  if category != "contract-violation" {
    return Err(format!("期待する分類は contract-violation ではない: {category}"));
  }
  if let Some(expected_rule) = expected.get("rule").and_then(Value::as_str) {
    let found = rule.to_string();
    if expected_rule != found {
      return Err(format!("期待する規則は {expected_rule}、実際は {found}"));
    }
  }
  let text = error.to_string();
  if let Some(message) = expected.get("message") {
    if let Some(must_contain) = message.get("must_contain").and_then(Value::as_array) {
      for needle in must_contain {
        let needle = needle
          .as_str()
          .ok_or_else(|| "must_contain の要素が文字列ではない".to_string())?;
        if !text.contains(needle) {
          return Err(format!("メッセージに {needle:?} を含まない: {text}"));
        }
      }
    }
    if let Some(must_not_contain) = message.get("must_not_contain").and_then(Value::as_array) {
      for needle in must_not_contain {
        let needle = needle
          .as_str()
          .ok_or_else(|| "must_not_contain の要素が文字列ではない".to_string())?;
        if text.contains(needle) {
          return Err(format!("メッセージに {needle:?} を含む: {text}"));
        }
      }
    }
  }
  Ok(())
}

/// 値の表の `buildAid` のケース 1 つを実行する。
///
/// 成功は `expect.value` と `AidString::as_str()` の一致、失敗は `expect.error` と
/// `EventStoreError::ContractViolation` の分類・規則・メッセージ条件の一致で判定する。
pub fn run_build_aid(case: &Value) -> CaseOutcome {
  let Some(aggregate_id) = case.pointer("/input/aggregate_id") else {
    return failed("input.aggregate_id がない".to_string(), None, None);
  };
  let Some(type_name) = aggregate_id.get("type_name").and_then(Value::as_str) else {
    return failed("input.aggregate_id.type_name がない".to_string(), None, None);
  };
  let Some(value) = aggregate_id.get("value").and_then(Value::as_str) else {
    return failed("input.aggregate_id.value がない".to_string(), None, None);
  };
  let user_string = case
    .pointer("/input/user_string")
    .and_then(Value::as_str)
    .unwrap_or_default();
  let id = CaseAggregateId {
    type_name: type_name.to_string(),
    value: value.to_string(),
    user_string: user_string.to_string(),
  };

  match AidString::from_aggregate_id(&id) {
    Ok(aid) => {
      let expected = case.pointer("/expect/value").and_then(Value::as_str);
      match expected {
        Some(expected) if expected == aid.as_str() => CaseOutcome::Passed {
          values: Some(ObservedValues {
            expected: case["expect"].clone(),
            actual: json!({"value":aid.as_str()}),
          }),
        },
        _ => failed(
          "組み立てた aid が期待値と一致しない".to_string(),
          case.pointer("/expect/value").cloned(),
          Some(Value::String(aid.as_str().to_string())),
        ),
      }
    }
    Err(error) => match case.pointer("/expect/error") {
      Some(expected) => match matches_expected_error(expected, &error) {
        Ok(()) => CaseOutcome::Passed {
          values: Some(ObservedValues {
            expected: case["expect"].clone(),
            actual: crate::case::error_value(&error),
          }),
        },
        Err(detail) => failed(
          detail,
          case.pointer("/expect/error").cloned(),
          Some(crate::case::error_value(&error)),
        ),
      },
      None => failed(
        "失敗したが expect.error がない".to_string(),
        None,
        Some(crate::case::error_value(&error)),
      ),
    },
  }
}

/// ケースが値の表の `buildAid` か。
pub fn is_build_aid(case: &crate::data::Case) -> bool {
  matches!(case.kind, crate::data::CaseKind::ValueTable)
    && case.body.get("operation") == Some(&Value::String("buildAid".to_string()))
}
