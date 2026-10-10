use crate::aggregate_id::{AggregateId, AidString};
use crate::error::{ContractRule, EventStoreError};

/// 試験用の集約 ID。`Display` を実装しない（`AggregateId` が `Display` を要求しないことの確認）。
#[derive(Debug, Clone)]
struct TestAggregateId {
  type_name: String,
  value: String,
  /// 利用者の文字列化を模した別表現。結果に影響しないことを確かめる。
  user_string: String,
}

impl TestAggregateId {
  fn new(type_name: &str, value: &str) -> Self {
    Self {
      type_name: type_name.to_string(),
      value: value.to_string(),
      user_string: "custom-display-value".to_string(),
    }
  }

  fn with_user_string(type_name: &str, value: &str, user_string: &str) -> Self {
    Self {
      type_name: type_name.to_string(),
      value: value.to_string(),
      user_string: user_string.to_string(),
    }
  }
}

impl AggregateId for TestAggregateId {
  fn type_name(&self) -> String {
    self.type_name.clone()
  }

  fn value(&self) -> String {
    self.value.clone()
  }
}

fn expect_contract_violation(error: EventStoreError, rule: ContractRule) -> EventStoreError {
  match error {
    EventStoreError::ContractViolation { rule: found, .. } if found == rule => error,
    other => panic!("expected ContractViolation({rule:?}), got {other:?}"),
  }
}

// T-1: 型名と値から aid を組み立て、利用者の文字列化に依存しない。
#[test]
fn should_build_aid_from_type_name_and_value_ignoring_user_string() {
  let id = TestAggregateId::with_user_string("Order", "123", "custom-display-value");

  let aid = AidString::from_aggregate_id(&id).expect("組み立てられる");

  assert_eq!(aid.as_str(), "Order-123");
  assert!(!aid.as_str().contains(&id.user_string));
}

// T-1 / T-11: 値に含まれるハイフンは保持する。
#[test]
fn should_build_aid_preserve_hyphen_in_value() {
  let id = TestAggregateId::new("order", "item-1");

  let aid = AidString::from_aggregate_id(&id).expect("組み立てられる");

  assert_eq!(aid.as_str(), "order-item-1");
}

// T-11: 型名のハイフンは組み立て時に拒む。無いと `order-item`+`1` と `order`+`item-1` が同じ aid になる。
#[test]
fn should_build_aid_reject_hyphen_in_type_name_with_t_11() {
  let id = TestAggregateId::new("order-item", "1");

  let error = expect_contract_violation(
    AidString::from_aggregate_id(&id).expect_err("型名のハイフンは拒む"),
    ContractRule::T11,
  );

  assert!(error.to_string().contains("T-11"));
}

// T-12: UTF-8 で 1025 バイトの aid は拒む。
#[test]
fn should_build_aid_reject_aid_longer_than_1024_bytes_with_t_12() {
  // 型名 "T"(1) + 区切り "-"(1) + 値 1023 = 1025 バイト。
  let id = TestAggregateId::new("T", &"a".repeat(1023));

  let error = expect_contract_violation(
    AidString::from_aggregate_id(&id).expect_err("1025 バイトは拒む"),
    ContractRule::T12,
  );

  assert!(error.to_string().contains("T-12"));
}

// T-12: 境界の 1024 バイトちょうどは許す（型名 1 + 区切り 1 + 値 1022）。
#[test]
fn should_build_aid_accept_aid_of_exactly_1024_bytes() {
  let id = TestAggregateId::new("T", &"a".repeat(1022));

  let aid = AidString::from_aggregate_id(&id).expect("1024 バイトは許す");

  assert_eq!(aid.as_str().len(), 1024);
}

// T-12: 長さは文字数ではなく UTF-8 バイト数で数える。
#[test]
fn should_build_aid_count_utf8_bytes_not_characters() {
  // 型名 "型"(3) + 区切り "-"(1) + 値 "あ"×340 + "a"(1) = 1025 バイト。文字数では 343 で通ってしまう。
  let value = format!("{}a", "あ".repeat(340));
  let id = TestAggregateId::new("型", &value);
  assert_eq!(format!("型-{value}").chars().count(), 343, "文字数では上限内");

  let error = expect_contract_violation(
    AidString::from_aggregate_id(&id).expect_err("バイト数では上限を超える"),
    ContractRule::T12,
  );

  assert!(error.to_string().contains("T-12"));
}

// T-11・T-12: 空の型名（`-値`）を許す。
#[test]
fn should_build_aid_allow_empty_type_name() {
  let id = TestAggregateId::new("", "123");

  let aid = AidString::from_aggregate_id(&id).expect("空の型名は許す");

  assert_eq!(aid.as_str(), "-123");
}

// T-11・T-12: 空の値（`型名-`）を許す。
#[test]
fn should_build_aid_allow_empty_value() {
  let id = TestAggregateId::new("Order", "");

  let aid = AidString::from_aggregate_id(&id).expect("空の値は許す");

  assert_eq!(aid.as_str(), "Order-");
}

// T-11・T-12: 両方が空（`-`）を許す。
#[test]
fn should_build_aid_allow_both_empty() {
  let id = TestAggregateId::new("", "");

  let aid = AidString::from_aggregate_id(&id).expect("両方が空でも許す");

  assert_eq!(aid.as_str(), "-");
}

// T-1: `AggregateId` は `Display`・serde を要求しない。`Display` を実装しない
// `TestAggregateId` が `AggregateId` を実装できること自体が、要求の不在のコンパイル検証である。
#[test]
fn should_aggregate_id_not_require_display() {
  fn assert_aggregate_id<AID: AggregateId>(_id: &AID) -> String {
    _id.type_name()
  }

  let id = TestAggregateId::new("Order", "1");
  assert_eq!(assert_aggregate_id(&id), "Order");
}
