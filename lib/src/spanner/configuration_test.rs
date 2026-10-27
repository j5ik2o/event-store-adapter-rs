//! 行削除ポリシーの式の解析（SPN-16）の試験。
//! `configuration.rs` の末尾から `#[cfg(test)] #[path = "configuration_test.rs"] mod tests;` で結ぶ。
//! 通信を行わない純粋な検査だけを扱う。エミュレーターを使う照合は `lib/tests/spanner_test.rs` に置く。
//!
//! 前提にしている関数: `policy_days(expression: &str) -> Option<u64>`。情報スキーマの
//! `TABLES.ROW_DELETION_POLICY_EXPRESSION` の値から、`expired_at` 列の `DAY` 単位の日数を取り出す。
//! `expired_at` 列と `DAY` 単位の式でなければ `None` を返す。

use super::*;

// 大文字小文字と空白の違いは、同じ式として受け入れる。
#[test]
fn should_policy_days_read_the_days_of_the_expression_the_service_returns() {
  assert_eq!(policy_days("OLDER_THAN(expired_at, INTERVAL 7 DAY)"), Some(7));
}

#[test]
fn should_policy_days_ignore_letter_case_and_extra_spaces_of_the_expression() {
  assert_eq!(policy_days("older_than( expired_at , interval 7 day )"), Some(7));
  assert_eq!(policy_days("  Older_Than(EXPIRED_AT,INTERVAL   30   Day)  "), Some(30));
}

// 列・日数・単位が違う式は、7 日のクライアントと一致しない式として扱う。
#[test]
fn should_policy_days_reject_an_expression_over_another_column() {
  assert_eq!(policy_days("OLDER_THAN(created_at, INTERVAL 7 DAY)"), None);
}

#[test]
fn should_policy_days_reject_an_expression_in_another_unit() {
  assert_eq!(policy_days("OLDER_THAN(expired_at, INTERVAL 7 HOUR)"), None);
}

#[test]
fn should_policy_days_return_the_different_days_so_that_the_caller_can_report_a_mismatch() {
  let days = policy_days("OLDER_THAN(expired_at, INTERVAL 8 DAY)");

  assert_eq!(days, Some(8));
  assert_ne!(days, Some(7), "クライアントの 7 日とは一致しない");
}
