//! DynamoDB の保存先の、ケースの分類に必要な値。

use crate::data::Case;
use crate::report::{CaseOutcome, UnverifiedReason};
use crate::runner::{PreparedCase, Target};

/// DynamoDB の保存先を表す。
///
/// 任意の能力のうち `ttl` を提供し、3 テーブルの配置のケースの対象になる。
pub const TARGET: Target = Target {
  name: "dynamodb",
  capabilities: &["ttl"],
  has_layout: true,
};

/// 未接続のDynamoDBのケースを未検証として返す。
pub fn run_case(_case: &Case, _prepared: PreparedCase) -> CaseOutcome {
  CaseOutcome::Unverified {
    reason: UnverifiedReason::NotExecuted {
      detail: "保存先が未接続（実行器の骨格）なので、実行していない".to_string(),
    },
  }
}
