//! DynamoDB の保存先の、ケースの分類に必要な値。

use crate::runner::Target;

/// DynamoDB の保存先を表す。
///
/// 任意の能力のうち `ttl` を提供し、3 テーブルの配置のケースの対象になる。
pub const TARGET: Target = Target {
  name: "dynamodb",
  capabilities: &["ttl"],
  has_layout: true,
};
