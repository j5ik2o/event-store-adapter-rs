//! メモリの保存先の、ケースの分類に必要な値。

use crate::runner::Target;

/// メモリの保存先を表す。
///
/// 任意の能力を提供しない（期限切れ方式の TTL は提供しない。MEM-12）。DynamoDB の 3 テーブルの配置もない。
pub const TARGET: Target = Target {
  name: "memory",
  capabilities: &[],
  has_layout: false,
};
