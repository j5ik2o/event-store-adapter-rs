//! Spanner の公開生成と実操作を、エミュレーターの場面ごとのデータベース（5 テーブル）で実行する。
//!
//! FNV-1a 64 の値の表（行キーの接頭辞のハッシュ）はエミュレーターを使わず、ライブラリの `test-hooks` の経路で実行する。
//! 機能 `spanner` がなければ、実保存先の場面を実行できないので、理由付きの未検証にする。

#[cfg(feature = "spanner")]
mod emulator;
#[cfg(feature = "spanner")]
mod execute;
#[cfg(feature = "spanner")]
mod scene;

#[cfg(feature = "spanner")]
pub use emulator::release_emulator;
#[cfg(feature = "spanner")]
pub use execute::run_case;

#[cfg(not(feature = "spanner"))]
use crate::data::Case;
#[cfg(not(feature = "spanner"))]
use crate::report::CaseOutcome;
#[cfg(not(feature = "spanner"))]
use crate::runner::PreparedCase;
use crate::runner::Target;

/// Spanner の保存先を表す。
///
/// 任意の能力を提供しない（期限切れ方式は `ttl_grace_days` の設定で表し、場面の `requires` の語は持たない）。
/// 3 テーブルの配置のケースの対象ではない。場面の `store.ttl_grace_days`・`store.transaction_retry_limit` と、
/// `seed.configuration`・`seed.row_deletion_policy_days` を解釈し、変更フィードを求める設定も設定の型で表せる
/// （生成が設定エラーになることを確かめる。SPN-17）。
pub const TARGET: Target = Target {
  name: "spanner",
  capabilities: &[],
  has_layout: false,
  extra_store_settings: &["ttl_grace_days", "transaction_retry_limit"],
  extra_seed_items: &["configuration", "row_deletion_policy_days"],
  represents_change_feed: true,
};

/// 機能 `spanner` が無効なケースを、理由付きの未検証として返す。
#[cfg(not(feature = "spanner"))]
pub fn run_case(_case: &Case, _prepared: PreparedCase) -> CaseOutcome {
  crate::case::unverified("spanner featureが無効なので実Spanner（エミュレーター）を実行できない")
}
