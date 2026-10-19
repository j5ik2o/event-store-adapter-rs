//! SQLite の公開生成と実操作を、場面ごとの一時ファイルのデータベースで実行する。
//!
//! 機能 `sqlite` がなければ、SQLite の実保存先を実行できないので、理由付きの未検証にする。

#[cfg(feature = "sqlite")]
mod execute;
#[cfg(feature = "sqlite")]
mod stores;

#[cfg(feature = "sqlite")]
pub use execute::run_case;

#[cfg(not(feature = "sqlite"))]
use crate::data::Case;
#[cfg(not(feature = "sqlite"))]
use crate::report::CaseOutcome;
#[cfg(not(feature = "sqlite"))]
use crate::runner::PreparedCase;
use crate::runner::Target;

/// SQLite の保存先を表す。
///
/// 任意の能力を提供しない（期限切れ方式の TTL は提供しない。SQ-11）。3 テーブルの配置のケースの対象ではない。
/// 場面の `store.lock_wait_milliseconds` と `seed.configuration` を解釈し、変更フィードを求める設定も
/// 設定の型で表せる（生成が設定エラーになることを確かめる。SQ-20）。
pub const TARGET: Target = Target {
  name: "sqlite",
  capabilities: &[],
  has_layout: false,
  extra_store_settings: &["lock_wait_milliseconds"],
  extra_seed_items: &["configuration"],
  represents_change_feed: true,
};

/// 機能 `sqlite` が無効なケースを、理由付きの未検証として返す。
#[cfg(not(feature = "sqlite"))]
pub fn run_case(_case: &Case, _prepared: PreparedCase) -> CaseOutcome {
  crate::case::unverified("sqlite featureが無効なので実SQLiteを実行できない")
}
