//! Bigtable の公開生成と実操作を、エミュレーターの場面ごとの journal・head のテーブルで実行する。
//!
//! 封筒の値の表（符号化と復元）と FNV-1a 64 の値の表（行キーの接頭辞のハッシュ）はエミュレーターを使わず、
//! ライブラリの `test-hooks` の経路で実行する。
//! 機能 `bigtable` がなければ、実保存先の場面を実行できないので、理由付きの未検証にする。

#[cfg(feature = "bigtable")]
mod emulator;
#[cfg(feature = "bigtable")]
mod envelope;
#[cfg(feature = "bigtable")]
mod execute;
#[cfg(feature = "bigtable")]
mod scene;

#[cfg(feature = "bigtable")]
pub use emulator::release_emulator;
#[cfg(feature = "bigtable")]
pub use execute::run_case;

#[cfg(not(feature = "bigtable"))]
use crate::data::Case;
#[cfg(not(feature = "bigtable"))]
use crate::report::CaseOutcome;
#[cfg(not(feature = "bigtable"))]
use crate::runner::PreparedCase;
use crate::runner::Target;

/// Bigtable の保存先を表す。
///
/// 任意の能力を提供しない（期限切れ方式の TTL は提供しない。BT-13）。3 テーブルの配置のケースの対象ではない。
/// 場面の `store.envelope_limit_bytes` と `seed.configuration` を解釈し、変更フィードを求める設定も
/// 設定の型で表せる（生成が設定エラーになることを確かめる。BT-17）。
pub const TARGET: Target = Target {
  name: "bigtable",
  capabilities: &[],
  has_layout: false,
  extra_store_settings: &["envelope_limit_bytes"],
  extra_seed_items: &["configuration"],
  represents_change_feed: true,
};

/// 機能 `bigtable` が無効なケースを、理由付きの未検証として返す。
#[cfg(not(feature = "bigtable"))]
pub fn run_case(_case: &Case, _prepared: PreparedCase) -> CaseOutcome {
  crate::case::unverified("bigtable featureが無効なので実Bigtable（エミュレーター）を実行できない")
}
