//! 適合テストデータ（`conformance/`）の実行器。
//!
//! データの読み込み、`manifest` の照合、generators の展開、JSON の値の比較、障害の登録と数え方、
//! 報告の組み立てを担う。保存先にはまだつながないので、値の表の `buildAid` だけを `Passed` と報告し、
//! それ以外は「未検証」か「理由のある対象外」と報告して成功に数えない。

pub mod compare;
pub mod data;
pub mod fault;
pub mod number;
pub mod observe;
pub mod report;
pub mod runner;
pub mod schema;
pub mod target_dynamodb;
pub mod target_memory;

mod aid;
