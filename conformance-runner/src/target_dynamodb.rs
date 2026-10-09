//! DynamoDBの公開生成と実操作、SDK要求と別clientの保存観測を接続する。

#[cfg(feature = "dynamodb")]
mod check_requests;
#[cfg(feature = "dynamodb")]
mod execute;
mod items;
#[cfg(feature = "dynamodb")]
mod layout;
mod request;
mod response;
mod transport;

#[cfg(feature = "dynamodb")]
pub use execute::Execution;

pub use request::{RequestLayout, RequestObservation};
pub use transport::{FaultTransport, OperationGuard, OperationReport, TransportError};

#[cfg(not(feature = "dynamodb"))]
use crate::data::Case;
#[cfg(not(feature = "dynamodb"))]
use crate::report::CaseOutcome;
#[cfg(not(feature = "dynamodb"))]
use crate::runner::PreparedCase;
use crate::runner::Target;

/// DynamoDB の保存先を表す。
///
/// 任意の能力のうち `ttl` を提供し、3 テーブルの配置のケースの対象になる。
pub const TARGET: Target = Target {
  name: "dynamodb",
  capabilities: &["ttl"],
  has_layout: true,
};

/// DynamoDBが無効なケースを、理由付きの未検証として返す。
#[cfg(not(feature = "dynamodb"))]
pub fn run_case(_case: &Case, _prepared: PreparedCase) -> CaseOutcome {
  crate::case::unverified("dynamodb featureが無効なので実DynamoDBを実行できない")
}
