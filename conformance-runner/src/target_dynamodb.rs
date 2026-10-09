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

use crate::data::Case;
use crate::report::CaseOutcome;
use crate::runner::{PreparedCase, Target};

/// DynamoDB の保存先を表す。
///
/// 任意の能力のうち `ttl` を提供し、3 テーブルの配置のケースの対象になる。
pub const TARGET: Target = Target {
  name: "dynamodb",
  capabilities: &["ttl"],
  has_layout: true,
};

/// 単独のケースを実Localで実行する。全宣言は同じExecutionを保持して実行する。
pub fn run_case(case: &Case, prepared: PreparedCase) -> CaseOutcome {
  #[cfg(feature = "dynamodb")]
  {
    match Execution::start() {
      Ok(execution) => execution.run_case(case, prepared),
      Err(e) => crate::case::unverified(&e),
    }
  }
  #[cfg(not(feature = "dynamodb"))]
  {
    let _ = (case, prepared);
    crate::case::unverified("dynamodb featureが無効なので実DynamoDBを実行できない")
  }
}
