//! Bigtable の移行の接続。エミュレーターは平文で認証なし、本番は TLS で Application Default Credentials を使う。
//!
//! Google の認証のライブラリ（`gcp_auth`）と tonic の TLS の機能は、ライブラリ本体ではなくこの道具だけが持つ。
//! トークンの保持と更新は `gcp_auth` に任せ、この道具は要求のたびに `gcp_auth` からトークンを受け取って渡すだけである。

use std::sync::Arc;

use async_trait::async_trait;
use event_store_adapter_rs::{BigtableConnection, BigtableTokenSource};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

/// 本番の Cloud Bigtable のデータ API。
const DATA_ENDPOINT: &str = "https://bigtable.googleapis.com";
/// 本番の Cloud Bigtable の管理 API。新しいテーブルの列ファミリの照合（`GetTable`）に使う。
const ADMIN_ENDPOINT: &str = "https://bigtableadmin.googleapis.com";
/// データ API の読み書きと、管理 API の `GetTable` に要る OAuth のスコープ。
const SCOPES: &[&str] = &[
  "https://www.googleapis.com/auth/bigtable.data",
  "https://www.googleapis.com/auth/bigtable.admin.table",
];

/// 接続先。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BigtableEndpoint {
  /// エミュレーター（`HOST:PORT`）。平文で接続し、認証は付けない。
  Emulator(String),
  /// 本番の Cloud Bigtable。TLS で接続し、Application Default Credentials でトークンを取得する。
  Production,
}

/// `gcp_auth` が見つけた Application Default Credentials のトークンを、ライブラリへ渡す。
struct AdcTokenSource {
  provider: Arc<dyn gcp_auth::TokenProvider>,
}

#[async_trait]
impl BigtableTokenSource for AdcTokenSource {
  async fn access_token(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let token = self.provider.token(SCOPES).await?;
    Ok(token.as_str().to_string())
  }
}

async fn tls_channel(uri: &'static str) -> Result<Channel, tonic::transport::Error> {
  Endpoint::from_static(uri)
    .tls_config(ClientTlsConfig::new().with_native_roots())?
    .connect()
    .await
}

/// 接続と、トークンの取得元（本番だけ）を作る。
pub async fn connect(
  endpoint: &BigtableEndpoint,
) -> Result<(BigtableConnection, Option<Arc<dyn BigtableTokenSource>>), Box<dyn std::error::Error>> {
  match endpoint {
    BigtableEndpoint::Emulator(host) => {
      let channel = Endpoint::from_shared(format!("http://{host}"))?.connect().await?;
      Ok((
        BigtableConnection {
          data: channel.clone(),
          admin: channel,
        },
        None,
      ))
    }
    BigtableEndpoint::Production => {
      let provider = gcp_auth::provider().await?;
      let connection = BigtableConnection {
        data: tls_channel(DATA_ENDPOINT).await?,
        admin: tls_channel(ADMIN_ENDPOINT).await?,
      };
      Ok((connection, Some(Arc::new(AdcTokenSource { provider }))))
    }
  }
}
