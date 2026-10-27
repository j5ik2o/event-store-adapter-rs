//! 要求ごとの認証のメタデータ（`authorization: Bearer <トークン>`）。
//!
//! 認証は規範の対象の外で、送る要求の種類と順番は変えない。取得元を渡さなければ、メタデータは付けない。

use std::sync::Arc;

use async_trait::async_trait;

use crate::bearer_token::bearer_value;
use crate::error::{EventStoreError, StorageOperation};

const AUTHORIZATION: &str = "authorization";
const RESOURCE_PREFIX: &str = "google-cloud-resource-prefix";

/// 利用者が用意する、アクセストークンの取得元。
///
/// 取得元を [`SpannerConnection::with_token_source`](super::SpannerConnection::with_token_source) で渡した接続を使う
/// ストアと初期化は、データ API と管理 API のすべての要求の直前に [`access_token`](Self::access_token) を呼び、
/// 返ったトークンを `authorization: Bearer <トークン>` として要求に付ける。
///
/// # 取得元の責任とストアの約束
///
/// - トークンの保持と更新は、取得元の責任である。期限が切れる前に新しいトークンを返すのは取得元の仕事である。
/// - ストアは、要求を送る直前に毎回呼ぶ。トークンを記憶せず、期限も判断しない。
/// - 取得元はトークンだけを返す。`Bearer ` は付けない。
/// - 返せる文字は、印字できる ASCII（`0x20` から `0x7E`）に限る。
/// - ストアのクローンから並行して呼ばれうる。
///
/// # 失敗の扱い
///
/// 取得元が失敗を返したとき、またはメタデータの値にできないトークンを返したときは、その要求を送らずに、
/// その操作が [`EventStoreError::Storage`] になる。ライブラリは取得元の呼び出しをやり直さない。
/// 取得元の誤りは、`Storage` の `source` にそのまま入る。
///
/// # 実装の例
///
/// ```
/// use std::sync::Arc;
///
/// use async_trait::async_trait;
/// use event_store_adapter_rs::SpannerTokenSource;
///
/// struct FixedToken(String);
///
/// #[async_trait]
/// impl SpannerTokenSource for FixedToken {
///   async fn access_token(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
///     Ok(self.0.clone())
///   }
/// }
///
/// let _source: Arc<dyn SpannerTokenSource> = Arc::new(FixedToken("token".to_string()));
/// ```
#[async_trait]
pub trait SpannerTokenSource: Send + Sync {
  /// アクセストークンを返す。
  async fn access_token(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>>;
}

/// 要求にメタデータを付ける。トークンは持たず、要求のたびに取得元を呼ぶ。
#[derive(Clone, Default)]
pub(crate) struct Authorizer {
  token_source: Option<Arc<dyn SpannerTokenSource>>,
}

impl Authorizer {
  pub(crate) fn new(token_source: Option<Arc<dyn SpannerTokenSource>>) -> Self {
    Self { token_source }
  }

  /// `message` を要求にする。データベースのリソース名を `google-cloud-resource-prefix` に付け、
  /// 取得元があれば、取得元を 1 回呼び、`authorization: Bearer <トークン>` を付ける。
  ///
  /// 取得元の失敗と、メタデータの値にできないトークンは、`operation` を持つ保存先エラーにする。
  /// 誤りの文言にトークンは含めない。
  pub(crate) async fn authorize<T>(
    &self,
    message: T,
    database: &str,
    operation: StorageOperation,
  ) -> Result<tonic::Request<T>, EventStoreError> {
    let mut request = tonic::Request::new(message);
    let prefix = tonic::metadata::AsciiMetadataValue::try_from(database).map_err(|error| EventStoreError::Storage {
      operation,
      source: Box::new(error),
    })?;
    request.metadata_mut().insert(RESOURCE_PREFIX, prefix);
    let Some(token_source) = &self.token_source else {
      return Ok(request);
    };
    let token = token_source
      .access_token()
      .await
      .map_err(|source| EventStoreError::Storage { operation, source })?;
    request
      .metadata_mut()
      .insert(AUTHORIZATION, bearer_value(&token, operation)?);
    Ok(request)
  }
}
