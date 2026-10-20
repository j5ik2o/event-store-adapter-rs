//! 要求ごとの認証のメタデータ（`authorization: Bearer <トークン>`）。
//!
//! 認証は規範の対象の外で、送る要求の種類と順番は変えない。取得元を渡さなければ、メタデータは付けない。

use std::sync::Arc;

use async_trait::async_trait;
use tonic::metadata::AsciiMetadataValue;

use crate::error::{EventStoreError, StorageOperation};

const AUTHORIZATION: &str = "authorization";

/// 利用者が用意する、アクセストークンの取得元。
///
/// 取得元を渡したストアは、データ API と管理 API のすべての要求の直前に [`access_token`](Self::access_token) を
/// 呼び、返ったトークンを `authorization: Bearer <トークン>` として要求に付ける。
///
/// # 取得元の責任とストアの約束
///
/// - トークンの保持と更新は、取得元の責任である。期限が切れる前に新しいトークンを返すのは取得元の仕事である。
/// - ストアは、要求を送る直前に毎回呼ぶ。
/// - ストアは、トークンを記憶しない。期限も判断しない。
/// - 取得元はトークンだけを返す。`Bearer ` は付けない。
/// - 返せる文字は、印字できる ASCII（`0x20` から `0x7E`）に限る。
/// - ストアのクローンから並行して呼ばれうる。
///
/// # 失敗の扱い
///
/// 取得元が失敗を返したとき、またはメタデータの値にできないトークン（使えない文字を含むなど）を返したときは、
/// その要求を送らずに、その操作が [`EventStoreError::Storage`] になる。ライブラリは取得元の呼び出しをやり直さない。
/// 取得元の誤りは、`Storage` の `source` にそのまま入る。
///
/// # 実装の例
///
/// ```
/// use std::sync::Arc;
///
/// use async_trait::async_trait;
/// use event_store_adapter_rs::BigtableTokenSource;
///
/// struct FixedToken(String);
///
/// #[async_trait]
/// impl BigtableTokenSource for FixedToken {
///   async fn access_token(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
///     Ok(self.0.clone())
///   }
/// }
///
/// let _source: Arc<dyn BigtableTokenSource> = Arc::new(FixedToken("token".to_string()));
/// ```
#[async_trait]
pub trait BigtableTokenSource: Send + Sync {
  /// アクセストークンを返す。
  async fn access_token(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>>;
}

/// 要求にメタデータを付ける。トークンは持たず、要求のたびに取得元を呼ぶ。
#[derive(Clone, Default)]
pub(super) struct Authorizer {
  token_source: Option<Arc<dyn BigtableTokenSource>>,
  #[cfg(feature = "test-hooks")]
  hooks: Option<Arc<dyn super::BigtableTestHooks>>,
}

impl Authorizer {
  pub(super) fn new(token_source: Arc<dyn BigtableTokenSource>) -> Self {
    Self {
      token_source: Some(token_source),
      #[cfg(feature = "test-hooks")]
      hooks: None,
    }
  }

  #[cfg(feature = "test-hooks")]
  pub(super) fn with_hooks(mut self, hooks: Option<Arc<dyn super::BigtableTestHooks>>) -> Self {
    self.hooks = hooks;
    self
  }

  /// `message` を要求にする。取得元があれば、取得元を 1 回呼び、`authorization: Bearer <トークン>` を付ける。
  ///
  /// 取得元の失敗と、メタデータの値にできないトークンは、`operation` を持つ保存先エラーにする。
  /// 誤りの文言にトークンは含めない。
  pub(super) async fn authorize<T>(
    &self,
    message: T,
    operation: StorageOperation,
  ) -> Result<tonic::Request<T>, EventStoreError> {
    let Some(token_source) = &self.token_source else {
      return Ok(tonic::Request::new(message));
    };
    let token = token_source
      .access_token()
      .await
      .map_err(|source| EventStoreError::Storage { operation, source })?;
    // gRPC の ASCII のメタデータの値は、印字できる ASCII に限られる。tonic の変換はタブと 0x80 以上も通すので、先に確かめる。
    if !token.bytes().all(|byte| (0x20..=0x7e).contains(&byte)) {
      return Err(EventStoreError::Storage {
        operation,
        source: Box::new(std::io::Error::other(
          "the access token contains characters that cannot be sent as gRPC metadata",
        )),
      });
    }
    let mut value =
      AsciiMetadataValue::try_from(format!("Bearer {token}")).map_err(|error| EventStoreError::Storage {
        operation,
        source: Box::new(error),
      })?;
    value.set_sensitive(true);
    let mut request = tonic::Request::new(message);
    request.metadata_mut().insert(AUTHORIZATION, value);
    Ok(request)
  }

  /// 送る直前の要求を、フックへ知らせる。要求に実際に付いた `authorization` の値を渡す。
  #[cfg(feature = "test-hooks")]
  pub(super) fn observe<T>(&self, kind: super::BigtableRequestKind, request: &tonic::Request<T>) {
    if let Some(hooks) = &self.hooks {
      let authorization = request
        .metadata()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
      hooks.before_request(kind, authorization);
    }
  }
}

#[cfg(test)]
#[path = "auth_test.rs"]
mod tests;
