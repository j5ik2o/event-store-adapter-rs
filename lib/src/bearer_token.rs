//! `authorization: Bearer <トークン>` のメタデータの値の組み立て。Bigtable と Spanner の認証が共有する。

use tonic::metadata::AsciiMetadataValue;

use crate::error::{EventStoreError, StorageOperation};

/// トークンから `Bearer <トークン>` のメタデータの値を作る。値は機密として印を付け、ログに出さない。
///
/// 使えない文字を含むトークンは、`operation` を持つ保存先エラーにする。誤りの文言にトークンは含めない。
pub(crate) fn bearer_value(token: &str, operation: StorageOperation) -> Result<AsciiMetadataValue, EventStoreError> {
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
  Ok(value)
}
