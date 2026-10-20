//! 要求ごとの認証のメタデータ（`authorization: Bearer <トークン>`）の付与の試験。
//! `auth.rs` の末尾から `#[cfg(test)] #[path = "auth_test.rs"] mod tests;` で結ぶ。
//! 通信を行わない。実際の要求に付くことと、データ API・管理 API の全要求への配線は、
//! `lib/tests/bigtable_auth_test.rs`（エミュレーター）で確かめる。

use std::sync::Arc;

use async_trait::async_trait;

use super::*;
use crate::error::{EventStoreError, StorageOperation};

/// 呼ばれるたびに、決まった文字列をトークンとして返す取得元。
struct FixedToken(&'static str);

#[async_trait]
impl BigtableTokenSource for FixedToken {
  async fn access_token(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    Ok(self.0.to_string())
  }
}

/// いつも失敗する取得元。
struct UnavailableToken;

#[async_trait]
impl BigtableTokenSource for UnavailableToken {
  async fn access_token(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    Err(Box::new(std::io::Error::other("token unavailable")))
  }
}

fn authorizer_issuing(token: &'static str) -> Authorizer {
  Authorizer::new(Arc::new(FixedToken(token)))
}

#[tokio::test]
async fn should_bigtable_authorize_add_no_authorization_without_a_token_source() {
  let request = Authorizer::default()
    .authorize((), StorageOperation::ReadConfiguration)
    .await
    .expect("取得元がなければ、そのまま要求にできる");

  assert!(
    request.metadata().get("authorization").is_none(),
    "取得元なしでは authorization を付けない（空の値も付けない）"
  );
}

#[tokio::test]
async fn should_bigtable_authorize_add_the_bearer_token_returned_by_the_source() {
  let request = authorizer_issuing("abc")
    .authorize((), StorageOperation::Append)
    .await
    .expect("取得元があれば、メタデータを付けた要求になる");

  let value = request.metadata().get("authorization").expect("authorization が付く");
  assert_eq!(value.to_str().expect("見える ASCII の値"), "Bearer abc");
}

#[tokio::test]
async fn should_bigtable_authorize_mark_the_authorization_value_as_sensitive() {
  let request = authorizer_issuing("abc")
    .authorize((), StorageOperation::Append)
    .await
    .expect("取得元があれば、メタデータを付けた要求になる");

  let value = request.metadata().get("authorization").expect("authorization が付く");
  assert!(value.is_sensitive(), "トークンをデバッグ表示に出さない");
}

#[tokio::test]
async fn should_bigtable_authorize_fail_with_the_operation_and_keep_the_source_error_when_the_source_fails() {
  let result = Authorizer::new(Arc::new(UnavailableToken))
    .authorize((), StorageOperation::LoadEvents)
    .await;

  match result {
    Err(EventStoreError::Storage { operation, source }) => {
      assert_eq!(operation, StorageOperation::LoadEvents, "送るはずだった要求の操作");
      assert_eq!(source.to_string(), "token unavailable", "取得元の誤りをそのまま持つ");
    }
    other => panic!("取得元の失敗は保存先エラーになる: {other:?}"),
  }
}

#[tokio::test]
async fn should_bigtable_authorize_reject_a_token_that_cannot_be_sent_as_metadata() {
  let cases = [
    ("改行を含む", "abc\ndef"),
    ("復帰を含む", "abc\rdef"),
    ("タブを含む", "abc\tdef"),
    ("DEL を含む", "abc\u{7f}"),
    ("ASCII の外の文字を含む", "トークン"),
  ];

  for (label, token) in cases {
    let result = authorizer_issuing(token).authorize((), StorageOperation::Append).await;

    assert!(
      matches!(
        result,
        Err(EventStoreError::Storage {
          operation: StorageOperation::Append,
          ..
        })
      ),
      "{label}: 送らずに、その操作の保存先エラーにする: {result:?}"
    );
  }
}

#[tokio::test]
async fn should_bigtable_authorize_not_expose_a_rejected_token_in_the_error() {
  const SECRET: &str = "s3cr3t-token-value\n";

  let error = authorizer_issuing(SECRET)
    .authorize((), StorageOperation::Append)
    .await
    .expect_err("使えない文字を含むトークンは拒否する");

  let source = std::error::Error::source(&error)
    .map(ToString::to_string)
    .unwrap_or_default();
  let shown = format!("{error} {error:?} {source}");
  assert!(
    !shown.contains("s3cr3t-token-value"),
    "誤りの文言・デバッグ表示・原因にトークンを含めない: {shown}"
  );
}
