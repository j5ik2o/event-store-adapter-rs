//! Spanner のデータ API の呼び出し（セッション、トランザクション、SQL の実行、コミット）。
//!
//! セッションは公開操作 1 回ごとに作り、終わりに削除する。プールも多重化セッションも持たない。
//! 読み書きトランザクションは SERIALIZABLE を明示し、読み取りは strong の読み取り専用トランザクションで行う（SPN-1）。
//! ABORTED だけを、上限まで待ってから再試行できる形で返す（SPN-7）。

#[cfg(feature = "test-hooks")]
use std::sync::Arc;
use std::time::Duration;

use googleapis_tonic_google_spanner_v1::google::spanner::v1::spanner_client::SpannerClient;
use googleapis_tonic_google_spanner_v1::google::spanner::v1::transaction_options::{
  read_only::TimestampBound, IsolationLevel, Mode, ReadOnly, ReadWrite,
};
use googleapis_tonic_google_spanner_v1::google::spanner::v1::{
  commit_request, result_set_stats, transaction_selector, BeginTransactionRequest, CommitRequest, CreateSessionRequest,
  DeleteSessionRequest, ExecuteSqlRequest, Mutation, RollbackRequest, Session, Transaction, TransactionOptions,
  TransactionSelector,
};
use prost_types::Value;
use tonic::transport::Channel;
use tonic::Code;

use super::auth::Authorizer;
use super::values::{merge_partial_result_sets, params_of, Param};
use crate::error::{EventStoreError, StorageOperation};

/// 読み書きトランザクションの試行の失敗。ABORTED だけを区別する。
pub(crate) enum TxError {
  /// サービスが ABORTED を返した、または試験用フックが ABORTED を差し込んだ。再試行できる（SPN-7）。
  Aborted,
  /// 再試行しない失敗。
  Failed(EventStoreError),
}

impl From<EventStoreError> for TxError {
  fn from(error: EventStoreError) -> Self {
    TxError::Failed(error)
  }
}

/// 保存先エラーにする。
pub(crate) fn storage_error(
  operation: StorageOperation,
  source: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> EventStoreError {
  EventStoreError::Storage {
    operation,
    source: source.into(),
  }
}

/// 読み取り専用の経路の失敗を保存先エラーにする。
///
/// 読み取り専用の経路では ABORTED は起きないので、起きたときも保存先エラーにする。
pub(crate) fn tx_failure(error: TxError, operation: StorageOperation) -> EventStoreError {
  match error {
    TxError::Failed(error) => error,
    TxError::Aborted => storage_error(operation, "a read-only transaction was aborted"),
  }
}

fn status_failure(operation: StorageOperation, status: tonic::Status) -> TxError {
  if status.code() == Code::Aborted {
    TxError::Aborted
  } else {
    TxError::Failed(storage_error(operation, status))
  }
}

/// 接続（利用者の `Channel`）と、要求ごとの認証。クローンは同じ `Channel` を共有する。
#[derive(Clone)]
pub(crate) struct Rpc {
  pub(crate) channel: Channel,
  pub(crate) authorizer: Authorizer,
  /// `projects/<project>/instances/<instance>/databases/<database>`。
  pub(crate) database: String,
  #[cfg(feature = "test-hooks")]
  pub(crate) hooks: Option<Arc<dyn super::SpannerTestHooks>>,
}

/// 1 つの公開操作が使うセッション。終わりに [`SessionHandle::close`] で削除する。
pub(crate) struct SessionHandle {
  rpc: Rpc,
  name: String,
  operation: StorageOperation,
}

impl Rpc {
  /// セッションを作る。失敗は `operation` の保存先エラーにする。
  pub(crate) async fn open_session(&self, operation: StorageOperation) -> Result<SessionHandle, EventStoreError> {
    let request = self
      .authorizer
      .authorize(
        CreateSessionRequest {
          database: self.database.clone(),
          session: Some(Session::default()),
        },
        &self.database,
        operation,
      )
      .await?;
    let session = SpannerClient::new(self.channel.clone())
      .create_session(request)
      .await
      .map_err(|status| storage_error(operation, status))?
      .into_inner();
    Ok(SessionHandle {
      rpc: self.clone(),
      name: session.name,
      operation,
    })
  }
}

pub(crate) fn read_only_strong() -> TransactionOptions {
  TransactionOptions {
    mode: Some(Mode::ReadOnly(ReadOnly {
      timestamp_bound: Some(TimestampBound::Strong(true)),
      ..Default::default()
    })),
    ..Default::default()
  }
}

pub(crate) fn read_write_serializable() -> TransactionOptions {
  TransactionOptions {
    mode: Some(Mode::ReadWrite(ReadWrite::default())),
    isolation_level: IsolationLevel::Serializable as i32,
    ..Default::default()
  }
}

impl SessionHandle {
  fn client(&self) -> SpannerClient<Channel> {
    SpannerClient::new(self.rpc.channel.clone())
  }

  async fn request<T>(&self, message: T) -> Result<tonic::Request<T>, EventStoreError> {
    self
      .rpc
      .authorizer
      .authorize(message, &self.rpc.database, self.operation)
      .await
  }

  /// トランザクションを始める。
  async fn begin(&self, options: TransactionOptions) -> Result<Transaction, TxError> {
    #[cfg(feature = "test-hooks")]
    if let Some(hooks) = &self.rpc.hooks {
      hooks.before_begin_transaction(&options);
    }
    let request = self
      .request(BeginTransactionRequest {
        session: self.name.clone(),
        options: Some(options),
        ..Default::default()
      })
      .await?;
    self
      .client()
      .begin_transaction(request)
      .await
      .map(tonic::Response::into_inner)
      .map_err(|status| status_failure(self.operation, status))
  }

  /// strong の読み取り専用トランザクションを始める。head と、続けて読む行は、このトランザクションの同じ読み取り時点で読む（SPN-9・SPN-10）。
  pub(crate) async fn begin_read_only(&self) -> Result<Transaction, TxError> {
    self.begin(read_only_strong()).await
  }

  /// SERIALIZABLE の読み書きトランザクションを始める（SPN-1）。
  pub(crate) async fn begin_read_write(&self) -> Result<Transaction, TxError> {
    self.begin(read_write_serializable()).await
  }

  fn selector(transaction: &Transaction) -> Option<TransactionSelector> {
    Some(TransactionSelector {
      selector: Some(transaction_selector::Selector::Id(transaction.id.clone())),
    })
  }

  /// ExecuteStreamingSql で問い合わせ、分割された値を結合した行を返す。途中で切れたら、それまでの結果を捨てて失敗にする。
  pub(crate) async fn query(
    &self,
    transaction: &Transaction,
    sql: &str,
    params: &[(&str, Param)],
  ) -> Result<Vec<Vec<Value>>, TxError> {
    let (params, param_types) = params_of(params);
    let request = self
      .request(ExecuteSqlRequest {
        session: self.name.clone(),
        transaction: Self::selector(transaction),
        sql: sql.to_owned(),
        params,
        param_types,
        ..Default::default()
      })
      .await?;
    let mut stream = self
      .client()
      .execute_streaming_sql(request)
      .await
      .map_err(|status| status_failure(self.operation, status))?
      .into_inner();
    let mut responses = Vec::new();
    loop {
      match stream.message().await {
        Ok(Some(response)) => responses.push(response),
        Ok(None) => break,
        Err(status) => return Err(status_failure(self.operation, status)),
      }
    }
    merge_partial_result_sets(responses).map_err(|error| TxError::Failed(storage_error(self.operation, error)))
  }

  /// DML を 1 つ実行し、影響した行数を返す。`seqno` はトランザクションの中で 1 から増やす。
  pub(crate) async fn execute_dml(
    &self,
    transaction: &Transaction,
    sql: &str,
    params: &[(&str, Param)],
    seqno: i64,
  ) -> Result<i64, TxError> {
    let (params, param_types) = params_of(params);
    let request = self
      .request(ExecuteSqlRequest {
        session: self.name.clone(),
        transaction: Self::selector(transaction),
        sql: sql.to_owned(),
        params,
        param_types,
        seqno,
        ..Default::default()
      })
      .await?;
    let result_set = self
      .client()
      .execute_sql(request)
      .await
      .map_err(|status| status_failure(self.operation, status))?
      .into_inner();
    Ok(match result_set.stats.and_then(|stats| stats.row_count) {
      Some(result_set_stats::RowCount::RowCountExact(count)) => count,
      _ => 0,
    })
  }

  /// `mutations` をコミットする。DML だけのトランザクションは空の `mutations` で呼ぶ。
  pub(crate) async fn commit(&self, transaction: &Transaction, mutations: Vec<Mutation>) -> Result<(), TxError> {
    let request = self
      .request(CommitRequest {
        session: self.name.clone(),
        transaction: Some(commit_request::Transaction::TransactionId(transaction.id.clone())),
        mutations,
        ..Default::default()
      })
      .await?;
    self
      .client()
      .commit(request)
      .await
      .map(|_| ())
      .map_err(|status| status_failure(self.operation, status))
  }

  /// トランザクションを捨てる。失敗は無視する（トランザクションは、サービス側でも期限切れになる）。
  pub(crate) async fn rollback(&self, transaction: &Transaction) {
    let Ok(request) = self
      .request(RollbackRequest {
        session: self.name.clone(),
        transaction_id: transaction.id.clone(),
      })
      .await
    else {
      return;
    };
    let _ = self.client().rollback(request).await;
  }

  /// セッションを削除する。失敗は無視する（セッションは、サービス側でも期限切れになる）。
  pub(crate) async fn close(self) {
    let Ok(request) = self
      .request(DeleteSessionRequest {
        name: self.name.clone(),
      })
      .await
    else {
      return;
    };
    let _ = self.client().delete_session(request).await;
  }
}

/// ABORTED の後に待って、次の試行の番号を返す。再試行の回数が上限（初回を数えない）に達していたら保存先エラーにする（SPN-7）。
///
/// 待つ時間は、10 ミリ秒から倍にして、1 秒を超えない。エミュレーターは重なった読み書きトランザクションを中断するので、
/// 待たずにやり直すと、同時の追記の結果が時刻に左右される。
pub(crate) async fn wait_for_next_attempt(
  attempt: u32,
  limit: u32,
  operation: StorageOperation,
) -> Result<u32, EventStoreError> {
  if attempt >= limit {
    return Err(storage_error(
      operation,
      "the transaction was aborted more often than the retry limit allows",
    ));
  }
  let next = attempt + 1;
  let millis = (10_u64 << (next - 1).min(7)).min(1_000);
  tokio::time::sleep(Duration::from_millis(millis)).await;
  Ok(next)
}
