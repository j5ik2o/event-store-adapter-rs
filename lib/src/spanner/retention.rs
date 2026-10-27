//! 履歴の整理（SPN-11〜SPN-13、6 章）。
//!
//! スナップショットを伴う追記の確定後に、別の SERIALIZABLE の読み書きトランザクションで、その aid の印のない履歴を
//! 降順に読み、新しい `keep` 件より古い行だけを処理する。即時削除方式では DELETE、期限切れ方式では
//! `expired_at IS NULL` の行にだけ `CURRENT_TIMESTAMP()` を 1 度だけ付ける。印を付けた行は数えず、選び直さない。
//! journal・head・snapshot_current は変えない。

use googleapis_tonic_google_spanner_v1::google::spanner::v1::Transaction;

use super::backend::{parse_seq_nr, retention_failure, seq_param, SpannerBackend};
use super::client::{storage_error, wait_for_next_attempt, SessionHandle, TxError};
use super::configuration::RetentionStyle;
use super::values::{cell, Param};
use crate::aggregate_id::AidString;
use crate::error::{EventStoreError, RetentionFailure, StorageOperation};
use crate::retention::select_expired_history_after_append;
use crate::seq_nr::SeqNr;

/// 1 つのトランザクションで処理する候補の上限（SPN-13）。
const RETENTION_BATCH: usize = 1000;

/// 保持の 1 回の試行の失敗。
enum Failure {
  /// サービスが ABORTED を返した。選び直して再試行する（SPN-11）。`phase` は中断した段階で、再試行が上限を超えたときの失敗の段階になる。
  Aborted {
    phase: &'static str,
  },
  Failed(RetentionFailure),
}

const QUERY: &str = "retention-query";
const DELETE: &str = "retention-delete";
const MARK: &str = "retention-mark";

impl<A, P> SpannerBackend<A, P> {
  /// 履歴を整理する。`seq_nr` は追記で確定した seq_nr である。
  ///
  /// 失敗は追記の成功を変えず、`RetentionFailure` として返す（SPN-13・S-4）。次のスナップショットを伴う追記でやり直す。
  /// 候補が 1000 件を超えるときは、トランザクションを分けて繰り返し、毎回選び直す。
  pub(super) async fn retain_history(
    &self,
    session: &SessionHandle,
    aid: &AidString,
    seq_nr: SeqNr,
  ) -> Result<(), RetentionFailure> {
    let Some(keep) = self.shared.options.keep_snapshot_count else {
      return Ok(());
    };
    let keep = usize::try_from(keep).unwrap_or(usize::MAX);
    let limit = self.shared.options.transaction_retry_limit;
    let mut attempt = 0;
    loop {
      match self.retain_once(session, aid, seq_nr, keep).await {
        Ok(true) => attempt = 0,
        Ok(false) => return Ok(()),
        Err(Failure::Aborted { phase }) => {
          attempt = wait_for_next_attempt(attempt, limit, StorageOperation::Append)
            .await
            .map_err(|error| retention_failure(aid, seq_nr, phase, error))?;
        }
        Err(Failure::Failed(failure)) => return Err(failure),
      }
    }
  }

  /// 1 つのトランザクションで、古い履歴を最大 1000 件処理する。まだ候補が残るなら `true` を返す。
  async fn retain_once(
    &self,
    session: &SessionHandle,
    aid: &AidString,
    seq_nr: SeqNr,
    keep: usize,
  ) -> Result<bool, Failure> {
    let transaction = session
      .begin_read_write()
      .await
      .map_err(|error| failure_of(error, aid, seq_nr, QUERY))?;
    let outcome = self
      .retain_in_transaction(session, &transaction, aid, seq_nr, keep)
      .await;
    if outcome.is_err() {
      session.rollback(&transaction).await;
    }
    outcome
  }

  async fn retain_in_transaction(
    &self,
    session: &SessionHandle,
    transaction: &Transaction,
    aid: &AidString,
    seq_nr: SeqNr,
    keep: usize,
  ) -> Result<bool, Failure> {
    let shared = &self.shared;
    let query_failure = |error| failure_of(error, aid, seq_nr, QUERY);
    let sql = format!(
      "SELECT seq_nr FROM `{}` WHERE hash_prefix = @hp AND aid = @aid AND expired_at IS NULL ORDER BY seq_nr DESC",
      shared.names.snapshot_history
    );
    let rows = session
      .query(transaction, &sql, &shared.key_params(aid))
      .await
      .map_err(query_failure)?;
    let visible: Vec<SeqNr> = rows
      .iter()
      .map(|row| {
        cell(row, 0)
          .map_err(|error| storage_error(StorageOperation::Append, error))
          .and_then(|value| parse_seq_nr(value, StorageOperation::Append))
      })
      .collect::<Result<_, EventStoreError>>()
      .map_err(|error| query_failure(TxError::Failed(error)))?;
    #[cfg(feature = "test-hooks")]
    let visible = match &shared.rpc.hooks {
      Some(hooks) => hooks
        .retention_visible_history(aid, &visible, Some(seq_nr))
        .map_err(|error| query_failure(TxError::Failed(error)))?,
      None => visible,
    };
    let mut expired = select_expired_history_after_append(&visible, Some(seq_nr), keep);
    if expired.is_empty() {
      // 処理する行がない。トランザクションを開いたままにしない。
      session.rollback(transaction).await;
      return Ok(false);
    }
    // 古い順に、最大 1000 件を処理する。
    expired.sort_unstable();
    let more = expired.len() > RETENTION_BATCH;
    expired.truncate(RETENTION_BATCH);
    let (phase, sql) = match shared.options.style {
      RetentionStyle::Delete => (
        DELETE,
        format!(
          "DELETE FROM `{}` WHERE hash_prefix = @hp AND aid = @aid AND seq_nr IN UNNEST(@seqs)",
          shared.names.snapshot_history
        ),
      ),
      RetentionStyle::Ttl { .. } => (
        MARK,
        format!(
          "UPDATE `{}` SET expired_at = CURRENT_TIMESTAMP() WHERE hash_prefix = @hp AND aid = @aid AND seq_nr IN UNNEST(@seqs) AND expired_at IS NULL",
          shared.names.snapshot_history
        ),
      ),
    };
    let write_failure = |error| failure_of(error, aid, seq_nr, phase);
    #[cfg(feature = "test-hooks")]
    if let Some(hooks) = &shared.rpc.hooks {
      match shared.options.style {
        RetentionStyle::Delete => hooks.retention_delete(aid, &expired),
        RetentionStyle::Ttl { .. } => hooks.retention_mark(aid, &expired),
      }
      .map_err(|error| write_failure(TxError::Failed(error)))?;
    }
    let [hash_prefix, aid_param] = shared.key_params(aid);
    session
      .execute_dml(
        transaction,
        &sql,
        &[
          hash_prefix,
          aid_param,
          (
            "seqs",
            Param::Int64Array(expired.iter().map(|seq_nr| seq_param(*seq_nr)).collect()),
          ),
        ],
        1,
      )
      .await
      .map_err(write_failure)?;
    #[cfg(feature = "test-hooks")]
    if let Some(hooks) = &shared.rpc.hooks {
      match hooks.before_retention_commit(aid, seq_nr) {
        Ok(()) => {}
        Err(super::SpannerCommitFault::Aborted) => return Err(Failure::Aborted { phase }),
        Err(super::SpannerCommitFault::Failed(error)) => return Err(write_failure(TxError::Failed(error))),
      }
    }
    session.commit(transaction, Vec::new()).await.map_err(write_failure)?;
    Ok(more)
  }
}

fn failure_of(error: TxError, aid: &AidString, seq_nr: SeqNr, phase: &'static str) -> Failure {
  match error {
    TxError::Aborted => Failure::Aborted { phase },
    TxError::Failed(error) => Failure::Failed(retention_failure(aid, seq_nr, phase, error)),
  }
}
