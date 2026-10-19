//! 履歴の整理（BT-11・BT-12・BT-13、7 章）。
//!
//! スナップショットを伴う追記の確定後に、head の位置 h を固定して journal の 1〜h で active な履歴を読み、
//! 新しい `keep` 件より古い行だけを `retired` にする。`retired` にするとき、履歴の封筒を削除し、event のセルは残す。
//! 遅れた書き手は、event のセルが既にあるので、`retired` の履歴を再作成できない（BT-5）。

use super::backend::{invalid_data, retention_failure, BigtableBackend, BigtableShared};
use super::keys::{journal_range, journal_row_key, journal_seq_nr};
use super::rows::{
  chain, column, delete_column, latest_cell, latest_cell_equals, replace_cell, FAMILY_HISTORY, HISTORY_ENVELOPE,
  HISTORY_STATE, STATE_ACTIVE, STATE_RETIRED,
};
use crate::aggregate_id::AidString;
use crate::error::{EventStoreError, RetentionFailure, StorageOperation};
use crate::retention::select_expired_history_after_append;
use crate::seq_nr::SeqNr;

impl<A, P> BigtableBackend<A, P> {
  /// 履歴を整理する。h は追記で確定した seq_nr である。
  ///
  /// 失敗は追記の成功を変えず、`RetentionFailure` として返す（BT-13・S-4）。次のスナップショットを伴う追記でやり直す。
  pub(super) async fn retain_history(
    &self,
    aid: &AidString,
    prefix: &[u8],
    h: SeqNr,
    keep: usize,
  ) -> Result<(), RetentionFailure> {
    let query_failure = |error| retention_failure(aid, h, "retention-query", error);
    let delete_failure = |error| retention_failure(aid, h, "retention-delete", error);
    let history = self.shared.scan_history(prefix, h).await.map_err(query_failure)?;
    let visible: Vec<SeqNr> = history
      .iter()
      .filter(|(_, active)| *active)
      .map(|(seq_nr, _)| *seq_nr)
      .collect();
    #[cfg(feature = "test-hooks")]
    let visible = if let Some(hooks) = &self.shared.hooks {
      hooks
        .retention_visible_history(aid, &visible, Some(h))
        .map_err(query_failure)?
    } else {
      visible
    };
    let expired = select_expired_history_after_append(&visible, Some(h), keep);
    if expired.is_empty() {
      return Ok(());
    }
    #[cfg(feature = "test-hooks")]
    if let Some(hooks) = &self.shared.hooks {
      hooks.retention_delete(aid, &expired).map_err(delete_failure)?;
    }
    for expired_seq_nr in expired {
      self
        .shared
        .retire_history(prefix, expired_seq_nr)
        .await
        .map_err(delete_failure)?;
    }
    Ok(())
  }
}

impl BigtableShared {
  /// journal の `[1, h]` で、履歴の状態を持つ行を読む。`(seq_nr, active か)` を seq_nr の昇順で返す。
  ///
  /// 状態だけを読むので、履歴の payload は転送しない（BT-11）。
  async fn scan_history(&self, prefix: &[u8], h: SeqNr) -> Result<Vec<(SeqNr, bool)>, EventStoreError> {
    let operation = StorageOperation::Append;
    let (range_start, range_end) = journal_range(prefix, 1, h + 1);
    let rows = self
      .journal
      .read_range(
        range_start,
        range_end,
        chain(vec![column(FAMILY_HISTORY, HISTORY_STATE), latest_cell()]),
        operation,
      )
      .await?;
    rows
      .iter()
      .map(|row| {
        let seq_nr = journal_seq_nr(prefix, &row.row_key)
          .ok_or_else(|| invalid_data(operation, "a journal row key does not hold a seq_nr"))?;
        match row.value(FAMILY_HISTORY, HISTORY_STATE) {
          Some(state) if state == STATE_ACTIVE => Ok((seq_nr, true)),
          Some(state) if state == STATE_RETIRED => Ok((seq_nr, false)),
          _ => Err(invalid_data(operation, "a journal row holds an unknown history state")),
        }
      })
      .collect()
  }

  /// active な履歴の行を、履歴の封筒を削除して `retired` にする。1 回の CheckAndMutateRow で行い、event のセルは残す。
  /// `active` でない行（既に `retired` の行を含む）には何もしない（BT-12）。
  async fn retire_history(&self, prefix: &[u8], seq_nr: SeqNr) -> Result<(), EventStoreError> {
    let mut retire = vec![delete_column(FAMILY_HISTORY, HISTORY_ENVELOPE)];
    retire.extend(replace_cell(FAMILY_HISTORY, HISTORY_STATE, STATE_RETIRED));
    self
      .journal
      .check_and_mutate(
        journal_row_key(prefix, seq_nr),
        latest_cell_equals(FAMILY_HISTORY, HISTORY_STATE, STATE_ACTIVE),
        retire,
        vec![],
        StorageOperation::Append,
      )
      .await
      .map(|_| ())
  }

  /// 試験用に、aid の履歴（active な行と、まだ書き出されていない head の末尾の履歴）の seq_nr を昇順で返す。
  #[cfg(feature = "test-hooks")]
  pub(super) async fn history(&self, aid: &AidString) -> Result<Vec<SeqNr>, EventStoreError> {
    let operation = StorageOperation::LoadSnapshot;
    let prefix = self.prefix(aid)?;
    let Some(head) = self.read_head(&prefix, aid, operation).await? else {
      return Ok(Vec::new());
    };
    let rows = self.scan_history(&prefix, head.seq_nr).await?;
    let mut history: Vec<SeqNr> = rows
      .iter()
      .filter(|(_, active)| *active)
      .map(|(seq_nr, _)| *seq_nr)
      .collect();
    let tail_row_exists = rows.iter().any(|(seq_nr, _)| *seq_nr == head.seq_nr);
    if self.keep_snapshot_count.is_some() && head.has_snapshot && !tail_row_exists {
      history.push(head.seq_nr);
    }
    history.sort_unstable();
    Ok(history)
  }
}
