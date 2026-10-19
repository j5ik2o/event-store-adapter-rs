use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use chrono::DateTime;
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};

use super::connection::{invalid_data, storage};
use crate::aggregate_id::{AggregateId, AidString};
use crate::error::{ContractRule, EventStoreError, RetentionFailure, StorageOperation};
use crate::event_envelope::{EventEnvelope, SnapshotEnvelope, SnapshotRead};
use crate::retention::select_expired_history_after_append;
use crate::seq_nr::SeqNr;
use crate::serializer::{EventSerializer, SnapshotSerializer};
use crate::storage_backend::{AppendReceipt, AppendRequest, StorageBackend};

/// ストアのインスタンスとそのクローンが共有する状態を表す。
///
/// 接続は 1 つで、`Mutex` が同じ接続を別の操作へ同時に渡さないことを保証する（SQ-5・SQ-17）。
/// 同じファイルを開く別のストアは別の接続を持つ。
pub(super) struct SqliteShared {
  pub connection: Mutex<Connection>,
  pub keep_snapshot_count: Option<usize>,
  pub store_id: String,
  #[cfg(feature = "test-hooks")]
  pub hooks: Option<Arc<dyn super::SqliteTestHooks>>,
}

pub(super) struct SqliteBackend<A, P> {
  pub shared: Arc<SqliteShared>,
  pub events: Arc<dyn EventSerializer<P>>,
  pub snapshots: Arc<dyn SnapshotSerializer<A>>,
}

impl<A, P> Clone for SqliteBackend<A, P> {
  fn clone(&self) -> Self {
    Self {
      shared: self.shared.clone(),
      events: self.events.clone(),
      snapshots: self.snapshots.clone(),
    }
  }
}

impl<A, P> std::fmt::Debug for SqliteBackend<A, P> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("SqliteBackend")
      .field("store_id", &self.shared.store_id)
      .field("keep_snapshot_count", &self.shared.keep_snapshot_count)
      .finish()
  }
}

fn lock(shared: &SqliteShared, operation: StorageOperation) -> Result<MutexGuard<'_, Connection>, EventStoreError> {
  shared
    .connection
    .lock()
    .map_err(|_| invalid_data(operation, "the sqlite connection lock is poisoned"))
}

fn seq_nr_from_row(value: i64, operation: StorageOperation) -> Result<SeqNr, EventStoreError> {
  SeqNr::try_from(value).map_err(|_| invalid_data(operation, "a stored seq_nr is out of range"))
}

/// aid のヘッドの seq_nr を、aid の完全一致で読む。ヘッドがなければ `None`（SQ-4）。
fn read_head(
  connection: &Connection,
  aid: &AidString,
  operation: StorageOperation,
) -> Result<Option<SeqNr>, EventStoreError> {
  connection
    .query_row("SELECT seq_nr FROM head WHERE aid = ?1", [aid.as_str()], |row| {
      row.get::<_, i64>(0)
    })
    .optional()
    .map_err(|error| storage(operation, error))?
    .map(|value| seq_nr_from_row(value, operation))
    .transpose()
}

/// journal または新規 head の主キーの重複だけを楽観ロックにし、ほかの失敗は保存先エラーにする（SQ-7）。
fn append_error(error: rusqlite::Error, aid: &AidString, seq_nr: SeqNr, head_seq_nr: Option<SeqNr>) -> EventStoreError {
  match &error {
    rusqlite::Error::SqliteFailure(failure, _)
      if failure.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY =>
    {
      EventStoreError::OptimisticLock {
        aid: aid.as_str().to_owned(),
        seq_nr,
        head_seq_nr,
      }
    }
    _ => storage(StorageOperation::Append, error),
  }
}

fn retention_failure(aid: &AidString, seq_nr: SeqNr, phase: &str, error: EventStoreError) -> RetentionFailure {
  RetentionFailure {
    aid: aid.as_str().to_owned(),
    seq_nr,
    phase: phase.to_owned(),
    error: error.to_string(),
  }
}

struct StoredSnapshot {
  seq_nr: SeqNr,
  manifest: String,
  payload: Vec<u8>,
}

struct StoredEvent {
  seq_nr: SeqNr,
  occurred_at_ns: i64,
  manifest: String,
  payload: Vec<u8>,
}

#[async_trait]
impl<AID: AggregateId, A: Send + Sync + 'static, P: Send + Sync + 'static> StorageBackend<AID, A, P>
  for SqliteBackend<A, P>
{
  async fn append(&self, request: AppendRequest<'_, AID, A, P>) -> Result<AppendReceipt, EventStoreError> {
    let operation = StorageOperation::Append;
    let aid = request.aid;
    let seq_nr = request.event.seq_nr();
    let violation = |rule| EventStoreError::ContractViolation {
      rule,
      seq_nr: Some(seq_nr),
      snapshot_seq_nr: None,
    };
    // 入力の検査と直列化を、トランザクションを始める前に済ませる（SQ-5）。
    let occurred_at_ns = request
      .event
      .occurred_at()
      .timestamp_nanos_opt()
      .ok_or_else(|| violation(ContractRule::T13))?;
    let seq_nr_value = i64::try_from(seq_nr).map_err(|_| violation(ContractRule::T9))?;
    let payload = self.events.serialize(request.event.payload())?;
    let snapshot = request
      .snapshot
      .map(|snapshot| {
        self
          .snapshots
          .serialize(snapshot.aggregate())
          .map(|bytes| (snapshot.manifest().to_owned(), bytes))
      })
      .transpose()?;
    let type_name = request.event.aggregate_id().type_name();
    let manifest = request.event.manifest();
    {
      let mut connection = lock(&self.shared, operation)?;
      // ヘッドを照合する前に書き込みのロックを確保する（SQ-17）。失敗はロールバックされ、確定しない（SQ-6）。
      let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| storage(operation, error))?;
      let head = read_head(&transaction, aid, operation)?;
      if head.is_some_and(|head| seq_nr <= head) {
        return Err(EventStoreError::OptimisticLock {
          aid: aid.as_str().to_owned(),
          seq_nr,
          head_seq_nr: head,
        });
      }
      if seq_nr != head.unwrap_or(0) + 1 {
        return Err(violation(ContractRule::W8Gap));
      }
      transaction
        .execute(
          "INSERT INTO journal (aid, seq_nr, occurred_at_ns, manifest, payload) VALUES (?1, ?2, ?3, ?4, ?5)",
          params![aid.as_str(), seq_nr_value, occurred_at_ns, manifest, payload],
        )
        .map_err(|error| append_error(error, aid, seq_nr, head))?;
      if head.is_some() {
        transaction
          .execute(
            "UPDATE head SET seq_nr = ?2, event_occurred_at_ns = ?3, event_manifest = ?4, event_payload = ?5
             WHERE aid = ?1",
            params![aid.as_str(), seq_nr_value, occurred_at_ns, manifest, payload],
          )
          .map_err(|error| append_error(error, aid, seq_nr, head))?;
      } else {
        transaction
          .execute(
            "INSERT INTO head (aid, type_name, seq_nr, event_occurred_at_ns, event_manifest, event_payload)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![aid.as_str(), type_name, seq_nr_value, occurred_at_ns, manifest, payload],
          )
          .map_err(|error| append_error(error, aid, seq_nr, head))?;
      }
      if let Some((snapshot_manifest, snapshot_payload)) = &snapshot {
        transaction
          .execute(
            "INSERT INTO snapshot_current (aid, seq_nr, manifest, payload) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(aid) DO UPDATE SET
               seq_nr = excluded.seq_nr, manifest = excluded.manifest, payload = excluded.payload",
            params![aid.as_str(), seq_nr_value, snapshot_manifest, snapshot_payload],
          )
          .map_err(|error| storage(operation, error))?;
        if self.shared.keep_snapshot_count.is_some() {
          transaction
            .execute(
              "INSERT INTO snapshot_history (aid, seq_nr, manifest, payload) VALUES (?1, ?2, ?3, ?4)",
              params![aid.as_str(), seq_nr_value, snapshot_manifest, snapshot_payload],
            )
            .map_err(|error| storage(operation, error))?;
        }
      }
      #[cfg(feature = "test-hooks")]
      if let Some(hooks) = &self.shared.hooks {
        hooks.before_commit(aid, seq_nr)?;
      }
      // COMMIT の成功が確定点。COMMIT が失敗したときは、`Transaction` の破棄が状態を確かめて後始末する。
      // 確定したかを確認できないので保存先エラーとし、別の seq_nr を採番し直さない（SQ-6）。
      transaction.commit().map_err(|error| storage(operation, error))?;
    }
    let retention_failure = match (&snapshot, self.shared.keep_snapshot_count) {
      (Some(_), Some(keep)) => self.retain_history(aid, seq_nr, keep).err(),
      _ => None,
    };
    Ok(AppendReceipt { retention_failure })
  }

  async fn load_snapshot(&self, aid: &AidString) -> Result<Option<SnapshotRead<A>>, EventStoreError> {
    let operation = StorageOperation::LoadSnapshot;
    let copied: Option<(SeqNr, Option<StoredSnapshot>)> = {
      let mut connection = lock(&self.shared, operation)?;
      // head と snapshot_current を、同じ読み取りトランザクションで読む（SQ-8）。
      let transaction = connection.transaction().map_err(|error| storage(operation, error))?;
      let head = read_head(&transaction, aid, operation)?;
      #[cfg(feature = "test-hooks")]
      if let Some(hooks) = &self.shared.hooks {
        hooks.read_snapshot(aid)?;
      }
      let copied = match head {
        None => None,
        Some(head) => {
          let snapshot = transaction
            .query_row(
              "SELECT seq_nr, manifest, payload FROM snapshot_current WHERE aid = ?1",
              [aid.as_str()],
              |row| {
                Ok((
                  row.get::<_, i64>(0)?,
                  row.get::<_, String>(1)?,
                  row.get::<_, Vec<u8>>(2)?,
                ))
              },
            )
            .optional()
            .map_err(|error| storage(operation, error))?
            .map(|(seq_nr, manifest, payload)| {
              seq_nr_from_row(seq_nr, operation).map(|seq_nr| StoredSnapshot {
                seq_nr,
                manifest,
                payload,
              })
            })
            .transpose()?;
          Some((head, snapshot))
        }
      };
      transaction.commit().map_err(|error| storage(operation, error))?;
      copied
    };
    copied
      .map(|(head, snapshot)| {
        let snapshot = snapshot
          .map(|stored| {
            self
              .snapshots
              .deserialize(&stored.payload)
              .map(|aggregate| SnapshotEnvelope::new(aggregate, stored.seq_nr).with_manifest(stored.manifest))
          })
          .transpose()?;
        Ok(SnapshotRead::new(snapshot, head))
      })
      .transpose()
  }

  async fn load_events(
    &self,
    aggregate_id: &AID,
    aid: &AidString,
    seq_nr: SeqNr,
  ) -> Result<Vec<EventEnvelope<AID, P>>, EventStoreError> {
    let operation = StorageOperation::LoadEvents;
    let start = seq_nr.max(1);
    let start_value = i64::try_from(start).map_err(|_| EventStoreError::ContractViolation {
      rule: ContractRule::T9,
      seq_nr: Some(seq_nr),
      snapshot_seq_nr: None,
    })?;
    let copied: Vec<StoredEvent> = {
      let mut connection = lock(&self.shared, operation)?;
      // head の h を読み、同じ読み取りトランザクションで journal を最後まで読む（SQ-9）。
      let transaction = connection.transaction().map_err(|error| storage(operation, error))?;
      let head = read_head(&transaction, aid, operation)?;
      #[cfg(feature = "test-hooks")]
      if let Some(hooks) = &self.shared.hooks {
        hooks.read_events(aid)?;
      }
      let copied = match head {
        None => Vec::new(),
        Some(head) => {
          let head_value =
            i64::try_from(head).map_err(|_| invalid_data(operation, "a stored seq_nr is out of range"))?;
          let mut statement = transaction
            .prepare(
              "SELECT seq_nr, occurred_at_ns, manifest, payload FROM journal
               WHERE aid = ?1 AND seq_nr >= ?2 AND seq_nr <= ?3 ORDER BY seq_nr",
            )
            .map_err(|error| storage(operation, error))?;
          let rows = statement
            .query_map(params![aid.as_str(), start_value, head_value], |row| {
              Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Vec<u8>>(3)?,
              ))
            })
            .and_then(|rows| rows.collect::<Result<Vec<_>, _>>())
            .map_err(|error| storage(operation, error))?;
          let events = rows
            .into_iter()
            .map(|(seq_nr, occurred_at_ns, manifest, payload)| {
              seq_nr_from_row(seq_nr, operation).map(|seq_nr| StoredEvent {
                seq_nr,
                occurred_at_ns,
                manifest,
                payload,
              })
            })
            .collect::<Result<Vec<_>, _>>()?;
          // 対象範囲の欠番は、短い結果を成功として返さず、保存先エラーにする。
          let expected_len = if start > head { 0 } else { head - start + 1 };
          let contiguous = events
            .iter()
            .zip(start..)
            .all(|(event, expected)| event.seq_nr == expected);
          if u64::try_from(events.len()).ok() != Some(expected_len) || !contiguous {
            return Err(invalid_data(operation, "the journal has a gap in the requested range"));
          }
          events
        }
      };
      transaction.commit().map_err(|error| storage(operation, error))?;
      copied
    };
    copied
      .into_iter()
      .map(|stored| {
        self.events.deserialize(&stored.payload).map(|payload| {
          EventEnvelope::new(
            aggregate_id.clone(),
            stored.seq_nr,
            DateTime::from_timestamp_nanos(stored.occurred_at_ns),
            payload,
          )
          .with_manifest(stored.manifest)
        })
      })
      .collect()
  }
}

impl<A, P> SqliteBackend<A, P> {
  /// スナップショットを伴う追記の確定後に、別の BEGIN IMMEDIATE で古い履歴だけを削除する（SQ-10）。
  ///
  /// 新しい `keep` 件を残す。current・head・journal は消さない。失敗は追記の成功を変えず、
  /// `RetentionFailure` として返す（SQ-11・S-4）。次のスナップショットを伴う追記でやり直す。
  fn retain_history(&self, aid: &AidString, seq_nr: SeqNr, keep: usize) -> Result<(), RetentionFailure> {
    let operation = StorageOperation::Append;
    let query_failure = |error| retention_failure(aid, seq_nr, "retention-query", error);
    let delete_failure = |error| retention_failure(aid, seq_nr, "retention-delete", error);
    let mut connection = lock(&self.shared, operation).map_err(query_failure)?;
    let transaction = connection
      .transaction_with_behavior(TransactionBehavior::Immediate)
      .map_err(|error| query_failure(storage(operation, error)))?;
    let mut statement = transaction
      .prepare("SELECT seq_nr FROM snapshot_history WHERE aid = ?1 ORDER BY seq_nr DESC")
      .map_err(|error| query_failure(storage(operation, error)))?;
    let rows = statement
      .query_map([aid.as_str()], |row| row.get::<_, i64>(0))
      .and_then(|rows| rows.collect::<Result<Vec<_>, _>>())
      .map_err(|error| query_failure(storage(operation, error)))?;
    drop(statement);
    let visible = rows
      .into_iter()
      .map(|value| seq_nr_from_row(value, operation))
      .collect::<Result<Vec<SeqNr>, _>>()
      .map_err(query_failure)?;
    #[cfg(feature = "test-hooks")]
    let visible = if let Some(hooks) = &self.shared.hooks {
      hooks
        .retention_visible_history(aid, &visible, Some(seq_nr))
        .map_err(query_failure)?
    } else {
      visible
    };
    let expired = select_expired_history_after_append(&visible, Some(seq_nr), keep);
    if expired.is_empty() {
      return Ok(());
    }
    #[cfg(feature = "test-hooks")]
    if let Some(hooks) = &self.shared.hooks {
      hooks.retention_delete(aid, &expired).map_err(delete_failure)?;
    }
    for expired_seq_nr in expired {
      // 削除の対象は、選んだ seq_nr を 1 件ずつパラメータで渡す。
      let value = i64::try_from(expired_seq_nr)
        .map_err(|_| delete_failure(invalid_data(operation, "a retained seq_nr is out of range")))?;
      transaction
        .execute(
          "DELETE FROM snapshot_history WHERE aid = ?1 AND seq_nr = ?2",
          params![aid.as_str(), value],
        )
        .map_err(|error| delete_failure(storage(operation, error)))?;
    }
    transaction
      .commit()
      .map_err(|error| delete_failure(storage(operation, error)))
  }
}

/// 試験用に、aid の `snapshot_history` の seq_nr を昇順で返す。
#[cfg(feature = "test-hooks")]
pub(super) fn history(shared: &SqliteShared, aid: &AidString) -> Result<Vec<SeqNr>, EventStoreError> {
  let operation = StorageOperation::LoadSnapshot;
  let connection = lock(shared, operation)?;
  let mut statement = connection
    .prepare("SELECT seq_nr FROM snapshot_history WHERE aid = ?1 ORDER BY seq_nr")
    .map_err(|error| storage(operation, error))?;
  let rows = statement
    .query_map([aid.as_str()], |row| row.get::<_, i64>(0))
    .and_then(|rows| rows.collect::<Result<Vec<_>, _>>())
    .map_err(|error| storage(operation, error))?;
  rows
    .into_iter()
    .map(|value| seq_nr_from_row(value, operation))
    .collect()
}
