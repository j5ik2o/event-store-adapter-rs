//! 追記と読み取り（SPN-6〜SPN-10、4 章・5 章）。
//!
//! 確定点は、SERIALIZABLE の読み書きトランザクションのコミットの成功である。そのトランザクションで head を読んで判定し、
//! journal・head・snapshot_current・snapshot_history の mutation をまとめてコミットする。
//! 読み取りは、head と続けて読む行を、同じ strong の読み取り専用トランザクションで読む。

use std::sync::Arc;

use async_trait::async_trait;
use chrono::DateTime;
use googleapis_tonic_google_spanner_v1::google::spanner::v1::{mutation, Mutation};
use prost_types::{ListValue, Value};

use super::client::{storage_error, tx_failure, wait_for_next_attempt, Rpc, SessionHandle, TxError};
use super::configuration::{TableNames, ValidatedOptions};
use super::values::{
  bytes_value, cell, int64_value, null_value, read_bytes, read_required_int64, read_string, string_value, Param,
};
use crate::aggregate_id::{AggregateId, AidString};
use crate::error::{ContractRule, EventStoreError, RetentionFailure, StorageOperation};
use crate::event_envelope::{EventEnvelope, SnapshotEnvelope, SnapshotRead};
use crate::key_hash::fnv1a64;
use crate::seq_nr::{SeqNr, SEQ_NR_MAX};
use crate::serializer::{EventSerializer, SnapshotSerializer};
use crate::storage_backend::{AppendReceipt, AppendRequest, StorageBackend};

/// ストアのインスタンスとそのクローンが共有する状態を表す。
pub(crate) struct SpannerShared {
  pub(crate) rpc: Rpc,
  pub(crate) names: TableNames,
  pub(crate) options: ValidatedOptions,
  pub(crate) store_id: String,
}

pub(super) struct SpannerBackend<A, P> {
  pub shared: Arc<SpannerShared>,
  pub events: Arc<dyn EventSerializer<P>>,
  pub snapshots: Arc<dyn SnapshotSerializer<A>>,
}

impl<A, P> Clone for SpannerBackend<A, P> {
  fn clone(&self) -> Self {
    Self {
      shared: self.shared.clone(),
      events: self.events.clone(),
      snapshots: self.snapshots.clone(),
    }
  }
}

impl<A, P> std::fmt::Debug for SpannerBackend<A, P> {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    formatter
      .debug_struct("SpannerBackend")
      .field("store_id", &self.shared.store_id)
      .field("keep_snapshot_count", &self.shared.options.keep_snapshot_count)
      .finish()
  }
}

pub(super) fn invalid_data(operation: StorageOperation, message: &'static str) -> EventStoreError {
  storage_error(operation, message)
}

pub(super) fn retention_failure(
  aid: &AidString,
  seq_nr: SeqNr,
  phase: &str,
  error: EventStoreError,
) -> RetentionFailure {
  RetentionFailure {
    aid: aid.as_str().to_owned(),
    seq_nr,
    phase: phase.to_owned(),
    error: error.to_string(),
  }
}

fn contract_violation(rule: ContractRule, seq_nr: SeqNr) -> EventStoreError {
  EventStoreError::ContractViolation {
    rule,
    seq_nr: Some(seq_nr),
    snapshot_seq_nr: None,
  }
}

/// SQL に渡す seq_nr。1 以上 2^53−1 以下なので、INT64 に収まる。
pub(super) fn seq_param(seq_nr: SeqNr) -> i64 {
  i64::try_from(seq_nr).unwrap_or(i64::MAX)
}

/// 保存された seq_nr を読む。1 以上 2^53−1 以下のときだけ値を返す（SPN-3）。
pub(super) fn parse_seq_nr(value: &Value, operation: StorageOperation) -> Result<SeqNr, EventStoreError> {
  let seq_nr = read_required_int64(value).map_err(|error| storage_error(operation, error))?;
  u64::try_from(seq_nr)
    .ok()
    .filter(|seq_nr| (1..=SEQ_NR_MAX).contains(seq_nr))
    .ok_or_else(|| invalid_data(operation, "a stored seq_nr is outside 1 to 2^53-1"))
}

impl SpannerShared {
  /// aid の `hash_prefix`。FNV-1a 64 を 8 バイトのビッグエンディアンにした値である（K-1）。試験では、ハッシュを差し替えて衝突させられる。
  pub(crate) fn hash_prefix(&self, aid: &AidString) -> Vec<u8> {
    #[cfg(feature = "test-hooks")]
    let hash = self
      .rpc
      .hooks
      .as_ref()
      .and_then(|hooks| hooks.aggregate_hash(aid))
      .unwrap_or_else(|| fnv1a64(aid.as_str().as_bytes()));
    #[cfg(not(feature = "test-hooks"))]
    let hash = fnv1a64(aid.as_str().as_bytes());
    hash.to_be_bytes().to_vec()
  }

  /// 主キーの `(hash_prefix, aid)` の組。キーは型付きの組の完全一致で比べる（SPN-4）。
  pub(crate) fn key_params(&self, aid: &AidString) -> [(&'static str, Param); 2] {
    [
      ("hp", Param::Bytes(self.hash_prefix(aid))),
      ("aid", Param::String(aid.as_str().to_owned())),
    ]
  }

  /// head の seq_nr を、渡されたトランザクションの読み取り時点で読む。head がなければ `None`。
  pub(crate) async fn read_head(
    &self,
    session: &SessionHandle,
    transaction: &googleapis_tonic_google_spanner_v1::google::spanner::v1::Transaction,
    aid: &AidString,
    operation: StorageOperation,
  ) -> Result<Option<SeqNr>, TxError> {
    let sql = format!(
      "SELECT seq_nr FROM `{}` WHERE hash_prefix = @hp AND aid = @aid",
      self.names.head
    );
    let rows = session.query(transaction, &sql, &self.key_params(aid)).await?;
    let Some(row) = rows.first() else {
      return Ok(None);
    };
    let value = cell(row, 0).map_err(|error| storage_error(operation, error))?;
    Ok(Some(parse_seq_nr(value, operation)?))
  }

  /// 試験用に、aid の履歴（印のない行）の seq_nr を昇順で返す。
  #[cfg(feature = "test-hooks")]
  pub(crate) async fn history(&self, aid: &AidString) -> Result<Vec<SeqNr>, EventStoreError> {
    let operation = StorageOperation::LoadSnapshot;
    let session = self.rpc.open_session(operation).await?;
    let result = async {
      let transaction = session
        .begin_read_only()
        .await
        .map_err(|error| tx_failure(error, operation))?;
      let sql = format!(
        "SELECT seq_nr FROM `{}` WHERE hash_prefix = @hp AND aid = @aid AND expired_at IS NULL ORDER BY seq_nr",
        self.names.snapshot_history
      );
      let rows = session
        .query(&transaction, &sql, &self.key_params(aid))
        .await
        .map_err(|error| tx_failure(error, operation))?;
      rows
        .iter()
        .map(|row| {
          parse_seq_nr(
            cell(row, 0).map_err(|error| storage_error(operation, error))?,
            operation,
          )
        })
        .collect()
    }
    .await;
    session.close().await;
    result
  }
}

/// 直列化を済ませた、追記する 1 組の封筒。
struct PreparedAppend<'a> {
  aid: &'a AidString,
  type_name: String,
  seq_nr: SeqNr,
  occurred_at_ns: i64,
  manifest: &'a str,
  payload: Vec<u8>,
  snapshot: Option<PreparedSnapshot<'a>>,
}

struct PreparedSnapshot<'a> {
  seq_nr: SeqNr,
  manifest: &'a str,
  payload: Vec<u8>,
}

fn write_mutation(
  kind: fn(mutation::Write) -> mutation::Operation,
  table: &str,
  columns: &[&str],
  values: Vec<Value>,
) -> Mutation {
  Mutation {
    operation: Some(kind(mutation::Write {
      table: table.to_owned(),
      columns: columns.iter().map(|column| (*column).to_owned()).collect(),
      values: vec![ListValue { values }],
    })),
  }
}

impl SpannerShared {
  /// 追記の mutation を組み立てる。試行ごとに同じ封筒・同じ seq_nr で組み立て直す（SPN-7）。
  fn append_mutations(&self, prepared: &PreparedAppend<'_>, head_exists: bool) -> Vec<Mutation> {
    let hash_prefix = bytes_value(&self.hash_prefix(prepared.aid));
    let aid = string_value(prepared.aid.as_str());
    let seq = int64_value(seq_param(prepared.seq_nr));
    let mut mutations = vec![write_mutation(
      mutation::Operation::Insert,
      &self.names.journal,
      &["hash_prefix", "aid", "seq_nr", "occurred_at_ns", "manifest", "payload"],
      vec![
        hash_prefix.clone(),
        aid.clone(),
        seq.clone(),
        int64_value(prepared.occurred_at_ns),
        string_value(prepared.manifest),
        bytes_value(&prepared.payload),
      ],
    )];
    mutations.push(if head_exists {
      write_mutation(
        mutation::Operation::Update,
        &self.names.head,
        &[
          "hash_prefix",
          "aid",
          "seq_nr",
          "event_occurred_at_ns",
          "event_manifest",
          "event_payload",
        ],
        vec![
          hash_prefix.clone(),
          aid.clone(),
          seq,
          int64_value(prepared.occurred_at_ns),
          string_value(prepared.manifest),
          bytes_value(&prepared.payload),
        ],
      )
    } else {
      write_mutation(
        mutation::Operation::Insert,
        &self.names.head,
        &[
          "hash_prefix",
          "aid",
          "type_name",
          "seq_nr",
          "event_occurred_at_ns",
          "event_manifest",
          "event_payload",
        ],
        vec![
          hash_prefix.clone(),
          aid.clone(),
          string_value(&prepared.type_name),
          seq,
          int64_value(prepared.occurred_at_ns),
          string_value(prepared.manifest),
          bytes_value(&prepared.payload),
        ],
      )
    });
    if let Some(snapshot) = &prepared.snapshot {
      let snapshot_seq = int64_value(seq_param(snapshot.seq_nr));
      mutations.push(write_mutation(
        mutation::Operation::InsertOrUpdate,
        &self.names.snapshot_current,
        &["hash_prefix", "aid", "seq_nr", "manifest", "payload"],
        vec![
          hash_prefix.clone(),
          aid.clone(),
          snapshot_seq.clone(),
          string_value(snapshot.manifest),
          bytes_value(&snapshot.payload),
        ],
      ));
      if self.options.keep_snapshot_count.is_some() {
        mutations.push(write_mutation(
          mutation::Operation::Insert,
          &self.names.snapshot_history,
          &["hash_prefix", "aid", "seq_nr", "manifest", "payload", "expired_at"],
          vec![
            hash_prefix,
            aid,
            snapshot_seq,
            string_value(snapshot.manifest),
            bytes_value(&snapshot.payload),
            null_value(),
          ],
        ));
      }
    }
    mutations
  }

  /// 追記の 1 回の試行（SPN-6）。head を読んで判定し、mutation をコミットする。
  async fn try_append(
    &self,
    session: &SessionHandle,
    prepared: &PreparedAppend<'_>,
    attempt: u32,
  ) -> Result<(), TxError> {
    let operation = StorageOperation::Append;
    let aid = prepared.aid;
    let seq_nr = prepared.seq_nr;
    #[cfg(feature = "test-hooks")]
    if let Some(hooks) = &self.rpc.hooks {
      hooks.before_attempt(aid, seq_nr, attempt);
    }
    #[cfg(not(feature = "test-hooks"))]
    let _ = attempt;
    let transaction = session.begin_read_write().await?;
    let outcome = async {
      // 手順 1・2: head を読み、入力の seq_nr を h に対して判定する（W-3・W-7・W-8）。
      let head = self.read_head(session, &transaction, aid, operation).await?;
      let current = head.unwrap_or(0);
      if seq_nr <= current {
        return Err(TxError::Failed(EventStoreError::OptimisticLock {
          aid: aid.as_str().to_owned(),
          seq_nr,
          head_seq_nr: Some(current),
        }));
      }
      if seq_nr != current + 1 {
        return Err(TxError::Failed(contract_violation(ContractRule::W8Gap, seq_nr)));
      }
      // 手順 3〜5: journal・head・snapshot の mutation を作る。
      let mutations = self.append_mutations(prepared, head.is_some());
      #[cfg(feature = "test-hooks")]
      if let Some(hooks) = &self.rpc.hooks {
        match hooks.before_commit(aid, seq_nr) {
          Ok(()) => {}
          Err(super::SpannerCommitFault::Aborted) => return Err(TxError::Aborted),
          Err(super::SpannerCommitFault::Failed(error)) => return Err(TxError::Failed(error)),
        }
      }
      // 手順 6: コミット。成功が確定点である（H-1）。
      session.commit(&transaction, mutations).await
    }
    .await;
    if outcome.is_err() {
      session.rollback(&transaction).await;
    }
    outcome?;
    // コミットの応答を受け取った後の失敗は、確定した後の失敗である。再試行も補償もしない（SPN-8）。
    #[cfg(feature = "test-hooks")]
    if let Some(hooks) = &self.rpc.hooks {
      hooks.after_commit(aid, seq_nr)?;
    }
    Ok(())
  }
}

#[async_trait]
impl<AID: AggregateId, A: Send + Sync + 'static, P: Send + Sync + 'static> StorageBackend<AID, A, P>
  for SpannerBackend<A, P>
{
  async fn append(&self, request: AppendRequest<'_, AID, A, P>) -> Result<AppendReceipt, EventStoreError> {
    let operation = StorageOperation::Append;
    let shared = &self.shared;
    let aid = request.aid;
    let seq_nr = request.event.seq_nr();
    // 入力の検査と直列化を、保存先へ書く前に済ませる。
    let occurred_at_ns = request
      .event
      .occurred_at()
      .timestamp_nanos_opt()
      .ok_or_else(|| contract_violation(ContractRule::T13, seq_nr))?;
    let payload = self.events.serialize(request.event.payload())?;
    let snapshot = request
      .snapshot
      .map(|snapshot| -> Result<PreparedSnapshot<'_>, EventStoreError> {
        Ok(PreparedSnapshot {
          seq_nr: snapshot.seq_nr(),
          manifest: snapshot.manifest(),
          payload: self.snapshots.serialize(snapshot.aggregate())?,
        })
      })
      .transpose()?;
    let prepared = PreparedAppend {
      aid,
      type_name: request.event.aggregate_id().type_name(),
      seq_nr,
      occurred_at_ns,
      manifest: request.event.manifest(),
      payload,
      snapshot,
    };

    let session = shared.rpc.open_session(operation).await?;
    let result = self.append_in_session(&session, &prepared).await;
    session.close().await;
    result
  }

  async fn load_snapshot(&self, aid: &AidString) -> Result<Option<SnapshotRead<A>>, EventStoreError> {
    let operation = StorageOperation::LoadSnapshot;
    let session = self.shared.rpc.open_session(operation).await?;
    let result = self.load_snapshot_in_session(&session, aid, operation).await;
    session.close().await;
    result
  }

  async fn load_events(
    &self,
    aggregate_id: &AID,
    aid: &AidString,
    seq_nr: SeqNr,
  ) -> Result<Vec<EventEnvelope<AID, P>>, EventStoreError> {
    let operation = StorageOperation::LoadEvents;
    let session = self.shared.rpc.open_session(operation).await?;
    let stored = self
      .load_events_in_session(&session, aid, seq_nr.max(1), operation)
      .await;
    session.close().await;
    stored?
      .into_iter()
      .map(|row| {
        self.events.deserialize(&row.payload).map(|payload| {
          EventEnvelope::new(
            aggregate_id.clone(),
            row.seq_nr,
            DateTime::from_timestamp_nanos(row.occurred_at_ns),
            payload,
          )
          .with_manifest(row.manifest)
        })
      })
      .collect()
  }
}

/// journal から読んだ 1 行。
struct StoredEvent {
  seq_nr: SeqNr,
  occurred_at_ns: i64,
  manifest: String,
  payload: Vec<u8>,
}

impl<A: 'static, P: 'static> SpannerBackend<A, P> {
  async fn append_in_session(
    &self,
    session: &SessionHandle,
    prepared: &PreparedAppend<'_>,
  ) -> Result<AppendReceipt, EventStoreError> {
    let operation = StorageOperation::Append;
    let shared = &self.shared;
    let mut attempt = 0;
    loop {
      match shared.try_append(session, prepared, attempt).await {
        Ok(()) => break,
        // ABORTED だけを、同じ封筒・同じ seq_nr で、上限まで再試行する（SPN-7）。
        Err(TxError::Aborted) => {
          attempt = wait_for_next_attempt(attempt, shared.options.transaction_retry_limit, operation).await?;
        }
        Err(TxError::Failed(error)) => return Err(error),
      }
    }
    // 追記の確定後に、別のトランザクションで履歴を整理する。失敗は確定した追記を変えない（SPN-13）。
    let retention_failure = match (&prepared.snapshot, shared.options.keep_snapshot_count) {
      (Some(_), Some(_)) => self.retain_history(session, prepared.aid, prepared.seq_nr).await.err(),
      _ => None,
    };
    Ok(AppendReceipt { retention_failure })
  }

  async fn load_snapshot_in_session(
    &self,
    session: &SessionHandle,
    aid: &AidString,
    operation: StorageOperation,
  ) -> Result<Option<SnapshotRead<A>>, EventStoreError> {
    let shared = &self.shared;
    let fail = |error| tx_failure(error, operation);
    // head と snapshot_current は、1 つの strong の読み取り専用トランザクションの同じ読み取り時点で読む（SPN-9）。
    let transaction = session.begin_read_only().await.map_err(fail)?;
    let head = shared
      .read_head(session, &transaction, aid, operation)
      .await
      .map_err(fail)?;
    #[cfg(feature = "test-hooks")]
    if let Some(hooks) = &shared.rpc.hooks {
      hooks.read_snapshot(aid)?;
    }
    let Some(head) = head else {
      return Ok(None);
    };
    let sql = format!(
      "SELECT seq_nr, manifest, payload FROM `{}` WHERE hash_prefix = @hp AND aid = @aid",
      shared.names.snapshot_current
    );
    let rows = session
      .query(&transaction, &sql, &shared.key_params(aid))
      .await
      .map_err(fail)?;
    let Some(row) = rows.first() else {
      return Ok(Some(SnapshotRead::new(None, head)));
    };
    let to_error = |error| storage_error(operation, error);
    let seq_nr = parse_seq_nr(cell(row, 0).map_err(to_error)?, operation)?;
    if seq_nr > head {
      return Err(invalid_data(operation, "the current snapshot is newer than the head"));
    }
    let manifest = read_string(cell(row, 1).map_err(to_error)?).map_err(to_error)?;
    let payload = read_bytes(cell(row, 2).map_err(to_error)?).map_err(to_error)?;
    let aggregate = self.snapshots.deserialize(&payload)?;
    Ok(Some(SnapshotRead::new(
      Some(SnapshotEnvelope::new(aggregate, seq_nr).with_manifest(manifest)),
      head,
    )))
  }

  /// head の h を固定し、journal の `[start, h]` を昇順に全件読む（SPN-10）。範囲に欠けがあれば保存先エラーにする。
  async fn load_events_in_session(
    &self,
    session: &SessionHandle,
    aid: &AidString,
    start: SeqNr,
    operation: StorageOperation,
  ) -> Result<Vec<StoredEvent>, EventStoreError> {
    let shared = &self.shared;
    let fail = |error| tx_failure(error, operation);
    let transaction = session.begin_read_only().await.map_err(fail)?;
    let head = shared
      .read_head(session, &transaction, aid, operation)
      .await
      .map_err(fail)?;
    #[cfg(feature = "test-hooks")]
    if let Some(hooks) = &shared.rpc.hooks {
      hooks.read_events(aid)?;
    }
    let Some(h) = head else {
      return Ok(Vec::new());
    };
    if start > h {
      return Ok(Vec::new());
    }
    let sql = format!(
      "SELECT seq_nr, occurred_at_ns, manifest, payload FROM `{}` WHERE hash_prefix = @hp AND aid = @aid AND seq_nr >= @start AND seq_nr <= @h ORDER BY seq_nr",
      shared.names.journal
    );
    let [hash_prefix, aid_param] = shared.key_params(aid);
    let rows = session
      .query(
        &transaction,
        &sql,
        &[
          hash_prefix,
          aid_param,
          ("start", Param::Int64(seq_param(start))),
          ("h", Param::Int64(seq_param(h))),
        ],
      )
      .await
      .map_err(fail)?;
    let gap = || invalid_data(operation, "the journal has a gap in the requested range");
    if u64::try_from(rows.len()).ok() != Some(h - start + 1) {
      return Err(gap());
    }
    let to_error = |error| storage_error(operation, error);
    rows
      .iter()
      .zip(start..=h)
      .map(|(row, expected)| {
        let seq_nr = parse_seq_nr(cell(row, 0).map_err(to_error)?, operation)?;
        if seq_nr != expected {
          return Err(gap());
        }
        Ok(StoredEvent {
          seq_nr,
          occurred_at_ns: read_required_int64(cell(row, 1).map_err(to_error)?).map_err(to_error)?,
          manifest: read_string(cell(row, 2).map_err(to_error)?).map_err(to_error)?,
          payload: read_bytes(cell(row, 3).map_err(to_error)?).map_err(to_error)?,
        })
      })
      .collect()
  }
}
