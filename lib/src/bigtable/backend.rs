//! 追記と読み取り（BT-5〜BT-10、5 章・6 章）。
//!
//! 確定点は、head の 1 行の条件付き更新である。head を進める前に、直前の確定済みイベントを journal へ書き出す。
//! 読み取りは、journal の範囲と、先に取得した head の末尾イベントを合わせて行う。

use std::sync::Arc;

use async_trait::async_trait;
use chrono::DateTime;

use super::envelope::{
  decode_event, decode_snapshot, encode_event, encode_snapshot, event_envelope_len, snapshot_envelope_len,
  EventEnvelopeFields, SnapshotEnvelopeFields,
};
use super::keys::{aggregate_prefix, fnv1a64, journal_range, journal_row_key, journal_seq_nr};
use super::rows::{
  chain, column, latest_cell, latest_cell_equals, latest_cell_exists, replace_cell, set_cell, TableTarget,
  EVENT_ENVELOPE, EVENT_HAS_SNAPSHOT, FAMILY_EVENT, FAMILY_HEAD, FAMILY_HISTORY, FAMILY_SNAPSHOT, FLAG_SET, FLAG_UNSET,
  HEAD_HAS_SNAPSHOT, HEAD_LAST_EVENT, HEAD_SEQ_NR, HISTORY_ENVELOPE, HISTORY_STATE, SNAPSHOT_CURRENT, STATE_ACTIVE,
  STATE_RETIRED,
};
use crate::aggregate_id::{AggregateId, AidString};
use crate::error::{ContractRule, EventStoreError, RetentionFailure, StorageOperation};
use crate::event_envelope::{EventEnvelope, SnapshotEnvelope, SnapshotRead};
use crate::seq_nr::{SeqNr, SEQ_NR_MAX};
use crate::serializer::{EventSerializer, SnapshotSerializer};
use crate::storage_backend::{AppendReceipt, AppendRequest, StorageBackend};

/// ストアのインスタンスとそのクローンが共有する状態を表す。
pub(super) struct BigtableShared {
  pub journal: TableTarget,
  pub head: TableTarget,
  pub keep_snapshot_count: Option<usize>,
  pub envelope_limit_bytes: u64,
  pub store_id: String,
  #[cfg(feature = "test-hooks")]
  pub hooks: Option<Arc<dyn super::BigtableTestHooks>>,
}

pub(super) struct BigtableBackend<A, P> {
  pub shared: Arc<BigtableShared>,
  pub events: Arc<dyn EventSerializer<P>>,
  pub snapshots: Arc<dyn SnapshotSerializer<A>>,
}

impl<A, P> Clone for BigtableBackend<A, P> {
  fn clone(&self) -> Self {
    Self {
      shared: self.shared.clone(),
      events: self.events.clone(),
      snapshots: self.snapshots.clone(),
    }
  }
}

impl<A, P> std::fmt::Debug for BigtableBackend<A, P> {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    formatter
      .debug_struct("BigtableBackend")
      .field("store_id", &self.shared.store_id)
      .field("keep_snapshot_count", &self.shared.keep_snapshot_count)
      .field("envelope_limit_bytes", &self.shared.envelope_limit_bytes)
      .finish()
  }
}

pub(super) fn invalid_data(operation: StorageOperation, message: &'static str) -> EventStoreError {
  EventStoreError::Storage {
    operation,
    source: Box::new(std::io::Error::other(message)),
  }
}

/// head の `head:seq_nr` に保存する、20 桁のゼロ詰めの ASCII 数字。
fn format_head_seq_nr(seq_nr: SeqNr) -> Vec<u8> {
  format!("{seq_nr:020}").into_bytes()
}

/// `head:seq_nr` を読む。20 桁の ASCII 数字で、1 以上 2^53−1 以下のときだけ値を返す。
fn parse_head_seq_nr(bytes: &[u8]) -> Option<SeqNr> {
  if bytes.len() != 20 || !bytes.iter().all(u8::is_ascii_digit) {
    return None;
  }
  let seq_nr = std::str::from_utf8(bytes).ok()?.parse::<SeqNr>().ok()?;
  (1..=SEQ_NR_MAX).contains(&seq_nr).then_some(seq_nr)
}

fn flag_of(has_snapshot: bool) -> &'static [u8] {
  if has_snapshot {
    FLAG_SET
  } else {
    FLAG_UNSET
  }
}

fn parse_flag(bytes: Option<&[u8]>, operation: StorageOperation) -> Result<bool, EventStoreError> {
  match bytes {
    Some(bytes) if bytes == FLAG_SET => Ok(true),
    Some(bytes) if bytes == FLAG_UNSET => Ok(false),
    _ => Err(invalid_data(
      operation,
      "a stored has_snapshot flag is missing or invalid",
    )),
  }
}

/// 取得して検証した head の行。
pub(super) struct HeadState {
  pub seq_nr: SeqNr,
  pub last_event_bytes: Vec<u8>,
  pub last_event: EventEnvelopeFields,
  pub has_snapshot: bool,
  pub current_snapshot: Option<CurrentSnapshot>,
}

pub(super) struct CurrentSnapshot {
  pub bytes: Vec<u8>,
  pub fields: SnapshotEnvelopeFields,
}

/// head の行の更新。`previous` が `None` なら新規作成である。
struct HeadUpdate<'a> {
  prefix: &'a [u8],
  previous: Option<SeqNr>,
  event: &'a [u8],
  snapshot: Option<&'a [u8]>,
}

/// 確定した追記の末尾を journal へ書き出す失敗を別の経路で知らせる。通知の失敗を、確定した追記へ戻さない。
fn notify_journal_write_failure(aid: &AidString, seq_nr: SeqNr, error: &EventStoreError) {
  let aid = aid.as_str().to_owned();
  let error = error.to_string();
  let _ = std::panic::catch_unwind(move || {
    tracing::warn!(
      target: "event_store_adapter::journal",
      category = "journal-write-failure",
      aid = %aid,
      seq_nr,
      error = %error,
      "journal write after the commit failed; the append was committed"
    );
  });
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

impl BigtableShared {
  /// aid の行キーの接頭辞 `P(aid)`。試験では、ハッシュを差し替えて衝突させられる。
  pub(super) fn prefix(&self, aid: &AidString) -> Result<Vec<u8>, EventStoreError> {
    #[cfg(feature = "test-hooks")]
    let hash = self
      .hooks
      .as_ref()
      .and_then(|hooks| hooks.aggregate_hash(aid))
      .unwrap_or_else(|| fnv1a64(aid.as_str().as_bytes()));
    #[cfg(not(feature = "test-hooks"))]
    let hash = fnv1a64(aid.as_str().as_bytes());
    aggregate_prefix(hash, aid)
  }

  /// head の行を強整合で読み、検証する。行がなければ `None`（BT-7 の手順 1）。
  ///
  /// 必要なセルが欠けた行は新規集約とみなさず、保存先エラーにする。`has_snapshot` の印と現在のスナップショットの
  /// seq_nr が食い違う行（印が 1 なのに head と一致しない、印が 0 なのに head 以上）も保存先エラーにする。
  pub(super) async fn read_head(
    &self,
    prefix: &[u8],
    aid: &AidString,
    operation: StorageOperation,
  ) -> Result<Option<HeadState>, EventStoreError> {
    let Some(row) = self.head.read_row(prefix.to_vec(), latest_cell(), operation).await? else {
      return Ok(None);
    };
    let seq_nr = row
      .value(FAMILY_HEAD, HEAD_SEQ_NR)
      .and_then(parse_head_seq_nr)
      .ok_or_else(|| invalid_data(operation, "the head row has a missing or invalid seq_nr"))?;
    let last_event_bytes = row
      .value(FAMILY_HEAD, HEAD_LAST_EVENT)
      .ok_or_else(|| invalid_data(operation, "the head row has no last event"))?
      .to_vec();
    let last_event = decode_event(&last_event_bytes, operation)?;
    if last_event.aid != aid.as_str() || last_event.seq_nr != seq_nr {
      return Err(invalid_data(
        operation,
        "the last event of the head row does not match the head",
      ));
    }
    let has_snapshot = parse_flag(row.value(FAMILY_HEAD, HEAD_HAS_SNAPSHOT), operation)?;
    let current_snapshot = row
      .value(FAMILY_SNAPSHOT, SNAPSHOT_CURRENT)
      .map(|bytes| {
        decode_snapshot(bytes, operation).map(|fields| CurrentSnapshot {
          bytes: bytes.to_vec(),
          fields,
        })
      })
      .transpose()?;
    match &current_snapshot {
      // head が伴う（`has_snapshot = 1`）なら現在のスナップショットは head と同じ seq_nr で、伴わない（`has_snapshot = 0`）
      // なら head より小さい（プロファイル 4 章）。印が 0 で head と等しい行は、この規則と矛盾する。
      Some(current)
        if (has_snapshot && current.fields.seq_nr != seq_nr) || (!has_snapshot && current.fields.seq_nr >= seq_nr) =>
      {
        return Err(invalid_data(
          operation,
          "the current snapshot does not match the head seq_nr",
        ))
      }
      None if has_snapshot => {
        return Err(invalid_data(
          operation,
          "the head row says it has a snapshot but holds none",
        ))
      }
      _ => {}
    }
    Ok(Some(HeadState {
      seq_nr,
      last_event_bytes,
      last_event,
      has_snapshot,
      current_snapshot,
    }))
  }

  /// journal の行を、`event:envelope` がない場合だけ、イベント・フラグ・必要な履歴を 1 回の CheckAndMutateRow で作る（BT-5）。
  ///
  /// 既存の行は上書きせず、イベントとフラグを照合する。履歴を伴う行が active なら履歴の封筒も照合し、欠損・不一致を
  /// 保存先エラーにする。`retired` の行や、履歴を持たない行の履歴は作り直さない。
  pub(super) async fn write_journal_row(
    &self,
    prefix: &[u8],
    seq_nr: SeqNr,
    event: &[u8],
    history: Option<&[u8]>,
    has_snapshot: bool,
  ) -> Result<(), EventStoreError> {
    let operation = StorageOperation::Append;
    let row_key = journal_row_key(prefix, seq_nr);
    let mut creation = vec![
      set_cell(FAMILY_EVENT, EVENT_ENVELOPE, event),
      set_cell(FAMILY_EVENT, EVENT_HAS_SNAPSHOT, flag_of(has_snapshot)),
    ];
    if let Some(history) = history {
      creation.push(set_cell(FAMILY_HISTORY, HISTORY_STATE, STATE_ACTIVE));
      creation.push(set_cell(FAMILY_HISTORY, HISTORY_ENVELOPE, history));
    }
    let existed = self
      .journal
      .check_and_mutate(
        row_key.clone(),
        latest_cell_exists(FAMILY_EVENT, EVENT_ENVELOPE),
        vec![],
        creation,
        operation,
      )
      .await?;
    if !existed {
      return Ok(());
    }
    let row = self
      .journal
      .read_row(row_key, latest_cell(), operation)
      .await?
      .ok_or_else(|| invalid_data(operation, "an existing journal row disappeared while it was verified"))?;
    if row.value(FAMILY_EVENT, EVENT_ENVELOPE) != Some(event) {
      return Err(invalid_data(
        operation,
        "an existing journal row holds a different event envelope",
      ));
    }
    if row.value(FAMILY_EVENT, EVENT_HAS_SNAPSHOT) != Some(flag_of(has_snapshot)) {
      return Err(invalid_data(
        operation,
        "an existing journal row holds a different has_snapshot flag",
      ));
    }
    if let Some(history) = history {
      match row.value(FAMILY_HISTORY, HISTORY_STATE) {
        // 保持件数を設定する前に作られた行。履歴は補作しない。
        None => {}
        // 削除済みの履歴は作り直さない。遅れた書き出しが履歴を復活させない。
        Some(state) if state == STATE_RETIRED => {}
        Some(state) if state == STATE_ACTIVE && row.value(FAMILY_HISTORY, HISTORY_ENVELOPE) != Some(history) => {
          return Err(invalid_data(
            operation,
            "an existing journal row holds a missing or different history envelope",
          ));
        }
        Some(state) if state == STATE_ACTIVE => {}
        Some(_) => {
          return Err(invalid_data(
            operation,
            "an existing journal row holds an unknown history state",
          ))
        }
      }
    }
    Ok(())
  }

  /// head の行を条件付きで更新する。成功が追記の確定点である（H-1）。
  ///
  /// 条件が成り立たなければ楽観ロックにする。不成立は別の書き手が head を進めた後なので、取得した h は古いと分かっている。
  /// そのため楽観ロックには head の seq_nr を含めず、head を読み直しもしない（E-2）。応答を受け取れなかったときは、
  /// 結果を決めつけず保存先エラーにし、再送しない。
  async fn commit_head(&self, aid: &AidString, seq_nr: SeqNr, update: HeadUpdate<'_>) -> Result<(), EventStoreError> {
    let operation = StorageOperation::Append;
    let has_snapshot = update.snapshot.is_some();
    let mutations: Vec<_> = match update.previous {
      None => {
        let mut mutations = vec![
          set_cell(FAMILY_HEAD, HEAD_SEQ_NR, &format_head_seq_nr(seq_nr)),
          set_cell(FAMILY_HEAD, HEAD_LAST_EVENT, update.event),
          set_cell(FAMILY_HEAD, HEAD_HAS_SNAPSHOT, flag_of(has_snapshot)),
        ];
        if let Some(snapshot) = update.snapshot {
          mutations.push(set_cell(FAMILY_SNAPSHOT, SNAPSHOT_CURRENT, snapshot));
        }
        mutations
      }
      Some(_) => {
        let mut mutations: Vec<_> = replace_cell(FAMILY_HEAD, HEAD_SEQ_NR, &format_head_seq_nr(seq_nr)).into();
        mutations.extend(replace_cell(FAMILY_HEAD, HEAD_LAST_EVENT, update.event));
        mutations.extend(replace_cell(FAMILY_HEAD, HEAD_HAS_SNAPSHOT, flag_of(has_snapshot)));
        if let Some(snapshot) = update.snapshot {
          mutations.extend(replace_cell(FAMILY_SNAPSHOT, SNAPSHOT_CURRENT, snapshot));
        }
        mutations
      }
    };
    let succeeded = match update.previous {
      // 新規作成は、`head:seq_nr` がまだない側で初期化する。先行する作成があれば条件が成立する。
      None => {
        !self
          .head
          .check_and_mutate(
            update.prefix.to_vec(),
            latest_cell_exists(FAMILY_HEAD, HEAD_SEQ_NR),
            vec![],
            mutations,
            operation,
          )
          .await?
      }
      // 更新は、`head:seq_nr` の最新の 1 セルが取得した h の側だけで行う。
      Some(previous) => {
        self
          .head
          .check_and_mutate(
            update.prefix.to_vec(),
            latest_cell_equals(FAMILY_HEAD, HEAD_SEQ_NR, &format_head_seq_nr(previous)),
            mutations,
            vec![],
            operation,
          )
          .await?
      }
    };
    if succeeded {
      return Ok(());
    }
    Err(EventStoreError::OptimisticLock {
      aid: aid.as_str().to_owned(),
      seq_nr,
      head_seq_nr: None,
    })
  }
}

fn contract_violation(rule: ContractRule, seq_nr: SeqNr, snapshot_seq_nr: Option<SeqNr>) -> EventStoreError {
  EventStoreError::ContractViolation {
    rule,
    seq_nr: Some(seq_nr),
    snapshot_seq_nr,
  }
}

/// 上限（BT-16）を超える封筒を、書き込みの前に契約違反として拒否する。
fn check_envelope_limit(
  length: usize,
  limit: u64,
  seq_nr: SeqNr,
  snapshot_seq_nr: Option<SeqNr>,
) -> Result<(), EventStoreError> {
  if u64::try_from(length).is_ok_and(|length| length <= limit) {
    Ok(())
  } else {
    Err(contract_violation(
      ContractRule::EnvelopeSizeLimit,
      seq_nr,
      snapshot_seq_nr,
    ))
  }
}

#[async_trait]
impl<AID: AggregateId, A: Send + Sync + 'static, P: Send + Sync + 'static> StorageBackend<AID, A, P>
  for BigtableBackend<A, P>
{
  async fn append(&self, request: AppendRequest<'_, AID, A, P>) -> Result<AppendReceipt, EventStoreError> {
    let operation = StorageOperation::Append;
    let shared = &self.shared;
    let aid = request.aid;
    let seq_nr = request.event.seq_nr();
    // 入力の検査と直列化を、保存先へ書く前に済ませる（BT-7）。
    let occurred_at_ns = request
      .event
      .occurred_at()
      .timestamp_nanos_opt()
      .ok_or_else(|| contract_violation(ContractRule::T13, seq_nr, None))?;
    let event_fields = EventEnvelopeFields {
      aid: aid.as_str().to_owned(),
      seq_nr,
      occurred_at_ns,
      manifest: request.event.manifest().to_owned(),
      payload: self.events.serialize(request.event.payload())?,
    };
    check_envelope_limit(
      event_envelope_len(&event_fields),
      shared.envelope_limit_bytes,
      seq_nr,
      None,
    )?;
    let event_bytes = encode_event(&event_fields);
    let snapshot_bytes = request
      .snapshot
      .map(|snapshot| -> Result<Vec<u8>, EventStoreError> {
        let fields = SnapshotEnvelopeFields {
          seq_nr: snapshot.seq_nr(),
          manifest: snapshot.manifest().to_owned(),
          payload: self.snapshots.serialize(snapshot.aggregate())?,
        };
        check_envelope_limit(
          snapshot_envelope_len(&fields),
          shared.envelope_limit_bytes,
          seq_nr,
          Some(snapshot.seq_nr()),
        )?;
        Ok(encode_snapshot(&fields))
      })
      .transpose()?;

    let prefix = shared.prefix(aid)?;
    // 手順 1・2: head を強整合で読み、入力の seq_nr を h に対して照合する（W-3・W-7・W-8）。
    let head = shared.read_head(&prefix, aid, operation).await?;
    let current = head.as_ref().map_or(0, |head| head.seq_nr);
    if seq_nr <= current {
      return Err(EventStoreError::OptimisticLock {
        aid: aid.as_str().to_owned(),
        seq_nr,
        head_seq_nr: Some(current),
      });
    }
    if seq_nr != current + 1 {
      return Err(contract_violation(ContractRule::W8Gap, seq_nr, None));
    }
    // 手順 3: 直前の確定済みイベント h を journal へ書き出す。確認できなければ、head を進めず保存先エラーにする。
    if let Some(head) = &head {
      #[cfg(feature = "test-hooks")]
      if let Some(hooks) = &shared.hooks {
        hooks.journal_write_previous(aid, head.seq_nr)?;
      }
      let previous_history = head
        .current_snapshot
        .as_ref()
        .filter(|_| head.has_snapshot && shared.keep_snapshot_count.is_some())
        .map(|current| current.bytes.as_slice());
      shared
        .write_journal_row(
          &prefix,
          head.seq_nr,
          &head.last_event_bytes,
          previous_history,
          head.has_snapshot,
        )
        .await?;
    }
    // 手順 4: head の条件付き更新。成功が確定点である。
    #[cfg(feature = "test-hooks")]
    if let Some(hooks) = &shared.hooks {
      hooks.before_commit(aid, seq_nr)?;
    }
    shared
      .commit_head(
        aid,
        seq_nr,
        HeadUpdate {
          prefix: &prefix,
          previous: head.as_ref().map(|head| head.seq_nr),
          event: &event_bytes,
          snapshot: snapshot_bytes.as_deref(),
        },
      )
      .await?;
    #[cfg(feature = "test-hooks")]
    if let Some(hooks) = &shared.hooks {
      hooks.after_commit(aid, seq_nr)?;
    }
    // 手順 5: 新しい末尾を journal へ書き出す。失敗は確定した追記を変えず、別の経路で知らせる。
    let tail_history = snapshot_bytes
      .as_deref()
      .filter(|_| shared.keep_snapshot_count.is_some());
    let tail_written = async {
      #[cfg(feature = "test-hooks")]
      if let Some(hooks) = &shared.hooks {
        hooks.journal_write_tail(aid, seq_nr)?;
      }
      shared
        .write_journal_row(&prefix, seq_nr, &event_bytes, tail_history, snapshot_bytes.is_some())
        .await
    }
    .await;
    if let Err(error) = tail_written {
      notify_journal_write_failure(aid, seq_nr, &error);
    }
    let retention_failure = match (&snapshot_bytes, shared.keep_snapshot_count) {
      (Some(_), Some(keep)) => self.retain_history(aid, &prefix, seq_nr, keep).await.err(),
      _ => None,
    };
    Ok(AppendReceipt { retention_failure })
  }

  async fn load_snapshot(&self, aid: &AidString) -> Result<Option<SnapshotRead<A>>, EventStoreError> {
    let operation = StorageOperation::LoadSnapshot;
    let prefix = self.shared.prefix(aid)?;
    // head と current は、同じ行の 1 回の読み取りで得る（BT-8）。
    let head = self.shared.read_head(&prefix, aid, operation).await?;
    #[cfg(feature = "test-hooks")]
    if let Some(hooks) = &self.shared.hooks {
      hooks.read_snapshot(aid)?;
    }
    let Some(head) = head else {
      return Ok(None);
    };
    let snapshot = head
      .current_snapshot
      .map(|current| {
        self.snapshots.deserialize(&current.fields.payload).map(|aggregate| {
          SnapshotEnvelope::new(aggregate, current.fields.seq_nr).with_manifest(current.fields.manifest)
        })
      })
      .transpose()?;
    Ok(Some(SnapshotRead::new(snapshot, head.seq_nr)))
  }

  async fn load_events(
    &self,
    aggregate_id: &AID,
    aid: &AidString,
    seq_nr: SeqNr,
  ) -> Result<Vec<EventEnvelope<AID, P>>, EventStoreError> {
    let operation = StorageOperation::LoadEvents;
    let start = seq_nr.max(1);
    let prefix = self.shared.prefix(aid)?;
    // 最初に head の h と末尾のイベントを取得し、以後はその値を使う（BT-9）。
    let head = self.shared.read_head(&prefix, aid, operation).await?;
    #[cfg(feature = "test-hooks")]
    if let Some(hooks) = &self.shared.hooks {
      hooks.read_events(aid)?;
    }
    let Some(head) = head else {
      return Ok(Vec::new());
    };
    let h = head.seq_nr;
    if start > h {
      return Ok(Vec::new());
    }
    let mut stored: Vec<EventEnvelopeFields> = Vec::new();
    if start < h {
      stored = self.read_journal_events(&prefix, aid, start, h).await?;
    }
    stored.push(head.last_event);
    stored
      .into_iter()
      .map(|fields| {
        self.events.deserialize(&fields.payload).map(|payload| {
          EventEnvelope::new(
            aggregate_id.clone(),
            fields.seq_nr,
            DateTime::from_timestamp_nanos(fields.occurred_at_ns),
            payload,
          )
          .with_manifest(fields.manifest)
        })
      })
      .collect()
  }
}

impl<A, P> BigtableBackend<A, P> {
  /// journal の `[start, h)` を読み切る。対象の範囲に欠番があれば、短い結果を成功とせず保存先エラーにする（BT-10）。
  async fn read_journal_events(
    &self,
    prefix: &[u8],
    aid: &AidString,
    start: SeqNr,
    h: SeqNr,
  ) -> Result<Vec<EventEnvelopeFields>, EventStoreError> {
    let operation = StorageOperation::LoadEvents;
    let (range_start, range_end) = journal_range(prefix, start, h);
    let rows = self
      .shared
      .journal
      .read_range(
        range_start,
        range_end,
        chain(vec![column(FAMILY_EVENT, EVENT_ENVELOPE), latest_cell()]),
        operation,
      )
      .await?;
    let gap = || invalid_data(operation, "the journal has a gap in the requested range");
    if u64::try_from(rows.len()).ok() != Some(h - start) {
      return Err(gap());
    }
    rows
      .iter()
      .zip(start..h)
      .map(|(row, expected)| {
        if journal_seq_nr(prefix, &row.row_key) != Some(expected) {
          return Err(gap());
        }
        let bytes = row
          .value(FAMILY_EVENT, EVENT_ENVELOPE)
          .ok_or_else(|| invalid_data(operation, "a journal row has no event envelope"))?;
        let fields = decode_event(bytes, operation)?;
        if fields.aid != aid.as_str() || fields.seq_nr != expected {
          return Err(invalid_data(
            operation,
            "a journal row holds an envelope of another aid or seq_nr",
          ));
        }
        Ok(fields)
      })
      .collect()
  }
}
