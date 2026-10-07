use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;

use crate::next::aggregate_id::{AggregateId, AidString};
use crate::next::error::{ContractRule, EventStoreError};
use crate::next::event_envelope::{EventEnvelope, SnapshotEnvelope, SnapshotRead};
use crate::next::event_store::EventStore;
use crate::next::seq_nr::{SeqNr, SEQ_NR_MAX};
use crate::next::storage_backend::{AppendReceipt, AppendRequest, StorageBackend};

// 共通の入口検査（設計 2.8 の 1〜5）を 1 か所に置く。順序は
// aid の組み立て（T-11・T-12）→ seq_nr の上限（T-9）→ イベントの seq_nr 0（W-6）→
// 時刻範囲（T-13）→ スナップショットの seq_nr 一致（W-9）である。
//
// PR 7 は保存先を実装しないため、これらの検査は単体試験だけが観測する。PR 8 の
// GenericEventStore から消費される。

fn contract_violation(rule: ContractRule, seq_nr: Option<SeqNr>, snapshot_seq_nr: Option<SeqNr>) -> EventStoreError {
  EventStoreError::ContractViolation {
    rule,
    seq_nr,
    snapshot_seq_nr,
  }
}

/// 読み取りの `seq_nr` 引数を検査する（T-9）。0 は有効（全件を返す）。
pub(crate) fn check_read_seq_nr(seq_nr: SeqNr) -> Result<(), EventStoreError> {
  if seq_nr > SEQ_NR_MAX {
    return Err(contract_violation(ContractRule::T9, Some(seq_nr), None));
  }
  Ok(())
}

/// イベント封筒の入口検査を行い、組み立てた aid を返す。
///
/// 順序: aid の組み立て（T-1・T-11・T-12）→ seq_nr の上限（T-9）→ seq_nr が 0 でない（W-6）→
/// `occurred_at` がナノ秒の範囲内（T-13）。
pub(crate) fn check_event<AID: AggregateId, P>(event: &EventEnvelope<AID, P>) -> Result<AidString, EventStoreError> {
  let aid = AidString::from_aggregate_id(event.aggregate_id())?;
  let seq_nr = event.seq_nr();
  if seq_nr > SEQ_NR_MAX {
    return Err(contract_violation(ContractRule::T9, Some(seq_nr), None));
  }
  if seq_nr == 0 {
    return Err(contract_violation(ContractRule::W6, Some(seq_nr), None));
  }
  if event.occurred_at().timestamp_nanos_opt().is_none() {
    return Err(contract_violation(ContractRule::T13, Some(seq_nr), None));
  }
  Ok(aid)
}

/// イベント封筒とスナップショット封筒の入口検査を行い、組み立てた aid を返す。
///
/// `check_event` の検査に加え、W-9: `snapshot.seq_nr() != event.seq_nr()` なら契約違反。
pub(crate) fn check_event_and_snapshot<AID: AggregateId, A, P>(
  event: &EventEnvelope<AID, P>,
  snapshot: &SnapshotEnvelope<A>,
) -> Result<AidString, EventStoreError> {
  let aid = check_event(event)?;
  let seq_nr = event.seq_nr();
  let snapshot_seq_nr = snapshot.seq_nr();
  if snapshot_seq_nr != seq_nr {
    return Err(contract_violation(
      ContractRule::W9,
      Some(seq_nr),
      Some(snapshot_seq_nr),
    ));
  }
  Ok(aid)
}

// fn ポインタ経由の型マーカー — Send/Sync の自動導出を阻害しない。
type TypeMarker<AID, A, P> = fn() -> (AID, A, P);

/// 保存先への委譲で `EventStore` を提供する汎用イベントストア（非公開 SPI）。
///
/// PR 7 では保存先を実装しないため、本番の利用者はまだいない。PR 8 で
/// `MemoryStorage` から使う。
#[allow(dead_code)]
pub(crate) struct GenericEventStore<AID, A, P, B> {
  backend: B,
  _phantom: PhantomData<TypeMarker<AID, A, P>>,
}

#[allow(dead_code)]
impl<AID, A, P, B: Debug> Debug for GenericEventStore<AID, A, P, B> {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    formatter
      .debug_struct("GenericEventStore")
      .field("backend", &self.backend)
      .finish()
  }
}

#[allow(dead_code)]
impl<AID, A, P, B: Clone> Clone for GenericEventStore<AID, A, P, B> {
  fn clone(&self) -> Self {
    Self {
      backend: self.backend.clone(),
      _phantom: PhantomData,
    }
  }
}

#[allow(dead_code)]
impl<AID, A, P, B> GenericEventStore<AID, A, P, B> {
  /// 保存先を受け取ってストアを作る。
  pub(crate) fn new(backend: B) -> Self {
    Self {
      backend,
      _phantom: PhantomData,
    }
  }
}

#[allow(dead_code)]
#[async_trait]
impl<AID, A, P, B> EventStore for GenericEventStore<AID, A, P, B>
where
  AID: AggregateId,
  A: Send + Sync + 'static,
  P: Send + Sync + 'static,
  B: StorageBackend<AID, A, P> + Debug + Clone,
{
  type A = A;
  type AID = AID;
  type P = P;

  async fn persist_event(&self, event: EventEnvelope<Self::AID, Self::P>) -> Result<(), EventStoreError> {
    let aid = check_event(&event)?;
    let receipt = self
      .backend
      .append(AppendRequest {
        aid: &aid,
        event: &event,
        snapshot: None,
      })
      .await?;
    notify_retention_failure(receipt);
    Ok(())
  }

  async fn persist_event_and_snapshot(
    &self,
    event: EventEnvelope<Self::AID, Self::P>,
    snapshot: SnapshotEnvelope<Self::A>,
  ) -> Result<(), EventStoreError> {
    let aid = check_event_and_snapshot(&event, &snapshot)?;
    let receipt = self
      .backend
      .append(AppendRequest {
        aid: &aid,
        event: &event,
        snapshot: Some(&snapshot),
      })
      .await?;
    notify_retention_failure(receipt);
    Ok(())
  }

  async fn get_latest_snapshot_by_id(&self, aid: &Self::AID) -> Result<Option<SnapshotRead<Self::A>>, EventStoreError> {
    let aid = AidString::from_aggregate_id(aid)?;
    self.backend.load_snapshot(&aid).await
  }

  async fn get_events_by_id_since_seq_nr(
    &self,
    aid: &Self::AID,
    seq_nr: SeqNr,
  ) -> Result<Vec<EventEnvelope<Self::AID, Self::P>>, EventStoreError> {
    let aid_string = AidString::from_aggregate_id(aid)?;
    check_read_seq_nr(seq_nr)?;
    self.backend.load_events(aid, &aid_string, seq_nr).await
  }
}

fn notify_retention_failure(receipt: AppendReceipt) {
  if let Some(failure) = receipt.retention_failure {
    // 通知処理の失敗を、確定済みの追記へ戻さない（MEM-11）。
    let _ = std::panic::catch_unwind(|| {
      tracing::warn!(
        target: "event_store_adapter::retention",
        category = "retention-failure",
        aid = %failure.aid,
        seq_nr = failure.seq_nr,
        phase = %failure.phase,
        error = %failure.error,
        "snapshot retention failed; the append was committed"
      );
    });
  }
}
