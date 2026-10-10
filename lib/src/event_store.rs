use std::fmt::Debug;

use async_trait::async_trait;

use crate::aggregate_id::AggregateId;
use crate::error::EventStoreError;
use crate::event_envelope::{EventEnvelope, SnapshotEnvelope, SnapshotRead};
use crate::seq_nr::SeqNr;

/// 4 つの操作を提供するイベントストアの契約（共通契約 3 章）。
///
/// 排他制御は保存先が持つので、`&self` で並行呼び出しを受けられる。非同期は `#[async_trait]` で表す。
#[async_trait]
pub trait EventStore: Debug + Clone + Send + Sync + 'static {
  /// 集約 ID の型。
  type AID: AggregateId;
  /// 集約状態（スナップショットの payload）の型。
  type A: Send + Sync + 'static;
  /// イベントの payload の型。
  type P: Send + Sync + 'static;

  /// 3.1 persistEvent。イベントだけを追記する。
  async fn persist_event(&self, event: EventEnvelope<Self::AID, Self::P>) -> Result<(), EventStoreError>;

  /// 3.2 persistEventAndSnapshot。イベントを追記し、同時にスナップショットを書く。
  ///
  /// W-9: `snapshot.seq_nr() != event.seq_nr()` なら契約違反。
  async fn persist_event_and_snapshot(
    &self,
    event: EventEnvelope<Self::AID, Self::P>,
    snapshot: SnapshotEnvelope<Self::A>,
  ) -> Result<(), EventStoreError>;

  /// 3.4 getLatestSnapshotById。ヘッドがなければ `None`（R-1）。
  async fn get_latest_snapshot_by_id(&self, aid: &Self::AID) -> Result<Option<SnapshotRead<Self::A>>, EventStoreError>;

  /// 3.5 getEventsByIdSinceSeqNr。`seq_nr` 以上のイベント封筒を昇順で、すべて返す（R-4〜R-6）。
  async fn get_events_by_id_since_seq_nr(
    &self,
    aid: &Self::AID,
    seq_nr: SeqNr,
  ) -> Result<Vec<EventEnvelope<Self::AID, Self::P>>, EventStoreError>;
}
