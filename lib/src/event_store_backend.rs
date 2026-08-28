use std::fmt::Debug;

use crate::event_envelope::{EventEnvelope, SnapshotEnvelope};
use crate::types::{EventStoreReadError, EventStoreWriteError};
use async_trait::async_trait;
use chrono::Duration;

// FR4.1 / FR5.1 / C2: 非公開 SPI。GenericEventStore と各バックエンドの境界を封筒
// （EventEnvelope / SnapshotEnvelope）の受け渡しに統一する。SnapshotEnvelope は v3 で
// `event_envelope` モジュールへ移設・公開昇格した（FR2.1）。

/// 保持ポリシー設定の値オブジェクト（SPI 運搬用）。
///
/// BR4.1: 有効値は `keep_snapshot_count = None`（剪定しない）と `Some(n >= 1)`
/// （履歴 n 件保持）のみ。`Some(0)` はビルダーで拒否されるため、ここには到達しない。
/// `delete_ttl` は `keep_snapshot_count` と併用時のみ効果を持つ（現行契約維持）。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SnapshotMaintenance {
  pub keep_snapshot_count: Option<usize>,
  pub delete_ttl: Option<Duration>,
}

/// ストレージバックエンドの SPI を表すトレイト（非公開 mod — semver 制約なし）。
///
/// 型パラメータへの境界は最小に留める（BR1.6 — payload への Debug / Clone 要求を
/// 作らない）。`AID: Sync` のみ、`on_event_persisted` の既定実装が `&AID` を
/// Send な Future に保持するために要求する。
#[async_trait]
pub trait StorageBackend<AID, A, P>: Send + Sync + Clone + Debug + 'static
where
  AID: Sync, {
  /// 最新のスナップショット封筒を取得する。存在しなければ `None` を返す（BR3.1）。
  async fn fetch_latest_snapshot(&self, aid: &AID) -> Result<Option<SnapshotEnvelope<A>>, EventStoreReadError>;

  /// 指定 seq_nr 以降のイベント封筒列を返す（裸の payload 列は返さない — FR5.1 / BR3.2）。
  async fn fetch_events_since(
    &self,
    aid: &AID,
    seq_nr: usize,
  ) -> Result<Vec<EventEnvelope<AID, P>>, EventStoreReadError>;

  /// 新規集約の journal + snapshot を原子的に作成する（W1）。
  ///
  /// snapshot は version = 1、seq_nr = `event.seq_nr()` で作成する。既存集約が
  /// 存在する場合は `OptimisticLockError` を返す（一意制約 / 条件付き書込）。
  async fn create_event_and_snapshot(
    &self,
    event: &EventEnvelope<AID, P>,
    aggregate: &A,
    maintenance: &SnapshotMaintenance,
  ) -> Result<(), EventStoreWriteError>;

  /// 既存集約の更新を version CAS で原子的に書く（W2 / W3）。
  ///
  /// BR2.3: 格納中の version と `expected_version` を照合し、競合時は
  /// `OptimisticLockError`、成功時は version = expected + 1 とする（version 加算は列側）。
  /// `aggregate = None` はイベントのみ更新（`persist_event` 経路）。
  async fn update_event_and_snapshot(
    &self,
    event: &EventEnvelope<AID, P>,
    aggregate: Option<&A>,
    expected_version: usize,
    maintenance: &SnapshotMaintenance,
  ) -> Result<(), EventStoreWriteError>;

  /// 書込成功後のフック（保持ポリシー実行 — BR4.2）。既定は no-op。
  async fn on_event_persisted(
    &self,
    _aid: &AID,
    _maintenance: &SnapshotMaintenance,
  ) -> Result<(), EventStoreWriteError> {
    Ok(())
  }
}
