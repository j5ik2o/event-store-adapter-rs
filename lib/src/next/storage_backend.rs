use async_trait::async_trait;

use crate::next::aggregate_id::AidString;
use crate::next::error::{EventStoreError, RetentionFailure};
use crate::next::event_envelope::{EventEnvelope, SnapshotEnvelope, SnapshotRead};
use crate::next::seq_nr::SeqNr;

// 保存先の差を吸収する非公開の trait。実装するのは、保存先のハンドルとシリアライザを持つ
// ストアのインスタンスである（PR 8 以降）。
#[async_trait]
pub(crate) trait StorageBackend<AID, A, P>: Send + Sync + 'static {
  /// 追記を確定し、保存先ごとの時機で保持処理を行う。
  ///
  /// 追記が確定していれば `Ok` を返す。保持処理の失敗は `AppendReceipt` に載せ、`Err` にしない（S-4）。
  async fn append(&self, request: AppendRequest<'_, AID, A, P>) -> Result<AppendReceipt, EventStoreError>;

  /// 最新スナップショットとヘッドの seq_nr を読む（R-1〜R-3）。スナップショットの封筒には aggregate_id がない。
  async fn load_snapshot(&self, aid: &AidString) -> Result<Option<SnapshotRead<A>>, EventStoreError>;

  /// `seq_nr` 以上のイベント封筒を昇順で、すべて返す（R-4〜R-6）。
  ///
  /// `aggregate_id` は、公開操作が受け取った元の値である。返す封筒の `aggregate_id` に複製して使う。
  async fn load_events(
    &self,
    aggregate_id: &AID,
    aid: &AidString,
    seq_nr: SeqNr,
  ) -> Result<Vec<EventEnvelope<AID, P>>, EventStoreError>;
}

/// 追記の入力。共通の層が検査を済ませた値を、参照で渡す。
// PR 7 は保存先を実装しないため、フィールドを読む本番の実装者がいない。PR 8 で除去する。
#[allow(dead_code)]
pub(crate) struct AppendRequest<'a, AID, A, P> {
  /// 検査済みの aid 文字列（T-1・T-11・T-12）。保存先のキーに使う。
  pub aid: &'a AidString,
  /// 元の aggregate_id を持つ。直列化は保存先が行う（MEM-6）。
  pub event: &'a EventEnvelope<AID, P>,
  /// `Some` のとき `seq_nr` は event と一致する（W-9 は検査済み）。
  pub snapshot: Option<&'a SnapshotEnvelope<A>>,
}

/// 追記の結果。保持処理の失敗は別の経路で知らせる（S-4）。
// PR 7 は保存先を実装しないため、フィールドを読む本番の呼び出し元がいない。PR 8 で除去する。
#[allow(dead_code)]
pub(crate) struct AppendReceipt {
  pub retention_failure: Option<RetentionFailure>,
}
