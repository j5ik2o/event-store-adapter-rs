//! SQLite の保存先（機能 `sqlite`）。
//!
//! 同じホストのローカルファイルを、WAL と `synchronous=FULL` で使う。ストアを使う前に、
//! [`initialize_sqlite_store`] で未初期化のデータベースを明示的に初期化する。
//! [`EventStoreForSqlite::open`] は、初期化済みのストアを開くだけで、欠けた定義を補わない。
//! 最初の版は、変更フィードと期限切れ方式を提供しない。
mod backend;
pub(crate) mod configuration;
pub(crate) mod connection;
#[cfg(test)]
mod connection_test;
pub(crate) mod layout;

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use backend::{SqliteBackend, SqliteShared};
pub use configuration::initialize_sqlite_store;
#[cfg(feature = "test-hooks")]
pub use configuration::{overwrite_sqlite_configuration_for_test, SqliteConfigurationRecord};
use serde::{de::DeserializeOwned, Serialize};

use crate::{
  aggregate_id::AggregateId,
  error::EventStoreError,
  event_envelope::{EventEnvelope, SnapshotEnvelope, SnapshotRead},
  event_store::EventStore,
  generic_event_store::GenericEventStore,
  retention::RetentionSettings,
  seq_nr::SeqNr,
  serializer::{EventSerializer, JsonEventSerializer, JsonSnapshotSerializer, SnapshotSerializer},
};

/// 障害を差し込む試験のためのフック。メモリの `MemoryTestHooks` と同じ流儀で、機能 `test-hooks` の中だけにある。
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub trait SqliteTestHooks: std::fmt::Debug + Send + Sync {
  /// 追記の COMMIT の直前に呼ぶ。`Err` を返すと追記はロールバックされる。
  fn before_commit(&self, aid: &crate::aggregate_id::AidString, seq_nr: SeqNr) -> Result<(), EventStoreError>;
  /// イベント取得の読み取りトランザクションの中で、head を読んだ直後に呼ぶ。
  fn read_events(&self, aid: &crate::aggregate_id::AidString) -> Result<(), EventStoreError>;
  /// 最新スナップショット取得の読み取りトランザクションの中で、head を読んだ直後に呼ぶ。
  fn read_snapshot(&self, aid: &crate::aggregate_id::AidString) -> Result<(), EventStoreError>;
  /// 保持処理が見る履歴を差し替える。
  fn retention_visible_history(
    &self,
    _aid: &crate::aggregate_id::AidString,
    history: &[SeqNr],
    _just_written: Option<SeqNr>,
  ) -> Result<Vec<SeqNr>, EventStoreError> {
    Ok(history.to_vec())
  }
  /// 保持処理の削除の直前に呼ぶ。
  fn retention_delete(&self, _aid: &crate::aggregate_id::AidString, _seq_nrs: &[SeqNr]) -> Result<(), EventStoreError> {
    Ok(())
  }
}

/// SQLite の保存先の設定。
#[derive(Debug, Clone)]
pub struct SqliteOptions {
  /// スナップショット保持の設定。保持件数 0 と期限切れ方式（`Ttl`）は設定エラーになる。
  pub retention: RetentionSettings,
  /// 変更フィードを求めるか。最初の版は提供しないので、`true` は設定エラーになる。
  pub change_feed: bool,
  /// 別の接続が書き込みのロックを持つ間に待つ時間（busy の待ち時間）。既定は 5 秒。
  /// 32bit のミリ秒に収まらない値は設定エラーになる。
  pub lock_wait: Duration,
}

impl Default for SqliteOptions {
  fn default() -> Self {
    Self {
      retention: RetentionSettings::current_only(),
      change_feed: false,
      lock_wait: Duration::from_secs(5),
    }
  }
}

/// SQLite の保存先に委譲するイベントストア。
pub struct EventStoreForSqlite<AID, A, P> {
  inner: GenericEventStore<AID, A, P, SqliteBackend<A, P>>,
  #[cfg(feature = "test-hooks")]
  shared: Arc<SqliteShared>,
}

impl<AID, A, P> Clone for EventStoreForSqlite<AID, A, P> {
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
      #[cfg(feature = "test-hooks")]
      shared: self.shared.clone(),
    }
  }
}

impl<AID, A, P> std::fmt::Debug for EventStoreForSqlite<AID, A, P> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("EventStoreForSqlite")
      .field("inner", &self.inner)
      .finish()
  }
}

impl<AID, A, P> EventStoreForSqlite<AID, A, P> {
  /// 初期化済みのストアを、与えた直列化で開く。配置と config をクライアントの設定と照合する。
  pub fn open_with_serializers(
    path: impl AsRef<Path>,
    options: SqliteOptions,
    event_serializer: Arc<dyn EventSerializer<P>>,
    snapshot_serializer: Arc<dyn SnapshotSerializer<A>>,
  ) -> Result<Self, EventStoreError> {
    Self::build(
      path.as_ref(),
      &options,
      event_serializer,
      snapshot_serializer,
      #[cfg(feature = "test-hooks")]
      None,
    )
  }

  /// 初期化済みのストアを、障害を差し込むフック付きで開く。
  #[cfg(feature = "test-hooks")]
  #[doc(hidden)]
  pub fn open_with_hooks(
    path: impl AsRef<Path>,
    options: SqliteOptions,
    event_serializer: Arc<dyn EventSerializer<P>>,
    snapshot_serializer: Arc<dyn SnapshotSerializer<A>>,
    hooks: Arc<dyn SqliteTestHooks>,
  ) -> Result<Self, EventStoreError> {
    Self::build(
      path.as_ref(),
      &options,
      event_serializer,
      snapshot_serializer,
      Some(hooks),
    )
  }

  fn build(
    path: &Path,
    options: &SqliteOptions,
    event_serializer: Arc<dyn EventSerializer<P>>,
    snapshot_serializer: Arc<dyn SnapshotSerializer<A>>,
    #[cfg(feature = "test-hooks")] hooks: Option<Arc<dyn SqliteTestHooks>>,
  ) -> Result<Self, EventStoreError> {
    let opened = configuration::open_checked(path, options)?;
    let shared = Arc::new(SqliteShared {
      connection: Mutex::new(opened.connection),
      keep_snapshot_count: opened.keep_snapshot_count,
      store_id: opened.store_id,
      #[cfg(feature = "test-hooks")]
      hooks,
    });
    Ok(Self {
      inner: GenericEventStore::new(SqliteBackend {
        shared: shared.clone(),
        events: event_serializer,
        snapshots: snapshot_serializer,
      }),
      #[cfg(feature = "test-hooks")]
      shared,
    })
  }

  /// 試験用に、aid の `snapshot_history` の seq_nr を昇順で返す。
  #[cfg(feature = "test-hooks")]
  #[doc(hidden)]
  pub fn history_view(&self, aid: &crate::aggregate_id::AidString) -> Result<Vec<SeqNr>, EventStoreError> {
    backend::history(&self.shared, aid)
  }
}

impl<AID, A, P> EventStoreForSqlite<AID, A, P>
where
  A: Serialize + DeserializeOwned + Send + Sync + 'static,
  P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
  /// 初期化済みのストアを、既定の JSON 直列化で開く。
  pub fn open(path: impl AsRef<Path>, options: SqliteOptions) -> Result<Self, EventStoreError> {
    Self::open_with_serializers(
      path,
      options,
      Arc::new(JsonEventSerializer::new()),
      Arc::new(JsonSnapshotSerializer::new()),
    )
  }
}

#[async_trait]
impl<AID: AggregateId, A: Send + Sync + 'static, P: Send + Sync + 'static> EventStore
  for EventStoreForSqlite<AID, A, P>
{
  type A = A;
  type AID = AID;
  type P = P;

  async fn persist_event(&self, event: EventEnvelope<AID, P>) -> Result<(), EventStoreError> {
    self.inner.persist_event(event).await
  }

  async fn persist_event_and_snapshot(
    &self,
    event: EventEnvelope<AID, P>,
    snapshot: SnapshotEnvelope<A>,
  ) -> Result<(), EventStoreError> {
    self.inner.persist_event_and_snapshot(event, snapshot).await
  }

  async fn get_latest_snapshot_by_id(&self, aid: &AID) -> Result<Option<SnapshotRead<A>>, EventStoreError> {
    self.inner.get_latest_snapshot_by_id(aid).await
  }

  async fn get_events_by_id_since_seq_nr(
    &self,
    aid: &AID,
    seq_nr: SeqNr,
  ) -> Result<Vec<EventEnvelope<AID, P>>, EventStoreError> {
    self.inner.get_events_by_id_since_seq_nr(aid, seq_nr).await
  }
}
