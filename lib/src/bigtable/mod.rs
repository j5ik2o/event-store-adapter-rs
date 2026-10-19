//! Bigtable の保存先（機能 `bigtable`）。
//!
//! 確定点を、head の 1 行の条件付き更新に置く。head を進める前に、置き換わる直前のイベントを journal の行へ書き出す。
//! 読み取りは、journal の範囲と、先に取得した head の末尾イベントを合わせて行う。履歴は journal の行に置き、
//! 削除しても、イベントのセルと削除済みの印を残す。
//!
//! ストアを使う前に、journal・head の 2 テーブルを列ファミリ付きで用意し（`config`・`event`・`history` と
//! `config`・`head`・`snapshot`）、[`initialize_bigtable_store`] で同じ store_id の設定行を作る。
//! [`EventStoreForBigtable::open`] は、列ファミリと設定行を照合するだけで、テーブルも設定行も作らない。
//! 最初の版は、変更フィードと期限切れ方式を提供しない。認証と、稼働中の操作を同じクラスタへ固定する app profile
//! （BT-1）は、利用者が `Channel` と [`BigtableOptions::app_profile_id`] で用意する。
mod backend;
mod configuration;
mod envelope;
mod keys;
mod retention;
mod rows;

use std::sync::Arc;

use async_trait::async_trait;
use backend::{BigtableBackend, BigtableShared};
pub use configuration::initialize_bigtable_store;
#[cfg(feature = "test-hooks")]
pub use configuration::{overwrite_bigtable_configuration_for_test, BigtableConfigurationRecord};
#[cfg(feature = "test-hooks")]
pub use envelope::{
  decode_event as decode_bigtable_event_envelope_for_test,
  decode_snapshot as decode_bigtable_snapshot_envelope_for_test,
  encode_event as encode_bigtable_event_envelope_for_test,
  encode_snapshot as encode_bigtable_snapshot_envelope_for_test, EventEnvelopeFields, SnapshotEnvelopeFields,
};
use serde::{de::DeserializeOwned, Serialize};
use tonic::transport::Channel;

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
///
/// フックは同期の関数で、追記・読み取りの途中の決まった時機に呼ばれる。`Err` を返すと、その時機の失敗を表す。
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub trait BigtableTestHooks: std::fmt::Debug + Send + Sync {
  /// head の条件付き更新を送る前に呼ぶ。`Err` を返すと、送らずに追記は失敗する。
  fn before_commit(&self, aid: &crate::aggregate_id::AidString, seq_nr: SeqNr) -> Result<(), EventStoreError>;
  /// head の条件付き更新を送り、応答を受け取った後に呼ぶ。`Err` を返すと、応答の消失を表す（確定した後の失敗）。
  fn after_commit(&self, aid: &crate::aggregate_id::AidString, seq_nr: SeqNr) -> Result<(), EventStoreError>;
  /// 直前の確定済みイベント（`seq_nr`）を journal へ書き出す前に呼ぶ。`Err` を返すと、書き出しの失敗を表す。
  fn journal_write_previous(&self, aid: &crate::aggregate_id::AidString, seq_nr: SeqNr) -> Result<(), EventStoreError>;
  /// 確定した新しい末尾のイベントを journal へ書き出す前に呼ぶ。`Err` を返すと、書き出しの失敗を表す。
  fn journal_write_tail(&self, aid: &crate::aggregate_id::AidString, seq_nr: SeqNr) -> Result<(), EventStoreError>;
  /// イベント取得で、head を読んだ直後に呼ぶ。
  fn read_events(&self, aid: &crate::aggregate_id::AidString) -> Result<(), EventStoreError>;
  /// 最新スナップショット取得で、head を読んだ直後に呼ぶ。
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
  /// 行キーのハッシュを差し替える。`Some` を返すと、FNV-1a 64 の代わりにその値を使う（ハッシュの衝突の試験）。
  fn aggregate_hash(&self, _aid: &crate::aggregate_id::AidString) -> Option<u64> {
    None
  }
}

/// データ API と管理 API の接続。
///
/// データ API は読み書きに、管理 API は生成時の列ファミリの照合（`GetTable`）だけに使う。
/// エミュレーターでは、同じ `Channel` を両方に入れられる。認証は、`Channel` を作る側が用意する。
#[derive(Debug, Clone)]
pub struct BigtableConnection {
  pub data: Channel,
  pub admin: Channel,
}

/// 用意済みの journal・head の 2 テーブル。
#[derive(Debug, Clone)]
pub struct BigtableTables {
  pub project_id: String,
  pub instance_id: String,
  pub journal_table_id: String,
  pub head_table_id: String,
}

/// Bigtable の保存先の設定。
#[derive(Debug, Clone)]
pub struct BigtableOptions {
  /// スナップショット保持の設定。保持件数 0 と期限切れ方式（`Ttl`）は設定エラーになる。
  pub retention: RetentionSettings,
  /// 変更フィードを求めるか。最初の版は提供しないので、`true` は設定エラーになる。
  pub change_feed: bool,
  /// メタデータを含む直列化後の 1 封筒の上限（バイト数）。既定は 1048576、17 以上 8388608 以下。
  /// 両テーブルの設定行に記録され、範囲外の値と、記録した値との食い違いは設定エラーになる。
  pub envelope_limit_bytes: u64,
  /// 全てのデータ API の要求に付ける app profile の ID。空なら、インスタンスの既定の app profile を使う。
  /// 稼働中の全操作を同じクラスタへ固定した app profile を指定する（BT-1）。ライブラリは app profile を照合しない。
  pub app_profile_id: String,
}

impl Default for BigtableOptions {
  fn default() -> Self {
    Self {
      retention: RetentionSettings::current_only(),
      change_feed: false,
      envelope_limit_bytes: 1_048_576,
      app_profile_id: String::new(),
    }
  }
}

/// Bigtable の保存先に委譲するイベントストア。
pub struct EventStoreForBigtable<AID, A, P> {
  inner: GenericEventStore<AID, A, P, BigtableBackend<A, P>>,
  #[cfg(feature = "test-hooks")]
  shared: Arc<BigtableShared>,
}

impl<AID, A, P> Clone for EventStoreForBigtable<AID, A, P> {
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
      #[cfg(feature = "test-hooks")]
      shared: self.shared.clone(),
    }
  }
}

impl<AID, A, P> std::fmt::Debug for EventStoreForBigtable<AID, A, P> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("EventStoreForBigtable")
      .field("inner", &self.inner)
      .finish()
  }
}

impl<AID, A, P> EventStoreForBigtable<AID, A, P> {
  /// 初期化済みのストアを、与えた直列化で開く。列ファミリと設定行をクライアントの設定と照合する。
  pub async fn open_with_serializers(
    connection: BigtableConnection,
    tables: BigtableTables,
    options: BigtableOptions,
    event_serializer: Arc<dyn EventSerializer<P>>,
    snapshot_serializer: Arc<dyn SnapshotSerializer<A>>,
  ) -> Result<Self, EventStoreError> {
    Self::build(
      &connection,
      &tables,
      &options,
      event_serializer,
      snapshot_serializer,
      #[cfg(feature = "test-hooks")]
      None,
    )
    .await
  }

  /// 初期化済みのストアを、障害を差し込むフック付きで開く。
  #[cfg(feature = "test-hooks")]
  #[doc(hidden)]
  pub async fn open_with_hooks(
    connection: BigtableConnection,
    tables: BigtableTables,
    options: BigtableOptions,
    event_serializer: Arc<dyn EventSerializer<P>>,
    snapshot_serializer: Arc<dyn SnapshotSerializer<A>>,
    hooks: Arc<dyn BigtableTestHooks>,
  ) -> Result<Self, EventStoreError> {
    Self::build(
      &connection,
      &tables,
      &options,
      event_serializer,
      snapshot_serializer,
      Some(hooks),
    )
    .await
  }

  async fn build(
    connection: &BigtableConnection,
    tables: &BigtableTables,
    options: &BigtableOptions,
    event_serializer: Arc<dyn EventSerializer<P>>,
    snapshot_serializer: Arc<dyn SnapshotSerializer<A>>,
    #[cfg(feature = "test-hooks")] hooks: Option<Arc<dyn BigtableTestHooks>>,
  ) -> Result<Self, EventStoreError> {
    let opened = configuration::open_checked(connection, tables, options).await?;
    let shared = Arc::new(BigtableShared {
      journal: opened.journal,
      head: opened.head,
      keep_snapshot_count: opened.keep_snapshot_count,
      envelope_limit_bytes: opened.envelope_limit_bytes,
      store_id: opened.store_id,
      #[cfg(feature = "test-hooks")]
      hooks,
    });
    Ok(Self {
      inner: GenericEventStore::new(BigtableBackend {
        shared: shared.clone(),
        events: event_serializer,
        snapshots: snapshot_serializer,
      }),
      #[cfg(feature = "test-hooks")]
      shared,
    })
  }

  /// 試験用に、aid の履歴の seq_nr を昇順で返す。journal の active な行と、まだ書き出されていない head の末尾の履歴である。
  #[cfg(feature = "test-hooks")]
  #[doc(hidden)]
  pub async fn history_view(&self, aid: &crate::aggregate_id::AidString) -> Result<Vec<SeqNr>, EventStoreError> {
    self.shared.history(aid).await
  }
}

impl<AID, A, P> EventStoreForBigtable<AID, A, P>
where
  A: Serialize + DeserializeOwned + Send + Sync + 'static,
  P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
  /// 初期化済みのストアを、既定の JSON 直列化で開く。
  pub async fn open(
    connection: BigtableConnection,
    tables: BigtableTables,
    options: BigtableOptions,
  ) -> Result<Self, EventStoreError> {
    Self::open_with_serializers(
      connection,
      tables,
      options,
      Arc::new(JsonEventSerializer::new()),
      Arc::new(JsonSnapshotSerializer::new()),
    )
    .await
  }
}

#[async_trait]
impl<AID: AggregateId, A: Send + Sync + 'static, P: Send + Sync + 'static> EventStore
  for EventStoreForBigtable<AID, A, P>
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
