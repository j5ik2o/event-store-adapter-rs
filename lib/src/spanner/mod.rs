//! Spanner の保存先（機能 `spanner`）。
//!
//! 5 テーブル（journal・head・snapshot_current・snapshot_history・config）を同じデータベースに置き、追記を
//! 1 つの SERIALIZABLE の読み書きトランザクションで確定する。確定点は、そのトランザクションのコミットの成功である。
//! 読み取りは、head と続けて読む行を、同じ strong の読み取り専用トランザクションの同じ読み取り時点で読む。
//! 古いスナップショットの履歴の整理は、追記の確定後の別のトランザクションで行い、その失敗は確定した追記を変えない。
//!
//! 5 テーブルの名前は、同じ接頭辞（[`SpannerTables::table_prefix`]）から `<接頭辞>_journal` のように導く。
//! ストアを使う前に、利用者が DDL で 5 テーブル（期限切れ方式では、snapshot_history の行削除ポリシーも）を用意し、
//! [`initialize_spanner_store`] で config の行を 1 行書く。[`EventStoreForSpanner::open`] は、方言・列・主キー・
//! 行削除ポリシー・config の行をクライアントの設定と照合するだけで、DDL を実行せず、行も作らない。
//! 最初の版は、GoogleSQL の方言だけを対象にし、変更フィードを提供しない。
//!
//! # 接続と認証
//!
//! 接続（tonic の `Channel`）は利用者が作って [`SpannerConnection`] に渡す。ライブラリは `Channel` を作らず、
//! Google の認証のライブラリにも依存しない。
//!
//! - エミュレーター: 平文の `Channel` を [`SpannerConnection::new`] に渡す。トークンの取得元は要らない。
//! - 本番の Cloud Spanner: TLS の `Channel`（利用者が tonic の TLS の機能を自分の依存で有効にして作る）と、
//!   トークンの取得元 [`SpannerTokenSource`] を [`SpannerConnection::with_token_source`] で渡す。
//!   すべての要求の直前に取得元を呼び、`authorization: Bearer <トークン>` を付ける。
//!
//! # v4 からの移行
//!
//! rs の旧版には Spanner の保存先がないので、移行ツールはない。新規に導入する手順は、README とスキーマ文書を参照する。
pub(crate) mod auth;
pub(crate) mod backend;
pub(crate) mod client;
pub(crate) mod configuration;
mod retention;
pub(crate) mod values;

use std::sync::Arc;

use async_trait::async_trait;
use auth::Authorizer;
pub use auth::SpannerTokenSource;
use backend::{SpannerBackend, SpannerShared};
#[cfg(feature = "test-hooks")]
use googleapis_tonic_google_spanner_v1::google::spanner::v1::TransactionOptions;
use serde::{de::DeserializeOwned, Serialize};
use tonic::transport::Channel;

use crate::aggregate_id::AggregateId;
use crate::error::EventStoreError;
use crate::event_envelope::{EventEnvelope, SnapshotEnvelope, SnapshotRead};
use crate::event_store::EventStore;
use crate::generic_event_store::GenericEventStore;
use crate::seq_nr::SeqNr;
use crate::serializer::{EventSerializer, JsonEventSerializer, JsonSnapshotSerializer, SnapshotSerializer};

#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub use crate::key_hash::fnv1a64 as fnv1a64_for_test;

/// コミットに差し込む障害（試験用）。
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
#[derive(Debug)]
pub enum SpannerCommitFault {
  /// コミットが ABORTED になったことを表す。再試行の対象になる（SPN-7）。
  Aborted,
  /// コミットが ABORTED 以外で失敗したことを表す。再試行しない。
  Failed(EventStoreError),
}

/// 障害を差し込む試験のためのフック。機能 `test-hooks` の中だけにある。
///
/// フックは同期の関数で、追記・読み取りの途中の決まった時機に呼ばれる。どの関数にも、何もしない既定の実装がある。
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub trait SpannerTestHooks: std::fmt::Debug + Send + Sync {
  /// トランザクションを始める直前に呼ぶ（読み書き・読み取り専用のどちらも）。
  fn before_begin_transaction(&self, _options: &TransactionOptions) {}
  /// 追記の 1 回の試行を始める前に呼ぶ。トランザクションを開く前である。`attempt` は 0 から始まる（0 が初回）。
  fn before_attempt(&self, _aid: &crate::aggregate_id::AidString, _seq_nr: SeqNr, _attempt: u32) {}
  /// コミットを送る直前に呼ぶ。`Err` を返すと、送らずに、その失敗を表す。
  fn before_commit(&self, _aid: &crate::aggregate_id::AidString, _seq_nr: SeqNr) -> Result<(), SpannerCommitFault> {
    Ok(())
  }
  /// コミットの応答を受け取った後に呼ぶ。`Err` を返すと、応答の消失を表す（確定した後の失敗）。
  fn after_commit(&self, _aid: &crate::aggregate_id::AidString, _seq_nr: SeqNr) -> Result<(), EventStoreError> {
    Ok(())
  }
  /// イベント取得で、読み取り専用トランザクションの中で head を読んだ直後に呼ぶ。
  fn read_events(&self, _aid: &crate::aggregate_id::AidString) -> Result<(), EventStoreError> {
    Ok(())
  }
  /// 最新スナップショット取得で、読み取り専用トランザクションの中で head を読んだ直後に呼ぶ。
  fn read_snapshot(&self, _aid: &crate::aggregate_id::AidString) -> Result<(), EventStoreError> {
    Ok(())
  }
  /// 保持処理が見る履歴（印のない行の seq_nr）を差し替える。`Err` を返すと、retention-query の失敗になる。
  fn retention_visible_history(
    &self,
    _aid: &crate::aggregate_id::AidString,
    history: &[SeqNr],
    _just_written: Option<SeqNr>,
  ) -> Result<Vec<SeqNr>, EventStoreError> {
    Ok(history.to_vec())
  }
  /// 保持処理の削除の DML の直前に呼ぶ。`Err` を返すと、retention-delete の失敗になり、トランザクションは確定しない。
  fn retention_delete(&self, _aid: &crate::aggregate_id::AidString, _seq_nrs: &[SeqNr]) -> Result<(), EventStoreError> {
    Ok(())
  }
  /// 保持処理の印付けの DML の直前に呼ぶ。`Err` を返すと、retention-mark の失敗になり、トランザクションは確定しない。
  fn retention_mark(&self, _aid: &crate::aggregate_id::AidString, _seq_nrs: &[SeqNr]) -> Result<(), EventStoreError> {
    Ok(())
  }
  /// 保持のトランザクションの Commit を送る直前に呼ぶ（削除・印付けの DML の後）。`Err(SpannerCommitFault::Aborted)` は保持の
  /// Commit が ABORTED になったことを表し、保持は候補を選び直して再試行する（SPN-11）。再試行の回数が上限を超えると、
  /// 追記の成功を変えずに保持の失敗になる（SPN-13）。`Err(SpannerCommitFault::Failed(e))` は ABORTED 以外の失敗で、再試行しない。
  fn before_retention_commit(
    &self,
    _aid: &crate::aggregate_id::AidString,
    _seq_nr: SeqNr,
  ) -> Result<(), SpannerCommitFault> {
    Ok(())
  }
  /// 行キーのハッシュを差し替える。`Some` を返すと、FNV-1a 64 の代わりにその値を使う（ハッシュの衝突の試験）。
  fn aggregate_hash(&self, _aid: &crate::aggregate_id::AidString) -> Option<u64> {
    None
  }
}

/// Spanner への接続。利用者が作った `Channel` と、任意のトークンの取得元を持つ。
///
/// データ API と管理 API は、同じ `Channel` で呼ぶ。クローンは同じ `Channel` を共有する。
/// 認証の情報（トークン）は持たず、トークンの取得元を呼ぶだけである。
#[derive(Clone)]
pub struct SpannerConnection {
  channel: Channel,
  token_source: Option<Arc<dyn SpannerTokenSource>>,
}

impl std::fmt::Debug for SpannerConnection {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    formatter
      .debug_struct("SpannerConnection")
      .field("channel", &self.channel)
      .field("token_source", &self.token_source.is_some())
      .finish()
  }
}

impl SpannerConnection {
  /// 利用者が作った `Channel` を持つ接続を作る。トークンの取得元は持たない（エミュレーター向け）。
  pub fn new(channel: Channel) -> Self {
    Self {
      channel,
      token_source: None,
    }
  }

  /// トークンの取得元を渡した接続を返す。本番の Cloud Spanner 向けである。
  ///
  /// この接続を使う生成・初期化・追記・読み取り・保持のすべての要求の直前に、[`SpannerTokenSource`] を呼び、
  /// `authorization: Bearer <トークン>` を付ける。取得元が失敗したとき、またはメタデータの値にできないトークンを返したときは、
  /// その要求を送らず、その操作が [`EventStoreError::Storage`] になる。取得元の呼び出しはやり直さない。
  pub fn with_token_source(mut self, token_source: Arc<dyn SpannerTokenSource>) -> Self {
    self.token_source = Some(token_source);
    self
  }

  pub(crate) fn rpc(
    &self,
    tables: &SpannerTables,
    #[cfg(feature = "test-hooks")] hooks: Option<Arc<dyn SpannerTestHooks>>,
  ) -> client::Rpc {
    client::Rpc {
      channel: self.channel.clone(),
      authorizer: Authorizer::new(self.token_source.clone()),
      database: format!(
        "projects/{}/instances/{}/databases/{}",
        tables.project_id, tables.instance_id, tables.database_id
      ),
      #[cfg(feature = "test-hooks")]
      hooks,
    }
  }
}

/// 5 テーブルを置くデータベースと、テーブル名の接頭辞。
///
/// 5 テーブルは、同じ接頭辞から `<接頭辞>_journal`・`<接頭辞>_head`・`<接頭辞>_snapshot_current`・
/// `<接頭辞>_snapshot_history`・`<接頭辞>_config` と導く。個別のテーブル名は受け付けない（SPN-D1b）。
/// 導いた名前が SQL の識別子でないか、128 文字を超えるときは、設定エラーになる。
#[derive(Debug, Clone)]
pub struct SpannerTables {
  pub project_id: String,
  pub instance_id: String,
  pub database_id: String,
  pub table_prefix: String,
}

/// スナップショット履歴の保持の設定。ストアの全体で 1 つに合わせる（SPN-D6）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SpannerRetention {
  /// 履歴を持たない。現在のスナップショットだけを持つ（既定）。
  #[default]
  CurrentOnly,
  /// 新しい `count` 件の履歴を残し、古い履歴を即時に削除する。`count` は 1 以上で、INT64 に収まる値でなければならない。
  KeepLatest { count: usize },
  /// 新しい `count` 件の履歴を残し、古い履歴に期限切れの印を付ける。印を付けた行は、猶予（`grace_days` 日）の後に
  /// Spanner の行削除ポリシーが削除する。`grace_days` は 1 以上で、INT64 に収まる値でなければならない。
  /// snapshot_history に `ROW DELETION POLICY (OLDER_THAN(expired_at, INTERVAL <grace_days> DAY))` が必要である。
  KeepLatestWithTtl { count: usize, grace_days: u64 },
}

/// Spanner の保存先の設定。
#[derive(Debug, Clone)]
pub struct SpannerOptions {
  /// スナップショット履歴の保持の設定。
  pub retention: SpannerRetention,
  /// 変更フィードを求めるか。最初の版は提供しないので、`true` は設定エラーになる。
  pub change_feed: bool,
  /// 中断（ABORTED）されたトランザクションを再試行する回数の上限（初回を数えない）。既定は 5。
  /// 超えると、保存先エラーになる。
  pub transaction_retry_limit: u32,
}

impl Default for SpannerOptions {
  fn default() -> Self {
    Self {
      retention: SpannerRetention::CurrentOnly,
      change_feed: false,
      transaction_retry_limit: 5,
    }
  }
}

/// ストアを使う前の初期化。config の行を 1 行書く（SPN-D5）。
///
/// 5 テーブルと、期限切れ方式の行削除ポリシーは、利用者が先に DDL で用意しておく。この関数は DDL を実行しない。
/// データベースの方言・列・主キー・行削除ポリシーを `options` と照合し、5 テーブルが空であることを確かめてから、
/// 小文字の UUID v4 の `store_id` を持つ config を書く。照合の食い違いは設定エラー、既に初期化済みか空でないときは
/// [`ConfigurationReason::SpannerAlreadyInitialized`](crate::ConfigurationReason::SpannerAlreadyInitialized) になる。
pub async fn initialize_spanner_store(
  connection: &SpannerConnection,
  tables: &SpannerTables,
  options: &SpannerOptions,
) -> Result<(), EventStoreError> {
  configuration::initialize(
    &connection.rpc(
      tables,
      #[cfg(feature = "test-hooks")]
      None,
    ),
    tables,
    options,
  )
  .await
}

/// Spanner の保存先に委譲するイベントストア。
pub struct EventStoreForSpanner<AID, A, P> {
  inner: GenericEventStore<AID, A, P, SpannerBackend<A, P>>,
  #[cfg(feature = "test-hooks")]
  shared: Arc<SpannerShared>,
}

impl<AID, A, P> Clone for EventStoreForSpanner<AID, A, P> {
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
      #[cfg(feature = "test-hooks")]
      shared: self.shared.clone(),
    }
  }
}

impl<AID, A, P> std::fmt::Debug for EventStoreForSpanner<AID, A, P> {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    formatter
      .debug_struct("EventStoreForSpanner")
      .field("inner", &self.inner)
      .finish()
  }
}

impl<AID, A, P> EventStoreForSpanner<AID, A, P> {
  /// 初期化済みのストアを、与えた直列化で開く。方言・列・主キー・行削除ポリシー・config の行をクライアントの設定と照合する。
  pub async fn open_with_serializers(
    connection: SpannerConnection,
    tables: SpannerTables,
    options: SpannerOptions,
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
    connection: SpannerConnection,
    tables: SpannerTables,
    options: SpannerOptions,
    event_serializer: Arc<dyn EventSerializer<P>>,
    snapshot_serializer: Arc<dyn SnapshotSerializer<A>>,
    hooks: Arc<dyn SpannerTestHooks>,
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
    connection: &SpannerConnection,
    tables: &SpannerTables,
    options: &SpannerOptions,
    event_serializer: Arc<dyn EventSerializer<P>>,
    snapshot_serializer: Arc<dyn SnapshotSerializer<A>>,
    #[cfg(feature = "test-hooks")] hooks: Option<Arc<dyn SpannerTestHooks>>,
  ) -> Result<Self, EventStoreError> {
    let rpc = connection.rpc(
      tables,
      #[cfg(feature = "test-hooks")]
      hooks,
    );
    let opened = configuration::open_checked(&rpc, tables, options).await?;
    let shared = Arc::new(SpannerShared {
      rpc,
      names: opened.names,
      options: opened.options,
      store_id: opened.store_id,
    });
    Ok(Self {
      inner: GenericEventStore::new(SpannerBackend {
        shared: shared.clone(),
        events: event_serializer,
        snapshots: snapshot_serializer,
      }),
      #[cfg(feature = "test-hooks")]
      shared,
    })
  }

  /// 試験用に、aid の履歴（印のない行）の seq_nr を昇順で返す。
  #[cfg(feature = "test-hooks")]
  #[doc(hidden)]
  pub async fn history_view(&self, aid: &crate::aggregate_id::AidString) -> Result<Vec<SeqNr>, EventStoreError> {
    self.shared.history(aid).await
  }
}

impl<AID, A, P> EventStoreForSpanner<AID, A, P>
where
  A: Serialize + DeserializeOwned + Send + Sync + 'static,
  P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
  /// 初期化済みのストアを、既定の JSON 直列化で開く。
  pub async fn open(
    connection: SpannerConnection,
    tables: SpannerTables,
    options: SpannerOptions,
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
  for EventStoreForSpanner<AID, A, P>
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
