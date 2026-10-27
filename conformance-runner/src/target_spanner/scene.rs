//! Spanner の場面が使う、新しいデータベース（5 テーブル）と、ストアを開く・初期化する操作。
//!
//! 各場面は新しいデータベースで実行する（エミュレーターは、データベースごとに読み書きトランザクションを直列にするので、
//! 場面の書き込みが重ならないようにする）。5 テーブルは接頭辞 [`PREFIX`] から導いた名前で作る。
//! `createStore` の `storage = shared` は、同じデータベースを別のストアとして開く。接続は、呼び出した実行基盤の中で作る。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use event_store_adapter_rs::{
  error::EventStoreError,
  initialize_spanner_store,
  serializer::{EventSerializer, SnapshotSerializer},
  spanner::SpannerTestHooks,
  EventStoreForSpanner, SpannerConnection, SpannerOptions, SpannerTables,
};
use event_store_adapter_test_utils_rs::spanner::{self, RawSql, SeedConfiguration};
use serde_json::Value;

use crate::case::CaseId;

/// 場面が使うストアの型。
pub(super) type Store = EventStoreForSpanner<CaseId, Value, Value>;

/// 5 テーブルの名前を導く接頭辞。
const PREFIX: &str = "esa";

/// データベースの割り当て番号。プロセスの中で重ならない連番にする。
static NEXT_ALLOCATION: AtomicU64 = AtomicU64::new(1);

/// 場面のデータベースと、それを開くための接続を表す。
pub(super) struct Scene {
  port: u16,
  connection: SpannerConnection,
  tables: SpannerTables,
  raw: RawSql,
}

impl Scene {
  /// エミュレーターへ接続し、`policy_days` に合う行削除ポリシーを持つ 5 テーブルの新しいデータベースを作る。
  /// 設定の行は作らない（初期化の前の状態）。誤りは、その説明を返す。
  ///
  /// 呼び出した実行基盤の中で接続を作るので、その実行基盤が生きている間だけ使える。
  pub(super) async fn provision(port: u16, policy_days: Option<i64>) -> Result<Self, String> {
    let channel = spanner::connect(port)
      .await
      .map_err(|error| format!("エミュレーターへ接続できない: {error:#}"))?;
    let database_id = format!(
      "c{}x{}",
      std::process::id(),
      NEXT_ALLOCATION.fetch_add(1, Ordering::Relaxed)
    );
    spanner::create_database(&channel, &database_id, spanner::store_ddl(PREFIX, policy_days))
      .await
      .map_err(|error| format!("場面のデータベースを作れない: {error:#}"))?;
    Ok(Self {
      tables: SpannerTables {
        project_id: spanner::PROJECT.to_owned(),
        instance_id: spanner::INSTANCE.to_owned(),
        database_id: database_id.clone(),
        table_prefix: PREFIX.to_owned(),
      },
      raw: RawSql::new(channel.clone(), &database_id),
      connection: SpannerConnection::new(channel),
      port,
    })
  }

  /// 場面のデータベースを削除する。エミュレーターはインスタンスごとのデータベース数に上限を持つので、実行し終えた場面が
  /// 残さないようにする。削除の失敗は場面の結果に影響しないので無視する（上限に達すれば、次の場面の作成が失敗として現れる）。
  pub(super) async fn discard(self) {
    if let Ok(channel) = spanner::connect(self.port).await {
      let _ = spanner::drop_database(&channel, &self.tables.database_id).await;
    }
  }

  /// 設定の行を、ライブラリを通さず生の DML で書く（初期化の手順を経ない状態を作る）。誤りは、その説明を返す。
  pub(super) async fn seed_configuration(&self, configuration: &SeedConfiguration) -> Result<(), String> {
    spanner::insert_configuration(&self.raw, PREFIX, configuration)
      .await
      .map_err(|error| format!("seed.configuration を書けない: {error:#}"))
  }

  /// 公開の初期化関数で、設定の行を書く。
  pub(super) async fn initialize(&self, options: &SpannerOptions) -> Result<(), EventStoreError> {
    initialize_spanner_store(&self.connection, &self.tables, options).await
  }

  /// 初期化済みのデータベースを、障害のフック付きのストアとして開く。
  pub(super) async fn open<H>(&self, options: &SpannerOptions, callbacks: &Arc<H>) -> Result<Store, EventStoreError>
  where
    H: SpannerTestHooks + EventSerializer<Value> + SnapshotSerializer<Value> + 'static, {
    Store::open_with_hooks(
      self.connection.clone(),
      self.tables.clone(),
      options.clone(),
      callbacks.clone(),
      callbacks.clone(),
      callbacks.clone(),
    )
    .await
  }
}
