//! Bigtable の場面が使う、新しい journal・head のテーブルの組と、ストアを開く操作。
//!
//! 各場面は新しいテーブルの組で実行する（プロファイル 2 章の配置、設定行は作らない）。`createStore` の
//! `storage = shared` は、同じテーブルの組を別のストアとして開く。接続は、呼び出した実行基盤の中で作る。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use event_store_adapter_rs::{
  bigtable::BigtableTestHooks,
  error::EventStoreError,
  serializer::{EventSerializer, SnapshotSerializer},
  BigtableConnection, BigtableOptions, BigtableTables, EventStoreForBigtable,
};
use event_store_adapter_test_utils_rs::bigtable;
use serde_json::Value;

use crate::case::CaseId;

/// 場面が使うストアの型。
pub(super) type Store = EventStoreForBigtable<CaseId, Value, Value>;

/// エミュレーターは、プロジェクトとインスタンスの名前を検査しない。
const PROJECT_ID: &str = "conformance";
const INSTANCE_ID: &str = "conformance";

/// テーブルの割り当て番号。プロセスの中で重ならない連番にする。
static NEXT_ALLOCATION: AtomicU64 = AtomicU64::new(1);

/// 場面のテーブルの組と、それを開くための接続を表す。
pub(super) struct Scene {
  pub(super) connection: BigtableConnection,
  pub(super) tables: BigtableTables,
}

impl Scene {
  /// エミュレーターへ接続し、新しい journal・head のテーブルの組を作る。誤りは、その説明を返す。
  ///
  /// 呼び出した実行基盤の中で接続を作るので、その実行基盤が生きている間だけ使える。
  pub(super) async fn provision(port: u16) -> Result<Self, String> {
    let channel = bigtable::connect(port)
      .await
      .map_err(|error| format!("エミュレーターへ接続できない: {error:#}"))?;
    let sequence = NEXT_ALLOCATION.fetch_add(1, Ordering::Relaxed);
    let tables = BigtableTables {
      project_id: PROJECT_ID.to_owned(),
      instance_id: INSTANCE_ID.to_owned(),
      journal_table_id: format!("j{}_{sequence}", std::process::id()),
      head_table_id: format!("h{}_{sequence}", std::process::id()),
    };
    bigtable::create_tables(
      &channel,
      &tables.project_id,
      &tables.instance_id,
      &tables.journal_table_id,
      &tables.head_table_id,
    )
    .await
    .map_err(|error| format!("場面のテーブルを作れない: {error:#}"))?;
    Ok(Self {
      connection: BigtableConnection {
        data: channel.clone(),
        admin: channel,
      },
      tables,
    })
  }

  /// 初期化済みのテーブルの組を、障害のフック付きのストアとして開く。
  pub(super) async fn open<H>(&self, options: &BigtableOptions, callbacks: &Arc<H>) -> Result<Store, EventStoreError>
  where
    H: BigtableTestHooks + EventSerializer<Value> + SnapshotSerializer<Value> + 'static, {
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
