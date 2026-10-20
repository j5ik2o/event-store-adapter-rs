use std::path::Path;

use rusqlite::{params, Connection, TransactionBehavior};
use uuid::Uuid;

use super::connection::{configuration, open_connection, storage, OpenMode};
use super::layout::{create_tables, is_uninitialized, verify_layout};
use super::SqliteOptions;
use crate::error::{ConfigurationReason, EventStoreError, StorageOperation};
use crate::retention::RetentionMode;

/// 設定値を検査し、config に記録する保持件数を返す（S-1・SQ-11・SQ-20）。
///
/// 保持件数 0、期限切れ方式、変更フィード、符号付き 64bit に収まらない保持件数は設定エラーにする。
/// ファイルを開く前に行うので、失敗してもファイルには何も書かない。
fn validate_options(options: &SqliteOptions) -> Result<Option<i64>, EventStoreError> {
  options.retention.validate()?;
  if matches!(options.retention.mode(), RetentionMode::Ttl { .. }) {
    return Err(configuration(ConfigurationReason::SqliteTtlUnsupported));
  }
  if options.change_feed {
    return Err(configuration(ConfigurationReason::SqliteChangeFeedUnsupported));
  }
  options
    .retention
    .keep_snapshot_count()
    .map(|count| {
      i64::try_from(count).map_err(|_| configuration(ConfigurationReason::SqliteKeepSnapshotCountOutOfRange))
    })
    .transpose()
}

/// 未初期化の空のデータベースを、ストアとして初期化する（SQ-2・SQ-D6）。
///
/// 設定値の検査に通ったあと、接続を開いて WAL（先行書き込みログ方式）と FULL の同期などの接続の設定を適用し、
/// その後で未初期化かを判定する。未初期化なら、5 つの STRICT のテーブルと config の行を、1 つのトランザクションで作る。
/// 新規のファイルか、アプリケーション用のオブジェクトが 1 つもないデータベースだけを対象にする。
/// それ以外（初期化済みを含む）はテーブルも config も作らず、設定エラーにする。ただし判定の前に適用した
/// WAL への切り替えは、拒否したファイルにも残る。通常の「開く」操作は初期化しない。
pub fn initialize_sqlite_store(path: impl AsRef<Path>, options: &SqliteOptions) -> Result<(), EventStoreError> {
  let keep = validate_options(options)?;
  let operation = StorageOperation::CreateConfiguration;
  let mut connection = open_connection(path.as_ref(), options.lock_wait, OpenMode::Create, operation)?;
  let transaction = connection
    .transaction_with_behavior(TransactionBehavior::Immediate)
    .map_err(|error| storage(operation, error))?;
  if !is_uninitialized(&transaction).map_err(|error| storage(operation, error))? {
    return Err(configuration(ConfigurationReason::SqliteDatabaseNotEmpty));
  }
  create_tables(&transaction).map_err(|error| storage(operation, error))?;
  transaction
    .execute(
      "INSERT INTO config (singleton, store_id, layout_version, keep_snapshot_count, retention_mode, change_feed_enabled)
       VALUES (1, ?1, 1, ?2, 'delete', 0)",
      params![Uuid::new_v4().to_string(), keep],
    )
    .map_err(|error| storage(operation, error))?;
  transaction.commit().map_err(|error| storage(operation, error))
}

/// 配置と config を、クライアントの設定と照合する。一致すれば store_id を返す（SQ-2・SQ-19・SQ-20）。
pub(crate) fn verify_configuration(connection: &Connection, keep: Option<i64>) -> Result<String, EventStoreError> {
  let operation = StorageOperation::ReadConfiguration;
  verify_layout(connection)?;
  let mismatch = || configuration(ConfigurationReason::SqliteConfigurationMismatch);
  let mut statement = connection
    .prepare("SELECT store_id, layout_version, keep_snapshot_count, retention_mode, change_feed_enabled FROM config")
    .map_err(|error| storage(operation, error))?;
  let rows: Vec<(String, i64, Option<i64>, String, i64)> = statement
    .query_map([], |row| {
      Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?))
    })
    .and_then(|rows| rows.collect())
    .map_err(|error| storage(operation, error))?;
  let [(store_id, layout_version, recorded_keep, retention_mode, change_feed_enabled)] = rows.as_slice() else {
    return Err(mismatch());
  };
  if *layout_version != 1 {
    return Err(configuration(ConfigurationReason::UnsupportedSqliteLayoutVersion));
  }
  // store_id は小文字の UUID 文字列。大文字やハイフンのない形は食い違いとして扱う。
  match Uuid::parse_str(store_id) {
    Ok(parsed) if parsed.to_string() == *store_id => {}
    _ => return Err(mismatch()),
  }
  match change_feed_enabled {
    0 => {}
    1 => return Err(configuration(ConfigurationReason::SqliteChangeFeedUnsupported)),
    _ => return Err(mismatch()),
  }
  if retention_mode != "delete" || *recorded_keep != keep {
    return Err(mismatch());
  }
  Ok(store_id.clone())
}

/// 配置と config を照合した、開いた接続を表す。
pub(super) struct OpenedStore {
  pub connection: Connection,
  pub store_id: String,
  pub keep_snapshot_count: Option<usize>,
}

/// 初期化済みのストアを開き、配置と config を 1 つの読み取りトランザクションで照合する。
///
/// 存在しないファイルは作らず、欠けた定義も補わない。
pub(super) fn open_checked(path: &Path, options: &SqliteOptions) -> Result<OpenedStore, EventStoreError> {
  let keep = validate_options(options)?;
  let operation = StorageOperation::ReadConfiguration;
  let mut connection = open_connection(path, options.lock_wait, OpenMode::Existing, operation)?;
  let transaction = connection.transaction().map_err(|error| storage(operation, error))?;
  let store_id = verify_configuration(&transaction, keep)?;
  transaction.commit().map_err(|error| storage(operation, error))?;
  Ok(OpenedStore {
    connection,
    store_id,
    keep_snapshot_count: options.retention.keep_snapshot_count(),
  })
}

/// 試験用に config の行へ書き込む値を表す。項目名はプロファイルの config のとおり。
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqliteConfigurationRecord {
  pub store_id: String,
  pub layout_version: i64,
  pub keep_snapshot_count: Option<i64>,
  pub retention_mode: String,
  pub change_feed_enabled: i64,
}

/// 初期化の後、ストアを開く前に、記録済みの config の行を `record` で書き換える（試験用）。
///
/// 適合テストデータの `seed.configuration` を、公開 API を増やさずに記録するための経路である。
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub fn overwrite_sqlite_configuration_for_test(
  path: impl AsRef<Path>,
  options: &SqliteOptions,
  record: &SqliteConfigurationRecord,
) -> Result<(), EventStoreError> {
  let operation = StorageOperation::CreateConfiguration;
  let mut connection = open_connection(path.as_ref(), options.lock_wait, OpenMode::Existing, operation)?;
  let transaction = connection
    .transaction_with_behavior(TransactionBehavior::Immediate)
    .map_err(|error| storage(operation, error))?;
  let changed = transaction
    .execute(
      "UPDATE config
       SET store_id = ?1, layout_version = ?2, keep_snapshot_count = ?3, retention_mode = ?4, change_feed_enabled = ?5
       WHERE singleton = 1",
      params![
        record.store_id,
        record.layout_version,
        record.keep_snapshot_count,
        record.retention_mode,
        record.change_feed_enabled
      ],
    )
    .map_err(|error| storage(operation, error))?;
  if changed != 1 {
    return Err(super::connection::invalid_data(operation, "the config row is missing"));
  }
  transaction.commit().map_err(|error| storage(operation, error))
}
