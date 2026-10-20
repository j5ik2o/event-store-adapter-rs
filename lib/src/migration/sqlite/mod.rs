//! v3 の既定の配置の SQLite のファイルを、v5 の SQLite のファイルへ移す（機能 `sqlite-migration`）。
//!
//! 新ファイルは、利用者が `initialize_sqlite_store` で用意した、初期化済みで空のファイルである。
//! 旧ファイルは読み取り専用で開き、変えない。

mod inspect;
mod legacy;
mod write;

use std::collections::HashMap;
use std::path::Path;

use rusqlite::{OptionalExtension, TransactionBehavior};

use super::{MigrationError, MigrationRejection, MigrationReport};
use crate::error::StorageOperation;
use crate::sqlite::configuration::verify_configuration;
use crate::sqlite::connection::{open_connection, OpenMode};
use crate::sqlite::layout::verify_layout;
use crate::sqlite::SqliteOptions;

/// 新ファイルで、行があってはならない表。
const NEW_TABLES: [&str; 4] = ["journal", "head", "snapshot_current", "snapshot_history"];

/// 全件検査の後、v3 の既定の配置の SQLite のファイル `legacy_path` を、新ファイル `path` へ転写する。
///
/// - 呼び出し前に、旧ファイルへの書き込みを止め、新ファイルを `initialize_sqlite_store` で初期化する。
///   この関数は、新ファイルのテーブルや config を作ったり補ったりしない。
/// - 旧ファイルは読み取り専用で開く。新ファイルの journal・head・スナップショットに行があれば、何も書かずに拒否する。
/// - 型名と値は、旧 `DefaultKeyResolver` の配置（`pkey={型名}-{シャード番号}`、`skey={型名}-{値}-{seq_nr}`）から復元する。
///   型名に `-` を含む場合だけ `type_mapping` で置き換える。既定でない配置は拒否する。
/// - 旧の保持の履歴は、新ファイルの保持件数が `n` なら集約ごとに seq_nr の新しい `n` 件を移し、
///   保持件数なしなら移さない。今のスナップショットは常に移す。旧 `version` と `last_updated_at` は移さない。
/// - 検査拒否は `reasons` で返し、新ファイルには何も書かない。書き込みは 1 つのトランザクションで行うので、
///   途中で失敗しても部分的な移行は残らない。保存先の失敗は `MigrationError` で返す。
pub fn migrate_v3_sqlite(
  legacy_path: impl AsRef<Path>,
  path: impl AsRef<Path>,
  type_mapping: &HashMap<String, String>,
) -> Result<MigrationReport, MigrationError> {
  let mut report = MigrationReport::default();
  let normalize = "パスの正規化";
  let legacy_path =
    std::fs::canonicalize(legacy_path.as_ref()).map_err(|error| MigrationError::new(&report, normalize, error))?;
  let path = std::fs::canonicalize(path.as_ref()).map_err(|error| MigrationError::new(&report, normalize, error))?;
  if legacy_path == path {
    report.reasons.push(MigrationRejection {
      table: String::new(),
      pkey: None,
      skey: None,
      aid: None,
      reason: "旧ファイルと新ファイルには互いに異なるファイルが必要です".into(),
    });
    return Ok(report);
  }

  // 新ファイルは作らず、補わない。書き込みのロックは、移行が終わるまで保持する。
  let open = "新ファイルを開く";
  let mut new = open_connection(
    &path,
    SqliteOptions::default().lock_wait,
    OpenMode::Existing,
    StorageOperation::ReadConfiguration,
  )
  .map_err(|error| MigrationError::new(&report, open, error))?;
  let transaction = new
    .transaction_with_behavior(TransactionBehavior::Immediate)
    .map_err(|error| MigrationError::new(&report, open, error))?;
  let verify = "新ファイルの照合";
  verify_layout(&transaction).map_err(|error| MigrationError::new(&report, verify, error))?;
  let keep: Option<i64> = transaction
    .query_row("SELECT keep_snapshot_count FROM config", [], |row| row.get(0))
    .optional()
    .map_err(|error| MigrationError::new(&report, verify, error))?
    .flatten();
  // 保持件数は config の記録をそのまま使う。移行は、保持の設定を変えない。
  verify_configuration(&transaction, keep).map_err(|error| MigrationError::new(&report, verify, error))?;
  for table in NEW_TABLES {
    let occupied: bool = transaction
      .query_row(&format!("SELECT EXISTS(SELECT 1 FROM {table})"), [], |row| row.get(0))
      .map_err(|error| MigrationError::new(&report, "新ファイルの確認", error))?;
    if occupied {
      report.reasons.push(MigrationRejection {
        table: format!("new.{table}"),
        pkey: None,
        skey: None,
        aid: None,
        reason: "新ファイルに行があります".into(),
      });
    }
  }
  if !report.reasons.is_empty() {
    return Ok(report);
  }

  // 旧ファイルは 1 つの読み取りトランザクションの中で、検査と書き込みのために 2 回読む。
  let mut old = legacy::open(&legacy_path).map_err(|error| MigrationError::new(&report, "旧ファイルを開く", error))?;
  let old_transaction = old
    .transaction()
    .map_err(|error| MigrationError::new(&report, "旧ファイルを開く", error))?;
  // 保持件数は 1 以上（config の CHECK）。`usize` に収まらない大きさなら、どの履歴も取り除かれないので上限にする。
  let keep = keep.map(|count| usize::try_from(count).unwrap_or(usize::MAX));
  let aggregates = inspect::inspect(&old_transaction, type_mapping, keep, &mut report)?;
  if !report.reasons.is_empty() {
    return Ok(report);
  }
  let counts = write::write(&old_transaction, &transaction, type_mapping, &aggregates, &report)?;
  transaction
    .commit()
    .map_err(|error| MigrationError::new(&report, "新ファイルのCOMMIT", error))?;
  // 件数は COMMIT に成功してから報告に入れる。
  report.aggregates = counts.aggregates;
  report.events = counts.events;
  report.snapshots = counts.snapshots;
  Ok(report)
}
