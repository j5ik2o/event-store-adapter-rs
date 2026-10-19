use rusqlite::{Connection, OptionalExtension};

use super::connection::{configuration, storage};
use crate::error::{ConfigurationReason, EventStoreError, StorageOperation};

/// seq_nr の列。範囲は 1 以上 2^53-1 以下（3 章）。
const SEQ_NR: &str = "seq_nr INTEGER NOT NULL CHECK (seq_nr BETWEEN 1 AND 9007199254740991)";

/// 5 つのテーブルを STRICT として作る（3 章・SQ-16）。`head_transition` は作らない（SQ-20）。
pub(super) fn create_tables(connection: &Connection) -> Result<(), rusqlite::Error> {
  connection.execute_batch(&format!(
    "CREATE TABLE journal (
       aid TEXT COLLATE BINARY NOT NULL,
       {SEQ_NR},
       occurred_at_ns INTEGER NOT NULL,
       manifest TEXT NOT NULL,
       payload BLOB NOT NULL,
       PRIMARY KEY (aid, seq_nr)
     ) STRICT;
     CREATE TABLE head (
       aid TEXT COLLATE BINARY NOT NULL PRIMARY KEY,
       type_name TEXT NOT NULL,
       {SEQ_NR},
       event_occurred_at_ns INTEGER NOT NULL,
       event_manifest TEXT NOT NULL,
       event_payload BLOB NOT NULL
     ) STRICT;
     CREATE TABLE snapshot_current (
       aid TEXT COLLATE BINARY NOT NULL PRIMARY KEY,
       {SEQ_NR},
       manifest TEXT NOT NULL,
       payload BLOB NOT NULL
     ) STRICT;
     CREATE TABLE snapshot_history (
       aid TEXT COLLATE BINARY NOT NULL,
       {SEQ_NR},
       manifest TEXT NOT NULL,
       payload BLOB NOT NULL,
       PRIMARY KEY (aid, seq_nr)
     ) STRICT;
     CREATE TABLE config (
       singleton INTEGER NOT NULL PRIMARY KEY CHECK (singleton = 1),
       store_id TEXT NOT NULL,
       layout_version INTEGER NOT NULL,
       keep_snapshot_count INTEGER CHECK (keep_snapshot_count IS NULL OR keep_snapshot_count >= 1),
       retention_mode TEXT NOT NULL CHECK (retention_mode = 'delete'),
       change_feed_enabled INTEGER NOT NULL CHECK (change_feed_enabled IN (0, 1))
     ) STRICT;"
  ))
}

/// アプリケーション用のテーブル・索引・ビュー・トリガーが 1 つもない、未初期化のデータベースなら真を返す（SQ-D6）。
pub(super) fn is_uninitialized(connection: &Connection) -> Result<bool, rusqlite::Error> {
  let objects: i64 = connection.query_row(
    "SELECT count(*) FROM sqlite_schema WHERE lower(name) NOT GLOB 'sqlite_*'",
    [],
    |row| row.get(0),
  )?;
  Ok(objects == 0)
}

/// 列の期待: 名前・型・NOT NULL（0 か 1）・主キーの中の位置（主キーでなければ 0）。
type Column = (&'static str, &'static str, i64, i64);

const EXPECTED_TABLES: [(&str, &[Column]); 5] = [
  (
    "journal",
    &[
      ("aid", "TEXT", 1, 1),
      ("seq_nr", "INTEGER", 1, 2),
      ("occurred_at_ns", "INTEGER", 1, 0),
      ("manifest", "TEXT", 1, 0),
      ("payload", "BLOB", 1, 0),
    ],
  ),
  (
    "head",
    &[
      ("aid", "TEXT", 1, 1),
      ("type_name", "TEXT", 1, 0),
      ("seq_nr", "INTEGER", 1, 0),
      ("event_occurred_at_ns", "INTEGER", 1, 0),
      ("event_manifest", "TEXT", 1, 0),
      ("event_payload", "BLOB", 1, 0),
    ],
  ),
  (
    "snapshot_current",
    &[
      ("aid", "TEXT", 1, 1),
      ("seq_nr", "INTEGER", 1, 0),
      ("manifest", "TEXT", 1, 0),
      ("payload", "BLOB", 1, 0),
    ],
  ),
  (
    "snapshot_history",
    &[
      ("aid", "TEXT", 1, 1),
      ("seq_nr", "INTEGER", 1, 2),
      ("manifest", "TEXT", 1, 0),
      ("payload", "BLOB", 1, 0),
    ],
  ),
  (
    "config",
    &[
      ("singleton", "INTEGER", 1, 1),
      ("store_id", "TEXT", 1, 0),
      ("layout_version", "INTEGER", 1, 0),
      ("keep_snapshot_count", "INTEGER", 0, 0),
      ("retention_mode", "TEXT", 1, 0),
      ("change_feed_enabled", "INTEGER", 1, 0),
    ],
  ),
];

/// `aid` に要求する照合順序（3 章）。宣言に COLLATE がなければ既定の BINARY になるので、書いていない列も受け入れる。
const AID_COLLATION: &str = "BINARY";

fn is_aid_collation(name: &[u8]) -> bool {
  name.eq_ignore_ascii_case(AID_COLLATION.as_bytes())
}

/// `table` の `aid` の照合順序が、列の宣言でも主キーの索引でも BINARY かを返す（3 章・SQ-2）。
///
/// `pragma_table_info` は照合順序を返さない。列の宣言は `sqlite3_table_column_metadata` で、主キーの索引は
/// `pragma_index_list`（origin が `pk`）と `pragma_index_xinfo`（キーの列 `aid` の `coll`）で読む。
/// 一意性と `aid = ?` の比較は、この 2 つの照合順序で決まる。
fn aid_collations_are_binary(connection: &Connection, table: &str) -> Result<bool, rusqlite::Error> {
  let (_, declared, _, _, _) = connection.column_metadata(Some("main"), table, "aid")?;
  if !declared.is_some_and(|name| is_aid_collation(name.to_bytes())) {
    return Ok(false);
  }
  let mut statement = connection.prepare(
    "SELECT x.coll FROM pragma_index_list(?1, 'main') AS l, pragma_index_xinfo(l.name, 'main') AS x \
     WHERE l.origin = 'pk' AND x.key = 1 AND x.name = 'aid'",
  )?;
  let primary_key: Vec<String> = statement
    .query_map([table], |row| row.get(0))
    .and_then(|rows| rows.collect())?;
  Ok(primary_key.len() == 1 && primary_key.iter().all(|name| is_aid_collation(name.as_bytes())))
}

/// 5 つのテーブルが STRICT で、列の名前・型・NOT NULL・主キーの位置が期待どおりか、`aid` を持つ表では
/// その照合順序が BINARY かを照合する（SQ-2・SQ-16）。
///
/// DDL の文字列ではなく、`pragma_table_list` と `pragma_table_info` などの意味で比べる。
/// 欠損や不一致は設定エラーにする。欠けた定義を補わない。
pub(super) fn verify_layout(connection: &Connection) -> Result<(), EventStoreError> {
  let operation = StorageOperation::ReadConfiguration;
  let mismatch = || configuration(ConfigurationReason::SqliteLayoutMismatch);
  for (table, expected_columns) in EXPECTED_TABLES {
    let strict: Option<i64> = connection
      .query_row(
        "SELECT \"strict\" FROM pragma_table_list WHERE schema = 'main' AND name = ?1",
        [table],
        |row| row.get(0),
      )
      .optional()
      .map_err(|error| storage(operation, error))?;
    if strict != Some(1) {
      return Err(mismatch());
    }
    let mut statement = connection
      .prepare("SELECT name, type, \"notnull\", pk FROM pragma_table_info(?1) ORDER BY cid")
      .map_err(|error| storage(operation, error))?;
    let actual: Vec<(String, String, i64, i64)> = statement
      .query_map([table], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)))
      .and_then(|rows| rows.collect())
      .map_err(|error| storage(operation, error))?;
    let matches_expected = actual.len() == expected_columns.len()
      && actual.iter().zip(expected_columns).all(|(actual, expected)| {
        actual.0 == expected.0 && actual.1 == expected.1 && actual.2 == expected.2 && actual.3 == expected.3
      });
    if !matches_expected {
      return Err(mismatch());
    }
    if expected_columns.iter().any(|column| column.0 == "aid") {
      let binary = aid_collations_are_binary(connection, table).map_err(|error| storage(operation, error))?;
      if !binary {
        return Err(mismatch());
      }
    }
  }
  Ok(())
}
