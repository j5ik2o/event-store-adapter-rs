use std::path::Path;
use std::time::Duration;

use rusqlite::{Connection, OpenFlags};

use crate::error::{ConfigurationReason, EventStoreError, StorageOperation};

/// SQLite 本体の版の下限（SQ-15）。
const MINIMUM_VERSION: (u32, u32, u32) = (3, 51, 3);

/// ファイルを開くときに、存在しないファイルを作るかどうかを表す。
///
/// 作成の権限を持つのは、明示的な初期化だけである（SQ-D6）。
pub(crate) enum OpenMode {
  Create,
  Existing,
}

pub(super) fn storage(operation: StorageOperation, error: rusqlite::Error) -> EventStoreError {
  EventStoreError::Storage {
    operation,
    source: Box::new(error),
  }
}

pub(super) fn invalid_data(operation: StorageOperation, message: &'static str) -> EventStoreError {
  EventStoreError::Storage {
    operation,
    source: Box::new(std::io::Error::other(message)),
  }
}

pub(super) fn configuration(reason: ConfigurationReason) -> EventStoreError {
  EventStoreError::Configuration { reason }
}

/// SQLite 本体の版の文字列が、下限（3.51.3）以上かを返す（SQ-15）。
///
/// `.` で 3 つの部分に分け、各部分を整数として比べる。3 つに分けられない、または整数でない文字列は偽を返す。
pub(super) fn meets_minimum_version(version: &str) -> bool {
  let mut parts = version.split('.').map(|part| part.parse::<u32>());
  match (parts.next(), parts.next(), parts.next(), parts.next()) {
    (Some(Ok(major)), Some(Ok(minor)), Some(Ok(patch)), None) => (major, minor, patch) >= MINIMUM_VERSION,
    _ => false,
  }
}

fn query_text(connection: &Connection, sql: &str, operation: StorageOperation) -> Result<String, EventStoreError> {
  connection
    .query_row(sql, [], |row| row.get::<_, String>(0))
    .map_err(|error| storage(operation, error))
}

fn query_integer(connection: &Connection, sql: &str, operation: StorageOperation) -> Result<i64, EventStoreError> {
  connection
    .query_row(sql, [], |row| row.get::<_, i64>(0))
    .map_err(|error| storage(operation, error))
}

/// ストアが使う接続を開き、版・待ち時間・接続の設定を確かめる（SQ-1・SQ-14・SQ-15）。
///
/// 待ち時間が 32bit のミリ秒に収まらなければ、ファイルを開く前に設定エラーにする。
/// 接続の設定は、設定した後で読み直し、要求と異なれば設定エラーにする。
pub(crate) fn open_connection(
  path: &Path,
  lock_wait: Duration,
  mode: OpenMode,
  operation: StorageOperation,
) -> Result<Connection, EventStoreError> {
  if i32::try_from(lock_wait.as_millis()).is_err() {
    return Err(configuration(ConfigurationReason::SqliteLockWaitOutOfRange));
  }
  // URI の権限は付けない。同梱の SQLite は URI の解釈が既定で有効なので、`file:` で始まる名前は
  // `./` を前に付けて、URI ではなく通常のファイル名として開く（`?cache=shared` などを有効にさせない）。
  let file_name;
  let path = if path.to_string_lossy().starts_with("file:") {
    file_name = Path::new(".").join(path);
    file_name.as_path()
  } else {
    path
  };
  let mut flags =
    OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX | OpenFlags::SQLITE_OPEN_PRIVATE_CACHE;
  if matches!(mode, OpenMode::Create) {
    flags |= OpenFlags::SQLITE_OPEN_CREATE;
  }
  let connection = Connection::open_with_flags(path, flags).map_err(|error| storage(operation, error))?;
  if !meets_minimum_version(&query_text(&connection, "SELECT sqlite_version()", operation)?) {
    return Err(configuration(ConfigurationReason::UnsupportedSqliteVersion));
  }
  // 後の PRAGMA の適用も同じ待ち時間で待つので、最初に設定する。
  connection
    .busy_timeout(lock_wait)
    .map_err(|error| storage(operation, error))?;
  let mismatch = || configuration(ConfigurationReason::SqliteConnectionSettingMismatch);
  // WAL を有効にする前に locking_mode を NORMAL にする（EXCLUSIVE は他の接続を締め出す）。
  if !query_text(&connection, "PRAGMA locking_mode = NORMAL", operation)?.eq_ignore_ascii_case("normal") {
    return Err(mismatch());
  }
  if !query_text(&connection, "PRAGMA journal_mode = WAL", operation)?.eq_ignore_ascii_case("wal") {
    return Err(mismatch());
  }
  connection
    .execute_batch("PRAGMA synchronous = FULL; PRAGMA read_uncommitted = OFF;")
    .map_err(|error| storage(operation, error))?;
  // synchronous の FULL は 2。
  if query_integer(&connection, "PRAGMA synchronous", operation)? != 2 {
    return Err(mismatch());
  }
  if query_integer(&connection, "PRAGMA read_uncommitted", operation)? != 0 {
    return Err(mismatch());
  }
  Ok(connection)
}
