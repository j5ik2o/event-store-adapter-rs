use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rusqlite::Connection;

use crate::error::StorageOperation;
use crate::sqlite::connection::{meets_minimum_version, open_connection, OpenMode};

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// 破棄時に中身ごと消える一時ディレクトリを表す。
struct TempDir(PathBuf);

impl TempDir {
  fn new() -> Self {
    let path = std::env::temp_dir().join(format!(
      "event-store-adapter-sqlite-connection-{}-{}",
      std::process::id(),
      SEQUENCE.fetch_add(1, Ordering::SeqCst)
    ));
    if path.exists() {
      fs::remove_dir_all(&path).expect("前回の一時ディレクトリを消せる");
    }
    fs::create_dir_all(&path).expect("一時ディレクトリを作れる");
    Self(path)
  }

  fn path(&self) -> &Path {
    &self.0
  }
}

impl Drop for TempDir {
  fn drop(&mut self) {
    // 後片付けの失敗は試験の結果を変えないので、握りつぶす。
    let _ = fs::remove_dir_all(&self.0);
  }
}

fn pragma_text(connection: &Connection, name: &str) -> String {
  connection
    .query_row(&format!("PRAGMA {name}"), [], |row| row.get(0))
    .unwrap_or_else(|error| panic!("PRAGMA {name} を読める: {error}"))
}

fn pragma_integer(connection: &Connection, name: &str) -> i64 {
  connection
    .query_row(&format!("PRAGMA {name}"), [], |row| row.get(0))
    .unwrap_or_else(|error| panic!("PRAGMA {name} を読める: {error}"))
}

#[test]
fn should_minimum_version_compare_each_part_as_an_integer_and_accept_the_exact_lower_bound() {
  assert!(meets_minimum_version("3.51.3"));
}

#[test]
fn should_minimum_version_compare_each_part_as_an_integer_and_accept_versions_that_look_smaller_as_text() {
  for version in ["3.51.10", "3.53.2"] {
    assert!(meets_minimum_version(version), "{version}");
  }
}

#[test]
fn should_minimum_version_compare_each_part_as_an_integer_and_reject_versions_that_look_larger_as_text() {
  for version in ["3.9.9", "3.51.2"] {
    assert!(!meets_minimum_version(version), "{version}");
  }
}

#[test]
fn should_minimum_version_compare_each_part_as_an_integer_and_reject_versions_without_three_parts() {
  for version in ["3.51", "3.51.3.1", "3.51.x", ""] {
    assert!(!meets_minimum_version(version), "{version:?}");
  }
}

#[test]
fn should_bundled_sqlite_meet_the_minimum_version() {
  assert!(
    meets_minimum_version(rusqlite::version()),
    "同梱の SQLite が下限より古い: {}",
    rusqlite::version()
  );
}

#[test]
fn should_open_connection_apply_wal_full_normal_read_uncommitted_off_and_lock_wait() {
  let directory = TempDir::new();
  let path = directory.path().join("store.sqlite3");
  let lock_wait = Duration::from_millis(1234);

  // 1 回目は作成の権限で新しいファイルを開き、2 回目は作られたファイルを開く。どちらの接続も同じ設定になる。
  for (label, mode) in [("create", OpenMode::Create), ("existing", OpenMode::Existing)] {
    let connection = open_connection(&path, lock_wait, mode, StorageOperation::ReadConfiguration)
      .unwrap_or_else(|error| panic!("{label}: 接続を開ける: {error}"));

    assert_eq!(pragma_text(&connection, "journal_mode"), "wal", "{label}");
    assert_eq!(pragma_integer(&connection, "synchronous"), 2, "{label}");
    assert_eq!(pragma_text(&connection, "locking_mode"), "normal", "{label}");
    assert_eq!(pragma_integer(&connection, "read_uncommitted"), 0, "{label}");
    assert_eq!(pragma_integer(&connection, "busy_timeout"), 1234, "{label}");
  }
}
