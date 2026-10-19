//! SQLite の場面が使う一時ファイルと、複数のストア、書き込みのロックを持つ別の接続。
//!
//! 各場面は新しい一時ファイルで実行する。`createStore` の `storage = shared` は、同じファイルを別のストアとして
//! 開く（別の接続になる）。`holdWriteLock`・`releaseWriteLock` は、ストアの接続とは別の接続で、
//! ファイル全体の書き込みのロックを持つ・放す（SQ-7）。

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use event_store_adapter_rs::{error::EventStoreError, EventStoreForSqlite};
use rusqlite::{Connection, OpenFlags};
use serde_json::Value;

use crate::case::CaseId;
use crate::store_registry::{CreateRequest, StorageRequest, StoreRegistry};

/// 場面が使うストアの型。
pub(super) type Store = EventStoreForSqlite<CaseId, Value, Value>;

/// 一時ファイルの割り当て番号。プロセスの中で重ならない連番にする。
static NEXT_ALLOCATION: AtomicU64 = AtomicU64::new(1);

/// 場面ごとの、新しい一時ファイルの場所を表す。破棄すると、その置き場ごと消す。
///
/// 置き場の名前は、プロセス番号と連番で決まる。プロセス番号が再利用されて、前の実行のファイルが同じ名前で
/// 残っていても、割り当て時に置き場ごと消してから作り直す。残ったファイルがテーブルを持っていても、
/// 場面の初期化が設定エラーにならないようにする。
#[derive(Debug)]
pub(super) struct ScenarioFile {
  directory: PathBuf,
  path: PathBuf,
}

impl ScenarioFile {
  /// 新しい置き場を割り当て、その下のデータベースのファイルの場所を返す。ファイルはまだ作らない。
  pub(super) fn new() -> Result<Self, String> {
    let sequence = NEXT_ALLOCATION.fetch_add(1, Ordering::Relaxed);
    let directory = std::env::temp_dir().join(format!(
      "event-store-adapter-conformance-sqlite-{}-{sequence}",
      std::process::id()
    ));
    match fs::remove_dir_all(&directory) {
      Ok(()) => {}
      Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
      Err(error) => {
        return Err(format!(
          "前の実行の一時ファイルの置き場を消せない（{}）: {error}",
          directory.display()
        ))
      }
    }
    fs::create_dir_all(&directory)
      .map_err(|error| format!("一時ファイルの置き場を作れない（{}）: {error}", directory.display()))?;
    let path = directory.join("store.db");
    Ok(Self { directory, path })
  }

  /// データベースのファイルの場所を返す。
  pub(super) fn path(&self) -> &Path {
    &self.path
  }
}

impl Drop for ScenarioFile {
  fn drop(&mut self) {
    // 後始末の失敗は、場面の結果に影響しない。残ったファイルは、次の割り当てが置き場ごと消す。
    let _ = fs::remove_dir_all(&self.directory);
  }
}

/// 場面の中の、参照名から引けるストアの表と、書き込みのロックを持つ別の接続を表す。
///
/// 参照名の規則は [`StoreRegistry`] が持つ。ここは、ファイルの開き直しと書き込みのロックだけを持つ。
pub(super) struct Stores<'a> {
  path: &'a Path,
  open: &'a dyn Fn() -> Result<Store, EventStoreError>,
  pub(super) registry: StoreRegistry<Store>,
  lock: Option<Connection>,
}

impl<'a> Stores<'a> {
  /// 1 つ目のストアを予約名 `primary` として持つ表を作る。
  ///
  /// `open` は、`path` の初期化済みのファイルを、同じ設定でもう 1 つのストアとして開く。
  pub(super) fn new(path: &'a Path, open: &'a dyn Fn() -> Result<Store, EventStoreError>, primary: Store) -> Self {
    Self {
      path,
      open,
      registry: StoreRegistry::new(primary),
      lock: None,
    }
  }

  /// `createStore` を実行する。
  ///
  /// データの誤りは外側の `Err`（検査は [`StoreRegistry::parse_create`]、SQLite は `storage = new` も誤り）、
  /// 2 つ目のストアを開く失敗は内側の `Err` で返す。
  pub(super) fn create(&mut self, arguments: &Value) -> Result<Result<(), EventStoreError>, String> {
    let CreateRequest { name, storage } = self.registry.parse_create(arguments)?;
    match storage {
      StorageRequest::New => {
        Err("storage = new は SQLite の場面では使えない（共有する場合は shared を使う）".to_owned())
      }
      // 共有するのは同じファイルで、設定は記録済みの config と照合される。別の接続を持つストアになる。
      StorageRequest::Shared(_) => match (self.open)() {
        Ok(store) => {
          self.registry.insert(name, store);
          Ok(Ok(()))
        }
        Err(error) => Ok(Err(error)),
      },
    }
  }

  /// ストアの接続とは別の接続を同じファイルに開き、書き込みのロックを取って持ち続ける。
  pub(super) fn hold_write_lock(&mut self) -> Result<(), String> {
    if self.lock.is_some() {
      return Err("すでに書き込みのロックを持っている".to_owned());
    }
    let connection = Connection::open_with_flags(self.path, OpenFlags::SQLITE_OPEN_READ_WRITE)
      .map_err(|error| format!("ロックを持つための接続を開けない: {error}"))?;
    connection
      .execute_batch("BEGIN IMMEDIATE")
      .map_err(|error| format!("書き込みのロックを取れない: {error}"))?;
    self.lock = Some(connection);
    Ok(())
  }

  /// 持っている書き込みのロックを、トランザクションを閉じて放す。
  pub(super) fn release_write_lock(&mut self) -> Result<(), String> {
    let connection = self
      .lock
      .take()
      .ok_or_else(|| "持っていない書き込みのロックは放せない".to_owned())?;
    connection
      .execute_batch("ROLLBACK")
      .map_err(|error| format!("書き込みのロックを放せない: {error}"))
  }
}

#[cfg(test)]
#[path = "stores_test.rs"]
mod tests;
