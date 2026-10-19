//! SQLite の結合試験が共有する、一時ファイルの補助。

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// 破棄時にディレクトリごと消える、データベースファイルの置き場を表す。ファイルそのものは作らない。
pub struct TempDatabase {
  directory: PathBuf,
  path: PathBuf,
}

impl TempDatabase {
  /// プロセスの番号と連番で名前が決まる空のディレクトリを作り、その下のファイル名を `path` に持つ。
  pub fn new() -> Self {
    let directory = std::env::temp_dir().join(format!(
      "event-store-adapter-sqlite-{}-{}",
      std::process::id(),
      SEQUENCE.fetch_add(1, Ordering::SeqCst)
    ));
    if directory.exists() {
      fs::remove_dir_all(&directory).expect("前回の一時ディレクトリを消せる");
    }
    fs::create_dir_all(&directory).expect("一時ディレクトリを作れる");
    let path = directory.join("store.sqlite3");
    Self { directory, path }
  }

  /// データベースファイルの場所を返す。
  pub fn path(&self) -> &Path {
    &self.path
  }
}

impl Drop for TempDatabase {
  fn drop(&mut self) {
    // 後片付けの失敗は試験の結果を変えないので、握りつぶす。
    let _ = fs::remove_dir_all(&self.directory);
  }
}
