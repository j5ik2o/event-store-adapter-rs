//! 適合テストデータと一時ディレクトリを扱う、結合テスト共通の補助。

use std::fs;
use std::path::{Path, PathBuf};

/// 配布されている適合テストデータのディレクトリを返す。
pub fn conformance_dir() -> PathBuf {
  Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance")
}

/// 破棄時に中身ごと消える一時ディレクトリを表す。
pub struct TempDir {
  path: PathBuf,
}

impl TempDir {
  /// プロセスとラベルで名前が決まる空の一時ディレクトリを作る。
  pub fn new(label: &str) -> TempDir {
    let path = std::env::temp_dir().join(format!("conformance-runner-{}-{label}", std::process::id()));
    if path.exists() {
      fs::remove_dir_all(&path).expect("前回の一時ディレクトリを消せる");
    }
    fs::create_dir_all(&path).expect("一時ディレクトリを作れる");
    TempDir { path }
  }

  /// 一時ディレクトリの場所を返す。
  pub fn path(&self) -> &Path {
    &self.path
  }
}

impl Drop for TempDir {
  fn drop(&mut self) {
    // 後片付けの失敗は試験の結果を変えないので、握りつぶす。
    let _ = fs::remove_dir_all(&self.path);
  }
}

/// ディレクトリを、ドットファイルを含めて再帰的に写す。
pub fn copy_dir_all(from: &Path, to: &Path) {
  fs::create_dir_all(to).expect("写し先を作れる");
  for entry in fs::read_dir(from).expect("写し元を読める") {
    let entry = entry.expect("エントリを読める");
    let target = to.join(entry.file_name());
    if entry.file_type().expect("種別を読める").is_dir() {
      copy_dir_all(&entry.path(), &target);
    } else {
      fs::copy(entry.path(), &target).expect("ファイルを写せる");
    }
  }
}
