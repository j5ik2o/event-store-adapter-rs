//! 固定日の nightly で rustdoc の JSON を作り、`rustdoc-types` で読み込む。

use std::path::Path;
use std::process::Command;

use rustdoc_types::{Crate, FORMAT_VERSION};

/// rustdoc の JSON を作る nightly。`rustdoc-types` の版と組になるので、上げるときは一緒に上げる。
///
/// 3 か所を一緒に変える: この定数、`.github/workflows/ci.yml` の `api-surface` ジョブ、`api-surface/check/Cargo.toml` の `rustdoc-types` の版。
/// `nightly-2026-09-27` は rustdoc の JSON の形式の版 61、`rustdoc-types` 0.61.0 と組になる。
pub const TOOLCHAIN: &str = "nightly-2026-09-27";

/// `package`（ワークスペースのメンバー）のライブラリを feature なしで文書化し、rustdoc の JSON を読み込む。
///
/// 外側の `cargo` とビルド先の取り合いを避けるため、`target_dir` は呼び出し側が別に指定する。
pub fn document_library(workspace_root: &Path, package: &str, target_dir: &Path) -> Result<Crate, String> {
  let manifest = workspace_root.join("Cargo.toml");
  let output = Command::new("rustup")
    .args([
      "run",
      TOOLCHAIN,
      "cargo",
      "rustdoc",
      "--locked",
      "-p",
      package,
      "--lib",
      "--manifest-path",
    ])
    .arg(&manifest)
    .arg("--target-dir")
    .arg(target_dir)
    .args(["--", "-Z", "unstable-options", "--output-format", "json"])
    .output()
    .map_err(|error| format!("rustup を起動できない: {error}。{}", install_hint()))?;
  if !output.status.success() {
    return Err(format!(
      "{TOOLCHAIN} の cargo rustdoc が失敗した ({})。{}\nstderr:\n{}",
      output.status,
      install_hint(),
      String::from_utf8_lossy(&output.stderr)
    ));
  }
  let json_path = target_dir
    .join("doc")
    .join(format!("{}.json", package.replace('-', "_")));
  let bytes = std::fs::read(&json_path).map_err(|error| format!("{} を読めない: {error}", json_path.display()))?;
  parse_rustdoc_json(&bytes)
}

fn install_hint() -> String {
  format!("固定日の nightly が要る: `rustup toolchain install {TOOLCHAIN} --profile minimal`")
}

/// rustdoc の JSON を読み込む。形式の版が `rustdoc-types` と違うときは、読み込む前に失敗させる。
pub(crate) fn parse_rustdoc_json(bytes: &[u8]) -> Result<Crate, String> {
  let value: serde_json::Value =
    serde_json::from_slice(bytes).map_err(|error| format!("rustdoc の JSON を読めない: {error}"))?;
  let format_version = value
    .get("format_version")
    .and_then(serde_json::Value::as_u64)
    .ok_or_else(|| "rustdoc の JSON に format_version がない".to_string())?;
  if format_version != u64::from(FORMAT_VERSION) {
    return Err(format!(
      "rustdoc の JSON の形式の版が {format_version}、rustdoc-types は {FORMAT_VERSION}。\
       nightly の日付 ({TOOLCHAIN}) と rustdoc-types の版を一緒に上げる"
    ));
  }
  serde_json::from_value(value).map_err(|error| format!("rustdoc の JSON を rustdoc-types で読めない: {error}"))
}
