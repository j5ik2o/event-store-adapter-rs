//! workspace への組み込みと、公開しないこと、実行対象のライブラリに依存しないことの試験。

use std::path::Path;
use std::process::Command;

use serde_json::{json, Value};

const PACKAGE_NAME: &str = "event-store-adapter-conformance-rs";

/// `cargo metadata` で、workspace 全体の構成を読む。
fn workspace_metadata() -> Value {
  let root_manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("../Cargo.toml");
  let output = Command::new(env!("CARGO"))
    .args(["metadata", "--no-deps", "--format-version", "1", "--manifest-path"])
    .arg(root_manifest)
    .output()
    .expect("cargo metadata を起動できる");
  assert!(
    output.status.success(),
    "stderr: {}",
    String::from_utf8_lossy(&output.stderr)
  );
  serde_json::from_slice(&output.stdout).expect("cargo metadata の出力は JSON")
}

fn runner_package(metadata: &Value) -> Value {
  metadata["packages"]
    .as_array()
    .expect("packages は配列")
    .iter()
    .find(|package| package["name"] == PACKAGE_NAME)
    .unwrap_or_else(|| panic!("{PACKAGE_NAME} が workspace にある"))
    .clone()
}

#[test]
fn test_runner_is_an_unpublished_workspace_member() {
  let metadata = workspace_metadata();

  let package = runner_package(&metadata);

  assert_eq!(package["publish"], json!([]), "publish = false");
  let package_id = package["id"].as_str().expect("id は文字列");
  let is_member = metadata["workspace_members"]
    .as_array()
    .expect("workspace_members は配列")
    .iter()
    .any(|member| member.as_str() == Some(package_id));
  assert!(is_member, "{package_id} が workspace の members にある");
}

#[test]
fn test_runner_does_not_depend_on_the_library_under_test() {
  let metadata = workspace_metadata();

  let package = runner_package(&metadata);

  let dependencies: Vec<&str> = package["dependencies"]
    .as_array()
    .expect("dependencies は配列")
    .iter()
    .map(|dependency| dependency["name"].as_str().expect("name は文字列"))
    .collect();
  assert!(!dependencies.contains(&"event-store-adapter-rs"), "{dependencies:?}");
  assert!(package["features"].get("test-hooks").is_none(), "test-hooks を使わない");
}
