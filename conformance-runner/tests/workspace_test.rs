//! workspace への組み込みと、公開しないこと、実行対象のライブラリに依存し・その `test-hooks` を有効にすることの試験。

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
fn should_runner_is_an_unpublished_workspace_member() {
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

/// `package` が依存 `dependency_name` に対して有効にした feature の名前を返す。依存がなければ空を返す。
fn requested_features(package: &Value, dependency_name: &str) -> Vec<String> {
  package["dependencies"]
    .as_array()
    .expect("dependencies は配列")
    .iter()
    .filter(|dependency| dependency["name"] == dependency_name)
    .flat_map(|dependency| dependency["features"].as_array().expect("features は配列"))
    .map(|feature| feature.as_str().expect("feature は文字列").to_string())
    .collect()
}

#[test]
fn should_runner_depends_on_the_library_under_test() {
  let metadata = workspace_metadata();

  let package = runner_package(&metadata);

  let dependencies: Vec<&str> = package["dependencies"]
    .as_array()
    .expect("dependencies は配列")
    .iter()
    .map(|dependency| dependency["name"].as_str().expect("name は文字列"))
    .collect();
  assert!(dependencies.contains(&"event-store-adapter-rs"), "{dependencies:?}");
}

#[test]
fn should_runner_enables_test_hooks_of_the_library_under_test() {
  let metadata = workspace_metadata();

  let package = runner_package(&metadata);

  let features = requested_features(&package, "event-store-adapter-rs");
  assert!(
    features.iter().any(|feature| feature == "test-hooks"),
    "実メモリの障害注入に test-hooks が必要: {features:?}"
  );
}

#[test]
fn should_requested_features_reads_the_features_that_cargo_metadata_lists_for_a_dependency() {
  let metadata = workspace_metadata();

  let package = runner_package(&metadata);

  // `Cargo.toml` が serde_json に有効にしている feature を、実際の `cargo metadata` から読めること。
  assert!(
    requested_features(&package, "serde_json").contains(&"arbitrary_precision".to_string()),
    "{:?}",
    requested_features(&package, "serde_json")
  );
  assert!(requested_features(&package, "no-such-dependency").is_empty());
}

#[test]
fn should_runner_pins_the_schema_crate_and_leaves_its_network_features_off() {
  let metadata = workspace_metadata();

  let package = runner_package(&metadata);

  let dependency = package["dependencies"]
    .as_array()
    .expect("dependencies は配列")
    .iter()
    .find(|dependency| dependency["name"] == "jsonschema")
    .expect("jsonschema に依存する");
  assert_eq!(dependency["req"], "=0.20.0", "版を固定する");
  assert_eq!(
    dependency["uses_default_features"], false,
    "HTTP・ファイルでの取得の既定の機能を有効にしない"
  );
  assert_eq!(dependency["features"], json!([]));
}

#[test]
fn should_library_not_enable_test_hooks_by_default() {
  let metadata = workspace_metadata();
  let library = metadata["packages"]
    .as_array()
    .unwrap()
    .iter()
    .find(|package| package["name"] == "event-store-adapter-rs")
    .unwrap();
  let defaults = library["features"]["default"].as_array().unwrap();
  assert!(!defaults.iter().any(|feature| feature == "test-hooks"));
}
