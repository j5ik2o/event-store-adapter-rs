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

#[test]
fn should_dynamodb_local_tests_require_the_opt_in_dynamodb_feature() {
  let package = runner_package(&workspace_metadata());
  assert_eq!(package["features"]["default"], json!([]));
  assert_eq!(
    package["features"]["dynamodb"],
    json!([
      "event-store-adapter-rs/dynamodb",
      "dep:event-store-adapter-test-utils-rs",
      "dep:testcontainers",
      "dep:aws-smithy-http-client"
    ])
  );
  assert!(!requested_features(&package, "event-store-adapter-rs").contains(&"dynamodb".to_string()));
  for name in ["dynamodb_config_read_test", "dynamodb_retention_ttl_test"] {
    let target = package["targets"]
      .as_array()
      .unwrap()
      .iter()
      .find(|target| target["name"] == name)
      .unwrap();
    assert_eq!(target["required-features"], json!(["dynamodb"]));
  }
}

#[test]
fn should_library_keep_the_bundled_sqlite_dependency_optional_behind_the_sqlite_feature() {
  let metadata = workspace_metadata();
  let library = metadata["packages"]
    .as_array()
    .unwrap()
    .iter()
    .find(|package| package["name"] == "event-store-adapter-rs")
    .unwrap();

  let defaults = library["features"]["default"].as_array().unwrap();
  assert!(
    !defaults.iter().any(|feature| feature == "sqlite"),
    "SQLite は既定の機能に入れない: {defaults:?}"
  );
  let sqlite = library["features"]["sqlite"]
    .as_array()
    .expect("ライブラリに機能 sqlite がある");
  assert!(
    sqlite.contains(&json!("dep:rusqlite")),
    "機能 sqlite が rusqlite を有効にする: {sqlite:?}"
  );
  let rusqlite = library["dependencies"]
    .as_array()
    .unwrap()
    .iter()
    .find(|dependency| dependency["name"] == "rusqlite" && dependency["kind"].is_null())
    .expect("rusqlite は通常の依存として宣言される");
  assert_eq!(
    rusqlite["optional"], true,
    "機能 sqlite を付けないビルドに rusqlite を入れない"
  );
  assert!(
    requested_features(library, "rusqlite").contains(&"bundled".to_string()),
    "SQLite 本体を同梱する: {:?}",
    requested_features(library, "rusqlite")
  );
}

#[test]
fn should_sqlite_tests_require_the_opt_in_sqlite_feature() {
  let package = runner_package(&workspace_metadata());

  assert_eq!(package["features"]["default"], json!([]));
  let sqlite = package["features"]["sqlite"]
    .as_array()
    .expect("実行器に機能 sqlite がある");
  assert!(
    sqlite.contains(&json!("event-store-adapter-rs/sqlite")),
    "実行器の機能 sqlite がライブラリの機能 sqlite を有効にする: {sqlite:?}"
  );
  assert!(
    !requested_features(&package, "event-store-adapter-rs").contains(&"sqlite".to_string()),
    "ライブラリの機能 sqlite を常には有効にしない"
  );
  let target = package["targets"]
    .as_array()
    .unwrap()
    .iter()
    .find(|target| target["name"] == "sqlite_test")
    .expect("sqlite_test がある");
  assert_eq!(target["required-features"], json!(["sqlite"]));
}

fn library_package(metadata: &Value) -> Value {
  metadata["packages"]
    .as_array()
    .unwrap()
    .iter()
    .find(|package| package["name"] == "event-store-adapter-rs")
    .expect("ライブラリが workspace にある")
    .clone()
}

/// 機能 `bigtable` だけが有効にする gRPC の crate。版は検査しない（`Cargo.lock` はコミットされず、
/// 互換な新しい版が出るたびに CI が解決し直すので、解決結果の版を固定する試験は何も変えずに失敗する）。
const BIGTABLE_GRPC_CRATES: [&str; 3] = [
  "tonic",
  "googleapis-tonic-google-bigtable-v2",
  "googleapis-tonic-google-bigtable-admin-v2",
];

#[test]
fn should_library_keep_the_bigtable_grpc_dependencies_optional_behind_the_bigtable_feature() {
  let library = library_package(&workspace_metadata());

  let defaults = library["features"]["default"].as_array().unwrap();
  assert!(
    !defaults.iter().any(|feature| feature == "bigtable"),
    "Bigtable は既定の機能に入れない: {defaults:?}"
  );
  let bigtable = library["features"]["bigtable"]
    .as_array()
    .expect("ライブラリに機能 bigtable がある");
  for name in BIGTABLE_GRPC_CRATES {
    assert!(
      bigtable.contains(&json!(format!("dep:{name}"))),
      "機能 bigtable が {name} を有効にする: {bigtable:?}"
    );
    let dependency = library["dependencies"]
      .as_array()
      .unwrap()
      .iter()
      .find(|dependency| dependency["name"] == name && dependency["kind"].is_null())
      .unwrap_or_else(|| panic!("{name} は通常の依存として宣言される"));
    assert_eq!(
      dependency["optional"], true,
      "機能 bigtable を付けないビルドに {name} を入れない"
    );
  }
}

#[test]
fn should_library_declare_no_unconditional_grpc_dependency_and_keep_test_support_out_of_normal_dependencies() {
  let library = library_package(&workspace_metadata());

  for dependency in library["dependencies"].as_array().unwrap() {
    let name = dependency["name"].as_str().unwrap();
    let is_normal = dependency["kind"].is_null();
    if is_normal && (name.starts_with("tonic") || name.starts_with("prost") || name.starts_with("googleapis")) {
      assert_eq!(
        dependency["optional"], true,
        "機能なしのビルドに入る gRPC の通常の依存がある: {name}"
      );
    }
    if name == "event-store-adapter-test-utils-rs" {
      assert_eq!(dependency["kind"], "dev", "試験の補助の依存が通常の依存へ漏れていない");
    }
  }
}

#[test]
fn should_library_not_enable_test_hooks_through_the_bigtable_feature() {
  let library = library_package(&workspace_metadata());

  let bigtable = library["features"]["bigtable"]
    .as_array()
    .expect("ライブラリに機能 bigtable がある");

  assert!(
    !bigtable.contains(&json!("test-hooks")),
    "フックや設定の上書きは機能 test-hooks の外で公開しない: {bigtable:?}"
  );
}

#[test]
fn should_bigtable_tests_require_the_opt_in_bigtable_feature() {
  let package = runner_package(&workspace_metadata());

  assert_eq!(package["features"]["default"], json!([]));
  let bigtable = package["features"]["bigtable"]
    .as_array()
    .expect("実行器に機能 bigtable がある");
  assert!(
    bigtable.contains(&json!("event-store-adapter-rs/bigtable")),
    "実行器の機能 bigtable がライブラリの機能 bigtable を有効にする: {bigtable:?}"
  );
  assert!(
    !requested_features(&package, "event-store-adapter-rs").contains(&"bigtable".to_string()),
    "ライブラリの機能 bigtable を常には有効にしない"
  );
  let target = package["targets"]
    .as_array()
    .unwrap()
    .iter()
    .find(|target| target["name"] == "bigtable_test")
    .expect("bigtable_test がある");
  assert_eq!(target["required-features"], json!(["bigtable"]));
}

/// 機能 `spanner` だけが有効にする gRPC の crate。版は検査しない（理由は `BIGTABLE_GRPC_CRATES` と同じ）。
const SPANNER_GRPC_CRATES: [&str; 3] = [
  "tonic",
  "googleapis-tonic-google-spanner-v1",
  "googleapis-tonic-google-spanner-admin-database-v1",
];

#[test]
fn should_library_keep_the_spanner_grpc_dependencies_optional_behind_the_spanner_feature() {
  let library = library_package(&workspace_metadata());

  let defaults = library["features"]["default"].as_array().unwrap();
  assert!(
    !defaults.iter().any(|feature| feature == "spanner"),
    "Spanner は既定の機能に入れない: {defaults:?}"
  );
  let spanner = library["features"]["spanner"]
    .as_array()
    .expect("ライブラリに機能 spanner がある");
  for name in SPANNER_GRPC_CRATES {
    assert!(
      spanner.contains(&json!(format!("dep:{name}"))),
      "機能 spanner が {name} を有効にする: {spanner:?}"
    );
    let dependency = library["dependencies"]
      .as_array()
      .unwrap()
      .iter()
      .find(|dependency| dependency["name"] == name && dependency["kind"].is_null())
      .unwrap_or_else(|| panic!("{name} は通常の依存として宣言される"));
    assert_eq!(
      dependency["optional"], true,
      "機能 spanner を付けないビルドに {name} を入れない"
    );
  }
}

#[test]
fn should_library_spanner_feature_not_pull_in_the_other_backends_or_the_test_hooks() {
  let library = library_package(&workspace_metadata());

  let spanner = library["features"]["spanner"]
    .as_array()
    .expect("ライブラリに機能 spanner がある");

  for other in [
    "dynamodb",
    "sqlite",
    "bigtable",
    "migration",
    "sqlite-migration",
    "bigtable-migration",
    "test-hooks",
    "dep:aws-sdk-dynamodb",
    "dep:rusqlite",
    "dep:googleapis-tonic-google-bigtable-v2",
    "dep:googleapis-tonic-google-bigtable-admin-v2",
  ] {
    assert!(
      !spanner.contains(&json!(other)),
      "機能 spanner は {other} を有効にしない（フックや設定の上書きは機能 test-hooks の外で公開しない）: {spanner:?}"
    );
  }
}

#[test]
fn should_spanner_tests_require_the_opt_in_spanner_feature() {
  let package = runner_package(&workspace_metadata());

  assert_eq!(package["features"]["default"], json!([]));
  let spanner = package["features"]["spanner"]
    .as_array()
    .expect("実行器に機能 spanner がある");
  assert!(
    spanner.contains(&json!("event-store-adapter-rs/spanner")),
    "実行器の機能 spanner がライブラリの機能 spanner を有効にする: {spanner:?}"
  );
  assert!(
    !requested_features(&package, "event-store-adapter-rs").contains(&"spanner".to_string()),
    "ライブラリの機能 spanner を常には有効にしない"
  );
  let target = package["targets"]
    .as_array()
    .unwrap()
    .iter()
    .find(|target| target["name"] == "spanner_test")
    .expect("spanner_test がある");
  assert_eq!(target["required-features"], json!(["spanner"]));
}
