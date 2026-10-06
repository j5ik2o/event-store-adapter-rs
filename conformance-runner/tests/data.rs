//! 適合テストデータの読み込み・`manifest` の照合・generators の展開の試験。

mod support;

use std::collections::HashSet;
use std::fs;
use std::path::Path;

use event_store_adapter_conformance_rs::data::{
  expand_generators, inventory, inventory_path, load, parse_strict_json, sha256_hex, verify_manifest, CaseKind,
  DataError, InventoryEntry, ManifestProblem,
};
use serde_json::{json, Value};
use support::{conformance_dir, copy_dir_all, TempDir};

/// `ci.yml` が固定している、配布中の `manifest.json` の SHA-256。
const FIXED_MANIFEST_SHA256: &str = "61c26614dbbfba88eebce72cc1d2b0220218839e74dcfb64c19268f7ee2302ce";
const SHA256_OF_ABC: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
const SHA256_OF_EMPTY: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

fn entry(path: &str, bytes: &[u8]) -> InventoryEntry {
  InventoryEntry {
    path: path.to_string(),
    bytes: bytes.to_vec(),
  }
}

fn manifest_value(version: &str, files: &[(&str, &str)]) -> Value {
  let files: Vec<Value> = files
    .iter()
    .map(|(path, sha256)| json!({"path": path, "sha256": sha256}))
    .collect();
  json!({"format": "manifest", "version": version, "files": files})
}

// ---------------------------------------------------------------------------
// 厳格な JSON の読み込み
// ---------------------------------------------------------------------------

#[test]
fn test_parse_strict_json_keeps_same_keys_in_strings_and_sibling_objects() {
  let input = br#"{"x":"{\"a\":1,\"a\":2}","l":[{"k":1},{"k":1}],"o":{"a":{"k":1},"b":{"k":2}}}"#;

  let value = parse_strict_json(input).expect("文字列の中や別のオブジェクトの同じキーは重複ではない");

  assert_eq!(value["x"], r#"{"a":1,"a":2}"#);
}

#[test]
fn test_parse_strict_json_rejects_duplicate_key_in_same_object() {
  let input = br#"{"o":{"k":1,"k":2}}"#;

  let error = parse_strict_json(input).expect_err("同じオブジェクトの重複キーは拒否する");

  assert!(error.to_string().contains("JSON キーが重複: k"), "message: {error}");
}

#[test]
fn test_parse_strict_json_rejects_nan() {
  assert!(parse_strict_json(br#"{"n":NaN}"#).is_err());
}

#[test]
fn test_parse_strict_json_rejects_infinity() {
  assert!(parse_strict_json(br#"{"n":Infinity}"#).is_err());
  assert!(parse_strict_json(br#"{"n":-Infinity}"#).is_err());
}

#[test]
fn test_parse_strict_json_rejects_input_that_is_not_utf8() {
  let input = b"{\"a\":\"\xff\"}";

  assert!(parse_strict_json(input).is_err());
}

#[test]
fn test_parse_strict_json_keeps_integer_beyond_128_bits_exactly() {
  let digits = "170141183460469231731687303715884105727";
  let input = format!(r#"{{"n":{digits}}}"#);

  let value = parse_strict_json(input.as_bytes()).expect("巨大な整数も読める");

  assert_eq!(serde_json::to_string(&value["n"]).unwrap(), digits);
}

#[test]
fn test_parse_strict_json_keeps_key_and_string_text_unchanged() {
  // 合成済みの é をキーに、分解した é と大文字小文字の混在を値に持つ。
  let input = r#"{"é":"é","Key":"MiXed"}"#;

  let value = parse_strict_json(input.as_bytes()).expect("読める");

  assert_eq!(value["\u{e9}"], "e\u{301}");
  assert_eq!(value["Key"], "MiXed");
}

// ---------------------------------------------------------------------------
// 全ファイルの読み込み（実データ）
// ---------------------------------------------------------------------------

#[test]
fn test_load_reads_every_case_of_real_data_once() {
  let data = load(&conformance_dir()).expect("実データを読める");

  let ids: HashSet<&str> = data.cases.iter().map(|case| case.id.as_str()).collect();
  assert_eq!(data.cases.len(), 116);
  assert_eq!(ids.len(), 116, "ID に重複がない");
}

#[test]
fn test_load_reads_value_tables_scenarios_and_layout() {
  let data = load(&conformance_dir()).expect("実データを読める");

  let count = |wanted: fn(&CaseKind) -> bool| data.cases.iter().filter(|case| wanted(&case.kind)).count();
  assert_eq!(count(|kind| matches!(kind, CaseKind::ValueTable)), 30);
  assert_eq!(count(|kind| matches!(kind, CaseKind::Scenario)), 85);
  assert_eq!(count(|kind| matches!(kind, CaseKind::Layout)), 1);
}

#[test]
fn test_load_reads_coverage_rules_and_exclusions() {
  let data = load(&conformance_dir()).expect("実データを読める");

  let exclusions: Vec<(&str, &str)> = data
    .coverage
    .exclusions
    .iter()
    .map(|exclusion| (exclusion.rule.as_str(), exclusion.status.as_str()))
    .collect();
  assert_eq!(data.coverage.required_rules.len(), 46);
  assert_eq!(exclusions, vec![("W-5", "deleted"), ("R-7", "caller-obligation")]);
}

#[test]
fn test_inventory_lists_the_22_files_covered_by_manifest_without_manifest_itself() {
  let entries = inventory(&conformance_dir()).expect("実データの一覧を作れる");

  let paths: Vec<&str> = entries.iter().map(|entry| entry.path.as_str()).collect();
  assert_eq!(paths.len(), 22);
  assert!(!paths.contains(&"manifest.json"));
  assert!(paths.contains(&".gitattributes"));
  assert!(paths.contains(&"scenarios/core/write-read.json"));
}

// ---------------------------------------------------------------------------
// 一覧の相対パス（UTF-8 でないパスは置換文字に潰さず、誤りにする）
// ---------------------------------------------------------------------------

#[test]
fn test_inventory_path_joins_utf8_components_with_slash() {
  assert_eq!(
    inventory_path(Path::new("scenarios/core/write-read.json")).expect("UTF-8 のパス"),
    "scenarios/core/write-read.json"
  );
  assert_eq!(
    inventory_path(Path::new("values/é.json")).expect("UTF-8 のパス"),
    "values/é.json"
  );
  assert_eq!(
    inventory_path(Path::new(".gitattributes")).expect("UTF-8 のパス"),
    ".gitattributes"
  );
}

/// UTF-8 でないバイトを含むパスを、ファイルを作らずに組み立てる。
#[cfg(unix)]
fn non_utf8_path(components: &[&[u8]]) -> std::path::PathBuf {
  use std::os::unix::ffi::OsStrExt;
  components
    .iter()
    .map(|component| std::ffi::OsStr::from_bytes(component))
    .collect()
}

#[cfg(unix)]
#[test]
fn test_inventory_path_rejects_a_component_that_is_not_utf8() {
  let paths: [&[&[u8]]; 3] = [
    &[b"values", b"bad\xff.json"],
    &[b"values", b"bad\xfe.json"],
    &[b"bad\xff", b"a.json"],
  ];

  for components in paths {
    let relative = non_utf8_path(components);

    let Err(error) = inventory_path(&relative) else {
      panic!("UTF-8 でないパスは誤りにする: {relative:?}");
    };

    assert!(
      matches!(&error, DataError::NotUtf8Path { path } if path.contains("bad") || path.ends_with("a.json")),
      "{error:?}"
    );
    assert!(error.to_string().contains("UTF-8"), "{error}");
  }
}

#[cfg(target_os = "linux")]
#[test]
fn test_inventory_rejects_files_whose_non_utf8_names_would_collapse_into_one_path() {
  use std::os::unix::ffi::OsStrExt;
  // 内容が同じで、不正なバイトだけが異なる 2 つのファイル。置換文字に潰すと、同じパスになる。
  let dir = TempDir::new("data-inventory-non-utf8");
  fs::create_dir_all(dir.path().join("values")).expect("ディレクトリを作れる");
  for name in [&b"bad\xff.json"[..], &b"bad\xfe.json"[..]] {
    fs::write(
      dir.path().join("values").join(std::ffi::OsStr::from_bytes(name)),
      b"same",
    )
    .expect("UTF-8 でない名前のファイルを作れる");
  }

  let Err(error) = inventory(dir.path()) else {
    panic!("UTF-8 でない名前のファイルがあれば、一覧を作れない");
  };

  assert!(matches!(&error, DataError::NotUtf8Path { .. }), "{error:?}");
}

#[test]
fn test_load_verifies_real_manifest_and_exposes_its_fixed_sha256() {
  let data = load(&conformance_dir()).expect("実データを読める");

  assert!(data.manifest.passed(), "problems: {:?}", data.manifest.problems);
  assert_eq!(data.manifest_files, 22);
  assert_eq!(data.manifest_sha256, FIXED_MANIFEST_SHA256);
  assert_eq!(data.manifest_version.as_deref(), Some("1.0.0"));
}

#[test]
fn test_load_skips_an_unlisted_case_file_and_reports_it_as_unlisted() {
  let dir = TempDir::new("data-load-unlisted");
  copy_dir_all(&conformance_dir(), dir.path());
  fs::write(dir.path().join("values").join("unlisted.json"), "{").expect("一覧にないファイルを書ける");

  let data = load(dir.path()).expect("一覧にないファイルが壊れていても、ケースの読み込みは続ける");

  assert_eq!(data.cases.len(), 116);
  assert!(
    data
      .manifest
      .problems
      .iter()
      .any(|problem| matches!(problem, ManifestProblem::Unlisted(path) if path == "values/unlisted.json")),
    "problems: {:?}",
    data.manifest.problems
  );
}

// ---------------------------------------------------------------------------
// format と version の確認（合成した一時ディレクトリ）
// ---------------------------------------------------------------------------

// 合成したケースのファイルを一覧に載せる（ハッシュは合わないが、一覧に載ったファイルだけをケースとして読むため）
const MANIFEST_JSON: &str = r#"{"format":"manifest","version":"1.0.0","files":[{"path":"values/synthetic.json","sha256":"0000000000000000000000000000000000000000000000000000000000000000"}]}"#;
const COVERAGE_JSON: &str =
  r#"{"format":"coverage","version":"1.0.0","required_rules":["T-1"],"exclusions":[],"notes":[]}"#;

fn values_file(format: &str, version: &str) -> String {
  json!({
    "format": format,
    "version": version,
    "cases": [{
      "id": "synthetic-1",
      "rules": ["T-1"],
      "description": "合成したケース",
      "operation": "validateSeqNr",
      "input": {"seq_nr": 1, "context": "value"},
      "expect": {"value": 1}
    }]
  })
  .to_string()
}

fn write_file(root: &Path, relative: &str, content: &str) {
  let path = root.join(relative);
  fs::create_dir_all(path.parent().unwrap()).expect("親ディレクトリを作れる");
  fs::write(path, content).expect("ファイルを書ける");
}

fn write_dataset(root: &Path, coverage: &str, values: &str) {
  copy_dir_all(&conformance_dir().join("schema"), &root.join("schema"));
  write_file(root, "manifest.json", MANIFEST_JSON);
  write_file(root, "coverage.json", coverage);
  write_file(root, "values/synthetic.json", values);
}

#[test]
fn test_load_accepts_case_file_with_expected_format_and_version() {
  let dir = TempDir::new("data-load-valid");
  write_dataset(dir.path(), COVERAGE_JSON, &values_file("values", "1.0.0"));

  let data = load(dir.path()).expect("format と version が正しければ読める");

  assert_eq!(data.cases.len(), 1);
  assert_eq!(data.cases[0].id, "synthetic-1");
  assert_eq!(data.cases[0].rules, vec!["T-1".to_string()]);
  assert!(matches!(data.cases[0].kind, CaseKind::ValueTable));
}

#[test]
fn test_load_rejects_case_file_with_other_version() {
  let dir = TempDir::new("data-load-version");
  write_dataset(dir.path(), COVERAGE_JSON, &values_file("values", "2.0.0"));

  let Err(error) = load(dir.path()) else {
    panic!("version が違えば読み込みを失敗にする");
  };

  assert!(
    matches!(&error, DataError::Invalid { path, .. } if path.ends_with("values/synthetic.json")),
    "{error:?}"
  );
}

#[test]
fn test_load_rejects_case_file_with_unknown_format() {
  let dir = TempDir::new("data-load-format");
  write_dataset(dir.path(), COVERAGE_JSON, &values_file("tables", "1.0.0"));

  let Err(error) = load(dir.path()) else {
    panic!("format が違えば読み込みを失敗にする");
  };

  assert!(
    matches!(&error, DataError::Invalid { path, .. } if path.ends_with("values/synthetic.json")),
    "{error:?}"
  );
}

#[test]
fn test_load_rejects_coverage_file_with_other_version() {
  let dir = TempDir::new("data-load-coverage-version");
  // `notes` もない（スキーマの違反でもある）。版の確認がスキーマの検査より先なので、誤りは `Invalid`。
  let coverage = r#"{"format":"coverage","version":"2.0.0","required_rules":["T-1"],"exclusions":[]}"#;
  write_dataset(dir.path(), coverage, &values_file("values", "1.0.0"));

  let Err(error) = load(dir.path()) else {
    panic!("coverage.json の version が違えば読み込みを失敗にする");
  };

  assert!(
    matches!(&error, DataError::Invalid { path, .. } if path.ends_with("coverage.json")),
    "{error:?}"
  );
}

// ---------------------------------------------------------------------------
// manifest の照合
// ---------------------------------------------------------------------------

#[test]
fn test_sha256_hex_matches_known_vectors() {
  assert_eq!(sha256_hex(b"abc"), SHA256_OF_ABC);
  assert_eq!(sha256_hex(b""), SHA256_OF_EMPTY);
}

#[test]
fn test_verify_manifest_accepts_consistent_inventory_in_any_order() {
  let manifest = manifest_value("1.0.0", &[("b.json", SHA256_OF_EMPTY), ("a.json", SHA256_OF_ABC)]);
  let inventory = [entry("a.json", b"abc"), entry("b.json", b"")];

  let verification = verify_manifest(&manifest, &inventory);

  assert!(verification.passed(), "problems: {:?}", verification.problems);
  assert!(verification.problems.is_empty());
}

#[test]
fn test_verify_manifest_reports_version_mismatch() {
  let manifest = manifest_value("2.0.0", &[("a.json", SHA256_OF_ABC)]);
  let inventory = [entry("a.json", b"abc")];

  let verification = verify_manifest(&manifest, &inventory);

  assert!(!verification.passed());
  assert!(
    matches!(verification.problems.as_slice(), [ManifestProblem::Version(_)]),
    "{:?}",
    verification.problems
  );
}

#[test]
fn test_verify_manifest_reports_unlisted_file() {
  let manifest = manifest_value("1.0.0", &[("a.json", SHA256_OF_ABC)]);
  let inventory = [entry("a.json", b"abc"), entry("b.json", b"")];

  let verification = verify_manifest(&manifest, &inventory);

  assert!(!verification.passed());
  assert_eq!(
    verification.problems,
    vec![ManifestProblem::Unlisted("b.json".to_string())]
  );
}

#[test]
fn test_verify_manifest_reports_missing_file() {
  let manifest = manifest_value("1.0.0", &[("a.json", SHA256_OF_ABC), ("b.json", SHA256_OF_EMPTY)]);
  let inventory = [entry("a.json", b"abc")];

  let verification = verify_manifest(&manifest, &inventory);

  assert!(!verification.passed());
  assert_eq!(
    verification.problems,
    vec![ManifestProblem::Missing("b.json".to_string())]
  );
}

#[test]
fn test_verify_manifest_reports_modified_file() {
  let manifest = manifest_value("1.0.0", &[("a.json", SHA256_OF_EMPTY)]);
  let inventory = [entry("a.json", b"abc")];

  let verification = verify_manifest(&manifest, &inventory);

  assert!(!verification.passed());
  assert_eq!(
    verification.problems,
    vec![ManifestProblem::Modified("a.json".to_string())]
  );
}

#[test]
fn test_verify_manifest_reports_duplicate_entry() {
  let manifest = manifest_value("1.0.0", &[("a.json", SHA256_OF_ABC), ("a.json", SHA256_OF_ABC)]);
  let inventory = [entry("a.json", b"abc")];

  let verification = verify_manifest(&manifest, &inventory);

  assert!(!verification.passed());
  assert_eq!(
    verification.problems,
    vec![ManifestProblem::Duplicate("a.json".to_string())]
  );
}

#[test]
fn test_verify_manifest_rejects_modified_copy_of_real_data() {
  let copy = TempDir::new("data-modified");
  copy_dir_all(&conformance_dir(), copy.path());
  let target = copy.path().join("values/aid.json");
  let mut bytes = fs::read(&target).expect("写した values/aid.json を読める");
  bytes.push(b' ');
  fs::write(&target, bytes).expect("1 バイト足せる");

  let data = load(copy.path()).expect("内容が JSON として正しければ、照合が失敗しても読み込める");

  assert!(!data.manifest.passed());
  assert_eq!(
    data.manifest.problems,
    vec![ManifestProblem::Modified("values/aid.json".to_string())]
  );
  let original = load(&conformance_dir()).expect("実データを読める");
  assert!(original.manifest.passed(), "conformance/ 自体は変わっていない");
}

#[test]
fn test_load_leaves_manifest_file_untouched_when_verification_fails() {
  let copy = TempDir::new("data-manifest-untouched");
  copy_dir_all(&conformance_dir(), copy.path());
  let target = copy.path().join("values/aid.json");
  let mut bytes = fs::read(&target).expect("写した values/aid.json を読める");
  bytes.push(b' ');
  fs::write(&target, bytes).expect("1 バイト足せる");
  let manifest_before = fs::read(copy.path().join("manifest.json")).expect("manifest.json を読める");

  let data = load(copy.path()).expect("読める");

  assert!(!data.manifest.passed());
  let manifest_after = fs::read(copy.path().join("manifest.json")).expect("manifest.json を読める");
  assert_eq!(
    manifest_before, manifest_after,
    "照合の失敗を manifest の書き換えで解消しない"
  );
}

// ---------------------------------------------------------------------------
// generators の展開
// ---------------------------------------------------------------------------

/// `fixtures.events.e1.payload` に `payload` を持ち、`generators` を宣言した場面を作る。
fn case_with_generators(payload: Value, generators: Value) -> Value {
  json!({
    "fixtures": {"events": {"e1": {"payload": payload}}, "snapshots": {}},
    "steps": [{"op": ""}],
    "generators": generators
  })
}

fn is_generator_error(result: Result<(), DataError>) -> bool {
  matches!(result, Err(DataError::Generator { .. }))
}

#[test]
fn test_expand_generators_replaces_only_the_key_decoded_from_tilde_one() {
  let mut case = case_with_generators(
    json!({"a/b": "", "a": {"b": ""}}),
    json!([{"target": "/fixtures/events/e1/payload/a~1b", "character": "x", "byte_length": 3}]),
  );

  expand_generators(&mut case).expect("展開できる");

  let payload = &case["fixtures"]["events"]["e1"]["payload"];
  assert_eq!(payload["a/b"], "xxx");
  assert_eq!(payload["a"]["b"], "");
}

#[test]
fn test_expand_generators_resolves_slash_as_nesting_and_leaves_slash_key_alone() {
  let mut case = case_with_generators(
    json!({"a/b": "", "a": {"b": ""}}),
    json!([{"target": "/fixtures/events/e1/payload/a/b", "character": "x", "byte_length": 3}]),
  );

  expand_generators(&mut case).expect("展開できる");

  let payload = &case["fixtures"]["events"]["e1"]["payload"];
  assert_eq!(payload["a"]["b"], "xxx");
  assert_eq!(payload["a/b"], "");
}

#[test]
fn test_expand_generators_decodes_tilde_zero_one_as_key_tilde_one_not_slash() {
  let mut case = case_with_generators(
    json!({"~1": "", "/": ""}),
    json!([{"target": "/fixtures/events/e1/payload/~01", "character": "é", "byte_length": 4}]),
  );

  expand_generators(&mut case).expect("展開できる");

  let payload = &case["fixtures"]["events"]["e1"]["payload"];
  assert_eq!(payload["~1"], "éé");
  assert_eq!(payload["/"], "");
}

#[test]
fn test_expand_generators_refuses_non_empty_target_outside_fixtures_duplicate_target_and_indivisible_length() {
  // 空でない文字列
  let mut non_empty = case_with_generators(
    json!({"k": "y"}),
    json!([{"target": "/fixtures/events/e1/payload/k", "character": "x", "byte_length": 3}]),
  );
  assert!(is_generator_error(expand_generators(&mut non_empty)));
  assert_eq!(non_empty["fixtures"]["events"]["e1"]["payload"]["k"], "y");

  // fixtures の外
  let mut outside = case_with_generators(
    json!({"k": ""}),
    json!([{"target": "/steps/0/op", "character": "x", "byte_length": 3}]),
  );
  assert!(is_generator_error(expand_generators(&mut outside)));
  assert_eq!(outside["steps"][0]["op"], "");

  // 同じ target が 2 回
  let mut duplicate = case_with_generators(
    json!({"k": ""}),
    json!([
      {"target": "/fixtures/events/e1/payload/k", "character": "x", "byte_length": 3},
      {"target": "/fixtures/events/e1/payload/k", "character": "x", "byte_length": 3}
    ]),
  );
  assert!(is_generator_error(expand_generators(&mut duplicate)));

  // 文字の UTF-8 幅（2 バイト）で割り切れない総バイト数
  let mut indivisible = case_with_generators(
    json!({"k": ""}),
    json!([{"target": "/fixtures/events/e1/payload/k", "character": "é", "byte_length": 3}]),
  );
  assert!(is_generator_error(expand_generators(&mut indivisible)));
  assert_eq!(indivisible["fixtures"]["events"]["e1"]["payload"]["k"], "");
}

#[test]
fn test_expand_generators_expands_item_size_event_of_real_data_to_420000_bytes() {
  let data = load(&conformance_dir()).expect("実データを読める");
  let mut body = data
    .cases
    .iter()
    .find(|case| case.id == "dynamodb-item-size-event")
    .expect("dynamodb-item-size-event がある")
    .body
    .clone();

  expand_generators(&mut body).expect("展開できる");

  let payload = body["fixtures"]["events"]["e1"]["payload"]
    .as_str()
    .expect("payload は文字列");
  assert_eq!(payload.len(), 420_000);
  assert!(payload.bytes().all(|byte| byte == b'x'));
}

#[test]
fn test_expand_generators_fills_every_declared_target_of_real_data_to_its_byte_length() {
  let data = load(&conformance_dir()).expect("実データを読める");
  let with_generators: Vec<_> = data
    .cases
    .iter()
    .filter(|case| case.body.get("generators").is_some())
    .collect();
  assert_eq!(with_generators.len(), 5, "generators を持つケースは 5 件");

  for case in with_generators {
    let mut body = case.body.clone();
    let generators = body["generators"].as_array().expect("generators は配列").clone();
    for generator in &generators {
      let target = generator["target"].as_str().expect("target は文字列");
      assert_eq!(
        body.pointer(target),
        Some(&json!("")),
        "{}: {target} は展開前は空文字列",
        case.id
      );
    }

    expand_generators(&mut body).unwrap_or_else(|error| panic!("{}: {error}", case.id));

    for generator in &generators {
      let target = generator["target"].as_str().unwrap();
      let character = generator["character"].as_str().unwrap();
      let byte_length = generator["byte_length"].as_u64().unwrap() as usize;
      let expanded = body.pointer(target).and_then(Value::as_str).expect("展開後も文字列");
      assert_eq!(expanded.len(), byte_length, "{}: {target}", case.id);
      assert_eq!(
        expanded,
        character.repeat(byte_length / character.len()),
        "{}: {target}",
        case.id
      );
    }
  }
}

// ---------------------------------------------------------------------------
// 配布データの写しを書き換える補助
// ---------------------------------------------------------------------------

/// 配布されているデータの写しを、一時ディレクトリに作る。
fn real_copy(label: &str) -> TempDir {
  let copy = TempDir::new(label);
  copy_dir_all(&conformance_dir(), copy.path());
  copy
}

/// 写しの JSON のファイル `relative` を読み、`edit` で書き換えて書き戻す。数値は元の書き方のまま残る。
fn edit_json(root: &Path, relative: &str, edit: impl FnOnce(&mut Value)) {
  let path = root.join(relative);
  let text = fs::read_to_string(&path).unwrap_or_else(|error| panic!("{relative} を読める: {error}"));
  let mut value: Value = serde_json::from_str(&text).expect("写しは JSON");
  edit(&mut value);
  fs::write(&path, serde_json::to_string_pretty(&value).expect("書き戻せる")).expect("書き込める");
}

/// 文字列から JSON の値を作る。数値の書き方（`3.0`・`3e0`）をそのまま持つ。
fn from_text(text: &str) -> Value {
  serde_json::from_str(text).unwrap_or_else(|error| panic!("{text} は JSON: {error}"))
}

fn load_error(root: &Path) -> DataError {
  match load(root) {
    Ok(_) => panic!("読み込みは失敗するはず"),
    Err(error) => error,
  }
}

// ---------------------------------------------------------------------------
// スキーマでの検査（対応するスキーマ・展開の前・実行器のエラー）
// ---------------------------------------------------------------------------

/// 全ケースファイルの先頭のケースの `rules` を空にして、`minItems: 1` に違反させる。
fn empty_first_rules(value: &mut Value) {
  value["cases"][0]["rules"] = json!([]);
}

fn remove_notes(value: &mut Value) {
  value
    .as_object_mut()
    .expect("最上位はオブジェクト")
    .remove("notes")
    .expect("notes がある");
}

/// データのファイルを、スキーマに違反させる書き換えを表す。
type Violation = fn(&mut Value);

#[test]
fn test_load_checks_each_kind_of_data_file_with_its_own_schema() {
  let violations: [(&str, Violation); 5] = [
    ("coverage.json", remove_notes),
    ("values/aid.json", empty_first_rules),
    ("scenarios/core/write-read.json", empty_first_rules),
    ("dynamodb/write-errors.json", empty_first_rules),
    ("dynamodb/layout.json", empty_first_rules),
  ];

  for (index, (relative, violate)) in violations.into_iter().enumerate() {
    let copy = real_copy(&format!("data-schema-file-{index}"));
    edit_json(copy.path(), relative, violate);

    let error = load_error(copy.path());

    assert!(
      matches!(&error, DataError::Schema { path, message } if path == relative && !message.is_empty()),
      "{relative}: {error:?}"
    );
  }
}

#[test]
fn test_load_names_the_violating_location_in_the_schema_error() {
  let copy = real_copy("data-schema-location");
  edit_json(copy.path(), "values/aid.json", empty_first_rules);

  let error = load_error(copy.path());

  let text = error.to_string();
  assert!(text.contains("values/aid.json"), "{text}");
  assert!(text.contains("/cases/0/rules"), "{text}");
}

#[test]
fn test_load_rejects_synthetic_case_that_violates_the_values_schema() {
  let dir = TempDir::new("data-schema-synthetic");
  let violating = values_file("values", "1.0.0").replace("validateSeqNr", "notAnOperation");
  write_dataset(dir.path(), COVERAGE_JSON, &violating);

  let error = load_error(dir.path());

  assert!(
    matches!(&error, DataError::Schema { path, .. } if path == "values/synthetic.json"),
    "{error:?}"
  );
}

#[test]
fn test_load_checks_the_data_before_expanding_generators() {
  let copy = real_copy("data-schema-before-expand");
  edit_json(copy.path(), "dynamodb/write-errors.json", |value| {
    value["cases"][5]["generators"][0]["character"] = json!("xy");
  });

  let error = load_error(copy.path());

  assert!(
    matches!(&error, DataError::Schema { path, message }
      if path == "dynamodb/write-errors.json" && message.contains("character")),
    "展開の誤り（Generator）ではなく、スキーマの違反として止まる: {error:?}"
  );
}

#[test]
fn test_load_does_not_check_manifest_json_with_the_schema() {
  let copy = real_copy("data-schema-manifest");
  edit_json(copy.path(), "manifest.json", |value| {
    value["version"] = json!("9.9.9");
    value["extra"] = json!(true);
  });

  let data = load(copy.path()).expect("manifest.json のスキーマ違反は、読み込みの失敗ではなく照合の不一致");

  assert!(!data.manifest.passed());
}

#[test]
fn test_load_uses_the_schemas_shipped_with_the_data() {
  let copy = real_copy("data-schema-shipped");
  edit_json(copy.path(), "schema/coverage.schema.json", |schema| {
    schema["required"] = json!(["format", "version", "required_rules", "exclusions", "notes", "added"]);
  });

  let error = load_error(copy.path());

  assert!(
    matches!(&error, DataError::Schema { path, message } if path == "coverage.json" && message.contains("added")),
    "配布物の schema/ を使う: {error:?}"
  );
}

#[test]
fn test_load_fails_without_the_schema_directory() {
  let copy = real_copy("data-schema-absent");
  fs::remove_dir_all(copy.path().join("schema")).expect("schema/ を消せる");

  let error = load_error(copy.path());

  assert!(
    matches!(&error, DataError::Missing { path } if path.starts_with("schema/")),
    "スキーマを埋め込んで補わない: {error:?}"
  );
}

#[test]
fn test_load_fails_without_fetching_a_schema_that_is_not_registered() {
  let copy = real_copy("data-schema-unresolved");
  fs::remove_file(copy.path().join("schema/common.schema.json")).expect("common.schema.json を消せる");

  let error = load_error(copy.path());

  assert!(
    matches!(&error, DataError::Schema { message, .. }
      if message.contains("common.schema.json") && message.contains("failed to resolve")),
    "登録していない参照を取りに行かずに、参照を解決できない誤りで失敗する: {error:?}"
  );
}

#[test]
fn test_load_fails_when_a_schema_has_no_id() {
  let copy = real_copy("data-schema-no-id");
  edit_json(copy.path(), "schema/values.schema.json", |schema| {
    schema.as_object_mut().expect("オブジェクト").remove("$id");
  });

  let error = load_error(copy.path());

  assert!(
    matches!(&error, DataError::Invalid { path, .. } if path == "schema/values.schema.json"),
    "{error:?}"
  );
}

#[test]
fn test_load_accepts_integer_written_with_a_zero_fraction_in_the_schema_check() {
  let copy = real_copy("data-schema-integer-spelling");
  edit_json(copy.path(), "dynamodb/write-errors.json", |value| {
    value["cases"][5]["generators"][0]["byte_length"] = from_text("420000.0");
  });

  let data = load(copy.path()).expect("420000.0 は JSON Schema の整数");

  let mut body = data
    .cases
    .iter()
    .find(|case| case.id == "dynamodb-item-size-event")
    .expect("ケースがある")
    .body
    .clone();
  expand_generators(&mut body).expect("展開できる");
  assert_eq!(
    body["fixtures"]["events"]["e1"]["payload"]
      .as_str()
      .expect("文字列")
      .len(),
    420_000
  );
}

#[test]
fn test_load_rejects_byte_length_with_a_fraction_in_the_schema_check() {
  let copy = real_copy("data-schema-integer-fraction");
  edit_json(copy.path(), "dynamodb/write-errors.json", |value| {
    value["cases"][5]["generators"][0]["byte_length"] = from_text("2.5");
  });

  let error = load_error(copy.path());

  assert!(
    matches!(&error, DataError::Schema { path, .. } if path == "dynamodb/write-errors.json"),
    "{error:?}"
  );
}

// ---------------------------------------------------------------------------
// 整数の書き方（generators の byte_length）
// ---------------------------------------------------------------------------

/// `payload` を空文字列にして、`generators` を JSON の文字列から作った場面を作る。
fn case_with_generator_text(generator: &str) -> Value {
  from_text(&format!(
    r#"{{"fixtures":{{"events":{{"e1":{{"payload":""}}}},"snapshots":{{}}}},"generators":[{generator}]}}"#
  ))
}

#[test]
fn test_expand_generators_accepts_byte_length_written_as_a_whole_number_in_any_spelling() {
  for spelling in ["3", "3.0", "3e0", "3E0", "30e-1", "0.3e1"] {
    let mut case = case_with_generator_text(&format!(
      r#"{{"target":"/fixtures/events/e1/payload","character":"x","byte_length":{spelling}}}"#
    ));

    expand_generators(&mut case).unwrap_or_else(|error| panic!("{spelling}: {error}"));

    assert_eq!(case["fixtures"]["events"]["e1"]["payload"], "xxx", "{spelling}");
  }
}

#[test]
fn test_expand_generators_rejects_byte_length_with_a_non_zero_fraction() {
  for spelling in ["2.5", "1e-1", "0.5", "3.0000000000000000000001"] {
    let mut case = case_with_generator_text(&format!(
      r#"{{"target":"/fixtures/events/e1/payload","character":"x","byte_length":{spelling}}}"#
    ));

    let result = expand_generators(&mut case);

    assert!(is_generator_error(result), "{spelling}");
    assert_eq!(case["fixtures"]["events"]["e1"]["payload"], "", "{spelling}");
  }
}

#[test]
fn test_expand_generators_rejects_byte_length_below_one_or_not_a_number() {
  for spelling in ["0", "0.0", "-3", "-3.0", r#""3""#, "null", "true"] {
    let mut case = case_with_generator_text(&format!(
      r#"{{"target":"/fixtures/events/e1/payload","character":"x","byte_length":{spelling}}}"#
    ));

    assert!(is_generator_error(expand_generators(&mut case)), "{spelling}");
  }
}

// ---------------------------------------------------------------------------
// U+FFFD を有効な文字として受け入れる（generators の character）
// ---------------------------------------------------------------------------

#[test]
fn test_parse_strict_json_accepts_replacement_character_in_both_spellings() {
  let raw = parse_strict_json("{\"c\":\"\u{fffd}\"}".as_bytes()).expect("生の U+FFFD は有効な UTF-8");
  let escaped = parse_strict_json(br#"{"c":"\ufffd"}"#).expect("エスケープした U+FFFD も有効");

  assert_eq!(raw["c"], "\u{fffd}");
  assert_eq!(escaped["c"], "\u{fffd}");
}

#[test]
fn test_expand_generators_accepts_replacement_character_as_the_character() {
  for text in ["\u{fffd}", r"\ufffd"] {
    let mut case = case_with_generator_text(&format!(
      r#"{{"target":"/fixtures/events/e1/payload","character":"{text}","byte_length":6}}"#
    ));

    expand_generators(&mut case).unwrap_or_else(|error| panic!("{text}: {error}"));

    assert_eq!(case["fixtures"]["events"]["e1"]["payload"], "\u{fffd}\u{fffd}");
  }
}

#[test]
fn test_expand_generators_rejects_replacement_character_byte_length_not_divisible_by_its_width() {
  let mut case =
    case_with_generator_text(r#"{"target":"/fixtures/events/e1/payload","character":"\ufffd","byte_length":4}"#);

  assert!(is_generator_error(expand_generators(&mut case)));
}

#[test]
fn test_load_and_expand_accept_replacement_character_in_the_real_data_generator() {
  let copy = real_copy("data-fffd-real");
  edit_json(copy.path(), "dynamodb/write-errors.json", |value| {
    value["cases"][5]["generators"][0]["character"] = json!("\u{fffd}");
  });

  let data = load(copy.path()).expect("U+FFFD 1 文字はスキーマの検査を通る");

  let mut body = data
    .cases
    .iter()
    .find(|case| case.id == "dynamodb-item-size-event")
    .expect("ケースがある")
    .body
    .clone();
  expand_generators(&mut body).expect("展開できる");
  let payload = body["fixtures"]["events"]["e1"]["payload"].as_str().expect("文字列");
  assert_eq!(payload.len(), 420_000);
  assert_eq!(payload.chars().count(), 140_000);
  assert!(payload.chars().all(|character| character == '\u{fffd}'));
}

// ---------------------------------------------------------------------------
// 読んだ manifest の版と、期待する版との比較
// ---------------------------------------------------------------------------

#[test]
fn test_load_exposes_the_version_that_manifest_json_actually_declares() {
  let copy = real_copy("data-manifest-other-version");
  edit_json(copy.path(), "manifest.json", |value| value["version"] = json!("9.9.9"));

  let data = load(copy.path()).expect("版の不一致は読み込みの失敗ではない");

  assert_eq!(data.manifest_version.as_deref(), Some("9.9.9"), "読んだ版を持つ");
  assert!(
    matches!(data.manifest.problems.as_slice(), [ManifestProblem::Version(found)] if found.contains("9.9.9")),
    "期待する版との比較は別に行う: {:?}",
    data.manifest.problems
  );
}

#[test]
fn test_load_has_no_manifest_version_when_manifest_json_version_is_not_a_string() {
  let copy = real_copy("data-manifest-numeric-version");
  edit_json(copy.path(), "manifest.json", |value| value["version"] = json!(1));

  let data = load(copy.path()).expect("読める");

  assert_eq!(data.manifest_version, None);
  assert!(matches!(
    data.manifest.problems.as_slice(),
    [ManifestProblem::Version(_)]
  ));
}

// ---------------------------------------------------------------------------
// manifest の未知のフィールド
// ---------------------------------------------------------------------------

#[test]
fn test_verify_manifest_reports_unknown_top_level_field() {
  let mut manifest = manifest_value("1.0.0", &[("a.json", SHA256_OF_ABC)]);
  manifest["extra"] = json!(true);
  let inventory = [entry("a.json", b"abc")];

  let verification = verify_manifest(&manifest, &inventory);

  assert_eq!(
    verification.problems,
    vec![ManifestProblem::UnknownField("extra".to_string())]
  );
  assert!(!verification.passed());
}

#[test]
fn test_verify_manifest_reports_unknown_field_of_a_file_entry_and_keeps_verifying_it() {
  let mut manifest = manifest_value("1.0.0", &[("a.json", SHA256_OF_ABC), ("b.json", SHA256_OF_EMPTY)]);
  manifest["files"][1]["note"] = json!("x");
  let inventory = [entry("a.json", b"abc"), entry("b.json", b"")];

  let verification = verify_manifest(&manifest, &inventory);

  assert_eq!(
    verification.problems,
    vec![ManifestProblem::UnknownField("files[1].note".to_string())],
    "path と sha256 が正しければ、欠落や一覧にないファイルとして報告しない"
  );
}

#[test]
fn test_verify_manifest_still_reports_a_modified_file_whose_entry_has_an_unknown_field() {
  let mut manifest = manifest_value("1.0.0", &[("a.json", SHA256_OF_EMPTY)]);
  manifest["files"][0]["note"] = json!("x");
  let inventory = [entry("a.json", b"abc")];

  let verification = verify_manifest(&manifest, &inventory);

  assert_eq!(
    verification.problems,
    vec![
      ManifestProblem::UnknownField("files[0].note".to_string()),
      ManifestProblem::Modified("a.json".to_string())
    ]
  );
}

#[test]
fn test_verify_manifest_reports_every_unknown_field() {
  let mut manifest = manifest_value("1.0.0", &[("a.json", SHA256_OF_ABC)]);
  manifest["alpha"] = json!(1);
  manifest["beta"] = json!(2);
  manifest["files"][0]["gamma"] = json!(3);
  let inventory = [entry("a.json", b"abc")];

  let verification = verify_manifest(&manifest, &inventory);

  let mut fields: Vec<String> = verification
    .problems
    .iter()
    .map(|problem| match problem {
      ManifestProblem::UnknownField(field) => field.clone(),
      other => panic!("未知のフィールド以外の問題: {other:?}"),
    })
    .collect();
  fields.sort();
  assert_eq!(fields, vec!["alpha", "beta", "files[0].gamma"]);
}

#[test]
fn test_verify_manifest_describes_an_unknown_field_in_its_message() {
  let problem = ManifestProblem::UnknownField("files[0].note".to_string());

  assert!(problem.to_string().contains("files[0].note"));
}

#[test]
fn test_load_reports_unknown_manifest_fields_as_mismatch_not_as_a_load_failure() {
  let copy = real_copy("data-manifest-unknown-fields");
  edit_json(copy.path(), "manifest.json", |value| {
    value["extra"] = json!(true);
    value["files"][0]["note"] = json!("x");
  });

  let data = load(copy.path()).expect("未知のフィールドは読み込みの失敗ではない");

  let mut problems = data.manifest.problems.clone();
  problems.sort_by_key(ToString::to_string);
  assert_eq!(
    problems,
    vec![
      ManifestProblem::UnknownField("extra".to_string()),
      ManifestProblem::UnknownField("files[0].note".to_string())
    ]
  );
}
