//! 適合テストデータの読み込み・`manifest` の照合・generators の展開の試験。

mod support;

use std::collections::HashSet;
use std::fs;
use std::path::Path;

use event_store_adapter_conformance_rs::data::{
  expand_generators, inventory, load, parse_strict_json, sha256_hex, verify_manifest, CaseKind, DataError,
  InventoryEntry, ManifestProblem,
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

#[test]
fn test_load_verifies_real_manifest_and_exposes_its_fixed_sha256() {
  let data = load(&conformance_dir()).expect("実データを読める");

  assert!(data.manifest.passed(), "problems: {:?}", data.manifest.problems);
  assert_eq!(data.manifest_files, 22);
  assert_eq!(data.manifest_sha256, FIXED_MANIFEST_SHA256);
  assert_eq!(data.version, "1.0.0");
}

// ---------------------------------------------------------------------------
// format と version の確認（合成した一時ディレクトリ）
// ---------------------------------------------------------------------------

const MANIFEST_JSON: &str = r#"{"format":"manifest","version":"1.0.0","files":[]}"#;
const COVERAGE_JSON: &str = r#"{"format":"coverage","version":"1.0.0","required_rules":["T-1"],"exclusions":[]}"#;

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
