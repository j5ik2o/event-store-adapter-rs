//! スキーマの登録・組み立て・検査と、データのファイルとスキーマの対応の試験。

use std::fs;
use std::path::{Path, PathBuf};

use event_store_adapter_conformance_rs::schema::{schema_format, SchemaError, SchemaSet};
use serde_json::{json, Value};

fn conformance_dir() -> PathBuf {
  Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance")
}

const FORMATS: [&str; 4] = ["coverage", "values", "scenarios", "layout"];

/// 配布されている `schema/` の全文書を、`schema/` の下の相対パスと組にして読む。
fn shipped_documents() -> Vec<(String, Value)> {
  let mut documents: Vec<(String, Value)> = fs::read_dir(conformance_dir().join("schema"))
    .expect("schema/ を読める")
    .map(|entry| {
      let path = entry.expect("エントリを読める").path();
      let name = path.file_name().expect("名前がある").to_string_lossy().into_owned();
      let text = fs::read_to_string(&path).expect("スキーマを読める");
      (name, serde_json::from_str(&text).expect("スキーマは JSON"))
    })
    .collect();
  documents.sort_by(|left, right| left.0.cmp(&right.0));
  documents
}

fn shipped_schemas() -> SchemaSet {
  SchemaSet::new(&shipped_documents(), &FORMATS).expect("配布のスキーマを組み立てられる")
}

fn valid_coverage() -> Value {
  json!({"format": "coverage", "version": "1.0.0", "required_rules": ["T-1"], "exclusions": [], "notes": []})
}

// ---------------------------------------------------------------------------
// データのファイルとスキーマの対応
// ---------------------------------------------------------------------------

#[test]
fn test_schema_format_maps_each_kind_of_data_file_to_its_schema() {
  let expected = [
    ("coverage.json", Some("coverage")),
    ("dynamodb/layout.json", Some("layout")),
    ("values/aid.json", Some("values")),
    ("scenarios/core/write-read.json", Some("scenarios")),
    ("dynamodb/configuration.json", Some("scenarios")),
    ("dynamodb/write-errors.json", Some("scenarios")),
    ("manifest.json", None),
    ("schema/coverage.schema.json", None),
    (".gitattributes", None),
    ("values/notes.md", None),
    ("COVERAGE.md", None),
  ];

  for (path, format) in expected {
    assert_eq!(schema_format(path), format, "{path}");
  }
}

// ---------------------------------------------------------------------------
// 登録と組み立て
// ---------------------------------------------------------------------------

#[test]
fn test_schema_set_builds_every_shipped_format_and_accepts_valid_data() {
  let schemas = shipped_schemas();

  assert_eq!(schemas.validate("coverage", &valid_coverage()), Ok(()));
}

#[test]
fn test_schema_set_reports_the_violating_location() {
  let schemas = shipped_schemas();
  let mut coverage = valid_coverage();
  coverage["required_rules"] = json!([]);

  let message = schemas.validate("coverage", &coverage).expect_err("minItems の違反");

  assert!(message.contains("/required_rules"), "{message}");
}

#[test]
fn test_schema_set_reports_at_most_five_distinct_violations() {
  let schemas = shipped_schemas();
  let coverage = json!({
    "format": "other", "version": "9", "required_rules": [], "exclusions": 1, "notes": 2, "a": 1, "b": 2, "c": 3
  });

  let message = schemas.validate("coverage", &coverage).expect_err("違反だらけ");

  let reported: Vec<&str> = message.split("; ").collect();
  assert!(reported.len() <= 5, "{message}");
  let mut distinct = reported.clone();
  distinct.sort_unstable();
  distinct.dedup();
  assert_eq!(distinct.len(), reported.len(), "同じ内容を繰り返さない: {message}");
}

#[test]
fn test_schema_set_rejects_a_format_it_did_not_build() {
  let schemas = SchemaSet::new(&shipped_documents(), &["coverage"]).expect("組み立てられる");

  assert!(schemas.validate("values", &json!({})).is_err());
}

#[test]
fn test_schema_set_requires_a_schema_file_for_each_format() {
  let documents: Vec<(String, Value)> = shipped_documents()
    .into_iter()
    .filter(|(path, _)| path != "layout.schema.json")
    .collect();

  let result = SchemaSet::new(&documents, &FORMATS);

  assert!(
    matches!(result, Err(SchemaError::MissingSchema { ref path }) if path == "layout.schema.json"),
    "{:?}",
    result.err()
  );
}

#[test]
fn test_schema_set_requires_every_document_to_have_an_id() {
  let mut documents = shipped_documents();
  documents.push(("extra.schema.json".to_string(), json!({"type": "object"})));

  let result = SchemaSet::new(&documents, &FORMATS);

  assert!(
    matches!(result, Err(SchemaError::MissingId { ref path }) if path == "extra.schema.json"),
    "{:?}",
    result.err()
  );
}

#[test]
fn test_schema_set_reports_a_schema_that_cannot_be_built() {
  let mut documents = shipped_documents();
  for (path, document) in &mut documents {
    if path == "coverage.schema.json" {
      document["type"] = json!(5);
    }
  }

  let result = SchemaSet::new(&documents, &FORMATS);

  assert!(
    matches!(result, Err(SchemaError::Build { ref path, .. }) if path == "coverage.schema.json"),
    "{:?}",
    result.err()
  );
}

// ---------------------------------------------------------------------------
// JSON Schema の整数（小数部が 0 の書き方を受け入れる）
// ---------------------------------------------------------------------------

#[test]
fn test_schema_set_treats_a_whole_number_with_a_zero_fraction_as_an_integer() {
  let schemas = shipped_schemas();
  let values = |seq_nr: &str| {
    serde_json::from_str::<Value>(&format!(
      r#"{{"format":"values","version":"1.0.0","cases":[{{
        "id":"synthetic","rules":["T-1"],"description":"d","operation":"validateSeqNr",
        "input":{{"seq_nr":{seq_nr},"context":"value"}},"expect":{{"value":1}}
      }}]}}"#
    ))
    .expect("JSON")
  };

  for spelling in ["1", "1.0", "1e0", "10e-1"] {
    assert_eq!(schemas.validate("values", &values(spelling)), Ok(()), "{spelling}");
  }
  assert!(schemas.validate("values", &values("1.5")).is_err());
}
