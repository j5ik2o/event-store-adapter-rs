//! 配布物の `schema/` の JSON Schema（draft 2020-12）による、データのファイルの検査。
//!
//! スキーマは配布物の `schema/` にあるものを `$id` で手元に登録して使い、参照をネットワークやファイルで
//! 取りに行かない。このモジュールはデータの読み込み（`data`）に依存しない。

use std::collections::HashMap;

use jsonschema::{Draft, Validator};
use serde_json::Value;

/// 1 回の検査で報告する違反の最大数。
const MAX_REPORTED_VIOLATIONS: usize = 5;

/// スキーマの登録と組み立ての誤りを表す。`path` は、誤りのあるスキーマの `schema/` の下の相対パス。
#[derive(Debug, thiserror::Error)]
pub enum SchemaError {
  #[error("{path}: `$id` が文字列ではない")]
  MissingId { path: String },
  #[error("{path}: スキーマのファイルがない")]
  MissingSchema { path: String },
  #[error("{path}: スキーマを組み立てられない: {message}")]
  Build { path: String, message: String },
}

/// 登録済みのスキーマから組み立てた、形式（`format`）ごとの検査器を表す。
pub struct SchemaSet {
  validators: HashMap<String, Validator>,
}

impl SchemaSet {
  /// `documents`（`schema/` の下の相対パスと、読み込んだスキーマの組）を `$id` で全部登録し、
  /// `formats` の各形式について `{format}.schema.json` を draft 2020-12 で組み立てる。
  pub fn new(documents: &[(String, Value)], formats: &[&str]) -> Result<SchemaSet, SchemaError> {
    let mut options = jsonschema::options();
    options.with_draft(Draft::Draft202012);
    for (path, document) in documents {
      let id = document
        .get("$id")
        .and_then(Value::as_str)
        .ok_or_else(|| SchemaError::MissingId { path: path.clone() })?;
      options.with_document(id.to_string(), document.clone());
    }
    let mut validators = HashMap::new();
    for format in formats {
      let path = format!("{format}.schema.json");
      let (_, document) = documents
        .iter()
        .find(|(candidate, _)| *candidate == path)
        .ok_or_else(|| SchemaError::MissingSchema { path: path.clone() })?;
      let validator = options.build(document).map_err(|error| SchemaError::Build {
        path: path.clone(),
        message: error.to_string(),
      })?;
      validators.insert((*format).to_string(), validator);
    }
    Ok(SchemaSet { validators })
  }

  /// `value` を形式 `format` のスキーマで検査する。違反があれば、重複を除いた先頭の数件を `位置: 内容` に
  /// して `; ` でつないだ文字列を返す。参照を解決できないなど、検査を終えられない誤りも同じく返す。
  /// 組み立てていない形式は違反として扱う。
  pub fn validate(&self, format: &str, value: &Value) -> Result<(), String> {
    let Some(validator) = self.validators.get(format) else {
      return Err(format!("形式 {format} のスキーマを組み立てていない"));
    };
    match validator.validate(value) {
      Ok(()) => Ok(()),
      Err(errors) => {
        let mut messages: Vec<String> = Vec::new();
        for error in errors {
          let message = format!("{}: {error}", error.instance_path);
          if !messages.contains(&message) {
            messages.push(message);
          }
          if messages.len() == MAX_REPORTED_VIOLATIONS {
            break;
          }
        }
        Err(messages.join("; "))
      }
    }
  }
}

/// 配布物の中の相対パス `path` のデータのファイルを検査する、スキーマの形式を返す。
///
/// `coverage.json` は coverage、`dynamodb/layout.json` は layout、`values/` の下は values、
/// `scenarios/` の下と `dynamodb/` の下（layout 以外）は scenarios。それ以外（`manifest.json` や、
/// ケースを持たないファイル）は `None` を返す。
pub fn schema_format(path: &str) -> Option<&'static str> {
  if !path.ends_with(".json") {
    return None;
  }
  match path {
    "coverage.json" => Some("coverage"),
    "dynamodb/layout.json" => Some("layout"),
    _ if path.starts_with("values/") => Some("values"),
    _ if path.starts_with("scenarios/") || path.starts_with("dynamodb/") => Some("scenarios"),
    _ => None,
  }
}
