//! 適合テストデータの読み込み、`manifest` の照合、generators の展開。

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs;
use std::path::Path;

use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::number::to_integer;
use crate::schema::{schema_format, SchemaError, SchemaSet};

/// 実行器が読めるデータの版を表す。
pub const DATA_VERSION: &str = "1.0.0";

/// JSON として読めない入力の理由を表す。
#[derive(Debug, thiserror::Error)]
pub enum JsonError {
  #[error("UTF-8 として読めない: {0}")]
  NotUtf8(#[from] std::str::Utf8Error),
  #[error("JSON として読めない: {0}")]
  Syntax(#[from] serde_json::Error),
}

/// データの読み込みと展開の誤りを表す。
#[derive(Debug, thiserror::Error)]
pub enum DataError {
  #[error("{path}: 入出力の誤り: {source}")]
  Io { path: String, source: std::io::Error },
  #[error("{path}: シンボリックリンクは配布できない")]
  Symlink { path: String },
  /// 相対パスに UTF-8 でない部分がある。`path` は表示用で、不正なバイトは置換文字になる。
  #[error("{path}: パスが UTF-8 ではない")]
  NotUtf8Path { path: String },
  #[error("{path}: ファイルがない")]
  Missing { path: String },
  #[error("{path}: {source}")]
  Json { path: String, source: JsonError },
  #[error("{path}: {message}")]
  Invalid { path: String, message: String },
  #[error("{path}: スキーマでの検査に失敗: {message}")]
  Schema { path: String, message: String },
  #[error("ケースの ID が重複: {id}")]
  DuplicateCaseId { id: String },
  #[error("generators の {target}: {message}")]
  Generator { target: String, message: String },
}

/// 重複キーだけを検査する読み取り先。値は捨てる。
struct DuplicateKeyCheck;

impl<'de> Deserialize<'de> for DuplicateKeyCheck {
  fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
    struct CheckVisitor;

    impl<'de> Visitor<'de> for CheckVisitor {
      type Value = DuplicateKeyCheck;

      fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("JSON の値")
      }

      fn visit_bool<E>(self, _: bool) -> Result<DuplicateKeyCheck, E> {
        Ok(DuplicateKeyCheck)
      }

      fn visit_i64<E>(self, _: i64) -> Result<DuplicateKeyCheck, E> {
        Ok(DuplicateKeyCheck)
      }

      fn visit_u64<E>(self, _: u64) -> Result<DuplicateKeyCheck, E> {
        Ok(DuplicateKeyCheck)
      }

      fn visit_f64<E>(self, _: f64) -> Result<DuplicateKeyCheck, E> {
        Ok(DuplicateKeyCheck)
      }

      fn visit_str<E>(self, _: &str) -> Result<DuplicateKeyCheck, E> {
        Ok(DuplicateKeyCheck)
      }

      fn visit_unit<E>(self) -> Result<DuplicateKeyCheck, E> {
        Ok(DuplicateKeyCheck)
      }

      fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<DuplicateKeyCheck, A::Error> {
        while seq.next_element::<DuplicateKeyCheck>()?.is_some() {}
        Ok(DuplicateKeyCheck)
      }

      fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<DuplicateKeyCheck, A::Error> {
        let mut seen = HashSet::new();
        while let Some(key) = map.next_key::<String>()? {
          if !seen.insert(key.clone()) {
            return Err(de::Error::custom(format!("JSON キーが重複: {key}")));
          }
          map.next_value::<DuplicateKeyCheck>()?;
        }
        Ok(DuplicateKeyCheck)
      }
    }

    deserializer.deserialize_any(CheckVisitor)
  }
}

/// 重複キー・NaN・Infinity・UTF-8 でない入力を拒否して、JSON の値を読む。
///
/// 数値は `arbitrary_precision` で元の桁のまま持つ。文字列は書き換えない。
pub fn parse_strict_json(bytes: &[u8]) -> Result<Value, JsonError> {
  let text = std::str::from_utf8(bytes)?;
  serde_json::from_str::<DuplicateKeyCheck>(text)?;
  Ok(serde_json::from_str::<Value>(text)?)
}

/// 配布のファイル 1 つの相対パスとバイト列を表す。
pub struct InventoryEntry {
  pub path: String,
  pub bytes: Vec<u8>,
}

fn io_error(path: &Path, source: std::io::Error) -> DataError {
  DataError::Io {
    path: path.display().to_string(),
    source,
  }
}

/// `root` からの相対パスを、`/` でつないだ文字列にする。
///
/// UTF-8 でない部分があれば `DataError::NotUtf8Path` を返す。置換文字に潰すと、別々のファイル名が同じ
/// パスになり、`manifest` の 1 つの項目が複数のファイルを覆えてしまうので、潰さない。
pub fn inventory_path(relative: &Path) -> Result<String, DataError> {
  let mut parts = Vec::new();
  for component in relative.components() {
    let part = component.as_os_str().to_str().ok_or_else(|| DataError::NotUtf8Path {
      path: relative.display().to_string(),
    })?;
    parts.push(part);
  }
  Ok(parts.join("/"))
}

fn collect_files(root: &Path, directory: &Path, entries: &mut Vec<InventoryEntry>) -> Result<(), DataError> {
  for entry in fs::read_dir(directory).map_err(|source| io_error(directory, source))? {
    let path = entry.map_err(|source| io_error(directory, source))?.path();
    let metadata = fs::symlink_metadata(&path).map_err(|source| io_error(&path, source))?;
    if metadata.file_type().is_symlink() {
      return Err(DataError::Symlink {
        path: path.display().to_string(),
      });
    }
    if metadata.is_dir() {
      collect_files(root, &path, entries)?;
    } else if metadata.is_file() {
      let relative = inventory_path(path.strip_prefix(root).expect("走査した path は root の下にある"))?;
      if relative == "manifest.json" {
        continue;
      }
      let bytes = fs::read(&path).map_err(|source| io_error(&path, source))?;
      entries.push(InventoryEntry { path: relative, bytes });
    }
  }
  Ok(())
}

/// `root` の下の全ファイル（`manifest.json` を除く）を、相対パスの順に読む。
///
/// ドットファイルを含む。シンボリックリンクと、UTF-8 でない相対パス（`DataError::NotUtf8Path`）は誤りにする。
pub fn inventory(root: &Path) -> Result<Vec<InventoryEntry>, DataError> {
  let mut entries = Vec::new();
  collect_files(root, root, &mut entries)?;
  entries.sort_by(|left, right| left.path.cmp(&right.path));
  Ok(entries)
}

/// バイト列の SHA-256 を、小文字の 16 進文字列で返す。
pub fn sha256_hex(bytes: &[u8]) -> String {
  Sha256::digest(bytes).iter().map(|byte| format!("{byte:02x}")).collect()
}

/// `manifest` の照合で見つかった問題を表す。文字列は、問題のあるファイルの相対パスなどを持つ。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestProblem {
  Format(String),
  Version(String),
  Malformed(String),
  Duplicate(String),
  Missing(String),
  Unlisted(String),
  Modified(String),
  /// `manifest.json` の最上位か、`files` の項目に、スキーマにないフィールドがある。
  UnknownField(String),
}

impl fmt::Display for ManifestProblem {
  fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      ManifestProblem::Format(found) => write!(formatter, "format が manifest ではない: {found}"),
      ManifestProblem::Version(found) => write!(formatter, "version が {DATA_VERSION} ではない: {found}"),
      ManifestProblem::Malformed(detail) => write!(formatter, "files の形が不正: {detail}"),
      ManifestProblem::Duplicate(path) => write!(formatter, "一覧に重複がある: {path}"),
      ManifestProblem::Missing(path) => write!(formatter, "一覧にあるファイルがない: {path}"),
      ManifestProblem::Unlisted(path) => write!(formatter, "一覧にないファイルがある: {path}"),
      ManifestProblem::Modified(path) => write!(formatter, "SHA-256 が一致しない（改変）: {path}"),
      ManifestProblem::UnknownField(field) => write!(formatter, "未知のフィールドがある: {field}"),
    }
  }
}

/// `manifest` の照合結果を表す。問題がなければ成功。
pub struct ManifestVerification {
  pub problems: Vec<ManifestProblem>,
}

impl ManifestVerification {
  /// 問題が 1 つもなければ真を返す。
  pub fn passed(&self) -> bool {
    self.problems.is_empty()
  }
}

/// `manifest.json` の最上位に置けるフィールド。
const MANIFEST_FIELDS: [&str; 3] = ["format", "version", "files"];
/// `manifest.json` の `files` の項目に置けるフィールド。
const MANIFEST_ENTRY_FIELDS: [&str; 2] = ["path", "sha256"];

fn describe(value: Option<&Value>) -> String {
  value.map(Value::to_string).unwrap_or_else(|| "なし".to_string())
}

/// `files` の項目 1 つが正しい形で載っているかを判定し、正しければ `(path, sha256)` を返す。
///
/// `path` と `sha256` がどちらも文字列なら正しい形。`manifest` の照合（`verify_manifest`）と、スキーマ・
/// ケースとして読むファイルの決定（`listed_paths`）が、この 1 つの判定を共有する。
fn listed_entry(file: &Value) -> Option<(&str, &str)> {
  Some((
    file.get("path").and_then(Value::as_str)?,
    file.get("sha256").and_then(Value::as_str)?,
  ))
}

/// `manifest` の `files` に正しい形で載ったファイルの相対パスの集合を返す。
///
/// `files` が配列でなければ空。形の誤った項目は含めない（`listed_entry`）。スキーマとケースは、この集合の
/// ファイルだけを読む。載っていないファイルと形の誤った項目は、`verify_manifest` が問題として報告する。
fn listed_paths(manifest: &Value) -> HashSet<&str> {
  manifest
    .get("files")
    .and_then(Value::as_array)
    .map(|files| {
      files
        .iter()
        .filter_map(|file| listed_entry(file).map(|(path, _)| path))
        .collect()
    })
    .unwrap_or_default()
}

/// `manifest` の版、ファイル集合、ファイルごとの SHA-256 を、実ファイルと照合する。
///
/// `manifest` は作り直さない。ファイルの並び順は検査しない。最上位と各項目の未知のフィールド
/// （スキーマの `additionalProperties: false`）も問題として報告する。見つけた問題をすべて集める。
pub fn verify_manifest(manifest: &Value, inventory: &[InventoryEntry]) -> ManifestVerification {
  let mut problems = Vec::new();
  if manifest.get("format") != Some(&Value::String("manifest".to_string())) {
    problems.push(ManifestProblem::Format(describe(manifest.get("format"))));
  }
  if manifest.get("version") != Some(&Value::String(DATA_VERSION.to_string())) {
    problems.push(ManifestProblem::Version(describe(manifest.get("version"))));
  }
  if let Some(object) = manifest.as_object() {
    for key in object.keys().filter(|key| !MANIFEST_FIELDS.contains(&key.as_str())) {
      problems.push(ManifestProblem::UnknownField(key.clone()));
    }
  }

  let mut listed: Vec<(&str, &str)> = Vec::new();
  match manifest.get("files").and_then(Value::as_array) {
    None => problems.push(ManifestProblem::Malformed("files が配列ではない".to_string())),
    Some(files) => {
      for (index, file) in files.iter().enumerate() {
        if let Some(object) = file.as_object() {
          for key in object
            .keys()
            .filter(|key| !MANIFEST_ENTRY_FIELDS.contains(&key.as_str()))
          {
            problems.push(ManifestProblem::UnknownField(format!("files[{index}].{key}")));
          }
        }
        match listed_entry(file) {
          Some(entry) => listed.push(entry),
          None => problems.push(ManifestProblem::Malformed(file.to_string())),
        }
      }
    }
  }

  let actual: HashMap<&str, &InventoryEntry> = inventory.iter().map(|entry| (entry.path.as_str(), entry)).collect();
  let mut seen: HashSet<&str> = HashSet::new();
  let mut reported_duplicates: HashSet<&str> = HashSet::new();
  for (path, sha256) in &listed {
    if !seen.insert(path) {
      if reported_duplicates.insert(path) {
        problems.push(ManifestProblem::Duplicate((*path).to_string()));
      }
      continue;
    }
    match actual.get(path) {
      None => problems.push(ManifestProblem::Missing((*path).to_string())),
      Some(entry) if sha256_hex(&entry.bytes) != *sha256 => {
        problems.push(ManifestProblem::Modified((*path).to_string()))
      }
      Some(_) => {}
    }
  }
  for entry in inventory {
    if !seen.contains(entry.path.as_str()) {
      problems.push(ManifestProblem::Unlisted(entry.path.clone()));
    }
  }
  ManifestVerification { problems }
}

/// ケースを持つファイルの種類を表す。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaseKind {
  ValueTable,
  Scenario,
  Layout,
}

/// 適合テストデータのケース 1 つを表す。`body` は読み込んだままの JSON。
pub struct Case {
  pub id: String,
  pub rules: Vec<String>,
  pub kind: CaseKind,
  pub file: String,
  pub body: Value,
}

/// `coverage.json` の、規則の単位の除外 1 つを表す。
pub struct CoverageExclusion {
  pub rule: String,
  pub status: String,
  pub reason: String,
}

/// `coverage.json` の内容を表す。
pub struct Coverage {
  pub required_rules: Vec<String>,
  pub exclusions: Vec<CoverageExclusion>,
}

/// 読み込んだ適合テストデータの全体を表す。
pub struct DataSet {
  /// 実際に読んだ `manifest.json` の `version`。文字列でなければ `None`。期待する版との比較は
  /// `manifest` の照合が別に行う。
  pub manifest_version: Option<String>,
  pub manifest_files: usize,
  pub manifest_sha256: String,
  pub manifest: ManifestVerification,
  pub coverage: Coverage,
  pub cases: Vec<Case>,
}

fn invalid(path: &str, message: impl Into<String>) -> DataError {
  DataError::Invalid {
    path: path.to_string(),
    message: message.into(),
  }
}

fn parse_file(path: &str, bytes: &[u8]) -> Result<Value, DataError> {
  parse_strict_json(bytes).map_err(|source| DataError::Json {
    path: path.to_string(),
    source,
  })
}

/// 最上位の `format` と `version` を確かめ、`format` を返す。
fn check_header(path: &str, value: &Value, formats: &[&str]) -> Result<String, DataError> {
  let format = value.get("format").and_then(Value::as_str).unwrap_or_default();
  if !formats.contains(&format) {
    return Err(invalid(
      path,
      format!("format が {formats:?} のどれでもない: {format:?}"),
    ));
  }
  let version = value.get("version").and_then(Value::as_str).unwrap_or_default();
  if version != DATA_VERSION {
    return Err(invalid(
      path,
      format!("version が {DATA_VERSION} ではない: {version:?}"),
    ));
  }
  Ok(format.to_string())
}

fn string_list(path: &str, value: &Value, what: &str) -> Result<Vec<String>, DataError> {
  let array = value
    .as_array()
    .ok_or_else(|| invalid(path, format!("{what} が配列ではない")))?;
  array
    .iter()
    .map(|item| {
      item
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| invalid(path, format!("{what} の要素が文字列ではない")))
    })
    .collect()
}

fn read_exclusions(path: &str, value: &Value) -> Result<Vec<CoverageExclusion>, DataError> {
  let array = value
    .as_array()
    .ok_or_else(|| invalid(path, "exclusions が配列ではない"))?;
  array
    .iter()
    .map(|exclusion| {
      let field = |name: &str| {
        exclusion
          .get(name)
          .and_then(Value::as_str)
          .map(str::to_string)
          .ok_or_else(|| invalid(path, format!("exclusions の {name} が文字列ではない")))
      };
      Ok(CoverageExclusion {
        rule: field("rule")?,
        status: field("status")?,
        reason: field("reason")?,
      })
    })
    .collect()
}

/// 配布物の `schema/` のスキーマのうち、`listed` に載ったものを読んで登録し、データのファイルの形式ごとに
/// 組み立てる。
///
/// 載っていないファイルは解析せず登録しない（`manifest` の照合が `Unlisted` として報告する）。必須のスキーマが
/// 載っていなければ、スキーマのファイルがないときと同じく `DataError::Missing` にする。
fn load_schemas(files: &[InventoryEntry], listed: &HashSet<&str>) -> Result<SchemaSet, DataError> {
  let mut documents = Vec::new();
  for entry in files {
    if let Some(relative) = entry
      .path
      .strip_prefix("schema/")
      .filter(|_| entry.path.ends_with(".json") && listed.contains(entry.path.as_str()))
    {
      documents.push((relative.to_string(), parse_file(&entry.path, &entry.bytes)?));
    }
  }
  SchemaSet::new(&documents, &["coverage", "values", "scenarios", "layout"]).map_err(|error| match error {
    SchemaError::MissingSchema { path } => DataError::Missing {
      path: format!("schema/{path}"),
    },
    SchemaError::MissingId { path } => invalid(&format!("schema/{path}"), "`$id` が文字列ではない"),
    SchemaError::Build { path, message } => invalid(
      &format!("schema/{path}"),
      format!("スキーマを組み立てられない: {message}"),
    ),
  })
}

/// データのファイル `path` を、対応するスキーマで検査する。対応するスキーマがないファイルは検査しない。
fn check_schema(schemas: &SchemaSet, path: &str, value: &Value) -> Result<(), DataError> {
  let Some(format) = schema_format(path) else {
    return Ok(());
  };
  schemas.validate(format, value).map_err(|message| DataError::Schema {
    path: path.to_string(),
    message,
  })
}

/// `root` の適合テストデータを全部読む。
///
/// 読む順は、`manifest.json`、`schema/` のスキーマの登録、`coverage.json`、ケースのファイル。スキーマと
/// ケースは、`manifest.json` に正しい形で載ったファイルだけを読む（`listed_paths`）。載っていないファイルと
/// 形の誤った項目は、`manifest` の照合が問題として報告するだけで、読み込みの失敗にしない。データの
/// ファイルは、`format` と `version` を確かめた後、generators の展開の前に、対応するスキーマで検査する。
/// スキーマの違反は読み込みの失敗にする。`manifest.json` はスキーマで検査せず、`manifest` の照合の失敗は
/// 読み込みの失敗にしない。結果を `DataSet::manifest` に載せる。
pub fn load(root: &Path) -> Result<DataSet, DataError> {
  let files = inventory(root)?;

  let manifest_path = root.join("manifest.json");
  let manifest_bytes = fs::read(&manifest_path).map_err(|source| {
    if source.kind() == std::io::ErrorKind::NotFound {
      DataError::Missing {
        path: manifest_path.display().to_string(),
      }
    } else {
      io_error(&manifest_path, source)
    }
  })?;
  let manifest_sha256 = sha256_hex(&manifest_bytes);
  let manifest_value = parse_file("manifest.json", &manifest_bytes)?;
  let manifest = verify_manifest(&manifest_value, &files);
  let manifest_version = manifest_value
    .get("version")
    .and_then(Value::as_str)
    .map(str::to_string);
  let manifest_files = manifest_value
    .get("files")
    .and_then(Value::as_array)
    .map_or(0, Vec::len);

  // 一覧に正しい形で載っていないファイルは manifest の照合が問題として報告するので、スキーマとしても
  // ケースとしても読まない
  let listed = listed_paths(&manifest_value);
  let schemas = load_schemas(&files, &listed)?;

  let coverage_entry = files
    .iter()
    .find(|entry| entry.path == "coverage.json")
    .ok_or_else(|| DataError::Missing {
      path: "coverage.json".to_string(),
    })?;
  let coverage_value = parse_file("coverage.json", &coverage_entry.bytes)?;
  check_header("coverage.json", &coverage_value, &["coverage"])?;
  check_schema(&schemas, "coverage.json", &coverage_value)?;
  let coverage = Coverage {
    required_rules: string_list(
      "coverage.json",
      coverage_value.get("required_rules").unwrap_or(&Value::Null),
      "required_rules",
    )?,
    exclusions: read_exclusions(
      "coverage.json",
      coverage_value.get("exclusions").unwrap_or(&Value::Null),
    )?,
  };

  let mut cases = Vec::new();
  let mut ids = HashSet::new();
  for entry in &files {
    let in_case_directory = ["values/", "scenarios/", "dynamodb/"]
      .iter()
      .any(|directory| entry.path.starts_with(directory));
    if !in_case_directory || !entry.path.ends_with(".json") || !listed.contains(entry.path.as_str()) {
      continue;
    }
    let value = parse_file(&entry.path, &entry.bytes)?;
    let format = check_header(&entry.path, &value, &["values", "scenarios", "layout"])?;
    check_schema(&schemas, &entry.path, &value)?;
    let kind = match format.as_str() {
      "values" => CaseKind::ValueTable,
      "scenarios" => CaseKind::Scenario,
      _ => CaseKind::Layout,
    };
    let declared = value
      .get("cases")
      .and_then(Value::as_array)
      .ok_or_else(|| invalid(&entry.path, "cases が配列ではない"))?;
    for case in declared {
      let id = case
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid(&entry.path, "ケースの id が文字列ではない"))?
        .to_string();
      if !ids.insert(id.clone()) {
        return Err(DataError::DuplicateCaseId { id });
      }
      let rules = string_list(&entry.path, case.get("rules").unwrap_or(&Value::Null), "rules")?;
      cases.push(Case {
        id,
        rules,
        kind,
        file: entry.path.clone(),
        body: case.clone(),
      });
    }
  }

  Ok(DataSet {
    manifest_version,
    manifest_files,
    manifest_sha256,
    manifest,
    coverage,
    cases,
  })
}

fn generator_error(target: &str, message: impl Into<String>) -> DataError {
  DataError::Generator {
    target: target.to_string(),
    message: message.into(),
  }
}

/// ケースの `generators` を展開して、fixtures の空文字列を `character` の繰り返しに置き換える。
///
/// `target` は、ケースを根とする JSON Pointer（`~0`・`~1` を復号する）。対象は fixtures のイベントと
/// スナップショットの中の空文字列だけで、同じ `target` は 1 回だけ使える。`byte_length` は UTF-8 の
/// 総バイト数で、`character` の UTF-8 の幅で割り切れなければデータの誤りにする。
pub fn expand_generators(case: &mut Value) -> Result<(), DataError> {
  let Some(generators) = case.get("generators").and_then(Value::as_array).cloned() else {
    return Ok(());
  };
  let mut used_targets = HashSet::new();
  for generator in &generators {
    let target = generator
      .get("target")
      .and_then(Value::as_str)
      .ok_or_else(|| generator_error("", "target が文字列ではない"))?;
    let tokens: Vec<&str> = target.split('/').skip(1).collect();
    let in_fixtures =
      tokens.len() >= 4 && tokens[0] == "fixtures" && (tokens[1] == "events" || tokens[1] == "snapshots");
    if !in_fixtures {
      return Err(generator_error(
        target,
        "fixtures のイベントかスナップショットの中だけを指せる",
      ));
    }
    if !used_targets.insert(target.to_string()) {
      return Err(generator_error(target, "同じ target を 2 回使えない"));
    }
    let character = generator
      .get("character")
      .and_then(Value::as_str)
      .ok_or_else(|| generator_error(target, "character が文字列ではない"))?;
    if character.chars().count() != 1 {
      return Err(generator_error(target, "character は 1 文字でなければならない"));
    }
    let byte_length = generator
      .get("byte_length")
      .and_then(to_integer)
      .and_then(|length| usize::try_from(length).ok())
      .filter(|length| *length >= 1)
      .ok_or_else(|| generator_error(target, "byte_length は 1 以上の整数でなければならない"))?;
    let slot = case
      .pointer_mut(target)
      .ok_or_else(|| generator_error(target, "target が指す値がない"))?;
    match slot {
      Value::String(text) if text.is_empty() => {
        let width = character.len();
        if !byte_length.is_multiple_of(width) {
          return Err(generator_error(
            target,
            format!("byte_length {byte_length} が文字の UTF-8 の幅 {width} で割り切れない"),
          ));
        }
        *text = character.repeat(byte_length / width);
      }
      _ => return Err(generator_error(target, "target が指す値が空文字列ではない")),
    }
  }
  Ok(())
}
