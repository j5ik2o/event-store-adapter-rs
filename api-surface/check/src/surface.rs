//! `surface.json`（共通契約 v5 の公開 API の形、ハブの写し）の読み込み。

use std::collections::BTreeMap;

use serde::Deserialize;

/// `surface.json` 全体。照合に使う項目だけを読む。
#[derive(Debug, Clone, Deserialize)]
pub struct Surface {
  pub version: String,
  /// 共通の型の引数（`P`・`A`）。キーが名前、値が説明。
  pub type_parameters: BTreeMap<String, String>,
  /// 値の種類（`aggregate_id`・`seq_nr`・`timestamp`・`string`）。キーが名前、値が説明。
  pub value_types: BTreeMap<String, String>,
  pub types: Vec<SurfaceType>,
  pub store: SurfaceStore,
  pub error_categories: SurfaceErrorCategories,
}

/// 封筒などのデータの型。
#[derive(Debug, Clone, Deserialize)]
pub struct SurfaceType {
  pub name: String,
  pub type_parameters: Vec<String>,
  pub elements: Vec<SurfaceElement>,
}

/// データの型の要素。`required` を定めていない型（`snapshot_read`）では `None`。
#[derive(Debug, Clone, Deserialize)]
pub struct SurfaceElement {
  pub name: String,
  #[serde(rename = "type")]
  pub type_: String,
  pub required: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SurfaceStore {
  pub name: String,
  pub type_parameters: Vec<String>,
  pub operations: Vec<SurfaceOperation>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SurfaceOperation {
  pub name: String,
  pub inputs: Vec<SurfaceInput>,
  pub output: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SurfaceInput {
  pub name: String,
  #[serde(rename = "type")]
  pub type_: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SurfaceErrorCategories {
  pub names: Vec<String>,
}

impl Surface {
  /// JSON の文字列から読み込む。
  pub fn from_json(json: &str) -> Result<Self, String> {
    serde_json::from_str(json).map_err(|error| format!("surface.json を読めない: {error}"))
  }
}

/// `surface.json` の型の書き方（`name`、`name<arg, ...>`）を解析した木。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeExpr {
  pub name: String,
  pub args: Vec<TypeExpr>,
}

impl TypeExpr {
  /// `optional<snapshot_envelope<A>>` のような書き方を解析する。
  pub fn parse(text: &str) -> Result<Self, String> {
    let chars: Vec<char> = text.chars().filter(|c| !c.is_whitespace()).collect();
    let mut position = 0;
    let expr = Self::parse_at(&chars, &mut position)?;
    if position != chars.len() {
      return Err(format!("型の書き方 {text:?} の {position} 文字目以降を読めない"));
    }
    Ok(expr)
  }

  fn parse_at(chars: &[char], position: &mut usize) -> Result<Self, String> {
    let start = *position;
    while *position < chars.len() && (chars[*position].is_alphanumeric() || chars[*position] == '_') {
      *position += 1;
    }
    if start == *position {
      return Err(format!("型の名前が空: {} 文字目", start));
    }
    let name: String = chars[start..*position].iter().collect();
    let mut args = Vec::new();
    if chars.get(*position) == Some(&'<') {
      *position += 1;
      loop {
        args.push(Self::parse_at(chars, position)?);
        match chars.get(*position) {
          Some(',') => *position += 1,
          Some('>') => {
            *position += 1;
            break;
          }
          _ => return Err(format!("型の引数の閉じ括弧がない: {} 文字目", position)),
        }
      }
    }
    Ok(Self { name, args })
  }
}
