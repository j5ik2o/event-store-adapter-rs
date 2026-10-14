//! `rs.json`（共通の名前と rs の公開 API の対応表）の読み込み。

use std::collections::BTreeMap;

use serde::Deserialize;

/// `rs.json` 全体。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mapping {
  /// 対応表の書式の名前。照合には使わない。
  pub format: String,
  /// 対応表の言語。照合には使わない。
  pub language: String,
  /// 対応する `surface.json` の版。
  pub surface_version: String,
  /// 照合するクレート（rustdoc の JSON のルートの名前）。
  #[serde(rename = "crate")]
  pub crate_name: String,
  /// 共通の型の引数（`P`・`A`）の対応。
  pub type_parameters: BTreeMap<String, TypeParameterMapping>,
  /// 型の引数に許す制約。
  pub allowed_type_parameter_bounds: Vec<String>,
  /// 値の種類の対応。
  pub value_types: BTreeMap<String, ValueTypeMapping>,
  pub conventions: Conventions,
  /// データの型の対応。
  pub types: BTreeMap<String, TypeMapping>,
  pub store: StoreMapping,
  pub error_categories: ErrorCategoriesMapping,
}

/// 共通の型の引数に対応する rs の型の引数。`{rs}` は `rs` の値に置き換える。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TypeParameterMapping {
  pub rs: String,
  /// 取得のメソッドの戻り値の型。
  pub getter: String,
  /// ストアの操作の入力の型。
  pub input: String,
  /// 構築と、省略できる要素を足すメソッドの引数の型。
  pub build: String,
}

/// 値の種類に対応する rs の型。`{rs}` は `rs` の値に置き換える。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValueTypeMapping {
  pub rs: String,
  /// `true` なら、`rs` はデータの型とストアが受け取る型の引数（関連型）の名前。
  #[serde(default)]
  pub type_parameter: bool,
  /// 型の引数に必要な制約（`type_parameter` が `true` のときだけ）。
  pub bound: Option<String>,
  pub getter: String,
  pub input: String,
  pub build: String,
}

/// rs の作法。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Conventions {
  /// 取得のメソッドと操作の受け手。
  pub receiver: String,
  /// 非同期の表し方（`async_trait` だけを解釈する）。
  #[serde(rename = "async")]
  pub async_style: String,
  /// 失敗の返し方。`{T}` は成功の値の型、`{error}` はエラーの列挙の名前。
  pub result: String,
  pub none: String,
  pub optional: String,
  pub list: String,
  /// ストアの中で、型の引数を書く形。`{name}` は型の引数の名前。
  pub store_type_parameter: String,
  /// データの型を取得・入力で渡すときの形。
  pub data_type: DataTypeConvention,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataTypeConvention {
  pub getter: String,
  pub input: String,
  pub build: String,
}

/// データの型の対応。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TypeMapping {
  pub rs: String,
  /// rs の型の引数の並び。
  pub type_parameters: Vec<String>,
  /// 要素の名前 → 取得のメソッドの名前。
  pub getters: BTreeMap<String, String>,
  /// 構築の手段。`surface.json` が `required` を定めていない型には置かない。
  pub constructor: Option<ConstructorMapping>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConstructorMapping {
  /// 構築する関数の名前。
  pub function: String,
  /// 構築が受け取る要素の並び（`required: true` の要素）。
  pub params: Vec<String>,
  /// 省略できる要素の名前 → 足すメソッドの名前。
  pub builders: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreMapping {
  pub rs: String,
  pub associated_types: Vec<String>,
  /// 操作の名前 → メソッドの名前。
  pub operations: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErrorCategoriesMapping {
  pub rs: String,
  /// 分類の名前 → 列挙の値の名前。
  pub variants: BTreeMap<String, String>,
}

impl Mapping {
  /// JSON の文字列から読み込む。
  pub fn from_json(json: &str) -> Result<Self, String> {
    serde_json::from_str(json).map_err(|error| format!("rs.json を読めない: {error}"))
  }
}
