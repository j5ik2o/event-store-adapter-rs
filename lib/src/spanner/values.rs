//! Spanner の値の符号化と、ExecuteStreamingSql の応答の結合（SPN-3・SPN-5・SPN-10）。
//!
//! INT64 は 10 進の文字列、BYTES は base64（STANDARD）の文字列で送り、数値（f64）を経由しない（SPN-5）。
//! 応答は `PartialResultSet` の列で、値が複数の応答に分割されることがある（`chunked_value`）。

use std::collections::BTreeMap;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use googleapis_tonic_google_spanner_v1::google::spanner::v1::{PartialResultSet, Type, TypeCode};
use prost_types::value::Kind;
use prost_types::{ListValue, Struct, Value};

/// 応答の形が想定と違う（値の型、列数、分割の途中での終了など）。
#[derive(Debug)]
pub(crate) struct MalformedResult(pub(crate) &'static str);

impl std::fmt::Display for MalformedResult {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(formatter, "malformed spanner result: {}", self.0)
  }
}

impl std::error::Error for MalformedResult {}

pub(crate) fn null_value() -> Value {
  Value {
    kind: Some(Kind::NullValue(0)),
  }
}

pub(crate) fn int64_value(value: i64) -> Value {
  Value {
    kind: Some(Kind::StringValue(value.to_string())),
  }
}

pub(crate) fn string_value(value: &str) -> Value {
  Value {
    kind: Some(Kind::StringValue(value.to_owned())),
  }
}

pub(crate) fn bytes_value(value: &[u8]) -> Value {
  Value {
    kind: Some(Kind::StringValue(STANDARD.encode(value))),
  }
}

pub(crate) fn bool_value(value: bool) -> Value {
  Value {
    kind: Some(Kind::BoolValue(value)),
  }
}

fn list_value(values: Vec<Value>) -> Value {
  Value {
    kind: Some(Kind::ListValue(ListValue { values })),
  }
}

/// SQL の名前付きパラメーター。値と型を一緒に持つ。
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Param {
  Int64(i64),
  String(String),
  Bytes(Vec<u8>),
  StringArray(Vec<String>),
  Int64Array(Vec<i64>),
}

impl Param {
  fn value(&self) -> Value {
    match self {
      Param::Int64(value) => int64_value(*value),
      Param::String(value) => string_value(value),
      Param::Bytes(value) => bytes_value(value),
      Param::StringArray(values) => list_value(values.iter().map(|value| string_value(value)).collect()),
      Param::Int64Array(values) => list_value(values.iter().map(|value| int64_value(*value)).collect()),
    }
  }

  fn spanner_type(&self) -> Type {
    let scalar = |code: TypeCode| Type {
      code: code as i32,
      ..Default::default()
    };
    match self {
      Param::Int64(_) => scalar(TypeCode::Int64),
      Param::String(_) => scalar(TypeCode::String),
      Param::Bytes(_) => scalar(TypeCode::Bytes),
      Param::StringArray(_) => Type {
        code: TypeCode::Array as i32,
        array_element_type: Some(Box::new(scalar(TypeCode::String))),
        ..Default::default()
      },
      Param::Int64Array(_) => Type {
        code: TypeCode::Array as i32,
        array_element_type: Some(Box::new(scalar(TypeCode::Int64))),
        ..Default::default()
      },
    }
  }
}

/// 名前付きパラメーターの値の構造体と、その型の表を作る。
pub(crate) fn params_of(params: &[(&str, Param)]) -> (Option<Struct>, std::collections::HashMap<String, Type>) {
  let mut fields = BTreeMap::new();
  let mut types = std::collections::HashMap::new();
  for (name, param) in params {
    fields.insert((*name).to_owned(), param.value());
    types.insert((*name).to_owned(), param.spanner_type());
  }
  (Some(Struct { fields }), types)
}

/// `PartialResultSet` の列を、先頭の応答の列数ごとの行にまとめる。
///
/// `chunked_value` が真の応答の最後の値は未完で、次の応答の最初の値と文字列として連結する（SPN-10・R-5）。
/// 連結できるのは文字列の値だけである。分割のまま終わった結果と、列数に満たない最後の行は誤りにして、
/// それまでの結果を返さない。
pub(crate) fn merge_partial_result_sets(responses: Vec<PartialResultSet>) -> Result<Vec<Vec<Value>>, MalformedResult> {
  let columns = responses
    .first()
    .and_then(|response| response.metadata.as_ref())
    .and_then(|metadata| metadata.row_type.as_ref())
    .map(|row_type| row_type.fields.len())
    .filter(|columns| *columns > 0)
    .ok_or(MalformedResult("the first response has no row type"))?;
  let mut cells: Vec<Value> = Vec::new();
  let mut pending_chunk = false;
  for response in responses {
    let chunked = response.chunked_value;
    let mut values = response.values.into_iter();
    if pending_chunk {
      let Some(continuation) = values.next() else {
        return Err(MalformedResult("a chunked value has no continuation"));
      };
      let Some(last) = cells.last_mut() else {
        return Err(MalformedResult("a chunked value has no head"));
      };
      match (&mut last.kind, continuation.kind) {
        (Some(Kind::StringValue(head)), Some(Kind::StringValue(tail))) => head.push_str(&tail),
        _ => return Err(MalformedResult("only string values can be chunked")),
      }
      pending_chunk = false;
    }
    cells.extend(values);
    if chunked {
      if cells.is_empty() {
        return Err(MalformedResult("a response marks a chunk but holds no value"));
      }
      pending_chunk = true;
    }
  }
  if pending_chunk {
    return Err(MalformedResult("the stream ended inside a chunked value"));
  }
  if !cells.len().is_multiple_of(columns) {
    return Err(MalformedResult("the stream ended inside a row"));
  }
  Ok(cells.chunks(columns).map(<[Value]>::to_vec).collect())
}

fn string_of(value: &Value) -> Result<&str, MalformedResult> {
  match &value.kind {
    Some(Kind::StringValue(text)) => Ok(text),
    _ => Err(MalformedResult("a STRING, INT64, or BYTES value is not a string")),
  }
}

/// INT64 の値。10 進の文字列を、損失なく読む。NULL は `None`。
pub(crate) fn read_int64(value: &Value) -> Result<Option<i64>, MalformedResult> {
  if matches!(value.kind, None | Some(Kind::NullValue(_))) {
    return Ok(None);
  }
  string_of(value)?
    .parse::<i64>()
    .map(Some)
    .map_err(|_| MalformedResult("an INT64 value is not a decimal integer"))
}

pub(crate) fn read_required_int64(value: &Value) -> Result<i64, MalformedResult> {
  read_int64(value)?.ok_or(MalformedResult("a NOT NULL INT64 value is NULL"))
}

pub(crate) fn read_string(value: &Value) -> Result<String, MalformedResult> {
  string_of(value).map(str::to_owned)
}

/// BYTES の値。base64（STANDARD）の文字列を復号する。
pub(crate) fn read_bytes(value: &Value) -> Result<Vec<u8>, MalformedResult> {
  STANDARD
    .decode(string_of(value)?)
    .map_err(|_| MalformedResult("a BYTES value is not base64"))
}

/// BOOL の値。
pub(crate) fn read_bool(value: &Value) -> Result<bool, MalformedResult> {
  match &value.kind {
    Some(Kind::BoolValue(value)) => Ok(*value),
    _ => Err(MalformedResult("a BOOL value is not a boolean")),
  }
}

/// 行の `index` 番目の値。列が足りなければ誤りにする。
pub(crate) fn cell(row: &[Value], index: usize) -> Result<&Value, MalformedResult> {
  row
    .get(index)
    .ok_or(MalformedResult("a row has fewer columns than selected"))
}

#[cfg(test)]
#[path = "values_test.rs"]
mod tests;
