use std::collections::{BTreeMap, HashMap};

use aws_sdk_dynamodb::{primitives::Blob, types::AttributeValue, Client};
use serde_json::{json, Value};

use super::RequestLayout;
use crate::{
  case::{required, string},
  compare::json_equal,
  number::to_integer,
};

pub(super) type Item = HashMap<String, AttributeValue>;

fn integer(written: &str) -> Result<i128, String> {
  serde_json::from_str::<Value>(written)
    .ok()
    .and_then(|v| to_integer(&v))
    .ok_or_else(|| format!("N属性が整数ではない: {written}"))
}

fn attribute(kind: &str, value: &Value, nested: Option<&Value>) -> Result<AttributeValue, String> {
  Ok(match kind {
    "S" => AttributeValue::S(string(value)?.into()),
    "N" => {
      integer(string(value)?)?;
      AttributeValue::N(string(value)?.into())
    }
    "L" => AttributeValue::L(
      value
        .as_array()
        .ok_or("Lが配列ではない")?
        .iter()
        .map(|v| build_map(nested.ok_or("Lの入れ子型がない")?, v).map(AttributeValue::M))
        .collect::<Result<_, _>>()?,
    ),
    _ => return Err(format!("valuesの未対応型: {kind}")),
  })
}

fn build_map(types: &Value, values: &Value) -> Result<Item, String> {
  values
    .as_object()
    .ok_or("valuesがオブジェクトではない")?
    .iter()
    .map(|(name, value)| Ok((name.clone(), attribute(string(required(types, name)?)?, value, None)?)))
    .collect()
}

#[cfg(any(feature = "dynamodb", test))]
fn path_pointer(path: &str) -> String {
  let path = path.replace('[', ".").replace(']', "");
  path
    .split('.')
    .map(|v| format!("/{}", v.replace('~', "~0").replace('/', "~1")))
    .collect()
}

fn set_binary(item: &mut Item, path: &str, bytes: Vec<u8>) -> Result<(), String> {
  if let Some((list, tail)) = path.split_once('[') {
    let (position, name) = tail.split_once("].").ok_or("バイナリのパスが不正")?;
    let index = position.parse::<usize>().map_err(|e| e.to_string())?;
    let AttributeValue::L(list) = item.get_mut(list).ok_or("バイナリのリストがない")? else {
      return Err("バイナリの親がLではない".into());
    };
    let Some(AttributeValue::M(map)) = list.get_mut(index) else {
      return Err("バイナリの要素がMではない".into());
    };
    map.insert(name.into(), AttributeValue::B(Blob::new(bytes)));
  } else {
    item.insert(path.into(), AttributeValue::B(Blob::new(bytes)));
  }
  Ok(())
}

pub(super) fn build_item(declared: &Value, bindings: &mut BTreeMap<String, String>) -> Result<Item, String> {
  let types = required(declared, "attributes")?;
  let values = required(declared, "values")?;
  let mut item = Item::new();
  for (name, value) in values.as_object().ok_or("valuesがオブジェクトではない")? {
    let nested = declared
      .get("nested_attributes")
      .and_then(|v| v.get(format!("{name}[0]")));
    item.insert(name.clone(), attribute(string(required(types, name)?)?, value, nested)?);
  }
  if let Some(binary) = declared.get("binary_json").and_then(Value::as_object) {
    for (path, value) in binary {
      set_binary(&mut item, path, serde_json::to_vec(value).map_err(|e| e.to_string())?)?;
    }
  }
  if let Some(declared_bindings) = declared.get("bindings").and_then(Value::as_object) {
    for (name, binding) in declared_bindings {
      let binding = string(binding)?;
      let value = bindings.get(binding).ok_or("seedの識別子束縛がない")?;
      item.insert(name.into(), AttributeValue::S(value.clone()));
    }
  }
  check_shape(declared, &item)?;
  Ok(item)
}

#[cfg(any(feature = "dynamodb", test))]
pub(super) fn key(declared: &Value) -> Result<Item, String> {
  let names: &[&str] = match string(required(declared, "table")?)? {
    "journal" => &["aid", "seq_nr"],
    "snapshot" => &["aid", "skey"],
    "head" => &["aid"],
    _ => return Err("未知の表".into()),
  };
  let values = required(declared, "values")?;
  let types = required(declared, "attributes")?;
  names
    .iter()
    .map(|name| {
      Ok((
        (*name).into(),
        attribute(string(required(types, name)?)?, required(values, name)?, None)?,
      ))
    })
    .collect()
}

pub(super) fn wire_attribute(attribute: &AttributeValue) -> Value {
  match attribute {
    AttributeValue::S(v) => json!({"S":v}),
    AttributeValue::N(v) => json!({"N":v}),
    AttributeValue::B(v) => json!({"B":aws_smithy_types::base64::encode(v.as_ref())}),
    AttributeValue::L(v) => json!({"L":v.iter().map(wire_attribute).collect::<Vec<_>>()}),
    AttributeValue::M(v) => json!({"M":wire_item(v)}),
    AttributeValue::Bool(v) => json!({"BOOL":v}),
    AttributeValue::Null(v) => json!({"NULL":v}),
    AttributeValue::Ss(v) => json!({"SS":v}),
    AttributeValue::Ns(v) => json!({"NS":v}),
    AttributeValue::Bs(v) => {
      json!({"BS":v.iter().map(|b| aws_smithy_types::base64::encode(b.as_ref())).collect::<Vec<_>>()})
    }
    _ => json!({"unknown":true}),
  }
}

pub(super) fn wire_item(item: &Item) -> Value {
  Value::Object(
    item
      .iter()
      .map(|(name, value)| (name.clone(), wire_attribute(value)))
      .collect(),
  )
}

fn type_map(item: &Value) -> Result<Value, String> {
  Ok(Value::Object(
    item
      .as_object()
      .ok_or("項目がMではない")?
      .iter()
      .map(|(name, value)| {
        let kind = value
          .as_object()
          .filter(|v| v.len() == 1)
          .and_then(|v| v.keys().next())
          .ok_or("属性の型が一意でない")?;
        Ok((name.clone(), json!(kind)))
      })
      .collect::<Result<_, String>>()?,
  ))
}

pub(super) fn check_shape(declared: &Value, item: &Item) -> Result<(), String> {
  let wire = wire_item(item);
  if !json_equal(required(declared, "attributes")?, &type_map(&wire)?) {
    return Err("手順2: 属性集合・型が一致しない".into());
  }
  if let Some(nested) = declared.get("nested_attributes").and_then(Value::as_object) {
    for (path, types) in nested {
      let (list, tail) = path.split_once('[').ok_or("入れ子パスが不正")?;
      let index = tail.trim_end_matches(']').parse::<usize>().map_err(|e| e.to_string())?;
      let elements = wire
        .get(list)
        .and_then(|v| v.get("L"))
        .and_then(Value::as_array)
        .ok_or("手順3: Lがない")?;
      let expected_count = declared
        .pointer(&format!("/values/{list}"))
        .and_then(Value::as_array)
        .ok_or("Lの期待値がない")?
        .len();
      if elements.len() != expected_count {
        return Err(format!("手順3: {list}の要素数が一致しない"));
      }
      let map = elements.get(index).and_then(|v| v.get("M")).ok_or("手順3: Mがない")?;
      if !json_equal(types, &type_map(map)?) {
        return Err(format!("手順3: {path}の属性集合・型が一致しない"));
      }
    }
  }
  Ok(())
}

#[cfg(any(feature = "dynamodb", test))]
fn plain(attribute: &AttributeValue) -> Value {
  match attribute {
    AttributeValue::S(v) | AttributeValue::N(v) => json!(v),
    AttributeValue::L(v) => json!(v.iter().map(plain).collect::<Vec<_>>()),
    AttributeValue::M(v) => Value::Object(v.iter().map(|(k, v)| (k.clone(), plain(v))).collect()),
    AttributeValue::B(v) => json!({"bytes":v.as_ref(),"byte_length":v.as_ref().len()}),
    _ => wire_attribute(attribute),
  }
}

#[cfg(any(feature = "dynamodb", test))]
pub(super) fn compare_item(
  declared: &Value,
  item: &Item,
  bindings: &mut BTreeMap<String, String>,
) -> Result<(), String> {
  check_shape(declared, item)?;
  let mut values = Value::Object(item.iter().map(|(k, v)| (k.clone(), plain(v))).collect());
  if let Some(binary) = declared.get("binary_json").and_then(Value::as_object) {
    for (path, expected) in binary {
      let slot = values
        .pointer_mut(&path_pointer(path))
        .ok_or_else(|| format!("手順4: {path}がない"))?;
      let bytes: Vec<u8> =
        serde_json::from_value(slot["bytes"].clone()).map_err(|e| format!("手順4: {path}がBではない: {e}"))?;
      let actual: Value = serde_json::from_slice(&bytes).map_err(|e| format!("手順4: {path}のJSON復元失敗: {e}"))?;
      if !json_equal(expected, &actual) {
        return Err(format!("手順4: {path}のJSON値が一致しない"));
      }
      remove_path(&mut values, path)?;
    }
  }
  if let Some(declared_bindings) = declared.get("bindings").and_then(Value::as_object) {
    for (path, binding) in declared_bindings {
      let actual = string(values.get(path).ok_or("手順5: 束縛属性がない")?)?;
      if actual.is_empty() {
        return Err("手順5: store_idが空".into());
      }
      let saved = bindings.entry(string(binding)?.into()).or_insert_with(|| actual.into());
      if saved != actual {
        return Err(format!("手順5: {path}の束縛が一致しない"));
      }
      remove_path(&mut values, path)?;
    }
  }
  compare_values(
    required(declared, "values")?,
    &values,
    required(declared, "attributes")?,
    declared.get("nested_attributes"),
  )
  .map_err(|e| format!("手順6: {e}"))
}

#[cfg(any(feature = "dynamodb", test))]
fn remove_path(value: &mut Value, path: &str) -> Result<(), String> {
  let pointer = path_pointer(path);
  let (parent, name) = pointer.rsplit_once('/').ok_or("パスが不正")?;
  value
    .pointer_mut(parent)
    .and_then(Value::as_object_mut)
    .ok_or("パスの親がオブジェクトではない")?
    .remove(name);
  Ok(())
}

#[cfg(any(feature = "dynamodb", test))]
fn compare_values(expected: &Value, actual: &Value, types: &Value, nested: Option<&Value>) -> Result<(), String> {
  let expected = expected.as_object().ok_or("値がオブジェクトではない")?;
  let actual = actual.as_object().ok_or("実値がオブジェクトではない")?;
  if expected.keys().ne(actual.keys()) {
    return Err("残りの属性集合が一致しない".into());
  }
  for (name, expected) in expected {
    let actual = &actual[name];
    let equal = if types[name] == "N" {
      integer(string(expected)?)? == integer(string(actual)?)?
    } else if types[name] == "L" {
      let types = nested
        .and_then(|v| v.get(format!("{name}[0]")))
        .ok_or("入れ子型がない")?;
      let expected = expected.as_array().ok_or("Lが配列ではない")?;
      let actual = actual.as_array().ok_or("実Lが配列ではない")?;
      expected.len() == actual.len()
        && expected
          .iter()
          .zip(actual)
          .all(|(e, a)| compare_values(e, a, types, None).is_ok())
    } else {
      json_equal(expected, actual)
    };
    if !equal {
      return Err(format!("{name}の値が一致しない"));
    }
  }
  Ok(())
}

pub(super) async fn seed(raw: &Client, layout: &RequestLayout, declared: &[Value]) -> Result<(), String> {
  let mut bindings = BTreeMap::from([("generated-store-id".into(), format!("seed-{}", layout.head))]);
  for item in declared {
    raw
      .put_item()
      .table_name(layout.actual_table(string(required(item, "table")?)?)?)
      .set_item(Some(build_item(item, &mut bindings)?))
      .send()
      .await
      .map_err(|e| e.to_string())?;
  }
  Ok(())
}

#[cfg(feature = "dynamodb")]
pub(super) async fn get(raw: &Client, layout: &RequestLayout, declared: &Value) -> Result<Item, String> {
  raw
    .get_item()
    .table_name(layout.actual_table(string(required(declared, "table")?)?)?)
    .set_key(Some(key(declared)?))
    .consistent_read(true)
    .send()
    .await
    .map_err(|e| e.to_string())?
    .item
    .ok_or_else(|| "手順1: 物理項目がない".into())
}

#[cfg(test)]
#[path = "items_test.rs"]
mod tests;
