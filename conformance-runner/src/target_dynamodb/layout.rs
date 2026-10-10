use std::collections::BTreeMap;

use aws_sdk_dynamodb::{
  types::{KeySchemaElement, KeyType, TableDescription},
  Client,
};
use serde_json::{json, Value};

use super::{
  items::{check_shape, Item},
  RequestLayout,
};
use crate::{
  case::{required, string},
  compare::json_equal,
};

fn key(table: &TableDescription, schema: &[KeySchemaElement], kind: KeyType) -> Result<Value, String> {
  let matching: Vec<_> = schema.iter().filter(|v| v.key_type() == &kind).collect();
  let [key] = matching.as_slice() else {
    return if matching.is_empty() && kind == KeyType::Range {
      Ok(Value::Null)
    } else {
      Err("実キー構成が一意ではない".into())
    };
  };
  let definition = table
    .attribute_definitions()
    .iter()
    .find(|v| v.attribute_name() == key.attribute_name())
    .ok_or("実キーの属性定義がない")?;
  Ok(json!({"name":key.attribute_name(),"type":definition.attribute_type().as_str()}))
}

pub(super) async fn describe(raw: &Client, layout: &RequestLayout) -> Result<Value, String> {
  let mut tables = Vec::new();
  for logical in ["journal", "snapshot", "head"] {
    let actual = layout.actual_table(logical)?;
    let output = raw
      .describe_table()
      .table_name(actual)
      .send()
      .await
      .map_err(|e| e.to_string())?;
    let table = output.table.ok_or("DescribeTableに表がない")?;
    let mut indexes = Vec::new();
    for index in table.global_secondary_indexes() {
      indexes.push(json!({"actual_name":index.index_name(), "partition_key":key(&table,index.key_schema(),KeyType::Hash)?,
        "sort_key":key(&table,index.key_schema(),KeyType::Range)?,"projection":index.projection().and_then(|v| v.projection_type()).map(|v| v.as_str())}));
    }
    let stream = table.stream_specification();
    let ttl = raw
      .describe_time_to_live()
      .table_name(actual)
      .send()
      .await
      .map_err(|e| e.to_string())?
      .time_to_live_description
      .ok_or("実TTL状態がない")?;
    tables.push(
      json!({"name":logical,"actual_name":actual,"partition_key":key(&table,table.key_schema(),KeyType::Hash)?,
      "sort_key":key(&table,table.key_schema(),KeyType::Range)?, "gsi":indexes,
      "streams":{"enabled":stream.map(|v| v.stream_enabled()).unwrap_or(false),
        "view_type":stream.and_then(|v| v.stream_view_type()).map(|v| v.as_str())},
      "ttl":{"status":ttl.time_to_live_status().map(|v| v.as_str()),"attribute":ttl.attribute_name()}}),
    );
  }
  Ok(json!(tables))
}

pub(super) fn check_tables(declared: &Value, actual: &Value, layout: &RequestLayout, ttl: bool) -> Result<(), String> {
  let expected = declared.as_array().ok_or("tablesが配列ではない")?;
  let actual = actual.as_array().ok_or("実tablesが配列ではない")?;
  if actual.len() != expected.len() {
    return Err("実表数が一致しない".into());
  }
  for expected in expected {
    let name = string(required(expected, "name")?)?;
    let table = actual.iter().find(|v| v["name"] == name).ok_or("実表がない")?;
    for field in ["partition_key", "sort_key", "streams"] {
      if !json_equal(required(expected, field)?, required(table, field)?) {
        return Err(format!("{name}/{field}が一致しない"));
      }
    }
    let expected_gsi = required(expected, "gsi")?.as_array().ok_or("gsiが配列ではない")?;
    let actual_gsi = required(table, "gsi")?.as_array().ok_or("実gsiが配列ではない")?;
    if expected_gsi.len() != actual_gsi.len() {
      return Err(format!("{name}/gsiの数が一致しない"));
    }
    for (expected, actual) in expected_gsi.iter().zip(actual_gsi) {
      if expected["name_binding"] != "configured-history-index" || actual["actual_name"] != layout.history_index {
        return Err("実GSI名が設定束縛と一致しない".into());
      }
      for field in ["partition_key", "sort_key", "projection"] {
        if !json_equal(required(expected, field)?, required(actual, field)?) {
          return Err(format!("{name}/gsi/{field}が一致しない"));
        }
      }
    }
    let enabled = expected.pointer("/ttl/enabled_when").and_then(Value::as_str) == Some("retention-mode-ttl") && ttl;
    if table.pointer("/ttl/status").and_then(Value::as_str) != Some(if enabled { "ENABLED" } else { "DISABLED" })
      || (enabled && table.pointer("/ttl/attribute") != expected.pointer("/ttl/attribute"))
    {
      return Err(format!("{name}/ttlの実状態が一致しない"));
    }
  }
  Ok(())
}

fn kind(table: &str, aid: &str, skey: Option<i128>, ttl: bool) -> Result<String, String> {
  if aid == "__config__" {
    return Ok(format!("configuration:{table}"));
  }
  match table {
    "journal" => Ok("journal".into()),
    "head" => Ok("head".into()),
    "snapshot" => match skey {
      Some(0) => Ok("current-snapshot".into()),
      Some(v) if v > 0 => Ok(if ttl { "marked-history" } else { "active-history" }.into()),
      _ => Err("snapshotの項目種別が決まらない".into()),
    },
    _ => Err("未知の項目表".into()),
  }
}

pub(super) fn item_kind(table: &str, item: &Item) -> Result<String, String> {
  let aid = item.get("aid").and_then(|v| v.as_s().ok()).ok_or("実aidがSではない")?;
  let skey = item
    .get("skey")
    .and_then(|v| v.as_n().ok())
    .and_then(|v| v.parse::<i128>().ok());
  kind(table, aid, skey, item.contains_key("ttl"))
}

pub(super) fn check_items(declared: &Value, observed: &BTreeMap<String, (String, Item)>) -> Result<(), String> {
  for item in declared.as_array().ok_or("配置itemsが配列ではない")? {
    let table = string(required(item, "table")?)?;
    let name = kind(
      table,
      string(item.pointer("/values/aid").ok_or("配置aidがない")?)?,
      item
        .pointer("/values/skey")
        .and_then(Value::as_str)
        .and_then(|v| v.parse::<i128>().ok()),
      item["attributes"].get("ttl").is_some(),
    )?;
    let (actual_table, actual) = observed
      .get(&name)
      .ok_or_else(|| format!("libraryが書いた項目種別の実観測がない: {name}"))?;
    if actual_table != table {
      return Err(format!("{name}の実表が一致しない"));
    }
    check_shape(item, actual).map_err(|e| format!("{name}: {e}"))?;
  }
  Ok(())
}

#[cfg(test)]
#[path = "layout_test.rs"]
mod tests;
