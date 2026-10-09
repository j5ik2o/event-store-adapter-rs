use std::collections::{BTreeMap, BTreeSet};

use serde_json::{json, Value};

use super::{OperationReport, RequestLayout, RequestObservation};
use crate::{
  case::{required, string},
  compare::json_equal,
  number::to_integer,
};

pub(super) struct RequestContext<'a> {
  pub layout: &'a RequestLayout,
  pub aid: Option<&'a str>,
  pub seq_nr: Option<u64>,
  pub history: Option<&'a Value>,
  pub projection: Option<&'a str>,
  pub waits: &'a [u128],
}

fn phase_name(request: &RequestObservation) -> String {
  request
    .phase
    .map(|v| serde_json::to_value(v).expect("段階").as_str().expect("文字列").into())
    .unwrap_or_else(|| {
      if matches!(
        request.api.as_str(),
        "GetItem" | "BatchGetItem" | "TransactGetItems" | "Query" | "Scan"
      ) {
        "classify-condition-failure-read".into()
      } else {
        "unclassified".into()
      }
    })
}

fn response_body(report: &OperationReport, request_index: usize) -> Result<Value, String> {
  let request = &report.requests[request_index];
  let phase = serde_json::to_value(request.phase).map_err(|e| e.to_string())?;
  let ordinal = report.requests[..request_index]
    .iter()
    .filter(|v| v.api == request.api && v.phase == request.phase)
    .count();
  let response = report
    .responses
    .iter()
    .filter(|v| v["api"] == request.api && v["phase"] == phase)
    .nth(ordinal)
    .ok_or("実要求に対応する応答記録がない")?;
  serde_json::from_str(string(response.pointer("/delivered/body").ok_or("実応答本文がない")?)?)
    .map_err(|e| e.to_string())
}

fn resolve_name<'a>(body: &'a Value, token: &'a str) -> Result<&'a str, String> {
  if token.starts_with('#') {
    body
      .get("ExpressionAttributeNames")
      .and_then(|v| v.get(token))
      .and_then(Value::as_str)
      .ok_or_else(|| format!("属性名の束縛がない: {token}"))
  } else {
    Ok(token)
  }
}

fn resolve_value<'a>(body: &'a Value, token: &str) -> Result<&'a Value, String> {
  body
    .get("ExpressionAttributeValues")
    .and_then(|v| v.get(token))
    .ok_or_else(|| format!("属性値の束縛がない: {token}"))
}

fn tokens(expression: &str) -> Vec<String> {
  let mut tokens = Vec::new();
  let mut current = String::new();
  for c in expression.chars() {
    if c.is_whitespace() || matches!(c, '(' | ')' | ',' | '=' | '>') {
      if !current.is_empty() {
        tokens.push(std::mem::take(&mut current));
      }
      if !c.is_whitespace() {
        tokens.push(c.to_string());
      }
    } else {
      current.push(c);
    }
  }
  if !current.is_empty() {
    tokens.push(current);
  }
  tokens
}

fn condition(body: &Value, expected: &Value) -> Result<(), String> {
  let tokens = tokens(string(required(body, "ConditionExpression")?)?);
  let expected = expected
    .as_object()
    .filter(|v| v.len() == 1)
    .ok_or("条件の関数が一意でない")?;
  let (function, name) = expected.iter().next().expect("1条件");
  if tokens.len() != 4
    || tokens[0] != *function
    || tokens[1] != "("
    || tokens[3] != ")"
    || resolve_name(body, &tokens[2])? != string(name)?
  {
    return Err("存在・不在条件が一致しない".into());
  }
  Ok(())
}

fn key_condition(body: &Value, expected: &Value, context: &RequestContext<'_>) -> Result<(), String> {
  let tokens = tokens(string(required(body, "KeyConditionExpression")?)?);
  let mut actual = BTreeMap::new();
  let mut position = 0;
  while position < tokens.len() {
    if matches!(tokens[position].as_str(), "(" | ")" | "AND") {
      position += 1;
      continue;
    }
    let name = resolve_name(body, &tokens[position])?;
    let op = tokens.get(position + 1).ok_or("キー条件の演算子がない")?;
    let (op, consumed) = match op.as_str() {
      "=" => ("eq", 3),
      ">" if tokens.get(position + 2).is_some_and(|v| v == "=") => ("gte", 4),
      _ => return Err("未対応のキー条件演算子".into()),
    };
    let value = resolve_value(body, tokens.get(position + consumed - 1).ok_or("キー条件の値がない")?)?;
    if actual.insert((name.to_owned(), op), value.clone()).is_some() {
      return Err("キー条件が重複".into());
    }
    position += consumed;
  }
  let all = required(expected, "all")?.as_array().ok_or("allが配列ではない")?;
  if actual.len() != all.len() {
    return Err("キー条件の数が一致しない".into());
  }
  for expected in all {
    let value = match string(required(expected, "argument")?)? {
      "aggregate_id" => json!({"S":context.aid.ok_or("集約の束縛がない")?}),
      "seq_nr" => json!({"N":context.seq_nr.ok_or("seq_nrの束縛がない")?.to_string()}),
      _ => return Err("未知のキー条件引数".into()),
    };
    let name = string(required(expected, "attribute")?)?.to_owned();
    let op = string(required(expected, "operator")?)?;
    if actual.get(&(name, op)) != Some(&value) {
      return Err("キー条件の属性・演算子・値が一致しない".into());
    }
  }
  Ok(())
}

fn update(body: &Value, expected: &Value, expires: &Value) -> Result<(), String> {
  let tokens = tokens(string(required(body, "UpdateExpression")?)?);
  let mut clause = "";
  let mut set = BTreeMap::new();
  let mut remove = BTreeSet::new();
  let mut position = 0;
  while position < tokens.len() {
    let token = &tokens[position];
    if matches!(token.as_str(), "SET" | "REMOVE") {
      clause = token;
      position += 1;
      continue;
    }
    if token == "," {
      position += 1;
      continue;
    }
    let name = resolve_name(body, token)?.to_owned();
    match clause {
      "SET" => {
        if tokens.get(position + 1).is_none_or(|v| v != "=") {
          return Err("SETの構造が不正".into());
        }
        let value = resolve_value(body, tokens.get(position + 2).ok_or("SET値がない")?)?;
        if set.insert(name, value.clone()).is_some() {
          return Err("SET属性が重複".into());
        }
        position += 3;
      }
      "REMOVE" => {
        if !remove.insert(name) {
          return Err("REMOVE属性が重複".into());
        }
        position += 1;
      }
      _ => return Err("未対応の更新節".into()),
    }
  }
  let expected_set = required(expected, "set")?
    .as_object()
    .ok_or("setがオブジェクトではない")?;
  if set.len() != expected_set.len() {
    return Err("SET属性集合が一致しない".into());
  }
  for (name, value) in expected_set {
    if value["value_binding"] != "expires" || set.get(name) != Some(&json!({"N":expires.to_string()})) {
      return Err("SETの期限束縛が一致しない".into());
    }
  }
  let expected_remove = required(expected, "remove")?
    .as_array()
    .ok_or("removeが配列ではない")?
    .iter()
    .map(|v| string(v).map(str::to_owned))
    .collect::<Result<BTreeSet<_>, _>>()?;
  if remove != expected_remove {
    return Err("REMOVE属性集合が一致しない".into());
  }
  Ok(())
}

fn batch_keys(body: &Value, layout: &RequestLayout) -> Result<BTreeSet<String>, String> {
  let mut keys = BTreeSet::new();
  for (table, request) in required(body, "RequestItems")?
    .as_object()
    .ok_or("RequestItemsがオブジェクトではない")?
  {
    let table = layout.table(table).ok_or("未知の表への要求")?;
    for key in required(request, "Keys")?.as_array().ok_or("Keysが配列ではない")? {
      let aid = string(key.pointer("/aid/S").ok_or("aidがSではない")?)?;
      let selector = if table == "head" {
        format!("head:{aid}")
      } else {
        let name = if table == "journal" { "seq_nr" } else { "skey" };
        format!(
          "{table}:{aid}:{}",
          string(key.get(name).and_then(|v| v.get("N")).ok_or("ソートキーがNではない")?)?
        )
      };
      if !keys.insert(selector) {
        return Err("読み取りキーが重複".into());
      }
    }
  }
  Ok(keys)
}

fn table_keys(items: &Value) -> Result<Value, String> {
  Ok(Value::Object(
    items
      .as_object()
      .ok_or("キー列がオブジェクトではない")?
      .iter()
      .map(|(table, v)| (table.clone(), v.get("Keys").cloned().unwrap_or_else(|| v.clone())))
      .collect(),
  ))
}

fn sequence(report: &OperationReport, indices: &[usize], word: &str, value: &Value) -> Result<(), String> {
  if word == "follow_last_evaluated_key" {
    let mut previous = None;
    for index in indices {
      let request = &report.requests[*index].body;
      if request
        .get("ExclusiveStartKey")
        .filter(|v| v.as_object().is_none_or(|v| !v.is_empty()))
        != previous.as_ref()
      {
        return Err("LastEvaluatedKeyを次の要求へ引き継いでいない".into());
      }
      let response = response_body(report, *index)?;
      previous = response
        .get("LastEvaluatedKey")
        .filter(|v| v.as_object().is_some_and(|v| !v.is_empty()))
        .cloned();
    }
    if value == &json!(true) && previous.is_some() {
      return Err("最終ページを読み切っていない".into());
    }
  } else {
    let mut pending = json!({});
    let mut sizes = Vec::new();
    let mut retried = false;
    for index in indices {
      let request = required(&report.requests[*index].body, "RequestItems")?;
      if pending.as_object().is_some_and(|v| !v.is_empty()) {
        if !json_equal(request, &pending) {
          return Err("未処理削除だけを再要求していない".into());
        }
        retried = true;
      } else {
        let count = request
          .as_object()
          .ok_or("削除要求がオブジェクトではない")?
          .values()
          .map(|v| v.as_array().map(Vec::len).ok_or("削除要求が配列ではない"))
          .collect::<Result<Vec<_>, _>>()?
          .into_iter()
          .sum::<usize>();
        sizes.push(count);
      }
      pending = response_body(report, *index)?
        .get("UnprocessedItems")
        .cloned()
        .unwrap_or_else(|| json!({}));
    }
    if pending.as_object().is_some_and(|v| !v.is_empty()) {
      return Err("未処理削除が残った".into());
    }
    if word == "initial_batch_sizes" && !json_equal(&json!(sizes), value) {
      return Err("初回削除バッチの件数が一致しない".into());
    }
    if word == "retry_unprocessed_items" && value == &json!(true) && !retried {
      return Err("未処理削除の実再送がない".into());
    }
  }
  Ok(())
}

fn constraint(
  report: &OperationReport,
  index: usize,
  indices: &[usize],
  word: &str,
  value: &Value,
  constraints: &Value,
  context: &RequestContext<'_>,
) -> Result<(), String> {
  let body = &report.requests[index].body;
  let same = |actual: Option<&Value>| {
    if actual.is_some_and(|v| json_equal(value, v)) {
      Ok(())
    } else {
      Err(format!("{word}が一致しない"))
    }
  };
  match word {
    "table" => same(Some(&json!(context
      .layout
      .table(string(required(body, "TableName")?)?)))),
    "index" => {
      if value == "configured-history-index" && body["IndexName"] == context.layout.history_index {
        Ok(())
      } else {
        Err("GSI名が一致しない".into())
      }
    }
    "projection" => {
      if context.projection == value.as_str() && body.get("ProjectionExpression").is_none() {
        Ok(())
      } else {
        Err("実GSI射影が一致しない".into())
      }
    }
    "consistent_read" => same(body.get("ConsistentRead")),
    "scan_index_forward" => same(body.get("ScanIndexForward")),
    "consistent_read_all_tables" => {
      if required(body, "RequestItems")?
        .as_object()
        .ok_or("RequestItemsがない")?
        .values()
        .all(|v| v["ConsistentRead"] == *value)
      {
        Ok(())
      } else {
        Err("強整合読み取りを維持していない".into())
      }
    }
    "keys" => {
      let expected = value
        .as_array()
        .ok_or("keysが配列ではない")?
        .iter()
        .map(|v| string(v).map(str::to_owned))
        .collect::<Result<BTreeSet<_>, _>>()?;
      if batch_keys(body, context.layout)? == expected {
        Ok(())
      } else {
        Err("要求キーが一致しない".into())
      }
    }
    "head_and_current_snapshot" => {
      let aid = context.aid.ok_or("aidがない")?;
      if batch_keys(body, context.layout)? == BTreeSet::from([format!("head:{aid}"), format!("snapshot:{aid}:0")]) {
        Ok(())
      } else {
        Err("headと現在snapshotの2キーではない".into())
      }
    }
    "only_unprocessed_keys" => {
      let previous = indices
        .iter()
        .take_while(|v| **v != index)
        .last()
        .ok_or("再要求の前の要求がない")?;
      let response = response_body(report, *previous)?;
      let pending = required(&response, "UnprocessedKeys")?;
      if pending.as_object().is_none_or(|v| v.is_empty())
        || !json_equal(&table_keys(pending)?, &table_keys(required(body, "RequestItems")?)?)
      {
        Err("未処理キーだけを再要求していない".into())
      } else {
        Ok(())
      }
    }
    "exponential_backoff" => {
      let expected: Vec<_> = (0..indices.len().saturating_sub(1))
        .map(|n| {
          50u128
            .saturating_mul(1u128.checked_shl(n as u32).unwrap_or(u128::MAX))
            .min(2000)
        })
        .collect();
      if value == &json!(true) && !expected.is_empty() && context.waits == expected {
        Ok(())
      } else {
        Err("実再要求の指数バックオフが一致しない".into())
      }
    }
    "put_tables" | "same_store_id" | "layout_version" | "condition"
      if report.requests[index].api == "TransactWriteItems" =>
    {
      let actions = required(body, "TransactItems")?
        .as_array()
        .ok_or("TransactItemsがない")?;
      let puts = actions
        .iter()
        .map(|v| v.get("Put").ok_or("設定作成にPut以外がある"))
        .collect::<Result<Vec<_>, _>>()?;
      match word {
        "put_tables" => {
          let actual: BTreeSet<_> = puts
            .iter()
            .map(|v| {
              string(required(v, "TableName")?).and_then(|v| context.layout.table(v).ok_or_else(|| "未知の表".into()))
            })
            .collect::<Result<_, _>>()?;
          let expected: BTreeSet<_> = value
            .as_array()
            .ok_or("put_tablesが配列ではない")?
            .iter()
            .map(string)
            .collect::<Result<_, _>>()?;
          if puts.len() == expected.len() && actual == expected {
            Ok(())
          } else {
            Err("設定作成の3表が一致しない".into())
          }
        }
        "same_store_id" => {
          let ids = puts
            .iter()
            .map(|v| {
              v.pointer("/Item/store_id/S")
                .and_then(Value::as_str)
                .filter(|v| !v.is_empty())
                .ok_or("store_idがない")
            })
            .collect::<Result<Vec<_>, _>>()?;
          if ids.len() == 3 && ids.iter().all(|v| *v == ids[0]) {
            Ok(())
          } else {
            Err("store_idが一致しない".into())
          }
        }
        "layout_version" => {
          let expected = to_integer(value).ok_or("layout_versionが整数ではない")?;
          if puts.len() == 3
            && puts.iter().all(|v| {
              v.pointer("/Item/layout_version/N")
                .and_then(Value::as_str)
                .and_then(|v| v.parse::<i128>().ok())
                == Some(expected)
            })
          {
            Ok(())
          } else {
            Err("layout_versionが一致しない".into())
          }
        }
        _ => {
          for put in puts {
            condition(put, value)?;
          }
          Ok(())
        }
      }
    }
    "head_return_values_on_condition_check_failure" => {
      let actions = required(body, "TransactItems")?
        .as_array()
        .ok_or("TransactItemsがない")?;
      let heads = actions
        .iter()
        .filter_map(|v| v.get("Put").or_else(|| v.get("Update")))
        .filter(|v| v["TableName"] == context.layout.head)
        .collect::<Vec<_>>();
      if heads.len() == 1 && heads[0]["ReturnValuesOnConditionCheckFailure"] == *value {
        Ok(())
      } else {
        Err("headの失敗時旧項目指定が一致しない".into())
      }
    }
    "condition" => condition(body, value),
    "key_condition" => key_condition(body, value, context),
    "update" => update(body, value, required(constraints, "expires")?),
    "expression_attribute_names" => {
      for (name, expected) in value.as_object().ok_or("別名表がオブジェクトではない")? {
        if body.get("ExpressionAttributeNames").and_then(|v| v.get(name)) != Some(expected) {
          return Err("属性別名が一致しない".into());
        }
      }
      Ok(())
    }
    "expires" => {
      let values = required(body, "ExpressionAttributeValues")?
        .as_object()
        .ok_or("属性値表がない")?;
      if values
        .values()
        .any(|v| v.get("N").and_then(Value::as_str) == Some(value.to_string().as_str()))
      {
        Ok(())
      } else {
        Err("期限の実属性値がない".into())
      }
    }
    "target_seq_nrs" => {
      let actual: BTreeSet<_> = indices
        .iter()
        .map(|index| {
          report.requests[*index]
            .body
            .pointer("/Key/skey/N")
            .and_then(Value::as_str)
            .and_then(|v| v.parse::<i128>().ok())
            .ok_or("更新対象番号がない")
        })
        .collect::<Result<_, _>>()?;
      let expected: BTreeSet<_> = value
        .as_array()
        .ok_or("target_seq_nrsが配列ではない")?
        .iter()
        .map(|v| to_integer(v).ok_or("対象番号が整数ではない"))
        .collect::<Result<_, _>>()?;
      if actual == expected {
        Ok(())
      } else {
        Err("更新対象番号の集合が一致しない".into())
      }
    }
    "follow_last_evaluated_key" | "retry_unprocessed_items" | "initial_batch_sizes" => {
      sequence(report, indices, word, value)
    }
    "include_just_written_history" => {
      let seq = context.seq_nr.ok_or("今書いた番号がない")?;
      let history = context.history.ok_or("実履歴がない")?;
      if value == &json!(true) && history["active"].as_array().is_some_and(|v| v.contains(&json!(seq))) {
        Ok(())
      } else {
        Err("今書いた履歴が保持されていない".into())
      }
    }
    _ => Err(format!("未対応の要求条件: {word}")),
  }
}

#[derive(Default, serde::Serialize)]
pub(super) struct Checks {
  pub entries: Vec<Value>,
  pub errors: Vec<String>,
}

impl Checks {
  fn record(&mut self, name: String, result: Result<(), String>) {
    self
      .entries
      .push(json!({"constraint":name,"error":result.as_ref().err()}));
    if let Err(error) = result {
      self.errors.push(format!("{name}: {error}"));
    }
  }
}

pub(super) fn check(observe: &Value, report: &OperationReport, context: &RequestContext<'_>) -> Checks {
  let mut checks = Checks::default();
  for (key, minimum) in [("request_count", false), ("minimum_request_count", true)] {
    if let Some(counts) = observe.get(key).and_then(Value::as_object) {
      for (phase, expected) in counts {
        let count = report.requests.iter().filter(|v| phase_name(v) == *phase).count();
        let Some(expected) = expected.as_u64() else {
          checks.record(format!("{key}.{phase}"), Err("要求数が整数ではない".into()));
          continue;
        };
        if if minimum {
          (count as u64) < expected
        } else {
          (count as u64) != expected
        } {
          checks.record(format!("{key}.{phase}"), Err(format!("期待{expected}、実{count}")));
        } else {
          checks.record(format!("{key}.{phase}"), Ok(()));
        }
      }
    }
  }
  if let Some(phases) = observe.get("no_requests_in_phases").and_then(Value::as_array) {
    for phase in phases {
      if report
        .requests
        .iter()
        .any(|v| phase_name(v) == phase.as_str().unwrap_or_default())
      {
        checks.record(
          format!("no_requests_in_phases.{phase}"),
          Err("禁止段階への実要求がある".into()),
        );
      } else {
        checks.record(format!("no_requests_in_phases.{phase}"), Ok(()));
      }
    }
  }
  let mut cursor = 0;
  if let Some(requests) = observe.get("requests").and_then(Value::as_array) {
    for expected in requests {
      let (Some(api), Some(phase), Some(constraints)) = (
        expected["api"].as_str(),
        expected["phase"].as_str(),
        expected["constraints"].as_object(),
      ) else {
        checks.record("requests".into(), Err("要求宣言のapi・phase・constraintsが不正".into()));
        continue;
      };
      let index = (cursor..report.requests.len())
        .find(|i| report.requests[*i].api == api && phase_name(&report.requests[*i]) == phase);
      let Some(index) = index else {
        checks.record(format!("{api}/{phase}"), Err("宣言順の別要求がない".into()));
        for word in constraints.keys() {
          checks.record(format!("{api}/{phase}/{word}"), Err("照合する実要求がない".into()));
        }
        continue;
      };
      cursor = index + 1;
      let indices: Vec<_> = report
        .requests
        .iter()
        .enumerate()
        .filter(|(_, v)| v.api == api && phase_name(v) == phase)
        .map(|(i, _)| i)
        .collect();
      for (word, value) in constraints {
        checks.record(
          format!("{api}/{phase}/{word}"),
          constraint(report, index, &indices, word, value, &expected["constraints"], context),
        );
      }
    }
  }
  if report.unfinished_history {
    checks.record(
      "history_pages".into(),
      Err("障害応答の履歴ページ列が終了まで読み切られていない".into()),
    );
  }
  checks
}

#[cfg(test)]
#[path = "check_requests_test.rs"]
mod tests;
