use std::collections::HashSet;
use std::fmt;

use aws_smithy_runtime_api::client::orchestrator::HttpRequest;
use serde_json::Value;

use super::transport::TransportError;
use crate::fault::Phase;
use crate::number::to_integer;

/// 要求の識別に使う実テーブル名と履歴GSI名を保持する。
#[derive(Clone)]
pub struct RequestLayout {
  journal: String,
  snapshot: String,
  head: String,
  history_index: String,
}

impl RequestLayout {
  /// 空でなく、互いに異なる3テーブルの名前を登録する。
  pub fn new(journal: &str, snapshot: &str, head: &str, history_index: &str) -> Result<Self, TransportError> {
    if [journal, snapshot, head, history_index]
      .iter()
      .any(|name| name.is_empty())
      || HashSet::from([journal, snapshot, head]).len() != 3
    {
      return Err(TransportError::InvalidRequest(
        "テーブル名・GSI名が空、またはテーブル名が重複",
      ));
    }
    Ok(Self {
      journal: journal.into(),
      snapshot: snapshot.into(),
      head: head.into(),
      history_index: history_index.into(),
    })
  }

  fn table(&self, name: &str) -> Option<&'static str> {
    if name == self.journal {
      Some("journal")
    } else if name == self.snapshot {
      Some("snapshot")
    } else if name == self.head {
      Some("head")
    } else {
      None
    }
  }
}

impl fmt::Debug for RequestLayout {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str("RequestLayout")
  }
}

/// 送信直前に観測したAPI名・本文と、内容から判定した段階を表す。
#[derive(Clone)]
pub struct RequestObservation {
  pub api: String,
  pub body: Value,
  pub phase: Option<Phase>,
}

impl fmt::Debug for RequestObservation {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("RequestObservation")
      .field("api", &self.api)
      .field("phase", &self.phase)
      .finish_non_exhaustive()
  }
}

pub(super) struct Action {
  pub target: &'static str,
  pub aid: String,
}

pub(super) struct ParsedRequest {
  pub observation: RequestObservation,
  pub actions: Vec<Action>,
  pub configuration_keys: Vec<ConfigurationKey>,
}

#[derive(Clone)]
pub(super) struct ConfigurationKey {
  pub table_name: String,
  pub table: &'static str,
  pub key: Value,
  pub selector: String,
}

fn invalid(message: &'static str) -> TransportError {
  TransportError::InvalidRequest(message)
}

fn aid(key: &Value) -> Result<&str, TransportError> {
  key
    .pointer("/aid/S")
    .and_then(Value::as_str)
    .ok_or_else(|| invalid("aidがS属性ではない"))
}

fn number(key: &Value, name: &str) -> Result<i128, TransportError> {
  let written = key
    .get(name)
    .and_then(|attribute| attribute.get("N"))
    .and_then(Value::as_str)
    .ok_or_else(|| invalid("キーがN属性ではない"))?;
  let value: Value = serde_json::from_str(written).map_err(|_| invalid("N属性が数値ではない"))?;
  to_integer(&value).ok_or_else(|| invalid("N属性が整数ではない"))
}

fn configuration_key(table: &str, key: &Value) -> Result<bool, TransportError> {
  if aid(key)? != "__config__" {
    return Ok(false);
  }
  let valid = match table {
    "journal" => number(key, "seq_nr")? == 0,
    "snapshot" => number(key, "skey")? == 0,
    "head" => true,
    _ => unreachable!("照合済みテーブルだけを受け取る"),
  };
  if !valid {
    return Err(invalid("設定項目のソートキーが0ではない"));
  }
  Ok(true)
}

fn transactions(layout: &RequestLayout, body: &Value) -> Result<(Option<Phase>, Vec<Action>), TransportError> {
  let items = body
    .get("TransactItems")
    .and_then(Value::as_array)
    .filter(|items| !items.is_empty())
    .ok_or_else(|| invalid("TransactItemsが空、または配列ではない"))?;
  let mut actions = Vec::new();
  let mut configuration = None;
  let mut targets = HashSet::new();
  for item in items {
    let object = item
      .as_object()
      .filter(|object| object.len() == 1)
      .ok_or_else(|| invalid("アクションが一意ではない"))?;
    let (operation, action) = object.iter().next().expect("1要素を確認済み");
    let table_name = action
      .get("TableName")
      .and_then(Value::as_str)
      .ok_or_else(|| invalid("TableNameがない"))?;
    let Some(table) = layout.table(table_name) else {
      return Ok((None, Vec::new()));
    };
    let key = match operation.as_str() {
      "Put" => action.get("Item"),
      "Update" if table == "head" => action.get("Key"),
      _ => return Err(invalid("対象テーブルのアクションがPutまたはheadのUpdateではない")),
    }
    .ok_or_else(|| invalid("アクションの項目・キーがない"))?;
    let is_config = configuration_key(table, key)?;
    if configuration.is_some_and(|previous| previous != is_config) {
      return Err(invalid("設定項目と通常項目が混在"));
    }
    configuration = Some(is_config);
    let target = if is_config {
      if operation != "Put" {
        return Err(invalid("設定作成がPutではない"));
      }
      match table {
        "journal" => "configuration:journal",
        "snapshot" => "configuration:snapshot",
        "head" => "configuration:head",
        _ => unreachable!(),
      }
    } else {
      match table {
        "journal" if number(key, "seq_nr")? > 0 => "journal",
        "head" => "head",
        "snapshot" => match number(key, "skey")? {
          0 => "current-snapshot",
          value if value > 0 => "history-snapshot",
          _ => return Err(invalid("snapshotのskeyが負数")),
        },
        _ => return Err(invalid("journalのseq_nrが正数ではない")),
      }
    };
    if !targets.insert(target) {
      return Err(invalid("アクションの対象名が重複"));
    }
    actions.push(Action {
      target,
      aid: aid(key)?.into(),
    });
  }
  Ok((
    Some(if configuration == Some(true) {
      Phase::ConfigurationCreate
    } else {
      Phase::Commit
    }),
    actions,
  ))
}

fn batch_get(layout: &RequestLayout, body: &Value) -> Result<(Option<Phase>, Vec<ConfigurationKey>), TransportError> {
  let tables = body
    .get("RequestItems")
    .and_then(Value::as_object)
    .filter(|tables| !tables.is_empty())
    .ok_or_else(|| invalid("RequestItemsが空、またはオブジェクトではない"))?;
  let mut configuration = None;
  let mut configuration_keys = Vec::new();
  for (name, request) in tables {
    let Some(table) = layout.table(name) else {
      return Ok((None, Vec::new()));
    };
    let keys = request
      .get("Keys")
      .and_then(Value::as_array)
      .filter(|keys| !keys.is_empty())
      .ok_or_else(|| invalid("Keysが空、または配列ではない"))?;
    for key in keys {
      let is_config = configuration_key(table, key)?;
      if configuration.is_some_and(|previous| previous != is_config) {
        return Err(invalid("設定と通常の読み取りが混在"));
      }
      if !is_config && (table == "journal" || (table == "snapshot" && number(key, "skey")? != 0)) {
        return Ok((None, Vec::new()));
      }
      if is_config {
        configuration_keys.push(ConfigurationKey {
          table_name: name.clone(),
          table,
          key: key.clone(),
          selector: if table == "head" {
            "head:__config__".into()
          } else {
            format!("{table}:__config__:0")
          },
        });
      }
      configuration = Some(is_config);
    }
  }
  Ok((
    Some(if configuration == Some(true) {
      Phase::ConfigurationRead
    } else {
      Phase::ReadSnapshot
    }),
    configuration_keys,
  ))
}

fn phase(layout: &RequestLayout, api: &str, body: &Value) -> Result<Option<Phase>, TransportError> {
  match api {
    "Query" => {
      let table = body.get("TableName").and_then(Value::as_str);
      let index = body.get("IndexName").and_then(Value::as_str);
      if table == Some(layout.journal.as_str()) && index.is_none() {
        Ok(Some(Phase::ReadEvents))
      } else if table == Some(layout.snapshot.as_str()) && index == Some(layout.history_index.as_str()) {
        Ok(Some(Phase::RetentionQuery))
      } else {
        Ok(None)
      }
    }
    "BatchWriteItem" => {
      let tables = body
        .get("RequestItems")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("RequestItemsがない"))?;
      if tables.len() != 1 {
        return Ok(None);
      }
      let Some(items) = tables
        .get(&layout.snapshot)
        .and_then(Value::as_array)
        .filter(|items| !items.is_empty())
      else {
        return Ok(None);
      };
      for item in items {
        let Some(key) = item.pointer("/DeleteRequest/Key") else {
          return Ok(None);
        };
        if item.as_object().is_none_or(|object| object.len() != 1)
          || aid(key)? == "__config__"
          || number(key, "skey")? <= 0
        {
          return Ok(None);
        }
      }
      Ok(Some(Phase::RetentionDelete))
    }
    "UpdateItem" => {
      if body.get("TableName").and_then(Value::as_str) != Some(layout.snapshot.as_str()) {
        return Ok(None);
      }
      let key = body.get("Key").ok_or_else(|| invalid("UpdateItemのKeyがない"))?;
      if aid(key)? == "__config__" || number(key, "skey")? <= 0 {
        return Ok(None);
      }
      let aliases = body.get("ExpressionAttributeNames").and_then(Value::as_object);
      let expression = body
        .get("UpdateExpression")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("UpdateExpressionがない"))?;
      let separated = expression.replace('=', " = ");
      let tokens: Vec<&str> = separated
        .split(|c: char| c.is_whitespace() || matches!(c, ',' | '(' | ')'))
        .filter(|token| !token.is_empty())
        .collect();
      let names: Vec<&str> = tokens
        .iter()
        .map(|token| {
          aliases
            .and_then(|aliases| aliases.get(*token))
            .and_then(Value::as_str)
            .unwrap_or(token)
        })
        .collect();
      let mut clause = "";
      let mut sets_ttl = false;
      let mut removes_active = false;
      for (position, name) in names.iter().enumerate() {
        if matches!(*name, "SET" | "REMOVE" | "ADD" | "DELETE") {
          clause = name;
        } else {
          sets_ttl |= clause == "SET" && *name == "ttl" && names.get(position + 1) == Some(&"=");
          removes_active |= clause == "REMOVE" && *name == "active_history_seq_nr";
        }
      }
      Ok((sets_ttl && removes_active).then_some(Phase::RetentionMark))
    }
    _ => Ok(None),
  }
}

pub(super) fn parse(layout: &RequestLayout, request: &HttpRequest) -> Result<ParsedRequest, TransportError> {
  let api = request
    .headers()
    .get("x-amz-target")
    .and_then(|target| target.strip_prefix("DynamoDB_20120810."))
    .ok_or_else(|| invalid("DynamoDBのAPI名がない"))?;
  let bytes = request
    .body()
    .bytes()
    .ok_or_else(|| invalid("要求本文がバッファではない"))?;
  let body: Value = serde_json::from_slice(bytes).map_err(|_| invalid("要求本文がJSONではない"))?;
  let (phase, actions, configuration_keys) = if api == "TransactWriteItems" {
    let (phase, actions) = transactions(layout, &body)?;
    (phase, actions, Vec::new())
  } else if api == "BatchGetItem" {
    let (phase, keys) = batch_get(layout, &body)?;
    (phase, Vec::new(), keys)
  } else {
    (phase(layout, api, &body)?, Vec::new(), Vec::new())
  };
  Ok(ParsedRequest {
    observation: RequestObservation {
      api: api.into(),
      body,
      phase,
    },
    actions,
    configuration_keys,
  })
}
