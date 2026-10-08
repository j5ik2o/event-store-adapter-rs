use std::collections::{HashMap, HashSet};

use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_runtime_api::http::{Response, StatusCode};
use aws_smithy_types::body::SdkBody;
use serde_json::{json, Value};

use super::request::{ConfigurationKey, ParsedRequest};
use super::transport::TransportError;
use crate::fault::{Fault, FaultKind, Phase};
use crate::number::to_integer;

fn invalid(message: &'static str) -> TransportError {
  TransportError::InvalidFault(message)
}

fn cancellation_reasons(request: &ParsedRequest, details: &Value) -> Result<Vec<Value>, TransportError> {
  if request.observation.api != "TransactWriteItems" || request.actions.is_empty() {
    return Err(invalid("取り消し理由の対象がトランザクションではない"));
  }
  let reasons = details
    .get("cancellation_reasons")
    .and_then(Value::as_array)
    .ok_or_else(|| invalid("cancellation_reasonsがない"))?;
  let mut by_target = HashMap::new();
  for reason in reasons {
    let target = reason
      .get("target")
      .and_then(Value::as_str)
      .ok_or_else(|| invalid("取り消し理由のtargetがない"))?;
    if by_target.insert(target, reason).is_some() {
      return Err(invalid("取り消し理由のtargetが重複"));
    }
  }
  if by_target.len() != request.actions.len() {
    return Err(invalid("取り消し理由と実アクションの数が不一致"));
  }
  request
    .actions
    .iter()
    .map(|action| {
      let reason = by_target
        .remove(action.target)
        .ok_or_else(|| invalid("取り消し理由の対象が実アクションに一致しない"))?;
      let code = reason
        .get("code")
        .and_then(Value::as_str)
        .filter(|code| !code.is_empty())
        .ok_or_else(|| invalid("取り消し理由のcodeがない"))?;
      let mut result = json!({"Code": code});
      if action.target == "head" && code == "ConditionalCheckFailed" {
        let old = reason
          .get("old_head_seq_nr")
          .ok_or_else(|| invalid("headの旧seq_nrがない"))?;
        if !old.is_null() {
          let sequence = to_integer(old).ok_or_else(|| invalid("headの旧seq_nrが整数ではない"))?;
          result["Item"] = json!({"aid": {"S": action.aid}, "seq_nr": {"N": sequence.to_string()}});
        }
      } else if reason.get("old_head_seq_nr").is_some() {
        return Err(invalid("旧ヘッド情報の対象がheadの条件不成立ではない"));
      }
      Ok(result)
    })
    .collect()
}

pub(super) fn error_response(request: &ParsedRequest, fault: &Fault) -> Result<HttpResponse, TransportError> {
  let code = match fault.kind {
    FaultKind::StorageError => "InternalServerError",
    FaultKind::SdkError => fault
      .details
      .get("code")
      .and_then(Value::as_str)
      .filter(|code| !code.is_empty())
      .ok_or_else(|| invalid("SDKエラーのcodeがない"))?,
    _ => return Err(TransportError::UnsupportedFault),
  };
  let mut body = json!({"__type": code});
  if let Some(message) = fault.details.get("message") {
    if !message.is_string() {
      return Err(invalid("messageが文字列ではない"));
    }
    body["Message"] = message.clone();
  }
  if code == "TransactionCanceledException" {
    body["CancellationReasons"] = Value::Array(cancellation_reasons(request, &fault.details)?);
  } else if fault.details.get("cancellation_reasons").is_some() {
    return Err(invalid(
      "取り消し理由がTransactionCanceledException以外に指定されている",
    ));
  }
  let status = if code == "InternalServerError" { 500 } else { 400 };
  let mut response = Response::new(
    StatusCode::try_from(status).expect("有効な固定HTTPステータス"),
    SdkBody::from(body.to_string()),
  );
  response
    .headers_mut()
    .insert("content-type", "application/x-amz-json-1.0");
  response
    .headers_mut()
    .try_insert("x-amzn-errortype", code.to_string())
    .map_err(|_| invalid("SDKエラーのcodeがHTTPヘッダーの値ではない"))?;
  Ok(response)
}

pub(super) enum ResponseReplacement {
  Error(HttpResponse),
  Configuration {
    responses: Vec<ConfigurationKey>,
    unprocessed: Vec<ConfigurationKey>,
  },
}

pub(super) fn prepare_response(request: &ParsedRequest, fault: &Fault) -> Result<ResponseReplacement, TransportError> {
  if fault.kind != FaultKind::SdkResponse {
    return error_response(request, fault).map(ResponseReplacement::Error);
  }
  if request.observation.phase != Some(Phase::ConfigurationRead) {
    return Err(invalid("設定読み取り以外の応答計画"));
  }
  let declared_responses = fault
    .details
    .get("responses")
    .and_then(Value::as_object)
    .ok_or_else(|| invalid("responsesがオブジェクトではない"))?;
  let declared_unprocessed = fault
    .details
    .get("unprocessed_keys")
    .and_then(Value::as_array)
    .ok_or_else(|| invalid("unprocessed_keysが配列ではない"))?;
  let keys = &request.configuration_keys;
  let mut selected = HashSet::new();
  let mut responses = Vec::new();
  for (table, source) in declared_responses {
    if source.as_str() != Some("seed-config") {
      return Err(invalid("設定応答の参照がseed-configではない"));
    }
    let key = keys
      .iter()
      .find(|key| key.table == table)
      .ok_or_else(|| invalid("設定応答の対象が送信キーにない"))?;
    if !selected.insert(key.selector.as_str()) {
      return Err(invalid("設定応答の対象が重複"));
    }
    responses.push(key.clone());
  }
  let mut unprocessed = Vec::new();
  for selector in declared_unprocessed {
    let selector = selector.as_str().ok_or_else(|| invalid("未処理キーが文字列ではない"))?;
    let key = keys
      .iter()
      .find(|key| key.selector == selector)
      .ok_or_else(|| invalid("未処理キーが送信キーにない"))?;
    if !selected.insert(key.selector.as_str()) {
      return Err(invalid("設定応答と未処理キーが重複"));
    }
    unprocessed.push(key.clone());
  }
  Ok(ResponseReplacement::Configuration { responses, unprocessed })
}

fn contains_key(item: &Value, key: &Value) -> bool {
  key.as_object().is_some_and(|key| {
    key.iter().all(|(name, value)| {
      let Some(actual) = item.get(name) else {
        return false;
      };
      if let (Some(expected), Some(actual)) = (
        value.get("N").and_then(Value::as_str),
        actual.get("N").and_then(Value::as_str),
      ) {
        let integer = |written: &str| {
          serde_json::from_str::<Value>(written)
            .ok()
            .and_then(|value| to_integer(&value))
        };
        matches!((integer(expected), integer(actual)), (Some(expected), Some(actual)) if expected == actual)
      } else {
        value == actual
      }
    })
  })
}

impl ResponseReplacement {
  pub(super) fn apply(self, upstream: &HttpResponse) -> Result<HttpResponse, TransportError> {
    let (responses, unprocessed) = match self {
      Self::Error(response) => return Ok(response),
      Self::Configuration { responses, unprocessed } => (responses, unprocessed),
    };
    if !upstream.status().is_success() {
      return Err(invalid("設定応答の実転送が成功していない"));
    }
    let bytes = upstream
      .body()
      .bytes()
      .ok_or_else(|| invalid("設定の実応答がバッファではない"))?;
    let mut body: Value = serde_json::from_slice(bytes).map_err(|_| invalid("設定の実応答がJSONではない"))?;
    let mut returned = serde_json::Map::new();
    for key in responses {
      let items = body
        .get("Responses")
        .and_then(|tables| tables.get(&key.table_name))
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("seed-configの実応答がない"))?;
      let mut matching = items.iter().filter(|item| contains_key(item, &key.key));
      let item = matching.next().ok_or_else(|| invalid("seed-configの実項目がない"))?;
      if matching.next().is_some() {
        return Err(invalid("seed-configの実項目が重複"));
      }
      returned.insert(key.table_name, json!([item]));
    }
    let mut pending = serde_json::Map::new();
    for key in unprocessed {
      pending.insert(key.table_name, json!({"Keys": [key.key], "ConsistentRead": true}));
    }
    let object = body
      .as_object_mut()
      .ok_or_else(|| invalid("設定の実応答がオブジェクトではない"))?;
    object.insert("Responses".into(), Value::Object(returned));
    object.insert("UnprocessedKeys".into(), Value::Object(pending));
    let mut response = Response::new(upstream.status(), SdkBody::from(body.to_string()));
    *response.headers_mut() = upstream.headers().clone();
    response.headers_mut().remove("content-length");
    Ok(response)
  }
}
