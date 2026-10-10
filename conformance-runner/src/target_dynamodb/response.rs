use std::collections::{HashMap, HashSet};

use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_runtime_api::http::{Response, StatusCode};
use aws_smithy_types::body::SdkBody;
use serde_json::{json, Value};

use super::request::{ConfigurationKey, ParsedRequest};
use super::transport::{replace_body, TransportError};
use crate::fault::{Fault, FaultKind, Phase};
use crate::number::to_integer;

const QUERY_PAGE_BYTES: usize = 1048576;

fn journal_item_bytes(item: &Value) -> Result<usize, TransportError> {
  item
    .as_object()
    .ok_or_else(|| invalid("イベントQueryの実項目がオブジェクトではない"))?
    .iter()
    .try_fold(0, |size, (name, value)| {
      let attribute = value
        .as_object()
        .filter(|attribute| attribute.len() == 1)
        .ok_or_else(|| invalid("イベントQueryの実属性型が一意ではない"))?;
      let (kind, value) = attribute.iter().next().expect("1型を確認済み");
      let written = value
        .as_str()
        .ok_or_else(|| invalid("journalの実S/N/B属性が文字列ではない"))?;
      let bytes = match kind.as_str() {
        "S" | "N" => written.len(),
        "B" => aws_smithy_types::base64::decode(written)
          .map_err(|_| invalid("journalの実B属性がbase64ではない"))?
          .len(),
        _ => return Err(invalid("journalの実属性がS/N/Bではない")),
      };
      Ok(size + name.len() + bytes)
    })
}

/// 実Localの越境応答だけを、実項目の連続prefixと実キーで1MiB頁へ補正する。
pub(super) fn event_page_response(
  request: &ParsedRequest,
  response: HttpResponse,
) -> Result<HttpResponse, TransportError> {
  if request.observation.phase != Some(Phase::ReadEvents) || !response.status().is_success() {
    return Ok(response);
  }
  let mut body: Value = serde_json::from_slice(
    response
      .body()
      .bytes()
      .ok_or_else(|| invalid("イベントQueryの実応答がバッファではない"))?,
  )
  .map_err(|_| invalid("イベントQueryの実応答がJSONではない"))?;
  let Some(items) = body.get("Items") else {
    return Ok(response);
  };
  let items = items
    .as_array()
    .ok_or_else(|| invalid("イベントQueryの実Itemsが配列ではない"))?;
  let mut bytes = 0;
  let mut prefix = 0;
  for item in items {
    bytes += journal_item_bytes(item)?;
    if bytes <= QUERY_PAGE_BYTES {
      prefix += 1;
    }
  }
  if prefix == items.len() {
    return Ok(response);
  }
  if prefix == 0 {
    return Err(invalid("実journalの1項目だけで1MiBを超えている"));
  }
  let last = &items[prefix - 1];
  let aid = last
    .get("aid")
    .filter(|value| value.get("S").is_some_and(Value::is_string))
    .ok_or_else(|| invalid("補正頁の実aidキーがない"))?;
  let sequence = last
    .get("seq_nr")
    .filter(|value| value.get("N").is_some_and(Value::is_string))
    .ok_or_else(|| invalid("補正頁の実seq_nrキーがない"))?;
  let continuation = json!({"aid":aid,"seq_nr":sequence});
  let returned = items[..prefix].to_vec();
  body["Items"] = json!(returned);
  body["LastEvaluatedKey"] = continuation;
  for count in ["Count", "ScannedCount"] {
    if body.get(count).is_some() {
      body[count] = json!(prefix);
    }
  }
  Ok(replace_body(&response, body))
}

/// 宣言したページの実キーを本体から読み、古い GSI 応答を組み立てる。
pub(super) async fn history_page_response(
  request: &ParsedRequest,
  fault: &Fault,
  raw: &aws_sdk_dynamodb::Client,
  position: usize,
  upstream: &HttpResponse,
) -> Result<(HttpResponse, Option<Value>), TransportError> {
  if !upstream.status().is_success() {
    return Err(invalid("履歴応答の実転送が成功していない"));
  }
  let pages = fault
    .details
    .get("history_pages")
    .and_then(Value::as_array)
    .filter(|pages| !pages.is_empty())
    .ok_or_else(|| invalid("history_pagesが空、または配列ではない"))?;
  let page = pages
    .get(position)
    .and_then(Value::as_array)
    .ok_or_else(|| invalid("履歴ページが配列ではない"))?;
  let body = &request.observation.body;
  let table = body
    .get("TableName")
    .and_then(Value::as_str)
    .ok_or_else(|| invalid("履歴の表名がない"))?;
  let aid = body
    .pointer("/ExpressionAttributeValues/:aid/S")
    .and_then(Value::as_str)
    .ok_or_else(|| invalid("履歴のaid束縛がない"))?;
  let mut items = Vec::new();
  for number in page {
    let number = to_integer(number)
      .and_then(|number| u64::try_from(number).ok())
      .filter(|number| (1..=event_store_adapter_rs::seq_nr::SEQ_NR_MAX).contains(number))
      .ok_or_else(|| invalid("履歴番号が正の範囲内整数ではない"))?;
    let saved = raw
      .get_item()
      .table_name(table)
      .key("aid", aws_sdk_dynamodb::types::AttributeValue::S(aid.into()))
      .key("skey", aws_sdk_dynamodb::types::AttributeValue::N(number.to_string()))
      .consistent_read(true)
      .send()
      .await
      .map_err(|_| invalid("履歴の実項目を読めない"))?
      .item
      .ok_or_else(|| invalid("宣言した履歴の実項目がない"))?;
    let saved_aid = saved
      .get("aid")
      .and_then(|attribute| attribute.as_s().ok())
      .filter(|saved_aid| saved_aid.as_str() == aid)
      .ok_or_else(|| invalid("履歴の実aidが一致しない"))?;
    let skey = saved
      .get("skey")
      .and_then(|attribute| attribute.as_n().ok())
      .filter(|skey| skey.parse::<u64>().ok() == Some(number))
      .ok_or_else(|| invalid("履歴の実skeyが一致しない"))?;
    items.push(json!({"aid": {"S": saved_aid}, "skey": {"N": skey}, "active_history_seq_nr": {"N": skey}}));
  }
  let continuation = if position + 1 < pages.len() {
    Some(
      items
        .last()
        .cloned()
        .ok_or_else(|| invalid("続く履歴ページの実キーがない"))?,
    )
  } else {
    None
  };
  let mut delivered = json!({"Items": items});
  if let Some(key) = &continuation {
    delivered["LastEvaluatedKey"] = key.clone();
  }
  let mut response = Response::new(upstream.status(), SdkBody::from(delivered.to_string()));
  *response.headers_mut() = upstream.headers().clone();
  response.headers_mut().remove("content-length");
  Ok((response, continuation))
}

fn invalid(message: &'static str) -> TransportError {
  TransportError::InvalidFault(message)
}

pub(super) fn validate_history_omission(
  request: &ParsedRequest,
  fault: &Fault,
  responses: &[Value],
) -> Result<(), TransportError> {
  if fault.details.get("omit_just_written_history").and_then(Value::as_bool) != Some(true) {
    return Ok(());
  }
  let table = request.observation.body.get("TableName").and_then(Value::as_str);
  let aid = request.observation.body.pointer("/ExpressionAttributeValues/:aid/S");
  let just_written = responses
    .iter()
    .rev()
    .find_map(|response| {
      if response["phase"] != "commit" || response.pointer("/upstream/status") != Some(&json!(200)) {
        return None;
      }
      response
        .pointer("/upstream_request/TransactItems")?
        .as_array()?
        .iter()
        .find_map(|action| {
          let put = action.get("Put")?;
          if put.get("TableName").and_then(Value::as_str) != table || put.pointer("/Item/aid/S") != aid {
            return None;
          }
          put
            .pointer("/Item/skey/N")?
            .as_str()?
            .parse::<u64>()
            .ok()
            .filter(|seq| *seq > 0)
        })
    })
    .ok_or_else(|| invalid("省略対象の履歴を実確定要求から確認できない"))?;
  let pages = fault
    .details
    .get("history_pages")
    .and_then(Value::as_array)
    .ok_or_else(|| invalid("history_pagesが配列ではない"))?;
  for page in pages {
    for number in page.as_array().ok_or_else(|| invalid("履歴ページが配列ではない"))? {
      if to_integer(number) == Some(i128::from(just_written)) {
        return Err(invalid("省略指定の応答計画に今書いた履歴が含まれる"));
      }
    }
  }
  Ok(())
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
  BatchGet {
    responses: Vec<ConfigurationKey>,
    unprocessed: Vec<ConfigurationKey>,
  },
}

pub(super) fn prepare_response(request: &ParsedRequest, fault: &Fault) -> Result<ResponseReplacement, TransportError> {
  if fault.kind != FaultKind::SdkResponse {
    return error_response(request, fault).map(ResponseReplacement::Error);
  }
  if !matches!(
    request.observation.phase,
    Some(Phase::ConfigurationRead | Phase::ReadSnapshot)
  ) {
    return Err(invalid("BatchGetItem読み取り以外の応答計画"));
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
    if !matches!(
      (request.observation.phase, table.as_str(), source.as_str()),
      (Some(Phase::ConfigurationRead), _, Some("seed-config"))
        | (Some(Phase::ReadSnapshot), "head", Some("stored-head"))
    ) {
      return Err(invalid("応答の参照が段階と一致しない"));
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
  Ok(ResponseReplacement::BatchGet { responses, unprocessed })
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
      Self::BatchGet { responses, unprocessed } => (responses, unprocessed),
    };
    if !upstream.status().is_success() {
      return Err(invalid("設定応答の実転送が成功していない"));
    }
    let bytes = upstream
      .body()
      .bytes()
      .ok_or_else(|| invalid("設定の実応答がバッファではない"))?;
    let mut body: Value = serde_json::from_slice(bytes).map_err(|_| invalid("設定の実応答がJSONではない"))?;
    if body
      .get("UnprocessedKeys")
      .and_then(Value::as_object)
      .is_some_and(|pending| !pending.is_empty())
    {
      return Err(invalid("設定の実応答に未処理キーがある"));
    }
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

#[cfg(test)]
#[path = "response_test.rs"]
mod tests;
