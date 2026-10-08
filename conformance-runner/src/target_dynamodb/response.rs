use std::collections::HashMap;

use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_runtime_api::http::{Response, StatusCode};
use aws_smithy_types::body::SdkBody;
use serde_json::{json, Value};

use super::request::ParsedRequest;
use super::transport::TransportError;
use crate::fault::{Fault, FaultKind};
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
