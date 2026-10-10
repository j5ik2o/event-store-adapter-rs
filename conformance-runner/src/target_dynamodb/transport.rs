use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use aws_sdk_dynamodb::{config::Builder, Client};
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::http::{
  HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpClient, SharedHttpConnector,
};
use aws_smithy_runtime_api::client::interceptors::context::BeforeTransmitInterceptorContextRef;
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::config_bag::ConfigBag;
use aws_smithy_types::retry::RetryConfig;
use aws_smithy_types::{body::SdkBody, byte_stream::ByteStream};
use serde_json::{json, Value};

use super::request::{parse, RequestLayout, RequestObservation};
use super::response::{
  error_response, event_page_response, history_page_response, prepare_response, validate_history_omission,
  ResponseReplacement,
};
use crate::fault::{FaultApplication, FaultKind, FaultPlan, Injection, OperationFaults, Phase, UnfiredFault};

/// 要求層の登録・解析・操作境界の失敗を表す。本文や障害詳細を含めない。
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
  #[error("要求の解析失敗: {0}")]
  InvalidRequest(&'static str),
  #[error("障害の応答構築失敗: {0}")]
  InvalidFault(&'static str),
  #[error("この要求層では未接続の障害")]
  UnsupportedFault,
  #[error("操作が既に開始されている")]
  OperationActive,
}

struct ActiveOperation {
  identity: Arc<()>,
  faults: OperationFaults,
  requests: Vec<RequestObservation>,
  responses: Vec<Value>,
  history: Option<HistoryContinuation>,
  interleaved_write: Option<InterleavedWrite>,
}

type InterleavedWrite = Arc<dyn Fn(Value) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> + Send + Sync>;

struct HistoryContinuation {
  fault: crate::fault::Fault,
  position: usize,
  key: Value,
}

struct State {
  layout: RequestLayout,
  active: Mutex<Option<ActiveOperation>>,
  history_client: Option<Client>,
}

/// 操作の観測記録と、未発火・回数未達の障害を返す。
pub struct OperationReport {
  pub requests: Vec<RequestObservation>,
  pub responses: Vec<Value>,
  pub unfired: Vec<UnfiredFault>,
  pub applications: Vec<FaultApplication>,
  pub unfinished_history: bool,
}

impl fmt::Debug for OperationReport {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("OperationReport")
      .field("request_count", &self.requests.len())
      .field("unfired", &self.unfired)
      .finish()
  }
}

/// 同じSDK Clientで操作ごとの障害を登録し、要求を観測・差し替えする。
#[derive(Clone)]
pub struct FaultTransport {
  state: Arc<State>,
}

impl fmt::Debug for FaultTransport {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str("FaultTransport")
  }
}

impl FaultTransport {
  /// 実テーブルの配置を使う要求層を作る。
  pub fn new(layout: RequestLayout) -> Self {
    Self {
      state: Arc::new(State {
        layout,
        active: Mutex::new(None),
        history_client: None,
      }),
    }
  }

  /// 保存済みの実キーを根拠にする履歴ページ応答計画を接続する。
  pub fn new_with_history_client(layout: RequestLayout, history_client: Client) -> Self {
    let mut transport = Self::new(layout);
    Arc::get_mut(&mut transport.state)
      .expect("生成直後の要求層")
      .history_client = Some(history_client);
    transport
  }

  /// 観測・HTTP差し替え・再試行無効化を同じ設定経路で組み込む。
  pub fn client(&self, builder: Builder, upstream: SharedHttpClient) -> Client {
    Client::from_conf(
      builder
        .interceptor(Observer {
          state: self.state.clone(),
        })
        .http_client(FaultHttpClient {
          state: self.state.clone(),
          upstream,
        })
        .retry_config(RetryConfig::disabled())
        .build(),
    )
  }

  /// 指定操作の計数を開始する。未接続の種類を成功扱いせず拒否する。
  pub fn begin_operation(&self, plan: &FaultPlan, operation: u32) -> Result<OperationGuard, TransportError> {
    if plan
      .faults()
      .iter()
      .filter(|fault| fault.operation == operation)
      .any(|fault| {
        !(matches!(fault.kind, FaultKind::StorageError | FaultKind::SdkError)
          || (fault.kind == FaultKind::SerializationError
            && matches!(
              fault.phase,
              Phase::SerializeEvent | Phase::SerializeSnapshot | Phase::DeserializeEvent | Phase::DeserializeSnapshot
            )
            && fault.injection == Injection::ReplaceRequest)
          || (fault.kind == FaultKind::ReadInterleave
            && fault.phase == Phase::ReadSnapshot
            && fault.injection == Injection::ReplaceResponse
            && self.state.history_client.is_some())
          || (fault.kind == FaultKind::SdkResponse
            && fault.phase == Phase::RetentionDelete
            && fault.details.get("unprocessed_first_n").is_some()
            && fault.injection == Injection::ReplaceRequest)
          || (fault.kind == FaultKind::SdkResponse
            && (matches!(fault.phase, Phase::ConfigurationRead | Phase::ReadSnapshot)
              || (fault.phase == Phase::RetentionQuery
                && fault.details.get("history_pages").is_some()
                && self.state.history_client.is_some()))
            && fault.injection == Injection::ReplaceResponse))
          || (fault.details.get("install_items").is_some() && self.state.history_client.is_none())
      })
    {
      return Err(TransportError::UnsupportedFault);
    }
    let mut active = self.state.active.lock().expect("操作状態のロック");
    if active.is_some() {
      return Err(TransportError::OperationActive);
    }
    *active = Some(ActiveOperation {
      identity: Arc::new(()),
      faults: plan.begin_operation(operation),
      requests: Vec::new(),
      responses: Vec::new(),
      history: None,
      interleaved_write: None,
    });
    Ok(OperationGuard {
      state: self.state.clone(),
      finished: false,
    })
  }

  /// 同じStoreへの追記を、操作identityを保持したまま差し込みへ接続する。
  pub fn interleaved_write<F, Fut>(&self, write: F) -> Result<(), TransportError>
  where
    F: Fn(Value) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), String>> + Send + 'static, {
    let mut active = self.state.active.lock().expect("操作状態のロック");
    let active = active
      .as_mut()
      .ok_or(TransportError::InvalidFault("操作が開始されていない"))?;
    active.interleaved_write = Some(Arc::new(move |value| Box::pin(write(value))));
    Ok(())
  }

  /// HTTP差し込みと同じOperationFaultsでserializer失敗を数える。
  pub fn inject_serialization(&self, phase: Phase) -> Result<(), event_store_adapter_rs::next::error::EventStoreError> {
    use event_store_adapter_rs::next::error::{EventStoreError, SerializationPhase};
    let mut active = self.state.active.lock().expect("操作状態のロック");
    let Some(fault) = active
      .as_mut()
      .and_then(|active| active.faults.start_application(phase))
    else {
      return Ok(());
    };
    let phase = match phase {
      Phase::SerializeEvent => SerializationPhase::SerializeEvent,
      Phase::SerializeSnapshot => SerializationPhase::SerializeSnapshot,
      Phase::DeserializeEvent => SerializationPhase::DeserializeEvent,
      Phase::DeserializeSnapshot => SerializationPhase::DeserializeSnapshot,
      _ => unreachable!("serializerの4段階だけを受け取る"),
    };
    Err(EventStoreError::Serialization {
      phase,
      source: Box::new(std::io::Error::other(
        fault
          .details
          .get("message")
          .and_then(Value::as_str)
          .unwrap_or("injected fault")
          .to_owned(),
      )),
    })
  }
}

/// 操作終了で計数結果を取り出し、中断時にも登録した障害を解除する。
pub struct OperationGuard {
  state: Arc<State>,
  finished: bool,
}

impl fmt::Debug for OperationGuard {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str("OperationGuard")
  }
}

impl OperationGuard {
  /// 操作状態を解除し、要求記録と未発火の情報を返す。
  pub fn finish(mut self) -> OperationReport {
    let active = self
      .state
      .active
      .lock()
      .expect("操作状態のロック")
      .take()
      .expect("操作ガードが有効");
    self.finished = true;
    OperationReport {
      requests: active.requests,
      responses: active.responses,
      applications: active.faults.applications(),
      unfired: active.faults.finish().err().unwrap_or_default(),
      unfinished_history: active.history.is_some(),
    }
  }
}

impl Drop for OperationGuard {
  fn drop(&mut self) {
    if !self.finished {
      self.state.active.lock().expect("操作状態のロック").take();
    }
  }
}

struct Observer {
  state: Arc<State>,
}

impl fmt::Debug for Observer {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str("DynamoDbObserver")
  }
}

impl Intercept for Observer {
  fn name(&self) -> &'static str {
    "DynamoDbObserver"
  }

  fn read_before_transmit(
    &self,
    context: &BeforeTransmitInterceptorContextRef<'_>,
    _components: &RuntimeComponents,
    _cfg: &mut ConfigBag,
  ) -> Result<(), BoxError> {
    let parsed = parse(&self.state.layout, context.request())?;
    if let Some(active) = self.state.active.lock().expect("操作状態のロック").as_mut() {
      active.requests.push(parsed.observation);
    }
    Ok(())
  }
}

struct FaultHttpClient {
  state: Arc<State>,
  upstream: SharedHttpClient,
}

impl fmt::Debug for FaultHttpClient {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str("DynamoDbFaultHttpClient")
  }
}

impl HttpClient for FaultHttpClient {
  fn http_connector(&self, settings: &HttpConnectorSettings, components: &RuntimeComponents) -> SharedHttpConnector {
    SharedHttpConnector::new(FaultConnector {
      state: self.state.clone(),
      upstream: self.upstream.http_connector(settings, components),
    })
  }
}

struct FaultConnector {
  state: Arc<State>,
  upstream: SharedHttpConnector,
}

enum PreparedReplacement {
  Request(HttpResponse),
  Response(ResponseReplacement),
  History { position: usize },
  Interleave,
  UnprocessedDelete { count: usize },
}

async fn buffer_response(response: &mut HttpResponse) -> Result<Value, ConnectorError> {
  let body = std::mem::replace(response.body_mut(), SdkBody::taken());
  let bytes = ByteStream::new(body)
    .collect()
    .await
    .map_err(|error| ConnectorError::other(Box::new(error), None))?
    .into_bytes();
  let observation = json!({"status": response.status().as_u16(), "body": String::from_utf8_lossy(&bytes)});
  *response.body_mut() = SdkBody::from(bytes);
  Ok(observation)
}

impl fmt::Debug for FaultConnector {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str("DynamoDbFaultConnector")
  }
}

impl HttpConnector for FaultConnector {
  fn call(&self, mut request: HttpRequest) -> HttpConnectorFuture {
    let state = self.state.clone();
    let upstream = self.upstream.clone();
    HttpConnectorFuture::new(async move {
      let connector_error = |error: TransportError| ConnectorError::other(Box::new(error), None);
      let parsed = parse(&state.layout, &request).map_err(connector_error)?;
      let identity = state
        .active
        .lock()
        .expect("操作状態のロック")
        .as_ref()
        .map(|v| v.identity.clone());
      let selected = {
        let mut active = state.active.lock().expect("操作状態のロック");
        parsed.observation.phase.and_then(|phase| {
          active.as_mut().and_then(|active| {
            let fault = active
              .history
              .as_ref()
              .filter(|_| phase == Phase::RetentionQuery)
              .map(|history| history.fault.clone())
              .or_else(|| active.faults.select_application(phase).cloned())?;
            let prepared = if fault.kind == FaultKind::ReadInterleave {
              if active.interleaved_write.is_none() {
                Err(TransportError::InvalidFault(
                  "同じStoreへの差し込み追記が接続されていない",
                ))
              } else {
                Ok(PreparedReplacement::Interleave)
              }
            } else if fault.kind == FaultKind::SdkResponse && fault.phase == Phase::RetentionDelete {
              fault
                .details
                .get("unprocessed_first_n")
                .and_then(Value::as_u64)
                .and_then(|v| usize::try_from(v).ok())
                .map(|count| PreparedReplacement::UnprocessedDelete { count })
                .ok_or(TransportError::InvalidFault("未処理件数が整数ではない"))
            } else if fault.kind == FaultKind::SdkResponse && fault.phase == Phase::RetentionQuery {
              match &active.history {
                Some(history)
                  if history.fault.index == fault.index
                    && parsed.observation.body.get("ExclusiveStartKey") == Some(&history.key) =>
                {
                  Ok(PreparedReplacement::History {
                    position: history.position,
                  })
                }
                Some(_) => Err(TransportError::InvalidFault("履歴ページの続きのキーが一致しない")),
                None if parsed.observation.body.get("ExclusiveStartKey").is_none() => {
                  validate_history_omission(&parsed, &fault, &active.responses)
                    .map(|()| PreparedReplacement::History { position: 0 })
                }
                None => Err(TransportError::InvalidFault(
                  "履歴ページ列の最初の要求に続きのキーがある",
                )),
              }
            } else {
              match fault.injection {
                Injection::ReplaceRequest => error_response(&parsed, &fault).map(PreparedReplacement::Request),
                Injection::ReplaceResponse => prepare_response(&parsed, &fault).map(PreparedReplacement::Response),
              }
            };
            Some((
              fault,
              prepared,
              active.identity.clone(),
              active.interleaved_write.clone(),
            ))
          })
        })
      };
      let Some((fault, prepared, fault_identity, write)) = selected else {
        let mut response = upstream.call(request).await?;
        let original = buffer_response(&mut response).await?;
        response = event_page_response(&parsed, response).map_err(connector_error)?;
        let delivered = json!({"status":response.status().as_u16(),
          "body":String::from_utf8_lossy(response.body().bytes().expect("バッファ済み応答"))});
        if let Some(identity) = identity {
          record(
            &state,
            &identity,
            &parsed,
            None,
            Some(parsed.observation.body.clone()),
            Some(original),
            delivered,
          );
        }
        return Ok(response);
      };
      let prepared = prepared.map_err(connector_error)?;
      if let PreparedReplacement::Request(mut response) = prepared {
        if let Some(items) = fault.details.get("install_items") {
          if fault.phase != Phase::ConfigurationCreate {
            return Err(connector_error(TransportError::InvalidFault(
              "設定作成以外のinstall_items",
            )));
          }
          super::items::seed(
            state.history_client.as_ref().expect("登録時に確認済み"),
            &state.layout,
            items
              .as_array()
              .ok_or_else(|| connector_error(TransportError::InvalidFault("install_itemsが配列ではない")))?,
          )
          .await
          .map_err(|_| connector_error(TransportError::InvalidFault("競合作成の実保存失敗")))?;
        }
        let delivered = buffer_response(&mut response).await?;
        complete(&state, &fault_identity, fault.index);
        record(
          &state,
          &fault_identity,
          &parsed,
          Some(fault.index),
          None,
          None,
          delivered,
        );
        return Ok(response);
      }
      let mut old_head = None;
      if matches!(prepared, PreparedReplacement::Interleave) {
        if fault.details.get("after").and_then(Value::as_str) != Some("capture-head-before-batch")
          || fault.details.get("then").and_then(Value::as_str) != Some("replace-batch-response-head")
        {
          return Err(connector_error(TransportError::InvalidFault(
            "read-interleaveの手順が不正",
          )));
        }
        let head = parsed
          .configuration_keys
          .iter()
          .find(|key| key.table == "head")
          .ok_or_else(|| connector_error(TransportError::InvalidFault("差し込みの要求にheadがない")))?;
        let aid = head
          .key
          .pointer("/aid/S")
          .and_then(Value::as_str)
          .ok_or_else(|| connector_error(TransportError::InvalidFault("差し込みのaidがない")))?;
        let saved = state
          .history_client
          .as_ref()
          .expect("登録時に確認済み")
          .get_item()
          .table_name(&head.table_name)
          .key("aid", aws_sdk_dynamodb::types::AttributeValue::S(aid.into()))
          .consistent_read(true)
          .send()
          .await
          .map_err(|_| connector_error(TransportError::InvalidFault("旧headの実取得失敗")))?;
        old_head = Some((
          head.table_name.clone(),
          saved.item.map(|item| super::items::wire_item(&item)),
        ));
        write.expect("登録時に確認済み")(
          fault
            .details
            .get("interleaved_operation")
            .ok_or_else(|| connector_error(TransportError::InvalidFault("差し込み追記の宣言がない")))?
            .clone(),
        )
        .await
        .map_err(|_| connector_error(TransportError::InvalidFault("同じStoreの差し込み追記が失敗")))?;
      }
      let mut transferred = parsed.observation.body.clone();
      let mut unprocessed = None;
      if let PreparedReplacement::UnprocessedDelete { count } = prepared {
        let tables = transferred
          .get_mut("RequestItems")
          .and_then(Value::as_object_mut)
          .ok_or_else(|| connector_error(TransportError::InvalidFault("削除のRequestItemsがない")))?;
        if tables.len() != 1 {
          return Err(connector_error(TransportError::InvalidFault("削除表が一意ではない")));
        }
        let (table, items) = tables.iter_mut().next().expect("1表を確認済み");
        let items = items
          .as_array_mut()
          .ok_or_else(|| connector_error(TransportError::InvalidFault("削除列が配列ではない")))?;
        if count > items.len() {
          return Err(connector_error(TransportError::InvalidFault(
            "未処理件数が削除件数を超える",
          )));
        }
        let pending: Vec<Value> = items.drain(..count).collect();
        unprocessed = Some(json!({table.clone():pending}));
        let bytes = transferred.to_string();
        *request.body_mut() = SdkBody::from(bytes.clone());
        request.headers_mut().remove("content-length");
        request.headers_mut().insert("content-length", bytes.len().to_string());
      }
      let empty_transfer = transferred
        .get("RequestItems")
        .and_then(Value::as_object)
        .is_some_and(|tables| tables.values().all(|v| v.as_array().is_some_and(Vec::is_empty)));
      let mut upstream_response = if empty_transfer {
        aws_smithy_runtime_api::http::Response::new(
          aws_smithy_runtime_api::http::StatusCode::try_from(200).expect("固定ステータス"),
          SdkBody::from("{}"),
        )
      } else {
        upstream.call(request).await?
      };
      let upstream_observation = buffer_response(&mut upstream_response).await?;
      let history = if let PreparedReplacement::History { position } = &prepared {
        Some((
          history_page_response(
            &parsed,
            &fault,
            state.history_client.as_ref().expect("登録時に確認済み"),
            *position,
            &upstream_response,
          )
          .await
          .map_err(connector_error)?,
          *position,
        ))
      } else {
        None
      };
      let mut active = state.active.lock().expect("操作状態のロック");
      let Some(active) = active.as_mut().filter(|v| Arc::ptr_eq(&v.identity, &fault_identity)) else {
        return event_page_response(&parsed, upstream_response).map_err(connector_error);
      };
      let mut applied_index = fault.index;
      let delivered_response = if let Some(((response, continuation), position)) = history {
        if position == 0 {
          active
            .faults
            .complete_application(fault.index)
            .expect("開始した履歴ページ列");
        }
        active.history = continuation.map(|key| HistoryContinuation {
          fault: fault.clone(),
          position: position + 1,
          key,
        });
        response
      } else if let Some((table, head)) = old_head {
        if !upstream_response.status().is_success() {
          return Err(connector_error(TransportError::InvalidFault(
            "差し込み元のBatchGetItemが失敗",
          )));
        }
        let mut body: Value = serde_json::from_slice(upstream_response.body().bytes().expect("バッファ済み"))
          .map_err(|_| connector_error(TransportError::InvalidFault("BatchGetItemの応答がJSONではない")))?;
        body["Responses"][&table] = json!(head.into_iter().collect::<Vec<_>>());
        active
          .faults
          .complete_application(fault.index)
          .expect("差し込み追記の適用");
        replace_body(&upstream_response, body)
      } else if let Some(pending) = unprocessed {
        if !upstream_response.status().is_success() {
          return Err(connector_error(TransportError::InvalidFault("部分削除の実転送失敗")));
        }
        let mut body: Value = serde_json::from_slice(upstream_response.body().bytes().expect("バッファ済み"))
          .map_err(|_| connector_error(TransportError::InvalidFault("部分削除の応答がJSONではない")))?;
        if body
          .get("UnprocessedItems")
          .and_then(Value::as_object)
          .is_some_and(|v| !v.is_empty())
        {
          return Err(connector_error(TransportError::InvalidFault(
            "実部分削除の未処理項目が残った",
          )));
        }
        body["UnprocessedItems"] = pending;
        active.faults.complete_application(fault.index).expect("部分削除の適用");
        replace_body(&upstream_response, body)
      } else {
        let Some(next_fault) = active
          .faults
          .select_application(fault.phase)
          .filter(|f| f.injection == Injection::ReplaceResponse)
        else {
          let response = event_page_response(&parsed, upstream_response).map_err(connector_error)?;
          active.responses.push(
            json!({"api":parsed.observation.api,"phase":parsed.observation.phase,"fault_index":null,
            "upstream_request":transferred,"upstream":upstream_observation,
            "delivered":{"status":response.status().as_u16(),
              "body":String::from_utf8_lossy(response.body().bytes().expect("バッファ済み応答"))}}),
          );
          return Ok(response);
        };
        let index = next_fault.index;
        applied_index = index;
        let replacement = if index == fault.index {
          match prepared {
            PreparedReplacement::Response(v) => v,
            _ => unreachable!("他の差し込みは処理済み"),
          }
        } else {
          prepare_response(&parsed, next_fault).map_err(connector_error)?
        };
        let response = replacement.apply(&upstream_response).map_err(connector_error)?;
        active
          .faults
          .complete_application(index)
          .expect("同じロック内で選択した障害");
        response
      };
      let delivered_response = event_page_response(&parsed, delivered_response).map_err(connector_error)?;
      active
        .responses
        .push(json!({"api":parsed.observation.api, "phase":parsed.observation.phase,
        "fault_index":applied_index, "upstream_request": if empty_transfer {None} else {Some(transferred)},
        "upstream": if empty_transfer {None} else {Some(upstream_observation)},
        "delivered":{"status":delivered_response.status().as_u16(),
          "body":String::from_utf8_lossy(delivered_response.body().bytes().expect("組み立てた応答"))}}));
      Ok(delivered_response)
    })
  }
}

pub(super) fn replace_body(upstream: &HttpResponse, body: Value) -> HttpResponse {
  let mut response = aws_smithy_runtime_api::http::Response::new(upstream.status(), SdkBody::from(body.to_string()));
  *response.headers_mut() = upstream.headers().clone();
  response.headers_mut().remove("content-length");
  response
}

fn complete(state: &State, identity: &Arc<()>, index: usize) {
  if let Some(active) = state
    .active
    .lock()
    .expect("操作状態のロック")
    .as_mut()
    .filter(|v| Arc::ptr_eq(&v.identity, identity))
  {
    active.faults.complete_application(index).expect("選択した障害");
  }
}

fn record(
  state: &State,
  identity: &Arc<()>,
  parsed: &super::request::ParsedRequest,
  fault_index: Option<usize>,
  upstream_request: Option<Value>,
  upstream: Option<Value>,
  delivered: Value,
) {
  if let Some(active) = state
    .active
    .lock()
    .expect("操作状態のロック")
    .as_mut()
    .filter(|v| Arc::ptr_eq(&v.identity, identity))
  {
    active
      .responses
      .push(json!({"api":parsed.observation.api, "phase":parsed.observation.phase,
      "fault_index":fault_index, "upstream_request":upstream_request, "upstream":upstream,"delivered":delivered}));
  }
}
