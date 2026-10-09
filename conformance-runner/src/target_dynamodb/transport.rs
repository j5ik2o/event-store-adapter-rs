use std::fmt;
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
use super::response::{error_response, history_page_response, prepare_response, ResponseReplacement};
use crate::fault::{FaultKind, FaultPlan, Injection, OperationFaults, Phase, UnfiredFault};

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
}

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
          || (fault.kind == FaultKind::SdkResponse
            && (fault.phase == Phase::ConfigurationRead
              || (fault.phase == Phase::RetentionQuery
                && fault.details.get("history_pages").is_some()
                && self.state.history_client.is_some()))
            && fault.injection == Injection::ReplaceResponse))
          || fault.details.get("install_items").is_some()
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
    });
    Ok(OperationGuard {
      state: self.state.clone(),
      finished: false,
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
      unfired: active.faults.finish().err().unwrap_or_default(),
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
  fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
    let state = self.state.clone();
    let upstream = self.upstream.clone();
    HttpConnectorFuture::new(async move {
      let parsed = parse(&state.layout, &request).map_err(|error| ConnectorError::other(Box::new(error), None))?;
      let identity = state
        .active
        .lock()
        .expect("操作状態のロック")
        .as_ref()
        .map(|active| active.identity.clone());
      let fault = {
        let mut active = state.active.lock().expect("操作状態のロック");
        parsed.observation.phase.and_then(|phase| {
          active.as_mut().and_then(|active| {
            let fault = active
              .history
              .as_ref()
              .filter(|_| phase == Phase::RetentionQuery)
              .map(|history| history.fault.clone())
              .or_else(|| active.faults.select_application(phase).cloned())?;
            let response = match fault.injection {
              Injection::ReplaceRequest => error_response(&parsed, &fault).map(PreparedReplacement::Request),
              Injection::ReplaceResponse
                if fault.kind == FaultKind::SdkResponse && fault.phase == Phase::RetentionQuery =>
              {
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
                    Ok(PreparedReplacement::History { position: 0 })
                  }
                  None => Err(TransportError::InvalidFault(
                    "履歴ページ列の最初の要求に続きのキーがある",
                  )),
                }
              }
              Injection::ReplaceResponse => prepare_response(&parsed, &fault).map(PreparedReplacement::Response),
            };
            if fault.injection == Injection::ReplaceRequest && response.is_ok() {
              active.faults.complete_application(fault.index);
            }
            Some((fault, response, active.identity.clone()))
          })
        })
      };
      match fault {
        None => {
          let mut response = upstream.call(request).await?;
          if let Some(identity) = identity {
            let observation = buffer_response(&mut response).await?;
            if let Some(active) = state
              .active
              .lock()
              .expect("操作状態のロック")
              .as_mut()
              .filter(|active| Arc::ptr_eq(&active.identity, &identity))
            {
              active
                .responses
                .push(json!({"api": parsed.observation.api, "phase": parsed.observation.phase,
                "upstream": observation, "delivered": observation}));
            }
          }
          Ok(response)
        }
        Some((fault, response, identity)) => {
          let response = response.map_err(|error| ConnectorError::other(Box::new(error), None))?;
          if let PreparedReplacement::Request(mut response) = response {
            let observation = buffer_response(&mut response).await?;
            if let Some(active) = state
              .active
              .lock()
              .expect("操作状態のロック")
              .as_mut()
              .filter(|active| Arc::ptr_eq(&active.identity, &identity))
            {
              active
                .responses
                .push(json!({"api": parsed.observation.api, "phase": parsed.observation.phase,
                "fault_index": fault.index, "upstream": null, "delivered": observation}));
            }
            return Ok(response);
          }
          let mut upstream_response = upstream.call(request).await?;
          let upstream_observation = buffer_response(&mut upstream_response).await?;
          let history = if let PreparedReplacement::History { position } = &response {
            Some((
              history_page_response(
                &parsed,
                &fault,
                state.history_client.as_ref().expect("登録時に確認済み"),
                *position,
                &upstream_response,
              )
              .await
              .map_err(|error| ConnectorError::other(Box::new(error), None))?,
              *position,
            ))
          } else {
            None
          };
          let mut active = state.active.lock().expect("操作状態のロック");
          let Some(active) = active
            .as_mut()
            .filter(|active| Arc::ptr_eq(&active.identity, &identity))
          else {
            return Ok(upstream_response);
          };
          if let Some(((response, continuation), position)) = history {
            let delivered = json!({"status": response.status().as_u16(),
              "body": String::from_utf8_lossy(response.body().bytes().expect("組み立てた履歴応答"))});
            active
              .responses
              .push(json!({"api": parsed.observation.api, "phase": parsed.observation.phase,
              "fault_index": fault.index, "upstream": upstream_observation, "delivered": delivered}));
            if position == 0 {
              active
                .faults
                .complete_application(fault.index)
                .expect("開始した履歴ページ列");
            }
            if let Some(key) = continuation {
              active.history = Some(HistoryContinuation {
                fault: fault.clone(),
                position: position + 1,
                key,
              });
            } else {
              active.history = None;
            }
            return Ok(response);
          }
          let Some(next_fault) = active
            .faults
            .select_application(fault.phase)
            .filter(|fault| fault.injection == Injection::ReplaceResponse)
          else {
            return Ok(upstream_response);
          };
          let index = next_fault.index;
          let response = if index == fault.index {
            match response {
              PreparedReplacement::Response(response) => response,
              _ => unreachable!("要求置換と履歴応答は処理済み"),
            }
          } else {
            prepare_response(&parsed, next_fault).map_err(|error| ConnectorError::other(Box::new(error), None))?
          };
          let response = response
            .apply(&upstream_response)
            .map_err(|error| ConnectorError::other(Box::new(error), None))?;
          active
            .responses
            .push(json!({"api": parsed.observation.api, "phase": parsed.observation.phase,
            "fault_index": index, "upstream": upstream_observation,
            "delivered": {"status": response.status().as_u16(),
              "body": String::from_utf8_lossy(response.body().bytes().expect("組み立てた置換応答"))}}));
          active
            .faults
            .complete_application(index)
            .expect("同じロック内で選択した未消費の障害");
          Ok(response)
        }
      }
    })
  }
}
