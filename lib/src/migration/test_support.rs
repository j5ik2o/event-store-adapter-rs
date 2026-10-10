use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use aws_sdk_dynamodb::config::{Credentials, Region};
use aws_sdk_dynamodb::Client;
use aws_smithy_runtime_api::client::http::{http_client_fn, HttpConnector, HttpConnectorFuture, SharedHttpConnector};
use aws_smithy_runtime_api::client::orchestrator::HttpRequest;
use aws_smithy_runtime_api::http::{Response, StatusCode};
use aws_smithy_types::{body::SdkBody, retry::RetryConfig};
use serde_json::{json, Value};

use crate::next::dynamodb::items::Item;

#[derive(Debug, Clone)]
pub(super) struct Replay {
  replies: Arc<Mutex<VecDeque<(u16, Value)>>>,
  pub requests: Arc<Mutex<Vec<Value>>>,
}

impl HttpConnector for Replay {
  fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
    self.requests.lock().unwrap().push(json!({
      "api":request.headers().get("x-amz-target").unwrap().rsplit('.').next().unwrap(),
      "input":serde_json::from_slice::<Value>(request.body().bytes().unwrap()).unwrap()
    }));
    let (status, reply) = self.replies.lock().unwrap().pop_front().expect("予期しないSDK要求");
    HttpConnectorFuture::new(async move {
      let body = reply.to_string();
      let mut response = Response::new(StatusCode::try_from(status).unwrap(), SdkBody::from(body.clone()));
      response.headers_mut().insert("content-length", body.len().to_string());
      Ok(response)
    })
  }
}

pub(super) fn client(replies: Vec<(u16, Value)>) -> (Client, Replay) {
  let replay = Replay {
    replies: Arc::new(Mutex::new(replies.into())),
    requests: Arc::new(Mutex::new(Vec::new())),
  };
  let connector = SharedHttpConnector::new(replay.clone());
  let client = Client::from_conf(
    aws_sdk_dynamodb::Config::builder()
      .behavior_version_latest()
      .region(Region::new("us-west-1"))
      .credentials_provider(Credentials::new("x", "x", None, None, "unit"))
      .endpoint_url("http://127.0.0.1:8000")
      .http_client(http_client_fn(move |_, _| connector.clone()))
      .retry_config(RetryConfig::disabled())
      .build(),
  );
  (client, replay)
}

pub(super) fn raw_event() -> Item {
  use aws_sdk_dynamodb::{primitives::Blob, types::AttributeValue as A};
  Item::from([
    ("pkey".into(), A::S("Old-Account-7".into())),
    ("skey".into(), A::S("Old-Account-value-with-hyphen-1".into())),
    ("aid".into(), A::S("caller representation".into())),
    ("seq_nr".into(), A::N("1".into())),
    ("occurred_at".into(), A::N("-876543211".into())),
    ("payload".into(), A::B(Blob::new([255, 0, 128]))),
  ])
}
