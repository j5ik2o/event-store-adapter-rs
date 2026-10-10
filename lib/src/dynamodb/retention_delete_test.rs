use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region, Sleep, StalledStreamProtectionConfig};
use aws_smithy_runtime_api::client::http::{http_client_fn, HttpConnector, HttpConnectorFuture, SharedHttpConnector};
use aws_smithy_runtime_api::client::orchestrator::HttpRequest;
use aws_smithy_runtime_api::http::{Response, StatusCode};
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::retry::RetryConfig;
use aws_smithy_types::timeout::TimeoutConfig;
use serde_json::{json, Value};

use super::*;
use crate::retention::RetentionSettings;
use crate::serializer::{JsonEventSerializer, JsonSnapshotSerializer};

#[derive(Debug, Clone)]
struct Id;

impl AggregateId for Id {
  fn type_name(&self) -> String {
    "Account".into()
  }

  fn value(&self) -> String {
    "value-with-dash".into()
  }
}

#[derive(Debug, Clone, Default)]
struct RecordedSleep(Arc<Mutex<Vec<Duration>>>);

impl AsyncSleep for RecordedSleep {
  fn sleep(&self, delay: Duration) -> Sleep {
    let recorded = self.clone();
    Sleep::new(async move { recorded.0.lock().unwrap().push(delay) })
  }
}

#[derive(Debug, Clone)]
struct Script {
  responses: Arc<Mutex<VecDeque<(u16, Value)>>>,
  inputs: Arc<Mutex<Vec<Value>>>,
}

impl HttpConnector for Script {
  fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
    let script = self.clone();
    HttpConnectorFuture::new(async move {
      script
        .inputs
        .lock()
        .unwrap()
        .push(serde_json::from_slice(request.body().bytes().unwrap()).unwrap());
      let (status, body) = script
        .responses
        .lock()
        .unwrap()
        .pop_front()
        .expect("unexpected request");
      let mut response = Response::new(StatusCode::try_from(status).unwrap(), SdkBody::from(body.to_string()));
      response
        .headers_mut()
        .insert("content-type", "application/x-amz-json-1.0");
      Ok(response)
    })
  }
}

fn tables() -> DynamoDbTables {
  DynamoDbTables {
    journal_table_name: "first".into(),
    snapshot_table_name: "second".into(),
    head_table_name: "third".into(),
    snapshot_history_index_name: "history".into(),
  }
}

fn setup(
  options: DynamoDbOptions,
  responses: Vec<(u16, Value)>,
) -> (EventStoreForDynamoDB<Id, Value, Value>, Script, RecordedSleep) {
  let script = Script {
    responses: Arc::new(Mutex::new(responses.into())),
    inputs: Arc::new(Mutex::new(Vec::new())),
  };
  let connector = script.clone();
  let sleep = RecordedSleep::default();
  let config = aws_sdk_dynamodb::Config::builder()
    .behavior_version(BehaviorVersion::latest())
    .region(Region::new("us-west-1"))
    .credentials_provider(Credentials::new("x", "x", None, None, "unit-test"))
    .http_client(http_client_fn(move |_, _| SharedHttpConnector::new(connector.clone())))
    .sleep_impl(sleep.clone())
    .retry_config(RetryConfig::disabled())
    .timeout_config(TimeoutConfig::disabled())
    .stalled_stream_protection(StalledStreamProtectionConfig::disabled())
    .build();
  let store = EventStoreForDynamoDB {
    client: aws_sdk_dynamodb::Client::from_conf(config),
    tables: tables(),
    options,
    store_id: "unit".into(),
    event_serializer: Arc::new(JsonEventSerializer::new()),
    snapshot_serializer: Arc::new(JsonSnapshotSerializer::new()),
    clock: Arc::new(super::super::clock::SystemClock),
    _aggregate_id: std::marker::PhantomData,
  };
  (store, script, sleep)
}

fn aid() -> AidString {
  AidString::from_aggregate_id(&Id).unwrap()
}

fn history(seq_nr: SeqNr) -> Value {
  json!({"aid": {"S": aid().as_str()}, "skey": {"N": seq_nr.to_string()}, "active_history_seq_nr": {"N": seq_nr.to_string()}})
}

fn page(numbers: &[SeqNr]) -> (u16, Value) {
  (
    200,
    json!({"Items": numbers.iter().copied().map(history).collect::<Vec<_>>() }),
  )
}

fn unprocessed(numbers: &[SeqNr]) -> (u16, Value) {
  (
    200,
    json!({"UnprocessedItems": {"second": numbers.iter().map(|number| json!({"DeleteRequest": {"Key": {"aid": {"S": aid().as_str()}, "skey": {"N": number.to_string()}}}})).collect::<Vec<_>>()}}),
  )
}

fn requested_numbers(input: &Value) -> Vec<SeqNr> {
  input["RequestItems"]["second"]
    .as_array()
    .unwrap()
    .iter()
    .map(|request| {
      request["DeleteRequest"]["Key"]["skey"]["N"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap()
    })
    .collect()
}

#[test]
fn should_restore_only_positive_matching_history_keys() {
  for number in [1, SEQ_NR_MAX] {
    let item = Item::from([
      ("aid".into(), AttributeValue::S(aid().as_str().into())),
      ("skey".into(), AttributeValue::N(number.to_string())),
      ("active_history_seq_nr".into(), AttributeValue::N(number.to_string())),
    ]);
    assert_eq!(history_seq_nr(&item, &aid()).unwrap(), number);
    for (name, replacement) in [
      ("aid", AttributeValue::S("Account-other".into())),
      ("skey", AttributeValue::N("0".into())),
      ("skey", AttributeValue::N((SEQ_NR_MAX + 1).to_string())),
      ("skey", AttributeValue::N("broken".into())),
      ("active_history_seq_nr", AttributeValue::S(number.to_string())),
      ("active_history_seq_nr", AttributeValue::N("0".into())),
    ] {
      let mut invalid = item.clone();
      invalid.insert(name.into(), replacement);
      assert!(history_seq_nr(&invalid, &aid()).is_err());
    }
    for name in ["aid", "skey", "active_history_seq_nr"] {
      let mut invalid = item.clone();
      invalid.remove(name);
      assert!(history_seq_nr(&invalid, &aid()).is_err());
    }
  }
}

#[test]
fn should_build_only_history_delete_keys() {
  let requests = delete_requests(&aid(), &[7, 3]).unwrap();
  assert_eq!(requests.len(), 2);
  for (request, number) in requests.iter().zip([7, 3]) {
    assert!(request.put_request().is_none());
    assert_eq!(
      request.delete_request().unwrap().key(),
      &Item::from([
        ("aid".into(), AttributeValue::S(aid().as_str().into())),
        ("skey".into(), AttributeValue::N(number.to_string())),
      ])
    );
  }
}

#[tokio::test]
async fn should_skip_retention_without_a_count_in_either_mode() {
  for retention in [
    RetentionSettings::current_only(),
    RetentionSettings::current_only().with_mode(RetentionMode::Ttl { grace_seconds: 60 }),
  ] {
    let (store, script, sleep) = setup(
      DynamoDbOptions {
        retention,
        ..Default::default()
      },
      Vec::new(),
    );
    assert!(store
      .retain_history_after_append(&aid(), 4)
      .await
      .retention_failure
      .is_none());
    assert!(script.inputs.lock().unwrap().is_empty());
    assert!(sleep.0.lock().unwrap().is_empty());
  }
}

#[path = "retention_ttl_test.rs"]
mod ttl;

#[tokio::test]
async fn should_read_all_pages_before_selecting_with_missing_or_duplicate_written_history() {
  for written_visible in [false, true] {
    let mut first = page(if written_visible { &[6, 5, 5] } else { &[5, 5] }).1;
    first["LastEvaluatedKey"] = history(5);
    let continuation = first["LastEvaluatedKey"].clone();
    let (store, script, _) = setup(
      DynamoDbOptions {
        retention: RetentionSettings::keep_latest(2),
        ..Default::default()
      },
      vec![(200, first), page(&[4, 3]), (200, json!({}))],
    );
    assert!(store
      .retain_history_after_append(&aid(), 6)
      .await
      .retention_failure
      .is_none());
    let inputs = script.inputs.lock().unwrap();
    assert_eq!(inputs.len(), 3);
    for query in &inputs[..2] {
      assert_eq!(query["TableName"], "second");
      assert_eq!(query["IndexName"], "history");
      assert_eq!(query["KeyConditionExpression"], "aid = :aid");
      assert_eq!(
        query["ExpressionAttributeValues"],
        json!({":aid": {"S": aid().as_str()}})
      );
      assert_eq!(query["ScanIndexForward"], false);
      assert_eq!(query["ConsistentRead"], false);
      assert!(query.get("Select").is_none());
    }
    assert_eq!(inputs[1]["ExclusiveStartKey"], continuation);
    assert_eq!(requested_numbers(&inputs[2]), vec![4, 3]);
  }
}

#[tokio::test]
async fn should_continue_an_empty_history_page_before_selection() {
  let first = json!({"Items": [], "LastEvaluatedKey": history(4)});
  let (store, script, _) = setup(
    DynamoDbOptions {
      retention: RetentionSettings::keep_latest(2),
      ..Default::default()
    },
    vec![(200, first), page(&[4, 3, 2]), (200, json!({}))],
  );
  assert!(store
    .retain_history_after_append(&aid(), 5)
    .await
    .retention_failure
    .is_none());
  let inputs = script.inputs.lock().unwrap();
  assert_eq!(inputs.len(), 3);
  assert_eq!(requested_numbers(&inputs[2]), vec![3, 2]);
}

#[tokio::test]
async fn should_split_thirty_expired_histories_into_twenty_five_and_five() {
  let (store, script, _) = setup(
    DynamoDbOptions {
      retention: RetentionSettings::keep_latest(1),
      ..Default::default()
    },
    vec![
      page(&(1..=30).collect::<Vec<_>>()),
      (200, json!({})),
      (200, json!({"UnprocessedItems": {"second": []}})),
    ],
  );
  assert!(store
    .retain_history_after_append(&aid(), 31)
    .await
    .retention_failure
    .is_none());
  let inputs = script.inputs.lock().unwrap();
  assert_eq!(inputs.len(), 3);
  assert_eq!(requested_numbers(&inputs[1]), (6..=30).rev().collect::<Vec<_>>());
  assert_eq!(requested_numbers(&inputs[2]), (1..=5).rev().collect::<Vec<_>>());
}

#[tokio::test]
async fn should_retry_only_unprocessed_deletes_with_capped_exponential_waits() {
  let (store, script, sleep) = setup(
    DynamoDbOptions {
      retention: RetentionSettings::keep_latest(1),
      unprocessed_retry_limit: 3,
      unprocessed_retry_max_delay: Duration::from_millis(120),
      ..Default::default()
    },
    vec![
      page(&[4, 3, 2, 1]),
      unprocessed(&[3, 2, 1]),
      unprocessed(&[2, 1]),
      unprocessed(&[1]),
      (200, json!({})),
    ],
  );
  assert!(store
    .retain_history_after_append(&aid(), 4)
    .await
    .retention_failure
    .is_none());
  let inputs = script.inputs.lock().unwrap();
  assert_eq!(inputs.len(), 5);
  for (request, numbers) in inputs[1..]
    .iter()
    .zip([vec![3, 2, 1], vec![3, 2, 1], vec![2, 1], vec![1]])
  {
    assert_eq!(requested_numbers(request), numbers);
  }
  assert_eq!(*sleep.0.lock().unwrap(), [50, 100, 120].map(Duration::from_millis));
}

#[tokio::test]
async fn should_stop_at_the_retry_limit_without_sending_the_next_batch() {
  for limit in [0, 2] {
    let mut responses = vec![page(&(1..=30).collect::<Vec<_>>())];
    responses.extend((0..=limit).map(|_| unprocessed(&[30])));
    let (store, script, sleep) = setup(
      DynamoDbOptions {
        retention: RetentionSettings::keep_latest(1),
        unprocessed_retry_limit: limit,
        ..Default::default()
      },
      responses,
    );
    let failure = store
      .retain_history_after_append(&aid(), 31)
      .await
      .retention_failure
      .unwrap();
    assert_eq!(failure.aid, aid().as_str());
    assert_eq!(failure.seq_nr, 31);
    assert_eq!(failure.phase, "retention-delete");
    assert!(failure.error.contains("unprocessed retry limit"));
    assert_eq!(script.inputs.lock().unwrap().len(), limit as usize + 2);
    assert_eq!(sleep.0.lock().unwrap().len(), limit as usize);
    assert!(script.responses.lock().unwrap().is_empty());
  }
}

#[tokio::test]
async fn should_preserve_query_and_delete_causes_in_the_receipt() {
  for phase in ["retention-query", "retention-delete"] {
    let error = (
      400,
      json!({"__type": "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException", "Message": "RETENTION_ORIGINAL_CAUSE"}),
    );
    let responses = if phase == "retention-query" {
      vec![error]
    } else {
      vec![page(&[2, 1]), error]
    };
    let (store, _, _) = setup(
      DynamoDbOptions {
        retention: RetentionSettings::keep_latest(1),
        ..Default::default()
      },
      responses,
    );
    let failure = store
      .retain_history_after_append(&aid(), 2)
      .await
      .retention_failure
      .unwrap();
    assert_eq!(failure.phase, phase);
    assert_eq!(failure.aid, aid().as_str());
    assert_eq!(failure.seq_nr, 2);
    assert!(failure.error.contains("RETENTION_ORIGINAL_CAUSE"), "{}", failure.error);
    assert!(failure.error.contains("ResourceNotFoundException"), "{}", failure.error);
  }
}
