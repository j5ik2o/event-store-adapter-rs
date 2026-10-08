use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_dynamodb::error::ProvideErrorMetadata;
use aws_sdk_dynamodb::types::{
  AttributeValue, CancellationReason, DeleteRequest, KeysAndAttributes, Put, ReturnValuesOnConditionCheckFailure,
  TransactWriteItem, Update, WriteRequest,
};
use aws_sdk_dynamodb::Client;
use aws_smithy_runtime_api::client::http::{http_client_fn, HttpConnector, HttpConnectorFuture, SharedHttpConnector};
use aws_smithy_runtime_api::client::orchestrator::HttpRequest;
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::http::{Response, StatusCode};
use aws_smithy_types::body::SdkBody;
use event_store_adapter_conformance_rs::fault::{FaultPlan, Phase, Repeat};
use event_store_adapter_conformance_rs::target_dynamodb::{FaultTransport, RequestLayout, TransportError};
use serde_json::{json, Value};

const JOURNAL: &str = "arbitrary-first";
const SNAPSHOT: &str = "arbitrary-second";
const HEAD: &str = "arbitrary-third";
const INDEX: &str = "arbitrary-index";
const AID: &str = "Account-PAYLOAD_SENTINEL";

#[derive(Clone, Default)]
struct Recorder {
  calls: Arc<AtomicUsize>,
  completed: Arc<Mutex<Vec<Value>>>,
  fail: bool,
}

impl std::fmt::Debug for Recorder {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.write_str("Recorder")
  }
}

impl HttpConnector for Recorder {
  fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
    self.calls.fetch_add(1, Ordering::SeqCst);
    let completed = self.completed.clone();
    let fail = self.fail;
    HttpConnectorFuture::new(async move {
      if fail {
        return Err(ConnectorError::other("upstream failure".into(), None));
      }
      completed
        .lock()
        .unwrap()
        .push(serde_json::from_slice(request.body().bytes().unwrap()).unwrap());
      Ok(Response::new(StatusCode::try_from(200).unwrap(), SdkBody::from("{}")))
    })
  }
}

fn run(future: impl Future<Output = ()>) {
  tokio::runtime::Builder::new_current_thread()
    .enable_all()
    .build()
    .unwrap()
    .block_on(future);
}

fn fixture_with(recorder: Recorder) -> (FaultTransport, Client, Recorder) {
  let transport = FaultTransport::new(RequestLayout::new(JOURNAL, SNAPSHOT, HEAD, INDEX).unwrap());
  let connector = SharedHttpConnector::new(recorder.clone());
  let upstream = http_client_fn(move |_, _| connector.clone());
  let builder = aws_sdk_dynamodb::Config::builder()
    .behavior_version(BehaviorVersion::latest())
    .region(Region::new("us-east-1"))
    .credentials_provider(Credentials::new("TEST_KEY", "CREDENTIAL_SENTINEL", None, None, "test"))
    .endpoint_url("http://localhost:8000");
  let client = transport.client(builder, upstream);
  (transport, client, recorder)
}

fn fixture() -> (FaultTransport, Client, Recorder) {
  fixture_with(Recorder::default())
}

fn plan(faults: Vec<Value>) -> FaultPlan {
  FaultPlan::register(&json!({"steps": [{}, {}, {}], "faults": faults})).unwrap()
}

fn fault(operation: u32, phase: &str, injection: &str, repeat: Value, details: Value) -> Value {
  json!({"operation": operation, "phase": phase, "kind": "sdk-error", "injection": injection, "repeat": repeat, "details": details})
}

fn once(operation: u32, phase: &str, injection: &str, details: Value) -> Value {
  fault(
    operation,
    phase,
    injection,
    json!({"mode": "count", "count": 1}),
    details,
  )
}

fn key(aid: &str, sort: Option<(&str, &str)>) -> HashMap<String, AttributeValue> {
  let mut key = HashMap::from([("aid".into(), AttributeValue::S(aid.into()))]);
  if let Some((name, value)) = sort {
    key.insert(name.into(), AttributeValue::N(value.into()));
  }
  key
}

fn put(table: &str, aid: &str, sort: Option<(&str, &str)>) -> TransactWriteItem {
  TransactWriteItem::builder()
    .put(
      Put::builder()
        .table_name(table)
        .set_item(Some(key(aid, sort)))
        .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld)
        .build()
        .unwrap(),
    )
    .build()
}

fn head_update() -> TransactWriteItem {
  TransactWriteItem::builder()
    .update(
      Update::builder()
        .table_name(HEAD)
        .set_key(Some(key(AID, None)))
        .update_expression("SET seq_nr = :next")
        .condition_expression("seq_nr = :prev")
        .expression_attribute_values(":next", AttributeValue::N("3".into()))
        .expression_attribute_values(":prev", AttributeValue::N("2".into()))
        .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld)
        .build()
        .unwrap(),
    )
    .build()
}

fn write_actions() -> Vec<TransactWriteItem> {
  vec![put(JOURNAL, AID, Some(("seq_nr", "3"))), head_update()]
}

async fn transact_codes(client: &Client, actions: Vec<TransactWriteItem>) -> Vec<CancellationReason> {
  let error = client
    .transact_write_items()
    .set_transact_items(Some(actions))
    .send()
    .await
    .unwrap_err();
  let service = error.as_service_error().expect("HTTP応答をSDKがサービスエラーへ復号");
  assert_eq!(service.code(), Some("TransactionCanceledException"));
  match service {
    aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError::TransactionCanceledException(error) => {
      error.cancellation_reasons().to_vec()
    }
    _ => panic!("取り消し例外ではない"),
  }
}

fn reason(target: &str, code: &str) -> Value {
  json!({"target": target, "code": code})
}

#[test]
fn should_replace_request_without_transferring_or_committing() {
  run(async {
    let (transport, client, recorder) = fixture();
    let plan = plan(vec![once(
      1,
      "commit",
      "replace-request",
      json!({"code": "TransactionCanceledException", "cancellation_reasons": [reason("journal", "None"), reason("head", "TransactionConflict")]}),
    )]);
    let operation = transport.begin_operation(&plan, 1).unwrap();
    let reasons = transact_codes(&client, write_actions()).await;
    assert_eq!(reasons[1].code(), Some("TransactionConflict"));
    assert_eq!(recorder.calls.load(Ordering::SeqCst), 0);
    assert!(recorder.completed.lock().unwrap().is_empty());
    let report = operation.finish();
    assert!(report.unfired.is_empty());
    assert_eq!(report.requests.len(), 1);
    assert_eq!(report.requests[0].phase, Some(Phase::Commit));
  });
}

#[test]
fn should_replace_response_after_transfer_finishes_and_preserve_its_effect() {
  run(async {
    let (transport, client, recorder) = fixture();
    let plan = plan(vec![once(
      1,
      "commit",
      "replace-response",
      json!({"code": "InternalServerError"}),
    )]);
    let operation = transport.begin_operation(&plan, 1).unwrap();
    let error = client
      .transact_write_items()
      .set_transact_items(Some(write_actions()))
      .send()
      .await
      .unwrap_err();
    assert!(error.as_service_error().unwrap().is_internal_server_error());
    assert_eq!(recorder.calls.load(Ordering::SeqCst), 1);
    assert_eq!(recorder.completed.lock().unwrap().len(), 1);
    let report = operation.finish();
    assert!(report.unfired.is_empty());
    assert_eq!(report.requests.len(), 1);
    assert_eq!(report.requests[0].body, recorder.completed.lock().unwrap()[0]);
  });
}

#[test]
fn should_observe_api_and_body_without_modifying_normal_requests() {
  run(async {
    let (transport, client, recorder) = fixture();
    let operation = transport.begin_operation(&plan(vec![]), 1).unwrap();
    client
      .transact_write_items()
      .set_transact_items(Some(write_actions()))
      .send()
      .await
      .unwrap();
    let report = operation.finish();
    assert_eq!(report.requests[0].api, "TransactWriteItems");
    assert_eq!(recorder.calls.load(Ordering::SeqCst), 1);
    assert_eq!(report.requests[0].body, recorder.completed.lock().unwrap()[0]);
    assert!(report.unfired.is_empty());
  });
}

#[test]
fn should_disable_sdk_retries_for_retryable_service_errors() {
  run(async {
    let (transport, client, recorder) = fixture();
    let plan = plan(vec![fault(
      1,
      "read-events",
      "replace-request",
      json!({"mode": "count", "count": 2}),
      json!({"code": "ProvisionedThroughputExceededException"}),
    )]);
    let operation = transport.begin_operation(&plan, 1).unwrap();
    let error = client
      .query()
      .table_name(JOURNAL)
      .key_condition_expression("aid = :aid")
      .expression_attribute_values(":aid", AttributeValue::S(AID.into()))
      .send()
      .await
      .unwrap_err();
    assert!(error
      .as_service_error()
      .unwrap()
      .is_provisioned_throughput_exceeded_exception());
    let report = operation.finish();
    assert_eq!(report.requests.len(), 1);
    assert_eq!(recorder.calls.load(Ordering::SeqCst), 0);
    assert_eq!(report.unfired[0].applied, 1);
    assert_eq!(report.unfired[0].declared, Repeat::Count { count: 2 });
    assert!(recorder.completed.lock().unwrap().is_empty());
  });
}

#[test]
fn should_align_cancellation_reasons_with_reordered_two_to_four_actions() {
  run(async {
    for head in [put(HEAD, AID, None), head_update()] {
      for snapshot_count in 0..=2 {
        let (transport, client, recorder) = fixture();
        let mut actions = vec![head.clone(), put(JOURNAL, AID, Some(("seq_nr", "3")))];
        let mut reasons = vec![
          reason("journal", "None"),
          json!({"target": "head", "code": "ConditionalCheckFailed", "old_head_seq_nr": 9007199254740993_i64}),
        ];
        if snapshot_count >= 1 {
          actions.insert(0, put(SNAPSHOT, AID, Some(("skey", "0.0"))));
          reasons.push(reason("current-snapshot", "None"));
        }
        if snapshot_count == 2 {
          actions.insert(1, put(SNAPSHOT, AID, Some(("skey", "9007199254740993"))));
          reasons.push(reason("history-snapshot", "ThrottlingError"));
        }
        let plan = plan(vec![once(
          1,
          "commit",
          "replace-request",
          json!({"code": "TransactionCanceledException", "cancellation_reasons": reasons}),
        )]);
        let operation = transport.begin_operation(&plan, 1).unwrap();
        let restored = transact_codes(&client, actions).await;
        assert_eq!(restored.len(), 2 + snapshot_count);
        let head_position = snapshot_count;
        assert_eq!(restored[head_position].code(), Some("ConditionalCheckFailed"));
        let item = restored[head_position].item().unwrap();
        assert_eq!(item["seq_nr"].as_n().unwrap(), "9007199254740993");
        assert_eq!(item["aid"].as_s().unwrap(), AID);
        assert_eq!(restored.last().unwrap().code(), Some("None"));
        if snapshot_count >= 1 {
          assert_eq!(restored[0].code(), Some("None"));
        }
        if snapshot_count == 2 {
          assert_eq!(restored[1].code(), Some("ThrottlingError"));
        }
        assert!(operation.finish().unfired.is_empty());
        assert!(recorder.completed.lock().unwrap().is_empty());
      }
    }
  });
}

#[test]
fn should_restore_absent_old_head_as_no_item() {
  run(async {
    for head in [put(HEAD, AID, None), head_update()] {
      let (transport, client, _) = fixture();
      let plan = plan(vec![once(
        1,
        "commit",
        "replace-request",
        json!({"code": "TransactionCanceledException", "cancellation_reasons": [reason("journal", "None"), {"target": "head", "code": "ConditionalCheckFailed", "old_head_seq_nr": null}]}),
      )]);
      let operation = transport.begin_operation(&plan, 1).unwrap();
      let reasons = transact_codes(&client, vec![put(JOURNAL, AID, Some(("seq_nr", "3"))), head]).await;
      assert_eq!(reasons[1].code(), Some("ConditionalCheckFailed"));
      assert!(reasons[1].item().is_none());
      assert!(operation.finish().unfired.is_empty());
    }
  });
}

#[test]
fn should_match_configuration_targets_in_actual_request_order() {
  run(async {
    let (transport, client, _) = fixture();
    let plan = plan(vec![once(
      0,
      "configuration-create",
      "replace-request",
      json!({"code": "TransactionCanceledException", "cancellation_reasons": [reason("configuration:journal", "ConditionalCheckFailed"), reason("configuration:snapshot", "None"), reason("configuration:head", "TransactionConflict")]}),
    )]);
    let operation = transport.begin_operation(&plan, 0).unwrap();
    let restored = transact_codes(
      &client,
      vec![
        put(HEAD, "__config__", None),
        put(SNAPSHOT, "__config__", Some(("skey", "0"))),
        put(JOURNAL, "__config__", Some(("seq_nr", "0"))),
      ],
    )
    .await;
    assert_eq!(
      restored.iter().map(CancellationReason::code).collect::<Vec<_>>(),
      vec![
        Some("TransactionConflict"),
        Some("None"),
        Some("ConditionalCheckFailed")
      ]
    );
    let report = operation.finish();
    assert_eq!(report.requests[0].phase, Some(Phase::ConfigurationCreate));
    assert!(report.unfired.is_empty());
  });
}

#[test]
fn should_reject_duplicate_missing_and_unmatched_reason_targets() {
  run(async {
    for reasons in [
      vec![reason("head", "None"), reason("head", "TransactionConflict")],
      vec![reason("journal", "None")],
      vec![reason("journal", "None"), reason("history-snapshot", "None")],
      vec![
        reason("journal", "None"),
        reason("head", "None"),
        reason("current-snapshot", "None"),
      ],
      vec![reason("journal", "None"), reason("head", "ConditionalCheckFailed")],
    ] {
      let (transport, client, recorder) = fixture();
      let plan = plan(vec![once(
        1,
        "commit",
        "replace-request",
        json!({"code": "TransactionCanceledException", "cancellation_reasons": reasons}),
      )]);
      let operation = transport.begin_operation(&plan, 1).unwrap();
      let error = client
        .transact_write_items()
        .set_transact_items(Some(write_actions()))
        .send()
        .await
        .unwrap_err();
      assert!(error.as_service_error().is_none());
      assert!(recorder.completed.lock().unwrap().is_empty());
      operation.finish();
    }
  });
}

async fn batch_read(client: &Client, requests: Vec<(&str, HashMap<String, AttributeValue>)>) {
  let mut call = client.batch_get_item();
  for (table, key) in requests {
    call = call.request_items(
      table,
      KeysAndAttributes::builder()
        .keys(key)
        .consistent_read(true)
        .build()
        .unwrap(),
    );
  }
  call.send().await.unwrap();
}

#[test]
fn should_distinguish_configuration_reads_including_partial_re_requests() {
  run(async {
    let (transport, client, _) = fixture();
    let operation = transport.begin_operation(&plan(vec![]), 0).unwrap();
    batch_read(
      &client,
      vec![
        (JOURNAL, key("__config__", Some(("seq_nr", "0")))),
        (SNAPSHOT, key("__config__", Some(("skey", "0")))),
        (HEAD, key("__config__", None)),
      ],
    )
    .await;
    for (table, sort) in [
      (JOURNAL, Some(("seq_nr", "0"))),
      (SNAPSHOT, Some(("skey", "0e2"))),
      (HEAD, None),
    ] {
      batch_read(&client, vec![(table, key("__config__", sort))]).await;
    }
    batch_read(
      &client,
      vec![(HEAD, key(AID, None)), (SNAPSHOT, key(AID, Some(("skey", "0"))))],
    )
    .await;
    batch_read(&client, vec![(SNAPSHOT, key(AID, Some(("skey", "0"))))]).await;
    let phases: Vec<_> = operation
      .finish()
      .requests
      .into_iter()
      .map(|request| request.phase)
      .collect();
    assert_eq!(
      phases,
      vec![
        Some(Phase::ConfigurationRead),
        Some(Phase::ConfigurationRead),
        Some(Phase::ConfigurationRead),
        Some(Phase::ConfigurationRead),
        Some(Phase::ReadSnapshot),
        Some(Phase::ReadSnapshot)
      ]
    );
  });
}

async fn query(client: &Client, table: &str, index: Option<&str>) {
  client
    .query()
    .table_name(table)
    .set_index_name(index.map(str::to_string))
    .key_condition_expression("aid = :aid")
    .expression_attribute_values(":aid", AttributeValue::S(AID.into()))
    .send()
    .await
    .unwrap();
}

async fn delete(client: &Client, table: &str, sort: &str) {
  client
    .batch_write_item()
    .request_items(
      table,
      vec![WriteRequest::builder()
        .delete_request(
          DeleteRequest::builder()
            .set_key(Some(key(AID, Some(("skey", sort)))))
            .build()
            .unwrap(),
        )
        .build()],
    )
    .send()
    .await
    .unwrap();
}

async fn mark(client: &Client, table: &str, sort: &str, ttl: &str) {
  client
    .update_item()
    .table_name(table)
    .set_key(Some(key(AID, Some(("skey", sort)))))
    .update_expression("SET #expires = :expires REMOVE active_history_seq_nr")
    .expression_attribute_names("#expires", ttl)
    .expression_attribute_values(":expires", AttributeValue::N("123".into()))
    .send()
    .await
    .unwrap();
}

#[test]
fn should_classify_queries_deletes_and_ttl_updates_by_request_content() {
  run(async {
    let (transport, client, _) = fixture();
    let operation = transport.begin_operation(&plan(vec![]), 1).unwrap();
    query(&client, JOURNAL, None).await;
    query(&client, JOURNAL, Some(INDEX)).await;
    query(&client, SNAPSHOT, Some(INDEX)).await;
    query(&client, SNAPSHOT, Some("other-index")).await;
    delete(&client, SNAPSHOT, "9007199254740993").await;
    delete(&client, SNAPSHOT, "0").await;
    delete(&client, HEAD, "5").await;
    mark(&client, SNAPSHOT, "4", "ttl").await;
    mark(&client, SNAPSHOT, "0", "ttl").await;
    mark(&client, SNAPSHOT, "4", "other").await;
    mark(&client, HEAD, "4", "ttl").await;
    let phases: Vec<_> = operation
      .finish()
      .requests
      .into_iter()
      .map(|request| request.phase)
      .collect();
    assert_eq!(
      phases,
      vec![
        Some(Phase::ReadEvents),
        None,
        Some(Phase::RetentionQuery),
        None,
        Some(Phase::RetentionDelete),
        None,
        None,
        Some(Phase::RetentionMark),
        None,
        None,
        None
      ]
    );
  });
}

#[test]
fn should_classify_ttl_assignment_and_removal_in_their_actual_clauses() {
  run(async {
    let (transport, client, _) = fixture();
    let operation = transport.begin_operation(&plan(vec![]), 1).unwrap();
    for expression in [
      "REMOVE #active SET #expires=:expiry",
      "SET #active=:expiry REMOVE #expires",
      "SET #other=if_not_exists(#expires,:expiry) REMOVE #active",
    ] {
      client
        .update_item()
        .table_name(SNAPSHOT)
        .set_key(Some(key(AID, Some(("skey", "3")))))
        .update_expression(expression)
        .expression_attribute_names("#expires", "ttl")
        .expression_attribute_names("#active", "active_history_seq_nr")
        .expression_attribute_names("#other", "other")
        .expression_attribute_values(":expiry", AttributeValue::N("123".into()))
        .send()
        .await
        .unwrap();
    }
    let phases: Vec<_> = operation
      .finish()
      .requests
      .into_iter()
      .map(|request| request.phase)
      .collect();
    assert_eq!(phases, vec![Some(Phase::RetentionMark), None, None]);
  });
}

#[test]
fn should_consume_counts_in_declaration_order_on_the_same_client() {
  run(async {
    let (transport, client, recorder) = fixture();
    let plan = plan(vec![
      fault(
        1,
        "commit",
        "replace-request",
        json!({"mode": "count", "count": 2}),
        json!({"code": "InternalServerError"}),
      ),
      once(
        1,
        "commit",
        "replace-response",
        json!({"code": "ProvisionedThroughputExceededException"}),
      ),
    ]);
    let before = transport.begin_operation(&plan, 0).unwrap();
    client
      .transact_write_items()
      .set_transact_items(Some(write_actions()))
      .send()
      .await
      .unwrap();
    assert!(before.finish().unfired.is_empty());
    let operation = transport.begin_operation(&plan, 1).unwrap();
    for expected in [
      "InternalServerError",
      "InternalServerError",
      "ProvisionedThroughputExceededException",
    ] {
      let error = client
        .transact_write_items()
        .set_transact_items(Some(write_actions()))
        .send()
        .await
        .unwrap_err();
      assert_eq!(error.as_service_error().unwrap().code(), Some(expected));
    }
    client
      .transact_write_items()
      .set_transact_items(Some(write_actions()))
      .send()
      .await
      .unwrap();
    let report = operation.finish();
    assert_eq!(report.requests.len(), 4);
    assert!(report.unfired.is_empty());
    let after = transport.begin_operation(&plan, 2).unwrap();
    client
      .transact_write_items()
      .set_transact_items(Some(write_actions()))
      .send()
      .await
      .unwrap();
    assert!(after.finish().unfired.is_empty());
    assert_eq!(recorder.completed.lock().unwrap().len(), 4);
  });
}

#[test]
fn should_report_unfired_phases_without_consuming_them_on_other_requests() {
  run(async {
    let (transport, client, _) = fixture();
    let plan = plan(vec![
      once(1, "commit", "replace-request", json!({"code": "InternalServerError"})),
      once(
        1,
        "read-events",
        "replace-request",
        json!({"code": "InternalServerError"}),
      ),
    ]);
    let operation = transport.begin_operation(&plan, 1).unwrap();
    query(&client, SNAPSHOT, Some(INDEX)).await;
    let error = client
      .transact_write_items()
      .set_transact_items(Some(write_actions()))
      .send()
      .await
      .unwrap_err();
    assert!(error.as_service_error().unwrap().is_internal_server_error());
    let report = operation.finish();
    assert_eq!(report.unfired.len(), 1);
    assert_eq!(report.unfired[0].index, 1);
    assert_eq!(report.unfired[0].phase, Phase::ReadEvents);
    assert_eq!(report.unfired[0].applied, 0);
  });
}

#[test]
fn should_remove_until_operation_finishes_faults_after_finish_and_interruption() {
  run(async {
    let (transport, client, recorder) = fixture();
    let plan = plan(vec![fault(
      1,
      "retention-mark",
      "replace-request",
      json!({"mode": "until-operation-finishes"}),
      json!({"code": "InternalServerError"}),
    )]);
    for interrupt in [false, true] {
      let operation = transport.begin_operation(&plan, 1).unwrap();
      assert!(matches!(
        transport.begin_operation(&plan, 2),
        Err(TransportError::OperationActive)
      ));
      for _ in 0..2 {
        let error = client
          .update_item()
          .table_name(SNAPSHOT)
          .set_key(Some(key(AID, Some(("skey", "3")))))
          .update_expression("SET #ttl = :expiry REMOVE #active")
          .expression_attribute_names("#ttl", "ttl")
          .expression_attribute_names("#active", "active_history_seq_nr")
          .expression_attribute_values(":expiry", AttributeValue::N("123".into()))
          .send()
          .await
          .unwrap_err();
        assert!(error.as_service_error().unwrap().is_internal_server_error());
      }
      if interrupt {
        drop(operation);
      } else {
        assert!(operation.finish().unfired.is_empty());
      }
      let next = transport.begin_operation(&plan, 2).unwrap();
      mark(&client, SNAPSHOT, "3", "ttl").await;
      assert!(next.finish().unfired.is_empty());
    }
    assert_eq!(recorder.completed.lock().unwrap().len(), 2);
  });
}

#[test]
fn should_preserve_upstream_transport_failure_when_replacing_a_response() {
  run(async {
    let (transport, client, recorder) = fixture_with(Recorder {
      fail: true,
      ..Recorder::default()
    });
    let plan = plan(vec![once(
      1,
      "commit",
      "replace-response",
      json!({"code": "InternalServerError"}),
    )]);
    let operation = transport.begin_operation(&plan, 1).unwrap();
    let error = client
      .transact_write_items()
      .set_transact_items(Some(write_actions()))
      .send()
      .await
      .unwrap_err();
    assert!(error.as_service_error().is_none());
    assert!(recorder.completed.lock().unwrap().is_empty());
    assert_eq!(operation.finish().requests.len(), 1);
  });
}

#[test]
fn should_reject_unconnected_faults_and_invalid_layouts() {
  let (transport, _, _) = fixture();
  for kind in ["sdk-response", "read-interleave", "serialization-error"] {
    let mut declaration = once(1, "commit", "replace-request", json!({"code": "InternalServerError"}));
    declaration["kind"] = json!(kind);
    assert!(matches!(
      transport.begin_operation(&plan(vec![declaration]), 1),
      Err(TransportError::UnsupportedFault)
    ));
  }
  assert!(RequestLayout::new("same", "same", "head", "index").is_err());
  assert!(RequestLayout::new("journal", "snapshot", "head", "").is_err());
}

#[test]
fn should_keep_request_credentials_and_fault_details_out_of_debug() {
  run(async {
    let (transport, client, _) = fixture();
    let plan = plan(vec![once(
      1,
      "commit",
      "replace-request",
      json!({"code": "InternalServerError", "message": "FAULT_SENTINEL"}),
    )]);
    let operation = transport.begin_operation(&plan, 1).unwrap();
    let debug = format!("{transport:?} {operation:?} {:?}", client.config());
    client
      .transact_write_items()
      .set_transact_items(Some(write_actions()))
      .send()
      .await
      .unwrap_err();
    let report = operation.finish();
    let debug = format!("{debug} {report:?} {:?}", report.requests[0]);
    for sentinel in [
      "CREDENTIAL_SENTINEL",
      "PAYLOAD_SENTINEL",
      "FAULT_SENTINEL",
      JOURNAL,
      SNAPSHOT,
      HEAD,
    ] {
      assert!(!debug.contains(sentinel));
    }
  });
}

#[test]
fn should_return_storage_failures_through_the_sdk_http_boundary() {
  run(async {
    let (transport, client, recorder) = fixture();
    let mut declaration = once(
      1,
      "retention-delete",
      "replace-request",
      json!({"message": "storage failed"}),
    );
    declaration["kind"] = json!("storage-error");
    let operation = transport.begin_operation(&plan(vec![declaration]), 1).unwrap();
    let error = client
      .batch_write_item()
      .request_items(
        SNAPSHOT,
        vec![WriteRequest::builder()
          .delete_request(
            DeleteRequest::builder()
              .set_key(Some(key(AID, Some(("skey", "5")))))
              .build()
              .unwrap(),
          )
          .build()],
      )
      .send()
      .await
      .unwrap_err();
    assert!(error.as_service_error().unwrap().is_internal_server_error());
    assert!(recorder.completed.lock().unwrap().is_empty());
    assert!(operation.finish().unfired.is_empty());
  });
}

#[test]
fn should_reject_ambiguous_actions_and_invalid_configuration_keys_before_transfer() {
  run(async {
    for actions in [
      vec![
        put(JOURNAL, AID, Some(("seq_nr", "3"))),
        put(JOURNAL, AID, Some(("seq_nr", "4"))),
      ],
      vec![put(JOURNAL, AID, Some(("seq_nr", "3"))), put(HEAD, "__config__", None)],
      vec![put(SNAPSHOT, "__config__", Some(("skey", "4")))],
      vec![put(SNAPSHOT, AID, Some(("skey", "0.5")))],
    ] {
      let (transport, client, recorder) = fixture();
      let operation = transport.begin_operation(&plan(vec![]), 1).unwrap();
      let error = client
        .transact_write_items()
        .set_transact_items(Some(actions))
        .send()
        .await
        .unwrap_err();
      assert!(error.as_service_error().is_none());
      assert!(recorder.completed.lock().unwrap().is_empty());
      operation.finish();
    }
  });
}

#[test]
fn should_reject_invalid_error_headers_without_panicking_or_transferring() {
  run(async {
    let (transport, client, recorder) = fixture();
    let operation = transport
      .begin_operation(
        &plan(vec![once(
          1,
          "commit",
          "replace-request",
          json!({"code": "bad\nheader"}),
        )]),
        1,
      )
      .unwrap();
    let error = client
      .transact_write_items()
      .set_transact_items(Some(write_actions()))
      .send()
      .await
      .unwrap_err();
    assert!(error.as_service_error().is_none());
    assert!(recorder.completed.lock().unwrap().is_empty());
    operation.finish();
  });
}

#[test]
fn should_report_unfired_until_operation_finishes_and_reset_on_the_same_client() {
  run(async {
    let (transport, client, recorder) = fixture();
    let plan = plan(vec![fault(
      1,
      "retention-delete",
      "replace-request",
      json!({"mode": "until-operation-finishes"}),
      json!({"code": "InternalServerError"}),
    )]);
    let operation = transport.begin_operation(&plan, 1).unwrap();
    query(&client, JOURNAL, None).await;
    let report = operation.finish();
    assert_eq!(report.unfired.len(), 1);
    assert_eq!(report.unfired[0].applied, 0);
    assert_eq!(report.unfired[0].declared, Repeat::UntilOperationFinishes);
    let next = transport.begin_operation(&plan, 2).unwrap();
    delete(&client, SNAPSHOT, "3").await;
    assert!(next.finish().unfired.is_empty());
    assert_eq!(recorder.completed.lock().unwrap().len(), 2);
  });
}
