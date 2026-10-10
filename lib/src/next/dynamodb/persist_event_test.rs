use aws_sdk_dynamodb::types::error::TransactionCanceledException;
use aws_sdk_dynamodb::types::CancellationReason;
use aws_smithy_runtime_api::http::{Response, StatusCode};
use aws_smithy_types::body::SdkBody;
use chrono::{DateTime, Utc};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::next::error::SerializationPhase;
use crate::next::serializer::{EventSerializer, SnapshotSerializer};

use super::*;

#[derive(Debug, Clone)]
struct Id(String, String);

impl AggregateId for Id {
  fn type_name(&self) -> String {
    self.0.clone()
  }

  fn value(&self) -> String {
    self.1.clone()
  }
}

fn event(seq_nr: SeqNr) -> EventEnvelope<Id, ()> {
  EventEnvelope::new(
    Id("Account".into(), "value-with-dash".into()),
    seq_nr,
    DateTime::<Utc>::from_timestamp(-1, 123456789).unwrap(),
    (),
  )
}

fn tables() -> DynamoDbTables {
  DynamoDbTables {
    journal_table_name: "first".into(),
    snapshot_table_name: "second".into(),
    head_table_name: "third".into(),
    snapshot_history_index_name: "history".into(),
  }
}

#[tokio::test]
async fn should_delegate_all_event_store_trait_operations_to_native_validation() {
  use crate::next::event_store::EventStore;
  let store = EventStoreForDynamoDB::<Id, (), ()> {
    client: aws_sdk_dynamodb::Client::from_conf(
      aws_sdk_dynamodb::Config::builder()
        .behavior_version_latest()
        .region(aws_sdk_dynamodb::config::Region::new("us-west-1"))
        .build(),
    ),
    tables: tables(),
    options: crate::next::dynamodb::DynamoDbOptions::default(),
    store_id: "unit".into(),
    event_serializer: Arc::new(crate::next::serializer::JsonEventSerializer::new()),
    snapshot_serializer: Arc::new(crate::next::serializer::JsonSnapshotSerializer::new()),
    clock: Arc::new(super::super::clock::SystemClock),
    _aggregate_id: std::marker::PhantomData,
  };
  assert!(matches!(
    EventStore::persist_event(&store, event(0)).await,
    Err(EventStoreError::ContractViolation {
      rule: ContractRule::W6,
      ..
    })
  ));
  assert!(matches!(
    EventStore::persist_event_and_snapshot(&store, event(1), SnapshotEnvelope::new((), 2)).await,
    Err(EventStoreError::ContractViolation {
      rule: ContractRule::W9,
      ..
    })
  ));
  let invalid = Id("type-with-hyphen".into(), "1".into());
  assert!(matches!(
    EventStore::get_latest_snapshot_by_id(&store, &invalid).await,
    Err(EventStoreError::ContractViolation {
      rule: ContractRule::T11,
      ..
    })
  ));
  assert!(matches!(
    EventStore::get_events_by_id_since_seq_nr(&store, event(1).aggregate_id(), crate::next::seq_nr::SEQ_NR_MAX + 1)
      .await,
    Err(EventStoreError::ContractViolation {
      rule: ContractRule::T9,
      ..
    })
  ));
}

#[test]
fn should_prepare_two_conditional_actions_and_preserve_event_attributes() {
  let event = event(1).with_manifest("e\u{301}🙂");
  let aid = check_event(&event).unwrap();
  let writes = transaction_items(&tables(), &aid, &event, vec![0, 255, 1]).unwrap();
  assert_eq!(writes.len(), 2);
  let journal = writes[0].put().unwrap();
  assert_eq!(journal.table_name(), "first");
  assert_eq!(journal.condition_expression(), Some("attribute_not_exists(aid)"));
  assert_eq!(journal.item().len(), 5);
  assert_eq!(
    journal.item()["aid"],
    AttributeValue::S("Account-value-with-dash".into())
  );
  assert_eq!(journal.item()["occurred_at"], AttributeValue::N("-876543211".into()));
  assert_eq!(journal.item()["manifest"], AttributeValue::S("e\u{301}🙂".into()));
  assert_eq!(journal.item()["payload"], AttributeValue::B(Blob::new(vec![0, 255, 1])));
  let head = writes[1].put().unwrap();
  assert_eq!(head.table_name(), "third");
  assert_eq!(head.condition_expression(), Some("attribute_not_exists(aid)"));
  assert_eq!(
    head.return_values_on_condition_check_failure(),
    Some(&ReturnValuesOnConditionCheckFailure::AllOld)
  );
  assert_eq!(head.item().len(), 4);
  assert_eq!(head.item()["type_name"], AttributeValue::S("Account".into()));
  let events = head.item()["events"].as_l().unwrap();
  assert_eq!(events.len(), 1);
  let metadata = events[0].as_m().unwrap();
  assert_eq!(metadata.len(), 4);
  for name in ["seq_nr", "occurred_at", "manifest", "payload"] {
    assert_eq!(metadata[name], journal.item()[name]);
  }
}

#[test]
fn should_replace_the_head_event_under_the_previous_number_condition() {
  let event = event(2);
  let aid = check_event(&event).unwrap();
  let writes = transaction_items(&tables(), &aid, &event, vec![2]).unwrap();
  assert_eq!(writes.len(), 2);
  let head = writes[1].update().unwrap();
  assert_eq!(head.table_name(), "third");
  assert_eq!(head.key()["aid"], AttributeValue::S(aid.as_str().into()));
  assert_eq!(
    head.return_values_on_condition_check_failure(),
    Some(&ReturnValuesOnConditionCheckFailure::AllOld)
  );
  let values = head.expression_attribute_values().unwrap();
  assert_eq!(values[":prev"], AttributeValue::N("1".into()));
  assert_eq!(values[":next"], AttributeValue::N("2".into()));
  let events = values[":events"].as_l().unwrap();
  assert_eq!(events.len(), 1);
  assert_eq!(events[0].as_m().unwrap()["manifest"], AttributeValue::S("".into()));
}

#[test]
fn should_check_both_complete_items_before_building_a_transaction() {
  for seq_nr in [1, 2] {
    for (type_name, manifest, payload_len) in [
      ("Account".to_string(), "".to_string(), 409600),
      ("Account".to_string(), "界".repeat(136534), 0),
      ("x".repeat(1022), "".to_string(), 408002),
    ] {
      let event = EventEnvelope::new(Id(type_name, "".into()), seq_nr, Utc::now(), ()).with_manifest(manifest);
      let aid = check_event(&event).unwrap();
      let result = transaction_items(&tables(), &aid, &event, vec![0; payload_len]);
      assert!(matches!(
        result,
        Err(EventStoreError::ContractViolation {
          rule: ContractRule::ItemSizeLimit,
          ..
        })
      ));
    }
  }
}

fn cancellation(journal: &str, head: &str, old: Option<AttributeValue>) -> SdkError<TransactWriteItemsError> {
  let head = CancellationReason::builder()
    .code(head)
    .set_item(old.map(|value| HashMap::from([("seq_nr".into(), value)])))
    .build();
  let error = TransactionCanceledException::builder()
    .message("TransactionConflict ConditionalCheckFailed SDK_SENTINEL")
    .cancellation_reasons(CancellationReason::builder().code(journal).build())
    .cancellation_reasons(head)
    .build();
  SdkError::service_error(
    TransactWriteItemsError::TransactionCanceledException(error),
    Response::new(StatusCode::try_from(400).unwrap(), SdkBody::from("original-response")),
  )
}

fn classify(journal: &str, head: &str, old: Option<AttributeValue>, seq_nr: SeqNr) -> EventStoreError {
  let aid = check_event(&event(seq_nr)).unwrap();
  classify_append_error(cancellation(journal, head, old), &aid, seq_nr, 2)
}

#[test]
fn should_prepare_unconditional_current_and_optional_history_snapshot_items() {
  for seq_nr in [1, 2] {
    let event = event(seq_nr);
    let aid = check_event(&event).unwrap();
    let snapshot = SnapshotEnvelope::new((), seq_nr).with_manifest("snapshot:e\u{301}🙂");
    for keep_history in [false, true] {
      let writes = snapshot_transaction_items(
        &tables(),
        &aid,
        &snapshot,
        event.occurred_at().timestamp_millis(),
        vec![255, 0, 128],
        keep_history,
      )
      .unwrap();
      assert_eq!(writes.len(), if keep_history { 2 } else { 1 });
      for (position, write) in writes.iter().enumerate() {
        let put = write.put().unwrap();
        assert_eq!(put.table_name(), "second");
        assert_eq!(put.condition_expression(), None);
        let mut expected = HashMap::from([
          ("aid".into(), AttributeValue::S(aid.as_str().into())),
          ("skey".into(), AttributeValue::N("0".into())),
          ("seq_nr".into(), AttributeValue::N(seq_nr.to_string())),
          ("manifest".into(), AttributeValue::S("snapshot:e\u{301}🙂".into())),
          ("payload".into(), AttributeValue::B(Blob::new(vec![255, 0, 128]))),
          ("last_updated_at".into(), AttributeValue::N("-877".into())),
        ]);
        if position == 1 {
          expected.insert("skey".into(), AttributeValue::N(seq_nr.to_string()));
          expected.insert("active_history_seq_nr".into(), AttributeValue::N(seq_nr.to_string()));
        }
        assert_eq!(put.item(), &expected);
      }
    }
  }
}

#[test]
fn should_check_snapshot_manifest_payload_and_history_overhead_before_sending() {
  let aid = check_event(&event(2)).unwrap();
  for (manifest, payload_len) in [(String::new(), ITEM_SIZE_LIMIT), ("界".repeat(136534), 0)] {
    let snapshot = SnapshotEnvelope::new((), 2).with_manifest(manifest);
    assert!(matches!(
      snapshot_transaction_items(&tables(), &aid, &snapshot, -877, vec![0; payload_len], false),
      Err(EventStoreError::ContractViolation {
        rule: ContractRule::ItemSizeLimit,
        seq_nr: Some(2),
        ..
      })
    ));
  }
  let snapshot = SnapshotEnvelope::new((), 2);
  let small = snapshot_transaction_items(&tables(), &aid, &snapshot, -877, Vec::new(), true).unwrap();
  let current_overhead = item_size_upper_bound(small[0].put().unwrap().item());
  let history_overhead = item_size_upper_bound(small[1].put().unwrap().item());
  assert!(history_overhead > current_overhead);
  let payload = vec![0; ITEM_SIZE_LIMIT - current_overhead];
  let current = snapshot_transaction_items(&tables(), &aid, &snapshot, -877, payload.clone(), false).unwrap();
  assert_eq!(item_size_upper_bound(current[0].put().unwrap().item()), ITEM_SIZE_LIMIT);
  assert!(matches!(
    snapshot_transaction_items(&tables(), &aid, &snapshot, -877, payload, true),
    Err(EventStoreError::ContractViolation {
      rule: ContractRule::ItemSizeLimit,
      ..
    })
  ));
}

#[test]
fn should_prioritize_transaction_conflict_at_every_requested_action_position() {
  let aid = check_event(&event(3)).unwrap();
  for action_count in [2, 3, 4] {
    for position in 0..action_count {
      let mut reasons = vec![CancellationReason::builder().code("None").build(); action_count];
      reasons[HEAD_POSITION] = CancellationReason::builder().code("ConditionalCheckFailed").build();
      reasons[position] = CancellationReason::builder().code("TransactionConflict").build();
      let canceled = TransactionCanceledException::builder()
        .set_cancellation_reasons(Some(reasons))
        .build();
      let error = SdkError::service_error(
        TransactWriteItemsError::TransactionCanceledException(canceled),
        Response::new(StatusCode::try_from(400).unwrap(), SdkBody::empty()),
      );
      assert!(matches!(
        classify_append_error(error, &aid, 3, action_count),
        EventStoreError::OptimisticLock {
          seq_nr: 3,
          head_seq_nr: None,
          ..
        }
      ));
    }
  }
}

#[derive(Debug)]
struct CountingSerializer {
  calls: AtomicUsize,
  fail: bool,
}

impl EventSerializer<()> for CountingSerializer {
  fn serialize(&self, _: &()) -> Result<Vec<u8>, EventStoreError> {
    self.calls.fetch_add(1, Ordering::SeqCst);
    if self.fail {
      return Err(EventStoreError::Serialization {
        phase: SerializationPhase::SerializeEvent,
        source: Box::new(std::io::Error::other("event cause")),
      });
    }
    Ok(vec![1])
  }

  fn deserialize(&self, _: &[u8]) -> Result<(), EventStoreError> {
    panic!("書込みの検証でdeserializeしない")
  }
}

impl SnapshotSerializer<()> for CountingSerializer {
  fn serialize(&self, _: &()) -> Result<Vec<u8>, EventStoreError> {
    self.calls.fetch_add(1, Ordering::SeqCst);
    if self.fail {
      return Err(EventStoreError::Serialization {
        phase: SerializationPhase::SerializeSnapshot,
        source: Box::new(std::io::Error::other("snapshot cause")),
      });
    }
    Ok(vec![2])
  }

  fn deserialize(&self, _: &[u8]) -> Result<(), EventStoreError> {
    panic!("書込みの検証でdeserializeしない")
  }
}

#[tokio::test]
async fn should_validate_pair_then_serialize_event_then_snapshot_before_sending() {
  use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
  use aws_smithy_runtime_api::client::http::{http_client_fn, HttpConnector, HttpConnectorFuture, SharedHttpConnector};
  use aws_smithy_runtime_api::client::orchestrator::HttpRequest;

  #[derive(Debug)]
  struct NoRequests;
  impl HttpConnector for NoRequests {
    fn call(&self, _: HttpRequest) -> HttpConnectorFuture {
      panic!("共通検査またはserializerの失敗時は送信0")
    }
  }
  let client = aws_sdk_dynamodb::Client::from_conf(
    aws_sdk_dynamodb::Config::builder()
      .behavior_version(BehaviorVersion::latest())
      .region(Region::new("us-west-1"))
      .credentials_provider(Credentials::new("x", "x", None, None, "unit-test"))
      .http_client(http_client_fn(|_, _| SharedHttpConnector::new(NoRequests)))
      .build(),
  );
  for (snapshot_seq_nr, fail_event, fail_snapshot, event_calls, snapshot_calls) in
    [(1, true, true, 0, 0), (2, true, false, 1, 0), (2, false, true, 1, 1)]
  {
    let event_serializer = Arc::new(CountingSerializer {
      calls: AtomicUsize::new(0),
      fail: fail_event,
    });
    let snapshot_serializer = Arc::new(CountingSerializer {
      calls: AtomicUsize::new(0),
      fail: fail_snapshot,
    });
    let store = EventStoreForDynamoDB {
      client: client.clone(),
      tables: tables(),
      options: crate::next::dynamodb::DynamoDbOptions::default(),
      store_id: "unit".into(),
      event_serializer: event_serializer.clone(),
      snapshot_serializer: snapshot_serializer.clone(),
      clock: Arc::new(super::super::clock::SystemClock),
      _aggregate_id: std::marker::PhantomData,
    };
    let result = store
      .persist_event_and_snapshot(event(2), SnapshotEnvelope::new((), snapshot_seq_nr))
      .await;
    if snapshot_seq_nr == 1 {
      assert!(matches!(
        result,
        Err(EventStoreError::ContractViolation {
          rule: ContractRule::W9,
          seq_nr: Some(2),
          snapshot_seq_nr: Some(1)
        })
      ));
    } else {
      let expected_phase = if fail_event {
        SerializationPhase::SerializeEvent
      } else {
        SerializationPhase::SerializeSnapshot
      };
      assert!(matches!(result, Err(EventStoreError::Serialization { phase, .. }) if phase == expected_phase));
    }
    assert_eq!(event_serializer.calls.load(Ordering::SeqCst), event_calls);
    assert_eq!(snapshot_serializer.calls.load(Ordering::SeqCst), snapshot_calls);
  }
}

#[test]
fn should_reject_head_overhead_when_the_complete_journal_fits() {
  let event = EventEnvelope::new(Id("x".repeat(1022), "".into()), 2, Utc::now(), ());
  let aid = check_event(&event).unwrap();
  let small = transaction_items(&tables(), &aid, &event, Vec::new()).unwrap();
  let mut journal = small[0].put().unwrap().item().clone();
  journal.insert("payload".into(), AttributeValue::B(Blob::new(vec![0; 408002])));
  assert!(item_size_upper_bound(&journal) <= ITEM_SIZE_LIMIT);
  let mut metadata = journal.clone();
  metadata.remove("aid");
  let head = HashMap::from([
    ("aid".into(), journal["aid"].clone()),
    ("type_name".into(), AttributeValue::S("x".repeat(1022))),
    ("seq_nr".into(), journal["seq_nr"].clone()),
    ("events".into(), AttributeValue::L(vec![AttributeValue::M(metadata)])),
  ]);
  assert!(item_size_upper_bound(&head) > ITEM_SIZE_LIMIT);
  assert!(matches!(
    transaction_items(&tables(), &aid, &event, vec![0; 408002]),
    Err(EventStoreError::ContractViolation {
      rule: ContractRule::ItemSizeLimit,
      ..
    })
  ));
}

#[test]
fn should_classify_head_conditions_using_only_typed_old_item_numbers() {
  let new = classify("None", "ConditionalCheckFailed", Some(AttributeValue::N("4".into())), 1);
  assert!(matches!(
    new,
    EventStoreError::OptimisticLock {
      seq_nr: 1,
      head_seq_nr: None,
      ..
    }
  ));
  for seq_nr in [2, 4] {
    let duplicate = classify(
      "None",
      "ConditionalCheckFailed",
      Some(AttributeValue::N("4".into())),
      seq_nr,
    );
    assert!(matches!(
      duplicate,
      EventStoreError::OptimisticLock {
        head_seq_nr: Some(4),
        ..
      }
    ));
  }
  for (seq_nr, old) in [(6, Some(AttributeValue::N("4".into()))), (2, None)] {
    let gap = classify("None", "ConditionalCheckFailed", old, seq_nr);
    assert!(
      matches!(gap, EventStoreError::ContractViolation { rule: ContractRule::W8Gap, seq_nr: Some(n), .. } if n == seq_nr)
    );
  }
}

#[test]
fn should_prioritize_conflict_then_head_then_journal_conditions() {
  for (journal, head) in [
    ("TransactionConflict", "ConditionalCheckFailed"),
    ("ConditionalCheckFailed", "TransactionConflict"),
    ("ThrottlingError", "TransactionConflict"),
  ] {
    assert!(matches!(
      classify(journal, head, None, 3),
      EventStoreError::OptimisticLock { head_seq_nr: None, .. }
    ));
  }
  assert!(matches!(
    classify("ConditionalCheckFailed", "ConditionalCheckFailed", None, 3),
    EventStoreError::ContractViolation {
      rule: ContractRule::W8Gap,
      ..
    }
  ));
  assert!(matches!(
    classify("ThrottlingError", "ConditionalCheckFailed", None, 3),
    EventStoreError::ContractViolation {
      rule: ContractRule::W8Gap,
      ..
    }
  ));
  assert!(matches!(
    classify("ConditionalCheckFailed", "ThrottlingError", None, 3),
    EventStoreError::OptimisticLock { head_seq_nr: None, .. }
  ));
}

#[test]
fn should_keep_the_original_sdk_error_for_unexpected_head_and_other_cancellations() {
  for (journal, head, old) in [
    ("None", "ConditionalCheckFailed", Some(AttributeValue::N("1".into()))),
    (
      "ConditionalCheckFailed",
      "ConditionalCheckFailed",
      Some(AttributeValue::N("1".into())),
    ),
    ("None", "ConditionalCheckFailed", Some(AttributeValue::N("NaN".into()))),
    (
      "None",
      "ConditionalCheckFailed",
      Some(AttributeValue::N((SEQ_NR_MAX + 1).to_string())),
    ),
    ("None", "ConditionalCheckFailed", Some(AttributeValue::S("1".into()))),
    ("ThrottlingError", "None", None),
    ("None", "ProvisionedThroughputExceeded", None),
    ("None", "None", None),
  ] {
    let error = classify(journal, head, old, 2);
    assert!(!error.to_string().contains("SDK_SENTINEL"));
    let EventStoreError::Storage { operation, source } = error else {
      panic!("{error:?}")
    };
    assert_eq!(operation, StorageOperation::Append);
    let sdk = source.downcast_ref::<SdkError<TransactWriteItemsError>>().unwrap();
    assert!(matches!(
      sdk.as_service_error(),
      Some(TransactWriteItemsError::TransactionCanceledException(_))
    ));
    assert_eq!(
      sdk.raw_response().unwrap().body().bytes().unwrap(),
      b"original-response"
    );
  }
}
