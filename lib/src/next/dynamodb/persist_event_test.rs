use aws_sdk_dynamodb::types::error::TransactionCanceledException;
use aws_sdk_dynamodb::types::CancellationReason;
use aws_smithy_runtime_api::http::{Response, StatusCode};
use aws_smithy_types::body::SdkBody;
use chrono::{DateTime, Utc};

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
  classify_append_error(cancellation(journal, head, old), &aid, seq_nr)
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
