use super::super::test_support;
use super::*;
use serde_json::json;

#[tokio::test]
async fn should_stop_on_conditional_conflict_without_counting_or_retrying_the_put() {
  let (client, replay) = test_support::client(vec![(
    400,
    json!({"__type":"com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException","Message":"controlled conflict"}),
  )]);
  let item = Item::from([
    ("aid".into(), AttributeValue::S("Account-a".into())),
    ("seq_nr".into(), AttributeValue::N("2".into())),
  ]);
  let mut report = MigrationReport {
    events: 1,
    ..Default::default()
  };
  assert!(!put(&client, "events", item, &mut report).await.unwrap());
  assert_eq!(report.events, 1);
  assert_eq!(report.reasons.len(), 1);
  assert_eq!(report.reasons[0].skey.as_deref(), Some("2"));
  let requests = replay.requests.lock().unwrap();
  assert_eq!(requests.len(), 1);
  assert_eq!(requests[0]["input"]["ConditionExpression"], "attribute_not_exists(aid)");
}

#[tokio::test]
async fn should_return_storage_failures_and_success_from_conditional_put() {
  let (client, _) = test_support::client(vec![
    (200, json!({})),
    (500, json!({"__type":"InternalServerError","Message":"controlled"})),
  ]);
  let mut report = MigrationReport::default();
  let item = Item::from([
    ("aid".into(), AttributeValue::S("Account-a".into())),
    ("seq_nr".into(), AttributeValue::N("1".into())),
  ]);
  assert!(put(&client, "events", item.clone(), &mut report).await.unwrap());
  assert!(put(&client, "events", item, &mut report).await.is_err());
  assert!(report.reasons.is_empty());
  assert_eq!(invalid(&report, "GetItem", "missing item".into()).operation, "GetItem");
}

#[tokio::test]
async fn should_rescan_both_legacy_tables_even_when_no_aggregates_exist() {
  let (client, replay) = test_support::client(vec![(200, json!({"Items":[]})), (200, json!({"Items":[]}))]);
  let legacy = LegacyDynamoDbTables {
    journal_table_name: "old-events".into(),
    snapshot_table_name: "old-state".into(),
  };
  let tables = DynamoDbTables {
    journal_table_name: "events".into(),
    snapshot_table_name: "state".into(),
    head_table_name: "heads".into(),
    snapshot_history_index_name: "history".into(),
  };
  let mut report = MigrationReport::default();
  write(
    &client,
    &legacy,
    &tables,
    &HashMap::new(),
    Aggregates::new(),
    &mut report,
  )
  .await
  .unwrap();
  assert_eq!(report, MigrationReport::default());
  let requests = replay.requests.lock().unwrap();
  assert_eq!(requests.len(), 2);
  assert_eq!(requests[0]["input"]["TableName"], "old-events");
  assert_eq!(requests[1]["input"]["TableName"], "old-state");
}
