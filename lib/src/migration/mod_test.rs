use super::*;

#[tokio::test]
async fn should_reject_overlapping_tables_before_any_request() {
  let (client, replay) = test_support::client(vec![]);
  let legacy = LegacyDynamoDbTables {
    journal_table_name: "old-events".into(),
    snapshot_table_name: "old-state".into(),
  };
  let tables = DynamoDbTables {
    journal_table_name: "old-events".into(),
    snapshot_table_name: "new-state".into(),
    head_table_name: "new-head".into(),
    snapshot_history_index_name: "history".into(),
  };
  let report = migrate_v3_dynamodb(&client, &legacy, &tables, &HashMap::new())
    .await
    .unwrap();
  assert_eq!(report.reasons.len(), 1);
  assert_eq!((report.events, report.snapshots, report.aggregates), (0, 0, 0));
  assert!(replay.requests.lock().unwrap().is_empty());
}

#[test]
fn should_keep_confirmed_counts_in_storage_errors_and_json_reports() {
  let report = MigrationReport {
    events: 2,
    ..Default::default()
  };
  let error = MigrationError::new(&report, "PutItem", std::io::Error::other("cause"));
  assert_eq!(error.report.events, 2);
  assert!(!error.to_string().contains("cause"));
  let restored: MigrationReport = serde_json::from_str(&serde_json::to_string(&report).unwrap()).unwrap();
  assert_eq!(restored, report);
}
