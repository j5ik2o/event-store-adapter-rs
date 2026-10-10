use super::super::test_support;
use super::*;
use serde_json::json;

#[test]
fn should_report_gap_and_head_upper_bound_independently() {
  let mut summary = AggregateSummary {
    pkey: "Account-7".into(),
    maximum: 3,
    count: 2,
    head_size: ITEM_SIZE_LIMIT + 1,
  };
  assert_eq!(summary.reasons().len(), 2);
  summary.count = 3;
  summary.head_size = ITEM_SIZE_LIMIT;
  assert!(summary.reasons().is_empty());
}

#[tokio::test]
async fn should_check_all_three_new_tables_and_report_nonconfiguration_items() {
  let (client, replay) = test_support::client(vec![
    (200, json!({"Items":[{"aid":{"S":"__config__"},"seq_nr":{"N":"0"}}]})),
    (200, json!({"Items":[{"aid":{"S":"__config__"},"skey":{"N":"9"}}]})),
    (200, json!({"Items":[{"aid":{"S":"Account-b"}}]})),
  ]);
  let tables = DynamoDbTables {
    journal_table_name: "events".into(),
    snapshot_table_name: "state".into(),
    head_table_name: "heads".into(),
    snapshot_history_index_name: "history".into(),
  };
  let mut report = MigrationReport::default();
  check_empty(&client, &tables, &mut report).await.unwrap();
  assert_eq!(report.reasons.len(), 2);
  assert_eq!(report.events, 0);
  assert_eq!(replay.requests.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn should_inspect_both_legacy_tables_and_collect_gap_and_orphan_reasons() {
  let (client, replay) = test_support::client(vec![
    (
      200,
      json!({"Items":[{"pkey":{"S":"Gap-1"},"skey":{"S":"Gap-a-2"},"aid":{"S":"display"},"seq_nr":{"N":"2"},"occurred_at":{"N":"0"},"payload":{"B":"AA=="}}]}),
    ),
    (
      200,
      json!({"Items":[{"pkey":{"S":"Alone-1"},"skey":{"S":"Alone-a-0"},"aid":{"S":"display"},"seq_nr":{"N":"1"},"last_updated_at":{"N":"0"},"payload":{"B":"AA=="},"ttl":{"N":"0"}}]}),
    ),
  ]);
  let tables = LegacyDynamoDbTables {
    journal_table_name: "old-events".into(),
    snapshot_table_name: "old-state".into(),
  };
  let mut report = MigrationReport::default();
  let aggregates = inspect(&client, &tables, &HashMap::new(), &mut report).await.unwrap();
  assert_eq!(aggregates.len(), 1);
  assert!(report.reasons.iter().any(|rejection| rejection.reason.contains("P-36")));
  assert!(report
    .reasons
    .iter()
    .any(|rejection| rejection.reason.contains("孤立snapshot")));
  assert_eq!(replay.requests.lock().unwrap().len(), 2);
  assert_eq!(report.events, 0);
}
