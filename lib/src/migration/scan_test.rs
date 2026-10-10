use super::super::test_support;
use super::*;
use serde_json::json;

#[tokio::test]
async fn should_continue_after_empty_items_using_the_last_evaluated_key() {
  let key = json!({"pkey":{"S":"Account-7"},"skey":{"S":"Account-a-1"}});
  let (client, replay) = test_support::client(vec![
    (200, json!({"Items":[],"LastEvaluatedKey":key})),
    (
      200,
      json!({"Items":[{"pkey":{"S":"Account-7"},"skey":{"S":"Account-a-2"}}]}),
    ),
  ]);
  let mut scan = Scan::new(&client, "old-events");
  let report = MigrationReport::default();
  assert!(scan.next(&report).await.unwrap().unwrap().is_empty());
  assert_eq!(scan.next(&report).await.unwrap().unwrap().len(), 1);
  assert!(scan.next(&report).await.unwrap().is_none());
  let requests = replay.requests.lock().unwrap();
  assert_eq!(requests.len(), 2);
  assert_eq!(requests[1]["input"]["ExclusiveStartKey"], key);
  for request in requests.iter() {
    assert_eq!(request["input"]["ConsistentRead"], true);
    assert!(request["input"].get("IndexName").is_none());
  }
}

#[tokio::test]
async fn should_propagate_later_scan_failure_with_confirmed_counts() {
  let (client, _) = test_support::client(vec![
    (
      200,
      json!({"Items":[],"LastEvaluatedKey":{"pkey":{"S":"Account-7"},"skey":{"S":"Account-a-1"}}}),
    ),
    (500, json!({"__type":"InternalServerError","Message":"controlled"})),
  ]);
  let report = MigrationReport {
    events: 2,
    ..Default::default()
  };
  let mut scan = Scan::new(&client, "old-events");
  assert!(scan.next(&report).await.unwrap().is_some());
  let error = scan.next(&report).await.unwrap_err();
  assert_eq!(error.report.events, 2);
  assert!(error.operation.contains("old-events"));
}
