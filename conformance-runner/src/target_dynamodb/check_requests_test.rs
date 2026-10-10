use super::*;
use crate::fault::Phase;

fn report(entries: Vec<(&str, Phase, Value, Value)>) -> OperationReport {
  let mut requests = Vec::new();
  let mut responses = Vec::new();
  for (api, phase, body, response) in entries {
    requests.push(RequestObservation {
      api: api.into(),
      phase: Some(phase),
      body,
    });
    responses.push(json!({"api":api,"phase":phase,"delivered":{"body":response.to_string()}}));
  }
  OperationReport {
    requests,
    responses,
    unfired: Vec::new(),
    applications: Vec::new(),
    unfinished_history: false,
  }
}

fn context<'a>(layout: &'a RequestLayout, waits: &'a [u128], history: Option<&'a Value>) -> RequestContext<'a> {
  RequestContext {
    layout,
    aid: Some("Order-9"),
    seq_nr: Some(4),
    history,
    projection: Some("KEYS_ONLY"),
    waits,
  }
}

#[test]
fn should_check_configuration_keys_puts_conditions_and_real_bindings() {
  let layout = RequestLayout::new("j", "s", "h", "gsi").unwrap();
  let mut puts = Vec::new();
  for table in ["j", "s", "h"] {
    puts.push(json!({"Put":{"TableName":table,"Item":{"aid":{"S":"__config__"},"store_id":{"S":"actual-id"},"layout_version":{"N":"1"}},
    "ConditionExpression":"attribute_not_exists(#id)","ExpressionAttributeNames":{"#id":"aid"}}}));
  }
  let read = json!({"RequestItems":{"j":{"ConsistentRead":true,"Keys":[{"aid":{"S":"__config__"},"seq_nr":{"N":"0"}}]},
    "s":{"ConsistentRead":true,"Keys":[{"aid":{"S":"__config__"},"skey":{"N":"0"}}]},"h":{"ConsistentRead":true,"Keys":[{"aid":{"S":"__config__"}}]}}});
  let mut report = report(vec![
    ("BatchGetItem", Phase::ConfigurationRead, read, json!({})),
    (
      "TransactWriteItems",
      Phase::ConfigurationCreate,
      json!({"TransactItems":puts}),
      json!({}),
    ),
  ]);
  let expected = json!({"request_count":{"configuration-read":1,"configuration-create":1,"classify-condition-failure-read":0},"no_requests_in_phases":["retention-mark"],"requests":[
    {"api":"BatchGetItem","phase":"configuration-read","constraints":{"keys":["journal:__config__:0","snapshot:__config__:0","head:__config__"],"consistent_read_all_tables":true}},
    {"api":"TransactWriteItems","phase":"configuration-create","constraints":{"put_tables":["journal","snapshot","head"],"same_store_id":true,"layout_version":1,"condition":{"attribute_not_exists":"aid"}}}]});
  assert!(check(&expected, &report, &context(&layout, &[], None))
    .errors
    .is_empty());
  let mut different = expected.clone();
  different["requests"][1]["constraints"]["layout_version"] = json!(2);
  assert!(!check(&different, &report, &context(&layout, &[], None))
    .errors
    .is_empty());
  report.requests[1].body["TransactItems"][1]["Put"]["Item"]["store_id"]["S"] = json!("other-id");
  assert!(!check(&expected, &report, &context(&layout, &[], None))
    .errors
    .is_empty());
  report.requests.push(RequestObservation {
    api: "GetItem".into(),
    phase: None,
    body: json!({}),
  });
  assert_eq!(
    phase_name(report.requests.last().unwrap()),
    "classify-condition-failure-read"
  );
  assert!(!check(&expected, &report, &context(&layout, &[], None))
    .errors
    .is_empty());
}

#[test]
fn should_check_only_unprocessed_keys_strong_reads_and_exponential_waits() {
  let layout = RequestLayout::new("j", "s", "h", "gsi").unwrap();
  let first = json!({"RequestItems":{"s":{"ConsistentRead":true,"Keys":[{"aid":{"S":"Order-9"},"skey":{"N":"0"}}]},"h":{"ConsistentRead":true,"Keys":[{"aid":{"S":"Order-9"}}]}}});
  let pending = json!({"h":{"ConsistentRead":true,"Keys":[{"aid":{"S":"Order-9"}}]}});
  let report = report(vec![
    (
      "BatchGetItem",
      Phase::ReadSnapshot,
      first,
      json!({"UnprocessedKeys":pending}),
    ),
    (
      "BatchGetItem",
      Phase::ReadSnapshot,
      json!({"RequestItems":pending}),
      json!({"Responses":{}}),
    ),
  ]);
  let expected = json!({"requests":[{"api":"BatchGetItem","phase":"read-snapshot","constraints":{"head_and_current_snapshot":true,"consistent_read_all_tables":true}},
    {"api":"BatchGetItem","phase":"read-snapshot","constraints":{"only_unprocessed_keys":true,"exponential_backoff":true}}]});
  assert!(check(&expected, &report, &context(&layout, &[50], None))
    .errors
    .is_empty());
  assert!(!check(&expected, &report, &context(&layout, &[100], None))
    .errors
    .is_empty());
  let twice = json!({"requests":[{"api":"BatchGetItem","phase":"read-snapshot","constraints":{}},{"api":"BatchGetItem","phase":"read-snapshot","constraints":{}},{"api":"BatchGetItem","phase":"read-snapshot","constraints":{}}]});
  assert!(!check(&twice, &report, &context(&layout, &[50], None)).errors.is_empty());
}

#[test]
fn should_check_inclusive_bound_queries_pages_and_written_history() {
  let layout = RequestLayout::new("j", "s", "h", "gsi").unwrap();
  let mut first = json!({"TableName":"j","ConsistentRead":true,"ScanIndexForward":true,"KeyConditionExpression":"#id = :id AND (#seq >= :seq)",
    "ExpressionAttributeNames":{"#id":"aid","#seq":"seq_nr"},"ExpressionAttributeValues":{":id":{"S":"Order-9"},":seq":{"N":"4"}}});
  let mut second = first.clone();
  second["ExclusiveStartKey"] = json!({"aid":{"S":"Order-9"},"seq_nr":{"N":"7"}});
  let events = report(vec![
    (
      "Query",
      Phase::ReadEvents,
      first.clone(),
      json!({"LastEvaluatedKey":second["ExclusiveStartKey"]}),
    ),
    ("Query", Phase::ReadEvents, second, json!({})),
  ]);
  let expected = json!({"minimum_request_count":{"read-events":2},"requests":[{"api":"Query","phase":"read-events","constraints":{"table":"journal","consistent_read":true,"scan_index_forward":true,"follow_last_evaluated_key":true,
    "key_condition":{"all":[{"attribute":"aid","operator":"eq","argument":"aggregate_id"},{"attribute":"seq_nr","operator":"gte","argument":"seq_nr"}]}}}]});
  assert!(check(&expected, &events, &context(&layout, &[], None))
    .errors
    .is_empty());
  first["TableName"] = json!("s");
  first["IndexName"] = json!("gsi");
  first["ScanIndexForward"] = json!(false);
  let retention = report(vec![("Query", Phase::RetentionQuery, first, json!({}))]);
  let history = json!({"active":[3,4],"marked":[]});
  let expected = json!({"requests":[{"api":"Query","phase":"retention-query","constraints":{"table":"snapshot","index":"configured-history-index","projection":"KEYS_ONLY","scan_index_forward":false,"include_just_written_history":true}}]});
  assert!(check(&expected, &retention, &context(&layout, &[], Some(&history)))
    .errors
    .is_empty());
  assert!(!check(
    &expected,
    &retention,
    &context(&layout, &[], Some(&json!({"active":[3]})))
  )
  .errors
  .is_empty());
}

#[test]
fn should_check_delete_batches_retries_ttl_expressions_and_failure_returns() {
  let layout = RequestLayout::new("j", "s", "h", "gsi").unwrap();
  let deletion = json!({"DeleteRequest":{"Key":{"aid":{"S":"Order-9"},"skey":{"N":"1"}}}});
  let pending = json!({"s":[deletion]});
  let mark = json!({"TableName":"s","Key":{"aid":{"S":"Order-9"},"skey":{"N":"2"}},"ConditionExpression":"attribute_exists(#active)",
    "ExpressionAttributeNames":{"#ttl":"ttl","#active":"active_history_seq_nr"},"ExpressionAttributeValues":{":expiry":{"N":"60"}},"UpdateExpression":"SET #ttl = :expiry REMOVE #active"});
  let commit = json!({"TransactItems":[{"Update":{"TableName":"h","ReturnValuesOnConditionCheckFailure":"ALL_OLD"}}]});
  let report = report(vec![
    (
      "BatchWriteItem",
      Phase::RetentionDelete,
      json!({"RequestItems":pending}),
      json!({"UnprocessedItems":pending}),
    ),
    (
      "BatchWriteItem",
      Phase::RetentionDelete,
      json!({"RequestItems":pending}),
      json!({}),
    ),
    ("UpdateItem", Phase::RetentionMark, mark, json!({})),
    ("TransactWriteItems", Phase::Commit, commit, json!({})),
  ]);
  let expected = json!({"requests":[{"api":"BatchWriteItem","phase":"retention-delete","constraints":{"initial_batch_sizes":[1],"retry_unprocessed_items":true}},
    {"api":"UpdateItem","phase":"retention-mark","constraints":{"target_seq_nrs":[2],"expires":60,"expression_attribute_names":{"#ttl":"ttl"},
      "condition":{"attribute_exists":"active_history_seq_nr"},"update":{"set":{"ttl":{"value_binding":"expires"}},"remove":["active_history_seq_nr"]}}},
    {"api":"TransactWriteItems","phase":"commit","constraints":{"head_return_values_on_condition_check_failure":"ALL_OLD"}}]});
  assert!(check(&expected, &report, &context(&layout, &[], None))
    .errors
    .is_empty());
  let mut malformed = report;
  malformed.requests[1].body["RequestItems"] = json!({"s":[]});
  assert!(!check(&expected, &malformed, &context(&layout, &[], None))
    .errors
    .is_empty());
  assert!(!check(
    &json!({"requests":[{"api":"BatchWriteItem","phase":"retention-delete","constraints":{"unknown_word":true}}]}),
    &malformed,
    &context(&layout, &[], None)
  )
  .errors
  .is_empty());
  malformed.unfinished_history = true;
  assert!(!check(&json!({}), &malformed, &context(&layout, &[], None))
    .errors
    .is_empty());
}
