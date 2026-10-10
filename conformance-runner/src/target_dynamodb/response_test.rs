use super::super::request::RequestObservation;
use super::*;

fn request(phase: Option<Phase>) -> ParsedRequest {
  ParsedRequest {
    observation: RequestObservation {
      api: "Query".into(),
      body: json!({"TableName":"actual-journal"}),
      phase,
    },
    actions: Vec::new(),
    configuration_keys: Vec::new(),
  }
}

fn response(body: &Value, status: u16) -> HttpResponse {
  let written = body.to_string();
  let mut response = Response::new(StatusCode::try_from(status).unwrap(), SdkBody::from(written.clone()));
  response
    .headers_mut()
    .insert("content-length", written.len().to_string());
  response.headers_mut().insert("x-amzn-requestid", "actual-request-id");
  response
}

fn item(sequence: u64, payload_bytes: usize) -> Value {
  json!({"aid":{"S":"Actual-9"},"seq_nr":{"N":sequence.to_string()},
    "occurred_at":{"N":"1740000000000000000"},"manifest":{"S":""},
    "payload":{"B":aws_smithy_types::base64::encode(vec![0xA5;payload_bytes])}})
}

fn body(response: &HttpResponse) -> Value {
  serde_json::from_slice(response.body().bytes().unwrap()).unwrap()
}

#[test]
fn should_count_utf8_attribute_names_values_and_decoded_binary_bytes() {
  let value = json!({"名前":{"S":"日本"},"n":{"N":"1740000000000000000"},
    "b":{"B":aws_smithy_types::base64::encode([0,1,255,2,3])}});
  assert_eq!(journal_item_bytes(&value).unwrap(), 6 + 6 + 1 + 19 + 1 + 5);
}

#[test]
fn should_preserve_empty_terminal_below_boundary_and_exact_boundary_responses() {
  let overhead = journal_item_bytes(&item(1, 0)).unwrap();
  let items: Vec<Value> = (1..=4).map(|seq| item(seq, QUERY_PAGE_BYTES / 4 - overhead)).collect();
  assert_eq!(
    items
      .iter()
      .map(|item| journal_item_bytes(item).unwrap())
      .sum::<usize>(),
    QUERY_PAGE_BYTES
  );
  for raw in [
    json!({}),
    json!({"Items":[],"Count":0,"ScannedCount":0}),
    json!({"Items":[item(4,320022)],"Count":1,"ScannedCount":1}),
    json!({"Items":items,"Count":4,"ScannedCount":4,
      "LastEvaluatedKey":{"aid":{"S":"Actual-9"},"seq_nr":{"N":"4"}}}),
  ] {
    let original = response(&raw, 200);
    let length = original.headers().get("content-length").unwrap().to_owned();
    let delivered = event_page_response(&request(Some(Phase::ReadEvents)), original).unwrap();
    assert_eq!(body(&delivered), raw);
    assert_eq!(delivered.headers().get("content-length"), Some(length.as_str()));
    assert_eq!(delivered.headers().get("x-amzn-requestid"), Some("actual-request-id"));
  }
}

#[test]
fn should_cut_only_real_contiguous_items_and_use_the_last_delivered_journal_key() {
  let items: Vec<Value> = (1..=4).map(|seq| item(seq, 320022)).collect();
  let raw = json!({"Items":items,"Count":4,"ScannedCount":4,
    "LastEvaluatedKey":{"aid":{"S":"Actual-9"},"seq_nr":{"N":"99"}},
    "ConsumedCapacity":{"TableName":"actual-journal","CapacityUnits":1}});
  let delivered = event_page_response(&request(Some(Phase::ReadEvents)), response(&raw, 200)).unwrap();
  let observed = body(&delivered);
  assert_eq!(observed["Items"], json!(items[..3]));
  assert_eq!(
    observed["LastEvaluatedKey"],
    json!({"aid":items[2]["aid"],"seq_nr":items[2]["seq_nr"]})
  );
  assert_eq!(observed["Count"], 3);
  assert_eq!(observed["ScannedCount"], 3);
  assert_eq!(observed["ConsumedCapacity"], raw["ConsumedCapacity"]);
  assert!(delivered.headers().get("content-length").is_none());
  assert_eq!(delivered.headers().get("x-amzn-requestid"), Some("actual-request-id"));
}

#[test]
fn should_leave_retention_queries_unclassified_queries_and_errors_unchanged() {
  let raw = json!({"Items":(1..=4).map(|seq|item(seq,320022)).collect::<Vec<_>>(),"Count":4});
  for (phase, status) in [
    (Some(Phase::RetentionQuery), 200),
    (None, 200),
    (Some(Phase::ReadEvents), 400),
  ] {
    let delivered = event_page_response(&request(phase), response(&raw, status)).unwrap();
    assert_eq!(body(&delivered), raw);
    assert_eq!(delivered.status().as_u16(), status);
    assert!(delivered.headers().get("content-length").is_some());
  }
}

#[test]
fn should_reject_invalid_actual_attributes_or_missing_real_continuation_keys() {
  let mut missing_key: Vec<Value> = (1..=4).map(|seq| item(seq, 320022)).collect();
  missing_key[2].as_object_mut().unwrap().remove("aid");
  for raw in [
    json!({"Items":[{"payload":{"B":"INVALID BASE64"}}]}),
    json!({"Items":[{"payload":{"B":1}}]}),
    json!({"Items":[{"payload":{"B":"","S":""}}]}),
    json!({"Items":missing_key}),
  ] {
    assert!(event_page_response(&request(Some(Phase::ReadEvents)), response(&raw, 200)).is_err());
  }
}
