use std::collections::VecDeque;

use tokio::sync::Notify;
use tracing::field::{Field, Visit};
use tracing::instrument::WithSubscriber;
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;
use tracing_subscriber::Layer;

use super::*;

#[derive(Debug, Clone, Default)]
pub(super) struct RecordedSleep(Arc<Mutex<Vec<Duration>>>);

impl AsyncSleep for RecordedSleep {
  fn sleep(&self, delay: Duration) -> Sleep {
    let recorded = self.clone();
    Sleep::new(async move { recorded.0.lock().unwrap().push(delay) })
  }
}

#[derive(Debug, Clone)]
enum Step {
  Query {
    limit: Option<u64>,
    omit: Vec<u64>,
    duplicate: Vec<u64>,
    continue_pages: bool,
    fail_next_page: bool,
  },
  Unprocessed(Vec<u64>),
  MissingIndex,
  MissingTable,
  Pause {
    step: Box<Step>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
  },
}

impl Step {
  fn matches(&self, api: &str) -> bool {
    match self {
      Self::Query { .. } | Self::MissingIndex => api == "Query",
      Self::Unprocessed(_) | Self::MissingTable => api == "BatchWriteItem",
      Self::Pause { step, .. } => step.matches(api),
    }
  }

  fn description(&self) -> Value {
    match self {
      Self::Query {
        limit,
        omit,
        duplicate,
        continue_pages,
        fail_next_page,
      } => {
        json!({"api": "Query", "limit": limit, "omit": omit, "duplicate": duplicate, "continue_pages": continue_pages, "fail_next_page": fail_next_page})
      }
      Self::Unprocessed(numbers) => json!({"api": "BatchWriteItem", "unprocessed": numbers}),
      Self::MissingIndex => json!({"api": "Query", "missing_index": true}),
      Self::MissingTable => json!({"api": "BatchWriteItem", "missing_table": true}),
      Self::Pause { step, .. } => json!({"pause_before_delivery": step.description()}),
    }
  }
}

fn query_step() -> Step {
  Step::Query {
    limit: None,
    omit: Vec::new(),
    duplicate: Vec::new(),
    continue_pages: false,
    fail_next_page: false,
  }
}

#[derive(Debug, Default)]
pub(super) struct Plan {
  pending: VecDeque<Step>,
  declarations: Vec<Value>,
  fired: usize,
  applied: usize,
}

impl Plan {
  fn declare(&mut self, step: Step) {
    self.declarations.push(step.description());
    self.pending.push_back(step);
  }

  fn report(&self) -> Value {
    json!({"declared": self.declarations.len(), "fired": self.fired, "applied": self.applied,
      "unfired": self.pending.len(), "declarations": self.declarations,
      "remaining": self.pending.iter().map(Step::description).collect::<Vec<_>>()})
  }
}

impl Observed {
  fn install_retention(&self, steps: Vec<Step>) {
    let mut plan = Plan::default();
    for step in steps {
      plan.declare(step);
    }
    *self.retention_plan.lock().unwrap() = plan;
  }

  fn retention_report(&self) -> Value {
    self.retention_plan.lock().unwrap().report()
  }
}

fn response_body(trace: &Value, field: &str) -> Value {
  serde_json::from_str(trace[field].as_str().unwrap()).unwrap()
}

fn delete_number(write: &Value) -> u64 {
  assert!(write.get("PutRequest").is_none());
  write["DeleteRequest"]["Key"]["skey"]["N"]
    .as_str()
    .unwrap()
    .parse()
    .unwrap()
}

fn merge_unprocessed(delivered: &mut Value, table: &str, injected: Vec<Value>) {
  if injected.is_empty() {
    return;
  }
  let tables = delivered
    .as_object_mut()
    .unwrap()
    .entry("UnprocessedItems")
    .or_insert_with(|| json!({}));
  let writes = tables
    .as_object_mut()
    .unwrap()
    .entry(table)
    .or_insert_with(|| json!([]))
    .as_array_mut()
    .unwrap();
  for write in injected {
    if !writes.contains(&write) {
      writes.push(write);
    }
  }
}

fn replace_body(request: &mut HttpRequest, input: &Value) {
  let body = serde_json::to_vec(input).unwrap();
  request.headers_mut().remove("content-length");
  *request.body_mut() = SdkBody::from(body);
}

pub(super) async fn deliver_retention(
  mut request: HttpRequest,
  upstream: SharedHttpConnector,
  traces: Traces,
  index: usize,
  plan: Arc<Mutex<Plan>>,
) -> Result<Response<SdkBody>, ConnectorError> {
  let api = request
    .headers()
    .get("x-amz-target")
    .unwrap()
    .rsplit('.')
    .next()
    .unwrap()
    .to_string();
  let input: Value = serde_json::from_slice(request.body().bytes().unwrap()).unwrap();
  let selected = {
    let mut plan = plan.lock().unwrap();
    if plan.pending.front().is_some_and(|step| step.matches(&api)) {
      plan.fired += 1;
      plan.pending.pop_front()
    } else {
      None
    }
  };
  let declaration = selected.as_ref().map(Step::description);
  let (step, pause) = match selected {
    Some(Step::Pause { step, entered, release }) => (Some(*step), Some((entered, release))),
    step => (step, None),
  };
  let mut forwarded = input.clone();
  let mut injected = Vec::new();
  let mut table = None;
  match &step {
    Some(Step::Query { limit: Some(limit), .. }) => forwarded["Limit"] = json!(limit),
    Some(Step::Unprocessed(numbers)) => {
      let tables = input["RequestItems"].as_object().unwrap();
      assert_eq!(tables.len(), 1);
      let (name, writes) = tables.iter().next().unwrap();
      let writes = writes.as_array().unwrap();
      injected = writes
        .iter()
        .filter(|write| numbers.contains(&delete_number(write)))
        .cloned()
        .collect();
      assert_eq!(
        injected.len(),
        numbers.len(),
        "未処理計画は実要求内のキーだけを指定する"
      );
      let actual = writes
        .iter()
        .filter(|write| !injected.contains(write))
        .cloned()
        .collect::<Vec<_>>();
      forwarded["RequestItems"][name] = json!(actual);
      table = Some(name.clone());
    }
    Some(Step::MissingIndex) => {
      forwarded["IndexName"] = json!(format!("missing-{}", input["IndexName"].as_str().unwrap()))
    }
    Some(Step::MissingTable) => {
      let tables = forwarded["RequestItems"].as_object_mut().unwrap();
      assert_eq!(tables.len(), 1);
      let name = tables.keys().next().unwrap().clone();
      let writes = tables.remove(&name).unwrap();
      tables.insert(format!("missing-{name}"), writes);
    }
    Some(Step::Pause { .. }) => unreachable!(),
    Some(Step::Query { limit: None, .. }) | None => {}
  }
  let empty_batch = api == "BatchWriteItem"
    && forwarded["RequestItems"]
      .as_object()
      .unwrap()
      .values()
      .all(|writes| writes.as_array().unwrap().is_empty());
  let (mut response, original, upstream_status) = if empty_batch {
    (
      Response::new(StatusCode::try_from(200).unwrap(), SdkBody::from("{}")),
      None,
      None,
    )
  } else {
    replace_body(&mut request, &forwarded);
    let mut response = upstream.call(request).await?;
    let body = std::mem::replace(response.body_mut(), SdkBody::taken());
    let bytes = ByteStream::new(body).collect().await.unwrap().into_bytes();
    let original = String::from_utf8(bytes.to_vec()).unwrap();
    let status = response.status().as_u16();
    *response.body_mut() = SdkBody::from(bytes);
    (response, Some(original), Some(status))
  };
  let mut delivered: Value = serde_json::from_slice(response.body().bytes().unwrap()).unwrap();
  let mut continuation = None;
  if response.status().as_u16() == 200 {
    if let Some(name) = &table {
      merge_unprocessed(&mut delivered, name, injected);
    }
    if let Some(Step::Query {
      omit,
      duplicate,
      continue_pages,
      fail_next_page,
      ..
    }) = &step
    {
      let items = delivered["Items"].as_array_mut().unwrap();
      items.retain(|item| !omit.contains(&item["skey"]["N"].as_str().unwrap().parse::<u64>().unwrap()));
      let repeated = items
        .iter()
        .filter(|item| duplicate.contains(&item["skey"]["N"].as_str().unwrap().parse::<u64>().unwrap()))
        .cloned()
        .collect::<Vec<_>>();
      items.extend(repeated);
      delivered["Count"] = json!(items.len());
      if delivered
        .get("LastEvaluatedKey")
        .is_some_and(|key| !key.as_object().unwrap().is_empty())
      {
        if *fail_next_page {
          continuation = Some(Step::MissingIndex);
        } else if *continue_pages {
          continuation = step.clone();
        }
      }
    }
  }
  *response.body_mut() = SdkBody::from(delivered.to_string());
  traces.lock().unwrap()[index] = json!({"api": api, "input": input,
    "forwarded_input": if empty_batch { Value::Null } else { forwarded },
    "upstream_body": original, "upstream_status": upstream_status,
    "delivered_body": null, "delivered_status": null, "injection": declaration,
    "injected_response": null});
  if let Some((entered, release)) = pause {
    entered.notify_one();
    release.notified().await;
  }
  if step.is_some() {
    let mut plan = plan.lock().unwrap();
    plan.applied += 1;
    if let Some(next) = continuation {
      plan.declarations.push(next.description());
      plan.pending.push_front(next);
    }
  }
  {
    let mut traces = traces.lock().unwrap();
    traces[index]["delivered_body"] = json!(delivered.to_string());
    traces[index]["delivered_status"] = json!(response.status().as_u16());
  }
  Ok(response)
}

#[derive(Default)]
struct Fields(serde_json::Map<String, Value>);

impl Visit for Fields {
  fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
    self.0.insert(field.name().into(), json!(format!("{value:?}")));
  }

  fn record_str(&mut self, field: &Field, value: &str) {
    self.0.insert(field.name().into(), json!(value));
  }

  fn record_u64(&mut self, field: &Field, value: u64) {
    self.0.insert(field.name().into(), json!(value));
  }
}

struct Capture(Traces);

impl<S: Subscriber> Layer<S> for Capture {
  fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
    if event.metadata().target() != "event_store_adapter::retention" {
      return;
    }
    let mut fields = Fields::default();
    event.record(&mut fields);
    fields.0.insert("target".into(), json!(event.metadata().target()));
    fields
      .0
      .insert("level".into(), json!(event.metadata().level().to_string()));
    self.0.lock().unwrap().push(Value::Object(fields.0));
  }
}

fn history_numbers(state: &Value) -> Vec<u64> {
  state["snapshot"]
    .as_array()
    .unwrap()
    .iter()
    .filter_map(|item| {
      let number = item["skey"]["N"].as_str().unwrap().parse::<u64>().unwrap();
      (number > 0).then_some(number)
    })
    .collect()
}

fn assert_retention_only(committed: &Value, after: &Value) {
  for name in ["journal", "head", "configuration"] {
    assert_eq!(after[name], committed[name]);
  }
  assert_eq!(after["snapshot"][0], committed["snapshot"][0]);
  for item in after["snapshot"].as_array().unwrap() {
    let original = committed["snapshot"]
      .as_array()
      .unwrap()
      .iter()
      .find(|original| original["skey"] == item["skey"])
      .unwrap();
    assert_eq!(item, original);
  }
}

async fn seed(fixture: &Fixture, value: &str, count: u64) {
  let (store, observed) = fixture
    .open_json_with_retention(RetentionSettings::keep_latest(100))
    .await;
  for number in 1..=count {
    append(&store, value, number, true).await.unwrap();
  }
  let state = fixture.state(&format!("Account-{value}")).await;
  assert_eq!(history_numbers(&state), (1..=count).collect::<Vec<_>>());
  fixture.record(
    &format!("retention-seed-{value}"),
    &observed.take(),
    &json!({"state": state}),
  );
}

async fn append(store: &JsonStore, value: &str, number: u64, pair: bool) -> Result<(), EventStoreError> {
  let event =
    event(Id::new("Account", value), number, json!({"event": number})).with_manifest(format!("event-{number}"));
  if pair {
    store
      .persist_event_and_snapshot(
        event,
        SnapshotEnvelope::new(json!({"state": number}), number).with_manifest(format!("snapshot-{number}")),
      )
      .await
  } else {
    store.persist_event(event).await
  }
}

struct AppendCase<'a> {
  value: &'a str,
  number: u64,
  steps: Vec<Step>,
}

async fn perform_pair(fixture: &Fixture, store: &JsonStore, observed: &Observed, mut case: AppendCase<'_>) -> Value {
  let aid = format!("Account-{}", case.value);
  let before = fixture.state(&aid).await;
  let entered = Arc::new(Notify::new());
  let release = Arc::new(Notify::new());
  let first = if case.steps.first().is_some_and(|step| step.matches("Query")) {
    case.steps.remove(0)
  } else {
    query_step()
  };
  case.steps.insert(
    0,
    Step::Pause {
      step: Box::new(first),
      entered: entered.clone(),
      release: release.clone(),
    },
  );
  observed.install_retention(case.steps);
  observed.sleep.0.lock().unwrap().clear();
  let notices = Arc::new(Mutex::new(Vec::new()));
  let subscriber = tracing_subscriber::registry().with(Capture(notices.clone()));
  let write = append(store, case.value, case.number, true).with_subscriber(subscriber);
  let (result, committed) = tokio::join!(write, async {
    entered.notified().await;
    let state = fixture.state(&aid).await;
    release.notify_one();
    state
  });
  assert!(result.is_ok(), "{result:?}");
  let traces = observed.take();
  assert_eq!(traces[0]["api"], "TransactWriteItems");
  assert_eq!(traces[0]["upstream_status"], 200);
  pair_actions(fixture, &traces[0], true);
  let after = fixture.state(&aid).await;
  assert_retention_only(&committed, &after);
  assert_head_matches_journal(&after, "Account");
  let result = json!({"before": before, "committed": committed, "after": after,
    "result": result_json(&result), "notifications": *notices.lock().unwrap(),
    "plan": observed.retention_report(), "waits_ms": observed.sleep.0.lock().unwrap().iter().map(|delay| delay.as_millis() as u64).collect::<Vec<_>>(), "traces": traces});
  fixture.record(&format!("retention-{}-{}", case.value, case.number), &traces, &result);
  assert_public_reads(fixture, store, observed, case.value, &after).await;
  result
}

async fn perform_event_only(
  fixture: &Fixture,
  store: &JsonStore,
  observed: &Observed,
  value: &str,
  number: u64,
) -> Value {
  let aid = format!("Account-{value}");
  let before = fixture.state(&aid).await;
  observed.install_retention(vec![Step::MissingIndex, Step::MissingTable]);
  observed.sleep.0.lock().unwrap().clear();
  let notices = Arc::new(Mutex::new(Vec::new()));
  let subscriber = tracing_subscriber::registry().with(Capture(notices.clone()));
  let result = append(store, value, number, false).with_subscriber(subscriber).await;
  assert!(result.is_ok(), "{result:?}");
  let traces = observed.take();
  assert_eq!(traces.len(), 1);
  assert_eq!(traces[0]["api"], "TransactWriteItems");
  assert_eq!(traces[0]["upstream_status"], 200);
  assert!(traces[0]["injected_response"].is_null());
  assert_eq!(traces[0]["upstream_body"], traces[0]["delivered_body"]);
  actions(fixture, &traces[0]);
  let after = fixture.state(&aid).await;
  assert_eq!(after["snapshot"], before["snapshot"]);
  assert_eq!(after["configuration"], before["configuration"]);
  let previous_events = before["journal"].as_array().unwrap();
  let saved_events = after["journal"].as_array().unwrap();
  assert_eq!(saved_events.len(), previous_events.len() + 1);
  assert_eq!(&saved_events[..previous_events.len()], previous_events);
  assert_saved_event(
    saved_events.last().unwrap(),
    &aid,
    number,
    -876543211,
    &format!("event-{number}"),
    &serde_json::to_vec(&json!({"event": number})).unwrap(),
  );
  assert_eq!(after["head"]["seq_nr"], json!({"N": number.to_string()}));
  assert_head_matches_journal(&after, "Account");
  let plan = observed.retention_report();
  assert_eq!(plan["declared"], 2);
  assert_eq!(plan["fired"], 0);
  assert_eq!(plan["applied"], 0);
  assert_eq!(plan["unfired"], 2);
  assert!(notices.lock().unwrap().is_empty());
  assert!(observed.sleep.0.lock().unwrap().is_empty());
  let result = json!({"before": before, "after": after, "result": result_json(&result),
    "notifications": *notices.lock().unwrap(), "plan": plan, "waits_ms": [], "traces": traces});
  fixture.record(&format!("retention-event-only-{value}-{number}"), &traces, &result);
  assert_public_reads(fixture, store, observed, value, &after).await;
  result
}

async fn assert_public_reads(fixture: &Fixture, store: &JsonStore, observed: &Observed, value: &str, state: &Value) {
  let id = Id::new("Account", value);
  let events = store.get_events_by_id_since_seq_nr(&id, 0).await.unwrap();
  assert_eq!(events.len(), state["journal"].as_array().unwrap().len());
  for (event, item) in events.iter().zip(state["journal"].as_array().unwrap()) {
    assert_eq!(event.seq_nr().to_string(), item["seq_nr"]["N"]);
    assert_eq!(
      event.occurred_at().timestamp_nanos_opt().unwrap().to_string(),
      item["occurred_at"]["N"]
    );
    assert_eq!(event.manifest(), item["manifest"]["S"]);
    assert_eq!(
      json!(serde_json::to_vec(event.payload()).unwrap()),
      item["payload"]["B"]
    );
  }
  let latest = store.get_latest_snapshot_by_id(&id).await.unwrap().unwrap();
  assert_eq!(latest.head_seq_nr().to_string(), state["head"]["seq_nr"]["N"]);
  let current = latest.snapshot().unwrap();
  assert_eq!(current.seq_nr().to_string(), state["snapshot"][0]["seq_nr"]["N"]);
  assert_eq!(current.manifest(), state["snapshot"][0]["manifest"]["S"]);
  assert_eq!(
    json!(serde_json::to_vec(current.aggregate()).unwrap()),
    state["snapshot"][0]["payload"]["B"]
  );
  fixture.record(&format!("retention-read-{value}-{}", latest.head_seq_nr()), &observed.take(),
    &json!({"head_seq_nr": latest.head_seq_nr(), "snapshot_seq_nr": current.seq_nr(), "events": events.iter().map(|event| event.seq_nr()).collect::<Vec<_>>() }));
}

fn batch_traces(observation: &Value) -> Vec<&Value> {
  observation["traces"]
    .as_array()
    .unwrap()
    .iter()
    .filter(|trace| trace["api"] == "BatchWriteItem")
    .collect()
}

fn batch_numbers(trace: &Value, field: &str, table: &str) -> Vec<u64> {
  trace[field]["RequestItems"][table]
    .as_array()
    .unwrap()
    .iter()
    .map(delete_number)
    .collect()
}

fn assert_warning(observation: &Value, aid: &str, number: u64, phase: &str) {
  let notifications = observation["notifications"].as_array().unwrap();
  assert_eq!(notifications.len(), 1);
  let notice = &notifications[0];
  assert_eq!(notice["target"], "event_store_adapter::retention");
  assert_eq!(notice["level"], "WARN");
  assert_eq!(notice["category"], "retention-failure");
  assert_eq!(notice["aid"], aid);
  assert_eq!(notice["seq_nr"], number);
  assert_eq!(notice["phase"], phase);
  assert!(!notice["error"].as_str().unwrap().is_empty());
}

#[tokio::test]
async fn should_skip_retention_for_both_writes_without_a_count_in_either_mode() {
  for retention in [
    RetentionSettings::current_only(),
    RetentionSettings::current_only().with_mode(RetentionMode::Ttl { grace_seconds: 60 }),
  ] {
    let fixture = Fixture::new().await;
    let (store, observed) = fixture.open_json_with_retention(retention).await;
    let notices = Arc::new(Mutex::new(Vec::new()));
    for (number, pair) in [(1, true), (2, false), (3, true)] {
      observed.install_retention(vec![Step::MissingIndex, Step::MissingTable]);
      let subscriber = tracing_subscriber::registry().with(Capture(notices.clone()));
      let result = append(&store, "skip-retention", number, pair)
        .with_subscriber(subscriber)
        .await;
      assert!(result.is_ok());
      let traces = observed.take();
      assert_eq!(traces.len(), 1);
      if pair {
        pair_actions(&fixture, &traces[0], false);
      } else {
        actions(&fixture, &traces[0]);
      }
      let report = observed.retention_report();
      assert_eq!(report["declared"], 2);
      assert_eq!(report["fired"], 0);
      assert_eq!(report["applied"], 0);
      assert_eq!(report["unfired"], 2);
      let state = fixture.state("Account-skip-retention").await;
      assert!(history_numbers(&state).is_empty());
      for item in state["snapshot"].as_array().unwrap() {
        assert!(item.get("ttl").is_none());
      }
      fixture.record(
        &format!("retention-skip-{number}"),
        &traces,
        &json!({"result": result_json(&result), "state": state, "plan": report}),
      );
    }
    assert!(notices.lock().unwrap().is_empty());
    let state = fixture.state("Account-skip-retention").await;
    assert_public_reads(&fixture, &store, &observed, "skip-retention", &state).await;
    fixture.close().await;
  }
}

#[tokio::test]
async fn should_leave_excess_history_unchanged_on_event_only_until_the_same_store_snapshot_append() {
  for keep in [1, 3] {
    let fixture = Fixture::new().await;
    let count = if keep == 1 { 2 } else { 5 };
    seed(&fixture, "event-only", count).await;
    let (store, observed) = fixture
      .open_json_with_retention(RetentionSettings::keep_latest(keep))
      .await;
    let first = perform_event_only(&fixture, &store, &observed, "event-only", count + 1).await;
    assert_eq!(history_numbers(&first["after"]), (1..=count).collect::<Vec<_>>());
    let second = perform_event_only(&fixture, &store, &observed, "event-only", count + 2).await;
    assert_eq!(second["before"], first["after"]);
    assert_eq!(history_numbers(&second["after"]), (1..=count).collect::<Vec<_>>());
    let recovered = perform_pair(
      &fixture,
      &store,
      &observed,
      AppendCase {
        value: "event-only",
        number: count + 3,
        steps: Vec::new(),
      },
    )
    .await;
    assert_eq!(recovered["before"], second["after"]);
    assert_eq!(
      history_numbers(&recovered["after"]),
      if keep == 1 { vec![5] } else { vec![4, 5, 8] }
    );
    assert_eq!(
      recovered["after"]["head"]["seq_nr"],
      json!({"N": (count + 3).to_string()})
    );
    assert!(recovered["notifications"].as_array().unwrap().is_empty());
    fixture.close().await;
  }
}

#[tokio::test]
async fn should_select_latest_one_or_multiple_from_all_real_gsi_pages_with_a_missing_written_history() {
  for keep in [1, 3] {
    let fixture = Fixture::new().await;
    seed(&fixture, "pages", 5).await;
    seed(&fixture, "pages-other", 2).await;
    let other = fixture.state("Account-pages-other").await;
    let (store, observed) = fixture
      .open_json_with_retention(RetentionSettings::keep_latest(keep))
      .await;
    let result = perform_pair(
      &fixture,
      &store,
      &observed,
      AppendCase {
        value: "pages",
        number: 6,
        steps: vec![Step::Query {
          limit: Some(2),
          omit: vec![6],
          duplicate: vec![5],
          continue_pages: true,
          fail_next_page: false,
        }],
      },
    )
    .await;
    assert_eq!(
      history_numbers(&result["after"]),
      ((7 - keep as u64)..=6).collect::<Vec<_>>()
    );
    assert!(result["notifications"].as_array().unwrap().is_empty());
    let queries = result["traces"]
      .as_array()
      .unwrap()
      .iter()
      .filter(|trace| trace["api"] == "Query")
      .collect::<Vec<_>>();
    assert!(queries.len() >= 3);
    for (index, trace) in queries.iter().enumerate() {
      let input = &trace["input"];
      assert_eq!(input["TableName"], fixture.tables.snapshot_table_name);
      assert_eq!(input["IndexName"], fixture.tables.snapshot_history_index_name);
      assert_eq!(input["KeyConditionExpression"], "aid = :aid");
      assert_eq!(
        input["ExpressionAttributeValues"],
        json!({":aid": {"S": "Account-pages"}})
      );
      assert_eq!(input["ScanIndexForward"], false);
      assert_eq!(input["ConsistentRead"], false);
      assert!(input.get("Select").is_none());
      assert_eq!(trace["forwarded_input"]["Limit"], 2);
      if index > 0 {
        assert_eq!(
          input["ExclusiveStartKey"],
          response_body(queries[index - 1], "upstream_body")["LastEvaluatedKey"]
        );
      }
      for item in response_body(trace, "upstream_body")["Items"].as_array().unwrap() {
        assert_eq!(item.as_object().unwrap().len(), 3);
        assert!(item.get("aid").is_some() && item.get("skey").is_some() && item.get("active_history_seq_nr").is_some());
      }
    }
    let upstream = response_body(queries[0], "upstream_body");
    let delivered = response_body(queries[0], "delivered_body");
    assert!(upstream["Items"]
      .as_array()
      .unwrap()
      .iter()
      .any(|item| item["skey"]["N"] == "6"));
    assert!(delivered["Items"]
      .as_array()
      .unwrap()
      .iter()
      .all(|item| item["skey"]["N"] != "6"));
    assert_eq!(
      delivered["Items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|item| item["skey"]["N"] == "5")
        .count(),
      2
    );
    for trace in batch_traces(&result) {
      assert_eq!(trace["input"]["RequestItems"].as_object().unwrap().len(), 1);
      for write in trace["input"]["RequestItems"][&fixture.tables.snapshot_table_name]
        .as_array()
        .unwrap()
      {
        assert_eq!(write["DeleteRequest"]["Key"]["aid"], json!({"S": "Account-pages"}));
        assert!(delete_number(write) < 7 - keep as u64);
      }
    }
    assert_eq!(result["plan"]["fired"], result["plan"]["declared"]);
    assert_eq!(result["plan"]["applied"], result["plan"]["declared"]);
    assert_eq!(result["plan"]["unfired"], 0);
    assert_eq!(fixture.state("Account-pages-other").await, other);
    fixture.record(
      "retention-other-aid",
      &[],
      &json!({"before": other, "after": fixture.state("Account-pages-other").await}),
    );
    fixture.close().await;
  }
}

#[tokio::test]
async fn should_deduplicate_the_written_history_and_split_thirty_real_deletes_into_twenty_five_and_five() {
  let fixture = Fixture::new().await;
  seed(&fixture, "batches", 30).await;
  let (store, observed) = fixture
    .open_json_with_retention(RetentionSettings::keep_latest(1))
    .await;
  let result = perform_pair(
    &fixture,
    &store,
    &observed,
    AppendCase {
      value: "batches",
      number: 31,
      steps: vec![Step::Query {
        limit: None,
        omit: Vec::new(),
        duplicate: vec![31],
        continue_pages: false,
        fail_next_page: false,
      }],
    },
  )
  .await;
  assert_eq!(history_numbers(&result["after"]), vec![31]);
  let queries = result["traces"]
    .as_array()
    .unwrap()
    .iter()
    .filter(|trace| trace["api"] == "Query")
    .collect::<Vec<_>>();
  let delivered = response_body(queries[0], "delivered_body");
  assert_eq!(
    delivered["Items"]
      .as_array()
      .unwrap()
      .iter()
      .filter(|item| item["skey"]["N"] == "31")
      .count(),
    2
  );
  let batches = batch_traces(&result);
  assert_eq!(batches.len(), 2);
  let table = &fixture.tables.snapshot_table_name;
  assert_eq!(
    batch_numbers(batches[0], "input", table),
    (6..=30).rev().collect::<Vec<_>>()
  );
  assert_eq!(
    batch_numbers(batches[1], "input", table),
    (1..=5).rev().collect::<Vec<_>>()
  );
  for batch in batches {
    assert_eq!(batch["input"], batch["forwarded_input"]);
    assert_eq!(batch["upstream_status"], 200);
    let original = response_body(batch, "upstream_body");
    assert!(original["UnprocessedItems"]
      .as_object()
      .is_none_or(|tables| tables.values().all(|items| items.as_array().unwrap().is_empty())));
  }
  assert!(result["notifications"].as_array().unwrap().is_empty());
  fixture.close().await;
}

#[tokio::test]
async fn should_retry_all_then_partial_unprocessed_real_deletes_with_finite_capped_waits() {
  let fixture = Fixture::new().await;
  seed(&fixture, "retries", 6).await;
  let (store, observed) = fixture
    .open_json_with_options(DynamoDbOptions {
      retention: RetentionSettings::keep_latest(2),
      unprocessed_retry_limit: 3,
      unprocessed_retry_max_delay: Duration::from_millis(120),
      ..DynamoDbOptions::default()
    })
    .await;
  let result = perform_pair(
    &fixture,
    &store,
    &observed,
    AppendCase {
      value: "retries",
      number: 7,
      steps: vec![
        Step::Unprocessed(vec![5, 4, 3, 2, 1]),
        Step::Unprocessed(vec![3, 1]),
        Step::Unprocessed(vec![1]),
      ],
    },
  )
  .await;
  assert_eq!(history_numbers(&result["after"]), vec![6, 7]);
  assert_eq!(result["waits_ms"], json!([50, 100, 120]));
  assert!(result["notifications"].as_array().unwrap().is_empty());
  let batches = batch_traces(&result);
  assert_eq!(batches.len(), 4);
  let table = &fixture.tables.snapshot_table_name;
  for (trace, expected) in batches
    .iter()
    .zip([vec![5, 4, 3, 2, 1], vec![5, 4, 3, 2, 1], vec![3, 1], vec![1]])
  {
    assert_eq!(batch_numbers(trace, "input", table), expected);
  }
  for pair in batches.windows(2) {
    assert_eq!(
      pair[1]["input"]["RequestItems"],
      response_body(pair[0], "delivered_body")["UnprocessedItems"]
    );
  }
  assert!(batches[0]["forwarded_input"].is_null());
  assert_eq!(batch_numbers(batches[1], "forwarded_input", table), vec![5, 4, 2]);
  assert_eq!(batch_numbers(batches[2], "forwarded_input", table), vec![3]);
  assert_eq!(batch_numbers(batches[3], "forwarded_input", table), vec![1]);
  assert_eq!(result["plan"]["declared"], 4);
  assert_eq!(result["plan"]["fired"], 4);
  assert_eq!(result["plan"]["applied"], 4);
  assert_eq!(result["plan"]["unfired"], 0);
  fixture.close().await;
}

#[tokio::test]
async fn should_stop_at_zero_or_positive_retry_limits_and_recover_on_the_same_store_snapshot_append() {
  for limit in [0, 2] {
    let fixture = Fixture::new().await;
    seed(&fixture, "limit", 30).await;
    let (store, observed) = fixture
      .open_json_with_options(DynamoDbOptions {
        retention: RetentionSettings::keep_latest(1),
        unprocessed_retry_limit: limit,
        ..DynamoDbOptions::default()
      })
      .await;
    let mut steps = vec![Step::Unprocessed((6..=30).rev().collect())];
    steps.extend((0..limit).map(|_| Step::Unprocessed(vec![30])));
    steps.push(Step::MissingTable);
    let failed = perform_pair(
      &fixture,
      &store,
      &observed,
      AppendCase {
        value: "limit",
        number: 31,
        steps,
      },
    )
    .await;
    assert_warning(&failed, "Account-limit", 31, "retention-delete");
    assert!(failed["notifications"][0]["error"]
      .as_str()
      .unwrap()
      .contains("unprocessed retry limit"));
    let batches = batch_traces(&failed);
    assert_eq!(batches.len(), limit as usize + 1);
    assert_eq!(
      failed["waits_ms"],
      if limit == 0 { json!([]) } else { json!([50, 100]) }
    );
    assert_eq!(failed["plan"]["fired"], limit + 2);
    assert_eq!(failed["plan"]["applied"], limit + 2);
    assert_eq!(failed["plan"]["unfired"], 1);
    assert_eq!(failed["plan"]["remaining"][0]["missing_table"], true);
    assert_eq!(
      history_numbers(&failed["after"]),
      if limit == 0 {
        (1..=31).collect::<Vec<_>>()
      } else {
        vec![1, 2, 3, 4, 5, 30, 31]
      }
    );
    for requests in batches.windows(2) {
      assert_eq!(
        requests[1]["input"]["RequestItems"],
        response_body(requests[0], "delivered_body")["UnprocessedItems"]
      );
    }
    let event_only = perform_event_only(&fixture, &store, &observed, "limit", 32).await;
    assert_eq!(event_only["before"], failed["after"]);
    let recovered = perform_pair(
      &fixture,
      &store,
      &observed,
      AppendCase {
        value: "limit",
        number: 33,
        steps: Vec::new(),
      },
    )
    .await;
    assert_eq!(recovered["before"], event_only["after"]);
    assert_eq!(history_numbers(&recovered["after"]), vec![33]);
    assert_eq!(recovered["after"]["snapshot"][0]["seq_nr"], json!({"N": "33"}));
    assert_eq!(recovered["after"]["head"]["seq_nr"], json!({"N": "33"}));
    assert!(recovered["notifications"].as_array().unwrap().is_empty());
    assert!(recovered["waits_ms"].as_array().unwrap().is_empty());
    for number in 6..=29 {
      assert!(!history_numbers(&recovered["after"]).contains(&number));
      if limit > 0 {
        assert!(!history_numbers(&failed["after"]).contains(&number));
      }
    }
    fixture.close().await;
  }
}

fn assert_original_failure(observation: &Value) {
  let trace = observation["traces"]
    .as_array()
    .unwrap()
    .iter()
    .find(|trace| trace["upstream_status"] == 400)
    .unwrap();
  let original = response_body(trace, "upstream_body");
  let message = original
    .get("Message")
    .or_else(|| original.get("message"))
    .unwrap()
    .as_str()
    .unwrap();
  assert_eq!(response_body(trace, "delivered_body"), original);
  assert!(observation["notifications"][0]["error"]
    .as_str()
    .unwrap()
    .contains(message));
}

#[tokio::test]
async fn should_keep_a_snapshot_append_committed_on_a_later_query_page_failure_and_recover_on_the_same_store() {
  let fixture = Fixture::new().await;
  seed(&fixture, "query-failure", 3).await;
  let (store, observed) = fixture
    .open_json_with_retention(RetentionSettings::keep_latest(1))
    .await;
  let failed = perform_pair(
    &fixture,
    &store,
    &observed,
    AppendCase {
      value: "query-failure",
      number: 4,
      steps: vec![Step::Query {
        limit: Some(2),
        omit: Vec::new(),
        duplicate: Vec::new(),
        continue_pages: false,
        fail_next_page: true,
      }],
    },
  )
  .await;
  assert_warning(&failed, "Account-query-failure", 4, "retention-query");
  assert_original_failure(&failed);
  assert_eq!(failed["after"], failed["committed"]);
  assert_eq!(failed["after"]["head"]["seq_nr"], json!({"N": "4"}));
  assert!(batch_traces(&failed).is_empty());
  let queries = failed["traces"]
    .as_array()
    .unwrap()
    .iter()
    .filter(|trace| trace["api"] == "Query")
    .collect::<Vec<_>>();
  assert_eq!(queries.len(), 2);
  assert_eq!(
    queries[1]["input"]["ExclusiveStartKey"],
    response_body(queries[0], "upstream_body")["LastEvaluatedKey"]
  );
  assert_ne!(
    queries[1]["forwarded_input"]["IndexName"],
    queries[1]["input"]["IndexName"]
  );
  assert_eq!(failed["plan"]["declared"], 2);
  assert_eq!(failed["plan"]["applied"], 2);
  assert!(failed["waits_ms"].as_array().unwrap().is_empty());
  let event_only = perform_event_only(&fixture, &store, &observed, "query-failure", 5).await;
  assert_eq!(event_only["before"], failed["after"]);
  let recovered = perform_pair(
    &fixture,
    &store,
    &observed,
    AppendCase {
      value: "query-failure",
      number: 6,
      steps: Vec::new(),
    },
  )
  .await;
  assert_eq!(recovered["before"], event_only["after"]);
  assert_eq!(history_numbers(&recovered["after"]), vec![6]);
  assert!(recovered["notifications"].as_array().unwrap().is_empty());
  fixture.close().await;
}

#[tokio::test]
async fn should_preserve_successful_deletions_before_a_final_delete_failure_and_recover_on_the_same_store() {
  let fixture = Fixture::new().await;
  seed(&fixture, "delete-failure", 30).await;
  let (store, observed) = fixture
    .open_json_with_retention(RetentionSettings::keep_latest(1))
    .await;
  let failed = perform_pair(
    &fixture,
    &store,
    &observed,
    AppendCase {
      value: "delete-failure",
      number: 31,
      steps: vec![
        Step::Unprocessed(Vec::new()),
        Step::MissingTable,
        Step::Unprocessed(Vec::new()),
      ],
    },
  )
  .await;
  assert_warning(&failed, "Account-delete-failure", 31, "retention-delete");
  assert_original_failure(&failed);
  let batches = batch_traces(&failed);
  assert_eq!(batches.len(), 2);
  let table = &fixture.tables.snapshot_table_name;
  assert_eq!(
    batch_numbers(batches[0], "input", table),
    (6..=30).rev().collect::<Vec<_>>()
  );
  assert_eq!(
    batch_numbers(batches[1], "input", table),
    (1..=5).rev().collect::<Vec<_>>()
  );
  assert_eq!(batches[0]["upstream_status"], 200);
  assert_eq!(batches[1]["upstream_status"], 400);
  assert_eq!(history_numbers(&failed["after"]), vec![1, 2, 3, 4, 5, 31]);
  assert_eq!(failed["plan"]["unfired"], 1);
  assert!(failed["waits_ms"].as_array().unwrap().is_empty());
  let event_only = perform_event_only(&fixture, &store, &observed, "delete-failure", 32).await;
  assert_eq!(event_only["before"], failed["after"]);
  let recovered = perform_pair(
    &fixture,
    &store,
    &observed,
    AppendCase {
      value: "delete-failure",
      number: 33,
      steps: Vec::new(),
    },
  )
  .await;
  assert_eq!(recovered["before"], event_only["after"]);
  assert_eq!(history_numbers(&recovered["after"]), vec![33]);
  assert!(recovered["notifications"].as_array().unwrap().is_empty());
  let batches = batch_traces(&recovered);
  assert_eq!(batches.len(), 1);
  assert_eq!(batch_numbers(batches[0], "input", table), vec![31, 5, 4, 3, 2, 1]);
  for number in 6..=30 {
    assert!(!history_numbers(&failed["after"]).contains(&number));
    assert!(!history_numbers(&event_only["after"]).contains(&number));
    assert!(!history_numbers(&recovered["after"]).contains(&number));
  }
  fixture.close().await;
}

#[test]
fn should_merge_injected_unprocessed_items_without_erasing_upstream_items() {
  let original = json!({"UnprocessedItems": {"second": [
    {"DeleteRequest": {"Key": {"aid": {"S": "Account-merge"}, "skey": {"N": "2"}}}}
  ]}});
  let mut delivered = original.clone();
  let duplicate = original["UnprocessedItems"]["second"][0].clone();
  let extra = json!({"DeleteRequest": {"Key": {"aid": {"S": "Account-merge"}, "skey": {"N": "1"}}}});
  merge_unprocessed(&mut delivered, "second", vec![duplicate.clone(), extra.clone()]);
  assert_eq!(delivered["UnprocessedItems"]["second"], json!([duplicate, extra]));
  let same = delivered.clone();
  merge_unprocessed(&mut delivered, "second", Vec::new());
  assert_eq!(delivered, same);
}

#[tokio::test]
async fn should_preserve_the_actual_upstream_unprocessed_response_when_another_plan_adds_unprocessed_items() {
  let fixture = Fixture::new().await;
  seed(&fixture, "upstream", 4).await;
  let inner_traces: Traces = Arc::new(Mutex::new(Vec::new()));
  let inner_plan = Arc::new(Mutex::new(Plan::default()));
  inner_plan.lock().unwrap().declare(Step::Unprocessed(vec![3]));
  let records = inner_traces.clone();
  let control = inner_plan.clone();
  let upstream = aws_smithy_http_client::Builder::new().build_http();
  let http = http_client_fn(move |settings, components| {
    SharedHttpConnector::new(ObservedConnector {
      upstream: upstream.http_connector(settings, components),
      traces: records.clone(),
      retention_plan: control.clone(),
      injection: Arc::new(Mutex::new(None)),
      transmit_scratch: Arc::new(Mutex::new(None)),
    })
  });
  let observed = Observed::with_upstream(&fixture.endpoint, http);
  let store = JsonStore::open(
    observed.client.clone(),
    fixture.tables.clone(),
    DynamoDbOptions {
      retention: RetentionSettings::keep_latest(1),
      ..Default::default()
    },
  )
  .await
  .unwrap();
  fixture.record("retention-upstream-open", &observed.take(), &json!(null));
  inner_traces.lock().unwrap().clear();
  let result = perform_pair(
    &fixture,
    &store,
    &observed,
    AppendCase {
      value: "upstream",
      number: 5,
      steps: vec![Step::Unprocessed(vec![4])],
    },
  )
  .await;
  assert_eq!(history_numbers(&result["after"]), vec![5]);
  let batches = batch_traces(&result);
  assert_eq!(batches.len(), 2);
  let table = &fixture.tables.snapshot_table_name;
  assert_eq!(batch_numbers(batches[0], "input", table), vec![4, 3, 2, 1]);
  assert_eq!(batch_numbers(batches[0], "forwarded_input", table), vec![3, 2, 1]);
  let original = response_body(batches[0], "upstream_body");
  let delivered = response_body(batches[0], "delivered_body");
  assert_eq!(
    original["UnprocessedItems"][table]
      .as_array()
      .unwrap()
      .iter()
      .map(delete_number)
      .collect::<Vec<_>>(),
    vec![3]
  );
  assert_eq!(
    delivered["UnprocessedItems"][table]
      .as_array()
      .unwrap()
      .iter()
      .map(delete_number)
      .collect::<Vec<_>>(),
    vec![3, 4]
  );
  assert_eq!(batch_numbers(batches[1], "input", table), vec![3, 4]);
  let inner = inner_traces.lock().unwrap().clone();
  let inner_batches = inner
    .iter()
    .filter(|trace| trace["api"] == "BatchWriteItem")
    .collect::<Vec<_>>();
  assert_eq!(batch_numbers(inner_batches[0], "forwarded_input", table), vec![2, 1]);
  assert_eq!(response_body(inner_batches[0], "delivered_body"), original);
  assert_eq!(inner_plan.lock().unwrap().report()["applied"], 1);
  fixture.record(
    "retention-upstream-response",
    &inner,
    &json!({"outer": result, "upstream_plan": inner_plan.lock().unwrap().report()}),
  );
  fixture.close().await;
}

#[tokio::test]
async fn should_distinguish_unfired_and_undelivered_plans_from_applied_plans() {
  let fixture = Fixture::new().await;
  seed(&fixture, "delivery", 2).await;
  let observed = Observed::new(&fixture.endpoint);
  observed.install_retention(vec![query_step()]);
  let future = observed
    .client
    .query()
    .table_name(&fixture.tables.snapshot_table_name)
    .index_name(&fixture.tables.snapshot_history_index_name)
    .key_condition_expression("aid = :aid")
    .expression_attribute_values(":aid", AttributeValue::S("Account-delivery".into()))
    .send();
  drop(future);
  assert!(observed.take().is_empty());
  let unfired = observed.retention_report();
  assert_eq!(unfired["declared"], 1);
  assert_eq!(unfired["fired"], 0);
  assert_eq!(unfired["applied"], 0);
  assert_eq!(unfired["unfired"], 1);
  let entered = Arc::new(Notify::new());
  let release = Arc::new(Notify::new());
  observed.install_retention(vec![Step::Pause {
    step: Box::new(Step::Unprocessed(vec![1])),
    entered: entered.clone(),
    release,
  }]);
  let writes = [2, 1].map(|number| {
    aws_sdk_dynamodb::types::WriteRequest::builder()
      .delete_request(
        aws_sdk_dynamodb::types::DeleteRequest::builder()
          .set_key(Some(key("Account-delivery", Some(("skey", number)))))
          .build()
          .unwrap(),
      )
      .build()
  });
  {
    let request = observed
      .client
      .batch_write_item()
      .set_request_items(Some(HashMap::from([(
        fixture.tables.snapshot_table_name.clone(),
        writes.to_vec(),
      )])))
      .send();
    tokio::pin!(request);
    tokio::select! {
      result = &mut request => panic!("pause must prevent delivery: {result:?}"),
      _ = entered.notified() => {},
    }
  }
  let undelivered = observed.retention_report();
  assert_eq!(undelivered["declared"], 1);
  assert_eq!(undelivered["fired"], 1);
  assert_eq!(undelivered["applied"], 0);
  assert_eq!(undelivered["unfired"], 0);
  let traces = observed.take();
  assert_eq!(traces.len(), 1);
  assert_eq!(traces[0]["upstream_status"], 200);
  assert!(traces[0]["delivered_body"].is_null());
  let state = fixture.state("Account-delivery").await;
  assert_eq!(history_numbers(&state), vec![1]);
  fixture.record(
    "retention-unfired-undelivered",
    &traces,
    &json!({"unfired": unfired, "undelivered": undelivered, "state": state}),
  );
  fixture.close().await;
}

#[tokio::test]
async fn should_not_start_retention_after_either_append_is_canceled_or_fails_to_transmit() {
  let fixture = Fixture::new().await;
  let (store, observed) = fixture
    .open_json_with_retention(RetentionSettings::keep_latest(1))
    .await;
  append(&store, "append-failure", 1, true).await.unwrap();
  observed.take();
  let before = fixture.state("Account-append-failure").await;
  for pair in [true, false] {
    for number in [1, 3, 2] {
      observed.install_retention(vec![Step::MissingIndex, Step::MissingTable]);
      if number == 2 {
        *observed.injection.lock().unwrap() = Some(Injection::Communication);
      }
      let notices = Arc::new(Mutex::new(Vec::new()));
      let result = append(&store, "append-failure", number, pair)
        .with_subscriber(tracing_subscriber::registry().with(Capture(notices.clone())))
        .await;
      match number {
        1 => assert!(matches!(&result, Err(EventStoreError::OptimisticLock { .. }))),
        3 => assert!(matches!(
          &result,
          Err(EventStoreError::ContractViolation {
            rule: ContractRule::W8Gap,
            ..
          })
        )),
        2 => assert!(matches!(
          &result,
          Err(EventStoreError::Storage {
            operation: StorageOperation::Append,
            ..
          })
        )),
        _ => unreachable!(),
      }
      let traces = observed.take();
      assert_eq!(traces.len(), 1);
      if pair {
        pair_actions(&fixture, &traces[0], true);
      } else {
        actions(&fixture, &traces[0]);
      }
      assert_eq!(observed.retention_report()["fired"], 0);
      assert_eq!(observed.retention_report()["applied"], 0);
      assert_eq!(observed.retention_report()["unfired"], 2);
      assert!(notices.lock().unwrap().is_empty());
      let after = fixture.state("Account-append-failure").await;
      assert_eq!(after, before);
      fixture.record(&format!("retention-append-failure-{pair}-{number}"), &traces,
        &json!({"before": before, "after": after, "plan": observed.retention_report(), "result": result_json(&result), "notifications": *notices.lock().unwrap()}));
    }
  }
  fixture.close().await;
}

#[tokio::test]
async fn should_not_send_retention_or_append_requests_after_serializer_or_size_validation_failures() {
  let fixture = Fixture::new().await;
  let observed = Observed::new(&fixture.endpoint);
  let event_serializer = Arc::new(BytesSerializer::default());
  let snapshot_serializer = Arc::new(BytesSerializer::default());
  let store = OpaqueStore::open_with_serializers(
    observed.client.clone(),
    fixture.tables.clone(),
    DynamoDbOptions {
      retention: RetentionSettings::keep_latest(1),
      ..Default::default()
    },
    event_serializer.clone(),
    snapshot_serializer.clone(),
  )
  .await
  .unwrap();
  fixture.record("retention-validation-open", &observed.take(), &json!(null));
  store
    .persist_event_and_snapshot(
      event(Id::new("Account", "validation"), 1, Opaque(vec![1])),
      SnapshotEnvelope::new(Opaque(vec![11]), 1),
    )
    .await
    .unwrap();
  observed.take();
  let before = fixture.state("Account-validation").await;
  for pair in [true, false] {
    let mut cases = vec![
      ("event-serialization", true, false, 1, 1),
      ("event-size", false, false, 409600, 1),
    ];
    if pair {
      cases.extend([
        ("snapshot-serialization", false, true, 1, 1),
        ("snapshot-size", false, false, 1, 409600),
      ]);
    }
    for (name, event_fail, snapshot_fail, event_len, snapshot_len) in cases {
      event_serializer.fail.store(event_fail, Ordering::SeqCst);
      snapshot_serializer.fail.store(snapshot_fail, Ordering::SeqCst);
      observed.install_retention(vec![Step::MissingIndex, Step::MissingTable]);
      let notices = Arc::new(Mutex::new(Vec::new()));
      let result = async {
        let event = event(Id::new("Account", "validation"), 2, Opaque(vec![2; event_len]));
        if pair {
          store
            .persist_event_and_snapshot(event, SnapshotEnvelope::new(Opaque(vec![22; snapshot_len]), 2))
            .await
        } else {
          store.persist_event(event).await
        }
      }
      .with_subscriber(tracing_subscriber::registry().with(Capture(notices.clone())))
      .await;
      if event_fail || snapshot_fail {
        assert!(
          matches!(&result, Err(EventStoreError::Serialization { phase, .. }) if *phase == if event_fail { SerializationPhase::SerializeEvent } else { SerializationPhase::SerializeSnapshot })
        );
        assert_eq!(
          result.as_ref().unwrap_err().source().unwrap().to_string(),
          "SERIALIZER_CAUSE"
        );
      } else {
        assert!(matches!(
          &result,
          Err(EventStoreError::ContractViolation {
            rule: ContractRule::ItemSizeLimit,
            ..
          })
        ));
      }
      let traces = observed.take();
      assert!(traces.is_empty());
      assert_eq!(observed.retention_report()["fired"], 0);
      assert_eq!(observed.retention_report()["applied"], 0);
      assert_eq!(observed.retention_report()["unfired"], 2);
      assert!(notices.lock().unwrap().is_empty());
      let after = fixture.state("Account-validation").await;
      assert_eq!(after, before);
      fixture.record(&format!("retention-validation-{pair}-{name}"), &traces,
        &json!({"before": before, "after": after, "plan": observed.retention_report(), "result": result_json(&result), "notifications": *notices.lock().unwrap()}));
    }
  }
  fixture.close().await;
}
