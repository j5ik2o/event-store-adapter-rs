use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};

use crate::aggregate_id::{AggregateId, AidString};
use crate::error::{ContractRule, EventStoreError};
use crate::event_envelope::{EventEnvelope, SnapshotEnvelope, SnapshotRead};
use crate::event_store::EventStore;
use crate::generic_event_store::{check_event, check_event_and_snapshot, check_read_seq_nr, GenericEventStore};
use crate::seq_nr::{SeqNr, SEQ_NR_MAX};
use crate::storage_backend::{AppendReceipt, AppendRequest, StorageBackend};

#[derive(Debug, Clone, PartialEq)]
struct TestAggregateId {
  type_name: String,
  value: String,
}

impl TestAggregateId {
  fn new(type_name: &str, value: &str) -> Self {
    Self {
      type_name: type_name.to_string(),
      value: value.to_string(),
    }
  }
}

impl AggregateId for TestAggregateId {
  fn type_name(&self) -> String {
    self.type_name.clone()
  }

  fn value(&self) -> String {
    self.value.clone()
  }
}

#[derive(Debug, Clone, PartialEq)]
struct TestPayload {
  name: String,
}

#[derive(Debug, Clone, PartialEq)]
struct TestAggregate {
  count: u32,
}

fn occurred_at() -> DateTime<Utc> {
  Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap()
}

fn event(seq_nr: SeqNr) -> EventEnvelope<TestAggregateId, TestPayload> {
  EventEnvelope::new(
    TestAggregateId::new("Order", "1"),
    seq_nr,
    occurred_at(),
    TestPayload {
      name: "created".to_string(),
    },
  )
}

fn expect_contract_violation(error: EventStoreError, rule: ContractRule) -> EventStoreError {
  match error {
    EventStoreError::ContractViolation { rule: found, .. } if found == rule => error,
    other => panic!("expected ContractViolation({rule:?}), got {other:?}"),
  }
}

// T-9: 読み取りの seq_nr は 0 と上限を受理する（0 は全件を返す）。
#[test]
fn should_check_read_seq_nr_accept_zero_and_max() {
  check_read_seq_nr(0).expect("0 は有効");
  check_read_seq_nr(SEQ_NR_MAX).expect("上限は有効");
}

// T-9: 上限を超える値は契約違反。
#[test]
fn should_check_read_seq_nr_reject_above_max_with_t_9() {
  let error = expect_contract_violation(
    check_read_seq_nr(SEQ_NR_MAX + 1).expect_err("上限超は拒む"),
    ContractRule::T9,
  );

  assert!(error.to_string().contains("T-9"));
  assert!(error.to_string().contains(&(SEQ_NR_MAX + 1).to_string()));
}

// W-6: イベントの seq_nr 0 は契約違反。読み取りの 0 とは区別する。
#[test]
fn should_check_event_seq_nr_reject_zero_with_w_6() {
  let error = expect_contract_violation(
    check_event(&event(0)).expect_err("イベントの 0 は拒む"),
    ContractRule::W6,
  );

  assert!(error.to_string().contains("W-6"));
  assert!(error.to_string().contains("seq_nr=0"));
}

// T-9: イベントの seq_nr が上限を超えれば契約違反。
#[test]
fn should_check_event_seq_nr_reject_above_max_with_t_9() {
  expect_contract_violation(
    check_event(&event(SEQ_NR_MAX + 1)).expect_err("上限超は拒む"),
    ContractRule::T9,
  );
}

// T-13: `occurred_at` がナノ秒の範囲外なら契約違反。
#[test]
fn should_check_event_reject_occurred_at_out_of_range_with_t_13() {
  let out_of_range = Utc.with_ymd_and_hms(10000, 1, 1, 0, 0, 0).unwrap();
  assert!(out_of_range.timestamp_nanos_opt().is_none(), "試験の前提: 範囲外");
  let envelope = EventEnvelope::new(
    TestAggregateId::new("Order", "1"),
    1,
    out_of_range,
    TestPayload {
      name: "created".to_string(),
    },
  );

  let error = expect_contract_violation(check_event(&envelope).expect_err("範囲外は拒む"), ContractRule::T13);

  assert!(error.to_string().contains("T-13"));
}

// T-11: 入口の aid 組み立ては型名のハイフンを拒む。検査順の先頭で行う。
#[test]
fn should_check_event_reject_hyphen_in_type_name_before_other_checks() {
  let envelope = EventEnvelope::new(
    TestAggregateId::new("order-item", "1"),
    0,
    occurred_at(),
    TestPayload {
      name: "created".to_string(),
    },
  );

  // seq_nr 0（W-6）と型名のハイフン（T-11）が同時に成り立つ。aid の組み立てが先なので T-11 になる。
  let error = expect_contract_violation(check_event(&envelope).expect_err("T-11 が先"), ContractRule::T11);

  assert!(error.to_string().contains("T-11"));
}

// T-11: 空の型名は入口検査で許す。
#[test]
fn should_check_event_allow_empty_type_name() {
  let envelope = EventEnvelope::new(
    TestAggregateId::new("", "1"),
    1,
    occurred_at(),
    TestPayload {
      name: "created".to_string(),
    },
  );

  let aid = check_event(&envelope).expect("空の型名は許す");

  assert_eq!(aid.as_str(), "-1");
}

// W-9: スナップショットの seq_nr が一致すれば通す。
#[test]
fn should_check_event_and_snapshot_accept_matching_seq_nr() {
  let snapshot = SnapshotEnvelope::new(TestAggregate { count: 1 }, 4);

  let aid = check_event_and_snapshot(&event(4), &snapshot).expect("一致すれば通す");

  assert_eq!(aid.as_str(), "Order-1");
}

// W-9: スナップショットの seq_nr が一致しなければ契約違反。表示に規則と両番号を含む。
#[test]
fn should_check_event_and_snapshot_reject_mismatch_with_w_9() {
  let snapshot = SnapshotEnvelope::new(TestAggregate { count: 1 }, 3);

  let error = expect_contract_violation(
    check_event_and_snapshot(&event(4), &snapshot).expect_err("不一致は拒む"),
    ContractRule::W9,
  );

  let text = error.to_string();
  assert!(text.contains("W-9"));
  assert!(text.contains("seq_nr=4"));
  assert!(text.contains("snapshot_seq_nr=3"));
}

// ---------------------------------------------------------------------------
// GenericEventStore から StorageBackend への委譲（PR 7 では試験が唯一の観測点）
// ---------------------------------------------------------------------------

#[derive(Default)]
struct FakeBackendState {
  appended: Vec<(String, SeqNr, Option<SeqNr>)>,
  snapshot: Option<SnapshotRead<TestAggregate>>,
  events: Vec<EventEnvelope<TestAggregateId, TestPayload>>,
}

#[derive(Clone, Default)]
struct FakeBackend {
  state: Arc<Mutex<FakeBackendState>>,
}

impl std::fmt::Debug for FakeBackend {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    formatter.debug_struct("FakeBackend").finish()
  }
}

#[async_trait]
impl StorageBackend<TestAggregateId, TestAggregate, TestPayload> for FakeBackend {
  async fn append(
    &self,
    request: AppendRequest<'_, TestAggregateId, TestAggregate, TestPayload>,
  ) -> Result<AppendReceipt, EventStoreError> {
    let mut state = self.state.lock().unwrap();
    state.appended.push((
      request.aid.as_str().to_string(),
      request.event.seq_nr(),
      request.snapshot.map(SnapshotEnvelope::seq_nr),
    ));
    Ok(AppendReceipt {
      retention_failure: None,
    })
  }

  async fn load_snapshot(&self, _aid: &AidString) -> Result<Option<SnapshotRead<TestAggregate>>, EventStoreError> {
    Ok(self.state.lock().unwrap().snapshot.clone())
  }

  async fn load_events(
    &self,
    _aggregate_id: &TestAggregateId,
    _aid: &AidString,
    _seq_nr: SeqNr,
  ) -> Result<Vec<EventEnvelope<TestAggregateId, TestPayload>>, EventStoreError> {
    Ok(self.state.lock().unwrap().events.clone())
  }
}

// 入口検査を通った追記が、検査済みの aid と seq_nr で保存先へ届く。
#[tokio::test]
async fn should_persist_event_and_snapshot_delegate_checked_values_to_backend() {
  let backend = FakeBackend::default();
  let store = GenericEventStore::<TestAggregateId, TestAggregate, TestPayload, _>::new(backend.clone());
  let snapshot = SnapshotEnvelope::new(TestAggregate { count: 1 }, 4);

  store
    .persist_event_and_snapshot(event(4), snapshot)
    .await
    .expect("追記できる");

  let state = backend.state.lock().unwrap();
  assert_eq!(state.appended, vec![("Order-1".to_string(), 4, Some(4))]);
}

// 契約違反の入力は保存先へ達しない（入口で拒む）。
#[tokio::test]
async fn should_persist_event_reject_before_reaching_backend() {
  let backend = FakeBackend::default();
  let store = GenericEventStore::<TestAggregateId, TestAggregate, TestPayload, _>::new(backend.clone());

  let result = store.persist_event(event(0)).await;

  expect_contract_violation(result.expect_err("W-6 で拒む"), ContractRule::W6);
  assert!(backend.state.lock().unwrap().appended.is_empty(), "保存先へ達しない");
}

// T-9: 読み取りの seq_nr 上限超は保存先へ達しない。
#[tokio::test]
async fn should_get_events_reject_read_seq_nr_above_max_before_backend() {
  let backend = FakeBackend::default();
  let store = GenericEventStore::<TestAggregateId, TestAggregate, TestPayload, _>::new(backend.clone());

  let result = store
    .get_events_by_id_since_seq_nr(&TestAggregateId::new("Order", "1"), SEQ_NR_MAX + 1)
    .await;

  expect_contract_violation(result.expect_err("T-9 で拒む"), ContractRule::T9);
}

// R-1: ヘッドがなければ `None` を返す（エラーにしない）。
#[tokio::test]
async fn should_get_latest_snapshot_return_none_when_absent() {
  let backend = FakeBackend::default();
  let store = GenericEventStore::<TestAggregateId, TestAggregate, TestPayload, _>::new(backend);

  let result = store
    .get_latest_snapshot_by_id(&TestAggregateId::new("Order", "1"))
    .await
    .expect("エラーにしない");

  assert!(result.is_none());
}
