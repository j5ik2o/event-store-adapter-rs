use chrono::{DateTime, TimeZone, Utc};

use crate::aggregate_id::{AggregateId, AidString};
use crate::error::RetentionFailure;
use crate::event_envelope::{EventEnvelope, SnapshotEnvelope};
use crate::storage_backend::{AppendReceipt, AppendRequest};

#[derive(Debug, Clone)]
struct TestAggregateId;

impl AggregateId for TestAggregateId {
  fn type_name(&self) -> String {
    "Order".to_string()
  }

  fn value(&self) -> String {
    "1".to_string()
  }
}

fn occurred_at() -> DateTime<Utc> {
  Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap()
}

// S-4: 追記の入力は、検査済みの aid・元の集団 ID を持つ封筒・任意のスナップショットを運ぶ。
#[test]
fn should_append_request_carry_aid_event_and_optional_snapshot() {
  let aid = AidString::from_aggregate_id(&TestAggregateId).expect("aid を作れる");
  let event = EventEnvelope::new(TestAggregateId, 1, occurred_at(), "payload".to_string());
  let snapshot = SnapshotEnvelope::new("aggregate".to_string(), 1);

  let request = AppendRequest {
    aid: &aid,
    event: &event,
    snapshot: Some(&snapshot),
  };

  assert_eq!(request.aid.as_str(), "Order-1");
  assert_eq!(request.event.seq_nr(), 1);
  assert_eq!(request.snapshot.map(SnapshotEnvelope::seq_nr), Some(1));
}

// S-4: 追記の結果は、保持処理の失敗を別の経路で知らせるために `retention_failure` を持つ。
#[test]
fn should_append_receipt_carry_retention_failure() {
  let failure = RetentionFailure {
    aid: "Order-1".to_string(),
    seq_nr: 3,
    phase: "retention-delete".to_string(),
    error: "boom".to_string(),
  };

  let receipt = AppendReceipt {
    retention_failure: Some(failure.clone()),
  };

  assert_eq!(receipt.retention_failure, Some(failure));

  let receipt = AppendReceipt {
    retention_failure: None,
  };
  assert!(receipt.retention_failure.is_none());
}
