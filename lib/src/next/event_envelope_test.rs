use chrono::{DateTime, TimeZone, Utc};

use crate::next::event_envelope::{EventEnvelope, SnapshotEnvelope, SnapshotRead};

#[derive(Debug, Clone, PartialEq)]
struct TestPayload {
  name: String,
}

fn fixed_occurred_at() -> DateTime<Utc> {
  Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap()
}

// T-2 / T-4: manifest の省略時は空文字列になる。
#[test]
fn should_event_envelope_default_manifest_to_empty_string() {
  let envelope = EventEnvelope::new(
    "Order-1".to_string(),
    1,
    fixed_occurred_at(),
    TestPayload {
      name: "created".to_string(),
    },
  );

  assert_eq!(envelope.manifest(), "");
  assert_eq!(envelope.seq_nr(), 1);
  assert_eq!(envelope.payload().name, "created");
  assert_eq!(envelope.aggregate_id(), "Order-1");
}

// T-4: `with_manifest` は値を解釈せず、そのまま運搬する。
#[test]
fn should_event_envelope_carry_manifest_verbatim() {
  let envelope = EventEnvelope::new(
    "Order-1".to_string(),
    2,
    fixed_occurred_at(),
    TestPayload {
      name: "renamed".to_string(),
    },
  )
  .with_manifest("com.example.OrderEvent.Renamed#v2");

  assert_eq!(envelope.manifest(), "com.example.OrderEvent.Renamed#v2");
}

// T-2: 封筒を消費して payload の所有権を返す。
#[test]
fn should_event_envelope_into_payload_moves_ownership() {
  let envelope = EventEnvelope::new(
    "Order-1".to_string(),
    1,
    fixed_occurred_at(),
    TestPayload {
      name: "created".to_string(),
    },
  );

  let payload = envelope.into_payload();

  assert_eq!(payload.name, "created");
}

// T-10: スナップショット封筒は `new(aggregate, seq_nr)` で作り、version を持たない。
#[test]
fn should_snapshot_envelope_carry_seq_nr_without_version() {
  let snapshot = SnapshotEnvelope::new(
    TestPayload {
      name: "current".to_string(),
    },
    7,
  );

  assert_eq!(snapshot.seq_nr(), 7);
  assert_eq!(snapshot.aggregate().name, "current");
  assert_eq!(snapshot.manifest(), "");
  assert_eq!(snapshot.into_aggregate().name, "current");
}

// T-10: スナップショット封筒の manifest は `with_manifest` で設定する。
#[test]
fn should_snapshot_envelope_carry_manifest_verbatim() {
  let snapshot = SnapshotEnvelope::new(
    TestPayload {
      name: "current".to_string(),
    },
    3,
  )
  .with_manifest("snapshot-v1");

  assert_eq!(snapshot.manifest(), "snapshot-v1");
}

// R-2 / R-3: `SnapshotRead` はスナップショット封筒（なくてもよい）とヘッドの seq_nr を返す。
#[test]
fn should_snapshot_read_return_snapshot_and_head_seq_nr() {
  let snapshot = SnapshotEnvelope::new(
    TestPayload {
      name: "current".to_string(),
    },
    4,
  );
  let read = SnapshotRead::new(Some(snapshot), 5);

  assert_eq!(read.head_seq_nr(), 5);
  assert_eq!(read.snapshot().expect("スナップショットがある").seq_nr(), 4);
  let (snapshot, head_seq_nr) = read.into_parts();
  assert_eq!(head_seq_nr, 5);
  assert_eq!(snapshot.expect("スナップショットがある").seq_nr(), 4);

  // スナップショットがなくても、ヘッドの seq_nr を返す。
  let read: SnapshotRead<TestPayload> = SnapshotRead::new(None, 9);
  assert!(read.snapshot().is_none());
  assert_eq!(read.head_seq_nr(), 9);
}

// T-6: 封筒は payload に serde（Serialize）や Clone を要求しない。
// どちらも実装しない `PlainPayload` を載せた封筒が構築・読取できることのコンパイル検証。
#[test]
fn should_event_envelope_not_require_serialize_or_clone_on_payload() {
  #[derive(Debug, PartialEq)]
  struct PlainPayload {
    name: String,
  }

  #[derive(Debug, PartialEq)]
  struct PlainAggregate {
    name: String,
  }

  let envelope = EventEnvelope::new(
    "Order-1".to_string(),
    1,
    fixed_occurred_at(),
    PlainPayload {
      name: "created".to_string(),
    },
  );
  assert_eq!(envelope.payload().name, "created");

  let snapshot = SnapshotEnvelope::new(
    PlainAggregate {
      name: "current".to_string(),
    },
    1,
  );
  assert_eq!(snapshot.aggregate().name, "current");
}
