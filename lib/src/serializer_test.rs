use serde::{Deserialize, Serialize};

use crate::error::{EventStoreError, SerializationPhase};
use crate::serializer::{EventSerializer, JsonEventSerializer, JsonSnapshotSerializer, SnapshotSerializer};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct TestPayload {
  name: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct TestAggregate {
  count: u32,
}

// T-8: JSON 版イベントシリアライザは payload を往復できる。
#[test]
fn should_json_event_serializer_round_trip_payload() {
  let serializer = JsonEventSerializer::<TestPayload>::new();
  let payload = TestPayload {
    name: "created".to_string(),
  };

  let bytes = serializer.serialize(&payload).expect("直列化できる");
  let restored = serializer.deserialize(&bytes).expect("復元できる");

  assert_eq!(restored, payload);
}

// T-8: JSON 版スナップショットシリアライザは集約状態を往復できる。
#[test]
fn should_json_snapshot_serializer_round_trip_aggregate() {
  let serializer = JsonSnapshotSerializer::<TestAggregate>::new();
  let aggregate = TestAggregate { count: 3 };

  let bytes = serializer.serialize(&aggregate).expect("直列化できる");
  let restored = serializer.deserialize(&bytes).expect("復元できる");

  assert_eq!(restored, aggregate);
}

// T-6: `EventSerializer` の境界は `Send + Sync + 'static` だけで、payload に `Debug` を要求しない。
// `Debug`・`Clone`・`PartialEq` を実装しない serde 型でも往復できることのコンパイル検証。
#[test]
fn should_event_serializer_not_require_debug_or_clone_on_payload() {
  #[derive(Serialize, Deserialize)]
  struct PlainPayload {
    value: String,
  }

  let serializer = JsonEventSerializer::<PlainPayload>::new();
  let payload = PlainPayload {
    value: "plain".to_string(),
  };

  let bytes = serializer.serialize(&payload).expect("直列化できる");
  let restored = serializer.deserialize(&bytes).expect("復元できる");

  assert_eq!(restored.value, "plain");
}

// E-1: 復元できない入力は Serialization（`deserialize-event`）になる。
#[test]
fn should_json_event_serializer_report_deserialization_failure_as_serialization() {
  let serializer = JsonEventSerializer::<TestPayload>::new();

  let error = serializer.deserialize(b"not json").expect_err("復元に失敗する");

  assert!(matches!(
    error,
    EventStoreError::Serialization {
      phase: SerializationPhase::DeserializeEvent,
      ..
    }
  ));
}
