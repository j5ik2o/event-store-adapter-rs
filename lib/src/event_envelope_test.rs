use chrono::{DateTime, TimeZone, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::event_envelope::{EventEnvelope, SnapshotEnvelope};
use crate::types::AggregateId;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TestId(String);

impl Display for TestId {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.0)
  }
}

impl AggregateId for TestId {
  fn type_name(&self) -> String {
    "TestAggregate".to_string()
  }

  fn value(&self) -> String {
    self.0.clone()
  }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TestPayload {
  name: String,
}

fn fixed_occurred_at() -> DateTime<Utc> {
  Utc.with_ymd_and_hms(2026, 8, 27, 12, 34, 56).unwrap()
}

// AC1.1.2 / US1.1: 封筒構築とアクセサの読取（メタデータ 4 点 + payload の運搬）
#[test]
fn test_event_envelope_construction_and_accessors() {
  let occurred_at = fixed_occurred_at();
  let envelope = EventEnvelope::new(
    TestId("a-1".to_string()),
    1,
    occurred_at,
    TestPayload {
      name: "created".to_string(),
    },
  );

  assert_eq!(envelope.aggregate_id(), &TestId("a-1".to_string()));
  assert_eq!(envelope.seq_nr(), 1);
  // BR1.3: occurred_at はドメイン供給値のまま維持される
  assert_eq!(envelope.occurred_at(), &occurred_at);
  assert_eq!(envelope.payload().name, "created");
}

// BR1.1 / FR1.2: manifest 省略時は空文字列
#[test]
fn test_event_envelope_manifest_defaults_to_empty_string() {
  let envelope = EventEnvelope::new(
    TestId("a-1".to_string()),
    1,
    fixed_occurred_at(),
    TestPayload {
      name: "created".to_string(),
    },
  );
  assert_eq!(envelope.manifest(), "");
}

// BR1.2: with_manifest で設定した値が同値で読み取れる（ライブラリは値を解釈しない）
#[test]
fn test_event_envelope_with_manifest_carries_value_verbatim() {
  let envelope = EventEnvelope::new(
    TestId("a-1".to_string()),
    2,
    fixed_occurred_at(),
    TestPayload {
      name: "renamed".to_string(),
    },
  )
  .with_manifest("com.example.UserAccountEvent.Renamed#v2");
  assert_eq!(envelope.manifest(), "com.example.UserAccountEvent.Renamed#v2");
}

// FR7.4 / AC1.2.1: manifest ラウンドトリップ — serde 往復で封筒全体が同値
#[test]
fn test_event_envelope_manifest_serde_round_trip() {
  let envelope = EventEnvelope::new(
    TestId("a-1".to_string()),
    3,
    fixed_occurred_at(),
    TestPayload {
      name: "renamed".to_string(),
    },
  )
  .with_manifest("manifest-v1");

  let json = serde_json::to_string(&envelope).unwrap();
  let restored: EventEnvelope<TestId, TestPayload> = serde_json::from_str(&json).unwrap();
  assert_eq!(restored, envelope);
  assert_eq!(restored.manifest(), "manifest-v1");
}

// FR7.4 / FR1.2: 省略時の空文字列 manifest も同値でラウンドトリップする
#[test]
fn test_event_envelope_default_manifest_serde_round_trip() {
  let envelope = EventEnvelope::new(
    TestId("a-1".to_string()),
    1,
    fixed_occurred_at(),
    TestPayload {
      name: "created".to_string(),
    },
  );

  let json = serde_json::to_string(&envelope).unwrap();
  let restored: EventEnvelope<TestId, TestPayload> = serde_json::from_str(&json).unwrap();
  assert_eq!(restored, envelope);
  assert_eq!(restored.manifest(), "");
}

// FR2.1: SnapshotEnvelope の構築とアクセサ（公開昇格した封筒の形状）
#[test]
fn test_snapshot_envelope_construction_and_accessors() {
  let snapshot = SnapshotEnvelope::new(
    TestPayload {
      name: "current".to_string(),
    },
    2,
    5,
  );
  assert_eq!(snapshot.aggregate().name, "current");
  assert_eq!(snapshot.seq_nr(), 2);
  assert_eq!(snapshot.version(), 5);
  assert_eq!(snapshot.into_aggregate().name, "current");
}

// FR7.3 ① / AC1.1.1 / NFR2: 型レベル検証 — derive(Serialize, Deserialize) のみの
// プレーン型（Debug / Clone なし）が payload 最小境界を満たし、封筒を構築できる
#[test]
fn test_plain_serde_type_satisfies_minimal_payload_bound() {
  // Debug / Clone を持たないプレーン型（ライブラリ trait 非実装）
  #[derive(Serialize, Deserialize)]
  struct PlainPayload {
    value: String,
  }

  #[derive(Serialize, Deserialize)]
  struct PlainAggregate {
    value: String,
  }

  // payload 最小境界（Serialize + DeserializeOwned + Send + Sync + 'static — BR1.6）を
  // 満たすことのコンパイル検証
  fn assert_minimal_payload_bound<T: Serialize + DeserializeOwned + Send + Sync + 'static>() {}
  assert_minimal_payload_bound::<PlainPayload>();
  assert_minimal_payload_bound::<PlainAggregate>();

  let envelope = EventEnvelope::new(
    TestId("a-1".to_string()),
    1,
    fixed_occurred_at(),
    PlainPayload {
      value: "plain".to_string(),
    },
  )
  .with_manifest("plain-v1");
  assert_eq!(envelope.seq_nr(), 1);
  assert_eq!(envelope.manifest(), "plain-v1");
  assert_eq!(envelope.payload().value, "plain");
  assert_eq!(envelope.into_payload().value, "plain");

  let snapshot = SnapshotEnvelope::new(
    PlainAggregate {
      value: "plain".to_string(),
    },
    1,
    1,
  );
  assert_eq!(snapshot.aggregate().value, "plain");
  assert_eq!(snapshot.into_aggregate().value, "plain");
}
