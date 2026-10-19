//! 封筒の固定バイナリ形式（4 章・BT-D6a・ADR-0008）の試験。
//! `envelope.rs` の末尾から `#[cfg(test)] #[path = "envelope_test.rs"] mod tests;` で結ぶ。

use super::*;
use crate::error::{EventStoreError, StorageOperation};

fn from_hex(text: &str) -> Vec<u8> {
  assert_eq!(text.len() % 2, 0, "16 進数は偶数桁: {text}");
  (0..text.len())
    .step_by(2)
    .map(|index| u8::from_str_radix(&text[index..index + 2], 16).expect("16 進数の 1 バイト"))
    .collect()
}

fn to_hex(bytes: &[u8]) -> String {
  bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// 空の manifest・payload を持つ、正しいイベント封筒（aid "Order-9"・seq_nr 1・occurred_at_ns 0）。
const EMPTY_EVENT_ENVELOPE_HEX: &str = "0100074f726465722d39000000000000000100000000000000000000000000000000";

// BT-D6a: イベント封筒は 0x01 ‖ U16(aid の長さ) ‖ aid ‖ U64(seq_nr) ‖ I64(occurred_at_ns) ‖ U32(manifest の長さ) ‖ manifest ‖ U32(payload の長さ) ‖ payload。
#[test]
fn should_encode_an_event_envelope_in_the_fixed_big_endian_layout() {
  let fields = EventEnvelopeFields {
    aid: "Order-9".to_string(),
    seq_nr: 2,
    occurred_at_ns: 1_000_000_000,
    manifest: "v1".to_string(),
    payload: vec![0x00, 0x01, 0x02, 0x03, 0xfe, 0xff],
  };

  let encoded = encode_event(&fields);

  assert_eq!(
    to_hex(&encoded),
    "0100074f726465722d390000000000000002000000003b9aca000000000276310000000600010203feff"
  );
}

// BT-D6a: 未使用の末尾のバイトがある封筒は、復元せずに保存先エラーにする。
#[test]
fn should_decode_reject_an_event_envelope_with_an_unused_trailing_byte_as_a_storage_error() {
  let mut bytes = from_hex(EMPTY_EVENT_ENVELOPE_HEX);
  bytes.push(0x00);

  let result = decode_event(&bytes, StorageOperation::LoadEvents);

  assert!(
    matches!(result, Err(EventStoreError::Storage { .. })),
    "保存先の分類のエラーで、封筒は返らない: {:?}",
    result.as_ref().err()
  );
}

// BT-D6a: 未知の形式の版番号（0x02）の封筒は、復元せずに保存先エラーにする。
#[test]
fn should_decode_reject_an_event_envelope_with_an_unknown_format_version_as_a_storage_error() {
  let mut bytes = from_hex(EMPTY_EVENT_ENVELOPE_HEX);
  bytes[0] = 0x02;

  let result = decode_event(&bytes, StorageOperation::LoadEvents);

  assert!(
    matches!(result, Err(EventStoreError::Storage { .. })),
    "保存先の分類のエラーで、封筒は返らない: {:?}",
    result.as_ref().err()
  );
}

// BT-16: 上限の判定に使う長さは、直列化した封筒のバイト数と一致する（固定部分・aid・manifest・payload を全て数える）。
#[test]
fn should_envelope_length_for_the_limit_equal_the_encoded_length() {
  let event = EventEnvelopeFields {
    aid: "注文-1".to_string(),
    seq_nr: 7,
    occurred_at_ns: -1,
    manifest: "é".to_string(),
    payload: vec![0xff; 33],
  };
  let empty_event = EventEnvelopeFields {
    aid: String::new(),
    seq_nr: 1,
    occurred_at_ns: 0,
    manifest: String::new(),
    payload: Vec::new(),
  };
  let snapshot = SnapshotEnvelopeFields {
    seq_nr: 7,
    manifest: "スナップ".to_string(),
    payload: vec![0x7b, 0x7d],
  };
  let empty_snapshot = SnapshotEnvelopeFields {
    seq_nr: 1,
    manifest: String::new(),
    payload: Vec::new(),
  };

  assert_eq!(event_envelope_len(&event), encode_event(&event).len());
  assert_eq!(event_envelope_len(&empty_event), encode_event(&empty_event).len());
  assert_eq!(snapshot_envelope_len(&snapshot), encode_snapshot(&snapshot).len());
  assert_eq!(
    snapshot_envelope_len(&empty_snapshot),
    encode_snapshot(&empty_snapshot).len()
  );
}
