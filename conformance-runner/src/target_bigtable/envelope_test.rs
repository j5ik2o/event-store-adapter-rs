//! 封筒の値の表の実行（`envelope.rs`）の試験。`envelope.rs` の末尾から
//! `#[cfg(test)] #[path = "envelope_test.rs"] mod tests;` で結ぶ。

use serde_json::{json, Value};

use super::*;

fn storage_error_expectation() -> Value {
  json!({"error": {"category": "storage", "message": {"must_contain": [], "must_not_contain": []}}})
}

fn encode_event_case(input: Value, expect_value: &str) -> Value {
  json!({"operation": ENCODE_EVENT, "input": input, "expect": {"value": expect_value}})
}

fn decode_case(kind: &str, bytes_hex: &str) -> Value {
  json!({
    "operation": DECODE,
    "input": {"kind": kind, "bytes_hex": bytes_hex},
    "expect": storage_error_expectation()
  })
}

const EVENT_EMPTY: &str = "0100074f726465722d39000000000000000100000000000000000000000000000000";

fn event_empty_input() -> Value {
  json!({"aid": "Order-9", "seq_nr": 1, "occurred_at_ns": "0", "manifest": "", "payload_hex": ""})
}

fn is_passed(outcome: &CaseOutcome) -> bool {
  matches!(outcome, CaseOutcome::Passed { .. })
}

fn failure_detail(outcome: &CaseOutcome) -> &str {
  match outcome {
    CaseOutcome::Failed { detail, .. } => detail,
    other => panic!("失敗のはずが {other:?}"),
  }
}

#[test]
fn should_is_envelope_operation_accept_only_the_three_envelope_operations() {
  for operation in [ENCODE_EVENT, ENCODE_SNAPSHOT, DECODE] {
    assert!(is_envelope_operation(&json!({"operation": operation})), "{operation}");
  }
  for operation in ["buildAid", "validateSeqNr", "validateOccurredAt", "fnv1a64"] {
    assert!(!is_envelope_operation(&json!({"operation": operation})), "{operation}");
  }
  assert!(!is_envelope_operation(&json!({})));
}

#[test]
fn should_from_hex_read_lowercase_pairs_and_to_hex_write_them_back() {
  let bytes = from_hex("00010203feff").expect("小文字の 16 進を読める");

  assert_eq!(bytes, vec![0x00, 0x01, 0x02, 0x03, 0xfe, 0xff]);
  assert_eq!(to_hex(&bytes), "00010203feff");
  assert_eq!(from_hex("").expect("空は空のバイト列"), Vec::<u8>::new());
}

#[test]
fn should_from_hex_reject_uppercase_odd_length_and_non_hex_characters() {
  assert!(from_hex("FE").is_err(), "データは小文字の 16 進と定めている");
  assert!(from_hex("abc").is_err());
  assert!(from_hex("0g").is_err());
  assert!(from_hex("0x01").is_err());
}

#[test]
fn should_run_pass_an_encode_case_whose_expectation_equals_the_library_output() {
  let outcome = run(&encode_event_case(event_empty_input(), EVENT_EMPTY));

  match outcome {
    CaseOutcome::Passed { values: Some(values) } => {
      assert_eq!(values.expected, json!({"value": EVENT_EMPTY}));
      assert_eq!(values.actual, json!({"value": EVENT_EMPTY}));
    }
    other => panic!("成功のはずが {other:?}"),
  }
}

#[test]
fn should_run_fail_an_encode_case_whose_expectation_differs_from_the_library_output() {
  let different = EVENT_EMPTY.replacen("0100", "0101", 1);

  let outcome = run(&encode_event_case(event_empty_input(), &different));

  assert!(!failure_detail(&outcome).is_empty());
}

#[test]
fn should_run_encode_snapshot_cases() {
  let case = json!({
    "operation": ENCODE_SNAPSHOT,
    "input": {"seq_nr": 1, "manifest": "", "payload_hex": ""},
    "expect": {"value": "0100000000000000010000000000000000"}
  });

  assert!(is_passed(&run(&case)));
}

#[test]
fn should_run_fail_an_encode_case_whose_expected_bytes_cannot_be_restored() {
  // 符号化の結果は期待値と一致するが、seq_nr 0 の封筒は復元できない（seq_nr は 1 以上）。
  let case = json!({
    "operation": ENCODE_SNAPSHOT,
    "input": {"seq_nr": 0, "manifest": "", "payload_hex": ""},
    "expect": {"value": "0100000000000000000000000000000000"}
  });

  let outcome = run(&case);

  assert!(
    failure_detail(&outcome).contains("復元できない"),
    "符号化の一致だけでは成功にしない: {outcome:?}"
  );
}

#[test]
fn should_run_fail_an_encode_case_whose_restored_fields_differ_from_the_input() {
  // aid の長さの欄は 65535 で止まるので、符号化の結果は期待値と一致するが、復元すると aid と payload が入力と食い違う。
  let aid = format!(
    "{}{}{}{}{}",
    "a".repeat(65535),
    "\0".repeat(7) + "\u{1}",
    "\0".repeat(8),
    "\0".repeat(4),
    "\0\0\0\u{18}"
  );
  let fields = EventEnvelopeFields {
    aid: aid.clone(),
    seq_nr: 1,
    occurred_at_ns: 0,
    manifest: String::new(),
    payload: vec![],
  };
  let expected_hex = to_hex(&encode_bigtable_event_envelope_for_test(&fields));
  let case = encode_event_case(
    json!({"aid": aid, "seq_nr": 1, "occurred_at_ns": "0", "manifest": "", "payload_hex": ""}),
    &expected_hex,
  );
  let restored =
    decode_bigtable_event_envelope_for_test(&from_hex(&expected_hex).unwrap(), StorageOperation::LoadEvents)
      .expect("試験の前提: 期待値のバイト列は復元できる");
  assert_ne!(restored, fields, "試験の前提: 復元した項目が入力と食い違う");

  let outcome = run(&case);

  assert!(
    failure_detail(&outcome).contains("一致しない"),
    "符号化の一致だけでは成功にしない: {outcome:?}"
  );
}

#[test]
fn should_run_fail_an_encode_case_whose_input_is_malformed() {
  let mut bad_time = event_empty_input();
  bad_time["occurred_at_ns"] = json!("9223372036854775808");
  let mut bad_payload = event_empty_input();
  bad_payload["payload_hex"] = json!("FF");
  let mut missing_manifest = event_empty_input();
  missing_manifest.as_object_mut().unwrap().remove("manifest");

  for input in [bad_time, bad_payload, missing_manifest] {
    let outcome = run(&encode_event_case(input.clone(), EVENT_EMPTY));

    assert!(!failure_detail(&outcome).is_empty(), "{input}");
  }
}

#[test]
fn should_run_pass_a_decode_case_whose_envelope_is_malformed() {
  let trailing = format!("{EVENT_EMPTY}00");
  let unknown_version = EVENT_EMPTY.replacen("0100", "0200", 1);

  assert!(is_passed(&run(&decode_case("event", &trailing))));
  assert!(is_passed(&run(&decode_case("event", &unknown_version))));
  assert!(is_passed(&run(&decode_case("snapshot", "01"))));
}

#[test]
fn should_run_fail_a_decode_case_whose_envelope_is_actually_valid() {
  let outcome = run(&decode_case("event", EVENT_EMPTY));

  assert!(!failure_detail(&outcome).is_empty());
}

#[test]
fn should_run_fail_a_decode_case_with_an_unknown_kind_or_unreadable_bytes() {
  for case in [
    decode_case("journal", EVENT_EMPTY),
    decode_case("event", "xyz"),
    json!({"operation": DECODE, "input": {"kind": "event"}, "expect": storage_error_expectation()}),
  ] {
    let outcome = run(&case);

    assert!(!failure_detail(&outcome).is_empty(), "{case}");
  }
}

#[test]
fn should_run_fail_when_the_decode_error_category_differs_from_the_expectation() {
  let mut case = decode_case("event", &format!("{EVENT_EMPTY}00"));
  case["expect"]["error"]["category"] = json!("optimistic-lock");

  let outcome = run(&case);

  assert!(!failure_detail(&outcome).is_empty());
}
