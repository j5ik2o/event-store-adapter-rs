//! FNV-1a 64 の値の表の実行（`hash_values.rs`）の試験。`hash_values.rs` の冒頭近くから
//! `#[cfg(test)] #[path = "hash_values_test.rs"] mod tests;` で結ぶ。
//!
//! ライブラリの関数は、保存先ごとの実行の試験（`tests/bigtable_test.rs`・`tests/spanner_test.rs`）が実データで確かめる。
//! ここでは、保存先に依存しない値の表の読み方と比べ方を、データの期待値と同じ値を返す参照実装で確かめる。

use std::path::Path;

use serde_json::{json, Value};

use super::*;
use crate::data::{load, Case, DataSet};

/// FNV-1a 64（hash.md の K-1）の参照実装。ライブラリの関数とは独立に、規格の定数から書く。
fn reference_fnv1a64(input: &[u8]) -> u64 {
  input.iter().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
    (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
  })
}

fn run(body: &Value) -> CaseOutcome {
  run_with(body, reference_fnv1a64)
}

fn hash_case(utf8: &str, expect_value: &str) -> Value {
  json!({"operation": FNV1A64, "input": {"utf8": utf8}, "expect": {"value": expect_value}})
}

fn failure_detail(outcome: &CaseOutcome) -> &str {
  match outcome {
    CaseOutcome::Failed { detail, .. } => detail,
    other => panic!("失敗のはずが {other:?}"),
  }
}

fn real_data() -> DataSet {
  load(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance")).expect("実データを読める")
}

fn hash_cases(data: &DataSet) -> Vec<&Case> {
  let cases: Vec<&Case> = data.cases.iter().filter(|case| is_hash_operation(&case.body)).collect();
  assert_eq!(
    cases.iter().map(|case| case.id.as_str()).collect::<Vec<_>>(),
    ["hash-fnv1a64-1", "hash-fnv1a64-2", "hash-fnv1a64-3", "hash-fnv1a64-4"],
    "配布データの FNV-1a 64 の値の表は 4 件"
  );
  cases
}

#[test]
fn should_is_hash_operation_accept_only_fnv1a64() {
  assert!(is_hash_operation(&json!({"operation": "fnv1a64"})));
  for operation in [
    "buildAid",
    "validateSeqNr",
    "validateOccurredAt",
    "encodeBigtableEventEnvelope",
    "decodeBigtableEnvelope",
    "FNV1A64",
  ] {
    assert!(!is_hash_operation(&json!({"operation": operation})), "{operation}");
  }
  assert!(!is_hash_operation(&json!({"operation": 1})));
  assert!(!is_hash_operation(&json!({})));
}

#[test]
fn should_expected_value_read_a_fixed_sixteen_digit_hexadecimal_with_prefix() {
  assert_eq!(expected_value("0xaf63dc4c8601ec8c"), Ok(0xaf63_dc4c_8601_ec8c));
  assert_eq!(expected_value("0x0aa990285e0388ca"), Ok(0x0aa9_9028_5e03_88ca));
  assert_eq!(expected_value("0x0000000000000000"), Ok(0));
  assert_eq!(expected_value("0xffffffffffffffff"), Ok(u64::MAX));
  assert_eq!(expected_value("0xAF63DC4C8601EC8C"), Ok(0xaf63_dc4c_8601_ec8c));
}

#[test]
fn should_expected_value_reject_values_that_are_not_a_fixed_sixteen_digit_hexadecimal_with_prefix() {
  for text in [
    "af63dc4c8601ec8c",
    "0Xaf63dc4c8601ec8c",
    "0xaf63dc4c8601ec8",
    "0xaf63dc4c8601ec8cc",
    "0x",
    "",
    "0xaf63dc4c8601ec8g",
    "0x+f63dc4c8601ec8c",
    "0x-f63dc4c8601ec8c",
    "0x af63dc4c8601ec8",
  ] {
    assert!(expected_value(text).is_err(), "{text:?}");
  }
}

#[test]
fn should_run_pass_a_case_whose_expectation_equals_the_hash_output() {
  let outcome = run(&hash_case("a", "0xaf63dc4c8601ec8c"));

  match outcome {
    CaseOutcome::Passed { values: Some(values) } => {
      assert_eq!(values.expected, json!({"value": "0xaf63dc4c8601ec8c"}));
      assert_eq!(values.actual, json!({"value": "0xaf63dc4c8601ec8c"}));
    }
    other => panic!("成功のはずが {other:?}"),
  }
}

#[test]
fn should_run_pass_every_fnv1a64_case_of_the_distributed_data_with_the_reference_hash() {
  let data = real_data();

  for case in hash_cases(&data) {
    let outcome = run(&case.body);

    assert!(
      matches!(outcome, CaseOutcome::Passed { .. }),
      "{}: {outcome:?}",
      case.id
    );
  }
}

#[test]
fn should_run_fail_a_case_whose_expectation_differs_from_the_hash_output() {
  let outcome = run(&hash_case("a", "0xaf63dc4c8601ec8d"));

  match outcome {
    CaseOutcome::Failed {
      detail,
      expected,
      actual,
      ..
    } => {
      assert!(!detail.is_empty());
      assert_eq!(expected, Some(json!({"value": "0xaf63dc4c8601ec8d"})));
      assert_eq!(actual, Some(json!({"value": "0xaf63dc4c8601ec8c"})));
    }
    other => panic!("失敗のはずが {other:?}"),
  }
}

#[test]
fn should_run_with_use_the_given_hash_and_report_its_value_in_sixteen_digits() {
  let outcome = run_with(&hash_case("a", "0x00000000000000ff"), |bytes| {
    assert_eq!(bytes, b"a", "入力の UTF-8 のバイト列がそのまま渡る");
    0xff
  });

  match outcome {
    CaseOutcome::Passed { values: Some(values) } => {
      assert_eq!(values.actual, json!({"value": "0x00000000000000ff"}));
    }
    other => panic!("成功のはずが {other:?}"),
  }
}

#[test]
fn should_run_with_pass_the_utf8_bytes_of_a_multibyte_input_to_the_hash() {
  let outcome = run_with(&hash_case("ユーザー", "0x309f20171820053e"), |bytes| {
    assert_eq!(bytes, "ユーザー".as_bytes());
    0x309f_2017_1820_053e
  });

  assert!(matches!(outcome, CaseOutcome::Passed { .. }), "{outcome:?}");
}

#[test]
fn should_run_fail_every_fnv1a64_case_of_the_distributed_data_when_the_hash_returns_a_wrong_value() {
  let data = real_data();
  let wrong_hash = |_: &[u8]| 0_u64;

  for case in hash_cases(&data) {
    let expected = case
      .body
      .pointer("/expect/value")
      .and_then(Value::as_str)
      .and_then(|text| expected_value(text).ok())
      .expect("期待値を読める");
    assert_ne!(
      expected,
      wrong_hash(&[]),
      "{}: 偽のハッシュの値が期待値と一致していない前提",
      case.id
    );

    let outcome = crate::runner::run_case(case, &crate::target_spanner::TARGET, &data.coverage, |_, prepared| {
      run_with(&prepared.body, wrong_hash)
    });

    assert!(
      matches!(outcome, CaseOutcome::Failed { .. }),
      "{}: {outcome:?}",
      case.id
    );
  }
}

#[test]
fn should_run_with_fail_when_the_hash_returns_the_value_of_another_input() {
  // 別の入力の期待値を返すハッシュは、このケースの期待値と違うので失敗にする。
  let outcome = run_with(&hash_case("a", "0xaf63dc4c8601ec8c"), |_| 0xadb1_03af_5fe2_a91a);

  assert!(failure_detail(&outcome).contains("一致しない"), "{outcome:?}");
}

#[test]
fn should_run_fail_a_case_whose_expectation_is_not_a_fixed_sixteen_digit_hexadecimal() {
  for expect_value in [
    "af63dc4c8601ec8c",
    "0xaf63dc4c8601ec8",
    "0xaf63dc4c8601ec8cc",
    "0xaf63dc4c8601ec8g",
    "0x+f63dc4c8601ec8c",
  ] {
    let outcome = run(&hash_case("a", expect_value));

    assert!(
      failure_detail(&outcome).contains("expect.value を読めない"),
      "{expect_value}: {outcome:?}"
    );
  }
}

#[test]
fn should_run_fail_a_case_whose_expectation_value_is_not_a_string() {
  let body = json!({"operation": FNV1A64, "input": {"utf8": "a"}, "expect": {"value": 123}});

  assert!(failure_detail(&run(&body)).contains("expect.value を読めない"));
}

#[test]
fn should_run_fail_a_case_without_the_expected_value() {
  let body = json!({"operation": FNV1A64, "input": {"utf8": "a"}, "expect": {}});

  assert!(failure_detail(&run(&body)).contains("expect"));
}

#[test]
fn should_run_fail_a_case_without_the_expectation() {
  let body = json!({"operation": FNV1A64, "input": {"utf8": "a"}});

  assert!(failure_detail(&run(&body)).contains("expect"));
}

#[test]
fn should_run_fail_a_case_whose_input_has_no_utf8_string() {
  let missing = json!({"operation": FNV1A64, "input": {}, "expect": {"value": "0xaf63dc4c8601ec8c"}});
  let not_a_string = json!({"operation": FNV1A64, "input": {"utf8": 97}, "expect": {"value": "0xaf63dc4c8601ec8c"}});
  let no_input = json!({"operation": FNV1A64, "expect": {"value": "0xaf63dc4c8601ec8c"}});

  assert!(failure_detail(&run(&missing)).contains("input.utf8 を読めない"));
  assert!(failure_detail(&run(&not_a_string)).contains("input.utf8 を読めない"));
  assert!(failure_detail(&run(&no_input)).contains("input"));
}
