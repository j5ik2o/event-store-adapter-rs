//! JSON の値の比較（payload と集約状態の比較）の試験。

use event_store_adapter_conformance_rs::compare::json_equal;
use serde_json::Value;

/// 数値の書式（`1.0`・`1e0` など）を保ったまま読むため、必ず文字列から作る。
fn json(text: &str) -> Value {
  serde_json::from_str(text).expect("試験の入力は正しい JSON")
}

#[test]
fn test_json_equal_ignores_object_key_order() {
  assert!(json_equal(
    &json(r#"{"a":1,"b":[1,2],"c":{"x":null,"y":"s"}}"#),
    &json(r#"{"c":{"y":"s","x":null},"b":[1,2],"a":1}"#)
  ));
}

#[test]
fn test_json_equal_distinguishes_array_order() {
  assert!(!json_equal(&json("[1,2,3]"), &json("[3,2,1]")));
}

#[test]
fn test_json_equal_distinguishes_boolean_from_number() {
  assert!(!json_equal(&json("true"), &json("1")));
  assert!(!json_equal(&json("false"), &json("0")));
}

#[test]
fn test_json_equal_treats_numbers_with_different_notation_as_equal() {
  let one = json("1");

  assert!(json_equal(&one, &json("1.0")));
  assert!(json_equal(&one, &json("1e0")));
  assert!(json_equal(&one, &json("10e-1")));
  assert!(json_equal(&json("100"), &json("1.00E+2")));
}

#[test]
fn test_json_equal_treats_zero_and_negative_zero_as_equal() {
  assert!(json_equal(&json("0"), &json("-0")));
}

#[test]
fn test_json_equal_distinguishes_composed_and_decomposed_unicode() {
  // U+00E9 と、e + U+0301。正規化しないので別の文字列として扱う。
  assert!(!json_equal(&json(r#""é""#), &json(r#""é""#)));
}

#[test]
fn test_json_equal_distinguishes_integers_beyond_128_bits_by_exact_value() {
  let maximum = json("170141183460469231731687303715884105727");
  let last_digit_changed = json("170141183460469231731687303715884105728");

  assert!(json_equal(&maximum, &json("170141183460469231731687303715884105727")));
  assert!(!json_equal(&maximum, &last_digit_changed));
}

#[test]
fn test_json_equal_distinguishes_different_values() {
  assert!(!json_equal(&json("1"), &json("2")));
  assert!(!json_equal(&json("null"), &json("false")));
  assert!(!json_equal(&json(r#""a""#), &json(r#""b""#)));
  assert!(!json_equal(&json(r#"{"a":1}"#), &json(r#"{"a":2}"#)));
  assert!(!json_equal(&json(r#"{"a":1}"#), &json(r#"{"a":1,"b":2}"#)));
  assert!(!json_equal(&json(r#"{"a":1,"b":2}"#), &json(r#"{"a":1}"#)));
}
