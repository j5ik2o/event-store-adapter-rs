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

// ---------------------------------------------------------------------------
// 指数が i128 に収まらない数値（書き方ではなく、値で比べる）
// ---------------------------------------------------------------------------

/// `i128::MAX` + 1。
const BEYOND_I128: &str = "170141183460469231731687303715884105728";
/// `i128::MAX` + 2。
const BEYOND_I128_PLUS_ONE: &str = "170141183460469231731687303715884105729";
/// `i128::MAX`。
const I128_MAX: &str = "170141183460469231731687303715884105727";
/// `i128::MAX` − 1。
const I128_MAX_MINUS_ONE: &str = "170141183460469231731687303715884105726";

/// 1 に、0 を `zeros` 個並べた 10 進の文字列（10 の `zeros` 乗）。
fn power_of_ten(zeros: usize) -> String {
  format!("1{}", "0".repeat(zeros))
}

/// 9 を `count` 個並べた 10 進の文字列（10 の `count` 乗 − 1）。
fn nines(count: usize) -> String {
  "9".repeat(count)
}

fn assert_equal_in_both_directions(left: &str, right: &str) {
  assert!(json_equal(&json(left), &json(right)), "{left} と {right} は等しい値");
  assert!(json_equal(&json(right), &json(left)), "{right} と {left} は等しい値");
}

fn assert_different_in_both_directions(left: &str, right: &str) {
  assert!(!json_equal(&json(left), &json(right)), "{left} と {right} は異なる値");
  assert!(!json_equal(&json(right), &json(left)), "{right} と {left} は異なる値");
}

#[test]
fn test_json_equal_treats_equal_values_with_exponent_beyond_i128_as_equal() {
  // 数字列と指数の分け方が違っても、同じ値。
  assert_equal_in_both_directions(&format!("1e{BEYOND_I128}"), &format!("10e{I128_MAX}"));
  assert_equal_in_both_directions(&format!("1e{BEYOND_I128}"), &format!("1E+{BEYOND_I128}"));
  assert_equal_in_both_directions(&format!("1e{BEYOND_I128}"), &format!("100e{I128_MAX_MINUS_ONE}"));
}

#[test]
fn test_json_equal_treats_values_across_the_i128_boundary_as_equal() {
  // 小数点の位置の調整で、指数が i128 の境界をまたぐ組。
  assert_equal_in_both_directions(&format!("0.1e{BEYOND_I128}"), &format!("1e{I128_MAX}"));
  assert_equal_in_both_directions(&format!("1.0e{BEYOND_I128}"), &format!("0.1e{BEYOND_I128_PLUS_ONE}"));
}

#[test]
fn test_json_equal_borrows_across_every_digit_of_a_huge_exponent() {
  // 10^42 と 10^42 − 1 の間の、繰り下がり。
  let power = power_of_ten(42);
  let below = nines(42);

  assert_equal_in_both_directions(&format!("0.1e{power}"), &format!("1e{below}"));
  assert_different_in_both_directions(&format!("1e{power}"), &format!("1e{below}"));
}

#[test]
fn test_json_equal_carries_across_every_digit_of_a_huge_exponent() {
  // 10^42 − 3 に、末尾の 0 の 3 個を足すと、10^42 になる。
  let below_by_three = format!("{}7", nines(41));

  assert_equal_in_both_directions(&format!("1000e{below_by_three}"), &format!("1e{}", power_of_ten(42)));
}

#[test]
fn test_json_equal_treats_negative_exponents_beyond_i128_as_equal() {
  assert_equal_in_both_directions(
    "1e-170141183460469231731687303715884105729",
    "10e-170141183460469231731687303715884105730",
  );
  assert_equal_in_both_directions(
    "0.01e-170141183460469231731687303715884105727",
    "1e-170141183460469231731687303715884105729",
  );
}

#[test]
fn test_json_equal_ignores_leading_zeros_of_an_exponent() {
  assert_equal_in_both_directions("1e0005", "100000");
  assert_equal_in_both_directions(&format!("1e0000{BEYOND_I128}"), &format!("1e{BEYOND_I128}"));
}

#[test]
fn test_json_equal_treats_zero_with_a_huge_exponent_as_zero() {
  let huge = "99999999999999999999999999999999999999999";

  assert_equal_in_both_directions(&format!("0e{huge}"), "0");
  assert_equal_in_both_directions(&format!("-0.0e-{huge}"), "0");
  assert_equal_in_both_directions(&format!("0e{huge}"), &format!("-0e-{huge}"));
  assert_different_in_both_directions(&format!("0e{huge}"), &format!("1e-{huge}"));
}

#[test]
fn test_json_equal_distinguishes_different_values_with_exponent_beyond_i128() {
  // 指数が 1 違う。
  assert_different_in_both_directions(&format!("1e{BEYOND_I128}"), &format!("1e{BEYOND_I128_PLUS_ONE}"));
  // 指数が同じで、数字列が違う。
  assert_different_in_both_directions(&format!("12e{BEYOND_I128}"), &format!("13e{BEYOND_I128}"));
  // 符号が違う。
  assert_different_in_both_directions(&format!("1e{BEYOND_I128}"), &format!("-1e{BEYOND_I128}"));
  // 指数の符号が違う。
  assert_different_in_both_directions(&format!("1e{BEYOND_I128}"), &format!("1e-{BEYOND_I128}"));
  // 巨大な指数の値と、0。
  assert_different_in_both_directions(&format!("1e{BEYOND_I128}"), "0");
}

#[test]
fn test_json_equal_compares_numbers_with_a_huge_exponent_inside_composite_values() {
  assert!(json_equal(
    &json(&format!(r#"{{"n":[1e{BEYOND_I128}]}}"#)),
    &json(&format!(r#"{{"n":[10e{I128_MAX}]}}"#))
  ));
  assert!(!json_equal(
    &json(&format!(r#"{{"n":[1e{BEYOND_I128}]}}"#)),
    &json(&format!(r#"{{"n":[2e{I128_MAX}]}}"#))
  ));
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
