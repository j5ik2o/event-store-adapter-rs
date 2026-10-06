//! JSON の数値の、整数としての読み取りの試験。
//! 値は必ず JSON の文字列から作る（`json!` の数値は書き方の違いを持たないため）。

use event_store_adapter_conformance_rs::number::to_integer;
use serde_json::Value;

fn parse(text: &str) -> Value {
  serde_json::from_str(text).unwrap_or_else(|error| panic!("{text} は JSON: {error}"))
}

#[test]
fn should_to_integer_accepts_every_way_json_schema_writes_an_integer() {
  let cases = [
    ("1", 1),
    ("0", 0),
    ("-7", -7),
    ("1.0", 1),
    ("3.0", 3),
    ("1e3", 1000),
    ("1E+3", 1000),
    ("10e-1", 1),
    ("2.0e0", 2),
    ("3e0", 3),
    ("-3.0", -3),
    ("0.0", 0),
    ("-0", 0),
    ("0e5", 0),
    ("12.50e1", 125),
  ];

  for (text, expected) in cases {
    assert_eq!(to_integer(&parse(text)), Some(expected), "{text}");
  }
}

#[test]
fn should_to_integer_rejects_values_with_a_non_zero_fraction() {
  for text in [
    "1.5",
    "2.5",
    "1e-1",
    "0.1",
    "-0.5",
    "15e-1",
    "1.0000000000000000000000000001",
  ] {
    assert_eq!(to_integer(&parse(text)), None, "{text}");
  }
}

#[test]
fn should_to_integer_keeps_digits_beyond_what_a_float_can_hold() {
  // 2^53 + 1 は f64 では表せない。
  assert_eq!(to_integer(&parse("9007199254740993")), Some(9_007_199_254_740_993));
  assert_eq!(to_integer(&parse("9007199254740993.0")), Some(9_007_199_254_740_993));
  assert_eq!(
    to_integer(&parse("170141183460469231731687303715884105727")),
    Some(i128::MAX)
  );
  assert_eq!(to_integer(&parse("1e38")), Some(10i128.pow(38)));
}

#[test]
fn should_to_integer_rejects_values_that_do_not_fit_in_i128() {
  for text in [
    "170141183460469231731687303715884105728",
    "-170141183460469231731687303715884105729",
    "1e39",
    "-1e39",
    "1e100",
    "1e1000000000",
  ] {
    assert_eq!(to_integer(&parse(text)), None, "{text}");
  }
}

#[test]
fn should_to_integer_reads_the_lower_bound_of_i128() {
  // `i128::MIN` の大きさ（2^127）は、正の `i128` には収まらない。負の値としては `i128` に収まる。
  assert_eq!(
    to_integer(&parse("-170141183460469231731687303715884105728")),
    Some(i128::MIN)
  );
  assert_eq!(
    to_integer(&parse("-170141183460469231731687303715884105727")),
    Some(-i128::MAX)
  );
  // 書き方が違っても、同じ値。
  for text in [
    "-170141183460469231731687303715884105728.0",
    "-1.70141183460469231731687303715884105728e38",
    "-17014118346046923173168730371588410572.8e1",
    "-1701411834604692317316873037158841057280e-1",
  ] {
    assert_eq!(to_integer(&parse(text)), Some(i128::MIN), "{text}");
  }
}

#[test]
fn should_to_integer_reads_zero_whatever_its_exponent() {
  let huge = "1000000000000000000000000000000000000000";

  for text in [format!("0e{huge}"), format!("0e-{huge}"), format!("-0.0e{huge}")] {
    assert_eq!(to_integer(&parse(&text)), Some(0), "{text}");
  }
}

#[test]
fn should_to_integer_rejects_non_zero_values_whose_exponent_does_not_fit_in_i128() {
  let huge = "1000000000000000000000000000000000000000";

  for text in [format!("1e{huge}"), format!("1e-{huge}"), format!("-1e{huge}")] {
    assert_eq!(to_integer(&parse(&text)), None, "{text}");
  }
}

#[test]
fn should_to_integer_reads_an_integer_written_with_a_leading_zero_exponent() {
  assert_eq!(to_integer(&parse("1e0003")), Some(1000));
  assert_eq!(to_integer(&parse("10e-0001")), Some(1));
}

#[test]
fn should_to_integer_rejects_values_that_are_not_numbers() {
  for text in [r#""1""#, "null", "true", "[1]", r#"{"n":1}"#] {
    assert_eq!(to_integer(&parse(text)), None, "{text}");
  }
}
