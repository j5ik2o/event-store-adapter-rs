//! payload と集約状態の、JSON の値の比較。

use serde_json::Value;

/// 数値の 10 進の値を、符号・先頭と末尾の 0 を除いた数字列・10 の指数に正規化する。
///
/// 0 は符号を持たない。指数が `i128` に収まらないときは `None` を返す。
fn normalize_number(text: &str) -> Option<(bool, String, i128)> {
  let (negative, unsigned) = match text.strip_prefix('-') {
    Some(rest) => (true, rest),
    None => (false, text),
  };
  let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
    Some(position) => (&unsigned[..position], unsigned[position + 1..].parse::<i128>().ok()?),
    None => (unsigned, 0),
  };
  let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
  let digits = format!("{integer}{fraction}");
  let exponent = exponent.checked_sub(i128::try_from(fraction.len()).ok()?)?;
  let without_leading_zeros = digits.trim_start_matches('0');
  if without_leading_zeros.is_empty() {
    return Some((false, String::new(), 0));
  }
  let trimmed = without_leading_zeros.trim_end_matches('0');
  let exponent = exponent.checked_add(i128::try_from(without_leading_zeros.len() - trimmed.len()).ok()?)?;
  Some((negative, trimmed.to_string(), exponent))
}

fn numbers_equal(expected: &str, actual: &str) -> bool {
  match (normalize_number(expected), normalize_number(actual)) {
    (Some(left), Some(right)) => left == right,
    _ => expected == actual,
  }
}

/// 2 つの JSON の値が、要求する意味で等しければ真を返す。
///
/// オブジェクトのキーの順と空白は無視し、配列の順は保つ。`null`・真偽値・文字列・数値の値を保ち、
/// 真偽値と数値は同一視しない。Unicode 正規化はしない。数値は書式（`1` と `1.0`）ではなく、
/// 10 進の値で比べる。`serde_json::Value` の `==` は、`arbitrary_precision` では数値を元の文字列で比べ
/// るので、使わない。
pub fn json_equal(expected: &Value, actual: &Value) -> bool {
  match (expected, actual) {
    (Value::Null, Value::Null) => true,
    (Value::Bool(left), Value::Bool(right)) => left == right,
    (Value::String(left), Value::String(right)) => left.as_bytes() == right.as_bytes(),
    (Value::Number(left), Value::Number(right)) => numbers_equal(&left.to_string(), &right.to_string()),
    (Value::Array(left), Value::Array(right)) => {
      left.len() == right.len() && left.iter().zip(right).all(|(l, r)| json_equal(l, r))
    }
    (Value::Object(left), Value::Object(right)) => {
      left.len() == right.len()
        && left
          .iter()
          .all(|(key, value)| right.get(key).is_some_and(|other| json_equal(value, other)))
    }
    _ => false,
  }
}
