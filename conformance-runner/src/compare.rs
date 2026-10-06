//! payload と集約状態の、JSON の値の比較。

use serde_json::Value;

use crate::number::normalize_decimal;

/// 2 つの数値の書き方を、10 進の値で比べる。指数の桁数に上限はない。どちらかが数値の書き方として読めない
/// ときは、値を比べられないので偽を返す。
fn numbers_equal(expected: &str, actual: &str) -> bool {
  match (normalize_decimal(expected), normalize_decimal(actual)) {
    (Some(left), Some(right)) => left == right,
    _ => false,
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
