//! 数値の 10 進の値の正規化と、JSON の数値の整数としての読み取り。

use serde_json::Value;

/// 数値の 10 進の値を、符号・先頭と末尾の 0 を除いた数字列・10 の指数に正規化する。
///
/// 0 は符号を持たない。指数が `i128` に収まらないときは `None` を返す。
pub(crate) fn normalize_decimal(text: &str) -> Option<(bool, String, i128)> {
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

/// JSON の数値が表す整数を返す。JSON Schema の整数と同じく、`1.0`・`1e3`・`10e-1` のように小数部が 0 の
/// 書き方も整数として読む。
///
/// 数値でない値、小数部が 0 でない値（`1.5`・`1e-1`）、`i128` に収まらない値は `None` を返す。元の桁の
/// 文字列から 10 進で読むので、浮動小数点を経由した精度の損失はない。
pub fn to_integer(value: &Value) -> Option<i128> {
  let Value::Number(number) = value else {
    return None;
  };
  let (negative, digits, exponent) = normalize_decimal(&number.to_string())?;
  if digits.is_empty() {
    return Some(0);
  }
  let exponent = u32::try_from(exponent).ok()?;
  let magnitude = digits
    .parse::<u128>()
    .ok()?
    .checked_mul(10u128.checked_pow(exponent)?)?;
  let magnitude = i128::try_from(magnitude).ok()?;
  Some(if negative { -magnitude } else { magnitude })
}
