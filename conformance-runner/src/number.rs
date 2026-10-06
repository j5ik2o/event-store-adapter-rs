//! 数値の 10 進の値の正規化と、JSON の数値の整数としての読み取り。

use std::cmp::Ordering;

use serde_json::Value;

/// 桁数に上限のない符号付き整数。10 の指数の計算だけに使う。
///
/// JSON の数値の指数は桁数に上限がないので、固定幅の整数では収まらないことがある。正規形は 1 つだけで、
/// 先頭に 0 を置かず、0 は桁が空で符号を持たない。このため `==` が値の比較になる。
#[derive(Debug, PartialEq, Eq)]
struct Exponent {
  negative: bool,
  /// 上位の桁から並べた 10 進の数字（各要素は 0〜9）。
  digits: Vec<u8>,
}

impl Exponent {
  fn zero() -> Exponent {
    Exponent {
      negative: false,
      digits: Vec::new(),
    }
  }

  /// `[+-]?[0-9]+`（先頭の 0 を許す）を読む。この形でなければ `None` を返す。
  fn parse(text: &str) -> Option<Exponent> {
    let (negative, digits) = match text.as_bytes().first()? {
      b'-' => (true, &text[1..]),
      b'+' => (false, &text[1..]),
      _ => (false, text),
    };
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
      return None;
    }
    let digits: Vec<u8> = digits
      .bytes()
      .skip_while(|byte| *byte == b'0')
      .map(|byte| byte - b'0')
      .collect();
    Some(Exponent {
      negative: negative && !digits.is_empty(),
      digits,
    })
  }

  fn from_usize(value: usize) -> Exponent {
    if value == 0 {
      return Exponent::zero();
    }
    Exponent {
      negative: false,
      digits: value.to_string().bytes().map(|byte| byte - b'0').collect(),
    }
  }

  fn negated(self) -> Exponent {
    Exponent {
      negative: !self.negative && !self.digits.is_empty(),
      digits: self.digits,
    }
  }

  fn add(&self, other: &Exponent) -> Exponent {
    if self.negative == other.negative {
      return Exponent {
        negative: self.negative,
        digits: add_magnitudes(&self.digits, &other.digits),
      };
    }
    match compare_magnitudes(&self.digits, &other.digits) {
      Ordering::Equal => Exponent::zero(),
      Ordering::Greater => Exponent {
        negative: self.negative,
        digits: subtract_magnitudes(&self.digits, &other.digits),
      },
      Ordering::Less => Exponent {
        negative: other.negative,
        digits: subtract_magnitudes(&other.digits, &self.digits),
      },
    }
  }

  /// `u32` に収まる 0 以上の値なら返す。負の値と、収まらない値は `None` を返す。
  fn to_u32(&self) -> Option<u32> {
    if self.negative {
      return None;
    }
    self.digits.iter().try_fold(0u32, |accumulated, digit| {
      accumulated.checked_mul(10)?.checked_add(u32::from(*digit))
    })
  }
}

/// 先頭に 0 を置かない 2 つの桁の列を、値の大きさで比べる。
fn compare_magnitudes(left: &[u8], right: &[u8]) -> Ordering {
  left.len().cmp(&right.len()).then_with(|| left.cmp(right))
}

/// 先頭に 0 を置かない 2 つの桁の列の和を返す。
fn add_magnitudes(left: &[u8], right: &[u8]) -> Vec<u8> {
  let mut result = Vec::with_capacity(left.len().max(right.len()) + 1);
  let mut left_digits = left.iter().rev();
  let mut right_digits = right.iter().rev();
  let mut carry = 0;
  loop {
    let (left_digit, right_digit) = (left_digits.next(), right_digits.next());
    if left_digit.is_none() && right_digit.is_none() {
      break;
    }
    let sum = left_digit.copied().unwrap_or(0) + right_digit.copied().unwrap_or(0) + carry;
    result.push(sum % 10);
    carry = sum / 10;
  }
  if carry > 0 {
    result.push(carry);
  }
  result.reverse();
  result
}

/// 先頭に 0 を置かない 2 つの桁の列の差（`larger` − `smaller`）を返す。`larger` は `smaller` 以上でなければ
/// ならない。
fn subtract_magnitudes(larger: &[u8], smaller: &[u8]) -> Vec<u8> {
  let mut result = Vec::with_capacity(larger.len());
  let mut smaller_digits = smaller.iter().rev();
  let mut borrow = 0;
  for digit in larger.iter().rev() {
    let subtrahend = smaller_digits.next().copied().unwrap_or(0) + borrow;
    if *digit >= subtrahend {
      result.push(*digit - subtrahend);
      borrow = 0;
    } else {
      result.push(*digit + 10 - subtrahend);
      borrow = 1;
    }
  }
  while result.last() == Some(&0) {
    result.pop();
  }
  result.reverse();
  result
}

/// 正規化した 10 進の値を表す。`==` が値の比較になる。
///
/// 符号・先頭と末尾の 0 を除いた数字列・10 の指数の 3 つを持つ。0 は符号を持たず、数字列が空で、指数も 0。
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Decimal {
  negative: bool,
  digits: String,
  exponent: Exponent,
}

impl Decimal {
  fn zero() -> Decimal {
    Decimal {
      negative: false,
      digits: String::new(),
      exponent: Exponent::zero(),
    }
  }
}

/// 数値の書き方（JSON の数値の文法）を、正規化した 10 進の値にする。
///
/// 指数の桁数に上限はない。指数が `[+-]?[0-9]+` の形でなければ `None` を返す。
pub(crate) fn normalize_decimal(text: &str) -> Option<Decimal> {
  let (negative, unsigned) = match text.strip_prefix('-') {
    Some(rest) => (true, rest),
    None => (false, text),
  };
  let (mantissa, written_exponent) = match unsigned.find(['e', 'E']) {
    Some(position) => (&unsigned[..position], Exponent::parse(&unsigned[position + 1..])?),
    None => (unsigned, Exponent::zero()),
  };
  let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
  let digits = format!("{integer}{fraction}");
  let without_leading_zeros = digits.trim_start_matches('0');
  if without_leading_zeros.is_empty() {
    return Some(Decimal::zero());
  }
  let trimmed = without_leading_zeros.trim_end_matches('0');
  let exponent = written_exponent
    .add(&Exponent::from_usize(without_leading_zeros.len() - trimmed.len()))
    .add(&Exponent::from_usize(fraction.len()).negated());
  Some(Decimal {
    negative,
    digits: trimmed.to_string(),
    exponent,
  })
}

/// JSON の数値が表す整数を返す。JSON Schema の整数と同じく、`1.0`・`1e3`・`10e-1` のように小数部が 0 の
/// 書き方も整数として読む。
///
/// 数値でない値、小数部が 0 でない値（`1.5`・`1e-1`）、`i128` に収まらない値は `None` を返す。`i128` の
/// 範囲は下限（`i128::MIN`）と上限（`i128::MAX`）を含む。値が 0 なら、指数がどれほど大きくても 0 を返す。
/// 元の桁の文字列から 10 進で読むので、浮動小数点を経由した精度の損失はない。
pub fn to_integer(value: &Value) -> Option<i128> {
  let Value::Number(number) = value else {
    return None;
  };
  let decimal = normalize_decimal(&number.to_string())?;
  if decimal.digits.is_empty() {
    return Some(0);
  }
  let exponent = decimal.exponent.to_u32()?;
  let magnitude = decimal
    .digits
    .parse::<u128>()
    .ok()?
    .checked_mul(10u128.checked_pow(exponent)?)?;
  if decimal.negative {
    // `i128::MIN` の大きさ（2^127）は正の `i128` に収まらないので、大きさを先に `i128` へ変換せず、0 から引く。
    0i128.checked_sub_unsigned(magnitude)
  } else {
    i128::try_from(magnitude).ok()
  }
}
