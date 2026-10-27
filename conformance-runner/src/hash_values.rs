//! FNV-1a 64 の値の表（K-1）。行キーの接頭辞に使うライブラリの関数を、保存先ごとに受け取って実行する。
//!
//! ハッシュの計算は実行器の中に持たず、保存先（Bigtable・Spanner）がライブラリの `test-hooks` の経路から渡す関数が
//! 返す値を、データの固定 16 桁の 16 進数（`0x` 付き）と符号なし 64bit として比べる。
//! 形の崩れたデータ（`0x` がない、16 桁でない、16 進でない文字がある、入力がない）は、そのケースの失敗にする。

use serde_json::{json, Value};

use crate::{
  case::{failed, required, string, ComparisonFailure},
  report::{CaseOutcome, ObservedValues},
};

#[cfg(test)]
#[path = "hash_values_test.rs"]
mod tests;

const FNV1A64: &str = "fnv1a64";

/// 値の表の `operation` が、FNV-1a 64 なら真。
pub(crate) fn is_hash_operation(body: &Value) -> bool {
  body.get("operation").and_then(Value::as_str) == Some(FNV1A64)
}

/// `0x` と 16 桁の 16 進数字からなる文字列を、符号なし 64bit にする。それ以外は誤りにする。
///
/// 桁数と文字を先に確かめる。`u64::from_str_radix` は先頭の `+` を受け付けるので、確かめずに渡すと、
/// 固定 16 桁の形でない値を読んでしまう。
fn expected_value(text: &str) -> Result<u64, String> {
  let digits = text
    .strip_prefix("0x")
    .ok_or_else(|| format!("0x で始まらない: {text}"))?;
  if digits.len() != 16 || !digits.bytes().all(|byte| byte.is_ascii_hexdigit()) {
    return Err(format!("0x に続く 16 桁の 16 進数字ではない: {text}"));
  }
  u64::from_str_radix(digits, 16).map_err(|error| format!("{text}: {error}"))
}

/// 値の表 `body` を、渡された `hash`（保存先が行キーに使うライブラリの関数）で実行する。
pub(crate) fn run_with(body: &Value, hash: impl Fn(&[u8]) -> u64) -> CaseOutcome {
  let (input, expect) = match (required(body, "input"), required(body, "expect")) {
    (Ok(input), Ok(expect)) => (input, expect),
    (Err(detail), _) | (_, Err(detail)) => return failed(None, detail),
  };
  let utf8 = match required(input, "utf8").and_then(string) {
    Ok(utf8) => utf8,
    Err(detail) => return failed(None, format!("input.utf8 を読めない: {detail}")),
  };
  let expected = match required(expect, "value").and_then(string).and_then(expected_value) {
    Ok(expected) => expected,
    Err(detail) => return failed(None, format!("expect.value を読めない: {detail}")),
  };
  let actual_hash = hash(utf8.as_bytes());
  let actual = json!({"value": format!("0x{actual_hash:016x}")});
  if actual_hash != expected {
    return failed(
      None,
      ComparisonFailure {
        detail: "期待値と実結果が一致しない".to_owned(),
        expected: Some(expect.clone()),
        actual: Some(actual),
      },
    );
  }
  CaseOutcome::Passed {
    values: Some(ObservedValues {
      expected: expect.clone(),
      actual,
    }),
  }
}
