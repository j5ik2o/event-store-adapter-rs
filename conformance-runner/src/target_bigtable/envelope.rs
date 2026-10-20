//! Bigtable の封筒の値の表（BT-D6a）。符号化と復元は、ライブラリの `test-hooks` の経路で実行する。
//!
//! 期待値の符号化は実行器の中に持たず、ライブラリが返すバイト列を、データの小文字の 16 進と比べる。
//! 符号化の値の表では、続けて期待値のバイト列を復元し、入力の項目と一致することも確かめる。
//! 形の崩れた封筒の復元は、保存先エラーになることを確かめる。復元に成功した封筒は、データの誤りとして失敗にする。

use event_store_adapter_rs::{
  bigtable::{
    decode_bigtable_event_envelope_for_test, decode_bigtable_snapshot_envelope_for_test,
    encode_bigtable_event_envelope_for_test, encode_bigtable_snapshot_envelope_for_test, EventEnvelopeFields,
    SnapshotEnvelopeFields,
  },
  error::{EventStoreError, StorageOperation},
};
use serde_json::{json, Value};

use crate::{
  case::{compare_result, failed, required, seq, string, ComparisonFailure},
  report::{CaseOutcome, ObservedValues},
};

#[cfg(test)]
#[path = "envelope_test.rs"]
mod tests;

const ENCODE_EVENT: &str = "encodeBigtableEventEnvelope";
const ENCODE_SNAPSHOT: &str = "encodeBigtableSnapshotEnvelope";
const DECODE: &str = "decodeBigtableEnvelope";

/// 値の表の `operation` が、封筒の符号化か復元なら真。
pub(super) fn is_envelope_operation(body: &Value) -> bool {
  matches!(
    body.get("operation").and_then(Value::as_str),
    Some(ENCODE_EVENT | ENCODE_SNAPSHOT | DECODE)
  )
}

/// 小文字の 16 進の文字列をバイト列にする。大文字・奇数桁・16 進でない文字は誤りにする。
fn from_hex(text: &str) -> Result<Vec<u8>, String> {
  if !text.len().is_multiple_of(2) {
    return Err(format!("16 進の桁数が奇数: {text}"));
  }
  let digit = |byte: u8| -> Result<u8, String> {
    match byte {
      b'0'..=b'9' => Ok(byte - b'0'),
      b'a'..=b'f' => Ok(byte - b'a' + 10),
      _ => Err(format!("小文字の 16 進ではない文字がある: {text}")),
    }
  };
  text
    .as_bytes()
    .chunks(2)
    .map(|pair| Ok(digit(pair[0])? << 4 | digit(pair[1])?))
    .collect()
}

fn to_hex(bytes: &[u8]) -> String {
  bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn text_of(input: &Value, key: &str) -> Result<String, String> {
  Ok(string(required(input, key)?)?.to_owned())
}

fn payload_of(input: &Value) -> Result<Vec<u8>, String> {
  from_hex(string(required(input, "payload_hex")?)?)
}

fn event_fields(input: &Value) -> Result<EventEnvelopeFields, String> {
  let occurred_at_ns = string(required(input, "occurred_at_ns")?)?
    .parse::<i64>()
    .map_err(|error| format!("occurred_at_ns が 64bit の符号付き整数ではない: {error}"))?;
  Ok(EventEnvelopeFields {
    aid: text_of(input, "aid")?,
    seq_nr: seq(required(input, "seq_nr")?)?,
    occurred_at_ns,
    manifest: text_of(input, "manifest")?,
    payload: payload_of(input)?,
  })
}

fn snapshot_fields(input: &Value) -> Result<SnapshotEnvelopeFields, String> {
  Ok(SnapshotEnvelopeFields {
    seq_nr: seq(required(input, "seq_nr")?)?,
    manifest: text_of(input, "manifest")?,
    payload: payload_of(input)?,
  })
}

/// 符号化の値の表を実行する。ライブラリが返したバイト列を `expect` の 16 進と比べ、一致したら、
/// 続けて期待値のバイト列を復元し、入力の項目と一致することも確かめる（`conformance/README.md`）。
///
/// 一致しない、復元できない、復元した項目が入力と食い違う場合は、どれもそのケースの失敗にする。
fn run_encode<Fields: PartialEq + std::fmt::Debug>(
  body: &Value,
  fields_of: impl Fn(&Value) -> Result<Fields, String>,
  encode: impl Fn(&Fields) -> Vec<u8>,
  decode: impl Fn(&[u8]) -> Result<Fields, EventStoreError>,
) -> CaseOutcome {
  let (input, expect) = match (required(body, "input"), required(body, "expect")) {
    (Ok(input), Ok(expect)) => (input, expect),
    (Err(detail), _) | (_, Err(detail)) => return failed(None, detail),
  };
  let fields = match fields_of(input) {
    Ok(fields) => fields,
    Err(detail) => return failed(None, detail),
  };
  let actual = json!({"value": to_hex(&encode(&fields))});
  if let Err(failure) = compare_result(expect, Ok(actual.clone())) {
    return failed(None, failure);
  }
  let expected_bytes = match required(expect, "value").and_then(string).and_then(from_hex) {
    Ok(bytes) => bytes,
    Err(detail) => return failed(None, format!("期待値のバイト列を読めない: {detail}")),
  };
  match decode(&expected_bytes) {
    Err(error) => return failed(None, format!("期待値のバイト列を復元できない: {error}")),
    Ok(restored) if restored != fields => {
      return failed(
        None,
        format!("期待値のバイト列を復元した項目が入力の項目と一致しない: 入力={fields:?} 復元={restored:?}"),
      )
    }
    Ok(_) => {}
  }
  CaseOutcome::Passed {
    values: Some(ObservedValues {
      expected: expect.clone(),
      actual,
    }),
  }
}

/// 復元の値の表を実行する。復元の失敗を `expect` と比べ、復元に成功した封筒はデータの誤りにする。
fn run_decode(body: &Value) -> CaseOutcome {
  let (input, expect) = match (required(body, "input"), required(body, "expect")) {
    (Ok(input), Ok(expect)) => (input, expect),
    (Err(detail), _) | (_, Err(detail)) => return failed(None, detail),
  };
  let bytes = match required(input, "bytes_hex").and_then(string).and_then(from_hex) {
    Ok(bytes) => bytes,
    Err(detail) => return failed(None, format!("bytes_hex を読めない: {detail}")),
  };
  let decoded = match required(input, "kind").and_then(string) {
    Ok("event") => decode_bigtable_event_envelope_for_test(&bytes, StorageOperation::LoadEvents).map(|_| ()),
    Ok("snapshot") => decode_bigtable_snapshot_envelope_for_test(&bytes, StorageOperation::LoadSnapshot).map(|_| ()),
    Ok(other) => return failed(None, format!("未知の封筒の種類: {other}")),
    Err(detail) => return failed(None, detail),
  };
  match decoded {
    Ok(()) => failed(
      None,
      ComparisonFailure {
        detail: "復元に成功した封筒を、復元の失敗を期待するデータで使っている".to_owned(),
        expected: Some(expect.clone()),
        actual: Some(json!({"result": "decoded"})),
      },
    ),
    Err(error) => match compare_result(expect, Err(error)) {
      Ok(_) => CaseOutcome::Passed { values: None },
      Err(failure) => failed(None, failure),
    },
  }
}

/// 封筒の値の表（[`is_envelope_operation`] が真のもの）を実行する。
pub(super) fn run(body: &Value) -> CaseOutcome {
  match body.get("operation").and_then(Value::as_str) {
    Some(ENCODE_EVENT) => run_encode(body, event_fields, encode_bigtable_event_envelope_for_test, |bytes| {
      decode_bigtable_event_envelope_for_test(bytes, StorageOperation::LoadEvents)
    }),
    Some(ENCODE_SNAPSHOT) => run_encode(
      body,
      snapshot_fields,
      encode_bigtable_snapshot_envelope_for_test,
      |bytes| decode_bigtable_snapshot_envelope_for_test(bytes, StorageOperation::LoadSnapshot),
    ),
    Some(DECODE) => run_decode(body),
    other => failed(None, format!("封筒の値の表の操作ではない: {other:?}")),
  }
}
