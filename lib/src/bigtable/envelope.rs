//! 封筒の固定バイナリ形式（4 章・BT-D6a・ADR-0008）。
//!
//! 先頭の `0x01` は形式の版番号である。整数はビッグエンディアンで、長さはバイト数である。
//!
//! ```text
//! イベント = 0x01 ‖ U16(aid の長さ) ‖ aid の UTF-8 ‖ U64(seq_nr)
//!           ‖ I64(occurred_at_ns) ‖ U32(manifest の長さ) ‖ manifest の UTF-8
//!           ‖ U32(payload の長さ) ‖ payload
//! スナップショット = 0x01 ‖ U64(seq_nr)
//!                 ‖ U32(manifest の長さ) ‖ manifest の UTF-8
//!                 ‖ U32(payload の長さ) ‖ payload
//! ```
//!
//! 未使用の末尾・長さの不一致・未知の形式の版は保存先エラーにする。payload の復元はここでは行わない。

use crate::error::{EventStoreError, StorageOperation};
use crate::seq_nr::{SeqNr, SEQ_NR_MAX};

/// 形式の版番号。
const FORMAT_VERSION: u8 = 0x01;
/// イベント封筒の固定部分（版・aid の長さ・seq_nr・occurred_at_ns・manifest の長さ・payload の長さ）のバイト数。
const EVENT_FIXED_BYTES: usize = 1 + 2 + 8 + 8 + 4 + 4;
/// スナップショット封筒の固定部分（版・seq_nr・manifest の長さ・payload の長さ）のバイト数。
const SNAPSHOT_FIXED_BYTES: usize = 1 + 8 + 4 + 4;

/// イベント封筒の項目。
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventEnvelopeFields {
  pub aid: String,
  pub seq_nr: u64,
  pub occurred_at_ns: i64,
  pub manifest: String,
  pub payload: Vec<u8>,
}

/// スナップショット封筒の項目。
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotEnvelopeFields {
  pub seq_nr: u64,
  pub manifest: String,
  pub payload: Vec<u8>,
}

/// 直列化した後のイベント封筒のバイト数。封筒の上限（BT-16）の検査に使う。
pub(crate) fn event_envelope_len(fields: &EventEnvelopeFields) -> usize {
  EVENT_FIXED_BYTES
    .saturating_add(fields.aid.len())
    .saturating_add(fields.manifest.len())
    .saturating_add(fields.payload.len())
}

/// 直列化した後のスナップショット封筒のバイト数。
pub(crate) fn snapshot_envelope_len(fields: &SnapshotEnvelopeFields) -> usize {
  SNAPSHOT_FIXED_BYTES
    .saturating_add(fields.manifest.len())
    .saturating_add(fields.payload.len())
}

// 長さの欄に収まらない値は、欄の最大値に丸める。丸めた封筒は、復元のときに長さの不一致として拒否される。
// 封筒の上限（最大 8MiB）の検査を先に行うので、この丸めは実際には起こらない。
fn length_u32(length: usize) -> u32 {
  u32::try_from(length).unwrap_or(u32::MAX)
}

fn length_u16(length: usize) -> u16 {
  u16::try_from(length).unwrap_or(u16::MAX)
}

#[doc(hidden)]
pub fn encode_event(fields: &EventEnvelopeFields) -> Vec<u8> {
  let mut bytes = Vec::with_capacity(event_envelope_len(fields));
  bytes.push(FORMAT_VERSION);
  bytes.extend_from_slice(&length_u16(fields.aid.len()).to_be_bytes());
  bytes.extend_from_slice(fields.aid.as_bytes());
  bytes.extend_from_slice(&fields.seq_nr.to_be_bytes());
  bytes.extend_from_slice(&fields.occurred_at_ns.to_be_bytes());
  bytes.extend_from_slice(&length_u32(fields.manifest.len()).to_be_bytes());
  bytes.extend_from_slice(fields.manifest.as_bytes());
  bytes.extend_from_slice(&length_u32(fields.payload.len()).to_be_bytes());
  bytes.extend_from_slice(&fields.payload);
  bytes
}

#[doc(hidden)]
pub fn encode_snapshot(fields: &SnapshotEnvelopeFields) -> Vec<u8> {
  let mut bytes = Vec::with_capacity(snapshot_envelope_len(fields));
  bytes.push(FORMAT_VERSION);
  bytes.extend_from_slice(&fields.seq_nr.to_be_bytes());
  bytes.extend_from_slice(&length_u32(fields.manifest.len()).to_be_bytes());
  bytes.extend_from_slice(fields.manifest.as_bytes());
  bytes.extend_from_slice(&length_u32(fields.payload.len()).to_be_bytes());
  bytes.extend_from_slice(&fields.payload);
  bytes
}

fn invalid(operation: StorageOperation, message: &'static str) -> EventStoreError {
  EventStoreError::Storage {
    operation,
    source: Box::new(std::io::Error::other(message)),
  }
}

/// 封筒のバイト列を前から順に読む。範囲を超える読み取りは保存先エラーにする。
struct Reader<'a> {
  remaining: &'a [u8],
  operation: StorageOperation,
}

impl<'a> Reader<'a> {
  fn new(bytes: &'a [u8], operation: StorageOperation) -> Self {
    Self {
      remaining: bytes,
      operation,
    }
  }

  fn take(&mut self, length: usize) -> Result<&'a [u8], EventStoreError> {
    if self.remaining.len() < length {
      return Err(invalid(
        self.operation,
        "the envelope ends before a declared field ends",
      ));
    }
    let (head, tail) = self.remaining.split_at(length);
    self.remaining = tail;
    Ok(head)
  }

  fn array<const N: usize>(&mut self) -> Result<[u8; N], EventStoreError> {
    let operation = self.operation;
    self
      .take(N)?
      .try_into()
      .map_err(|_| invalid(operation, "the envelope ends before a declared field ends"))
  }

  fn version(&mut self) -> Result<(), EventStoreError> {
    let [version] = self.array::<1>()?;
    if version != FORMAT_VERSION {
      return Err(invalid(self.operation, "the envelope has an unknown format version"));
    }
    Ok(())
  }

  fn utf8(&mut self, length: usize) -> Result<String, EventStoreError> {
    let operation = self.operation;
    String::from_utf8(self.take(length)?.to_vec()).map_err(|_| invalid(operation, "the envelope has invalid UTF-8"))
  }

  fn length_u32(&mut self) -> Result<usize, EventStoreError> {
    let operation = self.operation;
    usize::try_from(u32::from_be_bytes(self.array()?))
      .map_err(|_| invalid(operation, "the envelope declares an unsupported length"))
  }

  fn finish(self) -> Result<(), EventStoreError> {
    if self.remaining.is_empty() {
      Ok(())
    } else {
      Err(invalid(self.operation, "the envelope has unused trailing bytes"))
    }
  }
}

/// イベント封筒を復元する。seq_nr が共通契約の範囲（1〜2^53−1）にないときも保存先エラーにする。
#[doc(hidden)]
pub fn decode_event(bytes: &[u8], operation: StorageOperation) -> Result<EventEnvelopeFields, EventStoreError> {
  let mut reader = Reader::new(bytes, operation);
  reader.version()?;
  let aid_length = usize::from(u16::from_be_bytes(reader.array()?));
  let aid = reader.utf8(aid_length)?;
  let seq_nr: SeqNr = u64::from_be_bytes(reader.array()?);
  if seq_nr == 0 || seq_nr > SEQ_NR_MAX {
    return Err(invalid(operation, "the envelope has a seq_nr out of range"));
  }
  let occurred_at_ns = i64::from_be_bytes(reader.array()?);
  let manifest_length = reader.length_u32()?;
  let manifest = reader.utf8(manifest_length)?;
  let payload_length = reader.length_u32()?;
  let payload = reader.take(payload_length)?.to_vec();
  reader.finish()?;
  Ok(EventEnvelopeFields {
    aid,
    seq_nr,
    occurred_at_ns,
    manifest,
    payload,
  })
}

/// スナップショット封筒を復元する。seq_nr が共通契約の範囲（1〜2^53−1）にないときも保存先エラーにする。
#[doc(hidden)]
pub fn decode_snapshot(bytes: &[u8], operation: StorageOperation) -> Result<SnapshotEnvelopeFields, EventStoreError> {
  let mut reader = Reader::new(bytes, operation);
  reader.version()?;
  let seq_nr: SeqNr = u64::from_be_bytes(reader.array()?);
  if seq_nr == 0 || seq_nr > SEQ_NR_MAX {
    return Err(invalid(operation, "the envelope has a seq_nr out of range"));
  }
  let manifest_length = reader.length_u32()?;
  let manifest = reader.utf8(manifest_length)?;
  let payload_length = reader.length_u32()?;
  let payload = reader.take(payload_length)?.to_vec();
  reader.finish()?;
  Ok(SnapshotEnvelopeFields {
    seq_nr,
    manifest,
    payload,
  })
}

#[cfg(test)]
#[path = "envelope_test.rs"]
mod tests;
