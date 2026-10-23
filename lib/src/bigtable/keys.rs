//! 行キーの組み立て（BT-3・BT-4・3 章）。
//!
//! `P(aid) = H ‖ L16(len(A)) ‖ A`。head の行キーは `P`、journal の行キーは `P ‖ U64BE(seq_nr)` である。
//! `H` は aid の UTF-8 の FNV-1a 64 を 8 バイトのビッグエンディアンにした値、`A` は aid の UTF-8 である。
//! 長さを含めるので、ハッシュが衝突しても別の aid の `P` は前方一致にならない。

use crate::aggregate_id::AidString;
use crate::error::{ContractRule, EventStoreError};
use crate::seq_nr::SeqNr;

/// 設定行のキー。データ行のキーは最小でも 11 バイトなので、この 1 バイトと完全一致しない。
pub(crate) const CONFIG_ROW_KEY: &[u8] = &[0x00];

/// FNV-1a 64（hash.md の K-1）。
pub fn fnv1a64(input: &[u8]) -> u64 {
  let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
  for byte in input {
    hash ^= u64::from(*byte);
    hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
  }
  hash
}

/// `H ‖ L16 ‖ A`。aid の長さが符号なし 16bit に収まらなければ T-12 の契約違反にする。
pub(super) fn aggregate_prefix(hash: u64, aid: &AidString) -> Result<Vec<u8>, EventStoreError> {
  let aid_bytes = aid.as_str().as_bytes();
  let length = u16::try_from(aid_bytes.len()).map_err(|_| EventStoreError::ContractViolation {
    rule: ContractRule::T12,
    seq_nr: None,
    snapshot_seq_nr: None,
  })?;
  let mut key = Vec::with_capacity(8 + 2 + aid_bytes.len());
  key.extend_from_slice(&hash.to_be_bytes());
  key.extend_from_slice(&length.to_be_bytes());
  key.extend_from_slice(aid_bytes);
  Ok(key)
}

/// `P ‖ U64BE(seq_nr)`。バイト列の辞書順は seq_nr の数値の順と一致する（BT-4）。
pub(super) fn journal_row_key(prefix: &[u8], seq_nr: SeqNr) -> Vec<u8> {
  let mut key = Vec::with_capacity(prefix.len() + 8);
  key.extend_from_slice(prefix);
  key.extend_from_slice(&seq_nr.to_be_bytes());
  key
}

/// journal の範囲 `[P ‖ U64BE(start), P ‖ U64BE(end_exclusive))` の境界を返す。終了は排他的である。
pub(super) fn journal_range(prefix: &[u8], start: SeqNr, end_exclusive: SeqNr) -> (Vec<u8>, Vec<u8>) {
  (journal_row_key(prefix, start), journal_row_key(prefix, end_exclusive))
}

/// journal の行キーから seq_nr を取り出す。`P ‖ U64BE(seq_nr)` の形でなければ `None`。
pub(super) fn journal_seq_nr(prefix: &[u8], row_key: &[u8]) -> Option<SeqNr> {
  let suffix = row_key.strip_prefix(prefix)?;
  let bytes: [u8; 8] = suffix.try_into().ok()?;
  Some(u64::from_be_bytes(bytes))
}

#[cfg(test)]
#[path = "keys_test.rs"]
mod tests;
