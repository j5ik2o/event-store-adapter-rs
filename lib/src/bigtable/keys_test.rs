//! 行キーの組み立て（BT-3・BT-4・K-1）の試験。`keys.rs` の末尾から `#[cfg(test)] #[path = "keys_test.rs"] mod tests;` で結ぶ。

use super::*;
use crate::aggregate_id::{AggregateId, AidString};

#[derive(Debug, Clone)]
struct Id {
  type_name: &'static str,
  value: &'static str,
}

impl AggregateId for Id {
  fn type_name(&self) -> String {
    self.type_name.to_string()
  }

  fn value(&self) -> String {
    self.value.to_string()
  }
}

fn aid_of(type_name: &'static str, value: &'static str) -> AidString {
  AidString::from_aggregate_id(&Id { type_name, value }).expect("aid を組み立てられる")
}

fn prefix_of(aid: &AidString) -> Vec<u8> {
  aggregate_prefix(fnv1a64(aid.as_str().as_bytes()), aid).expect("行キーの接頭辞を組み立てられる")
}

// BT-4: seq_nr は 8 バイトの符号なしビッグエンディアンで、行キーのバイト列の辞書順は seq_nr の数値の順と一致する。
#[test]
fn should_journal_row_keys_sort_in_numeric_seq_nr_order_and_end_with_the_u64_big_endian_seq_nr() {
  let prefix = prefix_of(&aid_of("Order", "9"));
  let seq_nrs: [SeqNr; 4] = [256, 9, 255, 10];

  let mut keys: Vec<(SeqNr, Vec<u8>)> = seq_nrs
    .iter()
    .map(|&seq_nr| (seq_nr, journal_row_key(&prefix, seq_nr)))
    .collect();
  keys.sort_by(|left, right| left.1.cmp(&right.1));

  assert_eq!(
    keys.iter().map(|(seq_nr, _)| *seq_nr).collect::<Vec<_>>(),
    vec![9, 10, 255, 256],
    "10 進文字列の順（10・255・256・9）ではなく、数値の順に並ぶ"
  );
  for (seq_nr, key) in &keys {
    assert_eq!(key.len(), prefix.len() + 8, "{seq_nr}");
    assert_eq!(&key[..prefix.len()], prefix.as_slice(), "{seq_nr}");
    assert_eq!(&key[prefix.len()..], &seq_nr.to_be_bytes(), "{seq_nr}");
  }
  assert_eq!(&keys[0].1[prefix.len()..], &[0, 0, 0, 0, 0, 0, 0, 9]);
}

// BT-4: 範囲の境界は 10 進文字列や Display ではなく、U64BE で作る。排他的な終了の境界に、seq_nr 10 の行は入らない。
#[test]
fn should_journal_range_use_u64_big_endian_bounds_and_exclude_the_row_at_the_end_bound() {
  let prefix = prefix_of(&aid_of("Order", "9"));

  let (start, end) = journal_range(&prefix, 9, 10);

  assert_eq!(start, [prefix.as_slice(), &9u64.to_be_bytes()].concat());
  assert_eq!(end, [prefix.as_slice(), &10u64.to_be_bytes()].concat());
  let suffix_of = |key: &[u8]| key[prefix.len()..].to_vec();
  for key in [&start, &end] {
    let suffix = suffix_of(key);
    assert_eq!(suffix.len(), 8, "境界の末尾は 8 バイトだけで、10 進文字列を付けない");
    assert!(
      !suffix.contains(&b'9') && !suffix.contains(&b'1') && !suffix.contains(&b'0'),
      "ASCII の数字を含まない: {suffix:?}"
    );
  }
  let row_9 = journal_row_key(&prefix, 9);
  let row_10 = journal_row_key(&prefix, 10);
  assert!(start <= row_9 && row_9 < end, "seq_nr 9 の行は範囲に入る");
  assert!(row_10 >= end, "seq_nr 10 の行は範囲に入らない");
}

// BT-3: head の行キーは H（FNV-1a 64 の 8 バイトのビッグエンディアン）‖L16（aid のバイト長）‖A（aid の UTF-8）。
#[test]
fn should_head_row_key_be_the_hash_then_the_two_byte_length_then_the_aid_utf8() {
  let aid = aid_of("Order", "9");

  let key = prefix_of(&aid);

  assert_eq!(key.len(), 17);
  assert_eq!(
    &key[..8],
    &[0x04, 0x69, 0x43, 0x5b, 0x07, 0xad, 0x63, 0x35],
    "FNV-1a 64(\"Order-9\")"
  );
  assert_eq!(&key[8..10], &[0x00, 0x07], "aid のバイト長 7 の U16 ビッグエンディアン");
  assert_eq!(&key[10..], "Order-9".as_bytes());
}

// BT-3: 設定行のキー [0x00] と、型名も値も空の aid "-" の head の行キー（11 バイト）は一致しない。
#[test]
fn should_data_row_key_never_equal_the_configuration_row_key() {
  let aid = aid_of("", "");
  assert_eq!(aid.as_str(), "-");

  let key = prefix_of(&aid);

  assert_eq!(CONFIG_ROW_KEY, &[0x00]);
  assert_eq!(key.len(), 11);
  assert_ne!(key.as_slice(), CONFIG_ROW_KEY);
}

// K-1（hash.md）: FNV-1a 64 は aid の UTF-8 バイト列に対する符号なし 64bit で、公開された 4 つの試験値と一致する。
#[test]
fn should_fnv1a64_match_the_published_test_vectors() {
  let vectors: [(&str, u64); 4] = [
    ("a", 0xaf63dc4c8601ec8c),
    ("01H42K4ABWQ5V2XQEP3A48VE0Z", 0xadb103af5fe2a91a),
    ("01H42KBHCW1BZG504J4ZXKA2F2", 0x0aa990285e0388ca),
    ("ユーザー", 0x309f20171820053e),
  ];

  for (input, expected) in vectors {
    assert_eq!(fnv1a64(input.as_bytes()), expected, "{input}");
  }
}
