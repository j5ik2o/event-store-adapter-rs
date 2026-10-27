//! キーのハッシュ（hash.md の K-1）。Bigtable と Spanner の行キーが共有する。

/// FNV-1a 64（hash.md の K-1）。入力のバイト列から 64 ビットの符号なし整数を得る。
///
/// 行キーには、この値を 8 バイトのビッグエンディアンにして使う。符号付きの整数への読み替えや、シャード数での剰余はしない。
pub fn fnv1a64(input: &[u8]) -> u64 {
  let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
  for byte in input {
    hash ^= u64::from(*byte);
    hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
  }
  hash
}
