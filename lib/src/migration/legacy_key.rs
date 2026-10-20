//! v3 の既定の `KeyResolver` の配置（`pkey={型名}-{シャード番号}`、`skey={型名}-{値}-{seq_nr}`）から、
//! 新版の aid と旧キーの番号を復元する。保存先の種類に依存しないので、DynamoDB と SQLite の移行が共有する。

use std::collections::HashMap;

use crate::aggregate_id::{AggregateId, AidString};
use crate::seq_nr::SeqNr;

#[derive(Debug, Clone)]
pub(super) struct Id {
  type_name: String,
  value: String,
}

impl AggregateId for Id {
  fn type_name(&self) -> String {
    self.type_name.clone()
  }

  fn value(&self) -> String {
    self.value.clone()
  }
}

/// 既定の配置のキーから復元した、新版の aid と旧キー末尾の番号。
pub(super) struct DefaultLayoutKey {
  pub aid: AidString,
  pub key_seq_nr: SeqNr,
}

fn unsigned_key_number(value: &str) -> Result<u64, String> {
  let number = value
    .parse::<u64>()
    .map_err(|_| "P-22: キー末尾が非負整数ではありません".to_string())?;
  if number.to_string() != value {
    return Err("P-22: キー末尾が既定の整数表記ではありません".into());
  }
  Ok(number)
}

/// `pkey` と `skey` を既定の配置として読み解く。既定でない配置は `P-22` の理由で返す。
///
/// 型名に `-` があるときだけ `mapping` で置き換える。対応表がなければ `P-23`、置き換え後の型名に `-` が残る、
/// または aid が 1024 バイトを超えるときは、`AidString` の検査の理由（`T-11`・`T-12`）で返す。
pub(super) fn parse(pkey: &str, skey: &str, mapping: &HashMap<String, String>) -> Result<DefaultLayoutKey, String> {
  let (old_type, shard) = pkey.rsplit_once('-').ok_or("P-22: pkeyにシャード番号がありません")?;
  unsigned_key_number(shard)?;
  let (prefix, suffix) = skey.rsplit_once('-').ok_or("P-22: skeyに番号がありません")?;
  let key_seq_nr = unsigned_key_number(suffix)?;
  let value = prefix
    .strip_prefix(old_type)
    .and_then(|value| value.strip_prefix('-'))
    .ok_or("P-22: skeyの型名がpkeyと一致しません")?;
  let type_name = if old_type.contains('-') {
    mapping
      .get(old_type)
      .ok_or_else(|| format!("P-23: 型名 {old_type} の対応表がありません"))?
      .clone()
  } else {
    old_type.to_string()
  };
  let aid = AidString::from_aggregate_id(&Id {
    type_name,
    value: value.into(),
  })
  .map_err(|error| error.to_string())?;
  Ok(DefaultLayoutKey { aid, key_seq_nr })
}
