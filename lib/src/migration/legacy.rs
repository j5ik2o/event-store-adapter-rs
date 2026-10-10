use std::collections::HashMap;

use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::AttributeValue;

use crate::aggregate_id::{AggregateId, AidString};
use crate::dynamodb::items::{Item, SnapshotKind, StoredEvent, StoredSnapshot};
use crate::seq_nr::{SeqNr, SEQ_NR_MAX};

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

pub(super) struct LegacyKey {
  pub aid: AidString,
  pub pkey: String,
  pub key_seq_nr: SeqNr,
}

pub(super) struct LegacyEvent {
  pub key: LegacyKey,
  pub stored: StoredEvent,
}

pub(super) struct LegacySnapshot {
  pub key: LegacyKey,
  pub stored: StoredSnapshot,
  pub ttl: Option<String>,
}

impl LegacySnapshot {
  pub fn item(&self) -> Item {
    let kind = if self.key.key_seq_nr == 0 {
      SnapshotKind::Current
    } else {
      SnapshotKind::History { ttl: self.ttl.clone() }
    };
    self.stored.item(&self.key.aid, kind)
  }
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

fn key(item: &Item, mapping: &HashMap<String, String>) -> Result<LegacyKey, String> {
  let pkey = string(item, "pkey")?;
  let skey = string(item, "skey")?;
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
  Ok(LegacyKey {
    aid,
    pkey: pkey.into(),
    key_seq_nr,
  })
}

pub(super) fn event(item: &Item, mapping: &HashMap<String, String>) -> Result<LegacyEvent, String> {
  let key = key(item, mapping)?;
  string(item, "aid")?;
  let stored = stored_event(item)?;
  if stored.seq_nr == 0 {
    return Err("W-6: journalのseq_nrは1以上が必要です".into());
  }
  if key.key_seq_nr != stored.seq_nr {
    return Err("P-22: キー番号とjournalのseq_nrが一致しません".into());
  }
  Ok(LegacyEvent { key, stored })
}

pub(super) fn stored_event(item: &Item) -> Result<StoredEvent, String> {
  let occurred_at = number(item, "occurred_at")?;
  occurred_at
    .parse::<i64>()
    .map_err(|_| "T-13: occurred_atが符号付き64bitナノ秒ではありません")?;
  let manifest = match item.get("manifest") {
    None => String::new(),
    Some(AttributeValue::S(value)) => value.clone(),
    _ => return Err("manifestがSではありません".into()),
  };
  Ok(StoredEvent {
    seq_nr: seq_nr(item)?,
    occurred_at: occurred_at.into(),
    manifest,
    payload: binary(item, "payload")?,
  })
}

pub(super) fn snapshot(item: &Item, mapping: &HashMap<String, String>) -> Result<LegacySnapshot, String> {
  let key = key(item, mapping)?;
  string(item, "aid")?;
  let seq_nr = seq_nr(item)?;
  if key.key_seq_nr != 0 && key.key_seq_nr != seq_nr {
    return Err("P-22: 履歴キー番号とsnapshotのseq_nrが一致しません".into());
  }
  let last_updated_at = number(item, "last_updated_at")?;
  last_updated_at
    .parse::<i64>()
    .map_err(|_| "last_updated_atが符号付き64bitミリ秒ではありません")?;
  let ttl = match item.get("ttl") {
    None => None,
    Some(AttributeValue::N(value)) => {
      let ttl = value.parse::<u128>().map_err(|_| "ttlが非負整数ではありません")?;
      if ttl == 0 {
        None
      } else {
        Some(value.clone())
      }
    }
    _ => return Err("ttlがNではありません".into()),
  };
  if key.key_seq_nr == 0 && ttl.is_some() {
    return Err("current snapshotに正のttlがあります".into());
  }
  Ok(LegacySnapshot {
    key,
    ttl,
    stored: StoredSnapshot {
      seq_nr,
      manifest: String::new(),
      payload: binary(item, "payload")?,
      last_updated_at: last_updated_at.into(),
    },
  })
}

fn seq_nr(item: &Item) -> Result<SeqNr, String> {
  let number = number(item, "seq_nr")?
    .parse::<SeqNr>()
    .map_err(|_| "T-9: seq_nrが非負整数ではありません")?;
  if number > SEQ_NR_MAX {
    return Err("T-9: seq_nrが2^53-1を超えています".into());
  }
  Ok(number)
}

fn string<'a>(item: &'a Item, name: &str) -> Result<&'a str, String> {
  match item.get(name) {
    Some(AttributeValue::S(value)) => Ok(value),
    _ => Err(format!("{name}が存在しないかSではありません")),
  }
}

fn number<'a>(item: &'a Item, name: &str) -> Result<&'a str, String> {
  match item.get(name) {
    Some(AttributeValue::N(value)) => Ok(value),
    _ => Err(format!("{name}が存在しないかNではありません")),
  }
}

fn binary(item: &Item, name: &str) -> Result<Blob, String> {
  match item.get(name) {
    Some(AttributeValue::B(value)) => Ok(value.clone()),
    _ => Err(format!("{name}が存在しないかBではありません")),
  }
}

#[cfg(test)]
#[path = "legacy_test.rs"]
mod tests;
