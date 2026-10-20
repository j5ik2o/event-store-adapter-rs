//! v3 の SQLite の旧ファイル（`journal`・`snapshot`）を読み取り専用で開き、1 行ずつ読み解く。
//!
//! 型名と値は旧 `DefaultKeyResolver` の配置から復元する。`aid` 列（v3 が `Display` で作った文字列）、
//! `version`、`last_updated_at` は使わない。

use std::collections::HashMap;
use std::path::Path;

use rusqlite::types::Value;
use rusqlite::{Connection, OpenFlags, Row};

use crate::aggregate_id::AidString;
use crate::migration::{legacy_key, MigrationRejection};
use crate::seq_nr::{SeqNr, SEQ_NR_MAX};
use crate::sqlite::SqliteOptions;

/// 旧 `journal` を読む SQL。`aid` 列は TEXT であることだけを確かめる。
pub(super) const EVENTS_SQL: &str = "SELECT pkey, skey, aid, seq_nr, payload, occurred_at, manifest FROM journal";

/// 旧 `snapshot` を読む SQL。`version` と `last_updated_at` は移さないので読まない。
pub(super) const SNAPSHOTS_SQL: &str = "SELECT pkey, skey, aid, seq_nr, payload FROM snapshot";

/// 旧ファイルを読み取り専用で開く。ファイルを作らず、WAL への切り替えのような書き込みもしない。
///
/// `path` には正規化した絶対パスを渡す。`file:` で始まる名前が URI として解釈されることを避けるためである。
pub(super) fn open(path: &Path) -> rusqlite::Result<Connection> {
  let connection = Connection::open_with_flags(
    path,
    OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX | OpenFlags::SQLITE_OPEN_PRIVATE_CACHE,
  )?;
  connection.busy_timeout(SqliteOptions::default().lock_wait)?;
  Ok(connection)
}

/// 既定の配置のキーから復元した値。
pub(super) struct LegacyKey {
  pub aid: AidString,
  pub pkey: String,
  pub skey: String,
  pub key_seq_nr: SeqNr,
}

impl LegacyKey {
  /// このキーの行を拒否する。
  pub fn reject(&self, reason: String) -> Rejected {
    Rejected {
      pkey: Some(self.pkey.clone()),
      skey: Some(self.skey.clone()),
      aid: Some(self.aid.as_str().into()),
      reason,
    }
  }
}

pub(super) struct LegacyEvent {
  pub key: LegacyKey,
  pub seq_nr: SeqNr,
  pub occurred_at_ns: i64,
  pub manifest: String,
  pub payload: Vec<u8>,
}

pub(super) struct LegacySnapshot {
  pub key: LegacyKey,
  pub seq_nr: SeqNr,
  pub payload: Vec<u8>,
}

/// 読み解けなかった行と、その理由。
pub(super) struct Rejected {
  pub pkey: Option<String>,
  pub skey: Option<String>,
  pub aid: Option<String>,
  pub reason: String,
}

impl Rejected {
  pub fn into_rejection(self, table: &str) -> MigrationRejection {
    MigrationRejection {
      table: table.into(),
      pkey: self.pkey,
      skey: self.skey,
      aid: self.aid,
      reason: self.reason,
    }
  }
}

/// `row` の先頭 `N` 列を、型を保ったまま取り出す。
fn columns<const N: usize>(row: &Row<'_>) -> rusqlite::Result<[Value; N]> {
  let mut values: [Value; N] = std::array::from_fn(|_| Value::Null);
  for (index, slot) in values.iter_mut().enumerate() {
    *slot = row.get(index)?;
  }
  Ok(values)
}

/// `journal` の 1 行を読み解く。行の読み出しの失敗は `Err`、行の内容の誤りは `Ok(Err(_))` で返す。
pub(super) fn event(
  row: &Row<'_>,
  mapping: &HashMap<String, String>,
) -> rusqlite::Result<Result<LegacyEvent, Rejected>> {
  Ok(parse_event(columns::<7>(row)?, mapping))
}

/// `snapshot` の 1 行を読み解く。
pub(super) fn snapshot(
  row: &Row<'_>,
  mapping: &HashMap<String, String>,
) -> rusqlite::Result<Result<LegacySnapshot, Rejected>> {
  Ok(parse_snapshot(columns::<5>(row)?, mapping))
}

fn parse_event(
  [pkey, skey, aid, seq_nr, payload, occurred_at, manifest]: [Value; 7],
  mapping: &HashMap<String, String>,
) -> Result<LegacyEvent, Rejected> {
  let key = key(pkey, skey, aid, mapping)?;
  let occurred_at_ns = integer(occurred_at, "occurred_at").map_err(|reason| key.reject(reason))?;
  let manifest = text(manifest, "manifest").map_err(|reason| key.reject(reason))?;
  let seq_nr = seq_nr_value(seq_nr).map_err(|reason| key.reject(reason))?;
  let payload = blob(payload, "payload").map_err(|reason| key.reject(reason))?;
  if seq_nr == 0 {
    return Err(key.reject("W-6: journalのseq_nrは1以上が必要です".into()));
  }
  if key.key_seq_nr != seq_nr {
    return Err(key.reject("P-22: キー番号とjournalのseq_nrが一致しません".into()));
  }
  Ok(LegacyEvent {
    key,
    seq_nr,
    occurred_at_ns,
    manifest,
    payload,
  })
}

fn parse_snapshot(
  [pkey, skey, aid, seq_nr, payload]: [Value; 5],
  mapping: &HashMap<String, String>,
) -> Result<LegacySnapshot, Rejected> {
  let key = key(pkey, skey, aid, mapping)?;
  let seq_nr = seq_nr_value(seq_nr).map_err(|reason| key.reject(reason))?;
  let payload = blob(payload, "payload").map_err(|reason| key.reject(reason))?;
  if key.key_seq_nr != 0 && key.key_seq_nr != seq_nr {
    return Err(key.reject("P-22: 履歴キー番号とsnapshotのseq_nrが一致しません".into()));
  }
  Ok(LegacySnapshot { key, seq_nr, payload })
}

/// `pkey` と `skey` から既定の配置のキーを復元する。`aid` 列は TEXT であることだけを確かめ、値は使わない。
fn key(pkey: Value, skey: Value, aid: Value, mapping: &HashMap<String, String>) -> Result<LegacyKey, Rejected> {
  let aid_column = match &aid {
    Value::Text(text) => Some(text.clone()),
    _ => None,
  };
  let (Value::Text(pkey), Value::Text(skey)) = (pkey, skey) else {
    return Err(Rejected {
      pkey: None,
      skey: None,
      aid: aid_column,
      reason: "pkeyとskeyがTEXTではありません".into(),
    });
  };
  let rejected = |aid: Option<String>, reason: String| Rejected {
    pkey: Some(pkey.clone()),
    skey: Some(skey.clone()),
    aid,
    reason,
  };
  let parsed = legacy_key::parse(&pkey, &skey, mapping).map_err(|reason| rejected(aid_column.clone(), reason))?;
  if aid_column.is_none() {
    return Err(rejected(
      Some(parsed.aid.as_str().into()),
      "aidがTEXTではありません".into(),
    ));
  }
  Ok(LegacyKey {
    aid: parsed.aid,
    pkey,
    skey,
    key_seq_nr: parsed.key_seq_nr,
  })
}

fn seq_nr_value(value: Value) -> Result<SeqNr, String> {
  let number = integer(value, "seq_nr")?;
  let seq_nr = SeqNr::try_from(number).map_err(|_| "T-9: seq_nrが非負整数ではありません".to_string())?;
  if seq_nr > SEQ_NR_MAX {
    return Err("T-9: seq_nrが2^53-1を超えています".into());
  }
  Ok(seq_nr)
}

fn integer(value: Value, name: &str) -> Result<i64, String> {
  match value {
    Value::Integer(number) => Ok(number),
    _ => Err(format!("{name}がINTEGERではありません")),
  }
}

fn text(value: Value, name: &str) -> Result<String, String> {
  match value {
    Value::Text(text) => Ok(text),
    _ => Err(format!("{name}がTEXTではありません")),
  }
}

fn blob(value: Value, name: &str) -> Result<Vec<u8>, String> {
  match value {
    Value::Blob(bytes) => Ok(bytes),
    _ => Err(format!("{name}がBLOBではありません")),
  }
}
