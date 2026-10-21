//! v3.0.6 の Bigtable の既定の配置（`DefaultKeyResolver`）の行キーとセルを読み解く。
//!
//! - パーティションキー: `{型名}-{シャード番号}`。シャード番号は 10 進で、先頭のゼロを持たない。
//! - journal の行キー: `{パーティションキー}#{型名}#{値}#{seq_nr を 20 桁にゼロ詰め}`。列ファミリ `event` に
//!   `payload`・`aggregate_id`（値だけ）・`seq_nr`（10 進）・`occurred_at`（RFC3339、ナノ秒）・`manifest`。
//! - snapshot の今の行キー: `{パーティションキー}#{型名}#{値}`。列ファミリ `snapshot` に `payload`・`version`・`seq_nr`・
//!   `last_updated_at`。移すのは `payload` と `seq_nr` だけで、`version` と `last_updated_at` は読まない。
//! - snapshot の履歴の行キー: 今の行キーの後ろに `#{seq_nr を 20 桁にゼロ詰め}`。置き換えられる前の像をその位置で書いたもの。
//!
//! 値に `#` を含むと、行キーの切り方が複数ありうる。ここでは構文上の候補を全て挙げるだけで、候補を選ぶのは検査である。
//! 推測では選ばず、旧 journal で確かめた集約にちょうど 1 つの候補が当たるときだけ受ける。

use std::collections::HashMap;

use chrono::DateTime;

use crate::aggregate_id::AidString;
use crate::bigtable::envelope::EventEnvelopeFields;
use crate::bigtable::rows::AssembledRow;
use crate::migration::legacy_key;
use crate::seq_nr::{SeqNr, SEQ_NR_MAX};

/// v3 の journal の列ファミリ。
const EVENT_FAMILY: &str = "event";
/// v3 の snapshot の列ファミリ。
const SNAPSHOT_FAMILY: &str = "snapshot";

/// 旧の行キーが指す集約。新しい aid ではなく、旧の配置での識別である。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct LegacyKey {
  /// 旧パーティションキー（`{型名}-{シャード番号}`）。
  pub pkey: String,
  /// 旧の型名。`-` を含みうる。
  pub old_type: String,
  pub value: String,
}

impl LegacyKey {
  /// snapshot のテーブルでの、今のスナップショットの行キー。
  pub(super) fn snapshot_row_key(&self) -> Vec<u8> {
    format!("{}#{}#{}", self.pkey, self.old_type, self.value).into_bytes()
  }

  /// snapshot のテーブルでの、位置 `seq_nr` の履歴の行キー。
  pub(super) fn history_row_key(&self, seq_nr: SeqNr) -> Vec<u8> {
    format!("{}#{}#{}#{seq_nr:020}", self.pkey, self.old_type, self.value).into_bytes()
  }
}

/// snapshot の行キーの読み方。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SnapshotKind {
  /// 今のスナップショットの行。
  Current,
  /// 位置 `SeqNr` の履歴の行。
  History(SeqNr),
}

/// snapshot の行キーの構文上の候補 1 つ。
#[derive(Debug, Clone)]
pub(super) struct SnapshotCandidate {
  pub key: LegacyKey,
  pub kind: SnapshotKind,
}

/// 読み解いた journal の行。
pub(super) struct LegacyEvent {
  pub key: LegacyKey,
  pub aid: AidString,
  pub fields: EventEnvelopeFields,
}

/// 拒否する行の、報告に入れる情報。
#[derive(Debug)]
pub(super) struct Rejected {
  pub pkey: Option<String>,
  pub aid: Option<String>,
  pub reason: String,
}

fn rejected(reason: impl Into<String>) -> Rejected {
  Rejected {
    pkey: None,
    aid: None,
    reason: reason.into(),
  }
}

/// 10 進の既定の表記（先頭のゼロなし、符号なし）の非負整数だけを受ける。
fn canonical_number(text: &str) -> Option<u64> {
  let number: u64 = text.parse().ok()?;
  (number.to_string() == text).then_some(number)
}

/// 行キーを `{パーティションキー}#{型名}#{残り}` に切った 1 通り。
struct Split<'a> {
  pkey: &'a str,
  old_type: &'a str,
  rest: &'a str,
}

/// 行キーの各 `-` を型名の終わりと見て、`{型名}-{シャード番号}#{型名}#` の形になる切り方を全て返す。
///
/// 型名は `-` を含みうるので、切り方は複数ありうる。シャード番号は既定の 10 進表記でなければならない。
fn splits(key: &str) -> Vec<Split<'_>> {
  key
    .match_indices('-')
    .filter_map(|(index, _)| {
      let old_type = &key[..index];
      let (shard, tail) = key[index + 1..].split_once('#')?;
      canonical_number(shard)?;
      let rest = tail.strip_prefix(old_type)?.strip_prefix('#')?;
      Some(Split {
        pkey: &key[..index + 1 + shard.len()],
        old_type,
        rest,
      })
    })
    .collect()
}

/// 末尾が `#` と 20 桁の ASCII 数字なら、その 21 文字を除いた値と、20 桁の数を返す。
fn split_number(rest: &str) -> Option<(&str, u64)> {
  let boundary = rest.len().checked_sub(21)?;
  if !rest.is_char_boundary(boundary) {
    return None;
  }
  let (value, tail) = rest.split_at(boundary);
  let digits = tail.strip_prefix('#')?;
  if digits.len() != 20 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
    return None;
  }
  Some((value, digits.parse().ok()?))
}

fn cell<'a>(row: &'a AssembledRow, family: &str, qualifier: &str) -> Result<&'a [u8], String> {
  row
    .value(family, qualifier)
    .ok_or_else(|| format!("{family}:{qualifier}のセルがありません"))
}

/// 旧 journal の行を読み解く。
///
/// 行キーが既定の配置として一意に読み解けない行、5 つのセルが揃わない行、キーとセルが食い違う行は拒否する。
/// `aggregate_id` のセルは、行キーから取り出した値との照合にだけ使う。
pub(super) fn event(row: &AssembledRow, mapping: &HashMap<String, String>) -> Result<LegacyEvent, Rejected> {
  let key = std::str::from_utf8(&row.row_key).map_err(|_| rejected("P-22: 行キーがUTF-8ではありません"))?;
  let mut candidates: Vec<(Split<'_>, &str, u64)> = splits(key)
    .into_iter()
    .filter_map(|split| split_number(split.rest).map(|(value, number)| (split, value, number)))
    .collect();
  if candidates.len() > 1 {
    return Err(rejected("P-22: 行キーを一意に読み解けません"));
  }
  let Some((split, value, number)) = candidates.pop() else {
    return Err(rejected("P-22: 既定の配置のjournalの行キーではありません"));
  };
  let with_pkey = |reason: String| Rejected {
    pkey: Some(split.pkey.to_string()),
    aid: None,
    reason,
  };
  if number < 1 {
    return Err(with_pkey("W-6: journalのseq_nrは1以上が必要です".into()));
  }
  if number > SEQ_NR_MAX {
    return Err(with_pkey("T-9: seq_nrが2^53-1を超えています".into()));
  }
  let payload = cell(row, EVENT_FAMILY, "payload").map_err(&with_pkey)?;
  let aggregate_id = cell(row, EVENT_FAMILY, "aggregate_id").map_err(&with_pkey)?;
  let seq_nr = cell(row, EVENT_FAMILY, "seq_nr").map_err(&with_pkey)?;
  let occurred_at = cell(row, EVENT_FAMILY, "occurred_at").map_err(&with_pkey)?;
  let manifest = cell(row, EVENT_FAMILY, "manifest").map_err(&with_pkey)?;
  if aggregate_id != value.as_bytes() {
    return Err(with_pkey("aggregate_idのセルが行キーの値と一致しません".into()));
  }
  if std::str::from_utf8(seq_nr).ok().and_then(canonical_number) != Some(number) {
    return Err(with_pkey("P-22: キー番号とjournalのseq_nrが一致しません".into()));
  }
  let occurred_at = std::str::from_utf8(occurred_at)
    .ok()
    .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
    .ok_or_else(|| with_pkey("T-13: occurred_atをRFC3339として読めません".into()))?;
  let occurred_at_ns = occurred_at
    .timestamp_nanos_opt()
    .ok_or_else(|| with_pkey("T-13: occurred_atが符号付き64bitナノ秒の範囲外です".into()))?;
  let manifest = String::from_utf8(manifest.to_vec()).map_err(|_| with_pkey("manifestがUTF-8ではありません".into()))?;
  let aid = legacy_key::aid(split.old_type, value, mapping).map_err(with_pkey)?;
  Ok(LegacyEvent {
    key: LegacyKey {
      pkey: split.pkey.to_string(),
      old_type: split.old_type.to_string(),
      value: value.to_string(),
    },
    fields: EventEnvelopeFields {
      aid: aid.as_str().to_string(),
      seq_nr: number,
      occurred_at_ns,
      manifest,
      payload: payload.to_vec(),
    },
    aid,
  })
}

/// snapshot の行キーの構文上の候補を全て返す。
///
/// どの切り方でも今の行として読める。末尾が `#` と 20 桁の数なら、履歴の行としても読める。
/// UTF-8 でない行キーと、既定の配置として全く読めない行キーは、理由を返す。
pub(super) fn snapshot_candidates(row_key: &[u8]) -> Result<Vec<SnapshotCandidate>, String> {
  let key = std::str::from_utf8(row_key).map_err(|_| "P-22: 行キーがUTF-8ではありません".to_string())?;
  let mut candidates = vec![];
  for split in splits(key) {
    let key_of = |value: &str| LegacyKey {
      pkey: split.pkey.to_string(),
      old_type: split.old_type.to_string(),
      value: value.to_string(),
    };
    candidates.push(SnapshotCandidate {
      key: key_of(split.rest),
      kind: SnapshotKind::Current,
    });
    if let Some((value, number)) = split_number(split.rest) {
      candidates.push(SnapshotCandidate {
        key: key_of(value),
        kind: SnapshotKind::History(number),
      });
    }
  }
  if candidates.is_empty() {
    return Err("P-22: 既定の配置のsnapshotの行キーではありません".into());
  }
  Ok(candidates)
}

/// 旧 snapshot の行から移すセル。`version` と `last_updated_at` は移さないので読まない。
pub(super) struct SnapshotCells<'a> {
  /// このスナップショットの位置。
  pub seq_nr: SeqNr,
  pub payload: &'a [u8],
}

pub(super) fn snapshot_cells(row: &AssembledRow) -> Result<SnapshotCells<'_>, String> {
  let payload = cell(row, SNAPSHOT_FAMILY, "payload")?;
  let seq_nr = cell(row, SNAPSHOT_FAMILY, "seq_nr")?;
  let seq_nr = std::str::from_utf8(seq_nr)
    .ok()
    .and_then(canonical_number)
    .ok_or("P-22: snapshotのseq_nrが既定の整数表記ではありません")?;
  Ok(SnapshotCells { seq_nr, payload })
}
