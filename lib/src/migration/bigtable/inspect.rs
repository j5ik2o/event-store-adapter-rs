//! 旧 2 テーブルを全件検査する。拒否の理由は全て `report.reasons` に集め、新しい 2 テーブルには何も書かない。
//!
//! 検査は旧 journal、旧 snapshot の順に 1 回ずつ読む。記憶に置くのは、集約ごとの seq_nr の集合とスナップショットの位置、
//! 封筒の大きさだけで、payload は持たない。書き込みは、検査で得た要約を使って旧テーブルを読み直す。

use std::collections::{BTreeMap, BTreeSet, HashMap};

use super::legacy::{self, LegacyKey, Rejected, SnapshotCandidate, SnapshotKind};
use super::read::Pages;
use crate::aggregate_id::AidString;
use crate::bigtable::envelope::{event_envelope_len, snapshot_envelope_len, SnapshotEnvelopeFields};
use crate::bigtable::rows::{AssembledRow, TableTarget};
use crate::error::StorageOperation;
use crate::migration::legacy_key;
use crate::migration::{MigrationError, MigrationRejection, MigrationReport};
use crate::retention::select_expired_history_after_append;
use crate::seq_nr::SeqNr;

/// 旧テーブルの読み取りで、誤りに付ける保存先の操作。
const OPERATION: StorageOperation = StorageOperation::LoadEvents;

/// 検査を通った集約について、書き込みが使う値。
pub(super) struct Summary {
  /// 旧の配置での識別。書き込みで旧テーブルを読み直すときに、同じ集約の行かを照合する。
  pub key: LegacyKey,
  /// 集約の最大の seq_nr。この封筒から新しい head を作る。
  pub maximum: SeqNr,
  /// 今のスナップショットの位置。
  pub current: Option<SeqNr>,
  /// 履歴の位置の集合（旧の履歴の行と今の位置を合わせたもの）。journal の行の `event:has_snapshot` が 1 になる位置である。
  pub snapshot_positions: BTreeSet<SeqNr>,
  /// 新しいストアへ移す履歴の位置。journal の行に active な履歴を書く位置である。保持件数がなければ空である。
  pub retained: BTreeSet<SeqNr>,
}

pub(super) type Aggregates = HashMap<AidString, Summary>;

/// 検査の途中の集約。
struct Draft {
  key: LegacyKey,
  /// 旧 journal にある seq_nr。
  seq_nrs: BTreeSet<SeqNr>,
  current: Option<SeqNr>,
  history: BTreeSet<SeqNr>,
  /// 位置ごとの、旧 snapshot の payload の長さ。封筒の大きさの検査に使う。
  payload_lengths: BTreeMap<SeqNr, usize>,
}

fn rejection(table: &str, row_key: &[u8], rejected: Rejected) -> MigrationRejection {
  MigrationRejection {
    table: table.to_string(),
    pkey: rejected.pkey,
    skey: Some(String::from_utf8_lossy(row_key).into_owned()),
    aid: rejected.aid,
    reason: rejected.reason,
  }
}

fn aggregate_rejection(table: &str, aid: &AidString, reason: String) -> MigrationRejection {
  MigrationRejection {
    table: table.to_string(),
    pkey: None,
    skey: None,
    aid: Some(aid.as_str().to_string()),
    reason,
  }
}

/// 集約を aid の順に並べる。報告の順序を実行ごとに変えないため。
fn sorted<T>(map: &HashMap<AidString, T>) -> Vec<(&AidString, &T)> {
  let mut entries: Vec<_> = map.iter().collect();
  entries.sort_by(|left, right| left.0.as_str().cmp(right.0.as_str()));
  entries
}

/// 旧 journal と旧 snapshot を全件検査する。
///
/// `keep` は新しいストアの保持件数。`Some(n)` なら、集約ごとに位置の新しい `n` 件の履歴を移す。
/// `None`（今のスナップショットだけを保持する設定）なら履歴を移さない。旧の履歴の行は、どちらの場合も検査する。
pub(super) async fn inspect(
  journal: &TableTarget,
  snapshot: &TableTarget,
  mapping: &HashMap<String, String>,
  keep: Option<usize>,
  envelope_limit_bytes: u64,
  report: &mut MigrationReport,
) -> Result<Aggregates, MigrationError> {
  let mut drafts: HashMap<AidString, Draft> = HashMap::new();
  inspect_journal(journal, mapping, envelope_limit_bytes, &mut drafts, report).await?;
  inspect_snapshot(snapshot, mapping, &mut drafts, report).await?;
  Ok(summarize(snapshot, drafts, keep, envelope_limit_bytes, report))
}

async fn inspect_journal(
  journal: &TableTarget,
  mapping: &HashMap<String, String>,
  envelope_limit_bytes: u64,
  drafts: &mut HashMap<AidString, Draft>,
  report: &mut MigrationReport,
) -> Result<(), MigrationError> {
  let table = journal.table_name.as_str();
  let mut pages = Pages::new(journal, OPERATION);
  while let Some(rows) = pages
    .next()
    .await
    .map_err(|error| MigrationError::new(report, "旧journalの読取", error))?
  {
    for row in rows {
      let event = match legacy::event(&row, mapping) {
        Ok(event) => event,
        Err(rejected) => {
          report.reasons.push(rejection(table, &row.row_key, rejected));
          continue;
        }
      };
      let draft = drafts.entry(event.aid.clone()).or_insert_with(|| Draft {
        key: event.key.clone(),
        seq_nrs: BTreeSet::new(),
        current: None,
        history: BTreeSet::new(),
        payload_lengths: BTreeMap::new(),
      });
      let mut reject = |reason: String| {
        report.reasons.push(MigrationRejection {
          table: table.to_string(),
          pkey: Some(event.key.pkey.clone()),
          skey: Some(String::from_utf8_lossy(&row.row_key).into_owned()),
          aid: Some(event.aid.as_str().to_string()),
          reason,
        });
      };
      if draft.key != event.key {
        reject("P-22: 複数の旧パーティションが同じaidへ対応しています".into());
      }
      if !draft.seq_nrs.insert(event.fields.seq_nr) {
        reject(format!("P-36: seq_nr {}が重複しています", event.fields.seq_nr));
      }
      if u64::try_from(event_envelope_len(&event.fields)).is_ok_and(|length| length > envelope_limit_bytes) {
        reject("BT-16: 移す封筒が新しいストアの上限を超えています".into());
      }
    }
  }
  // 番号は 1 以上で重複しないので、1 から最大まで欠けず続くのは、件数が最大に等しいときに限る。
  for (aid, draft) in sorted(drafts) {
    let maximum = draft.seq_nrs.last().copied().unwrap_or(0);
    if u64::try_from(draft.seq_nrs.len()).ok() != Some(maximum) {
      report.reasons.push(aggregate_rejection(
        table,
        aid,
        format!(
          "P-36: 番号1..{maximum}が連続していません（件数{}）",
          draft.seq_nrs.len()
        ),
      ));
    }
  }
  Ok(())
}

/// 行キーの候補のうち、旧 journal で確かめた集約に当たるものを探す。空でない組を返す。
///
/// 当たる候補が 1 つもないときは、その理由を返す。旧パーティションだけが違う集約があれば、その食い違い。
/// どの候補も aid を作れなければ、その理由（`P-23`・`T-11`・`T-12`）。どちらでもなければ孤立である。
fn resolve(
  candidates: &[SnapshotCandidate],
  by_identity: &HashMap<LegacyKey, AidString>,
  drafts: &HashMap<AidString, Draft>,
  mapping: &HashMap<String, String>,
) -> Result<Vec<(AidString, SnapshotKind)>, String> {
  let matched: Vec<_> = candidates
    .iter()
    .filter_map(|candidate| by_identity.get(&candidate.key).map(|aid| (aid.clone(), candidate.kind)))
    .collect();
  if !matched.is_empty() {
    return Ok(matched);
  }
  let aids: Vec<_> = candidates
    .iter()
    .map(|candidate| legacy_key::aid(&candidate.key.old_type, &candidate.key.value, mapping))
    .collect();
  if aids
    .iter()
    .any(|aid| aid.as_ref().is_ok_and(|aid| drafts.contains_key(aid)))
  {
    return Err("P-22: snapshotとjournalの旧パーティションが一致しません".into());
  }
  let mut failures: Vec<String> = aids.into_iter().filter_map(Result::err).collect();
  if failures.len() == candidates.len() {
    return Err(failures.swap_remove(0));
  }
  Err("孤立snapshot: journalに集約がありません".into())
}

async fn inspect_snapshot(
  snapshot: &TableTarget,
  mapping: &HashMap<String, String>,
  drafts: &mut HashMap<AidString, Draft>,
  report: &mut MigrationReport,
) -> Result<(), MigrationError> {
  let table = snapshot.table_name.as_str();
  let by_identity: HashMap<LegacyKey, AidString> = drafts
    .iter()
    .map(|(aid, draft)| (draft.key.clone(), aid.clone()))
    .collect();
  let mut pages = Pages::new(snapshot, OPERATION);
  while let Some(rows) = pages
    .next()
    .await
    .map_err(|error| MigrationError::new(report, "旧snapshotの読取", error))?
  {
    for row in rows {
      if let Err(rejected) = accept_snapshot_row(&row, &by_identity, mapping, drafts) {
        report.reasons.push(rejection(table, &row.row_key, rejected));
      }
    }
  }
  Ok(())
}

/// snapshot の行 1 つを、当たる集約の下書きに入れる。
fn accept_snapshot_row(
  row: &AssembledRow,
  by_identity: &HashMap<LegacyKey, AidString>,
  mapping: &HashMap<String, String>,
  drafts: &mut HashMap<AidString, Draft>,
) -> Result<(), Rejected> {
  let unlocated = |reason: String| Rejected {
    pkey: None,
    aid: None,
    reason,
  };
  let candidates = legacy::snapshot_candidates(&row.row_key).map_err(unlocated)?;
  let matched = resolve(&candidates, by_identity, drafts, mapping).map_err(unlocated)?;
  let [(aid, kind)] = matched.as_slice() else {
    return Err(unlocated(
      "P-22: 行キーを一意に読み解けません（今のスナップショットの行と履歴の行の区別がつきません）".into(),
    ));
  };
  let Some(draft) = drafts.get_mut(aid) else {
    return Err(unlocated("孤立snapshot: journalに集約がありません".into()));
  };
  let located = |reason: &str| Rejected {
    pkey: Some(draft.key.pkey.clone()),
    aid: Some(aid.as_str().to_string()),
    reason: reason.to_string(),
  };
  let cells = legacy::snapshot_cells(row).map_err(|reason| located(&reason))?;
  if let SnapshotKind::History(number) = kind {
    if *number != cells.seq_nr {
      return Err(located("P-22: 履歴キー番号とsnapshotのseq_nrが一致しません"));
    }
  }
  let maximum = draft.seq_nrs.last().copied().unwrap_or(0);
  if cells.seq_nr > maximum {
    return Err(located("未来snapshot: seq_nrがjournal最大番号を超えています"));
  }
  if !draft.seq_nrs.contains(&cells.seq_nr) {
    return Err(located("snapshotに対応するイベントがありません"));
  }
  match kind {
    SnapshotKind::Current => draft.current = Some(cells.seq_nr),
    SnapshotKind::History(_) => {
      draft.history.insert(cells.seq_nr);
    }
  }
  draft.payload_lengths.insert(cells.seq_nr, cells.payload.len());
  Ok(())
}

/// 集約ごとに、移す履歴を選び、スナップショットの封筒の大きさを上限と比べる。
fn summarize(
  snapshot: &TableTarget,
  drafts: HashMap<AidString, Draft>,
  keep: Option<usize>,
  envelope_limit_bytes: u64,
  report: &mut MigrationReport,
) -> Aggregates {
  let table = snapshot.table_name.as_str();
  let mut aggregates = HashMap::new();
  let mut entries: Vec<_> = drafts.into_iter().collect();
  entries.sort_by(|left, right| left.0.as_str().cmp(right.0.as_str()));
  for (aid, draft) in entries {
    let maximum = draft.seq_nrs.last().copied().unwrap_or(0);
    // v3 の履歴の行は、置き換えられる前の像なので、位置は今のスナップショットより小さい。
    for position in &draft.history {
      if draft.current.is_none_or(|current| *position >= current) {
        report.reasons.push(aggregate_rejection(
          table,
          &aid,
          "履歴のseq_nrが今のスナップショット以上です".into(),
        ));
      }
    }
    let mut snapshot_positions = draft.history.clone();
    snapshot_positions.extend(draft.current);
    // 保持は v5 の数え方に合わせる。今のスナップショットも件数に数え、位置の新しい n 件を残す。
    let retained: BTreeSet<SeqNr> = match keep {
      Some(count) => {
        let positions: Vec<SeqNr> = snapshot_positions.iter().copied().collect();
        let expired = select_expired_history_after_append(&positions, None, count);
        snapshot_positions
          .iter()
          .copied()
          .filter(|position| !expired.contains(position))
          .collect()
      }
      None => BTreeSet::new(),
    };
    // 新しいストアへ書くスナップショットは、今のものと移す履歴だけである。
    // 今の位置は保持にも入りうるので、集合にして位置ごとに1回だけ検査する。
    let mut written_positions = retained.clone();
    written_positions.extend(draft.current);
    for position in &written_positions {
      let payload_length = draft.payload_lengths.get(position).copied().unwrap_or(0);
      let empty = SnapshotEnvelopeFields {
        seq_nr: *position,
        manifest: String::new(),
        payload: Vec::new(),
      };
      let length = snapshot_envelope_len(&empty).saturating_add(payload_length);
      if u64::try_from(length).is_ok_and(|length| length > envelope_limit_bytes) {
        report.reasons.push(aggregate_rejection(
          table,
          &aid,
          "BT-16: 移す封筒が新しいストアの上限を超えています".into(),
        ));
      }
    }
    aggregates.insert(
      aid,
      Summary {
        key: draft.key,
        maximum,
        current: draft.current,
        snapshot_positions,
        retained,
      },
    );
  }
  aggregates
}
