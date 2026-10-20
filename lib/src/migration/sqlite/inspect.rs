//! 旧ファイルを全件検査する。拒否の理由はすべて `report.reasons` に集め、新ファイルには何も書かない。

use std::collections::{BTreeSet, HashMap};

use rusqlite::Connection;

use super::legacy;
use crate::aggregate_id::AidString;
use crate::migration::{MigrationError, MigrationRejection, MigrationReport};
use crate::retention::select_expired_history_after_append;
use crate::seq_nr::SeqNr;

const JOURNAL_TABLE: &str = "old.journal";
const SNAPSHOT_TABLE: &str = "old.snapshot";

/// 検査を通った集約について、書き込みが使う値。
pub(super) struct Summary {
  /// 集約の最大の seq_nr。この封筒から新しい head を作る。
  pub maximum: SeqNr,
  /// 新ファイルへ移す保持の履歴の seq_nr。
  pub retained_history: BTreeSet<SeqNr>,
}

pub(super) type Aggregates = HashMap<AidString, Summary>;

/// 検査の途中の集約。
struct Draft {
  pkey: String,
  maximum: SeqNr,
  seq_nrs: BTreeSet<SeqNr>,
  history: Vec<SeqNr>,
}

/// 旧 `journal` と `snapshot` を全件検査する。
///
/// `keep` は新ファイルの保持件数。`Some(n)` なら、集約ごとに seq_nr の新しい `n` 件の履歴を移す。
/// `None`（今のスナップショットだけを保持する設定）なら履歴を移さない。旧の履歴はどちらの場合も検査する。
pub(super) fn inspect(
  old: &Connection,
  mapping: &HashMap<String, String>,
  keep: Option<usize>,
  report: &mut MigrationReport,
) -> Result<Aggregates, MigrationError> {
  let mut drafts: HashMap<AidString, Draft> = HashMap::new();
  inspect_journal(old, mapping, &mut drafts, report)?;
  for (aid, draft) in &drafts {
    if !draft.seq_nrs.iter().copied().eq(1..=draft.maximum) {
      report.reasons.push(MigrationRejection {
        table: JOURNAL_TABLE.into(),
        pkey: Some(draft.pkey.clone()),
        skey: None,
        aid: Some(aid.as_str().into()),
        reason: format!(
          "P-36: 番号1..{}が連続していません（件数{}）",
          draft.maximum,
          draft.seq_nrs.len()
        ),
      });
    }
  }
  inspect_snapshots(old, mapping, &mut drafts, report)?;
  Ok(
    drafts
      .into_iter()
      .map(|(aid, draft)| {
        let summary = Summary {
          maximum: draft.maximum,
          retained_history: retained_history(draft.history, keep),
        };
        (aid, summary)
      })
      .collect(),
  )
}

fn inspect_journal(
  old: &Connection,
  mapping: &HashMap<String, String>,
  drafts: &mut HashMap<AidString, Draft>,
  report: &mut MigrationReport,
) -> Result<(), MigrationError> {
  let operation = "旧journalの読み取り";
  let mut statement = old
    .prepare(legacy::EVENTS_SQL)
    .map_err(|error| MigrationError::new(report, operation, error))?;
  let mut rows = statement
    .query([])
    .map_err(|error| MigrationError::new(report, operation, error))?;
  while let Some(row) = rows
    .next()
    .map_err(|error| MigrationError::new(report, operation, error))?
  {
    let parsed = legacy::event(row, mapping).map_err(|error| MigrationError::new(report, operation, error))?;
    let event = match parsed {
      Ok(event) => event,
      Err(rejected) => {
        report.reasons.push(rejected.into_rejection(JOURNAL_TABLE));
        continue;
      }
    };
    let draft = drafts.entry(event.key.aid.clone()).or_insert_with(|| Draft {
      pkey: event.key.pkey.clone(),
      maximum: 0,
      seq_nrs: BTreeSet::new(),
      history: Vec::new(),
    });
    // 同じ aid へ複数の旧パーティションが写ると、番号の連続を 1 つの集約として証明できない。
    if draft.pkey != event.key.pkey {
      let reason = "P-22: 複数の旧パーティションが同じaidへ対応しています";
      report
        .reasons
        .push(event.key.reject(reason.into()).into_rejection(JOURNAL_TABLE));
      continue;
    }
    if !draft.seq_nrs.insert(event.seq_nr) {
      let reason = format!("P-36: seq_nr {}が重複しています", event.seq_nr);
      report
        .reasons
        .push(event.key.reject(reason).into_rejection(JOURNAL_TABLE));
      continue;
    }
    draft.maximum = draft.maximum.max(event.seq_nr);
  }
  Ok(())
}

fn inspect_snapshots(
  old: &Connection,
  mapping: &HashMap<String, String>,
  drafts: &mut HashMap<AidString, Draft>,
  report: &mut MigrationReport,
) -> Result<(), MigrationError> {
  let operation = "旧snapshotの読み取り";
  let mut statement = old
    .prepare(legacy::SNAPSHOTS_SQL)
    .map_err(|error| MigrationError::new(report, operation, error))?;
  let mut rows = statement
    .query([])
    .map_err(|error| MigrationError::new(report, operation, error))?;
  while let Some(row) = rows
    .next()
    .map_err(|error| MigrationError::new(report, operation, error))?
  {
    let parsed = legacy::snapshot(row, mapping).map_err(|error| MigrationError::new(report, operation, error))?;
    let snapshot = match parsed {
      Ok(snapshot) => snapshot,
      Err(rejected) => {
        report.reasons.push(rejected.into_rejection(SNAPSHOT_TABLE));
        continue;
      }
    };
    let reject = |report: &mut MigrationReport, reason: &str| {
      report
        .reasons
        .push(snapshot.key.reject(reason.into()).into_rejection(SNAPSHOT_TABLE));
    };
    let Some(draft) = drafts.get_mut(&snapshot.key.aid) else {
      reject(report, "孤立snapshot: journalに集約がありません");
      continue;
    };
    if draft.pkey != snapshot.key.pkey {
      reject(report, "P-22: snapshotとjournalの旧パーティションが一致しません");
    } else if snapshot.seq_nr > draft.maximum {
      reject(report, "未来snapshot: seq_nrがjournal最大番号を超えています");
    } else if !draft.seq_nrs.contains(&snapshot.seq_nr) {
      reject(report, "snapshotに対応するイベントがありません");
    } else if snapshot.key.key_seq_nr != 0 {
      draft.history.push(snapshot.seq_nr);
    }
  }
  Ok(())
}

/// 新ファイルの保持件数に従って、移す履歴を選ぶ（プロファイル 6 章・S-2: 新しい `keep` 件を残す）。
fn retained_history(history: Vec<SeqNr>, keep: Option<usize>) -> BTreeSet<SeqNr> {
  match keep {
    None => BTreeSet::new(),
    Some(keep) => {
      let expired = select_expired_history_after_append(&history, None, keep);
      history.into_iter().filter(|seq_nr| !expired.contains(seq_nr)).collect()
    }
  }
}
