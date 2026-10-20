//! 検査を通った旧データを、新ファイルへ書く。書き込みは呼び出し元のトランザクションの中で行い、COMMIT しない。
//!
//! payload・manifest・発生時刻は直列化し直さず、旧ファイルの値をそのまま移す。

use std::collections::HashMap;

use rusqlite::{params, Connection, Transaction};

use super::inspect::Aggregates;
use super::legacy;
use crate::migration::{MigrationError, MigrationReport};
use crate::seq_nr::SeqNr;

/// 書き込んだ行の数。COMMIT に成功するまでは報告に入れない。
pub(super) struct Counts {
  pub aggregates: u64,
  pub events: u64,
  pub snapshots: u64,
}

/// 旧ファイルを読み直しながら、新ファイルの `journal`・`head`・`snapshot_current`・`snapshot_history` へ書く。
///
/// 旧ファイルは検査のときと同じ読み取りトランザクションの中にあるので、検査した内容をそのまま読む。
/// `journal` を全件書いてから、集約ごとに最大の seq_nr の封筒から `head` を作る。
pub(super) fn write(
  old: &Connection,
  new: &Transaction<'_>,
  mapping: &HashMap<String, String>,
  aggregates: &Aggregates,
  report: &MigrationReport,
) -> Result<Counts, MigrationError> {
  let events = write_events(old, new, mapping, report)?;
  let heads = write_heads(new, aggregates, report)?;
  let snapshots = write_snapshots(old, new, mapping, aggregates, report)?;
  Ok(Counts {
    aggregates: heads,
    events,
    snapshots,
  })
}

fn write_events(
  old: &Connection,
  new: &Transaction<'_>,
  mapping: &HashMap<String, String>,
  report: &MigrationReport,
) -> Result<u64, MigrationError> {
  let read = "旧journalの再読み取り";
  let write = "journalの書き込み";
  let mut statement = old
    .prepare(legacy::EVENTS_SQL)
    .map_err(|error| MigrationError::new(report, read, error))?;
  let mut rows = statement
    .query([])
    .map_err(|error| MigrationError::new(report, read, error))?;
  let mut insert = new
    .prepare("INSERT INTO journal (aid, seq_nr, occurred_at_ns, manifest, payload) VALUES (?1, ?2, ?3, ?4, ?5)")
    .map_err(|error| MigrationError::new(report, write, error))?;
  let mut count = 0;
  while let Some(row) = rows.next().map_err(|error| MigrationError::new(report, read, error))? {
    let event = legacy::event(row, mapping)
      .map_err(|error| MigrationError::new(report, read, error))?
      .map_err(|rejected| invalid(report, read, rejected.reason))?;
    insert
      .execute(params![
        event.key.aid.as_str(),
        sql_seq_nr(report, write, event.seq_nr)?,
        event.occurred_at_ns,
        event.manifest,
        event.payload
      ])
      .map_err(|error| MigrationError::new(report, write, error))?;
    count += 1;
  }
  Ok(count)
}

fn write_heads(
  new: &Transaction<'_>,
  aggregates: &Aggregates,
  report: &MigrationReport,
) -> Result<u64, MigrationError> {
  let write = "headの書き込み";
  // head は、書き込み済みの journal の最大の seq_nr の行から作る。
  let mut insert = new
    .prepare(
      "INSERT INTO head (aid, type_name, seq_nr, event_occurred_at_ns, event_manifest, event_payload)
       SELECT aid, ?2, seq_nr, occurred_at_ns, manifest, payload FROM journal WHERE aid = ?1 AND seq_nr = ?3",
    )
    .map_err(|error| MigrationError::new(report, write, error))?;
  let mut count = 0;
  for (aid, summary) in aggregates {
    let Some((type_name, _)) = aid.as_str().split_once('-') else {
      return Err(invalid(
        report,
        write,
        format!("aid {} に型名の区切りがありません", aid.as_str()),
      ));
    };
    let changed = insert
      .execute(params![
        aid.as_str(),
        type_name,
        sql_seq_nr(report, write, summary.maximum)?
      ])
      .map_err(|error| MigrationError::new(report, write, error))?;
    if changed != 1 {
      return Err(invalid(
        report,
        write,
        format!(
          "aid {} の最大番号 {} の journal が 1 件ではありません",
          aid.as_str(),
          summary.maximum
        ),
      ));
    }
    count += 1;
  }
  Ok(count)
}

fn write_snapshots(
  old: &Connection,
  new: &Transaction<'_>,
  mapping: &HashMap<String, String>,
  aggregates: &Aggregates,
  report: &MigrationReport,
) -> Result<u64, MigrationError> {
  let read = "旧snapshotの再読み取り";
  let write = "snapshotの書き込み";
  let mut statement = old
    .prepare(legacy::SNAPSHOTS_SQL)
    .map_err(|error| MigrationError::new(report, read, error))?;
  let mut rows = statement
    .query([])
    .map_err(|error| MigrationError::new(report, read, error))?;
  // 旧スナップショットに manifest の列はないので、空文字列にする。旧 `version` は移さない。
  let mut insert_current = new
    .prepare("INSERT INTO snapshot_current (aid, seq_nr, manifest, payload) VALUES (?1, ?2, '', ?3)")
    .map_err(|error| MigrationError::new(report, write, error))?;
  let mut insert_history = new
    .prepare("INSERT INTO snapshot_history (aid, seq_nr, manifest, payload) VALUES (?1, ?2, '', ?3)")
    .map_err(|error| MigrationError::new(report, write, error))?;
  let mut count = 0;
  while let Some(row) = rows.next().map_err(|error| MigrationError::new(report, read, error))? {
    let snapshot = legacy::snapshot(row, mapping)
      .map_err(|error| MigrationError::new(report, read, error))?
      .map_err(|rejected| invalid(report, read, rejected.reason))?;
    let aid = &snapshot.key.aid;
    let is_current = snapshot.key.key_seq_nr == 0;
    if !is_current {
      let Some(summary) = aggregates.get(aid) else {
        return Err(invalid(
          report,
          read,
          format!("aid {} の検査結果がありません", aid.as_str()),
        ));
      };
      if !summary.retained_history.contains(&snapshot.seq_nr) {
        continue;
      }
    }
    let insert = if is_current {
      &mut insert_current
    } else {
      &mut insert_history
    };
    insert
      .execute(params![
        aid.as_str(),
        sql_seq_nr(report, write, snapshot.seq_nr)?,
        snapshot.payload
      ])
      .map_err(|error| MigrationError::new(report, write, error))?;
    count += 1;
  }
  Ok(count)
}

fn sql_seq_nr(report: &MigrationReport, operation: &str, seq_nr: SeqNr) -> Result<i64, MigrationError> {
  i64::try_from(seq_nr).map_err(|_| invalid(report, operation, format!("seq_nr {seq_nr} がINTEGERに収まりません")))
}

fn invalid(report: &MigrationReport, operation: &str, reason: String) -> MigrationError {
  MigrationError::new(
    report,
    operation,
    std::io::Error::new(std::io::ErrorKind::InvalidData, reason),
  )
}
