//! 検査を通った旧データを、新しい 2 テーブルへ書く。
//!
//! 旧 journal を行キーの順に読み直す。同じ集約の行は seq_nr の昇順に並ぶので、集約ごとに次の順になる。
//! 1. 最大の seq_nr（h）より前の行を、journal へ書く。
//! 2. h の行に着いたら、head を作る（確定点）。
//! 3. その後、h の行を journal へ書く。
//!
//! これは v5 の書き込みの順（プロファイル 5 章）と同じで、「h より前のイベントは journal にあり、h は head にある」
//! という不変条件を、集約ごとの全ての時点で保つ。別の集約の行が間に挟まることはある。
//!
//! journal の行は `BigtableShared::write_journal_row` で、head は v5 の追記と同じ形の条件付き更新で作る。
//! 封筒は `encode_event`・`encode_snapshot` で組み立て、payload は旧のバイト列のまま入れる。

use std::collections::HashMap;

use super::inspect::{Aggregates, Summary};
use super::legacy::{self, LegacyEvent, SnapshotKind};
use super::read::Pages;
use crate::aggregate_id::AidString;
use crate::bigtable::backend::{flag_of, format_head_seq_nr, BigtableShared};
use crate::bigtable::envelope::{encode_event, encode_snapshot, SnapshotEnvelopeFields};
use crate::bigtable::rows::{
  latest_cell, latest_cell_exists, set_cell, TableTarget, FAMILY_HEAD, FAMILY_SNAPSHOT, HEAD_HAS_SNAPSHOT,
  HEAD_LAST_EVENT, HEAD_SEQ_NR, SNAPSHOT_CURRENT,
};
use crate::error::StorageOperation;
use crate::migration::{MigrationError, MigrationRejection, MigrationReport};
use crate::seq_nr::SeqNr;

/// 旧テーブルの読み取りで、誤りに付ける保存先の操作。
const READ_OPERATION: StorageOperation = StorageOperation::LoadEvents;

fn invalid(report: &MigrationReport, operation: &str, reason: String) -> MigrationError {
  MigrationError::new(
    report,
    operation,
    std::io::Error::new(std::io::ErrorKind::InvalidData, reason),
  )
}

/// 位置 `position` の旧スナップショットの payload を、旧 snapshot テーブルから読む。
///
/// 検査の後に旧テーブルが変わっていて、行がない、または位置が合わないときは、誤りにする。
async fn snapshot_payload(
  legacy_snapshot: &TableTarget,
  summary: &Summary,
  position: SeqNr,
  report: &MigrationReport,
) -> Result<Vec<u8>, MigrationError> {
  let operation = "旧snapshotの再読";
  let (kind, row_key) = if summary.current == Some(position) {
    (SnapshotKind::Current, summary.key.snapshot_row_key())
  } else {
    (SnapshotKind::History(position), summary.key.history_row_key(position))
  };
  let row = legacy_snapshot
    .read_row(row_key, latest_cell(), READ_OPERATION)
    .await
    .map_err(|error| MigrationError::new(report, operation, error))?
    .ok_or_else(|| {
      invalid(
        report,
        operation,
        format!("検査の後にsnapshot（{kind:?}）の行がなくなりました"),
      )
    })?;
  let cells = legacy::snapshot_cells(&row).map_err(|reason| invalid(report, operation, reason))?;
  if cells.seq_nr != position {
    return Err(invalid(
      report,
      operation,
      format!(
        "検査の後にsnapshotのseq_nrが{position}から{}に変わりました",
        cells.seq_nr
      ),
    ));
  }
  Ok(cells.payload.to_vec())
}

fn snapshot_envelope(position: SeqNr, payload: Vec<u8>) -> Vec<u8> {
  // 旧 snapshot に manifest のセルはないので、空文字列にする。
  encode_snapshot(&SnapshotEnvelopeFields {
    seq_nr: position,
    manifest: String::new(),
    payload,
  })
}

/// 旧 journal の行が、検査した集約の行であることを確かめる。
fn verify<'a>(
  event: &LegacyEvent,
  aggregates: &'a Aggregates,
  report: &MigrationReport,
) -> Result<&'a Summary, MigrationError> {
  let operation = "旧journalの再読";
  let summary = aggregates.get(&event.aid).ok_or_else(|| {
    invalid(
      report,
      operation,
      format!("検査の後に旧journalへ集約 {} の行が増えました", event.aid.as_str()),
    )
  })?;
  if summary.key != event.key || event.fields.seq_nr > summary.maximum {
    return Err(invalid(
      report,
      operation,
      format!("検査の後に旧journalの集約 {} の行が変わりました", event.aid.as_str()),
    ));
  }
  Ok(summary)
}

/// head を作る。既に head があれば作らず、`false` を返す。
///
/// 形は v5 の追記で新しい集約の head を作るときと同じで、`head:seq_nr` がまだない側で初期化する。
/// 今のスナップショットが h より古くてもよいので、`head:has_snapshot` は今の位置が h と等しいかで決める。
async fn create_head(
  shared: &BigtableShared,
  prefix: &[u8],
  event: &[u8],
  summary: &Summary,
  current_payload: Option<Vec<u8>>,
) -> Result<bool, crate::error::EventStoreError> {
  let has_snapshot = summary.current == Some(summary.maximum);
  let mut mutations = vec![
    set_cell(FAMILY_HEAD, HEAD_SEQ_NR, &format_head_seq_nr(summary.maximum)),
    set_cell(FAMILY_HEAD, HEAD_LAST_EVENT, event),
    set_cell(FAMILY_HEAD, HEAD_HAS_SNAPSHOT, flag_of(has_snapshot)),
  ];
  if let (Some(position), Some(payload)) = (summary.current, current_payload) {
    mutations.push(set_cell(
      FAMILY_SNAPSHOT,
      SNAPSHOT_CURRENT,
      &snapshot_envelope(position, payload),
    ));
  }
  let existed = shared
    .head
    .check_and_mutate(
      prefix.to_vec(),
      latest_cell_exists(FAMILY_HEAD, HEAD_SEQ_NR),
      vec![],
      mutations,
      StorageOperation::Append,
    )
    .await?;
  Ok(!existed)
}

/// 検査を通った集約を、新しい 2 テーブルへ書く。
///
/// 件数は、成功の応答を受けたものだけを数える。head が既にあれば、理由を報告に入れて止める。
pub(super) async fn write(
  shared: &BigtableShared,
  legacy_journal: &TableTarget,
  legacy_snapshot: &TableTarget,
  mapping: &HashMap<String, String>,
  aggregates: &Aggregates,
  report: &mut MigrationReport,
) -> Result<(), MigrationError> {
  let write_journal = "journalの書込";
  let write_head = "headの書込";
  let mut pages = Pages::new(legacy_journal, READ_OPERATION);
  while let Some(rows) = pages
    .next()
    .await
    .map_err(|error| MigrationError::new(report, "旧journalの再読", error))?
  {
    for row in rows {
      let event =
        legacy::event(&row, mapping).map_err(|rejected| invalid(report, "旧journalの再読", rejected.reason))?;
      let summary = verify(&event, aggregates, report)?;
      let seq_nr = event.fields.seq_nr;
      let prefix = shared
        .prefix(&event.aid)
        .map_err(|error| MigrationError::new(report, write_journal, error))?;
      let event_bytes = encode_event(&event.fields);
      if seq_nr == summary.maximum {
        let current_payload = match summary.current {
          Some(position) => Some(snapshot_payload(legacy_snapshot, summary, position, report).await?),
          None => None,
        };
        let created = create_head(shared, &prefix, &event_bytes, summary, current_payload)
          .await
          .map_err(|error| MigrationError::new(report, write_head, error))?;
        if !created {
          report.reasons.push(head_exists(shared, &event.aid));
          return Ok(());
        }
        report.aggregates += 1;
        report.snapshots += u64::from(summary.current.is_some());
      }
      let history = if summary.retained.contains(&seq_nr) {
        let payload = snapshot_payload(legacy_snapshot, summary, seq_nr, report).await?;
        Some(snapshot_envelope(seq_nr, payload))
      } else {
        None
      };
      shared
        .write_journal_row(
          &prefix,
          seq_nr,
          &event_bytes,
          history.as_deref(),
          summary.snapshot_positions.contains(&seq_nr),
        )
        .await
        .map_err(|error| MigrationError::new(report, write_journal, error))?;
      report.events += 1;
      // 今のスナップショットは head の作成で数えたので、履歴の行だけを数える。
      if history.is_some() && summary.current != Some(seq_nr) {
        report.snapshots += 1;
      }
    }
  }
  let heads = u64::try_from(aggregates.len()).unwrap_or(u64::MAX);
  if report.aggregates != heads {
    return Err(invalid(
      report,
      "旧journalの再読",
      format!(
        "検査した集約{heads}件のうち、headを作れたのは{}件です",
        report.aggregates
      ),
    ));
  }
  Ok(())
}

fn head_exists(shared: &BigtableShared, aid: &AidString) -> MigrationRejection {
  MigrationRejection {
    table: shared.head.table_name.clone(),
    pkey: None,
    skey: None,
    aid: Some(aid.as_str().to_string()),
    reason: "条件競合: headが既にあります".into(),
  }
}
