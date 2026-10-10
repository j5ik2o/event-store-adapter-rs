use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client;

use super::legacy;
use super::scan::Scan;
use super::{LegacyDynamoDbTables, MigrationError, MigrationRejection, MigrationReport};
use crate::aggregate_id::AidString;
use crate::dynamodb::item_size::{item_size_upper_bound, ITEM_SIZE_LIMIT};
use crate::dynamodb::items::Item;
use crate::dynamodb::DynamoDbTables;

pub(super) struct AggregateSummary {
  pub pkey: String,
  pub maximum: u64,
  count: u64,
  head_size: usize,
}

pub(super) type Aggregates = HashMap<AidString, AggregateSummary>;

pub(super) async fn check_empty(
  client: &Client,
  tables: &DynamoDbTables,
  report: &mut MigrationReport,
) -> Result<(), MigrationError> {
  for (table, sort_key) in [
    (&tables.journal_table_name, Some("seq_nr")),
    (&tables.snapshot_table_name, Some("skey")),
    (&tables.head_table_name, None),
  ] {
    let mut scan = Scan::new(client, table);
    while let Some(page) = scan.next(report).await? {
      for item in page {
        let is_configuration = item.get("aid") == Some(&AttributeValue::S("__config__".into()))
          && sort_key.is_none_or(|name| item.get(name) == Some(&AttributeValue::N("0".into())));
        if !is_configuration {
          reject(
            report,
            table,
            &item,
            None,
            "新テーブルに設定以外の項目があります".into(),
          );
        }
      }
    }
  }
  Ok(())
}

pub(super) async fn inspect(
  client: &Client,
  tables: &LegacyDynamoDbTables,
  mapping: &HashMap<String, String>,
  report: &mut MigrationReport,
) -> Result<Aggregates, MigrationError> {
  let mut aggregates = Aggregates::new();
  let table = &tables.journal_table_name;
  let mut scan = Scan::new(client, table);
  while let Some(page) = scan.next(report).await? {
    for item in page {
      match legacy::event(&item, mapping) {
        Ok(event) => {
          let aid = &event.key.aid;
          if item_size_upper_bound(&event.stored.journal(aid)) > ITEM_SIZE_LIMIT {
            reject(
              report,
              table,
              &item,
              Some(aid),
              "D-7: journalサイズ上界が409600を超えています".into(),
            );
          }
          let summary = aggregates.entry(aid.clone()).or_insert_with(|| AggregateSummary {
            pkey: event.key.pkey.clone(),
            maximum: 0,
            count: 0,
            head_size: 0,
          });
          // 同じ新版キーへ複数の旧パーティションが写ると、件数=最大だけでは連続性を証明できない。
          if summary.pkey != event.key.pkey {
            reject(
              report,
              table,
              &item,
              Some(aid),
              "P-22: 複数の旧パーティションが同じaidへ対応しています".into(),
            );
          }
          summary.count += 1;
          if event.stored.seq_nr > summary.maximum {
            summary.maximum = event.stored.seq_nr;
            summary.head_size = item_size_upper_bound(&event.stored.head(aid));
          }
        }
        Err(reason) => reject(report, table, &item, None, reason),
      }
    }
  }
  for (aid, summary) in &aggregates {
    for reason in summary.reasons() {
      report.reasons.push(MigrationRejection {
        table: table.clone(),
        pkey: Some(summary.pkey.clone()),
        skey: None,
        aid: Some(aid.as_str().into()),
        reason,
      });
    }
  }
  let table = &tables.snapshot_table_name;
  let mut scan = Scan::new(client, table);
  while let Some(page) = scan.next(report).await? {
    for item in page {
      match legacy::snapshot(&item, mapping) {
        Ok(snapshot) => {
          let aid = &snapshot.key.aid;
          match aggregates.get(aid) {
            None => reject(
              report,
              table,
              &item,
              Some(aid),
              "孤立snapshot: journalに集約がありません".into(),
            ),
            Some(summary) => {
              if snapshot.stored.seq_nr > summary.maximum {
                reject(
                  report,
                  table,
                  &item,
                  Some(aid),
                  "未来snapshot: seq_nrがjournal最大番号を超えています".into(),
                );
              }
              if snapshot.key.pkey != summary.pkey {
                reject(
                  report,
                  table,
                  &item,
                  Some(aid),
                  "P-22: snapshotとjournalの旧パーティションが一致しません".into(),
                );
              }
            }
          }
          if item_size_upper_bound(&snapshot.item()) > ITEM_SIZE_LIMIT {
            reject(
              report,
              table,
              &item,
              Some(aid),
              "D-7: snapshotサイズ上界が409600を超えています".into(),
            );
          }
        }
        Err(reason) => reject(report, table, &item, None, reason),
      }
    }
  }
  Ok(aggregates)
}

impl AggregateSummary {
  fn reasons(&self) -> Vec<String> {
    let mut reasons = Vec::new();
    if self.count != self.maximum {
      reasons.push(format!(
        "P-36: 番号1..{}が連続していません（件数{}）",
        self.maximum, self.count
      ));
    }
    if self.head_size > ITEM_SIZE_LIMIT {
      reasons.push("D-7: 最大イベントのheadサイズ上界が409600を超えています".into());
    }
    reasons
  }
}

pub(super) fn reject(report: &mut MigrationReport, table: &str, item: &Item, aid: Option<&AidString>, reason: String) {
  let text = |name| item.get(name).and_then(|value| value.as_s().ok()).cloned();
  report.reasons.push(MigrationRejection {
    table: table.into(),
    pkey: text("pkey"),
    skey: text("skey").or_else(|| {
      item
        .get("skey")
        .or_else(|| item.get("seq_nr"))
        .and_then(|value| value.as_n().ok())
        .cloned()
    }),
    aid: aid.map(|aid| aid.as_str().into()).or_else(|| text("aid")),
    reason,
  });
}

#[cfg(test)]
#[path = "inspect_test.rs"]
mod tests;
