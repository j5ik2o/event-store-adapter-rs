use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client;

use super::inspect::{reject, Aggregates};
use super::legacy;
use super::scan::Scan;
use super::{LegacyDynamoDbTables, MigrationError, MigrationReport};
use crate::dynamodb::items::Item;
use crate::dynamodb::DynamoDbTables;

pub(super) async fn write(
  client: &Client,
  legacy_tables: &LegacyDynamoDbTables,
  tables: &DynamoDbTables,
  mapping: &HashMap<String, String>,
  aggregates: Aggregates,
  report: &mut MigrationReport,
) -> Result<(), MigrationError> {
  let mut scan = Scan::new(client, &legacy_tables.journal_table_name);
  while let Some(page) = scan.next(report).await? {
    for item in page {
      let event = legacy::event(&item, mapping).map_err(|reason| invalid(report, "journal再Scan", reason))?;
      if !put(
        client,
        &tables.journal_table_name,
        event.stored.journal(&event.key.aid),
        report,
      )
      .await?
      {
        return Ok(());
      }
      report.events += 1;
    }
  }
  for (aid, summary) in aggregates {
    let response = client
      .get_item()
      .table_name(&tables.journal_table_name)
      .key("aid", AttributeValue::S(aid.as_str().into()))
      .key("seq_nr", AttributeValue::N(summary.maximum.to_string()))
      .consistent_read(true)
      .send()
      .await
      .map_err(|error| MigrationError::new(report, "head用journal GetItem", error))?;
    let item = response.item.ok_or_else(|| {
      invalid(
        report,
        "head用journal GetItem",
        "検査最大番号の新journalがありません".into(),
      )
    })?;
    let event = legacy::stored_event(&item).map_err(|reason| invalid(report, "head用journal GetItem", reason))?;
    if item.get("aid") != Some(&AttributeValue::S(aid.as_str().into())) || event.seq_nr != summary.maximum {
      return Err(invalid(
        report,
        "head用journal GetItem",
        "journalのaid/番号が要求と一致しません".into(),
      ));
    }
    if !put(client, &tables.head_table_name, event.head(&aid), report).await? {
      return Ok(());
    }
    report.aggregates += 1;
  }
  let mut scan = Scan::new(client, &legacy_tables.snapshot_table_name);
  while let Some(page) = scan.next(report).await? {
    for item in page {
      let snapshot = legacy::snapshot(&item, mapping).map_err(|reason| invalid(report, "snapshot再Scan", reason))?;
      if !put(client, &tables.snapshot_table_name, snapshot.item(), report).await? {
        return Ok(());
      }
      report.snapshots += 1;
    }
  }
  Ok(())
}

async fn put(client: &Client, table: &str, item: Item, report: &mut MigrationReport) -> Result<bool, MigrationError> {
  match client
    .put_item()
    .table_name(table)
    .set_item(Some(item.clone()))
    .condition_expression("attribute_not_exists(aid)")
    .send()
    .await
  {
    Ok(_) => Ok(true),
    Err(error)
      if error
        .as_service_error()
        .is_some_and(|error| error.is_conditional_check_failed_exception()) =>
    {
      reject(
        report,
        table,
        &item,
        None,
        "条件競合: attribute_not_exists(aid)が不成立です".into(),
      );
      Ok(false)
    }
    Err(error) => Err(MigrationError::new(report, format!("PutItem {table}"), error)),
  }
}

fn invalid(report: &MigrationReport, operation: &str, reason: String) -> MigrationError {
  MigrationError::new(
    report,
    operation,
    std::io::Error::new(std::io::ErrorKind::InvalidData, reason),
  )
}

#[cfg(test)]
#[path = "write_test.rs"]
mod tests;
