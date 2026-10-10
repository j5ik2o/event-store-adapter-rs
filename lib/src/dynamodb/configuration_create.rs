use std::collections::HashMap;

use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::types::{AttributeValue, Put, TransactWriteItem};
use aws_sdk_dynamodb::Client;

use super::configuration::{read_configuration, ConfigurationRead};
use super::{DynamoDbOptions, DynamoDbTables};
use crate::error::{EventStoreError, StorageOperation};

fn storage(source: impl std::error::Error + Send + Sync + 'static) -> EventStoreError {
  EventStoreError::Storage {
    operation: StorageOperation::CreateConfiguration,
    source: Box::new(source),
  }
}

pub(super) async fn resolve_configuration(
  client: &Client,
  tables: &DynamoDbTables,
  options: &DynamoDbOptions,
) -> Result<String, EventStoreError> {
  match read_configuration(client, tables, options).await? {
    ConfigurationRead::Matched { store_id } => return Ok(store_id),
    ConfigurationRead::CreationRequired => {}
  }

  let store_id = uuid::Uuid::new_v4().to_string();
  let writes = [
    (&tables.journal_table_name, Some("seq_nr")),
    (&tables.snapshot_table_name, Some("skey")),
    (&tables.head_table_name, None),
  ]
  .into_iter()
  .map(|(table, sort)| {
    let mut item = HashMap::from([
      ("aid".into(), AttributeValue::S("__config__".into())),
      ("store_id".into(), AttributeValue::S(store_id.clone())),
      ("layout_version".into(), AttributeValue::N("1".into())),
    ]);
    if let Some(sort) = sort {
      item.insert(sort.into(), AttributeValue::N("0".into()));
    }
    let put = Put::builder()
      .table_name(table)
      .set_item(Some(item))
      .condition_expression("attribute_not_exists(aid)")
      .build()
      .map_err(storage)?;
    Ok(TransactWriteItem::builder().put(put).build())
  })
  .collect::<Result<Vec<_>, EventStoreError>>()?;

  match client
    .transact_write_items()
    .set_transact_items(Some(writes))
    .send()
    .await
  {
    Ok(_) => Ok(store_id),
    Err(error) => {
      let concurrent_creation = match error.as_service_error() {
        Some(TransactWriteItemsError::TransactionCanceledException(canceled)) => canceled
          .cancellation_reasons()
          .iter()
          .any(|reason| matches!(reason.code(), Some("ConditionalCheckFailed" | "TransactionConflict"))),
        _ => false,
      };
      if concurrent_creation {
        match read_configuration(client, tables, options).await? {
          ConfigurationRead::Matched { store_id } => return Ok(store_id),
          ConfigurationRead::CreationRequired => {}
        }
      }
      Err(storage(error))
    }
  }
}
