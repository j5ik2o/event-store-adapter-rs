use std::collections::HashMap;

use aws_sdk_dynamodb::config::AsyncSleep;
use aws_sdk_dynamodb::operation::batch_get_item::BatchGetItemOutput;
use aws_sdk_dynamodb::types::{AttributeValue, KeysAndAttributes};

use super::{DynamoDbTables, EventStoreForDynamoDB};
use crate::aggregate_id::{AggregateId, AidString};
use crate::error::{ConfigurationReason, EventStoreError, StorageOperation};
use crate::event_envelope::{SnapshotEnvelope, SnapshotRead};
use crate::seq_nr::{SeqNr, SEQ_NR_MAX};
use crate::serializer::SnapshotSerializer;

type Item = HashMap<String, AttributeValue>;
type RequestItems = HashMap<String, KeysAndAttributes>;

impl<AID: AggregateId, A: Send + Sync + 'static, P: Send + Sync + 'static> EventStoreForDynamoDB<AID, A, P> {
  /// headと現在のsnapshotを強整合で読み、独立した番号と復元した封筒を返す（DY-9・R-1〜R-3・R-8）。
  pub async fn get_latest_snapshot_by_id(
    &self,
    aggregate_id: &AID,
  ) -> Result<Option<SnapshotRead<A>>, EventStoreError> {
    let aid = AidString::from_aggregate_id(aggregate_id)?;
    let sleeper = self
      .client
      .config()
      .sleep_impl()
      .ok_or(EventStoreError::Configuration {
        reason: ConfigurationReason::MissingRetrySleeper,
      })?;
    let mut pending = initial_request(&self.tables, &aid)?;
    let mut accumulated = HashMap::new();
    let mut retries = 0;
    let mut delay = self
      .options
      .unprocessed_retry_initial_delay
      .min(self.options.unprocessed_retry_max_delay);
    loop {
      let output = self
        .client
        .batch_get_item()
        .set_request_items(Some(pending.clone()))
        .send()
        .await
        .map_err(storage)?;
      pending = accumulate(&mut accumulated, &pending, output)?;
      if pending.is_empty() {
        return restore_read(&self.tables, &accumulated, self.snapshot_serializer.as_ref());
      }
      if retries == self.options.unprocessed_retry_limit {
        return Err(storage(std::io::Error::other(
          "snapshot unprocessed retry limit reached",
        )));
      }
      sleeper.sleep(delay).await;
      retries += 1;
      delay = delay.saturating_mul(2).min(self.options.unprocessed_retry_max_delay);
    }
  }
}

fn storage(source: impl std::error::Error + Send + Sync + 'static) -> EventStoreError {
  EventStoreError::Storage {
    operation: StorageOperation::LoadSnapshot,
    source: Box::new(source),
  }
}

fn invalid_data(message: &'static str) -> EventStoreError {
  storage(std::io::Error::new(std::io::ErrorKind::InvalidData, message))
}

fn initial_request(tables: &DynamoDbTables, aid: &AidString) -> Result<RequestItems, EventStoreError> {
  [(&tables.head_table_name, false), (&tables.snapshot_table_name, true)]
    .into_iter()
    .map(|(table, current)| {
      let mut key = Item::from([("aid".into(), AttributeValue::S(aid.as_str().into()))]);
      if current {
        key.insert("skey".into(), AttributeValue::N("0".into()));
      }
      let attributes = KeysAndAttributes::builder()
        .keys(key)
        .consistent_read(true)
        .build()
        .map_err(storage)?;
      Ok((table.clone(), attributes))
    })
    .collect()
}

fn matches_key(item: &Item, key: &Item) -> Result<bool, EventStoreError> {
  for (name, expected) in key {
    let actual = item
      .get(name)
      .ok_or_else(|| invalid_data("snapshot read key is missing"))?;
    let matches = match (actual, expected) {
      (AttributeValue::S(actual), AttributeValue::S(expected)) => actual == expected,
      (AttributeValue::N(actual), AttributeValue::N(expected)) => {
        actual.parse::<SeqNr>().map_err(storage)? == expected.parse::<SeqNr>().map_err(storage)?
      }
      _ => return Err(invalid_data("snapshot read key has the wrong type")),
    };
    if !matches {
      return Ok(false);
    }
  }
  Ok(true)
}

fn accumulate(
  accumulated: &mut HashMap<String, Item>,
  requested: &RequestItems,
  output: BatchGetItemOutput,
) -> Result<RequestItems, EventStoreError> {
  for (table, items) in output.responses.unwrap_or_default() {
    let request = requested
      .get(&table)
      .ok_or_else(|| invalid_data("unrequested snapshot read table"))?;
    for item in items {
      if !matches_key(&item, &request.keys()[0])? {
        return Err(invalid_data("unrequested snapshot read key"));
      }
      if accumulated.insert(table.clone(), item).is_some() {
        return Err(invalid_data("duplicate snapshot read response"));
      }
    }
  }
  let mut pending = RequestItems::new();
  for (table, attributes) in output.unprocessed_keys.unwrap_or_default() {
    if attributes.keys().is_empty() {
      continue;
    }
    let request = requested
      .get(&table)
      .ok_or_else(|| invalid_data("unrequested snapshot unprocessed table"))?;
    if attributes.keys().len() != 1
      || attributes.keys()[0].len() != request.keys()[0].len()
      || !matches_key(&attributes.keys()[0], &request.keys()[0])?
      || accumulated.contains_key(&table)
    {
      return Err(invalid_data("invalid snapshot unprocessed key"));
    }
    pending.insert(
      table,
      KeysAndAttributes::builder()
        .set_keys(Some(attributes.keys))
        .consistent_read(true)
        .build()
        .map_err(storage)?,
    );
  }
  Ok(pending)
}

fn stored_seq_nr(item: &Item) -> Result<SeqNr, EventStoreError> {
  let seq_nr = match item.get("seq_nr") {
    Some(AttributeValue::N(number)) => number.parse::<SeqNr>().map_err(storage)?,
    _ => return Err(invalid_data("snapshot read seq_nr is missing or not N")),
  };
  if seq_nr > SEQ_NR_MAX {
    return Err(invalid_data("snapshot read seq_nr is out of range"));
  }
  Ok(seq_nr)
}

fn restore_read<A: 'static>(
  tables: &DynamoDbTables,
  items: &HashMap<String, Item>,
  serializer: &dyn SnapshotSerializer<A>,
) -> Result<Option<SnapshotRead<A>>, EventStoreError> {
  let Some(head) = items.get(&tables.head_table_name) else {
    return Ok(None);
  };
  let head_seq_nr = stored_seq_nr(head)?;
  let snapshot = items
    .get(&tables.snapshot_table_name)
    .map(|item| {
      let seq_nr = stored_seq_nr(item)?;
      let manifest = match item.get("manifest") {
        Some(AttributeValue::S(value)) => value,
        _ => return Err(invalid_data("snapshot manifest is missing or not S")),
      };
      let aggregate = match item.get("payload") {
        Some(AttributeValue::B(value)) => serializer.deserialize(value.as_ref())?,
        _ => return Err(invalid_data("snapshot payload is missing or not B")),
      };
      Ok(SnapshotEnvelope::new(aggregate, seq_nr).with_manifest(manifest.clone()))
    })
    .transpose()?;
  Ok(Some(SnapshotRead::new(snapshot, head_seq_nr)))
}

#[cfg(test)]
#[path = "read_snapshot_test.rs"]
mod tests;
