use std::collections::HashMap;
use std::time::Duration;

use aws_sdk_dynamodb::config::{AsyncSleep, SharedAsyncSleep};
use aws_sdk_dynamodb::operation::batch_get_item::BatchGetItemOutput;
use aws_sdk_dynamodb::types::{AttributeValue, KeysAndAttributes};
use aws_sdk_dynamodb::Client;

use super::{DynamoDbOptions, DynamoDbTables};
use crate::next::error::{ConfigurationReason, EventStoreError, StorageOperation};

#[cfg(test)]
#[path = "configuration_test.rs"]
mod tests;

type Item = HashMap<String, AttributeValue>;
type RequestItems = HashMap<String, KeysAndAttributes>;

/// 設定の作成要否または照合結果を表す。ストアの生成成功ではない。
#[derive(Debug, PartialEq, Eq)]
pub enum ConfigurationRead {
  CreationRequired,
  Matched { store_id: String },
}

pub(super) enum RetryWait {
  Sdk(SharedAsyncSleep),
  #[cfg(feature = "test-hooks")]
  Hook(std::sync::Arc<dyn super::RetrySleeper>),
}

impl RetryWait {
  async fn sleep(&self, delay: Duration) {
    match self {
      Self::Sdk(sleeper) => sleeper.sleep(delay).await,
      #[cfg(feature = "test-hooks")]
      Self::Hook(sleeper) => sleeper.sleep(delay).await,
    }
  }
}

fn storage(source: impl std::error::Error + Send + Sync + 'static) -> EventStoreError {
  EventStoreError::Storage {
    operation: StorageOperation::ReadConfiguration,
    source: Box::new(source),
  }
}

fn invalid_data(message: &'static str) -> EventStoreError {
  storage(std::io::Error::new(std::io::ErrorKind::InvalidData, message))
}

fn configuration(reason: ConfigurationReason) -> EventStoreError {
  EventStoreError::Configuration { reason }
}

// N属性の数値として比較する。浮動小数点への変換や文字列表記の一致を使わない。
fn number_is(attribute: &AttributeValue, expected: u8) -> Result<bool, EventStoreError> {
  let written = attribute
    .as_n()
    .map_err(|_| invalid_data("configuration attribute is not N"))?;
  let mut parts = written.split(['e', 'E']);
  let coefficient = parts.next().expect("split always has a first element");
  let exponent = match parts.next() {
    Some(exponent) => exponent
      .parse::<i32>()
      .map_err(|_| invalid_data("invalid N exponent"))?,
    None => 0,
  };
  if parts.next().is_some() {
    return Err(invalid_data("invalid N exponent"));
  }
  let negative = coefficient.starts_with('-');
  let coefficient = coefficient.strip_prefix(['-', '+']).unwrap_or(coefficient);
  let (integer, fraction) = coefficient.split_once('.').unwrap_or((coefficient, ""));
  let digits = format!("{integer}{fraction}");
  if digits.is_empty() || !digits.bytes().all(|digit| digit.is_ascii_digit()) {
    return Err(invalid_data("invalid N coefficient"));
  }
  let significant = digits.trim_start_matches('0');
  if significant.is_empty() {
    return Ok(expected == 0);
  }
  Ok(
    !negative
      && expected == 1
      && significant.trim_end_matches('0') == "1"
      && significant.len() as i64 + i64::from(exponent) - fraction.len() as i64 == 1,
  )
}

fn matches_key(item: &Item, key: &Item) -> Result<bool, EventStoreError> {
  for (name, expected) in key {
    let actual = item
      .get(name)
      .ok_or_else(|| invalid_data("configuration key is missing"))?;
    let matches = if matches!(expected, AttributeValue::N(_)) {
      number_is(actual, 0)?
    } else {
      actual == expected
    };
    if !matches {
      return Ok(false);
    }
  }
  Ok(true)
}

fn initial_request(tables: &DynamoDbTables) -> Result<RequestItems, EventStoreError> {
  [
    (&tables.journal_table_name, Some("seq_nr")),
    (&tables.snapshot_table_name, Some("skey")),
    (&tables.head_table_name, None),
  ]
  .into_iter()
  .map(|(table, sort)| {
    let mut key = Item::from([("aid".into(), AttributeValue::S("__config__".into()))]);
    if let Some(sort) = sort {
      key.insert(sort.into(), AttributeValue::N("0".into()));
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

fn accumulate(
  accumulated: &mut HashMap<String, Item>,
  requested: &RequestItems,
  output: BatchGetItemOutput,
) -> Result<RequestItems, EventStoreError> {
  for (table, items) in output.responses.unwrap_or_default() {
    let request = requested
      .get(&table)
      .ok_or_else(|| invalid_data("unrequested configuration table"))?;
    for item in items {
      if !matches_key(&item, &request.keys()[0])? {
        return Err(invalid_data("unrequested configuration key"));
      }
      if accumulated.insert(table.clone(), item).is_some() {
        return Err(invalid_data("duplicate configuration response"));
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
      .ok_or_else(|| invalid_data("unrequested unprocessed table"))?;
    if attributes.keys().len() != 1
      || !matches_key(&attributes.keys()[0], &request.keys()[0])?
      || accumulated.contains_key(&table)
    {
      return Err(invalid_data("invalid unprocessed configuration key"));
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

fn reconcile(tables: &DynamoDbTables, items: &HashMap<String, Item>) -> Result<ConfigurationRead, EventStoreError> {
  if items.is_empty() {
    return Ok(ConfigurationRead::CreationRequired);
  }
  let names = [
    &tables.journal_table_name,
    &tables.snapshot_table_name,
    &tables.head_table_name,
  ];
  if names.iter().any(|table| !items.contains_key(*table)) {
    return Err(configuration(ConfigurationReason::PartialDynamoDbConfiguration));
  }
  let mut store_id: Option<&String> = None;
  for table in names {
    let item = &items[table];
    let id = item
      .get("store_id")
      .and_then(|value| value.as_s().ok())
      .ok_or_else(|| invalid_data("configuration store_id is missing or not S"))?;
    let version = item
      .get("layout_version")
      .ok_or_else(|| invalid_data("configuration layout_version is missing"))?;
    if !number_is(version, 1)? {
      return Err(configuration(ConfigurationReason::UnsupportedDynamoDbLayoutVersion));
    }
    if store_id.is_some_and(|previous| previous != id) {
      return Err(configuration(ConfigurationReason::DynamoDbStoreIdMismatch));
    }
    store_id = Some(id);
  }
  Ok(ConfigurationRead::Matched {
    store_id: store_id.expect("all three items were checked").clone(),
  })
}

pub(super) async fn read_configuration(
  client: &Client,
  tables: &DynamoDbTables,
  options: &DynamoDbOptions,
) -> Result<ConfigurationRead, EventStoreError> {
  let sleeper = client
    .config()
    .sleep_impl()
    .ok_or_else(|| configuration(ConfigurationReason::MissingRetrySleeper))?;
  read_with_wait(client, tables, options, RetryWait::Sdk(sleeper)).await
}

pub(super) async fn read_with_wait(
  client: &Client,
  tables: &DynamoDbTables,
  options: &DynamoDbOptions,
  sleeper: RetryWait,
) -> Result<ConfigurationRead, EventStoreError> {
  options.retention.validate()?;
  if tables.journal_table_name == tables.snapshot_table_name
    || tables.journal_table_name == tables.head_table_name
    || tables.snapshot_table_name == tables.head_table_name
  {
    return Err(configuration(ConfigurationReason::DuplicateDynamoDbTableNames));
  }
  let mut pending = initial_request(tables)?;
  let mut accumulated = HashMap::new();
  let mut retries = 0;
  let mut delay = options
    .unprocessed_retry_initial_delay
    .min(options.unprocessed_retry_max_delay);
  loop {
    let output = client
      .batch_get_item()
      .set_request_items(Some(pending.clone()))
      .send()
      .await
      .map_err(storage)?;
    pending = accumulate(&mut accumulated, &pending, output)?;
    if pending.is_empty() {
      return reconcile(tables, &accumulated);
    }
    if retries == options.unprocessed_retry_limit {
      return Err(storage(std::io::Error::other(
        "configuration unprocessed retry limit reached",
      )));
    }
    sleeper.sleep(delay).await;
    retries += 1;
    delay = delay.saturating_mul(2).min(options.unprocessed_retry_max_delay);
  }
}
