use std::collections::HashMap;

use aws_sdk_dynamodb::config::AsyncSleep;
use aws_sdk_dynamodb::error::DisplayErrorContext;
use aws_sdk_dynamodb::types::{AttributeValue, DeleteRequest, WriteRequest};

use super::{DynamoDbOptions, DynamoDbTables, EventStoreForDynamoDB};
use crate::aggregate_id::{AggregateId, AidString};
use crate::error::RetentionFailure;
use crate::retention::{select_expired_history_after_append, RetentionMode};
use crate::seq_nr::{SeqNr, SEQ_NR_MAX};
use crate::storage_backend::AppendReceipt;

type Item = HashMap<String, AttributeValue>;

impl<AID: AggregateId, A: Send + Sync + 'static, P: Send + Sync + 'static> EventStoreForDynamoDB<AID, A, P> {
  pub(super) async fn retain_history_after_append(&self, aid: &AidString, seq_nr: SeqNr) -> AppendReceipt {
    let Some(keep) = self.options.retention.keep_snapshot_count() else {
      return AppendReceipt {
        retention_failure: None,
      };
    };
    let result = match query_history(&self.client, &self.tables, aid).await {
      Ok(visible) => {
        let expired = select_expired_history_after_append(&visible, Some(seq_nr), keep);
        match self.options.retention.mode() {
          RetentionMode::Delete => delete_history(&self.client, &self.tables, &self.options, aid, &expired)
            .await
            .map_err(|error| ("retention-delete", error)),
          RetentionMode::Ttl { grace_seconds } => super::retention_ttl::mark_history(
            &self.client,
            &self.tables,
            self.clock.as_ref(),
            aid,
            &expired,
            *grace_seconds,
          )
          .await
          .map_err(|error| ("retention-mark", error)),
        }
      }
      Err(error) => Err(("retention-query", error)),
    };
    AppendReceipt {
      retention_failure: result.err().map(|(phase, error)| RetentionFailure {
        aid: aid.as_str().into(),
        seq_nr,
        phase: phase.into(),
        error,
      }),
    }
  }
}

async fn query_history(
  client: &aws_sdk_dynamodb::Client,
  tables: &DynamoDbTables,
  aid: &AidString,
) -> Result<Vec<SeqNr>, String> {
  let mut visible = Vec::new();
  let mut exclusive_start_key = None;
  loop {
    let page = client
      .query()
      .table_name(&tables.snapshot_table_name)
      .index_name(&tables.snapshot_history_index_name)
      .key_condition_expression("aid = :aid")
      .expression_attribute_values(":aid", AttributeValue::S(aid.as_str().into()))
      .consistent_read(false)
      .scan_index_forward(false)
      .set_exclusive_start_key(exclusive_start_key)
      .send()
      .await
      .map_err(|error| DisplayErrorContext(&error).to_string())?;
    for item in page.items() {
      visible.push(history_seq_nr(item, aid)?);
    }
    exclusive_start_key = page.last_evaluated_key.filter(|key| !key.is_empty());
    if exclusive_start_key.is_none() {
      return Ok(visible);
    }
  }
}

fn history_seq_nr(item: &Item, aid: &AidString) -> Result<SeqNr, String> {
  if item.get("aid") != Some(&AttributeValue::S(aid.as_str().into())) {
    return Err("history aid is missing, not S, or differs from the requested aid".into());
  }
  let number = |name: &str| match item.get(name) {
    Some(AttributeValue::N(number)) => number.parse::<SeqNr>().map_err(|error| error.to_string()),
    _ => Err(format!("history {name} is missing or not N")),
  };
  let skey = number("skey")?;
  if skey == 0 || skey > SEQ_NR_MAX || number("active_history_seq_nr")? != skey {
    return Err("history skey is not a positive in-range active history number".into());
  }
  Ok(skey)
}

fn delete_requests(aid: &AidString, expired: &[SeqNr]) -> Result<Vec<WriteRequest>, String> {
  expired
    .iter()
    .map(|seq_nr| {
      let request = DeleteRequest::builder()
        .key("aid", AttributeValue::S(aid.as_str().into()))
        .key("skey", AttributeValue::N(seq_nr.to_string()))
        .build()
        .map_err(|error| error.to_string())?;
      Ok(WriteRequest::builder().delete_request(request).build())
    })
    .collect()
}

async fn delete_history(
  client: &aws_sdk_dynamodb::Client,
  tables: &DynamoDbTables,
  options: &DynamoDbOptions,
  aid: &AidString,
  expired: &[SeqNr],
) -> Result<(), String> {
  let requests = delete_requests(aid, expired)?;
  for batch in requests.chunks(25) {
    let mut pending = HashMap::from([(tables.snapshot_table_name.clone(), batch.to_vec())]);
    let mut retries = 0;
    let mut delay = options
      .unprocessed_retry_initial_delay
      .min(options.unprocessed_retry_max_delay);
    loop {
      let output = client
        .batch_write_item()
        .set_request_items(Some(pending))
        .send()
        .await
        .map_err(|error| DisplayErrorContext(&error).to_string())?;
      pending = output.unprocessed_items.unwrap_or_default();
      pending.retain(|_, items| !items.is_empty());
      if pending.is_empty() {
        break;
      }
      if retries == options.unprocessed_retry_limit {
        return Err("history unprocessed retry limit reached".into());
      }
      let sleeper = client
        .config()
        .sleep_impl()
        .ok_or_else(|| "DynamoDB client has no retry sleeper".to_string())?;
      sleeper.sleep(delay).await;
      retries += 1;
      delay = delay.saturating_mul(2).min(options.unprocessed_retry_max_delay);
    }
  }
  Ok(())
}

#[cfg(test)]
#[path = "retention_delete_test.rs"]
mod tests;
