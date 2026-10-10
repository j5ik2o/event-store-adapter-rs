use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{AttributeValue, Put, ReturnValuesOnConditionCheckFailure, TransactWriteItem, Update};

use super::item_size::{item_size_upper_bound, ITEM_SIZE_LIMIT};
use super::items::{SnapshotKind, StoredEvent, StoredSnapshot};
use super::{DynamoDbTables, EventStoreForDynamoDB};
use crate::aggregate_id::{AggregateId, AidString};
use crate::error::{ContractRule, EventStoreError, StorageOperation};
use crate::event_envelope::{EventEnvelope, SnapshotEnvelope};
use crate::generic_event_store::{check_event, check_event_and_snapshot, notify_retention_failure};
use crate::seq_nr::{SeqNr, SEQ_NR_MAX};

// この順で組み立てた要求のアクションだけを取消理由に対応付ける。
const JOURNAL_POSITION: usize = 0;
const HEAD_POSITION: usize = 1;

impl<AID: AggregateId, A: Send + Sync + 'static, P: Send + Sync + 'static> EventStoreForDynamoDB<AID, A, P> {
  /// イベント1件とヘッドを原子的に確定する（H-1・D-9）。
  pub async fn persist_event(&self, event: EventEnvelope<AID, P>) -> Result<(), EventStoreError> {
    let aid = check_event(&event)?;
    let payload = self.event_serializer.serialize(event.payload())?;
    let writes = transaction_items(&self.tables, &aid, &event, payload)?;
    let action_count = writes.len();
    self
      .client
      .transact_write_items()
      .set_transact_items(Some(writes))
      .send()
      .await
      .map_err(|error| classify_append_error(error, &aid, event.seq_nr(), action_count))?;
    Ok(())
  }

  /// イベント・ヘッド・現在のsnapshotと、保持件数設定時の履歴を原子的に確定する（H-1・W-9）。
  pub async fn persist_event_and_snapshot(
    &self,
    event: EventEnvelope<AID, P>,
    snapshot: SnapshotEnvelope<A>,
  ) -> Result<(), EventStoreError> {
    let aid = check_event_and_snapshot(&event, &snapshot)?;
    let event_payload = self.event_serializer.serialize(event.payload())?.to_vec();
    let snapshot_payload = self.snapshot_serializer.serialize(snapshot.aggregate())?.to_vec();
    let mut writes = transaction_items(&self.tables, &aid, &event, event_payload)?;
    writes.extend(snapshot_transaction_items(
      &self.tables,
      &aid,
      &snapshot,
      event.occurred_at().timestamp_millis(),
      snapshot_payload,
      self.options.retention.keep_snapshot_count().is_some(),
    )?);
    let action_count = writes.len();
    self
      .client
      .transact_write_items()
      .set_transact_items(Some(writes))
      .send()
      .await
      .map_err(|error| classify_append_error(error, &aid, event.seq_nr(), action_count))?;
    notify_retention_failure(self.retain_history_after_append(&aid, event.seq_nr()).await);
    Ok(())
  }
}

fn storage(source: impl std::error::Error + Send + Sync + 'static) -> EventStoreError {
  EventStoreError::Storage {
    operation: StorageOperation::Append,
    source: Box::new(source),
  }
}

fn transaction_items<AID, P>(
  tables: &DynamoDbTables,
  aid: &AidString,
  event: &EventEnvelope<AID, P>,
  payload: Vec<u8>,
) -> Result<Vec<TransactWriteItem>, EventStoreError> {
  let seq_nr = event.seq_nr();
  let stored = StoredEvent {
    seq_nr,
    occurred_at: event
      .occurred_at()
      .timestamp_nanos_opt()
      .expect("入口で検査済みの時刻")
      .to_string(),
    manifest: event.manifest().into(),
    payload: Blob::new(payload),
  };
  let journal = stored.journal(aid);
  let head = stored.head(aid);
  let events = head["events"].clone();
  // Updateでも更新後のhead全体を検査する（type_name、List/Map、manifestを含む）。
  if item_size_upper_bound(&journal) > ITEM_SIZE_LIMIT || item_size_upper_bound(&head) > ITEM_SIZE_LIMIT {
    return Err(EventStoreError::ContractViolation {
      rule: ContractRule::ItemSizeLimit,
      seq_nr: Some(seq_nr),
      snapshot_seq_nr: None,
    });
  }

  let journal_put = Put::builder()
    .table_name(&tables.journal_table_name)
    .set_item(Some(journal))
    .condition_expression("attribute_not_exists(aid)")
    .build()
    .map_err(storage)?;
  let head_write = if seq_nr == 1 {
    let put = Put::builder()
      .table_name(&tables.head_table_name)
      .set_item(Some(head))
      .condition_expression("attribute_not_exists(aid)")
      .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld)
      .build()
      .map_err(storage)?;
    TransactWriteItem::builder().put(put).build()
  } else {
    let update = Update::builder()
      .table_name(&tables.head_table_name)
      .key("aid", AttributeValue::S(aid.as_str().into()))
      .condition_expression("seq_nr = :prev")
      .update_expression("SET seq_nr = :next, events = :events")
      .expression_attribute_values(":prev", AttributeValue::N((seq_nr - 1).to_string()))
      .expression_attribute_values(":next", AttributeValue::N(seq_nr.to_string()))
      .expression_attribute_values(":events", events)
      .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld)
      .build()
      .map_err(storage)?;
    TransactWriteItem::builder().update(update).build()
  };
  Ok(vec![TransactWriteItem::builder().put(journal_put).build(), head_write])
}

fn snapshot_transaction_items<A>(
  tables: &DynamoDbTables,
  aid: &AidString,
  snapshot: &SnapshotEnvelope<A>,
  last_updated_at: i64,
  payload: Vec<u8>,
  keep_history: bool,
) -> Result<Vec<TransactWriteItem>, EventStoreError> {
  let seq_nr = snapshot.seq_nr();
  let stored = StoredSnapshot {
    seq_nr,
    manifest: snapshot.manifest().into(),
    payload: Blob::new(payload),
    last_updated_at: last_updated_at.to_string(),
  };
  let mut items = vec![stored.item(aid, SnapshotKind::Current)];
  if keep_history {
    items.push(stored.item(aid, SnapshotKind::History { ttl: None }));
  }
  items
    .into_iter()
    .map(|item| {
      if item_size_upper_bound(&item) > ITEM_SIZE_LIMIT {
        return Err(EventStoreError::ContractViolation {
          rule: ContractRule::ItemSizeLimit,
          seq_nr: Some(seq_nr),
          snapshot_seq_nr: None,
        });
      }
      let put = Put::builder()
        .table_name(&tables.snapshot_table_name)
        .set_item(Some(item))
        .build()
        .map_err(storage)?;
      Ok(TransactWriteItem::builder().put(put).build())
    })
    .collect()
}

fn optimistic_lock(aid: &AidString, seq_nr: SeqNr, head_seq_nr: Option<SeqNr>) -> EventStoreError {
  EventStoreError::OptimisticLock {
    aid: aid.as_str().into(),
    seq_nr,
    head_seq_nr,
  }
}

fn classify_append_error(
  error: SdkError<TransactWriteItemsError>,
  aid: &AidString,
  seq_nr: SeqNr,
  action_count: usize,
) -> EventStoreError {
  if let Some(TransactWriteItemsError::TransactionCanceledException(canceled)) = error.as_service_error() {
    let reasons = canceled.cancellation_reasons();
    if reasons
      .iter()
      .take(action_count)
      .any(|reason| reason.code() == Some("TransactionConflict"))
    {
      return optimistic_lock(aid, seq_nr, None);
    }
    if let Some(head) = reasons
      .get(HEAD_POSITION)
      .filter(|reason| reason.code() == Some("ConditionalCheckFailed"))
    {
      if seq_nr == 1 {
        return optimistic_lock(aid, seq_nr, None);
      }
      let old_seq_nr = match head.item() {
        None => Some(0),
        Some(item) => item
          .get("seq_nr")
          .and_then(|value| value.as_n().ok())
          .and_then(|number| number.parse::<SeqNr>().ok())
          .filter(|number| *number <= SEQ_NR_MAX),
      };
      if let Some(h) = old_seq_nr {
        if seq_nr <= h {
          return optimistic_lock(aid, seq_nr, Some(h));
        }
        if seq_nr > h + 1 {
          return EventStoreError::ContractViolation {
            rule: ContractRule::W8Gap,
            seq_nr: Some(seq_nr),
            snapshot_seq_nr: None,
          };
        }
      }
      // 期待前番号での条件不成立や、存在する旧項目の不正な番号は想定外の取消。
      return storage(error);
    }
    if reasons.get(JOURNAL_POSITION).and_then(|reason| reason.code()) == Some("ConditionalCheckFailed") {
      return optimistic_lock(aid, seq_nr, None);
    }
  }
  storage(error)
}

#[cfg(test)]
#[path = "persist_event_test.rs"]
mod tests;
