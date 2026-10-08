use std::collections::HashMap;

use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{AttributeValue, Put, ReturnValuesOnConditionCheckFailure, TransactWriteItem, Update};

use super::item_size::{item_size_upper_bound, ITEM_SIZE_LIMIT};
use super::{DynamoDbTables, EventStoreForDynamoDB};
use crate::next::aggregate_id::{AggregateId, AidString};
use crate::next::error::{ContractRule, EventStoreError, StorageOperation};
use crate::next::event_envelope::EventEnvelope;
use crate::next::generic_event_store::check_event;
use crate::next::seq_nr::{SeqNr, SEQ_NR_MAX};

// この順で組み立てた要求のアクションだけを取消理由に対応付ける。
const JOURNAL_POSITION: usize = 0;
const HEAD_POSITION: usize = 1;

impl<AID: AggregateId, A: Send + Sync + 'static, P: Send + Sync + 'static> EventStoreForDynamoDB<AID, A, P> {
  /// イベント1件とヘッドを原子的に確定する。snapshot書込み・保持処理は行わない（H-1・D-9）。
  pub async fn persist_event(&self, event: EventEnvelope<AID, P>) -> Result<(), EventStoreError> {
    let aid = check_event(&event)?;
    let payload = self.event_serializer.serialize(event.payload())?;
    let writes = transaction_items(&self.tables, &aid, &event, payload)?;
    self
      .client
      .transact_write_items()
      .set_transact_items(Some(writes))
      .send()
      .await
      .map_err(|error| classify_append_error(error, &aid, event.seq_nr()))?;
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
  let metadata = HashMap::from([
    ("seq_nr".into(), AttributeValue::N(seq_nr.to_string())),
    (
      "occurred_at".into(),
      AttributeValue::N(
        event
          .occurred_at()
          .timestamp_nanos_opt()
          .expect("入口で検査済みの時刻")
          .to_string(),
      ),
    ),
    ("manifest".into(), AttributeValue::S(event.manifest().into())),
    ("payload".into(), AttributeValue::B(Blob::new(payload))),
  ]);
  let mut journal = metadata.clone();
  journal.insert("aid".into(), AttributeValue::S(aid.as_str().into()));
  // 利用者のtype_name()を再評価せず、検査を通ったaidから型名を取り出す。
  let (type_name, _) = aid.as_str().split_once('-').expect("検査済みaidの区切り");
  let events = AttributeValue::L(vec![AttributeValue::M(metadata)]);
  let head = HashMap::from([
    ("aid".into(), AttributeValue::S(aid.as_str().into())),
    ("type_name".into(), AttributeValue::S(type_name.into())),
    ("seq_nr".into(), AttributeValue::N(seq_nr.to_string())),
    ("events".into(), events.clone()),
  ]);
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

fn optimistic_lock(aid: &AidString, seq_nr: SeqNr, head_seq_nr: Option<SeqNr>) -> EventStoreError {
  EventStoreError::OptimisticLock {
    aid: aid.as_str().into(),
    seq_nr,
    head_seq_nr,
  }
}

fn classify_append_error(error: SdkError<TransactWriteItemsError>, aid: &AidString, seq_nr: SeqNr) -> EventStoreError {
  if let Some(TransactWriteItemsError::TransactionCanceledException(canceled)) = error.as_service_error() {
    let reasons = canceled.cancellation_reasons();
    if reasons
      .iter()
      .take(HEAD_POSITION + 1)
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
