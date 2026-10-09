use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use chrono::DateTime;

use super::EventStoreForDynamoDB;
use crate::next::aggregate_id::{AggregateId, AidString};
use crate::next::error::{EventStoreError, StorageOperation};
use crate::next::event_envelope::EventEnvelope;
use crate::next::generic_event_store::check_read_seq_nr;
use crate::next::seq_nr::SeqNr;
use crate::next::serializer::EventSerializer;

impl<AID: AggregateId, A: Send + Sync + 'static, P: Send + Sync + 'static> EventStoreForDynamoDB<AID, A, P> {
  /// 指定番号以上のイベントをjournal本体から強整合・昇順で全件返す（R-4〜R-6・DY-11）。
  pub async fn get_events_by_id_since_seq_nr(
    &self,
    aggregate_id: &AID,
    seq_nr: SeqNr,
  ) -> Result<Vec<EventEnvelope<AID, P>>, EventStoreError> {
    let aid = AidString::from_aggregate_id(aggregate_id)?;
    check_read_seq_nr(seq_nr)?;
    let start_seq_nr = seq_nr.to_string();
    let mut exclusive_start_key = None;
    let mut events = Vec::new();
    loop {
      let page = self
        .client
        .query()
        .table_name(&self.tables.journal_table_name)
        .key_condition_expression("aid = :aid AND seq_nr >= :seq_nr")
        .expression_attribute_values(":aid", AttributeValue::S(aid.as_str().into()))
        .expression_attribute_values(":seq_nr", AttributeValue::N(start_seq_nr.clone()))
        .consistent_read(true)
        .scan_index_forward(true)
        .set_exclusive_start_key(exclusive_start_key)
        .send()
        .await
        .map_err(storage)?;
      for item in page.items() {
        events.push(restore_event(item, aggregate_id, &aid, self.event_serializer.as_ref())?);
      }
      exclusive_start_key = page.last_evaluated_key.filter(|key| !key.is_empty());
      if exclusive_start_key.is_none() {
        return Ok(events);
      }
    }
  }
}

fn storage(source: impl std::error::Error + Send + Sync + 'static) -> EventStoreError {
  EventStoreError::Storage {
    operation: StorageOperation::LoadEvents,
    source: Box::new(source),
  }
}

fn invalid_data(message: &'static str) -> EventStoreError {
  storage(std::io::Error::new(std::io::ErrorKind::InvalidData, message))
}

fn restore_event<AID: AggregateId, P: 'static>(
  item: &HashMap<String, AttributeValue>,
  aggregate_id: &AID,
  aid: &AidString,
  serializer: &dyn EventSerializer<P>,
) -> Result<EventEnvelope<AID, P>, EventStoreError> {
  match item.get("aid") {
    Some(AttributeValue::S(stored)) if stored == aid.as_str() => {}
    _ => {
      return Err(invalid_data(
        "journal aid is missing, not S, or differs from the requested aid",
      ))
    }
  }
  let seq_nr = match item.get("seq_nr") {
    Some(AttributeValue::N(number)) => number.parse::<SeqNr>().map_err(storage)?,
    _ => return Err(invalid_data("journal seq_nr is missing or not N")),
  };
  let occurred_at = match item.get("occurred_at") {
    Some(AttributeValue::N(number)) => DateTime::from_timestamp_nanos(number.parse::<i64>().map_err(storage)?),
    _ => return Err(invalid_data("journal occurred_at is missing or not N")),
  };
  let manifest = match item.get("manifest") {
    Some(AttributeValue::S(value)) => value,
    _ => return Err(invalid_data("journal manifest is missing or not S")),
  };
  let payload = match item.get("payload") {
    Some(AttributeValue::B(value)) => serializer.deserialize(value.as_ref())?,
    _ => return Err(invalid_data("journal payload is missing or not B")),
  };
  Ok(EventEnvelope::new(aggregate_id.clone(), seq_nr, occurred_at, payload).with_manifest(manifest.clone()))
}

#[cfg(test)]
#[path = "read_events_test.rs"]
mod tests;
