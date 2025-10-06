use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::transact_write_items::{TransactWriteItemsError, TransactWriteItemsOutput};
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{AttributeValue, DeleteRequest, Put, Select, TransactWriteItem, Update, WriteRequest};
use aws_sdk_dynamodb::Client;
use chrono::{Duration, Utc};
use tracing::Instrument;

use crate::event_store_backend::{SnapshotEnvelope, SnapshotMaintenance, StorageBackend};
use crate::generic_event_store::GenericEventStore;
use crate::key_resolver::{DefaultKeyResolver, KeyResolver};
use crate::serializer::{EventSerializer, SnapshotSerializer};
use crate::types::{
  Aggregate, AggregateId, Event, EventStore, EventStoreReadError, EventStoreWriteError,
  TransactionCanceledExceptionWrapper,
};

#[derive(Clone, Debug)]
pub struct EventStoreForDynamoDB<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>, {
  inner: GenericEventStore<AID, A, E, DynamoDbBackend<AID, A, E>>,
}

impl<AID, A, E> EventStoreForDynamoDB<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>,
{
  pub fn new(
    client: Client,
    journal_table_name: String,
    journal_aid_index_name: String,
    snapshot_table_name: String,
    snapshot_aid_index_name: String,
    shard_count: u64,
  ) -> Self {
    let backend = DynamoDbBackend::new(
      client,
      journal_table_name,
      journal_aid_index_name,
      snapshot_table_name,
      snapshot_aid_index_name,
      shard_count,
    );
    Self {
      inner: GenericEventStore::new(backend),
    }
  }

  pub fn with_keep_snapshot_count(mut self, keep_snapshot_count: Option<usize>) -> Self {
    self.inner = self.inner.with_keep_snapshot_count(keep_snapshot_count);
    self
  }

  pub fn with_delete_ttl(mut self, delete_ttl: Option<Duration>) -> Self {
    self.inner = self.inner.with_delete_ttl(delete_ttl);
    self
  }

  pub fn with_key_resolver(mut self, key_resolver: Arc<dyn KeyResolver<ID = AID>>) -> Self {
    self.inner.backend_mut().set_key_resolver(key_resolver);
    self
  }

  pub fn with_event_serializer(mut self, serializer: Arc<dyn EventSerializer<E>>) -> Self {
    self.inner.backend_mut().set_event_serializer(serializer);
    self
  }

  pub fn with_snapshot_serializer(mut self, serializer: Arc<dyn SnapshotSerializer<A>>) -> Self {
    self.inner.backend_mut().set_snapshot_serializer(serializer);
    self
  }

  pub fn maintenance(&self) -> &SnapshotMaintenance {
    self.inner.maintenance()
  }
}

#[async_trait]
impl<AID, A, E> EventStore for EventStoreForDynamoDB<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>,
{
  type AG = A;
  type AID = AID;
  type EV = E;

  async fn persist_event(&mut self, event: &Self::EV, version: usize) -> Result<(), EventStoreWriteError> {
    self.inner.persist_event(event, version).await
  }

  async fn persist_event_and_snapshot(
    &mut self,
    event: &Self::EV,
    aggregate: &Self::AG,
  ) -> Result<(), EventStoreWriteError> {
    self.inner.persist_event_and_snapshot(event, aggregate).await
  }

  async fn get_latest_snapshot_by_id(&self, aid: &Self::AID) -> Result<Option<Self::AG>, EventStoreReadError> {
    self.inner.get_latest_snapshot_by_id(aid).await
  }

  async fn get_events_by_id_since_seq_nr(
    &self,
    aid: &Self::AID,
    seq_nr: usize,
  ) -> Result<Vec<Self::EV>, EventStoreReadError> {
    self.inner.get_events_by_id_since_seq_nr(aid, seq_nr).await
  }
}

#[derive(Clone, Debug)]
struct DynamoDbBackend<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>, {
  client: Client,
  journal_table_name: String,
  journal_aid_index_name: String,
  snapshot_table_name: String,
  snapshot_aid_index_name: String,
  shard_count: u64,
  key_resolver: Arc<dyn KeyResolver<ID = AID>>,
  event_serializer: Arc<dyn EventSerializer<E>>,
  snapshot_serializer: Arc<dyn SnapshotSerializer<A>>,
}

impl<AID, A, E> DynamoDbBackend<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>,
{
  fn new(
    client: Client,
    journal_table_name: String,
    journal_aid_index_name: String,
    snapshot_table_name: String,
    snapshot_aid_index_name: String,
    shard_count: u64,
  ) -> Self {
    Self {
      client,
      journal_table_name,
      journal_aid_index_name,
      snapshot_table_name,
      snapshot_aid_index_name,
      shard_count,
      key_resolver: Arc::new(DefaultKeyResolver::default()),
      event_serializer: Arc::new(crate::serializer::JsonEventSerializer::default()),
      snapshot_serializer: Arc::new(crate::serializer::JsonSnapshotSerializer::default()),
    }
  }

  fn set_key_resolver(&mut self, key_resolver: Arc<dyn KeyResolver<ID = AID>>) {
    self.key_resolver = key_resolver;
  }

  fn set_event_serializer(&mut self, serializer: Arc<dyn EventSerializer<E>>) {
    self.event_serializer = serializer;
  }

  fn set_snapshot_serializer(&mut self, serializer: Arc<dyn SnapshotSerializer<A>>) {
    self.snapshot_serializer = serializer;
  }

  fn resolve_pkey(&self, id: &AID) -> String {
    self.key_resolver.resolve_partition_key(id, self.shard_count)
  }

  fn resolve_skey(&self, id: &AID, seq_nr: usize) -> String {
    self.key_resolver.resolve_sort_key(id, seq_nr)
  }

  fn put_snapshot(&self, event: &E, seq_nr: usize, aggregate: &A) -> Result<Put, EventStoreWriteError> {
    let pkey = self.resolve_pkey(event.aggregate_id());
    let skey = self.resolve_skey(event.aggregate_id(), seq_nr);
    let payload = self.snapshot_serializer.serialize(aggregate)?;
    Put::builder()
      .table_name(self.snapshot_table_name.clone())
      .item("pkey", AttributeValue::S(pkey))
      .item("skey", AttributeValue::S(skey))
      .item("payload", AttributeValue::B(Blob::new(payload)))
      .item("aid", AttributeValue::S(event.aggregate_id().to_string()))
      .item("seq_nr", AttributeValue::N(seq_nr.to_string()))
      .item("version", AttributeValue::N(aggregate.version().to_string()))
      .item("ttl", AttributeValue::N("0".to_string()))
      .item(
        "last_updated_at",
        AttributeValue::N(event.occurred_at().timestamp_millis().to_string()),
      )
      .condition_expression("attribute_not_exists(pkey) AND attribute_not_exists(skey)")
      .build()
      .map_err(|err| EventStoreWriteError::IOError(err.into()))
  }

  fn update_snapshot(
    &self,
    event: &E,
    seq_nr: usize,
    expected_version: usize,
    aggregate: Option<&A>,
  ) -> Result<Update, EventStoreWriteError> {
    let pkey = self.resolve_pkey(event.aggregate_id());
    let skey = self.resolve_skey(event.aggregate_id(), seq_nr);
    let mut update_snapshot = Update::builder()
      .table_name(self.snapshot_table_name.clone())
      .update_expression("SET #version=:after_version, #last_updated_at=:last_updated_at")
      .key("pkey", AttributeValue::S(pkey))
      .key("skey", AttributeValue::S(skey))
      .expression_attribute_names("#version", "version")
      .expression_attribute_names("#last_updated_at", "last_updated_at")
      .expression_attribute_values(":before_version", AttributeValue::N(expected_version.to_string()))
      .expression_attribute_values(":after_version", AttributeValue::N((expected_version + 1).to_string()))
      .expression_attribute_values(
        ":last_updated_at",
        AttributeValue::N(event.occurred_at().timestamp_millis().to_string()),
      )
      .condition_expression("#version=:before_version");
    if let Some(aggregate) = aggregate {
      let payload = self.snapshot_serializer.serialize(aggregate)?;
      update_snapshot = update_snapshot
        .update_expression(
          "SET #payload=:payload, #seq_nr=:seq_nr, #version=:after_version, #last_updated_at=:last_updated_at",
        )
        .expression_attribute_names("#seq_nr", "seq_nr")
        .expression_attribute_names("#payload", "payload")
        .expression_attribute_values(":seq_nr", AttributeValue::N(seq_nr.to_string()))
        .expression_attribute_values(":payload", AttributeValue::B(Blob::new(payload)));
    }
    update_snapshot
      .build()
      .map_err(|err| EventStoreWriteError::IOError(err.into()))
  }

  fn put_journal(&self, event: &E) -> Result<Put, EventStoreWriteError> {
    let pkey = self.resolve_pkey(event.aggregate_id());
    let skey = self.resolve_skey(event.aggregate_id(), event.seq_nr());
    let payload = self.event_serializer.serialize(event)?;
    let occurred_at = event.occurred_at().timestamp_millis().to_string();
    Put::builder()
      .table_name(self.journal_table_name.clone())
      .item("pkey", AttributeValue::S(pkey))
      .item("skey", AttributeValue::S(skey))
      .item("aid", AttributeValue::S(event.aggregate_id().to_string()))
      .item("seq_nr", AttributeValue::N(event.seq_nr().to_string()))
      .item("payload", AttributeValue::B(Blob::new(payload)))
      .item("occurred_at", AttributeValue::N(occurred_at))
      .build()
      .map_err(|err| EventStoreWriteError::IOError(err.into()))
  }

  async fn maintain_snapshots(
    &self,
    aggregate_id: &AID,
    maintenance: &SnapshotMaintenance,
  ) -> Result<(), EventStoreWriteError> {
    let keep_snapshot_count = match maintenance.keep_snapshot_count {
      Some(count) if count > 0 => count,
      _ => return Ok(()),
    };
    if let Some(delete_ttl) = maintenance.delete_ttl {
      self
        .update_ttl_of_excess_snapshots(aggregate_id, keep_snapshot_count, delete_ttl)
        .await
    } else {
      self.delete_excess_snapshots(aggregate_id, keep_snapshot_count).await
    }
  }

  async fn delete_excess_snapshots(
    &self,
    aggregate_id: &AID,
    keep_snapshot_count: usize,
  ) -> Result<(), EventStoreWriteError> {
    let snapshot_count = self
      .get_snapshot_count(aggregate_id)
      .await
      .map_err(|err| EventStoreWriteError::OtherError(err.to_string()))?;
    let excess_count = snapshot_count.saturating_sub(keep_snapshot_count + 1);
    if excess_count == 0 {
      return Ok(());
    }
    let keys = self
      .get_last_snapshot_keys(aggregate_id, excess_count as i32)
      .await
      .map_err(|err| EventStoreWriteError::OtherError(err.to_string()))?;
    if keys.is_empty() {
      return Ok(());
    }
    let request_items = keys
      .into_iter()
      .map(|(pkey, skey)| {
        WriteRequest::builder()
          .delete_request(
            DeleteRequest::builder()
              .key("pkey", AttributeValue::S(pkey))
              .key("skey", AttributeValue::S(skey))
              .build()
              .unwrap(),
          )
          .build()
      })
      .collect::<Vec<_>>();
    let result = self
      .client
      .batch_write_item()
      .request_items(self.snapshot_table_name.clone(), request_items)
      .send()
      .instrument(tracing::debug_span!("batch_write_item"))
      .await;
    match result {
      Ok(_) => Ok(()),
      Err(err) => Err(EventStoreWriteError::IOError(Box::new(err.into_service_error()))),
    }
  }

  async fn update_ttl_of_excess_snapshots(
    &self,
    aggregate_id: &AID,
    keep_snapshot_count: usize,
    delete_ttl: Duration,
  ) -> Result<(), EventStoreWriteError> {
    let snapshot_count = self
      .get_snapshot_count(aggregate_id)
      .await
      .map_err(|err| EventStoreWriteError::OtherError(err.to_string()))?;
    let excess_count = snapshot_count.saturating_sub(keep_snapshot_count + 1);
    if excess_count == 0 {
      return Ok(());
    }
    let keys = self
      .get_last_snapshot_keys(aggregate_id, excess_count as i32)
      .await
      .map_err(|err| EventStoreWriteError::OtherError(err.to_string()))?;
    if keys.is_empty() {
      return Ok(());
    }
    let ttl = (Utc::now() + delete_ttl).timestamp();
    for (pkey, skey) in keys {
      let result = self
        .client
        .update_item()
        .table_name(self.snapshot_table_name.clone())
        .key("pkey", AttributeValue::S(pkey))
        .key("skey", AttributeValue::S(skey))
        .update_expression("SET #ttl=:ttl")
        .expression_attribute_names("#ttl", "ttl")
        .expression_attribute_values(":ttl", AttributeValue::N(ttl.to_string()))
        .send()
        .await;
      if let Err(err) = result {
        return Err(EventStoreWriteError::IOError(err.into()));
      }
    }
    Ok(())
  }

  async fn get_snapshot_count(&self, aid: &AID) -> Result<usize, EventStoreReadError> {
    let response = self
      .client
      .query()
      .table_name(self.snapshot_table_name.clone())
      .index_name(self.snapshot_aid_index_name.clone())
      .key_condition_expression("#aid = :aid")
      .expression_attribute_names("#aid", "aid")
      .expression_attribute_values(":aid", AttributeValue::S(aid.to_string()))
      .select(Select::Count)
      .send()
      .await;
    match response {
      Err(err) => Err(EventStoreReadError::IOError(Box::new(err.into_service_error()))),
      Ok(response) => Ok(response.count as usize),
    }
  }

  async fn get_last_snapshot_keys(&self, aid: &AID, limit: i32) -> Result<Vec<(String, String)>, EventStoreReadError> {
    let response = self
      .client
      .query()
      .table_name(self.snapshot_table_name.clone())
      .index_name(self.snapshot_aid_index_name.clone())
      .key_condition_expression("#aid = :aid AND #seq_nr > :seq_nr")
      .expression_attribute_names("#aid", "aid")
      .expression_attribute_names("#seq_nr", "seq_nr")
      .expression_attribute_values(":aid", AttributeValue::S(aid.to_string()))
      .expression_attribute_values(":seq_nr", AttributeValue::N("0".to_string()))
      .limit(limit)
      .scan_index_forward(false)
      .send()
      .await;
    match response {
      Err(err) => Err(EventStoreReadError::IOError(Box::new(err.into_service_error()))),
      Ok(response) => {
        let mut keys = Vec::new();
        if let Some(items) = response.items {
          for item in items {
            let pkey = item.get("pkey").and_then(|v| v.as_s().ok()).cloned();
            let skey = item.get("skey").and_then(|v| v.as_s().ok()).cloned();
            if let (Some(pkey), Some(skey)) = (pkey, skey) {
              keys.push((pkey, skey));
            }
          }
        }
        Ok(keys)
      }
    }
  }
}

#[async_trait]
impl<AID, A, E> StorageBackend<AID, A, E> for DynamoDbBackend<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>,
{
  async fn fetch_latest_snapshot(&self, aid: &AID) -> Result<Option<SnapshotEnvelope<A>>, EventStoreReadError> {
    let item = match self.fetch_snapshot_items(aid).await? {
      Some(item) => item,
      None => return Ok(None),
    };
    let payload_attr = item
      .get("payload")
      .and_then(|v| v.as_b().ok())
      .ok_or_else(|| EventStoreReadError::OtherError("snapshot payload is missing".to_string()))?;
    let version = item
      .get("version")
      .and_then(|v| v.as_n().ok())
      .ok_or_else(|| EventStoreReadError::OtherError("snapshot version is missing".to_string()))?
      .parse::<usize>()
      .map_err(|err| EventStoreReadError::OtherError(err.to_string()))?;
    let seq_nr = item
      .get("seq_nr")
      .and_then(|v| v.as_n().ok())
      .and_then(|s| s.parse::<usize>().ok())
      .unwrap_or_default();
    let payload_bytes = payload_attr.clone().into_inner();
    let mut aggregate = *self.snapshot_serializer.deserialize(&payload_bytes)?;
    aggregate.set_version(version);
    Ok(Some(SnapshotEnvelope {
      aggregate,
      seq_nr,
      version,
    }))
  }

  async fn fetch_events_since(&self, aid: &AID, seq_nr: usize) -> Result<Vec<E>, EventStoreReadError> {
    let response = self
      .client
      .query()
      .table_name(self.journal_table_name.clone())
      .index_name(self.journal_aid_index_name.clone())
      .key_condition_expression("#aid = :aid AND #seq_nr >= :seq_nr")
      .expression_attribute_names("#aid", "aid")
      .expression_attribute_names("#seq_nr", "seq_nr")
      .expression_attribute_values(":aid", AttributeValue::S(aid.to_string()))
      .expression_attribute_values(":seq_nr", AttributeValue::N(seq_nr.to_string()))
      .send()
      .await;
    match response {
      Err(err) => Err(EventStoreReadError::IOError(err.into())),
      Ok(response) => {
        let mut events = Vec::new();
        if let Some(items) = response.items {
          for item in items {
            if let Some(payload) = item.get("payload").and_then(|v| v.as_b().ok()) {
              let bytes = payload.clone().into_inner();
              let event = *self.event_serializer.deserialize(&bytes)?;
              events.push(event);
            }
          }
        }
        Ok(events)
      }
    }
  }

  async fn create_event_and_snapshot(
    &self,
    event: &E,
    aggregate: &A,
    maintenance: &SnapshotMaintenance,
  ) -> Result<(), EventStoreWriteError> {
    let mut builder = self
      .client
      .transact_write_items()
      .transact_items(
        TransactWriteItem::builder()
          .put(self.put_snapshot(event, 0, aggregate)?)
          .build(),
      )
      .transact_items(TransactWriteItem::builder().put(self.put_journal(event)?).build());
    if maintenance.keep_snapshot_count.is_some() {
      builder = builder.transact_items(
        TransactWriteItem::builder()
          .put(self.put_snapshot(event, aggregate.seq_nr(), aggregate)?)
          .build(),
      );
    }
    let result = builder.send().await;
    write_error_handling(result)
  }

  async fn update_event_and_snapshot(
    &self,
    event: &E,
    aggregate: Option<&A>,
    expected_version: usize,
    maintenance: &SnapshotMaintenance,
  ) -> Result<(), EventStoreWriteError> {
    let mut builder = self
      .client
      .transact_write_items()
      .transact_items(
        TransactWriteItem::builder()
          .update(self.update_snapshot(event, 0, expected_version, aggregate)?)
          .build(),
      )
      .transact_items(TransactWriteItem::builder().put(self.put_journal(event)?).build());
    if let (Some(aggregate), Some(_)) = (aggregate, maintenance.keep_snapshot_count) {
      builder = builder.transact_items(
        TransactWriteItem::builder()
          .put(self.put_snapshot(event, aggregate.seq_nr(), aggregate)?)
          .build(),
      );
    }
    let result = builder.send().await;
    write_error_handling(result)
  }

  async fn on_event_persisted(&self, aid: &AID, maintenance: &SnapshotMaintenance) -> Result<(), EventStoreWriteError> {
    self.maintain_snapshots(aid, maintenance).await
  }
}

impl<AID, A, E> DynamoDbBackend<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>,
{
  async fn fetch_snapshot_items(
    &self,
    aid: &AID,
  ) -> Result<Option<HashMap<String, AttributeValue>>, EventStoreReadError> {
    let response = self
      .client
      .query()
      .table_name(self.snapshot_table_name.clone())
      .index_name(self.snapshot_aid_index_name.clone())
      .key_condition_expression("#aid = :aid AND #seq_nr = :seq_nr")
      .expression_attribute_names("#aid", "aid")
      .expression_attribute_names("#seq_nr", "seq_nr")
      .expression_attribute_values(":aid", AttributeValue::S(aid.to_string()))
      .expression_attribute_values(":seq_nr", AttributeValue::N("0".to_string()))
      .limit(1)
      .send()
      .await;
    match response {
      Err(err) => Err(EventStoreReadError::IOError(err.into())),
      Ok(response) => Ok(response.items.and_then(|mut items| items.pop())),
    }
  }
}

fn write_error_handling(
  result: Result<TransactWriteItemsOutput, SdkError<TransactWriteItemsError>>,
) -> Result<(), EventStoreWriteError> {
  match result {
    Ok(_) => Ok(()),
    Err(e) => match e.into_service_error() {
      TransactWriteItemsError::TransactionCanceledException(e) => {
        if !e.cancellation_reasons().is_empty() {
          Err(EventStoreWriteError::OptimisticLockError(
            TransactionCanceledExceptionWrapper(Some(e)),
          ))
        } else {
          Err(EventStoreWriteError::IOError(e.into()))
        }
      }
      error => Err(EventStoreWriteError::IOError(error.into())),
    },
  }
}

unsafe impl<AID, A, E> Sync for EventStoreForDynamoDB<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>,
{
}

unsafe impl<AID, A, E> Send for EventStoreForDynamoDB<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>,
{
}
