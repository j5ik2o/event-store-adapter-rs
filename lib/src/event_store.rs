use std::fmt::Debug;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{AttributeValue, DeleteRequest, Put, Select, TransactWriteItem, Update, WriteRequest};
use aws_sdk_dynamodb::Client;
use chrono::{Duration, Utc};

use crate::key_resolver::{DefaultKeyResolver, KeyResolver};
use crate::serializer::{EventSerializer, SnapshotSerializer};
use crate::types::{Aggregate, AggregateId, Event, EventStore};

/// Event Store for DynamoDB
#[derive(Debug, Clone)]
pub struct EventStoreForDynamoDB<AID: AggregateId, A: Aggregate, E: Event> {
  client: Client,
  journal_table_name: String,
  journal_aid_index_name: String,
  snapshot_table_name: String,
  snapshot_aid_index_name: String,
  shard_count: u64,
  keep_snapshot_count: Option<usize>,
  delete_ttl: Option<Duration>,
  key_resolver: Arc<dyn KeyResolver<ID = AID>>,
  event_serializer: Arc<dyn EventSerializer<E>>,
  snapshot_serializer: Arc<dyn SnapshotSerializer<A>>,
}

unsafe impl<AID: AggregateId, A: Aggregate, E: Event> Sync for EventStoreForDynamoDB<AID, A, E> {}
unsafe impl<AID: AggregateId, A: Aggregate, E: Event> Send for EventStoreForDynamoDB<AID, A, E> {}

#[async_trait]
impl<AID: AggregateId, A: Aggregate<ID = AID>, E: Event<AggregateID = AID>> EventStore
  for EventStoreForDynamoDB<AID, A, E>
{
  type AG = A;
  type AID = AID;
  type EV = E;

  async fn get_latest_snapshot_by_id(&self, aid: &Self::AID) -> Result<Option<(Self::AG, usize)>> {
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
      .await?;
    if let Some(items) = response.items {
      if items.len() == 0 {
        return Ok(None);
      }
      let payload = items[0].get("payload").unwrap();
      let bytes = payload.as_b().unwrap().clone().into_inner();
      let aggregate = *self.snapshot_serializer.deserialize(&bytes)?;
      let version = items[0]
        .get("version")
        .unwrap()
        .as_n()
        .unwrap()
        .parse::<usize>()
        .unwrap();
      let seq_nr = aggregate.seq_nr();
      log::debug!("seq_nr: {}", seq_nr);
      Ok(Some((aggregate, version)))
    } else {
      Err(anyhow::anyhow!("No snapshot found for aggregate id: {}", aid))
    }
  }

  async fn get_events_by_id_since_seq_nr(&self, aid: &Self::AID, seq_nr: usize) -> Result<Vec<Self::EV>> {
    let response = self
      .client
      .query()
      .table_name(self.journal_table_name.clone())
      .index_name(self.journal_aid_index_name.clone())
      .key_condition_expression("#aid = :aid AND #seq_nr > :seq_nr")
      .expression_attribute_names("#aid", "aid")
      .expression_attribute_values(":aid", AttributeValue::S(aid.to_string()))
      .expression_attribute_names("#seq_nr", "seq_nr")
      .expression_attribute_values(":seq_nr", AttributeValue::N(seq_nr.to_string()))
      .send()
      .await?;
    let mut events = Vec::new();
    if let Some(items) = response.items {
      for item in items {
        let payload = item.get("payload").unwrap();
        let bytes = payload.as_b().unwrap().clone().into_inner();
        let event = *self.event_serializer.deserialize(&bytes)?;
        events.push(event);
      }
    }
    Ok(events)
  }

  async fn persist_event(&mut self, event: &Self::EV, version: usize) -> Result<()> {
    if event.is_created() {
      panic!("Invalid event: {:?}", event);
    }
    self.update_event_and_snapshot_opt(event, version, None).await?;
    if self.keep_snapshot_count.is_some() {
      if self.delete_ttl.is_none() {
        self.delete_excess_snapshots(event.aggregate_id()).await?;
      } else {
        self.update_ttl_of_excess_snapshots(event.aggregate_id()).await?;
      }
    }
    Ok(())
  }

  async fn persist_event_and_snapshot(&mut self, event: &Self::EV, aggregate: &Self::AG) -> Result<()> {
    if event.is_created() {
      self.create_event_and_snapshot(event, aggregate).await?;
    } else {
      self
        .update_event_and_snapshot_opt(event, aggregate.version(), Some(aggregate))
        .await?;
      if self.keep_snapshot_count.is_some() {
        if self.delete_ttl.is_none() {
          self.delete_excess_snapshots(event.aggregate_id()).await?;
        } else {
          self.update_ttl_of_excess_snapshots(event.aggregate_id()).await?;
        }
      }
    }
    Ok(())
  }
}

impl<AID: AggregateId, A: Aggregate<ID = AID>, E: Event<AggregateID = AID>> EventStoreForDynamoDB<AID, A, E> {
  pub fn new(
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
      keep_snapshot_count: None,
      delete_ttl: None,
      key_resolver: Arc::new(DefaultKeyResolver::default()),
      event_serializer: Arc::new(crate::serializer::JsonEventSerializer::default()),
      snapshot_serializer: Arc::new(crate::serializer::JsonSnapshotSerializer::default()),
    }
  }

  pub fn with_keep_snapshot_count(mut self, keep_snapshot_count: Option<usize>) -> Self {
    self.keep_snapshot_count = keep_snapshot_count;
    self
  }

  pub fn with_delete_ttl(mut self, delete_ttl: Option<Duration>) -> Self {
    self.delete_ttl = delete_ttl;
    self
  }

  pub fn with_key_resolver(mut self, key_resolver: Arc<dyn KeyResolver<ID = AID>>) -> Self {
    self.key_resolver = key_resolver;
    self
  }

  pub fn with_event_serializer(mut self, event_serializer: Arc<dyn EventSerializer<E>>) -> Self {
    self.event_serializer = event_serializer;
    self
  }

  pub fn with_snapshot_serializer(mut self, snapshot_serializer: Arc<dyn SnapshotSerializer<A>>) -> Self {
    self.snapshot_serializer = snapshot_serializer;
    self
  }

  async fn create_event_and_snapshot(&mut self, event: &E, ar: &A) -> Result<()> {
    let mut builder = self
      .client
      .transact_write_items()
      .transact_items(
        TransactWriteItem::builder()
          .put(self.put_snapshot(event, 0, ar)?)
          .build(),
      )
      .transact_items(TransactWriteItem::builder().put(self.put_journal(event)?).build());
    if self.keep_snapshot_count.is_some() {
      builder = builder.transact_items(
        TransactWriteItem::builder()
          .put(self.put_snapshot(event, ar.seq_nr(), ar)?)
          .build(),
      );
    }
    builder.send().await?;
    Ok(())
  }

  async fn update_event_and_snapshot_opt(&mut self, event: &E, version: usize, ar: Option<&A>) -> Result<()> {
    let mut builder = self
      .client
      .transact_write_items()
      .transact_items(
        TransactWriteItem::builder()
          .update(self.update_snapshot(event, 0, version, ar)?)
          .build(),
      )
      .transact_items(TransactWriteItem::builder().put(self.put_journal(event)?).build());
    if let (true, Some(ar)) = (self.keep_snapshot_count.is_some(), ar) {
      builder = builder.transact_items(
        TransactWriteItem::builder()
          .put(self.put_snapshot(event, ar.seq_nr(), ar)?)
          .build(),
      );
    }
    builder.send().await?;
    Ok(())
  }

  /// Delete excess snapshots
  async fn delete_excess_snapshots(&mut self, aggregate_id: &AID) -> Result<()> {
    if let Some(keep_snapshot_count) = self.keep_snapshot_count {
      let snapshot_count = self.get_snapshot_count(aggregate_id).await? - 1;
      let excess_count = snapshot_count - keep_snapshot_count;
      if excess_count > 0 {
        log::debug!("excess_count: {}", excess_count);
        let keys = self.get_last_snapshot_keys(aggregate_id, excess_count as i32).await?;
        log::debug!("keys = {:?}", keys);
        if !keys.is_empty() {
          let request_items = keys
            .into_iter()
            .map(|(pkey, skey)| {
              WriteRequest::builder()
                .delete_request(
                  DeleteRequest::builder()
                    .key("pkey", AttributeValue::S(pkey))
                    .key("skey", AttributeValue::S(skey))
                    .build(),
                )
                .build()
            })
            .collect::<Vec<_>>();
          let result = self
            .client
            .batch_write_item()
            .request_items(self.snapshot_table_name.clone(), request_items)
            .send()
            .await;
          if result.is_err() {
            log::warn!("Failed to delete excess snapshots: {:?}", result);
          } else {
            log::debug!("excess snapshots are deleted");
          }
        }
      }
    }
    Ok(())
  }

  /// Update ttl of excess snapshots
  async fn update_ttl_of_excess_snapshots(&mut self, aggregate_id: &AID) -> Result<()> {
    if let Some(keep_snapshot_count) = self.keep_snapshot_count {
      let snapshot_count = self.get_snapshot_count(aggregate_id).await? - 1;
      let excess_count = snapshot_count - keep_snapshot_count;
      if excess_count > 0 {
        log::debug!("excess_count: {}", excess_count);
        let keys = self.get_last_snapshot_keys(aggregate_id, excess_count as i32).await?;
        log::debug!("keys = {:?}", keys);
        let ttl = (Utc::now() + self.delete_ttl.unwrap()).timestamp();
        for (pkey, skey) in keys {
          self
            .client
            .update_item()
            .table_name(self.snapshot_table_name.clone())
            .key("pkey", AttributeValue::S(pkey))
            .key("skey", AttributeValue::S(skey))
            .update_expression("SET #ttl=:ttl")
            .expression_attribute_names("#ttl", "ttl")
            .expression_attribute_values(":ttl", AttributeValue::N(ttl.to_string()))
            .send()
            .await?;
        }
      }
    }
    Ok(())
  }

  async fn get_snapshot_count(&mut self, aid: &AID) -> Result<usize> {
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
      .await?;
    Ok(response.count as usize)
  }

  async fn get_last_snapshot_keys(&mut self, aid: &AID, limit: i32) -> Result<Vec<(String, String)>> {
    let mut query_builder = self
      .client
      .query()
      .table_name(self.snapshot_table_name.clone())
      .index_name(self.snapshot_aid_index_name.clone())
      .key_condition_expression("#aid = :aid AND #seq_nr > :seq_nr")
      .expression_attribute_names("#aid", "aid")
      .expression_attribute_values(":aid", AttributeValue::S(aid.to_string()))
      .expression_attribute_names("#seq_nr", "seq_nr")
      .expression_attribute_values(":seq_nr", AttributeValue::N(0.to_string()))
      .scan_index_forward(false)
      .limit(limit);
    if self.delete_ttl.is_some() {
      query_builder = query_builder
        .expression_attribute_names("#ttl", "ttl")
        .expression_attribute_values(":ttl", AttributeValue::N(0.to_string()))
        .filter_expression("#ttl = :ttl");
    }
    let response = query_builder.send().await?;
    if let Some(items) = response.items {
      let mut result = Vec::new();
      for item in items {
        log::debug!("aid: {}", item.get("aid").unwrap().as_s().unwrap());
        log::debug!("seq_nr: {}", item.get("seq_nr").unwrap().as_n().unwrap());
        let pkey = item.get("pkey").unwrap().as_s().unwrap().clone();
        let skey = item.get("skey").unwrap().as_s().unwrap().clone();
        result.push((pkey, skey));
      }
      Ok(result)
    } else {
      Err(anyhow::anyhow!("No snapshot found for aggregate id: {}", aid))
    }
  }

  fn put_snapshot(&mut self, event: &E, seq_nr: usize, ar: &A) -> Result<Put> {
    let pkey = self.resolve_pkey(event.aggregate_id(), self.shard_count);
    let skey = self.resolve_skey(event.aggregate_id(), seq_nr);
    let payload = self.snapshot_serializer.serialize(ar)?;
    log::debug!(">--- put_snapshot ---");
    log::debug!("pkey: {}", pkey);
    log::debug!("skey: {}", skey);
    log::debug!("aid: {}", event.aggregate_id().to_string());
    log::debug!("seq_nr: {}", seq_nr);
    log::debug!("<--- put_snapshot ---");
    let put_snapshot = Put::builder()
      .table_name(self.snapshot_table_name.clone())
      .item("pkey", AttributeValue::S(pkey))
      .item("skey", AttributeValue::S(skey))
      .item("payload", AttributeValue::B(Blob::new(payload)))
      .item("aid", AttributeValue::S(event.aggregate_id().to_string()))
      .item("seq_nr", AttributeValue::N(seq_nr.to_string()))
      .item("version", AttributeValue::N("1".to_string()))
      .item("ttl", AttributeValue::N("0".to_string()))
      .item(
        "last_updated_at",
        AttributeValue::N(event.occurred_at().timestamp_millis().to_string()),
      )
      .condition_expression("attribute_not_exists(pkey) AND attribute_not_exists(skey)");
    Ok(put_snapshot.build())
  }

  fn update_snapshot(&mut self, event: &E, seq_nr: usize, version: usize, ar_opt: Option<&A>) -> Result<Update> {
    let pkey = self.resolve_pkey(event.aggregate_id(), self.shard_count);
    let skey = self.resolve_skey(event.aggregate_id(), seq_nr);
    log::debug!(">--- update_snapshot ---");
    log::debug!("pkey: {}", pkey);
    log::debug!("skey: {}", skey);
    log::debug!("aid: {}", event.aggregate_id().to_string());
    log::debug!("seq_nr: {}", seq_nr);
    log::debug!("<--- update_snapshot ---");
    let mut update_snapshot = Update::builder()
      .table_name(self.snapshot_table_name.clone())
      .update_expression("SET #version=:after_version, #last_updated_at=:last_updated_at")
      .key("pkey", AttributeValue::S(pkey))
      .key("skey", AttributeValue::S(skey))
      .expression_attribute_names("#version", "version")
      .expression_attribute_names("#last_updated_at", "last_updated_at")
      .expression_attribute_values(":before_version", AttributeValue::N(version.to_string()))
      .expression_attribute_values(":after_version", AttributeValue::N((version + 1).to_string()))
      .expression_attribute_values(
        ":last_updated_at",
        AttributeValue::N(event.occurred_at().timestamp_millis().to_string()),
      )
      .condition_expression("#version=:before_version");
    if let Some(ar) = ar_opt {
      let payload = self.snapshot_serializer.serialize(ar)?;
      update_snapshot = update_snapshot
        .update_expression(
          "SET #payload=:payload, #seq_nr=:seq_nr, #version=:after_version, #last_updated_at=:last_updated_at",
        )
        .expression_attribute_names("#seq_nr", "seq_nr")
        .expression_attribute_names("#payload", "payload")
        .expression_attribute_values(":seq_nr", AttributeValue::N(seq_nr.to_string()))
        .expression_attribute_values(":payload", AttributeValue::B(Blob::new(payload)));
    }
    Ok(update_snapshot.build())
  }

  fn resolve_pkey(&self, id: &AID, shard_count: u64) -> String {
    self.key_resolver.resolve_partition_key(id, shard_count)
  }

  fn resolve_skey(&self, id: &AID, seq_nr: usize) -> String {
    self.key_resolver.resolve_sort_key(id, seq_nr)
  }

  fn put_journal(&mut self, event: &E) -> Result<Put> {
    let pkey = self.resolve_pkey(event.aggregate_id(), self.shard_count);
    let skey = self.resolve_skey(event.aggregate_id(), event.seq_nr());
    let aid = event.aggregate_id().to_string();
    let seq_nr = event.seq_nr().to_string();
    let payload = self.event_serializer.serialize(event)?;
    let occurred_at = event.occurred_at().timestamp_millis().to_string();

    let put_journal = Put::builder()
      .table_name(self.journal_table_name.clone())
      .item("pkey", AttributeValue::S(pkey))
      .item("skey", AttributeValue::S(skey))
      .item("aid", AttributeValue::S(aid))
      .item("seq_nr", AttributeValue::N(seq_nr))
      .item("payload", AttributeValue::B(Blob::new(payload)))
      .item("occurred_at", AttributeValue::N(occurred_at))
      .build();

    Ok(put_journal)
  }
}
