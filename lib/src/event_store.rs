use std::fmt::Debug;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{AttributeValue, Delete, Put, Select, TransactWriteItem, Update};
use aws_sdk_dynamodb::Client;

use crate::key_resolver::{DefaultPartitionKeyResolver, KeyResolver};
use crate::serializer::{EventSerializer, SnapshotSerializer};
use crate::types::{Aggregate, AggregateId, Event, EventPersistenceGateway};

#[derive(Debug, Clone)]
pub struct EventStore<AG: Aggregate, EV: Event> {
  client: Client,
  journal_table_name: String,
  journal_aid_index_name: String,
  snapshot_table_name: String,
  snapshot_aid_index_name: String,
  shard_count: u64,
  key_resolver: Arc<dyn KeyResolver>,
  event_serializer: Arc<dyn EventSerializer<EV>>,
  snapshot_serializer: Arc<dyn SnapshotSerializer<AG>>,
}

unsafe impl<AG: Aggregate, EV: Event> Sync for EventStore<AG, EV> {}

unsafe impl<AG: Aggregate, EV: Event> Send for EventStore<AG, EV> {}

#[async_trait]
impl<A: Aggregate, E: Event> EventPersistenceGateway for EventStore<A, E> {
  type AG = A;
  type AID = A::ID;
  type EV = E;

  async fn get_snapshot_by_id(&self, aid: &Self::AID) -> Result<(Self::AG, usize, usize)> {
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
      .scan_index_forward(false)
      .limit(1)
      .send()
      .await?;
    if let Some(items) = response.items {
      if items.len() == 1 {
        let payload = items[0].get("payload").unwrap();
        let bytes = payload.as_b().unwrap().clone().into_inner();
        let aggregate = *self.snapshot_serializer.deserialize(&bytes)?;
        let seq_nr = items[0]
          .get("seq_nr")
          .unwrap()
          .as_n()
          .unwrap()
          .parse::<usize>()
          .unwrap();
        let version = items[0]
          .get("version")
          .unwrap()
          .as_n()
          .unwrap()
          .parse::<usize>()
          .unwrap();
        Ok((aggregate, seq_nr, version))
      } else {
        Err(anyhow::anyhow!("No snapshot found for aggregate id: {}", aid))
      }
    } else {
      Err(anyhow::anyhow!("No snapshot found for aggregate id: {}", aid))
    }
  }

  async fn get_events_by_id_and_seq_nr(&self, aid: &Self::AID, seq_nr: usize) -> Result<Vec<Self::EV>> {
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

  async fn store_event_with_snapshot_opt(&mut self, event: &E, version: usize, aggregate: Option<&A>) -> Result<()> {
    match (event.is_created(), aggregate) {
      (true, Some(ar)) => {
        self.create(event, ar).await?;
      }
      (true, None) => {
        panic!("Aggregate is not found");
      }
      (false, ar) => {
        self.update(event, version, ar).await?;
      }
    }
    Ok(())
  }
}

impl<A: Aggregate, E: Event> EventStore<A, E> {
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
      key_resolver: Arc::new(DefaultPartitionKeyResolver),
      event_serializer: Arc::new(crate::serializer::JsonEventSerializer::default()),
      snapshot_serializer: Arc::new(crate::serializer::JsonSnapshotSerializer::default()),
    }
  }

  pub fn with_key_resolver(mut self, key_resolver: Arc<dyn KeyResolver>) -> Self {
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

  async fn create(&mut self, event: &E, ar: &A) -> Result<()> {
    let mut builder = self
      .client
      .transact_write_items()
      .transact_items(
        TransactWriteItem::builder()
          .put(self.put_snapshot(event, 0, ar)?)
          .build(),
      )
      .transact_items(TransactWriteItem::builder().put(self.put_journal(event)?).build());
    builder.send().await?;
    Ok(())
  }

  async fn update(&mut self, event: &E, version: usize, ar: Option<&A>) -> Result<()> {
    let mut builder = self
      .client
      .transact_write_items()
      .transact_items(
        TransactWriteItem::builder()
          .update(self.update_snapshot(event, 0, version, ar)?)
          .build(),
      )
      .transact_items(TransactWriteItem::builder().put(self.put_journal(event)?).build());
    builder.send().await?;
    Ok(())
  }

  async fn get_snapshot_count<AID: AggregateId>(&mut self, aid: &AID) -> Result<usize> {
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

  async fn get_last_snapshot_keys<AID: AggregateId>(&mut self, aid: &AID) -> Result<(String, String)> {
    let response = self
      .client
      .query()
      .table_name(self.snapshot_table_name.clone())
      .index_name(self.snapshot_aid_index_name.clone())
      .key_condition_expression("#aid = :aid AND #seq_nr <= :max_seq_nr")
      .expression_attribute_names("#aid", "aid")
      .expression_attribute_values(":aid", AttributeValue::S(aid.to_string()))
      .expression_attribute_names("#seq_nr", "seq_nr")
      .expression_attribute_values(":seq_nr", AttributeValue::N(usize::MAX.to_string()))
      .scan_index_forward(false)
      .limit(1)
      .send()
      .await?;
    if let Some(items) = response.items {
      if items.len() == 1 {
        let pkey = items[0].get("pkey").unwrap().as_s().unwrap().clone();
        let skey = items[0].get("skey").unwrap().as_s().unwrap().clone();
        Ok((pkey, skey))
      } else {
        Err(anyhow::anyhow!("No snapshot found for aggregate id: {}", aid))
      }
    } else {
      Err(anyhow::anyhow!("No snapshot found for aggregate id: {}", aid))
    }
  }

  fn put_snapshot(&mut self, event: &E, seq_nr: usize, ar: &A) -> Result<Put> {
    let pkey = self.resolve_pkey(event.aggregate_id(), self.shard_count);
    let skey = self.resolve_skey(event.aggregate_id(), seq_nr);
    let payload = self.snapshot_serializer.serialize(ar)?;
    let put_snapshot = Put::builder()
      .table_name(self.snapshot_table_name.clone())
      .item("pkey", AttributeValue::S(pkey))
      .item("skey", AttributeValue::S(skey))
      .item("payload", AttributeValue::B(Blob::new(payload)))
      .item("aid", AttributeValue::S(event.aggregate_id().to_string()))
      .item("seq_nr", AttributeValue::N(ar.seq_nr().to_string()))
      .item("version", AttributeValue::N("1".to_string()))
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
        .expression_attribute_values(":seq_nr", AttributeValue::N(ar.seq_nr().to_string()))
        .expression_attribute_values(":payload", AttributeValue::B(Blob::new(payload)));
    }
    Ok(update_snapshot.build())
  }

  fn resolve_pkey<AID: AggregateId>(&self, id: &AID, shard_count: u64) -> String {
    self
      .key_resolver
      .resolve_pkey(&id.type_name(), &id.value(), shard_count)
  }

  fn resolve_skey<AID: AggregateId>(&self, id: &AID, seq_nr: usize) -> String {
    self.key_resolver.resolve_skey(&id.type_name(), &id.value(), seq_nr)
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
