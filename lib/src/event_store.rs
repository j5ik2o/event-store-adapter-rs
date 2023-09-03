use std::fmt::Debug;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{AttributeValue, Put, TransactWriteItem, Update};
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
      .expression_attribute_values(":seq_nr", AttributeValue::N(0.to_string()))
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
        let builder = self
          .client
          .transact_write_items()
          .transact_items(
            TransactWriteItem::builder()
              .put(self.put_snapshot(event, 0, ar)?)
              .build(),
          )
          .transact_items(TransactWriteItem::builder().put(self.put_journal(event)?).build());
        builder.send().await?;
      }
      (true, None) => {
        panic!("Aggregate is not found");
      }
      (false, ar) => {
        let builder = self
          .client
          .transact_write_items()
          .transact_items(
            TransactWriteItem::builder()
              .update(self.update_snapshot(event, 0, version, ar)?)
              .build(),
          )
          .transact_items(TransactWriteItem::builder().put(self.put_journal(event)?).build());
        builder.send().await?;
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

#[cfg(test)]
mod tests {
  use std::env;
  use std::fmt::{Display, Formatter};
  use std::thread::sleep;
  use std::time::Duration;

  use anyhow::Result;

  use chrono::{DateTime, Utc};
  use event_store_adapter_test_utils_rs::docker::dynamodb_local;
  use event_store_adapter_test_utils_rs::dynamodb::{
    create_client, create_journal_table, create_snapshot_table, wait_table,
  };
  use event_store_adapter_test_utils_rs::id_generator::id_generate;
  use serde::{Deserialize, Serialize};
  use testcontainers::clients::Cli;

  use ulid_generator_rs::ULID;

  use crate::event_store::EventStore;
  use crate::types::{Aggregate, AggregateId, Event, EventPersistenceGateway};

  #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
  pub struct UserAccountId {
    value: String,
  }

  impl Display for UserAccountId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
      write!(f, "{}", self.value)
    }
  }

  impl AggregateId for UserAccountId {
    fn type_name(&self) -> String {
      "UserAccount".to_string()
    }

    fn value(&self) -> String {
      self.value.clone()
    }
  }

  #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
  pub enum UserAccountEvent {
    Created {
      id: ULID,
      aggregate_id: UserAccountId,
      seq_nr: usize,
      name: String,
      occurred_at: chrono::DateTime<chrono::Utc>,
    },
    Renamed {
      id: ULID,
      aggregate_id: UserAccountId,
      seq_nr: usize,
      name: String,
      occurred_at: chrono::DateTime<chrono::Utc>,
    },
  }

  impl Event for UserAccountEvent {
    type AggregateID = UserAccountId;
    type ID = ULID;

    fn id(&self) -> &Self::ID {
      match self {
        UserAccountEvent::Created { id, .. } => id,
        UserAccountEvent::Renamed { id, .. } => id,
      }
    }

    fn aggregate_id(&self) -> &Self::AggregateID {
      match self {
        UserAccountEvent::Created { aggregate_id, .. } => aggregate_id,
        UserAccountEvent::Renamed { aggregate_id, .. } => aggregate_id,
      }
    }

    fn seq_nr(&self) -> usize {
      match self {
        UserAccountEvent::Created { seq_nr, .. } => *seq_nr,
        UserAccountEvent::Renamed { seq_nr, .. } => *seq_nr,
      }
    }

    fn occurred_at(&self) -> &DateTime<Utc> {
      match self {
        UserAccountEvent::Created { occurred_at, .. } => occurred_at,
        UserAccountEvent::Renamed { occurred_at, .. } => occurred_at,
      }
    }

    fn is_created(&self) -> bool {
      match self {
        UserAccountEvent::Created { .. } => true,
        UserAccountEvent::Renamed { .. } => false,
      }
    }
  }

  #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
  pub struct UserAccount {
    id: UserAccountId,
    name: String,
    seq_nr: usize,
    version: usize,
    last_updated_at: DateTime<Utc>,
  }

  impl UserAccount {
    fn new(id: UserAccountId, name: String) -> Result<(Self, UserAccountEvent)> {
      let mut my_self = Self {
        id: id.clone(),
        name,
        seq_nr: 0,
        version: 1,
        last_updated_at: chrono::Utc::now(),
      };
      my_self.seq_nr += 1;
      let event = UserAccountEvent::Created {
        id: id_generate(),
        aggregate_id: id,
        seq_nr: my_self.seq_nr,
        name: my_self.name.clone(),
        occurred_at: chrono::Utc::now(),
      };
      Ok((my_self, event))
    }

    fn replay(
      events: impl IntoIterator<Item = UserAccountEvent>,
      snapshot_opt: Option<UserAccount>,
      version: usize,
    ) -> Self {
      let mut result = events
        .into_iter()
        .fold(snapshot_opt, |result, event| match (result, event) {
          (Some(mut this), event) => {
            this.apply_event(event.clone());
            Some(this)
          }
          (..) => None,
        })
        .unwrap();
      result.version = version;
      result
    }

    fn apply_event(&mut self, event: UserAccountEvent) {
      match event {
        UserAccountEvent::Renamed { name, .. } => {
          self.name = name;
        }
        _ => {}
      }
    }

    pub fn rename(&mut self, name: &str) -> Result<UserAccountEvent> {
      self.name = name.to_string();
      self.seq_nr += 1;
      let event = UserAccountEvent::Renamed {
        id: id_generate(),
        aggregate_id: self.id.clone(),
        seq_nr: self.seq_nr,
        name: name.to_string(),
        occurred_at: chrono::Utc::now(),
      };
      Ok(event)
    }
  }

  impl Aggregate for UserAccount {
    type ID = UserAccountId;

    fn id(&self) -> &Self::ID {
      &self.id
    }

    fn seq_nr(&self) -> usize {
      self.seq_nr
    }

    fn version(&self) -> usize {
      self.version
    }

    fn set_version(&mut self, version: usize) {
      self.version = version
    }

    fn last_updated_at(&self) -> &DateTime<Utc> {
      todo!()
    }
  }

  async fn find_by_id(
    event_store: &mut EventStore<UserAccount, UserAccountEvent>,
    id: &UserAccountId,
  ) -> Result<UserAccount> {
    let (snapshot, seq_nr, version) = event_store.get_snapshot_by_id(id).await?;
    let events = event_store.get_events_by_id_and_seq_nr(id, seq_nr).await?;
    let user_account = UserAccount::replay(events, Some(snapshot), version);
    Ok(user_account)
  }

  #[tokio::test]
  async fn test_event_store() {
    env::set_var("RUST_LOG", "debug");
    let _ = env_logger::builder().is_test(true).try_init();

    let docker = Cli::docker();
    let dynamodb_node = dynamodb_local(&docker);
    let port = dynamodb_node.get_host_port_ipv4(4566);
    log::debug!("DynamoDB port: {}", port);

    let test_time_factor = env::var("TEST_TIME_FACTOR")
      .unwrap_or("1".to_string())
      .parse::<f32>()
      .unwrap();

    sleep(Duration::from_millis((1000f32 * test_time_factor) as u64));

    let client = create_client(port);

    let journal_table_name = "journal";
    let journal_aid_index_name = "journal-aid-index";
    let _journal_table_output = create_journal_table(&client, journal_table_name, journal_aid_index_name).await;

    let snapshot_table_name = "snapshot";
    let snapshot_aid_index_name = "snapshot-aid-index";
    let _snapshot_table_output = create_snapshot_table(&client, snapshot_table_name, snapshot_aid_index_name).await;

    while !(wait_table(&client, journal_table_name).await) {
      log::info!("Waiting for journal table to be created");
      sleep(Duration::from_millis((1000f32 * test_time_factor) as u64));
    }

    while !(wait_table(&client, snapshot_table_name).await) {
      log::info!("Waiting for snapshot table to be created");
      sleep(Duration::from_millis((1000f32 * test_time_factor) as u64));
    }

    let mut event_store = EventStore::new(
      client.clone(),
      journal_table_name.to_string(),
      journal_aid_index_name.to_string(),
      snapshot_table_name.to_string(),
      snapshot_aid_index_name.to_string(),
      64,
    );

    let (user_account, event) = UserAccount::new(UserAccountId { value: "1".to_string() }, "test".to_string()).unwrap();
    event_store
      .store_event_with_snapshot_opt(&event, user_account.version(), Some(&user_account))
      .await
      .unwrap();
    let id = UserAccountId { value: "1".to_string() };
    let mut user_account = find_by_id(&mut event_store, &id).await.unwrap();

    let event = user_account.rename("test2").unwrap();

    event_store
      .store_event_with_snapshot_opt(&event, user_account.version(), None)
      .await
      .unwrap();

    let user_account = find_by_id(&mut event_store, &id).await.unwrap();

    assert_eq!(user_account.name, "test2");
  }
}
