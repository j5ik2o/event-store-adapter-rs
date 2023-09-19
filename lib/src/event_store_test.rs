use std::env;
use std::fmt::{Display, Formatter};
use std::thread::sleep;

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

use crate::event_store::EventStoreForDynamoDB;
use crate::types::{Aggregate, AggregateId, Event, EventStore};

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

  fn replay(events: impl IntoIterator<Item = UserAccountEvent>, snapshot: UserAccount) -> Self {
    events.into_iter().fold(snapshot, |mut result, event| {
      result.apply_event(event.clone());
      result
    })
  }

  fn apply_event(&mut self, event: UserAccountEvent) {
    if let UserAccountEvent::Renamed { name, .. } = event {
      self.rename(&name).unwrap();
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
  event_store: &mut EventStoreForDynamoDB<UserAccountId, UserAccount, UserAccountEvent>,
  id: &UserAccountId,
) -> Result<Option<UserAccount>> {
  let snapshot = event_store.get_latest_snapshot_by_id(id).await?;
  match snapshot {
    Some(snapshot) => {
      let events = event_store
        .get_events_by_id_since_seq_nr(id, snapshot.seq_nr + 1)
        .await?;
      let user_account = UserAccount::replay(events, snapshot);
      Ok(Some(user_account))
    }
    None => Ok(None),
  }
}

#[tokio::test]
async fn test_event_store() {
  env::set_var("RUST_LOG", "event_store_adapter_rs=debug");
  let _ = env_logger::builder().is_test(true).try_init();

  let docker = Cli::docker();
  let dynamodb_node = dynamodb_local(&docker);
  let port = dynamodb_node.get_host_port_ipv4(4566);
  log::debug!("DynamoDB port: {}", port);

  let test_time_factor = env::var("TEST_TIME_FACTOR")
    .unwrap_or("1".to_string())
    .parse::<f32>()
    .unwrap();

  sleep(std::time::Duration::from_millis((1000f32 * test_time_factor) as u64));

  let client = create_client(port);

  let journal_table_name = "journal";
  let journal_aid_index_name = "journal-aid-index";
  let _journal_table_output = create_journal_table(&client, journal_table_name, journal_aid_index_name).await;

  let snapshot_table_name = "snapshot";
  let snapshot_aid_index_name = "snapshot-aid-index";
  let _snapshot_table_output = create_snapshot_table(&client, snapshot_table_name, snapshot_aid_index_name).await;

  while !(wait_table(&client, journal_table_name).await) {
    log::info!("Waiting for journal table to be created");
    sleep(std::time::Duration::from_millis((1000f32 * test_time_factor) as u64));
  }

  while !(wait_table(&client, snapshot_table_name).await) {
    log::info!("Waiting for snapshot table to be created");
    sleep(std::time::Duration::from_millis((1000f32 * test_time_factor) as u64));
  }

  let mut event_store = EventStoreForDynamoDB::new(
    client.clone(),
    journal_table_name.to_string(),
    journal_aid_index_name.to_string(),
    snapshot_table_name.to_string(),
    snapshot_aid_index_name.to_string(),
    64,
  )
  .with_keep_snapshot_count(Some(1))
  .with_delete_ttl(Some(chrono::Duration::seconds(5)));

  let id_value = id_generate();
  let id = UserAccountId {
    value: id_value.to_string(),
  };

  let (user_account, event) = UserAccount::new(id.clone(), "test".to_string()).unwrap();
  // Persist the event and the snapshot
  event_store
    .persist_event_and_snapshot(&event, &user_account)
    .await
    .unwrap();

  let mut user_account = find_by_id(&mut event_store, &id).await.unwrap().unwrap();
  assert_eq!(user_account.name, "test");
  assert_eq!(user_account.seq_nr, 1);
  assert_eq!(user_account.version, 1);

  let event = user_account.rename("test2").unwrap();

  // Persist the event only.
  event_store.persist_event(&event, user_account.version()).await.unwrap();

  let mut user_account = find_by_id(&mut event_store, &id).await.unwrap().unwrap();
  assert_eq!(user_account.name, "test2");
  assert_eq!(user_account.seq_nr, 2);
  assert_eq!(user_account.version, 2);

  let event = user_account.rename("test3").unwrap();

  // Persist the event and the snapshot
  event_store
    .persist_event_and_snapshot(&event, &user_account)
    .await
    .unwrap();

  let user_account = find_by_id(&mut event_store, &id).await.unwrap().unwrap();
  assert_eq!(user_account.name, "test3");
  assert_eq!(user_account.seq_nr, 3);
  assert_eq!(user_account.version, 3);
}
