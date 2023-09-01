use anyhow::Result;
use chrono::{DateTime, Utc};
use event_store_adapter_rs::event_store::EventStore;
use event_store_adapter_rs::types::{Aggregate, AggregateId, Event, EventPersistenceGateway};
use event_store_adapter_test_utils_rs::id_generator::id_generate;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use ulid_generator_rs::ULID;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserAccountId {
  value: String,
}

impl UserAccountId {
  pub fn new(value: String) -> Self {
    Self { value }
  }
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
}

impl UserAccount {
  pub fn new(id: UserAccountId, name: String) -> Result<(Self, UserAccountEvent)> {
    let mut my_self = Self {
      id: id.clone(),
      name,
      seq_nr: 0,
      version: 1,
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

  pub fn replay(
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

  pub fn apply_event(&mut self, event: UserAccountEvent) {
    match event {
      UserAccountEvent::Renamed { name, .. } => {
        self.rename(&name).unwrap();
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
}

pub struct UserAccountRepository {
  event_store: EventStore<UserAccount, UserAccountEvent>,
}

impl UserAccountRepository {
  pub fn new(event_store: EventStore<UserAccount, UserAccountEvent>) -> Self {
    Self { event_store }
  }

  pub async fn store(
    &mut self,
    event: &UserAccountEvent,
    version: usize,
    snapshot_opt: Option<&UserAccount>,
  ) -> Result<()> {
    self
      .event_store
      .store_event_with_snapshot_opt(event, version, snapshot_opt)
      .await
  }

  pub async fn find_by_id(&self, id: &UserAccountId) -> Result<UserAccount> {
    let (snapshot, seq_nr, version) = self.event_store.get_snapshot_by_id(id).await?;
    let events = self.event_store.get_events_by_id_and_seq_nr(id, seq_nr).await?;
    let result = UserAccount::replay(events, Some(snapshot), version);
    Ok(result)
  }
}
