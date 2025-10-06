#![cfg(test)]

use std::error::Error as StdError;
use std::fmt::{Display, Formatter};

use chrono::{DateTime, Utc};
use event_store_adapter_test_utils_rs::id_generator::id_generate;
use serde::{Deserialize, Serialize};
use ulid_generator_rs::ULID;

use crate::types::{Aggregate, AggregateId, Event, EventStore};

#[derive(Debug)]
pub enum UserAccountError {
  AlreadyRenamed(#[allow(dead_code)] String),
}

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
    occurred_at: DateTime<Utc>,
  },
  Renamed {
    id: ULID,
    aggregate_id: UserAccountId,
    seq_nr: usize,
    name: String,
    occurred_at: DateTime<Utc>,
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
    matches!(self, UserAccountEvent::Created { .. })
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
  pub fn new(id: UserAccountId, name: String) -> (Self, UserAccountEvent) {
    let mut my_self = Self {
      id: id.clone(),
      name,
      seq_nr: 0,
      version: 1,
      last_updated_at: Utc::now(),
    };
    my_self.seq_nr += 1;
    let event = UserAccountEvent::Created {
      id: id_generate(),
      aggregate_id: id,
      seq_nr: my_self.seq_nr,
      name: my_self.name.clone(),
      occurred_at: Utc::now(),
    };
    (my_self, event)
  }

  fn replay(events: impl IntoIterator<Item = UserAccountEvent>, snapshot: UserAccount) -> Self {
    events.into_iter().fold(snapshot, |mut result, event| {
      result.apply_event(event);
      result
    })
  }

  fn apply_event(&mut self, event: UserAccountEvent) {
    if let UserAccountEvent::Renamed { name, .. } = event {
      self.rename(&name).unwrap();
    }
  }

  pub fn rename(&mut self, name: &str) -> Result<UserAccountEvent, UserAccountError> {
    if self.name == name {
      return Err(UserAccountError::AlreadyRenamed(name.to_string()));
    }
    self.name = name.to_string();
    self.seq_nr += 1;
    let event = UserAccountEvent::Renamed {
      id: id_generate(),
      aggregate_id: self.id.clone(),
      seq_nr: self.seq_nr,
      name: name.to_string(),
      occurred_at: Utc::now(),
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
    &self.last_updated_at
  }
}

pub async fn find_by_id<T>(
  event_store: &mut T,
  id: &UserAccountId,
) -> Result<Option<UserAccount>, Box<dyn StdError + Send + Sync>>
where
  T: EventStore<AID = UserAccountId, AG = UserAccount, EV = UserAccountEvent>, {
  let snapshot = event_store.get_latest_snapshot_by_id(id).await?;
  match snapshot {
    Some(snapshot) => {
      let events = event_store
        .get_events_by_id_since_seq_nr(id, snapshot.seq_nr() + 1)
        .await?;
      let user_account = UserAccount::replay(events, snapshot);
      Ok(Some(user_account))
    }
    None => Ok(None),
  }
}

pub async fn exercise_user_account_flow<T>(
  event_store: &mut T,
  id: &UserAccountId,
) -> Result<(), Box<dyn StdError + Send + Sync>>
where
  T: EventStore<AID = UserAccountId, AG = UserAccount, EV = UserAccountEvent>, {
  let (user_account, event) = UserAccount::new(id.clone(), "test".to_string());
  event_store.persist_event_and_snapshot(&event, &user_account).await?;

  let mut user_account = find_by_id(event_store, id).await?.unwrap();
  assert_eq!(user_account.name, "test");
  assert_eq!(user_account.seq_nr, 1);
  assert_eq!(user_account.version, 1);

  let event = user_account.rename("test2").unwrap();
  event_store.persist_event(&event, user_account.version()).await?;

  let mut user_account = find_by_id(event_store, id).await?.unwrap();
  assert_eq!(user_account.name, "test2");
  assert_eq!(user_account.seq_nr, 2);
  assert_eq!(user_account.version, 2);

  let event = user_account.rename("test3").unwrap();
  event_store.persist_event_and_snapshot(&event, &user_account).await?;

  let user_account = find_by_id(event_store, id).await?.unwrap();
  assert_eq!(user_account.name, "test3");
  assert_eq!(user_account.seq_nr, 3);
  assert_eq!(user_account.version, 3);

  Ok(())
}

pub fn init_tracing() {
  let _ = tracing_subscriber::fmt()
    .with_max_level(tracing::Level::INFO)
    .with_target(false)
    .with_ansi(false)
    .without_time()
    .try_init();
}
