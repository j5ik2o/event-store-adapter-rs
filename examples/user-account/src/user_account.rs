use event_store_adapter_rs::AggregateId;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Manifest value carried by the creation event envelope.
pub const CREATED_MANIFEST: &str = "user-account-created/v1";
/// Manifest value carried by the rename event envelope.
pub const RENAMED_MANIFEST: &str = "user-account-renamed/v1";

#[derive(Debug, thiserror::Error)]
pub enum UserAccountError {
  #[error("account already has name {0}")]
  AlreadyRenamed(String),
  #[error("invalid event replay: {0}")]
  InvalidReplay(&'static str),
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

/// Event payload. The envelope carries the ID, sequence number, timestamp and manifest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UserAccountEvent {
  Created { name: String },
  Renamed { name: String },
}

/// Aggregate state. The snapshot envelope carries the replay position.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserAccount {
  id: UserAccountId,
  name: String,
}

impl UserAccount {
  pub fn new(id: UserAccountId, name: String) -> (Self, UserAccountEvent) {
    let my_self = Self { id, name: name.clone() };
    (my_self, UserAccountEvent::Created { name })
  }

  pub fn replay_from(
    id: &UserAccountId,
    snapshot: Option<Self>,
    events: impl IntoIterator<Item = UserAccountEvent>,
  ) -> Result<Self, UserAccountError> {
    let mut state = snapshot;
    for event in events {
      match (state.as_mut(), event) {
        (None, UserAccountEvent::Created { name }) => state = Some(Self { id: id.clone(), name }),
        (Some(account), UserAccountEvent::Renamed { name }) => account.name = name,
        (None, UserAccountEvent::Renamed { .. }) => {
          return Err(UserAccountError::InvalidReplay("rename before creation"));
        }
        (Some(_), UserAccountEvent::Created { .. }) => {
          return Err(UserAccountError::InvalidReplay("duplicate creation"));
        }
      }
    }
    state.ok_or(UserAccountError::InvalidReplay("creation event is missing"))
  }

  pub fn rename(&mut self, name: &str) -> Result<UserAccountEvent, UserAccountError> {
    if self.name == name {
      return Err(UserAccountError::AlreadyRenamed(name.to_string()));
    }
    self.name = name.to_string();
    Ok(UserAccountEvent::Renamed { name: name.to_string() })
  }
}

#[cfg(test)]
#[path = "user_account_test.rs"]
mod tests;
