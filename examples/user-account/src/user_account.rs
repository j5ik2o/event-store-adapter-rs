use event_store_adapter_rs::types::AggregateId;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

// FR8.4 / FR3.1 / FR3.2: v3 example domain model. The `Event` / `Aggregate` traits are gone;
// the event and the aggregate state are plain serde types (payloads), and the metadata
// (aggregate_id / seq_nr / occurred_at / manifest) travels in the envelopes.

/// Manifest value carried by the creation event envelope (FR1.2 — user-supplied, free-form).
pub const CREATED_MANIFEST: &str = "user-account-created/v1";
/// Manifest value carried by the rename event envelope.
pub const RENAMED_MANIFEST: &str = "user-account-renamed/v1";

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

/// Event payload: pure domain content only — no event ID, no seq_nr, no timestamp.
/// The envelope carries those (FR1.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UserAccountEvent {
  Created { name: String },
  Renamed { name: String },
}

/// Aggregate payload: pure domain state only — no seq_nr, no version, no last_updated_at.
/// The snapshot envelope carries seq_nr / version (FR2.1 / FR3.2).
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

  pub fn replay(events: impl IntoIterator<Item = UserAccountEvent>, snapshot: UserAccount) -> Self {
    events.into_iter().fold(snapshot, |mut result, event| {
      result.apply_event(&event);
      result
    })
  }

  fn apply_event(&mut self, event: &UserAccountEvent) {
    if let UserAccountEvent::Renamed { name } = event {
      self.name = name.clone();
    }
  }

  pub fn rename(&mut self, name: &str) -> Result<UserAccountEvent, UserAccountError> {
    if self.name == name {
      return Err(UserAccountError::AlreadyRenamed(name.to_string()));
    }
    self.name = name.to_string();
    Ok(UserAccountEvent::Renamed { name: name.to_string() })
  }
}
