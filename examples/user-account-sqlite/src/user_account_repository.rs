use crate::user_account::{UserAccount, UserAccountEvent, UserAccountId};
use event_store_adapter_rs::event_envelope::EventEnvelope;
use event_store_adapter_rs::types::{EventStore, EventStoreReadError, EventStoreWriteError};
use event_store_adapter_rs::EventStoreForSqlite;

// FR8.4 / FR2.2 / FR5.1: v3 repository. Writes hand envelopes to the store together with an
// explicit expected_version; reads recover the replay position (seq_nr) and the optimistic-lock
// version from the envelopes instead of aggregate fields (BR2.5 — no set_version write-back).

pub struct UserAccountRepository {
  event_store: EventStoreForSqlite<UserAccountId, UserAccount, UserAccountEvent>,
}

#[derive(Debug)]
pub enum RepositoryError {
  OptimisticLockError(#[allow(dead_code)] String),
  ContractViolation(#[allow(dead_code)] String),
  IOError(#[allow(dead_code)] String),
}

/// Read result restored from the latest snapshot plus the differential replay.
///
/// `seq_nr` numbers the next event as `seq_nr + 1`; `version` is the expected_version
/// for the next write (FR2.2).
#[derive(Debug)]
pub struct ReplayedUserAccount {
  pub state: UserAccount,
  pub seq_nr: usize,
  pub version: usize,
}

impl UserAccountRepository {
  pub fn new(event_store: EventStoreForSqlite<UserAccountId, UserAccount, UserAccountEvent>) -> Self {
    Self { event_store }
  }

  pub async fn store_event(
    &mut self,
    event: EventEnvelope<UserAccountId, UserAccountEvent>,
    expected_version: usize,
  ) -> Result<(), RepositoryError> {
    let result = self.event_store.persist_event(event, expected_version).await;
    match result {
      Ok(_) => Ok(()),
      Err(err) => Err(Self::handle_event_store_write_error(err)),
    }
  }

  pub async fn store_event_and_snapshot(
    &mut self,
    event: EventEnvelope<UserAccountId, UserAccountEvent>,
    snapshot: UserAccount,
    expected_version: usize,
  ) -> Result<(), RepositoryError> {
    let result = self
      .event_store
      .persist_event_and_snapshot(event, snapshot, expected_version)
      .await;
    match result {
      Ok(_) => Ok(()),
      Err(err) => Err(Self::handle_event_store_write_error(err)),
    }
  }

  pub async fn find_by_id(&self, id: &UserAccountId) -> Result<Option<ReplayedUserAccount>, RepositoryError> {
    let snapshot = match self.event_store.get_latest_snapshot_by_id(id).await {
      Ok(Some(snapshot)) => snapshot,
      Ok(None) => return Ok(None),
      Err(err) => return Err(Self::handle_event_store_read_error(err)),
    };
    let snapshot_seq_nr = snapshot.seq_nr();
    let version = snapshot.version();
    let events = match self
      .event_store
      .get_events_by_id_since_seq_nr(id, snapshot_seq_nr + 1)
      .await
    {
      Ok(events) => events,
      Err(err) => return Err(Self::handle_event_store_read_error(err)),
    };
    let seq_nr = events.last().map(|event| event.seq_nr()).unwrap_or(snapshot_seq_nr);
    let state = UserAccount::replay(
      events.into_iter().map(EventEnvelope::into_payload),
      snapshot.into_aggregate(),
    );
    Ok(Some(ReplayedUserAccount { state, seq_nr, version }))
  }

  fn handle_event_store_write_error(err: EventStoreWriteError) -> RepositoryError {
    match err {
      EventStoreWriteError::OptimisticLockError(e) => RepositoryError::OptimisticLockError(e.to_string()),
      EventStoreWriteError::ContractViolation(e) => RepositoryError::ContractViolation(e.to_string()),
      EventStoreWriteError::SerializationError(e) => RepositoryError::IOError(e.to_string()),
      EventStoreWriteError::IOError(e) => RepositoryError::IOError(e.to_string()),
      EventStoreWriteError::OtherError(e) => RepositoryError::IOError(e.to_string()),
    }
  }

  fn handle_event_store_read_error(err: EventStoreReadError) -> RepositoryError {
    match err {
      EventStoreReadError::DeserializationError(e) => RepositoryError::IOError(e.to_string()),
      EventStoreReadError::IOError(e) => RepositoryError::IOError(e.to_string()),
      EventStoreReadError::OtherError(e) => RepositoryError::IOError(e.to_string()),
    }
  }
}
