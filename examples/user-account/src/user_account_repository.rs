use crate::user_account::{UserAccount, UserAccountEvent, UserAccountId};
use event_store_adapter_rs::event_store::EventStoreForDynamoDB;
use event_store_adapter_rs::types::{Aggregate, EventStore, EventStoreReadError, EventStoreWriteError};

pub struct UserAccountRepository {
  event_store: EventStoreForDynamoDB<UserAccountId, UserAccount, UserAccountEvent>,
}

#[derive(Debug)]
pub enum RepositoryError {
  OptimisticLockError(String),
  IOError(String),
}

impl UserAccountRepository {
  pub fn new(event_store: EventStoreForDynamoDB<UserAccountId, UserAccount, UserAccountEvent>) -> Self {
    Self { event_store }
  }

  pub async fn store_event(&mut self, event: &UserAccountEvent, version: usize) -> Result<(), RepositoryError> {
    let result = self.event_store.persist_event(event, version).await;
    match result {
      Ok(_) => Ok(()),
      Err(err) => Err(Self::handle_event_store_write_error(err)),
    }
  }

  pub async fn store_event_and_snapshot(
    &mut self,
    event: &UserAccountEvent,
    snapshot: &UserAccount,
  ) -> Result<(), RepositoryError> {
    let result = self.event_store.persist_event_and_snapshot(event, snapshot).await;
    match result {
      Ok(_) => Ok(()),
      Err(err) => Err(Self::handle_event_store_write_error(err)),
    }
  }

  pub async fn find_by_id(&self, id: &UserAccountId) -> Result<Option<UserAccount>, RepositoryError> {
    let snapshot_result = self.event_store.get_latest_snapshot_by_id(id).await;
    match snapshot_result {
      Ok(snapshot_opt) => match snapshot_opt {
        Some(snapshot) => {
          let events = self
            .event_store
            .get_events_by_id_since_seq_nr(id, snapshot.seq_nr() + 1)
            .await;
          match events {
            Ok(events) => Ok(Some(UserAccount::replay(events, snapshot))),
            Err(err) => Err(Self::handle_event_store_read_error(err)),
          }
        }
        None => Ok(None),
      },
      Err(err) => Err(Self::handle_event_store_read_error(err)),
    }
  }

  fn handle_event_store_write_error(err: EventStoreWriteError) -> RepositoryError {
    match err {
      EventStoreWriteError::OptimisticLockError(e) => RepositoryError::OptimisticLockError(e.to_string()),
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
