use crate::user_account::{UserAccount, UserAccountEvent, UserAccountId};
use anyhow::Result;
use event_store_adapter_rs::event_store::EventStoreForDynamoDB;
use event_store_adapter_rs::types::{Aggregate, EventStore};

pub struct UserAccountRepository {
  event_store: EventStoreForDynamoDB<UserAccountId, UserAccount, UserAccountEvent>,
}

impl UserAccountRepository {
  pub fn new(event_store: EventStoreForDynamoDB<UserAccountId, UserAccount, UserAccountEvent>) -> Self {
    Self { event_store }
  }

  pub async fn store_event(&mut self, event: &UserAccountEvent, version: usize) -> Result<()> {
    return self.event_store.persist_event(event, version).await;
  }

  pub async fn store_event_and_snapshot(&mut self, event: &UserAccountEvent, snapshot: &UserAccount) -> Result<()> {
    return self.event_store.persist_event_and_snapshot(event, snapshot).await;
  }

  pub async fn find_by_id(&self, id: &UserAccountId) -> Result<Option<UserAccount>> {
    let snapshot = self.event_store.get_latest_snapshot_by_id(id).await?;
    match snapshot {
      Some(snapshot) => {
        let events = self
          .event_store
          .get_events_by_id_since_seq_nr(id, snapshot.seq_nr() + 1)
          .await?;
        let result = UserAccount::replay(events, snapshot);
        Ok(Some(result))
      }
      None => Ok(None),
    }
  }
}
