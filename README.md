# event-store-adapter-rs

This library is designed to turn DynamoDB into an Event Store for Event Sourcing.

## Usage

You can easily implement an Event Sourcing-enabled repository using EventStore.

```rust
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
```
