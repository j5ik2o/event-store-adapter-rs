# event-store-adapter-rs

[![Workflow Status](https://github.com/j5ik2o/event-store-adapter-rs/workflows/ci/badge.svg)](https://github.com/j5ik2o/event-store-adapter-rs/actions?query=workflow%3A%22ci%22)
[![crates.io](https://img.shields.io/crates/v/event-store-adapter-rs.svg)](https://crates.io/crates/event-store-adapter-rs)
[![docs.rs](https://docs.rs/event-store-adapter-rs/badge.svg)](https://docs.rs/event-store-adapter-rs)
[![Renovate](https://img.shields.io/badge/renovate-enabled-brightgreen.svg)](https://renovatebot.com)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![](https://tokei.rs/b1/github/j5ik2o/event-store-adapter-rs)](https://github.com/XAMPPRocky/tokei)

This library is designed to turn DynamoDB into an Event Store for CQRS/Event Sourcing.

[日本語](./README.ja.md)

## Usage

You can easily implement an Event Sourcing-enabled repository using EventStore.

```rust
pub struct UserAccountRepository {
  event_store: EventStore<UserAccount, UserAccountEvent>,
}

impl UserAccountRepository {
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
    
}
```

The following is an example of the repository usage.

```rust
let event_store = EventStore::new(
  aws_dynamodb_client.clone(),
  journal_table_name.to_string(),
  journal_aid_index_name.to_string(),
  snapshot_table_name.to_string(),
  snapshot_aid_index_name.to_string(),
  64,
);
 
let mut repository = UserAccountRepository::new(event_store);
 
// Replay the aggregate from the event store
let mut user_account = repository.find_by_id(user_account_id).await.unwrap();

// Execute a command on the aggregate
let user_account_event = user_account.rename(name).unwrap();
 
// Store the new event without a snapshot
repository
  .store_event(&user_account_event, user_account.version())
  .await
// Store the new event with a snapshot
//  repository
//  .store_event_and_snapshot(&user_account_event, &user_account)
//  .await
```

## Table Specifications

See [docs/DATABASE_SCHEMA.md](docs/DATABASE_SCHEMA.md).

## CQRS/Event Sourcing Example

See [j5ik2o/cqrs-es-example-rs](https://github.com/j5ik2o/cqrs-es-example-rs).

## License.

MIT License. See [LICENSE](LICENSE) for details.

## Links

- [Common Documents](https://github.com/j5ik2o/event-store-adapter)


