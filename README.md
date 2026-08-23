# event-store-adapter-rs

[![Workflow Status](https://github.com/j5ik2o/event-store-adapter-rs/workflows/ci/badge.svg)](https://github.com/j5ik2o/event-store-adapter-rs/actions?query=workflow%3A%22ci%22)
[![crates.io](https://img.shields.io/crates/v/event-store-adapter-rs.svg)](https://crates.io/crates/event-store-adapter-rs)
[![docs.rs](https://docs.rs/event-store-adapter-rs/badge.svg)](https://docs.rs/event-store-adapter-rs)
[![Renovate](https://img.shields.io/badge/renovate-enabled-brightgreen.svg)](https://renovatebot.com)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![](https://tokei.rs/b1/github/j5ik2o/event-store-adapter-rs)](https://github.com/XAMPPRocky/tokei)

This library provides an Event Store for CQRS/Event Sourcing with multiple storage backends: DynamoDB, Google Cloud Bigtable, SQLite, and in-memory.

[日本語](./README.ja.md)

## Backends and Cargo features

No backend is enabled by default. Pick the backend(s) you use via Cargo features:

| Feature | Backend | What it enables |
|:--------|:--------|:----------------|
| `dynamodb` | Amazon DynamoDB (`EventStoreForDynamoDB`) | `aws-config` / `aws-sdk-dynamodb` |
| `bigtable` | Google Cloud Bigtable (`EventStoreForBigtable`) | `tonic` / `googleapis-tonic-google-bigtable-v2` |
| `sqlite` | SQLite with a bundled library (`EventStoreForSqlite`) | `rusqlite` with its `bundled` feature — SQLite is compiled in, no system SQLite required |
| `sqlite-system` | SQLite linked against the system library (`EventStoreForSqlite`) | `rusqlite` without `bundled` — links to the SQLite installed on the system |
| (none) | In-memory (`EventStoreForMemory`) | Always available, no feature required |

```toml
[dependencies]
event-store-adapter-rs = { version = "<latest>", features = ["sqlite"] }
```

Notes:

- Because there is no default feature, existing users must add an explicit `features = [...]` entry when upgrading (see [Migration from 1.x](#migration-from-1x)).
- If both `sqlite` and `sqlite-system` are enabled, the **bundled** SQLite wins. This is a consequence of Cargo feature additivity: `sqlite` turns on `rusqlite/bundled`, and features can only be added, never subtracted. Enable only `sqlite-system` if you want to link against the system SQLite.

## Usage

You can easily implement an Event Sourcing-enabled repository using an event store. The following uses the SQLite backend (`features = ["sqlite"]`):

```rust
use event_store_adapter_rs::types::{Aggregate, EventStore, EventStoreReadError, EventStoreWriteError};
use event_store_adapter_rs::EventStoreForSqlite;

pub struct UserAccountRepository {
  event_store: EventStoreForSqlite<UserAccountId, UserAccount, UserAccountEvent>,
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

The following is an example of the repository usage with SQLite. The store persists to a database file (or to `:memory:`) and creates the required tables and indexes automatically on construction — no DDL on your side:

```rust
// A file-backed database. Use EventStoreForSqlite::new_in_memory() for `:memory:`.
let event_store = EventStoreForSqlite::new("user-account.db")?;

let mut repository = UserAccountRepository::new(event_store);

// Replay the aggregate from the event store
let mut user_account = repository.find_by_id(&user_account_id).await?.unwrap();

// Execute a command on the aggregate
let user_account_event = user_account.rename("new-name").unwrap();

// Store the new event without a snapshot
repository
  .store_event(&user_account_event, user_account.version())
  .await?;
// Store the new event with a snapshot
// repository
//   .store_event_and_snapshot(&user_account_event, &user_account)
//   .await?;
```

A complete runnable example is [examples/user-account-sqlite](examples/user-account-sqlite) (`cargo run -p example-user-account-sqlite` — no cloud connection, no Docker).

With `features = ["dynamodb"]`, the same repository works against DynamoDB by replacing the store construction:

```rust
let event_store = EventStoreForDynamoDB::new(
  aws_dynamodb_client.clone(),
  journal_table_name.to_string(),
  journal_aid_index_name.to_string(),
  snapshot_table_name.to_string(),
  snapshot_aid_index_name.to_string(),
  64,
);
```

A complete runnable DynamoDB example is [examples/user-account](examples/user-account).

### SQLite support boundary

- The supported sharing unit is **one store instance and its clones** (clones share the underlying connection). Opening the same database file from multiple store instances or from multiple processes is **not supported**. Guarding against concurrent multi-process access (for example, preventing multiple instances of a CLI tool from running at once) is the application's responsibility.
- An in-memory store (`new_in_memory`) is shared by the instance and its clones only, and disappears when the last clone is dropped.

## Migration from 1.x

The next major release contains breaking changes (see [CHANGELOG.md](CHANGELOG.md)).

### 1. Backends are now opt-in Cargo features

In 1.x every backend was always compiled. Now there is no default feature, so specify the backend(s) you use:

```toml
# Before (1.x)
[dependencies]
event-store-adapter-rs = "1"

# After — pick your backend(s); "<latest>" is the latest version on crates.io
[dependencies]
event-store-adapter-rs = { version = "<latest>", features = ["dynamodb"] }
```

The in-memory backend (`EventStoreForMemory`) needs no feature and is always available.

### 2. Error type changes

| Item | Before (1.x) | After |
|:-----|:-------------|:------|
| `EventStoreWriteError::OptimisticLockError` | Wrapped the AWS SDK type (`TransactionCanceledException` via `TransactionCanceledExceptionWrapper`) | Carries a backend-neutral `String` message: `optimistic lock failed, aid=<id>, expected_version=<n>[, actual_version=<m>]` |
| In-memory backend failures | Some operations panicked (e.g. unsupported create) | Returns `Err(EventStoreWriteError` / `EventStoreReadError)` — no panic |

If you matched on `OptimisticLockError(cause)` to inspect the AWS SDK error, switch to the message string (or handle the variant without inspecting its payload). Retrying after an optimistic lock failure remains the caller's responsibility.

## Table Specifications

See [docs/DATABASE_SCHEMA.md](docs/DATABASE_SCHEMA.md). Note that for SQLite the tables are created automatically by the library; the document is informational.

## CQRS/Event Sourcing Example

See [j5ik2o/cqrs-es-example-rs](https://github.com/j5ik2o/cqrs-es-example-rs).

## License.

MIT License. See [LICENSE](LICENSE) for details.

## Links

- [Common Documents](https://github.com/j5ik2o/event-store-adapter)
