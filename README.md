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
use event_store_adapter_rs::event_envelope::EventEnvelope;
use event_store_adapter_rs::types::{EventStore, EventStoreReadError, EventStoreWriteError};
use event_store_adapter_rs::EventStoreForSqlite;

// UserAccount / UserAccountEvent are plain `#[derive(Serialize, Deserialize)]` types —
// v3 requires no library traits on your domain types.
pub struct UserAccountRepository {
  event_store: EventStoreForSqlite<UserAccountId, UserAccount, UserAccountEvent>,
}

/// Read result restored from the latest snapshot plus the differential replay.
/// `seq_nr` numbers the next event as `seq_nr + 1`; `version` is the expected_version for the next write.
pub struct ReplayedUserAccount {
  pub state: UserAccount,
  pub seq_nr: usize,
  pub version: usize,
}

impl UserAccountRepository {
  pub async fn store_event(
    &mut self,
    event: EventEnvelope<UserAccountId, UserAccountEvent>,
    expected_version: usize,
  ) -> Result<(), RepositoryError> {
    self
      .event_store
      .persist_event(event, expected_version)
      .await
      .map_err(Self::handle_event_store_write_error)
  }

  pub async fn store_event_and_snapshot(
    &mut self,
    event: EventEnvelope<UserAccountId, UserAccountEvent>,
    snapshot: UserAccount,
    expected_version: usize,
  ) -> Result<(), RepositoryError> {
    self
      .event_store
      .persist_event_and_snapshot(event, snapshot, expected_version)
      .await
      .map_err(Self::handle_event_store_write_error)
  }

  pub async fn find_by_id(&self, id: &UserAccountId) -> Result<Option<ReplayedUserAccount>, RepositoryError> {
    let snapshot = match self.event_store.get_latest_snapshot_by_id(id).await {
      Ok(Some(snapshot)) => snapshot,
      Ok(None) => return Ok(None),
      Err(err) => return Err(Self::handle_event_store_read_error(err)),
    };
    let (snapshot_seq_nr, version) = (snapshot.seq_nr(), snapshot.version());
    let events = self
      .event_store
      .get_events_by_id_since_seq_nr(id, snapshot_seq_nr + 1)
      .await
      .map_err(Self::handle_event_store_read_error)?;
    let seq_nr = events.last().map(|event| event.seq_nr()).unwrap_or(snapshot_seq_nr);
    let state = UserAccount::replay(
      events.into_iter().map(EventEnvelope::into_payload),
      snapshot.into_aggregate(),
    );
    Ok(Some(ReplayedUserAccount { state, seq_nr, version }))
  }
}
```

The following is an example of the repository usage with SQLite. The store persists to a database file (or to `:memory:`) and creates the required tables and indexes automatically on construction — no DDL on your side:

```rust
// A file-backed database. Use EventStoreForSqlite::new_in_memory() for `:memory:`.
let event_store = EventStoreForSqlite::new("user-account.db")?;
let mut repository = UserAccountRepository::new(event_store);

// Create: the first event of a stream is seq_nr == 1 and is written with expected_version == 0.
let (user_account, created) = UserAccount::new(user_account_id.clone(), "test-1".to_string());
let envelope = EventEnvelope::new(user_account_id.clone(), 1, Utc::now(), created).with_manifest(CREATED_MANIFEST);
repository.store_event_and_snapshot(envelope, user_account, 0).await?;

// Replay the aggregate from the event store: seq_nr / version come from the
// envelopes (the store columns), not from aggregate fields.
let mut replayed = repository.find_by_id(&user_account_id).await?.unwrap();

// Execute a command, number the next event as replayed.seq_nr + 1, and pass the
// replayed version as expected_version for the optimistic lock.
let renamed = replayed.state.rename("new-name").unwrap();
let envelope = EventEnvelope::new(user_account_id.clone(), replayed.seq_nr + 1, Utc::now(), renamed);
repository.store_event(envelope, replayed.version).await?;
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

## Migration to 3.x

The 3.x line replaces the `Event` / `Aggregate` traits with the `EventEnvelope` API: domain event and aggregate types are plain `serde` types, metadata (`aggregate_id` / `seq_nr` / `occurred_at` / `manifest`) travels in the envelope, and the optimistic-lock version lives in the store columns only. **Data written by 2.x cannot be read by 3.x** — migrating stored data is the user's responsibility. See [docs/MIGRATION_GUIDE_v3.md](docs/MIGRATION_GUIDE_v3.md) ([日本語](docs/MIGRATION_GUIDE_v3.ja.md)) for the complete guide.

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
