# event-store-adapter-rs

[![Workflow Status](https://github.com/j5ik2o/event-store-adapter-rs/workflows/ci/badge.svg)](https://github.com/j5ik2o/event-store-adapter-rs/actions?query=workflow%3A%22ci%22)
[![crates.io](https://img.shields.io/crates/v/event-store-adapter-rs.svg)](https://crates.io/crates/event-store-adapter-rs)
[![docs.rs](https://docs.rs/event-store-adapter-rs/badge.svg)](https://docs.rs/event-store-adapter-rs)
[![Renovate](https://img.shields.io/badge/renovate-enabled-brightgreen.svg)](https://renovatebot.com)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![](https://tokei.rs/b1/github/j5ik2o/event-store-adapter-rs)](https://github.com/XAMPPRocky/tokei)


This library provides Memory and DynamoDB event stores for CQRS/Event Sourcing.

> This checkout is developing the unpublished 4.x API (`4.0.0-alpha.0`). The examples below use a local checkout. SQLite and Bigtable users should stay on the 3.x line. Stored v3 DynamoDB data is supported only through the explicit migration function or CLI.

[日本語](./README.ja.md)

## Backends and features

| Feature | Availability |
|:--------|:-------------|
| (none) | `EventStoreForMemory` and `MemoryStorage`, always available |
| `dynamodb` | `EventStoreForDynamoDB`, using `aws-sdk-dynamodb` |
| `migration` | `migrate_v3_dynamodb`; also enables `dynamodb` |

Default features are empty. The `sqlite`, `sqlite-system`, and `bigtable` features have been removed. The public API is exported from the crate root; there is no `next` namespace or old API alias.

## Memory example

Create an application with these dependencies, replacing the path with your checkout:

```toml
[dependencies]
event-store-adapter-rs = { path = "/path/to/event-store-adapter-rs/lib" }
chrono = "0.4"
serde_json = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

This complete example exercises all four operations. Two stores share one `MemoryStorage`; a separately created storage remains isolated.

```rust
use chrono::Utc;
use event_store_adapter_rs::{
  AggregateId, EventEnvelope, EventStore, EventStoreError, EventStoreForMemory,
  MemoryStorage, RetentionSettings, SnapshotEnvelope,
};
use serde_json::{json, Value};

#[derive(Debug, Clone)]
struct AccountId(String);

impl AggregateId for AccountId {
  fn type_name(&self) -> String { "Account".into() }
  fn value(&self) -> String { self.0.clone() }
}

#[tokio::main]
async fn main() -> Result<(), EventStoreError> {
  let storage = MemoryStorage::new(RetentionSettings::keep_latest(2))?;
  let writer: EventStoreForMemory<AccountId, Value, Value> =
    EventStoreForMemory::new(storage.clone());
  let reader: EventStoreForMemory<AccountId, Value, Value> =
    EventStoreForMemory::new(storage);
  let isolated: EventStoreForMemory<AccountId, Value, Value> =
    EventStoreForMemory::new(MemoryStorage::new(RetentionSettings::current_only())?);
  let id = AccountId("1".into());

  writer.persist_event(
    EventEnvelope::new(id.clone(), 1, Utc::now(), json!({"created": "Alice"}))
      .with_manifest("account-created/v1"),
  ).await?;
  let first = reader.get_latest_snapshot_by_id(&id).await?.unwrap();
  assert_eq!(first.head_seq_nr(), 1);
  assert!(first.snapshot().is_none());

  writer.persist_event_and_snapshot(
    EventEnvelope::new(id.clone(), 2, Utc::now(), json!({"renamed": "Bob"})),
    SnapshotEnvelope::new(json!({"name": "Bob"}), 2).with_manifest("account/v1"),
  ).await?;
  let events = reader.get_events_by_id_since_seq_nr(&id, 1).await?;
  assert_eq!(events.len(), 2);
  assert_eq!(events[1].seq_nr(), 2);
  let latest = reader.get_latest_snapshot_by_id(&id).await?.unwrap();
  assert_eq!(latest.head_seq_nr(), 2);
  assert_eq!(latest.snapshot().unwrap().aggregate(), &json!({"name": "Bob"}));
  assert!(isolated.get_latest_snapshot_by_id(&id).await?.is_none());
  Ok(())
}
```

`AggregateId` supplies the type name and value. The library constructs `aid` as `type_name-value`; type names cannot contain a hyphen and the complete ID is limited to 1024 UTF-8 bytes. It does not use a domain `Display` implementation or a `KeyResolver`.

`SeqNr` is `u64`, limited to `0..=SEQ_NR_MAX` (`2^53 - 1`). The first write uses 1 and later writes must be contiguous. Reads include the lower bound; 0 reads from the beginning. There is no `expected_version` argument or snapshot `version`. `SnapshotEnvelope::seq_nr()` describes the snapshot; `SnapshotRead::head_seq_nr()` describes the event head. A stream with events but no snapshot returns `Some(SnapshotRead)` with `snapshot() == None`. Replay starts from the creation event in that case.

## DynamoDB example

Enable `features = ["dynamodb"]`. Provision three distinct tables and the snapshot history GSI before calling the asynchronous `EventStoreForDynamoDB::open(client, tables, options)`. Table creation is outside the library. `DynamoDbTables` supplies the three table names and GSI name; `DynamoDbOptions` supplies retention and bounded retry settings.

The SDK client must have a retry sleeper. The runnable [user-account example](examples/user-account/src/main.rs) installs a Tokio sleeper, starts DynamoDB Local, creates the tables, and calls the new `open`. Its [repository](examples/user-account/src/user_account_repository.rs) writes an event, writes an event plus snapshot, reads snapshots, and replays events. It verifies creation without a snapshot, a snapshot at the head, and later events after the snapshot:

```sh
cargo +1.99.0 run -p example-user-account
```

Docker is required for this example. See [the DynamoDB schema](docs/DATABASE_SCHEMA.md) for keys, configuration records, transactions, and retention.

## Serialization, retention, and errors

The default constructors use JSON and require `Serialize + DeserializeOwned` on event and aggregate payloads. Use `EventStoreForMemory::with_serializers` or `EventStoreForDynamoDB::open_with_serializers` with `Arc<dyn EventSerializer<P>>` and `Arc<dyn SnapshotSerializer<A>>` for another byte format. These serializers receive only the payload. Payloads require `Send + Sync + 'static`; serde, Clone, and Debug are not required for these constructors. See the [Memory](lib/tests/memory_test.rs) and [DynamoDB](lib/tests/dynamodb_persist_event_test.rs) integration tests for non-serde payloads.

`RetentionSettings::current_only()` is the default. `keep_latest(n)` retains the newest `n` history snapshots; 0 is invalid. Memory supports deletion and rejects TTL combined with a history count. DynamoDB supports deletion or `RetentionMode::Ttl { grace_seconds }`; configure the snapshot table's TTL attribute `ttl` separately. With a history count, Memory runs retention after successful appends, including event-only appends. DynamoDB runs retention only when a history count is configured and an append that writes a history snapshot succeeds; event-only appends do not run retention. A retention failure emits a `tracing` warning with aid, seq_nr, phase, and error; the committed write still succeeds.

`EventStoreError` distinguishes `OptimisticLock`, `ContractViolation`, `Serialization`, `Configuration`, and `Storage`. Match `ContractRule`, `SerializationPhase`, `ConfigurationReason`, or `StorageOperation` rather than parsing messages. Serialization and storage errors retain their source.

## Migration

The normal stores read only the new layout. For v3's default DynamoDB layout, use the feature-gated `migrate_v3_dynamodb` function or the thin CLI described in [the v4 migration guide](docs/MIGRATION_GUIDE_v4.md). Stop old writes and provision empty new tables before migration. Custom `KeyResolver` layouts, SQLite, and Bigtable are outside this migration's scope.

Historical [v3 migration guidance](docs/MIGRATION_GUIDE_v3.md) remains available for users staying on 3.x.

## Validation

```sh
cargo +1.99.0 build --workspace --all-targets --all-features
cargo +1.99.0 clippy --workspace --all-targets --all-features -- -D warnings
cargo +nightly fmt --all -- --check
cargo +1.99.0 test --workspace --all-features
```

DynamoDB and migration integration tests require Docker.

## License

MIT or Apache-2.0. See [LICENSE-MIT](LICENSE-MIT) and [LICENSE-APACHE](LICENSE-APACHE).

## Links

- [Common Documents](https://github.com/j5ik2o/event-store-adapter)
- [CQRS/Event Sourcing Example](https://github.com/j5ik2o/cqrs-es-example-rs)
