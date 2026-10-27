# event-store-adapter-rs

[![Workflow Status](https://github.com/j5ik2o/event-store-adapter-rs/workflows/ci/badge.svg)](https://github.com/j5ik2o/event-store-adapter-rs/actions?query=workflow%3A%22ci%22)
[![crates.io](https://img.shields.io/crates/v/event-store-adapter-rs.svg)](https://crates.io/crates/event-store-adapter-rs)
[![docs.rs](https://docs.rs/event-store-adapter-rs/badge.svg)](https://docs.rs/event-store-adapter-rs)
[![Renovate](https://img.shields.io/badge/renovate-enabled-brightgreen.svg)](https://renovatebot.com)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![](https://tokei.rs/b1/github/j5ik2o/event-store-adapter-rs)](https://github.com/XAMPPRocky/tokei)


## Overview

This library provides Memory, DynamoDB, SQLite, Spanner, and Bigtable event stores for CQRS/Event Sourcing.

This crate follows [common contract v5](https://github.com/j5ik2o/event-store-adapter/blob/main/docs/spec/README.md); its major version 5 matches the contract revision.

> 5.0.0 is the first release of the new API. Stored v3 DynamoDB, SQLite, and Bigtable data is supported only through the explicit migration functions or CLI. Earlier releases had no Spanner storage, so there is nothing to migrate for it.

The public API is exported from the crate root; there is no `next` namespace or old API alias.

### Links

- [Common Documents](https://github.com/j5ik2o/event-store-adapter)
- [CQRS/Event Sourcing Example](https://github.com/j5ik2o/cqrs-es-example-rs)
- [Contributing](./CONTRIBUTING.md)
- [日本語](./README.ja.md)

## Installation

| Feature | Availability |
|:--------|:-------------|
| (none) | `EventStoreForMemory` and `MemoryStorage`, always available |
| `dynamodb` | `EventStoreForDynamoDB`, using `aws-sdk-dynamodb` |
| `sqlite` | `EventStoreForSqlite`, `initialize_sqlite_store`, and `SqliteOptions`, using `rusqlite` with a bundled SQLite |
| `spanner` | `EventStoreForSpanner`, `initialize_spanner_store`, `SpannerOptions`, `SpannerRetention`, `SpannerConnection`, `SpannerTables`, and `SpannerTokenSource`, using `tonic` and the generated Spanner gRPC clients |
| `bigtable` | `EventStoreForBigtable`, `initialize_bigtable_store`, `BigtableOptions`, `BigtableConnection`, `BigtableTables`, and `BigtableTokenSource`, using `tonic` and the generated Bigtable gRPC clients |
| `migration` | `migrate_v3_dynamodb`; also enables `dynamodb` |
| `sqlite-migration` | `migrate_v3_sqlite`; also enables `sqlite`, and brings in no AWS or Bigtable crates |
| `bigtable-migration` | `migrate_v3_bigtable` and `LegacyBigtableTables`; also enables `bigtable`, and brings in no AWS or SQLite crates |

Default features are empty. The `sqlite-system` feature has been removed.

## Quick start

Create an application with these dependencies:

```toml
[dependencies]
event-store-adapter-rs = "5.0.0"
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

## Storages

### Memory

The memory storage lives in a single process. It does not persist to files, survive restarts, or share data across processes.

See [Quick start](#quick-start) for a complete example.

### DynamoDB

Enable `features = ["dynamodb"]`. Provision three distinct tables and the snapshot history GSI before calling the asynchronous `EventStoreForDynamoDB::open(client, tables, options)`. Table creation is outside the library. `DynamoDbTables` supplies the three table names and GSI name; `DynamoDbOptions` supplies retention and bounded retry settings.

Keep the three tables in one Region and write to them only from that Region. Global tables (multi-Region replicas) are not supported.

This crate depends on `aws-sdk-dynamodb` without its default features and enables only `default-https-client` and `rt-tokio`, so it does not bring in the SDK's legacy HTTP client (hyper 0.14 with rustls 0.21). Cargo merges the features that every crate in a build requests, so if your own `aws-sdk-dynamodb` dependency keeps its default features, the `rustls` default turns the legacy client back on. To keep it out, declare the SDK the same way:

```toml
aws-sdk-dynamodb = { version = "1.68.0", default-features = false, features = ["default-https-client", "rt-tokio"] }
```

`cargo tree -i rustls@0.21` reports that no package matches when the legacy client is gone.

The SDK client must have a retry sleeper. The runnable [user-account example](examples/user-account/src/main.rs) installs a Tokio sleeper, starts DynamoDB Local, creates the tables, and calls the new `open`. Its [repository](examples/user-account/src/user_account_repository.rs) writes an event, writes an event plus snapshot, reads snapshots, and replays events. It verifies creation without a snapshot, a snapshot at the head, and later events after the snapshot:

```sh
cargo +1.99.0 run -p example-user-account
```

Docker is required for this example. See [the DynamoDB schema](docs/DYNAMODB_DATABASE_SCHEMA.md) for keys, configuration records, transactions, and retention.

### SQLite

Enable `features = ["sqlite"]`. The storage is one SQLite database file on the local file system of the host that uses it, run with WAL and `synchronous=FULL`; SQLite 3.51.3 or later is required, and the bundled build satisfies it. Initialization and opening are separate, explicit steps:

- `initialize_sqlite_store(path, &options)` initializes a new file, or an existing file that has no application tables, once. It creates the five tables and the configuration row in one transaction.
- `EventStoreForSqlite::open(path, options)` (or `open_with_serializers` for non-serde payloads) opens an initialized file and checks its layout and configuration against `options`. It never creates tables and never repairs a mismatch. A missing file, or one that SQLite cannot open, is a storage error (`EventStoreError::Storage`); an uninitialized file, a missing or different table, or a configuration mismatch is a configuration error (`EventStoreError::Configuration`).

`SqliteOptions` has three fields. `retention` defaults to `RetentionSettings::current_only()`; `keep_latest(n)` keeps the newest `n` history snapshots, and the value used by `open` must equal the one used to initialize the file. `RetentionMode::Ttl` and `keep_latest(0)` are configuration errors. `change_feed` must stay `false`: this storage does not provide a change feed, and `true` is a configuration error. `lock_wait` is how long a connection waits for another connection's write lock, 5 seconds by default; running out of time is a storage error, not an optimistic-lock error.

```toml
[dependencies]
event-store-adapter-rs = { version = "5", features = ["sqlite"] }
chrono = "0.4"
serde_json = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use chrono::Utc;
use event_store_adapter_rs::{
  initialize_sqlite_store, AggregateId, EventEnvelope, EventStore, EventStoreError,
  EventStoreForSqlite, RetentionSettings, SnapshotEnvelope, SqliteOptions,
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
  let path = "accounts.sqlite3";
  let options = SqliteOptions {
    retention: RetentionSettings::keep_latest(2),
    ..SqliteOptions::default()
  };
  // Run once for a new file; the file is no longer uninitialized afterwards.
  initialize_sqlite_store(path, &options)?;
  let store: EventStoreForSqlite<AccountId, Value, Value> =
    EventStoreForSqlite::open(path, options)?;
  let id = AccountId("1".into());

  store.persist_event(
    EventEnvelope::new(id.clone(), 1, Utc::now(), json!({"created": "Alice"}))
      .with_manifest("account-created/v1"),
  ).await?;
  store.persist_event_and_snapshot(
    EventEnvelope::new(id.clone(), 2, Utc::now(), json!({"renamed": "Bob"})),
    SnapshotEnvelope::new(json!({"name": "Bob"}), 2).with_manifest("account/v1"),
  ).await?;
  let events = store.get_events_by_id_since_seq_nr(&id, 1).await?;
  assert_eq!(events.len(), 2);
  let latest = store.get_latest_snapshot_by_id(&id).await?.unwrap();
  assert_eq!(latest.head_seq_nr(), 2);
  assert_eq!(latest.snapshot().unwrap().aggregate(), &json!({"name": "Bob"}));
  Ok(())
}
```

A store instance and its clones share one connection. See [the SQLite schema](docs/SQLITE_DATABASE_SCHEMA.md) for the tables, the connection settings, how to initialize a file by hand, transactions, and retention. To move data from a v3 SQLite file, see [Migration](#migration).

### Spanner

Enable `features = ["spanner"]`. The store talks to Cloud Spanner (or the Spanner emulator) over gRPC through five tables in one GoogleSQL-dialect database: `<prefix>_journal`, `<prefix>_head`, `<prefix>_snapshot_current`, `<prefix>_snapshot_history`, and `<prefix>_config`, all named from the one `table_prefix` you give. The library never runs DDL. Create the database and apply the DDL from [the Spanner schema](docs/SPANNER_DATABASE_SCHEMA.md) before the first `initialize_spanner_store` (for the Spanner TTL style, also the row deletion policy on `<prefix>_snapshot_history`).

Initialization and opening are separate, explicit steps:

- `initialize_spanner_store(&connection, &tables, &options)` checks the database dialect, the columns, the primary keys, and the row deletion policy against `options`, confirms that the five tables are empty, and writes one configuration row with a generated `store_id`. It never creates tables, and it fails with a configuration error (`ConfigurationReason::SpannerAlreadyInitialized`) when the configuration row exists or a table already holds rows.
- `EventStoreForSpanner::open(connection, tables, options)` (or `open_with_serializers` for non-serde payloads) runs the same check and reads the configuration row. It never creates anything and never repairs a mismatch. A missing or different table, a missing or malformed configuration row, or a setting that differs from the recorded one is a configuration error (`EventStoreError::Configuration`); a connection or service failure is a storage error (`EventStoreError::Storage`).

`SpannerOptions` has three fields. `retention` defaults to `SpannerRetention::CurrentOnly`. `KeepLatest { count }` keeps the newest `count` history snapshots and deletes older ones at once. `KeepLatestWithTtl { count, grace_days }` instead marks older history with `expired_at` and leaves the deletion to a `ROW DELETION POLICY (OLDER_THAN(expired_at, INTERVAL <grace_days> DAY))` on `<prefix>_snapshot_history`. `count` and `grace_days` must be at least 1 and fit in an INT64, and the value used by `open` must equal the one used to initialize the store and the days of the row deletion policy. `change_feed` must stay `false`: this storage does not provide a change feed, and `true` is a configuration error. `transaction_retry_limit` is how many times an aborted transaction is retried, not counting the first attempt, 5 by default; running out of retries is a storage error.

An append is one SERIALIZABLE read-write transaction, and the commit is its commit point. Reads use strong read-only transactions, and the head and the rows read after it come from the same read point. The library retries only an ABORTED transaction, with the same event and the same `seq_nr`. If the response to a commit is lost, the append is a storage error whose outcome is unknown: nothing is retried or compensated, and sending the same `seq_nr` again is an optimistic-lock error when the first attempt did commit. Old history is retired in a separate transaction after the append commits. When that fails, a `tracing` warning is emitted and the append still succeeds.

The connection is a `SpannerConnection` that holds one `tonic::transport::Channel`; the same channel serves the data API and the database admin API. The emulator needs a plaintext channel and no credentials:

```toml
[dependencies]
event-store-adapter-rs = { version = "5", features = ["spanner"] }
chrono = "0.4"
serde_json = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
tonic = { version = "0.14", default-features = false, features = ["channel"] }
```

```rust
use event_store_adapter_rs::{EventStoreForSpanner, SpannerOptions, SpannerRetention};
use serde_json::Value;
use tonic::transport::Endpoint;
use chrono::Utc;
use event_store_adapter_rs::{
  initialize_spanner_store, AggregateId, EventEnvelope, EventStore, SnapshotEnvelope, SpannerConnection, SpannerTables,
};
use serde_json::json;

#[derive(Debug, Clone)]
struct AccountId(String);

impl AggregateId for AccountId {
  fn type_name(&self) -> String {
    "Account".into()
  }

  fn value(&self) -> String {
    self.0.clone()
  }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  // The emulator listens without TLS. The database and its five tables already exist.
  let channel = Endpoint::from_static("http://localhost:9010").connect().await?;
  let tables = SpannerTables {
    project_id: "my-project".into(),
    instance_id: "my-instance".into(),
    database_id: "my-database".into(),
    table_prefix: "esa".into(),
  };
  let connection = SpannerConnection::new(channel);
  let options = SpannerOptions {
    retention: SpannerRetention::KeepLatest { count: 2 },
    ..SpannerOptions::default()
  };
  // Run once for new tables; they are no longer uninitialized afterwards.
  initialize_spanner_store(&connection, &tables, &options).await?;
  let store: EventStoreForSpanner<AccountId, Value, Value> =
    EventStoreForSpanner::open(connection, tables, options).await?;
  let id = AccountId("1".into());

  store
    .persist_event(
      EventEnvelope::new(id.clone(), 1, Utc::now(), json!({"created": "Alice"})).with_manifest("account-created/v1"),
    )
    .await?;
  store
    .persist_event_and_snapshot(
      EventEnvelope::new(id.clone(), 2, Utc::now(), json!({"renamed": "Bob"})),
      SnapshotEnvelope::new(json!({"name": "Bob"}), 2).with_manifest("account/v1"),
    )
    .await?;
  let events = store.get_events_by_id_since_seq_nr(&id, 1).await?;
  assert_eq!(events.len(), 2);
  let latest = store.get_latest_snapshot_by_id(&id).await?.unwrap();
  assert_eq!(latest.head_seq_nr(), 2);
  assert_eq!(latest.snapshot().unwrap().aggregate(), &json!({"name": "Bob"}));
  Ok(())
}
```

Production Cloud Spanner needs a TLS channel and an access token on every request. The library does not depend on a TLS stack or on Google's authentication libraries. Enable the TLS features of `tonic` in your own dependencies (for example `tls-ring` with `tls-native-roots`), build the `Channel` for `https://spanner.googleapis.com` yourself, implement `SpannerTokenSource`, whose `access_token` returns a fresh token, and pass it with `SpannerConnection::with_token_source`. The store calls the source right before every data and admin request and sends `authorization: Bearer <token>`; holding and refreshing the token is the source's job, and a failing source turns that operation into a storage error without sending the request.

The rules of this storage hold for one Spanner database in a configuration where read-write transactions and strongly consistent reads of that database hold, and all tables are in that database. A store instance and its clones share the connection, and several store instances may open the same database as long as they use the same settings. Deployments that read old points in time or split the five tables across databases are out of scope. This release does not provide a change feed. Earlier releases had no Spanner storage, so there is no migration tool; see [Migration](#migration). See [the Spanner schema](docs/SPANNER_DATABASE_SCHEMA.md) for the layout, the configuration row, initialization, writes and reads, and retention.

### Bigtable

Enable `features = ["bigtable"]`. The store talks to Cloud Bigtable (or the Bigtable emulator) over gRPC through two tables, `journal` and `head`. Table creation is outside the library, and so is the app profile. Prepare these before the first `initialize_bigtable_store`:

- Create the two tables with their column families: `event`, `history`, and `config` in the journal table, and `head`, `snapshot`, and `config` in the head table. Give the four data families (`event`, `history`, `head`, `snapshot`) a garbage-collection rule of "keep at most one version" and no age rule; `open` rejects any other rule. The `config` family may have any version rule but no age rule: the configuration row is written with cell timestamp 0, so an age rule would expire it at once and every later `open` and initialization check would fail.
- Create an app profile that pins every operation to a single cluster with single-row transactions allowed, and pass its ID as `BigtableOptions::app_profile_id`. The library does not check the app profile.

Initialization and opening are separate, explicit steps:

- `initialize_bigtable_store(&connection, &tables, &options)` writes the same `store_id` and settings into a configuration row of each table, once. It never creates tables or column families, and it fails with a configuration error (`ConfigurationReason::BigtableAlreadyInitialized`) when either table already has a configuration row.
- `EventStoreForBigtable::open(connection, tables, options)` (or `open_with_serializers` for non-serde payloads) checks the column families and both configuration rows against `options`. It never creates anything and never repairs a mismatch. A missing column family, a missing or malformed configuration row, or a setting that differs from the stored one is a configuration error (`EventStoreError::Configuration`).

`BigtableOptions` has four fields. `retention` defaults to `RetentionSettings::current_only()`; `keep_latest(n)` keeps the newest `n` history snapshots, and the value used by `open` must equal the one used to initialize the store. `RetentionMode::Ttl` and `keep_latest(0)` are configuration errors. `change_feed` must stay `false`: this storage does not provide a change feed, and `true` is a configuration error. `envelope_limit_bytes` caps one serialized envelope including its metadata, 1,048,576 bytes by default; it must be between 17 and 8,388,608 and is recorded in both tables, so every client has to use the same value. `app_profile_id` is sent with every data request; an empty string uses the instance's default app profile.

The connection is a `BigtableConnection` with one `tonic::transport::Channel` for the data API and one for the admin API. The admin API is used only to check the column families when the store is opened. The emulator needs a plaintext channel and no credentials:

```toml
[dependencies]
event-store-adapter-rs = { version = "5", features = ["bigtable"] }
chrono = "0.4"
serde_json = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
tonic = { version = "0.14", default-features = false, features = ["channel"] }
```

```rust
use chrono::Utc;
use event_store_adapter_rs::{
  initialize_bigtable_store, AggregateId, BigtableConnection, BigtableOptions, BigtableTables,
  EventEnvelope, EventStore, EventStoreForBigtable, RetentionSettings, SnapshotEnvelope,
};
use serde_json::{json, Value};
use tonic::transport::Endpoint;

#[derive(Debug, Clone)]
struct AccountId(String);

impl AggregateId for AccountId {
  fn type_name(&self) -> String { "Account".into() }
  fn value(&self) -> String { self.0.clone() }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  // The emulator listens without TLS. The tables and column families already exist.
  let channel = Endpoint::from_static("http://localhost:8086").connect().await?;
  let connection = BigtableConnection { data: channel.clone(), admin: channel };
  let tables = BigtableTables {
    project_id: "my-project".into(),
    instance_id: "my-instance".into(),
    journal_table_id: "journal".into(),
    head_table_id: "head".into(),
  };
  let options = BigtableOptions {
    retention: RetentionSettings::keep_latest(2),
    ..BigtableOptions::default()
  };
  // Run once for new tables; they are no longer uninitialized afterwards.
  initialize_bigtable_store(&connection, &tables, &options).await?;
  let store: EventStoreForBigtable<AccountId, Value, Value> =
    EventStoreForBigtable::open(connection, tables, options).await?;
  let id = AccountId("1".into());

  store.persist_event(
    EventEnvelope::new(id.clone(), 1, Utc::now(), json!({"created": "Alice"}))
      .with_manifest("account-created/v1"),
  ).await?;
  store.persist_event_and_snapshot(
    EventEnvelope::new(id.clone(), 2, Utc::now(), json!({"renamed": "Bob"})),
    SnapshotEnvelope::new(json!({"name": "Bob"}), 2).with_manifest("account/v1"),
  ).await?;
  let events = store.get_events_by_id_since_seq_nr(&id, 1).await?;
  assert_eq!(events.len(), 2);
  let latest = store.get_latest_snapshot_by_id(&id).await?.unwrap();
  assert_eq!(latest.head_seq_nr(), 2);
  assert_eq!(latest.snapshot().unwrap().aggregate(), &json!({"name": "Bob"}));
  Ok(())
}
```

Production Cloud Bigtable needs a TLS channel and an access token on every request. The library does not depend on a TLS stack or on Google's authentication libraries. Enable the TLS features of `tonic` in your own dependencies (for example `tls-ring` with `tls-native-roots`), build the two `Channel`s yourself (`https://bigtable.googleapis.com` for data and `https://bigtableadmin.googleapis.com` for admin), and implement `BigtableTokenSource`, whose `access_token` returns a fresh token. Pass the source to `EventStoreForBigtable::open_with_token_source` (or `open_with_serializers_and_token_source`) and `initialize_bigtable_store_with_token_source`. The store calls the source right before every data and admin request and sends `authorization: Bearer <token>`; holding and refreshing the token is the source's job, and a failing source turns that operation into a storage error without sending the request.

A store instance and its clones share the connection. This release does not provide a change feed. See [the Bigtable schema](docs/BIGTABLE_DATABASE_SCHEMA.md) for the keys, configuration rows, initialization (including recovery from an interrupted initialization), writes and reads, and retention. To move data from a v3 Bigtable, see [Migration](#migration).

## Serialization

The default constructors use JSON and require `Serialize + DeserializeOwned` on event and aggregate payloads. Use `EventStoreForMemory::with_serializers` or `EventStoreForDynamoDB::open_with_serializers` with `Arc<dyn EventSerializer<P>>` and `Arc<dyn SnapshotSerializer<A>>` for another byte format. These serializers receive only the payload. Payloads require `Send + Sync + 'static`; serde, Clone, and Debug are not required for these constructors. See the [Memory](lib/tests/memory_test.rs) and [DynamoDB](lib/tests/dynamodb_persist_event_test.rs) integration tests for non-serde payloads.

## Retention

`RetentionSettings::current_only()` is the default. `keep_latest(n)` retains the newest `n` history snapshots; 0 is invalid. Memory supports deletion only; `MemoryStorage::new` returns a configuration error for any `RetentionMode::Ttl`, with or without a history count. DynamoDB supports deletion or `RetentionMode::Ttl { grace_seconds }`; configure the snapshot table's TTL attribute `ttl` separately. With a history count, Memory runs retention after successful appends, including event-only appends. DynamoDB runs retention only when a history count is configured and an append that writes a history snapshot succeeds; event-only appends do not run retention. Spanner supports `SpannerRetention::KeepLatest { count }`, which deletes older history at once, and `KeepLatestWithTtl { count, grace_days }`, which marks older history and leaves the deletion to the row deletion policy; it runs retention in a separate transaction after an append that writes a history snapshot succeeds. A retention failure emits a `tracing` warning with aid, seq_nr, phase, and error; the committed write still succeeds.

## Errors

`EventStoreError` distinguishes `OptimisticLock`, `ContractViolation`, `Serialization`, `Configuration`, and `Storage`. Match `ContractRule`, `SerializationPhase`, `ConfigurationReason`, or `StorageOperation` rather than parsing messages. Serialization and storage errors retain their source.

## Migration

The normal stores read only the new layout. For v3's default DynamoDB layout, use the feature-gated `migrate_v3_dynamodb` function or the thin CLI described in [the v5 migration guide](docs/MIGRATION_GUIDE.md). For v3's default SQLite layout, use the `migrate_v3_sqlite` function (feature `sqlite-migration`) or the same CLI with `--old-sqlite` and `--sqlite`; it reads the old SQLite file without changing it and writes a new file that you initialized with `initialize_sqlite_store`. For v3.0.6's default Bigtable layout (the journal and snapshot tables), use the `migrate_v3_bigtable` function (feature `bigtable-migration`) or the same CLI with `--old-bigtable-*` and `--bigtable-*`; it only reads the two old tables and writes two new tables that you created with their column families and initialized with `initialize_bigtable_store`. Stop old writes and provision empty new tables (for SQLite, an initialized, empty file; for Bigtable, initialized, empty tables) before migration. Earlier releases had no Spanner storage, so there is no Spanner migration function or CLI option; start a Spanner store from an empty database as described in [the migration guide](docs/MIGRATION_GUIDE.md). Custom `KeyResolver` layouts are outside this migration's scope.

Historical [v3 migration guidance](docs/MIGRATION_GUIDE_v3.md) remains available for users staying on 3.x.

## Conformance

```sh
cargo +1.99.0 build --workspace --all-targets --all-features
cargo +1.99.0 clippy --workspace --all-targets --all-features -- -D warnings
cargo +nightly fmt --all -- --check
rustup toolchain install nightly-2026-09-27 --profile minimal
cargo +1.99.0 test --workspace --all-features
```

DynamoDB, Spanner, Bigtable, and migration integration tests require Docker.

## License

MIT or Apache-2.0. See [LICENSE-MIT](LICENSE-MIT) and [LICENSE-APACHE](LICENSE-APACHE).
