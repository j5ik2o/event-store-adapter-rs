# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

This checkout contains the unpublished 4.x API with breaking changes. See the
migration guide in [docs/MIGRATION_GUIDE_v4.md](docs/MIGRATION_GUIDE_v4.md)
(Japanese: [docs/MIGRATION_GUIDE_v4.ja.md](docs/MIGRATION_GUIDE_v4.ja.md)).

### Changed

- **BREAKING**: The accepted Memory and DynamoDB implementations are now exposed
  through the crate's public modules and root exports. `EventStore` provides
  `persist_event(EventEnvelope<AID, P>)`,
  `persist_event_and_snapshot(EventEnvelope<AID, P>, SnapshotEnvelope<A>)`,
  `get_latest_snapshot_by_id -> Option<SnapshotRead<A>>`, and
  `get_events_by_id_since_seq_nr -> Vec<EventEnvelope<AID, P>>`.
- **BREAKING**: Writes use `SeqNr` and the stored head for contiguous appends;
  there is no `expected_version` argument or snapshot `version`.
  `SnapshotRead<A>` carries an optional snapshot envelope and the head sequence
  number, so event-only creation and snapshots behind the head can be replayed.
- **BREAKING**: Memory constructors receive a `MemoryStorage`. Cloning that storage
  shares state; separately created storage is isolated. DynamoDB uses asynchronous
  `open` with `DynamoDbTables` and `DynamoDbOptions` over three pre-created tables
  and a snapshot history GSI. See [the schema](docs/DATABASE_SCHEMA.md).
- **BREAKING**: `EventStoreError` replaces the separate read/write errors and
  distinguishes `OptimisticLock`, `ContractViolation`, `Serialization`,
  `Configuration`, and `Storage` with structured details.
- JSON constructors require serde payloads. `with_serializers` for Memory and
  `open_with_serializers` for DynamoDB accept custom payload serializers without
  serde bounds. Serializer inputs and stored bytes contain only domain payloads.
- `RetentionSettings::current_only()` is the default; `keep_latest(n)` keeps the
  newest history snapshots and rejects 0. With a history count, Memory runs
  retention after successful appends, including event-only appends. DynamoDB runs
  retention only with a history count and after a successful append that writes
  a history snapshot; event-only appends do not run retention.
  DynamoDB supports deletion or TTL; Memory rejects TTL with a history count.
  Retention failures emit a `tracing` warning while the committed write succeeds.

### Added

- The feature-gated `migrate_v3_dynamodb` function and thin migration CLI support
  v3's default DynamoDB layout. Stop old writes and provision empty new tables
  before migrating. Normal stores read only the new layout; legacy data is read
  only through migration.
- Updated English and Japanese README, DynamoDB schema, migration guides, and
  runnable examples for the public API and actual CLI flags.

### Removed

- The `next` namespace, old API, `KeyResolver`, compatibility aliases, and fallback
  routes to old storage layouts.
- SQLite and Bigtable features, implementations, re-exports, tests, examples, and
  related dependencies, plus old LocalStack helpers and old-layout normal tests.
  SQLite and Bigtable users should remain on 3.x. Custom `KeyResolver` layouts are
  outside the migration scope.

### Internal

- Examples, the conformance runner, migration imports, and CLI consumers use the
  new public routes. The accepted backend behavior is preserved.
- CI removes obsolete SQLite and Bigtable feature entries while retaining strict
  lint and the CI Success gate.

## [2.0.0]

This release contains breaking changes.
See the migration guide in [README.md](README.md#migration-from-1x).
(The automated release flow does not stamp this file; this section was retitled
from "Unreleased" after v2.0.0 was published to crates.io.)

### Changed

- **BREAKING**: Backends are now gated behind Cargo features with an empty default.
  No backend is compiled unless you enable `dynamodb`, `bigtable`, `sqlite`, or
  `sqlite-system` explicitly in your `Cargo.toml` (the in-memory backend remains
  always available without a feature). Existing users must add a
  `features = [...]` entry when upgrading.
- **BREAKING**: `EventStoreWriteError::OptimisticLockError` no longer wraps the AWS SDK
  error type (`TransactionCanceledException` via `TransactionCanceledExceptionWrapper`).
  It now carries a backend-neutral `String` message of the form
  `optimistic lock failed, aid=<id>, expected_version=<n>[, actual_version=<m>]`.
- **BREAKING**: The in-memory backend (`EventStoreForMemory`) no longer panics on
  internal failures (e.g. an unsupported create path); it returns
  `Err(EventStoreWriteError` / `EventStoreReadError)` like the other backends.
- The in-memory backend was refactored to the `StorageBackend` + `GenericEventStore`
  structure shared by all backends.

### Added

- `sqlite` and `sqlite-system` Cargo features providing a SQLite backend via `rusqlite`.
  `sqlite` bundles SQLite into the build (self-contained); `sqlite-system` links against
  the system SQLite. When both are enabled, the bundled variant wins (Cargo feature
  additivity).
- `EventStoreForSqlite` — an `EventStore` implementation backed by a SQLite database
  file or `:memory:`, with the same builder API as the other backends
  (`with_keep_snapshot_count`, `with_delete_ttl`, `with_shard_count`,
  `with_key_resolver`, `with_event_serializer`, `with_snapshot_serializer`).
- Automatic schema creation for the SQLite backend: the `journal` / `snapshot` tables
  and their indexes are created idempotently on store construction — no user DDL.
- Snapshot retention for the SQLite backend: `with_keep_snapshot_count` /
  `with_delete_ttl` prune excess and expired history snapshots after each persist.
- A runnable SQLite example: `examples/user-account-sqlite`
  (`cargo run -p example-user-account-sqlite` — no cloud connection, no Docker).

### Removed

- Hand-written `unsafe impl Send/Sync` blocks were removed from the backends;
  the auto-derived implementations are used instead.

### Internal

- CI now builds and tests the feature matrix (`--no-default-features`, each backend
  feature on its own, `--all-features`), runs clippy with `-D warnings`, and audits
  dependencies with `cargo-deny`.
