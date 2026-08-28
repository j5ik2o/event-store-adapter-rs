# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

This release contains breaking changes and corresponds to the next **major** version
(v3). See the migration guide in [docs/MIGRATION_GUIDE_v3.md](docs/MIGRATION_GUIDE_v3.md)
(Japanese: [docs/MIGRATION_GUIDE_v3.ja.md](docs/MIGRATION_GUIDE_v3.ja.md)).

### Changed

- **BREAKING**: The `Event` / `Aggregate` traits were removed. Domain events and
  aggregate state are now plain serde types (payloads); the store-facing metadata
  travels in the new public envelope types `EventEnvelope<AID, P>` (aggregate_id /
  seq_nr / occurred_at / manifest + payload) and `SnapshotEnvelope<A>` (payload +
  seq_nr / version). `AggregateId` is unchanged.
- **BREAKING**: The `EventStore` API now speaks envelopes with an explicit
  `expected_version`: `persist_event(EventEnvelope, expected_version)`,
  `persist_event_and_snapshot(EventEnvelope, aggregate, expected_version)`,
  `get_latest_snapshot_by_id -> Option<SnapshotEnvelope<A>>`,
  `get_events_by_id_since_seq_nr -> Vec<EventEnvelope<AID, P>>`. Creation is
  `seq_nr == 1` with `expected_version == 0`; updates pass the version read from the
  snapshot envelope. The store no longer writes the version back into the aggregate
  (`set_version` is gone); the snapshot column/cell is authoritative.
- **BREAKING**: Stored payloads are pure domain content. The library no longer
  injects `seq_nr` / `version` metadata into the serialized JSON; the journal gained a
  `manifest` column and `occurred_at` is stored with nanosecond precision (the
  domain-supplied value round-trips exactly). See
  [docs/DATABASE_SCHEMA.md](docs/DATABASE_SCHEMA.md) for the v3 layouts. **Rows
  written by v2 are not readable by v3**, and no migration tooling is provided —
  migrating stored data is the application's responsibility.
- **BREAKING**: `with_keep_snapshot_count` now returns
  `Result<Self, EventStoreWriteError>` and rejects `Some(0)` uniformly across all
  backends (`None` disables retention).
- **BREAKING**: The serializer SPI is payload-only (`EventSerializer<P>` /
  `SnapshotSerializer<A>`; `deserialize` returns the payload directly).
- An update addressed at an absent aggregate now uniformly returns
  `OptimisticLockError` (message without `actual_version`) on every backend.
- The Bigtable backend now performs its optimistic-lock check as a single-row
  atomic CAS (`CheckAndMutateRow`) instead of a non-transactional
  read→check→write sequence.

### Added

- `EventStoreWriteError::ContractViolation` — a dedicated variant for calls that
  break the write contract (`seq_nr == 0`, creation/update mismatch,
  `keep_snapshot_count == 0`), distinguishable from storage failures.
- Snapshot retention for the Bigtable backend (`with_keep_snapshot_count` /
  `with_delete_ttl`, history rows keyed by a zero-padded seq_nr suffix). Note that
  which history rows survive pruning differs per backend: DynamoDB keeps the oldest,
  Bigtable / SQLite keep the newest (documented in the schema document).
- Migration guides: [docs/MIGRATION_GUIDE_v3.md](docs/MIGRATION_GUIDE_v3.md) and
  [docs/MIGRATION_GUIDE_v3.ja.md](docs/MIGRATION_GUIDE_v3.ja.md).
- A declared MSRV: `rust-version = "1.94.1"` in `lib/Cargo.toml`, measured with
  `cargo build --all-features` (the AWS SDK stack currently requires 1.94.1).

### Removed

- The `Event` and `Aggregate` traits, `Aggregate::set_version`, and the metadata
  injection into stored payloads.
- The remaining `.unwrap()` on the DynamoDB `DeleteRequest` builder path (all
  backend errors map to `EventStoreWriteError` / `EventStoreReadError`).

### Internal

- Optimistic-lock conflict and error-contract tests now cover DynamoDB and
  Bigtable at the same depth as SQLite and the in-memory backend, and every
  backend asserts at least one envelope-metadata round trip.
- The clippy CI job runs a six-configuration feature matrix
  (`--no-default-features`, each backend feature on its own, `--all-features`)
  with `-D warnings`, so the Bigtable backend is linted too.
- The examples were rewritten against the envelope API.

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
