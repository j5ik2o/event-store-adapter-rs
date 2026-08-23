# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

This release contains breaking changes and corresponds to the next **major** version.
See the migration guide in [README.md](README.md#migration-from-1x).

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
