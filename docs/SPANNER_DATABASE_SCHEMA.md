# Spanner storage schema

This document describes the 5.x layout used by the public `EventStoreForSpanner` (feature `spanner`). The library writes the configuration row only through the explicit `initialize_spanner_store`; the regular `EventStoreForSpanner::open` never creates tables, rows, or a row deletion policy. The database, the tables, and the row deletion policy are always created by you.

The first release of this storage targets databases of the GoogleSQL dialect only, and does not provide a change feed. Earlier releases had no Spanner storage, so there is no older layout to migrate from; see [the migration guide](MIGRATION_GUIDE.md) for how to start a new store.

## Layout

### Tables and keys

The store uses five tables in one database. Their names are derived together from one prefix (`esa` in the examples below): `<prefix>_journal`, `<prefix>_head`, `<prefix>_snapshot_current`, `<prefix>_snapshot_history`, and `<prefix>_config`. Individual table names are not accepted, so a store cannot be built from tables that belong to different stores.

| Table | Primary key | Other columns |
|:------|:------------|:--------------|
| journal | `(hash_prefix BYTES(8), aid STRING(1024), seq_nr INT64)` | `occurred_at_ns INT64`, `manifest STRING(MAX)`, `payload BYTES(MAX)` |
| head | `(hash_prefix BYTES(8), aid STRING(1024))` | `type_name STRING(1024)`, `seq_nr INT64`, `event_occurred_at_ns INT64`, `event_manifest STRING(MAX)`, `event_payload BYTES(MAX)` |
| snapshot_current | `(hash_prefix BYTES(8), aid STRING(1024))` | `seq_nr INT64`, `manifest STRING(MAX)`, `payload BYTES(MAX)` |
| snapshot_history | `(hash_prefix BYTES(8), aid STRING(1024), seq_nr INT64)` | `manifest STRING(MAX)`, `payload BYTES(MAX)`, `expired_at TIMESTAMP` (nullable) |
| config | `singleton INT64` (always 1) | `store_id STRING(36)` and the settings described under Configuration |

`aid` is the UTF-8 string `type_name-value`, assembled from `AggregateId`. The type name cannot contain a hyphen; the value may. The complete string is limited to 1024 bytes, and the library checks that before it writes.

- `hash_prefix` is the [FNV-1a 64](https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function) hash of the UTF-8 bytes of `aid`, as 8 big-endian bytes. It is not reinterpreted as a signed integer and no shard count is applied.
- Keys are compared as the exact typed tuple `(hash_prefix, aid)`. A prefix match on `aid` is never used, so two aggregates whose hashes collide still have different primary keys and never see each other's rows.
- `seq_nr` is between 1 and 2^53 − 1. `occurred_at_ns` is a signed 64-bit count of nanoseconds from the Unix epoch in an INT64 column, not a TIMESTAMP column. The library sends and reads every INT64 as a decimal string, and every BYTES value as base64, so no value passes through a floating-point number.

### Prefix and table names

The five derived names must be SQL identifiers: they start with an ASCII letter or `_`, continue with ASCII letters, digits, or `_`, and are at most 128 characters long. An empty prefix, or a prefix that derives an invalid name, is a configuration error (`ConfigurationReason::SpannerInvalidTablePrefix`) that is returned before any request is sent. Table names are always quoted in the SQL the library sends, and values are always passed as parameters.

## Configuration

### The configuration row

The `config` table holds exactly one row, with `singleton = 1`:

| Column | Type | Requirement |
|:-------|:-----|:------------|
| `singleton` | INT64 | Primary key, always 1 |
| `store_id` | STRING(36) | Lowercase hyphenated UUID v4, generated at initialization |
| `layout_version` | INT64 | 1 |
| `keep_snapshot_count` | INT64 | NULL for "current snapshot only", otherwise a positive integer |
| `retention_mode` | STRING(8) | `delete` or `ttl` |
| `ttl_grace_days` | INT64 | NULL for `delete`, a positive number of days for `ttl` |
| `change_feed_enabled` | BOOL | false (the first release rejects true) |

### Client settings that must match

`open` and `initialize_spanner_store` read the database with strong, read-only queries (`INFORMATION_SCHEMA` and the database admin API, which needs the `spanner.databases.get` permission) and check, in this order: the dialect, the columns and primary keys of the five tables, the row deletion policy on `snapshot_history`, and, for `open`, the configuration row. The `SpannerOptions::retention` value must equal what the row records, and the row deletion policy must match the retention style:

- `CurrentOnly` records `keep_snapshot_count = NULL` and `retention_mode = 'delete'`.
- `KeepLatest { count }` records `keep_snapshot_count = count` and `retention_mode = 'delete'`.
- `KeepLatestWithTtl { count, grace_days }` records `retention_mode = 'ttl'` and `ttl_grace_days = grace_days`, and `snapshot_history` must have `ROW DELETION POLICY (OLDER_THAN(expired_at, INTERVAL <grace_days> DAY))` with exactly that number of days. The comparison ignores letter case and white space. A policy over another column, in another unit, or with other days is a mismatch.
- With the delete style, `snapshot_history` must have no row deletion policy.

The check never runs DDL and never fills in anything that is missing. Every failure is a configuration error (`EventStoreError::Configuration`), except that a connection or service failure is a storage error (`EventStoreError::Storage`):

| `ConfigurationReason` | Meaning |
|:----------------------|:--------|
| `SpannerInvalidTablePrefix` | The prefix does not derive valid table names |
| `SpannerLayoutMismatch` | The database is not GoogleSQL, or a table, column, type, or primary key differs from the layout |
| `SpannerConfigurationInvalid` | The configuration row is missing or malformed |
| `SpannerConfigurationMismatch` | The version, history count, retention style, grace days, or row deletion policy differs from the client |
| `SpannerChangeFeedUnsupported` | `change_feed` is true, or the row records `change_feed_enabled = true` |
| `SpannerTtlGraceDaysOutOfRange`, `SpannerKeepSnapshotCountOutOfRange`, `KeepSnapshotCountZero` | A value is zero, or does not fit the INT64 column. These are returned before any request is sent, and values are never rounded |
| `SpannerAlreadyInitialized` | Returned by `initialize_spanner_store` when a configuration row exists or a table already has rows |

## Initialization

### Creating the tables

Create a database in the GoogleSQL dialect and apply the DDL below once, while no writer is running. The names use the prefix `esa`; replace it consistently with your own prefix.

<!-- spanner-schema:ddl -->
```sql
CREATE TABLE esa_journal (
  hash_prefix BYTES(8) NOT NULL,
  aid STRING(1024) NOT NULL,
  seq_nr INT64 NOT NULL,
  occurred_at_ns INT64 NOT NULL,
  manifest STRING(MAX) NOT NULL,
  payload BYTES(MAX) NOT NULL
) PRIMARY KEY (hash_prefix, aid, seq_nr);

CREATE TABLE esa_head (
  hash_prefix BYTES(8) NOT NULL,
  aid STRING(1024) NOT NULL,
  type_name STRING(1024) NOT NULL,
  seq_nr INT64 NOT NULL,
  event_occurred_at_ns INT64 NOT NULL,
  event_manifest STRING(MAX) NOT NULL,
  event_payload BYTES(MAX) NOT NULL
) PRIMARY KEY (hash_prefix, aid);

CREATE TABLE esa_snapshot_current (
  hash_prefix BYTES(8) NOT NULL,
  aid STRING(1024) NOT NULL,
  seq_nr INT64 NOT NULL,
  manifest STRING(MAX) NOT NULL,
  payload BYTES(MAX) NOT NULL
) PRIMARY KEY (hash_prefix, aid);

CREATE TABLE esa_snapshot_history (
  hash_prefix BYTES(8) NOT NULL,
  aid STRING(1024) NOT NULL,
  seq_nr INT64 NOT NULL,
  manifest STRING(MAX) NOT NULL,
  payload BYTES(MAX) NOT NULL,
  expired_at TIMESTAMP
) PRIMARY KEY (hash_prefix, aid, seq_nr);

CREATE TABLE esa_config (
  singleton INT64 NOT NULL,
  store_id STRING(36) NOT NULL,
  layout_version INT64 NOT NULL,
  keep_snapshot_count INT64,
  retention_mode STRING(8) NOT NULL,
  ttl_grace_days INT64,
  change_feed_enabled BOOL NOT NULL
) PRIMARY KEY (singleton);
```

For the Spanner TTL style, also add the row deletion policy to `snapshot_history`. The 7 days below are an example, not a default; the number must equal `grace_days` and `ttl_grace_days`. Do not add the policy to a store that uses the delete style.

<!-- spanner-schema:ttl-policy -->
```sql
ALTER TABLE esa_snapshot_history ADD ROW DELETION POLICY (OLDER_THAN(expired_at, INTERVAL 7 DAY));
```

### Writing the configuration row

Call `initialize_spanner_store` once, after the DDL and before the first writer starts:

```rust
use event_store_adapter_rs::{initialize_spanner_store, SpannerConnection, SpannerOptions, SpannerRetention, SpannerTables};

async fn initialize(connection: &SpannerConnection, tables: &SpannerTables) -> Result<(), event_store_adapter_rs::EventStoreError> {
  let options = SpannerOptions {
    retention: SpannerRetention::KeepLatestWithTtl { count: 2, grace_days: 7 },
    ..SpannerOptions::default()
  };
  initialize_spanner_store(connection, tables, &options).await
}
```

It validates the options, runs the check above for everything except the configuration row, and then, in one SERIALIZABLE read-write transaction, confirms that all five tables are empty and inserts the configuration row with a generated UUID. If a configuration row exists, or any table has rows, it returns `SpannerAlreadyInitialized` and writes nothing. It never creates tables.

To prepare the row by hand instead, run the following with a lowercase UUID v4 for `store_id`. The example is a Spanner TTL store that keeps the newest 2 history snapshots with a 7-day grace period. Use `NULL` for `keep_snapshot_count` to keep only the current snapshot, `'delete'` for `retention_mode`, and `NULL` for `ttl_grace_days` for the delete style:

<!-- spanner-schema:config -->
```sql
INSERT INTO esa_config (singleton, store_id, layout_version, keep_snapshot_count, retention_mode, ttl_grace_days, change_feed_enabled) VALUES (1, '5f0c8b8e-3c1a-4d7e-9a4b-2f6d1c0e7a11', 1, 2, 'ttl', 7, false);
```

## Records

### Journal rows

Each event is one journal row keyed by `(hash_prefix, aid, seq_nr)`. A row is only inserted and never overwritten or deleted by the library. `manifest` is the event manifest, an empty string when there is none. `payload` is the serializer output (JSON by default) of the event payload only; the manifest and the time are columns.

### Head rows

The head table holds one row per aggregate: the `seq_nr` of the latest event and a copy of that event's occurrence time, manifest, and payload. `type_name` is written when the aggregate is created (`seq_nr = 1`) and is not changed afterwards. The head is the record that decides concurrency: an append is accepted only against the `seq_nr` stored here.

### Snapshot rows

`snapshot_current` holds the latest snapshot of an aggregate, replaced by each append that carries a snapshot. An aggregate created with events only has no row there. When a history count is configured, the same snapshot is also inserted into `snapshot_history` with `expired_at = NULL`. In the TTL style, `expired_at` is later set once, by the library, to the server's `CURRENT_TIMESTAMP()`; `NULL` rows are never touched by the row deletion policy.

## Writes and reads

### Appending

An append runs one SERIALIZABLE read-write transaction, whose commit is the commit point:

1. Read the head of `(hash_prefix, aid)`.
2. For `seq_nr = 1`, continue only if there is no head. For a larger number, treat a missing head as 0. A `seq_nr` at or below the head is an optimistic-lock error; a gap is a contract violation.
3. Insert the event into `journal`.
4. Insert the head (with `type_name`) or update it with the new `seq_nr` and event copy.
5. If the append carries a snapshot, upsert `snapshot_current`, and, when a history count is configured, insert it into `snapshot_history`.
6. Commit. Nothing about history retirement is part of this transaction.

Serialization and range checks run before the transaction. The optimistic-lock decision always comes from the head read inside the transaction. A duplicate-key failure reported at commit is a storage error, not an optimistic-lock error.

### Retrying and unknown outcomes

The library retries only a transaction that Spanner (or the emulator) ABORTs, with the same event, the same `seq_nr`, and no new numbering. It waits 10 ms before the first retry, doubling up to 1 s. `SpannerOptions::transaction_retry_limit` (5 by default) counts retries, not the first attempt; running out is a storage error (`StorageOperation::Append`). If an append with the same aggregate and a number at least as large committed first, the retry is an optimistic-lock error.

If the response to a commit is lost, the append is a storage error of unknown outcome. The library neither retries nor writes compensating rows, and does not claim the append failed. Sending the same `seq_nr` again is an optimistic-lock error when the first attempt did commit. Any other failure, including one at commit, is not retried.

### Reading

Reads use strong, read-only transactions and never an old read time. `get_latest_snapshot_by_id` reads the head and `snapshot_current` in one transaction, so the pair is from the same read point; with no head it returns nothing, and with only a head it returns the head position without a snapshot. `get_events_by_id_since_seq_nr` reads the head first, then the journal rows from `max(1, seq_nr)` through the head's `seq_nr` in ascending order, in the same transaction. A missing number in the range is a storage error (`StorageOperation::LoadEvents`); a payload that the serializer cannot decode is a serialization error, not a storage error.

Results are read to the end through `ExecuteStreamingSql`. A value that Spanner splits across several responses (`chunked_value`) is joined before it is decoded. A stream that ends inside a split value or inside a row discards everything read so far and is a storage error; no partial result is returned.

## Retention

### Retention styles

Retention starts after an append that carries a snapshot commits, when a history count `n` is configured, in separate SERIALIZABLE read-write transactions (at most 1000 rows per transaction, selecting again each time). It reads the aggregate's rows with `expired_at IS NULL` in descending `seq_nr` order, keeps the newest `n`, and processes only the older ones:

- Delete style: the older rows of `snapshot_history` are deleted.
- Spanner TTL style: `expired_at` of the older rows is set to `CURRENT_TIMESTAMP()`, once. Marked rows are not counted in `n` and are never marked again. Spanner deletes them asynchronously after the grace period, so the library does not depend on physical deletion.

`journal`, `head`, and `snapshot_current` are never changed by retention. A retention transaction that Spanner ABORTs is retried like an append, with the same `transaction_retry_limit`, and selects its candidates again. If retention fails, including when the retries run out, the committed append still returns success, a `tracing` warning with the aggregate, `seq_nr`, phase, and error is emitted, and the next append that carries a snapshot retries.

### Changing the history count

Do not change the history count, the retention style, or the grace days of a running store. Writers with different settings fail the check at `open`, so a change is made while every client is stopped:

1. Stop every client of the store and confirm that appends, reads, and retention have ended.
2. For every aggregate, in a SERIALIZABLE read-write transaction, read the unmarked history in descending `seq_nr` order and keep the newest `n` of the target setting (or none). Delete the older rows in the delete style, or set `expired_at = CURRENT_TIMESTAMP()` only on rows with `expired_at IS NULL` in the TTL style. Split large work, and select again in each transaction. Do not touch `journal`, `head`, or `snapshot_current`.
3. After every aggregate is processed, update the `config` row (and the row deletion policy when the grace days change), then restart the clients with the new setting.

If it fails midway, keep the clients stopped and run the same target again. Raising the count, or setting a count on a store that had none, does not bring back history that was already deleted or marked.

## Operations

### Connection, authentication, and the emulator

`SpannerConnection::new(channel)` takes a `tonic::transport::Channel` that you create and own; the library creates no channel and depends on neither a TLS stack nor Google's authentication libraries. The same channel serves the data API and the database admin API (which reads the dialect). Each public operation opens its own session and deletes it at the end; there is no session pool.

For the emulator, use a plaintext channel (`http://localhost:9010`) and no credentials. The library is tested against the emulator image `gcr.io/cloud-spanner-emulator/emulator` 1.5.58, which is pinned by digest in `test-utils/src/docker.rs`. For Cloud Spanner, create a TLS channel for `https://spanner.googleapis.com` and pass a `SpannerTokenSource` through `SpannerConnection::with_token_source`; the source is called right before every request, and a failing source makes that operation a storage error without sending the request.

### Consistency preconditions

The rules above hold when all five tables are in one database and read-write transactions and strongly consistent reads of that database hold. The library requests SERIALIZABLE for every read-write transaction and a strong read for every read, and never an old read time. It adds no restriction on the region configuration. Several store instances, in one process or many, may share a database as long as they use the same settings. DDL changes during operation, and data changes that do not go through the library, are out of scope.

The emulator aborts overlapping read-write transactions even when they do not conflict, so the retry above is part of every concurrent test, and a pass on the emulator does not prove the service's conflict detection. The service-side limits on mutation and commit size, and the physical deletion by the row deletion policy, are not verified here.

### Quotas and limits

Column and commit size limits are Spanner's [quotas](https://cloud.google.com/spanner/quotas). The library checks the range of values and the length of `aid` before it writes. A rejection by a quota, a connection failure, and a commit failure are storage errors, and the raw gRPC status is kept as the error's source.
