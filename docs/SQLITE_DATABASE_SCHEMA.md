# SQLite storage schema (5.x)

This document describes the layout used by the public `EventStoreForSqlite` (feature `sqlite`). The library creates this layout only through the explicit `initialize_sqlite_store`; the regular `EventStoreForSqlite::open` never creates tables or configuration. The manual procedure below produces the same layout for callers that prefer to prepare the file themselves.

The regular store reads only this layout. For v3's default SQLite layout, use the explicit [migration procedure](MIGRATION_GUIDE_v5.md). The first release of this storage does not provide a change feed.

## Tables and keys

Everything lives in one local SQLite database file. There are no logical shards or key prefixes: `aid` is compared for exact equality and `seq_nr` as an integer. Every table is `STRICT`, and every `aid` column uses `COLLATE BINARY`.

| Table | Primary key | Other columns |
|:------|:------------|:--------------|
| `journal` | `(aid, seq_nr)` | `occurred_at_ns`, `manifest`, `payload` |
| `head` | `aid` | `type_name`, `seq_nr`, `event_occurred_at_ns`, `event_manifest`, `event_payload` |
| `snapshot_current` | `aid` | `seq_nr`, `manifest`, `payload` |
| `snapshot_history` | `(aid, seq_nr)` | `manifest`, `payload` |
| `config` | `singleton` (always 1) | `store_id`, `layout_version`, `keep_snapshot_count`, `retention_mode`, `change_feed_enabled` |

Key and envelope columns are `NOT NULL`. `seq_nr` carries `CHECK (seq_nr BETWEEN 1 AND 9007199254740991)`, which is 1 through `SEQ_NR_MAX`. There is no `head_transition` table in this release.

`aid` is the UTF-8 string `type_name-value`, assembled from `AggregateId`. The type name cannot contain a hyphen; the value may. The complete string is limited to 1024 bytes. An empty type name or value is allowed.

## Connection settings and configuration

Every connection the store uses is checked and configured as follows. A value that differs from the requirement is a configuration error; the store never continues with another value.

| Setting | Value |
|:--------|:------|
| SQLite version | 3.51.3 or later, checked on every open |
| `locking_mode` | `NORMAL`, set before WAL is enabled |
| `journal_mode` | `WAL` |
| `synchronous` | `FULL` |
| `read_uncommitted` | `OFF` |
| Shared cache | not used |
| Busy wait | `SqliteOptions::lock_wait`, default 5 seconds; a value that does not fit 32-bit milliseconds is a configuration error |

Place the file on a local file system of the host that uses it. Network file systems, layouts split across files with `ATTACH`, and memory-only databases are not supported. A store instance and its clones share one connection. WAL mode is a property of the database file; the other settings are applied again on every connection.

The `config` table holds exactly one row:

| Column | Type | Requirement |
|:-------|:-----|:------------|
| `singleton` | INTEGER | Primary key, always 1 |
| `store_id` | TEXT | Lowercase hyphenated UUID, generated at initialization |
| `layout_version` | INTEGER | 1 |
| `keep_snapshot_count` | INTEGER | NULL for "current snapshot only", otherwise a positive integer |
| `retention_mode` | TEXT | `delete` |
| `change_feed_enabled` | INTEGER | 0 (the first release rejects 1) |

`open`/`open_with_serializers` verify the table layout (STRICT, column names, types, `NOT NULL`, primary keys, binary collation) and the `config` row in one read transaction. The history count in `SqliteOptions::retention` must equal `keep_snapshot_count`, and a store with `change_feed_enabled = 1` is rejected. A missing file, or one that SQLite cannot open, is a storage error (`EventStoreError::Storage`). An uninitialized file, a missing or different table, or any mismatch is a configuration error (`EventStoreError::Configuration`); `open` does not create or repair anything. Options are validated before the file is opened: `keep_latest(0)`, `RetentionMode::Ttl`, and `change_feed: true` are configuration errors.

## Initialization

Initialize a new file, or an existing file that has no application tables, indexes, views, or triggers, exactly once before the first `open`:

```rust
use event_store_adapter_rs::{initialize_sqlite_store, RetentionSettings, SqliteOptions};

let options = SqliteOptions {
  retention: RetentionSettings::keep_latest(2),
  ..SqliteOptions::default()
};
initialize_sqlite_store("accounts.sqlite3", &options)?;
```

`initialize_sqlite_store` validates the options, opens the file (creating it if it does not exist), applies the connection settings including WAL, and then starts `BEGIN IMMEDIATE`. If the database is not uninitialized it returns a configuration error without creating tables. Otherwise it creates the five tables, inserts the `config` row with a generated UUID, and commits them in one transaction. The switch to WAL is applied before the check and can remain on a file that was rejected.

To prepare the file by hand, run the following with SQLite 3.51.3 or later. Use a lowercase UUID for `store_id`, and `NULL` for `keep_snapshot_count` to keep only the current snapshot, or a positive integer `n` to keep the newest `n` history snapshots:

```sql
PRAGMA locking_mode = NORMAL;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;

BEGIN IMMEDIATE;
CREATE TABLE journal (
  aid TEXT COLLATE BINARY NOT NULL,
  seq_nr INTEGER NOT NULL CHECK (seq_nr BETWEEN 1 AND 9007199254740991),
  occurred_at_ns INTEGER NOT NULL,
  manifest TEXT NOT NULL,
  payload BLOB NOT NULL,
  PRIMARY KEY (aid, seq_nr)
) STRICT;
CREATE TABLE head (
  aid TEXT COLLATE BINARY NOT NULL PRIMARY KEY,
  type_name TEXT NOT NULL,
  seq_nr INTEGER NOT NULL CHECK (seq_nr BETWEEN 1 AND 9007199254740991),
  event_occurred_at_ns INTEGER NOT NULL,
  event_manifest TEXT NOT NULL,
  event_payload BLOB NOT NULL
) STRICT;
CREATE TABLE snapshot_current (
  aid TEXT COLLATE BINARY NOT NULL PRIMARY KEY,
  seq_nr INTEGER NOT NULL CHECK (seq_nr BETWEEN 1 AND 9007199254740991),
  manifest TEXT NOT NULL,
  payload BLOB NOT NULL
) STRICT;
CREATE TABLE snapshot_history (
  aid TEXT COLLATE BINARY NOT NULL,
  seq_nr INTEGER NOT NULL CHECK (seq_nr BETWEEN 1 AND 9007199254740991),
  manifest TEXT NOT NULL,
  payload BLOB NOT NULL,
  PRIMARY KEY (aid, seq_nr)
) STRICT;
CREATE TABLE config (
  singleton INTEGER NOT NULL PRIMARY KEY CHECK (singleton = 1),
  store_id TEXT NOT NULL,
  layout_version INTEGER NOT NULL,
  keep_snapshot_count INTEGER CHECK (keep_snapshot_count IS NULL OR keep_snapshot_count >= 1),
  retention_mode TEXT NOT NULL CHECK (retention_mode = 'delete'),
  change_feed_enabled INTEGER NOT NULL CHECK (change_feed_enabled IN (0, 1))
) STRICT;
INSERT INTO config (singleton, store_id, layout_version, keep_snapshot_count, retention_mode, change_feed_enabled)
VALUES (1, '5f0c8b8e-3c1a-4d7e-9a4b-2f6d1c0e7a11', 1, NULL, 'delete', 0);
COMMIT;
```

Back the database up with SQLite's backup mechanisms. Do not copy only the main file while the database runs in WAL mode.

## Journal rows

One event envelope becomes one row.

| Column | Type | Meaning |
|:-------|:-----|:--------|
| `aid` | TEXT | Complete aggregate ID |
| `seq_nr` | INTEGER | Domain-supplied event number, 1 through `SEQ_NR_MAX` |
| `occurred_at_ns` | INTEGER | Signed 64-bit Unix epoch nanoseconds supplied by the event; never stored as REAL |
| `manifest` | TEXT | Envelope manifest; empty when omitted |
| `payload` | BLOB | Serializer output for the event payload only |

## Head rows

| Column | Type | Meaning |
|:-------|:-----|:--------|
| `aid` | TEXT | Complete aggregate ID |
| `type_name` | TEXT | Aggregate type name |
| `seq_nr` | INTEGER | Last committed event number |
| `event_occurred_at_ns` | INTEGER | Occurrence time of that event |
| `event_manifest` | TEXT | Manifest of that event |
| `event_payload` | BLOB | Payload of that event |

The head is created by sequence 1 and updated on every append, including appends without a snapshot. Its event columns hold the most recently appended envelope.

## Snapshot rows

| Table | Column | Type | Meaning |
|:------|:-------|:-----|:--------|
| `snapshot_current` | `aid` | TEXT | Complete aggregate ID |
| `snapshot_current` | `seq_nr` | INTEGER | Sequence number reflected in the snapshot |
| `snapshot_current` | `manifest` | TEXT | Snapshot envelope manifest; empty when omitted |
| `snapshot_current` | `payload` | BLOB | Serializer output for the aggregate only |
| `snapshot_history` | `aid`, `seq_nr`, `manifest`, `payload` | as above | One row per kept history snapshot |

An append that carries a snapshot replaces the aggregate's `snapshot_current` row. There is no `version` column. `snapshot_history` rows are written only when `keep_snapshot_count` is set, and each carries the same envelope as the current snapshot that was written with it.

## Writes and reads

Input checks and serialization finish before any transaction starts. `persist_event` and `persist_event_and_snapshot` then run in one transaction on one connection:

```text
BEGIN IMMEDIATE
  read the head by exact aid
  compare it with the event's seq_nr
  INSERT the journal row
  INSERT or UPDATE the head
  UPSERT snapshot_current when a snapshot is attached
  INSERT snapshot_history when a snapshot is attached and a history count is set
COMMIT
```

A failure rolls everything back. A successful `COMMIT` is the commit point; if its outcome cannot be confirmed the error is a storage error, and the store does not allocate another sequence number. An event number at or below the head is an optimistic-lock error. A number that skips the next expected one (or a first event other than 1) is a contract violation. A duplicate primary key in `journal` or a new `head` row is also an optimistic-lock error. Other constraint failures, `SQLITE_BUSY`, `SQLITE_LOCKED`, and a busy wait that runs out are storage errors; they are not reported as optimistic-lock conflicts.

`get_latest_snapshot_by_id` reads the head and `snapshot_current` in one read transaction. No head returns `None`. A head without a snapshot returns `SnapshotRead::new(None, head_seq_nr)`. Head and snapshot keep independent sequence numbers; the snapshot's number is never above the head's.

`get_events_by_id_since_seq_nr` reads the head `h` and then the journal in the same read transaction: `aid = :aid AND seq_nr >= :start AND seq_nr <= :h ORDER BY seq_nr`, where `:start` is `max(1, seq_nr)`. A gap inside the requested range is a storage error; a short result is never returned as a success.

## Retention

`keep_snapshot_count` is `NULL` for `RetentionSettings::current_only()`, and `n` for `keep_latest(n)`. With `NULL`, only `snapshot_current` is kept. With `n`, each append that carries a snapshot also writes a `snapshot_history` row. After that append commits, a separate `BEGIN IMMEDIATE` transaction reads the aggregate's history in descending `seq_nr` order, keeps the newest `n`, and deletes only older `snapshot_history` rows. `snapshot_current`, `head`, and `journal` are never deleted. If this step fails, the committed append still succeeds, a `tracing` warning with aid, seq_nr, phase, and error is emitted, and the next append that carries a snapshot retries. Reclaiming file space with `VACUUM` is a separate operation and is not part of an append.

TTL retention (`RetentionMode::Ttl`) is not provided. The `retention_mode` column is always `delete`.

### Changing the history count

Changing `keep_snapshot_count` is an offline administration procedure; the stored value and every client's `SqliteOptions::retention` must agree.

1. Stop every client of the store and make sure no append, read, or retention work is still running. Record the current and target settings so that an interrupted run can resume.
2. For each aggregate in `snapshot_history`, run `BEGIN IMMEDIATE`, read its history in descending `seq_nr` order, and delete all but the newest `n` rows (all rows when the target is `NULL`), then `COMMIT`. Split large cleanups into several transactions and reselect inside each. Leave `journal`, `head`, and `snapshot_current` unchanged.
3. When every aggregate is done, run a separate `BEGIN IMMEDIATE`, update `config.keep_snapshot_count`, and `COMMIT`. Reopen every client with the new options so they verify the configuration, then resume work.

If a step fails, keep the clients stopped and rerun with the same target. Deleted history cannot be restored. Raising the count, or setting a count where there was none, does not recreate history that was deleted or never kept; the new setting applies from the next snapshot write.
