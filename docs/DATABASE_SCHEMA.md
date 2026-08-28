# Database schemas used by the event stores (v3)

**This document is informational.** It describes how each backend lays out the v3
envelope types (`EventEnvelope` / `SnapshotEnvelope`) physically. The envelope metadata
(`aggregate_id` / `seq_nr` / `occurred_at` / `manifest` for events, `seq_nr` / `version`
for snapshots) is stored in dedicated columns/attributes/cells, and the payload
column holds **pure domain content only** — the library no longer injects `version`,
`seq_nr`, or timestamps into the serialized payload.

> The v3 layout differs from the v2 layout (the journal gained a `manifest`
> column, `occurred_at` became nanosecond-precision, the snapshot payload lost the
> injected metadata, and the current/history discrimination moved to the key). Reading
> rows written by v2 is not supported; see
> [MIGRATION_GUIDE_v3.md](MIGRATION_GUIDE_v3.md) for the migration stance.

### Snapshot-retention asymmetry across backends

When `with_keep_snapshot_count(Some(n))` is enabled, every backend bounds the
*number* of history snapshots to `n`, but **which** rows survive differs:

- **DynamoDB** prunes the excess from the **newest** history items (descending
  `seq_nr` query), so the **oldest** history items remain.
- **Bigtable / SQLite** keep the **newest `n`** history rows and delete the oldest
  excess.

The count semantics are identical; only the residual selection is asymmetric
(kept as-is from the pre-v3 behavior of each backend).

## DynamoDB table schema used by EventStore

- Journal
- Snapshot

The key design assumption for both tables is that writes are distributed to the greatest extent possible within the logical shard. Table creation stays outside the library (see `test-utils` for a reference definition).

### Journal table

The table used to store events that have occurred in an aggregate. In principle, this event is used to replay the aggregate state. One `EventEnvelope` maps to one item.

| attribute name | type | description | example |
|:------------|:----|:---------------------------------------------------------------------|:--------|
| pkey        | S | Partition key (`${aggregate-type-name}-hash($aid) % logical-shard-size`) | `user-account-1` |
| skey        | S | Sort key (`${aggregate-type-name}-${aid.value}-${seq_nr}`) | `user-account-01H42K4ABWQ5V2XQEP3A48VE0Z-12345` |
| aid         | S | Aggregate ID | `user-account-01H42K4ABWQ5V2XQEP3A48VE0Z` |
| seq_nr      | N | Sequence number (origin=1, domain-numbered) | `12345` |
| payload     | B | Event payload — pure domain content, serialized JSON by default | `{"Created":{"name":"test"}}` |
| occurred_at | N | Occurred datetime of the event in Unix epoch **nanoseconds** (domain-supplied; round-trips exactly) | `1688009557404481000` |
| manifest    | S | User-supplied, free-form type discriminator carried by the envelope (empty string when omitted) | `user-account-created/v1` |

A GSI on `(aid, seq_nr)` is used during replay.

### Snapshot table

This table is used to store aggregate state and to speed up replay of aggregates. It may not represent the latest aggregate state because events are saved even after the snapshot is saved.

| attribute name | type | description | example |
|:------------|:----|:-----------------------------------------------------------------------------------------------|:--------|
| pkey        | S | Partition key (`${aggregate-type-name}-hash($aid) % logical-shard-size`) | `user-account-1` |
| skey        | S | Sort key (`${aggregate-type-name}-${aid.value}-${seq_nr}`). The current snapshot lives in the slot whose skey is formatted with the **marker `0`**; history items use the event's seq_nr | `user-account-01H42K4ABWQ5V2XQEP3A48VE0Z-0` |
| aid         | S | Aggregate ID | `user-account-01H42K4ABWQ5V2XQEP3A48VE0Z` |
| seq_nr      | N | Sequence number the snapshot reflects. **Unlike v2, the current item stores the real value** (the marker `0` appears only inside the skey) | `12345` |
| version     | N | Version for optimistic locking (origin=1). The column value is authoritative — it is never recovered from the payload | `1` |
| payload     | B | State of the aggregate — pure domain content, serialized JSON by default (no injected `version` / `seq_nr`) | `{"id":{"value":"..."},"name":"test"}` |
| ttl         | N | TTL for deletion in epoch seconds (`0` = no expiry; set on excess history items when `with_delete_ttl` is configured) | `1624980000` |
| last_updated_at | N | Last updated datetime in Unix epoch milliseconds (derived from the event's `occurred_at`) | `1688009557404` |

- The current snapshot is read by a strongly consistent `GetItem` on the primary key
  (`pkey` + the marker-`0` skey); its `version` is what callers pass back as
  `expected_version` on the next write.
- History items (skey = event seq_nr) are written **only when
  `with_keep_snapshot_count(Some(n))` is enabled**, inside the same transaction as the
  current item and the journal item.
- The GSI on `(aid, seq_nr)` is used by the retention query; the excess above
  `n` history items is either deleted or, when `with_delete_ttl` is configured, given a
  future `ttl` value so DynamoDB expires the items (see the asymmetry note above:
  the pruned excess is taken from the newest side).

### Writing events and snapshots

1. When the command is accepted by the aggregate, the domain produces the next event
   and wraps it in an `EventEnvelope` with `seq_nr` numbered by the domain (origin=1).
2. The journal Put and the snapshot write always run in one `TransactWriteItems`.
   The first event of a stream (seq_nr=1, expected_version=0) creates both items with
   condition `attribute_not_exists`; every later write updates the current snapshot item
   under the condition `version = expected_version` and sets `version = expected_version + 1`.
   A failed condition surfaces as `OptimisticLockError`.

### Replaying an aggregate with events and snapshots

1. Specify the ID of the aggregate and get the latest `SnapshotEnvelope`.
2. Read the events after the envelope's `seq_nr` from the journal table.
3. Apply the read events to the snapshot state to obtain the latest aggregate state;
   pass the envelope's `version` as `expected_version` on the next write.

## Bigtable table schema used by EventStoreForBigtable

- journal table — column family `event`
- snapshot table — column family `snapshot`

All values are stored as bytes; numeric cells hold decimal strings. Reads and the
CAS predicate use a cells-per-column limit of 1, so the latest cell of each column
is authoritative.

### journal table (Bigtable)

Row key: `${partition-key}#${aggregate-type-name}#${aid.value}#${seq_nr zero-padded to 20 digits}`
(the partition key is `${aggregate-type-name}-hash($aid) % shard-count`). One `EventEnvelope` maps to one row.

| column (family `event`) | description | example |
|:------------|:---------------------------------------------------------------------|:--------|
| payload     | Event payload — pure domain content, serialized JSON by default | `{"Created":{"name":"test"}}` |
| aggregate_id | Aggregate ID value part | `01H42K4ABWQ5V2XQEP3A48VE0Z` |
| seq_nr      | Sequence number (origin=1, domain-numbered) | `12345` |
| occurred_at | Occurred datetime as an RFC 3339 string with **nanosecond** precision (domain-supplied; round-trips exactly) | `2023-06-29T03:32:37.404481000Z` |
| manifest    | User-supplied, free-form type discriminator (empty string when omitted) | `user-account-created/v1` |

The zero-padded seq_nr keeps the row keys of one aggregate contiguous and ordered,
so replay is a prefix range scan.

### snapshot table (Bigtable)

Current row key: `${partition-key}#${aggregate-type-name}#${aid.value}`.
History row key: current row key + `#` + zero-padded seq_nr (key order = oldest first).

| column (family `snapshot`) | description | example |
|:------------|:---------------------------------------------------------------------|:--------|
| payload     | State of the aggregate — pure domain content (no injected `version` / `seq_nr`) | `{"id":{"value":"..."},"name":"test"}` |
| seq_nr      | Sequence number the snapshot reflects | `12345` |
| version     | Version for optimistic locking (origin=1); the cell value is authoritative | `1` |
| last_updated_at | Last updated datetime in Unix epoch milliseconds | `1688009557404` |

- Writes go through `CheckAndMutateRow` (single-row atomic CAS): creation asserts the
  absence of the `version` cell; updates assert `version == expected_version` byte-exactly
  and set `version = expected_version + 1`. A failed predicate surfaces as
  `OptimisticLockError`.
- History rows are written **only when `with_keep_snapshot_count(Some(n))` is enabled**:
  the CAS winner copies the pre-image of the current row to the history row key in a
  separate best-effort write.
- Retention keeps the newest `n` history rows and deletes the oldest excess
  (`DeleteFromRow`); `with_delete_ttl` additionally deletes history rows whose
  `last_updated_at` is older than the TTL.

## SQLite table schema used by EventStoreForSqlite

- journal
- snapshot

The tables and indexes are created automatically by the library when the store is constructed (idempotent `CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS`) — no user DDL is required or expected.

The key design mirrors the DynamoDB tables: `pkey` / `skey` form the write address (`PRIMARY KEY (pkey, skey)`) and distribute writes across logical shards, while `(aid, seq_nr)` is the read key used for replay.

```sql
CREATE TABLE IF NOT EXISTS journal (
  pkey TEXT NOT NULL,
  skey TEXT NOT NULL,
  aid TEXT NOT NULL,
  seq_nr INTEGER NOT NULL,
  payload BLOB NOT NULL,
  occurred_at INTEGER NOT NULL,
  manifest TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (pkey, skey)
);
CREATE UNIQUE INDEX IF NOT EXISTS journal_aid_seq_nr_idx ON journal (aid, seq_nr);
CREATE TABLE IF NOT EXISTS snapshot (
  pkey TEXT NOT NULL,
  skey TEXT NOT NULL,
  aid TEXT NOT NULL,
  seq_nr INTEGER NOT NULL,
  version INTEGER NOT NULL,
  payload BLOB NOT NULL,
  last_updated_at INTEGER NOT NULL,
  PRIMARY KEY (pkey, skey)
);
CREATE INDEX IF NOT EXISTS snapshot_aid_seq_nr_idx ON snapshot (aid, seq_nr);
```

### journal table (SQLite)

| column name | type | description |
|:------------|:-----|:------------|
| pkey | TEXT | Partition key (`${aggregate-type-name}-hash($aid) % shard-count`) — write-distribution key, part of the primary key |
| skey | TEXT | Sort key (`${aggregate-type-name}-${aid.value}-${seq_nr}`) — part of the primary key |
| aid | TEXT | Aggregate ID |
| seq_nr | INTEGER | Sequence number (origin=1, domain-numbered) |
| payload | BLOB | Event payload — pure domain content (serialized JSON by default) |
| occurred_at | INTEGER | Occurred datetime of the event in Unix epoch **nanoseconds** (domain-supplied; round-trips exactly; out-of-range values are rejected at write time) |
| manifest | TEXT | User-supplied, free-form type discriminator carried by the envelope (empty string when omitted) |

A unique index on `(aid, seq_nr)` plays the role of the DynamoDB GSI and is used during replay.

### snapshot table (SQLite)

| column name | type | description |
|:------------|:-----|:------------|
| pkey | TEXT | Partition key (`${aggregate-type-name}-hash($aid) % shard-count`) — write-distribution key, part of the primary key |
| skey | TEXT | Sort key (`${aggregate-type-name}-${aid.value}-${seq_nr}`). The current snapshot lives in the slot whose skey is formatted with the **marker `0`**; history rows use the event's seq_nr |
| aid | TEXT | Aggregate ID |
| seq_nr | INTEGER | Sequence number the snapshot reflects. **Unlike v2, the current row stores the real value** (the marker `0` appears only inside the skey; current/history discrimination is by skey, not by this column) |
| version | INTEGER | Version for optimistic locking (origin=1); the column value is authoritative — it is never recovered from the payload |
| payload | BLOB | State of the aggregate — pure domain content (serialized JSON by default, no injected `version` / `seq_nr`) |
| last_updated_at | INTEGER | Last updated datetime in Unix epoch milliseconds; also used to evaluate the snapshot retention TTL |

An index on `(aid, seq_nr)` is used during replay.

- Optimistic-lock verification and writes happen inside a single SQLite transaction:
  the journal insert and the conditional snapshot update (`WHERE version = expected`)
  either commit together or roll back together. The first event of a stream
  (seq_nr=1, expected_version=0) inserts both rows; a primary-key / unique-index
  conflict on that path surfaces as `OptimisticLockError` with `expected_version=0`.
- History rows are inserted **only when `with_keep_snapshot_count(Some(n))` is enabled**,
  in the same transaction. Retention keeps the newest `n` history rows and deletes the
  oldest excess (`ORDER BY seq_nr ASC LIMIT excess`); `with_delete_ttl` additionally
  deletes history rows whose `last_updated_at` is older than the TTL.
