# Migrating from Rust v3 to the 5.x API

5.0.0 is the first release of the 5.x API. It exposes the accepted Memory and DynamoDB implementation directly from `event_store_adapter_rs`. There is no `next` namespace, old API alias, or transparent read fallback.

## Application changes

- Implement `AggregateId::type_name()` and `value()`. Remove `KeyResolver` and logical-shard configuration.
- Import `EventStore`, `EventEnvelope`, `SnapshotEnvelope`, `SnapshotRead`, `SeqNr`, and `EventStoreError` from the crate root. Replace old read/write error types with the five `EventStoreError` categories.
- Remove `expected_version` arguments and snapshot `version`. Number the first event 1 and subsequent events contiguously, up to `SEQ_NR_MAX`. The snapshot envelope's sequence number must equal its paired event.
- Use `EventStoreForMemory::new(MemoryStorage::new(retention)?)`. Clone the storage to share data between stores; create a separate storage for isolation.
- Use asynchronous `EventStoreForDynamoDB::open(client, tables, options).await` with three precreated tables, a history GSI, and a client configured with an SDK retry sleeper.
- For SQLite, enable the `sqlite` feature, call `initialize_sqlite_store(path, &options)` once on a new file, and then call the synchronous `EventStoreForSqlite::open(path, options)`. `open` creates nothing, and this storage provides no change feed. See the [README](../README.md) and [the SQLite schema](SQLITE_DATABASE_SCHEMA.md).
- Replay from creation when `SnapshotRead::snapshot()` is absent. Snapshot and head sequence numbers are separate; read events after the snapshot and check that replay reaches the observed head. The [compiled repository example](../examples/user-account/src/user_account_repository.rs) handles these cases.
- For non-serde payloads, pass your event and snapshot serializers to `with_serializers` / `open_with_serializers`. See [README](../README.md) for all four operations and [the schema](DATABASE_SCHEMA.md) for retention.

The `sqlite-system` and `bigtable` features, their implementations and examples have been removed. Users of those backends should remain on the 3.x line. This migration does not support their data. The `sqlite` feature is a new v5 SQLite storage with its own layout; data written by v3's SQLite store is moved by the [SQLite migration](#migrating-v3-sqlite-data) below.

## Supported stored data

Only the **Rust v3 default DynamoDB layout** is supported: the old journal and snapshot tables with `pkey`/`skey` keys. Custom `KeyResolver` layouts, v2 data, and other backends are outside this migration's scope; v3's default SQLite file has its own [SQLite migration](#migrating-v3-sqlite-data) below. The normal 5.x store never reads the old tables.

The migration parses the old keys to reconstruct the ID, validates sequence continuity and snapshot relationships, and copies payload bytes without using the old hasher or deserializing/re-serializing the payload. You must configure compatible serializers when opening the migrated data.

Type names containing a hyphen require a JSON mapping to a new type name without hyphens, for example:

```json
{"user-account": "UserAccount"}
```

Update the application's `AggregateId` to return the new type name. Type names without hyphens are retained; the mapping is used only for hyphenated old names. Mapping collisions or invalid IDs are rejected during inspection.

## Operational procedure

1. Stop **all writes to both old tables** before inspection and keep them stopped throughout the migration. The function and CLI do not enforce this operational precondition.
2. Provision empty, distinct new journal, snapshot, and head tables plus the snapshot history GSI from [the schema](DATABASE_SCHEMA.md). The old two and new three table names must all differ. Configure head Streams and, if required, snapshot TTL separately.
3. Run the migration function or CLI below with the appropriate credentials, region, endpoint, and type mapping.
4. Check the JSON report and process exit status. Read the migrated data using the public 5.x API before switching application writes to the new tables.
5. If writing fails partway through or a conditional collision occurs, recreate the **new three tables** and rerun after addressing the cause. A retry against partly populated tables is rejected. The old tables are never changed by migration.

Inspection follows every Scan page of both old tables. It rejects gaps, conflicting source partitions, orphan or future snapshots, invalid keys/attributes/IDs/timestamps/TTL, and oversized target items before sending any migrated data. Configuration records may already have been created by `open` at this point.

On successful inspection, migration scans the old tables again, conditionally writes the new journal, constructs each head from its maximum-numbered event, and copies current and history snapshots. It drops old snapshot `version`, sets the snapshot manifest to the empty string, and preserves a positive history TTL. Unmarked history receives `active_history_seq_nr`; marked history does not. A missing event manifest becomes an empty string. It does not rewrite the old data or provide an ongoing compatibility layer.

## Library function

Enable `features = ["migration"]`, which also enables DynamoDB. In an async context with a configured `aws_sdk_dynamodb::Client` named `client`:

```rust
use std::collections::HashMap;
use event_store_adapter_rs::{
  migrate_v3_dynamodb, DynamoDbTables, LegacyDynamoDbTables,
};

let old = LegacyDynamoDbTables {
  journal_table_name: "old-journal".into(),
  snapshot_table_name: "old-snapshot".into(),
};
let new = DynamoDbTables {
  journal_table_name: "journal".into(),
  snapshot_table_name: "snapshot".into(),
  head_table_name: "head".into(),
  snapshot_history_index_name: "snapshot-history".into(),
};
let mapping = HashMap::from([("user-account".into(), "UserAccount".into())]);
let report = migrate_v3_dynamodb(&client, &old, &new, &mapping).await?;
assert!(report.reasons.is_empty(), "{:?}", report.reasons);
```

`Ok(MigrationReport)` can still contain rejections in `reasons`; callers must check them. Transport or storage failures return `MigrationError` with the source and the partial report. Report counts reflect writes that received successful responses.

## Thin CLI

From this checkout:

```sh
cargo run -p event-store-adapter-migration-rs -- --help
cargo run -p event-store-adapter-migration-rs -- \
  --old-journal old-journal --old-snapshot old-snapshot \
  --journal journal --snapshot snapshot --head head \
  --history-index snapshot-history --type-mapping type-mapping.json \
  --region us-west-1
```

| Flag | Requirement |
|:-----|:------------|
| `--old-journal`, `--old-snapshot` | Required old table names |
| `--journal`, `--snapshot`, `--head` | Required new table names |
| `--history-index` | Required new snapshot GSI name |
| `--type-mapping` | Optional JSON file |
| `--endpoint-url` | Optional endpoint, such as a local emulator |
| `--region` | Optional AWS region |
| `--help` / `-h` | Show usage and the operational precondition |

Authentication uses normal AWS configuration. The CLI prints a `MigrationReport` JSON object with `aggregates`, `events`, `snapshots`, and `reasons`. Rejections and storage failures produce a nonzero exit status; storage failures also print the partial report and an error. Invalid arguments fail before migration. There are no table-creation or write-stop flags.

The [CLI integration tests](../migration-cli/tests/cli_test.rs) invoke the actual executable against DynamoDB Local, read migrated data through the public API, and verify that the old tables are unchanged. They require Docker:

```sh
cargo test -p event-store-adapter-migration-rs --test cli_test
```

## Migrating v3 SQLite data

v3's SQLite store wrote one database file with a `journal` table and a `snapshot` table. The 5.x SQLite storage uses a different layout ([the schema](SQLITE_DATABASE_SCHEMA.md)), so the data is copied into a **new file**. The old file is opened read-only and is never changed. The normal 5.x store never reads it.

### Supported stored data (SQLite)

Only the **Rust v3 default SQLite layout** is supported: the `journal` and `snapshot` tables with `pkey`/`skey` primary keys created by v3.0.6, whose keys were built by the default `KeyResolver` (`pkey = {type_name}-{shard}`, `skey = {type_name}-{value}-{seq_nr}`). Custom `KeyResolver` layouts, v2 data, and other backends are outside this migration's scope, and rows that do not follow the default layout are rejected during inspection.

The migration reconstructs the type name and the value from `pkey` and `skey` only, with the same rules as the DynamoDB migration. It never reads them from the old `aid` column, which holds the application's `Display` string. Type names containing a hyphen need the JSON mapping described [above](#supported-stored-data); the mapping is used only for hyphenated old names, and collisions, a hyphen left after mapping, or an `aid` longer than 1024 bytes are rejected.

What is copied:

- **Events.** Payload bytes are copied without deserializing or re-serializing them. Occurrence times keep their exact nanosecond value. Manifests are copied as stored; an empty manifest stays empty. For each aggregate, the sequence numbers must run from 1 without gaps or duplicates, and the head is built from the event with the largest number.
- **Current snapshot.** The row whose old `skey` ends in `-0` is copied to `snapshot_current` with an empty manifest. Its position must be at most the aggregate's largest event number and must match an existing event. The old `version` and `last_updated_at` are dropped.
- **Snapshot history.** Old history rows (those whose `skey` ends in a number of 1 or more) are copied according to the **new file's** history count, following the retention rules in [the SQLite schema](SQLITE_DATABASE_SCHEMA.md#retention) (the newest `n` are kept, and history that was never kept is not recreated): if the new file keeps the newest `n`, the newest `n` history snapshots of each aggregate by sequence number are copied to `snapshot_history`; if it keeps only the current snapshot, no history is copied. The current snapshot is never added to history, and history is never created where the old file had none. Every old history row is inspected in either case, so an invalid history row rejects the migration even if it would not be copied.

### Operational procedure (SQLite)

1. Stop **all writes to the old file** before inspection and keep them stopped throughout the migration. The function and CLI do not enforce this operational precondition.
2. Create the new file with `initialize_sqlite_store`, or with the manual procedure in [the schema](SQLITE_DATABASE_SCHEMA.md#initialization), choosing the history count you want (`keep_latest(n)` or `current_only()`). The new file must be initialized and **empty**: any row in `journal`, `head`, `snapshot_current`, or `snapshot_history` rejects the migration, and the migration never creates tables or configuration. It must also be a different file from the old one. Both paths are normalized (symbolic links and `.`/`..` resolved) before they are compared; hard links to the same file are not detected.
3. Run the migration function or CLI below with the old path, the new path, and the type mapping.
4. Check the JSON report and process exit status. Open the new file with the public 5.x API and the same options, and read the migrated data before switching application writes to it.
5. If inspection rejects the data, or a write fails, **nothing is left in the new file**: the whole write is one transaction, and the old file is unchanged. Address the cause and rerun against the same new file. Rerunning after a successful migration is rejected because the new file now has rows.

Inspection reads every row of both old tables before anything is written to the new file. It rejects gaps and duplicates, keys that do not follow the default layout, orphan snapshots and snapshots beyond the aggregate's largest event number, unmapped, colliding, or over-long IDs, and wrongly typed columns. Then the migration reads the old file again inside the same read transaction and writes the new file in one `BEGIN IMMEDIATE` transaction. The counts in the report are set only after that transaction commits.

Each rejection names the table in `table`: `old.journal` or `old.snapshot` for a row of the old file, `new.journal`, `new.head`, `new.snapshot_current`, or `new.snapshot_history` for a table of the new file that already has rows, and an empty string when the old and new paths are the same file. In the report, `aggregates` counts head rows, `events` counts journal rows, and `snapshots` counts the current and history snapshot rows copied.

### Library function (SQLite)

Enable `features = ["sqlite-migration"]`, which also enables `sqlite` but not DynamoDB or Bigtable. The function is synchronous:

```rust
use std::collections::HashMap;
use event_store_adapter_rs::migrate_v3_sqlite;

let mapping = HashMap::from([("user-account".into(), "UserAccount".into())]);
let report = migrate_v3_sqlite("old.sqlite3", "new.sqlite3", &mapping)?;
assert!(report.reasons.is_empty(), "{:?}", report.reasons);
```

`Ok(MigrationReport)` can still contain rejections in `reasons`; callers must check them, and a rejection leaves the counts at zero. Storage failures, such as an unreadable old file or an uninitialized new file, return `MigrationError` with the source and the report; its counts are zero because nothing was committed.

### Thin CLI (SQLite)

From this checkout:

```sh
cargo run -p event-store-adapter-migration-rs -- --help
cargo run -p event-store-adapter-migration-rs -- \
  --old-sqlite old.sqlite3 --sqlite new.sqlite3 \
  --type-mapping type-mapping.json
```

| Flag | Requirement |
|:-----|:------------|
| `--old-sqlite` | Required path of the old v3 file |
| `--sqlite` | Required path of the initialized, empty new file |
| `--type-mapping` | Optional JSON file, shared with the DynamoDB migration |
| `--help` / `-h` | Show usage and the operational preconditions |

SQLite flags cannot be combined with the DynamoDB flags (`--old-journal`, `--old-snapshot`, `--journal`, `--snapshot`, `--head`, `--history-index`, `--endpoint-url`, `--region`); mixing them fails as an invalid argument before migration. The DynamoDB invocation is unchanged.

The CLI prints a `MigrationReport` JSON object with `aggregates`, `events`, `snapshots`, and `reasons`. Rejections and storage failures produce a nonzero exit status; storage failures also print the report and an error. There are no file-creation or write-stop flags.

The [SQLite CLI integration tests](../migration-cli/tests/sqlite_cli_test.rs) invoke the actual executable on local files, read migrated data through the public API, and verify that the old file's bytes are unchanged. They do not require Docker:

```sh
cargo test -p event-store-adapter-migration-rs --test sqlite_cli_test
```
