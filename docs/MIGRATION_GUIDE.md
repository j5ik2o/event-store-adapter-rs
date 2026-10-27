# Migration guide

## Overview

This guide describes migrating from Rust v3 to the 5.x API. 5.0.0 is the first release of the 5.x API. It exposes the accepted Memory and DynamoDB implementation directly from `event_store_adapter_rs`. There is no `next` namespace, old API alias, or transparent read fallback.

## Code changes

- Implement `AggregateId::type_name()` and `value()`. Remove `KeyResolver` and logical-shard configuration.
- Import `EventStore`, `EventEnvelope`, `SnapshotEnvelope`, `SnapshotRead`, `SeqNr`, and `EventStoreError` from the crate root. Replace old read/write error types with the five `EventStoreError` categories.
- Remove `expected_version` arguments and snapshot `version`. Number the first event 1 and subsequent events contiguously, up to `SEQ_NR_MAX`. The snapshot envelope's sequence number must equal its paired event.
- Use `EventStoreForMemory::new(MemoryStorage::new(retention)?)`. Clone the storage to share data between stores; create a separate storage for isolation.
- Use asynchronous `EventStoreForDynamoDB::open(client, tables, options).await` with three precreated tables, a history GSI, and a client configured with an SDK retry sleeper.
- For SQLite, enable the `sqlite` feature, call `initialize_sqlite_store(path, &options)` once on a new file, and then call the synchronous `EventStoreForSqlite::open(path, options)`. `open` creates nothing, and this storage provides no change feed. See the [README](../README.md) and [the SQLite schema](SQLITE_DATABASE_SCHEMA.md).
- For Bigtable, enable the `bigtable` feature, create the two tables with their column families, call `initialize_bigtable_store(&connection, &tables, &options).await` once, and then call `EventStoreForBigtable::open(connection, tables, options).await`. `open` creates nothing, and this storage provides no change feed. Production Cloud Bigtable needs a TLS channel and a `BigtableTokenSource` (`open_with_token_source`). See the [README](../README.md) and [the Bigtable schema](BIGTABLE_DATABASE_SCHEMA.md).
- For Spanner, enable the `spanner` feature, create a GoogleSQL-dialect database and apply the DDL from [the Spanner schema](SPANNER_DATABASE_SCHEMA.md), call `initialize_spanner_store(&connection, &tables, &options).await` once, and then call `EventStoreForSpanner::open(connection, tables, options).await`. `open` creates nothing, and this storage provides no change feed. Production Cloud Spanner needs a TLS channel and a `SpannerTokenSource` (`SpannerConnection::with_token_source`). See the [README](../README.md) and [the Spanner schema](SPANNER_DATABASE_SCHEMA.md).
- Replay from creation when `SnapshotRead::snapshot()` is absent. Snapshot and head sequence numbers are separate; read events after the snapshot and check that replay reaches the observed head. The [compiled repository example](../examples/user-account/src/user_account_repository.rs) handles these cases.
- For non-serde payloads, pass your event and snapshot serializers to `with_serializers` / `open_with_serializers`. See [README](../README.md) for all four operations and [the schema](DYNAMODB_DATABASE_SCHEMA.md) for retention.

The `sqlite-system` feature, its implementation and examples have been removed. The `sqlite`, `spanner`, and `bigtable` features are new v5 storages with their own layouts. In v3.0.6 the `sqlite` and `sqlite-system` features select the same SQLite implementation, so a file written by either is moved by the [SQLite migration](#migrating-v3-sqlite-data) below, and data written by v3's Bigtable store is moved by the [Bigtable migration](#migrating-v3-bigtable-data) below. No earlier release had a Spanner storage, so there is nothing to migrate for it; see [Starting a Spanner store](#starting-a-spanner-store) below.

## Data migration

### Migrating v3 DynamoDB data

#### Supported stored data

Only the **Rust v3 default DynamoDB layout** is supported: the old journal and snapshot tables with `pkey`/`skey` keys. Custom `KeyResolver` layouts, v2 data, and other backends are outside this migration's scope; v3's default SQLite file has its own [SQLite migration](#migrating-v3-sqlite-data) below, and v3's default Bigtable tables have their own [Bigtable migration](#migrating-v3-bigtable-data) below. The normal 5.x store never reads the old tables.

The migration parses the old keys to reconstruct the ID, validates sequence continuity and snapshot relationships, and copies payload bytes without using the old hasher or deserializing/re-serializing the payload. You must configure compatible serializers when opening the migrated data.

Type names containing a hyphen require a JSON mapping to a new type name without hyphens, for example:

```json
{"user-account": "UserAccount"}
```

Update the application's `AggregateId` to return the new type name. Type names without hyphens are retained; the mapping is used only for hyphenated old names. Mapping collisions or invalid IDs are rejected during inspection.

#### Operational procedure

1. Stop **all writes to both old tables** before inspection and keep them stopped throughout the migration. The function and CLI do not enforce this operational precondition.
2. Provision empty, distinct new journal, snapshot, and head tables plus the snapshot history GSI from [the schema](DYNAMODB_DATABASE_SCHEMA.md). The old two and new three table names must all differ. Configure head Streams and, if required, snapshot TTL separately.
3. Run the migration function or CLI below with the appropriate credentials, region, endpoint, and type mapping.

Inspection follows every Scan page of both old tables. It rejects gaps, conflicting source partitions, orphan or future snapshots, invalid keys/attributes/IDs/timestamps/TTL, and oversized target items before sending any migrated data. Configuration records may already have been created by `open` at this point.

On successful inspection, migration scans the old tables again, conditionally writes the new journal, constructs each head from its maximum-numbered event, and copies current and history snapshots. It drops old snapshot `version`, sets the snapshot manifest to the empty string, and preserves a positive history TTL. Unmarked history receives `active_history_seq_nr`; marked history does not. A missing event manifest becomes an empty string. It does not rewrite the old data or provide an ongoing compatibility layer.

#### Library function

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

#### Thin CLI

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

### Migrating v3 SQLite data

v3's SQLite store wrote one database file with a `journal` table and a `snapshot` table. The 5.x SQLite storage uses a different layout ([the schema](SQLITE_DATABASE_SCHEMA.md)), so the data is copied into a **new file**. The old file is opened read-only and is never changed. The normal 5.x store never reads it.

#### Supported stored data (SQLite)

Only the **Rust v3 default SQLite layout** is supported: the `journal` and `snapshot` tables with `pkey`/`skey` primary keys created by v3.0.6, whose keys were built by the default `KeyResolver` (`pkey = {type_name}-{shard}`, `skey = {type_name}-{value}-{seq_nr}`). Custom `KeyResolver` layouts, v2 data, and other backends are outside this migration's scope, and rows that do not follow the default layout are rejected during inspection.

The migration reconstructs the type name and the value from `pkey` and `skey` only, with the same rules as the DynamoDB migration. It never reads them from the old `aid` column, which holds the application's `Display` string. Type names containing a hyphen need the JSON mapping described [above](#supported-stored-data); the mapping is used only for hyphenated old names, and collisions, a hyphen left after mapping, or an `aid` longer than 1024 bytes are rejected.

What is copied:

- **Events.** Payload bytes are copied without deserializing or re-serializing them. Occurrence times keep their exact nanosecond value. Manifests are copied as stored; an empty manifest stays empty. For each aggregate, the sequence numbers must run from 1 without gaps or duplicates, and the head is built from the event with the largest number.
- **Current snapshot.** The row whose old `skey` ends in `-0` is copied to `snapshot_current` with an empty manifest. Its position must be at most the aggregate's largest event number and must match an existing event. The old `version` and `last_updated_at` are dropped.
- **Snapshot history.** Old history rows (those whose `skey` ends in a number of 1 or more) are copied according to the **new file's** history count, following the retention rules in [the SQLite schema](SQLITE_DATABASE_SCHEMA.md#retention) (the newest `n` are kept, and history that was never kept is not recreated): if the new file keeps the newest `n`, the newest `n` history snapshots of each aggregate by sequence number are copied to `snapshot_history`; if it keeps only the current snapshot, no history is copied. The current snapshot is never added to history, and history is never created where the old file had none. Every old history row is inspected in either case, so an invalid history row rejects the migration even if it would not be copied.

#### Operational procedure (SQLite)

1. Stop **all writes to the old file** before inspection and keep them stopped throughout the migration. The function and CLI do not enforce this operational precondition.
2. Create the new file with `initialize_sqlite_store`, or with the manual procedure in [the schema](SQLITE_DATABASE_SCHEMA.md#initialization), choosing the history count you want (`keep_latest(n)` or `current_only()`). The new file must be initialized and **empty**: any row in `journal`, `head`, `snapshot_current`, or `snapshot_history` rejects the migration, and the migration never creates tables or configuration. It must also be a different file from the old one. Both paths are normalized (symbolic links and `.`/`..` resolved) before they are compared; hard links to the same file are not detected.
3. Run the migration function or CLI below with the old path, the new path, and the type mapping.

Inspection reads every row of both old tables before anything is written to the new file. It rejects gaps and duplicates, keys that do not follow the default layout, orphan snapshots and snapshots beyond the aggregate's largest event number, unmapped, colliding, or over-long IDs, and wrongly typed columns. Then the migration reads the old file again inside the same read transaction and writes the new file in one `BEGIN IMMEDIATE` transaction. The counts in the report are set only after that transaction commits.

Each rejection names the table in `table`: `old.journal` or `old.snapshot` for a row of the old file, `new.journal`, `new.head`, `new.snapshot_current`, or `new.snapshot_history` for a table of the new file that already has rows, and an empty string when the old and new paths are the same file. In the report, `aggregates` counts head rows, `events` counts journal rows, and `snapshots` counts the current and history snapshot rows copied.

#### Library function (SQLite)

Enable `features = ["sqlite-migration"]`, which also enables `sqlite` but not DynamoDB or Bigtable. The function is synchronous:

```rust
use std::collections::HashMap;
use event_store_adapter_rs::migrate_v3_sqlite;

let mapping = HashMap::from([("user-account".into(), "UserAccount".into())]);
let report = migrate_v3_sqlite("old.sqlite3", "new.sqlite3", &mapping)?;
assert!(report.reasons.is_empty(), "{:?}", report.reasons);
```

`Ok(MigrationReport)` can still contain rejections in `reasons`; callers must check them, and a rejection leaves the counts at zero. Storage failures, such as an unreadable old file or an uninitialized new file, return `MigrationError` with the source and the report; its counts are zero because nothing was committed.

#### Thin CLI (SQLite)

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

### Migrating v3 Bigtable data

v3's Bigtable store wrote two tables, a journal table and a snapshot table. The 5.x Bigtable storage uses a different layout ([the schema](BIGTABLE_DATABASE_SCHEMA.md)): a journal table and a head table, with the history kept in the journal rows. The data is copied into **two new tables**. The old tables only receive `ReadRows` requests and are never changed. The normal 5.x store never reads them.

#### Supported stored data (Bigtable)

Only the **Rust v3.0.6 default Bigtable layout** is supported: the journal and snapshot tables whose row keys were built by the default `KeyResolver`.

| Row | Row key | Cells |
|:----|:--------|:------|
| journal | `{pkey}#{type_name}#{value}#{seq_nr, 20 digits, zero-padded}` where `pkey = {type_name}-{shard}` | family `event`: `payload`, `aggregate_id`, `seq_nr`, `occurred_at` (RFC 3339 with nanoseconds), `manifest` |
| snapshot, current | `{pkey}#{type_name}#{value}` | family `snapshot`: `payload`, `seq_nr` (and `version`, `last_updated_at`, which are not copied) |
| snapshot, history | the current key followed by `#{seq_nr, 20 digits, zero-padded}` | the same cells; the row holds the snapshot that a later snapshot replaced, at its own position |

Custom `KeyResolver` layouts, v2 data, and other backends are outside this migration's scope, and rows that do not follow the default layout are rejected during inspection.

The migration reconstructs the type name and the value from the row key only, with the same rules as the DynamoDB migration; the `aggregate_id` cell is used only to check the value taken from the key. Type names containing a hyphen need the JSON mapping described [above](#supported-stored-data); the mapping is used only for hyphenated old names, and collisions, a hyphen left after mapping, or an `aid` longer than 1024 bytes are rejected. A value that contains `#` can make a snapshot row key readable in more than one way. The migration never guesses: it accepts such a row only when exactly one reading matches an aggregate it has verified in the old journal, and rejects it otherwise.

What is copied:

- **Events.** Payload bytes are copied without deserializing or re-serializing them. The RFC 3339 occurrence time becomes a signed 64-bit nanosecond value; a time that cannot be read or does not fit is rejected. Manifests are copied as stored; an empty manifest stays empty. For each aggregate, the sequence numbers must run from 1 without gaps or duplicates, and the head is built from the event with the largest number. Every event envelope must fit the new store's envelope limit.
- **Current snapshot.** The row without a number suffix is copied to the head's `snapshot:current` with an empty manifest. Its position must be at most the aggregate's largest event number and must match an existing event. The old `version` and `last_updated_at` are dropped. The head's `has_snapshot` is `1` only when the current snapshot sits at the largest event number.
- **Snapshot history.** In v3, a history row is the snapshot that a newer snapshot replaced, written at its own position. v5 counts the head's current snapshot toward the history count ([the Bigtable schema](BIGTABLE_DATABASE_SCHEMA.md#retention)), so the migration takes the old history positions together with the current snapshot's position and keeps the newest `n` of them, where `n` is the **new tables'** history count. Those `n` snapshots become `active` history in the journal rows of their positions (the current snapshot's position is written as well, and its snapshot also stays in `snapshot:current`). If the new tables keep only the current snapshot, no history is copied. History that was never kept is not recreated. `event:has_snapshot` is `1` for every one of those positions, whether or not its history was copied. Every old history row is inspected in either case: a history row at or above the current snapshot's position, or a history row with no current snapshot, rejects the migration.

#### Operational procedure (Bigtable)

1. Stop **all writes to both old tables** before inspection and keep them stopped throughout the migration. The function and CLI do not enforce this operational precondition.
2. Create the two new tables with their column families, create an app profile for them, and call `initialize_bigtable_store` ([the schema](BIGTABLE_DATABASE_SCHEMA.md#initialization)) with the history count, envelope limit, and app profile you want. The new tables must be initialized and **hold no data rows**: any row other than the configuration row rejects the migration, and the migration never creates tables, column families, or configuration rows. The old and new tables must be different tables. The paths (project, instance, and table ID) are compared; the old tables may live in another project or instance.
3. Run the migration function or CLI below with the old and new tables, the **same settings** you initialized with, and the type mapping.

Inspection reads every row of both old tables, a page at a time, before anything is written to the new tables, and keeps only a small summary per aggregate rather than the payloads. It collects all rejections in the report: gaps and duplicates, keys that do not follow the default layout or cannot be read uniquely, cells that are missing or inconsistent with the key, orphan snapshots and snapshots beyond the aggregate's largest event number, unmapped, colliding, or over-long IDs, unreadable occurrence times, and envelopes that would exceed the new limit. On success, the migration reads the old journal again in key order and writes each aggregate in the same order that a normal append would: the rows before the largest event first, then the head, then the largest event's journal row. At every moment the head is at `h`, events `1` through `h - 1` are in the journal and event `h` is in the head, so a read through the public API never sees a gap. Payloads are read again from the old tables as they are written.

Each rejection names the table in `table` as a full path, `projects/<project>/instances/<instance>/tables/<table>`: an old table for a row of the old data, a new table for one that already has data rows, and an empty string when the old and new tables overlap. In the report, `aggregates` counts the heads created, `events` counts the journal rows written, and `snapshots` counts the snapshots written (the current snapshots and the history snapshots). The counts include only writes that received a successful response. A rejection found by inspection leaves them at zero.

#### Library function (Bigtable)

Enable `features = ["bigtable-migration"]`, which also enables `bigtable` but not DynamoDB or SQLite. In an async context with a `BigtableConnection` named `connection`:

```rust
use std::collections::HashMap;
use event_store_adapter_rs::{
  migrate_v3_bigtable, BigtableOptions, BigtableTables, LegacyBigtableTables,
  RetentionSettings,
};

let legacy = LegacyBigtableTables {
  project_id: "my-project".into(),
  instance_id: "my-instance".into(),
  journal_table_id: "old-journal".into(),
  snapshot_table_id: "old-snapshot".into(),
};
let tables = BigtableTables {
  project_id: "my-project".into(),
  instance_id: "my-instance".into(),
  journal_table_id: "journal".into(),
  head_table_id: "head".into(),
};
// The same settings that were passed to initialize_bigtable_store.
let options = BigtableOptions {
  retention: RetentionSettings::keep_latest(2),
  ..BigtableOptions::default()
};
let mapping = HashMap::from([("user-account".into(), "UserAccount".into())]);
let report = migrate_v3_bigtable(&connection, None, &legacy, &tables, &options, &mapping).await?;
assert!(report.reasons.is_empty(), "{:?}", report.reasons);
```

The second argument is the token source. Pass `None` for the emulator. For production Cloud Bigtable, pass `Some(Arc<dyn BigtableTokenSource>)` and a `connection` with TLS channels; the token is then sent with every request to the old tables and to the new tables. One `connection` serves both sets of tables, so they must be reachable through the same pair of channels. The app profile in `options` is sent only with requests to the new tables.

`Ok(MigrationReport)` can still contain rejections in `reasons`; callers must check them. A rejection found by inspection leaves the counts at zero. A head that already exists when it is about to be created is reported as a rejection too, with the counts of what had been written before it. Transport or storage failures return `MigrationError` with the source and the partial report; its counts reflect the writes that received successful responses. A configuration mismatch, such as a different history count or envelope limit, a column family that does not match, or an uninitialized new table, returns `MigrationError` before anything is written.

#### Thin CLI (Bigtable)

From this checkout, against the Bigtable emulator:

```sh
cargo run -p event-store-adapter-migration-rs -- --help
cargo run -p event-store-adapter-migration-rs -- \
  --old-bigtable-project my-project --old-bigtable-instance my-instance \
  --old-bigtable-journal old-journal --old-bigtable-snapshot old-snapshot \
  --bigtable-project my-project --bigtable-instance my-instance \
  --bigtable-journal journal --bigtable-head head \
  --bigtable-keep-snapshot-count 2 \
  --bigtable-emulator-host localhost:8086 \
  --type-mapping type-mapping.json
```

| Flag | Requirement |
|:-----|:------------|
| `--old-bigtable-project`, `--old-bigtable-instance` | Required project and instance of the old tables |
| `--old-bigtable-journal`, `--old-bigtable-snapshot` | Required old table IDs |
| `--bigtable-project`, `--bigtable-instance` | Required project and instance of the new tables |
| `--bigtable-journal`, `--bigtable-head` | Required new table IDs |
| `--bigtable-keep-snapshot-count` | Optional history count; omitted means "current snapshot only". Must equal the value used to initialize the new tables |
| `--bigtable-envelope-limit-bytes` | Optional envelope limit, 1048576 by default. Must equal the value used to initialize the new tables |
| `--bigtable-app-profile` | Optional app profile ID for requests to the new tables; omitted uses the instance's default |
| `--bigtable-emulator-host` | Optional `HOST:PORT` of a Bigtable emulator; omitted connects to production Cloud Bigtable |
| `--type-mapping` | Optional JSON file, shared with the DynamoDB and SQLite migrations |
| `--help` / `-h` | Show usage and the operational preconditions |

**Emulator or production.** With `--bigtable-emulator-host`, the CLI connects over plaintext and adds no authentication. Without it, the CLI connects to production Cloud Bigtable (`bigtable.googleapis.com` for data and `bigtableadmin.googleapis.com` for admin) over TLS with the platform's native root certificates, and obtains an access token through Application Default Credentials using the `gcp_auth` crate, following its usual credential discovery (for example the file named by `GOOGLE_APPLICATION_CREDENTIALS`, the credentials of the gcloud tool, or the metadata server on Google Cloud). The token is requested before every request and refreshed by `gcp_auth`. The credentials need permission to read the old tables and to read, write, and look up the schema of the new tables. The library itself depends on neither a TLS stack nor `gcp_auth`; only the CLI does.

Bigtable flags cannot be combined with the DynamoDB or SQLite flags; mixing them fails as an invalid argument before migration, and nothing is printed to standard output. The DynamoDB and SQLite invocations are unchanged.

The CLI prints a `MigrationReport` JSON object with `aggregates`, `events`, `snapshots`, and `reasons`. Rejections and storage failures produce a nonzero exit status; storage failures also print the partial report and an error. It prints the write-stop precondition on standard error. There are no table-creation, initialization, or write-stop flags.

The [Bigtable CLI integration tests](../migration-cli/tests/bigtable_cli_test.rs) invoke the actual executable against the Bigtable emulator, read migrated data through the public API, and verify that the old tables are unchanged. They never connect to Google's production services. They require Docker:

```sh
cargo test -p event-store-adapter-migration-rs --test bigtable_cli_test
```

### Starting a Spanner store

No earlier release of this crate had a Spanner storage. There is therefore no Spanner migration function or CLI option, and the migration CLI does not change. A Spanner store always starts empty. Moving events from another store into it is outside this library.

1. Create a database in the GoogleSQL dialect. A PostgreSQL-dialect database is rejected with `ConfigurationReason::SpannerLayoutMismatch`.
2. Apply the DDL from [the Spanner schema](SPANNER_DATABASE_SCHEMA.md#creating-the-tables) with your table prefix. For the Spanner TTL style (`SpannerRetention::KeepLatestWithTtl`), also add the row deletion policy with the same number of days as `grace_days`; for the other retention settings, do not add a policy.
3. Call `initialize_spanner_store` once, with the options the application will use, while no writer is running.
4. Open the store with `EventStoreForSpanner::open` and the same options.

## Verification and cutover

Each storage's steps below continue from step 3 of its operational procedure under Data migration.

### DynamoDB

4. Check the JSON report and process exit status. Read the migrated data using the public 5.x API before switching application writes to the new tables.
5. If writing fails partway through or a conditional collision occurs, recreate the **new three tables** and rerun after addressing the cause. A retry against partly populated tables is rejected. The old tables are never changed by migration.

### SQLite

4. Check the JSON report and process exit status. Open the new file with the public 5.x API and the same options, and read the migrated data before switching application writes to it.
5. If inspection rejects the data, or a write fails, **nothing is left in the new file**: the whole write is one transaction, and the old file is unchanged. Address the cause and rerun against the same new file. Rerunning after a successful migration is rejected because the new file now has rows.

### Bigtable

4. Check the JSON report and process exit status. Open the new tables with the public 5.x API and the same options, and read the migrated data (`get_latest_snapshot_by_id` and `get_events_by_id_since_seq_nr`) before switching application writes to them. Compare what you read with what you expect, for example the head's sequence number and the events of a few aggregates.
5. If inspection rejects the data, **nothing is written to the new tables** and the old tables are unchanged. Address the cause and rerun against the same new tables. A mismatch between the settings and the stored configuration, or a new table that was never initialized, also writes nothing and returns an error.
6. If a write fails partway through (a lost connection, an error from Bigtable, a head that already exists), a **partial migration is left in the new tables**: Bigtable has no transaction across rows, so the rows written so far stay. The migration does not resume from where it stopped, and a rerun against those tables is rejected because they now hold data rows. Recover as follows:
   1. Keep every writer of the old tables stopped. Do not delete the old tables.
   2. Delete the **two new tables** and create them again with their column families.
   3. Call `initialize_bigtable_store` again with the same settings.
   4. Run the migration again with the same input.

### Spanner

Spanner has no data migration, so there is nothing to compare. Before switching application writes to a new database, check that `EventStoreForSpanner::open` succeeds with the options you will use. A configuration error (`ConfigurationReason::SpannerLayoutMismatch`, `SpannerConfigurationInvalid`, or `SpannerConfigurationMismatch`) means that the DDL, the row deletion policy, or the configuration row differs from those options. `open` changes nothing, so fix the cause and open it again. If `initialize_spanner_store` fails with `SpannerAlreadyInitialized`, the configuration row was already written or a table is not empty; do not run it again against a store that is in use.
