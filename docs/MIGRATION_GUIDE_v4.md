# Migrating from Rust v3 to the 4.x API

The 4.x API in this checkout is not published yet. It exposes the accepted Memory and DynamoDB implementation directly from `event_store_adapter_rs`. There is no `next` namespace, old API alias, or transparent read fallback.

## Application changes

- Implement `AggregateId::type_name()` and `value()`. Remove `KeyResolver` and logical-shard configuration.
- Import `EventStore`, `EventEnvelope`, `SnapshotEnvelope`, `SnapshotRead`, `SeqNr`, and `EventStoreError` from the crate root. Replace old read/write error types with the five `EventStoreError` categories.
- Remove `expected_version` arguments and snapshot `version`. Number the first event 1 and subsequent events contiguously, up to `SEQ_NR_MAX`. The snapshot envelope's sequence number must equal its paired event.
- Use `EventStoreForMemory::new(MemoryStorage::new(retention)?)`. Clone the storage to share data between stores; create a separate storage for isolation.
- Use asynchronous `EventStoreForDynamoDB::open(client, tables, options).await` with three precreated tables, a history GSI, and a client configured with an SDK retry sleeper.
- Replay from creation when `SnapshotRead::snapshot()` is absent. Snapshot and head sequence numbers are separate; read events after the snapshot and check that replay reaches the observed head. The [compiled repository example](../examples/user-account/src/user_account_repository.rs) handles these cases.
- For non-serde payloads, pass your event and snapshot serializers to `with_serializers` / `open_with_serializers`. See [README](../README.md) for all four operations and [the schema](DATABASE_SCHEMA.md) for retention.

The `sqlite`, `sqlite-system`, and `bigtable` features, their implementations and examples have been removed. Users of those backends should remain on the 3.x line. This migration does not support their data.

## Supported stored data

Only the **Rust v3 default DynamoDB layout** is supported: the old journal and snapshot tables with `pkey`/`skey` keys. Custom `KeyResolver` layouts, v2 data, and other backends are outside this migration's scope. The normal 4.x store never reads the old tables.

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
4. Check the JSON report and process exit status. Read the migrated data using the public 4.x API before switching application writes to the new tables.
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
cargo +1.99.0 run -p event-store-adapter-migration-rs -- --help
cargo +1.99.0 run -p event-store-adapter-migration-rs -- \
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
cargo +1.99.0 test -p event-store-adapter-migration-rs --test cli_test
```
