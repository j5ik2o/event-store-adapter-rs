# DynamoDB storage schema (4.x)

This document describes the layout used by the public `EventStoreForDynamoDB`. The runnable [user-account example](../examples/user-account/src/main.rs) provisions it with [the table helper](../test-utils/src/dynamodb.rs). Table creation, Streams, and TTL configuration remain the caller's responsibility.

The regular store reads only this layout. For v3's default two-table layout, use the explicit [migration procedure](MIGRATION_GUIDE_v4.md). SQLite and Bigtable users should stay on 3.x.

## Tables and index

All three table names must be distinct. There are no logical shards, `pkey`, or journal GSI.

| Table | Partition key | Sort key | Additional configuration |
|:------|:--------------|:---------|:-------------------------|
| Journal | `aid` (S) | `seq_nr` (N) | Replay queries the base table |
| Snapshot | `aid` (S) | `skey` (N) | History GSI below; enable TTL on `ttl` when using TTL retention |
| Head | `aid` (S) | none | Streams with `NEW_IMAGE` |

The snapshot history GSI uses `aid` (S) and `active_history_seq_nr` (N), with `KEYS_ONLY` projection. Its name is `DynamoDbTables::snapshot_history_index_name`.

`aid` is the UTF-8 string `type_name-value`, assembled from `AggregateId`. The type name cannot contain a hyphen; the value may. The complete string is limited to 1024 bytes. An empty type name or value is allowed.

## Configuration records

The asynchronous `open`/`open_with_serializers` reads all three configuration records with a strongly consistent `BatchGetItem`:

| Table | Configuration key |
|:------|:------------------|
| Journal | `aid = "__config__"`, `seq_nr = 0` |
| Snapshot | `aid = "__config__"`, `skey = 0` |
| Head | `aid = "__config__"` |

Each record contains the same `store_id` (S, a generated UUID) and `layout_version` (N, 1). If all are absent, `open` creates them together using conditional `TransactWriteItems`. Partial configuration, different store IDs, unsupported layout versions, duplicate table names, or a missing SDK retry sleeper cause a configuration error. Client options are not persisted as configuration attributes.

Unprocessed reads are retried with strong consistency and bounded backoff. Defaults are 10 retries, an initial delay of 50 ms, and a maximum delay of 2 seconds; `DynamoDbOptions` allows callers to set them.

## Journal items

One event envelope becomes one item.

| Attribute | Type | Meaning |
|:----------|:-----|:--------|
| `aid` | S | Complete aggregate ID |
| `seq_nr` | N | Domain-supplied event number, 1 through `SEQ_NR_MAX` |
| `occurred_at` | N | Signed 64-bit Unix epoch nanoseconds supplied by the event |
| `manifest` | S | Envelope manifest; empty when omitted |
| `payload` | B | Serializer output for the event payload only |

Replay uses a strongly consistent, ascending base-table `Query` on `aid` and `seq_nr >= lower_bound`, following all pages. A lower bound of 0 reads from the beginning. Payload bytes and nanosecond timestamps round-trip through the selected serializer and envelope; metadata is not injected into the payload.

## Head items

| Attribute | Type | Meaning |
|:----------|:-----|:--------|
| `aid` | S | Complete aggregate ID |
| `type_name` | S | Aggregate type name |
| `seq_nr` | N | Last committed event number |
| `events` | L | One M containing the just-appended event's `seq_nr`, `occurred_at`, `manifest`, and `payload` |

The head is updated on every append, including appends without a snapshot. Its `events` supplies the appended event to the head table's stream.

## Snapshot items

| Attribute | Type | Meaning |
|:----------|:-----|:--------|
| `aid` | S | Complete aggregate ID |
| `skey` | N | 0 for current; actual snapshot sequence number for history |
| `seq_nr` | N | Sequence number reflected in the snapshot |
| `manifest` | S | Snapshot envelope manifest; empty when omitted |
| `payload` | B | Serializer output for the aggregate only |
| `last_updated_at` | N | Event occurrence time in Unix epoch milliseconds |
| `active_history_seq_nr` | N | Actual history sequence number while active; absent from current and TTL-marked history |
| `ttl` | N | Expiry in epoch seconds for TTL-marked history only |

The current snapshot has no `version`, `ttl`, or `active_history_seq_nr`. Active history is written only when `RetentionSettings::keep_latest(n)` is configured. TTL-marked history retains its payload, manifest, sequence number, and update time; marking removes `active_history_seq_nr`, so it disappears from the history GSI before physical deletion by DynamoDB.

## Writes and reads

`persist_event` commits exactly two actions in a single `TransactWriteItems`: journal Put and head Put/Update. Sequence 1 creates the head conditionally; later appends require `head.seq_nr == event.seq_nr - 1`. The journal Put also asserts absence of its key.

`persist_event_and_snapshot` additionally puts the current snapshot. With history enabled it also puts the history snapshot: three or four actions in the same transaction. Event and snapshot sequence numbers must match. Event-only appends leave the snapshot table unchanged. The caller supplies no optimistic-lock version.

`get_latest_snapshot_by_id` uses a strongly consistent `BatchGetItem` for the head and current snapshot, with finite retries for unprocessed keys. No head returns `None`. A head without a snapshot returns `SnapshotRead::new(None, head_seq_nr)`. Head and snapshot have independent sequence numbers; a concurrent read can observe values from different writes. The [repository example](../examples/user-account/src/user_account_repository.rs) replays after the snapshot, or from creation when it is absent, and checks that replay reaches the observed head.

Items are checked against the 409600-byte size limit before the transaction; an oversized journal, head, current snapshot, or history snapshot returns a contract violation without a partial write.

## Retention

When a history count is configured, retention runs only after an append that writes a history snapshot commits. It queries the snapshot GSI, combines its results with the just-written history sequence number, and keeps the newest configured count. Event-only appends do not run retention. `keep_latest(0)` is invalid.

- `RetentionMode::Delete` deletes excess history using bounded batches and retries.
- `RetentionMode::Ttl { grace_seconds }` marks excess history with expiry equal to the marking clock's epoch seconds plus grace, using `SET ttl = expiry REMOVE active_history_seq_nr`. Configure DynamoDB TTL separately. Grace can be 0.
- `current_only()` writes no history. Current snapshots never expire through this policy.
- Retention errors emit a `tracing` warning with aid, seq_nr, phase, and error; the append already committed and continues to return success.

Memory uses the same public envelopes and sequence rules, but stores serialized bytes in `MemoryStorage` rather than these tables. A storage clone shares state; a separately created storage is isolated. Memory rejects TTL combined with a history count.
