# Migration Guide: v2 → v3 (the EventEnvelope API)

v3 replaces the `Event` / `Aggregate` trait contract with an envelope-based API.
Domain events and aggregate state become plain serde types (payloads), and the
metadata the store needs travels in two new public types:

- [`EventEnvelope<AID, P>`] — one journal row: `aggregate_id` / `seq_nr` /
  `occurred_at` / `manifest` + the event payload.
- [`SnapshotEnvelope<A>`] — one snapshot: the aggregate payload + `seq_nr`
  (replay start position) and `version` (optimistic-lock version).

This is a breaking release. **v3 does not read rows written by v2** — see
[Data compatibility](#data-compatibility) below.

## Breaking changes at a glance

| Area | v2 | v3 |
|:-----|:---|:---|
| Domain traits | `E: Event`, `A: Aggregate` (with `id()`, `seq_nr()`, `version()`, `set_version()`, ...) | Removed. Payloads only need `Serialize + DeserializeOwned + Send + Sync + 'static` (`AggregateId` remains) |
| Write API | `persist_event(&event, version)`, `persist_event_and_snapshot(&event, &aggregate)` | `persist_event(EventEnvelope, expected_version)`, `persist_event_and_snapshot(EventEnvelope, aggregate, expected_version)` — envelopes and payloads by value |
| Read API | `get_latest_snapshot_by_id -> Option<A>`, `get_events_by_id_since_seq_nr -> Vec<E>` | `-> Option<SnapshotEnvelope<A>>`, `-> Vec<EventEnvelope<AID, P>>` — metadata survives the boundary |
| Version bookkeeping | Store wrote the version back into the aggregate via `set_version` | The snapshot column/cell is authoritative; read it from `SnapshotEnvelope::version()` |
| Serialized payloads | Library injected `seq_nr` / `version` metadata into stored JSON | Payload columns hold pure domain content; metadata lives in dedicated columns |
| Serializers | `EventSerializer<E>` / `SnapshotSerializer<A>` over trait-bound types | Payload-only `EventSerializer<P>` / `SnapshotSerializer<A>`; `deserialize` returns the payload directly |
| `with_keep_snapshot_count` | `Self` (no validation) | `Result<Self, EventStoreWriteError>`; **`Some(0)` is rejected** (use `None` to disable retention) |
| Errors | `OptimisticLockError`, `SerializationError`, `IOError`, `OtherError` | + **`ContractViolation`** for calls that break the write contract (see below) |
| Update of an absent aggregate | Backend-dependent | Uniformly `OptimisticLockError` (message without `actual_version`) |
| Bigtable CAS | Non-transactional read→check→write | Single-row atomic `CheckAndMutateRow` |
| MSRV | Undeclared | `rust-version = "1.94.1"` (measured with `--all-features`) |

The Cargo feature set is unchanged: `default = []`, backends are opt-in via
`dynamodb` / `bigtable` / `sqlite` / `sqlite-system`, and the in-memory backend is
always compiled.

## 1. Strip the metadata from your domain types

Delete the `Event` / `Aggregate` implementations and every field that only existed
to satisfy them (`seq_nr`, `version`, `last_updated_at`, per-event IDs and
aggregate-ID copies if you do not need them as domain data):

```rust
// v2
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UserAccountEvent {
  Created { id: ULID, aggregate_id: UserAccountId, seq_nr: usize, name: String, occurred_at: DateTime<Utc> },
  Renamed { id: ULID, aggregate_id: UserAccountId, seq_nr: usize, name: String, occurred_at: DateTime<Utc> },
}
impl Event for UserAccountEvent { /* ... */ }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserAccount { id: UserAccountId, name: String, seq_nr: usize, version: usize, last_updated_at: DateTime<Utc> }
impl Aggregate for UserAccount { /* ... set_version ... */ }
```

```rust
// v3 — plain serde types, no library traits
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UserAccountEvent {
  Created { name: String },
  Renamed { name: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserAccount { id: UserAccountId, name: String }
```

`AggregateId` is unchanged and still required for the ID type.

## 2. Wrap writes in envelopes

The domain numbers events itself: `seq_nr` starts at 1 and must be contiguous per
stream (the library does not validate contiguity; duplicates are rejected by the
optimistic lock, gaps are written as-is). `occurred_at` is domain-supplied and
round-trips exactly (nanosecond precision). `manifest` is an optional free-form
type discriminator (empty string when omitted).

The `expected_version` convention (enforced, violations return
`EventStoreWriteError::ContractViolation`):

- creation: `seq_nr == 1` **and** `expected_version == 0`, via
  `persist_event_and_snapshot`;
- update: pass the `version()` of the `SnapshotEnvelope` you read;
  `persist_event` refuses `seq_nr == 1`.

```rust
// creation
let (state, created) = UserAccount::new(id.clone(), "alice".to_string());
let envelope = EventEnvelope::new(id.clone(), 1, Utc::now(), created)
  .with_manifest("user-account-created/v1");
event_store.persist_event_and_snapshot(envelope, state, 0).await?;

// update (event only)
let envelope = EventEnvelope::new(id.clone(), replayed.seq_nr + 1, Utc::now(), renamed);
event_store.persist_event(envelope, replayed.version).await?;
```

## 3. Replay from envelopes

Reads return envelopes, so the replay position and the next `expected_version`
come from the store instead of from aggregate fields:

```rust
pub struct ReplayedUserAccount {
  pub state: UserAccount,
  pub seq_nr: usize,   // last applied event position; number the next event as seq_nr + 1
  pub version: usize,  // pass as expected_version on the next write
}

async fn find_by_id(store: &impl EventStore<AID = UserAccountId, A = UserAccount, P = UserAccountEvent>,
                    id: &UserAccountId) -> Result<Option<ReplayedUserAccount>, ...> {
  let snapshot = match store.get_latest_snapshot_by_id(id).await? {
    Some(snapshot) => snapshot,
    None => return Ok(None),
  };
  let snapshot_seq_nr = snapshot.seq_nr();
  let version = snapshot.version();
  let events = store.get_events_by_id_since_seq_nr(id, snapshot_seq_nr + 1).await?;
  let seq_nr = events.last().map(|e| e.seq_nr()).unwrap_or(snapshot_seq_nr);
  let state = UserAccount::replay(events.into_iter().map(EventEnvelope::into_payload),
                                  snapshot.into_aggregate());
  Ok(Some(ReplayedUserAccount { state, seq_nr, version }))
}
```

The runnable examples (`examples/user-account`, `examples/user-account-sqlite`)
implement exactly this pattern.

## 4. Adjust builder and error handling

- `with_keep_snapshot_count` now returns
  `Result<Self, EventStoreWriteError>` and rejects `Some(0)` uniformly across all
  backends (`None` disables retention; history rows are only written while it is
  `Some(n)`). Which history rows survive pruning differs per backend — see the
  [retention asymmetry note](DATABASE_SCHEMA.md#snapshot-retention-asymmetry-across-backends).
- Match the new `EventStoreWriteError::ContractViolation` variant where you
  exhaustively match write errors. It signals a caller-side contract break
  (`seq_nr == 0`, creation/update mismatch, `keep_snapshot_count == 0`), not a
  storage failure.
- An update addressed at an aggregate that does not exist now uniformly returns
  `OptimisticLockError` with the message form
  `optimistic lock failed, aid=<id>, expected_version=<n>` (no `actual_version`).

## Data compatibility

**v3 does not read data written by v2, and no migration tooling or compatibility
layer is provided.** The physical layout changed in all backends (journal
`manifest` column, nanosecond `occurred_at`, pure payloads, key-based
current/history discrimination — see [DATABASE_SCHEMA.md](DATABASE_SCHEMA.md)).
Migrating existing stored data — for example by replaying v2 streams through a
v2 reader and re-persisting them through v3 — is the responsibility of the
application. Plan for new streams or a one-off conversion before upgrading a
system with data you need to keep.

## MSRV

v3 declares `rust-version = "1.94.1"` in `lib/Cargo.toml`, measured as the minimum
toolchain that builds `--all-features` with a fresh dependency resolution (the AWS
SDK stack currently requires 1.94.1). Feature-limited builds may work on older
toolchains, but cargo enforces the declared floor for the whole package.
