<a id="次のメジャー版400の設計"></a>

# Design of the next major version (4.0.0)

> This document is the design record from building v5. For current usage, see the [README](../../README.md) and the current code.

This document defines the design of the next major version of `event-store-adapter-rs`. It is rewritten to match the new common contract. No code is written this time. The coordinator reviewed this document. The points that needed a judgment were decided as described in chapter 9 (2026-10-06). The implementation work uses this document.

<a id="0-この文書の読み方"></a>

## 0. How to read this document

<a id="01-規範と出典"></a>

### 0.1 Normative documents and sources

- The normative sources are the following documents in the hub repository (`j5ik2o/event-store-adapter`). This document does not change or add rules.
  - Common contract (`docs/spec/core-contract.md`)
  - DynamoDB profile (`docs/spec/storage/dynamodb.md`)
  - Memory profile (`docs/spec/storage/memory.md`)
  - Implementation plan (`docs/plan/implementation-plan.md`)
  - Decision records (`docs/adr/`)
- The conformance test data is `conformance/` in this repository (version 1.0.0). How to read it is described in `conformance/README.md`.
- Rule numbers are the numbers of the documents above, used as they are. The following abbreviations are used.
  - `T-`, `H-`, `W-`, `R-`, `S-`, `E-`, `F-`, `SP-`: common contract
  - `D-`, `DY-`: DynamoDB profile
  - `MEM-`: memory profile
  - `IP-`: implementation plan (`IP 3` is chapter 3 of the implementation plan)
  - `CR`: `conformance/README.md`
  - `P-`: the number of an agreement record of the specification. Only those that appear in the body of the specification are cited.
- "Current" means version 3.0.6 of this repository (`lib/Cargo.toml`).

<a id="011-用語"></a>

### 0.1.1 Terms

Technical terms are read as follows when they first appear.

| Term | Meaning |
|:--|:--|
| aid (aggregate ID string) | The string identifier of an aggregate. It has the form `${typeName}-${value}`, and the library builds it (T-1) |
| head | A record kept once per aggregate. It holds the latest `seq_nr` and the event envelope settled at that `seq_nr` (common contract chapter 2) |
| envelope | A type that carries an event or a snapshot, split into metadata and the content (payload) |
| payload | The content of an event or of an aggregate state. The domain decides it |
| manifest | A free-form string that the user attaches. The library does not interpret it (T-4) |
| optimistic lock | A comparison method that rejects a write when the write's precondition (the head's `seq_nr`) has changed (W-8) |
| GSI (global secondary index) | A DynamoDB index for reading by a key other than the primary key. In this document, it means the sparse index that reads history snapshots (D-3) |
| TTL (expiry) | A DynamoDB mechanism that removes items after their expiry time has passed (S-3) |
| Streams | A DynamoDB mechanism that streams the record of changes to a table. It can be the source of a change feed (DY-12) |
| SDK | The Rust component for DynamoDB that AWS distributes: `aws-sdk-dynamodb` |
| IAM | The permission mechanism of AWS |
| MSRV (minimum supported Rust version) | The oldest Rust version on which the library is guaranteed to work |
| CI | The checks that run automatically on every change to main (`.github/workflows/ci.yml`) |
| PR (pull request) | A proposal of a change to main |
| hook | A point where a test plugs into the store's internal processing. Ordinary users do not use it |

<a id="02-記載の種類"></a>

### 0.2 Kinds of statements

This document has the following four kinds of statements. Do not mix them.

| Kind | Meaning | How to recognize it |
|:--|:--|:--|
| Normative | What the specification defines | A rule number is attached |
| Confirmed fact | What was verified by reading this repository or the documents | Written as "confirmed", with the file name shown |
| Design proposal | The way of realizing it that this document proposes | Written as "proposal" |
| Judgment decided in chapter 9 | A judgment made at the design stage and reviewed by the coordinator. Only those that needed a judgment were escalated to the owner | Written as `(chapter 9 D-n)`. Chapter 9 gives the decider, the date, and the reason. A decision moved into the body is written there as "Decision (decider, 2026-10-06)" |

Places that relate to a judgment in chapter 9 are written with the decided content. Chapter 9 has no undecided item.

<a id="03-確認していないこと"></a>

### 0.3 What was not verified

- This time, no Rust code and no container was run. The shape of the API, the use of the SDK, and the test procedure are all design proposals.
- The existence of the SDK's APIs was confirmed in the source of `aws-sdk-dynamodb 1.130.0`, which was already fetched locally. Reconfirm with the version in use at implementation time.
- The current repository has no `Cargo.lock`. The resolved version of `aws-sdk-dynamodb` has not been confirmed.
- How crates.io and Cargo handle pre-releases (that a published version cannot be withdrawn and can only be yanked with `yank`, that a version requirement does not select a pre-release unless it names one explicitly, and how SemVer orders pre-releases) is general knowledge. It was not confirmed on a real machine this time (chapter 9 D-6).

<a id="1-目的と範囲"></a>

## 1. Purpose and scope

<a id="11-目的"></a>

### 1.1 Purpose

We rewrite the library so that it conforms to the common contract and each profile. The current version checks the optimistic lock against the version (the snapshot's version number). It uses logical shard keys. The aid (aggregate ID string) relies on the user's own stringification. The specification changes all of these (IP 2.1, ADR-0002, 0004, and 0006). In substance, this is a rewrite.

<a id="12-次の版の番号"></a>

### 1.2 Version number of the next release

- The final release of the next major version is **4.0.0**. The current version is 3.0.6.
- The main Snapshot is published to crates.io as a pre-release of the next major version (IP 3). Publication is a requirement, not something to be decided.
- Decision (coordinator, 2026-10-06; chapter 9 D-6): Pre-releases are published by hand. The version is `4.0.0-alpha.N` (N increases from 1), and the owner chooses when to publish. Changes to main are not published automatically (a consequence of IP 3). The preparation PRs are PR 1 to PR 3 in chapter 7. They change the version number and the conditions for publishing.

<a id="13-最初のメジャーに含めるもの"></a>

### 1.3 What the first major version includes

Following IP-D3, IP-D4, and IP-D8, it includes the following.

| What to include | Content | Main rules |
|:--|:--|:--|
| Core | Building the aid, the event envelope, the snapshot envelope, the error classification, the four operations, and the retention settings | T-, H-, W-, R-, S-, E- |
| Memory | A single-process in-memory storage. It declares conformance | MEM-1 to MEM-13 |
| DynamoDB | The layout of three tables and the configuration item | DY-, D- |
| Runner for the conformance test data | Runs all cases on DynamoDB (DynamoDB Local) and on memory | CR, IP 5 |
| Migration tool from rs v3 DynamoDB | Moves old data to the new layout | D-8, chapter 11 of dynamodb.md, IP-D8 |

<a id="14-最初のメジャーから外すもの"></a>

### 1.4 What the first major version excludes

| What to exclude | Reason and handling |
|:--|:--|
| SQLite storage | IP-D3. It is re-released under the new contract in stage 5. It is removed from the new major version (decision; chapter 9 D-2). It remains in the old major version (3.x) |
| Bigtable storage | Same as above |
| Providing a change feed | Memory does not provide it (MEM-13). Reading DynamoDB Streams is not provided either. By the owner's decision (2026-10-06, common to all languages), neither a function that assembles head transitions nor a helper for resynchronization (DY-15) is included. Writes leave records in the head table's Streams as the specification requires, so users can read them themselves. It will be decided again when a change feed scene is added to the conformance data |
| Providing the TTL mode (expiry mode) on memory | MEM-12. A request for the TTL mode is a configuration error, whether or not a history count is given |
| Transparent reading of the old layout | The normal API does not read the old layout. The migration tool is the only support for old data (chapter 8) |
| Migrating the old data of users with their own `KeyResolver` | Chapter 11 of dynamodb.md puts it out of scope |
| Old layouts of other languages such as js, java, and go | Out of scope of this document |
| Implementation of the FNV-1a 64 hash | DynamoDB and memory do not use the hash (ADR-0006). The 4 cases in the conformance data are reported as not applicable (owner's decision, 2026-10-06; sections 5.2.1 and 5.8). They count as neither success nor failure. They run in stage 5, when a storage that uses the hash is released |

<a id="15-この文書で扱わないこと"></a>

### 1.5 What this document does not cover

- Changes to the source code, the tests, the CI settings, and the existing documents (README, DATABASE_SCHEMA, MIGRATION_GUIDE, CHANGELOG, AGENTS.md). These are done in the PRs of chapter 7.
- Procedures and tools of aidlc, openspec, and `.kiro/`. The `aidlc/` directory is not used.

<a id="2-公開-api"></a>

## 2. Public API

<a id="21-設計の方針"></a>

### 2.1 Design policy

The design follows these policies.

- Build on the current structure. The current structure that was confirmed is as follows.
  - The event envelope has private fields and the constructors `new` and `with_manifest` (`lib/src/event_envelope.rs`).
  - The serializer handles only the payload (`lib/src/serializer.rs`).
  - `StorageBackend` absorbs the differences between storages, and `GenericEventStore` holds the shared processing. Both are private `mod`s (`lib/src/lib.rs`, `lib/src/event_store_backend.rs`, `lib/src/generic_event_store.rs`).
  - The facade types of the storages are `EventStoreForMemory` and `EventStoreForDynamoDB`.
- Do not copy the current code. ADR-0001 takes the envelope model as the standard.
- For asynchrony, keep using `#[async_trait]` and `&self` as the current code does, and make the Future `Send`. Decision (coordinator, 2026-10-06). Reason: it keeps the current convention and agrees with MEM-4 (protecting concurrent calls is the storage's responsibility).
- Names follow Rust conventions. Types are `UpperCamelCase`, and functions are `snake_case`. The specification's `persistEvent` is written `persist_event`.

The code below consists of design-proposal signatures. Details of the type bounds may change at implementation time.

<a id="22-モジュールの構成案"></a>

### 2.2 Module structure (proposal)

```text
lib/src/
  lib.rs                 公開する型の再エクスポート
  aggregate_id.rs        AggregateId、AidString             (T-1, T-11, T-12)
  seq_nr.rs              SeqNr、SEQ_NR_MAX                  (T-9, 1.5)
  event_envelope.rs      EventEnvelope、SnapshotEnvelope、SnapshotRead   (T-2, T-3, T-5, T-10, T-13, R-2)
  error.rs               EventStoreError と 5 分類           (E-1, E-2, E-3)
  serializer.rs          EventSerializer、SnapshotSerializer、Json 版   (T-6, T-7, T-8)
  retention.rs           RetentionSettings、保持対象の選択     (S-1, S-2, S-3, S-4)
  event_store.rs         EventStore trait（4 つの操作）        (3 章)
  generic_event_store.rs 共通の入力検査と保存先への委譲（非公開）
  storage_backend.rs     保存先の差を吸収する trait（非公開）
  memory/                メモリの保存先                       (MEM-)
  dynamodb/              DynamoDB の保存先                    (DY-, D-)
  migration/             rs v3 の DynamoDB からの移行の本体。feature `migration`  (9 章 D-1、8.2 節)
```

While the old API coexists, the new code is placed in the `next` module (`#[doc(hidden)] pub mod next`) and moved to the top level in PR 20. Decision (coordinator, 2026-10-06). Reason: it keeps each PR small and lets the CI on main keep passing (section 7.3).

<a id="23-集約-idt-1t-11t-12"></a>

### 2.3 Aggregate ID (T-1, T-11, T-12)

```rust
/// 集約 ID。型名と値の 2 つの文字列を持つ。
/// 利用者の `Display` や `ToString` には依存しない（T-1）。
pub trait AggregateId: Debug + Clone + Send + Sync + 'static {
  /// 集約の種別名。`-` を含んではならない（T-11）。
  fn type_name(&self) -> String;
  /// 集約の値。`-` を含んでよい。
  fn value(&self) -> String;
}

/// ライブラリが組み立てた aid 文字列（T-1）。構築時に T-11・T-12 を検査する。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AidString(String);

impl AidString {
  /// `${型名}-${値}` を組み立てる。
  /// 型名に `-` を含めば契約違反（T-11）、UTF-8 で 1024 バイトを超えれば契約違反（T-12）。
  pub fn from_aggregate_id<AID: AggregateId>(id: &AID) -> Result<Self, EventStoreError>;
  pub fn as_str(&self) -> &str;
}
```

| Element | Rule satisfied | Description |
|:--|:--|:--|
| `AggregateId` does not require `Display` | T-1 | The current version requires `Display + Serialize + Deserialize`. This separates the string used for storage and comparison from the user's stringification |
| `AidString::from_aggregate_id` | T-1 | The separator is the single character `-`. It is built from the type name and the value |
| Check of `-` in the type name | T-11 | Checked at build time. A violation is a `ContractViolation` (rule `T-11`) |
| The 1024-byte check | T-12 | Judged by the total number of UTF-8 bytes of the type name, the separator, and the value. It is not counted in characters (`aid-multibyte-1025`) |
| Where the check lives | T-11, T-12 | Placed in exactly one place, the construction of `AidString`. Memory and DynamoDB receive this type |
| Empty type name and empty value | T-11, T-12 | Allowed. The specification does not forbid them, and the aid string contains `-`, so it never becomes empty (answer of the orchestrator, 2026-10-06). The conformance data has no such case, so the unit tests in `lib` check an empty type name (`-value`), an empty value (`typeName-`), and both empty (`-`) |

`AidString` is public. The `buildAid` check of the conformance data calls this function directly (section 5.2).

<a id="24-整数と時刻の型t-9t-1315"></a>

### 2.4 Integer and time types (T-9, T-13, 1.5)

```rust
/// イベントとスナップショットの通し番号（seq_nr）。
/// T-9: 0 以上 2^53 − 1 以下。上限を超える値を操作に渡すと契約違反になる。
pub type SeqNr = u64;

/// T-9 の上限（2^53 − 1 = 9007199254740991）。
pub const SEQ_NR_MAX: SeqNr = (1 << 53) - 1;

// 発生時刻には chrono の `DateTime<Utc>` を使う。ナノ秒を表せるので、T-3 の丸めは不要である。
```

| Element | Rule satisfied | Description |
|:--|:--|:--|
| `SeqNr = u64` | T-9 | Decision (coordinator, 2026-10-06). Reason: T-9 allows an unsigned type to skip the check. It is the smallest change from the current `usize`. Because it is unsigned, it cannot represent a negative value. T-9 states that "if the language's type cannot represent an out-of-range value, that check is unnecessary". The check for values above the upper limit 2^53 − 1 is done at the entrance of the operation |
| Handling of 0 | T-9, W-6 | 0 is valid under T-9. For an event it is a contract violation (W-6). The `seq_nr` argument of a read allows 0 (it returns all items) |
| `DateTime<Utc>` | T-3 | Can represent nanoseconds. It is not replaced with the store's time. The stored value is returned unchanged by a read |
| Range check | T-13 | The range in which the nanoseconds since the epoch fit in a signed 64-bit integer. If `timestamp_nanos_opt()` is `None`, it is a contract violation. The check is done at the entrance of a write |

Cases of the conformance data with `representation.signed_seq_nr = true` cannot represent a negative number with `u64`. They are reported as not applicable (reason: not representable) and are not counted as successes (section 5.8). Only the cases with this mark are made not representable. Cases with millisecond precision (`representation.time_precision = milliseconds`) are not run, because the type can represent nanoseconds. The reason is recorded, and they are not counted as successes.

<a id="25-イベント封筒t-2t-3t-4t-5t-13"></a>

### 2.5 Event envelope (T-2, T-3, T-4, T-5, T-13)

```rust
/// 1 件のイベントを運ぶ封筒。ライブラリは manifest と payload を解釈しない（T-4）。
/// フィールドは非公開。要素の追加が既存の利用コードを壊さない（T-5）。
#[derive(Debug, Clone, PartialEq)]
pub struct EventEnvelope<AID, P> { /* aggregate_id, seq_nr, occurred_at, manifest, payload */ }

impl<AID, P> EventEnvelope<AID, P> {
  /// aggregate_id・seq_nr・occurred_at・payload は必須（T-2）。manifest は空文字列になる。
  pub fn new(aggregate_id: AID, seq_nr: SeqNr, occurred_at: DateTime<Utc>, payload: P) -> Self;
  pub fn with_manifest(self, manifest: impl Into<String>) -> Self;

  pub fn aggregate_id(&self) -> &AID;
  pub fn seq_nr(&self) -> SeqNr;
  pub fn occurred_at(&self) -> &DateTime<Utc>;
  pub fn manifest(&self) -> &str;
  pub fn payload(&self) -> &P;
  pub fn into_payload(self) -> P;
}
```

| Element | Rule satisfied | Description |
|:--|:--|:--|
| Required arguments of `new` | T-2 | The four elements are enforced by arguments. A missing one does not compile. Because the type cannot represent a missing one, no check for it is needed (T-2, T-10, P-42) |
| Omitting the manifest | T-2 | `new` creates it with an empty string. It is set with `with_manifest` |
| Private fields and `with_*` | T-5 | Carries over the current form. Future elements can be added with `with_*` |
| Storing `occurred_at` | T-3 | Memory keeps it as it is. DynamoDB writes it as a number of epoch nanoseconds |
| Checks for contract violations | T-9, T-13, W-6 | Not checked at construction. Checked at the entrance of an operation. The reason is in section 2.8 |
| Proposal to remove the `Serialize` and `Deserialize` derives | T-7 | Only the payload is serialized. No path that serializes the whole envelope is used. Whether they can be removed will be confirmed at implementation time, by checking that nothing other than the old Bigtable and SQLite uses them (within what was confirmed, the current `EventEnvelope` has `derive(Serialize, Deserialize)` attached) |

<a id="26-スナップショット封筒と読み取りの結果t-10r-2r-3"></a>

### 2.6 Snapshot envelope and the result of a read (T-10, R-2, R-3)

```rust
/// 1 件のスナップショットを運ぶ封筒。書き込みと読み取りで同じ型を使う（1.4）。
/// ヘッドの seq_nr は含めない（ADR-0002）。version は持たない。
#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotEnvelope<A> { /* aggregate, seq_nr, manifest */ }

impl<A> SnapshotEnvelope<A> {
  /// aggregate と seq_nr は必須（T-10）。manifest は空文字列になる。
  pub fn new(aggregate: A, seq_nr: SeqNr) -> Self;
  pub fn with_manifest(self, manifest: impl Into<String>) -> Self;

  pub fn aggregate(&self) -> &A;
  pub fn seq_nr(&self) -> SeqNr;
  pub fn manifest(&self) -> &str;
  pub fn into_aggregate(self) -> A;
}

/// 最新スナップショットの読み取り結果。スナップショット封筒（なくてもよい）と、
/// 読み取り時点のヘッドの seq_nr の組（R-2）。
#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotRead<A> { /* snapshot: Option<SnapshotEnvelope<A>>, head_seq_nr: SeqNr */ }

impl<A> SnapshotRead<A> {
  pub fn snapshot(&self) -> Option<&SnapshotEnvelope<A>>;
  pub fn head_seq_nr(&self) -> SeqNr;
  pub fn into_parts(self) -> (Option<SnapshotEnvelope<A>>, SeqNr);
}
```

| Element | Rule satisfied | Description |
|:--|:--|:--|
| `SnapshotEnvelope::new(aggregate, seq_nr)` | T-10 | Removes the current `version` argument. The two required elements are enforced by arguments |
| `with_manifest` | T-10 | Adds a manifest. The current snapshot envelope has no manifest |
| `SnapshotRead` | R-2, R-3 | Returns the head's seq_nr even when there is no snapshot |
| Keeping the head's seq_nr separately | ADR-0002 | The envelope's seq_nr can be lower than the head. In DynamoDB it can also be higher (R-8) |

<a id="27-payload-とシリアライザt-6t-7t-8"></a>

### 2.7 Payload and serializers (T-6, T-7, T-8)

```rust
/// イベント payload の直列化契約。
pub trait EventSerializer<P>: Debug + Send + Sync + 'static {
  fn serialize(&self, payload: &P) -> Result<Vec<u8>, EventStoreError>;     // 失敗は Serialization
  fn deserialize(&self, data: &[u8]) -> Result<P, EventStoreError>;          // 失敗は Serialization
}

/// 集約状態（スナップショットの aggregate）の直列化契約。
pub trait SnapshotSerializer<A>: Debug + Send + Sync + 'static {
  fn serialize(&self, aggregate: &A) -> Result<Vec<u8>, EventStoreError>;
  fn deserialize(&self, data: &[u8]) -> Result<A, EventStoreError>;
}

/// 既定のシリアライザ。JSON を使う（T-8）。
pub struct JsonEventSerializer<P>(/* PhantomData */);
pub struct JsonSnapshotSerializer<A>(/* PhantomData */);
```

| Element | Rule satisfied | Description |
|:--|:--|:--|
| The serialization input is only the payload | T-7 | No envelope metadata appears in the arguments. Carries over the current structure |
| Proposal not to require serde of `P` and `A` of `EventStore` | T-6 | The bounds of the trait are only `Send + Sync + 'static`. `Serialize + DeserializeOwned` is attached only to the constructor functions that use the default JSON serializer |
| Arbitrary serializers | T-6 | Replaced with `with_serializers`. Usable even for types that do not implement serde |
| JSON by default | T-8 | Uses `serde_json`. A domain type needs only the serde derives. It is not made to implement a trait of the library |

The current `EventStore` trait requires `Serialize + DeserializeOwned` of `P` and `A`. Read literally, T-6 means serde cannot be required when an arbitrary serializer is used. The decision was made with this reading (chapter 10 Q-2).

<a id="28-4-つの操作と非同期の表現共通契約-3-章"></a>

### 2.8 The four operations and the form of asynchrony (common contract chapter 3)

```rust
#[async_trait]
pub trait EventStore: Debug + Clone + Send + Sync + 'static {
  type AID: AggregateId;
  type A: Send + Sync + 'static;   // 集約状態（スナップショットの payload）
  type P: Send + Sync + 'static;   // イベントの payload

  /// 3.1 persistEvent。イベントだけを追記する。
  async fn persist_event(
    &self,
    event: EventEnvelope<Self::AID, Self::P>,
  ) -> Result<(), EventStoreError>;

  /// 3.2 persistEventAndSnapshot。イベントを追記し、同時にスナップショットを書く。
  /// W-9: `snapshot.seq_nr() != event.seq_nr()` なら契約違反。
  async fn persist_event_and_snapshot(
    &self,
    event: EventEnvelope<Self::AID, Self::P>,
    snapshot: SnapshotEnvelope<Self::A>,
  ) -> Result<(), EventStoreError>;

  /// 3.4 getLatestSnapshotById。ヘッドがなければ `None`（R-1）。
  async fn get_latest_snapshot_by_id(
    &self,
    aid: &Self::AID,
  ) -> Result<Option<SnapshotRead<Self::A>>, EventStoreError>;

  /// 3.5 getEventsByIdSinceSeqNr。`seq_nr` 以上のイベント封筒を昇順で、すべて返す（R-4・R-5・R-6）。
  async fn get_events_by_id_since_seq_nr(
    &self,
    aid: &Self::AID,
    seq_nr: SeqNr,
  ) -> Result<Vec<EventEnvelope<Self::AID, Self::P>>, EventStoreError>;
}
```

| Element | Rule satisfied | Description |
|:--|:--|:--|
| No expected-value argument | Chapter 3, W-8 | Removes the current `expected_version`. The expected value for the comparison is determined from `event.seq_nr()` |
| The seq_nr of `persist_event` may be 1 | W-3, MEM-7 | The current version rejects an event-only seq_nr = 1. Under the new contract it can create the head |
| `persist_event_and_snapshot` receives an envelope | T-10, W-9 | The current version receives the value of the aggregate state. It receives an envelope and also carries the manifest |
| `SnapshotRead` as the return value | R-2, R-3 | The current version returns a `SnapshotEnvelope` (with version) |
| One write is one event | H-3 | The argument type is a single `EventEnvelope` |
| `&self` | MEM-4 | The current writes take `&mut self`. The storage owns the exclusive control, so `&self` can accept concurrent calls (decision; section 2.1) |
| Asynchrony with `#[async_trait]` | Chapter 8 | Carries over the current convention. It returns a `Send` Future (decision; section 2.1) |

The entrance checks of an operation (1 to 5 in the next table) are done in one place by the shared layer (`GenericEventStore`). The serialization in 6 is done by the storage after the checks. The order is as follows.

| Order | Check | Rule | Classification |
|:--|:--|:--|:--|
| 1 | Building the aid (`-` in the type name, 1024 bytes) | T-1, T-11, T-12 | Contract violation |
| 2 | `seq_nr` is at most 2^53 − 1 | T-9 | Contract violation |
| 3 | The event's `seq_nr` is not 0 | W-6 | Contract violation |
| 4 | `occurred_at` is within the nanosecond range | T-13 | Contract violation |
| 5 | The snapshot and `seq_nr` match | W-9 | Contract violation |
| 6 | Serializing the payload (done by the storage implementation. Memory does it before the lock, DynamoDB before sending. The store instance holds the serializer. `MemoryStorage` does not) | T-6, T-7 | Serialization |

The comparison (W-3, W-4, W-7, W-8) is realized differently by each storage. Memory does it inside the exclusive control, and DynamoDB does it with a conditional write. The shared layer does not compare. The reason is that H-1 leaves the mechanism of settling to the profile.

The private trait that absorbs the differences between storages is, as a proposal, of the following form. The failure of the retention processing is separated from the result of the append by the type of the return value (S-4). What implements this trait is the store instance, which holds the storage handle (`MemoryStorage` for memory, `Client` for DynamoDB) and the serializers. The owner of the serializers is the store instance (section 2.12).

```rust
#[async_trait]
pub(crate) trait StorageBackend<AID, A, P>: Send + Sync + 'static {
  /// 追記を確定し、保存先ごとの時機で保持処理を行う。
  /// 追記が確定していれば Ok を返す。保持処理の失敗は `AppendReceipt` に載せ、Err にしない。
  async fn append(&self, request: AppendRequest<'_, AID, A, P>) -> Result<AppendReceipt, EventStoreError>;

  /// 最新スナップショットとヘッドの seq_nr を読む（R-1〜R-3）。スナップショットの封筒には aggregate_id がない。
  async fn load_snapshot(&self, aid: &AidString) -> Result<Option<SnapshotRead<A>>, EventStoreError>;

  /// `seq_nr` 以上のイベント封筒を昇順で、すべて返す（R-4〜R-6）。
  /// `aggregate_id` は、公開操作が受け取った元の値である。返す封筒の `aggregate_id` に複製して使う。
  /// 保存先は元の値を保存せず（ジャーナルの属性は aid 文字列だけ。4.2.1 節）、aid 文字列から元の型を復元できない（R-6）。
  async fn load_events(
    &self,
    aggregate_id: &AID,
    aid: &AidString,
    seq_nr: SeqNr,
  ) -> Result<Vec<EventEnvelope<AID, P>>, EventStoreError>;
}

/// 追記の入力。共通の層が検査を済ませた値を、参照で渡す。
pub(crate) struct AppendRequest<'a, AID, A, P> {
  pub aid: &'a AidString,                         // 検査済みの aid 文字列（T-1・T-11・T-12）。保存先のキーに使う
  pub event: &'a EventEnvelope<AID, P>,           // 元の aggregate_id を持つ。直列化は保存先が行う（MEM-6、4.4 節）
  pub snapshot: Option<&'a SnapshotEnvelope<A>>,  // `Some` のとき `seq_nr` は event と一致する（W-9 は検査済み）
}

pub(crate) struct AppendReceipt {
  pub retention_failure: Option<RetentionFailure>, // S-4: 別の経路で知らせる
}
```

`GenericEventStore` builds the `AidString` from the `&AID` of the public operation, then passes both the `&AID` and the `&AidString` to the storage. The `aggregate_id` of the envelopes that `get_events_by_id_since_seq_nr` returns is a copy of the value the caller passed. This is the same when existing data is read with a new store instance (R-6).

If `retention_failure` is present after `append` returns `Ok`, `GenericEventStore` reports it through the path of section 2.11. It then returns `Ok(())`. The notification call is made after the exclusive control of memory is released (MEM-11).

<a id="29-エラー分類4-章e-1e-2e-3"></a>

### 2.9 Error classification (chapter 4, E-1, E-2, E-3)

```rust
/// 5 つの分類を 1 つの型で表す（E-1）。呼び出し側は `match` で分類を区別する。
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EventStoreError {
  /// 楽観ロック。既存集約への新規作成（W-3）、seq_nr の重複（W-7）、ヘッド以下での追記（W-8）。
  #[error("optimistic lock failed: aid={aid}, seq_nr={seq_nr}{}", head_seq_nr_suffix(.head_seq_nr))]
  OptimisticLock { aid: String, seq_nr: SeqNr, head_seq_nr: Option<SeqNr> },

  /// 契約違反。W-6・W-9・T-9・T-11・T-12・T-13 と、W-8 の飛び番。
  #[error("contract violation: rule={rule}{}", contract_violation_suffix(.seq_nr, .snapshot_seq_nr))]
  ContractViolation { rule: ContractRule, seq_nr: Option<SeqNr>, snapshot_seq_nr: Option<SeqNr> },

  /// 直列化。payload の直列化・復元の失敗。
  #[error("serialization failed: phase={phase}")]
  Serialization { phase: SerializationPhase, #[source] source: Box<dyn Error + Send + Sync> },

  /// 設定。生成時の不正な設定値と、保存先に記録された設定との食い違い。
  #[error("configuration error: {reason}")]
  Configuration { reason: ConfigurationReason },

  /// 保存先。通信・保存先の失敗と、読み取ったデータの欠損。
  #[error("storage error: operation={operation}")]
  Storage { operation: StorageOperation, #[source] source: Box<dyn Error + Send + Sync> },
}

/// 契約違反の規則。文字列表現は仕様の規則番号（E-3）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ContractRule { T9, T11, T12, T13, W6, W8Gap, W9, ItemSizeLimit /* D-7（判断の番号であり、必須の規則番号ではない。10 章 Q-3） */ }
```

| Element | Rule satisfied | Description |
|:--|:--|:--|
| Five variants | Chapter 4, E-1 | Optimistic lock, contract violation, serialization, configuration, and storage. The caller tells them apart by type. The classification is not guessed from the message string |
| One type for reads and writes | E-1 | The current version splits into `EventStoreWriteError` and `EventStoreReadError`. The five classifications also appear in reads (failure to restore, missing data), so the proposal merges them into one |
| Display of `OptimisticLock` | E-2 | It includes only the aid string, the seq_nr being appended, and, if known, the head's seq_nr. It does not include connection strings, credentials, or the SDK's raw error text. It has no `source` either |
| Display of `ContractViolation` | E-3 | It includes the violated rule number (such as `W-9`) and the related seq_nr. For W-9, it also includes the differing snapshot number |
| The head number is not a required value of a contract violation | E-3, CR | `ContractViolation` has no head number |
| `source` of `Storage` | Chapter 4 | The SDK's error is put in a `Box<dyn Error>`, and no SDK type appears in the public API's types. The display does not include the raw error text |
| `#[non_exhaustive]` | T-5 | Adding variants or fields does not break the user's `match` |

The string form of `ContractRule` matches the specification's rule number, such as `W-9`. `Display` prints this form. This is to satisfy `must_contain: ["W-9", ...]` of the conformance data.

The signature proposal above does not define the contents of the following types and functions. They are defined at implementation time.

- `SerializationPhase`: a proposal to map it to the phases of the conformance data `serialize-event`, `serialize-snapshot`, `deserialize-event`, and `deserialize-snapshot`.
- `ConfigurationReason`: the reasons for configuration errors. A proposal to map it to the "values that cause a configuration error" in the table of section 2.10 (a history count of 0, `Ttl` on memory, and a mismatch with the configuration recorded in the storage).
- `StorageOperation`: the kind of operation in which a storage error occurred.
- `head_seq_nr_suffix`, `contract_violation_suffix`: helper functions for display. They add `head_seq_nr` or `seq_nr` to the display only when the value exists.
- `RetentionFailure` (section 2.8): the record of a retention-processing failure. A proposal that it has the fields of the `tracing` event of section 2.11 (`aid`, `seq_nr`, `phase`, `error`).

An example of a caller telling the classifications apart.

```rust
match store.persist_event(event).await {
  Ok(()) => { /* 確定した */ }
  Err(EventStoreError::OptimisticLock { .. }) => { /* 読み直して再試行する */ }
  Err(EventStoreError::ContractViolation { rule, .. }) => { /* 呼び出しの誤り。再試行しない */ }
  Err(EventStoreError::Serialization { .. }) => { /* payload の直列化の失敗 */ }
  Err(EventStoreError::Configuration { .. }) => { /* 設定の誤り */ }
  Err(EventStoreError::Storage { .. }) => { /* 保存先の失敗。確定状態は呼び出しの種類で判断する */ }
  Err(_) => { /* 将来の変種 */ }
}
```

The item size excess of D-7 is classified as a contract violation. Here D-7 is the number of a judgment in `dynamodb.md`, not a required rule number. The conformance data expects only the classification (it has neither `error.rule` nor a message condition). The string form of `ContractRule::ItemSizeLimit` is `D-7` (chapter 10 Q-3).

<a id="210-設定s-1s-3mem-3dy-8"></a>

### 2.10 Settings (S-1, S-3, MEM-3, DY-8)

```rust
/// スナップショット保持の設定。メモリと DynamoDB が共有する。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionSettings { /* keep_snapshot_count: Option<usize>, mode: RetentionMode */ }

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetentionMode {
  /// 古い履歴を削除する（S-2）。
  Delete,
  /// 期限切れの印を付ける（S-3）。猶予は秒単位。印を付ける時刻にこの秒数を足した値が期限になる。
  Ttl { grace_seconds: u64 },
}

impl RetentionSettings {
  /// 履歴を持たない。現在のスナップショットだけを持つ（既定）。
  pub fn current_only() -> Self;
  /// 新しい `count` 件の履歴を残す。0 の検査はストアの生成時に行う（S-1）。
  pub fn keep_latest(count: usize) -> Self;
  pub fn with_mode(self, mode: RetentionMode) -> Self;
}
```

| Setting | Type | Rule | Value that causes a configuration error |
|:--|:--|:--|:--|
| History count | `Option<usize>` | S-1, MEM-3 | `Some(0)` |
| Retention mode | `RetentionMode` | S-3, MEM-12 | When `Ttl` is requested on memory, whether or not a history count is given (MEM-3, MEM-12). In DynamoDB, the mode is not used when there is no history count, so `Ttl` is not a configuration error either |
| TTL grace | `u64` (seconds) | S-3 | No check is added. Because it is `u64`, it cannot represent a negative value. 0 is valid (the time of marking becomes the expiry as it is). No upper limit is defined (the orchestrator's answer, 2026-10-06; chapter 10 Q-5). Because there is no upper limit, the expiry (the epoch seconds of the marking time + the grace, section 4.6) is calculated in `u128` and written as a decimal string to an N attribute (the addition in `u64` overflows). The unit tests in `lib` check the boundary where the grace is `u64::MAX` |
| Three table names | `String` | DY-8, chapter 2 of `dynamodb.md` | A mismatch with the configuration recorded in the storage (step 3 of DY-8). There is no check of the name values |
| History index name | `String` | D-3 | Same as above |
| Limit of unprocessed re-requests | `u32` | DY-8 | None. 0 is allowed. The default is 10 (the first attempt is not counted). Reaching the limit is a storage error, not a configuration error |
| Initial wait of a re-request | `Duration` | DY-8 | None. The default is 50 milliseconds |
| Maximum wait of a re-request | `Duration` | DY-8 | None. The default is 2 seconds |

- When there is no history count (`keep_snapshot_count` is `None`), DynamoDB does not use `mode`. Even if `mode` is `Ttl`, it is ignored without an error. No history is created, so there is no place to use the mode. Memory does not provide the expiry mode, so `Ttl` is a configuration error whether or not a history count is given (MEM-3, MEM-12).
- The policy for re-requests (values, formula, jitter, and shortening of the wait) is defined in section 2.12.1.
- The check of the settings is done when the store is created and returned as a `Result`. The approach of making 0 unrepresentable by type (`NonZeroUsize`) is not taken. The reason is that `core-retention-zero` of the conformance data expects a configuration error as a failure of creation. If the type rejected 0, it would never reach the library's check (step 5 of CR).
- The current `with_keep_snapshot_count(Some(0))` returns a contract violation. The new version returns a configuration error (chapter 4, S-1).
- The current `with_delete_ttl(Option<Duration>)` works only when used together with `keep_snapshot_count`. The new version states the mode explicitly with `RetentionMode`.
- No setting item that requests a change feed is created. Neither memory nor DynamoDB has one. Therefore, there is no target in the types for which a request would be a configuration error (MEM-3, MEM-13, chapter 10 Q-7).

<a id="211-保持処理の失敗を知らせる経路s-4mem-11"></a>

### 2.11 The path for reporting retention-processing failures (S-4, MEM-11)

It is reported with a `tracing` WARN event. It is common to memory and DynamoDB and is mandatory. Decision (coordinator, 2026-10-06). Reason: as S-4 and MEM-D7 say, logging is made mandatory. A callback for notification is not added to the public API. If it becomes necessary, it can be added later in a non-breaking form.

```rust
// 保持処理が失敗しても、追記が確定していれば書き込みは成功として返す（S-4）。
// 失敗は、次の固定の対象（target）と項目で WARN のイベントを出す。
tracing::warn!(
  target: "event_store_adapter::retention",
  category = "retention-failure",
  aid = %aid,
  seq_nr,                       // 追記した seq_nr
  phase = "retention-delete",   // retention-query / retention-delete / retention-mark
  error = %error,
  "snapshot retention failed; the append was committed"
);
```

| Element | Rule satisfied | Description |
|:--|:--|:--|
| A write returns success | S-4, MEM-11 | The retention failure is carried in `AppendReceipt` and does not become an `EventStoreError` |
| Notification through a separate path | S-4 | Reported by the log. No type for notification in the public API is fixed (S-4) |
| Memory's notification comes after the exclusive control is released | MEM-11 | `GenericEventStore` emits it after `append` returns |
| Retried in the retention processing after the next append | S-4, MEM-11 | History left behind is selected again and cleaned up in the next retention processing. No dedicated retry mechanism is kept |
| A notification failure does not change the result of the append | MEM-11 | Emitting a log returns no result |

The conformance runner checks `observe.notifications` with a layer (`Layer`) that collects these `tracing` events (section 5.5).

The notification is emitted inside the Future of the public operation. No background task is created. Therefore, the notification appears while the operation's Future is being polled. Even if the async runtime polls that Future on another thread, it can be observed as a notification emitted inside the same Future (section 5.5).

<a id="212-ストアの生成の-api案"></a>

### 2.12 API for creating a store (proposal)

```rust
// メモリ（3 章）
pub struct MemoryStorage { /* Arc<...> */ }           // 保存先。clone すると同じ保存先を共有する（MEM-2）。持つのは記録（バイト列とメタデータ）・設定・排他制御だけで、シリアライザは持たない
impl MemoryStorage {
  pub fn new(retention: RetentionSettings) -> Result<Self, EventStoreError>;  // MEM-1, MEM-3
}
impl<AID, A, P> EventStoreForMemory<AID, A, P>
where
  AID: AggregateId,
  A: Serialize + DeserializeOwned + Send + Sync + 'static,   // 既定の JSON シリアライザの境界（T-6・T-8）
  P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
  pub fn new(storage: MemoryStorage) -> Self;
}
impl<AID, A, P> EventStoreForMemory<AID, A, P>
where
  AID: AggregateId,
  A: Send + Sync + 'static,   // 任意のシリアライザを使うので、serde の境界は付けない（T-6）
  P: Send + Sync + 'static,
{
  pub fn with_serializers(
    storage: MemoryStorage,
    event_serializer: Arc<dyn EventSerializer<P>>,
    snapshot_serializer: Arc<dyn SnapshotSerializer<A>>,
  ) -> Self;
}

// DynamoDB（4 章）
pub struct DynamoDbTables {
  pub journal_table_name: String,
  pub snapshot_table_name: String,
  pub head_table_name: String,
  pub snapshot_history_index_name: String,
}
#[derive(Debug, Clone)]
pub struct DynamoDbOptions {
  pub retention: RetentionSettings,             // S-1・S-3。既定は `RetentionSettings::current_only()`
  pub unprocessed_retry_limit: u32,              // DY-8。未処理の再要求の上限（初回を数えない）。既定は 10。0 を許す
  pub unprocessed_retry_initial_delay: Duration, // DY-8。指数バックオフの待ちの初期値（std::time::Duration）。既定は 50 ミリ秒
  pub unprocessed_retry_max_delay: Duration,     // DY-8。待ちの上限。既定は 2 秒
}
impl Default for DynamoDbOptions { /* 上の既定値 */ }

impl<AID, A, P> EventStoreForDynamoDB<AID, A, P>
where
  AID: AggregateId,
  A: Serialize + DeserializeOwned + Send + Sync + 'static,   // 既定の JSON シリアライザの境界（T-6・T-8）
  P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
  /// 設定項目を照合し、なければ作る（DY-8）。非同期で、失敗しうる。
  pub async fn open(
    client: aws_sdk_dynamodb::Client,
    tables: DynamoDbTables,
    options: DynamoDbOptions,
  ) -> Result<Self, EventStoreError>;
}

impl<AID, A, P> EventStoreForDynamoDB<AID, A, P>
where
  AID: AggregateId,
  A: Send + Sync + 'static,   // 任意のシリアライザを使うので、serde の境界は付けない（T-6）
  P: Send + Sync + 'static,
{
  /// `open` と同じ照合を行い、イベントとスナップショットのシリアライザを差し替える。
  pub async fn open_with_serializers(
    client: aws_sdk_dynamodb::Client,
    tables: DynamoDbTables,
    options: DynamoDbOptions,
    event_serializer: Arc<dyn EventSerializer<P>>,
    snapshot_serializer: Arc<dyn SnapshotSerializer<A>>,
  ) -> Result<Self, EventStoreError>;
}
```

- Memory's storage appears in the types as `MemoryStorage`. Decision (coordinator, 2026-10-06). Reason: as MEM-3 and MEM-D11 say, the form in which the storage holds the settings immutably can be expressed by the type.
- The owner of the serializers is the store instance. `MemoryStorage` does not hold them. If it did, `MemoryStorage` would need the type parameters `A` and `P`, which would conflict with the arguments of `with_serializers`. One `MemoryStorage` may be shared by multiple stores that have different serializers. What MEM-2 and MEM-3 require to be shared are the records, the settings, and the exclusive control, not the serializers. If bytes written by one store cannot be restored by a store with a different serializer, the read returns `Serialization` (`deserialize-event`, `deserialize-snapshot`).
- The constructor functions are split into the group with serde bounds (`new`, `open`; the default JSON serializer) and the group without bounds (`with_serializers`, `open_with_serializers`; arbitrary serializers) (T-6, T-8, section 2.7). The arguments of `open_with_serializers` are the three of `open` plus the two serializers for events and snapshots.
- `EventStoreForMemory` and `EventStoreForDynamoDB` keep the current names. This is to reduce the migration effort.
- `EventStoreForDynamoDB::open` is asynchronous and returns a `Result` because DY-8 reads the three tables when the store is created and writes them if they are missing.
- `KeyResolver`, `DefaultKeyResolver`, the shard count, and the GSI name of the old journal are abolished (DY-16, ADR-0006).

<a id="2121-未処理の再要求の方針dy-8dy-9"></a>

#### 2.12.1 Policy for unprocessed re-requests (DY-8, DY-9)

These are values decided in this design. They are confirmed in the coordinator's review. The reading of the settings in DY-8 has a case that does not specify `retry_limit` (`dynamodb-config-unprocessed-keys`). For this reason, the default limit must be at least 1.

| Item | Decided value |
|:--|:--|
| Limit | `unprocessed_retry_limit`. The default is 10. The first attempt is not counted. 0 is also allowed |
| Initial wait | `unprocessed_retry_initial_delay`. The default is 50 milliseconds |
| Maximum wait | `unprocessed_retry_max_delay`. The default is 2 seconds |
| Wait formula | Before the k-th re-request, wait min(initial wait × 2^(k−1), maximum wait). k is counted from 1 |
| Jitter | Not added. If the sequence of waits changed every time, the runner could not check `exponential_backoff` |
| Total wait | With the defaults, at most about 11 seconds (after 50, 100, 200, 400, 800, and 1600 milliseconds, 2000 milliseconds four times) |
| Reaching the limit | `Storage`. When resending for retention, it is made a retention failure |
| Where it is used | DY-8 (reading the settings), DY-9 (reading the latest snapshot), and the resending of `UnprocessedItems` of the retention `BatchWriteItem`. The same policy is used (chapter 10 Q-6) |

- Reason: the target keys are few, 2 to 3. The waits become fixed values, so `exponential_backoff` of the conformance data can be checked.
- The items are named `unprocessed_retry_*` because they are used not only for reading the settings but also for the resends of reads and retention.
- The waiting is done by `RetrySleeper`. `RetrySleeper` is a `#[doc(hidden)]` trait that exists only with the `test-hooks` feature, and has `async fn sleep(&self, delay: Duration)`. It is separate from `Clock` (the clock for TTL marking; section 4.6).
  - How to pass it: pass it as an argument of a `#[doc(hidden)]` constructor function. The shape of `DynamoDbOptions` does not change with or without the feature.
  - Default when not passed: use the SDK's waiting mechanism that `Client`'s `config().sleep_impl()` returns. If `sleep_impl()` is `None`, `open` returns `Configuration`.
  - Confirmed range: `aws-sdk-dynamodb 1.130.0` has `Client::config()` and `Config::sleep_impl()`. Whether a `Client` built with defaults has a `sleep_impl` has not been confirmed.
  - The runner records the waits requested of `RetrySleeper` and does not actually wait. This is how `exponential_backoff` (section 5.5.1) is checked.

<a id="213-現行の公開-api-との対応表"></a>

### 2.13 Correspondence table with the current public API

The current (3.0.6) public API is `lib/src/lib.rs`, `lib/src/types.rs`, `lib/src/event_envelope.rs`, `lib/src/key_resolver.rs`, `lib/src/serializer.rs`, and the types of each storage.

| Current | Next major | Handling |
|:--|:--|:--|
| `AggregateId` (requires `Display + Serialize + Deserialize`) | `AggregateId` (only `type_name` and `value`) | Replaced. Removes the `Display` and serde requirements (T-1) |
| Building the aid (`to_string()` and `KeyResolver`) | `AidString::from_aggregate_id` | Replaced. The library builds it (T-1, T-11, T-12) |
| `EventEnvelope<AID, P>` (seq_nr of `usize`) | `EventEnvelope<AID, P>` (`SeqNr`) | Keeps the form. Changes the type (T-9) |
| `SnapshotEnvelope<A>::new(aggregate, seq_nr, version)` | `SnapshotEnvelope::new(aggregate, seq_nr)` + `with_manifest` | Removes `version` and adds `manifest` (T-10, ADR-0004) |
| `SnapshotEnvelope::version()` | None | Deleted |
| `EventStore::persist_event(&mut self, event, expected_version)` | `persist_event(&self, event)` | Deletes the expected-value argument (W-8) |
| `EventStore::persist_event_and_snapshot(&mut self, event, aggregate, expected_version)` | `persist_event_and_snapshot(&self, event, snapshot)` | Receives an envelope. Deletes the expected value (T-10, W-9) |
| `get_latest_snapshot_by_id → Option<SnapshotEnvelope<A>>` | `→ Option<SnapshotRead<A>>` | Returns the head's seq_nr separately (R-2) |
| `get_events_by_id_since_seq_nr(aid, usize)` | `(aid, SeqNr)` | Changes the type |
| `EventStoreWriteError` (5 variants), `EventStoreReadError` (3 variants) | `EventStoreError` (5 variants) | Replaced. There are 5 classifications (E-1) |
| `OptimisticLockError(String)`, `ContractViolation(String)` | Structured variants | Replaced. The function that builds the string, `format_optimistic_lock_message`, is deleted (E-2, E-3) |
| `IOError`, `OtherError` | `Storage` | Merged. Missing data that was read is also `Storage` |
| `SerializationError`, `DeserializationError` | `Serialization { phase }` | Merged |
| `EventSerializer`, `SnapshotSerializer` (return `EventStoreWriteError`/`ReadError`) | Same names (return `EventStoreError`) | Keeps the form. Changes the error type |
| `JsonEventSerializer`, `JsonSnapshotSerializer` | Same names | Carried over (T-8) |
| `KeyResolver`, `DefaultKeyResolver` | None | Deleted (DY-16) |
| `EventStoreForDynamoDB::new(client, journal, journal_aid_index, snapshot, snapshot_aid_index, shard_count)` | `EventStoreForDynamoDB::open(client, DynamoDbTables, DynamoDbOptions).await` | Replaced. Three tables, no shard count |
| `with_key_resolver` | None | Deleted |
| `with_keep_snapshot_count`, `with_delete_ttl` | `RetentionSettings` | Replaced (S-1, S-3) |
| `with_event_serializer`, `with_snapshot_serializer` | `open_with_serializers`, `with_serializers` | Replaced |
| `EventStoreForMemory::new()` | `MemoryStorage::new(..)?` + `EventStoreForMemory::new(storage)` | Replaced. Makes independence and sharing explicit (MEM-2) |
| The `Clone` requirement on memory's `A` and `P` | None | Deleted (MEM-6) |
| `EventStoreForBigtable`, `EventStoreForSqlite`, features `bigtable`, `sqlite`, `sqlite-system` | None | Deleted from the new major version (IP-D3; decision: chapter 9 D-2). They remain in the old major version (3.x) |
| `GenericEventStore`, `StorageBackend`, `SnapshotMaintenance` (private) | Rebuilt (private) | Not part of the public API. Outside the scope of semver |
| `maintenance()` of each storage | None | Deleted. `RetentionSettings` holds the settings |

The replaced old names and functions are not kept. No `#[deprecated]` aliases are made either. Guidance for users of the old API is written in chapter 8.

<a id="3-メモリの実装方針"></a>

## 3. Implementation policy for memory

Satisfy the memory profile (`memory.md`, agreed 2026-10-05). The differences from the current `lib/src/event_store_for_memory.rs` are listed in chapter 8 of the profile. The current state that was confirmed is as follows.

- It shares one lock of `Arc<Mutex<HashMap<String, ..>>>`. It does not `.await` while holding the lock.
- The comparison is done with the snapshot envelope's `version`. It has no separate head.
- It rejects an event-only `seq_nr = 1`.
- It requires `Clone` of the payload and the aggregate state, and stores and returns copies.
- It does no retention processing (thinning out the history).

<a id="31-規則ごとの実現方針"></a>

### 3.1 Implementation policy for each rule

| Rule | Policy |
|:--|:--|
| MEM-1 | The storage `MemoryStorage` holds only in-process memory. No API for files, recovery after restart, or sharing between processes is created. No API to wipe everything during use is created either. All records are lost with the storage's lifetime (when the last reference is dropped) |
| MEM-2 | `MemoryStorage::new` creates an empty, independent storage. Only stores that were given a `clone` of a `MemoryStorage` share the same storage. `clone` increases the `Arc` reference, so the sharing is explicit. Storages are not shared by sameness of a name or an identifier. The current `EventStoreForMemory::new()` implicitly creates a new storage and shares it by `clone`. The unit of sharing is shown in the type (`MemoryStorage`) |
| MEM-3 | The settings are checked at creation (`MemoryStorage::new`). A history count of 0 and `RetentionMode::Ttl` (whether or not a history count is given; MEM-12) are configuration errors. The storage holds the settings immutably. Stores that share it use the storage's settings. A store has no API that overwrites the settings |
| MEM-4 | The storage has one exclusive control (`std::sync::Mutex`) for the whole store. All operations of reading, writing, and retention read and change the state inside this lock. It can be called from multiple threads at once. It does not `.await` while holding the lock. The conformance data has no concurrent scene, so the unit tests in `lib` are the only check. They check that when multiple threads append the same `seq_nr` only one is settled, that a gap in numbers is a contract violation, and that the history of another aggregate is not lost (PR 8 and PR 9 of section 7.2) |
| MEM-5 | The index of records is an exact match on `AidString` (`HashMap<AidString, AggregateRecord>`). It does not identify by hash alone and does not select by prefix match. The aid and the seq_nr are different values, and within a record the seq_nr is a separate key. The shared layer completes the checks of T-9, T-11, T-12, and T-13 |
| MEM-6 | The stored value is held split into the metadata (`seq_nr`, `occurred_at`, `manifest`) and the serialized payload bytes. It is serialized with the serializer at write time and restored at fetch time. Changes to the user's input or to fetched results do not propagate to the stored value. `Clone` is not required of `A` and `P` (T-6). The store instance holds the serializers. `MemoryStorage` does not. Stores with different serializers may share the same `MemoryStorage`, and if restoration is impossible, `Serialization` is returned (section 2.12) |
| MEM-7 | Section 3.2 |
| MEM-8 | Section 3.3 |
| MEM-9 | Section 3.3 |
| MEM-10 | Section 3.4 |
| MEM-11 | Section 3.4 |
| MEM-12 | `RetentionMode::Ttl` is a configuration error in `MemoryStorage::new`, whether or not a history count is given. It has no TTL mark, grace, or clock |
| MEM-13 | Does not provide a change feed. Does not create a setting item that requests a change feed (chapter 10 Q-7, answered) |

<a id="32-書き込みmem-7h-1h-2w-3w-9"></a>

### 3.2 Writes (MEM-7, H-1, H-2, W-3 to W-9)

The settling method is "prepare first, then publish everything that cannot fail in one go".

```text
共通の層
  1. aid の組み立て、seq_nr・時刻・W-6・W-9 の検査          （契約違反）
保存先（排他制御の前）
  2. payload とスナップショットの直列化                      （直列化。ここで失敗しても記録は変わらない）
保存先（排他制御の中）
  3. ロックを取る
  4. ヘッドを aid の完全一致で読む
  5. 照合する                                              （3.2.1）
  6. 新しい記録を、ローカルの値として準備する
  7. 確定前のフック点（commit。試験だけ）                   （失敗しても記録は変わらない）
  8. 準備した記録を、状態へまとめて公開する                 （ここが確定。この手順は失敗しない代入だけ）
  9. 保持処理を行う                                        （失敗しても確定を取り消さない。3.4 節）
 10. ロックを外す
共通の層
 11. 保持の失敗があればログで知らせる                        （MEM-11）
 12. 成功を返す
```

- Up to step 6, the state is not changed. The publication in step 8 updates the journal, the head, the current snapshot, and the history inside one lock. No intermediate state is visible from another operation (H-1).
- Step 8 consists only of additions and replacements in memory and contains no processing that can fail. A failure during preparation leaves no change in the head, the journal, or the snapshot (MEM-7).
- The comparison is done only against the head's seq_nr (H-2). The snapshot is not used.

<a id="321-照合w-3w-4w-7w-8"></a>

#### 3.2.1 Comparison (W-3, W-4, W-7, W-8)

Let `h` be the head's seq_nr. If there is no head, regard `h = 0`.

| Condition | Result | Rule |
|:--|:--|:--|
| `event.seq_nr == 1` and no head | New creation. Creates the journal and the head. If there is a snapshot, it also creates the current item | W-3 |
| `event.seq_nr == 1` and a head exists | Optimistic lock | W-3 |
| `event.seq_nr == h + 1` (`h ≥ 1`) | Update. Advances the head | W-4, W-8 |
| `event.seq_nr ≤ h` (`h ≥ 1`) | Optimistic lock (duplicate or old number) | W-7, W-8 |
| `event.seq_nr ≥ h + 2` | Contract violation (a gap; including when there is no head) | W-8 |

- An event-only `seq_nr = 1` can also create a new aggregate (MEM-7). It creates the head without a snapshot. The current version rejects this.
- The optimistic lock error carries only the aid string, the seq_nr being appended, and, if known, the head's seq_nr (E-2). Memory knows the head, so it carries the head's seq_nr.
- The contract violation for a gap carries the rule `W-8` and `event.seq_nr` (E-3).

<a id="33-読み取りmem-8mem-9r-1r-8"></a>

### 3.3 Reads (MEM-8, MEM-9, R-1 to R-8)

- `get_latest_snapshot_by_id`: inside the lock, read the head and the current snapshot at the same point in time (MEM-8). If there is no head, `None` (R-1). If there is a head, return the pair of the envelope (which may be absent) and the head's seq_nr (R-2, R-3). Because it is atomic, the envelope's seq_nr never exceeds the head (R-8, MEM-8).
- `get_events_by_id_since_seq_nr`: inside the lock, take out all matching envelopes at the same point in time (MEM-9). Restore them after releasing the lock. A failure to restore is `Serialization` (R-5, R-6). The envelope's `aggregate_id` is a copy of the original value the caller passed (section 2.8). The record does not hold the original value.
- The values taken out are copies of the serialized bytes. Restoration happens after the lock is released, so the time to restore does not block other operations. A restored value belongs to the caller and does not propagate to the stored value (MEM-6).
- Calling two public operations in a row does not guarantee reading the same point in time. The memory profile also admits this.

<a id="34-保持処理mem-10mem-11s-1s-4"></a>

### 3.4 Retention processing (MEM-10, MEM-11, S-1 to S-4)

| Aspect | Policy |
|:--|:--|
| Condition for keeping history | When the history count is `Some(n)`, a write with a snapshot adds one history item. For `None`, only the current snapshot |
| Timing | After the append is settled, inside the same exclusive control. It also runs for an event-only append, to clean up history left behind last time (MEM-10). The timing differs from DynamoDB (D-9). It is decided inside the storage's `append`, so the shared layer does not hold the retention timing |
| Selection | The four steps "take the visible history", "add the history just written (without overlap)", "sort in descending order", and "keep the newest n and select those older" are made into the pure function `select_expired_history`. It is shared with DynamoDB (S-2). In memory the visible history is always accurate, but the steps are kept the same so that `history_pages` of the conformance data is reproduced on both |
| Deletion | Removes the selected history from the logical history. The journal and the head are not removed (MEM-10) |
| Failure | Even if retention processing fails, the settlement is not cancelled. The failure is carried in `AppendReceipt` (S-4) |
| Notification | After the lock is released, the shared layer reports it with a `tracing` log (MEM-11, section 2.11) |
| Retry | There is no dedicated retry. The retention processing after the next append re-reads the current history and cleans up (MEM-11) |

The normal retention processing of memory contains no processing that can fail. A retention failure occurs only when injected by a test (section 5.6).

<a id="35-試験のためのフック点test-hooks-feature"></a>

### 3.5 Hook points for tests (the `test-hooks` feature)

As a proposal, the following injection points are placed in the storage so that tests can inject failures before settling, retention failures, and observe the history. They are enclosed by the cargo feature `test-hooks` (disabled by default, `#[doc(hidden)]`) and do not appear in the API of ordinary users.

| Hook point | Where it is called | Purpose |
|:--|:--|:--|
| `before_commit(aid, seq_nr)` | Step 7 of section 3.2 | A `commit` failure. If it returns a failure, the record does not change |
| `read_events(aid)` | Before taking out in section 3.3 | A `read-events` failure |
| `read_snapshot(aid)` | Before taking out in section 3.3 | A `read-snapshot` failure |
| `retention_visible_history(aid)` | "Take the visible history" in section 3.4 | A `retention-query` failure, and replacing the visible history with `history_pages` |
| `retention_delete(aid, seq_nrs)` | Before the deletion in section 3.4 | A `retention-delete` failure, `final-retention-failure` |
| `history_view(aid)` | Test observation | `observe.history` (the set of history without marks. Memory has no marked history) |

- When to add the hooks: `before_commit`, `read_events`, `read_snapshot`, and `history_view` are added in PR 8, and `retention_visible_history` and `retention_delete` in PR 9 (section 7.2).
- A hook does not change whether a write succeeds or fails. An instruction to return a failure is issued only by the scenes that inject failures.
- Failures of `serialize-*` and `deserialize-*` do not use hooks. The runner wraps the serializer and fails it on the specified call (`with_serializers`, section 5.6).
- The presence of hooks does not change the production processing order. When a hook is `None`, each step runs in the same order.

<a id="36-現行との差の対応"></a>

### 3.6 Correspondence with the differences from the current version

| Current (confirmed fact) | Next major | Rule |
|:--|:--|:--|
| Compares with `SnapshotEnvelope::version` | Compares with the head's seq_nr | H-2, W-8 |
| Has no head | Has a head (seq_nr and the last event envelope) | H-4 |
| New creation only with `persist_event_and_snapshot` | New creation is also possible with an event alone | W-3, MEM-7 |
| An update to a non-existent aggregate is an optimistic lock | Contract violation (a gap) | W-8 |
| Stores and returns copies made with `Clone` | Stores serialized and restores at fetch | MEM-6, T-6 |
| No retention processing | Retention processing after settlement | MEM-10 |
| Turns a lock failure (poisoning) into a string error | Returns it in the `Storage` classification. Does not panic | Chapter 4 |

<a id="4-dynamodb-の実装方針"></a>

## 4. DynamoDB implementation policy

Satisfy the DynamoDB profile (`dynamodb.md`). The current state that was confirmed (`lib/src/event_store_for_dynamodb.rs`) is as follows.

- It uses two tables (journal and snapshot) and the keys `pkey` (the shard) and `skey` (a string).
- The optimistic lock is a conditional update of the `version` attribute of the snapshot item.
- Events are read with a `Query` on the old journal's GSI. Paging continues with `LastEvaluatedKey`.
- Retention processing counts the items and then selects the excess. It is returned as a write failure.
- A failure is turned into an `OptimisticLockError` whenever `TransactionCanceledException` has a reason.

The scope of the rewrite is large. Keys, attributes, the table structure, comparison, reads, and retention all change.

<a id="41-使う-sdk-とその版"></a>

### 4.1 The SDK to use and its version

| Item | Content |
|:--|:--|
| SDK | `aws-sdk-dynamodb` (the official AWS SDK for Rust). As the constructor functions receive a `Client`, the proposal is not to make `aws-config` a required dependency of the library itself (the current version has it as a dependency of the feature `dynamodb`) |
| Current specification | The workspace's `aws-sdk-dynamodb = "1.23.0"` and `aws-config = "1.2.1"` (`Cargo.toml`). Confirmed fact |
| APIs the design assumes | Their existence was confirmed in the source of `aws-sdk-dynamodb 1.130.0`: `return_values_on_condition_check_failure` of `Put` and `Update`, `code()` and `item()` of `CancellationReason`, `interceptor`, `http_client`, and `retry_config` of `Config::Builder`, and `read_before_transmit` and `modify_before_transmit` of the `Intercept` trait. The confirmation consisted only of reading the source that was already fetched locally |
| Minimum version | Whether the APIs above exist in 1.23.0 has not been confirmed. Confirm the minimum version at implementation time. If it cannot be confirmed, raise the lower bound to 1.130.0 |
| Resolved version | Because there is no `Cargo.lock`, it has not been confirmed. This document does not treat a candidate version as the resolved version |
| Minimum supported Rust version (MSRV) | Do not change the current `rust-version = "1.94.1"` (`lib/Cargo.toml`). It depends on the version the AWS SDK requires (confirmed fact) |
| Added dependency | Random numbers for creating the store identifier (`store_id`). The proposal is to make `uuid` (v4) a dependency of the `dynamodb` feature. The version is decided at implementation time |
| Reading Streams | The library does not read Streams. It includes no change feed functions (assembling head transitions and a resynchronization helper) (section 1.4; the owner's decision, 2026-10-06). `aws-sdk-dynamodbstreams` is not made a dependency |

<a id="42-3-テーブルの配置dy-2dy-3dy-16dy-17dy-18dy-19"></a>

### 4.2 Layout of the three tables (DY-2, DY-3, DY-16, DY-17, DY-18, DY-19)

Creating the tables is done outside the library. The names are given with `DynamoDbTables`.

| Table | Partition key | Sort key | GSI | Streams | TTL |
|:--|:--|:--|:--|:--|:--|
| journal | `aid` (S) | `seq_nr` (N) | None | Disabled | None |
| snapshot | `aid` (S) | `skey` (N) | For history (section 4.6). The key is `(aid, active_history_seq_nr (N))`, the projection is `KEYS_ONLY` | Disabled | Attribute `ttl`. Enabled only in TTL mode (DY-2) |
| head | `aid` (S) | None | None | Enabled, `NEW_IMAGE` (DY-3, DY-12, D-4) | None |

- The partition key is the `AidString` string itself. Neither logical shards nor hashes are used (DY-16).
- The sort key of the journal is `seq_nr`. The `skey` of the snapshot is 0 for the current one and the history's `seq_nr` for history (DY-17).
- The three tables are placed in the same region. Writes are also made only from that region (DY-18). Global tables are out of scope. The library does not detect differences in region (a way to detect them has not been confirmed). That the three tables go through the same `Client` is guaranteed by the shape of the API (chapter 10 Q-13).
- Reads are strongly consistent. Only the GSI read of the retention processing is eventually consistent (DY-18).
- The APIs used are only `GetItem`, `BatchGetItem`, `Query`, `TransactWriteItems`, `BatchWriteItem`, and `UpdateItem`, narrowed to the aid of one aggregate. `Scan` is not used (DY-19). Only the migration tool (chapter 8) uses `Scan`.

<a id="421-項目の属性dynamodbmd-の-5-章"></a>

#### 4.2.1 Attributes of the items (chapter 5 of `dynamodb.md`)

| Item | Attributes (type) |
|:--|:--|
| Journal | `aid` (S), `seq_nr` (N), `occurred_at` (N, epoch nanoseconds), `manifest` (S), `payload` (B) |
| Snapshot (current) | `aid`, `skey` (N, 0), `seq_nr` (N), `manifest` (S), `payload` (B), `last_updated_at` (N, milliseconds of `occurred_at`) |
| Snapshot (history) | The same attributes as the current one (`skey` is the history's `seq_nr`). `active_history_seq_nr` (N) only while there is no mark. When the TTL mark is set, it has `ttl` (N, epoch seconds) and loses `active_history_seq_nr` |
| Head | `aid` (S), `type_name` (S), `seq_nr` (N), `events` (L). Each element of `events` is an M with `seq_nr`, `occurred_at`, `manifest`, and `payload`. The number of elements is 1 (H-3) |
| Configuration item | `aid` = `__config__`, `store_id` (S), `layout_version` (N). The journal has `seq_nr` = 0, the snapshot has `skey` = 0, and the head has no sort key |

- The current snapshot has neither `active_history_seq_nr` nor `ttl`. An item with no expiry does not have the `ttl` attribute itself (the current version writes 0; D-3).
- `occurred_at` is made from `DateTime<Utc>` as epoch nanoseconds (`timestamp_nanos_opt()`) and written as a decimal number (N). An out-of-range value is a contract violation before writing (T-13).
- `manifest` is always written. When omitted, it is an empty string (T-2, T-10).
- The attribute names and types match the expectations of `conformance/dynamodb/layout.json` and `item-shapes.json` (item 2 of IP 5).

<a id="43-設定項目と照合dy-8"></a>

### 4.3 Configuration item and comparison (DY-8)

`EventStoreForDynamoDB::open` performs the following steps.

```text
1. 3 つの設定項目のキーを 1 回の BatchGetItem に入れる（各テーブル ConsistentRead = true）
     journal:  (__config__, seq_nr = 0)   snapshot: (__config__, skey = 0)   head: (__config__)
2. Responses を、テーブル名とキーごとに蓄積する
3. UnprocessedKeys があれば、そのキーだけを、指数バックオフで強整合のまま再要求する
     未処理がなくなるまで、「存在しない」と判定せず、次の手順に進まない
     再要求の回数が上限（unprocessed_retry_limit）に達したら、保存先エラー（設定エラーではない）
4. 3 つの結果で場合分けする
     (a) 3 つともない        → 新しい store_id を作る。5 へ
     (b) 3 つともあり、store_id が一致し、layout_version が自分の版（1）と同じ → 生成を続ける
     (c) それ以外（一部だけ、store_id の食い違い、layout_version の違い）    → 設定エラー
5. 1 つの TransactWriteItems で、3 つの設定項目を attribute_not_exists(aid) の条件つき Put で書く
     条件が成り立たず失敗したら、別の生成が先に書いた。応答を捨て、1 から 3 をやり直して 4 へ
     取り消し理由が TransactionConflict（別の生成が書いている最中）のときも、応答を捨て、1 から 3 をやり直して 4 へ。
     ただし、やり直した 4 が (a)（3 つともない）なら、作成を繰り返さずに保存先エラー（DY-8、P-44）
     成功したら生成を続ける
```

| Aspect | Design | Rule |
|:--|:--|:--|
| Reading | Put the 3 items in one `BatchGetItem`. `ConsistentRead = true`. Re-requests are also strongly consistent | DY-8 |
| Partial response | Re-request only the keys in `UnprocessedKeys`. While unprocessed items remain, items missing from the response are not treated as "nonexistent" | DY-8 |
| Limit of re-requests | `unprocessed_retry_limit` of `DynamoDbOptions` (the number of times, not counting the first attempt. The default is 10. 0 is allowed). The specification does not define a value. Reaching the limit is `Storage` (`dynamodb-config-retry-exhausted`) | DY-8 |
| Backoff | Exponential. Before the k-th time, min(initial wait × 2^(k−1), maximum wait). The initial wait is 50 milliseconds, the maximum wait is 2 seconds, and there is no jitter (section 2.12.1). The waiting is done with `RetrySleeper`, and the runner can shorten it. It is separate from `Clock` (section 5.5) | DY-8 |
| Creation | Write the 3 items in one `TransactWriteItems` with conditional `Put`s only. Do not use an independent `ConditionCheck` | DY-8 (P-19) |
| Overlapping creation | If the cancellation reason is a failed condition or `TransactionConflict`, read again. If all three are still missing even after the re-read following a `TransactionConflict`, return `Storage` without repeating the creation. The conformance data has no `TransactionConflict` scene, so the unit tests in `lib` check the case where the configuration items are found by the re-read and the case where all three are missing and the result is `Storage` (PR 11) | DY-8 (P-44) |
| Conflict | If the condition fails, discard the response and re-read the 3 items strongly consistently. Redo the case analysis with the result | DY-8 (P-19) |
| Mismatch | If only some of the tables have it, the `store_id` differs, or the `layout_version` differs, return `Configuration` and stop creation | DY-8, chapter 4 (P-40) |
| Permissions | `dynamodb:BatchGetItem` and `dynamodb:PutItem` on the 3 tables | DY-8 |
| `store_id` | Created randomly at the first creation. The same value is written to the 3 items | DY-8 |
| Attributes of the configuration item | Only `store_id` and `layout_version`. The snapshot's configuration item has no `active_history_seq_nr` | DY-8 |

- `open` does not re-read the configuration item in the processing after the store has been created. A mismatch of the settings is detected only at creation.
- The configuration item's `aid` (`__config__`) does not contain `-`, so it does not collide with an `AidString` (which always contains `-`) (T-11).

<a id="44-書き込みd-5d-6d-7w-8h-1"></a>

### 4.4 Writes (D-5, D-6, D-7, W-8, H-1)

One write is one `TransactWriteItems`. The point at which the transaction succeeds is the settlement (H-1).

<a id="441-トランザクションの構成"></a>

#### 4.4.1 Structure of the transaction

| Order (proposal) | Action | Target | Condition | When included |
|:--|:--|:--|:--|:--|
| 1 | `Put` | journal | `attribute_not_exists(aid)` | Always |
| 2 | `Put` | head | `attribute_not_exists(aid)` | `event.seq_nr == 1` (new creation) |
| 2 | `Update` | head | `seq_nr = :prev` (`:prev` = `event.seq_nr − 1`). Overwrites `seq_nr` and `events` | `event.seq_nr > 1` |
| 3 | `Put` | snapshot (current, `skey` = 0) | None | There is a snapshot |
| 4 | `Put` | snapshot (history, `skey` = `seq_nr`) | None | There is a snapshot and the history count is `Some(n)` |

- At most 4 actions, which fits within the limits of `TransactWriteItems` (100 actions, 4 MB in total).
- `ReturnValuesOnConditionCheckFailure = ALL_OLD` is attached to the head's `Put` and `Update` (D-5).
- The order is the order in which the library builds the request. The specification does not enforce an order. The cancellation reasons are read by the position of the action in the request (section 4.4.2).
- The current approach of "writing the current snapshot with a conditional update on version" is dropped. The current snapshot is an unconditional `Put`. The head's condition serializes writes (chapter 6.1 of `dynamodb.md`).
- The snapshot's `seq_nr` equals `event.seq_nr` (W-9). The shared layer completes the check.

<a id="442-失敗の分類d-5d-6w-3w-7w-8"></a>

#### 4.4.2 Classifying failures (D-5, D-6, W-3, W-7, W-8)

Read the `CancellationReasons` of `TransactionCanceledException` by the position of the action in the request. No additional read is made (D-5).

| How the reason is read | Classification | Rule |
|:--|:--|:--|
| On a new creation, the head is `ConditionalCheckFailed` | Optimistic lock (head number unknown) | W-3 |
| On an update, the head is `ConditionalCheckFailed` and the old item's `seq_nr` is `h`. `event.seq_nr ≤ h` | Optimistic lock (`head_seq_nr = h`) | W-8 |
| Same as above. `event.seq_nr ≥ h + 2` | Contract violation (rule `W-8`, a gap) | W-8 |
| Same as above. No old item is returned | Regard `h = 0`. The update's `seq_nr ≥ 2`, so it is a contract violation (a gap) | W-8 (D-5) |
| The journal is `ConditionalCheckFailed` | Optimistic lock | W-7 |
| Any item is `TransactionConflict` | Optimistic lock | D-6 |
| A cancellation that matches none of the above (`ProvisionedThroughputExceeded`, `ThrottlingError`, and so on), a communication failure, and others | Storage | Chapter 6.2 of `dynamodb.md` |

- When cancellation reasons are attached to multiple items, it is an optimistic lock (D-6) if any item has `TransactionConflict`. If none does, look in the order of the head's `ConditionalCheckFailed` (to distinguish W-3 and W-8), the journal's `ConditionalCheckFailed` (W-7), and others (storage), and classify by the first one that matches (`dynamodb.md` 6.2, P-43. Owner's decision, 2026-10-06). This is the same treatment as the statement in `conformance/README.md` that a `TransactionConflict` on any item is D-6. In the conformance data's cancellation reasons, every case has exactly one element that is not `None`, so this order is checked by the unit tests in `lib`.
- The `code` string of a cancellation reason and the extraction of `item` use the public methods of `CancellationReason`. The classification is not made from the SDK's error display string.
- In an update, it cannot happen that the condition fails although the old head equals `event.seq_nr − 1`. If it happens, the proposal is not to treat it silently as a success or an optimistic lock but to return `Storage` (an unexpected cancellation).
- The optimistic lock message contains only the aid string, the `seq_nr` being appended, and, if known, the head's `seq_nr` (E-2). It does not contain the SDK's raw error text.
- The contract violation message contains the rule `W-8` and `event.seq_nr` (E-3).

<a id="443-項目サイズの検査d-7"></a>

#### 4.4.3 Item size check (D-7)

- Before writing, estimate the size of the items to send. The journal item, the head item, and the snapshot items (current and history) are all covered. The payload is carried in both the journal and the head.
- The limit is 409600 bytes (400 KB). An estimate above it is returned as `ContractViolation` (`ContractRule::ItemSizeLimit`). It is not sent (H-1).
- The estimate is a single function that computes the total size of attribute names and values according to the item size counting method that AWS publishes. Exactness right at the boundary is not required (`conformance/README.md`).
- The manifest is included in the attributes counted. These are the event's manifest (the journal item and `events[0]` of the head item) and the snapshot's manifest (the current and history items). The manifest is a string the user decides and has no size limit.
- The estimate is at least the actual size (an upper bound). Numeric attributes are counted at the upper limit of the numeric size that AWS shows. For lists and maps, the size of the outer frame is added. Near the boundary, an item that would actually fit within the limit may be rejected.
- `dynamodb-item-size-head-overhead` is a case in which the journal is within the limit and the head exceeds it. For the head, the attribute names and values of `type_name` and `events` are also counted.
- Because the estimate is an upper bound, DynamoDB never returns an item-size validation error. If it does, return `Storage` (chapter 10 Q-4).

<a id="444-書き込みの後d-9s-4"></a>

#### 4.4.4 After the write (D-9, S-4)

- When the transaction succeeds, retention processing is done only if a history snapshot was written (D-9). It is not done for an event-only append or for a snapshot write without a history count.
- A retention failure does not change the result of the write (S-4). See section 4.6.

<a id="45-読み取りdy-9dy-10dy-11r-1r-8"></a>

### 4.5 Reads (DY-9, DY-10, DY-11, R-1 to R-8)

<a id="451-最新のスナップショット"></a>

#### 4.5.1 The latest snapshot

```text
1. 1 回の BatchGetItem で、head（aid）と snapshot（aid, skey = 0）を読む。両方 ConsistentRead = true
2. UnprocessedKeys があれば、読み切るまで再要求する（DY-9）
3. head がなければ None（R-1）。snapshot があっても None（dynamodb-no-head-with-snapshot）
4. head があれば、SnapshotRead { snapshot: 封筒または None, head_seq_nr } を返す（R-2・R-3）
```

- The read of the two items is not atomic (R-8). `TransactGetItems` is not used (P-25). If another write is settled between the two items, the snapshot's `seq_nr` may be above or below the head. Neither breaks consistency.
- DY-9 does not define a limit for re-requesting `UnprocessedKeys`. The same re-request policy as DY-8 (section 2.12.1) is used, and reaching the limit is `Storage` (chapter 10 Q-6).
- It is introduced in stages. PR 12 is a minimal read that has no re-request and returns `Storage` if `UnprocessedKeys` remain. PR 13 adds the re-request (section 7.2).
- A missing attribute or a type mismatch is returned as `Storage` (missing data). It is not silently ignored (chapter 4).
- A failure to restore a snapshot is `Serialization` (`deserialize-snapshot`).

<a id="452-イベント"></a>

#### 4.5.2 Events

```text
journal を Query する
  KeyConditionExpression: aid = :aid AND seq_nr >= :seq_nr
  ConsistentRead = true、昇順（ScanIndexForward = true）
LastEvaluatedKey が返る間は、ExclusiveStartKey に渡して読み続ける
読み切ってから、封筒の列として返す（DY-11・R-4・R-5・R-6）
```

- The old journal's GSI is not used. The journal itself is read strongly consistently (P-29). All events that were settled at the time the read started are included (R-5).
- The envelope is built from each item's `seq_nr`, `occurred_at`, `manifest`, and `payload`, and the original `aggregate_id` that the caller passed (a copy of the `&AID` of `load_events`; section 2.8). The stored `aid` is used only to confirm that it matches the requested aid string. A missing attribute is `Storage`. An item whose `aid` does not match the request is also returned as `Storage` (the same as the current check).
- The conformance case for reads over 1 MB is `dynamodb-events-over-one-megabyte`. It is read to the end with paging.

<a id="46-スナップショットの保持処理dynamodbmd-8-章d-3d-9p-18p-24s-1s-4"></a>

### 4.6 Snapshot retention processing (chapter 8 of `dynamodb.md`, D-3, D-9, P-18, P-24, S-1 to S-4)

Done only when the history count `n` is `Some(n)`, after the write that wrote a history is settled (D-9).

```text
1. 履歴用の GSI を Query する        aid = :aid、ScanIndexForward = false、KEYS_ONLY
   LastEvaluatedKey が返る間は読み続ける。印のない履歴の seq_nr の列を得る
2. 今書いた履歴の seq_nr を加える（すでに見えていれば重ねない）
3. 降順に並べ、先頭 n 件を残す。それより古いものを、取り除く対象にする（S-2）
4. 方式で分ける
   Delete: BatchWriteItem の DeleteRequest を 25 件ずつに分けて送る（P-18）
           UnprocessedItems は再送する
   Ttl:    対象を 1 件ずつ UpdateItem する
           SET #ttl = :expires REMOVE active_history_seq_nr
           条件 attribute_exists(active_history_seq_nr)
           #ttl は ExpressionAttributeNames の別名。:expires は、印を付ける時点のエポック秒 + ttl_grace_seconds
           条件不成立（ConditionalCheckFailed）は「印付け済み」として読み飛ばす
```

| Aspect | Design | Rule |
|:--|:--|:--|
| Selection | The pure function `select_expired_history` (section 3.4; shared with memory). The inputs are the "visible history", the "history just written", and `n` | S-2 |
| The approach of counting and selecting the excess | Not used. The GSI is eventually consistent and cannot count the history just written. If processing overlaps, it may remove even the newest `n` | P-24 |
| Marked history | It is not in the GSI, so it is not counted. It is also not selected as a target for the next marking. The expiry is not postponed | S-3 |
| Calculating the expiry | The marking time (the clock) + the grace. It is not computed from the event's `occurred_at` | S-3 |
| Splitting into 25 | Matches the limit of `BatchWriteItem`. Unprocessed items are resent. The resend follows the policy in section 2.12.1 (`unprocessed_retry_*`), and reaching the limit is a retention failure | P-18 |
| Failure | A failure in the three phases (query, delete, mark) is carried in `retention_failure` of `AppendReceipt`. The write returns success | S-4 |
| Leftovers | In the next retention processing, select again from the current GSI contents and clean up. What could not be seen because of a delay in reflection is left in excess and cleaned up in the next processing | S-4, P-24 |

- The current version counts the items, selects the excess, and returns a failure as a failure of the write (confirmed fact). IP 2.2 records that this is the cause of the bug that deletes new history.
- The clock is a trait `Clock` (it returns the current epoch seconds), and the default is real time. Tests replace it (section 5.5, the `test-hooks` feature).

<a id="47-sdk-の差し込みの仕組みの置き場所ip-6"></a>

### 4.7 Where to put the SDK injection mechanism (IP 6)

| Place | Content | Location |
|:--|:--|:--|
| Library itself | It has no injection mechanism. `EventStoreForDynamoDB::open` receives the `aws_sdk_dynamodb::Client` that the caller built. Therefore, the `Client`'s configuration is the only injection point | `lib/src/dynamodb/` |
| Observing requests | `Config::builder().interceptor(...)`. Uses `read_before_transmit` of the `Intercept` trait (right before sending, it reads the request's API name and body). Observation does not change the request | Conformance runner (section 5.5) |
| Replacing requests and responses | `Config::builder().http_client(...)`. It receives the HTTP request that the SDK sends and either forwards it to the real DynamoDB Local or returns a response it built (`replace-request`, `replace-response`) | Conformance runner |
| Disabling automatic retries | `Config::builder().retry_config(RetryConfig::disabled())`. A `sdk-error` failure is not erased by retries | Conformance runner |
| Failing only the retention requests | At the HTTP layer, identify the retention requests from the request content (`Query` reading the GSI, `BatchWriteItem`, `#ttl` of `UpdateItem`) and fail them | Conformance runner |

- `modify_before_transmit` of `Intercept` can stop a request by returning `Err`. However, a failure stopped this way does not become a typed service error (such as `TransactionCanceledException`). `TransactionCanceledException` and `CancellationReasons` are made by having the SDK interpret a response built at the HTTP layer. The reason is in section 5.6.
- The library itself has no observation mechanism other than `tracing` spans. The current version uses `tracing::Instrument`. This is carried over.

<a id="48-現行との差の対応"></a>

### 4.8 Correspondence with the differences from the current version

| Current (confirmed fact) | Next major | Rule |
|:--|:--|:--|
| Two tables, journal and snapshot | Three tables, journal, snapshot, and head | DY-16 |
| `pkey` (shard) and `skey` (string) | `aid` and `seq_nr`/`skey` (numeric) | DY-16, DY-17 |
| `KeyResolver` and the shard count | None | DY-16 |
| Conditional update on `version` | Conditional update on the head's `seq_nr` | H-2, W-8 |
| Turns all cancellations into an optimistic lock | Classifies by reason (D-5, D-6) | W-3, W-7, W-8 |
| No item size check | Estimate before writing (D-7) | D-7 |
| No configuration item | Configuration items on the 3 tables and comparison | DY-8 |
| Reads through the old journal's GSI | Reads the journal itself strongly consistently | DY-11 |
| GetItem of the current snapshot | BatchGetItem of the head and the current one | DY-9 |
| Retention counts and selects. A failure is a failure of the write | Selects with a sparse GSI. A failure goes through a separate path | D-3, D-9, P-24, S-4 |
| Writes 0 to `ttl` | Does not have the `ttl` attribute itself | D-3 |

<a id="5-適合テストデータの実行器"></a>

## 5. Runner for the conformance test data

Build a runner that reads the conformance test data (`conformance/`, version 1.0.0) and runs all cases on memory and DynamoDB (DynamoDB Local) (item 1 of IP 5). The requirements are in `conformance/README.md` (CR). The runner adds no rules.

Confirmed facts:

- The contents of the distribution are 30 value cases (aid 9, time 11, seq_nr 6, FNV-1a 4), 85 scenes (42 common, 43 DynamoDB), and 1 layout. The count per file was checked and matches the README's statement.
- The names of the `store` settings in a scene are only these four: `retention_count`, `retention_mode`, `ttl_grace_seconds`, and `retry_limit`.
- The enumeration of failure phases (`phase`) has 12 kinds (`conformance/schema/common.schema.json`; chapter 10 Q-1).
- In this repository's `tools/conformance/`, there is only `manifest.py`. There are no `validate.py`, no `data.py` (the generators' reference implementation `materialize`), and no `uv.lock`. Only `conformance/` and `manifest.py` are distributed (chapter 10 Q-11).
- `conformance/.gitattributes` (`* -text`) already exists. It is also listed in `conformance/manifest.json`.
- As the README states, the runner builds the expansion of generators itself (section 5.2).

<a id="51-置き場所"></a>

### 5.1 Location

Create a new workspace member `conformance-runner/` (crate name `event-store-adapter-conformance-rs`, `publish = false`). Decision (coordinator, 2026-10-06). Reason: it can be an executable that outputs a JSON report. It does not add heavy dependencies on AWS or Docker to the dev-dependencies of `lib`, which keeps `lib`'s dependencies light.

```text
conformance-runner/
  src/
    main.rs            引数（--backend memory|dynamodb、--data conformance、--report <path>、--require-all）
    data.rs            JSON の読み込み、manifest の照合、generators の展開
    number.rs          任意精度の整数（i128）と時刻の変換
    compare.rs         JSON の比較（payload・集約状態）
    runner.rs          場面の実行手順
    target_memory.rs   メモリの生成・フック
    target_dynamodb.rs DynamoDB の生成・テーブル作成・SDK 差し込み
    observe.rs         履歴・通知・要求・属性・配置の観測
    report.rs          報告
```

- The proposal is to use `event-store-adapter-rs` (features `dynamodb` and `test-hooks`), `event-store-adapter-test-utils-rs` (starting DynamoDB Local), `serde_json`, `sha2`, and `tracing-subscriber` as dependencies. `test-hooks` is added in PR 8 (section 3.5). The skeleton in PR 4 does not use `test-hooks`.
- Differences between storages are confined to `target_*`. The part that runs scenes (`runner.rs`) does not know the storage.
- The shared scenario of the current `lib/src/event_store_test_support.rs` (`exercise_user_account_flow`) has a part that the conformance data replaces and a part that remains as a domain example. How to handle it is decided in the switching PR of chapter 7.
- The runner is not made a `#[cfg(test)]` inside `lib`. This is because it uses hooks beyond `pub(crate)`, and because an executable that outputs a JSON report is needed separately from `cargo test`.

<a id="52-データの読み方"></a>

### 5.2 How to read the data

| Requirement (CR) | Design |
|:--|:--|
| UTF-8 JSON, with `format` and `version` at the top level | At read time, check `format` and `version` (`1.0.0`). If they differ, the read fails |
| Duplicate keys, NaN, and Infinity are forbidden | `serde_json` does not accept NaN or Infinity as JSON. For duplicate keys, write a custom `Deserialize` that counts the occurrences of each key, and fail the read if one is detected |
| Do not rewrite string values or attribute names | Use the strings read as they are. No normalization or case conversion |
| Arbitrary-precision integers | Read JSON numbers as `serde_json::Number` (`arbitrary_precision`) and convert them to `i128`. Do not decode a `seq_nr` such as `-1` or `2^53` directly into `u64`. Try the conversion to `u64` just before calling the operation. Only for cases that carry the mark `representation.signed_seq_nr = true`, if the conversion is impossible, report it as not applicable (reason: not representable). If the conversion fails in a case without the mark, it is an error in the data or in the runner, so it is `failed` |
| `epoch_nanoseconds` | Read the decimal string as `i128` and compute with integers only. Do not use floating point |
| The envelope's `occurred_at` | Convert a UTC ISO 8601 string with nine fractional digits into `DateTime<Utc>` |
| Policy for time precision | In a success with `precision_policy = native-time-type`, the expected value is the value obtained by converting the input to the standard time type (`DateTime<Utc>`). `expect.value` is the reference value before rounding. The runner reports the converted value and the actual value. `DateTime<Utc>` can represent nanoseconds, so the converted value equals the input |
| `representation.time_precision` | Run the `nanoseconds` cases. Do not run the `milliseconds` cases, because the type used can represent nanoseconds. Report them as "not applicable" (reason: a choice that does not match the precision of the standard time type) |
| `representation.signed_seq_nr = true` | `SeqNr = u64` cannot represent negative numbers. Do not run them; report them as "not applicable" (reason: not representable). Do not count them as successes. Only the cases with this mark are made not representable |
| generators | `target` (a JSON Pointer rooted at the case; decode `~0` and `~1`), `character` (one Unicode character), and `byte_length` (the total number of UTF-8 bytes). The targets are only empty strings inside fixtures, and each pointer appears once. If `byte_length` is not divisible by the UTF-8 width of the character, fail it as an error in the data. Check the schema on the JSON before expansion, and run the operations with the values after expansion. That the distribution has no schema-checking tool is stated at the beginning of this chapter (chapter 10 Q-11) |
| 400 KB and 1 MB | Treated as 409600 bytes and 1048576 bytes (D-7, DY-11) |
| Comparing payloads and aggregate states | Compare the JSON values serialized and restored with the default JSON serializer. Ignore key order and whitespace. Preserve the order of arrays, `null`, booleans, strings, and numeric values. Do not equate booleans and numbers. Do no Unicode normalization. Differences in the notation of `1` and `1.0` are not required |
| Verifying the distribution | The runner checks the version of `manifest.json`, the SHA-256 of all listed files, and the set of files, and includes the result in the report. CI separately checks `python3 tools/conformance/manifest.py verify` and the SHA-256 of `manifest.json` (it is in the current `lint` job). Do not regenerate the manifest to hide a difference |

As a proposal, the payload type is `serde_json::Value` and the aggregate state type is also `serde_json::Value`. It checks the round trip of the default JSON serializer.

<a id="521-値の表の操作"></a>

#### 5.2.1 Operations of the value tables

The operations `buildAid`, `validateOccurredAt`, `validateSeqNr`, and `fnv1a64` do not require functions of the same names in the public API. The runner maps them as follows.

| Operation | Mapping | Rule |
|:--|:--|:--|
| `buildAid` | The runner has a test type that implements `AggregateId`. `type_name()` and `value()` return the input type name and value, and `user_string` is held as a separate representation (`Display`). It calls `AidString::from_aggregate_id` and compares `as_str()`. It checks that a different `user_string` does not affect the result. The byte count is the total of the type name, the value, and the separator (`aid-multibyte-1025`) | T-1, T-11, T-12 |
| `validateSeqNr` (`context = value`) | Call `get_events_by_id_since_seq_nr(aid, seq_nr)` of the store of the storage being run (memory or DynamoDB). Check that a value in range succeeds and that `2^53` or more is a contract violation (T-9). 0 is valid (it returns all items) | T-9 |
| `validateSeqNr` (`context = event`) | Build an envelope and call `persist_event`. `seq_nr = 1` succeeds, and `0` is a contract violation (W-6). Use the construction of the envelope and the entrance check of the public API, and do not let the runner merely compute the same expression | T-9, W-6 |
| `validateOccurredAt` | With the type name `ConformanceTime` and an aggregate whose value is the case ID, `persist_event` numbers 1 to `event_seq_nr − 1` at the time `1970-01-01T00:00:00.123000000Z`, then write number `event_seq_nr` with the input's time (the payload is an empty object and the manifest is an empty string). For a success case, read back number 1 and compare. For an out-of-range case, compare the contract violation (T-13) at number 7 | T-3, T-13 |
| `fnv1a64` | DynamoDB and memory do not use the hash (ADR-0006). The first major version has no shared hash implementation. Report it as "not applicable" (reason: in the first major version there is no storage that uses the hash; owner's decision, 2026-10-06; acceptance criterion 1 of chapter 5 of the implementation plan). It counts as neither success nor failure. It runs in stage 5, when a storage that uses the hash is released. FNV-1a 64 is not implemented and computed inside the runner (that would not be a check of the library) | K-1 |

`validateSeqNr` and `validateOccurredAt` of the value tables depend on the storage (they use writes and read-backs), so they run on both memory and DynamoDB.

<a id="53-場面の実行手順とストアを場面ごとに分ける方法"></a>

### 5.3 Steps of running a scene, and how to separate stores per scene

Each scene runs on an independent store. Items of other cases are not reused (CR).

<a id="531-ストアの分離"></a>

#### 5.3.1 Separating stores

| Storage | How to separate |
|:--|:--|
| Memory | Create an empty storage with `MemoryStorage::new` for each scene |
| DynamoDB | One DynamoDB Local container is shared across the whole run. For each scene, the runner assigns the names of the 3 tables and the history GSI. The proposal is the names `cf-{run id}-{scene number}-{journal\|snapshot\|head}` and, for the GSI, `cf-{run id}-{scene number}-history`. The run id is generated randomly for each run. The 3 tables are created in the same region and with the same `Client` (DY-18). When the scene ends, the tables are deleted (a failure to do so does not change the scene's result) |

- The table names and the GSI name are decided by the runner as values passed to the library's settings. The library assumes no form of names.
- The tables are created per the expectations of `layout.json` (keys, GSI, Streams). The snapshot's TTL is enabled only for scenes with `retention_mode = ttl` (`enabled_when = retention-mode-ttl` in `layout.json`).
- After creation, wait for `ACTIVE` with `DescribeTable`.

<a id="532-手順"></a>

#### 5.3.2 Steps

Steps 1 to 7 of the README are run as follows.

| Step | Content | Design |
|:--|:--|:--|
| 1 | Whether `backends` includes the storage being run | If not, "not applicable" (reason: the storage is not a target). `requires = ["ttl"]` requires the TTL mode. Memory does not provide TTL (MEM-12), so the TTL scenes of v1 target only DynamoDB |
| 2 | Create the store with the `store` settings | `retention_count = null` → `RetentionSettings::current_only()`. `retention_mode` is not used (in DynamoDB, even if `ttl` is specified, the mode is ignored without an error. In memory, specifying `ttl` is a configuration error (MEM-12); section 2.10). When `retention_count` is an integer, `delete` and `ttl` of `retention_mode` → `RetentionMode`. `ttl_grace_seconds` → `grace_seconds`. `retry_limit` → `unprocessed_retry_limit` (the default 10 when absent). The common names are mapped to the implementation's settings API. For `retention_count = 0`, the type does not reject it; `0` is passed as it is and the library's configuration error is observed (S-1) |
| 3 | `seed.items` | Before creating the store, put them into the 3 tables with test permissions (a separate `Client` of the runner) |
| 4 | `initialization` | Check the result of creation. A case of a creation failure has no operation sequence. A failure before creation (`operation = 0`) is registered first |
| 5 | Expand `generators` and build the envelopes of `fixtures` | Just before each operation, build the referenced envelopes. A rule violation during the construction of an invalid input's envelope is also caught as a failure of that operation. The runner's own pre-checks do not substitute for the library's checks |
| 6 | `steps` | Run them in array order, without concurrency |
| 7 | `expect` and `observe` | For scenes with retention failures or delays, observe after the operation completes (section 5.4). Hooks do not change whether a write succeeds or fails |

- `expect` is one of `success`, `none`, `snapshot` (the pair of `head_seq_nr` and the envelope), `events` (the order is also compared), and `error`. The returned events are compared as whole envelopes (`aggregate_id`, `seq_nr`, `occurred_at`, `manifest`, `payload`). For a snapshot, the envelope and the head's number are compared separately.
- `error` maps the variants of `EventStoreError` to the five classifications in the data (`optimistic-lock`, `contract-violation`, `serialization`, `configuration`, `storage`). The classification is not guessed from the message string (E-1). `error.rule` is compared with the string form of `ContractRule`. `message.must_contain` and `must_not_contain` are checked as substrings of `to_string()`.
- No concurrent execution. The data has no scene that decides a concurrent winner (a note in `coverage.json`).

<a id="54-観測の対象と時機"></a>

### 5.4 What to observe and when

Retention processing is done inside the call to `persist_*` after the append is settled (memory inside the exclusive control, DynamoDB right after settlement). It does not run in the background. Therefore, at the time `persist_*` returns, the retention processing has completed or failed. The runner observes the history, notifications, and requests after the operation returns. The physical deletion by DynamoDB's expiry is not waited for and not checked (because of the clock of the year 2100; section 5.5).

<a id="55-フックの置き場所"></a>

### 5.5 Where to put the hooks

"The library" means the `test-hooks` feature of `lib` (disabled by default, `#[doc(hidden)]`), and "the runner" means `conformance-runner`.

| Hook (CR) | Memory | DynamoDB |
|:--|:--|:--|
| Deterministic execution of retention (`delete`, `ttl`) | Runs synchronously inside the append. No dedicated hook is needed | The same. It runs synchronously right after the append. Not needed |
| Internal history (`observe.history`) | The library's `history_view(aid)` (section 3.5). It returns the set of history without marks. Memory has no marked history | The runner reads the snapshot table by `aid` strongly consistently with a separate `Client`. An item that has `active_history_seq_nr` is `active`, and an item that has `ttl` is `marked` (a pair of `seq_nr` and the expiry). The current item (`skey` = 0) and the configuration item are not counted. The target is only that aggregate |
| Failure notifications (`observe.notifications`) | The runner registers a `tracing` `Layer` once as the global subscriber and collects the events of the target `event_store_adapter::retention`. The classification is `retention-failure`. The range in which none are missed is as explained after the table | The same |
| SDK requests (`observe.requests`) | None (there is no SDK) | The runner's `Intercept` (`read_before_transmit`) records the request just before sending (section 4.7) |
| Attributes (`observe.items`, `seed.items`) | None | The runner's separate `Client` reads with `GetItem` (`ConsistentRead = true`) and, by the steps in section 5.5.2, compares the attribute set, types, values, nesting, binary, and bindings. `seed.items` is built by the reverse of the same steps |
| Clock (`clock`) | Memory has no TTL. Not used | The library's `Clock` trait (`now_epoch_seconds`). `Clock` is used only for TTL marking. The runner passes a fixed clock. It is set with `clock.epoch_seconds` and advanced before an operation's `clock_epoch_seconds`. v1 is the year 2100 |
| Waits (exponential backoff) | Does not wait (there are no re-requests) | The library's `RetrySleeper` trait (section 2.12.1). It is separate from `Clock`. The runner records the requested waits and does not actually wait |
| Layout (`layout.json`) | None | The runner checks the actual 3 tables with `DescribeTable` and `DescribeTimeToLive`. What is checked is in section 5.5.3 |

- What is added to the library is only the memory hook points (section 3.5), `Clock`, and `RetrySleeper`. The observation of SDK requests and the injection of failures are done by the runner through the `Client` configuration (section 4.7). No SDK injection mechanism is added to the library.
- In observing failure notifications, if there are multiple logs of the same final failure, they may be normalized into one failure notification (CR). An empty array means there is no failure notification in that operation.
- The range in which failure notifications are not missed (including the case where the async runtime polls an operation's Future on another thread):
  - The `Layer` is registered once for the whole run as the global subscriber (`tracing::subscriber::set_global_default`). `with_default` replaces only that thread's subscriber, so notifications emitted on another thread would be missed. It is not used.
  - The runner wraps each operation's Future in a span that carries the case ID and the operation number (`tracing::Instrument`).
  - Whichever thread the Future is polled on, events of the target `event_store_adapter::retention` emitted inside that span are counted as that operation's notifications.
  - The range of observation is the notifications emitted during the poll of the operation's Future. This presupposes that the library does not create background tasks (section 2.11). If a task were created, its notifications would be outside the span and would be missed.
  - Notifications are sorted by the span's case ID and operation number. Notifications of another scene do not get mixed in.

<a id="551-sdk-要求の検査observerequests"></a>

#### 5.5.1 Checking SDK requests (`observe.requests`)

- Expressions (`update.set`, `update.remove`, `condition`, `key_condition`) are not compared by string match. They are converted, using the SDK request's `ExpressionAttributeNames` and `ExpressionAttributeValues`, into a structure in which the bindings of attribute names and values are resolved, and then compared. The order of whitespace, clauses, and `AND` is not compared.
- `key_condition.all` is checked with `eq`, `gte`, and the bindings to `aggregate_id` and `seq_nr`. For TTL, check the correspondence between `#ttl` and `expression_attribute_names`. `expires` is epoch seconds.
- The elements of `requests` are matched to the actual separate requests in array order. When multiple elements of the same API and phase are placed, it is not judged that one request satisfied multiple elements.
- Paging, re-requests of unprocessed keys, and the splitting of delete batches are checked against the whole request sequence of that phase. `initial_batch_sizes` is the number of delete batches excluding resends.
- `no_requests_in_phases`, `request_count`, and `minimum_request_count` are checked by the number of requests per phase. `classify-condition-failure-read` refers to an additional read made only for classifying a condition failure. The library does not do it (D-5).
- The phase of a request is determined from the API name, the table, the index, and the key (a configuration item key in `BatchGetItem` → `configuration-read`, a write to the journal in `TransactWriteItems` → `commit`, and so on).

The words for the conditions of request observation (`observe.requests[].constraints`) are the 24 words that `conformance/schema/common.schema.json` defines (`additionalProperties` is `false`; no other words can be written). The runner implements all 24 words. A word is implemented in the PR that gets the cases using that word to pass (the "PR that implements it" column of the table). In earlier PRs, the cases that use the word are `unverified`.

| Classification | Word | Meaning | Cases that use it (confirmed range) | PR that implements it |
|:--|:--|:--|:--|:--|
| Structured expression (the comparison method in the bullets above) | `update` | The expression of `UpdateItem`. `set` (an attribute and `value_binding`) and `remove` (attributes to remove) | `dynamodb-retention-ttl-once` | PR 15 |
| Same as above | `condition` | A condition on the existence or absence of attributes (`attribute_not_exists`, `attribute_exists`) | `dynamodb-config-new`, `dynamodb-config-create-race` (`attribute_not_exists: aid`), `dynamodb-retention-ttl-once` (`attribute_exists: active_history_seq_nr`) | PR 11, PR 15 |
| Same as above | `key_condition` | The `KeyConditionExpression` of `Query`. `all` lists the conditions `eq` and `gte` | `dynamodb-events-over-one-megabyte` | PR 12 |
| Same as above | `expression_attribute_names` | The mapping of aliases that avoid reserved words (`#ttl` → `ttl`) | `dynamodb-retention-ttl-once` | PR 15 |
| Same as above | `expires` | The expiry to set when marking (epoch seconds). The value that the ttl's `value_binding = expires` refers to | `dynamodb-retention-ttl-once` (4102444860 and 4102445860) | PR 15 |
| Shape of read requests | `table` | The target table of `Query`. The value is `journal`. It is bound to the name assigned for that scene (section 5.3.1) | `dynamodb-events-over-one-megabyte` | PR 12 |
| Same as above | `index` | The target index of `Query`. The value is `configured-history-index`. It is bound to the history index name in the settings | `dynamodb-retention-delete-paginated-batch` | PR 14 |
| Same as above | `projection` | The projection of the history GSI. The value is `KEYS_ONLY`. The proposal is to compare the projection of the index that `index` points to by taking it from the result that section 5.5.3 checked with `DescribeTable`. It also checks that `Query` does not widen the projection with `ProjectionExpression`. The README does not define how to compare, so this is a design proposal | `dynamodb-retention-delete-paginated-batch` | PR 14 |
| Same as above | `consistent_read` | `Query` has `ConsistentRead = true` | `dynamodb-events-over-one-megabyte` | PR 12 |
| Same as above | `consistent_read_all_tables` | `ConsistentRead = true` on all tables of `BatchGetItem` | The 13 configuration-check cases (except `dynamodb-config-retry-exhausted`), `dynamodb-latest-unprocessed`, `dynamodb-snapshot-ahead-of-head` | PR 11, PR 13 |
| Same as above | `keys` | The keys that `BatchGetItem` reads. Strings of `table:aid:sort key`. The configuration items are `journal:__config__:0`, `snapshot:__config__:0`, and `head:__config__` | The 13 configuration-check cases (same as above) | PR 11 |
| Same as above | `scan_index_forward` | `ScanIndexForward` of `Query`. `true` for reading the journal and `false` for reading the history GSI | `dynamodb-events-over-one-megabyte` (`true`), `dynamodb-retention-delete-paginated-batch` (`false`) | PR 12, PR 14 |
| Same as above | `follow_last_evaluated_key` | While `LastEvaluatedKey` is returned, pass it to `ExclusiveStartKey` of the next request and keep reading | The two cases above | PR 12, PR 14 |
| Same as above | `head_and_current_snapshot` | One `BatchGetItem` has the two keys of the head and the current snapshot (DY-9) | `dynamodb-latest-unprocessed`, `dynamodb-snapshot-ahead-of-head` | PR 13 |
| Same as above | `only_unprocessed_keys` | The re-request contains only the keys of the preceding `UnprocessedKeys` | `dynamodb-config-unprocessed-keys`, `dynamodb-latest-unprocessed` | PR 11, PR 13 |
| Writes and creating the configuration | `put_tables` | `TransactWriteItems` has only `Put`s to the specified 3 tables | `dynamodb-config-new` | PR 11 |
| Same as above | `same_store_id` | The `store_id` of the 3 `Put`s is the same value | `dynamodb-config-new` | PR 11 |
| Same as above | `layout_version` | The `layout_version` of the 3 `Put`s is 1 | `dynamodb-config-new` | PR 11 |
| Same as above | `head_return_values_on_condition_check_failure` | The head's `Put` or `Update` has `ReturnValuesOnConditionCheckFailure = ALL_OLD` (the value is the string `ALL_OLD`) | `dynamodb-condition-equal`, `dynamodb-condition-older`, `dynamodb-condition-gap`, `dynamodb-condition-no-head` | PR 12 |
| Re-requests and retention | `exponential_backoff` | Before a re-request, the wait of the formula in section 2.12.1 was requested of `RetrySleeper` | `dynamodb-config-unprocessed-keys` | PR 11 |
| Same as above | `include_just_written_history` | The history just written is added to the retention candidates. It is verified through the subsequent targets of deletion and marking and `observe.history` | `dynamodb-retention-delete-paginated-batch` | PR 14 |
| Same as above | `retry_unprocessed_items` | Resent the `UnprocessedItems` of `BatchWriteItem` | `dynamodb-retention-delete-paginated-batch` | PR 14 |
| Same as above | `initial_batch_sizes` | The number of delete batches excluding the resend of unprocessed items. The value is `[25, 5]` | `dynamodb-retention-delete-paginated-batch` | PR 14 |
| Same as above | `target_seq_nrs` | The set of history seq_nrs that the `UpdateItem` of that phase targeted | `dynamodb-retention-ttl-once` | PR 15 |

If a request check contains a condition word that the runner has not implemented, that case is `unverified` and is not counted as a success. It does not ignore the word and judge success by the remaining conditions alone. Every word in the table above has its implementing PR decided.

<a id="552-項目の属性の検査observeitemsseeditems"></a>

#### 5.5.2 Checking the attributes of items (`observe.items`, `seed.items`)

The expectation has `table`, `attributes`, `values`, `nested_attributes`, `binary_json`, and `bindings` (the "Observation of retention, SDK requests, and attributes" in `conformance/README.md`). The runner compares in the following order. The subject of the comparison is the whole item that was actually read.

| Step | Procedure | Condition for failure |
|:--|:--|:--|
| 1 | Map `table` (`journal`, `snapshot`, `head`) to the actual table name assigned for that scene (section 5.3.1). Build the key from `values`: for the journal, `aid` and `seq_nr`; for the snapshot, `aid` and `skey`; for the head, `aid` only. Send `GetItem` (`ConsistentRead = true`) with a separate `Client` | The item does not exist |
| 2 | Compare the set of attribute names of the actual item with the set of keys of `attributes` as an exact match. Also compare the type descriptors (`S`, `N`, `B`, `L`, `M`) per attribute | Extra or missing attributes, a type difference. An attribute not in `attributes` (such as `ttl` on the current snapshot, `active_history_seq_nr` on marked history, or an attribute for a GSI on the configuration item) fails if it exists |
| 3 | For an `L` attribute, check the number of elements. The number of elements of `events` is 1 (H-3). Check that the element is an `M`, and compare its set of attribute names and types with `nested_attributes` (for example `events[0]`) as an exact match | A difference in count, extra or missing nested attributes, a type difference |
| 4 | Take out each path of `binary_json` from the actual item. A path is an attribute name (`payload`) or follows list indexes and map attribute names, like `events[0].payload`. Restore the `B` bytes as JSON and compare with the expected JSON value | The path is missing, it is not `B`, it cannot be restored as JSON, or the value differs. The appearance of the bytes (key order, whitespace) is not compared |
| 5 | Resolve `generated-store-id` of `bindings`. Within the same `observe.items` array, bind it to the actual value of the target attribute (`store_id`, `S`) of the item that appears first, and compare that subsequent items have the same value. No concrete value is expected | The values of the 3 items (the configuration items of the journal, snapshot, and head) disagree |
| 6 | Remove the paths and attributes compared in steps 4 and 5 from a copy of the item (`events[0].payload` is removed from the map of the list element). Compare the remaining values with `values`. `S` is a string match. `N` is compared after converting the decimal string to an integer. A value that cannot be converted to an integer does not match. The elements of `L` are compared attribute by attribute with the map at the same position in `values` | A difference in value. If an attribute not in `values` remains, it has already failed in step 2 |

- Steps 2 and 3 are done against the whole actual item, before the paths are removed in steps 4 to 6. If the attribute set or counts were compared against the copy after removal, a missing `payload` would be overlooked.
- The head's `values.events` does not write the binary of `payload` (README). `payload` is compared in step 4.
- The order is to keep failure reports stable. It stops at the first failed step and puts the step number and the attribute path in the report.
- `seed.items` builds items by the reverse of the same expectation. It writes the `S` and `N` of `values` with the type of `attributes`. It serializes the JSON of each path of `binary_json` into bytes and puts it, as `B`, at the position of the path (including inside the map of a list element). For an item that has `bindings`, the runner puts one generated identifier into all items with the same binding name. The built items are written with `PutItem` by the test-permission `Client` before the store is created (step 3 of section 5.3.2).
- Within the confirmed range, in the v1 data the only ones that have `bindings` are the `observe.items` of the 3 configuration items (`generated-store-id`). A scene that uses `bindings` in `seed.items` has not been confirmed.
- These steps are a design proposal. In PR 11, which implements the attribute observation, read the actual items from DynamoDB Local and confirm that it works for all cases.

<a id="553-配置の照合dynamodblayoutjson"></a>

#### 5.5.3 Checking the layout (`dynamodb/layout.json`)

As `conformance/README.md` says, check the `tables` and `items` of `layout.json` against the actual 3 tables that the runner prepared and the items that the library wrote. The concrete values of the table names and the GSI name are bound to the settings (section 5.3.1).

| Aspect | How to check | Condition for failure |
|:--|:--|:--|
| Key structure and types | Compare the `KeySchema` and `AttributeDefinitions` of `DescribeTable` with `partition_key` and `sort_key` (name and type). The head has no sort key | A difference in the key names, types, or presence |
| GSI | The snapshot has one GSI. Its name is bound to the setting (`snapshot_history_index_name`). The key is `(aid (S), active_history_seq_nr (N))`. The projection is `KEYS_ONLY`. The journal and the head have no GSI | A difference in count, keys, or projection |
| Items that do not appear in the GSI | Check that the current snapshot (`skey` = 0), marked history, and the configuration item do not appear in the GSI. Whether an item appears in a GSI is determined by the sparse index mechanism, so check by the item's attributes that these items do not have `active_history_seq_nr` (step 2 of section 5.5.2). Do not check by running a `Query` on the GSI. Reads from a GSI are eventually consistent, and the result is not the same every time | An item that should not appear has `active_history_seq_nr` |
| Streams | Only the head is enabled, with `NEW_IMAGE`. The journal and the snapshot are disabled. Check with `StreamSpecification` of `DescribeTable` | A difference in enabled/disabled or in `StreamViewType` |
| TTL | Check with `DescribeTimeToLive`. The snapshot is `ENABLED` (attribute `ttl`) only in scenes with `retention_mode = ttl`. It is disabled in other scenes. The journal and the head are always disabled (`enabled_when = never`) | A difference in status or attribute name |
| Matching `items` with `item-shapes` | Match each element of `items` in `layout.json` (8 kinds of items) against the items the library actually wrote, using the operation sequences of `item-shapes`. For each kind of item (journal, current snapshot, history without a mark, marked history, head), compare the set of attribute names and types of the actual item with `attributes` (and `nested_attributes`) as an exact match. Values are not compared (the `observe.items` of `item-shapes` checks the values) | Extra or missing attributes, a type difference |
| Configuration items | In `item-shapes`, the 3 kinds of configuration items (journal, snapshot, head) are items that the runner put in as seeds. To compare with what the library wrote, compare with the configuration items that the library wrote in `dynamodb-config-new` (chapter 10 Q-18) | Extra or missing attributes, a type difference |

- The layout scene is `unverified` until PR 15, in which `item-shapes` passes. Until then, only the creation of tables and the checking of keys, GSI, Streams, and TTL are done, from PR 11.
- The result of the layout scene is reported as one case, the case of `layout.json` (`dynamodb-layout-v1`). The rules are mapped to the case's `rules` (DY-2, DY-3, DY-12, DY-16, DY-17, D-1 to D-4, T-7, H-3, H-4).

<a id="56-障害の差し込み"></a>

### 5.6 Injecting faults

The `faults` in the data have `operation` (0 is creating the store, 1 or more is a 1-based operation number), `phase`, `kind`, `injection`, and `repeat`. If a registered fault does not fire, the scene fails. If it cannot be injected, it is "unverified" and is not counted as a success (CR). How to count whether it fired is defined in section 5.6.6.

There are 12 phases (chapter 10 Q-1). The combinations used in the v1 data were checked and tabulated.

| Phase | Kind and storage in v1 | Injection in DynamoDB | Injection in memory |
|:--|:--|:--|:--|
| `serialize-event` | `serialization-error`, memory and DynamoDB | The runner wraps `EventSerializer` and fails the specified call of `serialize` | The same |
| `serialize-snapshot` | Same as above | Wraps `SnapshotSerializer` | The same |
| `deserialize-event` | Same as above | Fails `deserialize`. It fires when restoring in a read | The same |
| `deserialize-snapshot` | Same as above | Same as above | The same |
| `commit` | `storage-error` (memory and DynamoDB), `sdk-error` (12 cancellation reasons of DynamoDB) | Stop the `TransactWriteItems` request at the HTTP layer and return a built response (`replace-request`). Nothing is settled. The cancellation reasons are in 5.6.2 | The library's `before_commit` hook. If it returns a failure, no change remains in the head, the journal, or the snapshot (MEM-7) |
| `read-events` | `storage-error`, memory and DynamoDB | Stop the journal's `Query` and return a failure | The `read_events` hook |
| `read-snapshot` | `storage-error` (memory and DynamoDB), `sdk-response` and `read-interleave` (DynamoDB) | Stop `BatchGetItem`. `sdk-response` is in 5.6.3. `read-interleave` is in 5.6.4 | The `read_snapshot` hook. `read-interleave` targets only DynamoDB in v1 |
| `retention-query` | `storage-error` (memory and DynamoDB), `sdk-response` (`history_pages`, memory and DynamoDB) | Stop the `Query` of the history GSI and return a failure. `history_pages` is in 5.6.3 | The `retention_visible_history` hook. In memory, map `history_pages` to the logical-history hook (README, chapter 10 Q-15). The page sequence is concatenated and returned as the "visible history" |
| `retention-delete` | `storage-error` (memory and DynamoDB), `sdk-error` and `sdk-response` (DynamoDB) | Stop `BatchWriteItem` and return a failure. `unprocessed_first_n` is in 5.6.3 | The `retention_delete` hook. `final-retention-failure` fails after selecting the candidates and before deleting |
| `retention-mark` | `sdk-error`, DynamoDB | Stop `UpdateItem` (`#ttl`) and return a failure | Not applicable. Memory does not provide TTL. That a TTL request is a configuration error is checked by the unit tests in `lib` (MEM-3, MEM-12) |
| `configuration-read` | `sdk-response`, DynamoDB | Replace the response of the configuration `BatchGetItem` (5.6.3) | Not applicable. The storage has no reading of settings (MEM-3). That the storage holds the settings immutably is checked by the unit tests in `lib` |
| `configuration-create` | `sdk-error`, DynamoDB | Stop the configuration `TransactWriteItems`, settle the items that another runner wrote first, and return a cancellation response (5.6.5) | Not applicable. That independently created storages do not get mixed is checked by the unit tests in `lib` (MEM-2) |

Among the phases that cannot be injected, those that the v1 data does not target are "the storage is not a target". The alternative way of checking is the unit tests of `lib` in the table above (MEM-2, MEM-3, and MEM-12 of memory). The atomicity of memory reads (R-8, MEM-8) has no scene in v1. Instead, the proposal is to add a unit test that repeats appends and reads from multiple threads and checks that `snapshot.seq_nr ≤ head_seq_nr` always holds. It is a check of an invariant that does not depend on who wins, and its result is deterministic.

DynamoDB is expected to be able to inject every phase used in v1. However, whether the injection at the HTTP layer actually works has not been confirmed this time. Confirm it with DynamoDB Local at implementation time. A phase that cannot be confirmed is reported as "unverified", and an alternative way of checking is redesigned and reviewed by the coordinator (item 1 of IP 5).

<a id="561-差し込みの層"></a>

#### 5.6.1 Injection layers

- DynamoDB requests are received by the `HttpClient` plugged in with `Config::builder().http_client(...)`. It either forwards them to the real DynamoDB Local or returns a built response. Observation is done with `Intercept` (section 4.7).
- `replace-request`: do not send the request to DynamoDB Local, and return the injected response. Nothing is settled. All write failures and retention failures in v1 are `replace-request`.
- `replace-response`: after sending the request to DynamoDB Local, replace the response handed to the SDK. The side effects of the request that was sent remain.
- `sdk-error`: disable the SDK's automatic retries (`RetryConfig::disabled()`) and build, from `details.code` and `cancellation_reasons`, a response that the SDK can interpret.
- `repeat`: `{"mode":"count","count":n}` refers to the first n applications of the injection to the target phase (the "number of applications to the target phase" in `conformance/README.md`). What one application refers to differs by the kind of injection (section 5.6.6). It is not necessarily one request. For `history_pages` in a response plan, one application continues until all the pages are returned. `{"mode":"until-operation-finishes"}` fails the target requests inside the specified operation, including retries. It is not carried over to the next operation. How firing is counted is in section 5.6.6.
- Faults of the same phase are consumed in array order. Faults of different phases are all registered.

<a id="562-取り消し理由cancellation_reasons"></a>

#### 5.6.2 Cancellation reasons (`cancellation_reasons`)

- Create one element for each item of the transaction. The target names of a write are `journal`, `head`, `current-snapshot`, and `history-snapshot` (only the actions that exist), and for creating the configuration, `configuration:journal`, `configuration:snapshot`, and `configuration:head`. The `code` of an item that did not fail is the string `None`.
- The runner checks the target names against the actual actions in the request (the table and the kind of operation) and rearranges the SDK's `CancellationReasons` into the actual action order. This order is not enforced as the library's request order.
- The head's `ConditionalCheckFailed` has `old_head_seq_nr`. `null` means no old item is returned. The runner puts the old item in the response as `Item` (D-5).
- The JSON shape of the response is copied from and matched to the actual cancellation response of DynamoDB Local (confirmed at implementation time).

<a id="563-応答計画sdk-response"></a>

#### 5.6.3 Response plans (`sdk-response`)

| Plan | Design |
|:--|:--|
| `responses` and `unprocessed_keys` (`BatchGetItem`) | Forward the request and rewrite the returned response's `Responses` and `UnprocessedKeys` as the plan says. Replace only the first specified response, and after that return the actual response for the unprocessed keys. Do not return keys that were not sent. `seed-config` and `stored-head` are the items stored in that scene. One application replaces one response. If `count` is n, the responses of the following n `BatchGetItem` calls are replaced one at a time (section 5.6.6) |
| `history_pages` (`Query` of the history GSI) | Return the sequence of history seq_nr pages as they are. Do not automatically add the history just written. Map each page's `LastEvaluatedKey` to the next request's `ExclusiveStartKey`. Use the stored history items of that scene. One application starts with the first `Query` and continues until the whole page sequence has been returned. The `Query` calls from the second page on, continued by `LastEvaluatedKey`, are a continuation of the same application. They are not counted as separate applications (section 5.6.6) |
| `omit_just_written_history` | The default is false. If true, check that the number just written is not in `history_pages`. Even if false, it is not added automatically. Adding to the candidates and removing duplicates is done by the library |
| `unprocessed_first_n` (`BatchWriteItem`) | Return the first n of the received delete requests as `UnprocessedItems` and actually process the rest. Forward the request without the n items to DynamoDB Local and add the n items to the response (`replace-request`). The library resends. One application is one request of the target `BatchWriteItem` (section 5.6.6) |

The physical results of deletions, TTL updates, and writes are actually applied and checked with `observe.history` and `observe.items`.

<a id="564-読み取りの重なりread-interleaver-8dy-9"></a>

#### 5.6.4 Overlapping reads (`read-interleave`, R-8, DY-9)

1. Just before sending `BatchGetItem`, the runner reads and captures the old head with a separate `Client`.
2. Settle the append of `interleaved_operation` through the store under test.
3. Send the original `BatchGetItem` to DynamoDB Local and obtain the current snapshot.
4. Replace the head in the response with the captured old head.

The pair that is returned is the old head and the new snapshot. The append is reflected in the physical head. A race in real time is not used. The library is not required to make separate requests for the head and the snapshot.

<a id="565-設定の作成の競合install_items"></a>

#### 5.6.5 Race when creating the configuration (`install_items`)

When a `configuration-create` fault has `install_items`, the runner stops the `TransactWriteItems`. At that time, it writes to DynamoDB Local the configuration items (`install_items`) that another runner wrote first, and settles them. Then it returns a cancellation response (such as `ConditionalCheckFailed` for `configuration:journal`). The library discards the response and re-reads the 3 items strongly consistently (DY-8).

<a id="566-発火の数え方"></a>

#### 5.6.6 How to count firing

Faults are counted in units of "applications". `count` is the number of applications to the target phase (`conformance/README.md`). What one application refers to differs by the kind of injection. It is not necessarily one request.

| Kind of injection | One application |
|:--|:--|
| Injections that return a failure (`storage-error`, `sdk-error`. `replace-request` in DynamoDB) | Replace one request of the target phase with a failure response. Automatic retries are disabled (section 5.6.1), so one call of the API is one request |
| `responses` and `unprocessed_keys` of `sdk-response` (`BatchGetItem`) | Replace the response of one request of the target phase as the plan says. The responses of later re-requests are actual responses. The next application starts from the next request |
| `history_pages` of `sdk-response` (`Query` of the history GSI) | From the start of the plan until all the pages have been returned. The multiple `Query` requests continued by `LastEvaluatedKey` are one application. It is counted as fired at the start of the application (the first `Query`). Whether the remaining pages are read to the end is checked by the observation of requests such as `follow_last_evaluated_key` (section 5.5.1) |
| `unprocessed_first_n` of `sdk-response` (`BatchWriteItem`) | One request of the target `BatchWriteItem` |
| `read-interleave` | One request of the target `BatchGetItem` (the whole of steps 1 to 4 of section 5.6.4 is one application) |
| Serialization faults (`serialization-error`) | One call of `serialize` or `deserialize` of the serializer the runner wrapped (when a read restores multiple events, there are multiple calls to restore) |
| Memory hooks | One call of a hook point (`before_commit`, `read_events`, `read_snapshot`, `retention_visible_history`, `retention_delete`; section 3.5). For `history_pages`, one call of `retention_visible_history`, which concatenates the page sequence and returns it, is one application |

| Case | How to count |
|:--|:--|
| Valid range | A fault whose `operation` is n applies only to the target phase inside operation n. Remove it when the operation ends. It is not carried over to the next operation |
| Operation 0 | A fault in creating the store. Register it before creating the store (step 4 of section 5.3.2) |
| `count` is n | Inside that operation, apply to the target phase the first n times. It counts as fired when all n applications have started. Example 1: `dynamodb-config-retry-exhausted` has `retry_limit` of 1 and `count` of 2. The responses of the two `BatchGetItem` calls, the first read and one re-request, are replaced one at a time. There are 2 applications, and both fire. Example 2: `dynamodb-retention-delete-paginated-batch` has `count` of 1 and `history_pages` of 2 pages. The application starts with the first `Query`, and the second page's `Query` also returns the plan's page as a continuation of the same application. There is 1 application, and it fires. If the second page were counted as a second request and put outside the range of the application, the actual response of DynamoDB Local would be returned and the candidates would not be 30 |
| `until-operation-finishes` | Apply to all requests of the target phase inside that operation, including retries. Replacing one request is one application. It counts as fired when it has been applied at least once. Remove it when the operation ends |
| Only partly consumed | If `count` is n and the number of applications by the end of the operation did not reach n, it counts as not fired and the scene is `failed`. The report includes the declared count and the count that could be applied |
| Never applied | The scene is `failed` (the registered fault did not fire) |
| Multiple faults of the same phase | Consumed in array order. Use up the count of the earlier fault before using the next. Faults of different phases are all registered |
| Cannot be injected | `unverified`. Not counted as a success |

- The v1 data has no memory fault with `count` of 2 or more. The only one with `count` of 2 or more is `dynamodb-config-retry-exhausted`, which targets only DynamoDB.
- In v1, only the retention deletion and marking of DynamoDB (`retention-delete`, `retention-mark`) use `until-operation-finishes`.

<a id="57-ci-での実行"></a>

### 5.7 Running in CI

The proposal is to add a `conformance` job to `ci.yml`.

```yaml
conformance:
  runs-on: ubuntu-latest
  strategy:
    matrix:
      backend: [memory, dynamodb]
  steps:
    - uses: actions/checkout@v7
    - uses: dtolnay/rust-toolchain@stable
    - run: python3 tools/conformance/manifest.py verify
    - run: cargo run -p event-store-adapter-conformance-rs --release -- --backend ${{ matrix.backend }} --data conformance --report conformance-report-${{ matrix.backend }}.json
    - uses: actions/upload-artifact@v4
      with: { name: conformance-${{ matrix.backend }}, path: conformance-report-${{ matrix.backend }}.json }
```

- The DynamoDB job uses Docker. The runner starts the container with a function in `test-utils` (chapter 6).
- The result outputs a tally per rule number to `GITHUB_STEP_SUMMARY`.
- Add this job to the `needs` of `CI Success` (chapter 7).
- In the intermediate PRs of chapter 7, cases of unimplemented storages and rules are reported as "unverified", and `--require-all` is not attached. "Unverified" and "not applicable" are not counted as successes. `--require-all` is attached in PR 16, after PR 15, when all cases can be run. The meaning of `--require-all` is defined in section 5.8.
- While `--require-all` is not attached, the runner makes the exit code a failure only for "failed" and for a failure of the manifest check. "Unverified" does not make the exit code a failure. However, it is always included in the report and not counted as a success.

<a id="58-報告の形"></a>

### 5.8 Shape of the report

The runner outputs one JSON per storage. The shape is a proposal.

```json
{
  "data": { "version": "1.0.0", "manifest": { "sha256": "61c26614…", "verification": "passed" } },
  "implementation": { "language": "rust", "crate": "event-store-adapter-rs", "version": "4.0.0-…", "revision": "<git の識別子>" },
  "backend": "dynamodb",
  "environment": { "dynamodb_local": "3.3.1", "image_digest": "sha256:ff89bd48…" },
  "cases": [
    { "id": "core-gap-event", "rules": ["W-4", "W-8", "E-1", "E-3", "H-1"], "status": "passed" },
    { "id": "core-seq-negative", "rules": ["T-9", "E-1", "E-3", "R-1"], "status": "not-applicable",
      "reason": { "kind": "unrepresentable", "detail": "SeqNr は u64。負数を表せない（signed_seq_nr）" } },
    { "id": "…", "rules": ["…"], "status": "failed", "failed_operation": 3, "expected": {}, "actual": {} }
  ],
  "rules": [ { "rule": "W-8", "passed": 0, "failed": 0, "not_applicable": 0, "unverified": 0 } ]
}
```

| Status | Meaning | Counted as success? |
|:--|:--|:--|
| `passed` | All operations as expected | Yes |
| `failed` | It differed from the expectation midway. The number of the failed operation, the expected value, and the actual value are included. Also here: a registered fault did not fire, the declared count was not reached (section 5.6.6), or the input could not be converted to the type in a case without the mark | No |
| `not-applicable` (not applicable) | With a reason. Only four reasons are accepted (the list below) | No (counted separately as not applicable) |
| `unverified` (unverified) | Could not be run. A fault could not be injected, there is a condition word the runner has not implemented (section 5.5.1), or a required case was skipped | No |

The reasons accepted for not applicable are the following four.

| Reason | Content |
|:--|:--|
| The storage is not a target | The case's `backends` does not include the storage being run |
| Difference in expressive power | Not representable (only cases with `representation.signed_seq_nr = true`; section 5.2). The choice of precision (cases with `representation.time_precision = milliseconds`) |
| The FNV-1a 64 decision | In the first major version there is no storage that uses the hash. The 4 `fnv1a64` cases of the value tables (K-1). The owner's decision (2026-10-06). They count as neither success nor failure. They run in stage 5 |
| Exclusion at the unit of rules in `coverage.json` | Rules listed in `exclusions`: `W-5` (deleted) and `R-7` (a recommendation to the caller). Cases of the optional capability (change feed) are not in v1 |

The meaning of `--require-all` is as follows.

| Specification | Condition for making the exit code a failure |
|:--|:--|
| Not attached | There is even one `failed`. The manifest check failed |
| Attached | In addition to the above, there is even one `unverified`. There is even one `not-applicable` without one of the four reasons above |

- In either case, not applicable is not counted as a success, and the reason is included in the report.
- Even with `--require-all` attached, the 4 FNV-1a 64 cases are not made a failure. This is because they are not applicable for a reason listed above.

- When one case has multiple rules, the result is mapped to all the rules. If any operation differed from the expectation midway, the whole case is not reported as a success based only on the operations that succeeded (IP 5).
- The tally per rule counts the status of that case. A rule with even one failure fails as a rule.
- "Not representable" is not a status but one of the reasons for not applicable. Only cases with the `signed_seq_nr` mark are made not representable (section 5.2).
- Not applicable is only the four reasons above. "The result of skipping a required case" and "the result of being unable to inject a fault" are not made not applicable (they are made `unverified`).

<a id="581-受け入れ条件との対応ip-5"></a>

#### 5.8.1 Correspondence with the acceptance criteria (IP 5)

| Acceptance criterion | The runner's response |
|:--|:--|
| 1. Pass all cases on DynamoDB (DynamoDB Local 3.3.1) and memory. The fault scenes are also run on both | Sections 5.3 to 5.6. Cases that are not targets of memory are "not applicable". Not applicable cases (expressive power and FNV-1a 64) are not counted as successes, and the reason is recorded. For storages where injection is impossible, the alternative way of checking in section 5.6 |
| 2. The shapes of items match `layout.json` and `item-shapes.json` | `observe.items` and the layout check (sections 5.5 and 5.5.3) |
| 3. `CI Success` on main succeeds | Section 5.7 |

<a id="6-試験環境"></a>

## 6. Test environment

### 6.1 DynamoDB Local 3.3.1

The environment for DynamoDB tests is unified to DynamoDB Local 3.3.1 (IP 4.1). The image is pinned by digest.

| Item | Value |
|:--|:--|
| Image | `amazon/dynamodb-local` |
| Version | 3.3.1 |
| digest | `sha256:ff89bd48ff32cd8d9be5fee8873b65b8854dc408f1afe881be6eb00247bc0dab` |
| Reference | `amazon/dynamodb-local@sha256:ff89bd48ff32cd8d9be5fee8873b65b8854dc408f1afe881be6eb00247bc0dab` |
| Startup arguments | `-jar DynamoDBLocal.jar -inMemory -sharedDb -disableTelemetry` |
| Port inside the container | 8000 |

- Source: the hub's comparison record (`tools/spikes/dynamodb-emulators/README.md` and `probe.py`). It was confirmed in the source of the record that the digests of the tags `latest` and `3.3.1` match, that the startup used the arguments above, and that the port inside the container is 8000. This time, no image was pulled and no container was started.
- Do not use `latest`. Pin by digest.

An example of starting it (for checking by hand).

```sh
docker run --rm -p 127.0.0.1:8000:8000 \
  amazon/dynamodb-local@sha256:ff89bd48ff32cd8d9be5fee8873b65b8854dc408f1afe881be6eb00247bc0dab \
  -jar DynamoDBLocal.jar -inMemory -sharedDb -disableTelemetry
```

- `-inMemory` does not persist. The data disappears when the container stops.
- `-sharedDb` uses one database regardless of the credentials or the region.
- `-disableTelemetry` stops sending usage data.

As a proposal, tests start it with a function in `test-utils` (`testcontainers`). Match the `GenericImage` name and tag to the reference above. Whether a digest can be passed in the tag will be confirmed at implementation time with `testcontainers 0.28.0` (the specification in `Cargo.toml`).

<a id="62-クライアントの設定"></a>

### 6.2 Client settings

- Specify the endpoint (`http://127.0.0.1:{published port}`) explicitly in the DynamoDB client.
- The first major version does not read Streams (section 1.4). If a DynamoDB Streams client is used in the future, the same endpoint is specified explicitly. The region of a DynamoDB Local Streams ARN is `ddblocal`. Do not infer the region from the ARN (IP 4.1).
- Specify fixed dummy values for the region and the credentials explicitly. Do not let them be searched for from environment variables or the AWS configuration file. The 3 tables are placed in the same region (DY-18).
- The current `create_client` in `test-utils/src/dynamodb.rs` explicitly specifies the region `us-west-1` and the credentials `x`/`x` (confirmed fact). This policy is carried over.

<a id="63-起動の待ち方"></a>

### 6.3 How to wait for startup

- The proposal is to confirm startup by waiting until `ListTables` succeeds, not by a string in the container's standard output. The current LocalStack waits for `Ready.` in the standard output (confirmed fact).
- After creating a table, wait for `ACTIVE` with `DescribeTable`.
- There is one container for the whole run of the runner. Use separate table names for each scene (section 5.3). The proposal is for the DynamoDB tests of `lib` to also share one container and use separate table names for each test. The current version starts one container per test.

<a id="64-現行の試験localstackからの移し方"></a>

### 6.4 How to move from the current tests (LocalStack)

The current state that was confirmed:

- `dynamodb_local()` in `test-utils/src/docker.rs` starts LocalStack `2.1.0`. The function name says DynamoDB Local, but what runs is LocalStack.
- `test-utils/src/dynamodb.rs` has functions that create the tables of the old layout (`pkey`, `skey`, and the old GSI).
- `lib/src/event_store_for_dynamodb_test.rs` connects to LocalStack and tests the store of the old layout.
- `bigtable_emulator()` in `test-utils/src/docker.rs` starts the Bigtable emulator.

The move is in the following four stages.

| Stage | PR of chapter 7 | Content |
|:--|:--|:--|
| 1 | PR 5 | Add to `test-utils` a function that starts DynamoDB Local and a function that creates the 3 tables (history GSI, Streams, and conditional TTL). Rename the LocalStack functions to match what they actually do (renaming the functions also fixes the calls from the old tests), and keep the old tests running |
| 2 | PR 11 to PR 15 | The new DynamoDB tests (the conformance runner and the unit tests of `lib`) use only DynamoDB Local |
| 3 | PR 20 | Delete the old API's tests, LocalStack's startup function, and the functions that create tables of the old layout. The Bigtable emulator function is deleted in PR 18, which removes Bigtable (chapter 9 D-2) |
| 4 | PR 21 | Fix the descriptions of the tests in AGENTS.md and the README |

- `TEST_TIME_FACTOR` (the multiplier of waiting time) of the old tests is removed with the old tests. The new tests use the waiting method of `DescribeTable`.
- The matter of LocalStack requiring a token (IP 4.1) is resolved by no longer using LocalStack.

<a id="7-main-への入れ方"></a>

## 7. How to bring it into main

<a id="71-進め方の前提"></a>

### 7.1 Premises of the approach

- Merge into main with a squash for each PR. Introduce changes under the new contract little by little (step 3 of IP 3). The coordinator does the merging (IP 9).
- Confirmed facts:
  - The version-bump workflow `lib-bump-version.yml` runs only when started by hand (`workflow_dispatch`). At startup the level `level` (auto, patch, minor, major) can be chosen. The scheduled run has been removed (a consequence of IP 3, [PR #244](https://github.com/j5ik2o/event-store-adapter-rs/pull/244)). Therefore, changes to main are never published automatically.
  - Publication is done by `lib-release.yml` running `cargo publish -p event-store-adapter-rs` on a push of a tag `v[0-9]+.[0-9]+.[0-9]+`. `lib-release.yml` does not check that the tag name matches the version in `Cargo.toml`.
  - `ci.yml` has a daily `schedule`, but this is a CI run, not a version bump.
- The final release of the next major version (4.0.0) is issued by hand with `level = major`, after the acceptance criteria of IP 5 are met and with the owner's approval (step 4 of IP 3). After issuing it, the scheduled run is restored (PR 23, section 7.5).
- One PR corresponds to one group of rules (IP 9). Changes to the same group of rules go into the same PR. Changes to a different group of rules go into a different PR.
- Number 22 in section 7.2 is not a PR. It is the final release (a manual start with `level = major`). The text calls it the "final release".

<a id="72-pr-の列"></a>

### 7.2 The sequence of PRs

"Newly passing cases" are the cases that should newly become `passed` in that PR. The distinction between memory and DynamoDB is written in each row. The counts were counted per file of `conformance/` (42 common, 43 DynamoDB, 1 layout, 30 values).

| Order | Scope of the PR | Rules and conditions covered | Newly passing cases |
|:--|:--|:--|:--|
| 1 | Protecting the version bump and calculating the version. Add protection for pre-releases to `lib-bump-version.yml`. Make `next-semver.py` support pre-releases and add new tests. Put `semver` into `release-control` in `ci.yml` and widen the `-p` specification | IP 3 | None |
| 2 | Conditions for publishing. Widen the tag condition of `lib-release.yml` to pre-releases, and check that the tag name matches the version in `Cargo.toml` | IP 3 | None |
| 3 | Version and README. Change the version of `lib/Cargo.toml` to `4.0.0-alpha.0` and add a notice to the README | IP 3 | None |
| 4 | Skeleton of the runner. Reading, the manifest check, expanding generators, comparison, and reporting; registering and counting faults (section 5.6.6); making unimplemented condition words `unverified`. It does not use `test-hooks` | CR | None. All cases are `unverified` except those whose reason for being not applicable is decided |
| 5 | Test environment. Starting DynamoDB Local and creating the 3 tables (`test-utils`). Rename the LocalStack functions to match what they actually do | IP 4.1 | None |
| 6 | The `conformance` job in CI (without `--require-all`). Add it to the `needs` of `CI Success` | Item 3 of IP 5 | None |
| 7 | Core | T-1 to T-13, W-6, W-9, E-1 to E-3, S-1, S-2 (the selection function) | The 9 `buildAid` cases of the value tables |
| 8 | Memory writes and reads. `test-hooks` (`before_commit`, `read_events`, `read_snapshot`, `history_view`). Unit tests (only one settled by concurrent appends, a gap in numbers, MEM-2, MEM-3 and MEM-12, MEM-6, R-8) | MEM-1 to MEM-9, MEM-12, MEM-13, H-1 to H-4, W-3, W-4, W-7, W-8, R-1 to R-6, R-8 | On memory: the common writes and reads (24 of the 29; 5 are not applicable because of expressive power), `core-retention-zero`, `core-retention-default-current-only`, the 4 serialization and restoration cases, `core-storage-commit-failure`, `core-storage-read-events`, `core-storage-read-snapshot`. `validateSeqNr` of the value tables (5 of 6) and `validateOccurredAt` (7 of 11) |
| 9 | Memory retention and notification. The `retention_visible_history` and `retention_delete` hooks. Unit tests (the history of another aggregate is not lost, MEM-10, MEM-11) | MEM-10, MEM-11, S-2, S-4 | On memory: `core-retention-delete-1`, `core-retention-delete-2`, `core-retention-failure-after-commit`, `core-retention-query-failure`. All of memory's cases are complete |
| 10 | Foundation for fault injection (the request layer). The HTTP layer (`http_client`), `Intercept`, disabling automatic retries, determining the phase, and building cancellation responses (section 5.6) | The "injection of faults and clocks" in CR | None. Checked by the runner's unit tests |
| 11 | DynamoDB layout and configuration check. `open`, the re-request policy, `RetrySleeper`, and building the item attributes. Runner: creating tables, checking the layout (keys, GSI, Streams, TTL), observing attributes, the response plan of `BatchGetItem`, and `install_items` | DY-2, DY-3, DY-8, DY-16 to DY-19, D-1 to D-4 of `dynamodb.md` | On DynamoDB: the 14 configuration-check cases (including `dynamodb-config-unprocessed-keys`, `dynamodb-config-retry-exhausted`, and `dynamodb-config-create-race`, which need fault injection), `core-retention-zero` |
| 12 | DynamoDB writes and a minimal read. The transaction, the classification of D-5, D-6, and the estimate of D-7. One `BatchGetItem` of DY-9 (`Storage` if `UnprocessedKeys` remain) and the journal's `Query` (DY-10, DY-11) | H-1 to H-4, W-3 to W-9, D-5 to D-7 of `dynamodb.md`, DY-10, DY-11, R-1 to R-6 | On DynamoDB: the 16 cases of `write-errors`, `dynamodb-events-over-one-megabyte`, `dynamodb-no-head-with-snapshot`. The common writes and reads same as in 8, `core-retention-default-current-only`, serialization and restoration, `core-storage-*`, and the value tables |
| 13 | Re-requests of DY-9 and `read-interleave` | DY-9, R-8 | On DynamoDB: `dynamodb-latest-unprocessed`, `dynamodb-snapshot-ahead-of-head` |
| 14 | Retention by the delete mode. `history_pages` and `unprocessed_first_n` | Steps 1 to 3 of chapter 8 of `dynamodb.md`, D-3, D-9, P-18, P-24, S-1, S-2, S-4 | On DynamoDB: the 5 delete-mode cases (`dynamodb-retention-delete-paginated-batch`, `dynamodb-retention-gsi-missing-new`, `dynamodb-retention-gsi-deduplicate`, `dynamodb-retention-event-only`, `dynamodb-retention-failure-delete`), the 4 common retention cases (same as in 9) |
| 15 | Retention by the TTL mode and `Clock` | S-3, DY-2, step 4 of chapter 8 of `dynamodb.md`, P-17 | On DynamoDB: `dynamodb-retention-ttl-once`, `dynamodb-retention-ttl-stale-marked`, `dynamodb-retention-failure-ttl`, `dynamodb-written-item-shapes`, and the layout (`dynamodb-layout-v1`). All of DynamoDB's cases are complete |
| 16 | Attach `--require-all` to the `conformance` job (section 5.8) | Items 1 and 3 of IP 5 | All cases (success, or not applicable with a reason) |
| 17 | Migration tool (C of chapter 9 D-1). Add `migration` to `feature-matrix` in CI | D-8 and chapter 11 of `dynamodb.md`, IP-D8 | None. Tests with DynamoDB Local (outside the conformance data) |
| 18 | Removing SQLite and Bigtable (A of chapter 9 D-2). Remove their entries from the CI matrix | IP-D3 | None |
| 19 | Move the example `examples/user-account` to the `next` API | Item 5 of IP 5 | None |
| 20 | Switchover and deleting the old API. Move `next` to the top level. Delete the old tests and the LocalStack functions. Re-point the example's paths | IP 4.2 | None |
| 21 | Preparing the documents | Items 5 and 6 of IP 5 | None |
| 22 | Final release (not a PR) | Step 4 of IP 3, IP 5 | None |
| 23 | Restore the `schedule` of `lib-bump-version.yml` (`cron: '0 0 * * *'`, removed in [PR #244](https://github.com/j5ik2o/event-store-adapter-rs/pull/244)) | Step 4 of IP 3 | None |

The reasons for the order are as follows.

- One PR per group of rules. Separate versions, publishing conditions, and pre-release protection (PR 1 to PR 3), the runner (PR 4 to PR 6), memory (PR 8 and PR 9), and DynamoDB (PR 10 to PR 15).
- `test-hooks` is added in PR 8. The skeleton in PR 4 does not use `test-hooks`. This is because the runner's skeleton is built before memory's implementation.
- The foundation for fault injection (PR 10) comes before DynamoDB's layout and configuration check (PR 11). This is because 3 of the 14 configuration-check cases (`dynamodb-config-unprocessed-keys`, `dynamodb-config-retry-exhausted`, `dynamodb-config-create-race`) need fault injection. The registration and counting of faults (section 5.6.6) is also used by memory's `core-serialize-*` and `core-storage-*`, so it goes into PR 4.
- All 16 cases of `write-errors` call a read at the end of the operation sequence. Therefore, they do not pass in a PR with writes only. The minimal read goes into PR 12, the same PR as the writes.
- `dynamodb-written-item-shapes` (`item-shapes`) is placed in PR 15. Its operations only call `persistEventAndSnapshot` twice, but it uses `retention_mode = ttl`, a clock fixed in the year 2100 (`clock.epoch_seconds = 4102444800`), the `history_pages` of the second `retention-query`, and the observation of the history's `ttl`. It does not pass without TTL-mode retention and `Clock`.
- `--require-all` in PR 16 is attached after all cases are available (after PR 15).

The details of each PR are as follows.

<a id="pr-1-版上げの保護と版の計算"></a>

#### PR 1: Protecting the version bump and calculating the version

- Add a protection step to `lib-bump-version.yml`. When the version in `lib/Cargo.toml` is a pre-release (the version contains `-`), fail if it is started with anything other than `level = major` (including `auto`). Reason: until the first pre-release tag is made, the latest tag stays `v3.0.6`. If it is started with `patch` or `auto` in the meantime, `next-semver.py` calculates `3.0.7`, and the version would drop from `4.0.0-alpha.N` to `3.0.7`. `level = major` is not stopped, because the final release is issued with `level = major`.
- Make `.github/next-semver.py` support pre-releases. Confirmed facts:
  - `next-semver.py` extracts only the three numbers of the version with the regular expression `.*v?(\d+\.\d+\.\d+)` and ignores parts such as `-alpha.1`.
  - `lib-bump-version.yml` passes the result of `git describe --abbrev=0 --tags` (the latest tag) to this script.
  - Therefore, when the latest tag is `v4.0.0-alpha.1`, the script extracts `4.0.0` (confirmed by reading the code. It was not run, because the Python module `semver` is not installed). With `level = major`, the final release would be `5.0.0` instead of `4.0.0`.
  - Rule (proposal): when the latest tag is a pre-release, `major` returns the core version of that pre-release (`4.0.0`). Other `level`s end with a failure. `major` when the latest tag is `v3.0.6` returns `4.0.0` as before.
- Add a new test file (for example `test_next_semver.py`) to `.github/` to fix the rule above. `release-control` in `ci.yml` runs only `python3 -m unittest discover -s .github -p 'test_semver_level.py'`. The test needs `pip install semver`, because `next-semver.py` uses `import semver`. Add `pip install semver` (the same as `lib-bump-version.yml`) to the `release-control` job and widen the `-p` specification. `release-control` is included in the `needs` of `CI Success`.
- No Rust source is changed.

<a id="pr-2-公開の条件"></a>

#### PR 2: Conditions for publishing

- Widen the tag condition of `lib-release.yml`. Confirmed fact: the condition `v[0-9]+.[0-9]+.[0-9]+` does not include a version such as `v4.0.0-alpha.1`. A pre-release tag (for example `v[0-9]+.[0-9]+.[0-9]+-*`) is also made a trigger.
- Before `cargo publish`, add a step that checks that the tag name matches the version in `lib/Cargo.toml`. If they do not match, fail without publishing. Reason: `lib-release.yml` publishes without checking that the tag name matches the version. In an operation where tags are made by hand, a tag made without bumping the version, or a mistyped tag, would publish an unintended version. A published version cannot be withdrawn.
- The same check also applies to the final release tag (`v4.0.0`).

<a id="pr-3-版と-readme"></a>

#### PR 3: Version and README

- Change `version` in `lib/Cargo.toml` to `4.0.0-alpha.0`. Publishing main's Snapshot as a pre-release is a requirement of IP 3. How publishing is started is by hand (chapter 9 D-6).
- Add a notice to the README: "main is developing 4.0.0. The stable version is 3.x".
- Because the pre-release protection (PR 1) is already in, starting `lib-bump-version.yml` with `patch` or `auto` after this PR will fail.

<a id="pr-4-実行器の骨格"></a>

#### PR 4: Skeleton of the runner

- The skeleton of `conformance-runner/` (a new workspace member). Reading the data, the manifest check, expanding generators, comparison, and reporting.
- Registering and counting faults (section 5.6.6), and the processing that makes unimplemented condition words `unverified` (section 5.5.1). These are the parts that do not depend on the storage. Determining the phase of a request depends on DynamoDB SDK requests, so it is built in PR 10.
- There is no storage yet, so all cases are reported as "unverified". `--require-all` is not attached.
- `test-hooks` is not used.

<a id="pr-5-試験環境"></a>

#### PR 5: Test environment

- `test-utils`: starting DynamoDB Local and creating the 3 tables (chapter 6). Rename the LocalStack functions to match what they actually do. Keep the old tests running.

<a id="pr-6-ci-の-conformance-ジョブ"></a>

#### PR 6: The `conformance` job in CI

- Add a `conformance` job to `ci.yml` and add it to the `needs` of `CI Success` (section 5.7).

<a id="pr-7-中核"></a>

#### PR 7: Core

- In the `next` module, place the types, traits, errors, serializers, the retention selection function, the shared entrance checks, and the storage trait of chapter 2.
- In the `#[cfg(test)]` unit tests of `lib`, check each check (T-9, T-11, T-12, T-13, W-6, W-9, S-1), the error messages (E-2, E-3), and the selection function (S-2).
- Implement the runner's `buildAid`.

<a id="pr-8-メモリの書き込みと読み取り"></a>

#### PR 8: Memory writes and reads

- In `next::memory`: `MemoryStorage`, `EventStoreForMemory`, writes, and reads (sections 3.2 and 3.3).
- `test-hooks` (`before_commit`, `read_events`, `read_snapshot`, `history_view`; section 3.5).
- The runner's memory target.
- Unit tests of `lib`:
  - MEM-4: From multiple threads, append the same `seq_nr` to the same aggregate. Only one is settled, and the others are `OptimisticLock`. The one that was settled is returned exactly once by a read. The conformance data has no concurrent scene, so this unit test is the only check.
  - W-8: A gap (a `seq_nr` larger than the head by 2 or more) is a contract violation (rule `W-8`). The same applies to a `seq_nr` of 2 or more for an aggregate with no head.
  - MEM-2: An independent storage and a storage shared by `clone`.
  - MEM-3, MEM-12: A history count of 0 and `Ttl` (whether or not a history count is given) are configuration errors.
  - MEM-6: Changes to the input and to fetched results do not propagate to the stored value. The handling when stores with different serializers share the same `MemoryStorage` (`Serialization` if restoration is impossible).
  - R-8: Repeat appends and reads from multiple threads, and `snapshot.seq_nr ≤ head_seq_nr` always holds.

<a id="pr-9-メモリの保持と通知"></a>

#### PR 9: Memory retention and notification

- The `retention_visible_history` and `retention_delete` hooks, and the retention processing and notification (sections 3.4 and 2.11).
- Unit tests of `lib`:
  - MEM-10: The history of another aggregate is not lost by retention processing.
  - MEM-10: Even an event-only append cleans up history left behind last time (chapter 10 Q-16).
  - MEM-11: The notification is emitted after the exclusive control is released. A retention failure does not change the result of the settled append.
- Runner: observation of failure notifications (section 5.5).

<a id="pr-10-障害の差し込みの基盤要求の層"></a>

#### PR 10: Foundation for fault injection (the request layer)

- In the runner, build the plugging in of `Config::builder().http_client(...)`, `Intercept` (`read_before_transmit`), `RetryConfig::disabled()`, determining the phase of a request, and building cancellation responses (sections 5.6.1 and 5.6.2).
- The library is not changed (section 4.7). The runner's unit tests check the phase determination and the shape of the responses. Confirming the behavior with DynamoDB Local is done in PR 11.

<a id="pr-11-dynamodb-の配置と設定照合"></a>

#### PR 11: DynamoDB layout and configuration check

- In `next::dynamodb`: `DynamoDbTables`, `DynamoDbOptions`, `open` (DY-8), the policy for unprocessed re-requests and `RetrySleeper` (section 2.12.1), and building the item attributes (section 4.2).
- The runner's DynamoDB target: creating tables, checking the layout (section 5.5.3; the part other than `item-shapes`), observing attributes (section 5.5.2), comparing the observation of requests (of the condition words in section 5.5.1, those that the configuration-check cases use; compared with the requests that PR 10's `Intercept` recorded), the response plan of `BatchGetItem`, and `install_items`.

<a id="pr-12-dynamodb-の書き込みと最小の読み取り"></a>

#### PR 12: DynamoDB writes and a minimal read

- The transaction, the classification of D-5, D-6, and the estimate of D-7 (section 4.4).
- A minimal read: one `BatchGetItem` for the latest snapshot (`Storage` if `UnprocessedKeys` remain) and the journal's `Query` (section 4.5). This is because the 16 cases of `write-errors` call a read at the end.
- Runner: with the request layer of PR 10, register `commit` faults (with cancellation reasons) and run the 16 cases of `write-errors`. The building of cancellation responses was made in PR 10.

<a id="pr-13-dy-9-の再要求と-read-interleave"></a>

#### PR 13: Re-requests of DY-9 and `read-interleave`

- Re-requesting `UnprocessedKeys` (the policy of section 2.12.1).
- Runner: `read-interleave` (section 5.6.4).

<a id="pr-14-削除方式の保持"></a>

#### PR 14: Retention by the delete mode

- The `Query` of the history GSI, `select_expired_history`, and the deletion and resending of `BatchWriteItem` (section 4.6).
- Runner: the response plans `history_pages` and `unprocessed_first_n` (section 5.6.3).

<a id="pr-15-ttl-方式の保持と-clock"></a>

#### PR 15: Retention by the TTL mode and `Clock`

- The marking `UpdateItem` and `Clock` (section 4.6).
- Runner: injecting the clock, and matching `items` in the layout check (section 5.5.3). With this PR, all of DynamoDB's cases are run rather than "unverified".

#### PR 16: `--require-all`

- Attach `--require-all` to the `conformance` job. The meaning is defined in section 5.8.

<a id="pr-17-移行ツール"></a>

#### PR 17: Migration tool

- The migration tool of chapter 8 (its form is C of chapter 9 D-1).

<a id="pr-18-sqlitebigtable-の除去"></a>

#### PR 18: Removing SQLite and Bigtable

- Delete the features `sqlite`, `sqlite-system`, and `bigtable`, the corresponding modules, re-exports, and tests, `examples/user-account-sqlite`, and the Bigtable emulator function of `test-utils` (chapter 9 D-2).
- Remove the `sqlite` and `bigtable` entries from `feature-matrix` and `clippy` in CI.

<a id="pr-19-利用例の移行"></a>

#### PR 19: Migrating the example

- Move `examples/user-account` to the `next` API.

<a id="pr-20-切り替えと旧-api-の削除"></a>

#### PR 20: Switchover and deleting the old API

- Move the contents of `next` to the top level of the crate (remove the `next` module). Delete the old API.
- What is deleted: `types.rs` (the old `EventStore` and old errors), the old `event_envelope.rs`, `key_resolver.rs`, the old `event_store_backend.rs`, the old `generic_event_store.rs`, the old memory and old DynamoDB, the old tests, the old version of `event_store_test_support.rs`, and the LocalStack startup function.
- Re-point the example's paths. Update CI (section 7.4).

<a id="pr-21-文書の整備"></a>

#### PR 21: Preparing the documents

- Align the README, README.ja.md, `docs/DATABASE_SCHEMA*.md`, and the migration guide (`docs/MIGRATION_GUIDE_v5*.md`) with the new contract (items 5 and 6 of IP 5).
- Fix the errors in `CHANGELOG.md` and `AGENTS.md` (IP 4.2).
- The documentation PR proceeds with the coordinator's review and an independent review (IP 11).

<a id="pr-23-定期実行を戻す"></a>

#### PR 23: Restoring the scheduled run

- Done after the final release (section 7.5).

<a id="73-旧-api-との共存と削除の時点"></a>

### 7.3 Coexistence with the old API and the point of deletion

- **Coexistence**: From PR 7 to PR 19, the old API stays at the top level of the crate and keeps working. The new code is placed in the `next` module (`#[doc(hidden)] pub mod next`). Decision (coordinator, 2026-10-06; reason: it keeps each PR small and lets the CI on main keep passing). The reason for coexistence is that the examples, the old tests, the old Bigtable, and the old SQLite that use the old API must compile until the switchover PR.
- **Deletion**: In PR 20, the old API and the old tests are deleted. The old storages (SQLite and Bigtable) are deleted in PR 18. After the switchover, no aliases for old names, no `#[deprecated]` bridges, and no compatibility path that converts version are kept. The only support for old data is the migration tool (PR 17) (chapter 8).
- Users of the old API stay on 3.x. If a fix to 3.x becomes necessary, a maintenance branch (`release/3.x`) is cut from the last tag (`v3.0.6`) (IP 3). The CI changes for that are made when they become necessary.

<a id="74-各-pr-で-main-の-ci-を通し続ける方法"></a>

### 7.4 How to keep the CI on main passing in every PR

For each job of the current CI that was confirmed (`.github/workflows/ci.yml`), the method is shown.

| Job | Content (confirmed fact) | Method in each PR |
|:--|:--|:--|
| `release-control` | Unit tests of the version-bump decision and `actionlint`. The Python module `semver` is not installed | In PR 1, add `pip install semver`, add the new test file for `next-semver.py`, and widen the `-p` specification (PR 1 of section 7.2). Since PR 2 changes `lib-release.yml`, confirm that `actionlint` passes |
| `lint` | The check of the conformance data's manifest (`manifest.py verify` and SHA-256), and nightly `cargo fmt -- --check` (`rustfmt.toml`) | Format new code with `rustfmt.toml`. Do not change `conformance/` (the SHA-256 of version 1.0.0 is not changed) |
| `test-lib` | `cargo test -p event-store-adapter-rs --all-features` (including the tests that use Docker) | The old tests keep running until PR 20. The new tests use DynamoDB Local. After PR 5, the new tests share a container |
| `feature-matrix` | The sets `--no-default-features`, `sqlite`, `sqlite-system`, `dynamodb`, `bigtable`, and `all-features`. It checks for leaks of cloud SDKs, `hashlink`, and hand-written `unsafe impl` | Between PR 7 and PR 17, the existing sets are not changed. The `test-hooks` sets (`--features test-hooks`, `--features dynamodb,test-hooks`) are added in PR 8, and the `migration` set is added in PR 17. In PR 18, the `sqlite` and `bigtable` sets are removed. The ban on hand-written `unsafe impl` applies to new code as well. Type markers take the form `PhantomData<fn() -> T>` (the current convention) and leave `Send` and `Sync` to automatic derivation |
| `clippy` | `cargo clippy --workspace --all-targets -- -D warnings` for each feature set | New code has no warnings. `conformance-runner` is a member of the workspace, so it is checked with the same sets (the note on feature unification is in the comments of `ci.yml`). In PR 18, the `sqlite` and `bigtable` sets are removed |
| `audit` | `cargo-deny check advisories licenses` | When a new dependency (`uuid`, `sha2`, and so on) is added, make it pass within the PR. `audit` is not included in the `needs` of `CI Success` (confirmed fact). Whether to include it is outside the scope of this document |
| `conformance` (new) | The conformance runner | Added in PR 6. Until PR 15, "unverified" is allowed. In PR 16, `--require-all` is attached |
| `ci-success` | Collects the results of `lint`, `test-lib`, `feature-matrix`, `clippy`, and `release-control` | In PR 6, add `conformance` to the `needs` and the decision |

- In one PR, the public shape of the old API is not broken (until PR 20). Therefore, the example can compile with the old API until PR 19. It is moved to the `next` API in PR 19, and the paths are re-pointed in PR 20.
- In each PR, confirm from the `conformance` job's report that the cases of the implemented group of rules run and succeed. The unconnected cases are "unverified", and a CI success is not proof of complete conformance. Before the final release, confirm the success of all cases and `CI Success` (IP 5).
- Renovate's dependency updates may collide with the rewriting PRs (IP 11). How to defer taking in major updates during stage 2 until after the rewrite is outside the scope of this document.

<a id="75-リリース"></a>

### 7.5 Release

Decision (coordinator, 2026-10-06; chapter 9 D-6): Publishing a pre-release is by hand. It is not published automatically (a consequence of IP 3).

<a id="751-開発版pre-releaseの出し方"></a>

#### 7.5.1 How to issue a pre-release

1. Merge a PR that bumps the version to `4.0.0-alpha.N` (N increases from 1). Rewrite `version` in `lib/Cargo.toml` with `cargo set-version -p event-store-adapter-rs 4.0.0-alpha.N` (cargo-edit). The PR's title is in the Conventional Commits form. `lib-bump-version.yml` is not used.
2. After merging, create an annotated tag `v4.0.0-alpha.N` by hand on that merge commit and push it.
3. The push of the tag starts `lib-release.yml`. `lib-release.yml` checks that the tag name matches the version in `lib/Cargo.toml` (PR 2). If they do not match, it does not publish. If they match, it publishes to crates.io with `cargo publish`.
4. The owner chooses when to publish. It is chosen for each working set (for example, after PR 9, PR 15, and PR 20).
5. A published version cannot be withdrawn. Only `yank` is possible, not withdrawal (general knowledge in section 0.3. It was not confirmed on a real machine this time). Cargo does not select a pre-release unless the version requirement names one explicitly. A published version does not automatically reach users of the 3 series.

Until the tag of the first pre-release (`v4.0.0-alpha.1`) is made, the latest tag stays `v3.0.6`. If `lib-bump-version.yml` is started with `patch` or `auto` in the meantime, it calculates `3.0.7` and the version drops. The pre-release protection (PR 1) makes this start fail.

<a id="752-正式版"></a>

#### 7.5.2 The final release

- After the acceptance criteria (IP 5) are met, with the owner's approval, start `lib-bump-version.yml` by hand with `level = major`. `next-semver.py` calculates the next version from the latest tag. Whether the latest tag is `v3.0.6` (no pre-release tag) or a pre-release tag (`v4.0.0-alpha.N`), after the fix in PR 1 it becomes `4.0.0`. It writes the version and makes the tag `v4.0.0`. The push of the tag starts `lib-release.yml`.
- Publishing a pre-release does not double as the approval for the final release `4.0.0`.

<a id="753-正式版の後に定期実行を戻すpr-23"></a>

#### 7.5.3 Restoring the scheduled run after the final release (PR 23)

As in step 4 of IP 3, the scheduled run is restored after the major is issued.

1. Restore `schedule` (`cron: '0 0 * * *'`) to the `on:` of `lib-bump-version.yml`. It is what [PR #244](https://github.com/j5ik2o/event-store-adapter-rs/pull/244) removed.
2. Confirmed fact: when `inputs.level` is empty (a start by the scheduled run), the current `lib-bump-version.yml` leaves the level value empty and skips all the version-bump steps. When restoring the scheduled run, fix it to treat an empty input as `auto`.
3. After it is restored, changes that enter main are published every day at 00:00 UTC with the version decided from Conventional Commits (the operation before step 1 of IP 3).
4. The time to restore is after the final release (`4.0.0`) is issued. Before the final release, the pre-release protection (PR 1) makes a start with `auto` fail.

<a id="8-移行の案内"></a>

## 8. Migration guidance

Split into two.

- Guidance for users of the current major version (3.x) on moving their code to 4.0 (8.1)
- The procedure for moving old data to the new layout, and the migration tool (8.2)

<a id="81-利用者向けの移行の案内"></a>

### 8.1 Migration guidance for users

In PR 21, create `docs/MIGRATION_GUIDE_v5.md` and `docs/MIGRATION_GUIDE_v5.ja.md` (currently `docs/MIGRATION_GUIDE_v3.md` and `.ja.md`). The structure is as follows.

| Section | Content |
|:--|:--|
| List of overall changes | Rewrite the correspondence table of section 2.13 in the users' words |
| 1. Dependencies and features | Raise the version to 4. `bigtable`, `sqlite`, and `sqlite-system` are not in 4.0. Users who use them stay on 3.x (IP-D3). DynamoDB is the feature `dynamodb` |
| 2. Aggregate ID | `Display` and serde implementations are no longer required. The type name cannot contain `-` (T-11). The library builds the aid from the type name and the value (T-1). An aid over 1024 bytes is a contract violation (T-12) |
| 3. Numbers and times | `seq_nr` is `u64`. The upper limit is 2^53 − 1 (T-9). `occurred_at` is in the range where the nanoseconds since the epoch fit in a signed 64-bit integer (T-13) |
| 4. Snapshots | `SnapshotEnvelope::new(state, seq_nr)`. `version` is abolished. A `manifest` can be added (T-10) |
| 5. Writes | Do not pass `expected_version`. `persist_event(event)` and `persist_event_and_snapshot(event, snapshot)`. The first event (`seq_nr = 1`) can also be written with `persist_event`. `snapshot.seq_nr() == event.seq_nr()` is required (W-9). The comparison is decided by whether the event's `seq_nr` is the head's `seq_nr + 1` (W-8) |
| 6. Reads | `get_latest_snapshot_by_id` returns `SnapshotRead`. Even without a snapshot, if there is a head, `Some` is returned (R-3). In that case, read all events from `seq_nr = 1`. Check whether the last `seq_nr` obtained by the replay has reached the `head_seq_nr` received (R-7, recommended). The number of the next event is the last `seq_nr + 1` |
| 7. Errors | The five variants of `EventStoreError`. Only `OptimisticLock` may be retried (after re-reading). `ContractViolation` is an error in the call (including the gap of W-8) |
| 8. Settings | `DynamoDbTables`, `DynamoDbOptions`, and `RetentionSettings`. `KeyResolver` and the shard count are abolished. `with_keep_snapshot_count(Some(0))` is a configuration error |
| 9. Retention processing | A retention failure is not returned as a failure of the write. It is reported by a `tracing` log. The append is settled, so the same event must not be resent. A resend becomes `OptimisticLock` as a duplicate of an already settled `seq_nr` (W-7, W-8). History left behind is selected again and cleaned up by the retention processing after the next append. For memory this is the next append (an event-only one is fine), and for DynamoDB it is the next append with a snapshot (S-4, MEM-10, D-9) |
| 10. Memory | Create a `MemoryStorage` and share it by cloning. `Clone` is not required of `A` and `P` |
| 11. Old data | The 3.x data of DynamoDB cannot be read in 4.0. Use the migration tool of 8.2 |

As an example of user code, the proposal is to show `find_by_id` of `examples/user-account` before and after.

```rust
// 4.0: スナップショットなしでも復元できる。ヘッドの番号は別に返る。
pub async fn find_by_id(&self, id: &UserAccountId) -> Result<Option<ReplayedUserAccount>, RepositoryError> {
  let read = match self.event_store.get_latest_snapshot_by_id(id).await.map_err(Self::to_repository_error)? {
    None => return Ok(None), // ヘッドがない = 集約が存在しない（R-1）
    Some(read) => read,
  };
  let head_seq_nr = read.head_seq_nr();
  let (snapshot, _) = read.into_parts();
  // スナップショットがなければ 1 から、あれば seq_nr + 1 から読む（R-3・R-4）
  let (from_seq_nr, snapshot_seq_nr) = match &snapshot {
    Some(snapshot) => (snapshot.seq_nr() + 1, snapshot.seq_nr()),
    None => (1, 0),
  };
  // スナップショットがヘッドに届いていれば、読むイベントはない。番号が上限（SEQ_NR_MAX）のスナップショットで
  // seq_nr + 1 を渡すと T-9 の範囲外になるので、読まない
  let events = if snapshot_seq_nr < head_seq_nr {
    self
      .event_store
      .get_events_by_id_since_seq_nr(id, from_seq_nr)
      .await
      .map_err(Self::to_repository_error)?
  } else {
    Vec::new()
  };
  let last_seq_nr = events.last().map(|event| event.seq_nr()).unwrap_or(snapshot_seq_nr);
  if last_seq_nr < head_seq_nr {
    // 結果整合の遅れで、ヘッドに届いていない。呼び出し側が読み直す（R-7）
    return Err(RepositoryError::StaleRead { last_seq_nr, head_seq_nr });
  }
  let state = UserAccount::replay_from(snapshot.map(SnapshotEnvelope::into_aggregate), events);
  Ok(Some(ReplayedUserAccount { state, seq_nr: last_seq_nr }))
}
```

The example above is a sketch showing the structure of the migration guidance. `RepositoryError::StaleRead`, `UserAccount::replay_from`, and `to_repository_error` are functions on the example's side and are not the library's API. The example in the actual guide is copied as it is from the example that was moved to the `next` API in PR 19 and had its paths re-pointed in PR 20, in a form that compiles.

<a id="82-旧データの扱いと移行ツールip-d8d-8"></a>

### 8.2 Handling old data and the migration tool (IP-D8, D-8)

<a id="821-範囲"></a>

#### 8.2.1 Scope

| Migration source | What is provided | Basis |
|:--|:--|:--|
| rs v3 DynamoDB (the default key layout) | The migration tool (PR 17) | IP-D8, chapter 11 of `dynamodb.md` |
| rs v3 DynamoDB that used its own `KeyResolver` | None. Out of scope | Chapter 11 of `dynamodb.md`. If, during the steps, an item whose old pkey or old skey is not in the default form is found, the migration stops (P-22) |
| rs v3 Bigtable and SQLite | None. 4.0 has no such storage | IP-D3. When the storage is re-released, a migration tool is attached (IP 7) |
| Old layouts of js, java, and go | Out of scope of this document | IP 7 |
| Memory | None. It does not persist, so there is no old data | MEM-1 |

- Normal reads do not read the old layout. No path (fallback) that transparently reads the old layout is built. The migration tool is the only support for old data.
- The approach of recomputing the old layout's `pkey` is not used. This is because there is no guarantee that the toolchain's `DefaultHasher` returns the same value as at write time (chapter 11 of `dynamodb.md`).

<a id="822-手順dynamodbmd-の-11-章"></a>

#### 8.2.2 Steps (chapter 11 of `dynamodb.md`)

Confirmed facts:

- Since v3.0.0, journal items have a `manifest` attribute (confirmed by the current `put_journal` and by the fact that the first tag containing the commit `081ba72`, which added `manifest`, is `v3.0.0`). The journal's attributes, other than the keys, have the same form as the new layout.
- A v3 journal item has `pkey`, `skey`, `aid` (the value of the user's `to_string()`), `seq_nr`, `payload`, `occurred_at`, and `manifest`.
- The `occurred_at` of the v3 journal is nanoseconds since the epoch (`format_occurred_at` since v3.0.0 writes it with `timestamp_nanos_opt()`). The unit is the same as the new layout, so it is copied without conversion. Only values whose nanoseconds fit in a signed 64-bit integer have been written, so they also fit in the range of T-13. Only the snapshot's `last_updated_at` is written in milliseconds.
- A v3 snapshot item has `pkey`, `skey`, `payload`, `aid`, `seq_nr`, `version`, `ttl` (0 is written when there is no expiry), and `last_updated_at`. It has no `manifest`. The trailing number of the current item's `skey` is 0, and for history it is the history's `seq_nr` (confirmed in `lib/src/event_store_for_dynamodb.rs`).

The steps follow the numbers of chapter 11 of `dynamodb.md`. The read and tally in step 3, the check in step 4, and the read in step 6 are done first (the check stage), and then the writes of steps 3, 5, and 6 are done (the write stage). Chapter 11 of the specification allows these two stages (P-45, owner's decision 2026-10-06; chapter 10 Q-12).

| Step | Responsible | Content |
|:--|:--|:--|
| 1 | Operator | Create the new 3 tables (journal, snapshot, head) and the history GSI. The library does not create tables. The tool `open`s the new store (DY-8) and writes the configuration items |
| 2 | Operator | Stop writes to the old tables. The tool cannot enforce this. If they are not stopped, writes made during the rewrite are missed |
| 3 | Tool | Read the old journal with a strongly consistent `Scan` (until `LastEvaluatedKey` is no longer returned). From each item's old `pkey`, remove the trailing shard number, and from the old `skey`, remove the trailing `seq_nr`, to extract the type name and the value. If the old pkey or old skey is not in the default form, stop (P-22). If there is a type name that contains `-` and is not in the mapping table, stop (P-23). A type name that contains `-` and is in the mapping table is replaced with the new type name. A type name that does not contain `-` is used as it is. **The items of the new journal have only `aid`, `seq_nr`, `occurred_at`, `manifest`, and `payload`. The old `aid` attribute (the user's stringification) is rewritten to the new aid (T-1) that the library builds from the type name and the value after replacement. `pkey` and `skey` are discarded.** An old item without a manifest gets an empty string (T-2. Coordinator's decision, 2026-10-06). In the check stage, tally per aggregate while reading and do not write. In the write stage, read the old journal again and write to the new journal |
| 4 | Tool | In the check stage, for each aggregate, check whether the `seq_nr` values line up from 1 to the maximum without gaps (whether the count equals the maximum). If any aggregate has gaps, output the list and stop (P-36). The user fixes the missing numbers and then migrates again. Additional checks are written after the table of steps |
| 5 | Tool | In the write stage, for each aggregate, create the head item with the maximum `seq_nr` tallied in the check stage. Do not re-read the new journal to find the maximum. Put in `events` the event of that `seq_nr`, read from the new journal with a strongly consistent `GetItem` (P-37). The old current item's `seq_nr` is the snapshot's position, so it is not used as the head's `seq_nr` |
| 6 | Tool | `Scan` the old snapshot in the same way as in step 3. Read it in the check stage and read it once more in the write stage, writing the current items and the history with the new keys (P-20). The `version` attribute is discarded. The old `aid` attribute is rewritten to the new aid, as in step 3. An item whose `ttl` is 0 gets no attribute, and history without a mark gets `active_history_seq_nr`. An item whose `ttl` is a positive value (marked history) keeps `ttl` and gets no `active_history_seq_nr` (coordinator's decision, 2026-10-06; it can be read from the sentence of step 6). `manifest` gets an empty string (T-10, P-21) |
| 7 | Operator | Resume writes with the 4.0 library. The old tables are discarded by the operator after checking. The tool does not rewrite or delete the old tables |

The conditions that stop the check stage are as follows. All of them are listed, and it stops without writing any data item to the new tables (the configuration items are written in step 1). Missing precision or type information in old data is not filled in by guesswork (IP 7).

| Condition | Rule and basis |
|:--|:--|
| An item whose old pkey or old skey is not in the default form | P-22 |
| A type name that contains `-` and is not in the mapping table | P-23 |
| An aggregate with missing numbers | W-8, P-36 |
| An aggregate whose maximum `seq_nr` exceeds 2^53 − 1 (the `seq_nr` of v3 is `usize`, and there was no check of the upper limit) | T-9 |
| An aggregate whose aid after replacing the type name exceeds 1024 bytes in UTF-8 | T-12 |
| An aggregate for which putting the event of the maximum `seq_nr` on the head makes the estimate (the upper bound of section 4.4.3) exceed 409600 bytes. If the old journal's payload is large, the head carries the payload and so becomes larger than the journal | D-7 |
| An orphan snapshot (an old snapshot item whose aggregate is not in the old journal) | Coordinator's decision (2026-10-06) |
| A snapshot with a `seq_nr` greater than the old journal's maximum `seq_nr` (both current items and history) | Same as above |

- A parallel `Scan` or an export taken at a point after writes were stopped may be used instead (DY-19). The proposal is that the tool first uses a simple `Scan`.
- The reason for two stages: when a missing number or an invalid key is found, it can stop before writing to the new tables.
- The second read of the write stage: `Scan` the old journal and the old snapshot once more. Writes were stopped in step 2, so both reads read the same content. All events are not held in memory. The size of memory is proportional to the number of aggregates, not to the total amount of events (for each aggregate it holds only the maximum `seq_nr`, the count, the number of bytes of the aid, and the head's estimate). The cost is that the read capacity of the old tables doubles.
- Protection against re-runs: at the start of the check stage, `Scan` the new 3 tables, and if there is even one item other than the configuration item (an `aid` of `__config__`), stop without starting the migration. A migration that stopped midway is redone by the operator after recreating the new 3 tables (coordinator's decision, 2026-10-06).
- The proposal is that the writes of the write stage be conditional `Put`s with `attribute_not_exists(aid)`. If the condition fails, list that item and stop. Reason: even if another write gets in after the check of the protection above, it does not overwrite.
- The tool's inputs are the 2 table names of the old journal and old snapshot, the 3 table names of the new journal, new snapshot, and new head and the history GSI name, a `Client` (credentials and endpoint), and the type-name mapping table. rs v3 has no head table. The new head is created in step 5 from the maximum `seq_nr` tallied in the check stage. The old GSI is not used (steps 3 and 6 `Scan` the table itself). The output is the counts of migrated aggregates, events, and snapshots, and the list of reasons for stopping.
- The IAM that the tool uses is `Scan` (and `DescribeTable`) on the 2 old tables, and `Scan` (protection against re-runs), `GetItem`, `PutItem`, and `BatchGetItem` on the 3 new tables. The specification does not define the tool's permissions. Confirm at implementation time.

<a id="823-ツールの形"></a>

#### 8.2.3 Form of the tool

Decision (owner, 2026-10-06; chapter 9 D-1 C): build the body of the migration as a library function, and also attach a thin executable that calls it.

- The body is a function of the `lib` feature `migration` (`lib/src/migration/`). It writes the items of the new layout with the same attributes and the same value forms as the library. This prevents the forms from drifting apart so that the library could no longer read the migrated data.
- The thin executable is placed in the workspace and set to `publish = false`. It only reads the arguments and calls the body's function. It can be run with `cargo run` after getting the repository.
- In any form, the steps above do not change.

Facts confirmed after the implementation (confirmed by reading `lib/Cargo.toml`, `lib/src/lib.rs`, `lib/src/migration/mod.rs`, `migration-cli/Cargo.toml`, and `migration-cli/src/main.rs`. The confirmation was done by reading the code, not by running a migration):

- Only with the feature `migration` (which also enables `dynamodb`) is the module `event_store_adapter_rs::migration` public. The following 5 public names can also be used from the crate root.

| Public name | Shape |
|:--|:--|
| `migrate_v3_dynamodb` | `async fn migrate_v3_dynamodb(client: &Client, legacy_tables: &LegacyDynamoDbTables, tables: &DynamoDbTables, type_mapping: &HashMap<String, String>) -> Result<MigrationReport, MigrationError>` |
| `LegacyDynamoDbTables` | `journal_table_name: String` and `snapshot_table_name: String`. The names of the 2 old tables |
| `MigrationReport` | `aggregates: u64`, `events: u64`, `snapshots: u64`, `reasons: Vec<MigrationRejection>`. Can be made JSON (`Serialize`, `Deserialize`) |
| `MigrationRejection` | `table: String`, `pkey: Option<String>`, `skey: Option<String>`, `aid: Option<String>`, `reason: String`. Can be made JSON |
| `MigrationError` | `report: MigrationReport`, `operation: String`, `source`. `report` holds the counts and reasons that could be confirmed up to the point of failure |

- Items stopped by the check, conflicts of conditional writes, and duplicate names between the 2 old tables and the 3 new tables are carried in the `reasons` of the `MigrationReport` returned as `Ok`. Failures such as communication are returned as `Err(MigrationError)`.
- The executable is the crate of the workspace member `migration-cli/`, with `publish = false`. The package name is `event-store-adapter-migration-rs`, and because no `[[bin]]` is specified, the executable name is also this name (the directory name is `migration-cli`). It is run with `cargo run -p event-store-adapter-migration-rs -- <arguments>`. How to use it is written in `docs/MIGRATION_GUIDE_v5.md`.
- The arguments: six are required, `--old-journal`, `--old-snapshot`, `--journal`, `--snapshot`, `--head`, and `--history-index`, and three are optional, `--type-mapping` (a file of a JSON object from old type names to new type names), `--endpoint-url`, and `--region`. `--help` (`-h`) prints the usage and ends successfully. Unsupported arguments, arguments without a value, and duplicate arguments are errors. Authentication uses the normal AWS configuration.
- It outputs one JSON of the `MigrationReport` to the standard output. If `reasons` is not empty, it ends with a non-zero exit code. When it fails with a `MigrationError`, it also outputs the JSON of the `MigrationError`'s `report` to the standard output, outputs the content of the error to the standard error, and ends with non-zero.

<a id="9-判断が要る点"></a>

## 9. Points that need a judgment

As in chapter 10 of the implementation plan, the judgments in this document were decided at the design stage and reviewed by the coordinator. Only those that needed a judgment were escalated to the owner. Each item gives the options, the advantages and disadvantages of each, the recommendation, the decider, the date, and the reason. This chapter has no undecided item (2026-10-06).

| Item | Title | Status |
|:--|:--|:--|
| D-1 | Form of the migration tool | Decided C (owner, 2026-10-06) |
| D-2 | How to remove SQLite and Bigtable from the first major version | Decided A (coordinator, 2026-10-06. In IP-D3, "remove" was already agreed. They remain in the old major version (3.x)) |
| D-6 | Number of the pre-release and how publishing is started | Decided A (coordinator, 2026-10-06) |

D-3 to D-5 and D-7 to D-10 were decided and moved into the body. The numbers are not reused. Where they were moved to and the decisions are as follows.

| Item | Title | Decision | Decider | Moved to |
|:--|:--|:--|:--|:--|
| D-3 | Handling of the change feed | Neither a function that assembles head transitions nor a helper for resynchronization (DY-15) is included | Owner (2026-10-06, common to all languages) | Section 1.4 |
| D-4 | The asynchronous boundary (`&self`, `async_trait`, `Send`) | `#[async_trait]` and `&self`. The Future is `Send` | Coordinator (2026-10-06) | Sections 2.1 and 2.8 |
| D-5 | The type of `SeqNr` | `pub type SeqNr = u64` | Coordinator (2026-10-06) | Section 2.4 |
| D-7 | How to coexist with the old API | Place it in `#[doc(hidden)] pub mod next` and move it to the top level in PR 20 | Coordinator (2026-10-06) | Sections 2.2 and 7.3 |
| D-8 | The path for reporting retention-processing failures | A `tracing` WARN is mandatory. No callback is added to the public API | Coordinator (2026-10-06) | Section 2.11 |
| D-9 | Where to put the conformance runner | The workspace member `conformance-runner/` | Coordinator (2026-10-06) | Section 5.1 |
| D-10 | How to express sharing of the memory storage | Make `MemoryStorage` a public type | Coordinator (2026-10-06) | Sections 2.12 and 3.1 |

<a id="d-1-移行ツールの形"></a>

### D-1 Form of the migration tool

**Background**: IP-D8 states that a tool for moving the old data of rs v3 DynamoDB to the new layout is attached to the new major version. The form is listed in chapter 10 of the implementation plan as a matter to be decided at the design stage. The tool needs to write the items of the new layout with the same attributes and the same value forms as the library (chapters 5 and 11 of `dynamodb.md`).

| Option | Content | Advantages | Disadvantages |
|:--|:--|:--|:--|
| A | An executable of the workspace (a separate crate) | Users can run it without writing Rust. The operational steps (such as confirming that writes are stopped) can be separated as command arguments. It does not increase the library's dependencies | To share the building of items, the library must make internal functions public. If not made public, it is a duplicate implementation and the forms may drift apart. To distribute it with `cargo install`, it must be published to crates.io as a separate crate, which requires changing `lib-release.yml` (it publishes only `-p event-store-adapter-rs`) |
| B | A library function (enclosed by a feature. For example the `migration` module) | The building of items can be shared with the library's internals. The forms do not drift. It is the same crate, so the versions match. The DynamoDB Local tests can be written in the same place | Users must write a small `main` that calls it. A feature used only once takes on semver responsibility as a public API |
| C | The function of B and a thin executable (inside the workspace. Not published) | In addition to the advantages of B, it can be run with `cargo run` after getting the repository | The maintenance of the executable increases. It is not installed with `cargo install` |

**Recommendation**: C. There are three reasons. (1) Sharing the code that writes the items of the new layout with the library is the top priority. If the forms drift, the library cannot read the migrated data. (2) Operators can run it without writing Rust. (3) The public API is confined to a feature and can be marked as "for migration only". If the responsibility of a public API is judged to be heavy, there is also a way to choose A, duplicate the building, and check the forms with `item-shapes` of the conformance data.

**Decision**: C. Decider: the owner. Date: 2026-10-06. Reason: as the "What was decided (2026-10-06)" of chapter 10 of the implementation plan says, build the body of the migration as a library function and also attach a thin executable that calls it. It follows the three reasons of the recommendation above. The details are in section 8.2.3.

<a id="d-2-sqlitebigtable-を最初のメジャーから外す方法"></a>

### D-2 How to remove SQLite and Bigtable from the first major version

**Background**: IP-D3 states that SQLite and Bigtable are removed from the first major version and re-released under the new contract in stage 5. It is not a discontinuation. Current state that was confirmed: the features are `sqlite`, `sqlite-system`, and `bigtable`. The implementations depend on the old `EventStore` and `StorageBackend` (comparison by `version`) and do not work as they are under the new contract. CI has entries for them in `feature-matrix` and `clippy`. There are `test-utils/src/bigtable.rs`, `examples/user-account-sqlite`, and `deny.toml` (checked with `all-features`).

| Option | Content | Advantages | Disadvantages |
|:--|:--|:--|:--|
| A | In PR 18, delete the features, modules, re-exports, tests, the example, and the CI entries from main. The history stays in git. Build them anew in stage 5 | main becomes simpler. No code of the old contract remains. The dependencies on `rusqlite` (the C source of the bundled SQLite), `tonic`, and `googleapis-*` disappear, and the target of `audit` shrinks. Adding a feature later is a non-breaking addition | Until stage 5, specifying `features = ["sqlite"]` on 4.x is an error. Users of SQLite and Bigtable must stay on 3.x |
| B | Split into a separate crate (such as `event-store-adapter-sqlite-rs`) | When adding in stage 5, the features of `lib` need not grow | They cannot be rewritten under the new contract now, so it would be a crate with no content or one that holds old code that does not work. Reserving the crate name, publishing, and changing the release workflow are needed. IP-D3 writes "re-release in a minor version of the same major version", but a separate crate is not a minor version of the same crate |
| C | Leave only the feature names, and when enabled, guide to "not provided" with `compile_error!` | The reason reaches users. In stage 5 it is enough to just bring back the features | It keeps declaring features that are not provided. The `compile_error!` code remains. How to handle the CI entries is needed |

**Recommendation**: A. The reason is that it is the most straightforward to IP-D3's "remove" and leaves no code of the old contract. That a name has disappeared is conveyed to users by a clear Cargo error. Adding a feature later can be done as a non-breaking addition (a minor version). Guidance for users of SQLite and Bigtable is written in the migration guide (chapter 8).

**Decision**: A. Decider: the coordinator. Date: 2026-10-06. Reason: "remove" was already agreed in IP-D3. As the "What was decided (2026-10-06)" of chapter 10 of the implementation plan says, rs's SQLite and Bigtable are deleted from the new major version. They remain in the old major version (3.x). No code of the old contract is left.

<a id="d-6-開発版pre-releaseの番号と公開の起動のしかた"></a>

### D-6 Number of the pre-release and how publishing is started

**Background**: Chapter 3 of the implementation plan (IP 3) states that changes under the new contract are squash-merged into main for each PR, and that "main's Snapshot is published with the next major version (such as java's `2.0.0-SNAPSHOT`)". Publication is a mandatory requirement. What this item decides is not whether to publish. It is the following three points.

- How to write the number of the pre-release.
- How publishing is started.
- When and how often to publish.

Confirmed facts:

- crates.io has no mechanism called Snapshot. The counterpart in Rust is a pre-release of the next major version (for example `4.0.0-alpha.N`).
- `lib-release.yml` runs `cargo publish` only on a push of a tag `v[0-9]+.[0-9]+.[0-9]+`. A pre-release tag such as `v4.0.0-alpha.1` does not match this condition. It also does not check that the tag name matches the version in `Cargo.toml`.
- `.github/next-semver.py` extracts only the three numbers of the version with a regular expression and ignores parts such as `-alpha.1`. `lib-bump-version.yml` passes the latest tag (`git describe --abbrev=0 --tags`) to this script. When the latest tag is `v4.0.0-alpha.1`, `level = major` returns `5.0.0` (confirmed by reading the code. It was not run). It must be fixed before the final release `4.0.0` is issued (PR 1 of section 7.2).
- Until the tag of the first pre-release, the latest tag is `v3.0.6`. If `lib-bump-version.yml` is started with `patch` or `auto`, it calculates `3.0.7`. The version drops from `4.0.0-alpha.N` to `3.0.7`.
- `lib-bump-version.yml` runs only when started by hand (`workflow_dispatch`). The choices of `level` are auto, patch, minor, and major, with no pre-release stage. The result of IP 3 (2026-10-05) states that "changes put into main are never published automatically".
- `release-control` in `ci.yml` runs only `test_semver_level.py` and does not install the Python module `semver`. There are no tests for `next-semver.py`.

General knowledge (not confirmed on a real machine this time; section 0.3):

- A version published to crates.io can be neither overwritten nor deleted. It can only be removed from the resolution of new dependencies with `yank`.
- Cargo does not select a pre-release unless the version requirement names one explicitly. For this reason, a published pre-release does not automatically reach users of the current `3` series.

main's Snapshot is published to crates.io as a pre-release. Publishing is started by hand. Changes to main are not published automatically (a consequence of IP 3). The options differ in when to publish.

| Option | Content | Advantages | Disadvantages |
|:--|:--|:--|:--|
| A | The owner chooses when to publish, for each working set. Make main's `version` `4.0.0-alpha.0` (PR 3). To publish, after merging a PR that bumps the version to `4.0.0-alpha.N`, push the tag `v4.0.0-alpha.N` by hand to start it. In PR 1 and PR 2, prepare the version-bump protection, `next-semver.py`, the tag condition of `lib-release.yml`, and the check that the tag matches the version. Examples of timing are after PR 9, PR 15, and PR 20 | It agrees with the result of IP 3 (changes put into main are never published automatically). Publishing can be chosen for each working set. Mistaken publication is unlikely | Each publication needs the owner's manual work. If the interval between publications grows, the published version falls behind main |
| B | Publishing is started by hand, the same as A. However, the timing of publication is fixed at an early stage, such as after PR 6. The version number is increased from `4.0.0-alpha.1` | In addition to the advantages of A, the publishing steps (the tag condition, the check that the tag matches the version, and reflection on crates.io) can be verified end to end while the content is still small | The early versions are almost empty. Publishing early shows users an empty version |

How to write the version number is a separate judgment. Examples of options are `4.0.0-alpha.N`, `4.0.0-rc.N`, and `4.0.0-dev.N`. SemVer orders pre-release identifiers by a sequence of numbers and letters. `alpha` sorts before `beta` and `rc`. Here `4.0.0-alpha.N` is written as the example.

**Recommendation**: A. The reason is that the result of IP 3 removed automatic publication, and a manual start matches the current operation. It satisfies the publication requirement. In PR 1, put in the version-bump protection and the pre-release support of `next-semver.py`, and in PR 2, widen the tag condition of `lib-release.yml` to pre-releases and check that the tag matches the version. A change that adds a pre-release level to `lib-bump-version.yml` can wait until it becomes necessary. The owner chooses when to publish, for each working set.

**Decision**: A. Decider: the coordinator. Date: 2026-10-06. Reason: as the result of chapter 3 of the implementation plan says, changes put into main are not published automatically. A manual start matches the current operation and also satisfies the publication requirement. The steps are in section 7.5, and the preparation PRs are PR 1 to PR 3 of section 7.2. The publication requirement (IP 3) is not a subject of the decision.

<a id="10-未解決の疑問"></a>

## 10. Open questions

This records points where the reading of the specification is unclear, and points that seem to conflict between the specification and the materials. This document does not interpret and fill in the specification. Each item gives the basis and how this design handled it provisionally. Section 10.1 is for those that received an answer. Section 10.2 is for those awaiting an answer. The provisional handling is to be reviewed once an answer is obtained.

<a id="101-回答を得た疑問"></a>

### 10.1 Questions that received an answer

The answers come from the coordinator's review (2026-10-06) and the specification decisions of the same day (P-42 to P-45).

| Number | Question | Answer | Where it is reflected |
|:--|:--|:--|:--|
| Q-1 | The instructions say there are 13 failure phases, but the README and the schema list 12 kinds. Is there a 13th? | There are 12 phases. The 13 in the instructions was the coordinator's mistake | The beginning of chapter 5, the table in section 5.6 |
| Q-2 | Does T-6 allow requiring serde's `Serialize` and `DeserializeOwned` when the default JSON serializer is used? Is it read as not allowing serde to be required when an arbitrary serializer is used? | As the provisional proposal says. Design with the latter reading. The trait's bound is only `Send + Sync + 'static`, and the serde bounds are attached only to the constructor functions that use the default JSON | Sections 2.7 and 2.12 |
| Q-3 | D-7 (item size excess) is classified as a contract violation, but E-3 states that the message includes "the number of the violated rule". D-7 is the number of a judgment, not a rule number. What does the message include? | As the provisional proposal says. Have `ContractRule::ItemSizeLimit` and make the display `D-7`. It is not made mandatory | Section 2.9 |
| Q-4 | If the item size estimate is smaller than the actual size and DynamoDB returns an item-size validation error, what is the classification? | Make the estimate at least the actual size (an upper bound). For this reason, an item-size validation error does not occur. If it occurs, return `Storage` | Section 4.4.3 |
| Q-6 | Does DY-9 define a limit and a way of waiting for re-requesting `UnprocessedKeys`? | As the provisional proposal says. Use the same re-request policy as DY-8 (exponential backoff, with a limit, and reaching the limit is a storage error). The same policy is used for resending the unprocessed items of the retention `BatchWriteItem` | Sections 2.12.1, 4.5.1, and 4.6 |
| Q-7 | MEM-3 states that a setting that requests a change feed is a configuration error. Does such a setting item exist in the memory API? | As the provisional proposal says. No setting item is created. Only a request for the TTL mode is made a configuration error (MEM-12) | Sections 2.10 and 3.1 |
| Q-8 | When retention failures are reported by log, does the specification define the shape of the log event (target and field names)? May they be fixed so that the runner can observe them? | As the provisional proposal says. Fix the `tracing` target `event_store_adapter::retention` and the field `category = "retention-failure"`. It is not a rule of the specification but an agreement of this implementation | Section 2.11 |
| Q-9 | How should the 4 cases of K-1 (FNV-1a 64) be reported in the first major version, which has no storage that uses the hash? Are they included in item 1 of IP 5, "pass all cases"? | The owner's decision (2026-10-06, common to all languages). The first major version has no storage that uses the hash, so report them as "not applicable" with a reason. They count as neither success nor failure. They run in stage 5, when a storage that uses the hash is released. It is also written in acceptance criterion 1 of chapter 5 of the implementation plan | Sections 1.4, 5.2.1, and 5.8 |
| Q-10 | Is item 1 of IP 5, "pass all cases", read as excluding the not-applicable cases due to expressive power (the `milliseconds` precision and `signed_seq_nr`)? | The not-applicable cases due to expressive power have their reasons recorded. They are not counted as successes | Sections 2.4, 5.2, and 5.8 |
| Q-11 | The verification tools that `conformance/README.md` asks to run are not in this repository. Is it enough for the scope of the distribution to be only `tools/conformance/manifest.py`? Is `.gitattributes` needed? | What is distributed is only `conformance/` and `manifest.py`. `conformance/.gitattributes` (`* -text`) already exists and is also listed in `conformance/manifest.json`. The generators' reference implementation `materialize` is not in the distribution, so, as the README says, the runner builds it itself | The beginning of chapter 5, section 5.2 |
| Q-13 | DY-18 presupposes that the 3 tables are in the same region. SP-1 states that a configuration that departs from the presupposition is rejected with a configuration error if it can be detected. Can the library detect a difference in the regions of the 3 tables? Should it? | As the provisional proposal says. A way to detect has not been confirmed, so detection is not implemented. That the 3 tables go through the same `Client` is guaranteed by the shape of the API | Section 4.2 |
| Q-14 | When the head's condition fails on an update, what is the classification if the old head's `seq_nr` equals `event.seq_nr − 1`? | As the provisional proposal says. As a state that should not occur, return a storage error (an unexpected cancellation). It is not silently treated as a success or an optimistic lock | Section 4.4.2 |
| Q-15 | The response plan (`history_pages`) of the common scene `retention-query` is said to be mapped to the "logical-history hook" in memory. Memory has no pages. May the page sequence be concatenated and regarded as the "visible history"? | As `conformance/README.md` says, map it to the logical-history hook. Concatenate the page sequence and return it as the "visible history". The order of concatenation is given no meaning | The table in section 5.6 |
| Q-16 | There is no conformance case that checks memory's MEM-10 (even an event-only append cleans up history left behind). Should it be added to the data, or done by tests on the implementation side? | As the provisional proposal says. The timing varies by storage. Memory's MEM-10 is checked by the unit tests of `lib`. Adding to the data is left to the hub's judgment | Sections 3.4 and 4.4.4, PR 9 of section 7.2 |
| Q-5 | What is the allowed range of the TTL-mode grace (`ttl_grace_seconds`)? Is 0 valid? Is there an upper limit? | 0 is valid (the minimum in the conformance test data's schema is 0. The marking time becomes the expiry as it is). Because it is `u64`, it cannot represent a negative value (a language that can represent a negative value makes it a configuration error as an "invalid setting value at creation" of chapter 4 of the common contract). No upper limit is defined. The orchestrator's answer (2026-10-06) | Section 2.10 |
| Q-12 | Chapter 11 of `dynamodb.md` writes to the new journal while tallying in step 3 and checks continuity in step 4. May it be split into two stages, "read the old tables to the end and do the check (the check stage), then read the old tables again and write to the new journal (the write stage)"? | It may be split into two stages. A statement was added to chapter 11 of `dynamodb.md` that the check of step 4 may be completed by reading the old journal to the end before writing (P-45, owner's decision 2026-10-06) | Section 8.2.2 |
| Q-17 | Does the specification have a priority order when there are multiple cancellation reasons (section 4.4.2)? | If any item has `TransactionConflict`, an optimistic lock. If not, in the order of the head, the journal's condition failure, and others (`dynamodb.md` 6.2, P-43, owner's decision 2026-10-06). The same treatment as the conformance data's description | Section 4.4.2 |
| Q-18 | For the layout check (section 5.5.3), two points need confirming. (a) Which items are the 3 kinds of configuration items in `items` of `layout.json` compared with? The configuration items of `item-shapes` are ones the runner put in as seeds. (b) May the fact that "the current one, marked history, and the configuration items do not appear in the GSI" be checked by the items' attributes, without running a `Query` on the GSI? | As the provisional proposal says. (a) Compare with the configuration items the library wrote in `dynamodb-config-new`. (b) Whether an item appears in a sparse GSI is determined by the presence of the key attribute `active_history_seq_nr`, so it may be checked by the items' attributes. The orchestrator's answer (2026-10-06) | Section 5.5.3 |

<a id="102-残る疑問"></a>

### 10.2 Remaining questions

There are no remaining questions. Q-5, Q-12, Q-17, and Q-18, which had been in section 10.2, were resolved by the specification decisions of 2026-10-06 (P-43 and P-45) and the orchestrator's answers, so they were moved to section 10.1.
