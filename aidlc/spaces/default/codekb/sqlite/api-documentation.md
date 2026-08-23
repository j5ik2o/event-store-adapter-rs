# API ドキュメント — event-store-adapter-rs

## 公開 API

### コアトレイト（`lib/src/types.rs`）

利用者が実装するドメイン側トレイト:

```rust
pub trait AggregateId: Display + Debug + Clone + Serialize + DeserializeOwned + Send + Sync + 'static {
  fn type_name(&self) -> String;  // 集約の種別名（KeyResolver のシャーディングに使用）
  fn value(&self) -> String;      // ID の文字列表現
}

pub trait Event: Debug + Clone + Serialize + DeserializeOwned + Send + Sync + 'static {
  type ID: Display;
  type AggregateID: AggregateId;
  fn id(&self) -> &Self::ID;
  fn aggregate_id(&self) -> &Self::AggregateID;
  fn seq_nr(&self) -> usize;
  fn occurred_at(&self) -> &DateTime<Utc>;
  fn is_created(&self) -> bool;   // 作成イベントか否か（書込み経路の分岐条件）
}

pub trait Aggregate: Debug + Clone + Serialize + DeserializeOwned + Send + Sync + 'static {
  type ID: AggregateId;
  fn id(&self) -> &Self::ID;
  fn seq_nr(&self) -> usize;
  fn version(&self) -> usize;
  fn set_version(&mut self, version: usize);
  fn last_updated_at(&self) -> &DateTime<Utc>;
}
```

ストア側の統一契約:

```rust
#[async_trait]
pub trait EventStore: Debug + Clone + Sync + Send + 'static {
  type EV: Event;
  type AG: Aggregate;
  type AID: AggregateId;

  async fn persist_event(&mut self, event: &Self::EV, version: usize)
    -> Result<(), EventStoreWriteError>;
  async fn persist_event_and_snapshot(&mut self, event: &Self::EV, aggregate: &Self::AG)
    -> Result<(), EventStoreWriteError>;
  async fn get_latest_snapshot_by_id(&self, aid: &Self::AID)
    -> Result<Option<Self::AG>, EventStoreReadError>;
  async fn get_events_by_id_since_seq_nr(&self, aid: &Self::AID, seq_nr: usize)
    -> Result<Vec<Self::EV>, EventStoreReadError>;
}
```

書込みメソッドが `&mut self` を取る点に注意（読出しは `&self`）。

### バックエンド具象型

| 型 | 生成 | ビルダーメソッド | 備考 |
|---|---|---|---|
| `EventStoreForDynamoDB<AID, A, E>` | `new(client, journal_table, journal_aid_index, snapshot_table, snapshot_aid_index, shard_count)` | `with_keep_snapshot_count` / `with_delete_ttl` / `with_key_resolver` / `with_event_serializer` / `with_snapshot_serializer` / `maintenance()` | 機能が最も完全。TransactWriteItems による原子的 CAS |
| `EventStoreForBigtable<AID, A, E>` | `new(...)` | 同上、ただし **`with_delete_ttl` なし** | `with_keep_snapshot_count` はサイレント無効（TD-08）。楽観ロックは read→write の2段階（TD-07） |
| `EventStoreForMemory<AID, A, E>` | `new()` のみ | なし | HashMap 直持ち。`persist_event` に作成イベントを渡すと `panic!`（TD-05） |

### 補助モジュール

- **`key_resolver`**（`pub mod`）: `trait KeyResolver { type ID; ... }` と `DefaultKeyResolver`。パーティションキーを `{type_name}-{hash % shard_count}` 形式で解決する。`Arc<dyn KeyResolver<ID = AID>>` で差替え可能。
- **`serializer`**（`pub mod`）: `trait EventSerializer<E>` / `trait SnapshotSerializer<A>` と JSON デフォルト実装。`Arc<dyn ...>` で差替え可能。

### エラー型（`lib/src/types.rs`）

```rust
pub enum EventStoreWriteError {
  SerializationError(Box<dyn StdError + Send + Sync>),
  OptimisticLockError(#[from] TransactionCanceledExceptionWrapper),  // AWS SDK 型をラップ
  IOError(#[from] Box<dyn StdError + Send + Sync>),
  OtherError(String),
}

pub enum EventStoreReadError { /* DeserializeError / IOError / OtherError */ }

pub struct TransactionCanceledExceptionWrapper(pub Option<TransactionCanceledException>);
```

`TransactionCanceledException` は `aws_sdk_dynamodb` の型であり、公開エラー契約に AWS SDK がリークしている（TD-01）。Memory / Bigtable も楽観ロック失敗時に `TransactionCanceledExceptionWrapper(None)` を返す。

## 内部 API（private）

### `StorageBackend`（`lib/src/event_store_backend.rs`）

バックエンド実装が満たすべき最小契約。5メソッドで、`on_event_persisted` のみデフォルト実装（no-op）を持つ:

```rust
#[async_trait]
pub trait StorageBackend<AID, A, E>: Send + Sync + Clone + Debug + 'static
where AID: AggregateId, A: Aggregate<ID = AID>, E: Event<AggregateID = AID> {
  async fn fetch_latest_snapshot(&self, aid: &AID)
    -> Result<Option<SnapshotEnvelope<A>>, EventStoreReadError>;
  async fn fetch_events_since(&self, aid: &AID, seq_nr: usize)
    -> Result<Vec<E>, EventStoreReadError>;
  async fn create_event_and_snapshot(&self, event: &E, aggregate: &A,
    maintenance: &SnapshotMaintenance) -> Result<(), EventStoreWriteError>;
  async fn update_event_and_snapshot(&self, event: &E, aggregate: Option<&A>,
    expected_version: usize, maintenance: &SnapshotMaintenance) -> Result<(), EventStoreWriteError>;
  async fn on_event_persisted(&self, _aid: &AID, _maintenance: &SnapshotMaintenance)
    -> Result<(), EventStoreWriteError> { Ok(()) }  // デフォルト no-op
}
```

### `GenericEventStore<AID, A, E, B>`（`lib/src/generic_event_store.rs`）

`StorageBackend` を `EventStore` に橋渡しする汎用実装。共通ポリシーを一箇所に集約する:

- `persist_event`: `event.is_created()` なら `Err(OtherError)`（作成イベント拒否）。それ以外は `update_event_and_snapshot(event, None, version, maintenance)` → `on_event_persisted`。
- `persist_event_and_snapshot`: `is_created()` なら `create_event_and_snapshot`、既存集約なら `update_event_and_snapshot(event, Some(aggregate), aggregate.version(), maintenance)` → いずれも `on_event_persisted`。
- 読出し2メソッドはバックエンドへの単純委譲（`SnapshotEnvelope` から `aggregate` を取り出す）。
- ビルダー: `with_keep_snapshot_count` / `with_delete_ttl`、アクセサ: `backend_mut()` / `maintenance()`。

### 付随型

- `SnapshotEnvelope<A>` — `{ aggregate: A, seq_nr: usize, version: usize }`。スナップショット行のメタデータ付き封筒。
- `SnapshotMaintenance` — `{ keep_snapshot_count: Option<usize>, delete_ttl: Option<Duration> }`。**到達不能 pub**: 公開ファサードの `maintenance()` の戻り値型なのに、モジュールが private のため利用者が型名を書けない（TD-04）。

## API 契約と不変条件

1. `persist_event` は更新イベント専用。作成イベントは `GenericEventStore` 系では `Err(OtherError)`、Memory では `panic!` — **バックエンド間で契約が非対称**（TD-05）。
2. 楽観ロック: 書込みは呼び出し時点の `version` を条件とする CAS。失敗は `OptimisticLockError`。呼び出し側はリトライ（再読込 → 再適用）を実装する。
3. 読出しプロトコル: `get_latest_snapshot_by_id` → `get_events_by_id_since_seq_nr(aid, snapshot.seq_nr + 1)` → 集約にリプレイ適用、が想定シーケンス（`examples/user-account` のリポジトリが範例）。
4. スナップショット保持: `keep_snapshot_count` / `delete_ttl` は `on_event_persisted` フックで整理される（DynamoDB のみ実装）。

## 既知の API 課題

- AWS SDK 型の公開エラー契約へのリーク（TD-01） — SQLite / feature 分割前の再設計候補。
- `SnapshotMaintenance` の到達不能 pub（TD-04）。
- `StorageBackend` / `GenericEventStore` が private のため、クレート外部からのバックエンド追加は不可能（クレート内追加は5メソッド実装+ファサード公開で低コスト）。
- README のコード例が現行 API と乖離（TD-10）。
