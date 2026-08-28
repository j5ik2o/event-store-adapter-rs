# 移行ガイド: v2 → v3（EventEnvelope API）

v3 は `Event` / `Aggregate` トレイト契約を封筒ベースの API に置き換えます。
ドメインイベントと集約状態はプレーンな serde 型（payload）になり、ストアが必要と
するメタデータは 2 つの新しい公開型が運搬します。

- [`EventEnvelope<AID, P>`] — journal 1 行に対応: `aggregate_id` / `seq_nr` /
  `occurred_at` / `manifest` + イベント payload。
- [`SnapshotEnvelope<A>`] — スナップショット 1 件に対応: 集約 payload + `seq_nr`
  （リプレイ開始位置）と `version`（楽観的ロックの版数）。

これは破壊的リリースです。**v3 は v2 で書き込まれた行を読み取りません** — 後述の
[データ互換性](#データ互換性)を参照してください。

## 破壊的変更の一覧

| 領域 | v2 | v3 |
|:-----|:---|:---|
| ドメイントレイト | `E: Event`、`A: Aggregate`（`id()`, `seq_nr()`, `version()`, `set_version()` など） | 廃止。payload に必要なのは `Serialize + DeserializeOwned + Send + Sync + 'static` のみ（`AggregateId` は存続） |
| 書き込み API | `persist_event(&event, version)`、`persist_event_and_snapshot(&event, &aggregate)` | `persist_event(EventEnvelope, expected_version)`、`persist_event_and_snapshot(EventEnvelope, aggregate, expected_version)` — 封筒・payload とも値渡し |
| 読み取り API | `get_latest_snapshot_by_id -> Option<A>`、`get_events_by_id_since_seq_nr -> Vec<E>` | `-> Option<SnapshotEnvelope<A>>`、`-> Vec<EventEnvelope<AID, P>>` — メタデータが境界を越えて保持される |
| version の管理 | ストアが `set_version` で集約へ書き戻し | スナップショットの列／セルが正。`SnapshotEnvelope::version()` から読む |
| 直列化 payload | ライブラリが保存 JSON へ `seq_nr` / `version` を注入 | payload 列は純粋なドメイン内容のみ。メタデータは専用列に置かれる |
| シリアライザ | トレイト境界付き型に対する `EventSerializer<E>` / `SnapshotSerializer<A>` | payload 専用の `EventSerializer<P>` / `SnapshotSerializer<A>`。`deserialize` は payload を直接返す |
| `with_keep_snapshot_count` | `Self`（検証なし） | `Result<Self, EventStoreWriteError>`。**`Some(0)` は拒否**（無効化は `None`） |
| エラー | `OptimisticLockError`、`SerializationError`、`IOError`、`OtherError` | + **`ContractViolation`**（書き込み契約に違反する呼び出し。後述） |
| 不在集約への更新 | バックエンド依存 | 一律 `OptimisticLockError`（`actual_version` なしの書式） |
| Bigtable の CAS | 非トランザクションの read→check→write | 単一行原子性の `CheckAndMutateRow` |
| MSRV | 未宣言 | `rust-version = "1.94.1"`（`--all-features` で実測） |

Cargo feature の構成は変更ありません: `default = []`、バックエンドは `dynamodb` /
`bigtable` / `sqlite` / `sqlite-system` のオプトイン、インメモリバックエンドは常時
コンパイルされます。

## 1. ドメイン型からメタデータを剥がす

`Event` / `Aggregate` の実装と、それらを満たすためだけに存在していたフィールド
（`seq_nr`、`version`、`last_updated_at`、ドメインデータとして不要ならイベント ID や
集約 ID の複製も）を削除します。

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
// v3 — ライブラリトレイトなしのプレーンな serde 型
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UserAccountEvent {
  Created { name: String },
  Renamed { name: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserAccount { id: UserAccountId, name: String }
```

`AggregateId` は変更なしで、ID 型には引き続き必要です。

## 2. 書き込みを封筒に包む

イベントの採番はドメイン側の責務です: `seq_nr` は 1 始まりで、同一ストリーム内で
連続している必要があります（ライブラリは連続性を検証しません。重複は楽観的ロックが
拒否し、飛び番はそのまま書き込まれます）。`occurred_at` はドメイン供給値で、ナノ秒
精度のまま完全往復します。`manifest` は省略可能・自由形式の型判別子です（省略時は
空文字列）。

`expected_version` の規約（違反は `EventStoreWriteError::ContractViolation`）:

- 新規作成: `seq_nr == 1` **かつ** `expected_version == 0`。
  `persist_event_and_snapshot` を使う。
- 更新: 読取済み `SnapshotEnvelope` の `version()` を渡す。
  `persist_event` は `seq_nr == 1` を受け付けない。

```rust
// 新規作成
let (state, created) = UserAccount::new(id.clone(), "alice".to_string());
let envelope = EventEnvelope::new(id.clone(), 1, Utc::now(), created)
  .with_manifest("user-account-created/v1");
event_store.persist_event_and_snapshot(envelope, state, 0).await?;

// 更新（イベントのみ）
let envelope = EventEnvelope::new(id.clone(), replayed.seq_nr + 1, Utc::now(), renamed);
event_store.persist_event(envelope, replayed.version).await?;
```

## 3. 封筒からリプレイする

読み取りは封筒を返すため、リプレイ位置と次回の `expected_version` は集約フィールド
ではなくストアから得ます。

```rust
pub struct ReplayedUserAccount {
  pub state: UserAccount,
  pub seq_nr: usize,   // 適用済み最後のイベント位置。次のイベントは seq_nr + 1 で採番する
  pub version: usize,  // 次回書込の expected_version として渡す
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

実行可能な examples（`examples/user-account`、`examples/user-account-sqlite`）が
まさにこのパターンを実装しています。

## 4. ビルダーとエラー処理を調整する

- `with_keep_snapshot_count` は `Result<Self, EventStoreWriteError>` を返すように
  なり、全バックエンド一律で `Some(0)` を拒否します（無効化は `None`。履歴行は
  `Some(n)` の間だけ書かれます）。剪定でどの履歴行が残るかはバックエンドごとに
  異なります — [保持の非対称の注記](DATABASE_SCHEMA.ja.md#スナップショット保持のバックエンド間非対称)
  を参照してください。
- 書き込みエラーを網羅的に match している箇所には、新しい
  `EventStoreWriteError::ContractViolation` を追加してください。これはストレージ
  障害ではなく、呼び出し側の契約違反（`seq_nr == 0`、新規作成／更新の対応崩れ、
  `keep_snapshot_count == 0`）を表します。
- 存在しない集約への更新は、一律で
  `optimistic lock failed, aid=<id>, expected_version=<n>`（`actual_version` なし）
  という書式の `OptimisticLockError` を返します。

## データ互換性

**v3 は v2 で書き込まれたデータを読み取らず、移行ツールや互換レイヤも提供しません。**
物理レイアウトは全バックエンドで変更されています（journal の `manifest` 列、ナノ秒
`occurred_at`、純化された payload、キーによる current/履歴判別 —
[DATABASE_SCHEMA.ja.md](DATABASE_SCHEMA.ja.md) を参照）。既存の保存データの移行 —
たとえば v2 リーダーで v2 ストリームをリプレイし v3 で再永続化するなど — は
アプリケーションの責務です。保持すべきデータがあるシステムでは、アップグレード前に
新規ストリームでの再出発か一回限りの変換を計画してください。

## MSRV

v3 は `lib/Cargo.toml` に `rust-version = "1.94.1"` を宣言しています。これは新規の
依存解決で `--all-features` がビルドできる最小ツールチェーンの実測値です（現時点で
AWS SDK スタックが 1.94.1 を要求します）。feature を絞ったビルドはより古いツール
チェーンでも動く可能性がありますが、cargo は宣言値をパッケージ全体に強制します。
