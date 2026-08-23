# コンポーネント一覧 — event-store-adapter-rs

## サマリ

| コンポーネント | 場所 | 責務 | 健全性 |
|---|---|---|---|
| types | `lib/src/types.rs` | 公開トレイト契約とエラー型 | at-risk |
| event_store_backend | `lib/src/event_store_backend.rs` | バックエンド抽象（StorageBackend） | healthy |
| generic_event_store | `lib/src/generic_event_store.rs` | 共通ポリシーの橋渡し実装 | healthy |
| event_store_for_dynamodb | `lib/src/event_store_for_dynamodb.rs` | DynamoDB バックエンド | healthy |
| event_store_for_bigtable | `lib/src/event_store_for_bigtable.rs` | Bigtable バックエンド | at-risk |
| event_store_for_memory | `lib/src/event_store_for_memory.rs` | インメモリバックエンド | degraded |
| key_resolver | `lib/src/key_resolver.rs` | パーティションキー解決 | healthy |
| serializer | `lib/src/serializer.rs` | シリアライズ抽象 | healthy |
| event_store_test_support | `lib/src/event_store_test_support.rs` | 共有テストシナリオ | healthy |
| event-store-adapter-test-utils-rs | `test-utils/` | テスト用インフラ起動・DDL | at-risk |
| example-user-account | `examples/user-account/` | E2E サンプル | at-risk |

健全性評価基準: **healthy** = 責務が明確で既知の重大課題なし / **at-risk** = 機能するが設計上の課題・欠落があり変更時に注意が必要 / **degraded** = 契約違反・テスト欠落など、現状のままの拡張は推奨されない。

## 各コンポーネント詳細

### types

- **責務**: `AggregateId` / `Event` / `Aggregate` / `EventStore` の公開トレイト契約、`EventStoreWriteError` / `EventStoreReadError` エラー型。
- **依存**: async-trait、serde、chrono、thiserror、**aws-sdk-dynamodb（型リーク）**。
- **健全性**: **at-risk** — 全バックエンド共通の公開契約に `TransactionCanceledExceptionWrapper` 経由で AWS SDK 型がリークしている（TD-01）。feature 分割・SQLite 追加の最重要ボトルネック。

### event_store_backend

- **責務**: バックエンドが実装する最小契約 `StorageBackend`（5メソッド、`on_event_persisted` はデフォルト no-op）と `SnapshotEnvelope` / `SnapshotMaintenance`。
- **依存**: types、async-trait、chrono。
- **健全性**: **healthy** — 抽象自体は簡潔で、SQLite 追加はこの5メソッド実装で既存パターンに乗れる。注意点: モジュールが private であること、`SnapshotMaintenance` が到達不能 pub であること（TD-04）。

### generic_event_store

- **責務**: `StorageBackend` を `EventStore` に橋渡しし、共通ポリシー（`is_created()` 分岐、作成イベント拒否、`on_event_persisted` フック、maintenance 設定）を一元化。
- **依存**: event_store_backend、types。
- **健全性**: **healthy** — モックによるユニットテストあり。DynamoDB / Bigtable の共通化に成功している。

### event_store_for_dynamodb

- **責務**: 公開ファサード `EventStoreForDynamoDB` と内部 StorageBackend 実装。TransactWriteItems による原子的 CAS、スナップショット保持数 / TTL 整理。
- **依存**: generic_event_store、event_store_backend、key_resolver、serializer、aws-sdk-dynamodb、aws-config。
- **健全性**: **healthy** — 機能が最も完全で統合テストあり。`unsafe impl Send/Sync` 手書き（TD-06）のみ注意。

### event_store_for_bigtable

- **責務**: 公開ファサード `EventStoreForBigtable` と内部 StorageBackend 実装（tonic gRPC）。
- **依存**: generic_event_store、event_store_backend、key_resolver、serializer、tonic、googleapis-tonic-google-bigtable-v2。
- **健全性**: **at-risk** — 楽観ロックが read→write の2段階で非原子的（レースウィンドウあり、TD-07）。イベント書込みとスナップショット更新も別 RPC。スナップショット保持機能が未実装で `with_keep_snapshot_count` がサイレント無効（TD-08）。`with_delete_ttl` 非提供。

### event_store_for_memory

- **責務**: HashMap ベースのインメモリ実装（テスト・プロトタイプ用）。
- **依存**: types のみ（StorageBackend **非経由**）。
- **健全性**: **degraded** — 旧構造のまま `EventStore` を直接実装しており抽象に乗っていない。`persist_event` に作成イベントを渡すと他バックエンドの `Err` と異なり `panic!`（契約非対称、TD-05）。`unsafe impl Send/Sync` 手書きで HashMap 直持ちのため Clone 時に状態が分岐。専用テストなし。

### key_resolver

- **責務**: パーティションキー解決（`{type_name}-{hash%shard_count}`）。`KeyResolver` トレイト + `DefaultKeyResolver`。
- **依存**: types（AggregateId）。
- **健全性**: **healthy** — 小さく安定。DynamoDB / Bigtable で共用。

### serializer

- **責務**: `EventSerializer` / `SnapshotSerializer` 抽象と JSON デフォルト実装。
- **依存**: serde / serde_json、types。
- **健全性**: **healthy** — 差替え可能な設計で安定。

### event_store_test_support

- **責務**: バックエンド横断の共有テストシナリオ `exercise_user_account_flow`（作成 → リネーム×2、スナップショット / リプレイ検証）。
- **依存**: types（`#[cfg(test)]` 限定）。
- **健全性**: **healthy** — バックエンド追加時の受入シナリオとして再利用可能。SQLite 追加時もこれに乗せるのが自然。

### event-store-adapter-test-utils-rs

- **責務**: testcontainers による LocalStack（DynamoDB）/ Bigtable エミュレータ起動、DynamoDB テーブル作成 DDL、ULID 生成器。
- **依存**: testcontainers、aws-sdk-dynamodb、googleapis-tonic-google-bigtable-admin-v2、ulid-generator-rs ほか。
- **健全性**: **at-risk** — 機能はするが、DynamoDB の DDL がここに置かれておりライブラリ側の自動テーブル作成方針と設計差分がある。serial_test / prost など宣言のみ未使用依存（TD-14）。example から通常依存されている（TD-13）。

### example-user-account

- **責務**: DynamoDB バックエンドの E2E サンプル（`UserAccountRepository` によるスナップショット + リプレイの読出しパターンの範例）。
- **依存**: lib（path）、test-utils（path・通常依存）。
- **健全性**: **at-risk** — test-utils への通常依存が「本番コードがテストユーティリティを必要とする」ように見え誤解を招く（TD-13）。CI でビルド検証されていない（TD-11）。
