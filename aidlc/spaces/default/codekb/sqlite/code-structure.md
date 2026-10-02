# コード構成 — event-store-adapter-rs

## ワークスペース構成

```text
event-store-adapter-rs/
├── Cargo.toml                 # workspace ルート (resolver = "2")、[workspace.dependencies] で版一元化
├── lib/                       # 本体クレート event-store-adapter-rs (v1.3.18, crates.io 公開)
│   └── src/
│       ├── lib.rs                             # モジュール宣言とグロブ再エクスポート
│       ├── types.rs                           # 公開トレイト群とエラー型
│       ├── event_store_backend.rs             # StorageBackend 抽象 (private)
│       ├── generic_event_store.rs             # GenericEventStore (private) + モックユニットテスト
│       ├── key_resolver.rs                    # KeyResolver / DefaultKeyResolver (pub mod)
│       ├── serializer.rs                      # EventSerializer / SnapshotSerializer (pub mod)
│       ├── event_store_for_memory.rs          # Memory バックエンド (EventStore 直接実装・旧構造)
│       ├── event_store_for_dynamodb.rs        # DynamoDB バックエンド (615行)
│       ├── event_store_for_bigtable.rs        # Bigtable バックエンド (627行)
│       ├── event_store_for_dynamodb_test.rs   # #[cfg(test)] 統合テスト
│       ├── event_store_for_bigtable_test.rs   # #[cfg(test)] 統合テスト
│       └── event_store_test_support.rs        # 共有テストシナリオ (242行, #[cfg(test)])
├── test-utils/                # event-store-adapter-test-utils-rs (v0.0.1, 内部利用)
│   └── src/                   # lib.rs / docker.rs / dynamodb.rs / bigtable.rs / id_generator.rs
├── examples/user-account/     # example-user-account (binary, publish = false)
│   └── src/                   # main.rs / user_account.rs / user_account_repository.rs
├── docs/                      # DATABASE_SCHEMA.md / DATABASE_SCHEMA.ja.md (DynamoDB の Journal/Snapshot 設計)
├── tools/                     # docker-compose.yaml (LocalStack / Bigtable emulator) ほか
├── .github/workflows/         # ci.yml / lib-release.yml / lib-bump-version.yml / openai-review.yml
├── Makefile.toml              # cargo-make (fmt タスクのみ)
└── rustfmt.toml               # max_width=120, tab_spaces=2
```

## モジュール構成とファイル分類（lib/src）

| ファイル | 分類 | 可視性 | 役割 |
|---|---|---|---|
| `lib.rs` | エントリポイント | — | モジュール宣言。`pub use event_store_for_*::*;` のグロブ再エクスポート3行（無条件）。dynamodb / bigtable モジュール宣言に `#[allow(dead_code)]` 付与 |
| `types.rs` | 公開契約 | `pub mod` | `AggregateId` / `Event` / `Aggregate` / `EventStore` トレイト、`EventStoreWriteError` / `EventStoreReadError`、`TransactionCanceledExceptionWrapper` |
| `event_store_backend.rs` | 内部抽象 | private | `StorageBackend` トレイト（5メソッド）、`SnapshotEnvelope`、`SnapshotMaintenance` |
| `generic_event_store.rs` | 内部抽象 | private | `GenericEventStore`（StorageBackend → EventStore 橋渡し） |
| `key_resolver.rs` | 公開補助 | `pub mod` | パーティションキー解決 |
| `serializer.rs` | 公開補助 | `pub mod` | イベント / スナップショットのシリアライズ抽象と JSON デフォルト実装 |
| `event_store_for_dynamodb.rs` | バックエンド | 再エクスポート | 公開ファサード + 内部 StorageBackend 実装。GenericEventStore へ委譲 |
| `event_store_for_bigtable.rs` | バックエンド | 再エクスポート | 同上（`with_delete_ttl` なし、保持管理未実装） |
| `event_store_for_memory.rs` | バックエンド | 再エクスポート | **StorageBackend 非経由**で `EventStore` を直接実装（旧構造） |
| `event_store_*_test.rs` / `event_store_test_support.rs` | テスト | `#[cfg(test)]` | 統合テストと共有シナリオ |

## コードパターン

- **ビルダー風構成**: 公開ファサード型は `new(...)` の後に `with_keep_snapshot_count` / `with_delete_ttl` / `with_key_resolver` / `with_event_serializer` / `with_snapshot_serializer` を method chaining で適用する（`self` 消費型ビルダー）。
- **委譲パターン**: `EventStoreForDynamoDB` / `EventStoreForBigtable` は内部の `GenericEventStore<AID, A, E, B>` に `EventStore` 実装を委譲し、バックエンド固有ロジックは private な `StorageBackend` 実装に閉じる。Memory のみこのパターンに乗っていない（TD-05）。
- **`#[async_trait]`**: すべての async トレイト（`EventStore` / `StorageBackend` / serializer 系）で使用。
- **テスト同居方式**: 専用 `tests/` ディレクトリはなく、`#[cfg(test)]` モジュールをソースと同じクレート内に置く。共有シナリオは `event_store_test_support.rs::exercise_user_account_flow`（作成 → リネーム×2、スナップショット / リプレイ検証）。
- **`unsafe impl Send/Sync` の手書き**: 3バックエンド型すべてに付与（`event_store_for_memory.rs:21-23`、`event_store_for_dynamodb.rs:601-615`、`event_store_for_bigtable.rs:613-627`）。不要の可能性が高い（TD-06）。
- **型パラメータ規約**: `<AID: AggregateId, A: Aggregate<ID = AID>, E: Event<AggregateID = AID>>` を全域で統一。

## スタイル規約

- rustfmt 設定は `max_width = 120` / `tab_spaces = 2`。CI（`ci.yml`）で fmt チェックが先行し、その後 `cargo test -p` で lib のテストが走る。
- clippy は未導入のため、リント水準はコンパイラ警告と rustfmt のみ（TD-11）。
- doc コメントは日本語主体（`types.rs`）で、一部英日併記。
