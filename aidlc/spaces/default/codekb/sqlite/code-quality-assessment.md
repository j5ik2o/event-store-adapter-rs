# コード品質評価 — event-store-adapter-rs

## テストの現状

- **構成**: 専用 `tests/` ディレクトリなし。`#[cfg(test)]` モジュール同居方式。
  - DynamoDB 統合テスト1本（`event_store_for_dynamodb_test.rs`、testcontainers + LocalStack 2.1.0）
  - Bigtable 統合テスト1本（`event_store_for_bigtable_test.rs`、cloud-sdk emulators）
  - `GenericEventStore` のモックユニットテスト（`generic_event_store.rs` 内）
  - 共有シナリオ `event_store_test_support.rs::exercise_user_account_flow`（作成 → リネーム×2、スナップショット / リプレイ検証）
- **タイミング調整**: 環境変数 `TEST_TIME_FACTOR`。
- **欠落**:
  - カバレッジ計測なし（tarpaulin / llvm-cov 等未導入）
  - **Memory バックエンド専用テストなし**
  - **楽観ロック競合パス（並行書込み → OptimisticLockError）の直接テストなし**
  - examples のビルド検証が CI にない

## リンタ・フォーマッタ

- rustfmt のみ（`max_width = 120`, `tab_spaces = 2`）。CI で fmt チェック実施。
- **clippy 未導入** — リント水準はコンパイラ警告どまり。`#[allow(dead_code)]` がモジュール宣言レベルで付与されており、警告が実質封じられている箇所がある（TD-03）。

## CI / CD

| ワークフロー | トリガ | 内容 |
|---|---|---|
| `ci.yml` | push / PR | fmt チェック → `cargo test -p`（lib、統合テスト含む） |
| `lib-release.yml` | `v` タグ | crates.io publish |
| `lib-bump-version.yml` | main へのマージ | Conventional Commits によるバージョン自動バンプ |
| `openai-review.yml` | PR | LLM レビュー補助 |

Renovate による依存更新は有効。CI の網羅性の欠落（clippy / カバレッジ / feature マトリクス / MSRV / examples ビルド）は TD-11 参照。

## ドキュメント品質

- README 英日、`docs/DATABASE_SCHEMA.md` 英日（DynamoDB の Journal / Snapshot 設計）。多言語ファミリーの中では文書が整っている部類。
- ただし **README のコード例が現行 API と乖離**、crate description も DynamoDB 専用時代のまま（TD-10）。
- `types.rs` の doc コメントは日本語主体で丁寧。`.github/CODEONWERS` は typo のため CODEOWNERS として機能していない（TD-12）。

## 技術的負債レジスタ

スキャンで検出された 14 シグナルを重要度順に整理する。「SQLite 影響」は SQLite バックエンド追加・feature 分割作業への影響度。

| ID | 重要度 | 内容 | 場所 | SQLite 影響 |
|---|---|---|---|---|
| TD-01 | critical | `EventStoreWriteError::OptimisticLockError(#[from] TransactionCanceledExceptionWrapper)` が aws_sdk_dynamodb の型を直接ラップ。Memory / Bigtable も失敗時に `TransactionCanceledExceptionWrapper(None)` を返す | `lib/src/types.rs` | **ブロッカー級**。dynamodb feature 化にはバックエンド中立エラー型への再設計が必要（破壊的変更になり得る） |
| TD-02 | high | aws-config / aws-sdk-dynamodb / aws-http / tonic / googleapis-tonic-google-bigtable-v2 がすべて無条件依存。aws-http は未使用 | `lib/Cargo.toml` | feature 分割には optional 化必須。aws-http は即削除可能 |
| TD-03 | high | `pub use event_store_for_*::*;` グロブ再エクスポート3行が無条件。dynamodb / bigtable モジュール宣言に `#[allow(dead_code)]` | `lib/src/lib.rs` | `#[cfg(feature)]` ガード要 |
| TD-04 | medium | `StorageBackend` / `GenericEventStore` が private。`SnapshotMaintenance` は `maintenance()` の公開戻り値なのに利用者が型名を書けない到達不能 pub | `lib/src/event_store_backend.rs`, `generic_event_store.rs` | SQLite はこの抽象に乗れば低コスト。到達不能 pub は公開 API 整理の論点 |
| TD-05 | medium | Memory が StorageBackend 非経由。`persist_event` に作成イベントを渡すと panic!（他バックエンドは Err） | `lib/src/event_store_for_memory.rs` | 契約対称化（Err 化 + 抽象準拠）の好機 |
| TD-06 | medium | `unsafe impl Send/Sync` が3バックエンド型すべてに手書き（不要の可能性大）。Memory は HashMap 直持ちで Clone 時に状態分岐 | 各 `event_store_for_*.rs` | SQLite では踏襲しない。既存分は削除検証候補 |
| TD-07 | medium-high | Bigtable の楽観ロックが read→write の2段階で非原子的（レースウィンドウ）。イベント書込みとスナップショット更新も別 RPC | `lib/src/event_store_for_bigtable.rs` | SQLite はトランザクションで原子性確保可能。DynamoDB の原子的 CAS 契約に合わせるべき |
| TD-08 | medium | Bigtable でスナップショット保持機能が未実装（`_maintenance` 無視、`with_keep_snapshot_count` サイレント無効） | `lib/src/event_store_for_bigtable.rs` | SQLite では保持機能を最初から実装し、サイレント無効を作らない |
| TD-09 | low | テスト内デバッグ `println!` 残存（bigtable process_chunk） | `lib/src/event_store_for_bigtable_test.rs` 系 | なし（掃除のみ） |
| TD-10 | low | crate メタデータ陳腐化（description が "crate to make DynamoDB an Event Store"）。README のコード例が現行 API と乖離 | `lib/Cargo.toml`, `README.md` | SQLite 追加時にドキュメント更新へ相乗り |
| TD-11 | medium | CI 網羅不足: clippy なし、カバレッジなし、feature マトリクスなし、MSRV 検証なし、examples ビルド検証なし | `.github/workflows/ci.yml` | feature 分割後は feature マトリクス CI が実質必須 |
| TD-12 | low | `.github/CODEONWERS` の typo（CODEOWNERS と認識されない） | `.github/` | なし（リネームのみ） |
| TD-13 | low | examples が test-utils に**通常依存**（本番向けサンプルがテストユーティリティに依存して見える） | `examples/user-account/Cargo.toml` | スキーマ自動作成をライブラリ内蔵にすれば解消方向 |
| TD-14 | low | serial_test / prost / anyhow など宣言のみ未使用の依存 | `lib/Cargo.toml`, `test-utils/Cargo.toml` | なし（掃除のみ） |

## 総合評価

コアの抽象設計（StorageBackend / GenericEventStore）と共有テストシナリオは健全で、バックエンド追加の受け皿はできている。一方で品質ゲート（clippy / カバレッジ / feature マトリクス / MSRV）が薄く、公開エラー契約への AWS SDK リーク（TD-01）と無条件依存（TD-02, TD-03）が feature 分割の前提整備として残っている。SQLite 追加は「実装コストは低いが、周辺負債の返済と抱き合わせで計画すべき」状態である。
