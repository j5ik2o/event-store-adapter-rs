# 統合テスト手順 — build-and-test (integration-test-instructions)

Standard戦略の統合テスト手順。本クレートの統合テストは実装ファイル同居の `#[cfg(test)]` モジュール（`event_store_for_*_test.rs`）であり、各バックエンドとGenericEventStore・実ストレージの境界を実DB（SQLiteはファイル/`:memory:`、Memoryはin-process）で検証する。ユニット別コマンドは各 `construction/*/code-generation/unit-test-instructions.md` が正。

## 境界テストの構成（クロスユニット観点）

| 境界 | テスト | 実行コマンド |
|---|---|---|
| U1エラー型 × 全バックエンド | BR1.2書式ヘルパー（`test_optimistic_lock_message*`） | `cargo test -p event-store-adapter-rs --all-features test_optimistic_lock_message` |
| U1 Memory準拠化 × GenericEventStore | 共有シナリオ・競合・エラー契約・Clone共有（`test_event_store_on_memory*`） | `cargo test -p event-store-adapter-rs --all-features test_event_store_on_memory` |
| U2 SQLite × GenericEventStore | 共有シナリオ・自動作成・競合パス・エラー契約・保持（`test_event_store_on_sqlite*`） | `cargo test -p event-store-adapter-rs --no-default-features --features sqlite test_event_store_on_sqlite` |
| U3 featureマトリクス（Docker不要3構成） | 構成ごとの全テスト | `cargo test -p event-store-adapter-rs --no-default-features` / `--features sqlite` / `--features sqlite-system` |
| U4 example × 公開API | 実行によるend-to-end検証（作成→リネーム→リプレイ） | `cargo run -p example-user-account-sqlite` |
| 全feature回帰（既存スイート） | 24テスト（DynamoDB/Bigtable含む — Docker要） | `cargo test -p event-store-adapter-rs --all-features` |

## セットアップ・データ管理

- 追加セットアップ不要（Docker必要なのは全feature回帰のDynamoDB/Bigtableテストのみ）
- 各テストは自前の集約ID・自前のDBを使い、テスト間の状態共有なし（並列安全 — NFR-5）

## 期待カバレッジ

- カバレッジは計測しない（チーム確定Q3）。品質は共有シナリオ＋楽観的ロック競合＋エラー契約＋保持ポリシーの充足で担保（project.md Mandated）
