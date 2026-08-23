# ユニットテスト手順 — u1-backend-features (unit-test-instructions)

## テストフレームワークと構成

- Rust組み込みテストハーネス＋`#[tokio::test]`（asyncテスト）。追加のテスト用設定ファイルは不要（既存の cargo workspace 構成のまま）
- テストは実装ファイルと同居する `#[cfg(test)]` モジュール方式（専用 `tests/` ディレクトリなし）。U1の新規テストは `lib/src/event_store_for_memory_test.rs`（`event_store_for_memory.rs` から `#[cfg(test)] mod` 参照）と `types.rs` 内の整形ヘルパーテストに置く

## このユニットのテスト実行コマンド（ユニットスコープ厳守）

最初のテストステップ（Step 3）より前に、以下のコマンドが実行可能なことを確認する（ランナー準備）:

```bash
# U1新規テストのみ（テスト名フィルタ — 全コマンドこのユニットにスコープ）
cargo test -p event-store-adapter-rs --all-features test_optimistic_lock_message   # Step 3: 整形ヘルパー
cargo test -p event-store-adapter-rs --all-features test_event_store_on_memory     # Step 7: Memory挙動契約・共有シナリオ・競合/エラー契約・Clone共有
```

- テスト関数名は既存慣行どおり `test_*` 形式とし、上記フィルタに一致する接頭辞（`test_optimistic_lock_message*` / `test_event_store_on_memory*`）で命名する
- プロジェクト全体の無差別 `cargo test` はこのファイルのコマンドとしては使わない（Build and Test 工程が全ユニットのコマンドを実行するため）

## 期待カバレッジ

- カバレッジは計測しない（チーム確定Q3）。テストの質は以下の充足で担保する:
  - 整形ヘルパー: 基本形／actual_version付加形／機密非混入（3テスト以上）
  - Memory: 作成イベントのErr（panicなし）／共有シナリオ／楽観ロック競合＋BR1.2書式のエラー契約／Clone間状態共有（4テスト以上）
- コンポーネントあたり5〜8テスト（Standard戦略）を目安とする

## モック/スタブ方針

- 不要（全テストがin-process・決定的）。testcontainers / Docker はU1の新規テストでは使用しない（既存のDynamoDB/Bigtable統合テストのみが引き続き使用）
- プロセスグローバルな `env::set_var` 等の変異は使用しない

## テストデータ管理

- 既存の共有テスト資産（`event_store_test_support.rs` の UserAccount 集約と `exercise_user_account_flow`）を再利用する
- 各テストは自前の集約ID（ULID等で一意）を使い、テスト間の状態共有を持たない（Memoryストアはテストごとに新規インスタンス）
