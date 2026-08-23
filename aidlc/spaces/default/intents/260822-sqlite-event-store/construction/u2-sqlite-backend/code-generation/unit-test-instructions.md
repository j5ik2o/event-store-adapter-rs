# ユニットテスト手順 — u2-sqlite-backend (unit-test-instructions)

## テストフレームワークと構成

- Rust組み込みテストハーネス＋`#[tokio::test]`（asyncテスト）。追加のテスト用設定ファイルは不要
- テストは実装ファイルと同居する `#[cfg(all(test, any(feature = "sqlite", feature = "sqlite-system")))]` モジュール方式。U2の新規テストは `lib/src/event_store_for_sqlite_test.rs`（`event_store_for_sqlite.rs` からモジュール参照）に置く

## このユニットのテスト実行コマンド（ユニットスコープ厳守）

最初のテストステップ（Step 4）より前に、以下のコマンドが実行可能なことを確認する（ランナー準備 — Step 2 のfeature実体化後に有効）:

```bash
# U2新規テストのみ（テスト名フィルタ — 全コマンドこのユニットにスコープ）
cargo test -p event-store-adapter-rs --no-default-features --features sqlite test_event_store_on_sqlite
```

- テスト関数名は既存慣行どおり `test_*` 形式とし、接頭辞 `test_event_store_on_sqlite*` で命名する（例: `test_event_store_on_sqlite`〔共有シナリオ〕、`test_event_store_on_sqlite_in_memory`、`test_event_store_on_sqlite_optimistic_lock_conflict`、`test_event_store_on_sqlite_error_contract`、`test_event_store_on_sqlite_snapshot_retention`）
- プロジェクト全体の無差別 `cargo test` はこのファイルのコマンドとしては使わない

## 期待カバレッジ

- カバレッジは計測しない（チーム確定Q3）。テストの質は以下の充足で担保する:
  - 共有シナリオ `exercise_user_account_flow`（ファイルDB — 契約対称性）
  - スキーマ自動作成（空ファイルから成功）／復元一致／書き込み不能パスの中立エラー（panicなし）
  - 楽観的ロック競合パス（2ハンドル順次コミットの決定的検証）＋原子性＋BR1.2書式のエラー契約
  - `:memory:` のインスタンス＋Clone共有／ファイルDB再構築復元
  - 保持ポリシー（keep_snapshot_count / delete_ttl が実際に効く）
- コンポーネントあたり5〜8テスト（Standard戦略）を目安に、合計8テスト以上

## モック/スタブ方針

- 不要（全テストがin-process・決定的）。testcontainers / Docker は使用しない（AC3.2.2）
- プロセスグローバルな `env::set_var` 等の変異は使用しない

## テストデータ管理

- 既存の共有テスト資産（`event_store_test_support.rs` の UserAccount 集約と `exercise_user_account_flow`）を再利用する
- ファイルDBは標準ライブラリの一時ディレクトリ＋一意名（既存dev依存のULID生成）で作成し、自テストで作成したファイルのみ後始末する。新規dev依存（tempfile等）は追加しない
- 各テストは自前の集約ID・自前のDB（ファイルまたは `:memory:`）を使い、テスト間の状態共有を持たない
