# ビルド・テストサマリー — build-and-test (build-and-test-summary)

全ユニット（u1〜u4）のコード生成成果（各 `code-generation-plan.md` / `unit-test-instructions.md` / `code-summary.md`）に対する検証の全体像。

## ビルム状態と前提

- ビルド前提: Rust stable＋nightly（fmtのみ）＋cargo-deny。SQLite系はDocker・クラウド不要
- 全ユニットのコード生成はレビューREADYで完了済み（`sqlite` ブランチにコミット済み）

## テスト種別インベントリ

| 手順書 | 生成 | 理由 |
|---|---|---|
| build-instructions.md | あり | featureマトリクスビルド＋検証コマンド |
| integration-test-instructions.md | あり | Standard戦略 — 境界テスト（バックエンド×GenericEventStore×実DB） |
| performance-test-instructions.md | 対象なしの記録 | 数値性能NFRなし・Standard戦略対象外（ライブラリ） |
| security-test-instructions.md | あり | 依存監査（cargo-deny）・静的検査・エラー契約（devsecops観点） |

## ユニット別カバレッジ期待

- U1: 整形ヘルパー3テスト＋Memory挙動4テスト以上（フィルタ: `test_optimistic_lock_message*` / `test_event_store_on_memory*`）
- U2: SQLite 8テスト以上（共有シナリオ・自動作成・競合・エラー契約・保持・`:memory:`共有）
- U3: CIジョブと同一コマンドのローカル緑化（マトリクス6構成・clippy・deny）
- U4: example build/run・fmt/clippy・記載照合・既存スイート緑
- カバレッジ数値は計測しない（チーム確定Q3 — スコープ床に数値床なし）

## 実行結果

`build-test-results.md` を参照（本工程で実測）。

## レディネス評価

- **build-ready**: 実測にて判定（build-test-results.md）
- **test-ready**: 同上
- **deployment-ready**: mainマージ後、`lib-bump-version.yml`（workflow_dispatch＋日次cron）→ `lib-release.yml` の既存自動フローで公開可能。手動承認は追加しない（チーム確定Q5）

## 既知の残項目（バックログ — 本ワークフローで対応しない）

- AWS SDK TLSスタック移行による RUSTSEC ignore 4件の解消
- bigtable 既存コードの `println!` デバッグ出力残存
- MSRV宣言・検証、examplesのCIビルド検証
