# ユニットテスト手順 — u4-docs (unit-test-instructions)

U4（ドキュメント）のテスト実行手順。本ユニットの検証対象は「examples（実行可能コード）」と「文書記載の実測照合」であり、ライブラリ本体のテストはU1/U2実装済み（既存スイートを緑のまま保つ）。

## テストフレームワークとセットアップ

- 追加セットアップ不要。既存ワークスペースの cargo がランナー（Docker・クラウド接続不要）
- 新規exampleクレート `example-user-account-sqlite` は `examples/*` ワークスペースグロブで自動的にメンバーになる

## 本ユニットのテスト実行コマンド（ユニットスコープ）

```bash
# 1. exampleのコンパイル検証（FR-6.1）
cargo build -p example-user-account-sqlite

# 2. exampleの実行検証（AC4.1.2: クラウド接続なし・Docker不要で完走）
cargo run -p example-user-account-sqlite

# 3. 新規追記分の整形・lintクリーン（新規exampleに限定できないツール仕様のため
#    ワークスペース実行だが、判定対象はexamples/user-account-sqlite/の差分）
cargo +nightly fmt -- --check
cargo clippy -p example-user-account-sqlite --all-targets -- -D warnings

# 4. 記載照合（文書の実測突合）
grep -n "sqlite" README.md README.ja.md docs/DATABASE_SCHEMA.md docs/DATABASE_SCHEMA.ja.md
grep -n "CREATE TABLE" lib/src/event_store_for_sqlite.rs   # DATABASE_SCHEMA転記元
grep -nE "(api[_-]?key|secret|password|token)\s*=" README.md README.ja.md CHANGELOG.md docs/DATABASE_SCHEMA.md docs/DATABASE_SCHEMA.ja.md examples/user-account-sqlite -r || echo "機密リテラルなし"

# 5. 既存スイートが緑のまま（スコープ床）
cargo test -p event-store-adapter-rs --all-features
```

## 期待するカバレッジ

- カバレッジは計測しない（Q3、A確定 — team.md）。判定基準は上記1〜5がすべて成功すること
- exampleの実行成功が事実上のend-to-end検証（作成→リネーム→スナップショット/リプレイを実DBで通す）

## モック・テストデータ管理

- モック不使用。exampleのDBは相対パスの一時ファイルまたは `:memory:` を使用し、実行後に残骸を残さない（一時ファイルの場合は実行内で削除、または `:memory:` を既定とする）
- 実在の個人環境パス・機密リテラルを使用しない（NFR-4.12）
