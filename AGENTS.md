# Repository Guidelines

## プロジェクト構成とモジュール整理
本リポジトリは Rust workspace です。メンバーは `lib`、`test-utils`、`examples/*`、`conformance-runner`、`migration-cli` です（ルートの `Cargo.toml`）。保存先は **メモリと DynamoDB の 2 つ**です。SQLite・Bigtable の保存先、LocalStack、`KeyResolver`（`key_resolver.rs`）は 5.x にはありません（SQLite・Bigtable は 3.x 系に残ります）。

- `lib/`: 公開クレート `event-store-adapter-rs`（版 `5.0.0-alpha.0`、最小サポートの Rust は 1.94.1）です。
  - feature: 既定は空です。`dynamodb`（AWS SDK を使う DynamoDB の保存先）、`migration`（`dynamodb` も有効にする、v3 の DynamoDB データの移行関数）、`test-hooks`（試験のためのフック点）があります。
  - 保存先: `src/memory/`（`EventStoreForMemory`・`MemoryStorage`。常に使えます）と `src/dynamodb/`（`EventStoreForDynamoDB`・`DynamoDbTables`・`DynamoDbOptions`。feature `dynamodb` のときだけ公開）です。
  - 共通のモジュール: `aggregate_id`・`event_envelope`・`event_store`・`error`・`seq_nr`・`serializer`・`retention`、非公開の `generic_event_store`・`storage_backend` です。
  - 移行: `src/migration/`（feature `migration` のときだけ公開）です。
- `conformance/`: 共通契約と DynamoDB プロファイルの期待値を配る適合テストデータ（データの版は 1.2.0）です。`python3 tools/conformance/manifest.py verify` で整合を確かめます。
- `conformance-runner/`: 適合テストデータを実行する実行器です。パッケージ名は `event-store-adapter-conformance-rs` で、公開しません。引数は `--backend memory|dynamodb --data <パス> --report <パス> [--require-all]` です。実際の DynamoDB に対して実行するには feature `dynamodb` が要ります。付けないと、DynamoDB のケースは未検証として報告されます。
- `migration-cli/`: v3 から 5.x への移行の実行ファイルです。パッケージ名は `event-store-adapter-migration-rs` で、公開しません。移行関数 `migrate_v3_dynamodb` を呼ぶ薄い実行ファイルです。使い方は `docs/MIGRATION_GUIDE_v5.md` にあります。
- `test-utils/`: パッケージ名は `event-store-adapter-test-utils-rs` です。`docker::dynamodb_local()` が、digest で固定した DynamoDB Local 3.3.1 を testcontainers で起動します。`dynamodb::create_dynamodb_local_client` はダミーの region と資格情報を明示したクライアントを返し、`dynamodb::create_tables` が journal・snapshot・head の 3 テーブルを作ります。
- `examples/user-account/`: パッケージ名は `example-user-account` です。DynamoDB Local を起動して event sourcing の使い方を end to end で確かめます。
- `docs/`: `DATABASE_SCHEMA*.md`（DynamoDB のテーブル配置）、`MIGRATION_GUIDE_v3*.md`（v2 から v3、旧版向け）、`MIGRATION_GUIDE_v5*.md`（v3 から 5.x）、`design/next-major.md`（5.x の設計）があります。レビュー前に、変更に関わる文書を読んでください。
- `tools/`: `conformance/manifest.py`（適合テストデータの整合の確認）と、観測基盤のサンプル設定ファイルがあります。ビルドとテストでは使いません。

## ビルド・テスト・開発コマンド
ツールチェーンは固定しません。CI は stable を使います（整形と、公開 API の形の照合だけ nightly）。公開 API の形の照合（`api-surface/`）は、日付を固定した nightly で rustdoc の JSON を作るので、`rustup toolchain install nightly-2026-09-27 --profile minimal` で入れておきます。

- ビルド: `cargo build --workspace --all-targets --all-features`
- Docker なしでメモリの保存先だけを確かめる: `cargo test -p event-store-adapter-rs --no-default-features`
- ライブラリの全テスト（DynamoDB の試験を含む。Docker が必要）: `cargo test -p event-store-adapter-rs --all-features`
- workspace 全体のテスト（Docker と固定日の nightly が必要）: `cargo test --workspace --all-features`
- 特定の保存先に絞るときは、テスト名で絞ります。例: `cargo test -p event-store-adapter-rs --all-features dynamodb`
- 静的解析: `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- 整形: `cargo +nightly fmt --all`（確認は `cargo +nightly fmt --all -- --check`）。nightly がなければ `rustup install nightly` を実行します。
- 依存の監査: `cargo deny check advisories licenses`
- 適合試験（メモリ）: `cargo run -p event-store-adapter-conformance-rs --release -- --backend memory --data conformance --report conformance-report-memory.json`。DynamoDB に対して実行するときは、`--features dynamodb` を付け、`--backend dynamodb` にします。
- 利用例: `cargo run -p example-user-account`（Docker が必要。ログの水準は環境変数 `LOG_LEVEL` で変えます）
- 移行の実行ファイル: `cargo run -p event-store-adapter-migration-rs -- --help`

## コーディングスタイルと命名規約
`rustfmt.toml` を正とします（`tab_spaces = 2`、`max_width = 120`）。インデントは 2 スペースです。名前は Rust の慣習に従い、型とトレイトは `UpperCamelCase`、関数・モジュール・テストは `snake_case` にします。責務ごとにファイルを分けます。コメントはビジネスロジックの意図を明確にする目的でのみ追加し、doc comment の先頭は三人称現在形で書き始めると統一感が出ます。手書きの `unsafe impl` は禁止です（CI が `lib/src/` を検査します）。feature なしの依存グラフに、クラウドの SDK を入れてはいけません（CI が検査します）。

## テスト指針
単体テストは、実装と同じ場所の `*_test.rs` に置き、`#[cfg(test)] #[path = "x_test.rs"] mod tests;` などで結びます。結合テストは各クレートの `tests/` に置きます。テスト関数名は `should_*` 形式を基本にします（既存のテストの大半が従っています）。

DynamoDB の保存先、移行、利用例、DynamoDB に対する適合試験は、testcontainers で DynamoDB Local を起動するので、Docker が起動していることを事前に確認してください。メモリの保存先のテストは Docker が要りません。`cargo test -- --nocapture` を使うと、ログ出力を調べやすくなります。

## コミットとプルリクエスト指針
Git 履歴は Conventional Commits の形式（`feat: ...`、`fix: ...`、`chore(deps): ...` など）が主流で、リリースコミットは `version up to vX.Y.Z` です。新機能には `feat:`、バグ修正には `fix:` を用い、範囲の広い変更は `refactor:` または `perf:` を検討してください。プルリクエストでは概要、動機、テスト結果、影響範囲を箇条書きで示し、関連 Issue を `Closes #123` 形式でリンクします。レビュアーが再現できる `Steps to verify` を明記することで、レビュー時間を短縮できます。

版上げとリリースは workflow で行います。`lib-bump-version.yml` は手動起動（`workflow_dispatch`）だけで動きます。`lib/Cargo.toml` の版が pre-release のときは、`major` の指定だけを受け付けます。入力 `version`（任意。`X.Y.Z`）を指定すると、`level` とこの制限を使わずにその版へ上げます。その版は、既存の `v*` タグの最大より大きく、同じ名前のタグがないものに限ります。`lib-release.yml` は、`v*.*.*` 形式と pre-release 形式のタグの push で起動し、タグと `lib/Cargo.toml` の版が一致することを確かめてから公開します。

## セキュリティと設定のヒント
AWS 認証情報は、通常の AWS 設定（`~/.aws/credentials` や環境変数）から読み込まれます（`migration-cli` も同じです）。`.env` やソースコードに平文で保存しないでください。試験は、ダミーの資格情報で DynamoDB Local へ接続します。CI や Renovate が生成するブランチと衝突させないよう、手動アップデート時は先に Issue を立てて計画を共有しましょう。
