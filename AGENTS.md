# Repository Guidelines

## プロジェクト構成とモジュール整理
本リポジトリは Rust workspace 構成で、core crate は `lib/` ディレクトリに置かれています。`lib/src` には DynamoDB 向け `event_store_for_dynamodb.rs`、Bigtable 向け `event_store_for_bigtable.rs`、インメモリ実装などのモジュールがあり、共通ユーティリティは `serializer.rs` / `key_resolver.rs` / `types.rs` に集約されています。サポート用 crate である `test-utils/` は LocalStack（DynamoDB）と Cloud SDK Bigtable emulator を起動するラッパーを提供し、`examples/user-account/` では event sourcing の利用例を end to end で確認できます。また `docs/` と `tools/` には設計資料や開発支援スクリプトが整理されているため、レビュー前に一読して背景を把握してください。

## ビルド・テスト・開発コマンド
`cargo build` を実行すると workspace に含まれる全クレートをリリースビルド前提で検証できます。`cargo test` はユニットテストと統合テストを同時に走らせるので、特定バックエンドのみ確認したい場合は `cargo test -p event-store-adapter-rs dynamodb` や `cargo test -p event-store-adapter-rs bigtable` のようにテスト名で絞り込んでください。Bigtable テストは Docker で `gcr.io/google.com/cloudsdktool/cloud-sdk:emulators` を起動するため、最初の実行はイメージ取得時間を考慮します。整形には `cargo +nightly fmt` を利用し、Nightly toolchain が無い場合は `rustup install nightly` でセットアップします。サンプルアプリを確認したい場合は `cargo run -p user-account --bin user-account` を使い、必要に応じて環境変数 `AWS_PROFILE` や `OTEL_EXPORTER_OTLP_ENDPOINT` を上書きします。

## コーディングスタイルと命名規約
Rustfmt ベースの 4 space インデントを厳守し、公開 API では `CamelCase`、内部関数とテストは `snake_case` を一貫させてください。モジュール分割は責務ごとにファイルを分け、serializer や key resolver のような cross cutting concern は `lib/` 直下へ配置します。コメントはビジネスロジックの意図を明確にする目的でのみ追加し、doc comment の先頭は三人称現在形で書き始めると統一感が出ます。`rustfmt.toml` に合わせて `cargo fmt --all` を走らせる場合でも、Nightly 指定を忘れないでください。

## テスト指針
テストは標準ライブラリと `serial_test` を併用しており、状態を共有するケースでは `#[serial]` で直列化します。外部サービスに依存する統合テストでは `testcontainers` crate を利用するため、Docker が起動していることを事前に確認してください。DynamoDB テストは LocalStack、Bigtable テストは Cloud SDK emulator を自動で立ち上げるので、CI/ローカルともに Docker エンジンが必須です。新しいシナリオを追加する際は `*_test.rs` ファイルにテスト関数を置き、関数名を `should_*` 形式で書くと期待値が読み取りやすくなります。また `cargo test -- --nocapture` を活用するとログ出力を調査しやすく、レースコンディションの検出に役立ちます。

## コミットとプルリクエスト指針
Git 履歴は `version up to vX.Y.Z` というリリースコミットと `chore(deps): update ...` のような Conventional Commits 互換メッセージが主流です。新機能には `feat: ...`、バグ修正には `fix: ...` を用い、範囲の広い変更は `refactor:` または `perf:` を検討してください。プルリクエストでは概要、動機、テスト結果、影響範囲を箇条書きで示し、関連 Issue を `Closes #123` 形式でリンクします。スクリーンショットやログがある場合は添付し、レビュアーが再現できる `Steps to verify` を明記することでレビュー時間を短縮できます。

## セキュリティと設定のヒント
AWS 認証情報は `~/.aws/credentials` または環境変数から読み込まれるため、`.env` やソースコードに平文で保存しないでください。Bigtable emulator はローカル認証を要求しませんが、本番設定では ADC（Application Default Credentials）を前提とした環境変数・サービスアカウント管理を徹底します。OpenTelemetry Collector のイメージは `OTELCOL_IMG` 環境変数で変更でき、ローカル検証では `docker compose -f tools/otel/docker-compose.yml up` を活用すると通信を隔離できます。CI や Renovate が生成するブランチと衝突させないよう、手動アップデート時は先に Issue を立てて計画を共有しましょう。

## ワークフロー補足
継続的な検証には cargo watch や justfile などのローカル自動化ツールが役立つため、開発中は `cargo watch -x "test -p lib"` を常時走らせて回帰を早期に検知してください。テストログやメトリクスは `docs/` 内のテンプレートに沿って整理し、レビューコメントと併せて共有するとチーム全体のフィードバックループが短縮されます。
