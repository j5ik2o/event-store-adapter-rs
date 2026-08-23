# ビルド手順 — build-and-test (build-instructions)

全ユニット（u1〜u4）のコード生成成果（`construction/*/code-generation/code-summary.md`）に対するビルド手順。OSS Rustクレートのため、ビルド＝cargoによるワークスペースビルドであり、環境変数・外部サービス・クラウド接続は不要。

## 依存インストール

- Rust stable ツールチェーン（edition 2021。MSRV宣言はスコープ外 — team.md確定）
- nightly ツールチェーン（rustfmtのみ — `cargo +nightly fmt`）
- `cargo-deny`（監査コマンド用。CIでは cargo-deny-action@v2 が導入）
- `sqlite-system` feature検証時のみ: システムSQLite（macOSは標準搭載。Linuxは `libsqlite3-dev`）
- Docker（**既存のDynamoDB/Bigtable統合テストのみ** — testcontainers使用。SQLite/Memory/exampleはDocker不要）

## 環境セットアップ

- 環境変数・設定ファイル・ローカルサービスは不要（SQLiteはバンドルまたはシステムライブラリ、テストは `:memory:`／一時ファイル）

## ビルドコマンド（featureマトリクス — CI feature-matrix ジョブと同一）

```bash
# ビルドのみの3構成（Docker依存テストを含む構成）
cargo build -p event-store-adapter-rs --no-default-features --features dynamodb
cargo build -p event-store-adapter-rs --no-default-features --features bigtable
cargo build -p event-store-adapter-rs --all-features

# ビルド＋テスト対象の3構成は統合テスト手順を参照（未指定 / sqlite / sqlite-system）

# example（U4）
cargo build -p example-user-account-sqlite
```

## ビルド検証

```bash
cargo +nightly fmt -- --check                                  # rustfmt.toml 準拠
cargo clippy --workspace --all-targets -- -D warnings          # 警告ゼロ
cargo deny check advisories licenses                           # 依存監査（deny.toml）
```

## トラブルシューティング

- `sqlite-system` でリンクエラー → システムにSQLiteが無い（Linux: `apt install libsqlite3-dev`）
- `sqlite`（bundled）でCコンパイルエラー → Cコンパイラ（Xcode CLT / build-essential）が必要
- DynamoDB/Bigtableテストの失敗 → Docker未起動が典型（SQLite系検証には無関係）
