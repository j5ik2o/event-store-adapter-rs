# 技術スタック — event-store-adapter-rs

## 言語とツールチェーン

| 項目 | 値 | 備考 |
|---|---|---|
| 言語 | Rust | edition 2021 |
| MSRV | **宣言なし** | `rust-version` フィールド未設定。CI でも未検証（TD-11） |
| ワークスペース | cargo workspace | `resolver = "2"`、バージョンはルート `[workspace.dependencies]` に一元化 |
| タスクランナー | cargo-make (`Makefile.toml`) | fmt タスクのみ |
| フォーマッタ | rustfmt (`rustfmt.toml`) | `max_width = 120`, `tab_spaces = 2` |
| リンタ | なし | clippy 未導入（TD-11） |
| 依存更新 | Renovate (`renovate.json`) | 有効 |

## 無条件依存（lib / event-store-adapter-rs）

feature ゲートが一切なく、**すべて無条件依存**である点が feature 分割の主要障害（`dependencies.md` 参照）。

| クレート | バージョン | 用途 |
|---|---|---|
| aws-sdk-dynamodb | 1.23.0 | DynamoDB バックエンド。`types.rs` のエラー型にも型がリーク（TD-01） |
| aws-config | 1.2.1 | AWS 認証・設定ロード |
| aws-http | 0.60.5 | **未使用**。削除可能（TD-02） |
| tonic | 0.14 | gRPC ランタイム（Bigtable 用） |
| googleapis-tonic-google-bigtable-v2 | 0.39.0 | Bigtable v2 API 生成クライアント |
| async-trait | 0.1.80 | トレイトの async メソッド |
| thiserror | 2.0.0 | エラー型導出 |
| serde / serde_json | 1.0 | シリアライズ（JSON デフォルト） |
| chrono | 0.4.38 | 日時・Duration（delete_ttl） |
| tracing | 0.1 | 計装ログ |

## dev 依存および test-utils 依存

| クレート | バージョン | 使用箇所 / 用途 |
|---|---|---|
| tokio | 1.37.0 (full) | async ランタイム（テスト・example） |
| testcontainers | 0.28.0 | LocalStack 2.1.0 / cloud-sdk emulators のコンテナ起動 |
| googleapis-tonic-google-bigtable-admin-v2 | 0.45.0 | Bigtable テーブル作成（admin API、test-utils） |
| once_cell | — | 遅延初期化（test-utils） |
| ulid-generator-rs | — | ULID 生成（test-utils の id_generator） |
| serial_test | 3.1.1 | **宣言のみ未使用**（TD-14） |
| prost | 0.14 | **宣言のみ未参照**（TD-14） |
| anyhow / log | — | test-utils のみ |

## 補助ツール・ローカルインフラ

- `tools/docker-compose.yaml` — LocalStack（DynamoDB）と Bigtable エミュレータのローカル起動。`tools/otel-collector-config.yaml` / `tools/prometheus.yaml` は観測系の補助設定（今回の分析ではスキム）。
- GitHub Actions 4 ワークフロー（`ci.yml` / `lib-release.yml` / `lib-bump-version.yml` / `openai-review.yml`）。詳細は `code-quality-assessment.md`。
- 環境変数 `TEST_TIME_FACTOR` — 統合テストのタイムアウト係数。
