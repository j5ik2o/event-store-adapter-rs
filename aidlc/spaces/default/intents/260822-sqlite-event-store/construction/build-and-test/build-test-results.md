# ビルド・テスト実行結果 — build-and-test (build-test-results)

実行日: 2026-08-23（ローカル、macOS / Darwin 25.5.0）。`build-instructions.md` と各ユニットの `unit-test-instructions.md`（重複コマンドは1回に集約）を実行した実測結果。

## ビルド結果 — 全成功

| コマンド | 結果 |
|---|---|
| `cargo build -p event-store-adapter-rs --no-default-features --features dynamodb` | 成功 |
| `cargo build -p event-store-adapter-rs --no-default-features --features bigtable` | 成功 |
| `cargo build -p event-store-adapter-rs --all-features` | 成功 |
| `cargo build -p example-user-account-sqlite` | 成功 |

## 検証コマンド — 全成功

| コマンド | 結果 |
|---|---|
| `cargo +nightly fmt -- --check` | クリーン |
| `cargo clippy --workspace --all-targets -- -D warnings` | 警告0件 |
| `cargo clippy -p example-user-account-sqlite --all-targets -- -D warnings` | 警告0件 |
| `cargo deny check advisories licenses` | エラー0（理由付きignore 4件のRUSTSECは受容済み） |

## テスト結果 — 全成功（失敗0・スキップ0）

| 構成/フィルタ | 結果 |
|---|---|
| featureマトリクス `--no-default-features` | 13 passed / 0 failed |
| featureマトリクス `--features sqlite` | 22 passed / 0 failed |
| featureマトリクス `--features sqlite-system` | 22 passed / 0 failed |
| U1フィルタ `test_optimistic_lock_message`（--all-features） | 3 passed |
| U1フィルタ `test_event_store_on_memory`（--all-features） | 5 passed |
| U2フィルタ `test_event_store_on_sqlite`（--features sqlite） | 9 passed |
| 全スイート `--all-features`（DynamoDB/Bigtable含む — Docker使用） | 24 passed / 0 failed |
| example実行 `cargo run -p example-user-account-sqlite` | 成功（作成→リネーム→リプレイ、version 1→2遷移をログで確認） |

## 静的検査 — 全成功

| 検査 | 結果 |
|---|---|
| `grep -rn "unsafe impl" lib/src/` | 0件（手書きunsafe不在） |
| 機密リテラルgrep（README英日・CHANGELOG・docs・examples・lib/src） | 0件 |
| `cargo tree --no-default-features` に aws/tonic/googleapis | 混入なし |
| `cargo tree --features sqlite` に hashlink | 混入なし（rusqlite `default-features = false` の効果） |

## カバレッジ

計測しない（チーム確定Q3）。品質判定は上記の全緑＋クロスユニットトレーサビリティ（`cross-unit-traceability.md` — PASS）で行う。

## 失敗詳細

なし（失敗0件のため、失敗エスカレーションラダー・Loop-Back Logの発動なし）。
