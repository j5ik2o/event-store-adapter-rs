# 制約レジスタ: SQLite対応EventStoreとバックエンドfeature分割

本レジスタは、インテントステートメント（`../intent-capture/intent-statement.md`、intent-statement）、競合分析（`../market-research/competitive-analysis.md`、competitive-analysis）、市場トレンド（`../market-research/market-trends.md`、market-trends）、Build vs Buy 評価（`../market-research/build-vs-buy.md`、build-vs-buy）、および実現性Q&A（`feasibility-questions.md`）から確定した制約を一覧化する。

## 技術的制約

| ID | 制約 | 出所 |
|---|---|---|
| C1 | SQLiteドライバは1クレートのみ許容。ORM等の大型フレームワーク依存は不可 | Q1回答 |
| C2 | SQLiteのバンドル版／システムライブラリ版を feature で選択可能にする（両対応） | Q2回答 |
| C3 | 既存の公開API（トレイトシグネチャ）は変更しない | Q6回答 |
| C4 | feature 構成は `dynamodb` / `bigtable` / `sqlite` の3分割。Memory は常時有効。デフォルト feature なし | intent-statement |
| C5 | SQLiteバックエンドは既存機能（イベント永続化・スナップショット・楽観的ロック）と完全互換であること | competitive-analysis（テーブルステークス） |
| C6 | 自作実装とする（既存イベントソーシングクレートへの依存・流用は行わない） | build-vs-buy |
| C7 | 現行の安定版Rustでビルド可能であること（MSRVの明示指定はなし、edition 2021） | Q6回答 |

## 組織的制約

| ID | 制約 | 出所 |
|---|---|---|
| C8 | タイムライン制約なし。品質優先 | Q4回答 |
| C9 | CI（GitHub Actions）への feature マトリクス追加を含む変更は自由 | Q5回答 |
| C10 | 意思決定者はメンテナのみ。破壊的変更は通常リリースとし CHANGELOG に記載 | intent-statement |

## 規制的制約

| ID | 制約 | 出所 |
|---|---|---|
| C11 | なし — ライブラリ自体への規制要件は存在しない。格納データのコンプライアンス（PII等の扱い）は利用者側の責務 | Q3回答 |

## 市場要請に由来する品質制約

- CLIツール用途（market-trends のトレンド整合）から、SQLiteバックエンドは外部サービス・常駐プロセスなしで動作しなければならない（単一ファイルDB・`:memory:` モード — competitive-analysis のテーブルステークス）。

## Assumptions & Open Questions

None.
