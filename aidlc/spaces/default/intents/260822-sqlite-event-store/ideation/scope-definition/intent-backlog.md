# インテントバックログ（プロトユニット）: SQLite対応EventStoreとバックエンドfeature分割

スコープ定義書（`scope-document.md`）のIN項目を、実装単位の候補（プロトユニット）として優先順に整理する。優先順位はリスク先行の方針（Q4）とfeature分割先行の進め方（Q2）に基づく。前提はインテントステートメント（`../intent-capture/intent-statement.md`、intent-statement）、実現性評価（`../feasibility/feasibility-assessment.md`、feasibility-assessment）、制約レジスタ（`../feasibility/constraint-register.md`、constraint-register）。

## バックログ（優先順）

| # | プロトユニット | 内容 | MoSCoW | 依存 | 主なリスク対応 |
|---|---|---|---|---|---|
| PU1 | バックエンドfeature分割 | 既存 DynamoDB / Bigtable を `dynamodb` / `bigtable` feature に分離（Memory常時有効・デフォルトなし）。CIのfeatureマトリクス追加を含む | Must（CIマトリクスはShould） | なし | R4（破壊的変更）を早期に形にし移行手順を確定 |
| PU2 | SQLiteバックエンド実装 | `sqlite` feature 配下に StorageBackend 実装を追加。スキーマ自動作成・楽観的ロック・スナップショット完全互換。リスクの高い楽観的ロック／ドライバ検証から着手 | Must | PU1 | R1/R5（単一ライタ・楽観的ロック）、A1/A2（ドライバ・トレイト適合） |
| PU3 | バンドル/システム両対応feature | SQLiteのバンドル版／システムライブラリ版を選択できるfeature構成 | Should | PU2 | R3（ビルド環境依存） |
| PU4 | ドキュメント整備 | examples追加／README（英/日）のfeature説明・移行手順／DATABASE_SCHEMA.mdへのSQLiteスキーマ記載／CHANGELOGの破壊的変更記載 | Must（examplesはShould相当だがQ1でIN確定） | PU1〜PU3 | R4（利用者影響）の緩和 |
| PU5 | スナップショット保持ポリシーのSQLite対応 | 保持数・TTL削除のSQLite実装（機能差が残る場合は文書化で可） | Could | PU2 | — |

## 価値ストリーム（capability → 顧客成果）

- PU1（feature分割）→ 利用者は必要なバックエンドだけを依存に含められる（ビルド軽量化・明示選択）
- PU2（SQLite実装）→ CLIツール等のクラウド非依存ユースケースが実現する（インテントの中心課題の解決）
- PU3（両対応feature）→ ビルド環境の異なる利用者への到達範囲が広がる
- PU4（ドキュメント）→ 破壊的変更の移行コストが下がり、新機能の発見性が上がる

## 補足

- PU1とPU2の分割は、feasibility-assessment の評価（feature分割は標準機構で低リスク、SQLite実装に不確実性が集中）に基づき、リスクの局在化を狙ったもの。
- 各プロトユニットの最終的なユニット分解と実装順序は、後続のインセプション工程（ユニット生成・デリバリ計画）で確定する。

## Assumptions & Open Questions

None.
