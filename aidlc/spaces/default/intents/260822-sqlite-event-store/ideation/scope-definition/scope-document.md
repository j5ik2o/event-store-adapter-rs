# スコープ定義書: SQLite対応EventStoreとバックエンドfeature分割

本書はインテントステートメント（`../intent-capture/intent-statement.md`、intent-statement）を出発点に、実現性評価（`../feasibility/feasibility-assessment.md`、feasibility-assessment）と制約レジスタ（`../feasibility/constraint-register.md`、constraint-register）の制約下でスコープ境界を確定する（Q&A: `scope-definition-questions.md`）。

## スコープ境界（IN / OUT）

### IN（今回のリリースに含む）

| 項目 | 分類 | 出所 |
|---|---|---|
| SQLiteバックエンド本体（イベント永続化・スナップショット・楽観的ロックの完全互換） | 機能 | intent-statement / Q1前提 |
| feature 3分割: `dynamodb` / `bigtable` / `sqlite`（Memory常時有効・デフォルトなし） | 機能 | intent-statement / Q1前提 |
| SQLiteスキーマの自動テーブル作成（初回接続時） | 機能 | Q5 |
| バンドル/システムSQLiteの両対応feature | 機能 | constraint-register C2 |
| 既存バックエンドと同等のSQLiteテスト | 品質 | intent-statement 成功指標 |
| CI featureマトリクス（GitHub Actions） | 品質 | Q3 / constraint-register C9 |
| examples への SQLite 利用例の追加 | ドキュメント | Q1 |
| README（英/日）へのfeature説明・移行手順の追記 | ドキュメント | Q1 |
| docs/DATABASE_SCHEMA.md へのSQLiteスキーマ記載 | ドキュメント | Q1 |
| CHANGELOG への破壊的変更の記載 | ドキュメント | Q1 / intent-statement リリース方針 |

### OUT（今回は含まない）

| 項目 | 理由 |
|---|---|
| Cloud Spanner 対応 | インテント確定時に対象外と確認済み（intent-statement） |
| 既存イベントソーシングクレートへの依存・流用 | build-vs-buy で自作一択（constraint-register C6） |
| DDL提供によるスキーマ作成の利用者委任 | Q5で自動作成のみを選択（スキーマのドキュメント記載は情報提供としてIN） |
| デプロイ・運用工程（インフラ、監視等） | ライブラリ開発の範囲（intent-statement スコープシグナル） |
| 既存公開API（トレイトシグネチャ）の変更 | constraint-register C3 |

## 優先度（MoSCoW）

| 優先度 | 項目 |
|---|---|
| **Must** | SQLiteバックエンド本体（完全互換）／feature 3分割（デフォルトなし）／既存テストと同等のSQLiteテスト |
| **Should** | バンドル/システム両対応feature／CI featureマトリクス |
| **Could** | スナップショット保持ポリシー（保持数・TTL削除）のSQLite対応（機能差があれば文書化で可） |
| **Won't（今回）** | Cloud Spanner対応／DDL提供方式のマイグレーション |

## 進め方の方針

1. **feature分割を先行**: 既存3バックエンド（DynamoDB / Bigtable / Memory常時有効）のfeature化で基盤を整える（Q2）
2. **SQLiteをその枠組みへ追加**: 実装内部はリスク先行 — 楽観的ロックのSQLite実装・ドライバ検証など不確実性の高い部分から着手（Q4、feasibility-assessment のリスク分析と整合）
3. ドキュメント（examples / README / DATABASE_SCHEMA.md / CHANGELOG）は対応機能の完成に合わせて更新

## Assumptions & Open Questions

None.
