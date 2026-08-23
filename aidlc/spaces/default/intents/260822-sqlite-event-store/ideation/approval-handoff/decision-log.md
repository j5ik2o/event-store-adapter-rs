# 意思決定ログ（構想フェーズ）: SQLite対応EventStoreとバックエンドfeature分割

構想フェーズの全工程で行われた意思決定の記録。出典は各工程の質問票および成果物（intent-statement / stakeholder-map / competitive-analysis / feasibility-assessment / constraint-register / scope-document / intent-backlog）。

## 決定一覧

| # | 決定 | 決定内容 | 工程 / 出典 |
|---|---|---|---|
| D1 | 中心課題 | CLIツールでのクラウド非依存利用 | インテント把握 Q1（intent-statement） |
| D2 | feature分割対象 | `dynamodb` / `bigtable` / `sqlite` の3分割。Memory常時有効。cloudspanner言及は誤認と確認 | インテント把握 Q2/Q9（intent-statement） |
| D3 | デフォルトfeature | なし — 利用者が必ず明示選択 | インテント把握 Q3（intent-statement） |
| D4 | リリース方針 | 破壊的変更として通常リリース。CHANGELOG記載で利用者に配慮 | インテント把握 Q10（intent-statement / stakeholder-map） |
| D5 | 主な利用者 | 外部OSS利用者（crates.io経由） | インテント把握 Q4（stakeholder-map） |
| D6 | 成功指標 | SQLiteが既存トレイト全機能（永続化・スナップショット・楽観的ロック）で同等テスト合格 | インテント把握 Q5（intent-statement） |
| D7 | 競合方針 | 競合比較は行わない。差別化は「アクターモデル非前提のCQRS/ES」 | 市場調査 Q1/Q2（competitive-analysis） |
| D8 | build-vs-buy | 自作一択（ドライバ利用は許容） | 市場調査 Q5（build-vs-buy） |
| D9 | ドライバ制約 | SQLiteドライバ1クレートのみ。ORM等不可 | 実現性 Q1（constraint-register C1） |
| D10 | ビルド方式 | バンドル/システム両対応をfeatureで提供 | 実現性 Q2（constraint-register C2） |
| D11 | 規制要件 | なし（利用者側の責務） | 実現性 Q3（constraint-register C11） |
| D12 | API互換 | 既存公開トレイトシグネチャは変更しない | 実現性 Q6（constraint-register C3） |
| D13 | in/out境界 | examples・README・DATABASE_SCHEMA.md・CHANGELOGすべてIN | 範囲定義 Q1（scope-document） |
| D14 | 進め方 | feature分割を先行し、SQLiteをその枠組みに追加 | 範囲定義 Q2（scope-document / intent-backlog） |
| D15 | 優先度 | Must: 本体/3分割/同等テスト、Should: 両対応/CIマトリクス、Could: 保持ポリシー | 範囲定義 Q3（scope-document） |
| D16 | 順序方針 | リスク先行（楽観的ロック・ドライバ検証から着手） | 範囲定義 Q4（intent-backlog） |
| D17 | スキーマ準備 | 自動テーブル作成のみ（ドキュメント記載は情報提供として別途IN） | 範囲定義 Q5（scope-document） |
| D18 | リスク受容 | RAID 5リスク・3前提と対応方針に同意 | 承認・引き継ぎ Q1（feasibility-assessment / raid-log） |
| D19 | 工程スキップ | チーム編成（ソロ開発）・ラフモックアップ（UI非保有）をスキップ | 各工程の条件判定 |

## Assumptions & Open Questions

None.
