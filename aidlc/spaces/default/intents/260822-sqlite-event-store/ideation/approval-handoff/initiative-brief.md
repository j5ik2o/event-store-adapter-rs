# イニシアチブブリーフ: SQLite対応EventStoreとバックエンドfeature分割

構想フェーズ全成果物の統合サマリー。詳細は各成果物を参照: インテントステートメント（`../intent-capture/intent-statement.md`、intent-statement）、ステークホルダーマップ（`../intent-capture/stakeholder-map.md`、stakeholder-map）、競合分析（`../market-research/competitive-analysis.md`、competitive-analysis）、実現性評価（`../feasibility/feasibility-assessment.md`、feasibility-assessment）、制約レジスタ（`../feasibility/constraint-register.md`、constraint-register）、スコープ定義書（`../scope-definition/scope-document.md`、scope-document）、インテントバックログ（`../scope-definition/intent-backlog.md`、intent-backlog）。

## インテントと問題

event-store-adapter-rs に SQLite 対応の EventStore 実装を追加し、バックエンド選択を cargo feature 化する（`dynamodb` / `bigtable` / `sqlite` の3分割、Memory常時有効、デフォルトなし）。中心課題は **CLIツールでのクラウド非依存利用**。主な利用者は crates.io 経由の外部 OSS 利用者（intent-statement）。

## 市場面の裏付け

競合比較は行わない方針（自エコシステム向け）。差別化は「アクターモデル非前提のCQRS/ES」。Rust製CLI/デスクトップツール増加を追い風とする。build-vs-buy は自作一択（competitive-analysis / market-trends / build-vs-buy 参照）。

## 実現性とリスクのハイライト

- **総合判定: 実現可能（HIGH confidence）**。既存3バックエンドの同一トレイト共存実績が根拠（feasibility-assessment）
- 主要リスク: 単一ライタ特性（受容＋テスト検証）／CIマトリクス増（代表組み合わせで軽減）／破壊的変更の利用者影響（CHANGELOG明示で受容）／楽観的ロック競合（同等テストで軽減）— 全件、承認Q&Aで同意済み
- 主要制約: ドライバ1クレートのみ・ORM不可／バンドル・システム両対応feature／既存APIシグネチャ不変更（constraint-register）

## スコープ境界

- **IN**: SQLite本体（完全互換・自動テーブル作成）／feature 3分割／同等テスト／CIマトリクス／両対応feature／examples・README・DATABASE_SCHEMA.md・CHANGELOG
- **OUT**: Cloud Spanner対応／既存クレート流用／DDL提供方式／運用工程／既存API変更
- **進め方**: feature分割先行 → SQLiteをリスク先行で実装（scope-document / intent-backlog: PU1〜PU5）

## 体制

ソロOSSプロジェクト。意思決定者はメンテナのみ、実装はAI主導のワークフローで進行（チーム編成工程は該当なしのためスキップ）。UI非保有のためモックアップ工程もスキップ。

## Go/No-Go 推奨

**Go を推奨。** スコープ・実現性・リスク対応が揃い、承認Q&Aでリスク受容と積み残しなしが確認された。次フェーズ（要件定義以降）へ移行する。

## Assumptions & Open Questions

None.
