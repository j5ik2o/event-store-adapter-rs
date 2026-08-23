# インフラ設計 質問 — u2-sqlite-backend

> U2はライブラリユニットのため、本工程の成果物はCI/CDパイプライン設計
> （cicd-pipeline.md）とトレーサビリティのみ。リリースフロー維持・CI恒久
> 強化のU3責務は確定済みのため、U2固有の残余のみを確認する。

## Q1: U2マージ時点のCIの扱い

U1で `ci.yml` の test-lib は `--all-features` 化済みのため、U2がマージされると `sqlite` featureのテストは既存CIで自動的に実行されます（`--all-features` に sqlite が含まれる）。sqlite単独構成・sqlite-system構成のマトリクス検証はU3の責務です。

A. U2ではCIに一切触れない — 既存の `--all-features` テストでsqliteテストのCI実行がカバーされ、単独構成・system構成のマトリクス化はU3が担う
B. U2で sqlite 単独ビルドのジョブを先行追加する（U3のマトリクス整備を待たずCI検証を厚くする）
X. Other (please specify)

[Answer]: A. U2ではCIに一切触れない

## Consolidated Summary Confirmation

Does this all look correct before I generate the artifact?

- Looks correct
- Request changes

[Answer]: Looks correct
