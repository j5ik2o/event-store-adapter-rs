# インフラ設計 質問 — u1-backend-features

> U1はライブラリユニットのため、本工程の成果物はCI/CDパイプライン設計
> （cicd-pipeline.md）とトレーサビリティのみ。リリースフロー（マージ→自動
> バージョンアップ→自動タグ→crates.io自動公開）の維持とCI恒久強化の
> U3責務は上流で確定済みのため、U1固有の残余のみを確認する。

## Q1: U1マージ時点のCI継続性

U1で `default = []`（デフォルトfeatureなし）になると、現行 `ci.yml` のテスト実行（feature指定なしの `cargo test`）ではDynamoDB/Bigtableのテストがfeatureゲートによりコンパイル対象外になり、U3のCIマトリクス整備までCI上で実行されなくなります。`main` へのマージは自動リリースに直結するため、この空白期間の扱いを確認します。

A. U1で `ci.yml` のテスト実行を `--all-features` へ最小修正する — 既存テストのCI実行を切らさない（BR1.9の検証をCIで維持）。マトリクス化・clippy・監査の恒久整備はU3のまま
B. U1ではCIに一切触れない — 空白期間はローカル検証（cargo test --all-features）で代替し、U3のマトリクス整備で回復する
X. Other (please specify)

[Answer]: A. U1で `ci.yml` のテスト実行を `--all-features` へ最小修正する

## Consolidated Summary Confirmation

Does this all look correct before I generate the artifact?

- Looks correct
- Request changes

[Answer]: Looks correct
