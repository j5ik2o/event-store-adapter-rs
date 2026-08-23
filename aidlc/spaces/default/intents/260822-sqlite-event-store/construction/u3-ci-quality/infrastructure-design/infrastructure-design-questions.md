# インフラ設計 質問 — u3-ci-quality

> U3（CI品質保証）のインフラ設計工程の質問。ツール・実行タイミング・マトリクス
> 6構成・ライセンス方針は上流で確定済み。NFR設計レビューが指摘した残余
> （Docker依存テストの扱い）のみを確認する。

## Q1: マトリクス構成でのDocker依存テストの扱い

featureマトリクス6構成のうち `dynamodb`／`bigtable`／`全feature` の3構成では、素の `cargo test` を実行すると testcontainers（Docker）依存の統合テストがそのまま実行され、既存 `test-lib` ジョブ（--all-features、Docker込み）と重複します。

A. **ビルド検証のみに留める** — dynamodb／bigtable／全feature の3構成は `cargo build` のみ（コンパイル成立の検証）とし、テスト実行は Docker不要の3構成（未指定／sqlite／sqlite-system）＋既存 `test-lib`（--all-features・Docker込み）に任せる。最も単純で重複ゼロ
B. **skipフィルタで除外して実行** — 3構成でも `cargo test -- --skip test_event_store_on_dynamodb --skip test_event_store_on_bigtable` でDocker不要テストのみ実行する（テスト名依存が増える）
X. Other (please specify)

[Answer]: A. ビルド検証のみに留める（dynamodb／bigtable／全featureは cargo build のみ、テスト実行はDocker不要3構成＋既存test-lib）

## Consolidated Summary Confirmation

Does this all look correct before I generate the artifact?

- Looks correct
- Request changes

[Answer]: Looks correct
