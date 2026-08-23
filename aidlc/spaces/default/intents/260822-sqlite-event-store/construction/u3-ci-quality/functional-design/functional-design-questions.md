# 機能設計 質問 — u3-ci-quality

> U3（CI品質保証）はpackaging種別のユニット（リポジトリCI設定 — `unit-of-work.md` U3、
> `unit-of-work-dependency.md` kind: packaging）。機能設計の成果物（エンティティ・
> ビジネスルール・機能仕様）はいずれもキンド適用で対象外のため、本工程は
> 適用範囲の確認のみを行う。

## Q1: U3における機能設計の適用範囲

U3の実体は `.github/workflows/ci.yml`（および必要なら新規ワークフロー）の整備で、ドメインエンティティ・ビジネスルール・振る舞い仕様を持ちません。CIマトリクスの構成・ジョブ設計はインフラ設計工程（CI/CDパイプライン設計 — U1/U2からの引き渡し要求仕様が入力）で扱います。

A. この整理で確定する — U3の機能設計は成果物なし（キンド適用）とし、設計判断はインフラ設計工程へ集約する
B. U3でも機能設計の成果物を作成する（内容を指定してください）
X. Other (please specify)

[Answer]: A. この整理で確定する — U3の機能設計は成果物なし（キンド適用）とし、設計判断はインフラ設計工程へ集約する

## Consolidated Summary Confirmation

Does this all look correct before I generate the artifact?

- Looks correct
- Request changes

[Answer]: Looks correct
