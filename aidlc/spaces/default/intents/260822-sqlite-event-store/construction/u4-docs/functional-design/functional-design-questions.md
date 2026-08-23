# 機能設計 質問 — u4-docs

> U4（ドキュメント）はpackaging種別のユニット（`unit-of-work.md` U4、
> `unit-of-work-dependency.md` kind: packaging）。機能設計の成果物（エンティティ・
> ビジネスルール・機能仕様）はいずれもキンド適用で対象外のため、本工程は
> 適用範囲の確認のみを行う。

## Q1: U4における機能設計の適用範囲

U4の実体は examples／README（英/日）／docs/DATABASE_SCHEMA.md（英/日）／CHANGELOG の整備で、ドメインエンティティ・ビジネスルール・振る舞い仕様を持ちません。記載内容の設計（何をどの文書に書くか）はU1〜U3の確定成果物（feature構成・スキーマ・サポート境界・破壊的変更）から導出し、コード生成工程の計画で確定します。

A. この整理で確定する — U4の機能設計は成果物なし（キンド適用）とし、文書構成の判断はコード生成工程の計画へ集約する
B. U4でも機能設計の成果物を作成する（内容を指定してください）
X. Other (please specify)

[Answer]: A. この整理で確定する — U4の機能設計は成果物なし（キンド適用）とし、文書構成の判断はコード生成工程の計画へ集約する

## Consolidated Summary Confirmation

Does this all look correct before I generate the artifact?

- Looks correct
- Request changes

[Answer]: Looks correct
