# NFR要件 質問 — u1-backend-features

> U1（feature分割・エラー型中立化・Memory準拠化）のNFR要件工程の確認質問。
> NFRターゲット・技術選定の大半は上流（要件定義書 NFR-1〜NFR-5、契約C-3、
> チームプラクティス確定事項）で決定済みのため、本工程で裁量が生じた
> 判断のみを確認する。

## Q1: U1のNFR成果物構成

U1はライブラリユニットのため、性能・スケーラビリティ・信頼性・可観測性の各要件書は対象外とし、成果物をセキュリティ要件（security-requirements.md）・技術スタック決定（tech-stack-decisions.md）・トレーサビリティ（traceability.json）の3点としました。

A. この構成で確定する
B. 対象外とした要件書のうち必要なものを追加作成する（どれかを指定してください）
X. Other (please specify)

[Answer]: A. この構成で確定する

## Q2: NFR-5（テスト実行環境）の扱い

上流NFR-5「SQLite関連テストはDocker不要・決定的・並列安全」は、U1がSQLiteテストを導入しないため本ユニットでは N/A（U2のNFR要件工程で導出）とし、既存テストの緑維持は機能設計のルールBR1.9が担う整理としました。

A. N/A（U2へ委譲）で確定する
B. U1スコープの派生要件として明文化する
X. Other (please specify)

[Answer]: A. N/A（U2へ委譲）で確定する

## Q3: 互換性・ビルド系派生要件の置き場

互換性（NFR-1.1/NFR-1.2）・ビルド互換（NFR-3.1)の派生要件は、U1に専用の要件書がないため tech-stack-decisions.md 内の「派生NFR要件」節に置きました。

A. この置き場で確定する
B. 別ファイルに分離する（ファイル名を指定してください）
X. Other (please specify)

[Answer]: A. この置き場で確定する

## Consolidated Summary Confirmation

Does this all look correct before I generate the artifact?

- Looks correct
- Request changes

[Answer]: Looks correct
