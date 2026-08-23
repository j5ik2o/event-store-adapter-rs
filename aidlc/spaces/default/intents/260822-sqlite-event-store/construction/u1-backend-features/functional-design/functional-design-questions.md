# 機能設計 質問票 — u1-backend-features (functional-design-questions)

前提: ユニット定義（`../../../inception/units-generation/unit-of-work.md` U1）、要件（`../../../inception/requirements-analysis/requirements.md` FR-2.x/FR-3.x）、コンポーネント（`../../../inception/domain-design/components.md`）、契約（`../../../inception/contract-design/contract-summary.md` C-1/C-2/C-3）。設計判断はほぼ確定済みのため、残る空白は公開エラー型の内部形状1点のみ。

## Q1. 中立化後の `OptimisticLockError` が保持するコンテキストの形は？（ADR-001は「集約ID・期待バージョン等の文字列コンテキスト」とし、単一文字列か構造体かを未確定のまま残している）

A. 単一の説明文字列 — `OptimisticLockError(String)`。集約ID・期待/実バージョンを整形済みメッセージに埋め込む（最軽量。表示・ログ用途に十分 — 推奨）
B. 構造体 — `OptimisticLockError { aggregate_id: String, expected_version: Option<usize>, actual_version: Option<usize> }`。利用者がプログラム的にフィールドへアクセスできる（再試行ロジックで集約IDを使う場合に便利だが公開APIの表面積が増える）
C. 提案がほしい（Not yet defined）
X. Other (please specify)

[Answer]: A. 単一の説明文字列 — `OptimisticLockError(String)`。集約ID・期待/実バージョンを整形済みメッセージに埋め込む

## Consolidated Summary Confirmation

回答（Q1）の統合サマリーを提示し、u1の設計アーティファクト生成前の確認を行う。

Does this all look correct before I generate the artifact?

- Looks correct
- Request changes

[Answer]: Looks correct