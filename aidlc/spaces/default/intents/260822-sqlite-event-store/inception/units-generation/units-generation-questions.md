# ユニット分割 質問票 (units-generation-questions)

前提: コンポーネントカタログ（`../domain-design/components.md`）とADR（`../domain-design/decisions.md`）、要件定義書（`../requirements-analysis/requirements.md`）、ストーリー（`../user-stories/stories.md`）。境界戦略は「変更のまとまり（基盤リファクタ／新バックエンド／CI／ドキュメント）」で分割し、デプロイモデルは単一クレート（library）で確定しているため、確認は粒度のみ。

## Q1. ユニットの粒度はどちらにしますか？

**案A（粗め・4ユニット）— 推奨**: ソロ開発では構築フェーズの工程がユニットごとに繰り返されるため、少ないユニットで進める方が手数が少ない。
- u1-backend-features（library・M）: feature 3分割＋エラー型中立化＋未使用依存削除＋Memory準拠化（US2.1 / US2.2 / US3.1）
- u2-sqlite-backend（library・L）: SQLite本体＋自動作成＋楽観的ロック＋`:memory:`＋保持ポリシー＋バンドル/システム両対応＋同等/競合テスト（US1.1〜US1.4 / US2.3 / US3.2）
- u3-ci-quality（packaging・S）: featureマトリクス・clippy・依存監査のCI追加（US3.3）
- u4-docs（packaging・S）: examples／README／DATABASE_SCHEMA／CHANGELOG（US4.1 / US4.2）

**案B（細かめ・6ユニット）**: Memory準拠化と保持ポリシー＋両対応featureを独立ユニットに分離（進捗の可視性・並行性は上がるが、ユニットごとの工程数が増える）

A. 案A（粗め・4ユニット）で進める（推奨）
B. 案B（細かめ・6ユニット）で進める
C. 見直したい（Xで指定してください）
X. Other (please specify)

[Answer]: A. 案A（粗め・4ユニット）で進める

## Consolidated Summary Confirmation

分割計画（4ユニット・依存DAG・kind割当）を提示し、アーティファクト生成前の確認を行う。

Does this all look correct before I generate the artifact?

- Looks correct
- Request changes

[Answer]: Looks correct