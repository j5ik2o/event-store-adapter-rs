# ユーザーストーリー計画 質問票 (user-stories-questions)

要件定義書（`../requirements-analysis/requirements.md`）のFR 22項目・NFR 5件をストーリー化する計画。ペルソナ・分割方針の2点のみ確認する。

## 計画案

- **ペルソナ案（2名）**:
  1. 「CLIツール開発者のケント」— Rust製CLIツールを作る外部開発者。クラウド不要のイベントソーシング永続化が欲しい（新規SQLite利用の主役）
  2. 「既存利用者のアリス」— 既にDynamoDB/Bigtableバックエンドでこのクレートを使っている外部開発者。バージョンアップ時の移行が関心事（破壊的変更の影響を受ける）
- **ストーリー形式**: 標準形式（As a / I want / so that）＋ INVEST 準拠＋ Given/When/Then のAC（`US{group}.{seq}` / `AC{g}.{s}.{n}` の安定ID付き）
- **優先度**: 要件のMoSCoWを継承
- **分割方針案**: 機能領域別（FRグループに対応: SQLite利用／feature選択ビルド／移行／品質保証・CI／ドキュメント）

## Q1. ペルソナ案（2名: 新規SQLite利用の「CLIツール開発者」＋破壊的変更の影響を受ける「既存利用者」）で進めてよいですか？

A. この2名で進める
B. 1名に絞る — CLIツール開発者のみ（移行はストーリー化せず要件のまま扱う）
C. 追加したいペルソナがある（Xで指定してください）
X. Other (please specify)

[Answer]: B. 1名に絞る — CLIツール開発者のみ（移行はストーリー化せず要件のまま扱う。traceability では移行系FRは要件直接カバーとして記録）
## Q2. ストーリーの分割方針は機能領域別（SQLite利用／featureビルド／移行／品質・CI／ドキュメント）でよいですか？

A. 機能領域別で進める（FRグループとの対応が明確でトレーサビリティが素直）
B. ペルソナ別に再構成する（ケントの旅程／アリスの旅程）
C. 提案を見直したい（Xで指定してください）
X. Other (please specify)

[Answer]: A. 機能領域別で進める（FRグループとの対応が明確でトレーサビリティが素直）

## Consolidated Summary Confirmation

全回答の統合サマリー（Q1〜Q2）を提示し、ストーリー生成前の確認を行う。

Does this all look correct before I generate the artifact?

- Looks correct
- Request changes

[Answer]: Looks correct