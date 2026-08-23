# 範囲定義 質問票 (scope-definition-questions)

前提: インテントステートメント（`../intent-capture/intent-statement.md`）、実現性評価（`../feasibility/feasibility-assessment.md`）、制約レジスタ（`../feasibility/constraint-register.md`）を参照。feature 3分割（dynamodb/bigtable/sqlite、Memory常時有効、デフォルトなし）、バンドル/システム両対応、既存APIシグネチャ不変更が確定済み。

## Q1. 今回のリリースに含める成果物の境界（in/out）を確定させてください（select all that apply — 選んだものがIN）

SQLiteバックエンド本体と feature 3分割は必須（IN確定）です。付随作業のin/outを確認します。

A. examples への SQLite 利用例の追加
B. README（英/日）のfeature説明・移行手順の追記
C. docs/DATABASE_SCHEMA.md へのSQLiteスキーマ記載
D. CHANGELOG への破壊的変更の記載（リリース方針で確定済みだが成果物として明示）
X. Other (please specify)

[Answer]: A, B, C, D（すべてIN: examples追加／README更新／DATABASE_SCHEMA.md更新／CHANGELOG記載）
## Q2. 「既存3バックエンドのfeature分割」と「SQLiteバックエンド新規実装」の2つの作業の進め方は？

A. feature分割を先に行う（既存構成をfeature化して基盤を整えてから、SQLiteをその枠組みに追加する）
B. SQLiteを先に実装する（現行構成のままSQLiteを追加し、最後にまとめてfeature分割する）
C. 同時に進める（SQLite実装とfeature分割を一体の変更として設計・実装する）
D. 提案がほしい（Not yet defined）
X. Other (please specify)

[Answer]: A. feature分割を先に行う（当初D「提案がほしい」→ 推奨Aを提示しユーザーが確定。SQLite実装内部はリスク先行で着手）
## Q3. 各機能の優先度（MoSCoW）を確認させてください。以下の分類案で合っていますか？

分類案:
- **Must**: SQLiteバックエンド本体（永続化・スナップショット・楽観的ロック完全互換）／feature 3分割（デフォルトなし）／既存テストと同等のSQLiteテスト
- **Should**: バンドル/システム両対応feature／CI featureマトリクス
- **Could**: スナップショット保持ポリシー（保持数・TTL削除）のSQLite対応（既存バックエンドとの機能差があれば文書化で可）

A. この分類案で合っている
B. バンドル/システム両対応は Must に上げたい
C. スナップショット保持ポリシー対応も Must に上げたい（完全互換の一部として必須）
D. 分類を見直したい（Xで指定してください）
X. Other (please specify)

[Answer]: A. この分類案で合っている（Must: SQLite本体/feature 3分割/同等テスト、Should: バンドル・システム両対応/CIマトリクス、Could: スナップショット保持ポリシー）
## Q4. 実装順序の考え方（シーケンス選好）は？

A. リスク先行 — 不確実性の高いもの（SQLiteでの楽観的ロック実装・ドライバ検証）から着手して早期にリスクを潰す
B. 依存先行 — 基盤（feature分割）から順に、依存関係に沿って積み上げる
C. 価値先行 — 使える成果（SQLiteバックエンド）を最短で出すことを優先する
D. 提案がほしい（Not yet defined）
X. Other (please specify)

[Answer]: A. リスク先行 — 楽観的ロック・ドライバ検証など不確実性の高いものから着手
## Q5. マイグレーション（スキーマ準備）のスコープはどこまで含めますか？

テーブルステークス（競合分析）で「テーブル自動作成またはスキーマDDLの提供」が期待事項とされています。

A. 自動テーブル作成のみ — 初回接続時にライブラリがテーブルを作成する
B. DDL提供のみ — スキーマSQLをドキュメント/ファイルで提供し、作成は利用者責務
C. 両方 — 自動作成機能＋DDLドキュメントの両方を提供する
D. まだ決めていない（Not yet defined）
X. Other (please specify)

[Answer]: A. 自動テーブル作成のみ — 初回接続時にライブラリがテーブルを作成する（DATABASE_SCHEMA.mdへのスキーマ記載は情報提供でありQ1-Cとして別途IN）

## Consolidated Summary Confirmation

全回答の統合サマリー（Q1〜Q5）を提示し、アーティファクト生成前の確認を行う。

Does this all look correct before I generate the artifact?

- Looks correct
- Request changes

[Answer]: Looks correct