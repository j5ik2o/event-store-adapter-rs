# 技術スタック決定 — u4-docs (tech-stack-decisions)

要件定義書（`../../../inception/requirements-analysis/requirements.md` FR-6.1〜FR-6.4）・契約（`../../../inception/contract-design/contract-summary.md` C-3/C-4）・技術スタック台帳（`aidlc/spaces/default/codekb/sqlite/technology-stack.md`）に基づく、U4（ドキュメント）のツール・構成決定。

## 決定一覧

| # | 決定 | 根拠 |
|---|---|---|
| D1 | 新規ツール・依存は追加しない。文書は既存のMarkdown（英/日ペア構成）を踏襲する | Q1=A確定／既存リポジトリ慣行（README.md / README.ja.md、docs/DATABASE_SCHEMA.md / DATABASE_SCHEMA.ja.md） |
| D2 | SQLite利用例は `examples/` 配下に既存 `examples/user-account` と同型のexampleクレート構成で追加する（feature指定は `sqlite` — Docker/クラウド接続なしで実行可能） | FR-6.1／AC4.1.2／既存examples慣行 |
| D3 | 文書の記載内容の正はU1〜U3の確定成果物とワークスペースの実装（実feature名・実API・実スキーマDDL）とし、文書側で新しい仕様を発明しない | NFR-1.4／FR-6.3（情報提供） |
| D4 | CHANGELOGは既存形式に従い、破壊的変更（デフォルトfeature廃止・エラー型変更）と新機能（sqlite/sqlite-system feature）を該当リリース項へ記載する | FR-6.4／C-1（semverメジャー扱い） |

## 適用外の説明

- 文書生成ツール（mdbook等）・リンクチェッカーの導入: スコープ外（既存リポジトリに存在しない — 必要ならバックログ）
- examplesのCIビルド検証: 既知の欠落としてバックログ（team.md確定）

## Assumptions & Open Questions

None.
