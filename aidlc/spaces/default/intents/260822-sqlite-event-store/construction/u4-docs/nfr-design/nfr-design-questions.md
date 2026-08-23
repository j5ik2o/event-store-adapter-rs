# NFR設計 質問 — u4-docs

> U4（ドキュメント）のNFR設計工程の質問。品質要件（正確性・文書衛生）は
> NFR要件工程で確定済み。文書ごとの記載割り当ての確認のみを行う。

## Q1: 開示事項・記載内容の文書割り当て

NFR-1.4/1.5/4.12を実現する記載割り当てを次のとおりとします:

- **README（英/日）**: feature構成の説明（dynamodb/bigtable/sqlite/sqlite-system・default廃止）、移行手順（Cargo.toml before/after・エラー型新旧対応表）、**サポート境界**（同一ファイルDB複数同時オープン非サポート）、**併用時bundled優先**の注記、SQLiteの最小利用例、crate説明の陳腐化解消（TD-10）
- **docs/DATABASE_SCHEMA.md（英/日）**: SQLiteのjournal/snapshotスキーマ（情報提供 — 作成はライブラリ自動作成の明記）
- **CHANGELOG**: 破壊的変更（デフォルトfeature廃止・エラー型変更）と新機能（sqlite/sqlite-system・EventStoreForSqlite）
- **examples/**: SQLite利用例（クラウド接続なし・ファイルまたは:memory:）

A. この割り当てで確定する
B. 割り当てを変更する（内容を指定してください）
X. Other (please specify)

[Answer]: A. この割り当てで確定する

## Consolidated Summary Confirmation

Does this all look correct before I generate the artifact?

- Looks correct
- Request changes

[Answer]: Looks correct
