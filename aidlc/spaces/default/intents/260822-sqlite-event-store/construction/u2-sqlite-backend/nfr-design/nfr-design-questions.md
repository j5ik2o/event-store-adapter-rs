# NFR設計 質問 — u2-sqlite-backend

> U2（SQLiteバックエンド本体）のNFR設計工程の質問。同期実行方式（Arc<Mutex<Connection>>）・
> エラー写像・書き込み分散キー・feature設計は上流Q&Aで確定済み。
> 実装を拘束する残余の設計分岐のみを確認する。

## Q1: SQLite接続の信頼性設定（PRAGMA）

同一ファイルDBを複数のストアインスタンス（または複数プロセスのCLIツール）が開いた場合、SQLiteの既定では書き込みロック競合が即時 `SQLITE_BUSY` エラーになります（本ライブラリでは `IOError` へ写像）。接続確立時の信頼性設定の方針が未確定です。

A. `busy_timeout` のみ設定（例: 5秒） — 一時的なクロスプロセス/クロスインスタンスのロック競合を待機で吸収する。ジャーナルモードはSQLite既定のまま（DBファイル形式に影響なし）
B. 何も設定しない — SQLite/rusqliteの既定のまま。競合は即時エラーとして利用者の再試行責務に委ねる（最もシンプル）
C. WAL＋busy_timeout — 読み書き並行性が最も高いが、`-wal`/`-shm` 付随ファイルが作られDBファイル形式の期待が変わる
X. Other (please specify)

[Answer]: X. Other — ユーザー回答（verbatim）: 「一つのDBファイルを複数で開く方悪いです。サポート外の使い方。CLI多重起動防止などはアプリケーション側の責任です。」よって: 同一ファイルDBの複数ストアインスタンス／複数プロセスからの同時オープンは**サポート外**と設計上明記し、PRAGMA調整（busy_timeout / WAL）は行わない（実質B＋サポート境界の明文化）。ロック競合（SQLITE_BUSY）は既定どおり即時エラーとして `IOError` 系へ写像。多重起動防止はアプリケーション側の責務とし、U4のドキュメントにサポート境界を明記する

## Consolidated Summary Confirmation

Does this all look correct before I generate the artifact?

- Looks correct
- Request changes

[Answer]: Looks correct
