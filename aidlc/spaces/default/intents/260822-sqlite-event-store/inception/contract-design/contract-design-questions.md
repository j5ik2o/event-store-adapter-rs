# 契約設計 質問票 (contract-design-questions)

前提: ユニット定義（`../units-generation/unit-of-work.md`）と依存DAG（`../units-generation/unit-of-work-dependency.md`）の統合点、コンポーネントカタログ（`../domain-design/components.md`）のエンティティ形状、要件定義書（`../requirements-analysis/requirements.md`）。境界は4つ（公開API／U1↔U2の内部抽象／U2↔U3のfeature名／U2↔U4のスキーマ）。バージョニングは確定済み（Conventional Commits＋semver自動判定、破壊的変更はCHANGELOG明示）。未確定の契約点2つのみ確認する。

## Q1. SQLiteのリンク方式を選ぶ cargo feature の命名はどうしますか？（利用者が書く名前になる公開契約です）

A. `sqlite`（単体でバンドル既定）＋ `sqlite-system`（システムSQLiteへ切替） — 迷ったら動く既定。`--features sqlite` だけで自己完結（推奨）
B. `sqlite-bundled` / `sqlite-system` の明示2択 ＋ `sqlite` は共通部のみ — 明示的だが利用者は必ず2つ指定する手間
C. `sqlite`（共通）＋ `bundled` フラグはrusqliteのfeatureをそのまま転送（`sqlite-bundled` 転送名なし）
D. 提案がほしい（Not yet defined）
X. Other (please specify)

[Answer]: A. `sqlite`（単体でバンドル既定）＋ `sqlite-system`（システムSQLiteへ切替）
## Q2. `EventStoreForSqlite` のコンストラクタ（公開APIの入口）の形はどうしますか？

A. `new(path)` ＋ `new_in_memory()` の2コンストラクタ — ファイル/`:memory:` の使い分けが型で明確（推奨）
B. 接続指定文字列1本の `new(conn_str)` — `":memory:"` を文字列で渡す（SQLite慣習に近いが誤指定しやすい）
C. 機能設計に委ねる（ここでは決めない）
X. Other (please specify)

[Answer]: A. `new(path)` ＋ `new_in_memory()` の2コンストラクタ

## Consolidated Summary Confirmation

全回答の統合サマリー（Q1〜Q2）を提示し、契約サマリー生成前の確認を行う。

Does this all look correct before I generate the artifact?

- Looks correct
- Request changes

[Answer]: Looks correct