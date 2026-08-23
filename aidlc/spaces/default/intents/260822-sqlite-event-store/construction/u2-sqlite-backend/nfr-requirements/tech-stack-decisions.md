# 技術スタック決定 — u2-sqlite-backend (tech-stack-decisions)

機能仕様（`../functional-design/functional-spec.md`）・ルール（`../functional-design/rules.md` BR2.9/BR2.10）・要件定義書（`../../../inception/requirements-analysis/requirements.md` NFR-1/NFR-2/NFR-3）・契約（`../../../inception/contract-design/contract-summary.md` C-3）・技術スタック台帳（`aidlc/spaces/default/codekb/sqlite/technology-stack.md`）に基づく、U2（SQLiteバックエンド本体）の技術選定と根拠。

## 決定一覧

| # | 決定 | 根拠 |
|---|---|---|
| D1 | SQLiteドライバは **rusqlite 0.40系**（追加時点の最新安定版 0.40.2）。workspace の `[workspace.dependencies]` に **`rusqlite = { version = "0.40.2", default-features = false }`** として登録し、既存依存と同じく Renovate 自動更新に追随する。**`default-features = false` は必須** — rusqlite のデフォルトfeature `cache` は `hashlink`（ターゲット非限定のランタイム依存）を有効化するため、既定のまま追加すると BR2.10／NFR-2.3（rusqlite 1クレートのみ）に違反する。必要なfeature（bundled等）はD2のfeature定義側でのみ明示的に有効化する | Q1=A確定／NFR-2.3・NFR-2.4／ADR-002（ドライバ1クレートのみ） |
| D2 | feature実装: `sqlite = ["dep:rusqlite", "rusqlite/bundled"]`（単体で自己完結 — バンドル既定）、`sqlite-system = ["dep:rusqlite"]`（システムSQLiteへリンク）。バックエンドコードのcfgゲートは `any(feature = "sqlite", feature = "sqlite-system")` とする。rusqlite側のfeatureはここに列挙したもの以外を有効化しない（D1の `default-features = false` が前提。`prepare_cached` 等 `cache` feature依存のAPIは使用しない） | C-3（bundled既定・system切替）／FR-2.5／BR2.9 |
| D3 | **C-3からの逸脱の明示**: cargoのfeatureは加算的で「併用による切替（bundled解除）」は実現不能のため、システムリンクは **`sqlite-system` 単独指定**を正規の使い方とする。`sqlite` と `sqlite-system` を併用した場合は libsqlite3-sys の優先規則により bundled が勝つ（この帰結はドキュメント〔U4〕に明記する）。feature名はC-3のまま維持し、意味論のみこの形で確定する | cargo featureの加算的解決（技術的制約）／C-3所有権（sqlite系はU2所有） |
| D4 | 非同期対応は追加ランタイムなし — `Arc<Mutex<Connection>>` 下の同期実行（tokioは引き続きdev依存のみ） | 機能設計Q1=A確定／BR2.6／NFR-2.3 |
| D5 | スキーマ・トランザクションはrusqliteの素のAPI（`execute_batch` / トランザクションAPI / prepared statement）で実装し、マイグレーションフレームワーク・ORMは導入しない | NFR-2（依存最小）／BR2.1（自動作成は冪等なCREATE IF NOT EXISTS相当） |
| D6 | テストの一時DBは標準ライブラリの一時ディレクトリ＋一意名（既存dev依存のULID生成を利用）で作り、新規dev依存（tempfile等）を追加しない | NFR-5.1／依存最小方針 |
| D7 | ツールチェーンは現状維持（Rust edition 2021・rustfmt nightly・MSRV宣言なし）。clippy/監査のCI恒久化はU3 | NFR-3.2／U1決定D6の踏襲 |

## 派生NFR要件（互換性・ビルド）

- **NFR-1.3（公開面の追加的変更）**: `EventStoreForSqlite` は新規公開型であり、既存公開トレイト（`EventStore` / `AggregateId` / `Event` / `Aggregate`）のシグネチャを変更しないこと（BR2.11 — 既存テストのコンパイル・実行で検証）
- **NFR-3.2（ビルド互換）**: `--features sqlite` 単独・`--features sqlite-system` 単独・全featureの各構成で現行の安定版Rust（edition 2021）でビルドが通ること（検証コマンドはU1のfeatureマトリクスにsqlite系2構成を加えたもの。CIマトリクス化はU3）

## 適用外の説明

- clippy / cargo-audit（または cargo-deny）のCI組み込み: U3の責務（FR-5.2/FR-5.3）
- ドキュメント（スキーマ記載・feature説明・移行手順）: U4の責務（FR-6.x — D3の併用時挙動の明記を含む）

## Assumptions & Open Questions

None.
