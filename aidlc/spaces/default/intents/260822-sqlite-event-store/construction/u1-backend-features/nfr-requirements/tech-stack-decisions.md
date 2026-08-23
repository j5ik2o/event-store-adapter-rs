# 技術スタック決定 — u1-backend-features (tech-stack-decisions)

機能仕様（`../functional-design/functional-spec.md`）・ルール（`../functional-design/rules.md`）・要件定義書（`../../../inception/requirements-analysis/requirements.md` NFR-1/NFR-2/NFR-3）・契約（`../../../inception/contract-design/contract-summary.md` C-3）・技術スタック台帳（`aidlc/spaces/default/codekb/sqlite/technology-stack.md`）に基づく、U1（feature分割・エラー型中立化・Memory準拠化）の技術選定と根拠。U1は新技術の導入ユニットではなく、既存スタックの再編ユニットであるため、決定は「何を足すか」より「何を隔離・削除・維持するか」に集中する。

## 決定一覧

| # | 決定 | 根拠 |
|---|---|---|
| D1 | U1では新規ランタイム依存を追加しない（rusqlite等のSQLiteドライバ導入はU2の決定事項） | ユニット境界（機能仕様: sqlite featureは「器のみ」）／NFR-2（依存最小） |
| D2 | クラウドSDK依存を `optional = true` 化し、featureに束ねる — `dynamodb` = aws-sdk-dynamodb + aws-config、`bigtable` = tonic + googleapis-tonic-google-bigtable-v2 | FR-2.1/FR-2.2、BR1.3/BR1.4、NFR-2.2（供給網隔離） |
| D3 | feature軸は契約C-3のとおり `dynamodb` / `bigtable` / `sqlite` を定義し、`default = []`（デフォルトfeatureなし — 破壊的変更）。`sqlite-system` はU2所有のためU1では定義しない | 契約C-3（U1=器、U2=sqlite系）／FR-2.2 |
| D4 | 依存削除: aws-http（未使用 — TD-02）、prost（宣言のみ — TD-14）、serial_test（未使用dev依存 — TD-14） | FR-3.2、BR1.5、NFR-2.1（攻撃面縮小） |
| D5 | エラー型は thiserror 2.0 を継続し、`OptimisticLockError` は整形済み `String` コンテキスト保持へ変更（BR1.2 — 契約C-1の「軽量コンテキスト」の機能設計での確定形）。SDK型リーク（TD-01 `TransactionCanceledExceptionWrapper`）を除去し、新規クレートは不要 | FR-2.4、BR1.1/BR1.2 |
| D6 | ツールチェーンは現状維持: Rust edition 2021、MSRV宣言なし（宣言・検証はスコープ外）、rustfmt（nightly、`rustfmt.toml` 準拠）。clippy / 依存監査のCI組み込みはU3の責務でありU1では行わない | NFR-3、FR-5.2/FR-5.3（U3）、team.md Code Style |
| D7 | async トレイトは既存どおり async-trait 0.1 を継続。Memory準拠化で `Send`/`Sync` は自動導出に任せ、手書き `unsafe impl` を書かない | BR1.7、確定慣行（team.md）／project.md Forbidden |

## 派生NFR要件（互換性・ビルド）

キンド適用の結果、本ユニットの専用要件書はセキュリティ（`security-requirements.md` — NFR-2.x/NFR-4.x）と本書のみのため、互換性・ビルド系の詳細要件は本書に置く。

- **NFR-1.1（公開シグネチャ不変更）**: 公開トレイト（`EventStore` / `AggregateId` / `Event` / `Aggregate`）のメソッドシグネチャはU1で変更しないこと。破壊的変更は feature構成・エラー型・モジュール公開範囲に限る（BR1.8 — 既存テストのコンパイル・実行で検証）
- **NFR-1.2（公開API維持）**: `EventStoreForMemory::new()` を含む既存公開コンストラクタ・ビルダー（`with_*`）の呼び出し互換を維持すること（BR1.8）
- **NFR-3.1（ビルド互換）**: feature分割後も現行の安定版Rust（edition 2021）でビルド可能であること。feature未指定／各feature単独／全featureの各組み合わせでコンパイルが通ること（BR1.3/BR1.4 — cargo tree・ビルドで検証。CIマトリクス化はU3）

## 適用外の説明

- SQLiteドライバ選定・リンク方式（バンドル/システム）: U2の決定事項（契約C-3の `sqlite` / `sqlite-system` 実体化を含む）
- clippy / cargo-audit（または cargo-deny）のCI組み込み: U3の責務（FR-5.2/FR-5.3）。U1はローカルでの rustfmt 準拠のみを前提とする

## Assumptions & Open Questions

None.
