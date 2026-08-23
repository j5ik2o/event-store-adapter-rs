# ユニット定義 (unit-of-work)

コンポーネントカタログ（`../domain-design/components.md`）とADR（`../domain-design/decisions.md`）の設計制約の下で、要件定義書（`../requirements-analysis/requirements.md`）とストーリー（`../user-stories/stories.md`）の実装作業を4ユニットに分割する（Q&A `units-generation-questions.md` で粒度確定）。デプロイモデルは単一クレート（library）であり、ユニットは「独立に実装・検証できる変更のまとまり」を表す。

## ユニット一覧

| Unit ID | Directory | 名前 | Kind | 複雑度 | デプロイモデル |
|---|---|---|---|---|---|
| U1 | u1-backend-features | バックエンドfeature分割と基盤整合 | library | M | 単一クレートに埋め込み |
| U2 | u2-sqlite-backend | SQLiteバックエンド | library | L | 単一クレートに埋め込み（feature `sqlite`） |
| U3 | u3-ci-quality | CI品質保証 | packaging | S | リポジトリCI設定 |
| U4 | u4-docs | ドキュメント | packaging | S | リポジトリドキュメント |

## U1: バックエンドfeature分割と基盤整合（u1-backend-features）

- **境界**: `Cargo.toml`（features・依存）、`lib.rs`（cfgゲート・再エクスポート）、`types.rs`（エラー型）、`event_store_for_memory.rs`（準拠化）、既存バックエンドのエラー写像更新
- **責務**: feature `dynamodb` / `bigtable` / `sqlite` の定義とデフォルトなし化（FR-2.1〜2.3）／エラー型の中立化（FR-2.4、ADR-001）／未使用依存の削除（FR-3.2）／Memoryの StorageBackend 準拠化とpanicのErr化（FR-3.1、ADR-005）／`unsafe impl` 除去（FR-3.3）
- **対応コンポーネント**: CoreTypes、StorageAbstraction、MemoryBackend、DynamoDbBackend（写像更新）、BigtableBackend（写像更新）
- **制約・注記**: 公開トレイトシグネチャ不変更（NFR-1）。既存のDynamoDB/Bigtable統合テストが緑のままであること。`sqlite` featureの器（空のfeature宣言）はU1で用意し、実装はU2が埋める

## U2: SQLiteバックエンド（u2-sqlite-backend）

- **境界**: `event_store_for_sqlite.rs` / `event_store_for_sqlite_test.rs`（新規）、`event_store_test_support.rs`（共有シナリオへの搭載）、`Cargo.toml`（rusqlite依存・bundled/system feature）
- **責務**: StorageBackend のSQLite実装＋EventStoreForSqlite公開ファサード（FR-1.1/1.2）／スキーマ自動作成（FR-1.4）／単一トランザクションの楽観的ロックCAS（FR-1.3、ADR-003）／ファイル・`:memory:` 両対応（FR-1.5）／保持ポリシー実装（FR-1.6）／バンドル・システム両対応feature（FR-2.5）／同等テスト・競合パステスト・エラー契約テスト（FR-4.1〜4.3）
- **対応コンポーネント**: SqliteBackend、TestSupport
- **制約・注記**: rusqlite唯一追加（NFR-2、ADR-002）。テストはDocker不要（FR-4.3）。ウォーキングスケルトン（最初の薄い1本）はこのユニットの最小スライス＋U1の必要最小部分で構成する（構成の確定はデリバリ計画）

## U3: CI品質保証（u3-ci-quality）

- **境界**: `.github/workflows/ci.yml`（および必要なら新規ワークフロー）
- **責務**: featureマトリクス（未指定／各feature単独／全feature）のビルド・テスト（FR-5.1）／clippy -D warnings（FR-5.2）／依存監査 cargo-audit または cargo-deny（FR-5.3）
- **対応コンポーネント**: なし（リポジトリ設定。検証対象は全コンポーネント）
- **制約・注記**: 既存のfmtチェック・統合テストジョブは維持。新規モジュールのclippyクリーンが最低線（team-practices）

## U4: ドキュメント（u4-docs）

- **境界**: `examples/`（SQLite利用例）、`README.md` / `README.ja.md`、`docs/DATABASE_SCHEMA.md` / `.ja.md`、`CHANGELOG.md`
- **責務**: SQLite利用例の追加（FR-6.1）／feature説明・移行手順（Cargo.toml before/after・エラー型新旧対応）の追記（FR-6.2）／SQLiteスキーマの記載（FR-6.3、情報提供 — 作成はライブラリの自動作成）／破壊的変更のCHANGELOG記載（FR-6.4）
- **対応コンポーネント**: なし（ドキュメント。対象はU1/U2の成果）
- **制約・注記**: 既存READMEのコード例と現行APIの乖離（TD-10）はこのユニットで更新の対象範囲に含めてよい（移行手順の正確性に必要な範囲で）

## Assumptions & Open Questions

None.

## Review

**Verdict:** READY
**Reviewer:** aidlc-architecture-reviewer-agent
**Date:** 2026-08-22T17:06:49Z
**Iteration:** 1

### Findings

| # | Severity | Location | Finding | Recommendation |
|---|---|---|---|---|
| 1 | Minor | `unit-of-work-dependency.md` 統合点表（U1↔U2） | U1↔U2の統合点は「`StorageBackend` トレイト（5メソッド）と中立エラー型 — U1が形を確定し、U2が実装する」と記述されており、依存トポロジー（`u2-sqlite-backend depends_on: [u1-backend-features]`）と整合する事実記述である一方、「先に確定→後に実装」という言い回しは経済的順序づけの表現に近く、Delivery Planning（2.9）の役割との境界がやや曖昧に読める。実害はない（DAGのエッジと矛盾せず、ステージノートの「本書は依存できるか否かのみを記述する」という宣言も別途明記されている）が、字句上の紛れを避けるならより中立な表現が望ましい。 | 「U1がインターフェース形状を提供し、U2がその実装を利用する（依存の方向を示すのみで着手順序は規定しない）」のように、依存の向きの説明であることをより明示する表現に調整する。 |
| 2 | Minor | `unit-of-work-story-map.md` 「ユニット内のストーリー実装順序」節 | U1内のストーリー順序（US2.2→US2.1→US3.1）の根拠として「エラー型の確定がfeature独立コンパイルの前提（stories.md の依存欄どおり）」を挙げているが、stories.md本文はUS2.1とUS2.2を「同一Boltで一体実施」と明記しており、そこから厳密な逐次順序を導出できるかは stories.md 側でも指摘済みの自己矛盾（Bolt 1が単一Boltか複数Boltか）に依存する。ユニット内順序自体はステージの許容範囲（within-unit story order）内であり、DAGやユニット境界に影響しないため本ユニット生成物の欠陥ではないが、上流のあいまいさをそのまま引用している点は次工程（Delivery Planning）に持ち越される。 | Delivery Planning着手前に、stories.mdレビューで指摘済みのBolt 1範囲確定（US1.1+US2.1+US2.2を一括Boltとするか否か）を解消し、必要であればこの節の「逐次」という表現を見直す。 |

### Validation Tool Results

このスキルはツール実行の指示を含んでおらず、当レビューではファイル間の手動突合を実施した。

| 検証項目 | 結果 |
|---|---|
| YAMLエッジブロック（`unit-of-work-dependency.md`）の構文・ユニット名（lowercase path-segment）・`kind`値 | PASS — `u1-backend-features` / `u2-sqlite-backend` / `u3-ci-quality` / `u4-docs` はいずれも小文字・ハイフン区切りの規約に適合。`kind` は `library`（U1/U2）・`packaging`（U3/U4）で許容セット内 |
| 依存の非循環性 | PASS — u2→u1、u3→u2、u4→u2 の3エッジのみで、逆方向・自己依存なし。手動でグラフを辿り循環なしを確認 |
| `depends_on` の宛先解決 | PASS — 全ての参照先ユニット名は `units` 一覧内に宣言済み |
| 経済的順序づけの混入チェック | PASS — 本文冒頭で「どれを先に出荷するかの経済的順序づけはデリバリ計画が決定する」と明記され、単一の「推奨実装順」やクリティカルパスの提示はない。ユニット内ストーリー順序（within-unit）のみが記載されており許容範囲内 |
| ストーリー・ユニット対応（12件） | PASS — stories.md の全12件（US1.1-1.4, US2.1-2.3, US3.1-3.3, US4.1-4.2）が `unit-of-work-story-map.md` の割り当て表に過不足なく出現し、各ユニットに1件以上割り当て済み（空ユニットなし） |
| `traceability.json` のcoverage/reverse | PASS — 12件全てstatus OK、targetはstory-mapのUnit ID列と一致。reverse側もstory-mapの割り当てと逆引き一致 |
| U{n}/directory 対応表 | PASS — `unit-of-work.md` 冒頭表に Unit ID ↔ Directory ↔ 名前 ↔ Kind ↔ 複雑度 ↔ デプロイモデルの対応が明記され、以降の全セクション見出しの `(u{n}-...)` 表記と一致 |
| ユニット境界とドメイン設計ADRの整合 | PASS — ADR-001(U1: エラー型中立化)・ADR-002(U2: rusqlite)・ADR-003(U2: 単一トランザクションCAS)・ADR-005(U1: Memory準拠化) は各ユニットの責務記述と一致。ADR-004（既存フラット構成踏襲）はユニット分割と直接関係せず不整合なし |
| FR/コンポーネント参照の実在性 | PASS — 引用されたFR-1.1〜FR-6.4は`requirements.md`に実在し番号・内容が一致。引用されたコンポーネント（CoreTypes, StorageAbstraction, MemoryBackend, DynamoDbBackend, BigtableBackend, SqliteBackend, TestSupport）は`components.md`に実在 |

### Summary

ユニット境界・YAMLエッジブロック・ストーリー対応表・トレーサビリティのいずれにも構造的欠陥（循環依存、未解決参照、空ユニット、経済的順序の混入）は見つからなかった。Critical・Majorはゼロ、Minorが2件（依存文書の字句が経済的順序づけに寄って読める余地／上流ストーリーの逐次順序記述をそのまま引用している点）で、いずれも実装をブロックする性質ではない。READYと判定する。
