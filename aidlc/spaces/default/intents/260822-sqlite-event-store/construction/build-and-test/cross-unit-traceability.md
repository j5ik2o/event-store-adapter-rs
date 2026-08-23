# クロスユニット最終トレーサビリティ — build-and-test (cross-unit-traceability)

要件（`../../inception/requirements-analysis/requirements.md` の全FR/NFR）と受け入れ基準（`../../inception/user-stories/stories.md` の3セグメントAC全件）を、全ユニットの `construction/*/code-generation/traceability.json` と突合した最終カバレッジゲート。

## 判定: **PASS**

- **AC（33件）**: 全件が code-generation トレーサビリティで `OK`、target実在確認済み
- **FR（24件）**: 全FRが1件以上のACに紐付き（stories.mdのAC行が出典FRを明記）、そのAC全件がOK — FR直接行はcode-generationに存在しないが、FR→AC→実装ファイルの連鎖で全件被覆
- **NFR（5件）**: 各ユニットの nfr-requirements トレーサビリティで NFR-1〜5 すべてに1件以上の `OK`（派生NFR-x.y へ分解）があり、派生IDは各ユニットの code-generation トレーサビリティで被覆（各ユニットのレビューで突合済み）

## AC別カバレッジ（所有ユニット・target）

| AC範囲 | 所有ユニット | 代表target |
|---|---|---|
| AC1.1.1〜AC1.4.2（10件 — SQLite永続化・CAS・リプレイ・保持） | u2-sqlite-backend | `lib/src/event_store_for_sqlite.rs` / `event_store_for_sqlite_test.rs` |
| AC2.1.1〜AC2.2.3（7件 — feature分割・エラー型） | u1-backend-features | `lib/Cargo.toml` / `lib/src/types.rs` / `lib/src/lib.rs` |
| AC2.3.1〜AC2.3.2（2件 — bundled/system） | u2-sqlite-backend | `lib/Cargo.toml` |
| AC3.1.1〜AC3.1.3（3件 — Memory準拠化） | u1-backend-features | `lib/src/event_store_for_memory.rs` / `_test.rs` |
| AC3.2.1〜AC3.2.3（3件 — SQLiteテストDocker不要） | u2-sqlite-backend | `lib/src/event_store_for_sqlite_test.rs` |
| AC3.3.1〜AC3.3.3（3件 — CIマトリクス・clippy・監査） | u3-ci-quality | `.github/workflows/ci.yml` / `deny.toml` |
| AC4.1.1〜AC4.2.2（4件 — README・example・スキーマ・CHANGELOG） | u4-docs | `README.md` / `examples/user-account-sqlite/src/main.rs` / `docs/DATABASE_SCHEMA.md` / `CHANGELOG.md` |

## NFR別カバレッジ（分解チェーン）

| NFR | OK保有ユニット（nfr-requirements） | 派生ID → code-generation被覆 |
|---|---|---|
| NFR-1（互換性） | u1 / u2 / u4 | NFR-1.1〜1.5 |
| NFR-2（依存最小） | u1 / u2 / u3 | NFR-2.1〜2.6 |
| NFR-3（ビルド互換） | u1 / u2 / u3 | NFR-3.1〜3.3 |
| NFR-4（コード品質） | u1 / u2 / u3 / u4 | NFR-4.1〜4.12 |
| NFR-5（テスト実行環境） | u2（他ユニットはN/A委譲 — 委譲注記の粒度は既報のMinor所見） | NFR-5.1 |

## 未被覆要素

なし。

## 既報の注記（ゲート承認済みのMinor — 再掲）

- u1のNFR-2/NFR-4行に部分委譲注記なし（nfr-requirementsゲートで報告・承認済み）
- u1のAC3.1.2 targetがテストファイル側のみ（code-generationゲートで報告・承認済み）
