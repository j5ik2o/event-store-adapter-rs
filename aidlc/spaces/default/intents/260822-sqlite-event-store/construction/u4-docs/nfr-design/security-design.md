# セキュリティ設計 — u4-docs (security-design)

セキュリティ要件（`../nfr-requirements/security-requirements.md` NFR-1.4/1.5・NFR-4.12）・技術スタック決定（`../nfr-requirements/tech-stack-decisions.md` D1〜D4）を、U4（ドキュメント）の具体設計へ落とす。性能系設計書・logical-components はキンド適用で対象外（packagingユニット）。

## 記載割り当ての設計（NFR-1.4 / NFR-1.5 — Q1確定）

| 文書 | 記載内容 | 出所（正） |
|---|---|---|
| README.md / README.ja.md | feature構成の説明（dynamodb/bigtable/sqlite/sqlite-system・default廃止）、移行手順（Cargo.toml before/after・エラー型新旧対応表）、**サポート境界**（同一ファイルDB複数同時オープン非サポート）、**併用時bundled優先**の注記、SQLiteの最小利用例、crate説明の陳腐化解消（TD-10 — `lib/Cargo.toml` の description 更新を含む） | C-3 feature表／U1エラー型変更／U2 D3・サポート境界／実装の公開API |
| docs/DATABASE_SCHEMA.md / .ja.md | SQLiteの journal / snapshot スキーマ（列・キー構成 — pkey/skey書き込み分散・(aid, seq_nr)読み込み索引を含む）。**情報提供であり作成はライブラリの自動作成が担う**ことを明記 | 実装のDDL（event_store_for_sqlite.rs）／FR-6.3 |
| CHANGELOG.md | 破壊的変更（デフォルトfeature廃止・エラー型 `OptimisticLockError(String)` 化）と新機能（sqlite/sqlite-system feature・`EventStoreForSqlite`） | C-1（semverメジャー扱い）／U1/U2コミット |
| examples/ | SQLite利用例（ファイルまたは `:memory:`・クラウド接続なし） | D2（既存example構成に倣う） |

- **正確性の担保手順**: 各記載は転記元（実装ファイル・確定成果物）を照合してから書く。文書側で新しい仕様を発明しない（D3）。コード例はコンパイル可能な形で書き、examplesの実行確認をもって裏付ける

## 文書衛生の設計（NFR-4.12）

- README・examples・スキーマ文書・CHANGELOGに資格情報・APIキー・秘密のリテラルを置かない（既存文書のAWS例は接続例の範囲のみ — 新規追記分はSQLite中心で機密を持たない）
- examplesのDBパスは相対パスまたは `:memory:` とし、実在の個人環境パスを埋め込まない
- 検証: 追記分に対するgrep目視（`(api[_-]?key|secret|password|token)\s*=`）

## 検証手順（U4実装後の受け入れ確認）

```bash
cargo run --example <sqlite-example>   # クラウド接続なしで動作（AC4.1.2）
grep -n "sqlite" README.md             # feature構成・境界事項の記載存在
grep -n "sqlite" docs/DATABASE_SCHEMA.md
```

## Assumptions & Open Questions

None.

## Review

**Verdict:** READY
**Reviewer:** aidlc-architecture-reviewer-agent
**Date:** 2026-08-23T09:25:41Z
**Iteration:** 1

### Findings

新規Critical/Major/Minor所見なし。

### Validation Tool Results

| Tool/確認 | 結果 | 解釈 |
|---|---|---|
| Q1=A確定（文書割り当て）との突合 | PASS | nfr-design-questions.mdのQ1回答（README/DATABASE_SCHEMA/CHANGELOG/examplesへの4分割割り当て）が「記載割り当ての設計」表の4行と一字一句レベルで一致している。 |
| traceability.json 3 ID（NFR-1.4, 1.5, 4.12）と上流nfr-requirements(u4)（iteration 1でREADY確定済み）の突合 | PASS | 上流の非N/A 3 IDと過不足なく一致。NFR-2/3/5（上流でN/A）はここでも扱われておらず二重定義もない。 |
| 割り当て表「出所（正）」欄の実在性確認（C-3・U2 D3・サポート境界） | PASS | C-3 feature表（`inception/contract-design/contract-summary.md`）、U2 `tech-stack-decisions.md` D3（併用時bundled優先）、U2 `nfr-design/security-design.md`のサポート境界（同一ファイルDB複数同時オープン非サポート）は、いずれも本レビューが許可された読み取り範囲（インセプション成果物・実ワークスペース）とnfr-requirements(u4)側の既存引用で裏付けられ、捏造や過大解釈はない。 |
| TD-10対応設計の実態整合確認（`grep -n "^description" lib/Cargo.toml`） | `description = "crate to make DynamoDB an Event Store"` | 「crate説明の陳腐化解消（TD-10 — `lib/Cargo.toml` の description 更新を含む）」という設計が指す対象ファイル・現状記述と完全に一致。なお本書自身はTD-10の出典ファイルを明記しておらず、nfr-requirements(u4)側で指摘した出典ファイルの誤記（`technology-stack.md`ではなく`code-quality-assessment.md`）はこの設計書には再発していない。 |
| DATABASE_SCHEMA.md行のスキーマ記述と実装DDLの突合（`lib/src/event_store_for_sqlite.rs`のCREATE TABLE文） | PASS | 「pkey/skey書き込み分散・(aid, seq_nr)読み込み索引を含む」という記述は、実際のjournal/snapshotテーブルDDL（PK (pkey, skey)・UNIQUE/通常INDEX (aid, seq_nr)）と一致する。 |
| `python3 -c "import json; json.load(...)"` によるtraceability.jsonのパース検証 | PASS | 構文エラーなし。 |

### Summary

Q1確定の文書割り当てが記載割り当ての設計表に過不足なく反映されており、traceability.jsonの3 IDも上流nfr-requirements(u4)と完全に一致しています。出所欄が引用するC-3・U2 D3・サポート境界の各事項はいずれも実在し正確で、TD-10対応（crate description更新）も実ワークスペースの現状と一致しています。前段階（nfr-requirements u4）で指摘した出典ファイルの誤記もこの設計書では再発していません。Critical 0件・Major 0件・Minor 0件のためREADYと判定します。
