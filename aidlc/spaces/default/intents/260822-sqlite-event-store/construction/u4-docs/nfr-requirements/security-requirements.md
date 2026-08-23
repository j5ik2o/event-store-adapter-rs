# セキュリティ要件 — u4-docs (security-requirements)

要件定義書（`../../../inception/requirements-analysis/requirements.md` FR-6.1〜FR-6.4・NFR-1/NFR-4）・契約（`../../../inception/contract-design/contract-summary.md` C-1/C-3/C-4）・技術スタック台帳（`aidlc/spaces/default/codekb/sqlite/technology-stack.md` — TD-10）に基づく、U4（ドキュメント）のセキュリティ・品質要件。U4は文書とexamplesのユニットであり、要件は「正確性」と「文書衛生」に集中する（Q1=A確定）。派生IDはU1〜U3と衝突しない連番を用いる。

## 要件一覧

- **NFR-1.4（文書の正確性）**: README（英/日）のfeature構成説明・移行手順（Cargo.toml before/after・エラー型新旧対応）・DATABASE_SCHEMA.md（英/日）のスキーマ記載・CHANGELOG の破壊的変更記載は、実装の公開面（実feature名・実API・実スキーマ・確定済みサポート境界）と一致すること。既知の乖離（TD-10: READMEコード例と現行APIの乖離）は更新範囲で解消すること
- **NFR-1.5（境界事項の開示）**: U2/U3から引き渡された開示事項を文書に明記すること — (1) `sqlite`+`sqlite-system` 併用時はbundled優先（cargo featureの加算性の帰結 — U2 D3）、(2) 同一ファイルDBの複数同時オープンはサポート外（ユーザー確定のサポート境界 — U2 nfr-design）、(3) スキーマ記載は情報提供でありテーブル作成はライブラリの自動作成が担う（FR-6.3）
- **NFR-4.12（文書衛生）**: README・examples・スキーマ文書・CHANGELOGに認証情報・APIキー・秘密のリテラルを記載しないこと。examplesはクラウド接続なしで動作すること（AC4.1.2 — SQLite exampleはファイルまたは `:memory:` のみ使用）

## 適用外の説明

- 供給網・依存要件: U4は依存を追加しない（新規ツールなし — 検証はU3で恒久化済みのCIが担う）
- アプリケーションレベルのセキュリティ要件: U1/U2で確定済み。U4はその内容を文書へ転記するのみ

## Assumptions & Open Questions

None.

## Review

**Verdict:** READY
**Reviewer:** aidlc-architecture-reviewer-agent
**Date:** 2026-08-23T09:11:17Z
**Iteration:** 1

### Findings

| # | Severity | Location | Finding | Recommendation |
|---|---|---|---|---|
| 1 | Minor | security-requirements.md 冒頭（3行目） | 「技術スタック台帳（`aidlc/spaces/default/codekb/sqlite/technology-stack.md` — TD-10）に基づく」と記載し、TD-10（READMEコード例と現行APIの乖離）の出典を`technology-stack.md`としているが、実際に`grep -rn "TD-10" aidlc/spaces/default/codekb/`で確認したところ、TD-10は`code-quality-assessment.md`（および参照元として`api-documentation.md`・`business-overview.md`）に記載されており、`technology-stack.md`には一切出現しない（同ファイルへの`grep`は0件）。TD-10自体の内容（READMEコード例の乖離、crate descriptionが"crate to make DynamoDB an Event Store"のまま）は実際のワークスペース（`README.md`にsqlite言及なし、`lib/Cargo.toml`のdescriptionが実際に旧記述のまま）と完全に一致しており実在する正当な技術的負債だが、その出典ファイルの引用が誤っている。 | 「技術スタック台帳（`technology-stack.md`）」を「コード品質評価（`code-quality-assessment.md`）」に修正する。 |

### Validation Tool Results

| Tool/確認 | 結果 | 解釈 |
|---|---|---|
| `grep -rn "TD-10" aidlc/spaces/default/codekb/sqlite/technology-stack.md` | 0件 | 所見#1の直接根拠。 |
| `grep -rn "TD-10" aidlc/spaces/default/codekb/sqlite/` | `code-quality-assessment.md`（2箇所）・`api-documentation.md`・`business-overview.md`の計4箇所でヒット | TD-10の正しい出典元を確認。 |
| TD-10の内容と実ワークスペースの突合（`grep -n "sqlite" README.md`、`grep -n "^description" lib/Cargo.toml`） | README.mdにsqlite言及0件、`lib/Cargo.toml`のdescriptionが`"crate to make DynamoDB an Event Store"`のまま | TD-10が主張する「READMEコード例と現行APIの乖離」「crate descriptionがDynamoDB専用時代のまま」の実在性を独立に確認した。NFR-1.4の要求内容自体は正当。 |
| NFR-1.5の開示3事項とU2上流成果物の突合 | PASS | (1) 併用時bundled優先はU2 `tech-stack-decisions.md` D3の文言「この帰結はドキュメント〔U4〕に明記する」と一致。(2) 同一ファイルDB複数同時オープン非サポートはU2 `nfr-design/security-design.md`の「この境界はU4のドキュメントに明記する」という文言と一致。(3) スキーマ記載=情報提供はFR-6.3・project.md Mandatedと一致。3事項ともU2側から明示的にU4への引き継ぎが予告されていた内容であり、捏造や過大解釈はない。 |
| 派生NFR ID連番の衝突チェック（NFR-1.4/1.5・NFR-4.12 とU1〜U3の既存ID） | 衝突なし | NFR-1系列: U1(1.1/1.2)→U2(1.3)→U4(1.4/1.5)、NFR-4系列: U1(4.1〜4.4)→U2(4.5〜4.8)→U3(4.9〜4.11)→U4(4.12)がいずれも欠番・重複なく連続している。NFR-2/NFR-3/NFR-5はU4がN/Aとして触れておらず、二重定義もない。 |
| traceability.json 5 NFR行のOK/N/A根拠の妥当性確認 | PASS | NFR-2 N/A（新規依存追加なし、D1）・NFR-3 N/A（examplesのCIビルド検証は既知の欠落としてバックログ — team.md確定と一致）・NFR-5 N/A（U2のNFR-5.1で充足済み）はいずれも上流の実際の充足状況・確定事項と整合する。 |
| Q1=A確定の反映確認 | PASS | 「正確性」「文書衛生」の2点整理・新規ツールなしという要件本文が、Q&Aの`[Answer]: A`の文言と一致。 |
| `python3 -c "import json; json.load(...)"` によるtraceability.jsonのパース検証 | PASS | 構文エラーなし。5件のNFR ID全てが`requirements.md`のNFR-1〜5に実在する。 |

### Summary

TD-10（READMEコード例と現行APIの乖離、crate description陳腐化）という要件の実質的な根拠は実在し、実際のワークスペース状態とも一致していますが、その出典ファイルの引用が「technology-stack.md」となっており、実際には「code-quality-assessment.md」に記載されている点をMinorとして記録しました。NFR-1.5の3つの開示事項はいずれもU2側の設計文書で「U4への明記」が明示的に予告されていた内容と正確に一致しており、派生NFR IDの連番（NFR-1.4/1.5、NFR-4.12）もU1〜U3と衝突していません。traceability.jsonのN/A根拠（NFR-2/3/5）も上流の充足実績と整合しています。Critical 0件・Major 0件・Minor 1件のためREADYと判定します。
