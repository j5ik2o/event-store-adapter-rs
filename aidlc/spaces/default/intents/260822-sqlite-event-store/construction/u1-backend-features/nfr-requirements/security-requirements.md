# セキュリティ要件 — u1-backend-features (security-requirements)

機能仕様（`../functional-design/functional-spec.md`）・ルール（`../functional-design/rules.md`）・要件定義書（`../../../inception/requirements-analysis/requirements.md` NFR-2/NFR-4）・契約（`../../../inception/contract-design/contract-summary.md` C-3）・技術スタック台帳（`aidlc/spaces/default/codekb/sqlite/technology-stack.md`）に基づく、U1（feature分割・エラー型中立化・Memory準拠化）のセキュリティ要件。ライブラリでありネットワーク境界・認証境界を持たないため、要件は依存供給網とコード健全性に集中する。

## 要件一覧

- **NFR-2.1（供給網・依存最小）**: U1完了時点で、未使用依存（aws-http）・宣言のみの依存（prost）・未使用dev依存（serial_test）が削除されていること。feature未指定ビルドの依存グラフにクラウドSDKが現れないこと（攻撃面の縮小 — cargo tree で検証）
- **NFR-2.2（供給網・隔離）**: クラウドSDK依存は `optional = true` 化され、対応featureのみが有効化すること（利用者は使わないSDKのCVE影響を受けない）
- **NFR-4.1（panic禁止）**: バックエンド内部のエラーは panic させず `EventStoreWriteError` / `EventStoreReadError` へ写像すること（Memoryの既存panic経路の除去を含む — BR1.6）
- **NFR-4.2（unsafe禁止）**: U1接触範囲に手書きの `unsafe impl Send/Sync` が残らないこと（BR1.7 — grepで検証）
- **NFR-4.3（機密情報）**: 認証情報・APIキー・秘密のハードコードを持ち込まないこと（既存コード同様 — 構築フェーズ規範）
- **NFR-4.4（エラー情報の衛生）**: `OptimisticLockError(String)` の整形メッセージに集約ID・バージョン以外の内部情報（接続文字列・資格情報等）を含めないこと（BR1.2の形式に限定）

## 適用外の説明

- 認証・認可・暗号化・データレジデンシ: 本ライブラリは格納データの取り扱い責務を持たず（実現性Q3確定 — 利用者側の責務）、U1はストレージ実装も追加しないため対象外

## Assumptions & Open Questions

None.

## Review

**Verdict:** READY
**Reviewer:** aidlc-architecture-reviewer-agent
**Date:** 2026-08-22T23:33:03Z
**Iteration:** 2

### Findings

| # | Severity | Location | Finding | Recommendation |
|---|---|---|---|---|
| 1 | Minor | traceability.json（`NFR-2` の `coverage` 行） | 上流 `NFR-2`（requirements.md 67行目）の文言は「SQLiteドライバは1クレートのみ。ORM等の大型フレームワーク依存は導入しない」であり、これはSQLiteドライバ選定（U2の責務、`tech-stack-decisions.md` D1で明示的にU2へ委譲済み）を指す。本ユニットの派生NFR-2.1/NFR-2.2（未使用依存削除・クラウドSDK隔離）はNFR-2の「依存最小」という上位概念の別側面には当たるが、原文が名指す対象（SQLiteドライバの単一クレート性）はU1側の成果物では一切扱っていない。それにも関わらず traceability.json は `status: "OK"` とだけ記載し、NFR-5で行っているような「他ユニットへ委譲」の注記が無い。 | NFR-2の coverage 行に「SQLiteドライバ選定に関する部分はU2へ委譲（tech-stack-decisions D1参照）」といった注記を加え、NFR-5と同じ透明性で部分委譲を明示する。 |
| 2 | Minor | traceability.json（`NFR-4` の `coverage` 行） | 上流 `NFR-4`（requirements.md 69行目）は「rustfmt・clippy（-D warnings）をパスすること」と「バックエンド内部のエラーをpanicさせない」の2要素からなる複合要件。本ユニットの派生NFR-4.1〜4.4はpanic禁止・unsafe禁止・機密情報・エラー衛生のみをカバーし、clippy CI導入は `tech-stack-decisions.md` D6で「U3の責務でありU1では行わない」と明記されている。traceability.json はこの分割を注記せず `status: "OK"` のみで、NFR-4の一部（rustfmt/clippy強制）がU1のスコープ外であることが traceability からは読み取れない。 | NFR-4の coverage 行に「clippy/rustfmt のCI強制はU3の責務（tech-stack-decisions D6）」の注記を加える。 |
| 3 | Minor | security-requirements.md NFR-4.3 | NFR-4.3（機密情報のハードコード禁止）は上流 `NFR-4` の原文（rustfmt/clippy・panic禁止）には含まれておらず、構築フェーズ規範（phases/construction.md Security節）由来の追加項目である。他の派生要件（NFR-4.1/4.2/4.4）が検証手段（BR参照・grep等）を明記しているのに対し、NFR-4.3には検出・検証手段の記載が無い。 | 出典を「構築フェーズ規範（NFR-4由来ではなく追加項目）」と明記し、検証手段（例: 既存コードのハードコード有無の目視/grep確認）を追記する。 |

### Previous Findings — Resolution Check

| # | Severity | Iteration 1 Finding | Status |
|---|---|---|---|
| 1 | Minor | NFR-2 の traceability `OK` 表示がU2委譲部分を注記していない | 未解消（成果物本文は iteration 1 と同一バイト。`nfr-requirements-questions.md` のQ2はNFR-5の委譲整理のみを確認しており、NFR-2の部分委譲注記は対象外のまま） |
| 2 | Minor | NFR-4 の traceability `OK` 表示がU3委譲部分（clippy/rustfmt CI）を注記していない | 未解消（同上、本文不変） |
| 3 | Minor | NFR-4.3 の出典・検証手段の記載が薄い | 未解消（同上、本文不変） |

### Validation Tool Results

| Tool | Result | Interpretation |
|---|---|---|
| バイト同一性確認（iteration 1 → 2） | PASS | `security-requirements.md`（`## Review` 除く本文）・`tech-stack-decisions.md`・`traceability.json` は iteration 1 でレビューした内容と現行内容が同一であることを再読で確認した |
| 追加確認: `nfr-requirements-questions.md` | PASS | Q1（3成果物構成）・Q2（NFR-5 N/A委譲）・Q3（互換性/ビルド派生要件の置き場）はいずれも成果物の実際の構成と一致し、回答は3件とも A（現状確定）、最終確認も「Looks correct」。iteration 1 の所見（NFR-2/NFR-4の部分委譲注記漏れ、NFR-4.3の出典・検証手段）はいずれの設問でも扱われておらず、質問工程を経ても未解消であることが裏付けられた |

### Summary

本文バイトが iteration 1 と同一であることを確認し、質問工程で新たに解決・悪化した論点もないため、iteration 1 の判定根拠（Critical/Major 0件・Minor 3件）はそのまま現行内容に該当する。READY を維持する。
