# セキュリティ要件 — u3-ci-quality (security-requirements)

要件定義書（`../../../inception/requirements-analysis/requirements.md` FR-5.1〜FR-5.3・NFR-2/NFR-4）・契約（`../../../inception/contract-design/contract-summary.md` C-3 feature軸）・技術スタック台帳（`aidlc/spaces/default/codekb/sqlite/technology-stack.md` — Renovate設定・既存CI）に基づく、U3（CI品質保証）のセキュリティ要件。U3はCI設定のユニットであり、要件は「供給網検証の恒久化」と「CI自体の健全性」に集中する。派生IDはU1/U2と衝突しない連番を用いる。

## 要件一覧

- **NFR-2.5（RUSTSEC照合の恒久化）**: cargo-deny による advisories（RUSTSEC）・licenses 検査をCIに組み込み、PR・main push・日次cronのすべてで実行すること（Q1/Q2=A確定）。Renovateの automerge（minor/patch/pin/digest）に対する脆弱性ゲートとして機能させる（team.md Q8の背景 — bundled SQLiteのC由来CVE面を含む）
- **NFR-2.6（依存隔離の恒久検証）**: featureマトリクス（未指定／dynamodb／bigtable／sqlite／sqlite-system／全feature）のビルド・テストと、依存グラフ検査（feature未指定でクラウドSDK不在・sqlite構成で hashlink 不在 — `cargo tree -e normal`）をCIで恒久化すること（U1/U2からの引き渡し要求仕様の実装）
- **NFR-4.9（静的解析の恒久化）**: `cargo clippy --workspace --all-targets -- -D warnings` をCIに組み込むこと。新規モジュール（sqlite系）はclippyクリーンを維持し、既存負債（generic_event_storeテスト内eager clone 1件）は既知として扱う（バックログ — 抑制する場合は理由を明記）
- **NFR-4.10（unsafe不在の恒久検証）**: `grep -rn "unsafe impl" lib/src/` が0件であることをCIで検証すること（U2引き渡し仕様 項目4）
- **NFR-4.11（CIワークフローの健全性）**: 監査・検証ジョブは資格情報を必要とせず、新しいシークレットを追加しないこと。ジョブの失敗は握りつぶさずCIの失敗として顕在化させること（silent failure禁止 — 構築フェーズ規範）

## 適用外の説明

- アプリケーションレベルの認証・認可・データ保護: U3はコードを追加せずCI設定のみのため対象外（U1/U2の要件が引き続き有効）
- GitHub Actions のバージョンピン（SHA固定）: 既存ワークフローはタグ参照（`actions/checkout@v6` 等）で統一されており、この慣行の変更は本イニシアチブのスコープ外（バックログ候補）

## Assumptions & Open Questions

None.

## Review

**Verdict:** READY
**Reviewer:** aidlc-architecture-reviewer-agent
**Date:** 2026-08-23T06:54:33Z
**Iteration:** 2

### Previous Findings — Resolution Check

| # | Severity | Iteration 1 所見 | 対応状況 | 確認内容 |
|---|---|---|---|---|
| 1 | Major | traceability.jsonがNFR-5を`status: "OK"`とし、target「tech-stack-decisions.md D5」を指すが、security-requirements.md・tech-stack-decisions.mdのどちらにも対応する派生ID（NFR-5.x）が実在しなかった | **解消（推奨案aを採用）** | traceability.jsonのNFR-5行は`status: "N/A"`へ変更され、target が「U2のNFR-5.1（Docker不要・決定的・並列安全なテスト環境）で既に充足済み。U3はCI側でその前提を維持するのみ（tech-stack-decisions.md D5の分担）で、新規の派生要件を追加しない」に書き換えられている。この記述はNFR-1のN/A行（「公開API互換はU1/U2のコード要件...で確定済み — U3はCI設定のみで公開面に触れない」）と同じ構造（既に上流ユニットで充足済み・本ユニットは新規要件を追加しない）を取っており、他の4件のOK行（NFR-2/3/4）がいずれも実在する派生IDを伴うのと矛盾しない一貫した状態になった。security-requirements.md・tech-stack-decisions.md本文は変更されておらず（意図どおり — D5は既存の技術判断としてそのまま残り、新たに架空のNFR-5.x見出しを捏造していない）、traceability.jsonのみの修正で不整合が解消されている。 |

### New Findings

新規Critical/Major/Minor所見なし。

### Validation Tool Results

| Tool/確認 | 結果 | 解釈 |
|---|---|---|
| `python3 -c "import json; json.load(...)"` によるtraceability.jsonのパース検証 | PASS | 構文エラーなし。5件のNFR ID全てを再確認。 |
| traceability.json NFR-5行の内容確認 | `status: "N/A"`、target が上記の正当化文言 | team-lead報告の修正内容と完全一致。 |
| security-requirements.md / tech-stack-decisions.md のバイト同一性確認（iteration 1時点との比較） | 変更なし | 今回の修正がtraceability.jsonのみに閉じており、他2ファイルの本文（NFR-2.5/2.6・NFR-3.3・NFR-4.9〜4.11・D1〜D6）はiteration 1で検証済みの内容のまま維持されている。 |
| NFR-1（N/A）とNFR-5（N/A、今回修正）の正当化文の構造比較 | 一貫 | 両者とも「他ユニットで既に充足済み・本ユニットは新規要件を追加しない」という同一パターンで記述されており、ステータス使い分けの基準がN/A row間で統一されている。 |
| `grep -c "^## Review$"`（追記前） | 0 | 前回セクションが規定どおり削除されていたことを確認。本追記により新たに1セクションのみ生成される。 |

### Summary

Iteration 1のMajor所見（traceability.jsonのNFR-5行が"OK"を主張しながら対応する派生要件IDが本文に存在しなかった不整合）は、team-lead提案の推奨案a（N/Aへの変更）の採用により解消を確認した。修正後の正当化文はNFR-1のN/A行と同じ構造を取っており、他4件のOK行との整合性も保たれている。修正はtraceability.jsonのみに閉じており、iteration 1で検証済みのsecurity-requirements.md・tech-stack-decisions.md本文（U1/U2引き渡し仕様の網羅、Q1/Q2確定事項の反映、派生ID連番の非衝突）は変更なくそのまま有効である。新規のCritical/Major/Minor所見はなし。Critical 0件・Major 0件のためREADYと判定する。
