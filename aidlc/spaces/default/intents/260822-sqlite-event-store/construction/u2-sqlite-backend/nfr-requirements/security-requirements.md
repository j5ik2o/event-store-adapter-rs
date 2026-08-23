# セキュリティ要件 — u2-sqlite-backend (security-requirements)

機能仕様（`../functional-design/functional-spec.md`）・ルール（`../functional-design/rules.md` BR2.1〜BR2.12）・要件定義書（`../../../inception/requirements-analysis/requirements.md` NFR-2/NFR-4/NFR-5）・契約（`../../../inception/contract-design/contract-summary.md` C-3）・技術スタック台帳（`aidlc/spaces/default/codekb/sqlite/technology-stack.md`）に基づく、U2（SQLiteバックエンド本体）のセキュリティ要件。ライブラリでありネットワーク境界・認証境界を持たないため、要件は依存供給網とコード健全性・テスト環境衛生に集中する。派生IDはU1の採番（NFR-2.1〜2.2 / NFR-4.1〜4.4）と衝突しない連番を用いる。

## 要件一覧

- **NFR-2.3（依存唯一性）**: U2で追加するランタイム依存は rusqlite（0.40系）の1クレートのみであること（ORM等の導入禁止 — cargo tree で検証、BR2.10）
- **NFR-2.4（供給網追随）**: rusqlite は workspace の依存テーブルで最新安定版を管理し Renovate の自動更新に追随すること（Q1=A確定）。同梱（bundled）SQLite本体由来のCVEは RUSTSEC照合（cargo-audit / cargo-deny advisories — U3のCI日次実行）で検出し、rusqlite / libsqlite3-sys のバージョン更新で取り込むこと（Q2=A確定 — 手動の追加運用は設けない）
- **NFR-4.5（panic禁止）**: SQLiteバックエンドの全経路（接続確立失敗・書き込み不能パス・ロック汚染・トランザクション失敗を含む）で panic せず、決定表どおり中立エラー型へ写像すること（BR2.8 — AC1.1.4で検証）
- **NFR-4.6（unsafe禁止）**: U2の新規コードに手書きの `unsafe impl Send/Sync` を書かないこと（grepで検証 — AC3.1.3の対象範囲）
- **NFR-4.7（エラー情報の衛生）**: `OptimisticLockError` の整形メッセージはBR1.2書式（aid・バージョン情報）のみとし、DBファイルパス・rusqliteの生エラー文字列を混入させないこと（下位詳細は `IOError` / `OtherError` のソース側に保持）
- **NFR-4.8（機密情報）**: 実装・テスト・examplesに認証情報・APIキー・秘密のリテラルを埋め込まないこと（DBパスは機密ではないがテストは一時ディレクトリ配下の一意パスを用いる）
- **NFR-5.1（テスト実行環境）**: SQLiteのテストは testcontainers / Docker を使わず、ファイル（一時ディレクトリ・一意名）または `:memory:` で完結し、決定的・並列安全であること。プロセスグローバルな変異（`env::set_var` 等）を用いないこと（BR2.12 — AC3.2.2で検証）

## 適用外の説明

- 認証・認可・暗号化・データレジデンシ: 本ライブラリは格納データの取り扱い責務を持たず（利用者側の責務 — U1と同一の整理）、SQLiteファイルの保護（ファイルパーミッション・暗号化拡張）は利用者環境の責務のため対象外

## Assumptions & Open Questions

None.

## Review

**Verdict:** READY
**Reviewer:** aidlc-architecture-reviewer-agent
**Date:** 2026-08-23T04:01:01Z
**Iteration:** 2

### Findings

新規Critical/Major/Minor所見なし。

### Previous Findings — Resolution Check

| # | Severity | Iteration 1 Finding | Status |
|---|---|---|---|
| 1 | Major | tech-stack-decisions.md D1/D2に`default-features = false`の明記がなく、rusqlite 0.40.2のデフォルトfeature`cache`が`hashlink`（ターゲット非限定の実行時依存）を有効化するため、BR2.10／NFR-2.3「追加ランタイム依存はrusqlite1クレートのみ」に違反する設計だった | 解消済み — D1が`rusqlite = { version = "0.40.2", default-features = false }`という具体的なworkspace依存宣言に書き換えられ、「`default-features = false`は必須」と明示のうえ理由（`cache`→`hashlink`がBR2.10/NFR-2.3に違反するため）まで正確に記載された。D2にも「rusqlite側featureはここに列挙したもの以外を有効化しない（D1の`default-features = false`が前提。`prepare_cached`等`cache`feature依存のAPIは使用しない）」という運用面の徹底まで追記されており、`cache`機能への依存経路（APIレベル）も明示的に塞がれている。 |

### Validation Tool Results

| Tool/確認 | 結果 | 解釈 |
|---|---|---|
| tech-stack-decisions.md D1/D2の修正内容と、iteration 1で確認したrusqlite 0.40.2実際のfeature構造（`+default = [cache, ffi-sqlite-wasm-rs]`、`cache = [hashlink]`、`bundled = [libsqlite3-sys?/bundled, modern_sqlite]`）との再突合 | PASS | `default-features = false`を明記した上でD2が`rusqlite/bundled`のみを明示的に有効化する構成になっており、`hashlink`が依存グラフに現れない設計に修正されている。`bundled`自体は`cache`に依存しないため、この修正でD2の`sqlite`/`sqlite-system`feature設計自体に副作用は生じない。 |
| security-requirements.md / traceability.json のバイト同一性確認 | PASS | iteration 1でレビューした内容と現行内容が同一であることを再読で確認した（今回の修正範囲外）。 |

### Summary

iteration 1で指摘したMajor 1件は、D1へのworkspace依存宣言の具体化（`default-features = false`の明記と理由の記載）とD2への運用面の徹底（列挙外feature・`cache`依存API不使用の明記）により、根本原因（rusqliteのデフォルトfeatureが`hashlink`を有効化する事実）に対して正確に対処する形で解消されました。security-requirements.md・traceability.jsonは変更範囲外で、iteration 1時点の健全性がそのまま維持されています。Critical 0件・Major 0件・Minor 0件のためREADYと判定します。
