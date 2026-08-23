# CI/CDパイプライン設計 — u2-sqlite-backend (cicd-pipeline)

U2（SQLiteバックエンド本体）の配信パイプライン設計。セキュリティ設計（`../nfr-design/security-design.md` 検証手順・サポート境界）・論理コンポーネント（`../nfr-design/logical-components.md`）・コンポーネントカタログ（`../../../inception/domain-design/components.md`）・機能仕様（`../functional-design/functional-spec.md`）・契約（`../../../inception/contract-design/contract-summary.md` C-3）に基づく。「デプロイ」= crates.io への公開でありクラウドインフラは存在しない。性能系設計書と infrastructure-specification.md / monitoring-design.md はキンド適用で対象外（ライブラリユニット — U1と同一の整理）。

## 現行パイプライン（U1完了後の正）

| 段階 | 実体 | 内容 |
|---|---|---|
| Lint | `ci.yml` `lint` ジョブ | nightly rustfmt で `cargo fmt -- --check`（PR・main push・日次cron） |
| Test | `ci.yml` `test-lib` ジョブ（lint後続、ブランチ保護の必須チェック） | stable で `cargo test --verbose -p event-store-adapter-rs --all-features`（U1で `--all-features` 化済み） |
| バージョン確定 | `lib-bump-version.yml` | `workflow_dispatch`（手動）または日次cron（`0 0 * * *`）で起動し、Conventional Commitsからsemverレベルを自動判定してバンプコミット＋タグpush（**mainマージへの即時反応ではない** — U1レビューでの実機確認事項） |
| 公開 | `lib-release.yml` | `v*` タグpushをトリガーに `cargo publish`（crates.io） |

## U2での変更（Q1=A確定）

- **CIには一切触れない**。U2マージ後、sqlite featureのテスト（共有シナリオ・競合パス・エラー契約 — すべてDocker不要）は既存 `test-lib` の `--all-features` 実行に自動的に含まれてCI実行される
- リリースフロー（自動バンプ・自動タグ・自動公開）は不変。新しいシークレットも追加しない（U2のコードに資格情報は入らない — NFR-4.8）

## ステージ→ゲート対応

| ステージ | ゲート | U2での状態 |
|---|---|---|
| PR上の `cargo fmt -- --check` | `test-lib` のジョブ依存（`needs: lint`）経由の間接ゲート | 不変 |
| PR上の `cargo test --all-features` | マージ前提条件（ブランチ保護の必須チェック `test-lib`） | 不変 — U2のsqliteテストがここに乗る |
| sqlite単独／sqlite-system構成のビルド・テスト | マージ前提条件（将来） | U3が整備（FR-5.1）。それまでは security-design.md の手動検証手順で代替 |
| clippy `-D warnings` / 依存監査（RUSTSEC照合） | マージ前提条件（将来） | U3が整備（FR-5.2/FR-5.3 — bundled SQLiteのCVE検出はNFR-2.4の分担どおり） |
| 本番公開 | なし（完全自動 — 手動または日次cronでのバンプ起点） | 不変 |

## U3への引き渡し要求仕様（U2完了時点の追加分）

U1引き渡し分（featureマトリクス・依存グラフ検査・clippy・監査）に加えて:

1. featureマトリクスへ **sqlite系2構成** を追加: `--no-default-features --features sqlite`（バンドル）／`--no-default-features --features sqlite-system`（システムリンク — ubuntuランナーは `libsqlite3-dev` 導入が前提）
2. 依存グラフ検査に **hashlink 不在** を追加: `cargo tree -p event-store-adapter-rs -e normal --no-default-features --features sqlite | grep hashlink` が空であること（rusqliteの `default-features = false` の退行検出）
3. sqliteテストのユニットスコープ実行コマンド: `cargo test -p event-store-adapter-rs --no-default-features --features sqlite test_event_store_on_sqlite`（Docker不要）
4. **unsafe不在検査（本書での新規追加）**: `grep -rn "unsafe impl" lib/src/` が0件であること。U1・U2いずれの従来の引き渡しリストにも未列挙だったため、U2完了時点の追加項目としてここで新規にCI恒久化対象へ加える（NFR-4.6 — U1のsecurity-design.mdでは手動ローカル手順としてのみ存在していた）

## ロールバック手順

- **マージ前**: PRクローズのみ（既存フロー）
- **マージ後・公開前**: `main` 上で `git revert`（Conventional Commits形式）。バンプは手動または日次cron起点のため、公開前ウィンドウは最大24時間程度存在する
- **公開後**: 欠陥バージョンは `cargo yank --vers <x.y.z>` で取り下げ、revert後の新バージョンを自動フローで公開。破壊的変更・新featureの告知はCHANGELOG（FR-6.4 — U4）が担う

## Assumptions & Open Questions

None.

## Review

**Verdict:** READY
**Reviewer:** aidlc-architecture-reviewer-agent
**Date:** 2026-08-23T04:57:08Z
**Iteration:** 2

### Previous Findings — Resolution Check

| # | Severity | Iteration 1 所見 | 対応状況 | 確認内容 |
|---|---|---|---|---|
| 1 | Major | traceability.json の NFR-4.6 行が「unsafe-grep のCI恒久化はU1引き渡し分に包含済み」と主張していたが、U1の実際の`cicd-pipeline.md`「U3への引き渡し要求仕様」節（3項目: featureマトリクス・依存グラフ検査・clippy/監査）にunsafe-grepの記載はなく、U2自身の従来の引き渡し追加節（当時3項目）にも記載がなかった。 | **解消** | U1の`cicd-pipeline.md`を再読し、`grep -n "unsafe"`で全文検索した結果、該当箇所は0件であることを確認した（本ターンのBash確認）。U2の`cicd-pipeline.md`「U3への引き渡し要求仕様」節に項目4「unsafe不在検査（本書での新規追加）」が新規追加され、「U1・U2いずれの従来の引き渡しリストにも未列挙だったため、U2完了時点の追加項目としてここで新規にCI恒久化対象へ加える」と事実どおりに記載されている（36行目）。traceability.json のNFR-4.6行も target を「本書のU3引き渡し要求仕様に新規追加（cicd-pipeline.md 項目4 — U1・U2の従来引き渡しリストに未列挙だったためU2完了時点で追加）」に修正済みで、cicd-pipeline.md 項目4の記述と完全に整合する。虚偽の「包含済み」主張は解消され、正確な「新規追加」という事実に置き換わっている。 |

### New Findings

新規Critical/Major/Minor所見なし。

### Validation Tool Results

| Tool/確認 | 結果 | 解釈 |
|---|---|---|
| `grep -n "unsafe" u1-backend-features/infrastructure-design/cicd-pipeline.md` | 0件ヒット | U1の引き渡しリストにunsafe-grep項目が存在しないことを再確認。iteration 1所見および今回の修正文の前提が正しいことの直接証拠。 |
| `grep -n "unsafe" u2-sqlite-backend/infrastructure-design/cicd-pipeline.md` | 項目4（36行目）のみヒット | 新規追加された項目4が実在し、内容が team-lead 報告と一致することを確認。 |
| `python3 -c "import json; json.load(...)"` によるtraceability.jsonのパース検証 | PASS（9エントリ） | 構文エラーなし。NFR-4.6行のtarget文言がcicd-pipeline.md項目4と一致することも確認。 |
| U2 security-design.md（許可済み既読ファイル）とのNFR-4.6整合確認 | PASS | security-design.mdは「手書き`unsafe impl`禁止（NFR-4.6、grep検証）」と「検証手順（U2時点 — 手動ローカル。CI恒久化はU3）」に同一grepコマンドを掲載しており、cicd-pipeline.md項目4の「U1のsecurity-design.mdでは手動ローカル手順としてのみ存在していた」という記述と整合する。design側（手動検証手順の提供）とinfra-design側（CI恒久化のU3引き渡し）の役割分担が矛盾なく描かれている。 |
| `## Review`セクション重複確認（`grep -c "^## Review$"`） | 0（追記前） | 前回セクションが規定どおり削除されていたことを確認。本追記により新たに1セクションのみ生成される。 |

### Summary

Iteration 1のMajor所見（traceability.jsonがunsafe-grepのCI恒久化を「U1引き渡し分に包含済み」と虚偽記載していた点）は、cicd-pipeline.mdへの項目4新規追加とtraceability.jsonのtarget修正により解消を確認した。U1側cicd-pipeline.mdの実文言を直接grep検索で再確認し、修正後の記述が事実と一致することを裏付けた。新規のCritical/Major/Minor所見はなし。Critical 0件・Major 0件のためREADYと判定する。
