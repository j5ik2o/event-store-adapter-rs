# CI/CDパイプライン設計 — u3-ci-quality (cicd-pipeline)

U3（CI品質保証）の配信パイプライン設計 — 本ユニットがCI恒久化の実装主体であり、U1/U2の引き渡し要求仕様（両ユニットの `infrastructure-design/cicd-pipeline.md`）をここで実装形に確定する。インフラ仕様（`infrastructure-specification.md`）・モニタリング設計（`monitoring-design.md`）・セキュリティ設計（`../nfr-design/security-design.md`）・技術スタック決定（`../nfr-requirements/tech-stack-decisions.md`）・契約（`../../../inception/contract-design/contract-summary.md` C-3）に基づく。

## 変更後のパイプライン（U3完了時の姿）

| 段階 | 実体 | 内容 | U3での変更 |
|---|---|---|---|
| Lint | `ci.yml` `lint` | nightly rustfmt `cargo fmt -- --check` | 不変更 |
| Test | `ci.yml` `test-lib` | stable `cargo test --all-features`（Docker込み・必須チェック） | 不変更 |
| **Feature Matrix** | `ci.yml` `feature-matrix`（新規） | 6構成 — 未指定／sqlite／sqlite-system: ビルド＋テスト（Docker不要）、dynamodb／bigtable／全feature: ビルドのみ（Q1=A）。＋依存グラフ検査（`cargo tree -e normal` でクラウドSDK不在・hashlink不在）＋unsafe grep 0件 | **追加** |
| **Clippy** | `ci.yml` `clippy`（新規） | stable `cargo clippy --workspace --all-targets -- -D warnings` | **追加** |
| **Audit** | `ci.yml` `audit`（新規） | `cargo deny check advisories licenses`（`deny.toml` 新規） | **追加** |
| バージョン確定 | `lib-bump-version.yml` | workflow_dispatch／日次cron起点の自動semver判定・タグpush（mainマージ即時ではない） | 不変更 |
| 公開 | `lib-release.yml` | `v*` タグ → `cargo publish` | 不変更 |

- トリガー: 新規3ジョブとも既存 `ci.yml` の `on:`（PR・main push・日次cron）をそのまま共有（Q2=A確定 — 新規ワークフローファイルは作らず `ci.yml` に追加）
- ブランチ保護: 変更しない（必須チェックは `test-lib` のまま。新規ジョブの必須化は運用判断としてスコープ外 — U2設計の整理を踏襲）
- シークレット: 追加なし（NFR-4.11）

## 引き渡し要求仕様の実装対応表

| 引き渡し元 | 要求 | 実装先 |
|---|---|---|
| U1 | featureマトリクス（未指定/各単独/全feature） | `feature-matrix` ジョブ（6構成） |
| U1 | 依存グラフ検査（クラウドSDK不在） | `feature-matrix` 内の `cargo tree` 検査 |
| U1 | clippy `-D warnings` | `clippy` ジョブ |
| U1 | 依存監査（cargo-deny advisories） | `audit` ジョブ |
| U2 | sqlite系2構成の追加（sqlite／sqlite-system＋libsqlite3-dev） | `feature-matrix` の2構成 |
| U2 | hashlink不在検査 | `feature-matrix` 内の `cargo tree` 検査 |
| U2 | sqliteユニットスコープテストコマンド | `feature-matrix` のsqlite構成テスト実行 |
| U2 | unsafe不在検査（grep 0件） | `feature-matrix` 内のgrepステップ |

## ロールバック手順

- CI設定の変更はワークフローファイルのみ: 問題があれば `git revert` で即時復元（リリースフローに影響なし）
- `audit` ジョブが既知の未対応advisoriesで恒常的に失敗する場合は、`deny.toml` に理由コメント付きignoreを追加して緑化し、対応をバックログ化（黙殺しない — NFR-4.11）

## Assumptions & Open Questions

None.

## Review

**Verdict:** READY
**Reviewer:** aidlc-architecture-reviewer-agent
**Date:** 2026-08-23T07:27:14Z
**Iteration:** 1

### Findings

新規Critical/Major/Minor所見なし。

### Validation Tool Results

| Tool/確認 | 結果 | 解釈 |
|---|---|---|
| nfr-design(u3) iteration 1のMinor所見（Docker依存テストの実現機構未明記）の解消確認 | PASS | Q1=A（dynamodb/bigtable/全featureはビルド検証のみ、テスト実行は未指定/sqlite/sqlite-system＋既存test-libに限定）がcicd-pipeline.md「変更後のパイプライン」表・infrastructure-design-questions.md・infrastructure-specification.mdの3箇所すべてで一貫して反映されており、`lib.rs`の実cfgゲート（`#[cfg(all(test, feature = "dynamodb"))]`等）と矛盾しない分担になっている。 |
| U1「U3への引き渡し要求仕様」（3項目）との突合 | PASS | featureマトリクス／依存グラフ検査／「clippy・依存監査」の3項目が、実装対応表で4行（clippyと依存監査を分割表記）に展開されているが、内容の欠落・水増しはない。 |
| U2「U3への引き渡し要求仕様（U2完了時点の追加分）」（4項目）との突合 | PASS | sqlite系2構成追加／hashlink不在検査／sqliteユニットスコープテストコマンド／unsafe不在検査の4項目すべてが実装対応表に1:1で反映されている。 |
| `lib-bump-version.yml`トリガー記述の再確認（過去のU1 Major所見の非再発チェック） | PASS | 「workflow_dispatch／日次cron起点の自動semver判定・タグpush（mainマージ即時ではない）」と正確に記載されており、U1レビューで是正済みの誤記が再発していない。 |
| `.github/workflows/ci.yml`の`runs-on: ubuntu-latest`確認 | PASS | infrastructure-specification.mdの「実行基盤: GitHub Actions（ubuntu-latestランナー）」の記載と一致。 |
| `python3 -c "import json; json.load(...)"` によるtraceability.jsonのパース検証 | PASS | 構文エラーなし。6件のNFR ID（NFR-2.5, 2.6, 3.3, 4.9, 4.10, 4.11）はnfr-design(u3)（iteration 1でREADY確定済み）の6 IDと完全一致し、target記述も本ユニット内の実在する節を指している。 |
| ステージ定義の tabular 要求（Deployment/Infrastructure Services/Monitoring各表）への適合確認 | PASS | infrastructure-specification.mdのDeployment（`\| Facet \| Choice \| Rationale \|`）・Infrastructure Services（`\| Service \| Role \| Configuration \| Notes \|`）、monitoring-design.mdのMetrics&KPIs・Alerts（規定の列構成）はいずれもステージ定義の表形式に適合。Shared InfrastructureとSLIs/SLOsは該当なしと明記（クラウドインフラ非存在・ライブラリという実態と整合し、他の判断でも同様のN/A表記パターンが確立済み）。 |

### Summary

nfr-design(u3) iteration 1で指摘したMinor所見（Docker依存テストの実現機構未明記）は、Q1=A（3構成はビルド検証のみに限定）としてcicd-pipeline.md・infrastructure-design-questions.md・infrastructure-specification.mdの3ファイルで一貫して解消されており、実際のcfgゲート構成とも矛盾しません。U1・U2からの引き渡し要求仕様（合計7項目）はいずれも実装対応表に過不足なく反映され、過去レビューで是正済みのlib-bump-version.ymlトリガー誤記も再発していません。traceability.jsonの6 ID・tabular構造要求もいずれも適合しています。Critical 0件・Major 0件・Minor 0件のためREADYと判定します。
