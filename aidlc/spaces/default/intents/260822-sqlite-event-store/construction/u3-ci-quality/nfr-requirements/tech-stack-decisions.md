# 技術スタック決定 — u3-ci-quality (tech-stack-decisions)

要件定義書（`../../../inception/requirements-analysis/requirements.md` FR-5.1〜FR-5.3）・契約（`../../../inception/contract-design/contract-summary.md` C-3 — CIマトリクスの共通軸）・技術スタック台帳（`aidlc/spaces/default/codekb/sqlite/technology-stack.md`）に基づく、U3（CI品質保証）のツール選定と根拠。

## 決定一覧

| # | 決定 | 根拠 |
|---|---|---|
| D1 | 依存監査は **cargo-deny**（`cargo deny check advisories licenses`）。設定は `deny.toml` をリポジトリルートに置く | Q1=A確定／FR-5.3／team.md Q8の文言に整合。advisories（RUSTSEC）に加えlicensesも一括検査 |
| D2 | 監査ジョブは `ci.yml` に追加し、既存トリガー（PR・main push・日次cron `0 0 * * *`）すべてで実行する | Q2=A確定／Renovate automergeへの脆弱性ゲート／新規開示CVEの日次検出 |
| D3 | clippy は `cargo clippy --workspace --all-targets -- -D warnings`（stable toolchain）を `ci.yml` に追加 | FR-5.2／team.md Q6確定 |
| D4 | featureマトリクスは C-3 の feature表を軸に6構成（未指定／dynamodb／bigtable／sqlite／sqlite-system／全feature）。sqlite-system は ubuntu ランナーに `libsqlite3-dev` を導入して検証。依存グラフ検査（クラウドSDK不在・hashlink不在）と unsafe grep も同ジョブ群で実行 | FR-5.1／U1・U2引き渡し要求仕様（cicd-pipeline.md 両ユニット） |
| D5 | マトリクスの実行は「ビルド＋Docker不要テスト」を基本とし、testcontainers前提の統合テストは既存 `test-lib`（--all-features）に残す — マトリクス側で重複実行しない | CI時間の抑制／既存ジョブとの責務分担 |
| D6 | 新しいGitHub Actionsアクションは最小限（cargo-deny は公式の EmbarkStudios/cargo-deny-action または `cargo install` 実行のいずれか — 実装時にランナーキャッシュ効率で選定）。新規シークレットなし | NFR-4.11／既存ワークフロー慣行 |

## 派生NFR要件（ビルド・互換）

- **NFR-3.3（マトリクスの網羅）**: CIのfeatureマトリクスは C-3 の6構成を網羅し、いずれの構成でもビルド（および該当するDocker不要テスト）が通ること（U1のNFR-3.1／U2のNFR-3.2のCI恒久化）

## 適用外の説明

- MSRV宣言・検証: 確定済みのスコープ外（team.md — バックログ）
- examplesのビルド検証: 既知の欠落としてバックログ（team.md）

## Assumptions & Open Questions

None.
