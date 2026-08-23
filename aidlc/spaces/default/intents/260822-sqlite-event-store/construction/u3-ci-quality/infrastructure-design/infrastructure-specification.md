# インフラ仕様 — u3-ci-quality (infrastructure-specification)

U3（CI品質保証）の「インフラ」= GitHub Actions のCI実行基盤。セキュリティ設計（`../nfr-design/security-design.md`）・技術スタック決定（`../nfr-requirements/tech-stack-decisions.md` D1〜D6）・契約（`../../../inception/contract-design/contract-summary.md` C-3 feature表）に基づく。クラウドインフラ・デプロイ環境は存在しない（OSSクレート — 「デプロイ」= crates.io公開はU1設計で確定済みの既存フロー）。機能設計成果物はU3ではキンド適用で存在しない（機能設計工程で確認済み）。

## Deployment（CI実行基盤の構成）

| Facet | Choice | Rationale |
|---|---|---|
| 実行基盤 | GitHub Actions（`ubuntu-latest` ランナー） | 既存 `ci.yml` と同一。新規ランナー種別は導入しない |
| ツールチェーン | stable（テスト・clippy・マトリクス）／nightly（既存lintのrustfmtのみ） | 既存慣行の維持。clippyはstableで実行（team.md Q6） |
| 追加システムパッケージ | `sqlite-system` 構成のジョブのみ `libsqlite3-dev` を apt 導入 | AC2.3.2（システムリンク検証）の前提 |
| cargo-deny の導入方式 | 実装時に EmbarkStudios/cargo-deny-action と `cargo install cargo-deny` を比較し実行時間の短い方を採用 | D6（新規アクション最小限・実装時選定） |
| ビルドキャッシュ | 導入しない（既存ジョブと同じ素の実行） | D6の最小方針。CI時間が問題化したらバックログで導入検討 |
| 環境 | 単一（CIのみ）。dev/staging/prod の区別なし | ライブラリのため環境レイヤなし |

## Infrastructure Services（ジョブ構成）

| Service（ジョブ） | Role | Configuration | Notes |
|---|---|---|---|
| `lint`（既存） | フォーマット検査 | nightly rustfmt `cargo fmt -- --check` | 不変更 |
| `test-lib`（既存） | 全機能テスト | stable `cargo test --all-features`（Docker/testcontainers込み） | 不変更。ブランチ保護の必須チェック |
| `feature-matrix`（新規） | featureマトリクス検証 | 6構成。**未指定／sqlite／sqlite-system = ビルド＋テスト実行（Docker不要）、dynamodb／bigtable／全feature = ビルド検証のみ**（Q1=A確定 — test-libとの重複ゼロ）。依存グラフ検査（クラウドSDK不在・hashlink不在）とunsafe grepを含む | FR-5.1／NFR-2.6/3.3/4.10 |
| `clippy`（新規） | 静的解析 | stable `cargo clippy --workspace --all-targets -- -D warnings` | FR-5.2／NFR-4.9 |
| `audit`（新規） | 依存監査 | `cargo deny check advisories licenses`（`deny.toml` 準拠） | FR-5.3／NFR-2.5。全トリガー（PR・main push・日次cron）で実行 |

## Shared Infrastructure

該当なし（単一リポジトリのCI設定のみ。ユニット間で共有するインフラ資源は存在しない）。

## Assumptions & Open Questions

None.
