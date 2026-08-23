# モニタリング設計 — u3-ci-quality (monitoring-design)

U3のモニタリング対象はCI自体の実行結果。アプリケーション実行時のメトリクス・ログ・トレースは存在しない（ライブラリ — NFR設計のキンド適用整理どおり）。セキュリティ設計（`../nfr-design/security-design.md` NFR-4.11）と技術スタック決定（`../nfr-requirements/tech-stack-decisions.md`）に基づく。

## Metrics & KPIs

| Metric | Source | Threshold | Why it matters |
|---|---|---|---|
| CIジョブ成否（lint / test-lib / feature-matrix / clippy / audit） | GitHub Actions チェック結果 | 全ジョブ成功 | マージ可否・品質ゲートの実効性 |
| audit検出件数（advisories / licenses違反） | `audit` ジョブのログ | 0件（違反はジョブ失敗で顕在化） | 供給網の健全性（Renovate automergeのゲート） |
| 日次cron実行の成否 | Actions実行履歴（scheduleトリガー） | 失敗時に検知 | 新規開示CVEの検出が止まっていないこと |

## Alerts

| Alert | Condition | Severity | Routes to |
|---|---|---|---|
| PR上のチェック失敗 | いずれかのジョブが失敗 | PRブロック相当（test-libは必須チェック、他はPR上で可視） | PR作成者（GitHub標準UI） |
| 日次cronの失敗 | scheduleトリガー実行の失敗（auditのCVE検出を含む） | 要対応 | リポジトリオーナーへのGitHub通知（Actions失敗メール — 追加の通知基盤は導入しない） |

## SLIs / SLOs

該当なし（ライブラリのCIに稼働率SLOは設定しない。CIの失敗は都度対応）。

## Logs & Tracing

- CI実行ログはGitHub Actionsの標準保持（追加のログ集約基盤・トレーシングは導入しない — NFR-4.11の最小構成）
- `audit` ジョブの検出詳細はジョブログで確認（deny.tomlのignoreには理由コメント必須 — 監査証跡）

## Assumptions & Open Questions

None.
