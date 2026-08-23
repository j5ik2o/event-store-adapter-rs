# NFR要件 質問 — u3-ci-quality

> U3（CI品質保証）のNFR要件工程の質問。featureマトリクス・clippy・依存監査の
> 導入自体は確定済み（FR-5.1〜5.3、team.md Q6/Q8）。ツール選定と実行方式の
> 残余のみを確認する。

## Q1: 依存監査ツールの選定

FR-5.3 / team.md Q8 は「cargo-audit または cargo-deny の advisories チェック」とツール選定を残しています。

A. **cargo-deny** — advisories（RUSTSEC照合）に加え licenses / bans / sources も一括検査可能。設定ファイル `deny.toml` で管理。team.mdの文言（`cargo-deny check advisories licenses`）にも整合
B. **cargo-audit** — advisories専用でシンプル。設定ファイル不要で導入最小
X. Other (please specify)

[Answer]: A. cargo-deny（advisories＋licenses、deny.toml管理）

## Q2: 依存監査の実行タイミング

team.md Q8 は「既存の日次cronに相乗りさせるのが最小コスト」としています。`ci.yml` には既に日次cron（`0 0 * * *`）があります。

A. `ci.yml` に監査ジョブを追加し、既存トリガー（PR・main push・日次cron）すべてで実行する — PR時にも新規依存の既知脆弱性を検出でき、日次cronで新規開示CVEも拾う
B. 日次cronのみで実行する（PRはブロックしない — 監査失敗でPRが止まるノイズを避ける）
X. Other (please specify)

[Answer]: A. `ci.yml` に監査ジョブを追加し、既存トリガー（PR・main push・日次cron）すべてで実行する

## Consolidated Summary Confirmation

Does this all look correct before I generate the artifact?

- Looks correct
- Request changes

[Answer]: Looks correct
