# 体制割当 (team-allocation)

「Bolt」は設計〜実装〜テストを一気に通す1回のビルドパス。Bolt計画（`bolt-plan.md`）の3 Boltすべてを、AIの開発者エージェント（aidlc-developer-agent）が実装する。本プロジェクトはソロOSSであり、チーム編成工程は該当なしのためスキップ済み — 複数チームの調整表（Program Board相当）は不要。

## 割当表

| Bolt | 実装担当 | レビュー | 承認 |
|---|---|---|---|
| bolt-skeleton | aidlc-developer-agent（AI） | 各工程の担当レビュアー（AI） | メンテナ（人間・ゲート承認） |
| bolt-full-implementation | aidlc-developer-agent（AI） | 同上 | メンテナ |
| bolt-ci-and-docs | aidlc-developer-agent（AI） | 同上 | メンテナ |

## 補足

- 意思決定者はメンテナのみ（ステークホルダーマップ確定事項）。すべてのBoltゲートはメンテナが承認する
- ウォーキングスケルトン（Bolt 1）は単独・ゲート付きで実行し、承認後に残りのBoltへ進む（チームプラクティス `../practices-discovery/team-practices.md` の確定スタンス）

## Assumptions & Open Questions

None.
