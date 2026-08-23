# デリバリ計画 質問票 (delivery-planning-questions)

前提: ユニット定義（`../units-generation/unit-of-work.md`）・依存DAG（`../units-generation/unit-of-work-dependency.md`: U1→U2→U3/U4）・ストーリー対応（`../units-generation/unit-of-work-story-map.md`）・契約（`../contract-design/contract-summary.md`）・チームプラクティス（`../practices-discovery/team-practices.md`: ウォーキングスケルトンを最初に作る＝確定）・リスク先行方針（範囲定義で確定）・実装担当はAI（開発者エージェント）。要件（`../requirements-analysis/requirements.md`）・ストーリー（`../user-stories/stories.md`）・コンポーネント（`../domain-design/components.md`）を参照。

ここでの「Bolt」は、仕事のひとまとまりを設計〜実装〜テストまで一気に通す1回のビルドパス（終わると動くものが残る単位）を指す。

## Q1. Bolt構成（ビルドの区切り方）はどちらにしますか？ ストーリーレビューから持ち越された「スケルトンの範囲確定」の裁定です。

A. **3 Bolt構成（薄いスケルトン優先— チームプラクティスの字義どおり・推奨）**
   - Bolt 1〔ウォーキングスケルトン・承認ゲート付き〕: U1の必要最小（エラー型中立化＋`sqlite` featureの器）＋U2の最小スライス（ファイルDBへの最小の永続化＋読出し）— 土台がつながることを最初に証明
   - Bolt 2: U1の残り（feature分割完成・Memory準拠化・依存削除）＋U2の残り（楽観的ロック・`:memory:`・保持ポリシー・両対応feature・テスト一式）
   - Bolt 3: U3（CI）＋U4（ドキュメント）
B. **4 Bolt構成（ユニット整合）**: Bolt 1=U1 → Bolt 2=U2 → Bolt 3=U3 → Bolt 4=U4。スケルトンの薄い1本はBolt 2冒頭の内部マイルストーンとし、Bolt単位の承認はユニット完結で行う
C. 提案を見直したい（Xで指定してください）
X. Other (please specify)

[Answer]: A. 3 Bolt構成（薄いスケルトン優先 — Bolt 1=スケルトン〔ゲート付〕、Bolt 2=残り全部、Bolt 3=CI＋ドキュメント）
## Q2. Boltの実行は直列でよいですか？（依存上はU3とU4のみ並行可能）

A. 直列 — ソロ＋AI実行のため1本ずつ確実に進める（推奨）
B. 末尾のみ並行 — CI（U3）とドキュメント（U4）は並行で実行する
X. Other (please specify)

[Answer]: A. 直列 — 1本ずつ確実に進める

## Consolidated Summary Confirmation

全回答の統合サマリー（Q1〜Q2）を提示し、Bolt計画生成前の確認を行う。

Does this all look correct before I generate the artifact?

- Looks correct
- Request changes

[Answer]: Looks correct