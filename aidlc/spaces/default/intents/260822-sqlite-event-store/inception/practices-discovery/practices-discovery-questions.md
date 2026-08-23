# プラクティス発見 インタビュー質問票 (practices-discovery-questions)

リードのドラフト（team-practices.md / evidence.md）と3件の独立検分（contributions/）で確定できなかった項目のみを確認する。証拠が示唆する答えはデフォルトとして提示するが、チームの意思は人間の判断に委ねる。

## Q1. [Way of Working] main へのマージ方式はどうしますか？（現状の履歴はマージコミット方式。この開発フローの既定は squash-merge）

A. 現状どおりマージコミット方式を継続する
B. squash-merge に統一する（1変更=1コミットのきれいな main 履歴）
C. どちらでもよい／自動化に任せる
X. Other (please specify)

[Answer]: A. 現状どおりマージコミット方式を継続する
## Q2. [Walking Skeleton] 薄いエンドツーエンドの1本を最初に作りますか？ ウォーキングスケルトンとは、全体を最初から最後まで貫いて動く最小版を最初に作り、部品がつながることを実機能の前に証明するやり方です。

A. はい — SQLite対応を薄い1本（最小のイベント永続化＋読出し）から始めて土台を証明する
B. いいえ — 通常の作業順で進める（スケルトン儀式は不要）
X. Other (please specify)

[Answer]: A. はい — SQLite対応を薄い1本（最小のイベント永続化＋読出し）から始めて土台を証明する
## Q3. [Testing Posture] テストカバレッジの目標・計測はどうしますか？（現状: 計測ツール未導入。品質検分では「数値床が検証不能」との指摘）

A. 目標を設けない — 現状どおり計測ツールなし。テストの質は同等テスト・競合テストの充足で担保する
B. 計測を導入し80%目標を設ける（cargo-llvm-cov 等をCIに追加）
C. 計測だけ導入する（目標値は設けない）
X. Other (please specify)

[Answer]: A. 目標を設けない — 現状どおり計測ツールなし。テストの質は同等テスト・競合テストの充足で担保する
## Q4. [Testing Posture] 楽観的ロックの競合パステスト（同時更新で OptimisticLockError が返ることの検証）をSQLite実装の必須テストに含めますか？（品質検分の勧告: 既存バックエンドには競合パスの直接テストがない）

A. 含める — SQLiteでは競合パスとエラー契約のテストを必須とする（推奨）
B. 含めない — 既存同等のハッピーパステストのみとする
X. Other (please specify)

[Answer]: A. 含める — SQLiteでは競合パスとエラー契約のテストを必須とする
## Q5. [Deployment] リリースフローはどうしますか？（現状: マージ→自動バージョンバンプ→自動タグ→crates.io自動publish の完全自動）

A. 現状維持 — 完全自動フローを継続する
B. 手動承認を追加 — publish 前に人間の確認ステップを入れる
X. Other (please specify)

[Answer]: A. 現状維持 — 完全自動フローを継続する
## Q6. [Code Style] clippy をCIに導入しますか？（現状: rustfmtのみ。開発・セキュリティ検分の双方が導入を推奨）

A. 今回のスコープで導入する（`clippy -D warnings` をCIに追加）
B. 導入するが別イニシアチブで（今回のスコープには含めない）
C. 導入しない
X. Other (please specify)

[Answer]: A. 今回のスコープで導入する（`clippy -D warnings` をCIに追加）
## Q7. [Code Style] SQLiteバックエンドの公開型名はどちらにしますか？（開発検分の指摘: 裁定が必要）

A. `EventStoreForSqlite`（Rust命名慣習: 頭字語もキャメル化。DynamoDB→Dynamodbとはしない既存例との整合はXで議論可）
B. `EventStoreForSQLite`（SQLite表記を維持）
X. Other (please specify)

[Answer]: A. `EventStoreForSqlite`（Rust命名慣習: 頭字語もキャメル化）
## Q8. [Deployment/供給網] 依存監査（cargo-audit / cargo-deny 等）の導入はどうしますか？（セキュリティ検分の指摘: Renovate自動マージに対しRUSTSEC照合がなく、bundled SQLiteでC由来のCVE面が加わる）

A. 今回のスコープでCIに追加する
B. 別イニシアチブで対応する（今回は含めない）
C. 導入しない
X. Other (please specify)

[Answer]: A. 今回のスコープでCIに追加する

## Consolidated Summary Confirmation

全回答の統合サマリー（Q1〜Q8）を提示し、最終統合前の確認を行う。

Does this all look correct before I generate the artifact?

- Looks correct
- Request changes

[Answer]: Looks correct