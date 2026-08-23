# 実現性・制約分析 質問票 (feasibility-questions)

前提コンテキスト: インテントステートメント（`../intent-capture/intent-statement.md`）で SQLite バックエンド追加と feature 3分割（dynamodb/bigtable/sqlite、Memory常時有効、デフォルトなし）が確定。build-vs-buy 評価（`../market-research/build-vs-buy.md`）で自作一択が確定。既存コードは `StorageBackend` / `EventStore` トレイト（async_trait ベース）を中心に構成されている。

## Q1. SQLiteドライバ（依存クレート）の選定に制約はありますか？

既存バックエンドはすべて async（async_trait）で動作します。SQLiteアクセスの実現方式は後続の設計工程で決めますが、依存の増やし方に関する制約をここで確認します。

A. 制約なし — 実装に最適なドライバを設計工程で自由に選んでよい（sqlx / rusqlite 等）
B. 依存は最小限に — SQLiteドライバ1つのみ許容。大型フレームワーク系依存（ORM等）は不可
C. 特定のドライバを指定したい（Xで指定してください）
D. まだ決めていない（Not yet defined）
X. Other (please specify)

[Answer]: B. 依存は最小限に — SQLiteドライバ1つのみ許容。大型フレームワーク系依存（ORM等）は不可
## Q2. SQLiteのビルド方式・対応プラットフォームに制約はありますか？

A. バンドル方式（クレートにSQLite同梱、`bundled` 相当）を前提としてよい — 利用者のシステムにSQLite不要。macOS/Linux/Windows対応
B. システムのSQLiteライブラリへのリンクを基本としたい
C. 両対応（featureで選べるように）したい
D. まだ決めていない・設計工程に委ねる（Not yet defined）
X. Other (please specify)

[Answer]: C. 両対応（featureでバンドル/システムを選べるように）したい
## Q3. 規制・コンプライアンス要件（PCI/HIPAA/GDPR/データレジデンシ等）はありますか？

本件はOSSライブラリであり、ライブラリ自体が個人情報等を扱うわけではないという理解です。

A. なし — OSSライブラリでありライブラリ自体への規制要件はない（利用者側の責務）
B. あり（Xで具体的に指定してください）
C. 分からない（Not identified）
X. Other (please specify)

[Answer]: A. なし — OSSライブラリでありライブラリ自体への規制要件はない（利用者側の責務）
## Q4. タイムライン・優先度の制約はありますか？

A. 特になし — 品質優先で進める
B. 早く使いたい — 直近のCLIツール開発で必要なため、早期のリリースを優先したい
C. 特定の期日がある（Xで指定してください）
D. まだ決めていない（Not yet defined）
X. Other (please specify)

[Answer]: A. 特になし — 品質優先で進める
## Q5. 既存リポジトリ・CI環境に関する組織的制約はありますか？（現在 `sqlite` ブランチで作業中、CIは GitHub Actions の `ci.yml` 等が存在）

A. 特になし — 既存CI（GitHub Actions）にfeatureマトリクスを追加する変更も自由に行ってよい
B. CI変更は最小限にしたい（既存ジョブ構成を維持し、追加は必要最小限）
C. 進行中の他ブランチ・PRとの競合に注意が必要（Xで具体的に指定してください）
D. 分からない（Not identified）
X. Other (please specify)

[Answer]: A. 特になし — 既存CI（GitHub Actions）にfeatureマトリクスを追加する変更も自由に行ってよい
## Q6. サポートするRustバージョン（MSRV）や既存APIの互換性に制約はありますか？（現在 edition 2021、rust-version 指定なし）

A. 特になし — 現行の安定版Rustでビルドできればよい。既存の公開API（トレイトシグネチャ）は変更しない前提
B. MSRVを明示したい（Xでバージョンを指定してください）
C. 既存APIの変更も許容する（feature化に伴うモジュール再編で公開パスが変わってもよい — デフォルトなし化で既に破壊的変更のため）
D. まだ決めていない（Not yet defined）
X. Other (please specify)

[Answer]: A. 特になし — 現行の安定版Rustでビルドできればよい。既存の公開API（トレイトシグネチャ）は変更しない前提

## Consolidated Summary Confirmation

全回答の統合サマリー（Q1〜Q6）を提示し、アーティファクト生成前の確認を行う。

Does this all look correct before I generate the artifact?

- Looks correct
- Request changes

[Answer]: Looks correct