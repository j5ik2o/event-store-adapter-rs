# 市場調査 質問票 (market-research-questions)

前工程のインテントステートメント（`../intent-capture/intent-statement.md`）より: 本イニシアチブは event-store-adapter-rs への SQLite バックエンド追加と `dynamodb` / `bigtable` / `sqlite` の feature 3分割であり、主な利用者は crates.io 経由の外部 OSS 利用者、直近のユースケースは CLI ツールでの利用である。

## Q1. 競合となる Rust のイベントソーシング系クレート（cqrs-es, esrs, eventually, thalo 等）をどの程度意識しますか？

A. 意識する — 競合比較を行い、差別化ポイントを明確にしたい（Web調査に基づく比較表を作成）
B. 部分的に意識する — SQLite対応の有無など、今回の機能に関わる範囲だけ比較したい
C. 意識しない — 本ライブラリは自分のエコシステム・思想（event-store-adapter-*ファミリー）向けであり、競合比較は参考程度でよい
D. 分からないので調査して提案してほしい（Not yet defined）
X. Other (please specify)

[Answer]: C. 意識しない — 本ライブラリは自分のエコシステム・思想（event-store-adapter-*ファミリー）向けであり、競合比較は参考程度でよい
## Q2. 本ライブラリの差別化ポイント（強み）をどこに置きますか？

A. 多言語ファミリー展開（event-store-adapter-scala/kotlin/go/ts/php/rust 等で同一設計を提供）と設計の一貫性
B. バックエンドの選択肢の広さ（DynamoDB / Bigtable / SQLite / Memory を同一トレイトで切替可能）
C. シンプルさ — フレームワークではなく軽量アダプタであること（CQRS/ESフレームワークを強制しない）
D. まだ定義していない（Not yet defined）
X. Other (please specify)

[Answer]: X. Other — 「アクターモデルでなくてもCQRS/ESができるって話です」（アクターモデルを前提とせずにCQRS/イベントソーシングを実現できることが差別化ポイント）
## Q3. SQLite対応で利用者が「当然あるもの」と期待するテーブルステークスは何だと考えますか？（select all that apply）

A. 単一ファイルDB・組み込み動作（サーバプロセス不要、`:memory:` モード含む）
B. 既存バックエンドと完全に同一のトレイト/APIで動くこと（コード変更なしで切替可能）
C. マイグレーション（テーブル自動作成またはスキーマDDLの提供）
D. 楽観的ロック・スナップショットなど既存機能との完全互換
X. Other (please specify)

[Answer]: A, B, C, D（すべて: 単一ファイルDB・組み込み動作 / 同一トレイト・API / マイグレーション / 既存機能との完全互換）
## Q4. 関連する市場・技術トレンドとして考慮すべきものはありますか？

A. SQLite再評価のトレンド（ローカルファースト、エッジ、Litestream/Turso等の周辺エコシステム拡大）を追い風として位置づける
B. Rust製CLI/デスクトップツールの増加を追い風として位置づける
C. AとB両方
D. トレンドは特に考慮しない（None）
X. Other (please specify)

[Answer]: B. Rust製CLI/デスクトップツールの増加を追い風として位置づける
## Q5. 「作る」以外の選択肢（build-vs-buy）は検討済みですか？ 例: 既存のSQLite対応イベントソーシングクレートを利用者に案内する、他クレートに依存して実装する等

A. 検討済み — 自作一択（本ライブラリのトレイト設計に合わせるには自前実装が必要）
B. 未検討 — 有力な既存実装があるなら流用・依存も検討したい（調査してほしい）
C. 未検討だが方針は自作 — 外部依存を増やしたくない（SQLiteドライバ以外の依存は不可）
D. まだ決めていない（Not yet defined）
X. Other (please specify)

[Answer]: A. 検討済み — 自作一択（本ライブラリのトレイト設計に合わせるには自前実装が必要）
## Q6. 対象とする利用者規模・到達目標のイメージはありますか？

A. 特に数値目標はない — 品質と自分のユースケース充足が第一（OSSとしての自然な普及に任せる）
B. ダウンロード数や GitHub Star 等の定量目標を置きたい
C. 特定コミュニティ（Rust CQRS/ES ユーザー）内での認知・採用を目標にしたい
D. 定義しない（Not applicable）
X. Other (please specify)

[Answer]: A. 特に数値目標はない — 品質と自分のユースケース充足が第一（OSSとしての自然な普及に任せる）

## Consolidated Summary Confirmation

全回答の統合サマリー（Q1〜Q6）を提示し、アーティファクト生成前の確認を行う。

Does this all look correct before I generate the artifact?

- Looks correct
- Request changes

[Answer]: Looks correct