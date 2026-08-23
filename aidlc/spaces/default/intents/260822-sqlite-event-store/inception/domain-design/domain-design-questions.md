# ドメイン設計 質問票 (domain-design-questions)

前提: 要件定義書（`../requirements-analysis/requirements.md`）、ストーリー（`../user-stories/stories.md`）、コード知識ベース（`aidlc/spaces/default/codekb/sqlite/architecture.md` / `component-inventory.md`）、チームプラクティス（`../practices-discovery/team-practices.md`）。コンポーネント境界の大枠は既存構成（CoreTypes / StorageAbstraction / 各バックエンド / KeyResolver / Serialization）を踏襲する。設計判断が割れる3点のみ確認する。

## Q1. バックエンド中立なエラー型（FR-2.4、TD-01解消）の再設計方向はどれにしますか？

現状 `EventStoreWriteError::OptimisticLockError` は AWS SDK の `TransactionCanceledException` をラップしています。中立化の形として:

A. ユニットバリアント化 — `OptimisticLockError` を情報なしの最小形にする（最も単純。競合の詳細はログ・tracingに委ねる）
B. 軽量コンテキスト付き — 集約ID・期待バージョン等の文字列コンテキストを保持する中立バリアントにする（デバッグ性が高い。SDK型には依存しない）
C. 提案がほしい（Not yet defined）
X. Other (please specify)

[Answer]: B. 軽量コンテキスト付き — 集約ID・期待バージョン等の文字列コンテキストを保持する中立バリアントにする
## Q2. SQLiteドライバ（1クレートのみ・ORM不可・バンドル/システム両対応が制約）はどれにしますか？

A. rusqlite — 同期ドライバ。async文脈では spawn_blocking で分離実行。`bundled` / システムリンクの両featureが成熟しており依存が最小（推奨）
B. sqlx（sqlite機能） — 純asyncだが依存ツリーが大きく、依存最小の制約と緊張する
C. ここでは決めず後続工程（機能設計）に委ねる
D. 提案がほしい（Not yet defined）
X. Other (please specify)

[Answer]: A. rusqlite — 同期ドライバをspawn_blockingで非同期化。bundled/システムリンク両featureで両対応制約を充足
## Q3. モジュール構成はどうしますか？

A. 既存踏襲 — フラットな `lib/src/*.rs` を維持し、`event_store_for_sqlite.rs` / `event_store_for_sqlite_test.rs` を追加（変更最小・チームプラクティスのファイル命名パターンどおり）
B. ディレクトリ再編 — バックエンドを `lib/src/backends/` 等のディレクトリへ再編する（整理は進むが差分が大きい）
X. Other (please specify)

[Answer]: A. 既存踏襲 — フラットな lib/src/*.rs に event_store_for_sqlite.rs / event_store_for_sqlite_test.rs を追加

## Consolidated Summary Confirmation

全回答の統合サマリー（Q1〜Q3）を提示し、コンポーネントカタログ生成前の確認を行う。

Does this all look correct before I generate the artifact?

- Looks correct
- Request changes

[Answer]: Looks correct