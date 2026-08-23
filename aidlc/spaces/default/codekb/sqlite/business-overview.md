# ビジネス概要 — event-store-adapter-rs

## ビジネスドメインと目的

event-store-adapter-rs は、CQRS / Event Sourcing パターンを実装するアプリケーションのための**イベントストアアダプタ**を提供する OSS Rust クレートである（crates.io 公開、v1.3.18）。ドメインイベントの永続化（Journal）と集約スナップショットの保存・復元（Snapshot）を統一トレイト `EventStore` の背後に隠蔽し、Amazon DynamoDB / Google Cloud Bigtable / インメモリの3バックエンドを同梱する。

本クレートは j5ik2o による **event-store-adapter 多言語ファミリー**（Java / Scala / Kotlin / TypeScript / Go / Rust など）の Rust 実装であり、各言語実装は同一の概念モデルを共有する: Journal / Snapshot の2テーブル構成、`version` ベースの楽観ロック、キー分散のためのシャーディング（`KeyResolver`）。

なお現在のクレートメタデータ（description: "crate to make DynamoDB an Event Store"）は DynamoDB 専用だった時期の名残で、マルチバックエンドという実態と乖離している（`code-quality-assessment.md` の TD-10 参照）。

## 主要機能

| 機能 | 内容 |
|---|---|
| イベント永続化 | `persist_event`（更新イベント専用。`version` 指定の楽観ロック付き。作成イベントは拒否） |
| イベント+スナップショット永続化 | `persist_event_and_snapshot`（作成イベントなら新規作成、以降は条件付き更新） |
| スナップショット読出し | `get_latest_snapshot_by_id`（最新スナップショットから集約を復元） |
| イベントリプレイ | `get_events_by_id_since_seq_nr`（スナップショット以降のイベントを取得し集約を再構築） |
| スナップショット保持管理 | `with_keep_snapshot_count` / `with_delete_ttl`（完全実装は DynamoDB のみ。Bigtable はサイレント無効） |
| キー分散 | `KeyResolver` / `DefaultKeyResolver`（`{type_name}-{hash%shard_count}` 形式のパーティションキー解決） |
| シリアライズ差替え | `EventSerializer` / `SnapshotSerializer`（デフォルトは JSON） |

## 対象ユーザーとユースケース

- **対象**: Rust で CQRS / Event Sourcing を採用するアプリケーション開発者。
- **典型ユースケース**: リポジトリ実装（`examples/user-account/src/user_account_repository.rs` が範例）が、書込み時に `persist_event(_and_snapshot)` を呼び、読出し時に「最新スナップショット取得 → それ以降のイベントをリプレイ」で集約を復元する。
- **アクターモデル非前提**: 兄弟プロジェクトの pekko / akka 系永続化プラグインと異なり、アクターフレームワークへの依存はない。tokio 上の任意の async Rust アプリケーションから利用できる。

## ビジネス上の設計原則

- **バックエンド可搬性**: アプリケーションコードは `EventStore` トレイトのみに依存し、バックエンドを差し替えられる。ただし現状はエラー型 `EventStoreWriteError::OptimisticLockError` が AWS SDK の型をラップしており、この原則を一部損なっている（TD-01）。
- **楽観ロックによる整合性**: 集約の `version` を条件とした CAS 書込みで並行更新を検出する。DynamoDB は TransactWriteItems により原子的、Bigtable は read→write の2段階で近似（レースウィンドウあり、TD-07）。
- **スナップショット+リプレイによる読出し効率**: 全イベントのリプレイを避け、最新スナップショットからの差分リプレイで復元コストを抑える。
- **テーブル自動作成の方針**: 本プロジェクトの方針として、スキーマ作成はライブラリの自動テーブル作成が担う（`docs/DATABASE_SCHEMA.md` のスキーマ記載は情報提供）。現状 DynamoDB の DDL は test-utils 側に置かれており、この方針との設計差分が SQLite バックエンド追加時の論点になる。
