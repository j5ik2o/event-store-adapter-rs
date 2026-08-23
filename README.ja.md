# event-store-adapter-rs

[![Workflow Status](https://github.com/j5ik2o/event-store-adapter-rs/workflows/ci/badge.svg)](https://github.com/j5ik2o/event-store-adapter-rs/actions?query=workflow%3A%22ci%22)
[![crates.io](https://img.shields.io/crates/v/event-store-adapter-rs.svg)](https://crates.io/crates/event-store-adapter-rs)
[![docs.rs](https://docs.rs/event-store-adapter-rs/badge.svg)](https://docs.rs/event-store-adapter-rs)
[![Renovate](https://img.shields.io/badge/renovate-enabled-brightgreen.svg)](https://renovatebot.com)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![tokei](https://tokei.rs/b1/github/j5ik2o/event-store-adapter-rs)](https://github.com/XAMPPRocky/tokei)

このライブラリは、CQRS/Event Sourcing用のEvent Storeを複数のストレージバックエンド（DynamoDB・Google Cloud Bigtable・SQLite・インメモリ）で提供します。

[English](./README.md)

## バックエンドとCargo feature

デフォルトで有効になるバックエンドはありません。使用するバックエンドをCargo featureで指定してください:

| Feature | バックエンド | 有効になるもの |
|:--------|:------------|:--------------|
| `dynamodb` | Amazon DynamoDB（`EventStoreForDynamoDB`） | `aws-config` / `aws-sdk-dynamodb` |
| `bigtable` | Google Cloud Bigtable（`EventStoreForBigtable`） | `tonic` / `googleapis-tonic-google-bigtable-v2` |
| `sqlite` | バンドル版SQLite（`EventStoreForSqlite`） | `rusqlite` の `bundled` feature — SQLiteをビルドに同梱するため、システム側のSQLiteは不要 |
| `sqlite-system` | システムライブラリ版SQLite（`EventStoreForSqlite`） | `rusqlite`（`bundled` なし）— システムにインストール済みのSQLiteへリンク |
| （なし） | インメモリ（`EventStoreForMemory`） | feature指定不要で常時利用可能 |

```toml
[dependencies]
event-store-adapter-rs = { version = "<latest>", features = ["sqlite"] }
```

注記:

- デフォルトfeatureがないため、既存の利用者はアップグレード時に明示的な `features = [...]` の指定が必要です（[1.x からの移行](#1x-からの移行)を参照）。
- `sqlite` と `sqlite-system` を併用した場合は**バンドル版**が優先されます。これはCargo featureの加算性の帰結です（`sqlite` が `rusqlite/bundled` を有効化し、featureは追加のみで打ち消せないため）。システムSQLiteへリンクしたい場合は `sqlite-system` のみを有効にしてください。

## 使い方

イベントストアを使えば、Event Sourcing対応リポジトリを簡単に実装できます。以下はSQLiteバックエンド（`features = ["sqlite"]`）を使う例です:

```rust
use event_store_adapter_rs::types::{Aggregate, EventStore, EventStoreReadError, EventStoreWriteError};
use event_store_adapter_rs::EventStoreForSqlite;

pub struct UserAccountRepository {
  event_store: EventStoreForSqlite<UserAccountId, UserAccount, UserAccountEvent>,
}

impl UserAccountRepository {
  pub async fn store_event(&mut self, event: &UserAccountEvent, version: usize) -> Result<(), RepositoryError> {
    let result = self.event_store.persist_event(event, version).await;
    match result {
      Ok(_) => Ok(()),
      Err(err) => Err(Self::handle_event_store_write_error(err)),
    }
  }

  pub async fn store_event_and_snapshot(
    &mut self,
    event: &UserAccountEvent,
    snapshot: &UserAccount,
  ) -> Result<(), RepositoryError> {
    let result = self.event_store.persist_event_and_snapshot(event, snapshot).await;
    match result {
      Ok(_) => Ok(()),
      Err(err) => Err(Self::handle_event_store_write_error(err)),
    }
  }

  pub async fn find_by_id(&self, id: &UserAccountId) -> Result<Option<UserAccount>, RepositoryError> {
    let snapshot_result = self.event_store.get_latest_snapshot_by_id(id).await;
    match snapshot_result {
      Ok(snapshot_opt) => match snapshot_opt {
        Some(snapshot) => {
          let events = self
            .event_store
            .get_events_by_id_since_seq_nr(id, snapshot.seq_nr() + 1)
            .await;
          match events {
            Ok(events) => Ok(Some(UserAccount::replay(events, snapshot))),
            Err(err) => Err(Self::handle_event_store_read_error(err)),
          }
        }
        None => Ok(None),
      },
      Err(err) => Err(Self::handle_event_store_read_error(err)),
    }
  }
}
```

以下はSQLiteでのリポジトリの使用例です。ストアはデータベースファイル（または `:memory:`）に永続化し、必要なテーブル・インデックスは構築時にライブラリが自動作成します — 利用者側のDDLは不要です:

```rust
// ファイルDB。`:memory:` を使う場合は EventStoreForSqlite::new_in_memory()
let event_store = EventStoreForSqlite::new("user-account.db")?;

let mut repository = UserAccountRepository::new(event_store);

// Replay the aggregate from the event store
let mut user_account = repository.find_by_id(&user_account_id).await?.unwrap();

// Execute a command on the aggregate
let user_account_event = user_account.rename("new-name").unwrap();

// Store the new event without a snapshot
repository
  .store_event(&user_account_event, user_account.version())
  .await?;
// Store the new event with a snapshot
// repository
//   .store_event_and_snapshot(&user_account_event, &user_account)
//   .await?;
```

実行可能な完全なサンプルは [examples/user-account-sqlite](examples/user-account-sqlite) です（`cargo run -p example-user-account-sqlite` — クラウド接続・Docker不要）。

`features = ["dynamodb"]` の場合は、ストアの構築部分を置き換えるだけで同じリポジトリがDynamoDBに対して動作します:

```rust
let event_store = EventStoreForDynamoDB::new(
  aws_dynamodb_client.clone(),
  journal_table_name.to_string(),
  journal_aid_index_name.to_string(),
  snapshot_table_name.to_string(),
  snapshot_aid_index_name.to_string(),
  64,
);
```

実行可能なDynamoDBのサンプルは [examples/user-account](examples/user-account) です。

### SQLiteのサポート境界

- サポートされる共有単位は**1つのストアインスタンスとそのクローン**です（クローンは基底の接続を共有します）。同一のデータベースファイルを複数のストアインスタンスや複数プロセスから同時に開くことは**サポート外**です。マルチプロセスでの同時アクセスの防止（例: CLIツールの多重起動防止）はアプリケーション側の責務です。
- インメモリストア（`new_in_memory`）はそのインスタンスとクローンの間でのみ共有され、最後のクローンがdropされた時点で消えます。

## 1.x からの移行

次のメジャーリリースには破壊的変更が含まれます（[CHANGELOG.md](CHANGELOG.md)を参照）。

### 1. バックエンドはopt-inのCargo featureに

1.x ではすべてのバックエンドが常にコンパイルされていました。現在はデフォルトfeatureがないため、使用するバックエンドを指定してください:

```toml
# Before（1.x）
[dependencies]
event-store-adapter-rs = "1"

# After — 使用するバックエンドを指定。"<latest>" はcrates.ioの最新バージョン
[dependencies]
event-store-adapter-rs = { version = "<latest>", features = ["dynamodb"] }
```

インメモリバックエンド（`EventStoreForMemory`）はfeature指定不要で常時利用可能です。

### 2. エラー型の変更

| 項目 | Before（1.x） | After |
|:-----|:-------------|:------|
| `EventStoreWriteError::OptimisticLockError` | AWS SDKの型（`TransactionCanceledException` を `TransactionCanceledExceptionWrapper` 経由で内包） | バックエンド中立な `String` メッセージ: `optimistic lock failed, aid=<id>, expected_version=<n>[, actual_version=<m>]` |
| インメモリバックエンドの失敗時挙動 | 一部の操作がpanicしていた（例: 未サポートのcreate） | `Err(EventStoreWriteError` / `EventStoreReadError)` を返す — panicしない |

`OptimisticLockError(cause)` をマッチしてAWS SDKのエラーを検査していた場合は、メッセージ文字列を使う形（またはペイロードを検査せずバリアントのみ処理する形）へ切り替えてください。楽観的ロック失敗時の再試行が呼び出し側の責務である点は変わりません。

## テーブル仕様

[docs/DATABASE_SCHEMA.ja.md](docs/DATABASE_SCHEMA.ja.md)を参照してください。なお、SQLiteのテーブルはライブラリが自動作成します。ドキュメントの記載は情報提供です。

## CQRS/Event Sourcing サンプル

[j5ik2o/cqrs-es-example-rs](https://github.com/j5ik2o/cqrs-es-example-rs)を参照してください。

## 他の言語のための実装

- [for Java](https://github.com/j5ik2o/event-store-adapter-java)
- [for Scala](https://github.com/j5ik2o/event-store-adapter-scala)
- [for Kotlin](https://github.com/j5ik2o/event-store-adapter-kotlin)
- [for Rust](https://github.com/j5ik2o/event-store-adapter-rs)
- [for Go](https://github.com/j5ik2o/event-store-adapter-go)
- [for JavaScript/TypeScript](https://github.com/j5ik2o/event-store-adapter-js)
- [for .NET](https://github.com/j5ik2o/event-store-adapter-dotnet)
- [for PHP](https://github.com/j5ik2o/event-store-adapter-php)
