# event-store-adapter-rs

[![Workflow Status](https://github.com/j5ik2o/event-store-adapter-rs/workflows/ci/badge.svg)](https://github.com/j5ik2o/event-store-adapter-rs/actions?query=workflow%3A%22ci%22)
[![crates.io](https://img.shields.io/crates/v/event-store-adapter-rs.svg)](https://crates.io/crates/event-store-adapter-rs)
[![docs.rs](https://docs.rs/event-store-adapter-rs/badge.svg)](https://docs.rs/event-store-adapter-rs)
[![Renovate](https://img.shields.io/badge/renovate-enabled-brightgreen.svg)](https://renovatebot.com)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![tokei](https://tokei.rs/b1/github/j5ik2o/event-store-adapter-rs)](https://github.com/XAMPPRocky/tokei)


このライブラリは、CQRS/Event Sourcing用のMemory・DynamoDBイベントストアを提供します。

> このcheckoutでは未公開の4系API（`4.0.0-alpha.0`）を開発しています。以下の例はローカルcheckoutを参照します。SQLite・Bigtableを利用する場合は3系を継続してください。v3 DynamoDBの旧データは明示的なmigration関数またはCLIだけで支援します。

[English](./README.md)

## バックエンドとfeature

| Feature | 利用できるもの |
|:--------|:-------------|
| （なし） | `EventStoreForMemory`、`MemoryStorage`。常時利用可能 |
| `dynamodb` | `aws-sdk-dynamodb`を使用する`EventStoreForDynamoDB` |
| `migration` | `migrate_v3_dynamodb`。`dynamodb`も有効になる |

デフォルトfeatureは空です。`sqlite`・`sqlite-system`・`bigtable`は削除しました。公開APIはcrate直下からexportされ、`next` namespaceや旧APIの別名はありません。

## Memoryの使用例

アプリケーションの依存を次のように設定し、pathをcheckoutの実際の場所へ置き換えてください。

```toml
[dependencies]
event-store-adapter-rs = { path = "/path/to/event-store-adapter-rs/lib" }
chrono = "0.4"
serde_json = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

次の完全な例は4操作を実行します。同じ`MemoryStorage`を渡した2つのstoreは保存状態を共有し、別に生成したstorageは隔離されます。

```rust
use chrono::Utc;
use event_store_adapter_rs::{
  AggregateId, EventEnvelope, EventStore, EventStoreError, EventStoreForMemory,
  MemoryStorage, RetentionSettings, SnapshotEnvelope,
};
use serde_json::{json, Value};

#[derive(Debug, Clone)]
struct AccountId(String);

impl AggregateId for AccountId {
  fn type_name(&self) -> String { "Account".into() }
  fn value(&self) -> String { self.0.clone() }
}

#[tokio::main]
async fn main() -> Result<(), EventStoreError> {
  let storage = MemoryStorage::new(RetentionSettings::keep_latest(2))?;
  let writer: EventStoreForMemory<AccountId, Value, Value> =
    EventStoreForMemory::new(storage.clone());
  let reader: EventStoreForMemory<AccountId, Value, Value> =
    EventStoreForMemory::new(storage);
  let isolated: EventStoreForMemory<AccountId, Value, Value> =
    EventStoreForMemory::new(MemoryStorage::new(RetentionSettings::current_only())?);
  let id = AccountId("1".into());

  writer.persist_event(
    EventEnvelope::new(id.clone(), 1, Utc::now(), json!({"created": "Alice"}))
      .with_manifest("account-created/v1"),
  ).await?;
  let first = reader.get_latest_snapshot_by_id(&id).await?.unwrap();
  assert_eq!(first.head_seq_nr(), 1);
  assert!(first.snapshot().is_none());

  writer.persist_event_and_snapshot(
    EventEnvelope::new(id.clone(), 2, Utc::now(), json!({"renamed": "Bob"})),
    SnapshotEnvelope::new(json!({"name": "Bob"}), 2).with_manifest("account/v1"),
  ).await?;
  let events = reader.get_events_by_id_since_seq_nr(&id, 1).await?;
  assert_eq!(events.len(), 2);
  assert_eq!(events[1].seq_nr(), 2);
  let latest = reader.get_latest_snapshot_by_id(&id).await?.unwrap();
  assert_eq!(latest.head_seq_nr(), 2);
  assert_eq!(latest.snapshot().unwrap().aggregate(), &json!({"name": "Bob"}));
  assert!(isolated.get_latest_snapshot_by_id(&id).await?.is_none());
  Ok(())
}
```

`AggregateId`は型名と値を返します。ライブラリが`type_name-value`形式の`aid`を組み立てます。型名にはhyphenを含められず、全体の上限はUTF-8で1024バイトです。ドメインの`Display`や`KeyResolver`は使用しません。

`SeqNr`は`u64`で、範囲は`0..=SEQ_NR_MAX`（`2^53 - 1`）です。最初の書込は1、以降は連続した番号を使います。読取は下限を含み、0で先頭から読み取ります。`expected_version`引数とsnapshotの`version`はありません。`SnapshotEnvelope::seq_nr()`はsnapshotの反映番号、`SnapshotRead::head_seq_nr()`はイベントのheadを表します。イベントだけがある場合は`Some(SnapshotRead)`の`snapshot() == None`となり、作成イベントから再生します。

## DynamoDBの使用例

`features = ["dynamodb"]`を指定します。互いに異なる3表とsnapshot履歴GSIを事前作成し、非同期の`EventStoreForDynamoDB::open(client, tables, options)`を呼びます。表作成はライブラリの責務外です。`DynamoDbTables`は3表とGSIの名前、`DynamoDbOptions`は保持と有限の再要求設定を指定します。

SDK Clientにはretry sleeperが必要です。実行可能な[user-account例](examples/user-account/src/main.rs)はTokioの待機処理を設定し、DynamoDB Localを起動して表を作成し、新しい`open`を呼びます。[repository](examples/user-account/src/user_account_repository.rs)はイベント書込、イベントとsnapshotの書込、snapshot読取、イベント再生を実行します。snapshotなしの作成、headと同じsnapshot、その後のイベントからの復元を確認します。

```sh
cargo +1.99.0 run -p example-user-account
```

Dockerが必要です。キー・設定項目・transaction・保持の詳細は[DynamoDB schema](docs/DATABASE_SCHEMA.ja.md)を参照してください。

## Serializer・保持・エラー

既定のconstructorはJSONを使い、イベントと集約payloadに`Serialize + DeserializeOwned`を要求します。別のバイト形式には`EventStoreForMemory::with_serializers`または`EventStoreForDynamoDB::open_with_serializers`へ`Arc<dyn EventSerializer<P>>`と`Arc<dyn SnapshotSerializer<A>>`を渡します。serializerが受け取るのはpayloadだけです。payloadの要件は`Send + Sync + 'static`であり、この入口ではserde・Clone・Debugを要求しません。非serde payloadの実例は[Memory](lib/tests/memory_test.rs)・[DynamoDB](lib/tests/dynamodb_persist_event_test.rs)のintegration試験にあります。

既定は`RetentionSettings::current_only()`です。`keep_latest(n)`は新しい順にn件の履歴snapshotを保持し、0は設定エラーになります。Memoryは削除を使用し、履歴件数とTTLの併用を拒否します。DynamoDBは削除または`RetentionMode::Ttl { grace_seconds }`を使用できます。snapshot表のTTL属性`ttl`は別途設定してください。保持はイベント単独を含む追記の成功後に実行されます。保持失敗はaid・seq_nr・phase・errorを含む`tracing`警告で通知し、確定済みの書込は成功を返します。

`EventStoreError`には`OptimisticLock`・`ContractViolation`・`Serialization`・`Configuration`・`Storage`の5分類があります。文字列の解析ではなく、`ContractRule`・`SerializationPhase`・`ConfigurationReason`・`StorageOperation`をmatchしてください。直列化と保存先のエラーはsourceを保持します。

## 移行

通常storeは新配置だけを読み取ります。v3の既定DynamoDB配置にはfeature付き`migrate_v3_dynamodb`関数または薄いCLIを使用します。[v4移行ガイド](docs/MIGRATION_GUIDE_v4.ja.md)の手順に従い、旧書込を停止して空の新表を用意してください。独自`KeyResolver`配置・SQLite・Bigtableはこの移行の対象外です。

3系を継続する利用者向けの[v3移行ガイド](docs/MIGRATION_GUIDE_v3.ja.md)は歴史資料として残しています。

## 検証

```sh
cargo +1.99.0 build --workspace --all-targets --all-features
cargo +1.99.0 clippy --workspace --all-targets --all-features -- -D warnings
cargo +nightly fmt --all -- --check
cargo +1.99.0 test --workspace --all-features
```

DynamoDBとmigrationのintegration試験にはDockerが必要です。

## ライセンス

MITまたはApache-2.0。[LICENSE-MIT](LICENSE-MIT)・[LICENSE-APACHE](LICENSE-APACHE)を参照してください。

## リンク

- [共通ドキュメント](https://github.com/j5ik2o/event-store-adapter)
- [CQRS/Event Sourcingサンプル](https://github.com/j5ik2o/cqrs-es-example-rs)

## 他の言語のための実装

- [for Java](https://github.com/j5ik2o/event-store-adapter-java)
- [for Scala](https://github.com/j5ik2o/event-store-adapter-scala)
- [for Kotlin](https://github.com/j5ik2o/event-store-adapter-kotlin)
- [for Rust](https://github.com/j5ik2o/event-store-adapter-rs)
- [for Go](https://github.com/j5ik2o/event-store-adapter-go)
- [for JavaScript/TypeScript](https://github.com/j5ik2o/event-store-adapter-js)
- [for .NET](https://github.com/j5ik2o/event-store-adapter-dotnet)
- [for PHP](https://github.com/j5ik2o/event-store-adapter-php)
