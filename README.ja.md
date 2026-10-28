> この文書は [README.md](README.md) の日本語版です。英語版が正本で、食い違うときは英語版に従います。

# event-store-adapter-rs

[![CI](https://github.com/j5ik2o/event-store-adapter-rs/actions/workflows/ci.yml/badge.svg?branch=main&event=push)](https://github.com/j5ik2o/event-store-adapter-rs/actions/workflows/ci.yml?query=branch%3Amain)
[![crates.io](https://img.shields.io/crates/v/event-store-adapter-rs.svg)](https://crates.io/crates/event-store-adapter-rs)
[![docs.rs](https://docs.rs/event-store-adapter-rs/badge.svg)](https://docs.rs/event-store-adapter-rs)
[![MSRV](https://img.shields.io/badge/MSRV-1.94.1-orange.svg)](https://github.com/j5ik2o/event-store-adapter-rs/blob/main/lib/Cargo.toml)
[![Contract v5](https://img.shields.io/badge/contract-v5-blueviolet.svg)](https://github.com/j5ik2o/event-store-adapter/tree/contract-v5/docs/spec)
[![Storages](https://img.shields.io/badge/storages-memory%20%7C%20DynamoDB%20%7C%20SQLite%20%7C%20Spanner%20%7C%20Bigtable-informational.svg)](https://github.com/j5ik2o/event-store-adapter/tree/main/docs/spec/storage)
[![Renovate](https://img.shields.io/badge/renovate-enabled-brightgreen.svg)](https://renovatebot.com)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](https://github.com/j5ik2o/event-store-adapter-rs#license)


## 概要

このライブラリは、CQRS/Event Sourcing用のMemory・DynamoDB・SQLite・Spanner・Bigtableイベントストアを提供します。

このクレートは[共通契約 v5](https://github.com/j5ik2o/event-store-adapter/blob/main/docs/spec/README.md)に従います。メジャーの番号 5 は、共通契約の版と同じです。

> 5.0.0は、新しいAPIの最初の版です。v3 DynamoDB・SQLite・Bigtableの旧データは明示的なmigration関数またはCLIだけで支援します。以前の版にはSpannerの保存先がないため、Spannerに移行するものはありません。

公開APIはcrate直下からexportされ、`next` namespaceや旧APIの別名はありません。

### リンク

- [共通ドキュメント](https://github.com/j5ik2o/event-store-adapter)
- [CQRS/Event Sourcingサンプル](https://github.com/j5ik2o/cqrs-es-example-rs)
- [開発者向けの案内](./CONTRIBUTING.ja.md)
- [English](./README.md)

## インストール

| Feature | 利用できるもの |
|:--------|:-------------|
| （なし） | `EventStoreForMemory`、`MemoryStorage`。常時利用可能 |
| `dynamodb` | `aws-sdk-dynamodb`を使用する`EventStoreForDynamoDB` |
| `sqlite` | 同梱のSQLiteを持つ`rusqlite`を使用する`EventStoreForSqlite`、`initialize_sqlite_store`、`SqliteOptions` |
| `spanner` | `tonic`と生成されたSpannerのgRPCクライアントを使用する`EventStoreForSpanner`、`initialize_spanner_store`、`SpannerOptions`、`SpannerRetention`、`SpannerConnection`、`SpannerTables`、`SpannerTokenSource` |
| `bigtable` | `tonic`と生成されたBigtableのgRPCクライアントを使用する`EventStoreForBigtable`、`initialize_bigtable_store`、`BigtableOptions`、`BigtableConnection`、`BigtableTables`、`BigtableTokenSource` |
| `migration` | `migrate_v3_dynamodb`。`dynamodb`も有効になる |
| `sqlite-migration` | `migrate_v3_sqlite`。`sqlite`も有効になり、AWS・Bigtableのcrateは入らない |
| `bigtable-migration` | `migrate_v3_bigtable`、`LegacyBigtableTables`。`bigtable`も有効になり、AWS・SQLiteのcrateは入らない |

デフォルトfeatureは空です。`sqlite-system`は削除しました。

## はじめに

アプリケーションの依存を次のように設定してください。

```toml
[dependencies]
event-store-adapter-rs = "5.0.0"
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

## 保存先

### メモリ

メモリの保存先は単一プロセスのメモリだけを使います。ファイルへの保存、再起動後の回復、プロセス間の共有はしません。

完全な例は[はじめに](#はじめに)を参照してください。

### DynamoDB

`features = ["dynamodb"]`を指定します。互いに異なる3表とsnapshot履歴GSIを事前作成し、非同期の`EventStoreForDynamoDB::open(client, tables, options)`を呼びます。表作成はライブラリの責務外です。`DynamoDbTables`は3表とGSIの名前、`DynamoDbOptions`は保持と有限の再要求設定を指定します。

3 つの表は同じリージョンに置き、書き込みもそのリージョンからだけ行ってください。グローバルテーブル（複数リージョンのレプリカ）は範囲外です。

このクレートは `aws-sdk-dynamodb` の既定の機能を外し、`default-https-client` と `rt-tokio` だけを有効にしています。そのため、SDK の古い通信部品（hyper 0.14 と rustls 0.21）を引き込みません。Cargo はビルドに含まれるすべてのクレートが求める機能を合わせて有効にするので、利用する側の `aws-sdk-dynamodb` の依存が既定の機能のままだと、既定の `rustls` によって古い通信部品がまた有効になります。外すには、SDK を同じ形で指定してください。

```toml
aws-sdk-dynamodb = { version = "1.68.0", default-features = false, features = ["default-https-client", "rt-tokio"] }
```

古い通信部品が外れていれば、`cargo tree -i rustls@0.21` は該当するパッケージがないと報告します。

SDK Clientにはretry sleeperが必要です。実行可能な[user-account例](examples/user-account/src/main.rs)はTokioの待機処理を設定し、DynamoDB Localを起動して表を作成し、新しい`open`を呼びます。[repository](examples/user-account/src/user_account_repository.rs)はイベント書込、イベントとsnapshotの書込、snapshot読取、イベント再生を実行します。snapshotなしの作成、headと同じsnapshot、その後のイベントからの復元を確認します。

```sh
cargo +1.99.0 run -p example-user-account
```

Dockerが必要です。キー・設定項目・transaction・保持の詳細は[DynamoDB schema](docs/DYNAMODB_DATABASE_SCHEMA.ja.md)を参照してください。

### SQLite

`features = ["sqlite"]`を指定します。保存先は、使うホストのローカルファイルシステムに置く1つのSQLiteデータベースファイルで、WALと`synchronous=FULL`で動かします。SQLite 3.51.3以上が必要で、同梱のSQLiteはこれを満たします。初期化と「開く」は、別々の明示的な操作です。

- `initialize_sqlite_store(path, &options)`は、新しいファイル、またはアプリケーション用の表が1つもない既存のファイルを、1回だけ初期化します。5つの表と設定の行を、1つのトランザクションで作ります。
- `EventStoreForSqlite::open(path, options)`（非serde payloadには`open_with_serializers`）は、初期化済みのファイルを開き、表の配置と設定を`options`と照合します。表は作らず、食い違いも修復しません。ファイルが存在しない、またはSQLiteが開けない場合は保存先エラー（`EventStoreError::Storage`）です。ファイルが未初期化、表が欠けている・異なる、設定が食い違う場合は設定エラー（`EventStoreError::Configuration`）です。

`SqliteOptions`は3つのフィールドを持ちます。`retention`の既定は`RetentionSettings::current_only()`です。`keep_latest(n)`は新しい順に`n`件の履歴snapshotを保持し、`open`に渡す値はファイルの初期化に使った値と一致が必要です。`RetentionMode::Ttl`と`keep_latest(0)`は設定エラーです。`change_feed`は`false`のままにしてください。この保存先は変更フィードを提供せず、`true`は設定エラーです。`lock_wait`は、別の接続の書込ロックを待つ時間で、既定は5秒です。時間切れは楽観ロックではなく保存先エラーになります。

```toml
[dependencies]
event-store-adapter-rs = { version = "5", features = ["sqlite"] }
chrono = "0.4"
serde_json = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use chrono::Utc;
use event_store_adapter_rs::{
  initialize_sqlite_store, AggregateId, EventEnvelope, EventStore, EventStoreError,
  EventStoreForSqlite, RetentionSettings, SnapshotEnvelope, SqliteOptions,
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
  let path = "accounts.sqlite3";
  let options = SqliteOptions {
    retention: RetentionSettings::keep_latest(2),
    ..SqliteOptions::default()
  };
  // Run once for a new file; the file is no longer uninitialized afterwards.
  initialize_sqlite_store(path, &options)?;
  let store: EventStoreForSqlite<AccountId, Value, Value> =
    EventStoreForSqlite::open(path, options)?;
  let id = AccountId("1".into());

  store.persist_event(
    EventEnvelope::new(id.clone(), 1, Utc::now(), json!({"created": "Alice"}))
      .with_manifest("account-created/v1"),
  ).await?;
  store.persist_event_and_snapshot(
    EventEnvelope::new(id.clone(), 2, Utc::now(), json!({"renamed": "Bob"})),
    SnapshotEnvelope::new(json!({"name": "Bob"}), 2).with_manifest("account/v1"),
  ).await?;
  let events = store.get_events_by_id_since_seq_nr(&id, 1).await?;
  assert_eq!(events.len(), 2);
  let latest = store.get_latest_snapshot_by_id(&id).await?.unwrap();
  assert_eq!(latest.head_seq_nr(), 2);
  assert_eq!(latest.snapshot().unwrap().aggregate(), &json!({"name": "Bob"}));
  Ok(())
}
```

storeのインスタンスとそのcloneは、1つの接続を共有します。表・接続の設定・手作業での初期化・transaction・保持の詳細は[SQLite schema](docs/SQLITE_DATABASE_SCHEMA.ja.md)を参照してください。v3のSQLiteファイルからデータを移す場合は、[移行](#移行)を参照してください。

### Spanner

`features = ["spanner"]`を指定します。保存先は、GoogleSQL方言の1つのデータベースにある5つの表（`<prefix>_journal`・`<prefix>_head`・`<prefix>_snapshot_current`・`<prefix>_snapshot_history`・`<prefix>_config`）で、Cloud Spanner（またはSpannerエミュレーター）にgRPCで接続します。表の名前は、渡した1つの`table_prefix`からすべて導きます。ライブラリはDDLを実行しません。最初の`initialize_spanner_store`の前に、データベースを作り、[Spannerのスキーマ](docs/SPANNER_DATABASE_SCHEMA.ja.md)のDDLを適用してください（Spanner TTL方式では、`<prefix>_snapshot_history`の行削除ポリシーも）。

初期化と「開く」は、別々の明示的な操作です。

- `initialize_spanner_store(&connection, &tables, &options)`は、データベースの方言・列・主キー・行削除ポリシーを`options`と照合し、5つの表が空であることを確かめてから、生成した`store_id`を持つ設定の行を1行書きます。表は作りません。設定の行がすでにある、またはいずれかの表に行がある場合は、設定エラー（`ConfigurationReason::SpannerAlreadyInitialized`）になります。
- `EventStoreForSpanner::open(connection, tables, options)`（非serde payloadには`open_with_serializers`）は、同じ照合を行い、設定の行を読みます。何も作らず、食い違いも修復しません。表が欠けている・異なる、設定の行が欠けている・壊れている、記録された設定と食い違う場合は設定エラー（`EventStoreError::Configuration`）です。接続やサービスの失敗は保存先エラー（`EventStoreError::Storage`）です。

`SpannerOptions`は3つのフィールドを持ちます。`retention`の既定は`SpannerRetention::CurrentOnly`です。`KeepLatest { count }`は新しい順に`count`件の履歴snapshotを保持し、古いものはすぐに削除します。`KeepLatestWithTtl { count, grace_days }`は、古い履歴を削除せず`expired_at`で印を付け、削除を`<prefix>_snapshot_history`の`ROW DELETION POLICY (OLDER_THAN(expired_at, INTERVAL <grace_days> DAY))`に任せます。`count`と`grace_days`は1以上でINT64に収まる値にし、`open`に渡す値は、ストアの初期化に使った値、および行削除ポリシーの日数と一致が必要です。`change_feed`は`false`のままにしてください。この保存先は変更フィードを提供せず、`true`は設定エラーです。`transaction_retry_limit`は、中断（ABORTED）されたトランザクションを再試行する回数で、最初の試行は数えず、既定は5です。使い切ると保存先エラーになります。

追記は、SERIALIZABLEの読み書きトランザクション1つで行い、そのコミットが確定点です。読み取りはstrongの読み取り専用トランザクションを使い、headとそれに続けて読む行は同じ読み取り時点のものです。ライブラリが再試行するのは中断（ABORTED）されたトランザクションだけで、同じイベントと同じ`seq_nr`で行います。コミットの応答が失われた追記は、結果が不明な保存先エラーです。再試行も補償もせず、同じ`seq_nr`を送り直すと、最初のコミットが成功していたなら楽観ロックのエラーになります。古い履歴の整理は、追記のコミットの後に別のトランザクションで行います。それが失敗しても、`tracing`の警告を出すだけで、追記は成功のままです。

接続は、1つの`tonic::transport::Channel`を持つ`SpannerConnection`です。同じchannelをデータAPIとデータベース管理APIの両方に使います。`open`と`initialize_spanner_store`は管理APIの`GetDatabase`で方言を読むので、認証情報にはデータAPIの権限に加えて`spanner.databases.get`の権限が要ります。エミュレーターには、平文のchannelが要り、認証情報は要りません。

```toml
[dependencies]
event-store-adapter-rs = { version = "5", features = ["spanner"] }
chrono = "0.4"
serde_json = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
tonic = { version = "0.14", default-features = false, features = ["channel"] }
```

```rust
use event_store_adapter_rs::{EventStoreForSpanner, SpannerOptions, SpannerRetention};
use serde_json::Value;
use tonic::transport::Endpoint;
use chrono::Utc;
use event_store_adapter_rs::{
  initialize_spanner_store, AggregateId, EventEnvelope, EventStore, SnapshotEnvelope, SpannerConnection, SpannerTables,
};
use serde_json::json;

#[derive(Debug, Clone)]
struct AccountId(String);

impl AggregateId for AccountId {
  fn type_name(&self) -> String {
    "Account".into()
  }

  fn value(&self) -> String {
    self.0.clone()
  }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  // The emulator listens without TLS. The database and its five tables already exist.
  let channel = Endpoint::from_static("http://localhost:9010").connect().await?;
  let tables = SpannerTables {
    project_id: "my-project".into(),
    instance_id: "my-instance".into(),
    database_id: "my-database".into(),
    table_prefix: "esa".into(),
  };
  let connection = SpannerConnection::new(channel);
  let options = SpannerOptions {
    retention: SpannerRetention::KeepLatest { count: 2 },
    ..SpannerOptions::default()
  };
  // Run once for new tables; they are no longer uninitialized afterwards.
  initialize_spanner_store(&connection, &tables, &options).await?;
  let store: EventStoreForSpanner<AccountId, Value, Value> =
    EventStoreForSpanner::open(connection, tables, options).await?;
  let id = AccountId("1".into());

  store
    .persist_event(
      EventEnvelope::new(id.clone(), 1, Utc::now(), json!({"created": "Alice"})).with_manifest("account-created/v1"),
    )
    .await?;
  store
    .persist_event_and_snapshot(
      EventEnvelope::new(id.clone(), 2, Utc::now(), json!({"renamed": "Bob"})),
      SnapshotEnvelope::new(json!({"name": "Bob"}), 2).with_manifest("account/v1"),
    )
    .await?;
  let events = store.get_events_by_id_since_seq_nr(&id, 1).await?;
  assert_eq!(events.len(), 2);
  let latest = store.get_latest_snapshot_by_id(&id).await?.unwrap();
  assert_eq!(latest.head_seq_nr(), 2);
  assert_eq!(latest.snapshot().unwrap().aggregate(), &json!({"name": "Bob"}));
  Ok(())
}
```

本番のCloud Spannerには、TLSのchannelと、すべての要求に付けるアクセストークンが要ります。ライブラリは、TLSの実装にもGoogleの認証のライブラリにも依存しません。利用者が自分の依存で`tonic`のTLSのfeature（たとえば`tls-ring`と`tls-native-roots`）を有効にして、`https://spanner.googleapis.com`への`Channel`を自分で作り、新しいトークンを返す`access_token`を持つ`SpannerTokenSource`を実装して、`SpannerConnection::with_token_source`に渡してください。ストアは、データとデータベース管理のすべての要求の直前に取得元を呼び、`authorization: Bearer <トークン>`を付けます。トークンの保持と更新は取得元の責任で、取得元が失敗した操作は、要求を送らずに保存先エラーになります。

この保存先の規則は、1つのSpannerデータベースの中で、そのデータベースの読み書きトランザクションとstrongな読み取りが成り立つ構成で、すべての表がそのデータベースにある場合に成り立ちます。storeのインスタンスとそのcloneは接続を共有し、同じ設定を使うなら、複数のstoreのインスタンスが同じデータベースを開いてかまいません。過去の時点を読む構成や、5つの表を複数のデータベースに分ける構成は対象外です。この版は変更フィードを提供しません。以前の版にはSpannerの保存先がないため、移行ツールはありません。[移行](#移行)を参照してください。配置・設定の行・初期化・書込と読取・保持の詳細は[Spannerのスキーマ](docs/SPANNER_DATABASE_SCHEMA.ja.md)を参照してください。

### Bigtable

`features = ["bigtable"]`を指定します。保存先は、Cloud Bigtable（またはBigtableエミュレーター）にgRPCでつなぐ、`journal`と`head`の2つのテーブルです。テーブルの作成とapp profileの用意はライブラリの範囲外です。最初の`initialize_bigtable_store`の前に、次を用意してください。

- 2つのテーブルを、列ファミリ付きで作ります。journalテーブルには`event`・`history`・`config`、headテーブルには`head`・`snapshot`・`config`です。データの4つの列ファミリ（`event`・`history`・`head`・`snapshot`）には、ガベージコレクションの規則として「最大1版を残す」だけを設定し、年齢による規則は設定しません。これ以外の規則は`open`が拒否します。`config`の版数の規則は任意ですが、年齢による規則は設定しません。設定の行はセルの時刻0で書くので、年齢による規則があるとすぐに期限切れになり、以後の`open`と初期化の確認が失敗します。
- すべての操作を1つのクラスタに固定し、単一行トランザクションを許可するapp profileを作り、そのIDを`BigtableOptions::app_profile_id`に渡します。ライブラリはapp profileを照合しません。

初期化と「開く」は、別々の明示的な操作です。

- `initialize_bigtable_store(&connection, &tables, &options)`は、同じ`store_id`と設定を、2つのテーブルの設定の行へ1回だけ書きます。テーブルも列ファミリも作りません。どちらかのテーブルに設定の行が既にあるときは、設定エラー（`ConfigurationReason::BigtableAlreadyInitialized`）です。
- `EventStoreForBigtable::open(connection, tables, options)`（非serde payloadには`open_with_serializers`）は、列ファミリと2つの設定の行を`options`と照合します。何も作らず、食い違いも修復しません。列ファミリが欠けている、設定の行が欠けている・壊れている、設定が保存された値と食い違う場合は設定エラー（`EventStoreError::Configuration`）です。

`BigtableOptions`は4つのフィールドを持ちます。`retention`の既定は`RetentionSettings::current_only()`です。`keep_latest(n)`は新しい順に`n`件の履歴snapshotを保持し、`open`に渡す値はストアの初期化に使った値と一致が必要です。`RetentionMode::Ttl`と`keep_latest(0)`は設定エラーです。`change_feed`は`false`のままにしてください。この保存先は変更フィードを提供せず、`true`は設定エラーです。`envelope_limit_bytes`は、メタデータを含む直列化後の1封筒の上限で、既定は1,048,576バイトです。17以上8,388,608以下で、2つのテーブルに記録されるため、すべてのクライアントが同じ値を使う必要があります。`app_profile_id`はすべてのデータ要求に付けます。空文字列ならインスタンスの既定のapp profileを使います。

接続は`BigtableConnection`で、データAPI用とadmin API用の`tonic::transport::Channel`を1つずつ持ちます。admin APIは、ストアを開くときの列ファミリの照合にだけ使います。エミュレーターには、平文のchannelを使い、認証は要りません。

```toml
[dependencies]
event-store-adapter-rs = { version = "5", features = ["bigtable"] }
chrono = "0.4"
serde_json = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
tonic = { version = "0.14", default-features = false, features = ["channel"] }
```

```rust
use chrono::Utc;
use event_store_adapter_rs::{
  initialize_bigtable_store, AggregateId, BigtableConnection, BigtableOptions, BigtableTables,
  EventEnvelope, EventStore, EventStoreForBigtable, RetentionSettings, SnapshotEnvelope,
};
use serde_json::{json, Value};
use tonic::transport::Endpoint;

#[derive(Debug, Clone)]
struct AccountId(String);

impl AggregateId for AccountId {
  fn type_name(&self) -> String { "Account".into() }
  fn value(&self) -> String { self.0.clone() }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  // The emulator listens without TLS. The tables and column families already exist.
  let channel = Endpoint::from_static("http://localhost:8086").connect().await?;
  let connection = BigtableConnection { data: channel.clone(), admin: channel };
  let tables = BigtableTables {
    project_id: "my-project".into(),
    instance_id: "my-instance".into(),
    journal_table_id: "journal".into(),
    head_table_id: "head".into(),
  };
  let options = BigtableOptions {
    retention: RetentionSettings::keep_latest(2),
    ..BigtableOptions::default()
  };
  // Run once for new tables; they are no longer uninitialized afterwards.
  initialize_bigtable_store(&connection, &tables, &options).await?;
  let store: EventStoreForBigtable<AccountId, Value, Value> =
    EventStoreForBigtable::open(connection, tables, options).await?;
  let id = AccountId("1".into());

  store.persist_event(
    EventEnvelope::new(id.clone(), 1, Utc::now(), json!({"created": "Alice"}))
      .with_manifest("account-created/v1"),
  ).await?;
  store.persist_event_and_snapshot(
    EventEnvelope::new(id.clone(), 2, Utc::now(), json!({"renamed": "Bob"})),
    SnapshotEnvelope::new(json!({"name": "Bob"}), 2).with_manifest("account/v1"),
  ).await?;
  let events = store.get_events_by_id_since_seq_nr(&id, 1).await?;
  assert_eq!(events.len(), 2);
  let latest = store.get_latest_snapshot_by_id(&id).await?.unwrap();
  assert_eq!(latest.head_seq_nr(), 2);
  assert_eq!(latest.snapshot().unwrap().aggregate(), &json!({"name": "Bob"}));
  Ok(())
}
```

本番のCloud Bigtableには、TLSのchannelと、すべての要求に付けるアクセストークンが要ります。ライブラリは、TLSの実装にもGoogleの認証のライブラリにも依存しません。利用者が自分の依存で`tonic`のTLSのfeature（たとえば`tls-ring`と`tls-native-roots`）を有効にして、2つの`Channel`（データは`https://bigtable.googleapis.com`、adminは`https://bigtableadmin.googleapis.com`）を自分で作り、新しいトークンを返す`access_token`を持つ`BigtableTokenSource`を実装してください。その取得元を、`EventStoreForBigtable::open_with_token_source`（または`open_with_serializers_and_token_source`）と`initialize_bigtable_store_with_token_source`に渡します。ストアは、データとadminのすべての要求の直前に取得元を呼び、`authorization: Bearer <トークン>`を付けます。トークンの保持と更新は取得元の責任で、取得元が失敗した操作は、要求を送らずに保存先エラーになります。

storeのインスタンスとそのcloneは、接続を共有します。この版は変更フィードを提供しません。キー・設定の行・初期化（中断した初期化の回復を含む）・書込と読取・保持の詳細は[Bigtable schema](docs/BIGTABLE_DATABASE_SCHEMA.ja.md)を参照してください。v3のBigtableからデータを移す場合は、[移行](#移行)を参照してください。

## 直列化

既定のconstructorはJSONを使い、イベントと集約payloadに`Serialize + DeserializeOwned`を要求します。別のバイト形式には`EventStoreForMemory::with_serializers`または`EventStoreForDynamoDB::open_with_serializers`へ`Arc<dyn EventSerializer<P>>`と`Arc<dyn SnapshotSerializer<A>>`を渡します。serializerが受け取るのはpayloadだけです。payloadの要件は`Send + Sync + 'static`であり、この入口ではserde・Clone・Debugを要求しません。非serde payloadの実例は[Memory](lib/tests/memory_test.rs)・[DynamoDB](lib/tests/dynamodb_persist_event_test.rs)のintegration試験にあります。

## 保持

既定は`RetentionSettings::current_only()`です。`keep_latest(n)`は新しい順にn件の履歴snapshotを保持し、0は設定エラーになります。Memoryは削除だけを使用し、`RetentionMode::Ttl`の指定は履歴件数の有無にかかわらず`MemoryStorage::new`で設定エラーになります。DynamoDBは削除または`RetentionMode::Ttl { grace_seconds }`を使用できます。snapshot表のTTL属性`ttl`は別途設定してください。履歴件数を設定したMemoryは、イベント単独を含む追記の成功後に保持処理を実行します。DynamoDBは履歴件数を設定し、履歴snapshotを書いた追記の成功後だけ保持処理を実行します。DynamoDBはイベント単独の追記では保持処理を実行しません。Spannerは、古い履歴をすぐに削除する`SpannerRetention::KeepLatest { count }`と、印を付けて削除を行削除ポリシーに任せる`KeepLatestWithTtl { count, grace_days }`を使用できます。履歴snapshotを書いた追記の成功後に、別のトランザクションで保持処理を実行します。保持失敗はaid・seq_nr・phase・errorを含む`tracing`警告で通知し、確定済みの書込は成功を返します。

## エラー

`EventStoreError`には`OptimisticLock`・`ContractViolation`・`Serialization`・`Configuration`・`Storage`の5分類があります。文字列の解析ではなく、`ContractRule`・`SerializationPhase`・`ConfigurationReason`・`StorageOperation`をmatchしてください。直列化と保存先のエラーはsourceを保持します。

## 移行

通常storeは新配置だけを読み取ります。v3の既定DynamoDB配置にはfeature付き`migrate_v3_dynamodb`関数、または[v5移行ガイド](docs/MIGRATION_GUIDE.ja.md)に書いてある薄いCLIを使用します。v3の既定SQLite配置には、`migrate_v3_sqlite`関数（feature `sqlite-migration`）、または同じCLIの`--old-sqlite`・`--sqlite`を使用します。旧SQLiteファイルは変更せずに読み取り、`initialize_sqlite_store`で初期化した新ファイルへ書き込みます。v3.0.6の既定のBigtable配置（journalとsnapshotの2つのテーブル）には、`migrate_v3_bigtable`関数（feature `bigtable-migration`）、または同じCLIの`--old-bigtable-*`・`--bigtable-*`を使用します。旧の2つのテーブルは読み取りだけで、列ファミリ付きで作り`initialize_bigtable_store`で初期化した新しい2つのテーブルへ書き込みます。移行の前に、旧書込を停止して空の新表（SQLiteでは初期化済みで空のファイル、Bigtableでは初期化済みで空のテーブル）を用意してください。以前の版にはSpannerの保存先がないため、Spannerの移行関数もCLIのオプションもありません。[移行ガイド](docs/MIGRATION_GUIDE.ja.md)の手順で、空のデータベースから始めてください。独自`KeyResolver`配置はこの移行の対象外です。

3系を継続する利用者向けの[v3移行ガイド](docs/MIGRATION_GUIDE_v3.ja.md)は歴史資料として残しています。

## 適合

```sh
cargo +1.99.0 build --workspace --all-targets --all-features
cargo +1.99.0 clippy --workspace --all-targets --all-features -- -D warnings
cargo +nightly fmt --all -- --check
rustup toolchain install nightly-2026-09-27 --profile minimal
cargo +1.99.0 test --workspace --all-features
```

DynamoDB・Spanner・Bigtable・migrationのintegration試験にはDockerが必要です。

## ライセンス

MIT と Apache-2.0 のどちらかを選べる二重ライセンス（`MIT OR Apache-2.0`）です。[LICENSE-MIT](LICENSE-MIT) と [LICENSE-APACHE](LICENSE-APACHE) を参照してください。
