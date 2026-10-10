# Rust v3から4系APIへの移行

このcheckoutの4系APIはまだ公開していません。受入済みMemory・DynamoDB本体を`event_store_adapter_rs`直下から利用します。`next` namespace・旧API別名・透過的な旧形式読取fallbackはありません。

## アプリケーションの変更

- `AggregateId::type_name()`・`value()`を実装し、`KeyResolver`と論理shard設定を除去します。
- crate直下から`EventStore`・`EventEnvelope`・`SnapshotEnvelope`・`SnapshotRead`・`SeqNr`・`EventStoreError`をimportします。旧読取／書込エラーを`EventStoreError`の5分類へ置き換えます。
- `expected_version`引数とsnapshotの`version`を除去します。最初のイベントは1、以降は連続した番号で`SEQ_NR_MAX`までです。対になったイベントとsnapshotの番号は一致させます。
- Memoryは`EventStoreForMemory::new(MemoryStorage::new(retention)?)`で生成します。storageのcloneでstore間の保存状態を共有し、独立したstorageで隔離します。
- DynamoDBは事前作成した3表・履歴GSIと、SDK retry sleeperを設定したClientで、非同期の`EventStoreForDynamoDB::open(client, tables, options).await`を呼びます。
- `SnapshotRead::snapshot()`が不在なら作成イベントから再生します。snapshotとheadの番号は別の意味です。snapshot後のイベントを読み、観測headまで再生できたことを確認します。[コンパイルしたrepository例](../examples/user-account/src/user_account_repository.rs)がこの場合を扱います。
- 非serde payloadには`with_serializers`／`open_with_serializers`へイベント・snapshot serializerを渡します。4操作は[README](../README.ja.md)、保持の実配置は[schema](DATABASE_SCHEMA.ja.md)を参照してください。

`sqlite`・`sqlite-system`・`bigtable`のfeature、本体、利用例は削除しました。これらの保存先は3系を継続してください。このmigrationはそのデータを支援しません。

## 保存データの対象

対象は**Rust v3の既定DynamoDB配置**だけです。旧journal・snapshotは`pkey`／`skey`キーを使用します。独自`KeyResolver`配置・v2データ・他の保存先は対象外です。通常の4系storeは旧表を読みません。

migrationは旧キーからIDを再構成し、番号の連続性とsnapshotの関係を検査します。旧hasherとpayloadの復元・再直列化は使用せず、payloadバイトをそのまま転写します。移行後のstoreには互換性のあるserializerを指定してください。

hyphenを含む旧型名は、hyphenなしの新型名へのJSON対応表が必要です。

```json
{"user-account": "UserAccount"}
```

アプリケーションの`AggregateId`も新型名を返すよう変更します。hyphenなしの旧型名は維持され、対応表を使うのはhyphenありの場合だけです。対応先の衝突と不正なIDは全件検査で拒否します。

## 運用手順

1. 検査前に**旧2表への全書込を停止**し、移行中も停止を維持します。関数とCLIはこの運用前提を強制しません。
2. [schema](DATABASE_SCHEMA.ja.md)に従い、空で互いに異なる新journal・snapshot・headとsnapshot履歴GSIを作成します。旧2表と新3表の名前は全て異なる必要があります。head Streamsと、必要に応じたsnapshot TTLも別途設定します。
3. 認証情報・region・endpoint・型対応表を指定し、下記の関数またはCLIを実行します。
4. JSON報告と終了statusを確認します。公開4系APIで移行後データを読み戻してから、アプリケーションの書込先を切り替えます。
5. 書込途中の失敗や条件競合があれば、**新3表を作り直し**、原因を解消して再実行します。部分移行先への再実行は拒否します。migrationは旧表を変更しません。

全件検査は旧2表のScan全ページを読みます。欠番、旧partition競合、孤立・未来snapshot、キー・属性・ID・時刻・TTLの不正、新項目サイズ超過を、移行データ送信前に拒否します。この時点で`open`による設定項目の作成は済んでいる場合があります。

検査成功後は旧表を再Scanし、条件付きで新journalを書き、各集約の最大番号イベントからheadを作成してcurrent・history snapshotを転写します。旧snapshotの`version`は捨て、snapshot manifestは空文字列にします。正のhistory TTLは維持します。未印付けhistoryには`active_history_seq_nr`を設定し、印付け済みhistoryには設定しません。欠けているイベントmanifestは空文字列になります。旧データの変更や継続的な互換層は提供しません。

## Library関数

`features = ["migration"]`を指定するとDynamoDBも有効になります。設定済みの`aws_sdk_dynamodb::Client`を`client`として持つ非同期の呼出し側では、次のように実行します。

```rust
use std::collections::HashMap;
use event_store_adapter_rs::{
  migrate_v3_dynamodb, DynamoDbTables, LegacyDynamoDbTables,
};

let old = LegacyDynamoDbTables {
  journal_table_name: "old-journal".into(),
  snapshot_table_name: "old-snapshot".into(),
};
let new = DynamoDbTables {
  journal_table_name: "journal".into(),
  snapshot_table_name: "snapshot".into(),
  head_table_name: "head".into(),
  snapshot_history_index_name: "snapshot-history".into(),
};
let mapping = HashMap::from([("user-account".into(), "UserAccount".into())]);
let report = migrate_v3_dynamodb(&client, &old, &new, &mapping).await?;
assert!(report.reasons.is_empty(), "{:?}", report.reasons);
```

`Ok(MigrationReport)`でも`reasons`に拒否理由があるため、呼出し側で確認します。通信・保存先の失敗はsourceと途中報告を持つ`MigrationError`になります。報告件数は成功応答を受けた書込を表します。

## 薄いCLI

このcheckoutから実行します。

```sh
cargo +1.99.0 run -p event-store-adapter-migration-rs -- --help
cargo +1.99.0 run -p event-store-adapter-migration-rs -- \
  --old-journal old-journal --old-snapshot old-snapshot \
  --journal journal --snapshot snapshot --head head \
  --history-index snapshot-history --type-mapping type-mapping.json \
  --region us-west-1
```

| Flag | 要件 |
|:-----|:-----|
| `--old-journal`・`--old-snapshot` | 必須。旧表名 |
| `--journal`・`--snapshot`・`--head` | 必須。新表名 |
| `--history-index` | 必須。新snapshot GSI名 |
| `--type-mapping` | 任意。JSONファイル |
| `--endpoint-url` | 任意。ローカルemulatorなどのendpoint |
| `--region` | 任意。AWS region |
| `--help`／`-h` | 利用方法と運用前提を表示 |

認証は通常のAWS設定を使います。CLIは`aggregates`・`events`・`snapshots`・`reasons`を持つ`MigrationReport` JSONを出力します。検査拒否と保存先の失敗は非0終了となり、保存先の失敗では途中報告とエラーを出力します。不正引数は移行前に失敗します。表作成・書込停止のflagはありません。

[CLI integration試験](../migration-cli/tests/cli_test.rs)はDynamoDB Localに対して実バイナリを起動し、公開APIによる読み戻しと旧表不変を確認します。Dockerが必要です。

```sh
cargo +1.99.0 test -p event-store-adapter-migration-rs --test cli_test
```
