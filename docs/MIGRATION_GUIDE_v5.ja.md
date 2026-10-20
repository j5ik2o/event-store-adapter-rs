# Rust v3から5系APIへの移行

5.0.0は、5系APIの最初の版です。受入済みMemory・DynamoDB本体を`event_store_adapter_rs`直下から利用します。`next` namespace・旧API別名・透過的な旧形式読取fallbackはありません。

## アプリケーションの変更

- `AggregateId::type_name()`・`value()`を実装し、`KeyResolver`と論理shard設定を除去します。
- crate直下から`EventStore`・`EventEnvelope`・`SnapshotEnvelope`・`SnapshotRead`・`SeqNr`・`EventStoreError`をimportします。旧読取／書込エラーを`EventStoreError`の5分類へ置き換えます。
- `expected_version`引数とsnapshotの`version`を除去します。最初のイベントは1、以降は連続した番号で`SEQ_NR_MAX`までです。対になったイベントとsnapshotの番号は一致させます。
- Memoryは`EventStoreForMemory::new(MemoryStorage::new(retention)?)`で生成します。storageのcloneでstore間の保存状態を共有し、独立したstorageで隔離します。
- DynamoDBは事前作成した3表・履歴GSIと、SDK retry sleeperを設定したClientで、非同期の`EventStoreForDynamoDB::open(client, tables, options).await`を呼びます。
- SQLiteはfeature `sqlite`を有効にし、新しいファイルに`initialize_sqlite_store(path, &options)`を1回呼んでから、同期の`EventStoreForSqlite::open(path, options)`を呼びます。`open`は何も作りません。この保存先は変更フィードを提供しません。[README](../README.ja.md)と[SQLite schema](SQLITE_DATABASE_SCHEMA.ja.md)を参照してください。
- `SnapshotRead::snapshot()`が不在なら作成イベントから再生します。snapshotとheadの番号は別の意味です。snapshot後のイベントを読み、観測headまで再生できたことを確認します。[コンパイルしたrepository例](../examples/user-account/src/user_account_repository.rs)がこの場合を扱います。
- 非serde payloadには`with_serializers`／`open_with_serializers`へイベント・snapshot serializerを渡します。4操作は[README](../README.ja.md)、保持の実配置は[schema](DATABASE_SCHEMA.ja.md)を参照してください。

`sqlite-system`・`bigtable`のfeature、本体、利用例は削除しました。これらの保存先は3系を継続してください。このmigrationはそのデータを支援しません。feature `sqlite`は、独自の配置を持つ5系の新しいSQLite保存先です。v3のSQLite保存先が書いたデータは、下記の[SQLiteの移行](#v3-sqliteデータの移行)で移します。

## 保存データの対象

対象は**Rust v3の既定DynamoDB配置**だけです。旧journal・snapshotは`pkey`／`skey`キーを使用します。独自`KeyResolver`配置・v2データ・他の保存先は対象外です。v3の既定SQLiteファイルには、下記の[SQLiteの移行](#v3-sqliteデータの移行)があります。通常の5系storeは旧表を読みません。

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
4. JSON報告と終了statusを確認します。公開5系APIで移行後データを読み戻してから、アプリケーションの書込先を切り替えます。
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
cargo run -p event-store-adapter-migration-rs -- --help
cargo run -p event-store-adapter-migration-rs -- \
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
cargo test -p event-store-adapter-migration-rs --test cli_test
```

## v3 SQLiteデータの移行

v3のSQLite保存先は、`journal`表と`snapshot`表を持つ1つのデータベースファイルを書きました。5系のSQLite保存先は別の配置（[schema](SQLITE_DATABASE_SCHEMA.ja.md)）を使うため、データは**新しいファイル**へ写します。旧ファイルは読み取り専用で開き、変更しません。通常の5系storeは旧ファイルを読みません。

### 保存データの対象（SQLite）

対象は**Rust v3の既定SQLite配置**だけです。v3.0.6が作る、`pkey`／`skey`を主キーとする`journal`表と`snapshot`表で、キーを既定の`KeyResolver`（`pkey = {型名}-{shard}`、`skey = {型名}-{値}-{seq_nr}`）で組み立てたものです。独自`KeyResolver`配置・v2データ・他の保存先は対象外で、既定の配置に従わない行は全件検査で拒否します。

migrationは、DynamoDBの移行と同じ規則で、型名と値を`pkey`と`skey`だけから再構成します。アプリケーションの`Display`文字列が入っている旧`aid`列からは読みません。hyphenを含む旧型名には、[前述](#保存データの対象)のJSON対応表が必要です。対応表を使うのはhyphenありの旧型名だけで、対応先の衝突・対応後に残るhyphen・1024バイトを超える`aid`は拒否します。

写すものは次のとおりです。

- **イベント。** payloadバイトは復元も再直列化もせずに写します。発生時刻はナノ秒の値のまま、manifestは保存されている値のまま写し、空のmanifestは空のままです。集約ごとに、番号は1から欠番・重複なしで続く必要があり、headは最大番号のイベントから作ります。
- **現在のsnapshot。** 旧`skey`の末尾が`-0`の行を、空のmanifestで`snapshot_current`へ写します。位置は集約の最大イベント番号以下で、対応するイベントが存在する必要があります。旧`version`と`last_updated_at`は捨てます。
- **snapshotの履歴。** 旧の履歴行（`skey`の末尾が1以上の番号の行）は、**新ファイルの**履歴件数に合わせて写します。[SQLite schema](SQLITE_DATABASE_SCHEMA.ja.md#保持)の保持の規則（新しい`n`件を残し、保持していなかった履歴は補わない）に従います。新ファイルが新しい`n`件を保持するなら、集約ごとに番号の新しい`n`件の履歴snapshotを`snapshot_history`へ写します。currentのsnapshotだけを保持するなら、履歴は写しません。currentのsnapshotを履歴に足すことはなく、旧ファイルに履歴がない集約に履歴を作ることもありません。どちらの場合も旧の履歴行はすべて検査するので、写さない履歴行でも不正があれば移行を拒否します。

### 運用手順（SQLite）

1. 検査前に**旧ファイルへの全書込を停止**し、移行中も停止を維持します。関数とCLIはこの運用前提を強制しません。
2. `initialize_sqlite_store`、または[schema](SQLITE_DATABASE_SCHEMA.ja.md#初期化)の手作業の手順で新ファイルを作り、履歴件数（`keep_latest(n)`か`current_only()`）を選びます。新ファイルは初期化済みで**空**である必要があります。`journal`・`head`・`snapshot_current`・`snapshot_history`のどれかに行があれば移行を拒否し、migrationは表も設定も作りません。新ファイルは旧ファイルとは別のファイルでなければなりません。2つのパスは正規化（シンボリックリンクと`.`／`..`の解決）してから比べ、同じファイルへのハードリンクは検出しません。
3. 旧パス・新パス・型対応表を指定し、下記の関数またはCLIを実行します。
4. JSON報告と終了statusを確認します。新ファイルを同じoptionsで公開5系APIから開き、移行後データを読み戻してから、アプリケーションの書込先を切り替えます。
5. 検査拒否や書込の失敗があっても、**新ファイルには何も残りません**。書込全体が1つのトランザクションで、旧ファイルも変わりません。原因を解消し、同じ新ファイルへ再実行できます。移行に成功した後の再実行は、新ファイルに行があるため拒否されます。

全件検査は、新ファイルへ何かを書く前に、旧2表の全行を読みます。欠番と重複、既定の配置に従わないキー、孤立snapshotと集約の最大イベント番号を超えるsnapshot、対応表のない・衝突する・長すぎるID、型の誤った列を拒否します。その後、同じ読み取りトランザクションの中で旧ファイルを再度読み、新ファイルへ1つの`BEGIN IMMEDIATE`トランザクションで書きます。報告の件数は、このトランザクションのコミット後に設定します。

拒否の理由は`table`に対象を示します。旧ファイルの行は`old.journal`か`old.snapshot`、すでに行がある新ファイルの表は`new.journal`・`new.head`・`new.snapshot_current`・`new.snapshot_history`、旧パスと新パスが同じファイルなら空文字列です。報告の`aggregates`はheadの行数、`events`はjournalの行数、`snapshots`は写したcurrentと履歴のsnapshotの行数です。

### Library関数（SQLite）

`features = ["sqlite-migration"]`を指定すると`sqlite`も有効になりますが、DynamoDBとBigtableは有効になりません。関数は同期です。

```rust
use std::collections::HashMap;
use event_store_adapter_rs::migrate_v3_sqlite;

let mapping = HashMap::from([("user-account".into(), "UserAccount".into())]);
let report = migrate_v3_sqlite("old.sqlite3", "new.sqlite3", &mapping)?;
assert!(report.reasons.is_empty(), "{:?}", report.reasons);
```

`Ok(MigrationReport)`でも`reasons`に拒否理由があるため、呼出し側で確認します。拒否では件数が0のままです。旧ファイルを読めない・新ファイルが未初期化といった保存先の失敗は、sourceと報告を持つ`MigrationError`になります。何もコミットされないため、件数は0です。

### 薄いCLI（SQLite）

このcheckoutから実行します。

```sh
cargo run -p event-store-adapter-migration-rs -- --help
cargo run -p event-store-adapter-migration-rs -- \
  --old-sqlite old.sqlite3 --sqlite new.sqlite3 \
  --type-mapping type-mapping.json
```

| Flag | 要件 |
|:-----|:-----|
| `--old-sqlite` | 必須。旧v3ファイルのパス |
| `--sqlite` | 必須。初期化済みで空の新ファイルのパス |
| `--type-mapping` | 任意。JSONファイル。DynamoDBの移行と共通 |
| `--help`／`-h` | 利用方法と運用前提を表示 |

SQLiteのflagとDynamoDBのflag（`--old-journal`・`--old-snapshot`・`--journal`・`--snapshot`・`--head`・`--history-index`・`--endpoint-url`・`--region`）は混ぜられません。混ぜると、移行前に不正引数として失敗します。DynamoDBの呼び出し方は変わりません。

CLIは`aggregates`・`events`・`snapshots`・`reasons`を持つ`MigrationReport` JSONを出力します。検査拒否と保存先の失敗は非0終了となり、保存先の失敗では報告とエラーも出力します。ファイル作成・書込停止のflagはありません。

[SQLiteのCLI integration試験](../migration-cli/tests/sqlite_cli_test.rs)は、ローカルのファイルに対して実バイナリを起動し、公開APIによる読み戻しと旧ファイルのバイト列の不変を確認します。Dockerは不要です。

```sh
cargo test -p event-store-adapter-migration-rs --test sqlite_cli_test
```
