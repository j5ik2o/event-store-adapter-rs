# Rust v3から5系APIへの移行

5.0.0は、5系APIの最初の版です。受入済みMemory・DynamoDB本体を`event_store_adapter_rs`直下から利用します。`next` namespace・旧API別名・透過的な旧形式読取fallbackはありません。

## アプリケーションの変更

- `AggregateId::type_name()`・`value()`を実装し、`KeyResolver`と論理shard設定を除去します。
- crate直下から`EventStore`・`EventEnvelope`・`SnapshotEnvelope`・`SnapshotRead`・`SeqNr`・`EventStoreError`をimportします。旧読取／書込エラーを`EventStoreError`の5分類へ置き換えます。
- `expected_version`引数とsnapshotの`version`を除去します。最初のイベントは1、以降は連続した番号で`SEQ_NR_MAX`までです。対になったイベントとsnapshotの番号は一致させます。
- Memoryは`EventStoreForMemory::new(MemoryStorage::new(retention)?)`で生成します。storageのcloneでstore間の保存状態を共有し、独立したstorageで隔離します。
- DynamoDBは事前作成した3表・履歴GSIと、SDK retry sleeperを設定したClientで、非同期の`EventStoreForDynamoDB::open(client, tables, options).await`を呼びます。
- SQLiteはfeature `sqlite`を有効にし、新しいファイルに`initialize_sqlite_store(path, &options)`を1回呼んでから、同期の`EventStoreForSqlite::open(path, options)`を呼びます。`open`は何も作りません。この保存先は変更フィードを提供しません。[README](../README.ja.md)と[SQLite schema](SQLITE_DATABASE_SCHEMA.ja.md)を参照してください。
- Bigtableはfeature `bigtable`を有効にし、列ファミリ付きで2つのテーブルを作り、`initialize_bigtable_store(&connection, &tables, &options).await`を1回呼んでから、`EventStoreForBigtable::open(connection, tables, options).await`を呼びます。`open`は何も作りません。この保存先は変更フィードを提供しません。本番のCloud BigtableにはTLSのchannelと`BigtableTokenSource`（`open_with_token_source`）が要ります。[README](../README.ja.md)と[Bigtable schema](BIGTABLE_DATABASE_SCHEMA.ja.md)を参照してください。
- `SnapshotRead::snapshot()`が不在なら作成イベントから再生します。snapshotとheadの番号は別の意味です。snapshot後のイベントを読み、観測headまで再生できたことを確認します。[コンパイルしたrepository例](../examples/user-account/src/user_account_repository.rs)がこの場合を扱います。
- 非serde payloadには`with_serializers`／`open_with_serializers`へイベント・snapshot serializerを渡します。4操作は[README](../README.ja.md)、保持の実配置は[schema](DATABASE_SCHEMA.ja.md)を参照してください。

`sqlite-system`のfeature、本体、利用例は削除しました。feature `sqlite`と`bigtable`は、独自の配置を持つ5系の新しい保存先です。v3.0.6では`sqlite`と`sqlite-system`が同じSQLite実装を選ぶため、どちらで書いたファイルも、下記の[SQLiteの移行](#v3-sqliteデータの移行)で移します。v3のBigtable保存先が書いたデータは、下記の[Bigtableの移行](#v3-bigtableデータの移行)で移します。

## 保存データの対象

対象は**Rust v3の既定DynamoDB配置**だけです。旧journal・snapshotは`pkey`／`skey`キーを使用します。独自`KeyResolver`配置・v2データ・他の保存先は対象外です。v3の既定SQLiteファイルには、下記の[SQLiteの移行](#v3-sqliteデータの移行)があり、v3の既定のBigtableのテーブルには、下記の[Bigtableの移行](#v3-bigtableデータの移行)があります。通常の5系storeは旧表を読みません。

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

## v3 Bigtableデータの移行

v3のBigtable保存先は、journalテーブルとsnapshotテーブルの2つのテーブルを書きました。5系のBigtable保存先は別の配置（[schema](BIGTABLE_DATABASE_SCHEMA.ja.md)）を使い、journalテーブルとheadテーブルを持ち、履歴はjournalの行に置きます。データは**新しい2つのテーブル**へ写します。旧テーブルへ送る要求は`ReadRows`だけで、旧テーブルは変更しません。通常の5系storeは旧テーブルを読みません。

### 保存データの対象（Bigtable）

対象は**Rust v3.0.6の既定Bigtable配置**だけです。行キーを既定の`KeyResolver`で組み立てた、journalテーブルとsnapshotテーブルです。

| 行 | 行キー | セル |
|:---|:-------|:-----|
| journal | `{pkey}#{型名}#{値}#{seq_nr（0詰めの20桁）}`。`pkey = {型名}-{shard}` | 列ファミリ`event`: `payload`・`aggregate_id`・`seq_nr`・`occurred_at`（ナノ秒つきのRFC 3339）・`manifest` |
| snapshot（現在） | `{pkey}#{型名}#{値}` | 列ファミリ`snapshot`: `payload`・`seq_nr`（と、写さない`version`・`last_updated_at`） |
| snapshot（履歴） | 現在の行キーの後ろに`#{seq_nr（0詰めの20桁）}` | 同じセル。後のsnapshotに置き換えられたsnapshotを、その位置で書いた行 |

独自`KeyResolver`配置・v2データ・他の保存先は対象外で、既定の配置に従わない行は全件検査で拒否します。

migrationは、DynamoDBの移行と同じ規則で、型名と値を行キーだけから再構成します。`aggregate_id`のセルは、キーから取り出した値の照合にだけ使います。hyphenを含む旧型名には、[前述](#保存データの対象)のJSON対応表が必要です。対応表を使うのはhyphenありの旧型名だけで、対応先の衝突・対応後に残るhyphen・1024バイトを超える`aid`は拒否します。値に`#`を含むと、snapshotの行キーを複数の読み方で読める場合があります。migrationは推測しません。旧journalで確かめた集約に、ちょうど1つの読み方が当たるときだけ受け、それ以外は拒否します。

写すものは次のとおりです。

- **イベント。** payloadバイトは復元も再直列化もせずに写します。RFC 3339の発生時刻は、符号付き64ビットのナノ秒の値にします。読めない時刻や範囲に収まらない時刻は拒否します。manifestは保存されている値のまま写し、空のmanifestは空のままです。集約ごとに、番号は1から欠番・重複なしで続く必要があり、headは最大番号のイベントから作ります。すべてのイベント封筒が、新しいstoreの封筒の上限に収まる必要があります。
- **現在のsnapshot。** 番号の接尾辞がない行を、空のmanifestで、headの`snapshot:current`へ写します。位置は集約の最大イベント番号以下で、対応するイベントが存在する必要があります。旧`version`と`last_updated_at`は捨てます。headの`has_snapshot`は、現在のsnapshotが最大のイベント番号にあるときだけ`1`です。
- **snapshotの履歴。** v3の履歴の行は、新しいsnapshotに置き換えられたsnapshotを、その位置で書いたものです。v5は、headの現在のsnapshotも履歴件数に数えます（[Bigtable schema](BIGTABLE_DATABASE_SCHEMA.ja.md#保持)）。そのためmigrationは、旧の履歴の位置と現在のsnapshotの位置を合わせた集合から、位置の新しい`n`件を残します。`n`は**新しいテーブルの**履歴件数です。残した`n`件は、それぞれの位置のjournalの行に`active`な履歴として書きます（現在のsnapshotの位置も書き、そのsnapshotは`snapshot:current`にも残ります）。新しいテーブルが現在のsnapshotだけを保持するなら、履歴は写しません。保持していなかった履歴は補いません。`event:has_snapshot`は、履歴を写したかどうかにかかわらず、これらの位置のすべてで`1`です。どちらの場合も旧の履歴の行はすべて検査します。現在のsnapshotの位置以上の履歴の行、または現在のsnapshotがない履歴の行があると、移行を拒否します。

### 運用手順（Bigtable）

1. 検査前に**旧の2つのテーブルへの全書込を停止**し、移行中も停止を維持します。関数とCLIはこの運用前提を強制しません。
2. 列ファミリ付きで新しい2つのテーブルを作り、そのためのapp profileを作って、希望する履歴件数・封筒の上限・app profileで`initialize_bigtable_store`（[schema](BIGTABLE_DATABASE_SCHEMA.ja.md#初期化)）を呼びます。新しいテーブルは初期化済みで**データの行がない**必要があります。設定の行以外のどの行があっても移行を拒否し、migrationはテーブルも列ファミリも設定の行も作りません。旧と新のテーブルは別のテーブルでなければなりません。パス（プロジェクト・インスタンス・テーブルID）を比べるので、旧テーブルは別のプロジェクトやインスタンスにあってかまいません。
3. 旧と新のテーブル、初期化に使ったものと**同じ設定**、型対応表を指定し、下記の関数またはCLIを実行します。
4. JSON報告と終了statusを確認します。新しいテーブルを同じoptionsで公開5系APIから開き、移行後データ（`get_latest_snapshot_by_id`と`get_events_by_id_since_seq_nr`）を読み戻してから、アプリケーションの書込先を切り替えます。読んだ内容を、headの番号や数個の集約のイベントなど、期待する内容と比べます。
5. 検査拒否があっても、**新しいテーブルには何も書きません**。旧テーブルも変わりません。原因を解消し、同じ新しいテーブルへ再実行できます。設定と保存された設定の食い違いや、初期化されていない新しいテーブルも、何も書かずにエラーを返します。
6. 書込が途中で失敗した場合（接続の切断、Bigtableからのエラー、既にあるheadなど）は、**部分的な移行が新しいテーブルに残ります**。Bigtableには行をまたぐトランザクションがないため、それまでに書いた行が残るからです。migrationは、止まった所から再開しません。そのテーブルへの再実行は、データの行があるため拒否されます。次の手順でやり直します。
   1. 旧の2つのテーブルへの書き手を止めたままにします。旧テーブルは消しません。
   2. **新しい2つのテーブル**を消し、列ファミリ付きで作り直します。
   3. 同じ設定で`initialize_bigtable_store`をもう一度呼びます。
   4. 同じ入力で、移行をもう一度実行します。

全件検査は、新しいテーブルへ何かを書く前に、旧の2つのテーブルの全行をページごとに読みます。保持するのは集約ごとの小さな要約だけで、payloadは持ちません。拒否はすべて報告に集めます。欠番と重複、既定の配置に従わない、または一意に読み解けないキー、欠けている、またはキーと食い違うセル、孤立snapshotと集約の最大イベント番号を超えるsnapshot、対応表のない・衝突する・長すぎるID、読めない発生時刻、新しい上限を超える封筒です。検査に通ると、旧journalをキーの順にもう一度読み、通常の追記と同じ順で集約ごとに書きます。最大のイベントより前の行を先に、次にhead、最後に最大のイベントのjournalの行です。headが`h`のとき、イベント`1`から`h - 1`はjournalにあり、イベント`h`はheadにある状態が常に保たれるので、公開APIで読んでも欠番は見えません。payloadは、書くときに旧テーブルから読み直します。

拒否の理由は`table`に、完全なパス`projects/<project>/instances/<instance>/tables/<table>`で対象を示します。旧データの行は旧テーブル、すでにデータの行がある新しいテーブルは新しいテーブル、旧と新のテーブルが重なっているなら空文字列です。報告の`aggregates`は作ったheadの数、`events`は書いたjournalの行数、`snapshots`は書いたsnapshot（現在と履歴）の数です。件数に含めるのは、成功の応答を受けた書込だけです。検査で見つかった拒否では、件数は0のままです。

### Library関数（Bigtable）

`features = ["bigtable-migration"]`を指定すると`bigtable`も有効になりますが、DynamoDBとSQLiteは有効になりません。`BigtableConnection`（`connection`）を持つ非同期の文脈で次のように呼びます。

```rust
use std::collections::HashMap;
use event_store_adapter_rs::{
  migrate_v3_bigtable, BigtableOptions, BigtableTables, LegacyBigtableTables,
  RetentionSettings,
};

let legacy = LegacyBigtableTables {
  project_id: "my-project".into(),
  instance_id: "my-instance".into(),
  journal_table_id: "old-journal".into(),
  snapshot_table_id: "old-snapshot".into(),
};
let tables = BigtableTables {
  project_id: "my-project".into(),
  instance_id: "my-instance".into(),
  journal_table_id: "journal".into(),
  head_table_id: "head".into(),
};
// The same settings that were passed to initialize_bigtable_store.
let options = BigtableOptions {
  retention: RetentionSettings::keep_latest(2),
  ..BigtableOptions::default()
};
let mapping = HashMap::from([("user-account".into(), "UserAccount".into())]);
let report = migrate_v3_bigtable(&connection, None, &legacy, &tables, &options, &mapping).await?;
assert!(report.reasons.is_empty(), "{:?}", report.reasons);
```

2番目の引数はトークンの取得元です。エミュレーターでは`None`を渡します。本番のCloud Bigtableでは、`Some(Arc<dyn BigtableTokenSource>)`とTLSのchannelを持つ`connection`を渡します。このとき、トークンは旧テーブルへの要求にも新しいテーブルへの要求にも付きます。1つの`connection`が両方のテーブルに使われるため、旧と新のテーブルは同じ2つのchannelでつながる必要があります。`options`のapp profileは、新しいテーブルへの要求にだけ付けます。

`Ok(MigrationReport)`でも`reasons`に拒否理由があるため、呼出し側で確認します。検査で見つかった拒否では件数が0のままです。作ろうとしたheadが既にあった場合も拒否として報告し、それまでに書いた分の件数を持ちます。通信や保存先の失敗は、sourceと途中までの報告を持つ`MigrationError`になります。件数は、成功の応答を受けた書込を反映します。履歴件数や封筒の上限の違い、列ファミリの不一致、初期化されていない新しいテーブルといった設定の食い違いは、何も書く前に`MigrationError`になります。

### 薄いCLI（Bigtable）

このcheckoutから、Bigtableエミュレーターに対して実行します。

```sh
cargo run -p event-store-adapter-migration-rs -- --help
cargo run -p event-store-adapter-migration-rs -- \
  --old-bigtable-project my-project --old-bigtable-instance my-instance \
  --old-bigtable-journal old-journal --old-bigtable-snapshot old-snapshot \
  --bigtable-project my-project --bigtable-instance my-instance \
  --bigtable-journal journal --bigtable-head head \
  --bigtable-keep-snapshot-count 2 \
  --bigtable-emulator-host localhost:8086 \
  --type-mapping type-mapping.json
```

| Flag | 要件 |
|:-----|:-----|
| `--old-bigtable-project`、`--old-bigtable-instance` | 必須。旧テーブルのプロジェクトとインスタンス |
| `--old-bigtable-journal`、`--old-bigtable-snapshot` | 必須。旧テーブルのID |
| `--bigtable-project`、`--bigtable-instance` | 必須。新しいテーブルのプロジェクトとインスタンス |
| `--bigtable-journal`、`--bigtable-head` | 必須。新しいテーブルのID |
| `--bigtable-keep-snapshot-count` | 任意。履歴件数。省略すると「現在のsnapshotだけ」。新しいテーブルの初期化に使った値と一致が必要 |
| `--bigtable-envelope-limit-bytes` | 任意。封筒の上限。既定は1048576。新しいテーブルの初期化に使った値と一致が必要 |
| `--bigtable-app-profile` | 任意。新しいテーブルへの要求に付けるapp profileのID。省略するとインスタンスの既定 |
| `--bigtable-emulator-host` | 任意。Bigtableエミュレーターの`HOST:PORT`。省略すると本番のCloud Bigtableへ接続 |
| `--type-mapping` | 任意。JSONファイル。DynamoDB・SQLiteの移行と共通 |
| `--help`／`-h` | 利用方法と運用前提を表示 |

**エミュレーターか本番か。** `--bigtable-emulator-host`を指定すると、CLIは平文で接続し、認証を付けません。指定しないと、CLIは本番のCloud Bigtable（データは`bigtable.googleapis.com`、adminは`bigtableadmin.googleapis.com`）へ、プラットフォームのネイティブなルート証明書を使ってTLSで接続し、`gcp_auth`クレートによるApplication Default Credentialsでアクセストークンを取得します。認証情報の探索は`gcp_auth`の通常の順序に従います（たとえば、`GOOGLE_APPLICATION_CREDENTIALS`が指すファイル、gcloudツールの認証情報、Google Cloudのメタデータサーバー）。トークンは要求のたびに取得し、更新は`gcp_auth`が行います。認証情報には、旧テーブルを読む権限と、新しいテーブルを読み・書き・schemaを調べる権限が必要です。TLSの実装にも`gcp_auth`にも、ライブラリ本体は依存せず、CLIだけが依存します。

Bigtableのflagと、DynamoDBまたはSQLiteのflagは混ぜられません。混ぜると、移行前に不正引数として失敗し、標準出力には何も出力しません。DynamoDBとSQLiteの呼び出し方は変わりません。

CLIは`aggregates`・`events`・`snapshots`・`reasons`を持つ`MigrationReport` JSONを出力します。検査拒否と保存先の失敗は非0終了となり、保存先の失敗では途中までの報告とエラーも出力します。書込停止の運用前提は標準エラーに出力します。テーブル作成・初期化・書込停止のflagはありません。

[BigtableのCLI integration試験](../migration-cli/tests/bigtable_cli_test.rs)は、Bigtableエミュレーターに対して実バイナリを起動し、公開APIによる読み戻しと旧テーブルの不変を確認します。Googleの本番サービスには接続しません。Dockerが必要です。

```sh
cargo test -p event-store-adapter-migration-rs --test bigtable_cli_test
```
