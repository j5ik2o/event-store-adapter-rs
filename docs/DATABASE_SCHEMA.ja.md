# DynamoDBの保存schema（4系）

公開`EventStoreForDynamoDB`が使う実配置を説明します。[user-accountの実例](../examples/user-account/src/main.rs)は[表作成helper](../test-utils/src/dynamodb.rs)でこの配置を作成します。表・Streams・TTLの設定は利用者の責務です。

通常storeはこの新配置だけを読み取ります。v3の既定2表配置は明示的な[移行手順](MIGRATION_GUIDE_v4.ja.md)で扱います。SQLite・Bigtableの利用者は3系を継続してください。

## 3表とindex

3つの表名は互いに異なる必要があります。論理shard・`pkey`・journal GSIはありません。

| 表 | Partition key | Sort key | 追加設定 |
|:---|:--------------|:---------|:---------|
| Journal | `aid`（S） | `seq_nr`（N） | 本体へのQueryで再生 |
| Snapshot | `aid`（S） | `skey`（N） | 下記の履歴GSI。TTL保持を使う場合は`ttl`属性のTTLを有効化 |
| Head | `aid`（S） | なし | `NEW_IMAGE`のStreams |

snapshot履歴GSIは`aid`（S）と`active_history_seq_nr`（N）、projectionは`KEYS_ONLY`です。名前は`DynamoDbTables::snapshot_history_index_name`で指定します。

`aid`は`AggregateId`の型名と値から組み立てるUTF-8の`type_name-value`文字列です。型名にはhyphenを含められず、値には含められます。全体の上限は1024バイトです。空の型名・値も許可します。

## 設定項目

非同期の`open`／`open_with_serializers`は強整合の`BatchGetItem`で3つの設定項目を読み取ります。

| 表 | 設定キー |
|:---|:---------|
| Journal | `aid = "__config__"`、`seq_nr = 0` |
| Snapshot | `aid = "__config__"`、`skey = 0` |
| Head | `aid = "__config__"` |

各項目は共通の`store_id`（S、生成したUUID）と`layout_version`（N、1）を持ちます。全て不在なら条件付き`TransactWriteItems`でまとめて作成します。一部だけの設定・store ID不一致・未対応layout版・表名重複・SDK retry sleeper不在は設定エラーになります。Client optionsは設定属性として保存しません。

未処理キーは強整合を維持し、有限のbackoffで再要求します。既定は再要求10回・初期待機50ms・最大待機2秒で、`DynamoDbOptions`から変更できます。

## Journal項目

イベント封筒1件が1項目になります。

| 属性 | 型 | 意味 |
|:-----|:---|:-----|
| `aid` | S | 集約ID全体 |
| `seq_nr` | N | ドメイン採番のイベント番号。1から`SEQ_NR_MAX`まで |
| `occurred_at` | N | イベントが供給する符号付き64bitのUnix epochナノ秒 |
| `manifest` | S | 封筒のmanifest。省略時は空文字列 |
| `payload` | B | イベントpayloadだけをserializerへ渡した出力 |

再生は`aid`と`seq_nr >= 下限`で本体へ強整合・昇順の`Query`を行い、全ページを読み取ります。下限0は先頭から読みます。payloadバイトとナノ秒時刻は指定serializerと封筒を通して往復し、payloadへメタデータを注入しません。

## Head項目

| 属性 | 型 | 意味 |
|:-----|:---|:-----|
| `aid` | S | 集約ID全体 |
| `type_name` | S | 集約の型名 |
| `seq_nr` | N | 最後に確定したイベント番号 |
| `events` | L | 今回のイベントの`seq_nr`・`occurred_at`・`manifest`・`payload`を持つMを1件格納 |

snapshotなしを含む全追記でheadを更新します。`events`はhead表のstreamへ追記イベントを渡します。

## Snapshot項目

| 属性 | 型 | 意味 |
|:-----|:---|:-----|
| `aid` | S | 集約ID全体 |
| `skey` | N | currentは0、履歴は実snapshot番号 |
| `seq_nr` | N | snapshotの反映番号 |
| `manifest` | S | snapshot封筒のmanifest。省略時は空文字列 |
| `payload` | B | 集約だけをserializerへ渡した出力 |
| `last_updated_at` | N | イベント発生時刻のUnix epochミリ秒 |
| `active_history_seq_nr` | N | active履歴の実番号。currentとTTL印付け済み履歴には存在しない |
| `ttl` | N | TTL印付け済み履歴だけに設定するepoch秒の期限 |

currentには`version`・`ttl`・`active_history_seq_nr`がありません。active履歴は`RetentionSettings::keep_latest(n)`設定時だけ書き込みます。TTL印付け時はpayload・manifest・番号・更新時刻を維持し、`active_history_seq_nr`を除去します。そのためDynamoDBが実削除する前に履歴GSIから外れます。

## 書込と読取

`persist_event`はjournal Putとhead Put／Updateの2 actionを単一の`TransactWriteItems`で確定します。番号1は条件付きでheadを新規作成し、以降は`head.seq_nr == event.seq_nr - 1`を要求します。journal Putも同じキーの不在を条件にします。

`persist_event_and_snapshot`はcurrent snapshot Putを追加し、履歴設定があればhistory snapshot Putも追加します。同じtransactionの3または4 actionです。イベントとsnapshotの番号は一致が必要です。イベント単独の追記ではsnapshot表を変更しません。呼出し側が楽観ロックのversionを渡すことはありません。

`get_latest_snapshot_by_id`はheadとcurrentを強整合の`BatchGetItem`で読み、未処理キーを有限回再要求します。headなしは`None`、headだけなら`SnapshotRead::new(None, head_seq_nr)`です。headとsnapshotの番号は独立しており、並行読取では異なる書込時点の値を観測できます。[repository実例](../examples/user-account/src/user_account_repository.rs)はsnapshot後、またはsnapshotなしなら作成から再生し、観測headへの到達を確認します。

transaction前にjournal・head・current・historyの項目サイズ上界を409600バイトと比較します。超過は部分書込なしの契約違反になります。

## 保持

履歴件数が設定されている場合、履歴snapshotを書いた追記の確定後だけ保持処理を実行します。snapshot GSIをQueryし、今回書いた履歴番号を結果へ合わせ、新しい順に設定件数を残します。イベント単独の追記では保持処理を実行しません。`keep_latest(0)`は無効です。

- `RetentionMode::Delete`は超過履歴を有限のbatchと再要求で削除します。
- `RetentionMode::Ttl { grace_seconds }`は印付け時計のepoch秒＋猶予を期限として、`SET ttl = 期限 REMOVE active_history_seq_nr`で超過履歴を印付けします。DynamoDB TTLは別途設定します。猶予0も利用できます。
- `current_only()`は履歴を書きません。この保持でcurrentを期限切れにしません。
- 保持失敗はaid・seq_nr・phase・errorを含む`tracing`警告で通知します。追記は確定済みで、成功を返します。

Memoryは同じ公開封筒と番号規則を使い、表の代わりに`MemoryStorage`へ直列化バイトを保存します。storage cloneは状態を共有し、独立して生成したstorageは隔離します。Memoryは履歴件数とTTLの併用を拒否します。
