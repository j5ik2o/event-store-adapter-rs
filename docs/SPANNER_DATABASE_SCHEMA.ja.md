> この文書は [SPANNER_DATABASE_SCHEMA.md](SPANNER_DATABASE_SCHEMA.md) の日本語版です。英語版が正本で、食い違うときは英語版に従います。

# Spanner の保存先のスキーマ

公開`EventStoreForSpanner`（feature `spanner`）が使う5系の実配置を説明します。ライブラリが設定の行を書くのは、明示的な`initialize_spanner_store`だけです。通常の`EventStoreForSpanner::open`は、テーブルも行も行削除ポリシーも作りません。データベース、テーブル、行削除ポリシーは、常に利用者が作ります。

この保存先の最初の版は、GoogleSQL方言のデータベースだけを対象にし、変更フィードを提供しません。以前の版にはSpannerの保存先がないため、移行元の旧配置はありません。新しいストアの始め方は[移行ガイド](MIGRATION_GUIDE.ja.md)を参照してください。

## 配置

### テーブルとキー

ストアは、1つのデータベースにある5つのテーブルを使います。名前は、1つの接頭辞（以下の例では`esa`）からまとめて導きます。`<prefix>_journal`・`<prefix>_head`・`<prefix>_snapshot_current`・`<prefix>_snapshot_history`・`<prefix>_config`です。テーブル名を個別に指定することはできないため、別のストアのテーブルを組み合わせたストアは作れません。

| テーブル | 主キー | その他の列 |
|:---------|:-------|:-----------|
| journal | `(hash_prefix BYTES(8), aid STRING(1024), seq_nr INT64)` | `occurred_at_ns INT64`、`manifest STRING(MAX)`、`payload BYTES(MAX)` |
| head | `(hash_prefix BYTES(8), aid STRING(1024))` | `type_name STRING(1024)`、`seq_nr INT64`、`event_occurred_at_ns INT64`、`event_manifest STRING(MAX)`、`event_payload BYTES(MAX)` |
| snapshot_current | `(hash_prefix BYTES(8), aid STRING(1024))` | `seq_nr INT64`、`manifest STRING(MAX)`、`payload BYTES(MAX)` |
| snapshot_history | `(hash_prefix BYTES(8), aid STRING(1024), seq_nr INT64)` | `manifest STRING(MAX)`、`payload BYTES(MAX)`、`expired_at TIMESTAMP`（NULLを許す） |
| config | `singleton INT64`（常に1） | `store_id STRING(36)`と、「設定」で説明する設定 |

`aid`は、`AggregateId`から組み立てたUTF-8の文字列`type_name-value`です。型名にはhyphenを含められず、値には含められます。全体の上限は1024バイトで、ライブラリは書く前にこれを検査します。

- `hash_prefix`は、`aid`のUTF-8のバイト列の[FNV-1a 64](https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function)のハッシュを、8バイトのビッグエンディアンにしたものです。符号付き整数として読み替えず、シャード数による剰余も使いません。
- キーは、型付きの組`(hash_prefix, aid)`の完全一致で比べます。`aid`の前方一致は使いません。そのため、ハッシュが衝突する2つの集約も主キーは別で、互いの行を読むことはありません。
- `seq_nr`は1以上2^53 − 1以下です。`occurred_at_ns`は、Unixエポックからの符号付き64ビットのナノ秒で、TIMESTAMP列ではなくINT64列に入れます。ライブラリは、INT64をすべて10進の文字列で、BYTESをすべてbase64で送受信し、浮動小数点数を経由する値はありません。

### 接頭辞とテーブル名

導いた5つの名前は、SQLの識別子でなければなりません。先頭はASCIIの英字か`_`、続きはASCIIの英数字か`_`で、128文字以下です。接頭辞が空、または無効な名前を導く場合は、設定エラー（`ConfigurationReason::SpannerInvalidTablePrefix`）になり、要求を送る前に返ります。ライブラリが送るSQLでは、テーブル名を常に引用符で囲み、値は常にパラメーターで渡します。

## 設定

### 設定の行

`config`テーブルの行は、`singleton = 1`の1行だけです。

| 列 | 型 | 要件 |
|:---|:---|:-----|
| `singleton` | INT64 | 主キー。常に1 |
| `store_id` | STRING(36) | 小文字でhyphen区切りのUUID v4。初期化で生成 |
| `layout_version` | INT64 | 1 |
| `keep_snapshot_count` | INT64 | 「現在のsnapshotだけ」ならNULL、それ以外は正の整数 |
| `retention_mode` | STRING(8) | `delete`または`ttl` |
| `ttl_grace_days` | INT64 | `delete`ならNULL、`ttl`なら正の日数 |
| `change_feed_enabled` | BOOL | false（最初の版はtrueを拒否） |

### クライアントの設定との一致

`open`と`initialize_spanner_store`は、strongの読み取り専用の問い合わせ（`INFORMATION_SCHEMA`と、`spanner.databases.get`の権限が要るデータベース管理API）でデータベースを読み、次の順に照合します。方言、5つのテーブルの列と主キー、`snapshot_history`の行削除ポリシー、そして`open`では設定の行です。`SpannerOptions::retention`の値は行の記録と等しく、行削除ポリシーは保持の方式に合っていなければなりません。

- `CurrentOnly`は、`keep_snapshot_count = NULL`と`retention_mode = 'delete'`を記録します。
- `KeepLatest { count }`は、`keep_snapshot_count = count`と`retention_mode = 'delete'`を記録します。
- `KeepLatestWithTtl { count, grace_days }`は、`retention_mode = 'ttl'`と`ttl_grace_days = grace_days`を記録します。`snapshot_history`には、ちょうどその日数の`ROW DELETION POLICY (OLDER_THAN(expired_at, INTERVAL <grace_days> DAY))`が必要です。比較では、大文字小文字と空白の違いを無視します。別の列、別の単位、別の日数のポリシーは不一致です。
- 削除方式では、`snapshot_history`に行削除ポリシーを付けません。

照合はDDLを実行せず、足りないものを補うこともしません。失敗はすべて設定エラー（`EventStoreError::Configuration`）です。ただし、接続やサービスの失敗は保存先エラー（`EventStoreError::Storage`）です。

| `ConfigurationReason` | 意味 |
|:----------------------|:-----|
| `SpannerInvalidTablePrefix` | 接頭辞から有効なテーブル名を導けない |
| `SpannerLayoutMismatch` | データベースがGoogleSQLでない、またはテーブル・列・型・主キーが配置と異なる |
| `SpannerConfigurationInvalid` | 設定の行が欠けている、または壊れている |
| `SpannerConfigurationMismatch` | 版・履歴件数・保持の方式・猶予の日数・行削除ポリシーが、クライアントと異なる |
| `SpannerChangeFeedUnsupported` | `change_feed`がtrue、または行が`change_feed_enabled = true`を記録している |
| `SpannerTtlGraceDaysOutOfRange`、`SpannerKeepSnapshotCountOutOfRange`、`KeepSnapshotCountZero` | 値が0、またはINT64の列に収まらない。要求を送る前に返り、値は丸めない |
| `SpannerAlreadyInitialized` | `initialize_spanner_store`が返す。設定の行がすでにある、またはいずれかのテーブルに行がある |

## 初期化

### テーブルの作成

GoogleSQL方言のデータベースを作り、書込を行うものがいない間に、次のDDLを1回適用します。名前には接頭辞`esa`を使っています。自分の接頭辞に、すべて同じように置き換えてください。

<!-- spanner-schema:ddl -->
```sql
CREATE TABLE esa_journal (
  hash_prefix BYTES(8) NOT NULL,
  aid STRING(1024) NOT NULL,
  seq_nr INT64 NOT NULL,
  occurred_at_ns INT64 NOT NULL,
  manifest STRING(MAX) NOT NULL,
  payload BYTES(MAX) NOT NULL
) PRIMARY KEY (hash_prefix, aid, seq_nr);

CREATE TABLE esa_head (
  hash_prefix BYTES(8) NOT NULL,
  aid STRING(1024) NOT NULL,
  type_name STRING(1024) NOT NULL,
  seq_nr INT64 NOT NULL,
  event_occurred_at_ns INT64 NOT NULL,
  event_manifest STRING(MAX) NOT NULL,
  event_payload BYTES(MAX) NOT NULL
) PRIMARY KEY (hash_prefix, aid);

CREATE TABLE esa_snapshot_current (
  hash_prefix BYTES(8) NOT NULL,
  aid STRING(1024) NOT NULL,
  seq_nr INT64 NOT NULL,
  manifest STRING(MAX) NOT NULL,
  payload BYTES(MAX) NOT NULL
) PRIMARY KEY (hash_prefix, aid);

CREATE TABLE esa_snapshot_history (
  hash_prefix BYTES(8) NOT NULL,
  aid STRING(1024) NOT NULL,
  seq_nr INT64 NOT NULL,
  manifest STRING(MAX) NOT NULL,
  payload BYTES(MAX) NOT NULL,
  expired_at TIMESTAMP
) PRIMARY KEY (hash_prefix, aid, seq_nr);

CREATE TABLE esa_config (
  singleton INT64 NOT NULL,
  store_id STRING(36) NOT NULL,
  layout_version INT64 NOT NULL,
  keep_snapshot_count INT64,
  retention_mode STRING(8) NOT NULL,
  ttl_grace_days INT64,
  change_feed_enabled BOOL NOT NULL
) PRIMARY KEY (singleton);
```

Spanner TTL方式では、`snapshot_history`に行削除ポリシーも付けます。次の7日は例であり、既定値ではありません。日数は`grace_days`と`ttl_grace_days`に一致させてください。削除方式のストアには、このポリシーを付けません。

<!-- spanner-schema:ttl-policy -->
```sql
ALTER TABLE esa_snapshot_history ADD ROW DELETION POLICY (OLDER_THAN(expired_at, INTERVAL 7 DAY));
```

### 設定の行の書き込み

DDLの後、最初の書込を行うものが起動する前に、`initialize_spanner_store`を1回呼びます。

```rust
use event_store_adapter_rs::{initialize_spanner_store, SpannerConnection, SpannerOptions, SpannerRetention, SpannerTables};

async fn initialize(connection: &SpannerConnection, tables: &SpannerTables) -> Result<(), event_store_adapter_rs::EventStoreError> {
  let options = SpannerOptions {
    retention: SpannerRetention::KeepLatestWithTtl { count: 2, grace_days: 7 },
    ..SpannerOptions::default()
  };
  initialize_spanner_store(connection, tables, &options).await
}
```

この関数は、設定を検査し、設定の行以外について上の照合を行います。その後、SERIALIZABLEの読み書きトランザクション1つで、5つのテーブルがすべて空であることを確かめ、生成したUUIDを持つ設定の行を挿入します。設定の行がすでにある、またはいずれかのテーブルに行がある場合は、`SpannerAlreadyInitialized`を返し、何も書きません。テーブルは作りません。

手で行を用意する場合は、次を実行します。`store_id`には小文字のUUID v4を使います。例は、新しい2件の履歴snapshotを保持し、猶予を7日にするSpanner TTL方式のストアです。現在のsnapshotだけを保持するなら`keep_snapshot_count`を`NULL`に、削除方式なら`retention_mode`を`'delete'`、`ttl_grace_days`を`NULL`にします。

<!-- spanner-schema:config -->
```sql
INSERT INTO esa_config (singleton, store_id, layout_version, keep_snapshot_count, retention_mode, ttl_grace_days, change_feed_enabled) VALUES (1, '5f0c8b8e-3c1a-4d7e-9a4b-2f6d1c0e7a11', 1, 2, 'ttl', 7, false);
```

## 記録

### journalの行

イベント1件が、`(hash_prefix, aid, seq_nr)`をキーとするjournalの行1つです。行は挿入されるだけで、ライブラリが上書きも削除もすることはありません。`manifest`はイベントのmanifestで、ないときは空文字列です。`payload`は、イベントのpayloadだけをserializerで変換したもの（既定はJSON）で、manifestと時刻は列に持ちます。

### headの行

headテーブルは、集約ごとに1行を持ちます。最新イベントの`seq_nr`と、そのイベントの発生時刻・manifest・payloadの写しです。`type_name`は集約の作成時（`seq_nr = 1`）に書き、以後は変えません。headは並行性を決める記録で、追記は、ここに保存された`seq_nr`に対してだけ受け付けます。

### snapshotの行

`snapshot_current`は、集約の最新のsnapshotを持ち、snapshotを伴う追記のたびに置き換えます。イベントだけで作った集約には、ここに行がありません。履歴件数を設定している場合は、同じsnapshotを`expired_at = NULL`で`snapshot_history`にも挿入します。TTL方式では、のちにライブラリが`expired_at`を、サーバーの`CURRENT_TIMESTAMP()`に1度だけ設定します。`NULL`の行は、行削除ポリシーの対象になりません。

## 書き込みと読み取り

### 追記

追記は、SERIALIZABLEの読み書きトランザクション1つで行い、そのコミットが確定点です。

1. `(hash_prefix, aid)`のheadを読みます。
2. `seq_nr = 1`では、headがないときだけ続けます。それより大きい番号では、headがなければ0として扱います。headの`seq_nr`以下の番号は楽観ロックのエラー、飛び番は契約違反です。
3. イベントを`journal`に挿入します。
4. headを挿入（`type_name`を含む）、または新しい`seq_nr`とイベントの写しで更新します。
5. snapshotを伴う追記なら、`snapshot_current`を挿入または更新し、履歴件数を設定している場合は`snapshot_history`にも挿入します。
6. コミットします。履歴の整理は、このトランザクションに含めません。

直列化と範囲の検査は、トランザクションの前に行います。楽観ロックかどうかは、常にトランザクションの中で読んだheadで決まります。コミット時に報告される主キーの重複は、楽観ロックではなく保存先エラーです。

### 再試行と結果の不明

ライブラリが再試行するのは、Spanner（またはエミュレーター）が中断（ABORTED）したトランザクションだけです。同じイベント、同じ`seq_nr`で、番号の採り直しはしません。最初の再試行の前に10ミリ秒待ち、倍にしながら1秒まで伸ばします。`SpannerOptions::transaction_retry_limit`（既定は5）は再試行の回数で、最初の試行は数えません。使い切ると保存先エラー（`StorageOperation::Append`）になります。同じ集約で、同じ番号以上の追記が先にコミットされていた場合、再試行は楽観ロックのエラーになります。

コミットの応答が失われた場合、その追記は結果が不明な保存先エラーです。ライブラリは、再試行も補償の書き込みもせず、追記が失敗したとも断定しません。同じ`seq_nr`を送り直すと、最初の追記がコミットされていたなら、楽観ロックのエラーになります。コミットでの失敗を含め、そのほかの失敗は再試行しません。

### 読み取り

読み取りは、strongの読み取り専用トランザクションで行い、過去の読み取り時刻は使いません。`get_latest_snapshot_by_id`は、headと`snapshot_current`を1つのトランザクションで読むため、組は同じ読み取り時点のものです。headがなければ何も返さず、headだけがあればsnapshotなしでheadの位置を返します。`get_events_by_id_since_seq_nr`は、先にheadを読み、同じトランザクションで`max(1, seq_nr)`からheadの`seq_nr`までのjournalの行を昇順に読みます。範囲の番号が欠けている場合は保存先エラー（`StorageOperation::LoadEvents`）です。serializerが復号できないpayloadは、保存先エラーではなく直列化のエラーです。

結果は、`ExecuteStreamingSql`で最後まで読みます。Spannerが複数の応答に分けた値（`chunked_value`）は、復号の前に結合します。分けられた値の途中、または行の途中でストリームが終わった場合は、それまでに読んだものをすべて捨てて保存先エラーにし、一部の結果を返すことはありません。

## 保持

### 保持の方式

保持処理は、履歴件数`n`を設定していて、snapshotを伴う追記がコミットされた後に、別のSERIALIZABLEの読み書きトランザクションで始めます（1トランザクションあたり最大1000行で、毎回選び直します）。集約の`expired_at IS NULL`の行を`seq_nr`の降順に読み、新しい`n`件を残して、古い行だけを処理します。

- 削除方式: `snapshot_history`の古い行を削除します。
- Spanner TTL方式: 古い行の`expired_at`を`CURRENT_TIMESTAMP()`にします。1度だけです。印を付けた行は`n`に数えず、印を付け直すこともありません。Spannerは猶予の後に非同期で削除するため、ライブラリは物理的な削除に依存しません。

保持処理が`journal`・`head`・`snapshot_current`を変えることはありません。Spannerが中断（ABORTED）した保持のトランザクションは、追記と同じ`transaction_retry_limit`で再試行し、候補を選び直します。再試行を使い切った場合を含め、保持処理が失敗しても、確定した追記は成功を返し、集約・`seq_nr`・段階・エラーを含む`tracing`の警告を出します。次のsnapshotを伴う追記が、再び保持処理を試みます。

### 履歴件数の変更

動いているストアの、履歴件数・保持の方式・猶予の日数は変えないでください。設定が異なる書込を行うものは、`open`の照合に失敗します。変更は、すべてのクライアントを止めている間に行います。

1. ストアのすべてのクライアントを止め、追記・読み取り・保持処理が終わったことを確かめます。
2. 集約ごとに、SERIALIZABLEの読み書きトランザクションで、印のない履歴を`seq_nr`の降順に読み、変更後の設定の新しい`n`件（なければ0件）を残します。削除方式では古い行を削除し、TTL方式では`expired_at IS NULL`の行にだけ`expired_at = CURRENT_TIMESTAMP()`を設定します。大きな作業は分割し、トランザクションごとに選び直します。`journal`・`head`・`snapshot_current`は変えません。
3. すべての集約の処理が終わったら、`config`の行（猶予の日数を変えるときは行削除ポリシーも）を更新し、新しい設定でクライアントを起動し直します。

途中で失敗したら、クライアントを止めたまま、同じ目標でもう一度実行します。件数を増やす場合や、件数のないストアに件数を設定する場合に、すでに削除した履歴や印を付けた履歴が元に戻ることはありません。

## 運用

### 接続、認証、エミュレーター

`SpannerConnection::new(channel)`は、利用者が作って所有する`tonic::transport::Channel`を受け取ります。ライブラリはchannelを作らず、TLSの実装にもGoogleの認証のライブラリにも依存しません。同じchannelを、データAPIと、方言を読むデータベース管理APIの両方に使います。公開操作ごとにセッションを作って最後に削除し、セッションのプールは持ちません。

エミュレーターには、平文のchannel（`http://localhost:9010`）を使い、認証情報は要りません。ライブラリは、エミュレーターのイメージ`gcr.io/cloud-spanner-emulator/emulator`の1.5.58で試験しています。このイメージは`test-utils/src/docker.rs`でdigestを固定しています。Cloud Spannerには、`https://spanner.googleapis.com`へのTLSのchannelを作り、`SpannerConnection::with_token_source`で`SpannerTokenSource`を渡します。取得元はすべての要求の直前に呼ばれ、取得元が失敗した操作は、要求を送らずに保存先エラーになります。

### 一貫性の前提

上の規則は、5つのテーブルがすべて1つのデータベースにあり、そのデータベースの読み書きトランザクションとstrongな読み取りが成り立つ構成で成り立ちます。ライブラリは、すべての読み書きトランザクションにSERIALIZABLEを、すべての読み取りにstrongな読み取りを指定し、過去の読み取り時刻は使いません。リージョンの構成には、追加の制限を設けません。同じ設定を使うなら、1つのプロセス内でも複数のプロセスでも、複数のstoreのインスタンスが同じデータベースを共有してかまいません。運用中のDDLの変更と、ライブラリを通さないデータの変更は、対象外です。

エミュレーターは、競合がなくても重なった読み書きトランザクションを中断するため、上の再試行は並行実行の試験のすべてに含まれます。また、エミュレーターで成功しても、サービス側の競合検出を証明したことにはなりません。ミューテーションやコミットの大きさのサービス側の上限と、行削除ポリシーによる物理的な削除は、ここでは確かめていません。

### 上限

列とコミットの大きさの上限は、Spannerの[クォータ](https://cloud.google.com/spanner/quotas)に従います。ライブラリは、書く前に値の範囲と`aid`の長さを検査します。クォータによる拒否、接続の失敗、コミットの失敗は保存先エラーで、gRPCの元の状態をエラーのsourceに保持します。
