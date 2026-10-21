# Bigtableの保存schema（5系）

公開`EventStoreForBigtable`（feature `bigtable`）が使う実配置を説明します。ライブラリが設定の行を作るのは、明示的な`initialize_bigtable_store`（または`initialize_bigtable_store_with_token_source`）だけです。通常の`EventStoreForBigtable::open`は、テーブルも列ファミリも設定の行も作りません。テーブルと列ファミリは、常に利用者が作ります。

通常storeはこの新配置だけを読み取ります。v3の既定Bigtable配置は明示的な[移行手順](MIGRATION_GUIDE_v5.ja.md)で扱います。この保存先の最初の版は、変更フィードを提供しません。

## テーブルとキー

1つのBigtableインスタンスに、journalテーブルとheadテーブルの2つのテーブルを使います。2つは別のテーブルでなければならず、同じテーブルIDを両方に指定すると設定エラーです。

| テーブル | 列ファミリ | 行キー | 行 |
|:---------|:-----------|:-------|:---|
| journal | `event`・`history`・`config` | `P(aid) \|\| U64BE(seq_nr)` | イベントごとに1行と、設定の行 |
| head | `head`・`snapshot`・`config` | `P(aid)` | 集約ごとに1行と、設定の行 |

`aid`は`AggregateId`から組み立てるUTF-8文字列`type_name-value`です。型名にハイフンは使えず、値には使えます。文字列全体は1024バイト以下です。型名または値が空でもかまいません。

```text
P(aid)             = H(aid) || L16(length(A)) || A
headの行キー       = P(aid)
journalの行キー    = P(aid) || U64BE(seq_nr)
設定の行のキー     = 0x00   （1バイト。両方のテーブルで同じ）
```

- `A`は`aid`のUTF-8のバイト列です。
- `H(aid)`は、`A`の[FNV-1a 64](https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function)ハッシュを、8バイトのビッグエンディアンにした値です。
- `L16`は、`A`のバイト長を符号なし16ビットのビッグエンディアンにした値です。長さがキーに含まれるので、2つのハッシュが衝突しても、別の集約のキーが`P(aid)`で始まることはありません。
- `U64BE(seq_nr)`は、シーケンス番号を符号なし64ビットのビッグエンディアンにした値です。キーの順序がシーケンスの順序と一致します。範囲を作るために、10進文字列・言語の`Display`の出力・`prefix + 0xff`は使いません。
- `aid`に含まれる`#`・NUL・非ASCIIの文字は、ただのバイトです。区切り文字としては解釈しません。
- 設定の行のキー`0x00`は1バイトなので、データの行のキー（先頭に8バイトのハッシュと2バイトの長さが必ず付く）と一致することはありません。

### 列ファミリとガベージコレクション

最初の`initialize_bigtable_store`の前に、すべての列ファミリを作ってください。

| テーブル | 列ファミリ | ガベージコレクションの規則 |
|:---------|:-----------|:---------------------------|
| journal | `event` | 最大1版を残す。年齢による規則はなし |
| journal | `history` | 最大1版を残す。年齢による規則はなし |
| journal | `config` | 任意（存在すれば足りる） |
| head | `head` | 最大1版を残す。年齢による規則はなし |
| head | `snapshot` | 最大1版を残す。年齢による規則はなし |
| head | `config` | 任意（存在すれば足りる） |

`open`は、各テーブルのschemaを`GetTable`で読み、列ファミリが欠けている場合と、データの列ファミリの規則が「最大1版を残す」だけではない場合を、設定エラー（`ConfigurationReason::BigtableColumnFamilyMismatch`）として拒否します。データの列ファミリに年齢による規則を設定しないでください。期限切れのセルがデータを失わせます。正しさは、ガベージコレクションがいつ走るかに依存しません。読み取りは常に列ごとの最新の1セルだけを求め、書き込みはセルの時刻を0にして、置き換える列の古い版を削除するからです。

## App profile・接続・認証

稼働中のシステムの読み取り・書き込み・保持・設定の照合は、1つのクラスタに固定し、単一行トランザクションを許可するapp profileで行ってください。インスタンスに複製用のクラスタがある場合も、アダプタの操作先はその1つのクラスタにそろえます。そのクラスタの障害中は、storeは保存先エラーを返して復旧を待ちます。複数クラスタへの同時書き込み、マルチクラスタルーティング、自動フェイルオーバーはサポートしません。ライブラリはapp profileを照合しません。利用者がapp profileを作り、そのIDを`BigtableOptions::app_profile_id`に渡します。IDが空ならインスタンスの既定のapp profileを使います。これは、その既定のprofileが上の要件を既に満たしている場合だけ正しい設定です。

接続は、`tonic::transport::Channel`を2つ持つ`BigtableConnection`です。

| Channel | 用途 |
|:--------|:-----|
| `data` | `ReadRows`・`CheckAndMutateRow`などのデータの要求 |
| `admin` | `GetTable`。列ファミリの照合のために、ストアを開くときだけ使う |

- **エミュレーター**: 2つのフィールドの両方に、1つの平文のchannel（たとえば`Endpoint::from_static("http://localhost:8086")`）を入れ、`EventStoreForBigtable::open`・`open_with_serializers`・`initialize_bigtable_store`を呼びます。認証は付けません。
- **本番のCloud Bigtable**: 利用者が自分の依存で`tonic`のTLSのfeatureを有効にして、2つのTLSのchannel（データは`https://bigtable.googleapis.com`、adminは`https://bigtableadmin.googleapis.com`）を作ります。`BigtableTokenSource`を実装して、`open_with_token_source`・`open_with_serializers_and_token_source`・`initialize_bigtable_store_with_token_source`に渡します。ストアは、データとadminのすべての要求の直前に取得元を呼び、`authorization: Bearer <トークン>`を付けます。トークンの保持と更新は取得元の責任です。取得元が失敗したとき、またはメタデータの値にできないトークンを返したときは、その要求を送らずに、その操作が`EventStoreError::Storage`になります。同じ要求のために取得元を呼び直すことはしません。ライブラリ自身は、TLSの実装にもGoogleの認証のライブラリにも依存しません。

## 設定の行

各テーブルは、行キー`0x00`の設定の行を1つ持ち、列`config:metadata`にUTF-8のJSONオブジェクトを保存します。値は2つのテーブルで同じです。

| フィールド | 型 | 要件 |
|:-----------|:---|:-----|
| `store_id` | 文字列 | ハイフン区切りの小文字のUUID。初期化のときに生成する |
| `layout_version` | 整数 | 1 |
| `keep_snapshot_count` | nullまたは文字列 | 「今のsnapshotだけ」なら`null`。それ以外は、指数表記も先頭のゼロもない10進の文字列で書いた正の整数 |
| `retention_mode` | 文字列 | `delete` |
| `envelope_limit_bytes` | 整数 | 17以上8388608以下。既定は1048576 |

`keep_snapshot_count`だけが文字列なのは、件数が符号付き64ビットの大きさまで取りえて、倍精度のJSONの数値では正確に表せない値があるためです。

`open`は、2つのテーブルの列ファミリを照合し、続けて2つの設定の行を読みます。行がない、JSONとして読めない、キーが重複している、フィールドが欠けている、フィールドの型や形が違う場合は、設定エラー（`ConfigurationReason::BigtableConfigurationInvalid`）です。比較は値で行い、キーの順序や空白は見ません。2つの行の間に違いがある、`layout_version`が1でない、`retention_mode`が`delete`でない、`store_id`が小文字のUUIDでない、またはクライアントの`BigtableOptions`（履歴件数と`envelope_limit_bytes`）と違う場合は、設定エラー（`ConfigurationReason::BigtableConfigurationMismatch`）です。`open`が設定の行を修復したり補ったりすることはありません。

要求を送る前に、オプションを検査します。`keep_latest(0)`・`RetentionMode::Ttl`・`change_feed: true`・上の範囲の外の`envelope_limit_bytes`は設定エラーです。

## 初期化

データの行も設定の行もない新しい2つのテーブルを、最初の`open`の前に、書き手がいない状態で1回だけ初期化します。

```rust
use event_store_adapter_rs::{
  initialize_bigtable_store, BigtableConnection, BigtableOptions, BigtableTables,
  RetentionSettings,
};

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
initialize_bigtable_store(&connection, &tables, &options).await?;
```

`initialize_bigtable_store`は、オプションとテーブルIDを検査し、2つのテーブルの設定の行を読みます。どちらかに既にあれば、何も書かずに`ConfigurationReason::BigtableAlreadyInitialized`を返します。なければ、1つの`store_id`を生成し、同じJSONを、先にjournalテーブル、次にheadテーブルへ書きます。どちらも、行の`config:metadata`がまだないときだけ成功する条件付きの書き込みです。テーブル・列ファミリ・データの行は作りません。また、列ファミリの照合もしません。列ファミリが欠けていれば、書き込みが保存先エラーで失敗します。誤った列ファミリや規則を拒否するのは`open`です。

設定の行を手作業で用意する場合は、好きなBigtableのクライアントで、上のJSONを両方のテーブルの行キー`0x00`の`config:metadata`へ書きます。2つの値は同じでなければならず、列ファミリを先に作っておく必要があります。

### 中断した初期化の回復

2つの設定の行は1つずつ順に書かれ、Bigtableには行をまたぐトランザクションも、テーブルをまたぐトランザクションもありません。1つ目を書いた後で呼び出しが止まる（クラッシュ、接続の切断、2つ目のテーブルからの誤り）と、journalテーブルにだけ設定の行が残ります。ライブラリは、足りない行を自動では補いません。ライブラリが今書いたのではない設定は信頼できないからです。この状態では、`initialize_bigtable_store`は`BigtableAlreadyInitialized`を返し、`open`は、headテーブルに行がないため`BigtableConfigurationInvalid`を返します。

回復の手順は次のとおりです。

1. 2つのテーブルへの書き手と読み手を、すべて止めたままにします。
2. 両方のテーブルで、行キー`0x00`の行を読みます。両方にあれば、初期化は終わっているので、何も削除しません。片方にだけあれば、どちらのテーブルにもデータの行がないこと、つまりstoreがまだ一度も使われていないことを確認します。データの行があれば、中断した初期化ではなく、この手順の対象外です。行を削除しないでください。
3. 行がある側のテーブルから、残った行キー`0x00`の行を削除します（好きなBigtableのクライアントで、`DeleteFromRow`の変更を使った`MutateRow`を送ります）。
4. 同じ設定で`initialize_bigtable_store`をもう一度呼びます。

## Journalの行

イベント1件が、journalテーブルの1行になります。行は`event:envelope`がないときだけ作り、上書きも削除もしません。

| セル | 値 |
|:-----|:---|
| `event:envelope` | 行キーのシーケンス番号のイベント封筒。削除しない |
| `event:has_snapshot` | その追記がsnapshotを伴うならASCIIの`1`、そうでなければASCIIの`0`。削除しない |
| `history:state` | 行に履歴snapshotを初めて書いたときはASCIIの`active`、削除した後は`retired`。履歴を書かなかった行にはない |
| `history:envelope` | 履歴snapshotの封筒。状態を`retired`にするときに削除する |

履歴件数を設定していない場合は、`history`のセルを書きません。セルの時刻は0（固定値）です。これはBigtable自身の管理にだけ使い、`occurred_at`をセルの時刻から求めることはありません。

## Headの行

headテーブルは、集約ごとに小さな1行を持ちます。全イベントや`n`件の履歴snapshotは置かないので、履歴件数が増えてもheadの行は育ちません。

| セル | 値 |
|:-----|:---|
| `head:seq_nr` | 確定した最後のシーケンス番号を、ゼロ詰めの20桁のASCII数字にしたもの。headの照合に使う |
| `head:last_event` | そのシーケンス番号のイベント封筒 |
| `head:has_snapshot` | その追記がsnapshotを伴うならASCIIの`1`、そうでなければASCIIの`0` |
| `snapshot:current` | 今のsnapshot封筒。集約にsnapshotがなければセルもない |

`head:has_snapshot`が`1`のとき、`snapshot:current`のシーケンス番号は`head:seq_nr`と等しくなります。イベントだけの追記は、既存の`snapshot:current`を残したまま、`head:has_snapshot`を`0`にします。snapshotの番号がheadの番号を超えることはありません。

## 封筒

ライブラリは、封筒を次の固定のバイナリ形式で組み立てます。`U16`・`U32`・`U64`はビッグエンディアン、`I64`はビッグエンディアンの符号付き64ビットの2の補数です。先頭の`0x01`は形式の版番号です。serializerが受け取るのはpayloadだけです。

```text
event    = 0x01 || U16(len(aid)) || aid || U64(seq_nr)
           || I64(occurred_at_ns) || U32(len(manifest)) || manifest
           || U32(len(payload)) || payload
snapshot = 0x01 || U64(seq_nr)
           || U32(len(manifest)) || manifest
           || U32(len(payload)) || payload
```

長さはバイト数です。読むときに、使われていない末尾・長さの不一致・未知の形式の版があれば、保存先エラーです。正しい封筒のpayloadを復元できない場合は、直列化エラーです。発生時刻はナノ秒で封筒に持ち、Bigtableのセルの時刻（マイクロ秒の精度）には持ちません。

### 封筒の大きさの上限

`BigtableOptions::envelope_limit_bytes`は、メタデータを含む直列化後の1封筒の上限です。既定は1MiB（1,048,576バイト）、最大は8MiB（8,388,608バイト）、最小は17バイト（最小のsnapshot封筒）です。値は2つの設定の行に保存され、すべてのクライアントが同じ値を使う必要があります。上限を超える入力は、書き込みの前に、シーケンス番号を含む契約違反（`ContractRule::EnvelopeSizeLimit`、規則BT-16）として拒否します。上限を変える場合は、全クライアントを止め、2つの設定の行を同じ新しい値に更新してから、全クライアントを開き直して照合します。

## 書込と読取

入力の検査と直列化は、要求を送る前に終えます。その後の`persist_event`と`persist_event_and_snapshot`は、次の手順で動きます。

1. 集約のheadの行を強整合で読みます。行がなければ、今のシーケンス番号`h`は0です。あれば、`h`と最後の封筒を検証します。必要なセルが欠けた行を、新しい集約とみなすことはありません。
2. 入力のシーケンス番号を`h`と比べます。次の連続した番号だけが続きます。
3. `h > 0`なら、headの最後のイベント`h`を、journalの行`h`へ、1回の条件付きの書き込みで書きます（履歴件数が設定され、headが`has_snapshot = 1`なら、`h`のsnapshotも同じ書き込みで書きます）。この書き込みを確認できなければ、追記は保存先エラーになり、headは進みません。
4. headの行に条件付きの書き込みをします。新しい集約は`head:seq_nr`がない側で作ります。更新は、`head:seq_nr`が手順1で読んだ`h`である側だけで成功し、`head:seq_nr`・`head:last_event`・`head:has_snapshot`（snapshotを伴うなら`snapshot:current`も）を置き換えます。**この条件付きの書き込みが確定点です。**
5. 新しい最後のイベントをjournalへ書き、保持の処理を行います。ここでの失敗は通知して後回しにし、確定した追記を失敗に変えることはありません。

確定点の書き込みが、手順1の後に別の書き込みが確定していたために適用されなかった場合は、楽観ロックのエラーです。新しい集約の書き込みが、先行する作成に負けた場合も同じです。確定点の書き込みの応答を失った場合は、失敗したと断定しない保存先エラーです。次に期待する番号を飛ばす番号（または1以外の最初のイベント）は契約違反です。

これにより、1つの不変条件が保たれます。headが`h`のとき、イベント`1`から`h - 1`はjournalにあり、イベント`h`はheadにあります。追記に付いたsnapshotは、次の追記の前にjournalの行へ書かれるので、まだ書かれていない最新のsnapshotは、常に`snapshot:current`にあります。

`get_latest_snapshot_by_id`は、headの行を1回読み、`head:seq_nr`と、あれば`snapshot:current`を返します。headなしは`None`、snapshotのないheadは`SnapshotRead::new(None, head_seq_nr)`です。

`get_events_by_id_since_seq_nr`は、最初にheadを読み、その`h`と最後のイベントを呼び出しの間ずっと使います。`start = max(1, seq_nr)`として、次のように分かれます。

1. headがない、または`start > h`: 空のリスト。journalへの要求はしません。
2. `start == h`: headの最後のイベントだけ。journalへの要求はしません。
3. `start < h`: `start`から`h`の手前までのjournalの行（`P || U64BE(start)`から`P || U64BE(h)`まで）と、その後ろにheadの最後のイベント。

呼び出しの途中でheadが進んでも、読み直しません。`h`以上の行は混ぜません。途中で終わった`ReadRows`のストリームは、最後に完了した行の次から再開し、不完全な行は読み直します。要求した範囲の中の欠番は保存先エラーです。

## 保持

`keep_snapshot_count`は、`RetentionSettings::current_only()`なら`null`、`keep_latest(n)`なら`n`です。`null`なら履歴を保持せず、`snapshot:current`だけがsnapshotを持ちます。`n`なら、履歴は、集約のjournalの行にある`active`な`history`のセルと、まだjournalの行にないheadの今のsnapshotの組です。つまり、headのsnapshotも`n`件に数えます。`snapshot:current`は、保持の処理で削除しません。

snapshotを伴う追記が確定した後、storeはheadの位置`h`を固定し、集約のjournalの行を`P || U64BE(1)`から`P || U64BE(h + 1)`の手前まで読んで、`history:state`が`active`の行を一覧にします。`h`がsnapshotを伴い、まだ行がない場合は、`h`も一覧に加えます。`retired`の行を`active`に戻すことはありません。新しい`n`件を残し、それより古い各行は、`history:state`がまだ`active`のときだけ成功する1回の条件付きの書き込みで退役させます。`history:envelope`を削除し、`history:state`を`retired`にします。`event`のセルは残します。読むのは状態の列だけなので、履歴のpayloadは転送しません。

退役させた行を、もう一度埋めることはありません。遅れた書き手が同じ履歴をコピーしようとしても、`event:envelope`が既にあるので何も書かず、削除した履歴は戻りません。保持が失敗しても、aid・seq_nr・phase・errorを含む`tracing`警告を出すだけで、確定済みの追記は成功し、次のsnapshotを伴う追記でやり直します。保持の費用は、集約のjournalの長さに応じて増えます。TTL方式（`RetentionMode::Ttl`）は提供しません。`retention_mode`は常に`delete`です。

### 履歴件数を変更する

`keep_snapshot_count`の変更は、停止して行う管理手順です。2つの設定の行に保存された値と、全クライアントの`BigtableOptions::retention`が一致する必要があります。

1. storeを使う全クライアントを止め、進行中の追記・読取・書き出し・保持の処理がないことを確認します。変更前と変更先の設定と、進捗を記録し、中断しても再開できるようにします。
2. すべてのheadの行（キー`0x00`の行を除くすべての行）について、`h`と最後のイベントを読み、journalの行`h`の書き出しを、**変更前の**履歴件数で完了させます。変更前に履歴を保持していて、最後のイベントがsnapshotを伴うなら、そのsnapshotも同じ最初の書き込みに含めます。まだ書かれていないsnapshotが、新しい選択から落ちるのを防ぐためです。
3. 集約ごとに、`h`までの`active`な履歴をすべて読みます。変更先の新しい`n`件を残し（変更先が`null`なら残さず）、残りを上の条件付きの書き込みで退役させます。イベントの封筒とフラグ、head、`snapshot:current`は変更しません。`retired`の状態は消さず、履歴は再作成しません。
4. 全集約が済んだら、両方のテーブルの設定の行の`keep_snapshot_count`を変更先の値に更新します。全クライアントを新しいオプションで開き直して設定を照合してから、処理を再開します。

途中で失敗したら、クライアントを止めたまま、同じ変更先で再実行します。済んだ書き出しや退役を繰り返しても、履歴は戻りません。2つの設定の行の片方だけを更新した場合は、両方が一致するまで再開しません。削除した履歴は戻せないので、storeの全体を元に戻すことは前提にしません。件数を増やす場合や、なしから件数を設定する場合も、削除済み・未保持の過去の履歴は補いません。新しい設定は、次のsnapshot書込から適用されます。
