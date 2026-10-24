> この文書は [SQLITE_DATABASE_SCHEMA.md](SQLITE_DATABASE_SCHEMA.md) の日本語版です。英語版が正本で、食い違うときは英語版に従います。

# SQLite の保存先のスキーマ

公開`EventStoreForSqlite`（feature `sqlite`）が使う5系の実配置を説明します。ライブラリがこの配置を作るのは、明示的な`initialize_sqlite_store`だけです。通常の`EventStoreForSqlite::open`は、表も設定も作りません。ファイルを自分で用意したい場合は、下記の手動の手順で同じ配置を作れます。

通常storeはこの新配置だけを読み取ります。v3の既定SQLite配置は明示的な[移行手順](MIGRATION_GUIDE.ja.md)で扱います。この保存先の最初の版は、変更フィードを提供しません。

## 配置

### 表とキー

データはすべて、ローカルの1つのSQLiteデータベースファイルに置きます。論理shardやキーの接頭辞はなく、`aid`は完全一致、`seq_nr`は整数として比較します。全表を`STRICT`にし、全`aid`列は`COLLATE BINARY`です。

| 表 | 主キー | その他の列 |
|:---|:-------|:-----------|
| `journal` | `(aid, seq_nr)` | `occurred_at_ns`・`manifest`・`payload` |
| `head` | `aid` | `type_name`・`seq_nr`・`event_occurred_at_ns`・`event_manifest`・`event_payload` |
| `snapshot_current` | `aid` | `seq_nr`・`manifest`・`payload` |
| `snapshot_history` | `(aid, seq_nr)` | `manifest`・`payload` |
| `config` | `singleton`（常に1） | `store_id`・`layout_version`・`keep_snapshot_count`・`retention_mode`・`change_feed_enabled` |

キーと封筒の列は`NOT NULL`です。`seq_nr`には`CHECK (seq_nr BETWEEN 1 AND 9007199254740991)`、つまり1から`SEQ_NR_MAX`までの制約があります。この版に`head_transition`表はありません。

`aid`は`AggregateId`の型名と値から組み立てるUTF-8の`type_name-value`文字列です。型名にはhyphenを含められず、値には含められます。全体の上限は1024バイトです。空の型名・値も許可します。

## 設定

### 接続の設定とconfig

storeが使う全接続を、次のとおり確認・設定します。要件と異なる値のまま処理を続けず、設定エラーにします。

| 設定 | 値 |
|:-----|:---|
| SQLiteの版 | 3.51.3以上。openのたびに確認 |
| `locking_mode` | `NORMAL`。WALを有効にする前に設定 |
| `journal_mode` | `WAL` |
| `synchronous` | `FULL` |
| `read_uncommitted` | `OFF` |
| shared cache | 使わない |
| busyの待ち時間 | `SqliteOptions::lock_wait`。既定は5秒。32bitのミリ秒に収まらない値は設定エラー |

ファイルは、使うホストのローカルファイルシステムに置いてください。ネットワークファイルシステム、`ATTACH`でファイルを分けた配置、メモリだけのデータベースは範囲外です。storeのインスタンスとそのcloneは、1つの接続を共有します。WALはデータベースファイルの性質で、それ以外の設定は接続のたびに適用し直します。

`config`表は、次の1行だけを持ちます。

| 列 | 型 | 要件 |
|:---|:---|:-----|
| `singleton` | INTEGER | 主キー。常に1 |
| `store_id` | TEXT | 初期化時に生成する、小文字でhyphen付きのUUID |
| `layout_version` | INTEGER | 1 |
| `keep_snapshot_count` | INTEGER | 「currentのsnapshotだけ」ならNULL、それ以外は正の整数 |
| `retention_mode` | TEXT | `delete` |
| `change_feed_enabled` | INTEGER | 0（最初の版は1を拒否） |

`open`／`open_with_serializers`は、表の配置（STRICT・列名・型・`NOT NULL`・主キー・binary照合）と`config`の行を、1つの読み取りトランザクションで照合します。`SqliteOptions::retention`の履歴件数は`keep_snapshot_count`と一致が必要で、`change_feed_enabled = 1`のstoreは拒否します。ファイルが存在しない、またはSQLiteが開けない場合は、保存先エラー（`EventStoreError::Storage`）です。ファイルが未初期化、表が欠けている・異なる、設定が食い違う場合は、設定エラー（`EventStoreError::Configuration`）です。`open`は何も作らず、修復もしません。設定値はファイルを開く前に検査し、`keep_latest(0)`・`RetentionMode::Ttl`・`change_feed: true`は設定エラーです。

## 初期化

新しいファイル、またはアプリケーション用の表・索引・view・triggerが1つもない既存のファイルを、最初の`open`の前に1回だけ初期化します。

```rust
use event_store_adapter_rs::{initialize_sqlite_store, RetentionSettings, SqliteOptions};

let options = SqliteOptions {
  retention: RetentionSettings::keep_latest(2),
  ..SqliteOptions::default()
};
initialize_sqlite_store("accounts.sqlite3", &options)?;
```

`initialize_sqlite_store`は、設定値を検査し、ファイルを開き（存在しなければ作り）、WALを含む接続の設定を適用してから、`BEGIN IMMEDIATE`を始めます。データベースが未初期化でなければ、表を作らずに設定エラーを返します。未初期化なら、5つの表を作り、生成したUUIDで`config`の行を挿入して、1つのトランザクションでコミットします。WALへの切り替えは検査の前に適用するので、拒否したファイルに残る場合があります。

手作業で用意するには、SQLite 3.51.3以上で次を実行します。`store_id`には小文字のUUIDを使います。`keep_snapshot_count`は、currentのsnapshotだけなら`NULL`、新しい`n`件の履歴snapshotを残すなら正の整数`n`です。

```sql
PRAGMA locking_mode = NORMAL;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;

BEGIN IMMEDIATE;
CREATE TABLE journal (
  aid TEXT COLLATE BINARY NOT NULL,
  seq_nr INTEGER NOT NULL CHECK (seq_nr BETWEEN 1 AND 9007199254740991),
  occurred_at_ns INTEGER NOT NULL,
  manifest TEXT NOT NULL,
  payload BLOB NOT NULL,
  PRIMARY KEY (aid, seq_nr)
) STRICT;
CREATE TABLE head (
  aid TEXT COLLATE BINARY NOT NULL PRIMARY KEY,
  type_name TEXT NOT NULL,
  seq_nr INTEGER NOT NULL CHECK (seq_nr BETWEEN 1 AND 9007199254740991),
  event_occurred_at_ns INTEGER NOT NULL,
  event_manifest TEXT NOT NULL,
  event_payload BLOB NOT NULL
) STRICT;
CREATE TABLE snapshot_current (
  aid TEXT COLLATE BINARY NOT NULL PRIMARY KEY,
  seq_nr INTEGER NOT NULL CHECK (seq_nr BETWEEN 1 AND 9007199254740991),
  manifest TEXT NOT NULL,
  payload BLOB NOT NULL
) STRICT;
CREATE TABLE snapshot_history (
  aid TEXT COLLATE BINARY NOT NULL,
  seq_nr INTEGER NOT NULL CHECK (seq_nr BETWEEN 1 AND 9007199254740991),
  manifest TEXT NOT NULL,
  payload BLOB NOT NULL,
  PRIMARY KEY (aid, seq_nr)
) STRICT;
CREATE TABLE config (
  singleton INTEGER NOT NULL PRIMARY KEY CHECK (singleton = 1),
  store_id TEXT NOT NULL,
  layout_version INTEGER NOT NULL,
  keep_snapshot_count INTEGER CHECK (keep_snapshot_count IS NULL OR keep_snapshot_count >= 1),
  retention_mode TEXT NOT NULL CHECK (retention_mode = 'delete'),
  change_feed_enabled INTEGER NOT NULL CHECK (change_feed_enabled IN (0, 1))
) STRICT;
INSERT INTO config (singleton, store_id, layout_version, keep_snapshot_count, retention_mode, change_feed_enabled)
VALUES (1, '5f0c8b8e-3c1a-4d7e-9a4b-2f6d1c0e7a11', 1, NULL, 'delete', 0);
COMMIT;
```

## 記録

### Journalの行

イベント封筒1件が1行になります。

| 列 | 型 | 意味 |
|:---|:---|:-----|
| `aid` | TEXT | 集約ID全体 |
| `seq_nr` | INTEGER | ドメイン採番のイベント番号。1から`SEQ_NR_MAX`まで |
| `occurred_at_ns` | INTEGER | イベントが供給する符号付き64bitのUnix epochナノ秒。REALでは保存しない |
| `manifest` | TEXT | 封筒のmanifest。省略時は空文字列 |
| `payload` | BLOB | イベントpayloadだけをserializerへ渡した出力 |

### Headの行

| 列 | 型 | 意味 |
|:---|:---|:-----|
| `aid` | TEXT | 集約ID全体 |
| `type_name` | TEXT | 集約の型名 |
| `seq_nr` | INTEGER | 最後に確定したイベント番号 |
| `event_occurred_at_ns` | INTEGER | そのイベントの発生時刻 |
| `event_manifest` | TEXT | そのイベントのmanifest |
| `event_payload` | BLOB | そのイベントのpayload |

headは番号1の追記で作られ、snapshotなしを含む全追記で更新されます。イベントの列には、最後に追記した封筒が入ります。

### Snapshotの行

| 表 | 列 | 型 | 意味 |
|:---|:---|:---|:-----|
| `snapshot_current` | `aid` | TEXT | 集約ID全体 |
| `snapshot_current` | `seq_nr` | INTEGER | snapshotの反映番号 |
| `snapshot_current` | `manifest` | TEXT | snapshot封筒のmanifest。省略時は空文字列 |
| `snapshot_current` | `payload` | BLOB | 集約だけをserializerへ渡した出力 |
| `snapshot_history` | `aid`・`seq_nr`・`manifest`・`payload` | 上と同じ | 保持している履歴snapshot1件につき1行 |

snapshotを伴う追記は、その集約の`snapshot_current`の行を置き換えます。`version`列はありません。`snapshot_history`の行は`keep_snapshot_count`を設定したときだけ書かれ、同時に書いたcurrentと同じ封筒を持ちます。

## 書き込みと読み取り

入力の検査と直列化は、トランザクションを始める前に済ませます。`persist_event`と`persist_event_and_snapshot`は、1接続の1トランザクションで次を行います。

```text
BEGIN IMMEDIATE
  headをaidの完全一致で読む
  イベントのseq_nrと照合する
  journalの行をINSERTする
  headをINSERTまたはUPDATEする
  snapshotを伴うならsnapshot_currentをUPSERTする
  snapshotを伴い、履歴件数を設定していればsnapshot_historyにINSERTする
COMMIT
```

失敗はすべてロールバックされます。`COMMIT`の成功が確定点で、結果を確認できない場合は保存先エラーとし、別のseq_nrを採番し直しません。headの番号以下のイベント番号は楽観ロックのエラーです。次に期待する番号を飛ばす番号（または1以外の最初のイベント）は契約違反です。`journal`または新規`head`の主キー重複も楽観ロックのエラーです。それ以外の制約違反、`SQLITE_BUSY`、`SQLITE_LOCKED`、busyの待ち時間切れは保存先エラーで、楽観ロックの競合として報告しません。

`get_latest_snapshot_by_id`は、headと`snapshot_current`を1つの読み取りトランザクションで読みます。headなしは`None`、headだけなら`SnapshotRead::new(None, head_seq_nr)`です。headとsnapshotの番号は独立しており、snapshotの番号がheadの番号を超えることはありません。

`get_events_by_id_since_seq_nr`は、同じ読み取りトランザクションでheadの`h`を読み、続けてjournalを`aid = :aid AND seq_nr >= :start AND seq_nr <= :h ORDER BY seq_nr`で読みます。`:start`は`max(1, seq_nr)`です。要求した範囲の欠番は保存先エラーで、短い結果を成功として返しません。

## 保持

`keep_snapshot_count`は、`RetentionSettings::current_only()`なら`NULL`、`keep_latest(n)`なら`n`です。`NULL`なら`snapshot_current`だけを保持します。`n`なら、snapshotを伴う追記が`snapshot_history`の行も書きます。その追記の確定後に、別の`BEGIN IMMEDIATE`トランザクションで、その集約の履歴を`seq_nr`の降順に読み、新しい`n`件を残して、より古い`snapshot_history`の行だけを削除します。`snapshot_current`・`head`・`journal`は削除しません。この処理が失敗しても、確定済みの追記は成功し、aid・seq_nr・phase・errorを含む`tracing`警告を出して、次のsnapshotを伴う追記でやり直します。`VACUUM`によるファイル領域の回収は別の操作で、追記には含めません。

TTL方式（`RetentionMode::Ttl`）は提供しません。`retention_mode`列は常に`delete`です。

### 履歴件数を変更する

`keep_snapshot_count`の変更は、停止して行う管理手順です。保存された値と、全クライアントの`SqliteOptions::retention`が一致する必要があります。

1. storeを使う全クライアントを止め、進行中の追記・読取・保持の処理がないことを確認します。変更前と変更先の設定を記録し、中断しても再開できるようにします。
2. `snapshot_history`にある集約ごとに、`BEGIN IMMEDIATE`で履歴を`seq_nr`の降順に読み、新しい`n`件を残して削除し（変更先が`NULL`なら全行を削除）、`COMMIT`します。大きな整理は複数のトランザクションに分け、各トランザクションで選び直します。`journal`・`head`・`snapshot_current`は変更しません。
3. 全集約が済んだら、別の`BEGIN IMMEDIATE`で`config.keep_snapshot_count`を更新して`COMMIT`します。全クライアントを新しい設定で開き直して設定を照合してから、処理を再開します。

途中で失敗したら、クライアントを止めたまま、同じ変更先で再実行します。削除した履歴は戻せません。件数を増やす場合や、なしから件数を設定する場合も、削除済み・未保持の過去の履歴は補いません。新しい設定は、次のsnapshot書込から適用されます。

## 運用

バックアップにはSQLiteのバックアップ機能を使ってください。WALで稼働中のデータベースの本体ファイルだけをコピーしてはいけません。
