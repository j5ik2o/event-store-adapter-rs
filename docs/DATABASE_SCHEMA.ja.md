# 各イベントストアが利用するデータベーススキーマ（v3）

**本ドキュメントは情報提供です。** v3 の封筒型（`EventEnvelope` / `SnapshotEnvelope`）を
各バックエンドがどのような物理レイアウトで保存するかを説明します。封筒メタデータ
（イベントは `aggregate_id` / `seq_nr` / `occurred_at` / `manifest`、スナップショットは
`seq_nr` / `version`）は専用の列・属性・セルに保存され、payload 列は**純粋なドメイン内容
のみ**を保持します — ライブラリが直列化 payload へ `version` / `seq_nr` / タイムスタンプを
注入することはなくなりました。

> v3 のレイアウトは v2 から変更されています（journal への `manifest` 列の追加、
> `occurred_at` のナノ秒精度化、snapshot payload からのメタデータ注入の廃止、
> current/履歴判別のキーへの移動）。v2 で書き込まれた行の読み取りはサポートしません。
> 移行方針は [MIGRATION_GUIDE_v3.ja.md](MIGRATION_GUIDE_v3.ja.md) を参照してください。

### スナップショット保持のバックエンド間非対称

`with_keep_snapshot_count(Some(n))` を有効にすると、どのバックエンドも履歴スナップ
ショットの**件数**を `n` に抑えますが、**どの行が残るか**は異なります。

- **DynamoDB** は超過分を**新しい側**の履歴から剪定します（seq_nr 降順クエリ）。
  そのため**旧い側**の履歴が残ります。
- **Bigtable / SQLite** は**新しい順に `n` 件**の履歴を残し、旧い側の超過分を削除します。

件数の意味論は同一で、残存選択のみが非対称です（各バックエンドの v3 以前の挙動を
そのまま維持したものです）。

## EventStore が利用する DynamoDB のテーブル構成

- Journal
- Snapshot

いずれのテーブルも、キー設計の前提として、論理シャード内で最大限に書き込みが分散することを想定しています。テーブル作成はライブラリの責務外です（参考定義は `test-utils` を参照）。

### Journal テーブル

集約で起きたイベントを保存するためのテーブル。原則的に、このイベントを使って集約状態を再生（リプレイ）します。`EventEnvelope` 1 つが 1 項目に対応します。

| 属性名 | 型 | 説明 | 具体的な値 |
|:------------|:----|:--------------------------------------|:---|
| pkey        | S | パーティションキー（`集約種別名-hash(集約ID) % 論理シャードサイズ`） | `user-account-1` |
| skey        | S | ソートキー（`集約種別名-集約IDの値部分-シーケンス番号`） | `user-account-01H42K4ABWQ5V2XQEP3A48VE0Z-12345` |
| aid         | S | 集約 ID | `user-account-01H42K4ABWQ5V2XQEP3A48VE0Z` |
| seq_nr      | N | シーケンス番号（開始番号は 1、採番はドメイン側の責務） | `12345` |
| payload     | B | イベント payload — 純粋なドメイン内容のみ（既定は JSON 直列化） | `{"Created":{"name":"test"}}` |
| occurred_at | N | イベント発生日時（Unix epoch **ナノ秒**。ドメイン供給値が完全往復する） | `1688009557404481000` |
| manifest    | S | 利用者供給・自由形式の型判別子（省略時は空文字列） | `user-account-created/v1` |

`(aid, seq_nr)` に GSI が適用されており、リプレイ時にこのインデックスを利用します。

### Snapshot テーブル

集約の状態を保存するためのテーブルであり、集約のリプレイを高速化するためのテーブルです。スナップショット保存後にもイベントは保存されるため、最新の集約状態を表さない場合があります。

| 属性名 | 型 | 説明 | 具体的な値 |
|:--------|:----|:-----------------------------------------------------------------|:---|
| pkey    | S | パーティションキー（`集約種別名-hash(集約ID) % 論理シャードサイズ`） | `user-account-1` |
| skey    | S | ソートキー（`集約種別名-集約IDの値部分-シーケンス番号`）。current スナップショットは**マーカー `0`** で整形した skey のスロットに置かれ、履歴項目はイベントの seq_nr を使う | `user-account-01H42K4ABWQ5V2XQEP3A48VE0Z-0` |
| aid     | S | 集約 ID | `user-account-01H42K4ABWQ5V2XQEP3A48VE0Z` |
| seq_nr  | N | スナップショットが反映済みのシーケンス番号。**v2 と異なり current 項目にも実値が入る**（マーカー `0` は skey の中にのみ現れる） | `12345` |
| version | N | 楽観的ロックの版数（開始番号は 1）。列値が正であり、payload から補正されることはない | `1` |
| payload | B | 集約の状態 — 純粋なドメイン内容のみ（既定は JSON 直列化。`version` / `seq_nr` の注入なし） | `{"id":{"value":"..."},"name":"test"}` |
| ttl     | N | 削除用 TTL（epoch 秒。`0` = 無期限。`with_delete_ttl` 設定時に超過履歴項目へ将来時刻が設定される） | `1624980000` |
| last_updated_at | N | 最終更新日時（Unix epoch ミリ秒。イベントの `occurred_at` から導出） | `1688009557404` |

- current スナップショットは主キー（`pkey` + マーカー `0` の skey）への強整合
  `GetItem` で読み取ります。その `version` を次回書込の `expected_version` として
  渡します。
- 履歴項目（skey = イベントの seq_nr）は **`with_keep_snapshot_count(Some(n))` が有効な
  ときのみ**、current 項目・journal 項目と同一トランザクションで書き込まれます。
- `(aid, seq_nr)` の GSI は保持ポリシーのクエリに使われます。`n` 件を超える履歴は
  削除されるか、`with_delete_ttl` 設定時は将来の `ttl` 値が設定され DynamoDB 側で
  期限切れ削除されます（上記の非対称の注記どおり、剪定対象は新しい側から選ばれます）。

### イベントとスナップショットの書き込み

1. コマンドが集約に受理されると、ドメインが次のイベントを生成し、ドメイン採番の
   `seq_nr`（開始番号 1）を持つ `EventEnvelope` に包みます。
2. journal への Put とスナップショットの書き込みは常に単一の `TransactWriteItems` で
   実行されます。ストリーム最初のイベント（seq_nr=1、expected_version=0）は
   `attribute_not_exists` 条件で両項目を作成し、以降の書き込みは
   `version = expected_version` 条件で current スナップショット項目を更新して
   `version = expected_version + 1` を設定します。条件不成立は `OptimisticLockError`
   になります。

### イベントとスナップショットによる集約のリプレイ

1. 集約 ID を指定して最新の `SnapshotEnvelope` を取得します。
2. journal テーブルから封筒の `seq_nr` より後のイベントを読み取ります。
3. 読み取ったイベントをスナップショット状態に適用して最新の集約状態を得ます。
   次回書込では封筒の `version` を `expected_version` として渡します。

## EventStoreForBigtable が利用する Bigtable のテーブル構成

- journal テーブル — カラムファミリ `event`
- snapshot テーブル — カラムファミリ `snapshot`

すべての値はバイト列として保存され、数値セルは 10 進文字列を保持します。読取と CAS 述語は
cells-per-column limit 1 を使うため、各カラムの最新セルが正です。

### journal テーブル（Bigtable）

行キー: `${パーティションキー}#${集約種別名}#${集約IDの値部分}#${seq_nr の 20 桁ゼロ詰め}`
（パーティションキーは `集約種別名-hash(集約ID) % シャード数`）。`EventEnvelope` 1 つが 1 行に対応します。

| カラム（ファミリ `event`） | 説明 | 具体的な値 |
|:------------|:--------------------------------------|:---|
| payload     | イベント payload — 純粋なドメイン内容のみ（既定は JSON 直列化） | `{"Created":{"name":"test"}}` |
| aggregate_id | 集約 ID の値部分 | `01H42K4ABWQ5V2XQEP3A48VE0Z` |
| seq_nr      | シーケンス番号（開始番号は 1、採番はドメイン側の責務） | `12345` |
| occurred_at | イベント発生日時（**ナノ秒**精度の RFC 3339 文字列。ドメイン供給値が完全往復する） | `2023-06-29T03:32:37.404481000Z` |
| manifest    | 利用者供給・自由形式の型判別子（省略時は空文字列） | `user-account-created/v1` |

seq_nr のゼロ詰めにより同一集約の行キーが連続・整列するため、リプレイは接頭辞の
範囲走査で行われます。

### snapshot テーブル（Bigtable）

current 行キー: `${パーティションキー}#${集約種別名}#${集約IDの値部分}`。
履歴行キー: current 行キー + `#` + seq_nr のゼロ詰め（キー昇順 = 旧い順）。

| カラム（ファミリ `snapshot`） | 説明 | 具体的な値 |
|:------------|:--------------------------------------|:---|
| payload     | 集約の状態 — 純粋なドメイン内容のみ（`version` / `seq_nr` の注入なし） | `{"id":{"value":"..."},"name":"test"}` |
| seq_nr      | スナップショットが反映済みのシーケンス番号 | `12345` |
| version     | 楽観的ロックの版数（開始番号は 1）。セル値が正 | `1` |
| last_updated_at | 最終更新日時（Unix epoch ミリ秒） | `1688009557404` |

- 書き込みは `CheckAndMutateRow`（単一行原子性 CAS）を通ります。新規作成は `version`
  セルの不在を、更新は `version == expected_version` のバイト完全一致を述語で検査し、
  `version = expected_version + 1` を設定します。述語不成立は `OptimisticLockError`
  になります。
- 履歴行は **`with_keep_snapshot_count(Some(n))` が有効なときのみ**書き込まれます。
  CAS 勝者が current 行のプレイメージを別呼び出しのベストエフォート書込で履歴行キーへ
  コピーします。
- 保持ポリシーは新しい順に `n` 件の履歴行を残し、旧い側の超過分を `DeleteFromRow` で
  削除します。`with_delete_ttl` を併用すると、`last_updated_at` が TTL より古い履歴行も
  削除されます。

## EventStoreForSqlite が利用する SQLite のテーブル構成

- journal
- snapshot

テーブルとインデックスはストア構築時にライブラリが自動作成します（冪等な
`CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS`）。利用者による DDL は
不要であり、想定していません。

キー設計は DynamoDB テーブルを踏襲しています。`pkey` / `skey` が書き込みアドレス
（`PRIMARY KEY (pkey, skey)`）となって論理シャードへ書き込みを分散し、`(aid, seq_nr)`
がリプレイに使う読み取りキーです。

```sql
CREATE TABLE IF NOT EXISTS journal (
  pkey TEXT NOT NULL,
  skey TEXT NOT NULL,
  aid TEXT NOT NULL,
  seq_nr INTEGER NOT NULL,
  payload BLOB NOT NULL,
  occurred_at INTEGER NOT NULL,
  manifest TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (pkey, skey)
);
CREATE UNIQUE INDEX IF NOT EXISTS journal_aid_seq_nr_idx ON journal (aid, seq_nr);
CREATE TABLE IF NOT EXISTS snapshot (
  pkey TEXT NOT NULL,
  skey TEXT NOT NULL,
  aid TEXT NOT NULL,
  seq_nr INTEGER NOT NULL,
  version INTEGER NOT NULL,
  payload BLOB NOT NULL,
  last_updated_at INTEGER NOT NULL,
  PRIMARY KEY (pkey, skey)
);
CREATE INDEX IF NOT EXISTS snapshot_aid_seq_nr_idx ON snapshot (aid, seq_nr);
```

### journal テーブル（SQLite）

| 列名 | 型 | 説明 |
|:------------|:-----|:------------|
| pkey | TEXT | パーティションキー（`集約種別名-hash(集約ID) % シャード数`）— 書き込み分散キー。主キーの一部 |
| skey | TEXT | ソートキー（`集約種別名-集約IDの値部分-シーケンス番号`）— 主キーの一部 |
| aid | TEXT | 集約 ID |
| seq_nr | INTEGER | シーケンス番号（開始番号は 1、採番はドメイン側の責務） |
| payload | BLOB | イベント payload — 純粋なドメイン内容のみ（既定は JSON 直列化） |
| occurred_at | INTEGER | イベント発生日時（Unix epoch **ナノ秒**。ドメイン供給値が完全往復する。範囲外の値は書込時に拒否） |
| manifest | TEXT | 利用者供給・自由形式の型判別子（省略時は空文字列） |

`(aid, seq_nr)` のユニークインデックスが DynamoDB の GSI に相当し、リプレイ時に利用されます。

### snapshot テーブル（SQLite）

| 列名 | 型 | 説明 |
|:------------|:-----|:------------|
| pkey | TEXT | パーティションキー（`集約種別名-hash(集約ID) % シャード数`）— 書き込み分散キー。主キーの一部 |
| skey | TEXT | ソートキー（`集約種別名-集約IDの値部分-シーケンス番号`）。current スナップショットは**マーカー `0`** で整形した skey のスロットに置かれ、履歴行はイベントの seq_nr を使う |
| aid | TEXT | 集約 ID |
| seq_nr | INTEGER | スナップショットが反映済みのシーケンス番号。**v2 と異なり current 行にも実値が入る**（マーカー `0` は skey の中にのみ現れ、current/履歴の判別はこの列ではなく skey で行う） |
| version | INTEGER | 楽観的ロックの版数（開始番号は 1）。列値が正であり、payload から補正されることはない |
| payload | BLOB | 集約の状態 — 純粋なドメイン内容のみ（既定は JSON 直列化。`version` / `seq_nr` の注入なし） |
| last_updated_at | INTEGER | 最終更新日時（Unix epoch ミリ秒）。スナップショット保持 TTL の評価にも使用 |

`(aid, seq_nr)` のインデックスがリプレイ時に利用されます。

- 楽観的ロックの検証と書き込みは単一の SQLite トランザクション内で行われます。
  journal への INSERT と条件付きスナップショット UPDATE（`WHERE version = expected`）は
  一緒にコミットされるか一緒にロールバックされます。ストリーム最初のイベント
  （seq_nr=1、expected_version=0）は両行を INSERT し、この経路での主キー／ユニーク
  インデックス衝突は `expected_version=0` の `OptimisticLockError` になります。
- 履歴行は **`with_keep_snapshot_count(Some(n))` が有効なときのみ**同一トランザク
  ションで INSERT されます。保持ポリシーは新しい順に `n` 件の履歴行を残し、旧い側の
  超過分を削除します（`ORDER BY seq_nr ASC LIMIT excess`）。`with_delete_ttl` を併用
  すると、`last_updated_at` が TTL より古い履歴行も削除されます。
