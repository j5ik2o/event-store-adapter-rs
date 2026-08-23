# 機能設計 質問 — u2-sqlite-backend

> U2（SQLiteバックエンド本体）の機能設計工程の質問。スキーマ形状（契約C-4）・
> 単一トランザクションCAS（ADR-003）・公開API形状（契約C-1）・`:memory:` の
> 共有範囲（インスタンス単位）・保持ポリシーの実行点（C-2 on_event_persisted）は
> 上流で確定済み。実装を拘束する残余の設計分岐のみを確認する。

## Q1: rusqlite（同期ドライバ）のasyncメソッド内での実行方式

`StorageBackend` のメソッドはasyncですが、rusqliteは同期ドライバです。また `rusqlite::Connection` は `Sync` でないため、`Send + Sync + Clone` を満たすには接続を `Arc<Mutex<Connection>>` で包む必要があります（`:memory:` のインスタンス単位共有にも接続共有が必須）。実行方式が未確定です。

A. `Arc<Mutex<Connection>>` で同期実行 — 各メソッド内でロック→同期実行→解放（ガード越しawaitなし）。追加依存ゼロで「rusqlite唯一追加」（U2制約・NFR-2）を厳守。SQLiteのローカルI/Oは短時間で、Memory準拠化と同型のパターン。ロック中は同一ストアの他操作が待つ（イベントストア用途では実用上許容）
B. `tokio::task::spawn_blocking` で分離 — コンポーネントカタログの記述踏襲。async実行器の阻害はゼロだが、tokioを `sqlite` feature配下のランタイム依存に追加することになり「rusqlite唯一追加」制約に抵触する
X. Other (please specify)

[Answer]: A. `Arc<Mutex<Connection>>` で同期実行

## Q2: KeyResolver の扱い（契約間の不整合解消）

公開API契約（C-1）は `EventStoreForSqlite` に `with_key_resolver` ビルダーを含めていますが、スキーマ契約（C-4）の journal / snapshot テーブルには pkey / skey 列がなく（複合キーは `(aid, seq_nr)` 直接）、単一ファイルDBにシャーディング用のキー解決の適用先がありません。この不整合の解消方法が未確定です。

A. `with_key_resolver` を提供しない — C-1のSQLite面（U2所有）を修正。未リリースの新APIのため利用者影響なし。何もしないAPIを公開面に置く誤解を避ける
B. C-1どおりビルダーを提供し、内部では使用しない — API面の他バックエンドとの対称性を優先し、docコメントで「SQLiteでは行アドレスに影響しない」ことを明記
X. Other (please specify)

[Answer]: X. Other — ユーザー回答（verbatim）: 「pkey,skeyの考え方は踏襲して下さい。これは書き込み分散です。aid,seq_nrは読み込み用のキーなのです。 設計を破綻させるな。」続けて「pkeyとskeyをどう使うは実装によります。テーブルやDBを分けるキーに使ってもよいですが、考え方は先ほどのとおり書き込み分散として設計してほしい」。よって: pkey/skey（KeyResolver）はSQLiteでも書き込み分散キーとして踏襲し、`with_key_resolver` は提供・実使用する。aid/seq_nr は読み込み用キー。pkey/skeyの具体的適用（列・テーブル分割・DB分割）は実装裁量だが、書き込み分散の概念を設計に保持する。C-4のスキーマ形状には pkey/skey 列を追加する（列の最終確定は機能設計の裁量範囲）

## Consolidated Summary Confirmation

Does this all look correct before I generate the artifact?

- Looks correct
- Request changes

[Answer]: Looks correct
