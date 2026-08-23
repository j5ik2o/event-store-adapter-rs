# ビジネスルール — u2-sqlite-backend (rules)

ユニット定義（`../../../inception/units-generation/unit-of-work.md` U2）・要件（`../../../inception/requirements-analysis/requirements.md` FR-1.x/FR-2.4/FR-2.5/FR-4.x/NFR-2/NFR-4）・コンポーネントカタログ（`../../../inception/domain-design/components.md` SqliteBackend）・契約（`../../../inception/contract-design/contract-summary.md` C-1/C-2/C-4）・ストーリー（`../../../inception/units-generation/unit-of-work-story-map.md` 経由のUS1.1〜US1.4/US2.3/US3.2）から導出。Q&A確定事項（Q1: Arc<Mutex<Connection>>同期実行、Q2: pkey/skey書き込み分散の踏襲）を設計不変条件とする。

## Source of truth

```yaml
rules:
  - id: BR2.1
    statement: journal / snapshot テーブルと読み込み索引は初回利用時にライブラリが自動作成する（利用者によるDDL適用は要求しない）
    category: policy
    applies_to: SqliteBackend（接続確立時）
    trigger: ストア構築（new / new_in_memory）時の接続確立
    logic: IF 必要なテーブル・索引が存在しない THEN 作成する（存在すれば何もしない — 冪等）
    violation: AC1.1.1（空DBファイルからの永続化成功）で検出
    source: FR-1.4
  - id: BR2.2
    statement: 書き込みアドレスは KeyResolver の (pkey, skey)、読み込みは (aid, seq_nr) 索引を用いる
    category: constraint
    applies_to: SqliteJournalRow / SqliteSnapshotRow の全読み書き
    trigger: 全ての永続化・読み出し操作
    logic: >-
      IF 行を書き込む THEN pkey = resolve_partition_key(aid, shard_count)（書き込み分散）、
      skey = resolve_sort_key(aid, seq_nr) を格納しアドレスとする。
      IF 集約を読み出す THEN (aid, seq_nr) 索引で取得する。
      pkey/skey によるテーブル分割・DB分割は実装裁量（書き込み分散の概念は不変条件）
    violation: コードレビュー・AC1.1.2（復元）で検出
    source: ユーザー確定（Q2）/ FR-1.1
  - id: BR2.3
    statement: 新規集約の作成は現行スナップショットスロット（skeyのseq_nr=0）の挿入一意性で保証し、重複作成は OptimisticLockError を返す
    category: constraint
    applies_to: create_event_and_snapshot
    trigger: 作成イベントの永続化
    logic: IF スロット0行が既に存在する THEN 挿入は一意性違反で失敗し BR1.2書式の OptimisticLockError へ写像する（U1確立の4バックエンド対称挙動）
    violation: エラー契約テストで検出
    source: FR-1.3
  - id: BR2.4
    statement: 更新は単一トランザクション内のCAS（version=expected 検証付き更新）とし、バージョン不一致は actual_version 付きBR1.2書式の OptimisticLockError を返す
    category: constraint
    applies_to: update_event_and_snapshot
    trigger: 既存集約への更新イベント永続化
    logic: >-
      IF 更新する THEN 1つのトランザクション内で
      (1) スロット0行を version=expected の条件付きで expected+1 へ更新（aggregateありなら payload / seq_nr も更新）、
      (2) journal へイベント追記、(3) 保持設定時は履歴スナップショット追記、を行いコミットする。
      IF 条件付き更新の影響行が0 THEN 実versionを読み取り OptimisticLockError（actual_version付加）を返しロールバックする
    violation: 楽観的ロック競合パステスト（AC1.2.1/AC1.2.2）で検出
    source: FR-1.3 / ADR-003（Bigtableの非原子方式TD-07は踏襲しない）
  - id: BR2.5
    statement: "`:memory:` の共有範囲はストアインスタンス単位とし、Clone は基底接続を共有する"
    category: constraint
    applies_to: SqliteStoreHandle
    trigger: new_in_memory 構築と Clone
    logic: IF `:memory:` 指定 THEN 同一インスタンス系列（Cloneを含む）は単一の基底接続を共有し整合した読み書きができる。別インスタンスとの共有は要求しない
    violation: AC1.3.1 で検出
    source: FR-1.5 / C-1
  - id: BR2.6
    statement: rusqlite の実行は各 StorageBackend メソッド内の排他制御下での同期実行とし、ロックガード保持中に await しない
    category: policy
    applies_to: SqliteBackend の全メソッド
    trigger: 全操作の実行
    logic: IF 操作を実行する THEN 接続ロックを取得し同期的に完了させ解放する（追加の非同期ランタイム依存を導入しない）
    violation: コードレビューで検出
    source: ユーザー確定（Q1=A）/ NFR-2
  - id: BR2.7
    statement: スナップショット保持ポリシー（keep_snapshot_count / delete_ttl）は on_event_persisted を実行点として履歴行に適用し、設定が実際に効く（サイレント無効を作らない）
    category: constraint
    applies_to: on_event_persisted / SqliteSnapshotRow（スロット0以外の履歴行）
    trigger: イベント永続化後の保守フック
    logic: >-
      IF keep_snapshot_count 設定あり THEN 履歴行が保持数を超えた分の古い行を削除する。
      IF delete_ttl 設定あり THEN 期限切れの履歴行を削除する。現行スロット行は削除対象外
    violation: AC1.4.1 / AC1.4.2 で検出
    source: FR-1.6 / C-2（TD-08のサイレント無効を複製しない）
  - id: BR2.8
    statement: バックエンド内部のエラーは panic させず、決定表どおり中立エラー型へ写像する
    category: constraint
    applies_to: SqliteBackend の全経路
    trigger: あらゆる失敗（書き込み不能パス・ロック汚染・直列化失敗等）
    logic: IF 失敗する THEN 競合→OptimisticLockError（BR1.2書式）、直列化→SerializationError、I/O→IOError、他→OtherError（rusqliteの生エラー詳細はメッセージ衛生規則の範囲で保持）
    violation: AC1.1.4・エラー契約テストで検出
    source: FR-2.4 / NFR-4 / U1決定表
  - id: BR2.9
    statement: リンク方式は `sqlite` featureで同梱（バンドル）を既定とし、`sqlite-system` featureでシステムSQLiteへのリンクに切り替えられる
    category: policy
    applies_to: Cargo.toml features
    trigger: ビルド構成の選択
    logic: IF `sqlite` のみ THEN 同梱ソースのコンパイルで自己完結。IF `sqlite-system` 併用 THEN システムSQLiteへリンクする
    violation: AC2.3.1 / AC2.3.2（ビルド検証）で検出
    source: FR-2.5 / C-3
  - id: BR2.10
    statement: U2の追加ランタイム依存は rusqlite の1クレートのみとする
    category: constraint
    applies_to: lib/Cargo.toml
    trigger: 依存追加時
    logic: IF ランタイム依存を追加する THEN rusqlite（とそのbundled/system制御）以外を追加しない
    violation: cargo tree 検査で検出
    source: NFR-2 / U2制約
  - id: BR2.11
    statement: SqliteBackend は StorageBackend（5メソッド）実装＋GenericEventStore 委譲で EventStore を提供し、EventStore を直接実装しない
    category: constraint
    applies_to: SqliteBackend / EventStoreForSqlite
    trigger: 実装構造
    logic: IF EventStore を提供する THEN GenericEventStore 経由とする（直接implの追加は設計違反）
    violation: AC1.1.3（コードレビュー・grep検査）で検出
    source: FR-1.2 / project.md Mandated
  - id: BR2.12
    statement: SQLiteのテストは共有シナリオ搭載・Docker不要・楽観的ロック競合パスとエラー契約のテストを必須とする
    category: policy
    applies_to: TestSupport / SQLiteテストスイート
    trigger: テスト設計・実行
    logic: >-
      IF SQLiteのテストを書く THEN exercise_user_account_flow に搭載し、
      testcontainers / Docker を使わずファイルまたは `:memory:` で完結させ、
      競合パス（並行更新で一方が OptimisticLockError）とエラー契約（中立表現・書式）を検証する
    violation: AC3.2.1 / AC3.2.2 / AC3.2.3 で検出
    source: FR-4.1〜FR-4.3 / project.md Mandated
```

## ルールサマリー

| ID | 区分 | 要旨 | 検出手段 |
|---|---|---|---|
| BR2.1 | policy | スキーマ自動作成（冪等） | AC1.1.1 |
| BR2.2 | constraint | 書き込み=(pkey,skey)分散 / 読み込み=(aid,seq_nr) | レビュー・AC1.1.2 |
| BR2.3 | constraint | 作成の一意性→OptimisticLockError | エラー契約テスト |
| BR2.4 | constraint | 単一トランザクションCAS＋actual_version | AC1.2.1/1.2.2 |
| BR2.5 | constraint | :memory: はインスタンス単位共有 | AC1.3.1 |
| BR2.6 | policy | Mutex下の同期実行・ガード越しawait禁止 | レビュー |
| BR2.7 | constraint | 保持ポリシーが実際に効く | AC1.4.1/1.4.2 |
| BR2.8 | constraint | panicなし・決定表写像 | AC1.1.4 |
| BR2.9 | policy | バンドル既定／system切替 | AC2.3.1/2.3.2 |
| BR2.10 | constraint | 追加依存はrusqliteのみ | cargo tree |
| BR2.11 | constraint | StorageBackend＋GenericEventStore経由 | AC1.1.3 |
| BR2.12 | policy | 共有シナリオ・Docker不要・競合/契約テスト | AC3.2.1〜3.2.3 |

## Assumptions & Open Questions

None.
