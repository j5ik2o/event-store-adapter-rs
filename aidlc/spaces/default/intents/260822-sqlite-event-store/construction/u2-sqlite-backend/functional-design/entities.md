# エンティティモデル — u2-sqlite-backend (entities)

ユニット定義（`../../../inception/units-generation/unit-of-work.md` U2）とストーリー対応（`../../../inception/units-generation/unit-of-work-story-map.md`: US1.1〜US1.4/US2.3/US3.2）、要件（`../../../inception/requirements-analysis/requirements.md` FR-1.x/FR-2.5/FR-4.x）、コンポーネントカタログ（`../../../inception/domain-design/components.md` SqliteBackend）、契約（`../../../inception/contract-design/contract-summary.md` C-1/C-2/C-4）に基づく。Q&A（`functional-design-questions.md`）の確定事項: **pkey/skey（KeyResolver）は書き込み分散キーとして踏襲し、aid/seq_nr は読み込み用キーとする（ユーザー確定）**。この確定により、契約C-4のスキーマ形状（列の最終確定は機能設計の裁量範囲）へ pkey/skey 列を追加する。**C-4の暫定スケッチからの乖離はさらに2点あり、ここで明示的に開示する**: (1) journal の `event_id` 列は設けない、(2) snapshot の時刻列は `created_at` ではなく `last_updated_at` とする。いずれも実際のDynamoDB参照実装の項目構造（`event_id` 属性なし・時刻属性名 `last_updated_at`）に合わせる判断であり、C-4スケッチより参照実装との対称性を優先する（列の最終確定は機能設計の裁量範囲）。

## Source of truth

```yaml
entities:
  - name: SqliteJournalRow
    description: イベントジャーナルの1行（1イベント）。書き込みは (pkey, skey)、読み込みは (aid, seq_nr) 索引
    attributes:
      - name: pkey
        logical_type: string
        required: true
        unique: false
        constraints: "書き込み分散キー — KeyResolver.resolve_partition_key(aid, shard_count) の値（例: <型名>-<hash % shard_count>）"
      - name: skey
        logical_type: string
        required: true
        unique: false
        constraints: "書き込みアドレスのソートキー — KeyResolver.resolve_sort_key(aid, seq_nr) の値（例: <型名>-<集約ID>-<seq_nr>）"
      - name: aid
        logical_type: string
        required: true
        unique: false
        constraints: "集約ID文字列 — 読み込み用キーの第1要素"
      - name: seq_nr
        logical_type: integer
        required: true
        unique: false
        constraints: "イベント連番 — 読み込み用キーの第2要素。(aid, seq_nr) は一意"
      - name: payload
        logical_type: binary
        required: true
        unique: false
        constraints: "EventSerializer.serialize の出力バイト列（既定はJSON）"
      - name: occurred_at
        logical_type: integer
        required: true
        unique: false
        constraints: "イベント発生時刻（エポックミリ秒）"
    entity_constraints:
      - "識別子は (pkey, skey)。読み込み索引 (aid, seq_nr) は一意（同一イベントの二重追記を排除）"
    relationships: []

  - name: SqliteSnapshotRow
    description: スナップショットの1行。skey の seq_nr=0 スロット行が「現行スナップショット」でありversion（楽観的ロックカウンタ）を保持する。履歴行は実 seq_nr（保持ポリシー有効時のみ作成）
    attributes:
      - name: pkey
        logical_type: string
        required: true
        unique: false
        constraints: "書き込み分散キー — resolve_partition_key(aid, shard_count)"
      - name: skey
        logical_type: string
        required: true
        unique: false
        constraints: "resolve_sort_key(aid, 0) = 現行スロット、resolve_sort_key(aid, seq_nr) = 履歴行"
      - name: aid
        logical_type: string
        required: true
        unique: false
        constraints: "集約ID文字列 — 読み込み用キー"
      - name: seq_nr
        logical_type: integer
        required: true
        unique: false
        constraints: "スナップショット時点のイベント連番（既存DynamoDB実装と同一の格納規約に従う）"
      - name: version
        logical_type: integer
        required: true
        unique: false
        constraints: "楽観的ロックカウンタ — 現行スロット行のみが検証対象。更新CASで expected → expected+1"
      - name: payload
        logical_type: binary
        required: true
        unique: false
        constraints: "SnapshotSerializer.serialize の出力バイト列"
      - name: last_updated_at
        logical_type: integer
        required: true
        unique: false
        constraints: "最終更新時刻（エポックミリ秒）"
    entity_constraints:
      - "識別子は (pkey, skey)。現行スロット行（skeyのseq_nr=0）の存在が集約の存在と同値（作成の一意性制約点）"
      - "読み込み索引 (aid, seq_nr)"
    relationships: []

  - name: SqliteStoreHandle
    description: SQLiteバックエンドの内部状態（公開されない）。基底接続の共有単位
    attributes:
      - name: connection
        logical_type: shared-reference
        required: true
        unique: false
        constraints: "単一の基底接続を排他制御付きで共有（Arc<Mutex<Connection>> — Q1=A確定）。ロック型・ガードは公開APIへ非露出（U1のMemory準拠化と同型の隠蔽境界）"
      - name: shard_count
        logical_type: integer
        required: true
        unique: false
        constraints: "pkey算出用のシャード数（既存バックエンドと同じ既定値・with_shard_countで変更可能）"
    entity_constraints:
      - "Clone は基底接続を共有する（`:memory:` のインスタンス単位共有と楽観的ロック競合テストの前提 — stories.md AC1.2.1実装ノート）"
    relationships: []
```

## サマリー

- **SqliteJournalRow / SqliteSnapshotRow**: 既存DynamoDBバックエンドの項目構造と対称。書き込みは KeyResolver が解決する (pkey, skey)（**書き込み分散** — ユーザー確定）、読み込みは (aid, seq_nr) 索引。pkey/skey によるテーブル分割・DB分割は実装裁量として設計上許容する（初期実装は単一テーブル＋列。分割しても書き込み分散の概念・キー構造は不変）
- **スナップショットのスロット規約**: skey の seq_nr=0 の行が現行スナップショット（version保持・CASの検証点）。履歴行は保持ポリシー有効時のみ実 seq_nr で追記され、保守（BR2.7）の削除対象になる
- **SqliteStoreHandle**: 内部状態。接続共有のClone意味論が `:memory:` 共有（AC1.3.1）と決定的競合テスト（AC1.2.1）を支える

## Assumptions & Open Questions

None.
