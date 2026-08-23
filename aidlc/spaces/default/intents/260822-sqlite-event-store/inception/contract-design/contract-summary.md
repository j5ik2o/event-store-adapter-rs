# 契約サマリー: SQLite対応EventStoreとバックエンドfeature分割

ユニット定義（`../units-generation/unit-of-work.md`）と依存DAG（`../units-generation/unit-of-work-dependency.md`）の統合点から契約境界を抽出し、コンポーネントカタログ（`../domain-design/components.md`）のエンティティ形状と要件定義書（`../requirements-analysis/requirements.md`）の契約要件（FR-2.4 中立エラー、NFR-1 シグネチャ不変更）で各契約の形を確定した。契約点の裁定はQ&A（`contract-design-questions.md`）で確定済み。

## Contracts table

| # | Provider Unit | Consumer | Mechanism | Owner |
|---|---|---|---|---|
| C-1 | U2 u2-sqlite-backend（＋U1の共通型） | External: crates.io利用者（Rust開発者） | Rust公開API（in-processトレイト契約） | U1/U2 |
| C-2 | U1 u1-backend-features | U2 u2-sqlite-backend | Rust内部トレイト（StorageBackend）＋中立エラー型 | U1 |
| C-3 | U1/U2（feature定義） | U3 u3-ci-quality・External: 利用者のCargo.toml | cargo feature 名（ビルド契約） | U1（器）/U2（sqlite系） |
| C-4 | U2 u2-sqlite-backend | U4 u4-docs・External: 利用者（スキーマ参照） | shared-schema（SQLite DDL形状） | U2 |

## C-1: 公開API契約（External）

利用者が依存する公開面。トレイトシグネチャは既存不変更（NFR-1）、エラー型のみ破壊的変更（FR-2.4、CHANGELOG必須）。

```rust
// contract: public-api (抜粋 — 形状の契約。実装詳細は機能設計で確定)

// 既存トレイト（シグネチャ不変更）
#[async_trait]
pub trait EventStore { /* persist_event / persist_event_and_snapshot /
                          get_latest_snapshot_by_id / get_events_by_id_since_seq_nr — 既存のまま */ }

// 新規公開型（feature = "sqlite"）
pub struct EventStoreForSqlite<AID, A, E> { /* ... */ }
impl EventStoreForSqlite<AID, A, E> {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, EventStoreWriteError>; // ファイルDB（初回利用時にスキーマ自動作成）
    pub fn new_in_memory() -> Result<Self, EventStoreWriteError>;             // :memory:（共有範囲はインスタンス単位）
    // 既存バックエンドと同型のビルダー
    pub fn with_keep_snapshot_count(self, count: Option<usize>) -> Self;
    pub fn with_delete_ttl(self, ttl: Option<Duration>) -> Self;
    pub fn with_key_resolver(self, resolver: ...) -> Self;
    pub fn with_event_serializer(self, ser: ...) -> Self;
    pub fn with_snapshot_serializer(self, ser: ...) -> Self;
}

// エラー型（破壊的変更 — 全バックエンド共通の中立表現）
pub enum EventStoreWriteError {
    SerializationError(...),
    OptimisticLockError(OptimisticLockContext), // 軽量コンテキスト（集約ID・期待/実バージョン等の文字列情報）。SDK型を含まない
    IOError(...),
    OtherError(...),
}
```

- エラー・再試行挙動: 楽観的ロック失敗は `OptimisticLockError` を返し、再試行は利用者責務（既存契約踏襲）。I/O失敗はpanicせず `IOError` 系へ写像
- 互換性: 追加的変更（新featureやメソッド追加）は安全。破壊的変更はConventional Commitsのsemver判定でメジャー扱い＋CHANGELOG記載

## C-2: 内部抽象契約（U1 → U2）

U1がインターフェース形状を提供し、U2がそれを実装する（依存の向きの記述であり着手順序は規定しない）。

```rust
// contract: storage-backend (既存5メソッド — U1で形が確定、シグネチャは現行維持)
#[async_trait]
pub trait StorageBackend<AID, A, E>: Send + Sync + Clone + Debug + 'static { // 非pubモジュール経由で事実上クレート内限定
    async fn fetch_latest_snapshot(&self, aid: &AID) -> Result<Option<SnapshotEnvelope<A>>, EventStoreReadError>;
    async fn fetch_events_since(&self, aid: &AID, seq_nr: usize) -> Result<Vec<E>, EventStoreReadError>;
    async fn create_event_and_snapshot(...) -> Result<(), EventStoreWriteError>;
    async fn update_event_and_snapshot(...) -> Result<(), EventStoreWriteError>;
    async fn on_event_persisted(...) -> Result<(), EventStoreWriteError>; // 保守フック（保持ポリシーの実行点）
}
```

- 失敗時挙動: 各メソッドは中立エラー型で失敗を返す（panic禁止）。`update_event_and_snapshot` のバージョン不一致は `OptimisticLockError`
- 保持ポリシー: `SnapshotMaintenance`（keep_snapshot_count / delete_ttl）の適用は `on_event_persisted` を実行点とする（サイレント無効禁止 — FR-1.6）

## C-3: cargo feature 契約（ビルド境界）

```yaml
# contract: shared-schema (Cargo features — 利用者のCargo.tomlとCIマトリクスの共通軸)
features:
  dynamodb: "DynamoDBバックエンド（aws-sdk-dynamodb / aws-config を有効化)"
  bigtable: "Bigtableバックエンド（tonic / googleapis-tonic-google-bigtable-v2 を有効化)"
  sqlite: "SQLiteバックエンド（rusqlite。単体でバンドル=SQLite同梱が既定 — これだけで自己完結)"
  sqlite-system: "sqliteのリンク方式をシステムSQLiteへ切替（sqliteと併用)"
default: []   # デフォルトfeatureなし（破壊的変更）
always_on:
  memory: "Memoryバックエンドはfeatureなしで常時有効"
```

- 契約規則: feature名は追加のみ安全。既存feature名の改名・削除はメジャー扱い。CIマトリクス（U3）はこの表を軸にビルド組み合わせを検証（FR-5.1）

## C-4: SQLiteスキーマ契約（shared-schema）

```yaml
# contract: shared-schema (SQLite DDL形状 — 作成はライブラリの自動作成が担い、本契約は形状の合意。列型・制約の最終確定は機能設計)
tables:
  journal:
    identifier: [aid, seq_nr]     # 複合キー（コンポーネントカタログ SqliteJournalRow）
    columns: [aid, seq_nr, event_id, payload, occurred_at]
  snapshot:
    identifier: [aid, seq_nr]     # 複合キー（SqliteSnapshotRow）
    columns: [aid, seq_nr, version, payload, created_at]
invariants:
  - "楽観的ロック検証と書き込みは単一トランザクション内（ADR-003）"
  - "スキーマ変更は自動作成ロジックのバージョン整合で吸収（利用者DDL適用なし）"
```

- U4（ドキュメント）はこの形状を DATABASE_SCHEMA.md（英/日）へ転記する（情報提供 — FR-6.3）

## Contract ownership rules

- C-1（公開API）: 共通型はU1、SQLite面はU2が所有。破壊的変更はCHANGELOG記載＋semverメジャーで合意（確定済みリリース方針）
- C-2（内部抽象）: U1が所有。U2実装中に形状変更が必要になった場合はU1へ差し戻して合意（勝手な拡張禁止）
- C-3（feature名）: 追加は安全。未定義feature指定はcargoがビルドエラーとする（黙って無視されることはない）ため、feature名の改称・削除は利用者のビルドを直接壊す破壊的変更＝メジャー扱いとする
- C-4（スキーマ）: U2が所有。列追加は自動作成の後方互換で吸収、列の改名・削除は破壊的変更として扱う

## Open questions

| Contract | Question | Blocks |
|---|---|---|
| — | なし（列型・インデックス等の最終確定は機能設計工程の通常作業として予定済み） | — |

## Assumptions & Open Questions

None.

## Review

**Verdict:** READY
**Reviewer:** aidlc-architecture-reviewer-agent
**Date:** 2026-08-22T17:39:04Z
**Iteration:** 2

### Findings

| # | Severity | Location | Finding | Recommendation |
|---|---|---|---|---|
| — | — | — | 新規Critical/Major/Minor所見なし | — |

### Previous Findings — Resolution Check

| # | Severity | Iteration 1 Finding | Status |
|---|---|---|---|
| 1 | Major | C-3所有権規則「未知featureの無視は利用者側で成立（cargoの性質上、未定義feature指定はビルドエラーとなるため追加のみ）」が主文と根拠で逆方向を主張していた矛盾 | 解消済み — 該当行（`## Contract ownership rules`のC-3）は「未定義feature指定はcargoがビルドエラーとする（黙って無視されることはない）ため、feature名の改称・削除は利用者のビルドを直接壊す破壊的変更＝メジャー扱いとする」に修正されており、主文と根拠の向きが一致し、実機検証済みのcargo挙動（未宣言feature指定は `error: package does not have that feature` で即失敗）とも整合する。 |
| 2 | Minor | C-2 Rustコードブロックが `pub(crate) trait StorageBackend` と記載し、実ソース（`lib/src/event_store_backend.rs:22`、`pub trait StorageBackend`）の可視性修飾子と不一致 | 解消済み — コードブロックは `pub trait StorageBackend<AID, A, E>: Send + Sync + Clone + Debug + 'static { // 非pubモジュール経由で事実上クレート内限定` に修正されており、実ソースの `pub` 修飾子と一致しつつ、実効的な非公開性（非pubモジュール経由）もコメントで正しく補足されている。 |

他の記載内容（Contracts table、C-1〜C-4本文、Contract ownership rulesの他項目、Open questions）に変更はなく、Iteration 1で確認済みの検証結果（DAG統合点との1:1対応、`EventStore`/`StorageBackend`メソッド名の実在性、`EventStoreWriteError`の破壊的変更前提、C-4スキーマとコンポーネントカタログの一致、ADR-003整合、Q&A反映）は引き続き有効。

### Summary

Iteration 1で指摘したMajor 1件・Minor 1件はいずれも本文修正により解消され、新規の所見は生じていない。Critical・Major・Minorとも0件のため、READYと判定する。
