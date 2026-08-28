use std::fmt::Debug;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use rusqlite::{params, Connection, ErrorCode, OptionalExtension, ToSql, Transaction};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::event_envelope::{EventEnvelope, SnapshotEnvelope};
use crate::event_store_backend::{SnapshotMaintenance, StorageBackend};
use crate::generic_event_store::GenericEventStore;
use crate::key_resolver::{DefaultKeyResolver, KeyResolver};
use crate::serializer::{EventSerializer, JsonEventSerializer, JsonSnapshotSerializer, SnapshotSerializer};
use crate::types::{
  format_optimistic_lock_message, AggregateId, EventStore, EventStoreReadError, EventStoreWriteError,
};

// FR6.1: SQLite バックエンドの v3 封筒化。封筒⇔列の 1:1 マッピング（AC2.1.1）、単一トランザクション
// 内の条件付き UPDATE による原子的 CAS の現行維持（BR1.1 / P10 / NFR4.4）、skey 判別へ一本化した
// 保持ポリシー（BR2.4 / BR3.1）を StorageBackend + GenericEventStore の 2 層委譲構造の上に実装する。

/// 既存バックエンドの利用実績と同じ既定シャード数（pkey書き込み分散幅）
const DEFAULT_SHARD_COUNT: u64 = 64;

// BR2.4: 現行スロット行の skey は seq_nr=0 マーカーで解決する（キー設計の現行維持）。
// このマーカーは物理キーの解決だけに使い、seq_nr 列の実値（event.seq_nr()）とは混用しない。
const CURRENT_SNAPSHOT_SKEY_MARKER: usize = 0;

/// スキーマは接続確立時に不存在なら作成する（冪等 — 利用者DDLなし）。
/// 書き込みアドレスはPK (pkey, skey)、読み込みは (aid, seq_nr) 索引。
/// BR2.1: journal は封筒メタデータ 4 点 + payload の列を持ち、manifest 列
/// （TEXT NOT NULL DEFAULT ''）を v3 で新設する（v2 で作成済み DB との読取互換は保証しない — 既決）。
/// occurred_at 列は epoch nanos の INTEGER（供給値の完全往復 — 下記 occurred_at_nanos 参照）。
const CREATE_SCHEMA_SQL: &str = "\
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
";

const INSERT_SNAPSHOT_SQL: &str =
  "INSERT INTO snapshot (pkey, skey, aid, seq_nr, version, payload, last_updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)";

// BR2.1 / AC2.1.1 / AC5.1.1: 1 封筒 = journal 1 行。メタデータ 4 点（aid / seq_nr / occurred_at /
// manifest）+ payload を列へ 1:1 で書く（manifest 省略時は空文字列がそのまま格納される — U1 BR1.2）
const INSERT_JOURNAL_SQL: &str =
  "INSERT INTO journal (pkey, skey, aid, seq_nr, payload, occurred_at, manifest) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)";

/// Event Store for SQLite.
///
/// Persists events and snapshots to a local SQLite database (a file or `:memory:`) and
/// creates the required tables and indexes on construction. The supported sharing unit is
/// one store instance and its clones (which share the underlying connection); opening the
/// same database file from multiple store instances or processes is out of scope (NFR3.9 —
/// unsupported concurrent access surfaces as an immediate `IOError`, not silent corruption).
pub struct EventStoreForSqlite<AID, A, P>
where
  AID: AggregateId, {
  inner: GenericEventStore<AID, A, P, SqliteBackend<AID, A, P>>,
}

// P2(U1): derive は型パラメータ（A / P）へ Debug / Clone 境界を課すため手動 impl とし、
// payload への要求を実フィールド由来のものに限定する（BR1.6 の最小境界維持）
impl<AID: AggregateId, A, P> Debug for EventStoreForSqlite<AID, A, P> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("EventStoreForSqlite").finish()
  }
}

impl<AID: AggregateId, A, P> Clone for EventStoreForSqlite<AID, A, P> {
  // クローン間で基底接続を共有する（`:memory:` のインスタンス単位共有と
  // 決定的競合テストの前提。Clone時の状態分岐を作らない — NFR3.9 の共有単位）
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}

impl<AID, A, P> EventStoreForSqlite<AID, A, P>
where
  AID: AggregateId,
  A: Serialize + DeserializeOwned + Send + Sync + 'static,
  P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
  /// Creates an event store backed by the SQLite database file at `path`.
  ///
  /// Opens (or creates) the database file and creates the journal/snapshot tables and
  /// indexes if they do not exist. Returns a neutral error instead of panicking when the
  /// connection cannot be established.
  pub fn new(path: impl AsRef<Path>) -> Result<Self, EventStoreWriteError> {
    let connection = Connection::open(path).map_err(map_write_error)?;
    Self::from_connection(connection)
  }

  /// Creates an event store backed by an in-memory SQLite database.
  ///
  /// The database is shared by this instance and its clones only (instance-scoped
  /// sharing); it disappears when the last clone is dropped.
  pub fn new_in_memory() -> Result<Self, EventStoreWriteError> {
    let connection = Connection::open_in_memory().map_err(map_write_error)?;
    Self::from_connection(connection)
  }

  fn from_connection(connection: Connection) -> Result<Self, EventStoreWriteError> {
    connection.execute_batch(CREATE_SCHEMA_SQL).map_err(map_write_error)?;
    Ok(Self {
      inner: GenericEventStore::new(SqliteBackend::new(connection)),
    })
  }

  /// 保持するスナップショット履歴数を設定する。
  ///
  /// BR4.1(U1) / P3: 検証（`Some(0)` の拒否）は `GenericEventStore` 側の 1 箇所で行い、
  /// このラッパーは Result を素通しする。
  pub fn with_keep_snapshot_count(mut self, keep_snapshot_count: Option<usize>) -> Result<Self, EventStoreWriteError> {
    self.inner = self.inner.with_keep_snapshot_count(keep_snapshot_count)?;
    Ok(self)
  }

  /// 履歴スナップショットの保持期限を設定する。
  ///
  /// `with_keep_snapshot_count` と併用時のみ効果を持つ（`SnapshotMaintenance` の現行契約）。
  /// 保持数の設定なしでは履歴行が記録されないため、この設定は作用対象を持たない。
  pub fn with_delete_ttl(mut self, delete_ttl: Option<Duration>) -> Self {
    self.inner = self.inner.with_delete_ttl(delete_ttl);
    self
  }

  /// Sets the shard count used to resolve partition keys and returns the updated store.
  /// Zero is rejected at write time with a neutral store error (key resolution would divide by zero).
  pub fn with_shard_count(mut self, shard_count: u64) -> Self {
    self.inner.backend_mut().set_shard_count(shard_count);
    self
  }

  /// Sets the key resolver and returns the updated store.
  pub fn with_key_resolver(mut self, key_resolver: Arc<dyn KeyResolver<ID = AID>>) -> Self {
    self.inner.backend_mut().set_key_resolver(key_resolver);
    self
  }

  /// Sets the event serializer and returns the updated store.
  pub fn with_event_serializer(mut self, serializer: Arc<dyn EventSerializer<P>>) -> Self {
    self.inner.backend_mut().set_event_serializer(serializer);
    self
  }

  /// Sets the snapshot serializer and returns the updated store.
  pub fn with_snapshot_serializer(mut self, serializer: Arc<dyn SnapshotSerializer<A>>) -> Self {
    self.inner.backend_mut().set_snapshot_serializer(serializer);
    self
  }

  /// Returns the snapshot maintenance configuration.
  pub fn maintenance(&self) -> &SnapshotMaintenance {
    self.inner.maintenance()
  }
}

#[async_trait]
impl<AID, A, P> EventStore for EventStoreForSqlite<AID, A, P>
where
  AID: AggregateId,
  A: Serialize + DeserializeOwned + Send + Sync + 'static,
  P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
  type A = A;
  type AID = AID;
  type P = P;

  async fn persist_event(
    &mut self,
    event: EventEnvelope<Self::AID, Self::P>,
    expected_version: usize,
  ) -> Result<(), EventStoreWriteError> {
    self.inner.persist_event(event, expected_version).await
  }

  async fn persist_event_and_snapshot(
    &mut self,
    event: EventEnvelope<Self::AID, Self::P>,
    aggregate: Self::A,
    expected_version: usize,
  ) -> Result<(), EventStoreWriteError> {
    self
      .inner
      .persist_event_and_snapshot(event, aggregate, expected_version)
      .await
  }

  async fn get_latest_snapshot_by_id(
    &self,
    aid: &Self::AID,
  ) -> Result<Option<SnapshotEnvelope<Self::A>>, EventStoreReadError> {
    self.inner.get_latest_snapshot_by_id(aid).await
  }

  async fn get_events_by_id_since_seq_nr(
    &self,
    aid: &Self::AID,
    seq_nr: usize,
  ) -> Result<Vec<EventEnvelope<Self::AID, Self::P>>, EventStoreReadError> {
    self.inner.get_events_by_id_since_seq_nr(aid, seq_nr).await
  }
}

/// SQLiteバックエンドの内部状態。基底接続は排他制御付きで共有し、
/// ロック型・ガード・Arcは公開シグネチャへ露出しない（隠蔽境界）
struct SqliteBackend<AID, A, P>
where
  AID: AggregateId, {
  connection: Arc<Mutex<Connection>>,
  shard_count: u64,
  key_resolver: Arc<dyn KeyResolver<ID = AID>>,
  event_serializer: Arc<dyn EventSerializer<P>>,
  snapshot_serializer: Arc<dyn SnapshotSerializer<A>>,
}

// P2(U1): derive は A / P へ Debug 境界を課すため手動 impl とする（内部構成は表示しない）
impl<AID: AggregateId, A, P> Debug for SqliteBackend<AID, A, P> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("SqliteBackend").finish()
  }
}

impl<AID: AggregateId, A, P> Clone for SqliteBackend<AID, A, P> {
  // Arcの共有クローン — 基底接続を共有する（NFR3.9 の共有単位）
  fn clone(&self) -> Self {
    Self {
      connection: Arc::clone(&self.connection),
      shard_count: self.shard_count,
      key_resolver: Arc::clone(&self.key_resolver),
      event_serializer: Arc::clone(&self.event_serializer),
      snapshot_serializer: Arc::clone(&self.snapshot_serializer),
    }
  }
}

impl<AID, A, P> SqliteBackend<AID, A, P>
where
  AID: AggregateId,
  A: Serialize + DeserializeOwned + Send + Sync + 'static,
  P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
  fn new(connection: Connection) -> Self {
    Self {
      connection: Arc::new(Mutex::new(connection)),
      shard_count: DEFAULT_SHARD_COUNT,
      key_resolver: Arc::new(DefaultKeyResolver::default()),
      event_serializer: Arc::new(JsonEventSerializer::default()),
      snapshot_serializer: Arc::new(JsonSnapshotSerializer::default()),
    }
  }

  fn set_shard_count(&mut self, shard_count: u64) {
    self.shard_count = shard_count;
  }

  fn set_key_resolver(&mut self, key_resolver: Arc<dyn KeyResolver<ID = AID>>) {
    self.key_resolver = key_resolver;
  }

  fn set_event_serializer(&mut self, serializer: Arc<dyn EventSerializer<P>>) {
    self.event_serializer = serializer;
  }

  fn set_snapshot_serializer(&mut self, serializer: Arc<dyn SnapshotSerializer<A>>) {
    self.snapshot_serializer = serializer;
  }

  fn resolve_pkey(&self, id: &AID) -> String {
    self.key_resolver.resolve_partition_key(id, self.shard_count)
  }

  // shard_count=0 はキー解決（hash % shard_count）がゼロ除算でpanicするため、
  // キー解決前に中立エラーで拒否する（panic禁止のエラー契約を維持 — BR4.1）
  fn ensure_shard_count(&self) -> Result<(), EventStoreWriteError> {
    if self.shard_count == 0 {
      return Err(EventStoreWriteError::OtherError(
        "shard_count must be greater than zero".to_string(),
      ));
    }
    Ok(())
  }

  fn resolve_skey(&self, id: &AID, seq_nr: usize) -> String {
    self.key_resolver.resolve_sort_key(id, seq_nr)
  }

  fn current_slot_skey(&self, id: &AID) -> String {
    self.resolve_skey(id, CURRENT_SNAPSHOT_SKEY_MARKER)
  }

  // ロックは各メソッド内で取得・解放し、ガード保持中に `.await` しない。
  // ポイズニングはpanicさせず文字列化してエラーへ写像する（ガード起因の機密混入なし）。
  fn lock_connection(&self) -> Result<MutexGuard<'_, Connection>, String> {
    self
      .connection
      .lock()
      .map_err(|_| "sqlite connection mutex is poisoned".to_string())
  }
}

#[async_trait]
impl<AID, A, P> StorageBackend<AID, A, P> for SqliteBackend<AID, A, P>
where
  AID: AggregateId,
  A: Serialize + DeserializeOwned + Send + Sync + 'static,
  P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
  async fn fetch_latest_snapshot(&self, aid: &AID) -> Result<Option<SnapshotEnvelope<A>>, EventStoreReadError> {
    // BR2.4: 現行スロット行は skey（主キー照合 — マーカー 0）で特定する
    // （seq_nr 列の実値化により、旧実装の「WHERE seq_nr = 0」列値判別は成立しない）
    let pkey = self.resolve_pkey(aid);
    let skey = self.current_slot_skey(aid);
    let row = {
      let connection = self.lock_connection().map_err(EventStoreReadError::OtherError)?;
      connection
        .query_row(
          "SELECT payload, version, seq_nr FROM snapshot WHERE pkey = ?1 AND skey = ?2",
          params![pkey, skey],
          |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?)),
        )
        .optional()
        .map_err(map_read_error)?
    };
    match row {
      None => Ok(None),
      Some((payload, version, seq_nr)) => {
        // FR4.3 / U1 BR2.5: 読取後の set_version 補正は存在しない — version / seq_nr は列値を
        // そのまま封筒に載せて返す（列型不整合・整数域逸脱は破損データとして読取エラー）。
        // NFR2.5: 封筒の構築は SnapshotEnvelope::new の公開ビルダーのみで行う
        let version = column_usize(version, "snapshot", "version")?;
        let seq_nr = column_usize(seq_nr, "snapshot", "seq_nr")?;
        let aggregate = self.snapshot_serializer.deserialize(&payload)?;
        Ok(Some(SnapshotEnvelope::new(aggregate, seq_nr, version)))
      }
    }
  }

  async fn fetch_events_since(
    &self,
    aid: &AID,
    seq_nr: usize,
  ) -> Result<Vec<EventEnvelope<AID, P>>, EventStoreReadError> {
    // BR2.2 / AC4.1.1 / FR5.1: 全列 SELECT から封筒を再構成する（payload 単独 SELECT の廃止）。
    // 列欠落・型不整合は破損データとして読取エラーになる
    let rows: Vec<(i64, i64, String, Vec<u8>)> = {
      let connection = self.lock_connection().map_err(EventStoreReadError::OtherError)?;
      let mut statement = connection
        .prepare("SELECT seq_nr, occurred_at, manifest, payload FROM journal WHERE aid = ?1 AND seq_nr >= ?2 ORDER BY seq_nr ASC")
        .map_err(map_read_error)?;
      let rows = statement
        .query_map(params![aid.to_string(), seq_nr as i64], |row| {
          Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, Vec<u8>>(3)?,
          ))
        })
        .map_err(map_read_error)?;
      rows.collect::<Result<_, _>>().map_err(map_read_error)?
    };
    let mut events = Vec::with_capacity(rows.len());
    for (seq_nr, occurred_at, manifest, payload) in rows {
      // NFR2.5 / AC5.1.1: 封筒の構築は EventEnvelope::new + with_manifest の公開ビルダーのみで行う
      let seq_nr = column_usize(seq_nr, "journal", "seq_nr")?;
      let payload = self.event_serializer.deserialize(&payload)?;
      events
        .push(EventEnvelope::new(aid.clone(), seq_nr, parse_occurred_at(occurred_at), payload).with_manifest(manifest));
    }
    Ok(events)
  }

  async fn create_event_and_snapshot(
    &self,
    event: &EventEnvelope<AID, P>,
    aggregate: &A,
    maintenance: &SnapshotMaintenance,
  ) -> Result<(), EventStoreWriteError> {
    self.ensure_shard_count()?;
    let aid = event.aggregate_id();
    let aid_string = aid.to_string();
    let pkey = self.resolve_pkey(aid);
    let slot_skey = self.current_slot_skey(aid);
    // BR2.3 / AC2.3.3: payload は純ドメイン内容のみ（メタデータ注入つき直列化の廃止）。
    // NFR2.5: 封筒の分解は公開アクセサのみで行う
    let snapshot_payload = self.snapshot_serializer.serialize(aggregate)?;
    let event_payload = self.event_serializer.serialize(event.payload())?;
    let occurred_at = occurred_at_nanos(event.occurred_at())?;
    let last_updated_at = event.occurred_at().timestamp_millis();

    let mut connection = self.lock_connection().map_err(EventStoreWriteError::OtherError)?;
    let tx = connection.transaction().map_err(map_write_error)?;
    // BR1.1 / AC2.2.1: 現行スロット行の INSERT 一意性（主キー衝突）が作成の重複を排除する（現行維持）。
    // W1: version = 1（固定値）、seq_nr 列 = event.seq_nr()（実値 — BR2.4。旧実装のスロット位置 0 と
    // aggregate.version() の列値流用は trait 廃止と共に消滅）。衝突時の expected_version は
    // リテラル 0（U1 の新規作成規約 expected_version == 0 — BR1.1）で、実 version を追補読取して
    // 統一書式で返す（トランザクションは drop でロールバック）
    let inserted = tx.execute(
      INSERT_SNAPSHOT_SQL,
      params![
        pkey,
        slot_skey,
        aid_string,
        event.seq_nr() as i64,
        1i64,
        snapshot_payload,
        last_updated_at
      ],
    );
    if let Err(err) = inserted {
      if is_constraint_violation(&err) {
        let actual_version = read_slot_version(&tx, &pkey, &slot_skey)?;
        return Err(EventStoreWriteError::OptimisticLockError(
          format_optimistic_lock_message(&aid_string, 0, actual_version),
        ));
      }
      return Err(map_write_error(err));
    }
    execute_write_mapping_conflict(
      &tx,
      INSERT_JOURNAL_SQL,
      params![
        pkey,
        self.resolve_skey(aid, event.seq_nr()),
        aid_string,
        event.seq_nr() as i64,
        event_payload,
        occurred_at,
        event.manifest()
      ],
      &aid_string,
      // BR1.1: create パスの衝突 3 箇所すべてで expected_version はリテラル 0
      0,
    )?;
    // BR3.1: keep_snapshot_count = Some(n) のときのみ履歴行（skey / seq_nr 列 = event.seq_nr() 由来、
    // version = 1）を同一トランザクションで挿入する。Some(0) は U1 BR4.1 のビルダー拒否により
    // 到達しないため、ガードは is_some() で足りる（旧実装の count > 0 条項の単純化）
    if maintenance.keep_snapshot_count.is_some() {
      execute_write_mapping_conflict(
        &tx,
        INSERT_SNAPSHOT_SQL,
        params![
          pkey,
          self.resolve_skey(aid, event.seq_nr()),
          aid_string,
          event.seq_nr() as i64,
          1i64,
          snapshot_payload,
          last_updated_at
        ],
        &aid_string,
        0,
      )?;
    }
    tx.commit().map_err(map_write_error)
  }

  async fn update_event_and_snapshot(
    &self,
    event: &EventEnvelope<AID, P>,
    aggregate: Option<&A>,
    expected_version: usize,
    maintenance: &SnapshotMaintenance,
  ) -> Result<(), EventStoreWriteError> {
    self.ensure_shard_count()?;
    let aid = event.aggregate_id();
    let aid_string = aid.to_string();
    let pkey = self.resolve_pkey(aid);
    let slot_skey = self.current_slot_skey(aid);
    // BR2.3 / AC2.3.3: payload は純ドメイン内容のみ
    let snapshot_payload = aggregate
      .map(|aggregate| self.snapshot_serializer.serialize(aggregate))
      .transpose()?;
    let event_payload = self.event_serializer.serialize(event.payload())?;
    let occurred_at = occurred_at_nanos(event.occurred_at())?;
    let last_updated_at = event.occurred_at().timestamp_millis();

    let mut connection = self.lock_connection().map_err(EventStoreWriteError::OtherError)?;
    let tx = connection.transaction().map_err(map_write_error)?;
    // BR1.1 / NFR4.4 / AC2.2.1 / P10: 単一トランザクション内の条件付き UPDATE
    // （WHERE version = expected）が唯一の競合判定点（現行維持）。version = expected + 1
    // （明示値 — BR2.4）。スナップショット付き更新（aggregate あり）のみ payload / seq_nr 列
    // （= event.seq_nr() 実値）も更新し、イベントのみ更新では集約状態が変わらないため
    // 反映位置も不変として据え置く（BR2.4）
    let affected = match &snapshot_payload {
      Some(payload) => tx.execute(
        "UPDATE snapshot SET payload = ?1, seq_nr = ?2, version = ?3, last_updated_at = ?4 WHERE pkey = ?5 AND skey = \
         ?6 AND version = ?7",
        params![
          payload,
          event.seq_nr() as i64,
          (expected_version + 1) as i64,
          last_updated_at,
          pkey,
          slot_skey,
          expected_version as i64
        ],
      ),
      None => tx.execute(
        "UPDATE snapshot SET version = ?1, last_updated_at = ?2 WHERE pkey = ?3 AND skey = ?4 AND version = ?5",
        params![
          (expected_version + 1) as i64,
          last_updated_at,
          pkey,
          slot_skey,
          expected_version as i64
        ],
      ),
    }
    .map_err(map_write_error)?;
    if affected == 0 {
      // BR1.1: 変更行数 0 = version 不一致または不在集約。実 version を追補読取して統一書式で返す。
      // 不在集約は actual_version なし書式になる（U1 リファレンス意味論 — AC2.2.1）
      let actual_version = read_slot_version(&tx, &pkey, &slot_skey)?;
      return Err(EventStoreWriteError::OptimisticLockError(
        format_optimistic_lock_message(&aid_string, expected_version, actual_version),
      ));
    }
    execute_write_mapping_conflict(
      &tx,
      INSERT_JOURNAL_SQL,
      params![
        pkey,
        self.resolve_skey(aid, event.seq_nr()),
        aid_string,
        event.seq_nr() as i64,
        event_payload,
        occurred_at,
        event.manifest()
      ],
      &aid_string,
      expected_version,
    )?;
    // BR3.1 / BR2.4: 履歴行は keep_snapshot_count = Some(n) かつスナップショット付き更新のときのみ。
    // skey / seq_nr 列 = event.seq_nr() 由来、version = expected + 1（明示値）
    if let (Some(payload), Some(_)) = (&snapshot_payload, maintenance.keep_snapshot_count) {
      execute_write_mapping_conflict(
        &tx,
        INSERT_SNAPSHOT_SQL,
        params![
          pkey,
          self.resolve_skey(aid, event.seq_nr()),
          aid_string,
          event.seq_nr() as i64,
          (expected_version + 1) as i64,
          payload,
          last_updated_at
        ],
        &aid_string,
        expected_version,
      )?;
    }
    tx.commit().map_err(map_write_error)
  }

  async fn on_event_persisted(&self, aid: &AID, maintenance: &SnapshotMaintenance) -> Result<(), EventStoreWriteError> {
    // BR3.1 / AC2.3.4: 保持ポリシーの実行点（現行の剪定計算 + delete_ttl 併用を維持）。
    // keep_snapshot_count = None は履歴行が書かれないため何もしない。Some(0) は
    // U1 BR4.1 のビルダー拒否によりここへ到達しない（Some/None の分岐構造は現行維持）
    let keep_snapshot_count = match maintenance.keep_snapshot_count {
      Some(count) => count,
      None => return Ok(()),
    };
    let aid_string = aid.to_string();
    // BR2.4: 履歴行の判別は skey != 現行スロット skey（主キー）で行う
    // （seq_nr 列の実値化により、旧実装の「WHERE seq_nr > 0」列値判別は成立しない）
    let slot_skey = self.current_slot_skey(aid);
    let expiration_cutoff = maintenance
      .delete_ttl
      .map(|delete_ttl| (Utc::now() - delete_ttl).timestamp_millis());

    let mut connection = self.lock_connection().map_err(EventStoreWriteError::OtherError)?;
    let tx = connection.transaction().map_err(map_write_error)?;
    let history_count: i64 = tx
      .query_row(
        "SELECT COUNT(*) FROM snapshot WHERE aid = ?1 AND skey != ?2",
        params![aid_string, slot_skey],
        |row| row.get(0),
      )
      .map_err(map_write_error)?;
    let excess_count = history_count - keep_snapshot_count as i64;
    if excess_count > 0 {
      tx.execute(
        "DELETE FROM snapshot WHERE rowid IN (SELECT rowid FROM snapshot WHERE aid = ?1 AND skey != ?2 ORDER BY \
         seq_nr ASC LIMIT ?3)",
        params![aid_string, slot_skey, excess_count],
      )
      .map_err(map_write_error)?;
    }
    if let Some(cutoff) = expiration_cutoff {
      tx.execute(
        "DELETE FROM snapshot WHERE aid = ?1 AND skey != ?2 AND last_updated_at < ?3",
        params![aid_string, slot_skey, cutoff],
      )
      .map_err(map_write_error)?;
    }
    tx.commit().map_err(map_write_error)
  }
}

// FR1.4 / U1 BR1.3 / C3: occurred_at はドメイン供給値のまま保存・読出しする。旧実装の epoch millis
// 格納ではサブミリ秒精度が失われ供給値の完全往復（共有シナリオの occurred_at 同値 assert）を
// 満たせないため、INTEGER 列へ epoch nanos（i64）で格納する（U2 dynamodb と同型の精度是正）。
// i64 nanos の表現範囲外（およそ西暦 1677〜2262 年の外）は黙って切り詰めずエラーで拒否する（fail fast）
fn occurred_at_nanos(occurred_at: &DateTime<Utc>) -> Result<i64, EventStoreWriteError> {
  occurred_at
    .timestamp_nanos_opt()
    .ok_or_else(|| EventStoreWriteError::OtherError("occurred_at is out of the epoch-nanos range".to_string()))
}

fn parse_occurred_at(nanos: i64) -> DateTime<Utc> {
  DateTime::from_timestamp_nanos(nanos)
}

// BR2.2: 整数列の負値は破損データとして読取エラーにする
// （メッセージはテーブル種別と列名のみで、値・DBファイルパスは展開しない — NFR3.8）
fn column_usize(value: i64, table: &'static str, column: &'static str) -> Result<usize, EventStoreReadError> {
  usize::try_from(value)
    .map_err(|_| EventStoreReadError::OtherError(format!("{} row column has an out-of-range value: {}", table, column)))
}

/// 現行スロット行の実versionを読み取る（不存在は `None` — actual_version なし書式の源）。
fn read_slot_version(tx: &Transaction<'_>, pkey: &str, skey: &str) -> Result<Option<usize>, EventStoreWriteError> {
  tx.query_row(
    "SELECT version FROM snapshot WHERE pkey = ?1 AND skey = ?2",
    params![pkey, skey],
    |row| row.get::<_, i64>(0),
  )
  .optional()
  .map(|version| version.map(|version| version as usize))
  .map_err(map_write_error)
}

/// トランザクション内の書き込みを実行し、一意性違反は楽観的ロック競合として写像する
/// （DynamoDBのTransactWriteItemsキャンセル→OptimisticLockError写像と対称 — BR1.1）。
fn execute_write_mapping_conflict(
  tx: &Transaction<'_>,
  sql: &str,
  params: &[&dyn ToSql],
  aid: &str,
  expected_version: usize,
) -> Result<(), EventStoreWriteError> {
  match tx.execute(sql, params) {
    Ok(_) => Ok(()),
    Err(err) if is_constraint_violation(&err) => Err(EventStoreWriteError::OptimisticLockError(
      format_optimistic_lock_message(aid, expected_version, None),
    )),
    Err(err) => Err(map_write_error(err)),
  }
}

fn is_constraint_violation(err: &rusqlite::Error) -> bool {
  matches!(err, rusqlite::Error::SqliteFailure(e, _) if e.code == ErrorCode::ConstraintViolation)
}

// BR4.1 / NFR3.8 / P12: rusqliteエラーの中立写像。SQLiteエンジン起因の失敗（SQLITE_BUSY・
// SQLITE_CANTOPEN 等）はIOError、それ以外（型変換等のAPI利用起因）はOtherError。
// SQLITE_BUSY は即時 IOError で返し、リトライ・PRAGMA 調整を持ち込まない（NFR3.9 —
// project.md Decided）。楽観的ロック競合は呼び出し箇所で is_constraint_violation により
// 先行判定するため、本写像には到達しない。生エラー詳細はソースエラー側にのみ保持し、
// 書式化メッセージへ DB ファイルパス以外の生詳細を展開しない（メッセージ衛生）。
fn map_write_error(err: rusqlite::Error) -> EventStoreWriteError {
  match err {
    rusqlite::Error::SqliteFailure(_, _) => EventStoreWriteError::IOError(Box::new(err)),
    _ => EventStoreWriteError::OtherError(err.to_string()),
  }
}

fn map_read_error(err: rusqlite::Error) -> EventStoreReadError {
  match err {
    rusqlite::Error::SqliteFailure(_, _) => EventStoreReadError::IOError(Box::new(err)),
    _ => EventStoreReadError::OtherError(err.to_string()),
  }
}
