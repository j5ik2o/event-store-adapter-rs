use std::fmt::Debug;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use chrono::{Duration, Utc};
use rusqlite::{params, Connection, ErrorCode, OptionalExtension, ToSql, Transaction};

use crate::event_store_backend::{SnapshotEnvelope, SnapshotMaintenance, StorageBackend};
use crate::generic_event_store::GenericEventStore;
use crate::key_resolver::{DefaultKeyResolver, KeyResolver};
use crate::serializer::{EventSerializer, JsonEventSerializer, JsonSnapshotSerializer, SnapshotSerializer};
use crate::types::{
  format_optimistic_lock_message, Aggregate, AggregateId, Event, EventStore, EventStoreReadError, EventStoreWriteError,
};

/// 既存バックエンドの利用実績と同じ既定シャード数（pkey書き込み分散幅）
const DEFAULT_SHARD_COUNT: u64 = 64;

/// スキーマは接続確立時に不存在なら作成する（冪等 — BR2.1。利用者DDLなし）。
/// 書き込みアドレスはPK (pkey, skey)、読み込みは (aid, seq_nr) 索引（BR2.2）。
const CREATE_SCHEMA_SQL: &str = "\
CREATE TABLE IF NOT EXISTS journal (
  pkey TEXT NOT NULL,
  skey TEXT NOT NULL,
  aid TEXT NOT NULL,
  seq_nr INTEGER NOT NULL,
  payload BLOB NOT NULL,
  occurred_at INTEGER NOT NULL,
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

const INSERT_JOURNAL_SQL: &str =
  "INSERT INTO journal (pkey, skey, aid, seq_nr, payload, occurred_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)";

/// Event Store for SQLite.
///
/// Persists events and snapshots to a local SQLite database (a file or `:memory:`) and
/// creates the required tables and indexes on construction. The supported sharing unit is
/// one store instance and its clones (which share the underlying connection); opening the
/// same database file from multiple store instances or processes is out of scope.
#[derive(Debug)]
pub struct EventStoreForSqlite<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>, {
  inner: GenericEventStore<AID, A, E, SqliteBackend<AID, A, E>>,
}

impl<AID, A, E> EventStoreForSqlite<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>,
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

  /// Sets the number of snapshots to keep and returns the updated store.
  pub fn with_keep_snapshot_count(mut self, keep_snapshot_count: Option<usize>) -> Self {
    self.inner = self.inner.with_keep_snapshot_count(keep_snapshot_count);
    self
  }

  /// Sets the retention period for historical snapshots and returns the updated store.
  pub fn with_delete_ttl(mut self, delete_ttl: Option<Duration>) -> Self {
    self.inner = self.inner.with_delete_ttl(delete_ttl);
    self
  }

  /// Sets the shard count used to resolve partition keys and returns the updated store.
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
  pub fn with_event_serializer(mut self, serializer: Arc<dyn EventSerializer<E>>) -> Self {
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

impl<AID, A, E> Clone for EventStoreForSqlite<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>,
{
  // クローン間で基底接続を共有する（BR2.5 — `:memory:` のインスタンス単位共有と
  // 決定的競合テストの前提。Clone時の状態分岐を作らない）
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}

#[async_trait]
impl<AID, A, E> EventStore for EventStoreForSqlite<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>,
{
  type AG = A;
  type AID = AID;
  type EV = E;

  async fn persist_event(&mut self, event: &Self::EV, version: usize) -> Result<(), EventStoreWriteError> {
    self.inner.persist_event(event, version).await
  }

  async fn persist_event_and_snapshot(
    &mut self,
    event: &Self::EV,
    aggregate: &Self::AG,
  ) -> Result<(), EventStoreWriteError> {
    self.inner.persist_event_and_snapshot(event, aggregate).await
  }

  async fn get_latest_snapshot_by_id(&self, aid: &Self::AID) -> Result<Option<Self::AG>, EventStoreReadError> {
    self.inner.get_latest_snapshot_by_id(aid).await
  }

  async fn get_events_by_id_since_seq_nr(
    &self,
    aid: &Self::AID,
    seq_nr: usize,
  ) -> Result<Vec<Self::EV>, EventStoreReadError> {
    self.inner.get_events_by_id_since_seq_nr(aid, seq_nr).await
  }
}

// fnポインタ経由の型マーカー — Send/Sync自動導出を阻害しない
type TypeMarker<AID, A, E> = fn() -> (AID, A, E);

/// SQLiteバックエンドの内部状態。基底接続は排他制御付きで共有し、
/// ロック型・ガード・Arcは公開シグネチャへ露出しない（隠蔽境界 — BR2.6）
#[derive(Debug)]
struct SqliteBackend<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>, {
  connection: Arc<Mutex<Connection>>,
  shard_count: u64,
  key_resolver: Arc<dyn KeyResolver<ID = AID>>,
  event_serializer: Arc<dyn EventSerializer<E>>,
  snapshot_serializer: Arc<dyn SnapshotSerializer<A>>,
  _marker: PhantomData<TypeMarker<AID, A, E>>,
}

impl<AID, A, E> SqliteBackend<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>,
{
  fn new(connection: Connection) -> Self {
    Self {
      connection: Arc::new(Mutex::new(connection)),
      shard_count: DEFAULT_SHARD_COUNT,
      key_resolver: Arc::new(DefaultKeyResolver::default()),
      event_serializer: Arc::new(JsonEventSerializer::default()),
      snapshot_serializer: Arc::new(JsonSnapshotSerializer::default()),
      _marker: PhantomData,
    }
  }

  fn set_shard_count(&mut self, shard_count: u64) {
    self.shard_count = shard_count;
  }

  fn set_key_resolver(&mut self, key_resolver: Arc<dyn KeyResolver<ID = AID>>) {
    self.key_resolver = key_resolver;
  }

  fn set_event_serializer(&mut self, serializer: Arc<dyn EventSerializer<E>>) {
    self.event_serializer = serializer;
  }

  fn set_snapshot_serializer(&mut self, serializer: Arc<dyn SnapshotSerializer<A>>) {
    self.snapshot_serializer = serializer;
  }

  fn resolve_pkey(&self, id: &AID) -> String {
    self.key_resolver.resolve_partition_key(id, self.shard_count)
  }

  fn resolve_skey(&self, id: &AID, seq_nr: usize) -> String {
    self.key_resolver.resolve_sort_key(id, seq_nr)
  }

  // ロックは各メソッド内で取得・解放し、ガード保持中に `.await` しない（BR2.6）。
  // ポイズニングはpanicさせず文字列化してエラーへ写像する（ガード起因の機密混入なし）。
  fn lock_connection(&self) -> Result<MutexGuard<'_, Connection>, String> {
    self
      .connection
      .lock()
      .map_err(|_| "sqlite connection mutex is poisoned".to_string())
  }
}

impl<AID, A, E> Clone for SqliteBackend<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>,
{
  // Arcの共有クローン — 基底接続を共有する（BR2.5）
  fn clone(&self) -> Self {
    Self {
      connection: Arc::clone(&self.connection),
      shard_count: self.shard_count,
      key_resolver: Arc::clone(&self.key_resolver),
      event_serializer: Arc::clone(&self.event_serializer),
      snapshot_serializer: Arc::clone(&self.snapshot_serializer),
      _marker: PhantomData,
    }
  }
}

#[async_trait]
impl<AID, A, E> StorageBackend<AID, A, E> for SqliteBackend<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>,
{
  async fn fetch_latest_snapshot(&self, aid: &AID) -> Result<Option<SnapshotEnvelope<A>>, EventStoreReadError> {
    // 現行スロット行（seq_nr=0）を読み込み索引 (aid, seq_nr) で取得する（BR2.2）
    let row = {
      let connection = self.lock_connection().map_err(EventStoreReadError::OtherError)?;
      connection
        .query_row(
          "SELECT payload, version, seq_nr FROM snapshot WHERE aid = ?1 AND seq_nr = 0 LIMIT 1",
          params![aid.to_string()],
          |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?)),
        )
        .optional()
        .map_err(map_read_error)?
    };
    match row {
      None => Ok(None),
      Some((payload, version, seq_nr)) => {
        let version = version as usize;
        let mut aggregate = *self.snapshot_serializer.deserialize(&payload)?;
        aggregate.set_version(version);
        Ok(Some(SnapshotEnvelope {
          aggregate,
          seq_nr: seq_nr as usize,
          version,
        }))
      }
    }
  }

  async fn fetch_events_since(&self, aid: &AID, seq_nr: usize) -> Result<Vec<E>, EventStoreReadError> {
    let payloads: Vec<Vec<u8>> = {
      let connection = self.lock_connection().map_err(EventStoreReadError::OtherError)?;
      let mut statement = connection
        .prepare("SELECT payload FROM journal WHERE aid = ?1 AND seq_nr >= ?2 ORDER BY seq_nr ASC")
        .map_err(map_read_error)?;
      let rows = statement
        .query_map(params![aid.to_string(), seq_nr as i64], |row| row.get::<_, Vec<u8>>(0))
        .map_err(map_read_error)?;
      rows.collect::<Result<_, _>>().map_err(map_read_error)?
    };
    let mut events = Vec::with_capacity(payloads.len());
    for payload in payloads {
      events.push(*self.event_serializer.deserialize(&payload)?);
    }
    Ok(events)
  }

  async fn create_event_and_snapshot(
    &self,
    event: &E,
    aggregate: &A,
    maintenance: &SnapshotMaintenance,
  ) -> Result<(), EventStoreWriteError> {
    let aid = event.aggregate_id();
    let aid_string = aid.to_string();
    let pkey = self.resolve_pkey(aid);
    let slot_skey = self.resolve_skey(aid, 0);
    let snapshot_payload = self.snapshot_serializer.serialize(aggregate)?;
    let event_payload = self.event_serializer.serialize(event)?;
    let occurred_at = event.occurred_at().timestamp_millis();
    let expected_version = aggregate.version();

    let mut connection = self.lock_connection().map_err(EventStoreWriteError::OtherError)?;
    let tx = connection.transaction().map_err(map_write_error)?;
    // 現行スロット（seq_nr=0）への挿入一意性が作成の重複を排除する（BR2.3）。
    // 一意性違反時は実versionを読み取りBR1.2書式で返す（トランザクションはdropでロールバック）
    let inserted = tx.execute(
      INSERT_SNAPSHOT_SQL,
      params![
        pkey,
        slot_skey,
        aid_string,
        0i64,
        expected_version as i64,
        snapshot_payload,
        occurred_at
      ],
    );
    if let Err(err) = inserted {
      if is_constraint_violation(&err) {
        let actual_version = read_slot_version(&tx, &pkey, &slot_skey)?;
        return Err(EventStoreWriteError::OptimisticLockError(
          format_optimistic_lock_message(&aid_string, expected_version, actual_version),
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
        occurred_at
      ],
      &aid_string,
      expected_version,
    )?;
    // 保持設定時は履歴スナップショット行（skey=実seq_nr）も同一トランザクションで挿入する
    if maintenance.keep_snapshot_count.is_some() {
      execute_write_mapping_conflict(
        &tx,
        INSERT_SNAPSHOT_SQL,
        params![
          pkey,
          self.resolve_skey(aid, aggregate.seq_nr()),
          aid_string,
          aggregate.seq_nr() as i64,
          expected_version as i64,
          snapshot_payload,
          occurred_at
        ],
        &aid_string,
        expected_version,
      )?;
    }
    tx.commit().map_err(map_write_error)
  }

  async fn update_event_and_snapshot(
    &self,
    event: &E,
    aggregate: Option<&A>,
    expected_version: usize,
    maintenance: &SnapshotMaintenance,
  ) -> Result<(), EventStoreWriteError> {
    let aid = event.aggregate_id();
    let aid_string = aid.to_string();
    let pkey = self.resolve_pkey(aid);
    let slot_skey = self.resolve_skey(aid, 0);
    let snapshot_payload = aggregate
      .map(|aggregate| self.snapshot_serializer.serialize(aggregate))
      .transpose()?;
    let event_payload = self.event_serializer.serialize(event)?;
    let occurred_at = event.occurred_at().timestamp_millis();

    let mut connection = self.lock_connection().map_err(EventStoreWriteError::OtherError)?;
    let tx = connection.transaction().map_err(map_write_error)?;
    // 単一トランザクション内のCAS: version=expected の条件付き更新（BR2.4）。
    // スロット0行の seq_nr 列は 0 のまま維持する（DynamoDB参照実装と対称）
    let affected = match (aggregate, &snapshot_payload) {
      (Some(_), Some(payload)) => tx.execute(
        "UPDATE snapshot SET payload = ?1, version = ?2, last_updated_at = ?3 WHERE pkey = ?4 AND skey = ?5 AND \
         version = ?6",
        params![
          payload,
          (expected_version + 1) as i64,
          occurred_at,
          pkey,
          slot_skey,
          expected_version as i64
        ],
      ),
      _ => tx.execute(
        "UPDATE snapshot SET version = ?1, last_updated_at = ?2 WHERE pkey = ?3 AND skey = ?4 AND version = ?5",
        params![
          (expected_version + 1) as i64,
          occurred_at,
          pkey,
          slot_skey,
          expected_version as i64
        ],
      ),
    }
    .map_err(map_write_error)?;
    if affected == 0 {
      // 影響行0 = バージョン不一致（または未作成）。実versionを読み取りBR1.2書式で返す
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
        occurred_at
      ],
      &aid_string,
      expected_version,
    )?;
    if let (Some(aggregate), Some(payload), Some(_)) = (aggregate, &snapshot_payload, maintenance.keep_snapshot_count) {
      execute_write_mapping_conflict(
        &tx,
        INSERT_SNAPSHOT_SQL,
        params![
          pkey,
          self.resolve_skey(aid, aggregate.seq_nr()),
          aid_string,
          aggregate.seq_nr() as i64,
          aggregate.version() as i64,
          payload,
          occurred_at
        ],
        &aid_string,
        expected_version,
      )?;
    }
    tx.commit().map_err(map_write_error)
  }

  async fn on_event_persisted(&self, aid: &AID, maintenance: &SnapshotMaintenance) -> Result<(), EventStoreWriteError> {
    // 保持ポリシーの実行点（BR2.7）。keep_snapshot_count 未設定なら何もしない
    let keep_snapshot_count = match maintenance.keep_snapshot_count {
      Some(count) if count > 0 => count,
      _ => return Ok(()),
    };
    let aid_string = aid.to_string();
    let expiration_cutoff = maintenance
      .delete_ttl
      .map(|delete_ttl| (Utc::now() - delete_ttl).timestamp_millis());

    let mut connection = self.lock_connection().map_err(EventStoreWriteError::OtherError)?;
    let tx = connection.transaction().map_err(map_write_error)?;
    // 削除対象は履歴行（seq_nr > 0）のみ。現行スロット行（seq_nr = 0）は対象外
    let history_count: i64 = tx
      .query_row(
        "SELECT COUNT(*) FROM snapshot WHERE aid = ?1 AND seq_nr > 0",
        params![aid_string],
        |row| row.get(0),
      )
      .map_err(map_write_error)?;
    let excess_count = history_count - keep_snapshot_count as i64;
    if excess_count > 0 {
      tx.execute(
        "DELETE FROM snapshot WHERE rowid IN (SELECT rowid FROM snapshot WHERE aid = ?1 AND seq_nr > 0 ORDER BY \
         seq_nr ASC LIMIT ?2)",
        params![aid_string, excess_count],
      )
      .map_err(map_write_error)?;
    }
    if let Some(cutoff) = expiration_cutoff {
      tx.execute(
        "DELETE FROM snapshot WHERE aid = ?1 AND seq_nr > 0 AND last_updated_at < ?2",
        params![aid_string, cutoff],
      )
      .map_err(map_write_error)?;
    }
    tx.commit().map_err(map_write_error)
  }
}

/// 現行スロット行の実versionを読み取る（不存在は `None`）。
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
/// （DynamoDBのTransactWriteItemsキャンセル→OptimisticLockError写像と対称 — BR2.3/BR2.4）。
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

// rusqliteエラーの中立写像（BR2.8の決定表）: SQLiteエンジン起因の失敗（SQLITE_BUSY・
// SQLITE_CANTOPEN 等）はIOError、それ以外（型変換等のAPI利用起因）はOtherError。
// 楽観的ロック競合は呼び出し箇所で is_constraint_violation により先行判定するため、
// 本写像には到達しない。生エラー詳細はソースエラー側にのみ保持する（メッセージ衛生）。
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
