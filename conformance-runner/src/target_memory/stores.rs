//! メモリの場面が使う複数のストアと、ペイロードの実体の受け渡し層。
//!
//! `createStore` で場面の途中に 2 つ目のストアを作り、各操作の `store` が指すストアで実行する（MEM-2）。
//! `mutateWrittenPayload`・`mutateReadPayload` は、書き込みに渡した実体と取得した結果の実体を、
//! 実行器が手元に持つ同じ実体のまま書き換える（MEM-6）。

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use event_store_adapter_rs::{
  error::EventStoreError,
  event_envelope::{EventEnvelope, SnapshotEnvelope, SnapshotRead},
  event_store::EventStore,
  memory::{EventStoreForMemory, MemoryStorage, MemoryTestHooks},
  retention::RetentionSettings,
  seq_nr::SeqNr,
  serializer::{EventSerializer, SnapshotSerializer},
};
use serde_json::Value;

use crate::case::{fixture, id, required, seq, string, CaseId};

/// 1 つ目のストアの予約名を表す。`store` を省略した操作はこのストアに対する操作になる。
pub(super) const PRIMARY: &str = "primary";

/// 実行器が手元に持つペイロードの実体を表す。
///
/// ライブラリへ渡した後も、同じ実体を書き換えられる。複製は同じ実体を指す。
#[derive(Debug, Clone)]
pub(super) struct PayloadHandle(Arc<Mutex<Value>>);

impl PayloadHandle {
  /// 値を実体として包む。
  pub(super) fn new(value: Value) -> Self {
    Self(Arc::new(Mutex::new(value)))
  }

  /// 実体の今の値の複製を返す。
  pub(super) fn value(&self) -> Value {
    self.0.lock().expect("ペイロードのロック").clone()
  }

  /// JSON Pointer（RFC 6901）が指す位置を `value` へ置き換える。
  ///
  /// 空のポインタと、実体にない位置は誤りとして返す。キーは作らない。
  pub(super) fn replace(&self, pointer: &str, value: &Value) -> Result<(), String> {
    if pointer.is_empty() {
      return Err("空のポインタは書き換えの位置に使えない".to_owned());
    }
    let mut payload = self.0.lock().expect("ペイロードのロック");
    let slot = payload
      .pointer_mut(pointer)
      .ok_or_else(|| format!("ポインタ {pointer} が指す位置がペイロードにない"))?;
    *slot = value.clone();
    Ok(())
  }
}

/// ストアの保存先と、保存先を開くストアの作り方を表す。
///
/// 本番の実装は [`LibraryStores`] だけで、試験が偽の保存先・ストアを差し込むための口になる。
pub(super) trait StoreFactory {
  /// 保存先を開いたストアの型。
  type Store: EventStore<AID = CaseId, A = Value, P = PayloadHandle>;

  /// 設定とフックを持つ、空の保存先を新しく作る。
  fn create_storage(
    &self,
    settings: RetentionSettings,
    hooks: Arc<dyn MemoryTestHooks>,
  ) -> Result<MemoryStorage, EventStoreError>;

  /// 保存先を、与えた直列化でストアとして開く。
  fn open(
    &self,
    storage: MemoryStorage,
    events: Arc<dyn EventSerializer<PayloadHandle>>,
    snapshots: Arc<dyn SnapshotSerializer<Value>>,
  ) -> Self::Store;

  /// `storage` と記録・設定・排他制御を共有する保存先を返す。既定は、公開 API の `MemoryStorage::clone` で共有する。
  fn share_storage(&self, storage: &MemoryStorage) -> MemoryStorage {
    storage.clone()
  }
}

/// ライブラリの公開 API でストアを作る。
#[derive(Debug)]
pub(super) struct LibraryStores;

impl StoreFactory for LibraryStores {
  type Store = EventStoreForMemory<CaseId, Value, PayloadHandle>;

  fn create_storage(
    &self,
    settings: RetentionSettings,
    hooks: Arc<dyn MemoryTestHooks>,
  ) -> Result<MemoryStorage, EventStoreError> {
    MemoryStorage::new_with_hooks(settings, hooks)
  }

  fn open(
    &self,
    storage: MemoryStorage,
    events: Arc<dyn EventSerializer<PayloadHandle>>,
    snapshots: Arc<dyn SnapshotSerializer<Value>>,
  ) -> Self::Store {
    EventStoreForMemory::with_serializers(storage, events, snapshots)
  }
}

/// 取得した結果の 1 件の、集約 ID・通し番号・ペイロードの実体を表す。
#[derive(Debug, Clone)]
struct ReadPayload {
  aggregate_id: CaseId,
  seq_nr: SeqNr,
  payload: PayloadHandle,
}

/// 操作番号ごとに、成功した書き込みと取得のペイロードの実体を持つ。
#[derive(Debug, Default)]
struct PayloadLog {
  operation: u32,
  written: BTreeMap<u32, PayloadHandle>,
  read: BTreeMap<u32, Vec<ReadPayload>>,
}

impl PayloadLog {
  /// これから実行する操作の番号を設定する。
  fn begin(&mut self, operation: u32) {
    self.operation = operation;
  }

  /// 今の操作が書き込みに渡したペイロードの実体を記録する。成功した書き込みだけを渡す。
  fn record_written(&mut self, payload: PayloadHandle) {
    self.written.insert(self.operation, payload);
  }

  /// 今の操作が取得した結果の実体を記録する。成功した取得だけを渡す。
  fn record_read(&mut self, events: Vec<ReadPayload>) {
    self.read.insert(self.operation, events);
  }

  /// `arguments.operation` が指す、それより前の成功した書き込みのペイロードの実体を書き換える。
  fn mutate_written(&self, arguments: &Value) -> Result<(), String> {
    let operation = operation_of(arguments)?;
    let payload = self
      .written
      .get(&operation)
      .ok_or_else(|| format!("操作 {operation} は、それより前の成功した書き込みではない"))?;
    payload.replace(string(required(arguments, "pointer")?)?, required(arguments, "value")?)
  }

  /// `arguments.operation` が指す、それより前の取得結果のうち `arguments.event` のイベントのペイロードの実体を書き換える。
  fn mutate_read(&self, body: &Value, arguments: &Value) -> Result<(), String> {
    let operation = operation_of(arguments)?;
    let event = fixture(body, "events", required(arguments, "event")?)?;
    let aggregate_id = id(required(event, "aggregate_id")?)?;
    let seq_nr = seq(required(event, "seq_nr")?)?;
    let read = self
      .read
      .get(&operation)
      .ok_or_else(|| format!("操作 {operation} は、それより前の成功した取得ではない"))?
      .iter()
      .find(|read| {
        read.seq_nr == seq_nr
          && read.aggregate_id.type_name == aggregate_id.type_name
          && read.aggregate_id.value == aggregate_id.value
      })
      .ok_or_else(|| format!("操作 {operation} の取得結果に、指定のイベントがない"))?;
    read
      .payload
      .replace(string(required(arguments, "pointer")?)?, required(arguments, "value")?)
  }
}

fn operation_of(arguments: &Value) -> Result<u32, String> {
  u32::try_from(seq(required(arguments, "operation")?)?).map_err(|e| e.to_string())
}

/// 封筒を、ペイロードの実体を指す封筒へ包み直す。実体のもう一方の参照も返す。
fn hold_payload(event: EventEnvelope<CaseId, Value>) -> (EventEnvelope<CaseId, PayloadHandle>, PayloadHandle) {
  let aggregate_id = event.aggregate_id().clone();
  let seq_nr = event.seq_nr();
  let occurred_at = *event.occurred_at();
  let manifest = event.manifest().to_owned();
  let handle = PayloadHandle::new(event.into_payload());
  (
    EventEnvelope::new(aggregate_id, seq_nr, occurred_at, handle.clone()).with_manifest(manifest),
    handle,
  )
}

/// ペイロードを `Value` で受け渡すストアとして振る舞い、内側のストアにはペイロードの実体を渡す。
///
/// 書き込みに渡した実体と取得した結果の実体を、操作番号ごとに記録する。比較の処理（`case::step`）は
/// `Value` のまま使える。
#[derive(Debug, Clone)]
pub(super) struct TrackedStore<S> {
  inner: S,
  log: Arc<Mutex<PayloadLog>>,
}

#[async_trait]
impl<S: EventStore<AID = CaseId, A = Value, P = PayloadHandle>> EventStore for TrackedStore<S> {
  type A = Value;
  type AID = CaseId;
  type P = Value;

  async fn persist_event(&self, event: EventEnvelope<CaseId, Value>) -> Result<(), EventStoreError> {
    let (event, payload) = hold_payload(event);
    self.inner.persist_event(event).await?;
    self
      .log
      .lock()
      .expect("ペイロードの記録のロック")
      .record_written(payload);
    Ok(())
  }

  async fn persist_event_and_snapshot(
    &self,
    event: EventEnvelope<CaseId, Value>,
    snapshot: SnapshotEnvelope<Value>,
  ) -> Result<(), EventStoreError> {
    let (event, payload) = hold_payload(event);
    self.inner.persist_event_and_snapshot(event, snapshot).await?;
    self
      .log
      .lock()
      .expect("ペイロードの記録のロック")
      .record_written(payload);
    Ok(())
  }

  async fn get_latest_snapshot_by_id(&self, aid: &CaseId) -> Result<Option<SnapshotRead<Value>>, EventStoreError> {
    self.inner.get_latest_snapshot_by_id(aid).await
  }

  async fn get_events_by_id_since_seq_nr(
    &self,
    aid: &CaseId,
    seq_nr: SeqNr,
  ) -> Result<Vec<EventEnvelope<CaseId, Value>>, EventStoreError> {
    let events = self.inner.get_events_by_id_since_seq_nr(aid, seq_nr).await?;
    let read = events
      .iter()
      .map(|event| ReadPayload {
        aggregate_id: event.aggregate_id().clone(),
        seq_nr: event.seq_nr(),
        payload: event.payload().clone(),
      })
      .collect();
    self.log.lock().expect("ペイロードの記録のロック").record_read(read);
    Ok(
      events
        .into_iter()
        .map(|event| {
          EventEnvelope::new(
            event.aggregate_id().clone(),
            event.seq_nr(),
            *event.occurred_at(),
            event.payload().value(),
          )
          .with_manifest(event.manifest())
        })
        .collect(),
    )
  }
}

/// 参照名で引く 1 つのストアを表す。観測のために、開いた保存先も持つ。
pub(super) struct NamedStore<S> {
  pub(super) storage: MemoryStorage,
  pub(super) store: TrackedStore<S>,
}

/// 場面の中の、参照名から引けるストアの表を表す。
pub(super) struct Stores<'f, F: StoreFactory> {
  factory: &'f F,
  settings: RetentionSettings,
  hooks: Arc<dyn MemoryTestHooks>,
  events: Arc<dyn EventSerializer<PayloadHandle>>,
  snapshots: Arc<dyn SnapshotSerializer<Value>>,
  log: Arc<Mutex<PayloadLog>>,
  named: BTreeMap<String, NamedStore<F::Store>>,
}

impl<'f, F: StoreFactory> Stores<'f, F> {
  /// 1 つ目の保存先を予約名 [`PRIMARY`] のストアとして開き、表を作る。
  pub(super) fn new(
    factory: &'f F,
    settings: RetentionSettings,
    hooks: Arc<dyn MemoryTestHooks>,
    events: Arc<dyn EventSerializer<PayloadHandle>>,
    snapshots: Arc<dyn SnapshotSerializer<Value>>,
    primary: MemoryStorage,
  ) -> Self {
    let mut stores = Self {
      factory,
      settings,
      hooks,
      events,
      snapshots,
      log: Arc::new(Mutex::new(PayloadLog::default())),
      named: BTreeMap::new(),
    };
    let primary = stores.open(primary);
    stores.named.insert(PRIMARY.to_owned(), primary);
    stores
  }

  fn open(&self, storage: MemoryStorage) -> NamedStore<F::Store> {
    let inner = self
      .factory
      .open(storage.clone(), self.events.clone(), self.snapshots.clone());
    NamedStore {
      storage,
      store: TrackedStore {
        inner,
        log: self.log.clone(),
      },
    }
  }

  /// 1 つ目のストアを返す。
  pub(super) fn primary(&self) -> &NamedStore<F::Store> {
    &self.named[PRIMARY]
  }

  /// 操作の `store` が指すストアを返す。省略時は 1 つ目のストア。生成前の名前は誤りとして返す。
  pub(super) fn select(&self, step: &Value) -> Result<&NamedStore<F::Store>, String> {
    let name = match step.get("store") {
      Some(name) => string(name)?,
      None => PRIMARY,
    };
    self
      .named
      .get(name)
      .ok_or_else(|| format!("ストア {name} はまだ生成していない"))
  }

  /// `createStore` を実行する。
  ///
  /// データの誤り（予約名・使用済みの名前、生成前の `shared_with`、未知の `storage`）は外側の `Err`、
  /// 保存先の生成の失敗は内側の `Err` で返す。
  pub(super) fn create(&mut self, arguments: &Value) -> Result<Result<(), EventStoreError>, String> {
    let name = string(required(arguments, "name")?)?.to_owned();
    if self.named.contains_key(&name) {
      return Err(format!("ストア名 {name} は予約名か、生成済みの名前である"));
    }
    let storage = match string(required(arguments, "storage")?)? {
      "new" => match self.factory.create_storage(self.settings.clone(), self.hooks.clone()) {
        Ok(storage) => storage,
        Err(error) => return Ok(Err(error)),
      },
      "shared" => {
        let shared_with = string(required(arguments, "shared_with")?)?;
        let shared = &self
          .named
          .get(shared_with)
          .ok_or_else(|| format!("shared_with のストア {shared_with} はまだ生成していない"))?
          .storage;
        // 設定は共有する保存先が持つ。`self.settings` を渡して新しく作らない（MEM-D11）。
        self.factory.share_storage(shared)
      }
      other => return Err(format!("未知の storage: {other}")),
    };
    let created = self.open(storage);
    self.named.insert(name, created);
    Ok(Ok(()))
  }

  /// これから実行する操作の番号を、ペイロードの記録へ伝える。
  pub(super) fn begin(&self, operation: u32) {
    self.log.lock().expect("ペイロードの記録のロック").begin(operation);
  }

  /// `mutateWrittenPayload` を実行する。
  pub(super) fn mutate_written(&self, arguments: &Value) -> Result<(), String> {
    self
      .log
      .lock()
      .expect("ペイロードの記録のロック")
      .mutate_written(arguments)
  }

  /// `mutateReadPayload` を実行する。
  pub(super) fn mutate_read(&self, body: &Value, arguments: &Value) -> Result<(), String> {
    self
      .log
      .lock()
      .expect("ペイロードの記録のロック")
      .mutate_read(body, arguments)
  }
}
