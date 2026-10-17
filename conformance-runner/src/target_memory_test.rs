use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use event_store_adapter_rs::{
  error::EventStoreError,
  event_envelope::{EventEnvelope, SnapshotEnvelope, SnapshotRead},
  event_store::EventStore,
  memory::{MemoryStorage, MemoryTestHooks},
  retention::RetentionSettings,
  seq_nr::SeqNr,
  serializer::{EventSerializer, SnapshotSerializer},
};
use serde_json::{json, Value};

use super::{
  run_with,
  stores::{LibraryStores, PayloadHandle, StoreFactory},
};
use crate::{case::CaseId, data::Case, report::CaseOutcome};

/// 実データのケースを ID で取り出す。
fn real_case(id: &str) -> Case {
  crate::data::load(&Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance"))
    .expect("実データを読める")
    .cases
    .into_iter()
    .find(|case| case.id == id)
    .unwrap_or_else(|| panic!("実データにケース {id} がある"))
}

/// 準備を通したケースを、与えた保存先の作り手で場面の実行経路に通す。
fn execute<F: StoreFactory>(case: &Case, factory: &F) -> (CaseOutcome, Vec<Value>) {
  let prepared = crate::runner::prepare(case).expect("実データのケースは準備を通る");
  let mut observations = Vec::new();
  let outcome = run_with(case, prepared, &mut observations, factory);
  (outcome, observations)
}

fn observation_of(observations: &[Value], operation: u32) -> &Value {
  observations
    .iter()
    .find(|observation| observation["operation"] == operation)
    .unwrap_or_else(|| panic!("操作 {operation} の観測がある"))
}

fn same_aggregate(left: &CaseId, right: &CaseId) -> bool {
  left.type_name == right.type_name && left.value == right.value
}

/// 新しい保存先の要求に、最初に作った保存先の共有を返す偽の作り手。記録を共有してしまう実装を表す。
#[derive(Debug, Default)]
struct SharingNewStorage {
  first: Mutex<Option<MemoryStorage>>,
}

impl StoreFactory for SharingNewStorage {
  type Store = <LibraryStores as StoreFactory>::Store;

  fn create_storage(
    &self,
    settings: RetentionSettings,
    hooks: Arc<dyn MemoryTestHooks>,
  ) -> Result<MemoryStorage, EventStoreError> {
    let mut first = self.first.lock().expect("最初の保存先のロック");
    if let Some(storage) = first.as_ref() {
      return Ok(storage.clone());
    }
    let storage = LibraryStores.create_storage(settings, hooks)?;
    *first = Some(storage.clone());
    Ok(storage)
  }

  fn open(
    &self,
    storage: MemoryStorage,
    events: Arc<dyn EventSerializer<PayloadHandle>>,
    snapshots: Arc<dyn SnapshotSerializer<Value>>,
  ) -> Self::Store {
    LibraryStores.open(storage, events, snapshots)
  }
}

/// 共有して生成するストアに、保存先の共有ではなく、自分の既定の設定（履歴を残さない）を持つ別の保存先を返す偽の作り手。
/// 設定を共有せず、共有した 2 つ目のストアが自分の既定の設定で動く実装を表す。
#[derive(Debug, Default)]
struct SharedWithOwnDefaultSettings;

impl StoreFactory for SharedWithOwnDefaultSettings {
  type Store = <LibraryStores as StoreFactory>::Store;

  fn create_storage(
    &self,
    settings: RetentionSettings,
    hooks: Arc<dyn MemoryTestHooks>,
  ) -> Result<MemoryStorage, EventStoreError> {
    LibraryStores.create_storage(settings, hooks)
  }

  fn open(
    &self,
    storage: MemoryStorage,
    events: Arc<dyn EventSerializer<PayloadHandle>>,
    snapshots: Arc<dyn SnapshotSerializer<Value>>,
  ) -> Self::Store {
    LibraryStores.open(storage, events, snapshots)
  }

  fn share_storage(&self, _storage: &MemoryStorage) -> MemoryStorage {
    MemoryStorage::new(RetentionSettings::current_only()).expect("既定の設定で作れる")
  }
}

/// 受け取った封筒をそのまま積み、読み取りには積んだ封筒の同じ実体を複製せずに返す偽のストア。
#[derive(Debug, Clone, Default)]
struct ReturnsStoredPayload {
  stored: Arc<Mutex<Vec<EventEnvelope<CaseId, PayloadHandle>>>>,
}

#[async_trait]
impl EventStore for ReturnsStoredPayload {
  type A = Value;
  type AID = CaseId;
  type P = PayloadHandle;

  async fn persist_event(&self, event: EventEnvelope<CaseId, PayloadHandle>) -> Result<(), EventStoreError> {
    self.stored.lock().expect("積んだ封筒のロック").push(event);
    Ok(())
  }

  async fn persist_event_and_snapshot(
    &self,
    event: EventEnvelope<CaseId, PayloadHandle>,
    _snapshot: SnapshotEnvelope<Value>,
  ) -> Result<(), EventStoreError> {
    self.persist_event(event).await
  }

  async fn get_latest_snapshot_by_id(&self, aid: &CaseId) -> Result<Option<SnapshotRead<Value>>, EventStoreError> {
    let stored = self.stored.lock().expect("積んだ封筒のロック");
    let head = stored
      .iter()
      .filter(|event| same_aggregate(event.aggregate_id(), aid))
      .map(EventEnvelope::seq_nr)
      .max();
    Ok(head.map(|head_seq_nr| SnapshotRead::new(None, head_seq_nr)))
  }

  async fn get_events_by_id_since_seq_nr(
    &self,
    aid: &CaseId,
    seq_nr: SeqNr,
  ) -> Result<Vec<EventEnvelope<CaseId, PayloadHandle>>, EventStoreError> {
    let stored = self.stored.lock().expect("積んだ封筒のロック");
    let mut events: Vec<_> = stored
      .iter()
      .filter(|event| same_aggregate(event.aggregate_id(), aid) && event.seq_nr() >= seq_nr)
      .cloned()
      .collect();
    events.sort_by_key(EventEnvelope::seq_nr);
    Ok(events)
  }
}

/// 保存した値の実体を複製せずにそのまま返す偽のストアを作る。保存と取得で値を複製する実装を持たない。
#[derive(Debug, Default)]
struct NoCopyStores;

impl StoreFactory for NoCopyStores {
  type Store = ReturnsStoredPayload;

  fn create_storage(
    &self,
    settings: RetentionSettings,
    hooks: Arc<dyn MemoryTestHooks>,
  ) -> Result<MemoryStorage, EventStoreError> {
    LibraryStores.create_storage(settings, hooks)
  }

  fn open(
    &self,
    _storage: MemoryStorage,
    _events: Arc<dyn EventSerializer<PayloadHandle>>,
    _snapshots: Arc<dyn SnapshotSerializer<Value>>,
  ) -> Self::Store {
    ReturnsStoredPayload::default()
  }
}

/// 本物のストアに書き込みを委譲し、書き込みに渡されたペイロードの実体を記録する。
#[derive(Debug, Clone)]
struct RecordingStore {
  inner: <LibraryStores as StoreFactory>::Store,
  written: Arc<Mutex<Vec<PayloadHandle>>>,
}

#[async_trait]
impl EventStore for RecordingStore {
  type A = Value;
  type AID = CaseId;
  type P = PayloadHandle;

  async fn persist_event(&self, event: EventEnvelope<CaseId, PayloadHandle>) -> Result<(), EventStoreError> {
    self.written.lock().expect("記録のロック").push(event.payload().clone());
    self.inner.persist_event(event).await
  }

  async fn persist_event_and_snapshot(
    &self,
    event: EventEnvelope<CaseId, PayloadHandle>,
    snapshot: SnapshotEnvelope<Value>,
  ) -> Result<(), EventStoreError> {
    self.written.lock().expect("記録のロック").push(event.payload().clone());
    self.inner.persist_event_and_snapshot(event, snapshot).await
  }

  async fn get_latest_snapshot_by_id(&self, aid: &CaseId) -> Result<Option<SnapshotRead<Value>>, EventStoreError> {
    self.inner.get_latest_snapshot_by_id(aid).await
  }

  async fn get_events_by_id_since_seq_nr(
    &self,
    aid: &CaseId,
    seq_nr: SeqNr,
  ) -> Result<Vec<EventEnvelope<CaseId, PayloadHandle>>, EventStoreError> {
    self.inner.get_events_by_id_since_seq_nr(aid, seq_nr).await
  }
}

/// 本物の保存先と直列化を使い、書き込みに渡されたペイロードの実体だけを記録するストアを作る。
#[derive(Debug, Default)]
struct RecordingStores {
  written: Arc<Mutex<Vec<PayloadHandle>>>,
}

impl RecordingStores {
  /// 書き込みに渡された順のペイロードの実体の、今の値を返す。
  fn written_values(&self) -> Vec<Value> {
    self
      .written
      .lock()
      .expect("記録のロック")
      .iter()
      .map(PayloadHandle::value)
      .collect()
  }
}

impl StoreFactory for RecordingStores {
  type Store = RecordingStore;

  fn create_storage(
    &self,
    settings: RetentionSettings,
    hooks: Arc<dyn MemoryTestHooks>,
  ) -> Result<MemoryStorage, EventStoreError> {
    LibraryStores.create_storage(settings, hooks)
  }

  fn open(
    &self,
    storage: MemoryStorage,
    events: Arc<dyn EventSerializer<PayloadHandle>>,
    snapshots: Arc<dyn SnapshotSerializer<Value>>,
  ) -> Self::Store {
    RecordingStore {
      inner: LibraryStores.open(storage, events, snapshots),
      written: self.written.clone(),
    }
  }
}

#[test]
fn should_pass_the_independent_scene_and_read_nothing_from_the_second_store_with_the_library() {
  let case = real_case("memory-store-new-independent");

  let (outcome, observations) = execute(&case, &LibraryStores);

  assert_eq!(outcome, CaseOutcome::Passed { values: None });
  assert_eq!(
    observation_of(&observations, 4)["actual"],
    json!({"result":"events","events":[]}),
    "新しく生成した 2 つ目のストアは、1 つ目に書いた記録を読めない"
  );
}

#[test]
fn should_fail_the_independent_scene_when_new_storage_shares_records() {
  let case = real_case("memory-store-new-independent");

  let (outcome, _) = execute(&case, &SharingNewStorage::default());

  assert!(
    matches!(
      outcome,
      CaseOutcome::Failed {
        failed_operation: Some(3),
        ..
      }
    ),
    "記録を共有する新しい保存先は、2 つ目のストアの最初の読み取りで失敗にする: {outcome:?}"
  );
}

#[test]
fn should_pass_the_shared_configuration_scene_and_keep_only_the_latest_history_with_the_library() {
  let case = real_case("memory-store-shared-configuration");

  let (outcome, observations) = execute(&case, &LibraryStores);

  assert_eq!(outcome, CaseOutcome::Passed { values: None });
  assert_eq!(
    observation_of(&observations, 4)["observed"]["history"],
    json!({"active":[3],"marked":[]}),
    "共有した 2 つ目のストアからの追記でも、1 つ目の保持件数 1 に従い、履歴は最新の 1 件だけが残る"
  );
}

#[test]
fn should_fail_the_shared_configuration_scene_when_the_shared_store_uses_its_own_default_settings() {
  let case = real_case("memory-store-shared-configuration");

  let (outcome, observations) = execute(&case, &SharedWithOwnDefaultSettings);

  assert!(
    matches!(
      outcome,
      CaseOutcome::Failed {
        failed_operation: Some(2),
        ..
      }
    ),
    "保存先の設定を使わない 2 つ目のストアは、最初の追記の後の履歴の観測で失敗にする: {outcome:?}"
  );
  assert_eq!(
    observation_of(&observations, 2)["actual"],
    json!({"result":"success"}),
    "追記そのものは成功し、設定の違いだけが履歴に現れる"
  );
  assert_eq!(
    observation_of(&observations, 2)["observed"],
    json!({"active":[],"marked":[]}),
    "保持件数を持たない既定の設定では、履歴が残らない"
  );
}

#[test]
fn should_fail_the_written_payload_scene_when_the_store_returns_the_stored_payload_without_copying() {
  let case = real_case("memory-payload-written-mutation");

  let (outcome, _) = execute(&case, &NoCopyStores);

  assert!(
    matches!(
      outcome,
      CaseOutcome::Failed {
        failed_operation: Some(3),
        ..
      }
    ),
    "書き込んだ実体を複製せずに返す保存先は、書き換えの後の読み直しで失敗にする: {outcome:?}"
  );
}

#[test]
fn should_fail_the_read_payload_scene_when_the_store_returns_the_stored_payload_without_copying() {
  let case = real_case("memory-payload-read-mutation");

  let (outcome, _) = execute(&case, &NoCopyStores);

  assert!(
    matches!(
      outcome,
      CaseOutcome::Failed {
        failed_operation: Some(4),
        ..
      }
    ),
    "取得結果の実体を複製せずに返す保存先は、書き換えの後の読み直しで失敗にする: {outcome:?}"
  );
}

#[test]
fn should_fail_create_store_with_the_reserved_name() {
  let mut case = real_case("memory-store-shared");
  case.body["steps"][1]["arguments"]["name"] = json!("primary");

  let (outcome, _) = execute(&case, &LibraryStores);

  assert!(
    matches!(
      outcome,
      CaseOutcome::Failed {
        failed_operation: Some(2),
        ..
      }
    ),
    "予約名 primary の生成は、その操作で失敗にする: {outcome:?}"
  );
}

#[test]
fn should_fail_a_step_on_a_store_that_was_not_created() {
  let mut case = real_case("memory-store-new-independent");
  // 1 つ目のストアの結果を期待値にして、primary へ読み替えたら成功になる操作にする。
  case.body["steps"][2]["store"] = json!("third");
  case.body["steps"][2]["expect"] = json!({"result":"snapshot","head_seq_nr":1,"snapshot":null});

  let (outcome, _) = execute(&case, &LibraryStores);

  assert!(
    matches!(
      outcome,
      CaseOutcome::Failed {
        failed_operation: Some(3),
        ..
      }
    ),
    "生成していないストア名の操作は、primary へ読み替えずに失敗にする: {outcome:?}"
  );
}

#[test]
fn should_mutate_the_written_payload_held_by_the_runner_and_keep_the_stored_value() {
  let case = real_case("memory-payload-written-mutation");
  let factory = RecordingStores::default();

  let (outcome, _) = execute(&case, &factory);

  assert_eq!(
    outcome,
    CaseOutcome::Passed { values: None },
    "書き換えの後に読み直した値は、書き込んだときの値のまま"
  );
  assert_eq!(
    factory.written_values(),
    vec![json!({"items":[{"qty":99}],"note":"original"})],
    "手元の実体は指定の位置だけが書き換わる"
  );
}

#[test]
fn should_replace_the_key_decoded_from_an_escaped_pointer() {
  let payload = PayloadHandle::new(json!({"a/b":{"~k":1}}));

  let result = payload.replace("/a~1b/~0k", &json!(2));

  assert_eq!(result, Ok(()));
  assert_eq!(payload.value(), json!({"a/b":{"~k":2}}));
}

#[test]
fn should_fail_payload_mutation_at_positions_missing_from_the_payload() {
  for pointer in ["/items/1/qty", "/items/00/qty", "/items/-", "/missing", ""] {
    let mut case = real_case("memory-payload-written-mutation");
    case.body["steps"][1]["arguments"]["pointer"] = json!(pointer);
    let factory = RecordingStores::default();

    let (outcome, _) = execute(&case, &factory);

    assert!(
      matches!(
        outcome,
        CaseOutcome::Failed {
          failed_operation: Some(2),
          ..
        }
      ),
      "{pointer:?}: ペイロードにない位置への書き換えは失敗にする: {outcome:?}"
    );
    assert_eq!(
      factory.written_values(),
      vec![json!({"items":[{"qty":1}],"note":"original"})],
      "{pointer:?}: 失敗した書き換えは手元の実体を変えず、キーも作らない"
    );
  }
}

#[test]
fn should_fail_payload_mutation_whose_operation_is_not_an_earlier_successful_write() {
  let mut case = real_case("memory-payload-written-mutation");
  case.body["steps"][1]["arguments"]["operation"] = json!(2);

  let (outcome, _) = execute(&case, &LibraryStores);

  assert!(
    matches!(
      outcome,
      CaseOutcome::Failed {
        failed_operation: Some(2),
        ..
      }
    ),
    "自分自身の操作番号は、書き換えの対象にしない: {outcome:?}"
  );
}
