use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
use chrono::Duration;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::event_envelope::{EventEnvelope, SnapshotEnvelope};
use crate::event_store_backend::{SnapshotMaintenance, StorageBackend};
use crate::types::{AggregateId, EventStore, EventStoreReadError, EventStoreWriteError};

// FR4.1: StorageBackend + GenericEventStore の 2 層委譲構造を維持したまま、境界の
// 受け渡しを封筒（EventEnvelope / SnapshotEnvelope）に変更する。契約検証
// （BR1.4 / BR2.2 / BR2.6 / BR4.1）と create/update 分岐（BR2.1）はこの層が持つ。

// fnポインタ経由の型マーカー — Send/Sync自動導出を阻害しない
type TypeMarker<AID, A, P> = fn() -> (AID, A, P);

/// StorageBackend への委譲で `EventStore` を提供する汎用イベントストア。
pub struct GenericEventStore<AID, A, P, B> {
  backend: B,
  maintenance: SnapshotMaintenance,
  _phantom: PhantomData<TypeMarker<AID, A, P>>,
}

// P2 / NFR2: derive は PhantomData の型パラメータ（AID / A / P）にも境界を課すため、
// Debug / Clone は実フィールドが要求する境界（B のみ）に限定した手動 impl とする
// （payload への Debug / Clone 要求を構造的に遮断する — BR1.6）
impl<AID, A, P, B: Debug> Debug for GenericEventStore<AID, A, P, B> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("GenericEventStore")
      .field("backend", &self.backend)
      .field("maintenance", &self.maintenance)
      .finish()
  }
}

impl<AID, A, P, B: Clone> Clone for GenericEventStore<AID, A, P, B> {
  fn clone(&self) -> Self {
    Self {
      backend: self.backend.clone(),
      maintenance: self.maintenance.clone(),
      _phantom: PhantomData,
    }
  }
}

impl<AID, A, P, B> GenericEventStore<AID, A, P, B> {
  pub fn new(backend: B) -> Self {
    Self {
      backend,
      maintenance: SnapshotMaintenance::default(),
      _phantom: PhantomData,
    }
  }

  /// 保持するスナップショット履歴数を設定する。
  ///
  /// BR4.1 / FR6.4 / P3: 有効値は `None`（剪定しない）と `Some(n >= 1)`（履歴 n 件保持）のみ。
  /// `Some(0)` は `EventStoreWriteError::ContractViolation` で拒否する（検証はこの 1 箇所で行い、
  /// 各ストア型のラッパーは Result を素通しする）。
  pub fn with_keep_snapshot_count(mut self, keep_snapshot_count: Option<usize>) -> Result<Self, EventStoreWriteError> {
    if keep_snapshot_count == Some(0) {
      return Err(EventStoreWriteError::ContractViolation(
        "BR4.1: keep_snapshot_count must be None or at least 1, keep_snapshot_count=0".to_string(),
      ));
    }
    self.maintenance.keep_snapshot_count = keep_snapshot_count;
    Ok(self)
  }

  pub fn with_delete_ttl(mut self, delete_ttl: Option<Duration>) -> Self {
    self.maintenance.delete_ttl = delete_ttl;
    self
  }

  // シリアライザ等のセッタを持つバックエンドのファサードだけが使う
  #[cfg(any(
    feature = "dynamodb",
    feature = "bigtable",
    feature = "sqlite",
    feature = "sqlite-system"
  ))]
  pub fn backend_mut(&mut self) -> &mut B {
    &mut self.backend
  }

  pub fn maintenance(&self) -> &SnapshotMaintenance {
    &self.maintenance
  }
}

// BR1.4: seq_nr は 1 以上。0 の封筒は書込時に契約エラーで拒否する
fn validate_seq_nr_is_positive(seq_nr: usize) -> Result<(), EventStoreWriteError> {
  if seq_nr == 0 {
    return Err(EventStoreWriteError::ContractViolation(format!(
      "BR1.4: seq_nr must be at least 1, seq_nr={}",
      seq_nr
    )));
  }
  Ok(())
}

#[async_trait]
impl<AID, A, P, B> EventStore for GenericEventStore<AID, A, P, B>
where
  AID: AggregateId,
  A: Serialize + DeserializeOwned + Send + Sync + 'static,
  P: Serialize + DeserializeOwned + Send + Sync + 'static,
  B: StorageBackend<AID, A, P>,
{
  type A = A;
  type AID = AID;
  type P = P;

  async fn persist_event(
    &mut self,
    event: EventEnvelope<Self::AID, Self::P>,
    expected_version: usize,
  ) -> Result<(), EventStoreWriteError> {
    validate_seq_nr_is_positive(event.seq_nr())?;
    // BR2.2: イベントのみ API への seq_nr == 1（新規作成）は契約エラーで拒否する
    if event.seq_nr() == 1 {
      return Err(EventStoreWriteError::ContractViolation(format!(
        "BR2.2: persist_event is update-only and cannot accept a creation event, seq_nr={}",
        event.seq_nr()
      )));
    }
    // W2: イベントのみ更新 — version 加算は列側で行う（BR2.3）
    self
      .backend
      .update_event_and_snapshot(&event, None, expected_version, &self.maintenance)
      .await?;
    self
      .backend
      .on_event_persisted(event.aggregate_id(), &self.maintenance)
      .await
  }

  async fn persist_event_and_snapshot(
    &mut self,
    event: EventEnvelope<Self::AID, Self::P>,
    aggregate: Self::A,
    expected_version: usize,
  ) -> Result<(), EventStoreWriteError> {
    validate_seq_nr_is_positive(event.seq_nr())?;
    // BR2.6: seq_nr == 1 ⇔ expected_version == 0 の対応が崩れる呼び出しを拒否する
    if (event.seq_nr() == 1 && expected_version != 0) || (event.seq_nr() > 1 && expected_version == 0) {
      return Err(EventStoreWriteError::ContractViolation(format!(
        "BR2.6: seq_nr and expected_version are inconsistent (seq_nr == 1 <=> expected_version == 0), seq_nr={}, \
         expected_version={}",
        event.seq_nr(),
        expected_version
      )));
    }
    // BR2.1: create / update の分岐は封筒の seq_nr == 1 の導出で行う（is_created は存在しない）
    if event.seq_nr() == 1 {
      // W1: 新規作成 — journal + snapshot（version = 1、seq_nr = event.seq_nr）を原子的に作成
      self
        .backend
        .create_event_and_snapshot(&event, &aggregate, &self.maintenance)
        .await?;
    } else {
      // W3: 更新（スナップショット付き）— version CAS（BR2.3）
      self
        .backend
        .update_event_and_snapshot(&event, Some(&aggregate), expected_version, &self.maintenance)
        .await?;
    }
    self
      .backend
      .on_event_persisted(event.aggregate_id(), &self.maintenance)
      .await
  }

  async fn get_latest_snapshot_by_id(
    &self,
    aid: &Self::AID,
  ) -> Result<Option<SnapshotEnvelope<Self::A>>, EventStoreReadError> {
    // FR2.2 / BR3.1: スナップショット封筒を透過返却し、seq_nr / version を境界で破棄しない
    self.backend.fetch_latest_snapshot(aid).await
  }

  async fn get_events_by_id_since_seq_nr(
    &self,
    aid: &Self::AID,
    seq_nr: usize,
  ) -> Result<Vec<EventEnvelope<Self::AID, Self::P>>, EventStoreReadError> {
    // FR5.1 / BR3.2: 封筒列を透過返却する
    self.backend.fetch_events_since(aid, seq_nr).await
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use async_trait::async_trait;
  use chrono::{DateTime, TimeZone, Utc};
  use serde::{Deserialize, Serialize};
  use std::fmt;
  use std::sync::{Arc, Mutex};

  #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
  struct TestAggregateId(u64);

  impl fmt::Display for TestAggregateId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
      write!(f, "{}", self.0)
    }
  }

  impl AggregateId for TestAggregateId {
    fn type_name(&self) -> String {
      "TestAggregate".to_string()
    }

    fn value(&self) -> String {
      self.0.to_string()
    }
  }

  #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
  struct TestAggregate {
    name: String,
  }

  #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
  struct TestPayload {
    name: String,
  }

  fn fixed_occurred_at() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap()
  }

  fn envelope(id: u64, seq_nr: usize, name: &str) -> EventEnvelope<TestAggregateId, TestPayload> {
    EventEnvelope::new(
      TestAggregateId(id),
      seq_nr,
      fixed_occurred_at(),
      TestPayload { name: name.to_string() },
    )
  }

  // 呼び出し記録 + 決め打ち応答のスタブ StorageBackend（外部モックライブラリは導入しない）
  #[derive(Clone)]
  struct TestBackend {
    state: Arc<Mutex<TestBackendState>>,
  }

  #[derive(Default)]
  struct TestBackendState {
    snapshot: Option<SnapshotEnvelope<TestAggregate>>,
    events: Vec<EventEnvelope<TestAggregateId, TestPayload>>,
    last_maintenance: Option<SnapshotMaintenance>,
    calls: Vec<String>,
  }

  struct TestBackendView {
    snapshot: Option<SnapshotEnvelope<TestAggregate>>,
    events: Vec<EventEnvelope<TestAggregateId, TestPayload>>,
    last_maintenance: Option<SnapshotMaintenance>,
    calls: Vec<String>,
  }

  impl fmt::Debug for TestBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
      f.debug_struct("TestBackend").finish()
    }
  }

  impl TestBackend {
    fn new() -> Self {
      Self {
        state: Arc::new(Mutex::new(TestBackendState::default())),
      }
    }

    fn view(&self) -> TestBackendView {
      let state = self.state.lock().unwrap();
      TestBackendView {
        snapshot: state.snapshot.clone(),
        events: state.events.clone(),
        last_maintenance: state.last_maintenance.clone(),
        calls: state.calls.clone(),
      }
    }
  }

  #[async_trait]
  impl StorageBackend<TestAggregateId, TestAggregate, TestPayload> for TestBackend {
    async fn fetch_latest_snapshot(
      &self,
      _aid: &TestAggregateId,
    ) -> Result<Option<SnapshotEnvelope<TestAggregate>>, EventStoreReadError> {
      let state = self.state.lock().unwrap();
      Ok(state.snapshot.clone())
    }

    async fn fetch_events_since(
      &self,
      _aid: &TestAggregateId,
      seq_nr: usize,
    ) -> Result<Vec<EventEnvelope<TestAggregateId, TestPayload>>, EventStoreReadError> {
      let state = self.state.lock().unwrap();
      Ok(
        state
          .events
          .iter()
          .filter(|event| event.seq_nr() >= seq_nr)
          .cloned()
          .collect(),
      )
    }

    async fn create_event_and_snapshot(
      &self,
      event: &EventEnvelope<TestAggregateId, TestPayload>,
      aggregate: &TestAggregate,
      _maintenance: &SnapshotMaintenance,
    ) -> Result<(), EventStoreWriteError> {
      let mut state = self.state.lock().unwrap();
      state.calls.push("create".to_string());
      state.snapshot = Some(SnapshotEnvelope::new(aggregate.clone(), event.seq_nr(), 1));
      state.events.push(event.clone());
      Ok(())
    }

    async fn update_event_and_snapshot(
      &self,
      event: &EventEnvelope<TestAggregateId, TestPayload>,
      aggregate: Option<&TestAggregate>,
      expected_version: usize,
      _maintenance: &SnapshotMaintenance,
    ) -> Result<(), EventStoreWriteError> {
      let mut state = self.state.lock().unwrap();
      state
        .calls
        .push(format!("update(expected_version={})", expected_version));
      // BR2.3 / AC2.2.3: 対象集約が不在の更新は楽観ロック競合（actual_version なし書式）— 実体と同じ意味論
      let current = state.snapshot.as_ref().ok_or_else(|| {
        EventStoreWriteError::OptimisticLockError(crate::types::format_optimistic_lock_message(
          &event.aggregate_id().to_string(),
          expected_version,
          None,
        ))
      })?;
      if current.version() != expected_version {
        return Err(EventStoreWriteError::OtherError("version mismatch".to_string()));
      }
      let new_version = expected_version + 1;
      state.snapshot = Some(match aggregate {
        Some(aggregate) => SnapshotEnvelope::new(aggregate.clone(), event.seq_nr(), new_version),
        None => SnapshotEnvelope::new(current.aggregate().clone(), current.seq_nr(), new_version),
      });
      state.events.push(event.clone());
      Ok(())
    }

    async fn on_event_persisted(
      &self,
      _aid: &TestAggregateId,
      maintenance: &SnapshotMaintenance,
    ) -> Result<(), EventStoreWriteError> {
      let mut state = self.state.lock().unwrap();
      state.calls.push("on_event".to_string());
      state.last_maintenance = Some(maintenance.clone());
      Ok(())
    }
  }

  fn new_store() -> (
    TestBackend,
    GenericEventStore<TestAggregateId, TestAggregate, TestPayload, TestBackend>,
  ) {
    let backend = TestBackend::new();
    let store = GenericEventStore::new(backend.clone());
    (backend, store)
  }

  // W1 / BR2.1 / AC2.3.1: seq_nr == 1 + expected_version == 0 は新規作成経路へ委譲される
  #[tokio::test]
  async fn test_persist_event_and_snapshot_with_seq_nr_one_delegates_to_create() {
    let (backend, mut store) = new_store();

    store
      .persist_event_and_snapshot(
        envelope(1, 1, "created"),
        TestAggregate {
          name: "test".to_string(),
        },
        0,
      )
      .await
      .expect("creation should succeed");

    let view = backend.view();
    assert_eq!(view.calls, vec!["create", "on_event"]);
    assert_eq!(view.events.len(), 1);
    let snapshot = view.snapshot.expect("snapshot stored");
    assert_eq!(snapshot.version(), 1);
    assert_eq!(snapshot.seq_nr(), 1);
    assert_eq!(snapshot.aggregate().name, "test");
  }

  // W3 / AC2.3.2: seq_nr > 1 は更新経路（aggregate 付き）へ委譲され expected_version が透過する
  #[tokio::test]
  async fn test_persist_event_and_snapshot_with_seq_nr_above_one_delegates_to_update() {
    let (backend, mut store) = new_store();
    store
      .persist_event_and_snapshot(
        envelope(1, 1, "created"),
        TestAggregate {
          name: "test".to_string(),
        },
        0,
      )
      .await
      .unwrap();

    store
      .persist_event_and_snapshot(
        envelope(1, 2, "renamed"),
        TestAggregate {
          name: "test2".to_string(),
        },
        1,
      )
      .await
      .expect("update should succeed");

    let view = backend.view();
    assert_eq!(
      view.calls,
      vec!["create", "on_event", "update(expected_version=1)", "on_event"]
    );
    let snapshot = view.snapshot.expect("snapshot stored");
    assert_eq!(snapshot.version(), 2);
    assert_eq!(snapshot.seq_nr(), 2);
    assert_eq!(snapshot.aggregate().name, "test2");
    assert_eq!(view.events.len(), 2);
  }

  // W2 / AC2.1.4 の対偶経路: persist_event は aggregate なしの更新へ委譲される
  #[tokio::test]
  async fn test_persist_event_delegates_to_update_without_aggregate() {
    let (backend, mut store) = new_store();
    store
      .persist_event_and_snapshot(
        envelope(1, 1, "created"),
        TestAggregate {
          name: "test".to_string(),
        },
        0,
      )
      .await
      .unwrap();

    store
      .persist_event(envelope(1, 2, "renamed"), 1)
      .await
      .expect("event-only update should succeed");

    let view = backend.view();
    assert_eq!(
      view.calls,
      vec!["create", "on_event", "update(expected_version=1)", "on_event"]
    );
    let snapshot = view.snapshot.expect("snapshot stored");
    // イベントのみ更新では version のみ加算され、スナップショットの aggregate / seq_nr は据え置き
    assert_eq!(snapshot.version(), 2);
    assert_eq!(snapshot.seq_nr(), 1);
    assert_eq!(snapshot.aggregate().name, "test");
    assert_eq!(view.events.len(), 2);
  }

  // BR1.4: seq_nr == 0 の封筒は persist_event で ContractViolation（バックエンド不達）
  #[tokio::test]
  async fn test_persist_event_rejects_seq_nr_zero() {
    let (backend, mut store) = new_store();
    let result = store.persist_event(envelope(1, 0, "zero"), 1).await;
    match result {
      Err(EventStoreWriteError::ContractViolation(reason)) => {
        assert_eq!(reason, "BR1.4: seq_nr must be at least 1, seq_nr=0");
      }
      other => panic!("expected ContractViolation, got {:?}", other),
    }
    assert!(backend.view().calls.is_empty(), "backend must not be reached");
  }

  // BR1.4: seq_nr == 0 の封筒は persist_event_and_snapshot でも ContractViolation
  #[tokio::test]
  async fn test_persist_event_and_snapshot_rejects_seq_nr_zero() {
    let (backend, mut store) = new_store();
    let result = store
      .persist_event_and_snapshot(
        envelope(1, 0, "zero"),
        TestAggregate {
          name: "test".to_string(),
        },
        0,
      )
      .await;
    match result {
      Err(EventStoreWriteError::ContractViolation(reason)) => {
        assert_eq!(reason, "BR1.4: seq_nr must be at least 1, seq_nr=0");
      }
      other => panic!("expected ContractViolation, got {:?}", other),
    }
    assert!(backend.view().calls.is_empty(), "backend must not be reached");
  }

  // BR2.2 / AC2.1.4: イベントのみ API への seq_nr == 1（新規作成）は ContractViolation
  #[tokio::test]
  async fn test_persist_event_rejects_creation_seq_nr() {
    let (backend, mut store) = new_store();
    let result = store.persist_event(envelope(1, 1, "created"), 0).await;
    match result {
      Err(EventStoreWriteError::ContractViolation(reason)) => {
        assert_eq!(
          reason,
          "BR2.2: persist_event is update-only and cannot accept a creation event, seq_nr=1"
        );
      }
      other => panic!("expected ContractViolation, got {:?}", other),
    }
    assert!(backend.view().calls.is_empty(), "backend must not be reached");
  }

  // BR2.6: seq_nr == 1 かつ expected_version != 0 は ContractViolation
  #[tokio::test]
  async fn test_persist_event_and_snapshot_rejects_creation_with_nonzero_expected_version() {
    let (backend, mut store) = new_store();
    let result = store
      .persist_event_and_snapshot(
        envelope(1, 1, "created"),
        TestAggregate {
          name: "test".to_string(),
        },
        5,
      )
      .await;
    match result {
      Err(EventStoreWriteError::ContractViolation(reason)) => {
        assert_eq!(
          reason,
          "BR2.6: seq_nr and expected_version are inconsistent (seq_nr == 1 <=> expected_version == 0), seq_nr=1, \
           expected_version=5"
        );
      }
      other => panic!("expected ContractViolation, got {:?}", other),
    }
    assert!(backend.view().calls.is_empty(), "backend must not be reached");
  }

  // BR2.6: seq_nr > 1 かつ expected_version == 0 は ContractViolation
  #[tokio::test]
  async fn test_persist_event_and_snapshot_rejects_update_with_zero_expected_version() {
    let (backend, mut store) = new_store();
    let result = store
      .persist_event_and_snapshot(
        envelope(1, 2, "renamed"),
        TestAggregate {
          name: "test".to_string(),
        },
        0,
      )
      .await;
    match result {
      Err(EventStoreWriteError::ContractViolation(reason)) => {
        assert_eq!(
          reason,
          "BR2.6: seq_nr and expected_version are inconsistent (seq_nr == 1 <=> expected_version == 0), seq_nr=2, \
           expected_version=0"
        );
      }
      other => panic!("expected ContractViolation, got {:?}", other),
    }
    assert!(backend.view().calls.is_empty(), "backend must not be reached");
  }

  // BR4.1 / W5: with_keep_snapshot_count は Some(0) を拒否し、None / Some(n >= 1) を受理する
  #[tokio::test]
  async fn test_with_keep_snapshot_count_rejects_zero() {
    let (_backend, store) = new_store();
    let result = store.with_keep_snapshot_count(Some(0));
    match result {
      Err(EventStoreWriteError::ContractViolation(reason)) => {
        assert_eq!(
          reason,
          "BR4.1: keep_snapshot_count must be None or at least 1, keep_snapshot_count=0"
        );
      }
      other => panic!("expected ContractViolation, got {:?}", other.map(|_| ())),
    }

    let (_backend, store) = new_store();
    let store = store.with_keep_snapshot_count(Some(1)).expect("Some(1) is valid");
    let store = store.with_keep_snapshot_count(None).expect("None is valid");
    assert_eq!(store.maintenance().keep_snapshot_count, None);
  }

  // BR4.2 経路の設定運搬: maintenance 設定がバックエンドの書込後フックまで透過する
  #[tokio::test]
  async fn test_maintenance_configuration_is_passed_to_backend() {
    let backend = TestBackend::new();
    let mut store = GenericEventStore::new(backend.clone())
      .with_keep_snapshot_count(Some(2))
      .unwrap()
      .with_delete_ttl(Some(Duration::seconds(30)));

    store
      .persist_event_and_snapshot(
        envelope(42, 1, "created"),
        TestAggregate {
          name: "test".to_string(),
        },
        0,
      )
      .await
      .unwrap();

    let view = backend.view();
    assert_eq!(
      view.last_maintenance,
      Some(SnapshotMaintenance {
        keep_snapshot_count: Some(2),
        delete_ttl: Some(Duration::seconds(30)),
      })
    );
  }

  // FR2.2 / BR3.1: スナップショット封筒が seq_nr / version を破棄されず透過返却される
  #[tokio::test]
  async fn test_get_latest_snapshot_returns_envelope_transparently() {
    let (_backend, mut store) = new_store();
    store
      .persist_event_and_snapshot(
        envelope(1, 1, "created"),
        TestAggregate {
          name: "test".to_string(),
        },
        0,
      )
      .await
      .unwrap();

    let snapshot = store
      .get_latest_snapshot_by_id(&TestAggregateId(1))
      .await
      .unwrap()
      .expect("snapshot must exist");
    assert_eq!(snapshot.version(), 1);
    assert_eq!(snapshot.seq_nr(), 1);
    assert_eq!(snapshot.aggregate().name, "test");

    // 不在は None（エラーにしない）
    let (_backend2, store2) = new_store();
    let missing = store2.get_latest_snapshot_by_id(&TestAggregateId(9)).await.unwrap();
    assert!(missing.is_none());
  }

  // FR5.1 / BR3.2: イベント読取は封筒列を返し、列由来メタデータが取得できる
  #[tokio::test]
  async fn test_get_events_since_returns_envelopes() {
    let (_backend, mut store) = new_store();
    store
      .persist_event_and_snapshot(
        envelope(1, 1, "created"),
        TestAggregate {
          name: "test".to_string(),
        },
        0,
      )
      .await
      .unwrap();
    store.persist_event(envelope(1, 2, "renamed"), 1).await.unwrap();

    let events = store
      .get_events_by_id_since_seq_nr(&TestAggregateId(1), 2)
      .await
      .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].seq_nr(), 2);
    assert_eq!(events[0].aggregate_id(), &TestAggregateId(1));
    assert_eq!(events[0].payload().name, "renamed");
  }

  // NFR3.2 / BR5.2 / P1: ContractViolation の理由文字列が「規約名 + seq_nr / expected_version の
  // 数値」テンプレートのみで構成されること（それ以外の情報を含まないこと）を機械的に固定する
  // （OptimisticLockError の許可キー検証と同格の回帰テスト）
  fn assert_contract_violation_reason_template(reason: &str) {
    let mut parts = reason.split(", ");
    let head = parts.next().unwrap();
    // 先頭は「BR<x>.<y>: <固定説明文>」— 規約名で始まる
    let (rule_id, description) = head.split_once(": ").expect("reason must start with a rule id prefix");
    assert!(
      rule_id.starts_with("BR")
        && rule_id[2..]
          .split('.')
          .all(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())),
      "reason must start with a BR rule id, got: {}",
      rule_id
    );
    assert!(!description.is_empty());
    // 後続はすべて許可キーの key=数値 のみ（集約 ID・payload 内容・接続情報は含めない）
    let allowed_keys = ["seq_nr", "expected_version", "keep_snapshot_count"];
    for part in parts {
      let (key, value) = part
        .split_once('=')
        .unwrap_or_else(|| panic!("non key=value part in reason: {}", part));
      assert!(allowed_keys.contains(&key), "unexpected field in reason: {}", part);
      assert!(
        !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()),
        "non-numeric value in reason: {}",
        part
      );
    }
    assert!(!reason.contains("://"), "reason must not contain connection strings");
  }

  #[tokio::test]
  async fn test_contract_violation_reason_strings_follow_fixed_template() {
    // ContractViolation を返す全 4 規則（BR1.4 / BR2.2 / BR2.6 / BR4.1）の理由文字列を収集する
    let (_backend, mut store) = new_store();
    let mut reasons = Vec::new();

    // BR1.4
    match store.persist_event(envelope(1, 0, "zero"), 1).await {
      Err(EventStoreWriteError::ContractViolation(reason)) => reasons.push(reason),
      other => panic!("expected ContractViolation, got {:?}", other),
    }
    // BR2.2
    match store.persist_event(envelope(1, 1, "created"), 0).await {
      Err(EventStoreWriteError::ContractViolation(reason)) => reasons.push(reason),
      other => panic!("expected ContractViolation, got {:?}", other),
    }
    // BR2.6
    match store
      .persist_event_and_snapshot(
        envelope(1, 1, "created"),
        TestAggregate {
          name: "test".to_string(),
        },
        5,
      )
      .await
    {
      Err(EventStoreWriteError::ContractViolation(reason)) => reasons.push(reason),
      other => panic!("expected ContractViolation, got {:?}", other),
    }
    // BR4.1
    let (_backend2, store2) = new_store();
    match store2.with_keep_snapshot_count(Some(0)) {
      Err(EventStoreWriteError::ContractViolation(reason)) => reasons.push(reason),
      other => panic!("expected ContractViolation, got {:?}", other.map(|_| ())),
    }

    assert_eq!(reasons.len(), 4);
    for reason in &reasons {
      assert_contract_violation_reason_template(reason);
    }
  }

  // FR7.3 ② / NFR2: derive なしプレーン型（Debug / Clone なし）でも GenericEventStore が
  // EventStore を実装できることのコンパイル証明（derive 戦略の回帰ガード）
  #[derive(Serialize, Deserialize)]
  struct PlainAggregate {
    name: String,
  }

  #[derive(Serialize, Deserialize)]
  struct PlainPayload {
    name: String,
  }

  #[derive(Clone, Debug)]
  struct PlainBackend;

  #[async_trait]
  impl StorageBackend<TestAggregateId, PlainAggregate, PlainPayload> for PlainBackend {
    async fn fetch_latest_snapshot(
      &self,
      _aid: &TestAggregateId,
    ) -> Result<Option<SnapshotEnvelope<PlainAggregate>>, EventStoreReadError> {
      Ok(None)
    }

    async fn fetch_events_since(
      &self,
      _aid: &TestAggregateId,
      _seq_nr: usize,
    ) -> Result<Vec<EventEnvelope<TestAggregateId, PlainPayload>>, EventStoreReadError> {
      Ok(Vec::new())
    }

    async fn create_event_and_snapshot(
      &self,
      _event: &EventEnvelope<TestAggregateId, PlainPayload>,
      _aggregate: &PlainAggregate,
      _maintenance: &SnapshotMaintenance,
    ) -> Result<(), EventStoreWriteError> {
      Ok(())
    }

    async fn update_event_and_snapshot(
      &self,
      _event: &EventEnvelope<TestAggregateId, PlainPayload>,
      _aggregate: Option<&PlainAggregate>,
      _expected_version: usize,
      _maintenance: &SnapshotMaintenance,
    ) -> Result<(), EventStoreWriteError> {
      Ok(())
    }
  }

  #[tokio::test]
  async fn test_generic_event_store_implements_event_store_for_plain_types() {
    // EventStore（Debug + Clone + Send + Sync + 'static を要求）の実装が
    // Debug / Clone なしの A / P で成立することのコンパイル検証
    fn assert_implements_event_store<T: EventStore>(_store: &T) {}

    let mut store: GenericEventStore<TestAggregateId, PlainAggregate, PlainPayload, PlainBackend> =
      GenericEventStore::new(PlainBackend);
    assert_implements_event_store(&store);

    let event = EventEnvelope::new(
      TestAggregateId(1),
      1,
      fixed_occurred_at(),
      PlainPayload {
        name: "created".to_string(),
      },
    );
    store
      .persist_event_and_snapshot(
        event,
        PlainAggregate {
          name: "test".to_string(),
        },
        0,
      )
      .await
      .unwrap();

    let snapshot = store.get_latest_snapshot_by_id(&TestAggregateId(1)).await.unwrap();
    assert!(snapshot.is_none(), "canned backend returns no snapshot");
    let events = store
      .get_events_by_id_since_seq_nr(&TestAggregateId(1), 1)
      .await
      .unwrap();
    assert!(events.is_empty(), "canned backend returns no events");
  }
}
