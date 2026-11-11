#![cfg(all(feature = "sqlite", feature = "test-hooks"))]

mod sqlite_support;

use std::fmt::Debug;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread::{self, ThreadId};
use std::time::Duration;

use chrono::{DateTime, Utc};
use event_store_adapter_rs::sqlite::SqliteTestHooks;
use event_store_adapter_rs::{
  initialize_sqlite_store, AggregateId, AidString, BlockingExecution, EventEnvelope, EventSerializer, EventStore,
  EventStoreError, EventStoreForSqlite, ExecutionRejected, JsonEventSerializer, JsonSnapshotSerializer,
  RetentionSettings, SeqNr, SnapshotEnvelope, SqliteOptions, StorageOperation, SEQ_NR_MAX,
};
use rusqlite::Connection;
use sqlite_support::TempDatabase;
use tracing::{Event, Instrument, Subscriber};
use tracing_subscriber::layer::{Context as LayerContext, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

#[derive(Debug, Clone, PartialEq)]
struct Id;
impl AggregateId for Id {
  fn type_name(&self) -> String {
    "Account".into()
  }

  fn value(&self) -> String {
    "1".into()
  }
}
type Store = EventStoreForSqlite<Id, u64, u64>;

const WAIT_LIMIT: Duration = Duration::from_secs(10);

fn event(seq: u64) -> EventEnvelope<Id, u64> {
  EventEnvelope::new(Id, seq, DateTime::<Utc>::from_timestamp(0, 0).unwrap(), seq)
}

fn injected(operation: StorageOperation) -> EventStoreError {
  EventStoreError::Storage {
    operation,
    source: Box::new(std::io::Error::other("INJECTED")),
  }
}

/// 閉じている間、通る操作をワーカーの上で止める。止まっている操作の数を数える。
#[derive(Debug, Default)]
struct Gate {
  state: Mutex<GateState>,
  changed: Condvar,
}

#[derive(Debug, Default)]
struct GateState {
  closed: bool,
  waiting: usize,
}

impl Gate {
  fn close(&self) {
    self.state.lock().unwrap().closed = true;
  }

  fn open(&self) {
    self.state.lock().unwrap().closed = false;
    self.changed.notify_all();
  }

  fn pass(&self) {
    let mut state = self.state.lock().unwrap();
    state.waiting += 1;
    self.changed.notify_all();
    while state.closed {
      state = self.changed.wait(state).unwrap();
    }
    state.waiting -= 1;
  }

  fn wait_for_waiter(&self) {
    let state = self.state.lock().unwrap();
    let (_state, waited) = self
      .changed
      .wait_timeout_while(state, WAIT_LIMIT, |state| state.waiting == 0)
      .unwrap();
    assert!(!waited.timed_out(), "ワーカーが閉じた門に着く");
  }
}

/// SQLite のトランザクションの中で呼ばれるフック。呼ばれたスレッドを記録し、確定の直前を門で止める。
#[derive(Debug, Default)]
struct Probe {
  gate: Gate,
  threads: Mutex<Vec<(&'static str, ThreadId)>>,
  fail_commit: AtomicBool,
  fail_retention: AtomicBool,
}

impl Probe {
  fn record(&self, step: &'static str) {
    self.threads.lock().unwrap().push((step, thread::current().id()));
  }

  fn steps(&self) -> Vec<&'static str> {
    self.threads.lock().unwrap().iter().map(|(step, _)| *step).collect()
  }
}

impl SqliteTestHooks for Probe {
  fn before_commit(&self, _: &AidString, _: SeqNr) -> Result<(), EventStoreError> {
    self.record("commit");
    self.gate.pass();
    if self.fail_commit.load(Ordering::SeqCst) {
      Err(injected(StorageOperation::Append))
    } else {
      Ok(())
    }
  }

  fn read_events(&self, _: &AidString) -> Result<(), EventStoreError> {
    self.record("load-events");
    Ok(())
  }

  fn read_snapshot(&self, _: &AidString) -> Result<(), EventStoreError> {
    self.record("load-snapshot");
    Ok(())
  }

  fn retention_visible_history(
    &self,
    _: &AidString,
    history: &[SeqNr],
    _: Option<SeqNr>,
  ) -> Result<Vec<SeqNr>, EventStoreError> {
    self.record("retention");
    Ok(history.to_vec())
  }

  fn retention_delete(&self, _: &AidString, _: &[SeqNr]) -> Result<(), EventStoreError> {
    if self.fail_retention.load(Ordering::SeqCst) {
      Err(injected(StorageOperation::Append))
    } else {
      Ok(())
    }
  }
}

/// payload 99 の直列化で panic する。
#[derive(Debug)]
struct PanickingEvents;

impl EventSerializer<u64> for PanickingEvents {
  fn serialize(&self, payload: &u64) -> Result<Vec<u8>, EventStoreError> {
    assert_ne!(*payload, 99, "payload 99 の直列化は panic する");
    JsonEventSerializer::new().serialize(payload)
  }

  fn deserialize(&self, data: &[u8]) -> Result<u64, EventStoreError> {
    JsonEventSerializer::new().deserialize(data)
  }
}

/// payload 1 の直列化を、門が開くまで止める。直列化は接続のロックを取る前に動く。
#[derive(Debug)]
struct GatedEvents(Arc<Gate>);

impl EventSerializer<u64> for GatedEvents {
  fn serialize(&self, payload: &u64) -> Result<Vec<u8>, EventStoreError> {
    if *payload == 1 {
      self.0.pass();
    }
    JsonEventSerializer::new().serialize(payload)
  }

  fn deserialize(&self, data: &[u8]) -> Result<u64, EventStoreError> {
    JsonEventSerializer::new().deserialize(data)
  }
}

fn options() -> SqliteOptions {
  SqliteOptions {
    retention: RetentionSettings::keep_latest(1),
    ..SqliteOptions::default()
  }
}

fn execution(queue_capacity: usize) -> BlockingExecution {
  BlockingExecution::single_worker(NonZeroUsize::new(queue_capacity).unwrap()).unwrap()
}

fn initialized(database: &TempDatabase) {
  initialize_sqlite_store(database.path(), &options()).unwrap();
}

fn offloaded(database: &TempDatabase, probe: &Arc<Probe>, execution: &BlockingExecution) -> Store {
  Store::open_offloaded_with_hooks(
    database.path(),
    options(),
    Arc::new(JsonEventSerializer::new()),
    Arc::new(JsonSnapshotSerializer::new()),
    probe.clone(),
    execution,
  )
  .unwrap()
}

fn caller_thread(database: &TempDatabase, probe: &Arc<Probe>) -> Store {
  Store::open_with_hooks(
    database.path(),
    options(),
    Arc::new(JsonEventSerializer::new()),
    Arc::new(JsonSnapshotSerializer::new()),
    probe.clone(),
  )
  .unwrap()
}

/// 1 度だけ poll して、操作を受け付けさせる。
fn admit<F: Future>(future: F) -> Pin<Box<F>> {
  let mut future = Box::pin(future);
  let poll = future.as_mut().poll(&mut Context::from_waker(Waker::noop()));
  assert!(
    poll.is_pending(),
    "受け付けた操作は、ワーカーが止まっている間は終わらない"
  );
  future
}

/// 1 度だけ poll して、待たずに拒否されることを確かめ、拒否の操作と理由を返す。
fn rejected_now<T: Debug>(
  future: impl Future<Output = Result<T, EventStoreError>>,
) -> (StorageOperation, ExecutionRejected) {
  let mut future = Box::pin(future);
  match future.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
    Poll::Ready(Err(EventStoreError::Storage { operation, source })) => (
      operation,
      *source
        .downcast_ref::<ExecutionRejected>()
        .unwrap_or_else(|| panic!("拒否の source は ExecutionRejected: {source:?}")),
    ),
    Poll::Ready(other) => panic!("保存先エラーの拒否を期待した: {other:?}"),
    Poll::Pending => panic!("拒否されずに受け付けられた"),
  }
}

fn read_back(database: &TempDatabase) -> Vec<EventEnvelope<Id, u64>> {
  let store = Store::open(database.path(), options()).unwrap();
  thread::spawn(move || {
    tokio::runtime::Builder::new_current_thread()
      .build()
      .unwrap()
      .block_on(store.get_events_by_id_since_seq_nr(&Id, 0))
      .unwrap()
  })
  .join()
  .unwrap()
}

// ---------------------------------------------------------------------------
// 呼び出し元を塞がない（L-1）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_offloaded_store_run_the_four_operations_and_retention_on_one_worker_thread() {
  let database = TempDatabase::new();
  initialized(&database);
  let probe = Arc::new(Probe::default());
  let store = offloaded(&database, &probe, &execution(4));

  store.persist_event(event(1)).await.unwrap();
  store
    .persist_event_and_snapshot(event(2), SnapshotEnvelope::new(2, 2))
    .await
    .unwrap();
  store.get_latest_snapshot_by_id(&Id).await.unwrap();
  store.get_events_by_id_since_seq_nr(&Id, 1).await.unwrap();

  assert_eq!(
    probe.steps(),
    ["commit", "commit", "retention", "load-snapshot", "load-events"],
    "4 つの操作と保持が、それぞれ SQLite のトランザクションの中でフックを呼ぶ"
  );
  let caller = thread::current().id();
  let threads = probe.threads.lock().unwrap().clone();
  let worker = threads[0].1;
  assert_ne!(worker, caller, "呼び出したスレッドでは SQLite を呼ばない");
  assert!(
    threads.iter().all(|(_, thread)| *thread == worker),
    "すべての操作が 1 つのワーカーで動く: {threads:?}"
  );
}

// `#[tokio::test]` のランタイムはスレッドが 1 つだけなので、そのスレッドを塞ぐと、ほかのタスクは進まない。
#[tokio::test]
async fn should_offloaded_store_keep_the_calling_runtime_thread_free_while_waiting_for_the_write_lock() {
  let database = TempDatabase::new();
  initialized(&database);
  let store = Store::open_offloaded(database.path(), options(), &execution(4)).unwrap();
  let holder = Connection::open(database.path()).unwrap();
  holder.execute_batch("BEGIN IMMEDIATE").unwrap();

  let pending = tokio::spawn({
    let store = store.clone();
    async move { store.persist_event(event(1)).await }
  });
  tokio::time::sleep(Duration::from_millis(300)).await;

  assert!(
    !pending.is_finished(),
    "追記が書き込みのロックを待つ間も、呼び出し元のスレッドはほかのタスク（この sleep）を進める"
  );
  holder.execute_batch("COMMIT").unwrap();
  pending
    .await
    .unwrap()
    .expect("ロックを放すと、待っていた追記が確定する");
  assert_eq!(read_back(&database), vec![event(1)]);
}

// ---------------------------------------------------------------------------
// 受け付け・拒否・順序・終わり
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_offloaded_store_reject_an_operation_without_running_it_when_the_queue_is_full() {
  let database = TempDatabase::new();
  initialized(&database);
  let probe = Arc::new(Probe::default());
  let store = offloaded(&database, &probe, &execution(1));
  probe.gate.close();

  let running = admit(store.persist_event(event(1)));
  probe.gate.wait_for_waiter();
  let queued = admit(store.persist_event(event(2)));

  assert_eq!(
    rejected_now(store.persist_event(event(3))),
    (StorageOperation::Append, ExecutionRejected::QueueFull)
  );
  assert_eq!(
    rejected_now(store.persist_event_and_snapshot(event(3), SnapshotEnvelope::new(3, 3))),
    (StorageOperation::Append, ExecutionRejected::QueueFull)
  );
  assert_eq!(
    rejected_now(store.get_latest_snapshot_by_id(&Id)),
    (StorageOperation::LoadSnapshot, ExecutionRejected::QueueFull)
  );
  assert_eq!(
    rejected_now(store.get_events_by_id_since_seq_nr(&Id, 1)),
    (StorageOperation::LoadEvents, ExecutionRejected::QueueFull)
  );

  probe.gate.open();
  running.await.unwrap();
  queued.await.unwrap();
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&Id, 1).await.unwrap(),
    vec![event(1), event(2)]
  );
  assert_eq!(
    probe.steps(),
    ["commit", "commit", "load-events"],
    "拒否した操作は実行していない"
  );
}

#[tokio::test]
async fn should_offloaded_store_run_operations_in_the_order_it_accepted_them() {
  let database = TempDatabase::new();
  initialized(&database);
  let gate = Arc::new(Gate::default());
  let store = Store::open_offloaded_with_serializers(
    database.path(),
    options(),
    Arc::new(GatedEvents(gate.clone())),
    Arc::new(JsonSnapshotSerializer::new()),
    &execution(8),
  )
  .unwrap();
  gate.close();

  let first = admit(store.persist_event(event(1)));
  gate.wait_for_waiter();
  let second = admit(store.persist_event(event(2)));
  let read_after_second = admit(store.get_events_by_id_since_seq_nr(&Id, 1));
  let third = admit(store.persist_event_and_snapshot(event(3), SnapshotEnvelope::new(3, 3)));
  let snapshot_after_third = admit(store.get_latest_snapshot_by_id(&Id));
  // 受け付けた順を守らない実装なら、後ろの操作がこの間に先に動く。正しい実装の結果は、この時間に左右されない。
  thread::sleep(Duration::from_millis(100));
  gate.open();

  first.await.unwrap();
  second.await.unwrap();
  assert_eq!(
    read_after_second.await.unwrap(),
    vec![event(1), event(2)],
    "2 番目の追記のあと、3 番目の追記の前に読む"
  );
  third.await.unwrap();
  let snapshot = snapshot_after_third.await.unwrap().expect("ヘッドがある");
  assert_eq!(
    (
      snapshot.head_seq_nr(),
      snapshot.snapshot().map(SnapshotEnvelope::seq_nr)
    ),
    (3, Some(3)),
    "3 番目の追記のあとに読む"
  );
}

#[tokio::test]
async fn should_offloaded_store_reject_after_close_and_finish_accepted_operations_before_drained() {
  let database = TempDatabase::new();
  initialized(&database);
  let probe = Arc::new(Probe::default());
  let execution = execution(4);
  let store = offloaded(&database, &probe, &execution);
  probe.gate.close();

  let running = admit(store.persist_event(event(1)));
  probe.gate.wait_for_waiter();
  let queued = admit(store.persist_event(event(2)));
  execution.close();
  execution.close();

  assert_eq!(
    rejected_now(store.persist_event(event(3))),
    (StorageOperation::Append, ExecutionRejected::Closed)
  );
  assert_eq!(
    rejected_now(store.get_events_by_id_since_seq_nr(&Id, 1)),
    (StorageOperation::LoadEvents, ExecutionRejected::Closed)
  );
  let drained = admit(execution.drained());

  probe.gate.open();
  tokio::time::timeout(WAIT_LIMIT, drained)
    .await
    .expect("受け付け済みの操作が終わると、drained が完了する");
  assert!(
    tokio::time::timeout(Duration::ZERO, execution.drained()).await.is_ok(),
    "終わったあとに待ち始めても、すぐに完了する"
  );
  running.await.unwrap();
  queued.await.unwrap();
  assert_eq!(read_back(&database), vec![event(1), event(2)]);
}

#[tokio::test]
async fn should_offloaded_store_commit_an_accepted_append_after_its_future_is_dropped() {
  let database = TempDatabase::new();
  initialized(&database);
  let probe = Arc::new(Probe::default());
  let execution = execution(4);
  let store = offloaded(&database, &probe, &execution);
  probe.gate.close();

  let accepted = admit(store.persist_event(event(1)));
  probe.gate.wait_for_waiter();
  drop(accepted);
  let queued_then_dropped = admit(store.persist_event(event(2)));
  drop(queued_then_dropped);
  probe.gate.open();
  execution.close();
  tokio::time::timeout(WAIT_LIMIT, execution.drained()).await.unwrap();

  assert_eq!(
    read_back(&database),
    vec![event(1), event(2)],
    "Future を破棄しても、受け付けた追記は確定する"
  );
}

#[tokio::test]
async fn should_offloaded_store_raise_an_operation_panic_on_the_awaiting_task_and_keep_the_worker_running() {
  let database = TempDatabase::new();
  initialized(&database);
  let store = Store::open_offloaded_with_serializers(
    database.path(),
    options(),
    Arc::new(PanickingEvents),
    Arc::new(JsonSnapshotSerializer::new()),
    &execution(4),
  )
  .unwrap();

  let panicked = tokio::time::timeout(
    WAIT_LIMIT,
    tokio::spawn({
      let store = store.clone();
      async move { store.persist_event(event(99)).await }
    }),
  )
  .await
  .expect("panic した操作の結果も、待つ側に届く");
  assert!(panicked.unwrap_err().is_panic(), "操作の panic は待つ側で起きる");

  tokio::time::timeout(WAIT_LIMIT, store.persist_event(event(1)))
    .await
    .expect("panic のあとも、ワーカーは次の操作を実行する")
    .unwrap();
}

// ---------------------------------------------------------------------------
// 結果・分類・確定の規則は変わらない（L-2）
// ---------------------------------------------------------------------------

/// 同じ操作の列を与え、各操作の結果を Debug の文字列で並べる。
async fn outcomes(store: &Store, probe: &Probe) -> Vec<String> {
  let mut outcomes = vec![
    format!("{:?}", store.persist_event(event(1)).await),
    format!("{:?}", store.persist_event(event(1)).await),
    format!("{:?}", store.persist_event(event(3)).await),
    format!("{:?}", store.persist_event(event(0)).await),
    format!(
      "{:?}",
      store
        .persist_event_and_snapshot(event(2), SnapshotEnvelope::new(3, 3))
        .await
    ),
  ];
  probe.fail_commit.store(true, Ordering::SeqCst);
  outcomes.push(format!("{:?}", store.persist_event(event(2)).await));
  probe.fail_commit.store(false, Ordering::SeqCst);
  outcomes.push(format!(
    "{:?}",
    store
      .persist_event_and_snapshot(event(2), SnapshotEnvelope::new(2, 2))
      .await
  ));
  probe.fail_retention.store(true, Ordering::SeqCst);
  outcomes.push(format!(
    "{:?}",
    store
      .persist_event_and_snapshot(event(3), SnapshotEnvelope::new(3, 3))
      .await
  ));
  probe.fail_retention.store(false, Ordering::SeqCst);
  outcomes.push(format!("{:?}", store.get_latest_snapshot_by_id(&Id).await));
  outcomes.push(format!("{:?}", store.get_events_by_id_since_seq_nr(&Id, 0).await));
  outcomes.push(format!(
    "{:?}",
    store.get_events_by_id_since_seq_nr(&Id, SEQ_NR_MAX + 1).await
  ));
  outcomes
}

#[tokio::test]
async fn should_offloaded_store_return_the_same_results_and_errors_as_the_caller_thread_store() {
  let caller_database = TempDatabase::new();
  initialized(&caller_database);
  let caller_probe = Arc::new(Probe::default());
  let expected = outcomes(&caller_thread(&caller_database, &caller_probe), &caller_probe).await;

  let offloaded_database = TempDatabase::new();
  initialized(&offloaded_database);
  let offloaded_probe = Arc::new(Probe::default());
  let actual = outcomes(
    &offloaded(&offloaded_database, &offloaded_probe, &execution(4)),
    &offloaded_probe,
  )
  .await;

  let prefixes = [
    "Ok(())",
    "Err(OptimisticLock",
    "Err(ContractViolation { rule: W8Gap",
    "Err(ContractViolation { rule: W6",
    "Err(ContractViolation { rule: W9",
    "Err(Storage { operation: Append",
    "Ok(())",
    "Ok(())",
    "Ok(Some(SnapshotRead",
    "Ok([EventEnvelope",
    "Err(ContractViolation { rule: T9",
  ];
  for (outcome, prefix) in expected.iter().zip(prefixes) {
    assert!(
      outcome.starts_with(prefix),
      "試験の前提: {outcome} は {prefix} で始まる"
    );
  }
  assert_eq!(
    actual, expected,
    "結果とエラーの分類が、呼び出したスレッドで実行するストアと同じ"
  );
  assert_eq!(
    read_back(&offloaded_database),
    read_back(&caller_database),
    "確定したイベントが同じ"
  );
}

/// 保持の失敗の通知ごとに、その通知を囲む span の名前を内側から並べて記録する。
struct RetentionNotices(Arc<Mutex<Vec<Vec<&'static str>>>>);

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for RetentionNotices {
  fn on_event(&self, event: &Event<'_>, ctx: LayerContext<'_, S>) {
    if event.metadata().target() != "event_store_adapter::retention" {
      return;
    }
    let spans = ctx
      .event_scope(event)
      .map(|scope| scope.map(|span| span.name()).collect())
      .unwrap_or_default();
    self.0.lock().unwrap().push(spans);
  }
}

#[test]
fn should_offloaded_store_report_a_retention_failure_to_the_callers_subscriber_and_span() {
  let notices = Arc::new(Mutex::new(Vec::new()));
  let subscriber = tracing_subscriber::registry().with(RetentionNotices(notices.clone()));
  let database = TempDatabase::new();
  initialized(&database);
  let probe = Arc::new(Probe::default());
  let store = offloaded(&database, &probe, &execution(4));
  let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();

  tracing::subscriber::with_default(subscriber, || {
    runtime.block_on(
      async {
        store
          .persist_event_and_snapshot(event(1), SnapshotEnvelope::new(1, 1))
          .await
          .unwrap();
        probe.fail_retention.store(true, Ordering::SeqCst);
        store
          .persist_event_and_snapshot(event(2), SnapshotEnvelope::new(2, 2))
          .await
          .expect("保持の失敗は、確定した追記を失敗にしない（S-4）");
      }
      .instrument(tracing::info_span!("caller")),
    )
  });

  assert_eq!(
    *notices.lock().unwrap(),
    vec![vec!["caller"]],
    "保持の失敗の通知が、呼び出した側の購読者に、呼び出した側の span の中で 1 度届く"
  );
}
