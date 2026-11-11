#![cfg(all(feature = "sqlite", feature = "test-hooks"))]
// 通知の callsite の interest は、プロセス全体で覚えられる。購読者のない別の試験と並ぶと結果が変わるので、
// この試験だけを 1 つの試験のバイナリに置く。

mod sqlite_support;

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use event_store_adapter_rs::sqlite::SqliteTestHooks;
use event_store_adapter_rs::{
  initialize_sqlite_store, AggregateId, AidString, BlockingExecution, EventEnvelope, EventStore, EventStoreError,
  EventStoreForSqlite, JsonEventSerializer, JsonSnapshotSerializer, RetentionSettings, SeqNr, SnapshotEnvelope,
  SqliteOptions, StorageOperation,
};
use sqlite_support::TempDatabase;
use tracing::{Event, Instrument, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
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

fn event(seq: u64) -> EventEnvelope<Id, u64> {
  EventEnvelope::new(Id, seq, DateTime::<Utc>::from_timestamp(0, 0).unwrap(), seq)
}

fn options() -> SqliteOptions {
  SqliteOptions {
    retention: RetentionSettings::keep_latest(1),
    ..SqliteOptions::default()
  }
}

fn execution() -> BlockingExecution {
  BlockingExecution::single_worker(NonZeroUsize::new(4).unwrap()).unwrap()
}

/// 有効にすると、保持の削除を失敗させる。
#[derive(Debug, Default)]
struct FailingDelete(AtomicBool);

impl SqliteTestHooks for FailingDelete {
  fn before_commit(&self, _: &AidString, _: SeqNr) -> Result<(), EventStoreError> {
    Ok(())
  }

  fn read_events(&self, _: &AidString) -> Result<(), EventStoreError> {
    Ok(())
  }

  fn read_snapshot(&self, _: &AidString) -> Result<(), EventStoreError> {
    Ok(())
  }

  fn retention_delete(&self, _: &AidString, _: &[SeqNr]) -> Result<(), EventStoreError> {
    if self.0.load(Ordering::SeqCst) {
      Err(EventStoreError::Storage {
        operation: StorageOperation::Append,
        source: Box::new(std::io::Error::other("INJECTED")),
      })
    } else {
      Ok(())
    }
  }
}

/// 保持の失敗の通知ごとに、その通知を囲む span の名前を内側から並べて記録する。
struct RetentionNotices(Arc<Mutex<Vec<Vec<&'static str>>>>);

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for RetentionNotices {
  fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
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
  initialize_sqlite_store(database.path(), &options()).unwrap();
  let hooks = Arc::new(FailingDelete::default());
  let store = Store::open_offloaded_with_hooks(
    database.path(),
    options(),
    Arc::new(JsonEventSerializer::new()),
    Arc::new(JsonSnapshotSerializer::new()),
    hooks.clone(),
    &execution(),
  )
  .unwrap();
  let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();

  tracing::subscriber::with_default(subscriber, || {
    runtime.block_on(
      async {
        store
          .persist_event_and_snapshot(event(1), SnapshotEnvelope::new(1, 1))
          .await
          .unwrap();
        hooks.0.store(true, Ordering::SeqCst);
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
