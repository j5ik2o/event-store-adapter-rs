#![cfg(all(feature = "spanner", feature = "test-hooks"))]
//! 保持の失敗を知らせる通知の経路が panic しても、確定済みの追記は成功のままである（SPN-13・S-4・MEM-11）。
//!
//! 通知の層が実際に呼ばれたことを確かめる。層が呼ばれないまま合格する試験にしない。

mod spanner_support;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use event_store_adapter_rs::spanner::SpannerTestHooks;
use event_store_adapter_rs::{
  AidString, EventStore, EventStoreError, JsonEventSerializer, JsonSnapshotSerializer, SeqNr, StorageOperation,
};
use spanner_support::{event, options_keeping, snapshot, Fixture, Id, Store};
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::Layer;

/// 保持の削除だけを失敗させるフック。
#[derive(Default)]
struct FailingDelete {
  armed: AtomicBool,
}

impl std::fmt::Debug for FailingDelete {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    formatter.debug_struct("FailingDelete").finish()
  }
}

impl SpannerTestHooks for FailingDelete {
  fn retention_delete(&self, _: &AidString, _: &[SeqNr]) -> Result<(), EventStoreError> {
    if self.armed.load(Ordering::SeqCst) {
      Err(EventStoreError::Storage {
        operation: StorageOperation::Append,
        source: Box::new(std::io::Error::other("RETENTION_FAILURE")),
      })
    } else {
      Ok(())
    }
  }
}

#[derive(Debug, Default, Clone)]
struct Observed {
  category: Option<String>,
  aid: Option<String>,
  phase: Option<String>,
}

impl Visit for Observed {
  fn record_str(&mut self, field: &Field, value: &str) {
    self.record(field, value.to_string());
  }

  fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
    self.record(field, format!("{value:?}"));
  }
}

impl Observed {
  fn record(&mut self, field: &Field, value: String) {
    match field.name() {
      "category" => self.category = Some(value),
      "aid" => self.aid = Some(value),
      "phase" => self.phase = Some(value),
      _ => {}
    }
  }
}

/// 保持の失敗の通知を受けたら、記録して panic する層。
struct PanickingLayer {
  calls: Arc<AtomicUsize>,
  seen: Arc<Mutex<Vec<Observed>>>,
}

impl<S: Subscriber> Layer<S> for PanickingLayer {
  fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
    if event.metadata().target() != "event_store_adapter::retention" {
      return;
    }
    let mut observed = Observed::default();
    event.record(&mut observed);
    self.calls.fetch_add(1, Ordering::SeqCst);
    self.seen.lock().unwrap().push(observed);
    panic!("notification subscriber failed");
  }
}

#[test]
fn should_spanner_committed_append_stay_successful_when_the_notification_subscriber_panics() {
  let calls = Arc::new(AtomicUsize::new(0));
  let seen = Arc::new(Mutex::new(Vec::new()));
  let subscriber = tracing_subscriber::registry().with(PanickingLayer {
    calls: calls.clone(),
    seen: seen.clone(),
  });
  let runtime = tokio::runtime::Builder::new_current_thread()
    .enable_all()
    .build()
    .unwrap();

  tracing::subscriber::with_default(subscriber, || {
    runtime.block_on(async {
      let options = options_keeping(1);
      let fixture = Fixture::initialized(&options).await;
      let hooks = Arc::new(FailingDelete::default());
      let store = Store::open_with_hooks(
        fixture.connection().await,
        fixture.tables.clone(),
        options,
        Arc::new(JsonEventSerializer::new()),
        Arc::new(JsonSnapshotSerializer::new()),
        hooks.clone(),
      )
      .await
      .unwrap();
      let id = Id::order();
      store
        .persist_event_and_snapshot(event(&id, 1), snapshot(1))
        .await
        .unwrap();
      assert_eq!(calls.load(Ordering::SeqCst), 0, "削除に成功する間は通知しない");
      hooks.armed.store(true, Ordering::SeqCst);

      let result = store.persist_event_and_snapshot(event(&id, 2), snapshot(2)).await;

      assert!(result.is_ok(), "通知の層が panic しても追記は成功のまま: {result:?}");
      assert_eq!(calls.load(Ordering::SeqCst), 1, "通知の層が実際に 1 回呼ばれた");
      let observed = seen.lock().unwrap().clone();
      assert_eq!(observed.len(), 1);
      assert_eq!(observed[0].category.as_deref(), Some("retention-failure"));
      assert_eq!(observed[0].aid.as_deref(), Some("Order-1"));
      assert_eq!(observed[0].phase.as_deref(), Some("retention-delete"));
      assert_eq!(
        store.get_events_by_id_since_seq_nr(&id, 1).await.unwrap(),
        vec![event(&id, 1), event(&id, 2)],
        "確定済みのデータは保存されている"
      );
      assert_eq!(fixture.head_rows().await[0].seq_nr, 2);
      assert_eq!(fixture.current_rows().await[0].seq_nr, 2);
      let history: Vec<i64> = fixture.history_rows().await.iter().map(|row| row.row.seq_nr).collect();
      assert_eq!(history, vec![1, 2], "削除に失敗したので履歴は残る");
    });
  });
}
