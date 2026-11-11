#![cfg(feature = "sqlite")]
// 購読者を 1 度も設定していないプロセスを試すので、この試験だけを 1 つの試験のバイナリに置く。

mod sqlite_support;

use std::num::NonZeroUsize;

use chrono::{DateTime, Utc};
use event_store_adapter_rs::{
  initialize_sqlite_store, AggregateId, BlockingExecution, EventEnvelope, EventStore, EventStoreForSqlite,
  RetentionSettings, SnapshotEnvelope, SqliteOptions,
};
use sqlite_support::TempDatabase;

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

// `tracing` は、購読者が 1 度も設定されていないときだけ、イベントを `log` へ出す。
// オフロードした操作が購読者を設定すると、プロセスの全てのイベントがそれ以後 `log` に出なくなる。
#[tokio::test]
async fn should_offloaded_store_leave_a_process_without_a_tracing_subscriber_untouched() {
  let database = TempDatabase::new();
  initialize_sqlite_store(database.path(), &options()).unwrap();
  let store = Store::open_offloaded(database.path(), options(), &execution()).unwrap();

  store.persist_event(event(1)).await.unwrap();
  store
    .persist_event_and_snapshot(event(2), SnapshotEnvelope::new(2, 2))
    .await
    .unwrap();
  store.get_latest_snapshot_by_id(&Id).await.unwrap();
  store.get_events_by_id_since_seq_nr(&Id, 1).await.unwrap();

  assert!(
    !tracing::dispatcher::has_been_set(),
    "オフロードした操作は、tracing の購読者を設定しない（log への出力が続く）"
  );
}
