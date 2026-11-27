#![cfg(feature = "dynamodb")]

mod caller_runtime_support;

use std::time::Duration;

use aws_sdk_dynamodb::config::{AsyncSleep, Sleep};
use aws_sdk_dynamodb::Client;
use caller_runtime_support::{assert_caller_not_blocked, measure_caller_stall, SlowProxy};
use chrono::{DateTime, Utc};
use event_store_adapter_rs::{
  AggregateId, DynamoDbOptions, DynamoDbTables, EventEnvelope, EventStoreForDynamoDB, SnapshotEnvelope,
};
use event_store_adapter_test_utils_rs::{docker, dynamodb};

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

type Store = EventStoreForDynamoDB<Id, u64, u64>;

#[derive(Debug)]
struct TokioSleep;

impl AsyncSleep for TokioSleep {
  fn sleep(&self, duration: Duration) -> Sleep {
    Sleep::new(tokio::time::sleep(duration))
  }
}

fn client(port: u16) -> Client {
  Client::from_conf(
    dynamodb::create_dynamodb_local_client(port)
      .config()
      .to_builder()
      .sleep_impl(TokioSleep)
      .build(),
  )
}

fn event(seq_nr: u64) -> EventEnvelope<Id, u64> {
  EventEnvelope::new(Id, seq_nr, DateTime::<Utc>::from_timestamp(0, 0).unwrap(), seq_nr)
}

/// DynamoDB Local と、その前に置いた遅らせる中継。ストアは中継を通して DynamoDB Local へ接続する。
struct Fixture {
  _container: docker::LocalContainer,
  proxy: SlowProxy,
  store: Store,
}

impl Fixture {
  async fn new() -> Self {
    let container = docker::dynamodb_local().unwrap();
    let names = dynamodb::TableNames {
      journal: "journal".into(),
      snapshot: "snapshot".into(),
      head: "head".into(),
      snapshot_history_index: "snapshot-history".into(),
    };
    dynamodb::create_tables(&client(container.port()), &names, false)
      .await
      .unwrap();
    let proxy = SlowProxy::start(container.port());
    let tables = DynamoDbTables {
      journal_table_name: names.journal,
      snapshot_table_name: names.snapshot,
      head_table_name: names.head,
      snapshot_history_index_name: names.snapshot_history_index,
    };
    let store = Store::open(client(proxy.port()), tables, DynamoDbOptions::default())
      .await
      .unwrap();
    Self {
      _container: container,
      proxy,
      store,
    }
  }
}

// `#[tokio::test]` のランタイムはスレッドが 1 つだけなので、そのスレッドを塞ぐと、ほかのタスクは進まない。
#[tokio::test]
async fn should_persist_event_keep_the_calling_runtime_thread_free_while_waiting_for_dynamodb() {
  let fixture = Fixture::new().await;
  fixture.proxy.delay_responses();

  let (result, stall) = measure_caller_stall(fixture.store.persist_event(event(1))).await;

  result.unwrap();
  assert_caller_not_blocked("persist_event", &stall);
}

#[tokio::test]
async fn should_persist_event_and_snapshot_keep_the_calling_runtime_thread_free_while_waiting_for_dynamodb() {
  let fixture = Fixture::new().await;
  fixture.proxy.delay_responses();

  let (result, stall) = measure_caller_stall(
    fixture
      .store
      .persist_event_and_snapshot(event(1), SnapshotEnvelope::new(1, 1)),
  )
  .await;

  result.unwrap();
  assert_caller_not_blocked("persist_event_and_snapshot", &stall);
}

#[tokio::test]
async fn should_get_events_keep_the_calling_runtime_thread_free_while_waiting_for_dynamodb() {
  let fixture = Fixture::new().await;
  fixture.store.persist_event(event(1)).await.unwrap();
  fixture.store.persist_event(event(2)).await.unwrap();
  fixture.proxy.delay_responses();

  let (result, stall) = measure_caller_stall(fixture.store.get_events_by_id_since_seq_nr(&Id, 1)).await;

  assert_eq!(result.unwrap(), vec![event(1), event(2)]);
  assert_caller_not_blocked("get_events_by_id_since_seq_nr", &stall);
}

#[tokio::test]
async fn should_get_latest_snapshot_keep_the_calling_runtime_thread_free_while_waiting_for_dynamodb() {
  let fixture = Fixture::new().await;
  fixture
    .store
    .persist_event_and_snapshot(event(1), SnapshotEnvelope::new(1, 1))
    .await
    .unwrap();
  fixture.proxy.delay_responses();

  let (result, stall) = measure_caller_stall(fixture.store.get_latest_snapshot_by_id(&Id)).await;

  let read = result.unwrap().expect("ヘッドがある");
  assert_eq!(
    (read.head_seq_nr(), read.snapshot().map(SnapshotEnvelope::seq_nr)),
    (1, Some(1))
  );
  assert_caller_not_blocked("get_latest_snapshot_by_id", &stall);
}
