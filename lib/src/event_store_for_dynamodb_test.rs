use event_store_adapter_test_utils_rs::docker::dynamodb_local;
use event_store_adapter_test_utils_rs::dynamodb::{
  create_client, create_journal_table, create_snapshot_table, wait_table,
};
use event_store_adapter_test_utils_rs::id_generator::id_generate;
use std::env;
use std::thread::sleep;
use std::time::Duration as StdDuration;

use crate::event_store_for_dynamodb::EventStoreForDynamoDB;
use crate::event_store_test_support::{exercise_user_account_flow, init_tracing, UserAccountId};

#[tokio::test]
async fn test_event_store_on_dynamodb() {
  env::set_var("RUST_LOG", "event_store_adapter_rs=debug");
  init_tracing();

  let dynamodb_node = dynamodb_local().await;
  let port = dynamodb_node
    .get_host_port_ipv4(4566)
    .await
    .expect("Failed to get port");

  let test_time_factor = env::var("TEST_TIME_FACTOR")
    .unwrap_or("1".to_string())
    .parse::<f32>()
    .unwrap();

  sleep(StdDuration::from_millis((1000f32 * test_time_factor) as u64));

  let client = create_client(port);

  let journal_table_name = "journal";
  let journal_aid_index_name = "journal-aid-index";
  let _ = create_journal_table(&client, journal_table_name, journal_aid_index_name).await;

  let snapshot_table_name = "snapshot";
  let snapshot_aid_index_name = "snapshot-aid-index";
  let _ = create_snapshot_table(&client, snapshot_table_name, snapshot_aid_index_name).await;

  while !(wait_table(&client, journal_table_name).await) {
    sleep(StdDuration::from_millis((1000f32 * test_time_factor) as u64));
  }
  while !(wait_table(&client, snapshot_table_name).await) {
    sleep(StdDuration::from_millis((1000f32 * test_time_factor) as u64));
  }

  let mut event_store = EventStoreForDynamoDB::new(
    client.clone(),
    journal_table_name.to_string(),
    journal_aid_index_name.to_string(),
    snapshot_table_name.to_string(),
    snapshot_aid_index_name.to_string(),
    64,
  )
  .with_keep_snapshot_count(Some(1))
  .with_delete_ttl(Some(chrono::Duration::seconds(5)));

  let id_value = id_generate();
  let id = UserAccountId::new(id_value.to_string());

  exercise_user_account_flow(&mut event_store, &id)
    .await
    .expect("scenario failed");
}
