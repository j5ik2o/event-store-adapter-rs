use std::env;
use std::thread::sleep;
use std::time::Duration;

use chrono::Utc;
use event_store_adapter_rs::event_envelope::EventEnvelope;
use event_store_adapter_rs::EventStoreForDynamoDB;
use event_store_adapter_test_utils_rs::docker::dynamodb_local;
use event_store_adapter_test_utils_rs::dynamodb::{create_client, create_journal_table, create_snapshot_table};
use event_store_adapter_test_utils_rs::id_generator::id_generate;

use crate::user_account::{UserAccount, UserAccountId, CREATED_MANIFEST, RENAMED_MANIFEST};
use crate::user_account_repository::{RepositoryError, UserAccountRepository};

mod user_account;
mod user_account_repository;

// FR8.4: v3 envelope-API walkthrough. Creation persists a seq_nr=1 envelope with
// expected_version=0; updates number the next envelope from the replayed seq_nr and pass the
// replayed version as expected_version (BR2.3 / BR2.6).

#[tokio::main]
async fn main() {
  let log_level = match env::var("LOG_LEVEL") {
    Ok(level) => level,
    Err(_) => "info".to_string(),
  };
  let subscriber = tracing_subscriber::fmt()
    .with_env_filter(log_level)
    .with_target(false)
    .with_ansi(false)
    .without_time()
    .finish();
  tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

  let dynamodb_node = dynamodb_local().await;
  let port = dynamodb_node
    .get_host_port_ipv4(4566)
    .await
    .expect("Failed to get port");
  tracing::debug!("DynamoDB port: {}", port);

  let test_time_factor = env::var("TEST_TIME_FACTOR")
    .unwrap_or("1".to_string())
    .parse::<f32>()
    .unwrap();

  sleep(Duration::from_millis((1000f32 * test_time_factor) as u64));

  let client = create_client(port);

  let journal_table_name = "journal";
  let journal_aid_index_name = "journal-aid-index";
  let _journal_table_output = create_journal_table(&client, journal_table_name, journal_aid_index_name).await;

  let snapshot_table_name = "snapshot";
  let snapshot_aid_index_name = "snapshot-aid-index";
  let _snapshot_table_output = create_snapshot_table(&client, snapshot_table_name, snapshot_aid_index_name).await;

  let event_store = EventStoreForDynamoDB::new(
    client.clone(),
    journal_table_name.to_string(),
    journal_aid_index_name.to_string(),
    snapshot_table_name.to_string(),
    snapshot_aid_index_name.to_string(),
    64,
  );

  let mut repository = UserAccountRepository::new(event_store);

  let id = id_generate();

  let user_account_id = create_user_account(&mut repository, &id.to_string(), "test-1")
    .await
    .unwrap();
  let user_account = repository.find_by_id(&user_account_id).await.unwrap();
  tracing::info!("1: user_account = {:?}", user_account);

  match rename_user_account(&mut repository, &user_account_id, "test-2").await {
    Ok(_) => (),
    Err(e) => tracing::error!("Failed to rename user account: {:?}", e),
  }

  let user_account = repository.find_by_id(&user_account_id).await.unwrap();
  tracing::info!("2: user_account = {:?}", user_account);
}

async fn create_user_account(
  repository: &mut UserAccountRepository,
  id: &str,
  name: &str,
) -> Result<UserAccountId, RepositoryError> {
  let user_account_id = UserAccountId::new(id.to_string());
  let (user_account, created) = UserAccount::new(user_account_id.clone(), name.to_string());
  // The first event of a stream is seq_nr == 1 and is written with expected_version == 0 (BR2.6).
  let envelope = EventEnvelope::new(user_account_id.clone(), 1, Utc::now(), created).with_manifest(CREATED_MANIFEST);
  repository.store_event_and_snapshot(envelope, user_account, 0).await?;
  Ok(user_account_id)
}

async fn rename_user_account(
  repository: &mut UserAccountRepository,
  user_account_id: &UserAccountId,
  name: &str,
) -> Result<(), RepositoryError> {
  let mut replayed = repository.find_by_id(user_account_id).await?.unwrap();
  let renamed = replayed.state.rename(name).unwrap();
  // The domain numbers the next event as replayed seq_nr + 1 (FR3.3) and passes the replayed
  // version as expected_version for the optimistic lock (FR2.2).
  let envelope = EventEnvelope::new(user_account_id.clone(), replayed.seq_nr + 1, Utc::now(), renamed)
    .with_manifest(RENAMED_MANIFEST);
  repository.store_event(envelope, replayed.version).await
}
