use std::env;
use std::thread::sleep;
use std::time::Duration;

use anyhow::Result;
use testcontainers::clients::Cli;

use event_store_adapter_rs::event_store::EventStoreForDynamoDB;
use event_store_adapter_rs::types::Aggregate;
use event_store_adapter_test_utils_rs::docker::dynamodb_local;
use event_store_adapter_test_utils_rs::dynamodb::{create_client, create_journal_table, create_snapshot_table};
use event_store_adapter_test_utils_rs::id_generator::id_generate;

use crate::user_account::{UserAccount, UserAccountId, UserAccountRepository};

mod user_account;

#[tokio::main]
async fn main() {
  env::set_var("RUST_LOG", "info");
  env_logger::init();

  let docker = Cli::docker();
  let dynamodb_node = dynamodb_local(&docker);
  let port = dynamodb_node.get_host_port_ipv4(4566);
  log::debug!("DynamoDB port: {}", port);

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
  log::info!("1: user_account = {:?}", user_account);

  rename_user_account(&mut repository, &user_account_id, "test-2")
    .await
    .unwrap();
  let user_account = repository.find_by_id(&user_account_id).await.unwrap();
  log::info!("2: user_account = {:?}", user_account);
}

async fn create_user_account(repository: &mut UserAccountRepository, id: &str, name: &str) -> Result<UserAccountId> {
  let user_account_id = UserAccountId::new(id.to_string());
  let (user_account, user_account_event) = UserAccount::new(user_account_id.clone(), name.to_string()).unwrap();
  repository
    .store(&user_account_event, user_account.version(), Some(&user_account))
    .await?;
  Ok(user_account_id)
}

async fn rename_user_account(
  repository: &mut UserAccountRepository,
  user_account_id: &UserAccountId,
  name: &str,
) -> Result<()> {
  let mut user_account = repository.find_by_id(user_account_id).await.unwrap().unwrap();
  let user_account_event = user_account.rename(name).unwrap();
  repository
    .store(&user_account_event, user_account.version(), None)
    .await
}
