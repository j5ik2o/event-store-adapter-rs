use std::env;

use aws_sdk_dynamodb::config::{AsyncSleep, Sleep};
use chrono::Utc;
use event_store_adapter_rs::{DynamoDbOptions, DynamoDbTables, EventEnvelope, EventStoreForDynamoDB, SnapshotEnvelope};
use event_store_adapter_test_utils_rs::docker::{dynamodb_local, DYNAMODB_LOCAL_PORT};
use event_store_adapter_test_utils_rs::dynamodb::{create_dynamodb_local_client, create_tables, TableNames};
use event_store_adapter_test_utils_rs::id_generator::id_generate;

use crate::user_account::{UserAccount, UserAccountId, CREATED_MANIFEST, RENAMED_MANIFEST};
use crate::user_account_repository::{RepositoryError, UserAccountRepository};

mod user_account;
mod user_account_repository;

#[derive(Debug)]
struct LocalSleep;

impl AsyncSleep for LocalSleep {
  fn sleep(&self, duration: std::time::Duration) -> Sleep {
    Sleep::new(tokio::time::sleep(duration))
  }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  let subscriber = tracing_subscriber::fmt()
    .with_env_filter(env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string()))
    .with_target(false)
    .with_ansi(false)
    .without_time()
    .finish();
  tracing::subscriber::set_global_default(subscriber)?;

  let _container = dynamodb_local().await?;
  let port = _container.get_host_port_ipv4(DYNAMODB_LOCAL_PORT).await?;
  let client = aws_sdk_dynamodb::Client::from_conf(
    create_dynamodb_local_client(port)
      .config()
      .to_builder()
      .sleep_impl(LocalSleep)
      .build(),
  );
  let names = TableNames {
    journal: "journal".into(),
    snapshot: "snapshot".into(),
    head: "head".into(),
    snapshot_history_index: "snapshot-history".into(),
  };
  create_tables(&client, &names, false).await?;
  let event_store = EventStoreForDynamoDB::open(
    client,
    DynamoDbTables {
      journal_table_name: names.journal,
      snapshot_table_name: names.snapshot,
      head_table_name: names.head,
      snapshot_history_index_name: names.snapshot_history_index,
    },
    DynamoDbOptions::default(),
  )
  .await?;
  let repository = UserAccountRepository::new(event_store);
  let id = create_user_account(&repository, &id_generate().to_string(), "test-1").await?;
  let first = repository
    .find_by_id(&id)
    .await?
    .ok_or_else(|| RepositoryError::NotFound(id.to_string()))?;
  assert_eq!(first.seq_nr, 1);
  assert_eq!(first.state, UserAccount::new(id.clone(), "test-1".into()).0);
  tracing::info!("event-only creation: {:?}", first);

  rename_user_account(&repository, &id, "test-2", true).await?;
  let second = repository
    .find_by_id(&id)
    .await?
    .ok_or_else(|| RepositoryError::NotFound(id.to_string()))?;
  assert_eq!(second.seq_nr, 2);
  assert_eq!(second.state, UserAccount::new(id.clone(), "test-2".into()).0);
  tracing::info!("snapshot at head: {:?}", second);

  rename_user_account(&repository, &id, "test-3", false).await?;
  let third = repository
    .find_by_id(&id)
    .await?
    .ok_or_else(|| RepositoryError::NotFound(id.to_string()))?;
  assert_eq!(third.seq_nr, 3);
  assert_eq!(third.state, UserAccount::new(id, "test-3".into()).0);
  tracing::info!("snapshot plus events: {:?}", third);
  Ok(())
}

async fn create_user_account(
  repository: &UserAccountRepository,
  id: &str,
  name: &str,
) -> Result<UserAccountId, RepositoryError> {
  let id = UserAccountId::new(id.to_string());
  let (_, created) = UserAccount::new(id.clone(), name.to_string());
  let event = EventEnvelope::new(id.clone(), 1, Utc::now(), created).with_manifest(CREATED_MANIFEST);
  repository.store_event(event).await?;
  Ok(id)
}

async fn rename_user_account(
  repository: &UserAccountRepository,
  id: &UserAccountId,
  name: &str,
  with_snapshot: bool,
) -> Result<(), RepositoryError> {
  let mut replayed = repository
    .find_by_id(id)
    .await?
    .ok_or_else(|| RepositoryError::NotFound(id.to_string()))?;
  let renamed = replayed.state.rename(name)?;
  let seq_nr = replayed.seq_nr + 1;
  let event = EventEnvelope::new(id.clone(), seq_nr, Utc::now(), renamed).with_manifest(RENAMED_MANIFEST);
  if with_snapshot {
    repository
      .store_event_and_snapshot(event, SnapshotEnvelope::new(replayed.state, seq_nr))
      .await
  } else {
    repository.store_event(event).await
  }
}

#[cfg(test)]
#[path = "main_test.rs"]
mod tests;
