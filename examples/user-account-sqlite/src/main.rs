use std::env;

use event_store_adapter_rs::types::Aggregate;
use event_store_adapter_rs::EventStoreForSqlite;

use crate::user_account::{id_generate, UserAccount, UserAccountId};
use crate::user_account_repository::{RepositoryError, UserAccountRepository};

mod user_account;
mod user_account_repository;

#[tokio::main]
async fn main() {
  let log_level = env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string());
  let subscriber = tracing_subscriber::fmt()
    .with_env_filter(log_level)
    .with_target(false)
    .with_ansi(false)
    .without_time()
    .finish();
  tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

  // No cloud connection and no Docker: the store runs on an in-memory SQLite database and
  // creates its tables automatically. Use `EventStoreForSqlite::new("<path>.db")` for a file DB.
  let event_store = EventStoreForSqlite::new_in_memory().expect("failed to open the in-memory SQLite database");

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
  let (user_account, user_account_event) = UserAccount::new(user_account_id.clone(), name.to_string());
  repository
    .store_event_and_snapshot(&user_account_event, &user_account)
    .await?;
  Ok(user_account_id)
}

async fn rename_user_account(
  repository: &mut UserAccountRepository,
  user_account_id: &UserAccountId,
  name: &str,
) -> Result<(), RepositoryError> {
  let mut user_account = repository.find_by_id(user_account_id).await?.unwrap();
  let user_account_event = user_account.rename(name).unwrap();
  repository
    .store_event(&user_account_event, user_account.version())
    .await
}
