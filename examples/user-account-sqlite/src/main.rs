use std::env;

use chrono::Utc;
use event_store_adapter_rs::event_envelope::EventEnvelope;
use event_store_adapter_rs::EventStoreForSqlite;

use crate::user_account::{id_generate, UserAccount, UserAccountId, CREATED_MANIFEST, RENAMED_MANIFEST};
use crate::user_account_repository::{RepositoryError, UserAccountRepository};

mod user_account;
mod user_account_repository;

// FR8.4: v3 envelope-API walkthrough. Creation persists a seq_nr=1 envelope with
// expected_version=0; updates number the next envelope from the replayed seq_nr and pass the
// replayed version as expected_version (BR2.3 / BR2.6).

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
