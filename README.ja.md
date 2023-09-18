# event-store-adapter-rs

[![Workflow Status](https://github.com/j5ik2o/event-store-adapter-rs/workflows/ci/badge.svg)](https://github.com/j5ik2o/event-store-adapter-rs/actions?query=workflow%3A%22ci%22)
[![crates.io](https://img.shields.io/crates/v/event-store-adapter-rs.svg)](https://crates.io/crates/event-store-adapter-rs)
[![docs.rs](https://docs.rs/event-store-adapter-rs/badge.svg)](https://docs.rs/event-store-adapter-rs)
[![dependency status](https://deps.rs/repo/github/j5ik2o/event-store-adapter-rs/status.svg)](https://deps.rs/repo/github/j5ik2o/event-store-adapter-rs)
[![tokei](https://tokei.rs/b1/github/j5ik2o/event-store-adapter-rs)](https://github.com/XAMPPRocky/tokei)

このライブラリは、DynamoDBをEvent Sourcing用のEvent Storeにするためのものです。

[English](./README.md)

## 使い方

EventStoreを使えば、Event Sourcing対応リポジトリを簡単に実装できます。

```rust
pub struct UserAccountRepository {
  event_store: EventStore<UserAccount, UserAccountEvent>,
}

impl UserAccountRepository {
  pub fn new(event_store: EventStore<UserAccount, UserAccountEvent>) -> Self {
    Self { event_store }
  }

  pub async fn store_event(&mut self, event: &UserAccountEvent, version: usize) -> Result<()> {
    return self.event_store.persist_event(event, version).await;
  }

  pub async fn store_event_and_snapshot(&mut self, event: &UserAccountEvent, snapshot: &UserAccount) -> Result<()> {
    return self.event_store.persist_event_and_snapshot(event, snapshot).await;
  }
    
  pub async fn find_by_id(&self, id: &UserAccountId) -> Result<UserAccount> {
    let snapshot = self.event_store.get_latest_snapshot_by_id(id).await?;
    match snapshot {
      Some(snapshot) => {
        let events = self.event_store
          .get_events_by_id_since_seq_nr(id, snapshot.seq_nr)
          .await?;
        let result = UserAccount::replay(events, snapshot);
        Ok(Some(result))
      }
      None => Ok(None),
    }
  }

}

```

以下はリポジトリの使用例です。

```rust
let event_store = EventStore::new(
  aws_dynamodb_client.clone(),
  journal_table_name.to_string(),
  journal_aid_index_name.to_string(),
  snapshot_table_name.to_string(),
  snapshot_aid_index_name.to_string(),
  64,
);
 
let mut repository = UserAccountRepository::new(event_store);
 
// Replay the aggregate from the event store
let mut user_account = repository.find_by_id(user_account_id).await.unwrap();

// Execute a command on the aggregate
let user_account_event = user_account.rename(name).unwrap();
 
// Store the new event without a snapshot
repository
  .store_event(&user_account_event, user_account.version())
  .await
// Store the new event with a snapshot
//  repository
//  .store_event_and_snapshot(&user_account_event, &user_account)
//  .await
```

## テーブル仕様

See [docs/DATABASE_SCHEMA.ja.md](docs/DATABASE_SCHEMA.ja.md).
