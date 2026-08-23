use event_store_adapter_test_utils_rs::id_generator::id_generate;

use crate::event_store_for_memory::EventStoreForMemory;
use crate::event_store_test_support::{
  exercise_user_account_flow, find_by_id, init_tracing, UserAccount, UserAccountEvent, UserAccountId,
};
use crate::types::{Aggregate, EventStore, EventStoreWriteError};

fn new_memory_event_store() -> EventStoreForMemory<UserAccountId, UserAccount, UserAccountEvent> {
  EventStoreForMemory::new()
}

#[tokio::test]
async fn test_event_store_on_memory() {
  init_tracing();

  let mut event_store = new_memory_event_store();
  let id = UserAccountId::new(id_generate().to_string());

  exercise_user_account_flow(&mut event_store, &id)
    .await
    .expect("scenario failed");
}

#[tokio::test]
async fn test_event_store_on_memory_persist_event_rejects_creation_event() {
  let mut event_store = new_memory_event_store();
  let id = UserAccountId::new(id_generate().to_string());
  let (user_account, created) = UserAccount::new(id, "test".to_string());

  // AC3.1.1: 作成イベントの persist_event 渡しは panic せず Err を返す
  let result = event_store.persist_event(&created, user_account.version()).await;
  assert!(matches!(result, Err(EventStoreWriteError::OtherError(_))));
}

#[tokio::test]
async fn test_event_store_on_memory_optimistic_lock_error_contract() {
  let mut store_a = new_memory_event_store();
  let mut store_b = store_a.clone();
  let id = UserAccountId::new(id_generate().to_string());
  let (user_account, created) = UserAccount::new(id.clone(), "test".to_string());
  store_a
    .persist_event_and_snapshot(&created, &user_account)
    .await
    .unwrap();

  let mut account_a = find_by_id(&mut store_a, &id).await.unwrap().unwrap();
  let mut account_b = find_by_id(&mut store_b, &id).await.unwrap().unwrap();

  let event_a = account_a.rename("first").unwrap();
  store_a.persist_event(&event_a, account_a.version()).await.unwrap();

  // 同一バージョン(1)への後発の書込みは BR1.2 書式の OptimisticLockError で失敗する
  let event_b = account_b.rename("second").unwrap();
  let result = store_b.persist_event(&event_b, account_b.version()).await;
  match result {
    Err(EventStoreWriteError::OptimisticLockError(message)) => {
      assert_eq!(
        message,
        format!(
          "optimistic lock failed, aid={}, expected_version=1, actual_version=2",
          id
        )
      );
    }
    other => panic!("expected OptimisticLockError, got {:?}", other),
  }
}

#[tokio::test]
async fn test_event_store_on_memory_concurrent_conflict_yields_optimistic_lock_error() {
  let store = new_memory_event_store();
  let id = UserAccountId::new(id_generate().to_string());
  let (user_account, created) = UserAccount::new(id.clone(), "test".to_string());
  {
    let mut writer = store.clone();
    writer
      .persist_event_and_snapshot(&created, &user_account)
      .await
      .unwrap();
  }

  let mut store_a = store.clone();
  let mut store_b = store.clone();
  let mut account_a = find_by_id(&mut store_a, &id).await.unwrap().unwrap();
  let mut account_b = find_by_id(&mut store_b, &id).await.unwrap().unwrap();
  let event_a = account_a.rename("conflict-a").unwrap();
  let event_b = account_b.rename("conflict-b").unwrap();
  let version_a = account_a.version();
  let version_b = account_b.version();

  let (result_a, result_b) = tokio::join!(
    async move { store_a.persist_event(&event_a, version_a).await },
    async move { store_b.persist_event(&event_b, version_b).await },
  );

  let err = match (result_a, result_b) {
    (Err(err), Ok(())) | (Ok(()), Err(err)) => err,
    (Ok(()), Ok(())) => panic!("exactly one concurrent write must win, but both succeeded"),
    (Err(err_a), Err(err_b)) => panic!(
      "exactly one concurrent write must win, but both failed: {:?} / {:?}",
      err_a, err_b
    ),
  };
  match err {
    EventStoreWriteError::OptimisticLockError(message) => {
      assert!(
        message.starts_with(&format!("optimistic lock failed, aid={}, expected_version=1", id)),
        "unexpected message: {}",
        message
      );
    }
    other => panic!("expected OptimisticLockError, got {:?}", other),
  }
}

#[tokio::test]
async fn test_event_store_on_memory_clone_shares_state() {
  let mut original = new_memory_event_store();
  let mut cloned = original.clone();
  let id = UserAccountId::new(id_generate().to_string());
  let (user_account, created) = UserAccount::new(id.clone(), "test".to_string());
  original
    .persist_event_and_snapshot(&created, &user_account)
    .await
    .unwrap();

  // クローン側から元の書込みが見える
  let mut from_clone = find_by_id(&mut cloned, &id)
    .await
    .unwrap()
    .expect("clone must see the original's write");
  assert_eq!(from_clone.version(), 1);

  // クローン側の書込みが元から見える（Clone間の状態分岐が存在しない）
  let event = from_clone.rename("renamed-via-clone").unwrap();
  cloned.persist_event(&event, from_clone.version()).await.unwrap();

  let from_original = find_by_id(&mut original, &id).await.unwrap().unwrap();
  assert_eq!(from_original.version(), 2);
  assert_eq!(from_original.seq_nr(), 2);
}
