use chrono::{TimeZone, Utc};
use event_store_adapter_test_utils_rs::id_generator::id_generate;
use serde::{Deserialize, Serialize};

use crate::event_envelope::EventEnvelope;
use crate::event_store_for_memory::EventStoreForMemory;
use crate::event_store_test_support::{
  assert_optimistic_lock_message_format, exercise_user_account_flow, find_by_id, init_tracing, UserAccount,
  UserAccountEvent, UserAccountId, RENAMED_MANIFEST,
};
use crate::types::{EventStore, EventStoreWriteError};

fn new_memory_event_store() -> EventStoreForMemory<UserAccountId, UserAccount, UserAccountEvent> {
  EventStoreForMemory::new()
}

fn renamed_envelope(
  id: &UserAccountId,
  seq_nr: usize,
  payload: UserAccountEvent,
) -> EventEnvelope<UserAccountId, UserAccountEvent> {
  EventEnvelope::new(id.clone(), seq_nr, Utc::now(), payload).with_manifest(RENAMED_MANIFEST)
}

// FR7.1 / AC2.1系: 共有シナリオ（8 手順・封筒メタデータ往復 assert 含む）が Memory で green
#[tokio::test]
async fn test_event_store_on_memory() {
  init_tracing();

  let mut event_store = new_memory_event_store();
  let id = UserAccountId::new(id_generate().to_string());

  exercise_user_account_flow(&mut event_store, &id)
    .await
    .expect("scenario failed");
}

// BR2.2: イベントのみ API への seq_nr == 1（新規作成）は ContractViolation（panic しない — BR5.1）
#[tokio::test]
async fn test_event_store_on_memory_persist_event_rejects_creation_event() {
  let mut event_store = new_memory_event_store();
  let id = UserAccountId::new(id_generate().to_string());
  let (_user_account, created) = UserAccount::new(id.clone(), "test".to_string());
  let envelope = EventEnvelope::new(id, 1, Utc::now(), created);

  let result = event_store.persist_event(envelope, 0).await;
  assert!(matches!(result, Err(EventStoreWriteError::ContractViolation(_))));
}

// BR4.1: keep_snapshot_count = Some(0) はビルダーで拒否される（ラッパーの Result 素通し — P3）
#[tokio::test]
async fn test_event_store_on_memory_with_keep_snapshot_count_zero_is_rejected() {
  let result = new_memory_event_store().with_keep_snapshot_count(Some(0));
  assert!(matches!(result, Err(EventStoreWriteError::ContractViolation(_))));

  let store = new_memory_event_store()
    .with_keep_snapshot_count(Some(1))
    .expect("Some(1) is valid");
  assert_eq!(store.maintenance().keep_snapshot_count, Some(1));
}

// NFR3.2 / AC2.2系: 楽観ロックエラーの統一書式契約（許可キーのみ）を固定する回帰テスト
#[tokio::test]
async fn test_event_store_on_memory_optimistic_lock_error_contract() {
  let mut store_a = new_memory_event_store();
  let mut store_b = store_a.clone();
  let id = UserAccountId::new(id_generate().to_string());
  let (user_account, created) = UserAccount::new(id.clone(), "test".to_string());
  store_a
    .persist_event_and_snapshot(EventEnvelope::new(id.clone(), 1, Utc::now(), created), user_account, 0)
    .await
    .unwrap();

  let mut account_a = find_by_id(&store_a, &id).await.unwrap().unwrap();
  let mut account_b = find_by_id(&store_b, &id).await.unwrap().unwrap();

  let event_a = account_a.state.rename("first").unwrap();
  store_a
    .persist_event(renamed_envelope(&id, account_a.seq_nr + 1, event_a), account_a.version)
    .await
    .unwrap();

  // 同一バージョン(1)への後発の書込みは統一書式の OptimisticLockError で失敗する
  let event_b = account_b.state.rename("second").unwrap();
  let result = store_b
    .persist_event(renamed_envelope(&id, account_b.seq_nr + 1, event_b), account_b.version)
    .await;
  match result {
    Err(EventStoreWriteError::OptimisticLockError(message)) => {
      assert_eq!(
        message,
        format!(
          "optimistic lock failed, aid={}, expected_version=1, actual_version=2",
          id
        )
      );
      assert_optimistic_lock_message_format(&message);
    }
    other => panic!("expected OptimisticLockError, got {:?}", other),
  }
}

// NFR4.1 / AC2.2.1: 同時更新の競合パス — 一方のみ成功し他方が OptimisticLockError になる
#[tokio::test]
async fn test_event_store_on_memory_concurrent_conflict_yields_optimistic_lock_error() {
  let store = new_memory_event_store();
  let id = UserAccountId::new(id_generate().to_string());
  let (user_account, created) = UserAccount::new(id.clone(), "test".to_string());
  {
    let mut writer = store.clone();
    writer
      .persist_event_and_snapshot(EventEnvelope::new(id.clone(), 1, Utc::now(), created), user_account, 0)
      .await
      .unwrap();
  }

  let mut store_a = store.clone();
  let mut store_b = store.clone();
  let mut account_a = find_by_id(&store_a, &id).await.unwrap().unwrap();
  let mut account_b = find_by_id(&store_b, &id).await.unwrap().unwrap();
  let envelope_a = renamed_envelope(&id, account_a.seq_nr + 1, account_a.state.rename("conflict-a").unwrap());
  let envelope_b = renamed_envelope(&id, account_b.seq_nr + 1, account_b.state.rename("conflict-b").unwrap());
  let version_a = account_a.version;
  let version_b = account_b.version;

  let (result_a, result_b) = tokio::join!(
    async move { store_a.persist_event(envelope_a, version_a).await },
    async move { store_b.persist_event(envelope_b, version_b).await },
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
      assert_optimistic_lock_message_format(&message);
    }
    other => panic!("expected OptimisticLockError, got {:?}", other),
  }
}

// クローン共有契約: クローン間で同一ストアを共有する（Clone時の状態分岐を作らない）
#[tokio::test]
async fn test_event_store_on_memory_clone_shares_state() {
  let mut original = new_memory_event_store();
  let mut cloned = original.clone();
  let id = UserAccountId::new(id_generate().to_string());
  let (user_account, created) = UserAccount::new(id.clone(), "test".to_string());
  original
    .persist_event_and_snapshot(EventEnvelope::new(id.clone(), 1, Utc::now(), created), user_account, 0)
    .await
    .unwrap();

  // クローン側から元の書込みが見える
  let mut from_clone = find_by_id(&cloned, &id)
    .await
    .unwrap()
    .expect("clone must see the original's write");
  assert_eq!(from_clone.version, 1);

  // クローン側の書込みが元から見える（Clone間の状態分岐が存在しない）
  let event = from_clone.state.rename("renamed-via-clone").unwrap();
  cloned
    .persist_event(renamed_envelope(&id, from_clone.seq_nr + 1, event), from_clone.version)
    .await
    .unwrap();

  let from_original = find_by_id(&original, &id).await.unwrap().unwrap();
  assert_eq!(from_original.version, 2);
  assert_eq!(from_original.seq_nr, 2);
  assert_eq!(from_original.state.name, "renamed-via-clone");
}

// FR7.3 ③ / NFR2 / US2.2: Memory 経由の永続化検証 — derive(Serialize, Deserialize, Clone) のみの
// プレーン型（Debug なし — BR1.6 の Memory 非対称は +Clone のみ）で W1〜W4 を一気通貫する
#[tokio::test]
async fn test_event_store_on_memory_persists_plain_clone_type_end_to_end() {
  #[derive(Serialize, Deserialize, Clone)]
  struct PlainAggregate {
    name: String,
  }

  #[derive(Serialize, Deserialize, Clone)]
  struct PlainPayload {
    name: String,
  }

  let mut store: EventStoreForMemory<UserAccountId, PlainAggregate, PlainPayload> = EventStoreForMemory::new();
  let id = UserAccountId::new(id_generate().to_string());
  let occurred_at = Utc.with_ymd_and_hms(2026, 8, 27, 0, 0, 0).unwrap();

  // W1: 新規作成（seq_nr=1, expected_version=0）
  store
    .persist_event_and_snapshot(
      EventEnvelope::new(
        id.clone(),
        1,
        occurred_at,
        PlainPayload {
          name: "created".to_string(),
        },
      )
      .with_manifest("plain/v1"),
      PlainAggregate {
        name: "one".to_string(),
      },
      0,
    )
    .await
    .unwrap();

  // W3: 更新（スナップショット付き、seq_nr=2, expected_version=1）
  store
    .persist_event_and_snapshot(
      EventEnvelope::new(
        id.clone(),
        2,
        occurred_at,
        PlainPayload {
          name: "renamed".to_string(),
        },
      ),
      PlainAggregate {
        name: "two".to_string(),
      },
      1,
    )
    .await
    .unwrap();

  // W2: イベントのみ更新（seq_nr=3, expected_version=2）
  store
    .persist_event(
      EventEnvelope::new(
        id.clone(),
        3,
        occurred_at,
        PlainPayload {
          name: "renamed-again".to_string(),
        },
      ),
      2,
    )
    .await
    .unwrap();

  // W4: 復元 — スナップショット封筒 + 差分イベント封筒の読取
  let snapshot = store
    .get_latest_snapshot_by_id(&id)
    .await
    .unwrap()
    .expect("snapshot must exist");
  assert_eq!(snapshot.version(), 3);
  assert_eq!(snapshot.seq_nr(), 2);
  assert_eq!(snapshot.aggregate().name, "two");

  let events = store
    .get_events_by_id_since_seq_nr(&id, snapshot.seq_nr() + 1)
    .await
    .unwrap();
  assert_eq!(events.len(), 1);
  assert_eq!(events[0].seq_nr(), 3);
  assert_eq!(events[0].manifest(), "");
  assert_eq!(events[0].payload().name, "renamed-again");
}
