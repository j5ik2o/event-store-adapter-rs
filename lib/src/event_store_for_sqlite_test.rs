use std::path::{Path, PathBuf};

use event_store_adapter_test_utils_rs::id_generator::id_generate;

use crate::event_store_for_sqlite::EventStoreForSqlite;
use crate::event_store_test_support::{
  exercise_user_account_flow, find_by_id, init_tracing, UserAccount, UserAccountEvent, UserAccountId,
};
use crate::types::{Aggregate, EventStore, EventStoreWriteError};

// 一時ディレクトリ＋一意名のファイルDB。Dropで自テスト作成分のみ後始末する
// （Docker/testcontainers不要・決定的・並列安全 — AC3.2.2）
struct TempDb {
  path: PathBuf,
}

impl TempDb {
  fn new() -> Self {
    Self {
      path: std::env::temp_dir().join(format!("event-store-adapter-rs-sqlite-test-{}.db", id_generate())),
    }
  }
}

impl Drop for TempDb {
  fn drop(&mut self) {
    let _ = std::fs::remove_file(&self.path);
  }
}

fn new_file_event_store(path: &Path) -> EventStoreForSqlite<UserAccountId, UserAccount, UserAccountEvent> {
  EventStoreForSqlite::new(path).expect("failed to open sqlite event store")
}

#[tokio::test]
async fn test_event_store_on_sqlite() {
  init_tracing();

  let db = TempDb::new();
  // AC1.1.1: 空のDBファイルからスキーマ自動作成のうえ永続化・復元が成功する
  std::fs::File::create(&db.path).expect("failed to create empty db file");
  let mut event_store = new_file_event_store(&db.path);
  let id = UserAccountId::new(id_generate().to_string());

  exercise_user_account_flow(&mut event_store, &id)
    .await
    .expect("scenario failed");
}

#[tokio::test]
async fn test_event_store_on_sqlite_in_memory() {
  // AC1.3.1: `:memory:` でも同一インスタンス上で永続化・リプレイが一貫する
  let mut event_store: EventStoreForSqlite<UserAccountId, UserAccount, UserAccountEvent> =
    EventStoreForSqlite::new_in_memory().expect("failed to open in-memory sqlite event store");
  let id = UserAccountId::new(id_generate().to_string());

  exercise_user_account_flow(&mut event_store, &id)
    .await
    .expect("scenario failed");
}

#[tokio::test]
async fn test_event_store_on_sqlite_in_memory_clone_shares_state() {
  let mut original: EventStoreForSqlite<UserAccountId, UserAccount, UserAccountEvent> =
    EventStoreForSqlite::new_in_memory().expect("failed to open in-memory sqlite event store");
  let mut cloned = original.clone();
  let id = UserAccountId::new(id_generate().to_string());
  let (user_account, created) = UserAccount::new(id.clone(), "test".to_string());
  original
    .persist_event_and_snapshot(&created, &user_account)
    .await
    .unwrap();

  // AC1.3.1: クローン側から元の書込みが見える（Cloneは基底接続を共有 — BR2.5）
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

#[tokio::test]
async fn test_event_store_on_sqlite_file_reopen_restores_state() {
  let db = TempDb::new();
  let id = UserAccountId::new(id_generate().to_string());
  {
    let mut event_store = new_file_event_store(&db.path);
    let (user_account, created) = UserAccount::new(id.clone(), "test".to_string());
    event_store
      .persist_event_and_snapshot(&created, &user_account)
      .await
      .unwrap();
    let mut account = find_by_id(&mut event_store, &id).await.unwrap().unwrap();
    let event = account.rename("renamed-before-reopen").unwrap();
    event_store.persist_event(&event, account.version()).await.unwrap();
  }

  // AC1.3.2: ストアを破棄してファイルから再構築しても状態が復元される
  let mut reopened = new_file_event_store(&db.path);
  let restored = find_by_id(&mut reopened, &id)
    .await
    .unwrap()
    .expect("state must survive store reconstruction");
  assert_eq!(restored.version(), 2);
  assert_eq!(restored.seq_nr(), 2);
}

#[tokio::test]
async fn test_event_store_on_sqlite_optimistic_lock_conflict() {
  let db = TempDb::new();
  let store = new_file_event_store(&db.path);
  let id = UserAccountId::new(id_generate().to_string());
  let (user_account, created) = UserAccount::new(id.clone(), "test".to_string());
  {
    let mut writer = store.clone();
    writer
      .persist_event_and_snapshot(&created, &user_account)
      .await
      .unwrap();
  }

  // 同一DBを共有する2ハンドル（Cloneは基底接続共有）で同一バージョンから順次コミットする
  let mut store_a = store.clone();
  let mut store_b = store.clone();
  let mut account_a = find_by_id(&mut store_a, &id).await.unwrap().unwrap();
  let mut account_b = find_by_id(&mut store_b, &id).await.unwrap().unwrap();

  let event_a = account_a.rename("first").unwrap();
  store_a.persist_event(&event_a, account_a.version()).await.unwrap();

  // AC1.2.1: 後発の同一バージョンへの書込みは決定的に OptimisticLockError で失敗する
  let event_b = account_b.rename("second").unwrap();
  let result = store_b.persist_event(&event_b, account_b.version()).await;
  assert!(
    matches!(result, Err(EventStoreWriteError::OptimisticLockError(_))),
    "expected OptimisticLockError, got {:?}",
    result
  );

  // AC1.2.2: 失敗した書込みはロールバックされ journal に残骸を残さない（勝者のイベントのみ）
  let events = store_a.get_events_by_id_since_seq_nr(&id, 0).await.unwrap();
  assert_eq!(events.len(), 2);
  let replayed = find_by_id(&mut store_a, &id).await.unwrap().unwrap();
  assert_eq!(replayed.version(), 2);
  assert_eq!(replayed.seq_nr(), 2);
}

#[tokio::test]
async fn test_event_store_on_sqlite_error_contract() {
  let db = TempDb::new();
  let mut store_a = new_file_event_store(&db.path);
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

  // AC3.2.3: 同一バージョン(1)への後発の書込みは actual_version 付きBR1.2書式で失敗する
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

  // BR2.3: 既存集約への重複作成もBR1.2書式の OptimisticLockError（AWS型リーク・panicなし）
  let (duplicate_account, duplicate_created) = UserAccount::new(id.clone(), "duplicate".to_string());
  let result = store_b
    .persist_event_and_snapshot(&duplicate_created, &duplicate_account)
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
    }
    other => panic!("expected OptimisticLockError, got {:?}", other),
  }
}

// ストア破棄後のDBファイルを直接開き、スナップショット行の (seq_nr=0スロット, 履歴) 件数を数える
// （保持ポリシーは公開APIへ露出しないため、テストのみrusqliteで直接観測する）
fn count_snapshot_rows(path: &Path, id: &UserAccountId) -> (i64, Vec<i64>) {
  let connection = rusqlite::Connection::open(path).expect("failed to open db for inspection");
  let slot_count: i64 = connection
    .query_row(
      "SELECT COUNT(*) FROM snapshot WHERE aid = ?1 AND seq_nr = 0",
      rusqlite::params![id.to_string()],
      |row| row.get(0),
    )
    .unwrap();
  let mut statement = connection
    .prepare("SELECT seq_nr FROM snapshot WHERE aid = ?1 AND seq_nr > 0 ORDER BY seq_nr ASC")
    .unwrap();
  let history_seq_nrs = statement
    .query_map(rusqlite::params![id.to_string()], |row| row.get::<_, i64>(0))
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
  (slot_count, history_seq_nrs)
}

#[tokio::test]
async fn test_event_store_on_sqlite_snapshot_retention() {
  let db = TempDb::new();
  let id = UserAccountId::new(id_generate().to_string());
  {
    // AC1.4.1: keep_snapshot_count=1 では古い履歴スナップショット行が残らない
    let mut event_store = new_file_event_store(&db.path).with_keep_snapshot_count(Some(1));
    let (user_account, created) = UserAccount::new(id.clone(), "test".to_string());
    event_store
      .persist_event_and_snapshot(&created, &user_account)
      .await
      .unwrap();
    let mut account = find_by_id(&mut event_store, &id).await.unwrap().unwrap();
    let event = account.rename("renamed-once").unwrap();
    event_store.persist_event_and_snapshot(&event, &account).await.unwrap();
  }

  let (slot_count, history_seq_nrs) = count_snapshot_rows(&db.path, &id);
  // 現行スロット行（seq_nr=0）は削除対象外
  assert_eq!(slot_count, 1);
  // 履歴は最新1件（seq_nr=2）のみ。作成時の履歴行（seq_nr=1）は保守で削除済み
  assert_eq!(history_seq_nrs, vec![2]);
}

#[tokio::test]
async fn test_event_store_on_sqlite_snapshot_ttl_expiration() {
  let db = TempDb::new();
  let id = UserAccountId::new(id_generate().to_string());
  {
    // AC1.4.2: delete_ttl 経過後の保守フックで期限切れ履歴行が削除される
    // （keep_snapshot_count=10 は超過削除を発火させないための値 — TTL削除だけを観測する）
    let mut event_store = new_file_event_store(&db.path)
      .with_keep_snapshot_count(Some(10))
      .with_delete_ttl(Some(chrono::Duration::milliseconds(500)));
    let (user_account, created) = UserAccount::new(id.clone(), "test".to_string());
    event_store
      .persist_event_and_snapshot(&created, &user_account)
      .await
      .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    let mut account = find_by_id(&mut event_store, &id).await.unwrap().unwrap();
    let event = account.rename("renamed-after-ttl").unwrap();
    event_store.persist_event_and_snapshot(&event, &account).await.unwrap();
  }

  let (slot_count, history_seq_nrs) = count_snapshot_rows(&db.path, &id);
  // 現行スロット行（seq_nr=0）は削除対象外
  assert_eq!(slot_count, 1);
  // 作成時の履歴行（seq_nr=1）はTTL期限切れで削除され、直近の履歴行（seq_nr=2）のみ残る
  assert_eq!(history_seq_nrs, vec![2]);
}

#[tokio::test]
async fn test_event_store_on_sqlite_write_failure_returns_neutral_error() {
  // AC1.1.4: 書き込み不能パスでも panic せずバックエンド中立なエラーを返す
  let missing_dir = std::env::temp_dir().join(format!("event-store-adapter-rs-missing-{}", id_generate()));
  let result: Result<EventStoreForSqlite<UserAccountId, UserAccount, UserAccountEvent>, _> =
    EventStoreForSqlite::new(missing_dir.join("db.sqlite"));
  match result {
    Err(EventStoreWriteError::IOError(_)) => {}
    other => panic!("expected IOError, got {:?}", other.map(|_| "Ok(store)")),
  }
}
