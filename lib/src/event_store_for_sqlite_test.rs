use std::path::{Path, PathBuf};

use chrono::Utc;
use event_store_adapter_test_utils_rs::id_generator::id_generate;

use crate::event_envelope::EventEnvelope;
use crate::event_store_for_sqlite::EventStoreForSqlite;
use crate::event_store_test_support::{
  assert_optimistic_lock_message_format, exercise_user_account_flow, find_by_id, init_tracing, UserAccount,
  UserAccountEvent, UserAccountId, CREATED_MANIFEST, RENAMED_MANIFEST,
};
use crate::key_resolver::{DefaultKeyResolver, KeyResolver};
use crate::types::{EventStore, EventStoreReadError, EventStoreWriteError};

type UserAccountStore = EventStoreForSqlite<UserAccountId, UserAccount, UserAccountEvent>;

// 一時ディレクトリ＋一意名のファイルDB。Dropで自テスト作成分のみ後始末する
// （Docker/testcontainers不要・決定的・並列安全 — NFR6.8）
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

fn new_file_event_store(path: &Path) -> UserAccountStore {
  EventStoreForSqlite::new(path).expect("failed to open sqlite event store")
}

// バックエンドのキースキーム（DefaultKeyResolver の sort key）の複製。
// スナップショット行の物理配置（現行スロット = skey マーカー 0 / 履歴 = skey seq_nr 修飾）を
// 白箱検証するために使う（BR2.4 の skey 判別）
fn snapshot_skey(id: &UserAccountId, seq_nr: usize) -> String {
  let resolver = DefaultKeyResolver::<UserAccountId>::default();
  resolver.resolve_sort_key(id, seq_nr)
}

fn current_snapshot_skey(id: &UserAccountId) -> String {
  snapshot_skey(id, 0)
}

// ストア破棄後のDBファイルを直接開き、対象集約の snapshot 行 (skey, seq_nr, version, payload) を返す
// （保持ポリシー・列実値は公開APIへ露出しないため、テストのみrusqliteで直接観測する）
fn snapshot_rows(path: &Path, id: &UserAccountId) -> Vec<(String, i64, i64, Vec<u8>)> {
  let connection = rusqlite::Connection::open(path).expect("failed to open db for inspection");
  let mut statement = connection
    .prepare("SELECT skey, seq_nr, version, payload FROM snapshot WHERE aid = ?1 ORDER BY seq_nr ASC")
    .unwrap();
  statement
    .query_map(rusqlite::params![id.to_string()], |row| {
      Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
    })
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

// 履歴行（skey != 現行スロット skey — BR2.4 の判別そのもの）の seq_nr 一覧を昇順で返す
fn history_seq_nrs(rows: &[(String, i64, i64, Vec<u8>)], id: &UserAccountId) -> Vec<i64> {
  let current_skey = current_snapshot_skey(id);
  rows
    .iter()
    .filter(|(skey, _, _, _)| *skey != current_skey)
    .map(|(_, seq_nr, _, _)| *seq_nr)
    .collect()
}

fn created_envelope(id: &UserAccountId, payload: UserAccountEvent) -> EventEnvelope<UserAccountId, UserAccountEvent> {
  EventEnvelope::new(id.clone(), 1, Utc::now(), payload).with_manifest(CREATED_MANIFEST)
}

fn renamed_envelope(
  id: &UserAccountId,
  seq_nr: usize,
  payload: UserAccountEvent,
) -> EventEnvelope<UserAccountId, UserAccountEvent> {
  EventEnvelope::new(id.clone(), seq_nr, Utc::now(), payload).with_manifest(RENAMED_MANIFEST)
}

// FR7.1 / C3 / AC5.1.1: 共有シナリオ（8 手順・封筒メタデータ往復 assert 含む — occurred_at の
// ナノ秒完全一致往復を含意）を無改変で green にする。空のDBファイルからのスキーマ自動作成も兼ねる
#[tokio::test]
async fn test_event_store_on_sqlite() {
  init_tracing();

  let db = TempDb::new();
  std::fs::File::create(&db.path).expect("failed to create empty db file");
  let mut event_store = new_file_event_store(&db.path);
  let id = UserAccountId::new(id_generate().to_string());

  exercise_user_account_flow(&mut event_store, &id)
    .await
    .expect("scenario failed");
}

// C3: `:memory:` でも同一シナリオが同一挙動で green になる（ファイル/インメモリの対称性）
#[tokio::test]
async fn test_event_store_on_sqlite_in_memory() {
  let mut event_store: UserAccountStore =
    EventStoreForSqlite::new_in_memory().expect("failed to open in-memory sqlite event store");
  let id = UserAccountId::new(id_generate().to_string());

  exercise_user_account_flow(&mut event_store, &id)
    .await
    .expect("scenario failed");
}

// NFR3.9 の共有単位（1 ストアインスタンス + クローン）: Cloneは基底接続を共有し、
// クローン間で書込みが相互に見える（状態分岐が存在しない）
#[tokio::test]
async fn test_event_store_on_sqlite_in_memory_clone_shares_state() {
  let mut original: UserAccountStore =
    EventStoreForSqlite::new_in_memory().expect("failed to open in-memory sqlite event store");
  let mut cloned = original.clone();
  let id = UserAccountId::new(id_generate().to_string());
  let (user_account, created) = UserAccount::new(id.clone(), "test".to_string());
  original
    .persist_event_and_snapshot(created_envelope(&id, created), user_account, 0)
    .await
    .unwrap();

  // クローン側から元の書込みが見える
  let from_clone = find_by_id(&cloned, &id)
    .await
    .unwrap()
    .expect("clone must see the original's write");
  assert_eq!(from_clone.version, 1);
  assert_eq!(from_clone.seq_nr, 1);

  // クローン側の書込みが元から見える
  let mut state = from_clone.state;
  let event = state.rename("renamed-via-clone").unwrap();
  cloned
    .persist_event(renamed_envelope(&id, from_clone.seq_nr + 1, event), from_clone.version)
    .await
    .unwrap();

  let from_original = find_by_id(&original, &id).await.unwrap().unwrap();
  assert_eq!(from_original.version, 2);
  assert_eq!(from_original.seq_nr, 2);
  assert_eq!(from_original.state.name, "renamed-via-clone");
}

// ストアを破棄してファイルから再構築しても状態が復元される（永続化の実体検証）
#[tokio::test]
async fn test_event_store_on_sqlite_file_reopen_restores_state() {
  let db = TempDb::new();
  let id = UserAccountId::new(id_generate().to_string());
  {
    let mut event_store = new_file_event_store(&db.path);
    let (user_account, created) = UserAccount::new(id.clone(), "test".to_string());
    event_store
      .persist_event_and_snapshot(created_envelope(&id, created), user_account, 0)
      .await
      .unwrap();
    let account = find_by_id(&event_store, &id).await.unwrap().unwrap();
    let mut state = account.state;
    let event = state.rename("renamed-before-reopen").unwrap();
    event_store
      .persist_event(renamed_envelope(&id, account.seq_nr + 1, event), account.version)
      .await
      .unwrap();
  }

  let reopened = new_file_event_store(&db.path);
  let restored = find_by_id(&reopened, &id)
    .await
    .unwrap()
    .expect("state must survive store reconstruction");
  assert_eq!(restored.version, 2);
  assert_eq!(restored.seq_nr, 2);
  assert_eq!(restored.state.name, "renamed-before-reopen");
}

// NFR4.4 / AC2.2.1 / BR1.1: 同一 expected_version からの後発書込みは決定的に
// OptimisticLockError で失敗する（トランザクション内条件付き UPDATE が唯一の競合判定点 — P10）。
// 失敗した書込みはロールバックされ journal に残骸を残さない
#[tokio::test]
async fn test_event_store_on_sqlite_optimistic_lock_conflict() {
  let db = TempDb::new();
  let store = new_file_event_store(&db.path);
  let id = UserAccountId::new(id_generate().to_string());
  let (user_account, created) = UserAccount::new(id.clone(), "test".to_string());
  {
    let mut writer = store.clone();
    writer
      .persist_event_and_snapshot(created_envelope(&id, created), user_account, 0)
      .await
      .unwrap();
  }

  // 同一DBを共有する2ハンドル（Cloneは基底接続共有）で同一バージョンから順次コミットする
  let mut store_a = store.clone();
  let mut store_b = store.clone();
  let mut account_a = find_by_id(&store_a, &id).await.unwrap().unwrap();
  let mut account_b = find_by_id(&store_b, &id).await.unwrap().unwrap();

  let event_a = account_a.state.rename("first").unwrap();
  store_a
    .persist_event(renamed_envelope(&id, account_a.seq_nr + 1, event_a), account_a.version)
    .await
    .unwrap();

  let event_b = account_b.state.rename("second").unwrap();
  let result = store_b
    .persist_event(renamed_envelope(&id, account_b.seq_nr + 1, event_b), account_b.version)
    .await;
  match result {
    Err(EventStoreWriteError::OptimisticLockError(message)) => {
      assert!(
        message.starts_with(&format!("optimistic lock failed, aid={}, expected_version=1", id)),
        "unexpected message: {}",
        message
      );
      assert_optimistic_lock_message_format(&message);
    }
    other => panic!("expected OptimisticLockError, got {:?}", other),
  }

  // 失敗した書込みはロールバックされ journal に残骸を残さない（勝者のイベントのみ）
  let events = store_a.get_events_by_id_since_seq_nr(&id, 0).await.unwrap();
  assert_eq!(events.len(), 2);
  let replayed = find_by_id(&store_a, &id).await.unwrap().unwrap();
  assert_eq!(replayed.version, 2);
  assert_eq!(replayed.seq_nr, 2);
}

// NFR4.4 / BR1.1: エラー契約の回帰固定 — 更新競合は actual_version 付き、既存集約への重複作成は
// create パスの expected_version リテラル 0 + actual_version 付きの統一書式で返る
#[tokio::test]
async fn test_event_store_on_sqlite_error_contract() {
  let db = TempDb::new();
  let mut store_a = new_file_event_store(&db.path);
  let mut store_b = store_a.clone();
  let id = UserAccountId::new(id_generate().to_string());
  let (user_account, created) = UserAccount::new(id.clone(), "test".to_string());
  store_a
    .persist_event_and_snapshot(created_envelope(&id, created), user_account, 0)
    .await
    .unwrap();

  let mut account_a = find_by_id(&store_a, &id).await.unwrap().unwrap();
  let mut account_b = find_by_id(&store_b, &id).await.unwrap().unwrap();

  let event_a = account_a.state.rename("first").unwrap();
  store_a
    .persist_event(renamed_envelope(&id, account_a.seq_nr + 1, event_a), account_a.version)
    .await
    .unwrap();

  // 同一バージョン(1)への後発の書込みは actual_version 付き統一書式で失敗する
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

  // BR1.1: 既存集約への重複作成は主キー衝突 → expected_version はリテラル 0（U1 の新規作成規約）
  let (duplicate_account, duplicate_created) = UserAccount::new(id.clone(), "duplicate".to_string());
  let result = store_b
    .persist_event_and_snapshot(created_envelope(&id, duplicate_created), duplicate_account, 0)
    .await;
  match result {
    Err(EventStoreWriteError::OptimisticLockError(message)) => {
      assert_eq!(
        message,
        format!(
          "optimistic lock failed, aid={}, expected_version=0, actual_version=2",
          id
        )
      );
      assert_optimistic_lock_message_format(&message);
    }
    other => panic!("expected OptimisticLockError, got {:?}", other),
  }
}

// BR3.1 / AC2.3.4: keep_snapshot_count = Some(n) でスナップショット付き更新を繰り返すと
// 履歴行が同一トランザクションで書かれ、保持フックが新しい順に n 件残して剪定する
// （skey 判別への置換後も件数意味論が保たれる — BR2.4）。
// AC2.3.3 / BR2.3: 現行・履歴行の payload は純ドメイン内容のみ（JSON 完全一致 — U2/U3 同型）。
// BR2.4: 現行スロット行の seq_nr 列は実イベント位置を持つ（白箱で固定）
#[tokio::test]
async fn test_event_store_on_sqlite_snapshot_retention() {
  let db = TempDb::new();
  let id = UserAccountId::new(id_generate().to_string());
  {
    let mut event_store = new_file_event_store(&db.path)
      .with_keep_snapshot_count(Some(2))
      .expect("Some(2) is valid");
    let (user_account, created) = UserAccount::new(id.clone(), "test".to_string());
    event_store
      .persist_event_and_snapshot(created_envelope(&id, created), user_account.clone(), 0)
      .await
      .unwrap();

    // スナップショット付き更新 × 3（履歴行は seq_nr = 1, 2, 3, 4 の 4 件生まれ、2 件へ剪定される）
    let mut state = user_account;
    for (seq_nr, name) in [(2_usize, "history-1"), (3, "history-2"), (4, "history-3")] {
      let event = state.rename(name).unwrap();
      event_store
        .persist_event_and_snapshot(renamed_envelope(&id, seq_nr, event), state.clone(), seq_nr - 1)
        .await
        .unwrap();
    }
  }

  let rows = snapshot_rows(&db.path, &id);
  // 現行スロット行は skey マーカーで 1 件のみ、seq_nr 列は実値 4・version は 4（BR2.4）
  let current_skey = current_snapshot_skey(&id);
  let current_rows: Vec<_> = rows.iter().filter(|(skey, _, _, _)| *skey == current_skey).collect();
  assert_eq!(current_rows.len(), 1);
  assert_eq!(current_rows[0].1, 4);
  assert_eq!(current_rows[0].2, 4);

  // 履歴は新しい順に 2 件（seq_nr = 3, 4）のみ。古い履歴行（seq_nr = 1, 2）は剪定済み
  assert_eq!(history_seq_nrs(&rows, &id), vec![3, 4]);

  // AC2.3.3: 現行スロット行の payload は純ドメイン内容のみ（version / seq_nr / last_updated_at の複製なし）
  let current_json: serde_json::Value = serde_json::from_slice(&current_rows[0].3).unwrap();
  let expected_current = UserAccount {
    id: id.clone(),
    name: "history-3".to_string(),
  };
  assert_eq!(current_json, serde_json::to_value(&expected_current).unwrap());

  // 履歴行（seq_nr = 3 のスナップショット付き更新の後像）の payload も純ドメイン内容のみ
  let history_payload = rows
    .iter()
    .find(|(skey, _, _, _)| *skey == snapshot_skey(&id, 3))
    .map(|(_, _, _, payload)| payload)
    .expect("history row for seq_nr 3 must exist");
  let history_json: serde_json::Value = serde_json::from_slice(history_payload).unwrap();
  let expected_history = UserAccount {
    id: id.clone(),
    name: "history-2".to_string(),
  };
  assert_eq!(history_json, serde_json::to_value(&expected_history).unwrap());
}

// BR3.1: delete_ttl 経過後の保持フックで期限切れ履歴行が削除される
// （keep_snapshot_count=10 は超過削除を発火させないための値 — TTL削除だけを観測する）
#[tokio::test]
async fn test_event_store_on_sqlite_snapshot_ttl_expiration() {
  let db = TempDb::new();
  let id = UserAccountId::new(id_generate().to_string());
  {
    let mut event_store = new_file_event_store(&db.path)
      .with_keep_snapshot_count(Some(10))
      .expect("Some(10) is valid")
      .with_delete_ttl(Some(chrono::Duration::milliseconds(500)));
    let (user_account, created) = UserAccount::new(id.clone(), "test".to_string());
    event_store
      .persist_event_and_snapshot(created_envelope(&id, created), user_account.clone(), 0)
      .await
      .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    let mut state = user_account;
    let event = state.rename("renamed-after-ttl").unwrap();
    event_store
      .persist_event_and_snapshot(renamed_envelope(&id, 2, event), state, 1)
      .await
      .unwrap();
  }

  let rows = snapshot_rows(&db.path, &id);
  // 現行スロット行（skey マーカー）は削除対象外
  let current_skey = current_snapshot_skey(&id);
  assert_eq!(rows.iter().filter(|(skey, _, _, _)| *skey == current_skey).count(), 1);
  // 作成時の履歴行（seq_nr=1）はTTL期限切れで削除され、直近の履歴行（seq_nr=2）のみ残る
  assert_eq!(history_seq_nrs(&rows, &id), vec![2]);
}

// AC2.2.2 / AC4.1.3 / BR4.1: 書込・読取のいずれの失敗経路でも panic せず
// バックエンド中立なエラーが返る（NFR3.8 — 生エラー詳細の書式化展開なし）
#[tokio::test]
async fn test_event_store_on_sqlite_write_failure_returns_neutral_error() {
  // 書込経路: 存在しないディレクトリ配下のDBファイルは IOError（AC2.2.2）
  let missing_dir = std::env::temp_dir().join(format!("event-store-adapter-rs-missing-{}", id_generate()));
  let result: Result<UserAccountStore, _> = EventStoreForSqlite::new(missing_dir.join("db.sqlite"));
  match result {
    Err(EventStoreWriteError::IOError(_)) => {}
    other => panic!("expected IOError, got {:?}", other.map(|_| "Ok(store)")),
  }

  // 読取経路: 列型が破損した journal 行は panic せず EventStoreReadError になる（AC4.1.3 / BR2.2）
  let db = TempDb::new();
  let store = new_file_event_store(&db.path);
  let id = UserAccountId::new(id_generate().to_string());
  {
    let connection = rusqlite::Connection::open(&db.path).expect("failed to open db for corruption");
    connection
      .execute(
        "INSERT INTO journal (pkey, skey, aid, seq_nr, payload, occurred_at, manifest) VALUES (?1, ?2, ?3, ?4, ?5, \
         ?6, ?7)",
        rusqlite::params!["p", "s", id.to_string(), 1i64, b"{}".to_vec(), "not-a-number", ""],
      )
      .expect("failed to insert corrupted row");
  }
  let result = store.get_events_by_id_since_seq_nr(&id, 0).await;
  match result {
    Err(EventStoreReadError::OtherError(_)) => {}
    other => panic!("expected OtherError, got {:?}", other),
  }
}

// BR4.1(U1) / P3: keep_snapshot_count = Some(0) はビルダーで拒否される（ラッパーの Result 素通し。
// 旧実装の「Some(0) = 全履歴剪定」という暗黙挙動は入力値ごと到達不能になった）
#[tokio::test]
async fn test_event_store_on_sqlite_with_keep_snapshot_count_zero_is_rejected() {
  let store: UserAccountStore =
    EventStoreForSqlite::new_in_memory().expect("failed to open in-memory sqlite event store");
  let result = store.with_keep_snapshot_count(Some(0));
  assert!(matches!(result, Err(EventStoreWriteError::ContractViolation(_))));

  let store: UserAccountStore =
    EventStoreForSqlite::new_in_memory().expect("failed to open in-memory sqlite event store");
  let store = store.with_keep_snapshot_count(Some(1)).expect("Some(1) is valid");
  assert_eq!(store.maintenance().keep_snapshot_count, Some(1));
}

// BR4.1: shard_count=0 はキー解決のゼロ除算panicではなく、バックエンド中立なエラーで拒否される
#[tokio::test]
async fn test_event_store_on_sqlite_zero_shard_count_returns_neutral_error() {
  let mut event_store: UserAccountStore = EventStoreForSqlite::new_in_memory()
    .expect("failed to open in-memory sqlite event store")
    .with_shard_count(0);
  let id = UserAccountId::new(id_generate().to_string());
  let (user_account, created) = UserAccount::new(id.clone(), "test".to_string());

  let result = event_store
    .persist_event_and_snapshot(created_envelope(&id, created), user_account, 0)
    .await;
  match result {
    Err(EventStoreWriteError::OtherError(message)) => {
      assert!(message.contains("shard_count"));
    }
    other => panic!("expected OtherError, got {:?}", other),
  }
}

// U1 リファレンス意味論との対称性: 未作成（Absent）の集約への更新系呼び出しは
// actual_version なし書式の OptimisticLockError を返す（条件付き UPDATE の変更行数 0 +
// 追補 SELECT の行不在 — BR1.1 / AC2.2.1）
#[tokio::test]
async fn test_event_store_on_sqlite_update_on_absent_aggregate_yields_optimistic_lock_error() {
  let mut store: UserAccountStore =
    EventStoreForSqlite::new_in_memory().expect("failed to open in-memory sqlite event store");
  let id = UserAccountId::new(id_generate().to_string());
  let (user_account, _created) = UserAccount::new(id.clone(), "test".to_string());

  // 未作成のまま seq_nr=2 / expected_version=1 のスナップショット付き更新（W3 経路）を呼ぶ
  let envelope = EventEnvelope::new(
    id.clone(),
    2,
    Utc::now(),
    UserAccountEvent::Renamed {
      name: "ghost".to_string(),
    },
  );
  let result = store.persist_event_and_snapshot(envelope, user_account, 1).await;
  match result {
    Err(EventStoreWriteError::OptimisticLockError(message)) => {
      // actual_version を含まない書式であることを完全一致で固定する
      assert_eq!(
        message,
        format!("optimistic lock failed, aid={}, expected_version=1", id)
      );
      assert_optimistic_lock_message_format(&message);
    }
    other => panic!("expected OptimisticLockError, got {:?}", other),
  }
}

// BR3.1 / AC2.3.4: keep_snapshot_count = None（既定）は履歴行を書かず剪定もしない
// （剪定なし設定に書込み増幅を持ち込まない — U2/U3 対称）
#[tokio::test]
async fn test_event_store_on_sqlite_without_keep_snapshot_count_writes_no_history() {
  let db = TempDb::new();
  let id = UserAccountId::new(id_generate().to_string());
  {
    let mut event_store = new_file_event_store(&db.path);
    let (user_account, created) = UserAccount::new(id.clone(), "test".to_string());
    event_store
      .persist_event_and_snapshot(created_envelope(&id, created), user_account.clone(), 0)
      .await
      .unwrap();

    let mut state = user_account;
    for (seq_nr, name) in [(2_usize, "no-history-1"), (3, "no-history-2")] {
      let event = state.rename(name).unwrap();
      event_store
        .persist_event_and_snapshot(renamed_envelope(&id, seq_nr, event), state.clone(), seq_nr - 1)
        .await
        .unwrap();
    }
  }

  let rows = snapshot_rows(&db.path, &id);
  assert!(
    history_seq_nrs(&rows, &id).is_empty(),
    "no history rows must be written when keep_snapshot_count is None"
  );
  // 現行スロット行は最新状態を保持する
  let current_skey = current_snapshot_skey(&id);
  let current_rows: Vec<_> = rows.iter().filter(|(skey, _, _, _)| *skey == current_skey).collect();
  assert_eq!(current_rows.len(), 1);
  assert_eq!(current_rows[0].1, 3);
  assert_eq!(current_rows[0].2, 3);
}
