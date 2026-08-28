use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client;
use chrono::Utc;
use event_store_adapter_test_utils_rs::docker::dynamodb_local;
use event_store_adapter_test_utils_rs::dynamodb::{
  create_client, create_journal_table, create_snapshot_table, wait_table,
};
use event_store_adapter_test_utils_rs::id_generator::id_generate;
use tokio::time::{sleep, Duration as TokioDuration};

use crate::event_envelope::EventEnvelope;
use crate::event_store_for_dynamodb::EventStoreForDynamoDB;
use crate::event_store_test_support::{
  assert_optimistic_lock_message_format, exercise_user_account_flow, find_by_id, init_tracing, UserAccount,
  UserAccountEvent, UserAccountId, CREATED_MANIFEST, RENAMED_MANIFEST,
};
use crate::key_resolver::{DefaultKeyResolver, KeyResolver};
use crate::types::{EventStore, EventStoreReadError, EventStoreWriteError};

const JOURNAL_TABLE: &str = "journal";
const JOURNAL_AID_INDEX: &str = "journal-aid-index";
const SNAPSHOT_TABLE: &str = "snapshot";
const SNAPSHOT_AID_INDEX: &str = "snapshot-aid-index";
const SHARD_COUNT: u64 = 64;

type UserAccountStore = EventStoreForDynamoDB<UserAccountId, UserAccount, UserAccountEvent>;

fn test_time_factor() -> f32 {
  std::env::var("TEST_TIME_FACTOR")
    .ok()
    .and_then(|value| value.parse::<f32>().ok())
    .unwrap_or(1.0)
}

/// LocalStack へ接続し、テーブルを作成してストアを構築する（1 テストにつき独立したコンテナ）。
async fn connect_store(port: u16) -> (Client, UserAccountStore) {
  let wait = TokioDuration::from_millis((1000f32 * test_time_factor()) as u64);
  sleep(wait).await;

  let client = create_client(port);
  let _ = create_journal_table(&client, JOURNAL_TABLE, JOURNAL_AID_INDEX).await;
  let _ = create_snapshot_table(&client, SNAPSHOT_TABLE, SNAPSHOT_AID_INDEX).await;
  while !(wait_table(&client, JOURNAL_TABLE).await) {
    sleep(wait).await;
  }
  while !(wait_table(&client, SNAPSHOT_TABLE).await) {
    sleep(wait).await;
  }

  let store = UserAccountStore::new(
    client.clone(),
    JOURNAL_TABLE.to_string(),
    JOURNAL_AID_INDEX.to_string(),
    SNAPSHOT_TABLE.to_string(),
    SNAPSHOT_AID_INDEX.to_string(),
    SHARD_COUNT,
  );
  (client, store)
}

// バックエンドのキースキーム（DefaultKeyResolver の sort key）の複製。
// スナップショット項目の物理配置（current = skey マーカー 0 / 履歴 = skey seq_nr 修飾）を
// 白箱検証するために使う
fn snapshot_skey(id: &UserAccountId, seq_nr: usize) -> String {
  let resolver = DefaultKeyResolver::<UserAccountId>::default();
  resolver.resolve_sort_key(id, seq_nr)
}

fn current_snapshot_skey(id: &UserAccountId) -> String {
  snapshot_skey(id, 0)
}

/// snapshot テーブルの対象集約の全アイテムを (skey, payload) で返す（剪定結果の白箱検証用）。
async fn scan_snapshot_items(client: &Client, id: &UserAccountId) -> HashMap<String, Vec<u8>> {
  let response = client
    .query()
    .table_name(SNAPSHOT_TABLE)
    .index_name(SNAPSHOT_AID_INDEX)
    .key_condition_expression("#aid = :aid")
    .expression_attribute_names("#aid", "aid")
    .expression_attribute_values(":aid", AttributeValue::S(id.to_string()))
    .send()
    .await
    .expect("query failed");
  response
    .items
    .unwrap_or_default()
    .into_iter()
    .map(|item| {
      let skey = item
        .get("skey")
        .and_then(|value| value.as_s().ok())
        .cloned()
        .expect("skey attribute must exist");
      let payload = item
        .get("payload")
        .and_then(|value| value.as_b().ok())
        .cloned()
        .expect("payload attribute must exist")
        .into_inner();
      (skey, payload)
    })
    .collect()
}

fn renamed_envelope(
  id: &UserAccountId,
  seq_nr: usize,
  payload: UserAccountEvent,
) -> EventEnvelope<UserAccountId, UserAccountEvent> {
  EventEnvelope::new(id.clone(), seq_nr, Utc::now(), payload).with_manifest(RENAMED_MANIFEST)
}

// FR7.1 / C3 / AC5.1.1: 共有シナリオ（8 手順・封筒メタデータ往復 assert 含む）を無改変で green にする。
// keep_snapshot_count + delete_ttl の組み合わせで ttl 更新方式の剪定経路も同時に通す（現行構成維持）
#[tokio::test]
async fn test_event_store_on_dynamodb() {
  init_tracing();

  let node = dynamodb_local().await;
  let port = node.get_host_port_ipv4(4566).await.expect("Failed to get port");
  let (_client, store) = connect_store(port).await;
  let mut event_store = store
    .with_keep_snapshot_count(Some(1))
    .expect("Some(1) is valid")
    .with_delete_ttl(Some(chrono::Duration::seconds(5)));

  let id = UserAccountId::new(id_generate().to_string());

  exercise_user_account_flow(&mut event_store, &id)
    .await
    .expect("scenario failed");
}

// NFR4.1 / AC2.2.1 / BR1.1: 同時更新の競合パス — 同一 expected_version の並行更新 2 本のうち
// 一方のみが成功し、他方は統一書式（許可キーのみ）の OptimisticLockError になる。
// TransactWriteItems の条件式が唯一の競合判定点であることの実機検証（P7）
#[tokio::test]
async fn test_event_store_on_dynamodb_concurrent_conflict_yields_optimistic_lock_error() {
  init_tracing();

  let node = dynamodb_local().await;
  let port = node.get_host_port_ipv4(4566).await.expect("Failed to get port");
  let (_client, store) = connect_store(port).await;

  let id = UserAccountId::new(id_generate().to_string());
  let (user_account, created) = UserAccount::new(id.clone(), "test".to_string());
  {
    let mut writer = store.clone();
    writer
      .persist_event_and_snapshot(
        EventEnvelope::new(id.clone(), 1, Utc::now(), created).with_manifest(CREATED_MANIFEST),
        user_account,
        0,
      )
      .await
      .expect("creation failed");
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

// AC2.2.2 / AC4.1.3 / BR5.1 / NFR3.7: 接続不能でも panic せず IOError 系の中立エラーが返り、
// メッセージに接続文字列・テーブル名が展開されない（P9）。
// 未使用ポートのエンドポイント指定で実現する（Docker 不要）
#[tokio::test]
async fn test_event_store_on_dynamodb_unreachable_endpoint_returns_io_error() {
  init_tracing();

  let client = create_client(1);
  let mut store: UserAccountStore = EventStoreForDynamoDB::new(
    client,
    JOURNAL_TABLE.to_string(),
    JOURNAL_AID_INDEX.to_string(),
    SNAPSHOT_TABLE.to_string(),
    SNAPSHOT_AID_INDEX.to_string(),
    SHARD_COUNT,
  );

  let id = UserAccountId::new(id_generate().to_string());

  // 読取経路（fetch_latest_snapshot の GetItem が接続不能）
  let read_result = store.get_latest_snapshot_by_id(&id).await;
  match read_result {
    Err(EventStoreReadError::IOError(source)) => {
      let message = source.to_string();
      assert!(!message.contains("://"), "message must not contain connection strings");
      assert!(
        !message.contains(JOURNAL_TABLE) && !message.contains(SNAPSHOT_TABLE),
        "message must not contain table names"
      );
    }
    other => panic!("expected IOError, got {:?}", other),
  }

  // 書込経路（W1: create の TransactWriteItems が接続不能）
  let (user_account, created) = UserAccount::new(id.clone(), "test".to_string());
  let write_result = store
    .persist_event_and_snapshot(EventEnvelope::new(id.clone(), 1, Utc::now(), created), user_account, 0)
    .await;
  match write_result {
    Err(EventStoreWriteError::IOError(source)) => {
      let message = source.to_string();
      assert!(!message.contains("://"), "message must not contain connection strings");
      assert!(
        !message.contains(JOURNAL_TABLE) && !message.contains(SNAPSHOT_TABLE),
        "message must not contain table names"
      );
    }
    other => panic!("expected IOError, got {:?}", other),
  }
}

// FR6.3 / AC2.3.4 / BR3.1 / BR4.1: keep_snapshot_count = Some(n) でスナップショット付き更新を
// 繰り返すと履歴項目が同一トランザクションで書かれ、保持フックが n 件残して剪定する
// （delete_ttl なし → 削除方式 = FR6.5 で .unwrap() を除去した DeleteRequest 経路を通す）。
// 現行の剪定走査（seq_nr 降順・超過分削除）では旧い側の履歴（seq_nr = 1, 2）が残る。
// AC2.3.3 / BR2.3: 現行・履歴項目の payload は純ドメイン内容のみ（JSON 完全一致 — U3 同型）
#[tokio::test]
async fn test_event_store_on_dynamodb_prunes_snapshot_history() {
  init_tracing();

  let node = dynamodb_local().await;
  let port = node.get_host_port_ipv4(4566).await.expect("Failed to get port");
  let (client, store) = connect_store(port).await;
  let mut store = store.with_keep_snapshot_count(Some(2)).expect("Some(2) is valid");

  let id = UserAccountId::new(id_generate().to_string());
  let (user_account, created) = UserAccount::new(id.clone(), "test".to_string());
  store
    .persist_event_and_snapshot(
      EventEnvelope::new(id.clone(), 1, Utc::now(), created).with_manifest(CREATED_MANIFEST),
      user_account.clone(),
      0,
    )
    .await
    .expect("creation failed");

  // スナップショット付き更新 × 3（履歴項目は seq_nr = 1, 2, 3, 4 の 4 件生まれ、2 件へ剪定される）
  let mut state = user_account;
  for (seq_nr, name) in [(2_usize, "history-1"), (3, "history-2"), (4, "history-3")] {
    let event = state.rename(name).unwrap();
    store
      .persist_event_and_snapshot(
        EventEnvelope::new(id.clone(), seq_nr, Utc::now(), event).with_manifest(RENAMED_MANIFEST),
        state.clone(),
        seq_nr - 1,
      )
      .await
      .expect("update failed");
  }

  // 剪定後は current 1 件 + 履歴 2 件（現行走査順では旧い側 seq_nr = 1, 2 が残る）
  let items = scan_snapshot_items(&client, &id).await;
  let mut history_skeys: Vec<String> = items
    .keys()
    .filter(|skey| **skey != current_snapshot_skey(&id))
    .cloned()
    .collect();
  history_skeys.sort();
  assert_eq!(history_skeys, vec![snapshot_skey(&id, 1), snapshot_skey(&id, 2)]);

  // current 項目は最新状態のまま（剪定は履歴項目のみに作用する）
  let snapshot = store
    .get_latest_snapshot_by_id(&id)
    .await
    .unwrap()
    .expect("snapshot must exist");
  assert_eq!(snapshot.version(), 4);
  assert_eq!(snapshot.seq_nr(), 4);
  assert_eq!(snapshot.aggregate().name, "history-3");

  // AC2.3.3: current 項目の payload は純ドメイン内容のみ（version / seq_nr / last_updated_at の複製なし）
  let current_payload = items
    .get(&current_snapshot_skey(&id))
    .expect("current payload must exist");
  let current_json: serde_json::Value = serde_json::from_slice(current_payload).unwrap();
  let expected_current = UserAccount {
    id: id.clone(),
    name: "history-3".to_string(),
  };
  assert_eq!(current_json, serde_json::to_value(&expected_current).unwrap());

  // 履歴項目（seq_nr = 2 のスナップショット付き更新の後像）の payload も純ドメイン内容のみ
  let history_payload = items.get(&snapshot_skey(&id, 2)).expect("history payload must exist");
  let history_json: serde_json::Value = serde_json::from_slice(history_payload).unwrap();
  let expected_history = UserAccount {
    id: id.clone(),
    name: "history-1".to_string(),
  };
  assert_eq!(history_json, serde_json::to_value(&expected_history).unwrap());
}

// BR3.1 / AC2.3.4: keep_snapshot_count = None（既定）は履歴項目を書かず剪定もしない
// （剪定なし設定に書込み増幅を持ち込まない — 現行挙動との互換）
#[tokio::test]
async fn test_event_store_on_dynamodb_without_keep_snapshot_count_writes_no_history() {
  init_tracing();

  let node = dynamodb_local().await;
  let port = node.get_host_port_ipv4(4566).await.expect("Failed to get port");
  let (client, mut store) = connect_store(port).await;

  let id = UserAccountId::new(id_generate().to_string());
  let (user_account, created) = UserAccount::new(id.clone(), "test".to_string());
  store
    .persist_event_and_snapshot(
      EventEnvelope::new(id.clone(), 1, Utc::now(), created).with_manifest(CREATED_MANIFEST),
      user_account.clone(),
      0,
    )
    .await
    .expect("creation failed");

  let mut state = user_account;
  for (seq_nr, name) in [(2_usize, "no-history-1"), (3, "no-history-2")] {
    let event = state.rename(name).unwrap();
    store
      .persist_event_and_snapshot(
        EventEnvelope::new(id.clone(), seq_nr, Utc::now(), event).with_manifest(RENAMED_MANIFEST),
        state.clone(),
        seq_nr - 1,
      )
      .await
      .expect("update failed");
  }

  let items = scan_snapshot_items(&client, &id).await;
  let history_skeys: Vec<&String> = items
    .keys()
    .filter(|skey| **skey != current_snapshot_skey(&id))
    .collect();
  assert!(
    history_skeys.is_empty(),
    "no history items must be written when keep_snapshot_count is None"
  );
}

// U1 リファレンス意味論との対称性: 未作成（Absent）の集約への更新系呼び出しは
// actual_version なし書式の OptimisticLockError を返す（条件式 #version=:before_version が
// 不在項目で不成立 → TransactionCanceledException — BR1.1）
#[tokio::test]
async fn test_event_store_on_dynamodb_update_on_absent_aggregate_yields_optimistic_lock_error() {
  init_tracing();

  let node = dynamodb_local().await;
  let port = node.get_host_port_ipv4(4566).await.expect("Failed to get port");
  let (_client, mut store) = connect_store(port).await;

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
