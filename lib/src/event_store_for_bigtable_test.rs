use std::collections::BTreeSet;

use chrono::Utc;
use event_store_adapter_test_utils_rs::bigtable::create_table;
use event_store_adapter_test_utils_rs::docker::bigtable_emulator;
use event_store_adapter_test_utils_rs::id_generator::id_generate;
use googleapis_tonic_google_bigtable_admin_v2::google::bigtable::admin::v2::bigtable_table_admin_client::BigtableTableAdminClient;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::bigtable_client::BigtableClient;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
  row_filter,
  row_range::{EndKey, StartKey},
  ReadRowsRequest, RowFilter, RowRange, RowSet,
};
use tonic::transport::Channel;

use crate::event_envelope::EventEnvelope;
use crate::event_store_for_bigtable::EventStoreForBigtable;
use crate::event_store_test_support::{
  assert_optimistic_lock_message_format, exercise_user_account_flow, find_by_id, init_tracing, UserAccount,
  UserAccountEvent, UserAccountId, CREATED_MANIFEST, RENAMED_MANIFEST,
};
use crate::key_resolver::{DefaultKeyResolver, KeyResolver};
use crate::types::{AggregateId, EventStore, EventStoreReadError, EventStoreWriteError};

const PROJECT: &str = "test-project";
const INSTANCE: &str = "test-instance";
const JOURNAL_TABLE: &str = "journal";
const SNAPSHOT_TABLE: &str = "snapshot";
const SNAPSHOT_FAMILY: &str = "snapshot";
const SHARD_COUNT: u64 = 64;

type UserAccountStore = EventStoreForBigtable<UserAccountId, UserAccount, UserAccountEvent>;

/// エミュレータへ接続し、テーブルを作成してストアを構築する（1 テストにつき独立したエミュレータ）。
async fn connect_store(port: u16) -> (BigtableClient<Channel>, UserAccountStore) {
  let endpoint = format!("http://127.0.0.1:{}", port);
  let channel = Channel::from_shared(endpoint)
    .expect("invalid endpoint")
    .connect()
    .await
    .expect("failed to connect to emulator");

  let parent = format!("projects/{}/instances/{}", PROJECT, INSTANCE);
  let mut table_admin = BigtableTableAdminClient::new(channel.clone());
  create_table(&mut table_admin, &parent, JOURNAL_TABLE, &["event"]).await;
  create_table(&mut table_admin, &parent, SNAPSHOT_TABLE, &["snapshot"]).await;

  let client = BigtableClient::new(channel);
  let store = UserAccountStore::new(
    client.clone(),
    PROJECT.to_string(),
    INSTANCE.to_string(),
    JOURNAL_TABLE.to_string(),
    SNAPSHOT_TABLE.to_string(),
    SHARD_COUNT,
  );
  (client, store)
}

fn snapshot_table_path() -> String {
  format!("projects/{}/instances/{}/tables/{}", PROJECT, INSTANCE, SNAPSHOT_TABLE)
}

// バックエンドの行キースキーム（key_resolver の partition key + type_name + value）の複製。
// 履歴行の物理配置（snapshot 行キー + ゼロ詰め seq_nr 修飾）を白箱検証するために使う
fn snapshot_row_key(id: &UserAccountId) -> Vec<u8> {
  let resolver = DefaultKeyResolver::<UserAccountId>::default();
  format!(
    "{}#{}#{}",
    resolver.resolve_partition_key(id, SHARD_COUNT),
    id.type_name(),
    id.value()
  )
  .into_bytes()
}

fn history_row_prefix(id: &UserAccountId) -> Vec<u8> {
  let mut key = snapshot_row_key(id);
  key.push(b'#');
  key
}

fn history_row_key(id: &UserAccountId, seq_nr: usize) -> Vec<u8> {
  let mut key = history_row_prefix(id);
  key.extend_from_slice(format!("{:020}", seq_nr).as_bytes());
  key
}

// ファミリ + 対象列 + 最新セル限定 → 1 行 = 1 セルになり、チャンク走査を単純化できる
fn single_cell_filter(qualifier: &[u8]) -> RowFilter {
  let filters = vec![
    row_filter::Filter::FamilyNameRegexFilter(SNAPSHOT_FAMILY.to_string()),
    row_filter::Filter::ColumnQualifierRegexFilter(qualifier.to_vec()),
    row_filter::Filter::CellsPerColumnLimitFilter(1),
  ];
  RowFilter {
    filter: Some(row_filter::Filter::Chain(row_filter::Chain {
      filters: filters
        .into_iter()
        .map(|filter| RowFilter { filter: Some(filter) })
        .collect(),
    })),
  }
}

/// snapshot テーブルの履歴行キーを昇順で返す（剪定結果の白箱検証用）。
async fn scan_history_row_keys(client: &BigtableClient<Channel>, id: &UserAccountId) -> Vec<Vec<u8>> {
  let prefix = history_row_prefix(id);
  let mut end_key = prefix.clone();
  end_key.push(0xFF);
  let request = ReadRowsRequest {
    table_name: snapshot_table_path(),
    rows: Some(RowSet {
      row_keys: vec![],
      row_ranges: vec![RowRange {
        start_key: Some(StartKey::StartKeyClosed(prefix)),
        end_key: Some(EndKey::EndKeyOpen(end_key)),
      }],
    }),
    filter: Some(single_cell_filter(b"version")),
    ..Default::default()
  };
  let mut stream = client
    .clone()
    .read_rows(request)
    .await
    .expect("read_rows failed")
    .into_inner();
  let mut keys = BTreeSet::new();
  while let Some(response) = stream.message().await.expect("read_rows stream failed") {
    for chunk in response.chunks {
      if !chunk.row_key.is_empty() {
        keys.insert(chunk.row_key);
      }
    }
  }
  keys.into_iter().collect()
}

/// snapshot テーブルの 1 行 1 セルを読み取る（payload 純度の白箱検証用）。
async fn read_snapshot_cell(client: &BigtableClient<Channel>, row_key: Vec<u8>, qualifier: &[u8]) -> Option<Vec<u8>> {
  let request = ReadRowsRequest {
    table_name: snapshot_table_path(),
    rows: Some(RowSet {
      row_keys: vec![row_key],
      row_ranges: vec![],
    }),
    filter: Some(single_cell_filter(qualifier)),
    rows_limit: 1,
    ..Default::default()
  };
  let mut stream = client
    .clone()
    .read_rows(request)
    .await
    .expect("read_rows failed")
    .into_inner();
  let mut value = Vec::new();
  let mut found = false;
  while let Some(response) = stream.message().await.expect("read_rows stream failed") {
    for chunk in response.chunks {
      if chunk.family_name.is_some() || chunk.qualifier.is_some() {
        found = true;
      }
      value.extend_from_slice(&chunk.value);
    }
  }
  found.then_some(value)
}

fn renamed_envelope(
  id: &UserAccountId,
  seq_nr: usize,
  payload: UserAccountEvent,
) -> EventEnvelope<UserAccountId, UserAccountEvent> {
  EventEnvelope::new(id.clone(), seq_nr, Utc::now(), payload).with_manifest(RENAMED_MANIFEST)
}

// FR7.1 / C3 / AC5.1.1: 共有シナリオ（8 手順・封筒メタデータ往復 assert 含む）を無改変で green にする
#[tokio::test]
async fn test_event_store_on_bigtable() {
  init_tracing();

  let node = bigtable_emulator().await;
  let port = node
    .get_host_port_ipv4(8086)
    .await
    .expect("Failed to get Bigtable port");
  let (_client, mut event_store) = connect_store(port).await;

  let id = UserAccountId::new(id_generate().to_string());

  exercise_user_account_flow(&mut event_store, &id)
    .await
    .expect("scenario failed");
}

// NFR4.1 / AC2.2.1 / BR1.1: 同時更新の競合パス — 同一 expected_version の並行更新 2 本のうち
// 一方のみが成功し、他方は統一書式（許可キーのみ）の OptimisticLockError になる。
// BR5.1 / NFR5.2: CheckAndMutateRow の述語評価とミューテーション適用の単一行原子性は
// Bigtable API 契約であり、エミュレータ（cloud-sdk）と本番で意味論は同一である。
// 本テストの検証結果はそのまま本番の競合意味論に対応する（性能特性の差は検証範囲外）
#[tokio::test]
async fn test_event_store_on_bigtable_concurrent_conflict_yields_optimistic_lock_error() {
  init_tracing();

  let node = bigtable_emulator().await;
  let port = node
    .get_host_port_ipv4(8086)
    .await
    .expect("Failed to get Bigtable port");
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

// AC2.2.2 / AC4.1.3 / BR4.1 / NFR3.4: 接続不能でも panic せず IOError 系の中立エラーが返り、
// メッセージに接続文字列・テーブルパス・生ステータス詳細が展開されない（P6）。
// 未使用ポートへの接続で実現する（現行のエラー契約テストパターン踏襲 — Docker 不要）
#[tokio::test]
async fn test_event_store_on_bigtable_unreachable_endpoint_returns_io_error() {
  init_tracing();

  let channel = Channel::from_shared("http://127.0.0.1:1".to_string())
    .expect("invalid endpoint")
    .connect_lazy();
  let client = BigtableClient::new(channel);
  let mut store: UserAccountStore = EventStoreForBigtable::new(
    client,
    PROJECT.to_string(),
    INSTANCE.to_string(),
    JOURNAL_TABLE.to_string(),
    SNAPSHOT_TABLE.to_string(),
    SHARD_COUNT,
  );

  let id = UserAccountId::new(id_generate().to_string());

  // 読取経路
  let read_result = store.get_latest_snapshot_by_id(&id).await;
  match read_result {
    Err(EventStoreReadError::IOError(source)) => {
      let message = source.to_string();
      assert!(!message.contains("://"), "message must not contain connection strings");
      assert!(!message.contains("projects/"), "message must not contain table paths");
    }
    other => panic!("expected IOError, got {:?}", other),
  }

  // 書込経路（W1: create の CAS 呼び出しが接続不能）
  let (user_account, created) = UserAccount::new(id.clone(), "test".to_string());
  let write_result = store
    .persist_event_and_snapshot(EventEnvelope::new(id.clone(), 1, Utc::now(), created), user_account, 0)
    .await;
  match write_result {
    Err(EventStoreWriteError::IOError(source)) => {
      let message = source.to_string();
      assert!(!message.contains("://"), "message must not contain connection strings");
      assert!(!message.contains("projects/"), "message must not contain table paths");
    }
    other => panic!("expected IOError, got {:?}", other),
  }
}

// U1 リファレンス意味論との対称性: 未作成（Absent）の集約への更新系呼び出しは
// actual_version なし書式の OptimisticLockError を返す（CAS 述語不成立 + 追補読取で行不在）
#[tokio::test]
async fn test_event_store_on_bigtable_update_on_absent_aggregate_yields_optimistic_lock_error() {
  init_tracing();

  let node = bigtable_emulator().await;
  let port = node
    .get_host_port_ipv4(8086)
    .await
    .expect("Failed to get Bigtable port");
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

// FR6.3 / AC2.3.4 / BR3.1: keep_snapshot_count = Some(n) でスナップショット付き更新を繰り返すと、
// プレイメージが履歴行として書かれ、保持フックが新しい順に n 件残して剪定する。
// AC2.3.3 / BR2.3: 現行行・履歴行の payload は純ドメイン内容のみ（version 等の複製キーを含まない）
#[tokio::test]
async fn test_event_store_on_bigtable_prunes_snapshot_history() {
  init_tracing();

  let node = bigtable_emulator().await;
  let port = node
    .get_host_port_ipv4(8086)
    .await
    .expect("Failed to get Bigtable port");
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

  // スナップショット付き更新 × 3（プレイメージは seq_nr = 1, 2, 3 の 3 件生まれる）
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

  // 剪定後の履歴行は新しい順に 2 件（プレイメージ seq_nr = 2, 3）だけが残る
  let history_keys = scan_history_row_keys(&client, &id).await;
  assert_eq!(history_keys, vec![history_row_key(&id, 2), history_row_key(&id, 3)]);

  // 現行行は最新状態のまま（剪定は履歴行のみに作用する）
  let snapshot = store
    .get_latest_snapshot_by_id(&id)
    .await
    .unwrap()
    .expect("snapshot must exist");
  assert_eq!(snapshot.version(), 4);
  assert_eq!(snapshot.seq_nr(), 4);
  assert_eq!(snapshot.aggregate().name, "history-3");

  // AC2.3.3: 現行行 payload は純ドメイン内容のみ（version / seq_nr / last_updated_at の複製なし）
  let current_payload = read_snapshot_cell(&client, snapshot_row_key(&id), b"payload")
    .await
    .expect("current payload cell must exist");
  let current_json: serde_json::Value = serde_json::from_slice(&current_payload).unwrap();
  let expected_current = UserAccount {
    id: id.clone(),
    name: "history-3".to_string(),
  };
  assert_eq!(current_json, serde_json::to_value(&expected_current).unwrap());

  // 履歴行（プレイメージ seq_nr = 3）の payload も純ドメイン内容のみで、更新前の状態を保持する
  let history_payload = read_snapshot_cell(&client, history_row_key(&id, 3), b"payload")
    .await
    .expect("history payload cell must exist");
  let history_json: serde_json::Value = serde_json::from_slice(&history_payload).unwrap();
  let expected_history = UserAccount {
    id: id.clone(),
    name: "history-2".to_string(),
  };
  assert_eq!(history_json, serde_json::to_value(&expected_history).unwrap());
}

// BR3.1 / AC2.3.4: keep_snapshot_count = None（既定）は履歴行を書かず剪定もしない
// （剪定なし設定に書込み増幅を持ち込まない — 現行挙動との互換）
#[tokio::test]
async fn test_event_store_on_bigtable_without_keep_snapshot_count_writes_no_history() {
  init_tracing();

  let node = bigtable_emulator().await;
  let port = node
    .get_host_port_ipv4(8086)
    .await
    .expect("Failed to get Bigtable port");
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

  let history_keys = scan_history_row_keys(&client, &id).await;
  assert!(
    history_keys.is_empty(),
    "no history rows must be written when keep_snapshot_count is None"
  );
}
