#![cfg(all(feature = "spanner", feature = "test-hooks"))]
//! Spanner の保存先の結合試験（エミュレーター）。
//!
//! 生成と設定の照合、初期化、追記、イベントとスナップショットの読み取り、保持、JavaScript 版と同じ行の形、
//! トークンの取得元、同時の追記を、実際のエミュレーターで確かめる。
//! 障害の差し込み（フック）が要る試験は `spanner_hooks_test.rs` に置く。

mod spanner_support;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use event_store_adapter_rs::{
  initialize_spanner_store, ConfigurationReason, ContractRule, EventStore, EventStoreError, SerializationPhase,
  SpannerOptions, SpannerRetention, SpannerTokenSource, StorageOperation,
};
use event_store_adapter_test_utils_rs::spanner::{self, Param, SeedConfiguration};
use serde_json::json;
use spanner_support::{
  event, event_at, event_with_payload, fnv1a64, hash_prefix, nanos_of, options_keeping, payload_bytes, snapshot,
  tables_with_prefix, ttl_options, unreachable_connection, ConfigRow, Env, Fixture, Id, JavascriptEvent, Store, PREFIX,
  RECORDED_STORE_ID,
};

fn reason_of<T: std::fmt::Debug>(result: &Result<T, EventStoreError>) -> Option<&ConfigurationReason> {
  match result {
    Err(EventStoreError::Configuration { reason }) => Some(reason),
    _ => None,
  }
}

async fn append_with_snapshots(store: &Store, id: &Id, range: std::ops::RangeInclusive<u64>) {
  for seq in range {
    store
      .persist_event_and_snapshot(event(id, seq), snapshot(seq))
      .await
      .unwrap();
  }
}

// ---------------------------------------------------------------------------
// 設定の検査は通信の前に行う（SPN-16・SPN-17・SPN-D1b）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_spanner_configuration_options_that_cannot_be_recorded_be_rejected_before_any_request() {
  let beyond_i64 = i64::MAX as u64 + 1;
  let cases: Vec<(&str, SpannerOptions, ConfigurationReason)> = vec![
    (
      "保持件数 0",
      options_keeping(0),
      ConfigurationReason::KeepSnapshotCountZero,
    ),
    (
      "保持件数が INT64 に収まらない",
      options_keeping(beyond_i64 as usize),
      ConfigurationReason::SpannerKeepSnapshotCountOutOfRange,
    ),
    (
      "猶予 0 日",
      ttl_options(1, 0),
      ConfigurationReason::SpannerTtlGraceDaysOutOfRange,
    ),
    (
      "猶予が INT64 に収まらない",
      ttl_options(1, beyond_i64),
      ConfigurationReason::SpannerTtlGraceDaysOutOfRange,
    ),
    (
      "変更フィードを求める",
      SpannerOptions {
        change_feed: true,
        ..SpannerOptions::default()
      },
      ConfigurationReason::SpannerChangeFeedUnsupported,
    ),
  ];

  for (label, options, expected) in cases {
    let opened = Store::open(unreachable_connection(), tables_with_prefix(PREFIX), options.clone()).await;
    let initialized = initialize_spanner_store(&unreachable_connection(), &tables_with_prefix(PREFIX), &options).await;

    assert_eq!(
      reason_of(&opened),
      Some(&expected),
      "open は通信の前に設定エラーにする（{label}）: {opened:?}"
    );
    assert_eq!(
      reason_of(&initialized),
      Some(&expected),
      "initialize も通信の前に設定エラーにする（{label}）: {initialized:?}"
    );
  }
}

#[tokio::test]
async fn should_spanner_largest_representable_options_pass_the_check_and_reach_the_network() {
  let options = SpannerOptions {
    retention: SpannerRetention::KeepLatestWithTtl {
      count: i64::MAX as usize,
      grace_days: i64::MAX as u64,
    },
    ..SpannerOptions::default()
  };

  let opened = Store::open(unreachable_connection(), tables_with_prefix(PREFIX), options).await;

  assert!(
    matches!(opened, Err(EventStoreError::Storage { .. })),
    "境界の値は設定エラーにせず、通信の失敗（保存先エラー）になる: {opened:?}"
  );
}

#[tokio::test]
async fn should_spanner_table_prefix_that_makes_an_invalid_identifier_be_rejected_before_any_request() {
  let too_long = "a".repeat(112); // "<prefix>_snapshot_history" が 129 文字になる
  for prefix in ["esa-1", "1esa", "esa 1", "esa`x", "esa;x", too_long.as_str()] {
    let options = SpannerOptions::default();
    let opened = Store::open(unreachable_connection(), tables_with_prefix(prefix), options.clone()).await;
    let initialized = initialize_spanner_store(&unreachable_connection(), &tables_with_prefix(prefix), &options).await;

    assert_eq!(
      reason_of(&opened),
      Some(&ConfigurationReason::SpannerInvalidTablePrefix),
      "open: {prefix:?}: {opened:?}"
    );
    assert_eq!(
      reason_of(&initialized),
      Some(&ConfigurationReason::SpannerInvalidTablePrefix),
      "initialize: {prefix:?}: {initialized:?}"
    );
  }
}

#[tokio::test]
async fn should_spanner_table_prefix_that_makes_the_longest_valid_table_name_be_accepted() {
  let prefix = "a".repeat(111); // "<prefix>_snapshot_history" がちょうど 128 文字
  let fixture = Fixture::with_statements(spanner::store_ddl(&prefix, None), &prefix).await;
  fixture.initialize(&SpannerOptions::default()).await.unwrap();

  let store = fixture.open(SpannerOptions::default()).await.unwrap();

  store.persist_event(event(&Id::order(), 1)).await.unwrap();
  assert_eq!(fixture.journal_rows().await.len(), 1);
}

#[tokio::test]
async fn should_spanner_derive_the_five_table_names_from_the_prefix_and_check_only_those() {
  let mut statements = spanner::store_ddl("esa", None);
  statements.extend(spanner::store_ddl("other", None));
  let fixture = Fixture::with_statements(statements, "esa").await;
  let other = Env {
    tables: event_store_adapter_rs::SpannerTables {
      table_prefix: "other".to_string(),
      ..fixture.env().tables
    },
    ..fixture.env()
  };
  fixture.initialize(&options_keeping(2)).await.unwrap();
  other.initialize(&options_keeping(3)).await.unwrap();

  let esa = fixture.open(options_keeping(2)).await;
  let other_store = other.open(options_keeping(3)).await;
  let esa_with_the_other_count = fixture.open(options_keeping(3)).await;

  assert!(esa.is_ok(), "esa_config の保持件数 2 と一致する: {esa:?}");
  assert!(
    other_store.is_ok(),
    "other_config の保持件数 3 と一致する: {other_store:?}"
  );
  assert_eq!(
    reason_of(&esa_with_the_other_count),
    Some(&ConfigurationReason::SpannerConfigurationMismatch),
    "接頭辞 esa は esa_config だけを照合する（other_config の 3 とは比べない）"
  );
  assert_eq!(fixture.config_rows().await[0].keep_snapshot_count, Some(2));
  assert_eq!(other.config_rows().await[0].keep_snapshot_count, Some(3));
}

// ---------------------------------------------------------------------------
// 初期化（SPN-D5）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_spanner_open_before_initialization_be_a_configuration_error_and_create_nothing() {
  let fixture = Fixture::new().await;

  let opened = fixture.open(SpannerOptions::default()).await;

  assert_eq!(
    reason_of(&opened),
    Some(&ConfigurationReason::SpannerConfigurationInvalid),
    "{opened:?}"
  );
  assert_eq!(
    fixture.row_counts().await,
    (0, 0, 0, 0, 0),
    "生成は行もテーブルも作らない"
  );
}

fn assert_lowercase_uuid_v4(text: &str) {
  assert_eq!(text.len(), 36, "{text}");
  assert_eq!(text, text.to_ascii_lowercase(), "store_id は小文字の UUID: {text}");
  let parts: Vec<&str> = text.split('-').collect();
  assert_eq!(
    parts.iter().map(|part| part.len()).collect::<Vec<_>>(),
    vec![8, 4, 4, 4, 12]
  );
  assert!(text.chars().all(|c| c == '-' || c.is_ascii_hexdigit()), "{text}");
  assert!(parts[2].starts_with('4'), "UUID v4: {text}");
  assert!(
    ["8", "9", "a", "b"].contains(&&parts[3][..1]),
    "UUID の variant: {text}"
  );
}

#[tokio::test]
async fn should_spanner_initialization_record_one_configuration_row_for_each_retention_style() {
  /// 初期化する設定と、データベースに付ける行削除ポリシーの日数、記録されるはずの値。
  struct Style {
    options: SpannerOptions,
    policy_days: Option<i64>,
    keep: Option<i64>,
    mode: &'static str,
    grace: Option<i64>,
  }
  let cases = vec![
    Style {
      options: SpannerOptions::default(),
      policy_days: None,
      keep: None,
      mode: "delete",
      grace: None,
    },
    Style {
      options: options_keeping(3),
      policy_days: None,
      keep: Some(3),
      mode: "delete",
      grace: None,
    },
    Style {
      options: ttl_options(2, 7),
      policy_days: Some(7),
      keep: Some(2),
      mode: "ttl",
      grace: Some(7),
    },
  ];

  for Style {
    options,
    policy_days,
    keep,
    mode,
    grace,
  } in cases
  {
    let fixture = Fixture::with_policy_days(policy_days).await;

    fixture.initialize(&options).await.unwrap();

    let rows = fixture.config_rows().await;
    assert_eq!(rows.len(), 1, "{options:?}");
    assert_lowercase_uuid_v4(&rows[0].store_id);
    assert_eq!(
      ConfigRow {
        store_id: RECORDED_STORE_ID.to_string(),
        ..rows[0].clone()
      },
      ConfigRow {
        singleton: 1,
        store_id: RECORDED_STORE_ID.to_string(),
        layout_version: 1,
        keep_snapshot_count: keep,
        retention_mode: mode.to_string(),
        ttl_grace_days: grace,
        change_feed_enabled: false,
      },
      "{options:?}"
    );
    assert!(fixture.open(options).await.is_ok(), "初期化の後は同じ設定で開ける");
  }
}

#[tokio::test]
async fn should_spanner_second_initialization_be_already_initialized_and_keep_the_recorded_configuration() {
  let fixture = Fixture::initialized(&options_keeping(2)).await;
  let recorded = fixture.config_rows().await;

  let again = fixture.initialize(&options_keeping(5)).await;

  assert_eq!(
    reason_of(&again),
    Some(&ConfigurationReason::SpannerAlreadyInitialized),
    "{again:?}"
  );
  assert_eq!(fixture.config_rows().await, recorded, "記録した設定を上書きしない");
}

#[tokio::test]
async fn should_spanner_initialization_of_non_empty_tables_be_already_initialized_and_write_no_configuration() {
  let fixture = Fixture::new().await;
  let id = Id::order();
  fixture
    .write_like_javascript(
      &JavascriptEvent {
        type_name: "Order",
        aid: &id.aid(),
        seq_nr: 1,
        nanos: 1_700_000_000_000_000_000,
        manifest: "",
        payload: br#"{"number":1}"#,
        snapshot: None,
      },
      false,
    )
    .await;

  let initialized = fixture.initialize(&SpannerOptions::default()).await;

  assert_eq!(
    reason_of(&initialized),
    Some(&ConfigurationReason::SpannerAlreadyInitialized),
    "既にイベントがあるテーブルは、空であることの確認で拒否する: {initialized:?}"
  );
  assert!(fixture.config_rows().await.is_empty(), "config の行を書かない");
}

#[tokio::test]
async fn should_spanner_initialization_never_create_a_missing_table() {
  let mut statements = spanner::store_ddl("esa", None);
  statements.pop(); // config テーブルを作らない
  let fixture = Fixture::with_statements(statements, "esa").await;

  let initialized = fixture.initialize(&SpannerOptions::default()).await;

  assert_eq!(
    reason_of(&initialized),
    Some(&ConfigurationReason::SpannerLayoutMismatch),
    "{initialized:?}"
  );
  let tables = fixture
    .raw()
    .await
    .query(
      "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '' AND TABLE_NAME = 'esa_config'",
      &[],
    )
    .await
    .unwrap();
  assert!(tables.is_empty(), "DDL を実行しない: {tables:?}");
}

// ---------------------------------------------------------------------------
// 生成時の照合（SPN-2・SPN-16・SPN-17）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_spanner_open_reject_a_layout_that_differs_from_the_profile() {
  let broken_statements: Vec<(&str, Vec<String>)> = {
    let base = spanner::store_ddl("esa", None);
    let mut without_payload = base.clone();
    without_payload[0] = without_payload[0].replace(", payload BYTES(MAX) NOT NULL", "");
    let mut narrow_aid = base.clone();
    narrow_aid[0] = narrow_aid[0].replace("aid STRING(1024)", "aid STRING(512)");
    let mut reordered_key = base.clone();
    reordered_key[0] = reordered_key[0].replace(
      "PRIMARY KEY (hash_prefix, aid, seq_nr)",
      "PRIMARY KEY (aid, hash_prefix, seq_nr)",
    );
    let mut without_history = base.clone();
    without_history.remove(3);
    vec![
      ("journal に payload 列がない", without_payload),
      ("journal の aid の長さが違う", narrow_aid),
      ("journal の主キーの順が違う", reordered_key),
      ("snapshot_history テーブルがない", without_history),
    ]
  };

  for (label, statements) in broken_statements {
    let fixture = Fixture::with_statements(statements, "esa").await;
    fixture
      .seed_configuration(&SeedConfiguration::current_only(RECORDED_STORE_ID))
      .await;

    let opened = fixture.open(SpannerOptions::default()).await;

    assert_eq!(
      reason_of(&opened),
      Some(&ConfigurationReason::SpannerLayoutMismatch),
      "{label}: {opened:?}"
    );
  }
}

#[tokio::test]
async fn should_spanner_open_reject_a_database_of_the_postgresql_dialect() {
  let emulator = spanner::shared_emulator().unwrap();
  let channel = spanner::connect(emulator.port()).await.unwrap();
  spanner::create_postgresql_database(&channel, "pg-dialect-db")
    .await
    .unwrap();
  let tables = event_store_adapter_rs::SpannerTables {
    project_id: spanner::PROJECT.to_string(),
    instance_id: spanner::INSTANCE.to_string(),
    database_id: "pg-dialect-db".to_string(),
    table_prefix: PREFIX.to_string(),
  };

  let opened = Store::open(
    event_store_adapter_rs::SpannerConnection::new(channel),
    tables,
    SpannerOptions::default(),
  )
  .await;

  assert_eq!(
    reason_of(&opened),
    Some(&ConfigurationReason::SpannerLayoutMismatch),
    "GoogleSQL 以外の方言は配置の不一致: {opened:?}"
  );
}

fn seeded(
  keep: Option<i64>,
  mode: &str,
  grace: Option<i64>,
  layout_version: i64,
  change_feed_enabled: bool,
) -> SeedConfiguration {
  SeedConfiguration {
    store_id: RECORDED_STORE_ID.to_string(),
    layout_version,
    keep_snapshot_count: keep,
    retention_mode: mode.to_string(),
    ttl_grace_days: grace,
    change_feed_enabled,
  }
}

#[tokio::test]
async fn should_spanner_open_check_the_recorded_configuration_and_the_row_deletion_policy_against_the_client() {
  type Case = (
    &'static str,
    Option<i64>,
    SeedConfiguration,
    SpannerOptions,
    Result<(), ConfigurationReason>,
  );
  let mismatch = ConfigurationReason::SpannerConfigurationMismatch;
  let cases: Vec<Case> = vec![
    (
      "記録した配置の版が 1 でない",
      None,
      seeded(None, "delete", None, 2, false),
      SpannerOptions::default(),
      Err(mismatch.clone()),
    ),
    (
      "保持件数が違う（記録 2・クライアント 1）",
      None,
      seeded(Some(2), "delete", None, 1, false),
      options_keeping(1),
      Err(mismatch.clone()),
    ),
    (
      "記録に保持件数があり、クライアントにない",
      None,
      seeded(Some(1), "delete", None, 1, false),
      SpannerOptions::default(),
      Err(mismatch.clone()),
    ),
    (
      "記録に保持件数がなく、クライアントにある",
      None,
      seeded(None, "delete", None, 1, false),
      options_keeping(1),
      Err(mismatch.clone()),
    ),
    (
      "保持方式が違う（記録 ttl・クライアント delete）",
      Some(7),
      seeded(Some(1), "ttl", Some(7), 1, false),
      options_keeping(1),
      Err(mismatch.clone()),
    ),
    (
      "猶予の日数が、記録だけ違う（記録 8・ポリシー 7・クライアント 7）",
      Some(7),
      seeded(Some(1), "ttl", Some(8), 1, false),
      ttl_options(1, 7),
      Err(mismatch.clone()),
    ),
    (
      "猶予の日数が、ポリシーだけ違う（記録 7・ポリシー 8・クライアント 7）",
      Some(8),
      seeded(Some(1), "ttl", Some(7), 1, false),
      ttl_options(1, 7),
      Err(mismatch.clone()),
    ),
    (
      "猶予の日数が、クライアントだけ違う（記録 7・ポリシー 7・クライアント 6）",
      Some(7),
      seeded(Some(1), "ttl", Some(7), 1, false),
      ttl_options(1, 6),
      Err(mismatch.clone()),
    ),
    (
      "ttl なのに行削除ポリシーがない",
      None,
      seeded(Some(1), "ttl", Some(7), 1, false),
      ttl_options(1, 7),
      Err(mismatch.clone()),
    ),
    (
      "delete なのに行削除ポリシーがある",
      Some(7),
      seeded(Some(1), "delete", None, 1, false),
      options_keeping(1),
      Err(mismatch.clone()),
    ),
    (
      "記録した change_feed_enabled が true",
      None,
      seeded(None, "delete", None, 1, true),
      SpannerOptions::default(),
      Err(ConfigurationReason::SpannerChangeFeedUnsupported),
    ),
    (
      "記録と一致する（delete・保持件数 1）",
      None,
      seeded(Some(1), "delete", None, 1, false),
      options_keeping(1),
      Ok(()),
    ),
    (
      "記録・ポリシー・クライアントの猶予が一致する（ttl）",
      Some(7),
      seeded(Some(1), "ttl", Some(7), 1, false),
      ttl_options(1, 7),
      Ok(()),
    ),
  ];

  for (label, policy_days, seed, options, expected) in cases {
    let fixture = Fixture::with_policy_days(policy_days).await;
    fixture.seed_configuration(&seed).await;

    let opened = fixture.open(options).await;

    match expected {
      Ok(()) => assert!(opened.is_ok(), "{label}: {opened:?}"),
      Err(reason) => assert_eq!(reason_of(&opened), Some(&reason), "{label}: {opened:?}"),
    }
  }
}

#[tokio::test]
async fn should_spanner_open_reject_a_malformed_configuration_row() {
  for store_id in ["5F0C8B8E-3C1A-4D7E-9A4B-2F6D1C0E7A11", "not-a-uuid", ""] {
    let fixture = Fixture::new().await;
    fixture
      .seed_configuration(&SeedConfiguration {
        store_id: store_id.to_string(),
        ..SeedConfiguration::current_only(RECORDED_STORE_ID)
      })
      .await;

    let opened = fixture.open(SpannerOptions::default()).await;

    assert_eq!(
      reason_of(&opened),
      Some(&ConfigurationReason::SpannerConfigurationInvalid),
      "store_id が小文字の UUID でない: {store_id:?}: {opened:?}"
    );
  }
}

// ---------------------------------------------------------------------------
// 追記と読み取り（SPN-6・SPN-9・SPN-10）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_spanner_round_trip_events_and_snapshots_and_read_them_again_after_a_reopen() {
  let fixture = Fixture::initialized(&SpannerOptions::default()).await;
  let store = fixture.open(SpannerOptions::default()).await.unwrap();
  let id = Id::order();
  store.persist_event(event(&id, 1)).await.unwrap();
  store
    .persist_event_and_snapshot(event(&id, 2), snapshot(2))
    .await
    .unwrap();
  store.persist_event(event(&id, 3)).await.unwrap();

  let reopened = fixture.open(SpannerOptions::default()).await.unwrap();

  assert_eq!(
    reopened.get_events_by_id_since_seq_nr(&id, 1).await.unwrap(),
    vec![event(&id, 1), event(&id, 2), event(&id, 3)]
  );
  let latest = reopened.get_latest_snapshot_by_id(&id).await.unwrap().unwrap();
  assert_eq!(latest.head_seq_nr(), 3, "ヘッドはイベントだけの追記で進む");
  let current = latest.snapshot().expect("現在のスナップショットは保存したまま");
  assert_eq!(current.seq_nr(), 2);
  assert_eq!(current.aggregate(), &json!({ "state": 2 }));
}

#[tokio::test]
async fn should_spanner_creation_with_events_only_leave_no_snapshot_row_and_read_as_an_existing_aggregate() {
  let fixture = Fixture::initialized(&options_keeping(2)).await;
  let store = fixture.open(options_keeping(2)).await.unwrap();
  let id = Id::order();
  assert!(
    store.get_latest_snapshot_by_id(&id).await.unwrap().is_none(),
    "ヘッドがなければ None"
  );

  store.persist_event(event(&id, 1)).await.unwrap();

  let read = store.get_latest_snapshot_by_id(&id).await.unwrap().unwrap();
  assert_eq!(read.head_seq_nr(), 1);
  assert!(read.snapshot().is_none(), "snapshot がなくても h と「なし」を返す");
  assert_eq!(fixture.current_rows().await.len(), 0, "snapshot_current に行を作らない");
  assert_eq!(fixture.history_rows().await.len(), 0, "snapshot_history に行を作らない");
}

#[tokio::test]
async fn should_spanner_duplicate_stale_and_existing_creation_appends_be_optimistic_locks_that_leave_rows_untouched() {
  let fixture = Fixture::initialized(&SpannerOptions::default()).await;
  let store = fixture.open(SpannerOptions::default()).await.unwrap();
  let id = Id::order();
  store.persist_event(event(&id, 1)).await.unwrap();
  store.persist_event(event(&id, 2)).await.unwrap();
  let journal_before = fixture.journal_rows().await;
  let head_before = fixture.head_rows().await;

  let duplicate = store
    .persist_event(event_with_payload(&id, 2, json!({ "number": 99 })))
    .await;
  let stale = store.persist_event(event(&id, 1)).await;
  let creation = store.persist_event(event(&id, 1)).await;

  for (label, result, seq_nr) in [
    ("重複", &duplicate, 2),
    ("古い番号", &stale, 1),
    ("既存への作成", &creation, 1),
  ] {
    assert!(
      matches!(
        result,
        Err(EventStoreError::OptimisticLock { aid, seq_nr: actual, head_seq_nr: Some(2) })
          if aid == "Order-1" && *actual == seq_nr
      ),
      "{label}: 契約違反や保存先エラーにしない: {result:?}"
    );
  }
  assert_eq!(fixture.journal_rows().await, journal_before, "journal を上書きしない");
  assert_eq!(fixture.head_rows().await, head_before, "head を変えない");
}

#[tokio::test]
async fn should_spanner_gap_append_and_update_without_a_head_be_a_w8_contract_violation_and_write_nothing() {
  let fixture = Fixture::initialized(&SpannerOptions::default()).await;
  let store = fixture.open(SpannerOptions::default()).await.unwrap();
  let id = Id::order();
  let without_head = store.persist_event(event(&id, 2)).await;
  store.persist_event(event(&id, 1)).await.unwrap();

  let gap = store.persist_event(event(&id, 3)).await;

  assert!(
    matches!(
      without_head,
      Err(EventStoreError::ContractViolation {
        rule: ContractRule::W8Gap,
        seq_nr: Some(2),
        ..
      })
    ),
    "head がなく 2 以上: {without_head:?}"
  );
  assert!(
    matches!(
      gap,
      Err(EventStoreError::ContractViolation {
        rule: ContractRule::W8Gap,
        seq_nr: Some(3),
        ..
      })
    ),
    "飛び番: {gap:?}"
  );
  assert_eq!(fixture.journal_rows().await.len(), 1, "飛び番の追記は何も書かない");
  assert_eq!(fixture.head_rows().await[0].seq_nr, 1);
}

#[tokio::test]
async fn should_spanner_get_events_return_the_range_from_the_requested_number_up_to_the_head_in_ascending_order() {
  let fixture = Fixture::initialized(&SpannerOptions::default()).await;
  let store = fixture.open(SpannerOptions::default()).await.unwrap();
  let id = Id::order();
  for seq in 1..=5 {
    store.persist_event(event(&id, seq)).await.unwrap();
  }
  let other = Id::new("Order", "12");
  store.persist_event(event(&other, 1)).await.unwrap();

  let all = store.get_events_by_id_since_seq_nr(&id, 0).await.unwrap();
  let from_three = store.get_events_by_id_since_seq_nr(&id, 3).await.unwrap();
  let last = store.get_events_by_id_since_seq_nr(&id, 5).await.unwrap();
  let beyond_head = store.get_events_by_id_since_seq_nr(&id, 6).await.unwrap();
  let unknown = store
    .get_events_by_id_since_seq_nr(&Id::new("Order", "404"), 0)
    .await
    .unwrap();

  assert_eq!(
    all,
    (1..=5).map(|seq| event(&id, seq)).collect::<Vec<_>>(),
    "0 は 1 から読む"
  );
  assert_eq!(from_three, (3..=5).map(|seq| event(&id, seq)).collect::<Vec<_>>());
  assert_eq!(last, vec![event(&id, 5)]);
  assert!(beyond_head.is_empty(), "h より大きい番号は空の列");
  assert!(unknown.is_empty(), "head がなければ空の列");
}

#[tokio::test]
async fn should_spanner_read_events_report_a_storage_error_for_a_missing_journal_row() {
  let fixture = Fixture::initialized(&SpannerOptions::default()).await;
  let store = fixture.open(SpannerOptions::default()).await.unwrap();
  let id = Id::order();
  for seq in 1..=3 {
    store.persist_event(event(&id, seq)).await.unwrap();
  }
  fixture
    .raw_execute(
      "DELETE FROM esa_journal WHERE aid = @aid AND seq_nr = 2",
      &[("aid", Param::String(id.aid()))],
    )
    .await;

  let across_the_gap = store.get_events_by_id_since_seq_nr(&id, 1).await;
  let after_the_gap = store.get_events_by_id_since_seq_nr(&id, 3).await.unwrap();

  assert!(
    matches!(
      across_the_gap,
      Err(EventStoreError::Storage {
        operation: StorageOperation::LoadEvents,
        ..
      })
    ),
    "範囲の欠番は、短い列を成功として返さず保存先エラー: {across_the_gap:?}"
  );
  assert_eq!(after_the_gap, vec![event(&id, 3)], "欠番を含まない範囲は読める");
}

#[tokio::test]
async fn should_spanner_undecodable_payloads_be_serialization_errors_and_not_storage_errors() {
  let fixture = Fixture::initialized(&SpannerOptions::default()).await;
  let store = fixture.open(SpannerOptions::default()).await.unwrap();
  let id = Id::order();
  store
    .persist_event_and_snapshot(event(&id, 1), snapshot(1))
    .await
    .unwrap();
  fixture
    .raw_execute(
      "UPDATE esa_journal SET payload = @payload WHERE aid = @aid",
      &[
        ("payload", Param::Bytes(b"not json".to_vec())),
        ("aid", Param::String(id.aid())),
      ],
    )
    .await;
  fixture
    .raw_execute(
      "UPDATE esa_snapshot_current SET payload = @payload WHERE aid = @aid",
      &[
        ("payload", Param::Bytes(b"not json".to_vec())),
        ("aid", Param::String(id.aid())),
      ],
    )
    .await;

  let events = store.get_events_by_id_since_seq_nr(&id, 1).await;
  let latest = store.get_latest_snapshot_by_id(&id).await;

  assert!(
    matches!(
      events,
      Err(EventStoreError::Serialization {
        phase: SerializationPhase::DeserializeEvent,
        ..
      })
    ),
    "{events:?}"
  );
  assert!(
    matches!(
      latest,
      Err(EventStoreError::Serialization {
        phase: SerializationPhase::DeserializeSnapshot,
        ..
      })
    ),
    "{latest:?}"
  );
}

#[tokio::test]
async fn should_spanner_open_against_an_unreachable_endpoint_be_a_storage_error() {
  let opened = Store::open(
    unreachable_connection(),
    tables_with_prefix(PREFIX),
    SpannerOptions::default(),
  )
  .await;

  assert!(matches!(opened, Err(EventStoreError::Storage { .. })), "{opened:?}");
}

// ---------------------------------------------------------------------------
// 時刻と大きな値（SPN-3・SPN-5・R-5）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_spanner_keep_the_nanosecond_boundaries_without_loss_in_the_int64_column() {
  let fixture = Fixture::initialized(&SpannerOptions::default()).await;
  let store = fixture.open(SpannerOptions::default()).await.unwrap();
  let min = DateTime::<Utc>::from_timestamp_nanos(i64::MIN);
  let max = DateTime::<Utc>::from_timestamp_nanos(i64::MAX);
  let early = Id::new("Order", "min");
  let late = Id::new("Order", "max");
  store.persist_event(event_at(&early, 1, min)).await.unwrap();
  store.persist_event(event_at(&late, 1, max)).await.unwrap();

  let read_early = store.get_events_by_id_since_seq_nr(&early, 1).await.unwrap();
  let read_late = store.get_events_by_id_since_seq_nr(&late, 1).await.unwrap();

  assert_eq!(*read_early[0].occurred_at(), min, "1677 年の境界が往復で変わらない");
  assert_eq!(*read_late[0].occurred_at(), max, "2262 年の境界が往復で変わらない");
  let journal = fixture.journal_rows().await;
  let nanos_of_aid = |aid: &str| journal.iter().find(|row| row.aid == aid).unwrap().occurred_at_ns;
  assert_eq!(nanos_of_aid("Order-min"), i64::MIN, "TIMESTAMP ではなく INT64 のナノ秒");
  assert_eq!(nanos_of_aid("Order-max"), i64::MAX);
}

// エミュレーター 1.5.58 は、ExecuteStreamingSql で 1 MiB の BYTES を `chunked_value` つきの複数の応答に分けて返す
// （`test-utils` の `test_spanner_emulator_streams_a_one_mebibyte_value_in_chunks` で確認している）。
// 2 MiB の payload は、journal と head の両方で分割され、結合して返さなければ元の値に戻らない。
// 3 MiB を超えると、非ストリーミングの ExecuteSql の応答が tonic の既定の受信上限（4 MiB）を超えるので、生の読み取りの補助が読めない。
#[tokio::test]
async fn should_spanner_round_trip_a_payload_that_the_service_returns_in_chunks() {
  let fixture = Fixture::initialized(&SpannerOptions::default()).await;
  let store = fixture.open(SpannerOptions::default()).await.unwrap();
  let id = Id::order();
  let large = json!({ "blob": "x".repeat(2 * 1024 * 1024) });
  store
    .persist_event_and_snapshot(event_with_payload(&id, 1, large.clone()), snapshot(1))
    .await
    .unwrap();

  let events = store.get_events_by_id_since_seq_nr(&id, 1).await.unwrap();

  assert_eq!(
    events,
    vec![event_with_payload(&id, 1, large.clone())],
    "分割された値を結合して返す"
  );
  assert_eq!(fixture.journal_rows().await[0].payload, payload_bytes(&large));
}

// ---------------------------------------------------------------------------
// 保持（SPN-11〜SPN-13・S-1〜S-3）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_spanner_retention_keep_the_newest_n_history_rows_and_never_touch_journal_head_or_current() {
  let fixture = Fixture::initialized(&options_keeping(2)).await;
  let store = fixture.open(options_keeping(2)).await.unwrap();
  let id = Id::order();
  let other = Id::new("Order", "2");
  store
    .persist_event_and_snapshot(event(&other, 1), snapshot(1))
    .await
    .unwrap();

  append_with_snapshots(&store, &id, 1..=5).await;

  let history = fixture.history_rows().await;
  let of = |aid: &str| -> Vec<i64> {
    history
      .iter()
      .filter(|row| row.row.aid == aid)
      .map(|row| row.row.seq_nr)
      .collect()
  };
  assert_eq!(of("Order-1"), vec![4, 5], "新しい 2 件だけを残す");
  assert_eq!(of("Order-2"), vec![1], "ほかの集約の履歴を巻き込まない");
  assert!(
    history.iter().all(|row| row.expired_at.is_none()),
    "即時削除方式は印を付けない"
  );
  assert_eq!(fixture.journal_rows().await.len(), 6, "journal は変えない");
  assert_eq!(
    fixture
      .head_rows()
      .await
      .iter()
      .find(|row| row.aid == "Order-1")
      .unwrap()
      .seq_nr,
    5
  );
  assert_eq!(
    fixture
      .current_rows()
      .await
      .iter()
      .find(|row| row.aid == "Order-1")
      .unwrap()
      .seq_nr,
    5,
    "snapshot_current は最新のまま"
  );
}

#[tokio::test]
async fn should_spanner_events_only_appends_and_current_only_stores_leave_the_history_untouched() {
  let kept = Fixture::initialized(&options_keeping(1)).await;
  let store = kept.open(options_keeping(1)).await.unwrap();
  let id = Id::order();
  append_with_snapshots(&store, &id, 1..=2).await;
  store.persist_event(event(&id, 3)).await.unwrap();
  let current_only = Fixture::initialized(&SpannerOptions::default()).await;
  let plain = current_only.open(SpannerOptions::default()).await.unwrap();

  append_with_snapshots(&plain, &id, 1..=3).await;

  assert_eq!(
    kept
      .history_rows()
      .await
      .iter()
      .map(|row| row.row.seq_nr)
      .collect::<Vec<_>>(),
    vec![2],
    "イベントだけの追記は履歴を変えない"
  );
  assert!(
    current_only.history_rows().await.is_empty(),
    "保持件数がなければ履歴を作らない"
  );
  assert_eq!(current_only.current_rows().await[0].seq_nr, 3);
}

#[tokio::test]
async fn should_spanner_ttl_retention_mark_old_history_once_and_never_count_or_remark_marked_rows() {
  let options = ttl_options(2, 7);
  let fixture = Fixture::initialized(&options).await;
  let store = fixture.open(options).await.unwrap();
  let id = Id::order();

  append_with_snapshots(&store, &id, 1..=3).await;
  let after_three = fixture.history_rows().await;
  append_with_snapshots(&store, &id, 4..=5).await;

  let after_five = fixture.history_rows().await;
  let marked: Vec<i64> = after_five
    .iter()
    .filter(|row| row.expired_at.is_some())
    .map(|row| row.row.seq_nr)
    .collect();
  let unmarked: Vec<i64> = after_five
    .iter()
    .filter(|row| row.expired_at.is_none())
    .map(|row| row.row.seq_nr)
    .collect();
  assert_eq!(marked, vec![1, 2, 3], "古い行だけに印を付ける（行は削除しない）");
  assert_eq!(unmarked, vec![4, 5], "印のない新しい 2 件が保持件数");
  assert_eq!(after_three.iter().filter(|row| row.expired_at.is_some()).count(), 1);
  assert_eq!(
    after_five[0].expired_at, after_three[0].expired_at,
    "印は 1 度だけ。後の保持で付け直さない"
  );
  assert_eq!(fixture.journal_rows().await.len(), 5, "journal は変えない");
}

// ---------------------------------------------------------------------------
// JavaScript 版と同じ行の形（SPN-3・SPN-5）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_spanner_read_rows_written_with_the_javascript_statements() {
  let fixture = Fixture::initialized(&options_keeping(2)).await;
  let id = Id::order();
  let aid = id.aid();
  // JavaScript 版の発生時刻はミリ秒の値を 1e6 倍したナノ秒で書く。
  let millis_nanos = |seq: i64| 1_790_985_600_123_000_000 + seq * 1_000_000_000;
  for seq in 1..=3_i64 {
    let payload = format!(r#"{{"number":{seq}}}"#);
    let state = format!(r#"{{"state":{seq}}}"#);
    fixture
      .write_like_javascript(
        &JavascriptEvent {
          type_name: "Order",
          aid: &aid,
          seq_nr: seq,
          nanos: millis_nanos(seq),
          manifest: "",
          payload: payload.as_bytes(),
          snapshot: (seq >= 2).then_some((seq, "", state.as_bytes())),
        },
        true,
      )
      .await;
  }

  let store = fixture.open(options_keeping(2)).await.unwrap();
  let events = store.get_events_by_id_since_seq_nr(&id, 1).await.unwrap();
  let latest = store.get_latest_snapshot_by_id(&id).await.unwrap().unwrap();

  assert_eq!(events.len(), 3);
  for (index, seq) in (1..=3_u64).enumerate() {
    assert_eq!(events[index].seq_nr(), seq);
    assert_eq!(events[index].payload(), &json!({ "number": seq }));
    assert_eq!(
      nanos_of(*events[index].occurred_at()),
      millis_nanos(seq as i64),
      "ミリ秒の値を 1e6 倍した発生時刻を、そのまま読む"
    );
  }
  assert_eq!(latest.head_seq_nr(), 3);
  assert_eq!(latest.snapshot().unwrap().seq_nr(), 3);
  assert_eq!(latest.snapshot().unwrap().aggregate(), &json!({ "state": 3 }));
  // JavaScript 版が書いた行の続きに、rs が追記できる。
  store
    .persist_event_and_snapshot(event(&id, 4), snapshot(4))
    .await
    .unwrap();
  let head = fixture.head_rows().await;
  assert_eq!(head.len(), 1);
  assert_eq!(head[0].seq_nr, 4);
  assert_eq!(head[0].type_name, "Order", "head の type_name を書き換えない");
  assert_eq!(
    fixture
      .history_rows()
      .await
      .iter()
      .map(|row| row.row.seq_nr)
      .collect::<Vec<_>>(),
    vec![3, 4],
    "JavaScript 版が書いた履歴も、保持の対象になる"
  );
}

#[tokio::test]
async fn should_spanner_write_rows_in_the_javascript_shape() {
  let fixture = Fixture::initialized(&options_keeping(2)).await;
  let store = fixture.open(options_keeping(2)).await.unwrap();
  let id = Id::order();
  let aid = id.aid();
  let expected_prefix = fnv1a64(aid.as_bytes()).to_be_bytes().to_vec();
  assert_eq!(expected_prefix, hash_prefix(&aid));

  store
    .persist_event_and_snapshot(event(&id, 1), snapshot(1))
    .await
    .unwrap();
  store.persist_event(event(&id, 2)).await.unwrap();

  let journal = fixture.journal_rows().await;
  assert_eq!(journal.len(), 2);
  for (row, seq) in journal.iter().zip(1..=2_u64) {
    assert_eq!(
      row.hash_prefix, expected_prefix,
      "H(aid): FNV-1a 64 の 8 バイトのビッグエンディアン"
    );
    assert_eq!(row.aid, "Order-1");
    assert_eq!(row.seq_nr, seq as i64);
    assert_eq!(row.occurred_at_ns, nanos_of(spanner_support::occurred_at(seq)));
    assert_eq!(row.manifest, "");
    assert_eq!(row.payload, payload_bytes(&json!({ "number": seq })), "JSON のバイト列");
  }
  let head = fixture.head_rows().await;
  assert_eq!(head.len(), 1);
  assert_eq!(head[0].hash_prefix, expected_prefix);
  assert_eq!(head[0].aid, "Order-1");
  assert_eq!(head[0].type_name, "Order");
  assert_eq!(head[0].seq_nr, 2);
  assert_eq!(head[0].event_occurred_at_ns, nanos_of(spanner_support::occurred_at(2)));
  assert_eq!(head[0].event_manifest, "");
  assert_eq!(
    head[0].event_payload,
    payload_bytes(&json!({ "number": 2 })),
    "head は最新のイベントの封筒を持つ"
  );
  let current = fixture.current_rows().await;
  assert_eq!(current.len(), 1);
  assert_eq!(current[0].hash_prefix, expected_prefix);
  assert_eq!(current[0].seq_nr, 1);
  assert_eq!(current[0].manifest, "");
  assert_eq!(current[0].payload, payload_bytes(&json!({ "state": 1 })));
  let history = fixture.history_rows().await;
  assert_eq!(history.len(), 1);
  assert_eq!(history[0].row, current[0], "履歴は現在のスナップショットと同じ封筒");
  assert_eq!(history[0].expired_at, None, "expired_at は NULL");
  let config = fixture.config_rows().await;
  assert_eq!(config.len(), 1);
  assert_eq!(config[0].singleton, 1);
  assert_eq!(config[0].layout_version, 1);
  assert_eq!(config[0].keep_snapshot_count, Some(2));
  assert_eq!(config[0].retention_mode, "delete");
  assert_eq!(config[0].ttl_grace_days, None);
  assert!(!config[0].change_feed_enabled);
}

#[tokio::test]
async fn should_spanner_compare_keys_by_exact_match_and_not_by_the_hash_prefix_alone() {
  let fixture = Fixture::initialized(&SpannerOptions::default()).await;
  let store = fixture.open(SpannerOptions::default()).await.unwrap();
  let order_1 = Id::new("Order", "1");
  let order_12 = Id::new("Order", "12");
  store.persist_event(event(&order_1, 1)).await.unwrap();
  store.persist_event(event(&order_12, 1)).await.unwrap();
  store.persist_event(event(&order_12, 2)).await.unwrap();

  let events = store.get_events_by_id_since_seq_nr(&order_1, 1).await.unwrap();

  assert_eq!(events, vec![event(&order_1, 1)], "aid の前方一致で Order-12 を混ぜない");
  let prefixes: Vec<Vec<u8>> = fixture
    .head_rows()
    .await
    .into_iter()
    .map(|row| row.hash_prefix)
    .collect();
  assert_eq!(
    prefixes,
    vec![hash_prefix("Order-1"), hash_prefix("Order-12")],
    "別の aid は別のハッシュの接頭辞"
  );
}

// ---------------------------------------------------------------------------
// トークンの取得元（接続の所有と認証）
// ---------------------------------------------------------------------------

struct CountingSource {
  calls: AtomicUsize,
}

#[async_trait]
impl SpannerTokenSource for CountingSource {
  async fn access_token(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    self.calls.fetch_add(1, Ordering::SeqCst);
    Ok("TOKEN_VALUE".to_string())
  }
}

struct SwitchableSource {
  fail: AtomicBool,
}

#[async_trait]
impl SpannerTokenSource for SwitchableSource {
  async fn access_token(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    if self.fail.load(Ordering::SeqCst) {
      Err(Box::new(std::io::Error::other("TOKEN_SOURCE_FAILURE")))
    } else {
      Ok("TOKEN_VALUE".to_string())
    }
  }
}

/// 返すトークンを後から取り替えられる取得元。
struct ReplaceableSource {
  token: Mutex<String>,
}

impl ReplaceableSource {
  fn new(token: &str) -> Arc<Self> {
    Arc::new(Self {
      token: Mutex::new(token.to_string()),
    })
  }

  fn replace_with(&self, token: &str) {
    *self.token.lock().unwrap() = token.to_string();
  }
}

#[async_trait]
impl SpannerTokenSource for ReplaceableSource {
  async fn access_token(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    Ok(self.token.lock().unwrap().clone())
  }
}

#[tokio::test]
async fn should_spanner_ask_the_token_source_for_every_public_operation_and_succeed() {
  let fixture = Fixture::new().await;
  let source = Arc::new(CountingSource {
    calls: AtomicUsize::new(0),
  });
  let connection = fixture.connection().await.with_token_source(source.clone());
  let calls = || source.calls.load(Ordering::SeqCst);

  initialize_spanner_store(&connection, &fixture.tables, &SpannerOptions::default())
    .await
    .unwrap();
  let after_initialize = calls();
  let store = Store::open(connection, fixture.tables.clone(), SpannerOptions::default())
    .await
    .unwrap();
  let after_open = calls();
  let id = Id::order();
  store.persist_event(event(&id, 1)).await.unwrap();
  let after_append = calls();
  store.get_events_by_id_since_seq_nr(&id, 1).await.unwrap();
  let after_events = calls();
  store.get_latest_snapshot_by_id(&id).await.unwrap();
  let after_snapshot = calls();

  assert!(after_initialize > 0, "初期化の要求の前に取得元を呼ぶ");
  assert!(after_open > after_initialize, "生成の照合の前に取得元を呼ぶ");
  assert!(after_append > after_open, "追記の要求の前に取得元を呼ぶ");
  assert!(after_events > after_append, "イベントの読み取りの前に取得元を呼ぶ");
  assert!(
    after_snapshot > after_events,
    "スナップショットの読み取りの前に取得元を呼ぶ"
  );
}

#[tokio::test]
async fn should_spanner_failing_token_source_be_a_storage_error_and_send_no_request() {
  let fixture = Fixture::new().await;
  let failing = Arc::new(SwitchableSource {
    fail: AtomicBool::new(true),
  });
  let connection = fixture.connection().await.with_token_source(failing.clone());

  let initialized = initialize_spanner_store(&connection, &fixture.tables, &SpannerOptions::default()).await;
  let opened = Store::open(connection.clone(), fixture.tables.clone(), SpannerOptions::default()).await;

  assert!(
    matches!(initialized, Err(EventStoreError::Storage { .. })),
    "{initialized:?}"
  );
  assert!(matches!(opened, Err(EventStoreError::Storage { .. })), "{opened:?}");
  assert!(
    fixture.config_rows().await.is_empty(),
    "認証なしでも受け付けるエミュレーターに、要求は届いていない"
  );

  // 取得元が後から失敗する場合も、公開操作は要求を送らずに保存先エラーになる。
  failing.fail.store(false, Ordering::SeqCst);
  initialize_spanner_store(&connection, &fixture.tables, &SpannerOptions::default())
    .await
    .unwrap();
  let store = Store::open(connection, fixture.tables.clone(), SpannerOptions::default())
    .await
    .unwrap();
  let id = Id::order();
  failing.fail.store(true, Ordering::SeqCst);

  let appended = store.persist_event(event(&id, 1)).await;
  let events = store.get_events_by_id_since_seq_nr(&id, 1).await;
  let latest = store.get_latest_snapshot_by_id(&id).await;

  assert!(
    matches!(
      appended,
      Err(EventStoreError::Storage {
        operation: StorageOperation::Append,
        ..
      })
    ),
    "{appended:?}"
  );
  assert!(
    matches!(
      events,
      Err(EventStoreError::Storage {
        operation: StorageOperation::LoadEvents,
        ..
      })
    ),
    "{events:?}"
  );
  assert!(
    matches!(
      latest,
      Err(EventStoreError::Storage {
        operation: StorageOperation::LoadSnapshot,
        ..
      })
    ),
    "{latest:?}"
  );
  assert!(fixture.journal_rows().await.is_empty(), "追記の要求は届いていない");
}

#[tokio::test]
async fn should_spanner_token_that_cannot_be_sent_as_metadata_be_a_storage_error_and_send_no_request() {
  const VALID_TOKEN: &str = "TOKEN_VALUE";
  // 改行は印字できる ASCII の範囲の外なので、メタデータの値にできない。
  const INVALID_TOKEN: &str = "TOKEN_VALUE\n";
  let fixture = Fixture::new().await;
  let source = ReplaceableSource::new(INVALID_TOKEN);
  let connection = fixture.connection().await.with_token_source(source.clone());

  let initialized = initialize_spanner_store(&connection, &fixture.tables, &SpannerOptions::default()).await;
  let opened = Store::open(connection.clone(), fixture.tables.clone(), SpannerOptions::default()).await;

  assert!(
    matches!(initialized, Err(EventStoreError::Storage { .. })),
    "{initialized:?}"
  );
  assert!(
    matches!(opened, Err(EventStoreError::Storage { .. })),
    "未初期化の保存先でも、設定の照合の誤りではなく保存先エラーになる: {opened:?}"
  );
  assert!(
    fixture.config_rows().await.is_empty(),
    "認証なしでも受け付けるエミュレーターに、要求は届いていない"
  );

  // 取得元が後から使えないトークンを返す場合も、公開操作は要求を送らずに保存先エラーになる。
  source.replace_with(VALID_TOKEN);
  initialize_spanner_store(&connection, &fixture.tables, &SpannerOptions::default())
    .await
    .unwrap();
  let store = Store::open(connection, fixture.tables.clone(), SpannerOptions::default())
    .await
    .unwrap();
  let id = Id::order();
  source.replace_with(INVALID_TOKEN);

  let appended = store.persist_event(event(&id, 1)).await;

  assert!(
    matches!(
      appended,
      Err(EventStoreError::Storage {
        operation: StorageOperation::Append,
        ..
      })
    ),
    "{appended:?}"
  );
  assert!(fixture.journal_rows().await.is_empty(), "追記の要求は届いていない");
}

// ---------------------------------------------------------------------------
// 同時の追記（SPN-6・SPN-7・W-3）
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn should_spanner_concurrent_creations_of_the_same_aggregate_commit_exactly_one() {
  let options = SpannerOptions {
    transaction_retry_limit: 20,
    ..SpannerOptions::default()
  };
  let fixture = Fixture::initialized(&options).await;
  let id = Id::order();
  let mut handles = vec![];
  for writer in 0..3_u64 {
    let store = fixture.open(options.clone()).await.unwrap();
    let id = id.clone();
    handles.push(tokio::spawn(async move {
      store
        .persist_event(event_with_payload(&id, 1, json!({ "writer": writer })))
        .await
    }));
  }

  let results: Vec<_> = futures_join(handles).await;

  let successes = results.iter().filter(|result| result.is_ok()).count();
  let locks = results
    .iter()
    .filter(|result| matches!(result, Err(EventStoreError::OptimisticLock { seq_nr: 1, .. })))
    .count();
  assert_eq!(successes, 1, "同時の作成は 1 件だけ成功する: {results:?}");
  assert_eq!(locks, 2, "負けた書き手は楽観ロック: {results:?}");
  assert_eq!(fixture.journal_rows().await.len(), 1, "イベントは 1 件だけ残る");
}

async fn futures_join<T>(handles: Vec<tokio::task::JoinHandle<T>>) -> Vec<T> {
  let mut results = vec![];
  for handle in handles {
    results.push(handle.await.expect("書き手のタスクが終了する"));
  }
  results
}
