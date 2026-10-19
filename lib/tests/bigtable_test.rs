#![cfg(feature = "bigtable")]
//! Bigtable の保存先の結合試験（エミュレーター）。ライブラリの公開操作と、生の行の読み取りだけを使う。
//! 障害の差し込みを使う試験は `bigtable_hooks_test.rs` に置く。

mod bigtable_support;

use bigtable_support::{
  cells_in, event, event_with_payload, expected_envelope_of, expected_event_envelope, expected_snapshot_envelope,
  expected_snapshot_envelope_of, head_seq_nr_text, journal_key, only_value, options_keeping, row_prefix, snapshot, Env,
  Fixture, Id, Store, TableKind, INSTANCE, PROJECT,
};
use event_store_adapter_rs::{
  BigtableOptions, BigtableTables, ConfigurationReason, ContractRule, EventSerializer, EventStore, EventStoreError,
  JsonEventSerializer, JsonSnapshotSerializer, RetentionMode, RetentionSettings, SerializationPhase, SnapshotEnvelope,
  SnapshotSerializer,
};
use event_store_adapter_test_utils_rs::bigtable::{self, FamilySpec, GarbageCollection};
use serde_json::{json, Value};

fn ttl_options() -> BigtableOptions {
  BigtableOptions {
    retention: RetentionSettings::keep_latest(1).with_mode(RetentionMode::Ttl { grace_seconds: 60 }),
    ..BigtableOptions::default()
  }
}

fn change_feed_options() -> BigtableOptions {
  BigtableOptions {
    change_feed: true,
    ..BigtableOptions::default()
  }
}

fn limit_options(envelope_limit_bytes: u64) -> BigtableOptions {
  BigtableOptions {
    envelope_limit_bytes,
    ..BigtableOptions::default()
  }
}

async fn both_configurations(env: &Env) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
  (
    env.raw_configuration(TableKind::Journal).await,
    env.raw_configuration(TableKind::Head).await,
  )
}

/// 設定エラーの理由を取り出す。設定エラー以外なら `None`。
fn reason_of<T: std::fmt::Debug>(result: &Result<T, EventStoreError>) -> Option<&ConfigurationReason> {
  match result {
    Err(EventStoreError::Configuration { reason }) => Some(reason),
    _ => None,
  }
}

/// 初期化していない新しいテーブルに対して、`options` での初期化と生成が通信の前に設定エラーになり、
/// 何も書かないことを確かめる。
async fn assert_rejected_without_writing(options: BigtableOptions, expected: ConfigurationReason) {
  let fixture = Fixture::new().await;

  let initialized = fixture.initialize(&options).await;
  let opened = fixture.open(options).await.map(|_| ());

  assert_eq!(reason_of(&initialized), Some(&expected), "{initialized:?}");
  assert_eq!(reason_of(&opened), Some(&expected), "{opened:?}");
  assert_eq!(both_configurations(&fixture).await, (None, None), "設定行を作らない");
}

// ---------------------------------------------------------------------------
// 初期化の二重実行と、片方だけの設定行（BT-D1）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_bigtable_second_initialization_be_already_initialized_and_keep_the_recorded_configuration() {
  let fixture = Fixture::initialized(&BigtableOptions::default()).await;
  let before = both_configurations(&fixture).await;

  let again = fixture.initialize(&BigtableOptions::default()).await;
  let with_other_options = fixture.initialize(&options_keeping(1)).await;

  assert_eq!(
    reason_of(&again),
    Some(&ConfigurationReason::BigtableAlreadyInitialized),
    "{again:?}"
  );
  assert_eq!(
    reason_of(&with_other_options),
    Some(&ConfigurationReason::BigtableAlreadyInitialized),
    "設定が違っても上書きしない: {with_other_options:?}"
  );
  assert_eq!(
    both_configurations(&fixture).await,
    before,
    "store_id を含む設定行は変わらない"
  );
  assert!(fixture.open(BigtableOptions::default()).await.is_ok());
}

#[tokio::test]
async fn should_bigtable_configuration_in_only_one_table_be_invalid_and_never_be_completed() {
  let options = BigtableOptions::default();
  for missing in [TableKind::Head, TableKind::Journal] {
    let fixture = Fixture::initialized(&options).await;
    fixture.raw_delete_row(missing, &[0x00]).await;
    let before = both_configurations(&fixture).await;
    assert!(
      matches!(before, (Some(_), None) | (None, Some(_))),
      "試験の前提: 片方だけに設定行がある: {before:?}"
    );

    let opened = fixture.open(options.clone()).await.map(|_| ());
    let initialized = fixture.initialize(&options).await;

    assert_eq!(
      reason_of(&opened),
      Some(&ConfigurationReason::BigtableConfigurationInvalid),
      "{missing:?}: {opened:?}"
    );
    assert_eq!(
      reason_of(&initialized),
      Some(&ConfigurationReason::BigtableAlreadyInitialized),
      "{missing:?}: {initialized:?}"
    );
    assert_eq!(
      both_configurations(&fixture).await,
      before,
      "{missing:?}: 欠けた側を自動で補わない"
    );
  }
}

// ---------------------------------------------------------------------------
// 封筒の上限の設定（BT-16）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_bigtable_envelope_limit_open_with_the_smallest_and_the_largest_limit_and_record_it_in_both_tables() {
  for limit in [17, 8_388_608, 1_048_576] {
    let options = limit_options(limit);
    let fixture = Fixture::initialized(&options).await;

    let opened = fixture.open(options).await;

    assert!(opened.is_ok(), "{limit}: {:?}", opened.err());
    for table in [TableKind::Journal, TableKind::Head] {
      let json: Value =
        serde_json::from_slice(&fixture.raw_configuration(table).await.expect("設定行がある")).expect("設定行は JSON");
      assert_eq!(json["envelope_limit_bytes"], json!(limit), "{limit} {table:?}");
    }
  }
}

#[tokio::test]
async fn should_bigtable_envelope_limit_out_of_range_be_rejected_before_anything_is_written() {
  for limit in [16, 8_388_609] {
    assert_rejected_without_writing(
      limit_options(limit),
      ConfigurationReason::BigtableEnvelopeLimitOutOfRange,
    )
    .await;
  }
}

// ---------------------------------------------------------------------------
// 上限を超える入力（BT-16）
// ---------------------------------------------------------------------------

/// `event_with_payload(id, seq_nr, payload)` を追記したときの、直列化後のイベント封筒のバイト数。
/// ライブラリとは独立に、固定の形式から数える。
fn event_envelope_bytes(id: &Id, seq_nr: u64, payload: &Value) -> usize {
  let event = event_with_payload(id, seq_nr, payload.clone());
  expected_event_envelope(
    &id.aid(),
    seq_nr,
    event.occurred_at().timestamp_nanos_opt().expect("ナノ秒に収まる"),
    "",
    &serde_json::to_vec(payload).expect("payload を直列化できる"),
  )
  .len()
}

#[tokio::test]
async fn should_bigtable_event_envelope_of_exactly_the_limit_be_accepted_and_one_byte_more_be_rejected() {
  let id = Id::new("Order", "9");
  let payload = Value::String("y".repeat(300));
  let length = event_envelope_bytes(&id, 1, &payload) as u64;

  let at_limit = limit_options(length);
  let fixture = Fixture::initialized(&at_limit).await;
  let store = fixture.open(at_limit).await.unwrap();
  let accepted = store.persist_event(event_with_payload(&id, 1, payload.clone())).await;

  let below_limit = limit_options(length - 1);
  let fixture = Fixture::initialized(&below_limit).await;
  let store_below = fixture.open(below_limit).await.unwrap();
  let rejected = store_below
    .persist_event(event_with_payload(&id, 1, payload.clone()))
    .await;

  assert!(accepted.is_ok(), "ちょうど上限のバイト数は受け入れる: {accepted:?}");
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&id, 1).await.unwrap(),
    vec![event_with_payload(&id, 1, payload)]
  );
  assert!(
    matches!(
      rejected,
      Err(EventStoreError::ContractViolation {
        rule: ContractRule::EnvelopeSizeLimit,
        seq_nr: Some(1),
        ..
      })
    ),
    "上限より 1 バイト大きい封筒は BT-16 の契約違反: {rejected:?}"
  );
  assert!(store_below
    .get_events_by_id_since_seq_nr(&id, 1)
    .await
    .unwrap()
    .is_empty());
}

#[tokio::test]
async fn should_bigtable_snapshot_envelope_over_the_limit_be_rejected_before_anything_is_written() {
  let id = Id::new("Order", "9");
  let aggregate = Value::String("z".repeat(500));
  let snapshot_length = expected_snapshot_envelope(1, "", &serde_json::to_vec(&aggregate).unwrap()).len() as u64;
  let small_event = event(&id, 1);
  assert!(
    (expected_envelope_of(&id, 1).len() as u64) < snapshot_length,
    "試験の前提: イベントの封筒はスナップショットの封筒より小さい"
  );

  let at_limit = limit_options(snapshot_length);
  let fixture = Fixture::initialized(&at_limit).await;
  let store = fixture.open(at_limit).await.unwrap();
  let accepted = store
    .persist_event_and_snapshot(small_event.clone(), SnapshotEnvelope::new(aggregate.clone(), 1))
    .await;

  let below_limit = limit_options(snapshot_length - 1);
  let fixture = Fixture::initialized(&below_limit).await;
  let store_below = fixture.open(below_limit).await.unwrap();
  let rejected = store_below
    .persist_event_and_snapshot(small_event, SnapshotEnvelope::new(aggregate, 1))
    .await;

  assert!(
    accepted.is_ok(),
    "ちょうど上限のスナップショットは受け入れる: {accepted:?}"
  );
  assert!(
    matches!(
      rejected,
      Err(EventStoreError::ContractViolation {
        rule: ContractRule::EnvelopeSizeLimit,
        seq_nr: Some(1),
        snapshot_seq_nr: Some(1),
      })
    ),
    "スナップショットの封筒が上限を超えたら、スナップショットの seq_nr を伴う契約違反: {rejected:?}"
  );
  assert!(
    store_below
      .get_events_by_id_since_seq_nr(&id, 1)
      .await
      .unwrap()
      .is_empty(),
    "イベントも確定しない"
  );
  assert!(store_below.get_latest_snapshot_by_id(&id).await.unwrap().is_none());
  let prefix = row_prefix(&id.aid(), None);
  assert!(
    fixture.raw_row(TableKind::Head, &prefix).await.is_empty(),
    "head の行を書かない"
  );
}

#[tokio::test]
async fn should_bigtable_envelope_over_the_limit_be_a_bt_16_contract_violation_with_the_seq_nr() {
  let options = limit_options(1024);
  let fixture = Fixture::initialized(&options).await;
  let store = fixture.open(options).await.unwrap();
  let id = Id::new("Order", "9");
  store.persist_event(event(&id, 1)).await.unwrap();

  let result = store
    .persist_event(event_with_payload(&id, 2, Value::String("x".repeat(2048))))
    .await;

  assert!(
    matches!(
      result,
      Err(EventStoreError::ContractViolation {
        rule: ContractRule::EnvelopeSizeLimit,
        seq_nr: Some(2),
        ..
      })
    ),
    "{result:?}"
  );
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&id, 1).await.unwrap(),
    vec![event(&id, 1)],
    "何も確定しない"
  );
}

#[tokio::test]
async fn should_bigtable_envelope_over_the_limit_leave_the_head_and_the_journal_rows_unchanged() {
  let options = limit_options(1024);
  let fixture = Fixture::initialized(&options).await;
  let store = fixture.open(options).await.unwrap();
  let id = Id::new("Order", "9");
  store.persist_event(event(&id, 1)).await.unwrap();
  let prefix = row_prefix(&id.aid(), None);
  let head_before = fixture.raw_row(TableKind::Head, &prefix).await;
  let journal_before = fixture.raw_rows_with_prefix(TableKind::Journal, &prefix).await;

  let result = store
    .persist_event(event_with_payload(&id, 2, Value::String("x".repeat(2048))))
    .await;

  assert!(result.is_err());
  assert_eq!(
    fixture.raw_row(TableKind::Head, &prefix).await,
    head_before,
    "head は h のまま"
  );
  assert_eq!(
    fixture.raw_rows_with_prefix(TableKind::Journal, &prefix).await,
    journal_before,
    "journal に行を足さない"
  );
}

// ---------------------------------------------------------------------------
// 列ファミリの照合（BT-2）
// ---------------------------------------------------------------------------

async fn env_with_tables(base: &Fixture, label: &str, journal: &[FamilySpec], head: &[FamilySpec]) -> Env {
  let tables = BigtableTables {
    project_id: PROJECT.to_string(),
    instance_id: INSTANCE.to_string(),
    journal_table_id: format!("{}-{label}", base.tables.journal_table_id),
    head_table_id: format!("{}-{label}", base.tables.head_table_id),
  };
  let channel = bigtable::connect(base.port).await.unwrap();
  bigtable::create_table(&channel, PROJECT, INSTANCE, &tables.journal_table_id, journal)
    .await
    .unwrap();
  bigtable::create_table(&channel, PROJECT, INSTANCE, &tables.head_table_id, head)
    .await
    .unwrap();
  Env {
    port: base.port,
    tables,
  }
}

fn profile_journal() -> Vec<FamilySpec> {
  vec![
    FamilySpec::current("event"),
    FamilySpec::current("history"),
    FamilySpec::current("config"),
  ]
}

fn profile_head() -> Vec<FamilySpec> {
  vec![
    FamilySpec::current("head"),
    FamilySpec::current("snapshot"),
    FamilySpec::current("config"),
  ]
}

#[tokio::test]
async fn should_bigtable_open_accept_tables_with_the_profile_column_families() {
  let base = Fixture::new().await;
  let env = env_with_tables(&base, "profile", &profile_journal(), &profile_head()).await;
  env.initialize(&BigtableOptions::default()).await.unwrap();

  let opened = env.open(BigtableOptions::default()).await;

  assert!(opened.is_ok(), "{:?}", opened.err());
}

#[tokio::test]
async fn should_bigtable_open_reject_tables_whose_column_families_differ_from_the_profile() {
  let base = Fixture::new().await;
  let aged = |name: &'static str| FamilySpec {
    name,
    garbage_collection: GarbageCollection::MaxAgeSeconds(3600),
  };
  let no_gc = |name: &'static str| FamilySpec {
    name,
    garbage_collection: GarbageCollection::None,
  };
  let variants: Vec<(&str, Vec<FamilySpec>, Vec<FamilySpec>)> = vec![
    (
      "missing-history",
      vec![FamilySpec::current("event"), FamilySpec::current("config")],
      profile_head(),
    ),
    (
      "age-gc",
      vec![
        aged("event"),
        FamilySpec::current("history"),
        FamilySpec::current("config"),
      ],
      profile_head(),
    ),
    (
      "no-version-gc",
      profile_journal(),
      vec![
        no_gc("head"),
        FamilySpec::current("snapshot"),
        FamilySpec::current("config"),
      ],
    ),
  ];

  for (label, journal, head) in variants {
    let env = env_with_tables(&base, label, &journal, &head).await;
    env.initialize(&BigtableOptions::default()).await.unwrap();

    let opened = env.open(BigtableOptions::default()).await.map(|_| ());

    assert_eq!(
      reason_of(&opened),
      Some(&ConfigurationReason::BigtableColumnFamilyMismatch),
      "{label}: {opened:?}"
    );
  }
}

// ---------------------------------------------------------------------------
// 生成は照合だけ（BT-D1）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_bigtable_open_before_initialization_be_a_configuration_error() {
  let fixture = Fixture::new().await;

  let opened = fixture.open(BigtableOptions::default()).await.map(|_| ());

  assert!(
    matches!(opened, Err(EventStoreError::Configuration { .. })),
    "{opened:?}"
  );
}

#[tokio::test]
async fn should_bigtable_open_never_create_configuration_rows_or_tables() {
  let fixture = Fixture::new().await;
  let missing = Env {
    port: fixture.port,
    tables: BigtableTables {
      project_id: PROJECT.to_string(),
      instance_id: INSTANCE.to_string(),
      journal_table_id: format!("{}-missing", fixture.tables.journal_table_id),
      head_table_id: format!("{}-missing", fixture.tables.head_table_id),
    },
  };

  let without_configuration = fixture.open(BigtableOptions::default()).await.map(|_| ());
  let without_tables = missing.open(BigtableOptions::default()).await.map(|_| ());

  assert!(without_configuration.is_err());
  assert!(without_tables.is_err());
  assert_eq!(both_configurations(&fixture).await, (None, None), "設定行を補わない");
  let channel = bigtable::connect(fixture.port).await.unwrap();
  for table_id in [&missing.tables.journal_table_id, &missing.tables.head_table_id] {
    let created = bigtable::create_table(&channel, PROJECT, INSTANCE, table_id, &profile_journal()).await;
    assert!(created.is_ok(), "生成がテーブルを作っていない: {table_id}: {created:?}");
  }
}

// ---------------------------------------------------------------------------
// 期限切れ方式・変更フィード（BT-13・BT-17）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_bigtable_ttl_request_be_rejected_on_initialize_and_open_without_writing() {
  assert_rejected_without_writing(ttl_options(), ConfigurationReason::BigtableTtlUnsupported).await;

  let fixture = Fixture::initialized(&BigtableOptions::default()).await;
  let recorded = both_configurations(&fixture).await;
  let opened = fixture.open(ttl_options()).await.map(|_| ());
  assert_eq!(
    reason_of(&opened),
    Some(&ConfigurationReason::BigtableTtlUnsupported),
    "{opened:?}"
  );
  assert_eq!(
    both_configurations(&fixture).await,
    recorded,
    "初期化済みの設定行を書き換えない"
  );
}

#[tokio::test]
async fn should_bigtable_change_feed_request_be_rejected_on_initialize_and_open_without_writing() {
  assert_rejected_without_writing(
    change_feed_options(),
    ConfigurationReason::BigtableChangeFeedUnsupported,
  )
  .await;

  let fixture = Fixture::initialized(&BigtableOptions::default()).await;
  let recorded = both_configurations(&fixture).await;
  let opened = fixture.open(change_feed_options()).await.map(|_| ());
  assert_eq!(
    reason_of(&opened),
    Some(&ConfigurationReason::BigtableChangeFeedUnsupported),
    "{opened:?}"
  );
  assert_eq!(
    both_configurations(&fixture).await,
    recorded,
    "初期化済みの設定行を書き換えない"
  );
}

// ---------------------------------------------------------------------------
// セルの形（4 章・BT-6）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_bigtable_raw_cells_follow_the_profile_layout_with_zero_journal_timestamps() {
  let options = options_keeping(2);
  let fixture = Fixture::initialized(&options).await;
  let store = fixture.open(options).await.unwrap();
  let id = Id::order();
  store
    .persist_event_and_snapshot(event(&id, 1), snapshot(1))
    .await
    .unwrap();
  store.persist_event(event(&id, 2)).await.unwrap();
  store
    .persist_event_and_snapshot(event(&id, 3), snapshot(3))
    .await
    .unwrap();
  let prefix = row_prefix(&id.aid(), None);

  let head = fixture.raw_row(TableKind::Head, &prefix).await;
  assert_eq!(only_value(&head, "head", "seq_nr"), Some(head_seq_nr_text(3)));
  assert_eq!(
    only_value(&head, "head", "last_event"),
    Some(expected_envelope_of(&id, 3))
  );
  assert_eq!(only_value(&head, "head", "has_snapshot"), Some(b"1".to_vec()));
  assert_eq!(
    only_value(&head, "snapshot", "current"),
    Some(expected_snapshot_envelope_of(3))
  );

  let row_1 = fixture.raw_row(TableKind::Journal, &journal_key(&prefix, 1)).await;
  assert_eq!(
    only_value(&row_1, "event", "envelope"),
    Some(expected_envelope_of(&id, 1))
  );
  assert_eq!(only_value(&row_1, "event", "has_snapshot"), Some(b"1".to_vec()));
  assert_eq!(only_value(&row_1, "history", "state"), Some(b"active".to_vec()));
  assert_eq!(
    only_value(&row_1, "history", "envelope"),
    Some(expected_snapshot_envelope_of(1))
  );
  let row_2 = fixture.raw_row(TableKind::Journal, &journal_key(&prefix, 2)).await;
  assert_eq!(
    only_value(&row_2, "event", "envelope"),
    Some(expected_envelope_of(&id, 2))
  );
  assert_eq!(only_value(&row_2, "event", "has_snapshot"), Some(b"0".to_vec()));
  assert!(
    cells_in(&row_2, "history", "state").is_empty(),
    "イベントだけの行に履歴のセルはない"
  );
  let row_3 = fixture.raw_row(TableKind::Journal, &journal_key(&prefix, 3)).await;
  assert_eq!(
    only_value(&row_3, "event", "envelope"),
    Some(expected_envelope_of(&id, 3))
  );
  assert_eq!(only_value(&row_3, "history", "state"), Some(b"active".to_vec()));
  for row in [&row_1, &row_2, &row_3] {
    assert!(
      row.iter().all(|cell| cell.timestamp_micros == 0),
      "journal の初回作成のセル時刻は 0µs: {row:?}"
    );
  }
}

#[tokio::test]
async fn should_bigtable_without_a_keep_count_create_no_history_cells() {
  let fixture = Fixture::initialized(&BigtableOptions::default()).await;
  let store = fixture.open(BigtableOptions::default()).await.unwrap();
  let id = Id::order();
  store
    .persist_event_and_snapshot(event(&id, 1), snapshot(1))
    .await
    .unwrap();
  store
    .persist_event_and_snapshot(event(&id, 2), snapshot(2))
    .await
    .unwrap();
  let prefix = row_prefix(&id.aid(), None);

  let rows = fixture.raw_rows_with_prefix(TableKind::Journal, &prefix).await;

  assert!(!rows.is_empty(), "journal の行が書かれている");
  for (key, row) in rows {
    assert!(
      row.iter().all(|cell| cell.family != "history"),
      "保持件数がなければ history のセルを作らない: {key:?}"
    );
  }
}

#[tokio::test]
async fn should_bigtable_updates_leave_exactly_one_cell_per_column() {
  let options = options_keeping(1);
  let fixture = Fixture::initialized(&options).await;
  let store = fixture.open(options).await.unwrap();
  let id = Id::order();
  for seq in 1..=4 {
    store
      .persist_event_and_snapshot(event(&id, seq), snapshot(seq))
      .await
      .unwrap();
  }
  let prefix = row_prefix(&id.aid(), None);

  let mut rows = vec![fixture.raw_row(TableKind::Head, &prefix).await];
  rows.extend(
    fixture
      .raw_rows_with_prefix(TableKind::Journal, &prefix)
      .await
      .into_iter()
      .map(|(_, row)| row),
  );

  for row in rows {
    let mut columns: Vec<(String, Vec<u8>)> = row
      .iter()
      .map(|cell| (cell.family.clone(), cell.qualifier.clone()))
      .collect();
    let total = columns.len();
    columns.sort();
    columns.dedup();
    assert_eq!(columns.len(), total, "古い版が残っていない: {row:?}");
  }
}

// ---------------------------------------------------------------------------
// 楽観ロック（H-2）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_bigtable_append_after_another_writer_committed_be_an_optimistic_lock() {
  let fixture = Fixture::initialized(&BigtableOptions::default()).await;
  let first = fixture.open(BigtableOptions::default()).await.unwrap();
  let second = fixture.open(BigtableOptions::default()).await.unwrap();
  let id = Id::order();
  first.persist_event(event(&id, 1)).await.unwrap();

  let result = second.persist_event(event(&id, 1)).await;

  assert!(
    matches!(
      &result,
      Err(EventStoreError::OptimisticLock { aid, seq_nr: 1, head_seq_nr: Some(1) }) if aid == "Order-1"
    ),
    "契約違反や保存先エラーにしない: {result:?}"
  );
  assert_eq!(
    second.get_events_by_id_since_seq_nr(&id, 1).await.unwrap(),
    vec![event(&id, 1)]
  );
}

// ---------------------------------------------------------------------------
// 最新スナップショットの読み取り（BT-8）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_bigtable_latest_snapshot_return_the_head_and_the_current_snapshot() {
  let fixture = Fixture::initialized(&BigtableOptions::default()).await;
  let store = fixture.open(BigtableOptions::default()).await.unwrap();
  let id = Id::order();
  assert!(
    store.get_latest_snapshot_by_id(&id).await.unwrap().is_none(),
    "ヘッドがなければ None"
  );

  store.persist_event(event(&id, 1)).await.unwrap();
  let events_only = store.get_latest_snapshot_by_id(&id).await.unwrap().unwrap();
  assert_eq!(events_only.head_seq_nr(), 1);
  assert!(
    events_only.snapshot().is_none(),
    "snapshot がなくても h と「なし」を返す"
  );

  store
    .persist_event_and_snapshot(event(&id, 2), snapshot(2))
    .await
    .unwrap();
  store.persist_event(event(&id, 3)).await.unwrap();
  let after_events_only = store.get_latest_snapshot_by_id(&id).await.unwrap().unwrap();
  assert_eq!(after_events_only.head_seq_nr(), 3, "ヘッドはイベントだけの追記で進む");
  let current = after_events_only
    .snapshot()
    .expect("現在のスナップショットは保存したまま");
  assert_eq!(current.seq_nr(), 2);
  assert_eq!(current.aggregate(), &json!({ "state": 2 }));
}

// ---------------------------------------------------------------------------
// 破損した head の行（BT-7 手順 1・BT-8）
// ---------------------------------------------------------------------------

/// 必要なセルが欠けた、または互いに矛盾する head の行を `cells` で作り、3 つの公開操作が保存先エラーになり、
/// head も journal も変わらないことを確かめる。`next_seq_nr` は、行が正しければ次に追記できる seq_nr。
async fn assert_corrupted_head_is_a_storage_error(cells: &[(&str, &str, Vec<u8>)], next_seq_nr: u64) {
  let fixture = Fixture::initialized(&BigtableOptions::default()).await;
  let store = fixture.open(BigtableOptions::default()).await.unwrap();
  let id = Id::new("Order", "7");
  let prefix = row_prefix(&id.aid(), None);
  for (family, qualifier, value) in cells {
    fixture
      .raw_put(TableKind::Head, &prefix, family, qualifier, 0, value)
      .await;
  }
  let before = fixture.raw_row(TableKind::Head, &prefix).await;

  let appended = store.persist_event(event(&id, next_seq_nr)).await;
  let events = store.get_events_by_id_since_seq_nr(&id, 1).await.map(|_| ());
  let latest = store.get_latest_snapshot_by_id(&id).await.map(|_| ());

  for (name, result) in [
    ("追記", appended),
    ("イベント取得", events),
    ("最新スナップショット取得", latest),
  ] {
    assert!(
      matches!(result, Err(EventStoreError::Storage { .. })),
      "{name}: 破損した head を新規の集約や正常な行として扱わない: {result:?}"
    );
  }
  assert_eq!(
    fixture.raw_row(TableKind::Head, &prefix).await,
    before,
    "破損した行を上書きしない"
  );
  assert!(
    fixture
      .raw_rows_with_prefix(TableKind::Journal, &prefix)
      .await
      .is_empty(),
    "journal の行も作らない"
  );
}

#[tokio::test]
async fn should_bigtable_head_row_without_a_seq_nr_be_a_storage_error_and_never_a_new_aggregate() {
  let id = Id::new("Order", "7");

  assert_corrupted_head_is_a_storage_error(
    &[
      ("head", "last_event", expected_envelope_of(&id, 1)),
      ("head", "has_snapshot", b"0".to_vec()),
    ],
    1,
  )
  .await;
}

#[tokio::test]
async fn should_bigtable_head_row_without_the_last_event_be_a_storage_error() {
  assert_corrupted_head_is_a_storage_error(
    &[
      ("head", "seq_nr", head_seq_nr_text(1)),
      ("head", "has_snapshot", b"0".to_vec()),
    ],
    2,
  )
  .await;
}

#[tokio::test]
async fn should_bigtable_head_row_without_the_snapshot_flag_be_a_storage_error() {
  let id = Id::new("Order", "7");

  assert_corrupted_head_is_a_storage_error(
    &[
      ("head", "seq_nr", head_seq_nr_text(1)),
      ("head", "last_event", expected_envelope_of(&id, 1)),
    ],
    2,
  )
  .await;
}

#[tokio::test]
async fn should_bigtable_head_row_whose_last_event_does_not_match_the_seq_nr_be_a_storage_error() {
  let id = Id::new("Order", "7");

  assert_corrupted_head_is_a_storage_error(
    &[
      ("head", "seq_nr", head_seq_nr_text(2)),
      ("head", "last_event", expected_envelope_of(&id, 1)),
      ("head", "has_snapshot", b"0".to_vec()),
    ],
    3,
  )
  .await;
}

#[tokio::test]
async fn should_bigtable_head_row_whose_last_event_belongs_to_another_aid_be_a_storage_error() {
  let other = Id::new("Order", "8");

  assert_corrupted_head_is_a_storage_error(
    &[
      ("head", "seq_nr", head_seq_nr_text(1)),
      ("head", "last_event", expected_envelope_of(&other, 1)),
      ("head", "has_snapshot", b"0".to_vec()),
    ],
    2,
  )
  .await;
}

#[tokio::test]
async fn should_bigtable_head_row_whose_current_snapshot_is_ahead_of_the_head_be_a_storage_error() {
  let id = Id::new("Order", "7");

  assert_corrupted_head_is_a_storage_error(
    &[
      ("head", "seq_nr", head_seq_nr_text(1)),
      ("head", "last_event", expected_envelope_of(&id, 1)),
      ("head", "has_snapshot", b"0".to_vec()),
      ("snapshot", "current", expected_snapshot_envelope_of(2)),
    ],
    2,
  )
  .await;
}

#[tokio::test]
async fn should_bigtable_head_row_that_claims_a_snapshot_it_does_not_hold_be_a_storage_error() {
  let id = Id::new("Order", "7");

  assert_corrupted_head_is_a_storage_error(
    &[
      ("head", "seq_nr", head_seq_nr_text(1)),
      ("head", "last_event", expected_envelope_of(&id, 1)),
      ("head", "has_snapshot", b"1".to_vec()),
    ],
    2,
  )
  .await;
}

// ---------------------------------------------------------------------------
// 読み取りの欠番（BT-10）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_bigtable_read_events_report_a_storage_error_for_a_missing_journal_row() {
  let fixture = Fixture::initialized(&BigtableOptions::default()).await;
  let store = fixture.open(BigtableOptions::default()).await.unwrap();
  let id = Id::order();
  for seq in 1..=4 {
    store.persist_event(event(&id, seq)).await.unwrap();
  }
  let prefix = row_prefix(&id.aid(), None);
  fixture
    .raw_delete_row(TableKind::Journal, &journal_key(&prefix, 2))
    .await;

  let result = store.get_events_by_id_since_seq_nr(&id, 1).await;

  assert!(
    matches!(result, Err(EventStoreError::Storage { .. })),
    "欠番のある短い結果を成功にしない: {result:?}"
  );
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&id, 3).await.unwrap(),
    vec![event(&id, 3), event(&id, 4)],
    "欠番を含まない範囲は読める"
  );
}

// ---------------------------------------------------------------------------
// 履歴の整理（BT-11・BT-12）
// ---------------------------------------------------------------------------

async fn append_with_snapshots(store: &Store, id: &Id, range: std::ops::RangeInclusive<u64>) {
  for seq in range {
    store
      .persist_event_and_snapshot(event(id, seq), snapshot(seq))
      .await
      .unwrap();
  }
}

#[tokio::test]
async fn should_bigtable_retention_retire_old_history_and_keep_the_event_cells() {
  let options = options_keeping(1);
  let fixture = Fixture::initialized(&options).await;
  let store = fixture.open(options).await.unwrap();
  let id = Id::order();

  append_with_snapshots(&store, &id, 1..=4).await;

  let prefix = row_prefix(&id.aid(), None);
  for seq in 1..=3 {
    let row = fixture.raw_row(TableKind::Journal, &journal_key(&prefix, seq)).await;
    assert_eq!(
      only_value(&row, "history", "state"),
      Some(b"retired".to_vec()),
      "行 {seq}"
    );
    assert!(
      cells_in(&row, "history", "envelope").is_empty(),
      "行 {seq}: 履歴の封筒は消える"
    );
    assert_eq!(
      only_value(&row, "event", "envelope"),
      Some(expected_envelope_of(&id, seq)),
      "行 {seq}: event のセルは残す"
    );
  }
  let row_4 = fixture.raw_row(TableKind::Journal, &journal_key(&prefix, 4)).await;
  assert_eq!(only_value(&row_4, "history", "state"), Some(b"active".to_vec()));
  assert_eq!(
    only_value(&row_4, "history", "envelope"),
    Some(expected_snapshot_envelope_of(4))
  );
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&id, 1).await.unwrap(),
    (1..=4).map(|seq| event(&id, seq)).collect::<Vec<_>>(),
    "イベントは全て読める"
  );
}

#[tokio::test]
async fn should_bigtable_retention_never_rewrite_a_row_that_is_already_retired() {
  let options = options_keeping(1);
  let fixture = Fixture::initialized(&options).await;
  let store = fixture.open(options).await.unwrap();
  let id = Id::order();
  append_with_snapshots(&store, &id, 1..=4).await;
  let prefix = row_prefix(&id.aid(), None);
  let mut retired_before = vec![];
  for seq in 1..=3 {
    retired_before.push(fixture.raw_row(TableKind::Journal, &journal_key(&prefix, seq)).await);
  }

  append_with_snapshots(&store, &id, 5..=5).await;

  for (index, before) in retired_before.iter().enumerate() {
    let seq = index as u64 + 1;
    assert_eq!(
      &fixture.raw_row(TableKind::Journal, &journal_key(&prefix, seq)).await,
      before,
      "retired の行 {seq} は変わらない"
    );
  }
  let row_4 = fixture.raw_row(TableKind::Journal, &journal_key(&prefix, 4)).await;
  assert_eq!(
    only_value(&row_4, "history", "state"),
    Some(b"retired".to_vec()),
    "行 4 は新しいスナップショットで retired になる"
  );
}

// ---------------------------------------------------------------------------
// 公開 API の形（共通のストアの操作）
// ---------------------------------------------------------------------------

async fn exercise_through_the_common_event_store<S: EventStore<AID = Id, A = Value, P = Value>>(store: &S) {
  let id = Id::order();
  store.persist_event(event(&id, 1)).await.unwrap();
  store
    .persist_event_and_snapshot(event(&id, 2), snapshot(2))
    .await
    .unwrap();
  let latest = store.get_latest_snapshot_by_id(&id).await.unwrap().unwrap();
  assert_eq!(latest.head_seq_nr(), 2);
  assert_eq!(
    store.get_events_by_id_since_seq_nr(&id, 1).await.unwrap(),
    vec![event(&id, 1), event(&id, 2)]
  );
}

#[tokio::test]
async fn should_bigtable_store_be_usable_through_the_common_event_store_operations() {
  let fixture = Fixture::initialized(&BigtableOptions::default()).await;
  let store: Store = fixture.open(BigtableOptions::default()).await.unwrap();

  exercise_through_the_common_event_store(&store).await;

  let cloned = store.clone();
  assert_eq!(
    cloned
      .get_events_by_id_since_seq_nr(&Id::order(), 1)
      .await
      .unwrap()
      .len(),
    2,
    "複製したストアは同じ保存先を読む"
  );
}

// ---------------------------------------------------------------------------
// 正しい封筒の payload の復元の失敗は、直列化エラー（4 章）
// ---------------------------------------------------------------------------

/// 保存は JSON で行い、payload の復元だけが必ず失敗する直列化。
#[derive(Debug)]
struct UndecodablePayloads;

impl EventSerializer<Value> for UndecodablePayloads {
  fn serialize(&self, payload: &Value) -> Result<Vec<u8>, EventStoreError> {
    JsonEventSerializer::<Value>::new().serialize(payload)
  }

  fn deserialize(&self, _: &[u8]) -> Result<Value, EventStoreError> {
    Err(EventStoreError::Serialization {
      phase: SerializationPhase::DeserializeEvent,
      source: Box::new(std::io::Error::other("UNDECODABLE")),
    })
  }
}

impl SnapshotSerializer<Value> for UndecodablePayloads {
  fn serialize(&self, aggregate: &Value) -> Result<Vec<u8>, EventStoreError> {
    JsonSnapshotSerializer::<Value>::new().serialize(aggregate)
  }

  fn deserialize(&self, _: &[u8]) -> Result<Value, EventStoreError> {
    Err(EventStoreError::Serialization {
      phase: SerializationPhase::DeserializeSnapshot,
      source: Box::new(std::io::Error::other("UNDECODABLE")),
    })
  }
}

async fn open_with_undecodable_payloads(fixture: &Fixture) -> Store {
  Store::open_with_serializers(
    fixture.connection().await,
    fixture.tables.clone(),
    BigtableOptions::default(),
    std::sync::Arc::new(UndecodablePayloads),
    std::sync::Arc::new(UndecodablePayloads),
  )
  .await
  .expect("直列化を差し替えたストアを開ける")
}

#[tokio::test]
async fn should_bigtable_event_payload_that_cannot_be_decoded_be_a_serialization_error_not_a_storage_error() {
  let fixture = Fixture::initialized(&BigtableOptions::default()).await;
  let store = open_with_undecodable_payloads(&fixture).await;
  let id = Id::order();
  store.persist_event(event(&id, 1)).await.unwrap();
  store.persist_event(event(&id, 2)).await.unwrap();

  let from_the_journal = store.get_events_by_id_since_seq_nr(&id, 1).await;
  let from_the_head = store.get_events_by_id_since_seq_nr(&id, 2).await;

  for result in [from_the_journal, from_the_head] {
    assert!(
      matches!(
        result,
        Err(EventStoreError::Serialization {
          phase: SerializationPhase::DeserializeEvent,
          ..
        })
      ),
      "封筒は正しいので保存先エラーにしない: {:?}",
      result.err()
    );
  }
}

#[tokio::test]
async fn should_bigtable_snapshot_payload_that_cannot_be_decoded_be_a_serialization_error_not_a_storage_error() {
  let fixture = Fixture::initialized(&BigtableOptions::default()).await;
  let store = open_with_undecodable_payloads(&fixture).await;
  let id = Id::order();
  store
    .persist_event_and_snapshot(event(&id, 1), snapshot(1))
    .await
    .unwrap();

  let result = store.get_latest_snapshot_by_id(&id).await;

  assert!(
    matches!(
      result,
      Err(EventStoreError::Serialization {
        phase: SerializationPhase::DeserializeSnapshot,
        ..
      })
    ),
    "封筒は正しいので保存先エラーにしない: {:?}",
    result.err()
  );
}
