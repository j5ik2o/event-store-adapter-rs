#![cfg(feature = "bigtable-migration")]
//! v3 の Bigtable の 2 テーブルから v5 の Bigtable の 2 テーブルへの移行の結合試験（エミュレーター）。
//!
//! v3 の行は試験の中で v3.0.6 の行キーとセルの形で直接書き（`bigtable_migration_test/support.rs`）、
//! 移行した結果は v5 の公開 API と生の行で確かめる。

mod bigtable_support;
#[path = "bigtable_migration_test/support.rs"]
mod support;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bigtable_support::{
  cells_in, expected_event_envelope, expected_snapshot_envelope, head_seq_nr_text, journal_key, occurred_at,
  only_value, options_keeping, row_prefix, Env, Fixture, Id, RawRow, TableKind, INSTANCE, PROJECT,
};
use event_store_adapter_rs::{
  migrate_v3_bigtable, BigtableOptions, BigtableTables, BigtableTokenSource, ConfigurationReason, EventEnvelope,
  EventStore, EventStoreError, LegacyBigtableTables, MigrationError, MigrationReport, SnapshotEnvelope,
};
use event_store_adapter_test_utils_rs::bigtable::{self, FamilySpec, GarbageCollection};
use support::*;

/// 旧の 2 テーブルを `legacy` として、`scene.options` で移行する。
async fn migrate_with(
  scene: &Scene,
  legacy: &LegacyBigtableTables,
  mapping: &HashMap<String, String>,
) -> MigrationReport {
  migrate_v3_bigtable(
    &scene.env.connection().await,
    None,
    legacy,
    &scene.env.tables,
    &scene.options,
    mapping,
  )
  .await
  .expect("移行の関数は失敗せずに報告を返す")
}

async fn migrate(scene: &Scene, mapping: &HashMap<String, String>) -> MigrationReport {
  migrate_with(scene, &scene.legacy, mapping).await
}

async fn migrate_failing(scene: &Scene) -> MigrationError {
  migrate_v3_bigtable(
    &scene.env.connection().await,
    None,
    &scene.legacy,
    &scene.env.tables,
    &scene.options,
    &HashMap::new(),
  )
  .await
  .expect_err("設定の食い違いは MigrationError で返る")
}

/// `MigrationError` の元になった設定エラーの理由。設定エラーでなければ `None`。
fn configuration_reason(error: &MigrationError) -> Option<&ConfigurationReason> {
  match error.source.downcast_ref::<EventStoreError>() {
    Some(EventStoreError::Configuration { reason }) => Some(reason),
    _ => None,
  }
}

fn options_with(keep: Option<usize>, envelope_limit_bytes: u64) -> BigtableOptions {
  BigtableOptions {
    envelope_limit_bytes,
    ..keep.map_or_else(BigtableOptions::default, options_keeping)
  }
}

async fn journal_row(scene: &Scene, aid: &str, seq_nr: u64) -> RawRow {
  scene
    .env
    .raw_row(TableKind::Journal, &journal_key(&row_prefix(aid, None), seq_nr))
    .await
}

async fn head_row(scene: &Scene, aid: &str) -> RawRow {
  scene.env.raw_row(TableKind::Head, &row_prefix(aid, None)).await
}

// ---------------------------------------------------------------------------
// 成功: 移したものを v5 の公開 API と生の行で確かめる
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_migrate_events_snapshots_and_newest_history_and_read_them_publicly() {
  let fixture = success_fixture();
  let scene = Scene::new(&fixture.events, &fixture.snapshots, options_keeping(2)).await;
  let before = scene.state().await;

  let report = migrate(&scene, &fixture.mapping).await;

  assert!(report.reasons.is_empty(), "{report:?}");
  assert_eq!((report.aggregates, report.events, report.snapshots), (3, 7, 3));
  assert_success_fixture_readable(&scene).await;
  assert_old_tables_unchanged(&before, &scene.state().await);
}

/// 保持件数 2 の移行後の journal と head の行。
/// 位置の集合は、履歴の行と今を合わせたもの（A は {1, 2, 3}）で、新しい 2 件（{2, 3}）の journal の行だけが active な履歴を持つ。
#[tokio::test]
async fn should_migrate_events_snapshots_and_newest_history_and_read_them_publicly_with_the_v5_row_layout() {
  let fixture = success_fixture();
  let scene = Scene::new(&fixture.events, &fixture.snapshots, options_keeping(2)).await;

  let report = migrate(&scene, &fixture.mapping).await;

  assert!(report.reasons.is_empty(), "{report:?}");
  let aggregates = success_aggregates();
  // (aid, 各イベントの (has_snapshot, active な履歴の封筒), head の (has_snapshot, 今のスナップショットの封筒))
  type Layout = (
    &'static str,
    Vec<(&'static str, Option<Vec<u8>>)>,
    (&'static str, Option<Vec<u8>>),
  );
  let layouts: Vec<Layout> = vec![
    (
      "Account-a-b",
      vec![
        ("1", None),
        ("1", Some(expected_snapshot_envelope(2, "", &[255, 22]))),
        ("1", Some(expected_snapshot_envelope(3, "", &[255, 33]))),
        ("0", None),
      ],
      ("0", Some(expected_snapshot_envelope(3, "", &[255, 33]))),
    ),
    (
      "Other-second",
      vec![("1", Some(expected_snapshot_envelope(1, "", &[128, 255])))],
      ("1", Some(expected_snapshot_envelope(1, "", &[128, 255]))),
    ),
    ("Plain-third", vec![("0", None), ("0", None)], ("0", None)),
  ];
  for (aggregate, (aid, rows, (head_flag, head_snapshot))) in aggregates.iter().zip(layouts) {
    for (event, (has_snapshot, history)) in aggregate.events.iter().zip(rows) {
      let row = journal_row(&scene, aid, event.seq_nr).await;
      let context = format!("{aid} の journal の行 {}", event.seq_nr);
      assert_eq!(
        only_value(&row, "event", "envelope"),
        Some(expected_event_envelope(
          aid,
          event.seq_nr,
          event.occurred_at_ns,
          event.manifest,
          &event.payload
        )),
        "{context}"
      );
      assert_eq!(
        only_value(&row, "event", "has_snapshot"),
        Some(has_snapshot.as_bytes().to_vec()),
        "{context}"
      );
      match history {
        Some(envelope) => {
          assert_eq!(
            only_value(&row, "history", "state"),
            Some(b"active".to_vec()),
            "{context}"
          );
          assert_eq!(only_value(&row, "history", "envelope"), Some(envelope), "{context}");
        }
        None => assert!(
          row.iter().all(|cell| cell.family != "history"),
          "{context}: 履歴を移さない位置に history のセルを作らない"
        ),
      }
      assert!(
        row.iter().all(|cell| cell.timestamp_micros == 0),
        "{context}: journal の初回作成のセル時刻は 0µs: {row:?}"
      );
    }
    let last = aggregate.events.last().expect("イベントがある");
    let head = head_row(&scene, aid).await;
    assert_eq!(
      only_value(&head, "head", "seq_nr"),
      Some(head_seq_nr_text(last.seq_nr)),
      "{aid} の head は最大の seq_nr"
    );
    assert_eq!(
      only_value(&head, "head", "last_event"),
      Some(expected_event_envelope(
        aid,
        last.seq_nr,
        last.occurred_at_ns,
        last.manifest,
        &last.payload
      )),
      "{aid} の head は最大の seq_nr のイベントの封筒"
    );
    assert_eq!(
      only_value(&head, "head", "has_snapshot"),
      Some(head_flag.as_bytes().to_vec()),
      "{aid}"
    );
    match head_snapshot {
      Some(envelope) => assert_eq!(only_value(&head, "snapshot", "current"), Some(envelope), "{aid}"),
      None => assert!(cells_in(&head, "snapshot", "current").is_empty(), "{aid}"),
    }
  }
}

/// 移した後に v5 の公開 API で追記する。履歴の数え方は v5 のもの（今のスナップショットも件数に数える）。
#[tokio::test]
async fn should_migrate_events_snapshots_and_newest_history_and_read_them_publicly_then_append_by_the_v5_retention_count(
) {
  let fixture = success_fixture();
  let scene = Scene::new(&fixture.events, &fixture.snapshots, options_keeping(2)).await;
  let report = migrate(&scene, &fixture.mapping).await;
  assert!(report.reasons.is_empty(), "{report:?}");
  let store = open_bytes_store(&scene).await;
  let account = Id::new("Account", "a-b");

  store
    .persist_event_and_snapshot(
      EventEnvelope::new(account.clone(), 5, occurred_at(5), vec![5]),
      SnapshotEnvelope::new(vec![255, 55], 5),
    )
    .await
    .expect("移行したデータへ、スナップショット付きで続けて追記できる");
  // 履歴が空でない B（head に今のスナップショットがある）と、スナップショットのない C にも、続けて追記できる。
  store
    .persist_event(EventEnvelope::new(
      Id::new("Other", "second"),
      2,
      occurred_at(2),
      vec![2],
    ))
    .await
    .expect("今のスナップショットが head の位置にある集約へ追記できる");
  store
    .persist_event(EventEnvelope::new(
      Id::new("Plain", "third"),
      3,
      occurred_at(3),
      vec![3],
    ))
    .await
    .expect("スナップショットのない集約へ追記できる");

  let history_state = |row: &RawRow| only_value(row, "history", "state");
  let row_2 = journal_row(&scene, "Account-a-b", 2).await;
  assert_eq!(
    history_state(&row_2),
    Some(b"retired".to_vec()),
    "新しい 2 件を超えた行 2"
  );
  assert!(
    cells_in(&row_2, "history", "envelope").is_empty(),
    "retired の行の履歴の封筒は消える"
  );
  assert_eq!(
    only_value(&row_2, "event", "envelope"),
    Some(expected_event_envelope(
      "Account-a-b",
      2,
      i64::MIN,
      "e\u{301}🙂",
      &[1, 254]
    )),
    "retired でも event のセルは残る"
  );
  let row_3 = journal_row(&scene, "Account-a-b", 3).await;
  assert_eq!(
    history_state(&row_3),
    Some(b"active".to_vec()),
    "移行した今のスナップショットの位置"
  );
  assert_eq!(
    only_value(&row_3, "history", "envelope"),
    Some(expected_snapshot_envelope(3, "", &[255, 33]))
  );
  let row_5 = journal_row(&scene, "Account-a-b", 5).await;
  assert_eq!(
    history_state(&row_5),
    Some(b"active".to_vec()),
    "追記したスナップショットの位置"
  );
  let row_1 = journal_row(&scene, "Account-a-b", 1).await;
  assert!(
    row_1.iter().all(|cell| cell.family != "history"),
    "移さなかった位置に履歴を補作しない"
  );
  let events = store.get_events_by_id_since_seq_nr(&account, 0).await.unwrap();
  assert_eq!(
    events.iter().map(|event| event.seq_nr()).collect::<Vec<_>>(),
    vec![1, 2, 3, 4, 5]
  );
  let latest = store.get_latest_snapshot_by_id(&account).await.unwrap().unwrap();
  assert_eq!(latest.head_seq_nr(), 5);
  assert_eq!(latest.snapshot().unwrap().seq_nr(), 5);
}

#[tokio::test]
async fn should_migrate_without_history_when_the_new_store_keeps_none() {
  let fixture = success_fixture();
  let scene = Scene::new(&fixture.events, &fixture.snapshots, BigtableOptions::default()).await;
  let before = scene.state().await;

  let report = migrate(&scene, &fixture.mapping).await;

  assert!(report.reasons.is_empty(), "{report:?}");
  assert_eq!((report.aggregates, report.events, report.snapshots), (3, 7, 2));
  assert_success_fixture_readable(&scene).await;
  let after = scene.state().await;
  assert!(
    after
      .new_journal
      .iter()
      .all(|(_, row)| row.iter().all(|cell| cell.family != "history")),
    "保持件数がなければ history のセルを 1 つも作らない"
  );
  assert_old_tables_unchanged(&before, &after);
}

#[tokio::test]
async fn should_migrate_values_with_hash_that_are_uniquely_readable() {
  let events = vec![
    V3Event::new("Other-0", "Other", "x#y", 1),
    V3Event::new("Other-0", "Other", "x#y", 2),
  ];
  let snapshots = vec![
    V3Snapshot::current("Other-0", "Other", "x#y", 2),
    V3Snapshot::history("Other-0", "Other", "x#y", 1),
  ];
  let scene = Scene::new(&events, &snapshots, options_keeping(2)).await;
  let before = scene.state().await;

  let report = migrate(&scene, &HashMap::new()).await;

  assert!(report.reasons.is_empty(), "{report:?}");
  let store = open_bytes_store(&scene).await;
  let id = Id::new("Other", "x#y");
  let read = store.get_events_by_id_since_seq_nr(&id, 0).await.unwrap();
  assert_eq!(read.iter().map(|event| event.seq_nr()).collect::<Vec<_>>(), vec![1, 2]);
  assert_eq!(read[0].payload(), &vec![1]);
  assert_eq!(read[1].payload(), &vec![2]);
  let latest = store.get_latest_snapshot_by_id(&id).await.unwrap().unwrap();
  assert_eq!(latest.head_seq_nr(), 2);
  assert_eq!(latest.snapshot().unwrap().seq_nr(), 2);
  assert_eq!(latest.snapshot().unwrap().aggregate(), &vec![255, 2]);
  for seq_nr in [1, 2] {
    let row = journal_row(&scene, "Other-x#y", seq_nr).await;
    assert_eq!(
      only_value(&row, "history", "state"),
      Some(b"active".to_vec()),
      "行 {seq_nr}"
    );
  }
  assert_old_tables_unchanged(&before, &scene.state().await);
}

#[tokio::test]
async fn should_replace_a_hyphenated_old_type_name_with_the_mapping() {
  let events = vec![V3Event::new("Old-Account-3", "Old-Account", "a-b", 1)];
  let scene = Scene::new(&events, &[], BigtableOptions::default()).await;

  let report = migrate(&scene, &mapping()).await;

  assert!(report.reasons.is_empty(), "{report:?}");
  let store = open_bytes_store(&scene).await;
  let read = store
    .get_events_by_id_since_seq_nr(&Id::new("Account", "a-b"), 0)
    .await
    .unwrap();
  assert_eq!(read.len(), 1);
  assert_eq!(read[0].seq_nr(), 1);
  assert_eq!(read[0].payload(), &vec![1]);
}

// ---------------------------------------------------------------------------
// 拒否: 旧データの中身
// ---------------------------------------------------------------------------

/// 拒否する旧データを、検査を通る集約と一緒に用意して移行し、何も書かれないことを確かめる。報告を返す。
async fn migrate_rejected(
  events: Vec<V3Event>,
  snapshots: Vec<V3Snapshot>,
  mapping: &HashMap<String, String>,
) -> (Scene, State, MigrationReport) {
  let (events, snapshots) = with_good_aggregates(events, snapshots);
  let scene = Scene::new(&events, &snapshots, options_keeping(1)).await;
  let before = scene.state().await;
  let report = migrate(&scene, mapping).await;
  assert_rejected_and_nothing_written(&scene, &before, &report).await;
  (scene, before, report)
}

#[tokio::test]
async fn should_reject_a_snapshot_row_that_cannot_be_told_from_a_history_row() {
  // 値 `x` の履歴の行（位置 1）とも、値 `x#00000000000000000001` の今の行とも読める。どちらの集約も journal にある。
  let events = vec![
    V3Event::new("T-0", "T", "x", 1),
    V3Event::new("T-0", "T", "x#00000000000000000001", 1),
  ];
  let snapshots = vec![V3Snapshot::current("T-0", "T", "x#00000000000000000001", 1)];

  let (_, _, report) = migrate_rejected(events, snapshots, &HashMap::new()).await;

  assert_reasons_contain(
    &report,
    &["P-22: 行キーを一意に読み解けません"],
    "区別のつかない snapshot の行",
  );
}

#[tokio::test]
async fn should_reject_a_journal_row_key_that_can_be_read_in_two_ways() {
  // 型名 `a`・値 `b-1#a-0#a#b#v` とも、型名 `a-0#a#b`・値 `v` とも読める journal の行キー。推測せず拒む。
  let ambiguous = V3Event::new("a-0", "a", "b-1#a-0#a#b#v", 1);
  let skey = String::from_utf8(ambiguous.row_key.clone()).unwrap();
  assert_eq!(skey, "a-0#a#b-1#a-0#a#b#v#00000000000000000001");

  let (_, _, report) = migrate_rejected(vec![ambiguous], vec![], &HashMap::new()).await;

  let rejection = report
    .reasons
    .iter()
    .find(|rejection| rejection.skey.as_deref() == Some(skey.as_str()))
    .unwrap_or_else(|| panic!("行キー {skey} の拒否がない: {report:?}"));
  assert!(
    rejection.reason.contains("P-22: 行キーを一意に読み解けません"),
    "{rejection:?}"
  );
}

#[tokio::test]
async fn should_reject_row_keys_that_are_not_the_default_layout() {
  let base = V3Event::new("A-0", "A", "v", 1);
  let events = vec![
    // パーティションキーの型名と、行キーの型名が違う。
    V3Event::new("A-0", "B", "v", 1),
    // シャード番号に先頭のゼロがある。
    V3Event::new("A-01", "A", "v", 1),
    // シャード番号が数ではない。
    V3Event::new("A-x", "A", "v", 1),
    // 末尾の番号が 20 桁ではない。
    V3Event {
      row_key: b"A-0#A#v#1".to_vec(),
      ..base.clone()
    },
    // 末尾の番号が 20 桁の 10 進数ではない。
    V3Event {
      row_key: b"A-0#A#v#0000000000000000000a".to_vec(),
      ..base.clone()
    },
    // UTF-8 ではない行キー。
    V3Event {
      row_key: vec![0xC3, 0x28],
      ..base
    },
  ];
  let row_keys: Vec<Vec<u8>> = events.iter().map(|event| event.row_key.clone()).collect();

  let (_, _, report) = migrate_rejected(events, vec![], &HashMap::new()).await;

  for row_key in row_keys {
    let skey = String::from_utf8_lossy(&row_key).into_owned();
    let rejection = report
      .reasons
      .iter()
      .find(|rejection| rejection.skey.as_deref() == Some(skey.as_str()))
      .unwrap_or_else(|| panic!("{skey:?} の拒否がない: {report:?}"));
    assert!(rejection.reason.starts_with("P-22"), "{skey:?}: {rejection:?}");
  }
}

#[tokio::test]
async fn should_reject_a_row_key_number_that_differs_from_the_seq_nr_cell() {
  let events = vec![
    V3Event::new("A-0", "A", "v", 1),
    V3Event {
      seq_nr: b"3".to_vec(),
      ..V3Event::new("A-0", "A", "v", 2)
    },
  ];

  let (_, _, report) = migrate_rejected(events, vec![], &HashMap::new()).await;

  assert_reasons_contain(
    &report,
    &["P-22: キー番号とjournalのseq_nrが一致しません"],
    "キーの番号と seq_nr のセルの食い違い",
  );
}

#[tokio::test]
async fn should_reject_a_mapped_aid_that_collides_with_an_existing_aid() {
  let events = vec![
    V3Event::new("Account-1", "Account", "a", 1),
    V3Event::new("Old-Account-1", "Old-Account", "a", 1),
  ];

  let (_, _, report) = migrate_rejected(events, vec![], &mapping()).await;

  assert_reasons_contain(
    &report,
    &["P-22: 複数の旧パーティションが同じaidへ対応しています"],
    "対応後の aid の衝突",
  );
}

#[tokio::test]
async fn should_reject_an_aid_over_1024_bytes() {
  // aid は `Long-` の 5 バイトと値の 1020 バイトで 1025 バイト。
  let events = vec![V3Event::new("Long-0", "Long", &"あ".repeat(340), 1)];

  let (_, _, report) = migrate_rejected(events, vec![], &HashMap::new()).await;

  assert_reasons_contain(&report, &["T-12"], "1024 バイトを超える aid");
}

#[tokio::test]
async fn should_reject_an_unmapped_hyphen_and_a_hyphen_left_after_mapping() {
  let events = vec![
    V3Event::new("Mapped-Old-2", "Mapped-Old", "a", 1),
    V3Event::new("Old-Account-1", "Old-Account", "a", 1),
  ];
  let mapping = HashMap::from([("Old-Account".to_string(), "A-B".to_string())]);

  let (_, _, report) = migrate_rejected(events, vec![], &mapping).await;

  assert_reasons_contain(&report, &["P-23", "T-11"], "対応表のない - と、置き換え後に残る -");
}

#[tokio::test]
async fn should_reject_each_case_and_write_nothing() {
  for case in rejection_cases() {
    for keep in &case.keeps {
      let options = keep.map_or_else(BigtableOptions::default, options_keeping);
      let scene = Scene::new(&case.events, &case.snapshots, options).await;
      let before = scene.state().await;
      let context = format!("{} (保持件数 {keep:?})", case.name);

      let report = migrate(&scene, &case.mapping).await;

      assert_reasons_contain(&report, &case.expected_reasons, &context);
      assert_rejected_and_nothing_written(&scene, &before, &report).await;
    }
  }
}

// ---------------------------------------------------------------------------
// 封筒の上限（BT-16）
// ---------------------------------------------------------------------------

/// 集約 `Big-a` の、サイズを指定した旧データ。イベントは 1 と 2、今のスナップショットは位置 2、履歴の行は位置 1。
/// 引数は、イベント 1 の payload・イベント 2 の payload・今のスナップショットの payload・履歴の行の payload の長さ。
fn sized_rows(event_1: usize, event_2: usize, current: usize, history: usize) -> (Vec<V3Event>, Vec<V3Snapshot>) {
  (
    vec![
      V3Event {
        payload: vec![1; event_1],
        ..V3Event::new("Big-0", "Big", "a", 1)
      },
      V3Event {
        payload: vec![2; event_2],
        ..V3Event::new("Big-0", "Big", "a", 2)
      },
    ],
    vec![
      V3Snapshot {
        payload: vec![3; current],
        ..V3Snapshot::current("Big-0", "Big", "a", 2)
      },
      V3Snapshot {
        payload: vec![4; history],
        ..V3Snapshot::history("Big-0", "Big", "a", 1)
      },
    ],
  )
}

fn event_envelope_len(payload_len: usize) -> u64 {
  let ns = occurred_at(1).timestamp_nanos_opt().unwrap();
  expected_event_envelope("Big-a", 1, ns, "", &vec![0; payload_len]).len() as u64
}

fn snapshot_envelope_len(payload_len: usize) -> u64 {
  expected_snapshot_envelope(1, "", &vec![0; payload_len]).len() as u64
}

#[tokio::test]
async fn should_reject_an_envelope_over_the_limit_and_write_nothing() {
  // (名前, 旧データ, 上限)。どの場合も、上限より 1 バイト大きい封筒が 1 つだけある。
  let cases = [
    ("イベントの封筒", sized_rows(200, 1, 1, 1), event_envelope_len(200) - 1),
    (
      "今のスナップショットの封筒",
      sized_rows(1, 1, 300, 1),
      snapshot_envelope_len(300) - 1,
    ),
  ];
  for (name, (events, snapshots), limit) in cases {
    let scene = Scene::new(&events, &snapshots, options_with(None, limit)).await;
    let before = scene.state().await;

    let report = migrate(&scene, &HashMap::new()).await;

    assert_reasons_contain(&report, &["BT-16"], name);
    assert_rejected_and_nothing_written(&scene, &before, &report).await;
  }
}

#[tokio::test]
async fn should_migrate_an_envelope_of_exactly_the_limit() {
  // 上限と同じ大きさの封筒は移せる。固定部分と aid を含めた長さで比べる。
  let cases = [
    ("イベントの封筒", sized_rows(200, 1, 1, 1), event_envelope_len(200)),
    (
      "今のスナップショットの封筒",
      sized_rows(1, 1, 300, 1),
      snapshot_envelope_len(300),
    ),
  ];
  for (name, (events, snapshots), limit) in cases {
    let scene = Scene::new(&events, &snapshots, options_with(None, limit)).await;

    let report = migrate(&scene, &HashMap::new()).await;

    assert!(report.reasons.is_empty(), "{name}: {report:?}");
    assert_eq!(
      (report.aggregates, report.events, report.snapshots),
      (1, 2, 1),
      "{name}"
    );
  }
}

#[tokio::test]
async fn should_not_count_the_size_of_a_history_row_that_is_not_migrated() {
  // 履歴の行（位置 1）だけが上限を超える。保持件数なしと 1（新しい 1 件は今の位置）では移さないので、拒まない。
  for keep in [None, Some(1)] {
    let (events, snapshots) = sized_rows(1, 1, 1, 200);
    let scene = Scene::new(&events, &snapshots, options_with(keep, 100)).await;

    let report = migrate(&scene, &HashMap::new()).await;

    assert!(report.reasons.is_empty(), "保持件数 {keep:?}: {report:?}");
  }
  // 保持件数 2 では履歴の行（位置 1）も移すので、その封筒の大きさで拒む。
  let (events, snapshots) = sized_rows(1, 1, 1, 200);
  let scene = Scene::new(&events, &snapshots, options_with(Some(2), 100)).await;
  let before = scene.state().await;

  let report = migrate(&scene, &HashMap::new()).await;

  assert_reasons_contain(&report, &["BT-16"], "保持件数 2");
  assert_rejected_and_nothing_written(&scene, &before, &report).await;
}

#[tokio::test]
async fn should_report_one_envelope_rejection_when_the_current_snapshot_is_also_a_retained_history() {
  // 保持件数 1 では、今のスナップショット（位置 2）が保持する履歴の 1 件にも入る。
  // 同じ位置の封筒の超過は、集約ごとに 1 件だけ報告する。
  let (events, snapshots) = sized_rows(1, 1, 300, 1);
  let scene = Scene::new(
    &events,
    &snapshots,
    options_with(Some(1), snapshot_envelope_len(300) - 1),
  )
  .await;
  let before = scene.state().await;

  let report = migrate(&scene, &HashMap::new()).await;

  let rejections = report
    .reasons
    .iter()
    .filter(|rejection| rejection.reason.contains("BT-16"))
    .count();
  assert_eq!(rejections, 1, "{report:?}");
  assert_rejected_and_nothing_written(&scene, &before, &report).await;
}

// ---------------------------------------------------------------------------
// 新しい 2 テーブルの状態
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_reject_new_tables_with_rows_and_write_nothing() {
  let events = vec![V3Event::new("Good-0", "Good", "a", 1)];
  for kind in [TableKind::Journal, TableKind::Head] {
    let scene = Scene::new(&events, &[], BigtableOptions::default()).await;
    let prefix = row_prefix("Occupied-a", None);
    match kind {
      TableKind::Journal => {
        scene
          .env
          .raw_put(kind, &journal_key(&prefix, 1), "event", "envelope", 0, b"occupied")
          .await
      }
      TableKind::Head => scene.env.raw_put(kind, &prefix, "head", "seq_nr", 0, b"occupied").await,
    }
    let before = scene.state().await;

    let report = migrate(&scene, &HashMap::new()).await;

    let path = scene.env.table_path(kind);
    assert!(
      report.reasons.iter().any(|rejection| rejection.table == path),
      "{path} の拒否がない: {report:?}"
    );
    assert_eq!(
      (report.aggregates, report.events, report.snapshots),
      (0, 0, 0),
      "{kind:?}"
    );
    assert_eq!(scene.state().await, before, "{kind:?}: 先にあった行も含めて変わらない");
  }
}

/// journal と head の両方の列ファミリを GC 付きで持つ 2 テーブルを作る。列ファミリは引数で選ぶ。
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

#[tokio::test]
async fn should_fail_on_a_configuration_mismatch_and_write_nothing() {
  let events = vec![V3Event::new("Good-0", "Good", "a", 1)];
  let snapshots = vec![V3Snapshot::current("Good-0", "Good", "a", 1)];
  // (名前, 新しいテーブルの初期化, 移行に渡す設定, 期待する設定エラーの理由)
  let cases = [
    (
      "保持件数が違う",
      Some(options_keeping(2)),
      options_keeping(1),
      ConfigurationReason::BigtableConfigurationMismatch,
    ),
    (
      "保持件数が記録にあって、渡す設定にない",
      Some(options_keeping(1)),
      BigtableOptions::default(),
      ConfigurationReason::BigtableConfigurationMismatch,
    ),
    (
      "封筒の上限が違う",
      Some(options_with(None, 4096)),
      BigtableOptions::default(),
      ConfigurationReason::BigtableConfigurationMismatch,
    ),
    (
      "初期化していない",
      None,
      BigtableOptions::default(),
      ConfigurationReason::BigtableConfigurationInvalid,
    ),
  ];
  for (name, initialized_with, options, expected) in cases {
    let base = match &initialized_with {
      Some(initialized) => Fixture::initialized(initialized).await,
      None => Fixture::new().await,
    };
    let scene = Scene::over(base, options, &events, &snapshots).await;
    let before = scene.state().await;

    let error = migrate_failing(&scene).await;

    assert_eq!(configuration_reason(&error), Some(&expected), "{name}: {error:?}");
    assert_eq!(
      (error.report.aggregates, error.report.events, error.report.snapshots),
      (0, 0, 0),
      "{name}"
    );
    assert_eq!(
      scene.state().await,
      before,
      "{name}: 設定行を含め、何も作らず、何も書かない"
    );
  }
}

#[tokio::test]
async fn should_fail_when_the_new_tables_column_families_differ_from_the_profile() {
  let events = vec![V3Event::new("Good-0", "Good", "a", 1)];
  let base = Fixture::new().await;
  // 設定行は書かれているが、journal の列ファミリ `event` に年齢による GC がある。
  let aged = FamilySpec {
    name: "event",
    garbage_collection: GarbageCollection::MaxAgeSeconds(3600),
  };
  let env = env_with_tables(
    &base,
    "age-gc",
    &[aged, FamilySpec::current("history"), FamilySpec::current("config")],
    &[
      FamilySpec::current("head"),
      FamilySpec::current("snapshot"),
      FamilySpec::current("config"),
    ],
  )
  .await;
  env.initialize(&BigtableOptions::default()).await.unwrap();
  let scene = Scene::over_env(base, env, BigtableOptions::default(), &events, &[]).await;
  let before = scene.state().await;

  let error = migrate_failing(&scene).await;

  assert_eq!(
    configuration_reason(&error),
    Some(&ConfigurationReason::BigtableColumnFamilyMismatch),
    "{error:?}"
  );
  assert_eq!(scene.state().await, before, "何も書かない");
}

// ---------------------------------------------------------------------------
// 旧と新のテーブルの重なり
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_reject_new_tables_that_overlap_the_old_tables() {
  let events = vec![V3Event::new("Good-0", "Good", "a", 1)];
  let scene = Scene::new(&events, &[], BigtableOptions::default()).await;
  let before = scene.state().await;
  let tables = &scene.env.tables;
  let legacy = &scene.legacy;
  // 旧の 1 つを新しい 1 つに重ねる全ての組み合わせ。もう片方は重ならない本物の旧テーブルのままにする。
  let overlaps = [
    (
      "旧 journal = 新 journal",
      &tables.journal_table_id,
      &legacy.snapshot_table_id,
    ),
    ("旧 journal = 新 head", &tables.head_table_id, &legacy.snapshot_table_id),
    (
      "旧 snapshot = 新 journal",
      &legacy.journal_table_id,
      &tables.journal_table_id,
    ),
    ("旧 snapshot = 新 head", &legacy.journal_table_id, &tables.head_table_id),
  ];
  for (name, journal_table_id, snapshot_table_id) in overlaps {
    let overlapping = LegacyBigtableTables {
      journal_table_id: journal_table_id.clone(),
      snapshot_table_id: snapshot_table_id.clone(),
      ..legacy.clone()
    };

    let report = migrate_with(&scene, &overlapping, &HashMap::new()).await;

    assert!(
      report
        .reasons
        .iter()
        .any(|rejection| rejection.table.is_empty() && rejection.reason.contains("互いに異なるテーブル")),
      "{name}: {report:?}"
    );
    assert_rejected_and_nothing_written(&scene, &before, &report).await;
  }
}

#[tokio::test]
async fn should_not_treat_the_same_table_ids_in_another_instance_as_overlapping() {
  let fixture = success_fixture();
  let scene = Scene::new(&[], &[], options_keeping(2)).await;
  // 旧の 2 テーブルを、新しい 2 テーブルと同じテーブル ID で、別のインスタンスに作る。完全なパスは重ならない。
  let legacy = LegacyBigtableTables {
    project_id: PROJECT.to_string(),
    instance_id: "other-instance".to_string(),
    journal_table_id: scene.env.tables.journal_table_id.clone(),
    snapshot_table_id: scene.env.tables.head_table_id.clone(),
  };
  create_legacy_tables_named(scene.env.port, &legacy).await;
  write_legacy_rows(scene.env.port, &legacy, &fixture.events, &fixture.snapshots).await;

  let report = migrate_with(&scene, &legacy, &fixture.mapping).await;

  assert!(report.reasons.is_empty(), "{report:?}");
  assert_eq!((report.aggregates, report.events, report.snapshots), (3, 7, 3));
  assert_success_fixture_readable(&scene).await;
}

// ---------------------------------------------------------------------------
// 旧テーブルの読み取りが複数のページにまたがる場合（1 ページは 1000 行）
// ---------------------------------------------------------------------------

/// 1 つの集約が `count` 件のイベントを持つ旧データを移し、公開 API と生の行で全件を確かめる。
///
/// 旧 journal は `count` 行になり、1000 行ごとのページの境界（ちょうど 1000 行、境界の直後の 1 行、3 ページ目の 1 行）を
/// またぐ。今のスナップショットは最後の位置、履歴の行は位置 1 と中ほどにある。保持 2 では、中ほどと最後が active な履歴になる。
async fn migrate_one_aggregate_with_events(count: u64) {
  let middle = count / 2;
  let events: Vec<V3Event> = (1..=count)
    .map(|seq_nr| V3Event::new("Account-0", "Account", "long", seq_nr))
    .collect();
  let snapshots = vec![
    V3Snapshot::history("Account-0", "Account", "long", 1),
    V3Snapshot::history("Account-0", "Account", "long", middle),
    V3Snapshot::current("Account-0", "Account", "long", count),
  ];
  let scene = Scene::new(&events, &snapshots, options_keeping(2)).await;
  let before = scene.state().await;

  let report = migrate(&scene, &HashMap::new()).await;

  assert!(report.reasons.is_empty(), "{count} 件: {report:?}");
  // 今のスナップショット 1 件と、位置 `middle` の履歴 1 件。最後の位置の履歴は今のスナップショットと同じものなので数えない。
  assert_eq!(
    (report.aggregates, report.events, report.snapshots),
    (1, count, 2),
    "{count} 件: {report:?}"
  );
  let store = open_bytes_store(&scene).await;
  let id = Id::new("Account", "long");
  let read = store
    .get_events_by_id_since_seq_nr(&id, 0)
    .await
    .expect("移行したイベントを読める");
  assert_eq!(read.len() as u64, count, "{count} 件: 読み戻した件数");
  for (index, event) in read.iter().enumerate() {
    let seq_nr = index as u64 + 1;
    assert_eq!(event.seq_nr(), seq_nr, "{count} 件: 番号が欠けず重ならずに続く");
    assert_eq!(event.payload(), &vec![seq_nr as u8], "{count} 件 / {seq_nr}");
  }
  let latest = store
    .get_latest_snapshot_by_id(&id)
    .await
    .expect("移行した head を読める")
    .expect("head がある");
  assert_eq!(latest.head_seq_nr(), count, "{count} 件");
  let snapshot = latest.snapshot().expect("今のスナップショットがある");
  assert_eq!(snapshot.seq_nr(), count, "{count} 件");
  assert_eq!(snapshot.aggregate(), &vec![255, count as u8], "{count} 件");

  let after = scene.state().await;
  assert_old_tables_unchanged(&before, &after);
  assert_eq!(
    after.new_journal.len() as u64,
    count + 1,
    "{count} 件: 設定行と全イベントの行"
  );
  assert_eq!(after.new_head.len(), 2, "{count} 件: 設定行と head の行");
  for (seq_nr, state, has_snapshot) in [
    (1, None, "1"),
    (middle, Some(b"active".to_vec()), "1"),
    (count, Some(b"active".to_vec()), "1"),
    (count - 1, None, "0"),
  ] {
    let row = journal_row(&scene, "Account-long", seq_nr).await;
    assert_eq!(only_value(&row, "history", "state"), state, "{count} 件 / {seq_nr}");
    assert_eq!(
      only_value(&row, "event", "has_snapshot"),
      Some(has_snapshot.as_bytes().to_vec()),
      "{count} 件 / {seq_nr}"
    );
  }
}

#[tokio::test]
async fn should_migrate_an_aggregate_whose_events_end_just_before_the_first_page_boundary() {
  migrate_one_aggregate_with_events(999).await;
}

#[tokio::test]
async fn should_migrate_an_aggregate_whose_events_fill_exactly_one_page() {
  migrate_one_aggregate_with_events(1000).await;
}

#[tokio::test]
async fn should_migrate_an_aggregate_whose_head_event_is_the_first_row_of_the_second_page() {
  migrate_one_aggregate_with_events(1001).await;
}

#[tokio::test]
async fn should_migrate_an_aggregate_whose_events_span_three_pages() {
  migrate_one_aggregate_with_events(2001).await;
}

/// 1 イベントずつと今のスナップショットを持つ集約を `count` 個移す。旧 journal と旧 snapshot が、どちらも `count` 行で
/// ページの境界をまたぐ。値は 4 桁の 10 進なので、行キーの順は番号の順で、番号 999 がページの最後、1000 が次のページの最初になる。
async fn migrate_aggregates(count: u64) {
  let value = |number: u64| format!("n{number:04}");
  let events: Vec<V3Event> = (0..count)
    .map(|number| V3Event::new("Account-0", "Account", &value(number), 1))
    .collect();
  let snapshots: Vec<V3Snapshot> = (0..count)
    .map(|number| V3Snapshot::current("Account-0", "Account", &value(number), 1))
    .collect();
  let scene = Scene::new(&events, &snapshots, options_keeping(2)).await;
  let before = scene.state().await;

  let report = migrate(&scene, &HashMap::new()).await;

  assert!(report.reasons.is_empty(), "{count} 集約: {report:?}");
  assert_eq!(
    (report.aggregates, report.events, report.snapshots),
    (count, count, count),
    "{count} 集約: {report:?}"
  );
  let after = scene.state().await;
  assert_old_tables_unchanged(&before, &after);
  assert_eq!(
    after.new_head.len() as u64,
    count + 1,
    "{count} 集約: 設定行と全ての head"
  );
  assert_eq!(
    after.new_journal.len() as u64,
    count + 1,
    "{count} 集約: 設定行と全ての journal の行"
  );
  for (key, row) in after.new_head.iter().filter(|(key, _)| key.as_slice() != [0x00]) {
    assert_eq!(
      only_value(row, "head", "seq_nr"),
      Some(head_seq_nr_text(1)),
      "{count} 集約: {key:?}"
    );
    assert!(
      only_value(row, "snapshot", "current").is_some(),
      "{count} 集約: 全ての head が今のスナップショットを持つ {key:?}"
    );
  }
  let store = open_bytes_store(&scene).await;
  for number in [0, 998, 999, 1000, count - 1]
    .into_iter()
    .filter(|number| *number < count)
  {
    let id = Id::new("Account", &value(number));
    let read = store
      .get_latest_snapshot_by_id(&id)
      .await
      .expect("移行した head を読める")
      .unwrap_or_else(|| panic!("{} の head がある", id.aid()));
    assert_eq!(read.head_seq_nr(), 1, "{}", id.aid());
    assert_eq!(
      read.snapshot().map(|snapshot| snapshot.seq_nr()),
      Some(1),
      "{}",
      id.aid()
    );
    let events = store
      .get_events_by_id_since_seq_nr(&id, 0)
      .await
      .expect("移行したイベントを読める");
    assert_eq!(events.len(), 1, "{}", id.aid());
  }
}

#[tokio::test]
async fn should_migrate_exactly_one_page_of_aggregates_in_both_old_tables() {
  migrate_aggregates(1000).await;
}

#[tokio::test]
async fn should_migrate_one_more_aggregate_than_one_page_in_both_old_tables() {
  migrate_aggregates(1001).await;
}

// ---------------------------------------------------------------------------
// トークンの取得元: 旧と新の全ての要求の直前に呼ばれ、失敗しても書き込みの順が保たれる
// ---------------------------------------------------------------------------

/// 呼び出しを数え、`fail_from` 回目以降の呼び出しを失敗させるトークンの取得元。
#[derive(Debug)]
struct CountingTokens {
  calls: AtomicUsize,
  fail_from: Option<usize>,
}

impl CountingTokens {
  fn new(fail_from: Option<usize>) -> Arc<Self> {
    Arc::new(Self {
      calls: AtomicUsize::new(0),
      fail_from,
    })
  }

  fn calls(&self) -> usize {
    self.calls.load(Ordering::SeqCst)
  }
}

#[async_trait]
impl BigtableTokenSource for CountingTokens {
  async fn access_token(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
    if self.fail_from.is_some_and(|from| call >= from) {
      return Err(format!("the token source failed at call {call}").into());
    }
    Ok(format!("token-{call}"))
  }
}

async fn migrate_with_tokens(
  scene: &Scene,
  tokens: &Arc<CountingTokens>,
  mapping: &HashMap<String, String>,
) -> Result<MigrationReport, MigrationError> {
  let source: Arc<dyn BigtableTokenSource> = tokens.clone();
  migrate_v3_bigtable(
    &scene.env.connection().await,
    Some(source),
    &scene.legacy,
    &scene.env.tables,
    &scene.options,
    mapping,
  )
  .await
}

/// 書き込みの途中で止まった新しい 2 テーブルが、プロファイル 5 章の不変条件と、報告の件数に合うことを確かめる。
///
/// - head がある集約は、h 未満の journal の行が全てある。
/// - head がない集約は、h の journal の行がなく、書いた行は 1 から欠けずに続く。
/// - 報告の件数は、書けた head・journal の行・スナップショットの数と等しい。
fn assert_partial_state(state: &State, report: &MigrationReport, context: &str) {
  let mut heads = 0_u64;
  let mut events = 0_u64;
  let mut snapshots = 0_u64;
  let mut known_head_rows = 0_usize;
  let mut known_journal_rows = 0_usize;
  for aggregate in success_aggregates() {
    let aid = aggregate.id().aid();
    let prefix = row_prefix(&aid, None);
    let maximum = aggregate.head_seq_nr();
    let current = aggregate.current.as_ref().map(|snapshot| snapshot.seq_nr);
    let head = state.new_head.iter().find(|(key, _)| *key == prefix);
    let journal: BTreeMap<u64, &RawRow> = state
      .new_journal
      .iter()
      .filter(|(key, _)| key.len() == prefix.len() + 8 && key.starts_with(&prefix))
      .map(|(key, row)| {
        (
          u64::from_be_bytes(key[prefix.len()..].try_into().expect("8 バイト")),
          row,
        )
      })
      .collect();
    let seq_nrs: BTreeSet<u64> = journal.keys().copied().collect();
    known_journal_rows += journal.len();
    match head {
      Some((_, row)) => {
        known_head_rows += 1;
        assert_eq!(
          only_value(row, "head", "seq_nr"),
          Some(head_seq_nr_text(maximum)),
          "{context}: {aid} の head は最大の番号"
        );
        for seq_nr in 1..maximum {
          assert!(
            seq_nrs.contains(&seq_nr),
            "{context}: {aid} は head より前に h 未満の journal の行 {seq_nr} を書く"
          );
        }
        assert!(
          seq_nrs.iter().all(|seq_nr| *seq_nr <= maximum),
          "{context}: {aid} は h を超える行を持たない"
        );
        heads += 1;
        snapshots += u64::from(only_value(row, "snapshot", "current").is_some());
      }
      None => {
        assert!(
          !seq_nrs.contains(&maximum),
          "{context}: {aid} は head より先に h の journal の行を書かない"
        );
        assert_eq!(
          seq_nrs.iter().copied().collect::<Vec<_>>(),
          (1..=seq_nrs.len() as u64).collect::<Vec<_>>(),
          "{context}: {aid} が書いた行は 1 から欠けずに続く"
        );
      }
    }
    events += journal.len() as u64;
    // 今のスナップショットは head の作成で数えるので、履歴の封筒のうち、その位置のものは数えない。
    snapshots += journal
      .iter()
      .filter(|(seq_nr, row)| only_value(row, "history", "envelope").is_some() && Some(**seq_nr) != current)
      .count() as u64;
  }
  assert_eq!(
    state.new_head.len(),
    known_head_rows + 1,
    "{context}: head の行は設定行と既知の集約だけ"
  );
  assert_eq!(
    state.new_journal.len(),
    known_journal_rows + 1,
    "{context}: journal の行は設定行と既知の集約だけ"
  );
  assert_eq!(
    (report.aggregates, report.events, report.snapshots),
    (heads, events, snapshots),
    "{context}: 報告の件数は書けた行の数と等しい: {report:?}"
  );
}

/// 成功する移行で取得元を数え、その回数だけ、k 回目以降を失敗させる取得元で移行をやり直す。
///
/// どの k でも `Err(MigrationError)` が返り、取得元は失敗した呼び出しの後に呼び直されず、旧テーブルは変わらず、
/// 新しい 2 テーブルは書き込みの順の不変条件を保つ。
///
/// 旧テーブルの要求にも取得元が呼ばれることは、最初の書き込みの前の呼び出し回数で確かめる。書き込みの前に、新しいテーブルの
/// 照合（`GetTable` 2 回、設定行の読み取り 2 回、データの行の確認 2 回）、旧 journal の検査、旧 snapshot の検査、
/// 旧 journal の再読の 9 回の読み取りがある。旧テーブルの要求に取得元を渡し忘れると、この回数が減る。
#[tokio::test]
async fn should_ask_the_token_source_before_every_request_and_keep_the_write_order_when_it_fails() {
  let fixture = success_fixture();
  let scene = Scene::new(&fixture.events, &fixture.snapshots, options_keeping(2)).await;
  let tokens = CountingTokens::new(None);

  let report = migrate_with_tokens(&scene, &tokens, &fixture.mapping)
    .await
    .expect("取得元が失敗しなければ移行できる");

  assert!(report.reasons.is_empty(), "{report:?}");
  assert_eq!((report.aggregates, report.events, report.snapshots), (3, 7, 3));
  assert_success_fixture_readable(&scene).await;
  let total = tokens.calls();
  assert!(
    total >= 9 + 7 + 3,
    "読み取り 9 回、journal の行 7 回、head 3 回より少なくはない: {total}"
  );

  let mut first_written_from = None;
  for from in 1..=total {
    let scene = Scene::new(&fixture.events, &fixture.snapshots, options_keeping(2)).await;
    let before = scene.state().await;
    let tokens = CountingTokens::new(Some(from));

    let error = migrate_with_tokens(&scene, &tokens, &fixture.mapping)
      .await
      .expect_err("取得元が失敗した移行は MigrationError で返る");

    let context = format!("{from} 回目から失敗");
    assert!(
      matches!(
        error.source.downcast_ref::<EventStoreError>(),
        Some(EventStoreError::Storage { .. })
      ),
      "{context}: 取得元の失敗は保存先エラー: {error:?}"
    );
    assert_eq!(tokens.calls(), from, "{context}: 失敗の後に取得元を呼び直さない");
    let after = scene.state().await;
    assert_old_tables_unchanged(&before, &after);
    assert_partial_state(&after, &error.report, &context);
    if first_written_from.is_none() && (after.new_journal.len() > 1 || after.new_head.len() > 1) {
      first_written_from = Some(from);
    }
  }
  // 書き込みの前の 9 回の読み取りがすべて取得元を呼ぶなら、10 回目が最初の書き込みで、11 回目から失敗させて初めて行が残る。
  let first_written_from = first_written_from.expect("どこかで行が書かれる");
  assert!(
    first_written_from >= 11,
    "書き込みの前に旧テーブルの読み取りを含む 9 回以上取得元を呼ぶ: 行が残る最初の失敗は {first_written_from} 回目から"
  );
}

// ---------------------------------------------------------------------------
// 検査の後に別の書き手が head を作った場合
// ---------------------------------------------------------------------------

/// 対象の集約の journal の行 1 が書かれた後の最初の呼び出しで、同じ集約の head を別の書き手として作るトークンの取得元。
///
/// 移行は、h より前の行を書いた後に head を作る。その直前に head を割り込ませるので、検査の後に別の書き手が入った場合に当たる。
#[derive(Debug)]
struct HeadRacingTokens {
  env: Env,
  journal_row_key: Vec<u8>,
  head_row_key: Vec<u8>,
  injected: AtomicBool,
}

#[async_trait]
impl BigtableTokenSource for HeadRacingTokens {
  async fn access_token(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    if !self.injected.load(Ordering::SeqCst)
      && !self
        .env
        .raw_row(TableKind::Journal, &self.journal_row_key)
        .await
        .is_empty()
    {
      self.injected.store(true, Ordering::SeqCst);
      self
        .env
        .raw_put(
          TableKind::Head,
          &self.head_row_key,
          "head",
          "seq_nr",
          0,
          &head_seq_nr_text(9),
        )
        .await;
    }
    Ok("token".to_string())
  }
}

#[tokio::test]
async fn should_not_overwrite_a_head_created_by_another_writer_after_the_inspection_and_report_the_conflict() {
  // 集約 `Aa-a` が先、`Bb-a` が後に書かれる。`Aa-a` の head を、行 1 を書いた後・head を作る前に別の書き手が作る。
  let events = vec![
    V3Event::new("Aa-0", "Aa", "a", 1),
    V3Event::new("Aa-0", "Aa", "a", 2),
    V3Event::new("Bb-0", "Bb", "a", 1),
    V3Event::new("Bb-0", "Bb", "a", 2),
  ];
  let scene = Scene::new(&events, &[], options_keeping(1)).await;
  let before = scene.state().await;
  let tokens = Arc::new(HeadRacingTokens {
    env: scene.env.clone(),
    journal_row_key: journal_key(&row_prefix("Aa-a", None), 1),
    head_row_key: row_prefix("Aa-a", None),
    injected: AtomicBool::new(false),
  });
  let source: Arc<dyn BigtableTokenSource> = tokens.clone();

  let report = migrate_v3_bigtable(
    &scene.env.connection().await,
    Some(source),
    &scene.legacy,
    &scene.env.tables,
    &scene.options,
    &HashMap::new(),
  )
  .await
  .expect("head の競合は Err ではなく、報告の理由で返る");

  assert!(
    tokens.injected.load(Ordering::SeqCst),
    "別の書き手の head を割り込ませた"
  );
  assert_eq!(report.reasons.len(), 1, "{report:?}");
  let rejection = &report.reasons[0];
  assert!(rejection.reason.contains("条件競合"), "{rejection:?}");
  assert_eq!(rejection.aid.as_deref(), Some("Aa-a"), "{rejection:?}");
  assert_eq!(rejection.table, scene.env.table_path(TableKind::Head), "{rejection:?}");
  // 書けたのは `Aa-a` の行 1 だけで、head は作れていない。
  assert_eq!(
    (report.aggregates, report.events, report.snapshots),
    (0, 1, 0),
    "{report:?}"
  );
  let after = scene.state().await;
  assert_old_tables_unchanged(&before, &after);
  // 別の書き手の head は上書きされない。
  let head = head_row(&scene, "Aa-a").await;
  assert_eq!(head.len(), 1, "別の書き手の head にセルを足さない: {head:?}");
  assert_eq!(
    only_value(&head, "head", "seq_nr"),
    Some(head_seq_nr_text(9)),
    "別の書き手の head を書き換えない"
  );
  // 止まった後は何も書かない: `Aa-a` の h の行も、`Bb-a` の行と head もない。
  assert!(!journal_row(&scene, "Aa-a", 1).await.is_empty());
  assert!(journal_row(&scene, "Aa-a", 2).await.is_empty());
  assert!(journal_row(&scene, "Bb-a", 1).await.is_empty());
  assert!(head_row(&scene, "Bb-a").await.is_empty());
  assert_eq!(after.new_journal.len(), 2, "journal は設定行と `Aa-a` の行 1 だけ");
  assert_eq!(after.new_head.len(), 2, "head は設定行と別の書き手の head だけ");
}
