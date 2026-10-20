#![cfg(feature = "sqlite-migration")]

#[path = "sqlite_migration_test/support.rs"]
mod support;

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use event_store_adapter_rs::{migrate_v3_sqlite, EventStore, MigrationError, MigrationRejection, MigrationReport};
use rusqlite::types::Value;
use rusqlite::Connection;
use support::*;

fn text(value: &str) -> Value {
  Value::Text(value.into())
}

fn migrate(files: &Files, mapping: &HashMap<String, String>) -> MigrationReport {
  migrate_v3_sqlite(&files.old, &files.new, mapping).expect("移行の関数は失敗せずに報告を返す")
}

fn rejection_of<'a>(report: &'a MigrationReport, pkey: &str, skey: &str) -> Option<&'a MigrationRejection> {
  report
    .reasons
    .iter()
    .find(|rejection| rejection.pkey.as_deref() == Some(pkey) && rejection.skey.as_deref() == Some(skey))
}

fn application_objects(path: &Path) -> i64 {
  Connection::open(path)
    .expect("新ファイルを開ける")
    .query_row(
      "SELECT count(*) FROM sqlite_schema WHERE lower(name) NOT GLOB 'sqlite_*'",
      [],
      |row| row.get(0),
    )
    .expect("sqlite_schema を読める")
}

fn chain_contains(error: &(dyn std::error::Error + 'static), needle: &str) -> bool {
  let mut current = Some(error);
  while let Some(error) = current {
    if error.to_string().contains(needle) {
      return true;
    }
    current = error.source();
  }
  false
}

#[tokio::test]
async fn should_migrate_events_current_snapshot_and_newest_history_and_read_them_publicly() {
  let fixture = success_fixture();
  let files = prepare_files(&fixture.events, &fixture.snapshots, Some(1));
  let old_before = old_bytes(&files.old);

  let report = migrate(&files, &fixture.mapping);

  assert!(report.reasons.is_empty(), "{report:?}");
  assert_eq!((report.aggregates, report.events, report.snapshots), (2, 4, 3));
  assert_success_fixture_readable(&files.new, Some(1)).await;
  let rows = new_rows(&files.new);
  assert_eq!(
    rows.snapshot_history,
    vec![vec![
      text("Account-value-with-hyphen"),
      Value::Integer(2),
      text(""),
      Value::Blob(vec![255, 22])
    ]]
  );
  let heads: Vec<_> = rows
    .head
    .iter()
    .map(|row| (row[0].clone(), row[1].clone(), row[2].clone()))
    .collect();
  assert_eq!(
    heads,
    vec![
      (text("Account-value-with-hyphen"), text("Account"), Value::Integer(3)),
      (text("Other-second"), text("Other"), Value::Integer(1)),
    ]
  );
  assert_eq!(old_bytes(&files.old), old_before);
}

#[test]
fn should_build_the_head_from_the_envelope_with_the_largest_seq_nr() {
  let fixture = success_fixture();
  let files = prepare_files(&fixture.events, &fixture.snapshots, None);
  let old_before = old_bytes(&files.old);

  let report = migrate(&files, &fixture.mapping);

  assert!(report.reasons.is_empty(), "{report:?}");
  let rows = new_rows(&files.new);
  let head = rows
    .head
    .iter()
    .find(|row| row[0] == text("Account-value-with-hyphen"))
    .expect("集約 A の head がある");
  let largest = rows
    .journal
    .iter()
    .find(|row| row[0] == text("Account-value-with-hyphen") && row[1] == Value::Integer(3))
    .expect("集約 A の seq_nr 3 の journal がある");
  // head の列は (aid, type_name, seq_nr, 発生時刻, manifest, payload)。最大の seq_nr の journal の行と封筒の部分が等しい。
  assert_eq!(head[2..], largest[1..]);
  assert_eq!(head[3], Value::Integer(i64::MAX));
  assert_eq!(head[4], text("last-event"));
  assert_eq!(head[5], Value::Blob(vec![3, 0]));
  assert_eq!(old_bytes(&files.old), old_before);
}

#[tokio::test]
async fn should_migrate_without_history_when_the_new_store_keeps_none() {
  let fixture = success_fixture();
  let files = prepare_files(&fixture.events, &fixture.snapshots, None);
  let old_before = old_bytes(&files.old);

  let report = migrate(&files, &fixture.mapping);

  assert!(report.reasons.is_empty(), "{report:?}");
  assert_eq!((report.aggregates, report.events, report.snapshots), (2, 4, 2));
  let rows = new_rows(&files.new);
  assert!(rows.snapshot_history.is_empty());
  assert_eq!(rows.snapshot_current.len(), 2);
  assert_success_fixture_readable(&files.new, None).await;
  assert_eq!(old_bytes(&files.old), old_before);
}

#[test]
fn should_migrate_all_history_rows_when_the_new_store_keeps_that_many() {
  let fixture = success_fixture();
  let files = prepare_files(&fixture.events, &fixture.snapshots, Some(2));
  let old_before = old_bytes(&files.old);

  let report = migrate(&files, &fixture.mapping);

  assert!(report.reasons.is_empty(), "{report:?}");
  assert_eq!((report.aggregates, report.events, report.snapshots), (2, 4, 4));
  let history: Vec<_> = new_rows(&files.new)
    .snapshot_history
    .iter()
    .map(|row| (row[0].clone(), row[1].clone()))
    .collect();
  assert_eq!(
    history,
    vec![
      (text("Account-value-with-hyphen"), Value::Integer(1)),
      (text("Account-value-with-hyphen"), Value::Integer(2)),
    ]
  );
  assert_eq!(old_bytes(&files.old), old_before);
}

#[test]
fn should_reject_each_case_and_write_nothing() {
  for case in rejection_cases() {
    let files = prepare_files(&case.events, &case.snapshots, Some(1));
    let old_before = old_bytes(&files.old);

    let report = migrate(&files, &case.mapping);

    assert!(
      report
        .reasons
        .iter()
        .any(|rejection| rejection.reason.contains(case.expected_reason)),
      "{}: {} がない: {report:?}",
      case.name,
      case.expected_reason
    );
    assert_eq!(
      (report.aggregates, report.events, report.snapshots),
      (0, 0, 0),
      "{}",
      case.name
    );
    assert_nothing_written(&files, &old_before);
  }
}

/// 履歴の行（`skey` の末尾が 1 以上）が不正な場合の一覧。名前・期待する文言・不正な履歴の行を返す。
///
/// どの場合も、集約 `Hist`（イベント 1〜3、今のスナップショットの seq_nr 3）自体は正しい。
fn invalid_history_cases() -> Vec<(&'static str, &'static str, Vec<LegacySnapshot>)> {
  vec![
    (
      "最大のseq_nrを超える履歴",
      "未来snapshot",
      vec![LegacySnapshot::new("Hist-2", "Hist-a-5", 5)],
    ),
    (
      "キー番号とseq_nr列が違う履歴",
      "履歴キー番号とsnapshotのseq_nrが一致しません",
      vec![LegacySnapshot::new("Hist-2", "Hist-a-2", 1)],
    ),
    (
      "新しい履歴が正しいとき、古い履歴の旧パーティションが違う",
      "旧パーティションが一致しません",
      vec![
        LegacySnapshot::new("Hist-3", "Hist-a-1", 1),
        LegacySnapshot::new("Hist-2", "Hist-a-2", 2),
      ],
    ),
    (
      "イベントのない集約の履歴",
      "孤立snapshot",
      vec![LegacySnapshot::new("Orphan-9", "Orphan-a-1", 1)],
    ),
  ]
}

#[test]
fn should_reject_an_invalid_history_row_even_when_the_row_would_not_be_copied() {
  let events: Vec<_> = (1..=3)
    .map(|seq| LegacyEvent::new("Hist-2", &format!("Hist-a-{seq}"), seq))
    .collect();
  for (name, expected_reason, history) in invalid_history_cases() {
    // 保持件数なしでは履歴を 1 件も移さない。それでも不正な履歴の行は拒む。
    for keep in [None, Some(1), Some(2)] {
      let mut snapshots = vec![LegacySnapshot::new("Hist-2", "Hist-a-0", 3)];
      snapshots.extend(history.clone());
      let files = prepare_files(&events, &snapshots, keep);
      let old_before = old_bytes(&files.old);

      let report = migrate(&files, &HashMap::new());

      assert!(
        report
          .reasons
          .iter()
          .any(|rejection| rejection.reason.contains(expected_reason)),
        "{name} (保持件数 {keep:?}): {expected_reason} がない: {report:?}"
      );
      assert_eq!(
        (report.aggregates, report.events, report.snapshots),
        (0, 0, 0),
        "{name} (保持件数 {keep:?})"
      );
      assert_nothing_written(&files, &old_before);
    }
  }
}

#[tokio::test]
async fn should_keep_hyphens_in_the_value_when_parsing_the_default_layout_key() {
  let events = vec![
    LegacyEvent::new("Old-Account-7", "Old-Account-value-with-hyphen-1", 1).with_aid_column("caller representation")
  ];
  let files = prepare_files(&events, &[], None);
  let old_before = old_bytes(&files.old);

  let report = migrate(&files, &mapping());

  assert!(report.reasons.is_empty(), "{report:?}");
  let store = open_bytes_store(&files.new, None);
  let id = Id("Account".into(), "value-with-hyphen".into());
  let read = store.get_events_by_id_since_seq_nr(&id, 0).await.unwrap();
  assert_eq!(read.len(), 1);
  assert_eq!(read[0].aggregate_id(), &id);
  assert_eq!(read[0].seq_nr(), 1);
  let rows = new_rows(&files.new);
  assert_eq!(rows.head.len(), 1);
  assert_eq!(rows.head[0][0], text("Account-value-with-hyphen"));
  assert_eq!(rows.head[0][1], text("Account"));
  assert_eq!(old_bytes(&files.old), old_before);
}

#[test]
fn should_reject_each_non_default_key_layout_and_write_nothing() {
  let events = non_default_layout_events();
  let files = prepare_files(&events, &[], None);
  let old_before = old_bytes(&files.old);

  let report = migrate(&files, &HashMap::new());

  for event in &events {
    let rejection = rejection_of(&report, &event.pkey, &event.skey)
      .unwrap_or_else(|| panic!("{} / {} の拒否がない: {report:?}", event.pkey, event.skey));
    assert!(rejection.reason.contains("P-22"), "{rejection:?}");
  }
  assert_nothing_written(&files, &old_before);
}

#[test]
fn should_replace_a_hyphenated_old_type_name_with_the_mapping() {
  let events: Vec<_> = (1..=3)
    .map(|seq| LegacyEvent::new("Old-Account-7", &format!("Old-Account-value-with-hyphen-{seq}"), seq))
    .collect();
  let files = prepare_files(&events, &[], None);
  let old_before = old_bytes(&files.old);

  let report = migrate(&files, &mapping());

  assert!(report.reasons.is_empty(), "{report:?}");
  let rows = new_rows(&files.new);
  assert_eq!(rows.head.len(), 1);
  assert_eq!(rows.head[0][0], text("Account-value-with-hyphen"));
  assert_eq!(rows.head[0][2], Value::Integer(3));
  assert_eq!(old_bytes(&files.old), old_before);
}

#[test]
fn should_reject_a_mapped_aid_that_collides_with_an_existing_aid() {
  let events = vec![
    LegacyEvent::new("Account-1", "Account-a-1", 1),
    LegacyEvent::new("Old-Account-7", "Old-Account-a-1", 1),
  ];
  let files = prepare_files(&events, &[], None);
  let old_before = old_bytes(&files.old);

  let report = migrate(&files, &mapping());

  assert!(
    report
      .reasons
      .iter()
      .any(|rejection| rejection.reason.contains("複数の旧パーティション")),
    "{report:?}"
  );
  assert_nothing_written(&files, &old_before);
}

#[test]
fn should_accept_an_aid_of_exactly_1024_bytes() {
  let events = vec![LegacyEvent::new("-0", &format!("-{}-1", "あ".repeat(341)), 1)];
  let files = prepare_files(&events, &[], None);
  let old_before = old_bytes(&files.old);

  let report = migrate(&files, &HashMap::new());

  assert!(report.reasons.is_empty(), "{report:?}");
  let rows = new_rows(&files.new);
  assert_eq!(rows.journal.len(), 1);
  let Value::Text(aid) = &rows.journal[0][0] else {
    panic!("aid は TEXT である: {:?}", rows.journal[0][0]);
  };
  assert_eq!(aid.len(), 1024);
  assert_eq!(old_bytes(&files.old), old_before);
}

#[test]
fn should_reject_an_aid_over_1024_bytes_and_an_unmapped_old_type_name() {
  let events = vec![
    LegacyEvent::new("Long-1", &format!("Long-{}-1", "あ".repeat(340)), 1),
    LegacyEvent::new("Mapped-Old-2", "Mapped-Old-a-1", 1),
  ];
  let files = prepare_files(&events, &[], None);
  let old_before = old_bytes(&files.old);

  let report = migrate(&files, &HashMap::new());

  for expected in ["T-12", "P-23"] {
    assert!(
      report
        .reasons
        .iter()
        .any(|rejection| rejection.reason.contains(expected)),
      "{expected} がない: {report:?}"
    );
  }
  assert_nothing_written(&files, &old_before);
}

#[test]
fn should_reject_the_same_file_after_normalizing_the_path() {
  let directory = TempDirectory::new();
  let old = directory.file("old.sqlite3");
  create_v3_file(&old, &minimal_events(), &[]);
  fs::create_dir(directory.file("sub")).expect("下のディレクトリを作れる");
  let old_before = old_bytes(&old);

  // `Path` の比較は途中の `.` を無視するが、`..` とシンボリックリンクは解決しない。
  // どちらも `Path` としては `old` と異なるので、正規化してから比べないと拒めない。
  let mut paths = vec![directory.file("sub/../old.sqlite3")];
  #[cfg(unix)]
  {
    let link = directory.file("link.sqlite3");
    std::os::unix::fs::symlink(&old, &link).expect("シンボリックリンクを作れる");
    paths.push(link);
  }
  for same in paths {
    assert_ne!(same, old);
    let notation = same.display();

    let report = migrate_v3_sqlite(&old, &same, &mapping()).expect("同じファイルは拒否として報告される");

    assert_eq!(report.reasons.len(), 1, "{notation}: {report:?}");
    assert!(
      report.reasons[0].reason.contains("異なるファイル"),
      "{notation}: {report:?}"
    );
    assert_eq!((report.aggregates, report.events, report.snapshots), (0, 0, 0));
    assert_eq!(old_bytes(&old), old_before);
  }
}

#[test]
fn should_not_reject_distinct_files_as_the_same_file() {
  let files = prepare_files(&minimal_events(), &[], None);
  let old_before = old_bytes(&files.old);

  let report = migrate(&files, &mapping());

  assert!(report.reasons.is_empty(), "{report:?}");
  assert_eq!((report.aggregates, report.events, report.snapshots), (1, 1, 0));
  assert_eq!(old_bytes(&files.old), old_before);
}

#[test]
fn should_not_create_or_complete_an_uninitialized_new_file_when_the_path_does_not_exist() {
  let directory = TempDirectory::new();
  let old = directory.file("old.sqlite3");
  let missing = directory.file("missing.sqlite3");
  create_v3_file(&old, &minimal_events(), &[]);
  let old_before = old_bytes(&old);

  let result: Result<MigrationReport, MigrationError> = migrate_v3_sqlite(&old, &missing, &mapping());

  assert!(result.is_err(), "{result:?}");
  assert!(!missing.exists());
  assert_eq!(old_bytes(&old), old_before);
}

#[test]
fn should_not_create_or_complete_an_uninitialized_new_file_when_the_file_is_empty() {
  let directory = TempDirectory::new();
  let old = directory.file("old.sqlite3");
  let empty = directory.file("empty.sqlite3");
  create_v3_file(&old, &minimal_events(), &[]);
  fs::File::create(&empty).expect("空のファイルを作れる");
  let old_before = old_bytes(&old);

  let result = migrate_v3_sqlite(&old, &empty, &mapping());

  assert!(result.is_err(), "{result:?}");
  assert_eq!(application_objects(&empty), 0);
  assert_eq!(old_bytes(&old), old_before);
}

#[test]
fn should_reject_a_new_file_with_rows_in_each_table() {
  for table in NEW_TABLES {
    let files = prepare_files(&minimal_events(), &[], None);
    insert_new_row(&files.new, table);
    let occupied = new_rows(&files.new);
    let old_before = old_bytes(&files.old);

    let report = migrate(&files, &mapping());

    let expected_table = format!("new.{table}");
    assert!(
      report.reasons.iter().any(|rejection| rejection.table == expected_table),
      "{expected_table} の拒否がない: {report:?}"
    );
    assert_eq!(
      (report.aggregates, report.events, report.snapshots),
      (0, 0, 0),
      "{table}"
    );
    assert_eq!(new_rows(&files.new), occupied, "{table}");
    assert_eq!(old_bytes(&files.old), old_before, "{table}");
  }
}

#[test]
fn should_leave_no_partial_migration_when_a_write_fails() {
  let fixture = success_fixture();
  let files = prepare_files(&fixture.events, &fixture.snapshots, Some(1));
  // 2 つ目の head の書き込みで失敗させる。集約ごとに COMMIT する実装なら、1 つ目の集約が新ファイルに残る。
  Connection::open(&files.new)
    .expect("新ファイルを開ける")
    .execute_batch(
      "CREATE TRIGGER fail_second_head BEFORE INSERT ON head WHEN (SELECT count(*) FROM head) >= 1
       BEGIN SELECT RAISE(ABORT, 'injected'); END;",
    )
    .expect("失敗を差し込むトリガーを作れる");
  let old_before = old_bytes(&files.old);

  let error =
    migrate_v3_sqlite(&files.old, &files.new, &fixture.mapping).expect_err("書き込みの失敗は MigrationError で返る");

  assert!(chain_contains(&*error.source, "injected"), "{error:?}");
  assert_eq!(
    (error.report.aggregates, error.report.events, error.report.snapshots),
    (0, 0, 0)
  );
  assert_nothing_written(&files, &old_before);
}
