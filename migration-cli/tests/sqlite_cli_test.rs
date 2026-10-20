#[path = "../../lib/tests/sqlite_migration_test/support.rs"]
mod support;

use std::collections::HashMap;
use std::ffi::OsString;
use std::fs;
use std::path::PathBuf;
use std::process::{Command, Output};

use event_store_adapter_rs::MigrationReport;
use support::*;

fn run(arguments: Vec<OsString>) -> Output {
  Command::new(env!("CARGO_BIN_EXE_event-store-adapter-migration-rs"))
    .args(arguments)
    .output()
    .expect("実際の実行ファイルを起動できる")
}

fn sqlite_arguments(files: &Files) -> Vec<OsString> {
  vec![
    "--old-sqlite".into(),
    files.old.clone().into_os_string(),
    "--sqlite".into(),
    files.new.clone().into_os_string(),
  ]
}

/// 旧型名の対応表の JSON を、旧ファイルと同じ一時ディレクトリに書く。
fn write_mapping(files: &Files, mapping: &HashMap<String, String>) -> PathBuf {
  let path = files.old.with_file_name("types.json");
  fs::write(&path, serde_json::to_vec(mapping).expect("対応表を JSON にできる")).expect("対応表を書ける");
  path
}

fn with_mapping(mut arguments: Vec<OsString>, mapping: PathBuf) -> Vec<OsString> {
  arguments.extend(["--type-mapping".into(), mapping.into_os_string()]);
  arguments
}

fn report_of(output: &Output) -> MigrationReport {
  serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
    panic!(
      "標準出力が MigrationReport の JSON ではない: {error}: {}",
      String::from_utf8_lossy(&output.stdout)
    )
  })
}

fn stderr_of(output: &Output) -> String {
  String::from_utf8_lossy(&output.stderr).into_owned()
}

#[tokio::test]
async fn should_migrate_sqlite_with_the_actual_cli_and_read_publicly() {
  let fixture = success_fixture();
  let files = prepare_files(&fixture.events, &fixture.snapshots, Some(1));
  let mapping = write_mapping(&files, &fixture.mapping);
  let old_before = old_bytes(&files.old);

  let output = run(with_mapping(sqlite_arguments(&files), mapping));

  assert!(output.status.success(), "{}", stderr_of(&output));
  let report = report_of(&output);
  assert!(report.reasons.is_empty(), "{report:?}");
  assert_eq!((report.aggregates, report.events, report.snapshots), (2, 4, 3));
  assert_success_fixture_readable(&files.new, Some(1)).await;
  assert_eq!(old_bytes(&files.old), old_before);
}

#[test]
fn should_exit_nonzero_and_write_nothing_for_each_rejection_case() {
  for case in rejection_cases() {
    let files = prepare_files(&case.events, &case.snapshots, Some(1));
    let mapping = write_mapping(&files, &case.mapping);
    let old_before = old_bytes(&files.old);

    let output = run(with_mapping(sqlite_arguments(&files), mapping));

    assert!(!output.status.success(), "{}: 拒否なのに終了コードが 0", case.name);
    let report = report_of(&output);
    assert!(
      report
        .reasons
        .iter()
        .any(|rejection| rejection.reason.contains(case.expected_reason)),
      "{}: {} がない: {report:?}",
      case.name,
      case.expected_reason
    );
    assert_nothing_written(&files, &old_before);
  }
}

#[test]
fn should_exit_nonzero_and_write_nothing_for_a_new_file_with_rows() {
  for table in NEW_TABLES {
    let files = prepare_files(&minimal_events(), &[], None);
    insert_new_row(&files.new, table);
    let occupied = new_rows(&files.new);
    let mapping_file = write_mapping(&files, &mapping());
    let old_before = old_bytes(&files.old);

    let output = run(with_mapping(sqlite_arguments(&files), mapping_file));

    assert!(!output.status.success(), "{table}: 拒否なのに終了コードが 0");
    let report = report_of(&output);
    let expected_table = format!("new.{table}");
    assert!(
      report.reasons.iter().any(|rejection| rejection.table == expected_table),
      "{expected_table} の拒否がない: {report:?}"
    );
    assert_eq!(new_rows(&files.new), occupied, "{table}");
    assert_eq!(old_bytes(&files.old), old_before, "{table}");
  }
}

#[test]
fn should_exit_nonzero_for_the_same_file() {
  let directory = TempDirectory::new();
  let old = directory.file("old.sqlite3");
  create_v3_file(&old, &minimal_events(), &[]);
  let old_before = old_bytes(&old);

  let output = run(vec![
    "--old-sqlite".into(),
    old.clone().into_os_string(),
    "--sqlite".into(),
    directory.file("./old.sqlite3").into_os_string(),
  ]);

  assert!(!output.status.success());
  let report = report_of(&output);
  assert!(
    report
      .reasons
      .iter()
      .any(|rejection| rejection.reason.contains("異なるファイル")),
    "{report:?}"
  );
  assert_eq!(old_bytes(&old), old_before);
}

#[test]
fn should_exit_nonzero_with_the_report_on_a_storage_failure() {
  let files = prepare_files(&minimal_events(), &[], None);
  fs::remove_file(&files.old).expect("旧ファイルを消せる");

  let output = run(sqlite_arguments(&files));

  assert!(!output.status.success());
  let report = report_of(&output);
  assert_eq!((report.aggregates, report.events, report.snapshots), (0, 0, 0));
  // 共有の `MigrationError` の表示は、保存先の名前を含まない。SQLite の失敗を DynamoDB の失敗と表示しない。
  assert!(
    stderr_of(&output).contains("migration failed during"),
    "{}",
    stderr_of(&output)
  );
  assert!(!stderr_of(&output).contains("DynamoDB"), "{}", stderr_of(&output));
  assert_eq!(new_rows(&files.new), NewRows::default());
}

#[test]
fn should_accept_sqlite_flags_together_with_the_type_mapping_flag() {
  let files = prepare_files(&minimal_events(), &[], None);
  let mapping_file = write_mapping(&files, &mapping());

  let output = run(with_mapping(sqlite_arguments(&files), mapping_file));

  assert!(output.status.success(), "{}", stderr_of(&output));
  let report = report_of(&output);
  assert!(report.reasons.is_empty(), "{report:?}");
  assert_eq!((report.aggregates, report.events, report.snapshots), (1, 1, 0));
}

#[test]
fn should_reject_mixed_arguments_before_migration() {
  let files = prepare_files(&minimal_events(), &[], None);
  let mapping_file = write_mapping(&files, &mapping());
  let old_before = old_bytes(&files.old);
  let mut arguments = with_mapping(sqlite_arguments(&files), mapping_file);
  arguments.extend(["--journal".into(), "events".into()]);

  let output = run(arguments);

  assert!(!output.status.success());
  assert!(output.stdout.is_empty(), "{}", String::from_utf8_lossy(&output.stdout));
  assert_nothing_written(&files, &old_before);
}

#[test]
fn should_show_sqlite_usage_and_preconditions_in_help() {
  let output = run(vec!["--help".into()]);

  assert!(output.status.success());
  let help = String::from_utf8_lossy(&output.stdout);
  for expected in [
    "--old-sqlite",
    "旧ファイルへの書込を停止",
    "initialize_sqlite_store",
    "旧2表への書込を停止",
  ] {
    assert!(help.contains(expected), "--help に {expected} がない: {help}");
  }
}
