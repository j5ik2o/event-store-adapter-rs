//! 移行の道具の実行ファイルを実際に起動して、Bigtable の移行をエミュレーターで確かめる。
//!
//! 本番の Cloud Bigtable（Application Default Credentials）へは接続しない。本番の経路が選ばれることは、引数の解析の
//! 単体試験（`src/main_test.rs`）で確かめる。

#[path = "../../lib/tests/bigtable_support/mod.rs"]
mod bigtable_support;
#[path = "../../lib/tests/bigtable_migration_test/support.rs"]
mod support;

use std::collections::HashMap;
use std::ffi::OsString;
use std::fs;
use std::path::PathBuf;
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};

use bigtable_support::{options_keeping, Fixture};
use event_store_adapter_rs::{BigtableOptions, MigrationReport};
use support::*;

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// 破棄時に消える、旧型名の対応表の JSON ファイル。
struct MappingFile(PathBuf);

impl MappingFile {
  fn new(mapping: &HashMap<String, String>) -> Self {
    let path = std::env::temp_dir().join(format!(
      "event-store-adapter-bigtable-cli-{}-{}.json",
      std::process::id(),
      SEQUENCE.fetch_add(1, Ordering::SeqCst)
    ));
    fs::write(&path, serde_json::to_vec(mapping).expect("対応表を JSON にできる")).expect("対応表を書ける");
    Self(path)
  }
}

impl Drop for MappingFile {
  fn drop(&mut self) {
    // 後片付けの失敗は試験の結果を変えないので、握りつぶす。
    let _ = fs::remove_file(&self.0);
  }
}

fn run(arguments: Vec<OsString>) -> Output {
  Command::new(env!("CARGO_BIN_EXE_event-store-adapter-migration-rs"))
    .args(arguments)
    .output()
    .expect("実際の実行ファイルを起動できる")
}

/// 旧と新のテーブルと、エミュレーターの接続先を指す引数。新しいストアの設定は `options` のとおりに渡す。
fn bigtable_arguments(scene: &Scene) -> Vec<OsString> {
  let legacy = &scene.legacy;
  let tables = &scene.env.tables;
  let emulator = format!("127.0.0.1:{}", scene.env.port);
  let mut arguments: Vec<OsString> = vec![];
  for (flag, value) in [
    ("--old-bigtable-project", &legacy.project_id),
    ("--old-bigtable-instance", &legacy.instance_id),
    ("--old-bigtable-journal", &legacy.journal_table_id),
    ("--old-bigtable-snapshot", &legacy.snapshot_table_id),
    ("--bigtable-project", &tables.project_id),
    ("--bigtable-instance", &tables.instance_id),
    ("--bigtable-journal", &tables.journal_table_id),
    ("--bigtable-head", &tables.head_table_id),
    ("--bigtable-emulator-host", &emulator),
  ] {
    arguments.extend([flag.into(), value.into()]);
  }
  arguments.extend([
    "--bigtable-envelope-limit-bytes".into(),
    scene.options.envelope_limit_bytes.to_string().into(),
  ]);
  if let Some(count) = scene.options.retention.keep_snapshot_count() {
    arguments.extend(["--bigtable-keep-snapshot-count".into(), count.to_string().into()]);
  }
  arguments
}

fn with_mapping(mut arguments: Vec<OsString>, mapping: &MappingFile) -> Vec<OsString> {
  arguments.extend(["--type-mapping".into(), mapping.0.clone().into_os_string()]);
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
async fn should_migrate_bigtable_with_the_actual_cli_on_the_emulator_and_read_publicly() {
  let fixture = success_fixture();
  // 保持件数・封筒の上限が、新しいテーブルの設定行の記録と同じ値で引数から渡されたときだけ、照合を通る。
  let options = BigtableOptions {
    envelope_limit_bytes: 4096,
    ..options_keeping(2)
  };
  let scene = Scene::new(&fixture.events, &fixture.snapshots, options).await;
  let mapping = MappingFile::new(&fixture.mapping);
  let before = scene.state().await;

  // エミュレーターはどの app profile の ID も受け付ける。旗が受理され、移行が成功することだけを確かめる。
  let mut arguments = with_mapping(bigtable_arguments(&scene), &mapping);
  arguments.extend(["--bigtable-app-profile".into(), "migration-profile".into()]);

  let output = run(arguments);

  assert!(output.status.success(), "{}", stderr_of(&output));
  let report = report_of(&output);
  assert!(report.reasons.is_empty(), "{report:?}");
  assert_eq!((report.aggregates, report.events, report.snapshots), (3, 7, 3));
  assert_success_fixture_readable(&scene).await;
  assert_old_tables_unchanged(&before, &scene.state().await);
}

#[tokio::test]
async fn should_exit_nonzero_and_write_nothing_for_each_bigtable_rejection_case() {
  for case in rejection_cases() {
    for keep in &case.keeps {
      let options = keep.map_or_else(BigtableOptions::default, options_keeping);
      let scene = Scene::new(&case.events, &case.snapshots, options).await;
      let mapping = MappingFile::new(&case.mapping);
      let before = scene.state().await;
      let context = format!("{} (保持件数 {keep:?})", case.name);

      let output = run(with_mapping(bigtable_arguments(&scene), &mapping));

      assert!(!output.status.success(), "{context}: 拒否なのに終了コードが 0");
      let report = report_of(&output);
      assert_reasons_contain(&report, &case.expected_reasons, &context);
      assert_rejected_and_nothing_written(&scene, &before, &report).await;
    }
  }
}

#[tokio::test]
async fn should_exit_nonzero_with_the_report_on_a_bigtable_configuration_failure() {
  let events = vec![V3Event::new("Good-0", "Good", "a", 1)];
  // 新しいテーブルを初期化していない。移行は設定行を作らず、MigrationError で失敗する。
  let scene = Scene::over(Fixture::new().await, BigtableOptions::default(), &events, &[]).await;
  let before = scene.state().await;

  let output = run(bigtable_arguments(&scene));

  assert!(!output.status.success());
  let report = report_of(&output);
  assert_eq!((report.aggregates, report.events, report.snapshots), (0, 0, 0));
  // 共有の `MigrationError` の表示は、保存先の名前を含まない。
  assert!(
    stderr_of(&output).contains("migration failed during"),
    "{}",
    stderr_of(&output)
  );
  assert_eq!(scene.state().await, before, "設定行を作らず、何も書かない");
}

#[tokio::test]
async fn should_reject_bigtable_arguments_mixed_with_other_backends_before_migration() {
  let scene = Scene::new(
    &[V3Event::new("Good-0", "Good", "a", 1)],
    &[],
    BigtableOptions::default(),
  )
  .await;
  let before = scene.state().await;

  for (flag, value) in [
    ("--journal", "events"),
    ("--region", "us-west-1"),
    ("--old-sqlite", "old.sqlite3"),
    ("--sqlite", "new.sqlite3"),
  ] {
    let mut arguments = bigtable_arguments(&scene);
    arguments.extend([flag.into(), value.into()]);

    let output = run(arguments);

    assert!(!output.status.success(), "{flag} を混ぜたのに終了コードが 0");
    assert!(
      output.stdout.is_empty(),
      "{flag}: {}",
      String::from_utf8_lossy(&output.stdout)
    );
    assert_nothing_written(&scene, &before).await;
  }
}

#[test]
fn should_show_bigtable_usage_and_preconditions_in_help() {
  let output = run(vec!["--help".into()]);

  assert!(output.status.success());
  let help = String::from_utf8_lossy(&output.stdout);
  for expected in [
    "--old-bigtable-journal",
    "--bigtable-emulator-host",
    "旧2テーブルへの書き手を止めて",
    "initialize_bigtable_store",
    "Application Default Credentials",
  ] {
    assert!(help.contains(expected), "--help に {expected} がない: {help}");
  }
}
