#[path = "../../lib/tests/dynamodb_migration_test/support.rs"]
mod support;

use event_store_adapter_rs::migration::MigrationReport;
use serde_json::json;
use std::process::{Command, Output};
use support::*;

fn execute(fixture: &Fixture, mapping: &std::path::Path) -> Output {
  Command::new(env!("CARGO_BIN_EXE_event-store-adapter-migration-rs"))
    .args([
      "--old-journal",
      &fixture.legacy.journal_table_name,
      "--old-snapshot",
      &fixture.legacy.snapshot_table_name,
      "--journal",
      &fixture.tables.journal_table_name,
      "--snapshot",
      &fixture.tables.snapshot_table_name,
      "--head",
      &fixture.tables.head_table_name,
      "--history-index",
      &fixture.tables.snapshot_history_index_name,
      "--endpoint-url",
      &fixture.endpoint,
      "--region",
      "us-west-1",
      "--type-mapping",
    ])
    .arg(mapping)
    .env("AWS_ACCESS_KEY_ID", "x")
    .env("AWS_SECRET_ACCESS_KEY", "x")
    .env("AWS_EC2_METADATA_DISABLED", "true")
    .env_remove("AWS_PROFILE")
    .env_remove("AWS_SESSION_TOKEN")
    .output()
    .unwrap()
}

#[tokio::test]
async fn should_run_the_actual_cli_into_migration_storage_and_public_reads() {
  let help = Command::new(env!("CARGO_BIN_EXE_event-store-adapter-migration-rs"))
    .arg("--help")
    .output()
    .unwrap();
  assert!(help.status.success());
  assert!(String::from_utf8(help.stdout).unwrap().contains("旧2表への書込を停止"));
  let mut fixture = Fixture::new().await;
  let events = vec![
    raw_event("Old-Account-7", "Old-Account-a-b-1", "1", vec![255, 0, 128], None),
    raw_event(
      "Old-Account-7",
      "Old-Account-a-b-2",
      "2",
      vec![128, 7],
      Some("cli-event"),
    ),
  ];
  let snapshots = vec![raw_snapshot(
    "Old-Account-7",
    "Old-Account-a-b-0",
    "1",
    vec![128, 255],
    "0",
  )];
  fixture.seed(&events, &snapshots).await;
  let old = fixture.old_items().await;
  let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
    .join("../target/migration-cli-tests")
    .join(std::process::id().to_string())
    .join("type-mapping.json");
  std::fs::create_dir_all(path.parent().unwrap()).unwrap();
  std::fs::write(&path, br#"{"Old-Account":"Account"}"#).unwrap();
  let output = execute(&fixture, &path);
  assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
  assert!(String::from_utf8_lossy(&output.stderr).contains("旧2表への書込を停止"));
  let report: MigrationReport = serde_json::from_slice(&output.stdout).unwrap();
  assert_eq!((report.events, report.snapshots, report.aggregates), (2, 1, 1));
  assert!(report.reasons.is_empty());
  let item = get(
    &fixture.raw,
    &fixture.tables.journal_table_name,
    "Account-a-b",
    Some(("seq_nr", "1")),
  )
  .await;
  assert_shapes(
    &item,
    &[
      ("aid", "S"),
      ("seq_nr", "N"),
      ("manifest", "S"),
      ("occurred_at", "N"),
      ("payload", "B"),
    ],
  );
  let observed = Observed::new(&fixture.endpoint, &fixture.raw);
  fixture.raw = observed.client.clone();
  let store = fixture.open().await;
  let read = store
    .get_events_by_id_since_seq_nr(&Id("Account".into(), "a-b".into()), 0)
    .await
    .unwrap();
  assert_eq!(read.len(), 2);
  assert_eq!(read[0].payload(), &vec![255, 0, 128]);
  assert_eq!(read[0].manifest(), "");
  assert_eq!(read[1].seq_nr(), 2);
  assert_eq!(read[1].manifest(), "cli-event");
  assert_eq!(read[1].payload(), &vec![128, 7]);
  assert_eq!(read[0].occurred_at().timestamp_nanos_opt(), Some(-876543211));
  let snapshot = store
    .get_latest_snapshot_by_id(&Id("Account".into(), "a-b".into()))
    .await
    .unwrap()
    .unwrap();
  assert_eq!(snapshot.head_seq_nr(), 2);
  assert_eq!(snapshot.snapshot().unwrap().seq_nr(), 1);
  assert_eq!(snapshot.snapshot().unwrap().aggregate(), &vec![128, 255]);
  assert_eq!(fixture.old_items().await, old);
  assert!(observed.install.lock().unwrap().is_none());
  let new_journal = scan(&fixture.raw, &fixture.tables.journal_table_name).await;
  let new_snapshot = scan(&fixture.raw, &fixture.tables.snapshot_table_name).await;
  let new_head = scan(&fixture.raw, &fixture.tables.head_table_name).await;
  let rerun = execute(&fixture, &path);
  assert!(!rerun.status.success());
  let rejected: MigrationReport = serde_json::from_slice(&rerun.stdout).unwrap();
  assert!(!rejected.reasons.is_empty());
  assert_eq!(rejected.events, 0);
  assert_eq!(
    scan(&fixture.raw, &fixture.tables.journal_table_name).await,
    new_journal
  );
  assert_eq!(
    scan(&fixture.raw, &fixture.tables.snapshot_table_name).await,
    new_snapshot
  );
  assert_eq!(scan(&fixture.raw, &fixture.tables.head_table_name).await, new_head);
  evidence(
    "migration-cli",
    json!({"original_old_journal":events.iter().map(wire).collect::<Vec<_>>(),"original_old_snapshot":snapshots.iter().map(wire).collect::<Vec<_>>(),"report":report,"actual_new_journal":new_journal,"actual_new_snapshot":new_snapshot,"actual_new_head":new_head,"public_read_seq_nrs":read.iter().map(|event|event.seq_nr()).collect::<Vec<_>>(),"public_read_requests_and_responses":observed.traces.lock().unwrap().clone(),"rerun_report":rejected,"old_unchanged":true}),
  );
}
