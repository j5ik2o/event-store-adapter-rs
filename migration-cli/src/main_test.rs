use super::*;

fn args() -> Vec<String> {
  [
    "--old-journal",
    "old-events",
    "--old-snapshot",
    "old-state",
    "--journal",
    "new-events",
    "--snapshot",
    "new-state",
    "--head",
    "new-head",
    "--history-index",
    "new-history",
  ]
  .map(String::from)
  .to_vec()
}

#[test]
fn should_pass_all_table_names_and_optional_client_inputs() {
  let mut inputs = args();
  inputs.extend(
    [
      "--type-mapping",
      "types.json",
      "--endpoint-url",
      "http://127.0.0.1:8000",
      "--region",
      "us-west-1",
    ]
    .map(String::from),
  );
  let parsed = Arguments::parse(inputs).unwrap().unwrap();
  assert_eq!(parsed.legacy.journal_table_name, "old-events");
  assert_eq!(parsed.legacy.snapshot_table_name, "old-state");
  assert_eq!(parsed.tables.journal_table_name, "new-events");
  assert_eq!(parsed.tables.snapshot_table_name, "new-state");
  assert_eq!(parsed.tables.head_table_name, "new-head");
  assert_eq!(parsed.tables.snapshot_history_index_name, "new-history");
  assert_eq!(parsed.mapping_path.as_deref(), Some("types.json"));
  assert_eq!(parsed.endpoint.as_deref(), Some("http://127.0.0.1:8000"));
  assert_eq!(parsed.region.as_deref(), Some("us-west-1"));
}

#[test]
fn should_reject_missing_duplicate_and_unknown_arguments() {
  assert!(Arguments::parse(Vec::new()).is_err());
  for additional in [vec!["--head", "again"], vec!["--journal"], vec!["--other", "x"]] {
    let mut inputs = args();
    inputs.extend(additional.into_iter().map(String::from));
    assert!(Arguments::parse(inputs).is_err());
  }
  assert!(Arguments::parse(["--help".into()]).unwrap().is_none());
}
