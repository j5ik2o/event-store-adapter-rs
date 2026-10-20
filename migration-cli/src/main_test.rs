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

fn sqlite_args() -> Vec<String> {
  ["--old-sqlite", "old.sqlite3", "--sqlite", "new.sqlite3"]
    .map(String::from)
    .to_vec()
}

#[test]
fn should_route_sqlite_arguments_and_keep_dynamodb_parsing() {
  let Some(Command::Sqlite(sqlite)) = parse_command(sqlite_args()).unwrap() else {
    panic!("SQLite の旗だけの引数は SQLite の移行へ回る");
  };
  assert_eq!(sqlite.legacy_path, "old.sqlite3");
  assert_eq!(sqlite.path, "new.sqlite3");
  assert_eq!(sqlite.mapping_path, None);

  let mut with_mapping = sqlite_args();
  with_mapping.extend(["--type-mapping", "types.json"].map(String::from));
  let Some(Command::Sqlite(sqlite)) = parse_command(with_mapping).unwrap() else {
    panic!("共通の --type-mapping は SQLite の旗と組み合わせられる");
  };
  assert_eq!(sqlite.mapping_path.as_deref(), Some("types.json"));

  let Some(Command::DynamoDb(dynamodb)) = parse_command(args()).unwrap() else {
    panic!("SQLite の旗がなければ従来の DynamoDB の解析へ回る");
  };
  assert_eq!(dynamodb.legacy.journal_table_name, "old-events");
  assert_eq!(dynamodb.legacy.snapshot_table_name, "old-state");
  assert_eq!(dynamodb.tables.journal_table_name, "new-events");
  assert_eq!(dynamodb.tables.snapshot_table_name, "new-state");
  assert_eq!(dynamodb.tables.head_table_name, "new-head");
  assert_eq!(dynamodb.tables.snapshot_history_index_name, "new-history");
  assert_eq!(dynamodb.mapping_path, None);

  assert_eq!(
    parse_command(vec!["--other".into(), "x".into()]).err().as_deref(),
    Some("未対応の引数: --other")
  );
  assert!(parse_command(vec!["--help".into()]).unwrap().is_none());
  assert!(parse_command(vec!["-h".into()]).unwrap().is_none());
}

#[test]
fn should_reject_mixed_sqlite_and_dynamodb_arguments() {
  for (flag, value) in [
    ("--old-journal", "old-events"),
    ("--old-snapshot", "old-state"),
    ("--journal", "new-events"),
    ("--snapshot", "new-state"),
    ("--head", "new-head"),
    ("--history-index", "new-history"),
    ("--endpoint-url", "http://127.0.0.1:8000"),
    ("--region", "us-west-1"),
  ] {
    let mut inputs = sqlite_args();
    inputs.extend([flag, value].map(String::from));
    assert!(
      parse_command(inputs).is_err(),
      "{flag} を SQLite の旗と混ぜたのに受け付けた"
    );
  }
  let mut all = args();
  all.extend(sqlite_args());
  assert!(parse_command(all).is_err());
}
