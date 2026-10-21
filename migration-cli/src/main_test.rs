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

fn bigtable_args() -> Vec<String> {
  [
    "--old-bigtable-project",
    "old-project",
    "--old-bigtable-instance",
    "old-instance",
    "--old-bigtable-journal",
    "old-journal",
    "--old-bigtable-snapshot",
    "old-snapshot",
    "--bigtable-project",
    "new-project",
    "--bigtable-instance",
    "new-instance",
    "--bigtable-journal",
    "new-journal",
    "--bigtable-head",
    "new-head",
  ]
  .map(String::from)
  .to_vec()
}

fn parse_bigtable(arguments: Vec<String>) -> BigtableArguments {
  let Some(Command::Bigtable(parsed)) = parse_command(arguments).unwrap() else {
    panic!("Bigtable の旗がある引数は Bigtable の移行へ回る");
  };
  parsed
}

#[test]
fn should_route_bigtable_arguments_and_pass_every_value() {
  let mut inputs = bigtable_args();
  inputs.extend(
    [
      "--bigtable-keep-snapshot-count",
      "3",
      "--bigtable-envelope-limit-bytes",
      "4096",
      "--bigtable-app-profile",
      "profile-1",
      "--type-mapping",
      "types.json",
    ]
    .map(String::from),
  );

  let parsed = parse_bigtable(inputs);

  assert_eq!(parsed.legacy.project_id, "old-project");
  assert_eq!(parsed.legacy.instance_id, "old-instance");
  assert_eq!(parsed.legacy.journal_table_id, "old-journal");
  assert_eq!(parsed.legacy.snapshot_table_id, "old-snapshot");
  assert_eq!(parsed.tables.project_id, "new-project");
  assert_eq!(parsed.tables.instance_id, "new-instance");
  assert_eq!(parsed.tables.journal_table_id, "new-journal");
  assert_eq!(parsed.tables.head_table_id, "new-head");
  assert_eq!(parsed.options.retention, RetentionSettings::keep_latest(3));
  assert_eq!(parsed.options.envelope_limit_bytes, 4096);
  assert_eq!(parsed.options.app_profile_id, "profile-1");
  assert_eq!(parsed.mapping_path.as_deref(), Some("types.json"));
}

#[test]
fn should_use_the_defaults_of_the_bigtable_store_options_when_they_are_omitted() {
  let parsed = parse_bigtable(bigtable_args());

  let defaults = BigtableOptions::default();
  assert_eq!(parsed.options.retention, defaults.retention);
  assert_eq!(parsed.options.envelope_limit_bytes, defaults.envelope_limit_bytes);
  assert_eq!(parsed.options.app_profile_id, defaults.app_profile_id);
  assert_eq!(parsed.options.change_feed, defaults.change_feed);
  assert_eq!(parsed.mapping_path, None);
}

#[test]
fn should_choose_the_emulator_endpoint_only_when_the_host_is_given_and_production_otherwise() {
  let production = parse_bigtable(bigtable_args());
  assert_eq!(production.endpoint, bigtable::BigtableEndpoint::Production);

  let mut inputs = bigtable_args();
  inputs.extend(["--bigtable-emulator-host", "127.0.0.1:8086"].map(String::from));
  let emulator = parse_bigtable(inputs);
  assert_eq!(
    emulator.endpoint,
    bigtable::BigtableEndpoint::Emulator("127.0.0.1:8086".to_string())
  );
}

#[test]
fn should_reject_bigtable_arguments_mixed_with_dynamodb_or_sqlite_arguments() {
  for (flag, value) in [
    ("--old-journal", "old-events"),
    ("--old-snapshot", "old-state"),
    ("--journal", "new-events"),
    ("--snapshot", "new-state"),
    ("--head", "new-head"),
    ("--history-index", "new-history"),
    ("--endpoint-url", "http://127.0.0.1:8000"),
    ("--region", "us-west-1"),
    ("--old-sqlite", "old.sqlite3"),
    ("--sqlite", "new.sqlite3"),
  ] {
    let mut inputs = bigtable_args();
    inputs.extend([flag, value].map(String::from));
    assert!(
      parse_command(inputs).is_err(),
      "{flag} を Bigtable の旗と混ぜたのに受け付けた"
    );
  }
  let mut with_dynamodb = args();
  with_dynamodb.extend(bigtable_args());
  assert!(parse_command(with_dynamodb).is_err());
  let mut with_sqlite = sqlite_args();
  with_sqlite.extend(bigtable_args());
  assert!(parse_command(with_sqlite).is_err());
}

#[test]
fn should_reject_missing_duplicate_and_invalid_bigtable_arguments() {
  // 必須の旗が 1 つ欠けても受け付けない。
  let required = bigtable_args();
  for index in (0..required.len()).step_by(2) {
    let mut inputs = required.clone();
    let removed: Vec<String> = inputs.drain(index..index + 2).collect();
    assert!(parse_command(inputs).is_err(), "{} がないのに受け付けた", removed[0]);
  }
  assert!(parse_command(vec!["--bigtable-project".into(), "new-project".into()]).is_err());
  // 同じ旗の重複と、値のない旗は受け付けない。
  for index in (0..required.len()).step_by(2) {
    let mut inputs = required.clone();
    inputs.extend([required[index].clone(), "again".to_string()]);
    assert!(parse_command(inputs).is_err(), "{} の重複を受け付けた", required[index]);
  }
  let mut without_value = bigtable_args();
  without_value.push("--bigtable-app-profile".into());
  assert!(parse_command(without_value).is_err());
  // 数でない値と、未対応の旗は受け付けない。
  for (flag, value) in [
    ("--bigtable-keep-snapshot-count", "abc"),
    ("--bigtable-keep-snapshot-count", "-1"),
    ("--bigtable-envelope-limit-bytes", "abc"),
    ("--bigtable-envelope-limit-bytes", "-1"),
    ("--bigtable-unknown", "x"),
  ] {
    let mut inputs = bigtable_args();
    inputs.extend([flag, value].map(String::from));
    assert!(parse_command(inputs).is_err(), "{flag} {value} を受け付けた");
  }
}
