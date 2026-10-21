mod bigtable;

use std::collections::HashMap;
use std::process::ExitCode;

use aws_sdk_dynamodb::config::Region;
use event_store_adapter_rs::DynamoDbTables;
use event_store_adapter_rs::{
  migrate_v3_bigtable, migrate_v3_dynamodb, migrate_v3_sqlite, BigtableOptions, BigtableTables, LegacyBigtableTables,
  LegacyDynamoDbTables, RetentionSettings,
};

const PRECONDITION: &str = "運用前提: 実行前に旧2表への書込を停止してください。このツールは書込停止を強制しません。";
const HELP: &str = "v3既定DynamoDB配置を全件検査して新版へ移行します。
Usage: event-store-adapter-migration-rs --old-journal NAME --old-snapshot NAME
  --journal NAME --snapshot NAME --head NAME --history-index NAME
  [--type-mapping FILE.json] [--endpoint-url URL] [--region REGION]
新3表と履歴GSIを事前作成してください。途中失敗後は新3表を作り直して再実行してください。
型対応表は旧型名から新型名へのJSONオブジェクトです。認証は通常のAWS設定を使用します。";
const SQLITE_PRECONDITION: &str =
  "運用前提: 実行前に旧ファイルへの書込を停止してください。このツールは書込停止を強制しません。";
const SQLITE_HELP: &str = "v3既定SQLite配置を全件検査して新版のSQLiteファイルへ移行します。
Usage: event-store-adapter-migration-rs --old-sqlite FILE --sqlite FILE [--type-mapping FILE.json]
新ファイルは事前に initialize_sqlite_store で初期化し、空にしてください。このツールは新ファイルのテーブルも設定も作りません。
検査拒否と途中失敗では新ファイルに何も残らないので、同じ新ファイルで再実行できます。
型対応表は旧型名から新型名へのJSONオブジェクトです。SQLiteの引数とDynamoDBの引数は同時に指定できません。";

const BIGTABLE_PRECONDITION: &str =
  "運用前提: 実行前に旧2テーブルへの書き手を止めてください。このツールは書込停止を強制しません。";
const BIGTABLE_HELP: &str = "v3既定Bigtable配置を全件検査して新版のBigtableへ移行します。
Usage: event-store-adapter-migration-rs --old-bigtable-project ID --old-bigtable-instance ID
  --old-bigtable-journal ID --old-bigtable-snapshot ID
  --bigtable-project ID --bigtable-instance ID --bigtable-journal ID --bigtable-head ID
  [--bigtable-keep-snapshot-count N] [--bigtable-envelope-limit-bytes N] [--bigtable-app-profile ID]
  [--bigtable-emulator-host HOST:PORT] [--type-mapping FILE.json]
新2テーブルは列ファミリ付きで事前に作成し、initialize_bigtable_store で初期化して、データの行がない状態にしてください。
このツールは新2テーブルのテーブルも列ファミリも設定行も作りません。
保持件数・封筒の上限・app profileは、新2テーブルの初期化に使った設定と同じ値を指定します（省略時は保持なし・1048576バイト・インスタンスの既定）。
--bigtable-emulator-host を指定するとエミュレーターへ平文・認証なしで接続します。指定しなければ本番のCloud Bigtableへ接続し、
Application Default Credentials でトークンを取得・更新します（通常のGoogleの認証情報の探索に従います）。
検査拒否では新2テーブルに何も書きません。途中失敗後は新2テーブルを作り直して初期化し直し、同じ引数で再実行してください。
型対応表は旧型名から新型名へのJSONオブジェクトです。Bigtableの引数とDynamoDB・SQLiteの引数は同時に指定できません。";

/// Bigtable の移行を選ぶ旗。1 つでもあれば Bigtable の移行として解析する。`--type-mapping` は 3 つの移行が共有するので含めない。
const BIGTABLE_FLAGS: [&str; 12] = [
  "--old-bigtable-project",
  "--old-bigtable-instance",
  "--old-bigtable-journal",
  "--old-bigtable-snapshot",
  "--bigtable-project",
  "--bigtable-instance",
  "--bigtable-journal",
  "--bigtable-head",
  "--bigtable-keep-snapshot-count",
  "--bigtable-envelope-limit-bytes",
  "--bigtable-app-profile",
  "--bigtable-emulator-host",
];

/// SQLite の移行を選ぶ旗。1 つでもあれば SQLite の移行として解析する。
const SQLITE_FLAGS: [&str; 2] = ["--old-sqlite", "--sqlite"];
/// DynamoDB の移行だけが使う旗。SQLite の旗と混ぜると、移行の前に引数の誤りにする。
const DYNAMODB_ONLY_FLAGS: [&str; 8] = [
  "--old-journal",
  "--old-snapshot",
  "--journal",
  "--snapshot",
  "--head",
  "--history-index",
  "--endpoint-url",
  "--region",
];

struct Arguments {
  legacy: LegacyDynamoDbTables,
  tables: DynamoDbTables,
  mapping_path: Option<String>,
  endpoint: Option<String>,
  region: Option<String>,
}

impl Arguments {
  fn parse(arguments: impl IntoIterator<Item = String>) -> Result<Option<Self>, String> {
    let mut args = arguments.into_iter();
    let mut options = HashMap::new();
    while let Some(name) = args.next() {
      if name == "--help" || name == "-h" {
        return Ok(None);
      }
      if ![
        "--old-journal",
        "--old-snapshot",
        "--journal",
        "--snapshot",
        "--head",
        "--history-index",
        "--type-mapping",
        "--endpoint-url",
        "--region",
      ]
      .contains(&name.as_str())
      {
        return Err(format!("未対応の引数: {name}"));
      }
      let value = args
        .next()
        .filter(|value| !value.starts_with("--") && !value.is_empty())
        .ok_or_else(|| format!("{name}には値が必要です"))?;
      if options.insert(name.clone(), value).is_some() {
        return Err(format!("引数の重複: {name}"));
      }
    }
    let mut required = |name: &str| options.remove(name).ok_or_else(|| format!("必須引数: {name}"));
    let legacy = LegacyDynamoDbTables {
      journal_table_name: required("--old-journal")?,
      snapshot_table_name: required("--old-snapshot")?,
    };
    let tables = DynamoDbTables {
      journal_table_name: required("--journal")?,
      snapshot_table_name: required("--snapshot")?,
      head_table_name: required("--head")?,
      snapshot_history_index_name: required("--history-index")?,
    };
    Ok(Some(Self {
      legacy,
      tables,
      mapping_path: options.remove("--type-mapping"),
      endpoint: options.remove("--endpoint-url"),
      region: options.remove("--region"),
    }))
  }
}

struct SqliteArguments {
  legacy_path: String,
  path: String,
  mapping_path: Option<String>,
}

impl SqliteArguments {
  fn parse(arguments: impl IntoIterator<Item = String>) -> Result<Option<Self>, String> {
    let mut args = arguments.into_iter();
    let mut options = HashMap::new();
    while let Some(name) = args.next() {
      if name == "--help" || name == "-h" {
        return Ok(None);
      }
      if DYNAMODB_ONLY_FLAGS.contains(&name.as_str()) {
        return Err(format!("SQLiteの引数とDynamoDBの引数は同時に指定できません: {name}"));
      }
      if !["--old-sqlite", "--sqlite", "--type-mapping"].contains(&name.as_str()) {
        return Err(format!("未対応の引数: {name}"));
      }
      let value = args
        .next()
        .filter(|value| !value.starts_with("--") && !value.is_empty())
        .ok_or_else(|| format!("{name}には値が必要です"))?;
      if options.insert(name.clone(), value).is_some() {
        return Err(format!("引数の重複: {name}"));
      }
    }
    let mut required = |name: &str| options.remove(name).ok_or_else(|| format!("必須引数: {name}"));
    Ok(Some(Self {
      legacy_path: required("--old-sqlite")?,
      path: required("--sqlite")?,
      mapping_path: options.remove("--type-mapping"),
    }))
  }
}

struct BigtableArguments {
  legacy: LegacyBigtableTables,
  tables: BigtableTables,
  /// 新しいストアの設定。省略した項目は `BigtableOptions::default()` の値のままにする。
  options: BigtableOptions,
  endpoint: bigtable::BigtableEndpoint,
  mapping_path: Option<String>,
}

impl BigtableArguments {
  fn parse(arguments: impl IntoIterator<Item = String>) -> Result<Option<Self>, String> {
    let mut args = arguments.into_iter();
    let mut options = HashMap::new();
    while let Some(name) = args.next() {
      if name == "--help" || name == "-h" {
        return Ok(None);
      }
      if DYNAMODB_ONLY_FLAGS.contains(&name.as_str()) || SQLITE_FLAGS.contains(&name.as_str()) {
        return Err(format!(
          "Bigtableの引数とDynamoDB・SQLiteの引数は同時に指定できません: {name}"
        ));
      }
      if !BIGTABLE_FLAGS.contains(&name.as_str()) && name != "--type-mapping" {
        return Err(format!("未対応の引数: {name}"));
      }
      let value = args
        .next()
        .filter(|value| !value.starts_with("--") && !value.is_empty())
        .ok_or_else(|| format!("{name}には値が必要です"))?;
      if options.insert(name.clone(), value).is_some() {
        return Err(format!("引数の重複: {name}"));
      }
    }
    let mut required = |name: &str| options.remove(name).ok_or_else(|| format!("必須引数: {name}"));
    let legacy = LegacyBigtableTables {
      project_id: required("--old-bigtable-project")?,
      instance_id: required("--old-bigtable-instance")?,
      journal_table_id: required("--old-bigtable-journal")?,
      snapshot_table_id: required("--old-bigtable-snapshot")?,
    };
    let tables = BigtableTables {
      project_id: required("--bigtable-project")?,
      instance_id: required("--bigtable-instance")?,
      journal_table_id: required("--bigtable-journal")?,
      head_table_id: required("--bigtable-head")?,
    };
    // 既定値はライブラリの `BigtableOptions::default()` が持つ。指定された旗だけ上書きする。
    let mut store_options = BigtableOptions::default();
    if let Some(value) = options.remove("--bigtable-keep-snapshot-count") {
      let count = value
        .parse::<usize>()
        .map_err(|_| "--bigtable-keep-snapshot-countには非負の整数が必要です".to_string())?;
      store_options.retention = RetentionSettings::keep_latest(count);
    }
    if let Some(value) = options.remove("--bigtable-envelope-limit-bytes") {
      store_options.envelope_limit_bytes = value
        .parse::<u64>()
        .map_err(|_| "--bigtable-envelope-limit-bytesには非負の整数が必要です".to_string())?;
    }
    if let Some(app_profile_id) = options.remove("--bigtable-app-profile") {
      store_options.app_profile_id = app_profile_id;
    }
    let endpoint = match options.remove("--bigtable-emulator-host") {
      Some(host) => bigtable::BigtableEndpoint::Emulator(host),
      None => bigtable::BigtableEndpoint::Production,
    };
    Ok(Some(Self {
      legacy,
      tables,
      options: store_options,
      endpoint,
      mapping_path: options.remove("--type-mapping"),
    }))
  }
}

/// 実行する移行の種類と、その引数。
enum Command {
  DynamoDb(Arguments),
  Sqlite(SqliteArguments),
  Bigtable(BigtableArguments),
}

/// Bigtable の旗が 1 つでもあれば Bigtable の引数として、なければ SQLite の旗が 1 つでもあれば SQLite の引数として解析し、
/// どちらもなければ従来の DynamoDB の解析へそのまま渡す。`--help` なら `None` を返す。
fn parse_command(arguments: Vec<String>) -> Result<Option<Command>, String> {
  if arguments
    .iter()
    .any(|argument| BIGTABLE_FLAGS.contains(&argument.as_str()))
  {
    return Ok(BigtableArguments::parse(arguments)?.map(Command::Bigtable));
  }
  if arguments
    .iter()
    .any(|argument| SQLITE_FLAGS.contains(&argument.as_str()))
  {
    return Ok(SqliteArguments::parse(arguments)?.map(Command::Sqlite));
  }
  Ok(Arguments::parse(arguments)?.map(Command::DynamoDb))
}

#[tokio::main]
async fn main() -> ExitCode {
  match run().await {
    Ok(success) => {
      if success {
        ExitCode::SUCCESS
      } else {
        ExitCode::FAILURE
      }
    }
    Err(error) => {
      eprintln!("{error}");
      ExitCode::FAILURE
    }
  }
}

async fn run() -> Result<bool, Box<dyn std::error::Error>> {
  let command = parse_command(std::env::args().skip(1).collect())
    .map_err(|message| std::io::Error::new(std::io::ErrorKind::InvalidInput, message))?;
  match command {
    None => {
      println!(
        "{HELP}\n{PRECONDITION}\n{SQLITE_HELP}\n{SQLITE_PRECONDITION}\n{BIGTABLE_HELP}\n{BIGTABLE_PRECONDITION}"
      );
      Ok(true)
    }
    Some(Command::DynamoDb(arguments)) => run_dynamodb(arguments).await,
    Some(Command::Sqlite(arguments)) => run_sqlite(arguments),
    Some(Command::Bigtable(arguments)) => run_bigtable(arguments).await,
  }
}

async fn run_dynamodb(arguments: Arguments) -> Result<bool, Box<dyn std::error::Error>> {
  eprintln!("{PRECONDITION}");
  let mapping: HashMap<String, String> = match arguments.mapping_path {
    Some(path) => serde_json::from_slice(&std::fs::read(path)?)?,
    None => HashMap::new(),
  };
  let mut config = aws_config::defaults(aws_config::BehaviorVersion::latest());
  if let Some(region) = arguments.region {
    config = config.region(Region::new(region));
  }
  if let Some(endpoint) = arguments.endpoint {
    config = config.endpoint_url(endpoint);
  }
  let client = aws_sdk_dynamodb::Client::new(&config.load().await);
  let report = match migrate_v3_dynamodb(&client, &arguments.legacy, &arguments.tables, &mapping).await {
    Ok(report) => report,
    Err(error) => {
      println!("{}", serde_json::to_string(&error.report)?);
      return Err(Box::new(error));
    }
  };
  println!("{}", serde_json::to_string(&report)?);
  Ok(report.reasons.is_empty())
}

fn run_sqlite(arguments: SqliteArguments) -> Result<bool, Box<dyn std::error::Error>> {
  eprintln!("{SQLITE_PRECONDITION}");
  let mapping: HashMap<String, String> = match arguments.mapping_path {
    Some(path) => serde_json::from_slice(&std::fs::read(path)?)?,
    None => HashMap::new(),
  };
  let report = match migrate_v3_sqlite(&arguments.legacy_path, &arguments.path, &mapping) {
    Ok(report) => report,
    Err(error) => {
      println!("{}", serde_json::to_string(&error.report)?);
      return Err(Box::new(error));
    }
  };
  println!("{}", serde_json::to_string(&report)?);
  Ok(report.reasons.is_empty())
}

async fn run_bigtable(arguments: BigtableArguments) -> Result<bool, Box<dyn std::error::Error>> {
  eprintln!("{BIGTABLE_PRECONDITION}");
  let mapping: HashMap<String, String> = match arguments.mapping_path {
    Some(path) => serde_json::from_slice(&std::fs::read(path)?)?,
    None => HashMap::new(),
  };
  let (connection, token_source) = bigtable::connect(&arguments.endpoint).await?;
  let report = match migrate_v3_bigtable(
    &connection,
    token_source,
    &arguments.legacy,
    &arguments.tables,
    &arguments.options,
    &mapping,
  )
  .await
  {
    Ok(report) => report,
    Err(error) => {
      println!("{}", serde_json::to_string(&error.report)?);
      return Err(Box::new(error));
    }
  };
  println!("{}", serde_json::to_string(&report)?);
  Ok(report.reasons.is_empty())
}

#[cfg(test)]
#[path = "main_test.rs"]
mod tests;
