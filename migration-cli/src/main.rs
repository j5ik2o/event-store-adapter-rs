use std::collections::HashMap;
use std::process::ExitCode;

use aws_sdk_dynamodb::config::Region;
use event_store_adapter_rs::DynamoDbTables;
use event_store_adapter_rs::{migrate_v3_dynamodb, LegacyDynamoDbTables};

const PRECONDITION: &str = "運用前提: 実行前に旧2表への書込を停止してください。このツールは書込停止を強制しません。";
const HELP: &str = "v3既定DynamoDB配置を全件検査して新版へ移行します。
Usage: event-store-adapter-migration-rs --old-journal NAME --old-snapshot NAME
  --journal NAME --snapshot NAME --head NAME --history-index NAME
  [--type-mapping FILE.json] [--endpoint-url URL] [--region REGION]
新3表と履歴GSIを事前作成してください。途中失敗後は新3表を作り直して再実行してください。
型対応表は旧型名から新型名へのJSONオブジェクトです。認証は通常のAWS設定を使用します。";

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
  let arguments = Arguments::parse(std::env::args().skip(1))
    .map_err(|message| std::io::Error::new(std::io::ErrorKind::InvalidInput, message))?;
  let Some(arguments) = arguments else {
    println!("{HELP}\n{PRECONDITION}");
    return Ok(true);
  };
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

#[cfg(test)]
#[path = "main_test.rs"]
mod tests;
