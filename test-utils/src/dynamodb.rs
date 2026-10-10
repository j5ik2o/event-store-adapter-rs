use anyhow::Result;
use aws_sdk_dynamodb::config::{Credentials, Region};
use aws_sdk_dynamodb::types::{
  AttributeDefinition, BillingMode, GlobalSecondaryIndex, KeySchemaElement, KeyType, Projection, ProjectionType,
  ScalarAttributeType, StreamSpecification, StreamViewType, TableStatus, TimeToLiveSpecification,
};
use aws_sdk_dynamodb::Client;

/// DynamoDB Local へ接続するクライアントを返す。endpoint は `127.0.0.1` を明示し、
/// リージョンと資格情報は固定のダミー値にする（環境変数や設定ファイルは探索しない）。
pub fn create_dynamodb_local_client(port: u16) -> Client {
  build_client(format!("http://127.0.0.1:{}", port))
}

/// リージョンと資格情報に固定のダミー値を明示したクライアントを組み立てる。
fn build_client(endpoint_url: String) -> Client {
  let region = Region::new("us-west-1");
  let config = aws_sdk_dynamodb::Config::builder()
    .region(Some(region))
    .endpoint_url(endpoint_url)
    .credentials_provider(Credentials::new("x", "x", None, None, "default"))
    .behavior_version_latest()
    .build();
  Client::from_conf(config)
}

/// `ListTables` が成功するまで待つ（250 ミリ秒ごと、上限 60 秒）。
/// 上限を超えたときは、最後の誤りを含めて `Err` を返す。
pub(crate) async fn wait_until_list_tables_succeeds(client: &Client) -> Result<()> {
  let started_at = std::time::Instant::now();
  loop {
    match client.list_tables().send().await {
      Ok(_) => return Ok(()),
      Err(e) => {
        if started_at.elapsed() >= std::time::Duration::from_secs(60) {
          anyhow::bail!("ListTables did not succeed within 60 seconds: {:?}", e);
        }
      }
    }
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
  }
}

/// 新しい配置の 3 テーブルとその GSI の名前。名前は呼び出し側が決める。
#[derive(Debug, Clone)]
pub struct TableNames {
  pub journal: String,
  pub snapshot: String,
  pub head: String,
  pub snapshot_history_index: String,
}

/// 新しい配置の journal・snapshot・head を作り、3 テーブルが `ACTIVE` になるまで待つ。
///
/// `snapshot_ttl_enabled` が true のときだけ、snapshot の TTL（属性 `ttl`）を有効にする。
/// どの操作の誤りも呼び出し側へ返す。
pub async fn create_tables(client: &Client, names: &TableNames, snapshot_ttl_enabled: bool) -> Result<()> {
  client
    .create_table()
    .table_name(&names.journal)
    .attribute_definitions(attribute_definition("aid", ScalarAttributeType::S)?)
    .attribute_definitions(attribute_definition("seq_nr", ScalarAttributeType::N)?)
    .key_schema(key_schema_element("aid", KeyType::Hash)?)
    .key_schema(key_schema_element("seq_nr", KeyType::Range)?)
    .billing_mode(BillingMode::PayPerRequest)
    .send()
    .await?;

  let snapshot_history_index = GlobalSecondaryIndex::builder()
    .index_name(&names.snapshot_history_index)
    .key_schema(key_schema_element("aid", KeyType::Hash)?)
    .key_schema(key_schema_element("active_history_seq_nr", KeyType::Range)?)
    .projection(Projection::builder().projection_type(ProjectionType::KeysOnly).build())
    .build()?;
  client
    .create_table()
    .table_name(&names.snapshot)
    .attribute_definitions(attribute_definition("aid", ScalarAttributeType::S)?)
    .attribute_definitions(attribute_definition("skey", ScalarAttributeType::N)?)
    .attribute_definitions(attribute_definition("active_history_seq_nr", ScalarAttributeType::N)?)
    .key_schema(key_schema_element("aid", KeyType::Hash)?)
    .key_schema(key_schema_element("skey", KeyType::Range)?)
    .global_secondary_indexes(snapshot_history_index)
    .billing_mode(BillingMode::PayPerRequest)
    .send()
    .await?;

  client
    .create_table()
    .table_name(&names.head)
    .attribute_definitions(attribute_definition("aid", ScalarAttributeType::S)?)
    .key_schema(key_schema_element("aid", KeyType::Hash)?)
    .stream_specification(
      StreamSpecification::builder()
        .stream_enabled(true)
        .stream_view_type(StreamViewType::NewImage)
        .build()?,
    )
    .billing_mode(BillingMode::PayPerRequest)
    .send()
    .await?;

  for table_name in [&names.journal, &names.snapshot, &names.head] {
    wait_until_active(client, table_name).await?;
  }

  if snapshot_ttl_enabled {
    client
      .update_time_to_live()
      .table_name(&names.snapshot)
      .time_to_live_specification(
        TimeToLiveSpecification::builder()
          .enabled(true)
          .attribute_name("ttl")
          .build()?,
      )
      .send()
      .await?;
  }

  Ok(())
}

fn attribute_definition(name: &str, attribute_type: ScalarAttributeType) -> Result<AttributeDefinition> {
  Ok(
    AttributeDefinition::builder()
      .attribute_name(name)
      .attribute_type(attribute_type)
      .build()?,
  )
}

fn key_schema_element(name: &str, key_type: KeyType) -> Result<KeySchemaElement> {
  Ok(
    KeySchemaElement::builder()
      .attribute_name(name)
      .key_type(key_type)
      .build()?,
  )
}

/// テーブルが `ACTIVE` になるまで待つ（100 ミリ秒ごと、上限 30 秒）。
async fn wait_until_active(client: &Client, table_name: &str) -> Result<()> {
  let started_at = std::time::Instant::now();
  loop {
    let output = client.describe_table().table_name(table_name).send().await?;
    if output.table().and_then(|t| t.table_status()) == Some(&TableStatus::Active) {
      return Ok(());
    }
    if started_at.elapsed() >= std::time::Duration::from_secs(30) {
      anyhow::bail!("table {} did not become ACTIVE within 30 seconds", table_name);
    }
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
  }
}
