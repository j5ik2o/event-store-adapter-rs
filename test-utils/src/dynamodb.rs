use anyhow::Result;
use aws_sdk_dynamodb::config::{Credentials, Region};
use aws_sdk_dynamodb::operation::create_table::CreateTableOutput;
use aws_sdk_dynamodb::types::{
  AttributeDefinition, GlobalSecondaryIndex, KeySchemaElement, KeyType, Projection, ProjectionType,
  ProvisionedThroughput, ScalarAttributeType,
};
use aws_sdk_dynamodb::Client;

pub async fn create_journal_table(client: &Client, table_name: &str, gsi_name: &str) -> Result<CreateTableOutput> {
  let pkey_attribute_definition = AttributeDefinition::builder()
    .attribute_name("pkey")
    .attribute_type(ScalarAttributeType::S)
    .build();

  let skey_attribute_definition = AttributeDefinition::builder()
    .attribute_name("skey")
    .attribute_type(ScalarAttributeType::S)
    .build();

  let pkey_schema = KeySchemaElement::builder()
    .attribute_name("pkey")
    .key_type(KeyType::Hash)
    .build();

  let skey_schema = KeySchemaElement::builder()
    .attribute_name("skey")
    .key_type(KeyType::Range)
    .build();

  let aid_attribute_definition = AttributeDefinition::builder()
    .attribute_name("aid")
    .attribute_type(ScalarAttributeType::S)
    .build();

  let seq_nr_attribute_definition = AttributeDefinition::builder()
    .attribute_name("seq_nr")
    .attribute_type(ScalarAttributeType::N)
    .build();

  let provisioned_throughput = ProvisionedThroughput::builder()
    .read_capacity_units(10)
    .write_capacity_units(5)
    .build();

  let gsi = GlobalSecondaryIndex::builder()
    .index_name(gsi_name)
    .key_schema(
      KeySchemaElement::builder()
        .attribute_name("aid")
        .key_type(KeyType::Hash)
        .build(),
    )
    .key_schema(
      KeySchemaElement::builder()
        .attribute_name("seq_nr")
        .key_type(KeyType::Range)
        .build(),
    )
    .projection(Projection::builder().projection_type(ProjectionType::All).build())
    .provisioned_throughput(provisioned_throughput.clone())
    .build();

  let result = client
    .create_table()
    .table_name(table_name)
    .attribute_definitions(pkey_attribute_definition)
    .attribute_definitions(skey_attribute_definition)
    .attribute_definitions(aid_attribute_definition)
    .attribute_definitions(seq_nr_attribute_definition)
    .key_schema(pkey_schema)
    .key_schema(skey_schema)
    .global_secondary_indexes(gsi)
    .provisioned_throughput(provisioned_throughput)
    .send()
    .await?;

  Ok(result)
}

pub async fn create_snapshot_table(client: &Client, table_name: &str, gsi_name: &str) -> Result<CreateTableOutput> {
  let pkey_attribute_definition = AttributeDefinition::builder()
    .attribute_name("pkey")
    .attribute_type(ScalarAttributeType::S)
    .build();

  let pkey_schema = KeySchemaElement::builder()
    .attribute_name("pkey")
    .key_type(KeyType::Hash)
    .build();

  let skey_attribute_definition = AttributeDefinition::builder()
    .attribute_name("skey")
    .attribute_type(ScalarAttributeType::S)
    .build();

  let skey_schema = KeySchemaElement::builder()
    .attribute_name("skey")
    .key_type(KeyType::Range)
    .build();

  let aid_attribute_definition = AttributeDefinition::builder()
    .attribute_name("aid")
    .attribute_type(ScalarAttributeType::S)
    .build();

  let seq_nr_attribute_definition = AttributeDefinition::builder()
    .attribute_name("seq_nr")
    .attribute_type(ScalarAttributeType::N)
    .build();

  let provisioned_throughput = ProvisionedThroughput::builder()
    .read_capacity_units(10)
    .write_capacity_units(5)
    .build();

  let gsi = GlobalSecondaryIndex::builder()
    .index_name(gsi_name)
    .key_schema(
      KeySchemaElement::builder()
        .attribute_name("aid")
        .key_type(KeyType::Hash)
        .build(),
    )
    .key_schema(
      KeySchemaElement::builder()
        .attribute_name("seq_nr")
        .key_type(KeyType::Range)
        .build(),
    )
    .projection(Projection::builder().projection_type(ProjectionType::All).build())
    .provisioned_throughput(provisioned_throughput.clone())
    .build();

  let result = client
    .create_table()
    .table_name(table_name)
    .attribute_definitions(pkey_attribute_definition)
    .attribute_definitions(skey_attribute_definition)
    .attribute_definitions(aid_attribute_definition)
    .attribute_definitions(seq_nr_attribute_definition)
    .key_schema(pkey_schema)
    .key_schema(skey_schema)
    .global_secondary_indexes(gsi)
    .provisioned_throughput(provisioned_throughput)
    .send()
    .await?;

  Ok(result)
}

pub fn create_client(dynamodb_port: u16) -> Client {
  let region = Region::new("us-west-1");
  let config = aws_sdk_dynamodb::Config::builder()
    .region(Some(region))
    .endpoint_url(format!("http://localhost:{}", dynamodb_port))
    .credentials_provider(Credentials::new("x", "x", None, None, "default"))
    .build();
  Client::from_conf(config)
}

pub async fn wait_table(client: &Client, target_table_name: &str) -> bool {
  let lto = client.list_tables().send().await;
  match lto {
    Ok(lto) => {
      log::info!("table_names: {:?}", lto.table_names());
      match lto.table_names() {
        Some(table_names) => table_names.iter().any(|tn| tn == target_table_name),
        None => false,
      }
    }
    Err(e) => {
      println!("Error: {}", e);
      false
    }
  }
}
