use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

pub async fn localstack() -> ContainerAsync<GenericImage> {
  let wait_for = WaitFor::message_on_stdout("Ready.");
  let container = GenericImage::new("localstack/localstack", "2.1.0")
    .with_wait_for(wait_for)
    .with_exposed_port(testcontainers::core::ContainerPort::Tcp(4566))
    .with_env_var("SERVICES", "dynamodb")
    .with_env_var("DEFAULT_REGION", "us-west-1")
    .with_env_var("EAGER_SERVICE_LOADING", "1")
    .with_env_var("DYNAMODB_SHARED_DB", "1")
    .with_env_var("DYNAMODB_IN_MEMORY", "1")
    .start()
    .await
    .expect("Failed to start dynamodb container");
  container
}

// testcontainers 0.28.0 は name:tag で結合するため、name に "@sha256" を含めて digest 参照にする（DynamoDB Local 3.3.1）
const DYNAMODB_LOCAL_IMAGE_NAME: &str = "amazon/dynamodb-local@sha256";
const DYNAMODB_LOCAL_IMAGE_DIGEST: &str = "ff89bd48ff32cd8d9be5fee8873b65b8854dc408f1afe881be6eb00247bc0dab";

/// DynamoDB Local がコンテナ内で待ち受けるポート。
pub const DYNAMODB_LOCAL_PORT: u16 = 8000;

/// DynamoDB Local 3.3.1 を digest 固定のイメージで起動し、`ListTables` が成功してから返す。
///
/// 返したコンテナは呼び出し側が保持する（drop で削除される）。
pub async fn dynamodb_local() -> anyhow::Result<ContainerAsync<GenericImage>> {
  let container = GenericImage::new(DYNAMODB_LOCAL_IMAGE_NAME, DYNAMODB_LOCAL_IMAGE_DIGEST)
    .with_exposed_port(testcontainers::core::ContainerPort::Tcp(DYNAMODB_LOCAL_PORT))
    .with_cmd([
      "-jar",
      "DynamoDBLocal.jar",
      "-inMemory",
      "-sharedDb",
      "-disableTelemetry",
    ])
    .start()
    .await?;
  let port = container.get_host_port_ipv4(DYNAMODB_LOCAL_PORT).await?;
  crate::dynamodb::wait_until_list_tables_succeeds(&crate::dynamodb::create_dynamodb_local_client(port)).await?;
  Ok(container)
}

pub async fn bigtable_emulator() -> ContainerAsync<GenericImage> {
  GenericImage::new("gcr.io/google.com/cloudsdktool/cloud-sdk", "emulators")
    .with_wait_for(WaitFor::seconds(20))
    .with_exposed_port(testcontainers::core::ContainerPort::Tcp(8086))
    .with_env_var("CLOUDSDK_CORE_DISABLE_PROMPTS", "1")
    .with_cmd(vec![
      "gcloud".to_string(),
      "beta".to_string(),
      "emulators".to_string(),
      "bigtable".to_string(),
      "start".to_string(),
      "--host-port=0.0.0.0:8086".to_string(),
    ])
    .start()
    .await
    .expect("Failed to start bigtable emulator")
}
