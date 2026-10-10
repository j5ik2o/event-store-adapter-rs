use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

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
