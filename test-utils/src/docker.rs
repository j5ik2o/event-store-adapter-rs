use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage};

pub async fn dynamodb_local() -> ContainerAsync<GenericImage> {
  let wait_for = WaitFor::message_on_stdout("Ready.");
  let container = GenericImage::new("localstack/localstack", "2.1.0")
    .with_env_var("SERVICES", "dynamodb")
    .with_env_var("DEFAULT_REGION", "us-west-1")
    .with_env_var("EAGER_SERVICE_LOADING", "1")
    .with_env_var("DYNAMODB_SHARED_DB", "1")
    .with_env_var("DYNAMODB_IN_MEMORY", "1")
    .with_wait_for(wait_for)
    .start()
    .await;
  container
}
