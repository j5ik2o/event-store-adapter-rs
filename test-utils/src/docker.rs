use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

pub async fn dynamodb_local() -> ContainerAsync<GenericImage> {
  let wait_for = WaitFor::message_on_stdout("Ready.");
  let container = GenericImage::new("localstack/localstack", "2.1.0")
    .with_wait_for(wait_for)
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
