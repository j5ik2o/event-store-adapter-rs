use testcontainers::clients::Cli;
use testcontainers::core::WaitFor;
use testcontainers::images::generic::GenericImage;
use testcontainers::Container;

pub fn dynamodb_local(docker: &Cli) -> Container<'_, GenericImage> {
  let wait_for = WaitFor::message_on_stdout("Ready.");
  let image = GenericImage::new("localstack/localstack", "2.1.0")
    .with_env_var("SERVICES", "dynamodb")
    .with_env_var("DEFAULT_REGION", "us-west-1")
    .with_env_var("EAGER_SERVICE_LOADING", "1")
    .with_env_var("DYNAMODB_SHARED_DB", "1")
    .with_env_var("DYNAMODB_IN_MEMORY", "1")
    .with_wait_for(wait_for);
  let dynamodb_node: Container<GenericImage> = docker.run::<GenericImage>(image);
  dynamodb_node
}
