use super::*;

#[tokio::test]
async fn should_complete_the_sdk_sleep_on_the_runtime() {
  LocalSleep.sleep(std::time::Duration::from_millis(1)).await;
}
