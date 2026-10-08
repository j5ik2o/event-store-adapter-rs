//! 新契約のDynamoDB設定読み取り。

// 公開openは後続。内部読み取りはtest-hooksから直接検証する。
#[cfg_attr(not(feature = "test-hooks"), allow(dead_code))]
mod configuration;

use std::time::Duration;

use crate::next::retention::RetentionSettings;

/// ３テーブルと履歴インデックスの名前を保持する。
#[derive(Debug, Clone)]
pub struct DynamoDbTables {
  pub journal_table_name: String,
  pub snapshot_table_name: String,
  pub head_table_name: String,
  pub snapshot_history_index_name: String,
}

/// 保持設定と未処理キーの再要求方針を保持する。
#[derive(Debug, Clone)]
pub struct DynamoDbOptions {
  pub retention: RetentionSettings,
  pub unprocessed_retry_limit: u32,
  pub unprocessed_retry_initial_delay: Duration,
  pub unprocessed_retry_max_delay: Duration,
}

impl Default for DynamoDbOptions {
  fn default() -> Self {
    Self {
      retention: RetentionSettings::current_only(),
      unprocessed_retry_limit: 10,
      unprocessed_retry_initial_delay: Duration::from_millis(50),
      unprocessed_retry_max_delay: Duration::from_secs(2),
    }
  }
}

#[cfg(feature = "test-hooks")]
#[doc(hidden)]
#[async_trait::async_trait]
pub trait RetrySleeper: std::fmt::Debug + Send + Sync {
  async fn sleep(&self, delay: Duration);
}

#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub use configuration::ConfigurationRead;

/// 内部読み取りの終端だけを試験に公開する。ストアは作成しない。
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub async fn read_configuration_for_test(
  client: &aws_sdk_dynamodb::Client,
  tables: &DynamoDbTables,
  options: &DynamoDbOptions,
  sleeper: Option<std::sync::Arc<dyn RetrySleeper>>,
) -> Result<ConfigurationRead, crate::next::error::EventStoreError> {
  match sleeper {
    Some(sleeper) => {
      configuration::read_with_wait(client, tables, options, configuration::RetryWait::Hook(sleeper)).await
    }
    None => configuration::read_configuration(client, tables, options).await,
  }
}
