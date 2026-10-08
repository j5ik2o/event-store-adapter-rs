//! 新契約のDynamoDBストア生成と設定確定。

mod configuration;
mod configuration_create;
mod item_size;
#[cfg(test)]
mod open_test;
mod persist_event;

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use aws_sdk_dynamodb::Client;
use serde::{de::DeserializeOwned, Serialize};

use crate::next::aggregate_id::AggregateId;
use crate::next::error::EventStoreError;
use crate::next::retention::RetentionSettings;
use crate::next::serializer::{EventSerializer, JsonEventSerializer, JsonSnapshotSerializer, SnapshotSerializer};

/// ３表で確定した設定と、その設定を使うクライアント・シリアライザを保持する。
pub struct EventStoreForDynamoDB<AID, A, P> {
  client: Client,
  tables: DynamoDbTables,
  options: DynamoDbOptions,
  store_id: String,
  event_serializer: Arc<dyn EventSerializer<P>>,
  snapshot_serializer: Arc<dyn SnapshotSerializer<A>>,
  _aggregate_id: PhantomData<fn() -> AID>,
}

impl<AID, A, P> Clone for EventStoreForDynamoDB<AID, A, P> {
  fn clone(&self) -> Self {
    Self {
      client: self.client.clone(),
      tables: self.tables.clone(),
      options: self.options.clone(),
      store_id: self.store_id.clone(),
      event_serializer: self.event_serializer.clone(),
      snapshot_serializer: self.snapshot_serializer.clone(),
      _aggregate_id: PhantomData,
    }
  }
}

impl<AID, A, P> std::fmt::Debug for EventStoreForDynamoDB<AID, A, P> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("EventStoreForDynamoDB")
      .field("tables", &self.tables)
      .field("options", &self.options)
      .field("store_id", &self.store_id)
      .field("event_serializer", &self.event_serializer)
      .field("snapshot_serializer", &self.snapshot_serializer)
      .finish_non_exhaustive()
  }
}

impl<AID: AggregateId, A: Send + Sync + 'static, P: Send + Sync + 'static> EventStoreForDynamoDB<AID, A, P> {
  /// 設定を確定し、任意のシリアライザを持つストアを生成する（DY-8・T-6）。
  pub async fn open_with_serializers(
    client: Client,
    tables: DynamoDbTables,
    options: DynamoDbOptions,
    event_serializer: Arc<dyn EventSerializer<P>>,
    snapshot_serializer: Arc<dyn SnapshotSerializer<A>>,
  ) -> Result<Self, EventStoreError> {
    let store_id = configuration_create::resolve_configuration(&client, &tables, &options).await?;
    Ok(Self {
      client,
      tables,
      options,
      store_id,
      event_serializer,
      snapshot_serializer,
      _aggregate_id: PhantomData,
    })
  }
}

impl<AID, A, P> EventStoreForDynamoDB<AID, A, P>
where
  AID: AggregateId,
  A: Serialize + DeserializeOwned + Send + Sync + 'static,
  P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
  /// 設定を確定し、既定のJSONシリアライザを持つストアを生成する（DY-8・T-8）。
  pub async fn open(client: Client, tables: DynamoDbTables, options: DynamoDbOptions) -> Result<Self, EventStoreError> {
    Self::open_with_serializers(
      client,
      tables,
      options,
      Arc::new(JsonEventSerializer::new()),
      Arc::new(JsonSnapshotSerializer::new()),
    )
    .await
  }
}

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
