use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{de, Serialize};
use std::fmt::Debug;

/// 集約のIDを表すトレイト。
pub trait AggregateId: std::fmt::Display + Debug + Clone + Serialize + for<'de> de::Deserialize<'de> + Send + Sync + 'static {
  /// 集約の種別名を返す。
  fn type_name(&self) -> String;
  /// 集約のIDを文字列として返す
  fn value(&self) -> String;
}

/// イベントを表すトレイト。
pub trait Event: Debug + Clone + Serialize + for<'de> de::Deserialize<'de> + Send + Sync + 'static {
  type ID: std::fmt::Display;
  type AggregateID: AggregateId;
  fn id(&self) -> &Self::ID;
  fn aggregate_id(&self) -> &Self::AggregateID;
  fn seq_nr(&self) -> usize;
  fn occurred_at(&self) -> &DateTime<Utc>;
  fn is_created(&self) -> bool;
}

/// 集約を表すトレイト。
pub trait Aggregate: Debug + Clone + Serialize + for<'de> de::Deserialize<'de> + Send + Sync + 'static {
  type ID: AggregateId;
  /// IDを返す。
  fn id(&self) -> &Self::ID;
  /// シーケンス番号を返す。
  fn seq_nr(&self) -> usize;
  /// バージョンを返す。
  fn version(&self) -> usize;
  /// シーケンス番号を設定する。
  fn set_version(&mut self, version: usize);
}

#[async_trait]
pub trait EventPersistenceGateway: Debug + Clone + Sync + Send + 'static {
  async fn get_snapshot_by_id<T, AID: AggregateId>(&self, aid: &AID) -> Result<(T, usize, usize)>
  where
    T: ?Sized + Serialize + Aggregate + for<'de> de::Deserialize<'de>;

  async fn get_events_by_id_and_seq_nr<T, AID: AggregateId>(&self, aid: &AID, seq_nr: usize) -> Result<Vec<T>>
  where
    T: Debug + for<'de> de::Deserialize<'de>;

  async fn store_event_with_snapshot_opt<A, E>(
    &mut self,
    event: &E,
    version: usize,
    aggregate: Option<&A>,
  ) -> Result<()>
  where
    A: ?Sized + Serialize + Aggregate,
    E: ?Sized + Serialize + Event;
}
