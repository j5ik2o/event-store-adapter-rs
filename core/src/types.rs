use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{de, Serialize};
use std::fmt::Debug;

/// 集約のIDを表すトレイト。
pub trait AggregateId:
std::fmt::Display + Debug + Clone + Serialize + for<'de> de::Deserialize<'de> + Send + Sync + 'static {
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
    type EV: Event;
    type AG: Aggregate;
    type AID: AggregateId;

    async fn get_snapshot_by_id(&self, aid: &Self::AID) -> Result<(Self::AG, usize, usize)>;

    async fn get_events_by_id_and_seq_nr(&self, aid: &Self::AID, seq_nr: usize) -> Result<Vec<Self::EV>>;

    async fn store_event_with_snapshot_opt(
        &mut self,
        event: &Self::EV,
        version: usize,
        aggregate: Option<&Self::AG>,
    ) -> Result<()>;
}
