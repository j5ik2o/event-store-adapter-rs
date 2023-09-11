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
  /// 最終更新日時を返す。
  fn last_updated_at(&self) -> &DateTime<Utc>;
}

/// イベントストアを表すトレイト。
#[async_trait]
pub trait EventStore: Debug + Clone + Sync + Send + 'static {
  /// イベントの型。
  type EV: Event;
  /// 集約の型。
  type AG: Aggregate;
  /// 集約のIDの型。
  type AID: AggregateId;

  /// イベントを保存します。
  ///
  /// # 引数
  /// - `event` - 保存するイベント
  /// - `version` - イベントを保存する集約のバージョン
  ///
  /// # 戻り値
  /// - `Ok(())` - 保存に成功した場合
  /// - `Err(e)` - 保存に失敗した場合
  async fn persist_event(&mut self, event: &Self::EV, version: usize) -> Result<()>;

  /// イベント及びスナップショットを保存します。
  ///
  /// # 引数
  /// - `event` - 保存するイベント
  /// - `aggregate` - スナップショットを保存する集約
  ///
  /// # 戻り値
  /// - `Ok(())` - 保存に成功した場合
  /// - `Err(e)` - 保存に失敗した場合
  async fn persist_event_and_snapshot(&mut self, event: &Self::EV, aggregate: &Self::AG) -> Result<()>;

  /// 最新のスナップショットを取得する。
  async fn get_latest_snapshot_by_id(&self, aid: &Self::AID) -> Result<Option<(Self::AG, usize)>>;

  /// 指定したIDとシーケンス番号以降のイベントを取得する。
  async fn get_events_by_id_since_seq_nr(&self, aid: &Self::AID, seq_nr: usize) -> Result<Vec<Self::EV>>;
}
