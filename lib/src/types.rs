use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{de, Serialize};
use std::error::Error as StdError;
use std::fmt::Debug;
use thiserror::Error;

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
  async fn persist_event(&mut self, event: &Self::EV, version: usize) -> Result<(), EventStoreWriteError>;

  /// Saves an event and a snapshot.<br/>
  /// イベント及びスナップショットを保存します。
  ///
  /// # 引数
  /// - `event` - event to be saved / 保存するイベント
  /// - `aggregate` - aggregate to be saved as a snapshot / スナップショットを保存する集約
  ///
  /// # 戻り値
  /// - `Ok(())` - if succeeded / 保存に成功した場合
  /// - `Err(e)` - if failed / 保存に失敗した場合
  async fn persist_event_and_snapshot(
    &mut self,
    event: &Self::EV,
    aggregate: &Self::AG,
  ) -> Result<(), EventStoreWriteError>;

  /// 最新のスナップショットを取得する。
  async fn get_latest_snapshot_by_id(&self, aid: &Self::AID) -> Result<Option<Self::AG>, EventStoreReadError>;

  /// 指定したIDとシーケンス番号以降のイベントを取得する。
  async fn get_events_by_id_since_seq_nr(
    &self,
    aid: &Self::AID,
    seq_nr: usize,
  ) -> Result<Vec<Self::EV>, EventStoreReadError>;
}

/// 楽観的ロック失敗の説明文字列を整形する。
///
/// 全バックエンドが同一形式（`optimistic lock failed, aid=<id>, expected_version=<n>[, actual_version=<m>]`）で
/// `EventStoreWriteError::OptimisticLockError` に格納する。集約IDとバージョン情報以外
/// （接続文字列・資格情報・下位SDKの生エラー等）は含めない。
pub(crate) fn format_optimistic_lock_message(
  aid: &str,
  expected_version: usize,
  actual_version: Option<usize>,
) -> String {
  match actual_version {
    Some(actual) => format!(
      "optimistic lock failed, aid={}, expected_version={}, actual_version={}",
      aid, expected_version, actual
    ),
    None => format!(
      "optimistic lock failed, aid={}, expected_version={}",
      aid, expected_version
    ),
  }
}

#[derive(Error, Debug)]
pub enum EventStoreWriteError {
  #[error("SerializeError: {0}")]
  SerializationError(Box<dyn StdError + Send + Sync>),
  #[error("OptimisticLockError: {0}")]
  OptimisticLockError(String),
  #[error("IOError: {0}")]
  IOError(#[from] Box<dyn StdError + Send + Sync>),
  #[error("OtherError: {0}")]
  OtherError(String),
}

#[derive(Error, Debug)]
pub enum EventStoreReadError {
  #[error("DeserializeError: {0}")]
  DeserializationError(Box<dyn StdError + Send + Sync>),
  #[error("IOError: {0}")]
  IOError(#[from] Box<dyn StdError + Send + Sync>),
  #[error("OtherError: {0}")]
  OtherError(String),
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_optimistic_lock_message_basic() {
    let message = format_optimistic_lock_message("UserAccount-01H42K4ABWQ5V2XQEP3A48VE0Z", 3, None);
    assert_eq!(
      message,
      "optimistic lock failed, aid=UserAccount-01H42K4ABWQ5V2XQEP3A48VE0Z, expected_version=3"
    );
  }

  #[test]
  fn test_optimistic_lock_message_with_actual_version() {
    let message = format_optimistic_lock_message("UserAccount-01H42K4ABWQ5V2XQEP3A48VE0Z", 3, Some(4));
    assert_eq!(
      message,
      "optimistic lock failed, aid=UserAccount-01H42K4ABWQ5V2XQEP3A48VE0Z, expected_version=3, actual_version=4"
    );
  }

  #[test]
  fn test_optimistic_lock_message_contains_only_aggregate_context() {
    // NFR-4.4: 整形文字列に含めてよいのは集約ID・バージョン情報のみ。
    // 全フィールドが許可キーであることを機械的に検証し、接続文字列等の混入余地がないことを固定する。
    let message = format_optimistic_lock_message("aid-1", 1, Some(2));
    let mut parts = message.split(", ");
    assert_eq!(parts.next(), Some("optimistic lock failed"));
    let allowed_keys = ["aid", "expected_version", "actual_version"];
    for part in parts {
      let key = part.split('=').next().unwrap();
      assert!(allowed_keys.contains(&key), "unexpected field in message: {}", part);
    }
    assert!(!message.contains("://"), "message must not contain connection strings");
  }
}
