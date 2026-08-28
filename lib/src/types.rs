use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::{de, Serialize};
use std::error::Error as StdError;
use std::fmt::Debug;
use thiserror::Error;

use crate::event_envelope::{EventEnvelope, SnapshotEnvelope};

// FR3.1 / FR3.2: v3 で `Event` / `Aggregate` trait は廃止した。ドメインイベント・集約状態は
// ライブラリ trait を実装しないプレーンな serde 型（payload）として扱い、メタデータは
// 封筒（EventEnvelope / SnapshotEnvelope）が運搬する。

/// 集約のIDを表すトレイト。
pub trait AggregateId:
  std::fmt::Display + Debug + Clone + Serialize + for<'de> de::Deserialize<'de> + Send + Sync + 'static {
  /// 集約の種別名を返す。
  fn type_name(&self) -> String;
  /// 集約のIDを文字列として返す
  fn value(&self) -> String;
}

/// イベントストアを表すトレイト。
///
/// イベント・集約状態は封筒（[`EventEnvelope`] / [`SnapshotEnvelope`]）で受け渡し、
/// ストアは封筒を透過運搬してメタデータを破棄しない（FR2.2 / FR5.1 / FR5.2）。
///
/// # payload の型要求（FR3.1 / BR1.6）
///
/// 集約 payload（`A`）とイベント payload（`P`）への要求は最小境界
/// `Serialize + DeserializeOwned + Send + Sync + 'static` のみで、`Debug` / `Clone` は
/// 要求しない（Memory バックエンドの実装のみ追加で `Clone` を要求する — 文書化済みの非対称）。
///
/// # seq_nr 契約（FR3.3 / FR3.4 / BR6.1）
///
/// - seq_nr は 1 始まりで、同一ストリーム内で連続していることを利用者（ドメイン側）が保証する。
///   採番はドメイン側の責務であり、ストアは採番しない
/// - ライブラリは連続性を検証しない。重複した seq_nr は楽観ロック（CAS / 一意制約）が拒否し、
///   飛び番は検出されずそのまま書き込まれる（利用者責務）
/// - 最初のイベント（新規作成）は seq_nr == 1 であり、create / update の分岐はこの導出で行う（BR2.1）
///
/// # expected_version の規約（BR2.3 / BR2.6）
///
/// 新規作成（seq_nr == 1）は `expected_version == 0`、更新は読取済み
/// [`SnapshotEnvelope::version`] の値を渡す。対応が崩れる呼び出しは
/// [`EventStoreWriteError::ContractViolation`] で拒否される。
#[async_trait]
pub trait EventStore: Debug + Clone + Sync + Send + 'static {
  /// 集約のIDの型。
  type AID: AggregateId;
  /// 集約 payload の型（純ドメイン状態 — 最小境界のみ、BR1.6）。
  type A: Serialize + DeserializeOwned + Send + Sync + 'static;
  /// イベント payload の型（純ドメイン内容 — 最小境界のみ、BR1.6）。
  type P: Serialize + DeserializeOwned + Send + Sync + 'static;

  /// イベント封筒のみを保存します（更新専用）。
  ///
  /// BR2.2: seq_nr == 1（新規作成）の封筒は受け付けず、
  /// [`EventStoreWriteError::ContractViolation`] を返します。新規作成は
  /// [`EventStore::persist_event_and_snapshot`] を使ってください。
  ///
  /// # 引数
  /// - `event` - 保存するイベント封筒（値渡し — 所有権移動）
  /// - `expected_version` - 読取済みスナップショット封筒の version（CAS 照合値）
  ///
  /// # 戻り値
  /// - `Ok(())` - 保存に成功した場合
  /// - `Err(e)` - 保存に失敗した場合（競合時は `OptimisticLockError`）
  async fn persist_event(
    &mut self,
    event: EventEnvelope<Self::AID, Self::P>,
    expected_version: usize,
  ) -> Result<(), EventStoreWriteError>;

  /// イベント封筒及びスナップショット（集約状態）を保存します。
  ///
  /// BR2.1: seq_nr == 1 は新規作成経路（journal + snapshot の原子的作成、version = 1）、
  /// seq_nr > 1 は更新経路（version CAS）に分岐します。
  /// BR2.6: seq_nr == 1 ⇔ expected_version == 0 の対応が崩れる呼び出しは
  /// [`EventStoreWriteError::ContractViolation`] で拒否します。
  ///
  /// # 引数
  /// - `event` - 保存するイベント封筒（値渡し — 所有権移動）
  /// - `aggregate` - スナップショットとして保存する集約 payload
  /// - `expected_version` - 新規作成は 0、更新は読取済みスナップショット封筒の version
  ///
  /// # 戻り値
  /// - `Ok(())` - 保存に成功した場合
  /// - `Err(e)` - 保存に失敗した場合（競合時は `OptimisticLockError`）
  async fn persist_event_and_snapshot(
    &mut self,
    event: EventEnvelope<Self::AID, Self::P>,
    aggregate: Self::A,
    expected_version: usize,
  ) -> Result<(), EventStoreWriteError>;

  /// 最新のスナップショット封筒を取得する。
  ///
  /// BR3.1: 存在しない場合は `None` を返す（エラーにしない）。封筒の seq_nr が
  /// リプレイ開始点、version が次回書込の expected_version となる（FR2.2）。
  async fn get_latest_snapshot_by_id(
    &self,
    aid: &Self::AID,
  ) -> Result<Option<SnapshotEnvelope<Self::A>>, EventStoreReadError>;

  /// 指定したIDとシーケンス番号以降のイベント封筒列を取得する。
  ///
  /// BR3.2: 裸の payload 列ではなく、列由来メタデータを載せた封筒の列を seq_nr 昇順で返す（FR5.1）。
  async fn get_events_by_id_since_seq_nr(
    &self,
    aid: &Self::AID,
    seq_nr: usize,
  ) -> Result<Vec<EventEnvelope<Self::AID, Self::P>>, EventStoreReadError>;
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
  // BR5.2: 契約違反（BR1.4 / BR2.2 / BR2.6 / BR4.1）は専用バリアントで返し、
  // 他の失敗と型で判別可能にする。理由文字列は規約名 + seq_nr / expected_version の
  // 数値に限定する（NFR3.2）
  #[error("ContractViolation: {0}")]
  ContractViolation(String),
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

  #[test]
  fn test_contract_violation_display_carries_reason() {
    // BR5.2: 契約違反は専用バリアントで判別でき、理由文字列を表示に含める
    let error = EventStoreWriteError::ContractViolation("BR1.4: seq_nr must be at least 1, seq_nr=0".to_string());
    assert_eq!(
      error.to_string(),
      "ContractViolation: BR1.4: seq_nr must be at least 1, seq_nr=0"
    );
  }
}
