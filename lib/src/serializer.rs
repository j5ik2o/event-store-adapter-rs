use crate::types::{EventStoreReadError, EventStoreWriteError};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use std::marker::PhantomData;

// FR4.2 / BR2.4: シリアライザは payload（P / A の値）のみを直列化の対象とする。
// 封筒メタデータ（aggregate_id / seq_nr / occurred_at / manifest / version）は列側が持ち、
// この境界には現れない（メタデータ非複製の構造的保証）。

// fnポインタ経由の型マーカー — Send/Sync自動導出を阻害しない
type SerializerTypeMarker<T> = fn() -> T;

/// イベント payload の直列化契約を表すトレイト。
pub trait EventSerializer<P>: Debug + Send + Sync + 'static {
  /// payload をバイト列へ直列化する。
  fn serialize(&self, payload: &P) -> Result<Vec<u8>, EventStoreWriteError>;
  /// バイト列から payload を復元する。
  fn deserialize(&self, data: &[u8]) -> Result<P, EventStoreReadError>;
}

/// JSON 形式の既定イベント payload シリアライザ。
pub struct JsonEventSerializer<P> {
  _phantom: PhantomData<SerializerTypeMarker<P>>,
}

// P2: derive は PhantomData の型パラメータにも境界を課すため、Debug は手動 impl とし
// payload への Debug 要求を作らない（BR1.6 の最小境界維持）
impl<P> Debug for JsonEventSerializer<P> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("JsonEventSerializer").finish()
  }
}

impl<P> Default for JsonEventSerializer<P> {
  fn default() -> Self {
    JsonEventSerializer { _phantom: PhantomData }
  }
}

impl<P> EventSerializer<P> for JsonEventSerializer<P>
where
  P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
  fn serialize(&self, payload: &P) -> Result<Vec<u8>, EventStoreWriteError> {
    serde_json::to_vec(payload).map_err(|e| EventStoreWriteError::SerializationError(e.into()))
  }

  fn deserialize(&self, data: &[u8]) -> Result<P, EventStoreReadError> {
    serde_json::from_slice(data).map_err(|e| EventStoreReadError::DeserializationError(e.into()))
  }
}

/// スナップショット（集約 payload）の直列化契約を表すトレイト。
pub trait SnapshotSerializer<A>: Debug + Send + Sync + 'static {
  /// 集約 payload をバイト列へ直列化する。
  fn serialize(&self, aggregate: &A) -> Result<Vec<u8>, EventStoreWriteError>;
  /// バイト列から集約 payload を復元する。
  fn deserialize(&self, data: &[u8]) -> Result<A, EventStoreReadError>;
}

/// JSON 形式の既定スナップショットシリアライザ。
pub struct JsonSnapshotSerializer<A> {
  _phantom: PhantomData<SerializerTypeMarker<A>>,
}

impl<A> Debug for JsonSnapshotSerializer<A> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("JsonSnapshotSerializer").finish()
  }
}

impl<A> Default for JsonSnapshotSerializer<A> {
  fn default() -> Self {
    JsonSnapshotSerializer { _phantom: PhantomData }
  }
}

impl<A> SnapshotSerializer<A> for JsonSnapshotSerializer<A>
where
  A: Serialize + DeserializeOwned + Send + Sync + 'static,
{
  fn serialize(&self, aggregate: &A) -> Result<Vec<u8>, EventStoreWriteError> {
    serde_json::to_vec(aggregate).map_err(|e| EventStoreWriteError::SerializationError(e.into()))
  }

  fn deserialize(&self, data: &[u8]) -> Result<A, EventStoreReadError> {
    serde_json::from_slice(data).map_err(|e| EventStoreReadError::DeserializationError(e.into()))
  }
}
