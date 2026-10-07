use std::fmt::Debug;
use std::marker::PhantomData;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::next::error::{EventStoreError, SerializationPhase};

// 直列化の入力は payload だけ（T-7）。封筒のメタデータはこの境界に現れない。
// fn ポインタ経由の型マーカー — Send/Sync の自動導出を阻害しない。
type SerializerTypeMarker<T> = fn() -> T;

/// イベント payload の直列化契約。
pub trait EventSerializer<P>: Debug + Send + Sync + 'static {
  /// payload をバイト列へ直列化する。失敗は `Serialization`（`serialize-event`）。
  fn serialize(&self, payload: &P) -> Result<Vec<u8>, EventStoreError>;
  /// バイト列から payload を復元する。失敗は `Serialization`（`deserialize-event`）。
  fn deserialize(&self, data: &[u8]) -> Result<P, EventStoreError>;
}

/// 集約状態（スナップショットの aggregate）の直列化契約。
pub trait SnapshotSerializer<A>: Debug + Send + Sync + 'static {
  /// 集約状態をバイト列へ直列化する。失敗は `Serialization`（`serialize-snapshot`）。
  fn serialize(&self, aggregate: &A) -> Result<Vec<u8>, EventStoreError>;
  /// バイト列から集約状態を復元する。失敗は `Serialization`（`deserialize-snapshot`）。
  fn deserialize(&self, data: &[u8]) -> Result<A, EventStoreError>;
}

/// 既定のイベント payload シリアライザ。JSON を使う（T-8）。
pub struct JsonEventSerializer<P> {
  _phantom: PhantomData<SerializerTypeMarker<P>>,
}

// derive は PhantomData の型パラメータにも境界を課すため、Debug は手動 impl とし、
// payload への Debug 要求を作らない。
impl<P> Debug for JsonEventSerializer<P> {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    formatter.debug_struct("JsonEventSerializer").finish()
  }
}

impl<P> Default for JsonEventSerializer<P> {
  fn default() -> Self {
    Self { _phantom: PhantomData }
  }
}

impl<P> JsonEventSerializer<P> {
  /// 既定の JSON シリアライザを作る。
  pub fn new() -> Self {
    Self::default()
  }
}

impl<P> EventSerializer<P> for JsonEventSerializer<P>
where
  P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
  fn serialize(&self, payload: &P) -> Result<Vec<u8>, EventStoreError> {
    serde_json::to_vec(payload).map_err(|source| EventStoreError::Serialization {
      phase: SerializationPhase::SerializeEvent,
      source: Box::new(source),
    })
  }

  fn deserialize(&self, data: &[u8]) -> Result<P, EventStoreError> {
    serde_json::from_slice(data).map_err(|source| EventStoreError::Serialization {
      phase: SerializationPhase::DeserializeEvent,
      source: Box::new(source),
    })
  }
}

/// 既定のスナップショット（集約状態）シリアライザ。JSON を使う（T-8）。
pub struct JsonSnapshotSerializer<A> {
  _phantom: PhantomData<SerializerTypeMarker<A>>,
}

impl<A> Debug for JsonSnapshotSerializer<A> {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    formatter.debug_struct("JsonSnapshotSerializer").finish()
  }
}

impl<A> Default for JsonSnapshotSerializer<A> {
  fn default() -> Self {
    Self { _phantom: PhantomData }
  }
}

impl<A> JsonSnapshotSerializer<A> {
  /// 既定の JSON シリアライザを作る。
  pub fn new() -> Self {
    Self::default()
  }
}

impl<A> SnapshotSerializer<A> for JsonSnapshotSerializer<A>
where
  A: Serialize + DeserializeOwned + Send + Sync + 'static,
{
  fn serialize(&self, aggregate: &A) -> Result<Vec<u8>, EventStoreError> {
    serde_json::to_vec(aggregate).map_err(|source| EventStoreError::Serialization {
      phase: SerializationPhase::SerializeSnapshot,
      source: Box::new(source),
    })
  }

  fn deserialize(&self, data: &[u8]) -> Result<A, EventStoreError> {
    serde_json::from_slice(data).map_err(|source| EventStoreError::Serialization {
      phase: SerializationPhase::DeserializeSnapshot,
      source: Box::new(source),
    })
  }
}
