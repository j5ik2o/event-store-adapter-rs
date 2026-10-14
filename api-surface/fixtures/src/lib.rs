//! 公開 API の形の照合の試験で使う、小さな定義。
//!
//! `baseline` はライブラリ (`event-store-adapter-rs`) と同じ形を最小限で書いたもの。
//! ほかのモジュールは、`baseline` から 1 か所だけ形をずらしたもので、ずれる項目とそれに依存する項目だけを定義し、
//! 残りは `baseline` から再公開する。照合の試験は、ずらしたモジュールごとに、期待する 1 か所だけが報告されることを確かめる。

/// ライブラリと同じ形。照合の報告は 0 件になる。
pub mod baseline {
  use async_trait::async_trait;
  use chrono::{DateTime, Utc};

  pub type SeqNr = u64;

  pub trait AggregateId: Send + Sync + 'static {}

  pub struct EventEnvelope<AID, P> {
    aggregate_id: AID,
    seq_nr: SeqNr,
    occurred_at: DateTime<Utc>,
    manifest: String,
    payload: P,
  }

  impl<AID, P> EventEnvelope<AID, P> {
    pub fn new(aggregate_id: AID, seq_nr: SeqNr, occurred_at: DateTime<Utc>, payload: P) -> Self {
      Self {
        aggregate_id,
        seq_nr,
        occurred_at,
        manifest: String::new(),
        payload,
      }
    }

    pub fn with_manifest(mut self, manifest: impl Into<String>) -> Self {
      self.manifest = manifest.into();
      self
    }

    pub fn aggregate_id(&self) -> &AID {
      &self.aggregate_id
    }

    pub fn seq_nr(&self) -> SeqNr {
      self.seq_nr
    }

    pub fn occurred_at(&self) -> &DateTime<Utc> {
      &self.occurred_at
    }

    pub fn manifest(&self) -> &str {
      &self.manifest
    }

    pub fn payload(&self) -> &P {
      &self.payload
    }
  }

  pub struct SnapshotEnvelope<A> {
    aggregate: A,
    seq_nr: SeqNr,
    manifest: String,
  }

  impl<A> SnapshotEnvelope<A> {
    pub fn new(aggregate: A, seq_nr: SeqNr) -> Self {
      Self {
        aggregate,
        seq_nr,
        manifest: String::new(),
      }
    }

    pub fn with_manifest(mut self, manifest: impl Into<String>) -> Self {
      self.manifest = manifest.into();
      self
    }

    pub fn aggregate(&self) -> &A {
      &self.aggregate
    }

    pub fn seq_nr(&self) -> SeqNr {
      self.seq_nr
    }

    pub fn manifest(&self) -> &str {
      &self.manifest
    }
  }

  pub struct SnapshotRead<A> {
    snapshot: Option<SnapshotEnvelope<A>>,
    head_seq_nr: SeqNr,
  }

  impl<A> SnapshotRead<A> {
    pub fn snapshot(&self) -> Option<&SnapshotEnvelope<A>> {
      self.snapshot.as_ref()
    }

    pub fn head_seq_nr(&self) -> SeqNr {
      self.head_seq_nr
    }
  }

  #[derive(Debug)]
  pub enum EventStoreError {
    OptimisticLock,
    ContractViolation,
    Serialization,
    Configuration,
    Storage,
  }

  #[async_trait]
  pub trait EventStore: Send + Sync + 'static {
    type AID: AggregateId;
    type A: Send + Sync + 'static;
    type P: Send + Sync + 'static;

    async fn persist_event(&self, event: EventEnvelope<Self::AID, Self::P>) -> Result<(), EventStoreError>;

    async fn persist_event_and_snapshot(
      &self,
      event: EventEnvelope<Self::AID, Self::P>,
      snapshot: SnapshotEnvelope<Self::A>,
    ) -> Result<(), EventStoreError>;

    async fn get_latest_snapshot_by_id(
      &self,
      aid: &Self::AID,
    ) -> Result<Option<SnapshotRead<Self::A>>, EventStoreError>;

    async fn get_events_by_id_since_seq_nr(
      &self,
      aid: &Self::AID,
      seq_nr: SeqNr,
    ) -> Result<Vec<EventEnvelope<Self::AID, Self::P>>, EventStoreError>;
  }
}

/// イベント封筒から `occurred_at()` を消した。
pub mod missing_occurred_at {
  use chrono::{DateTime, Utc};

  pub use super::baseline::{AggregateId, EventStore, EventStoreError, SeqNr, SnapshotEnvelope, SnapshotRead};

  pub struct EventEnvelope<AID, P> {
    aggregate_id: AID,
    seq_nr: SeqNr,
    manifest: String,
    payload: P,
  }

  impl<AID, P> EventEnvelope<AID, P> {
    pub fn new(aggregate_id: AID, seq_nr: SeqNr, _occurred_at: DateTime<Utc>, payload: P) -> Self {
      Self {
        aggregate_id,
        seq_nr,
        manifest: String::new(),
        payload,
      }
    }

    pub fn with_manifest(mut self, manifest: impl Into<String>) -> Self {
      self.manifest = manifest.into();
      self
    }

    pub fn aggregate_id(&self) -> &AID {
      &self.aggregate_id
    }

    pub fn seq_nr(&self) -> SeqNr {
      self.seq_nr
    }

    pub fn manifest(&self) -> &str {
      &self.manifest
    }

    pub fn payload(&self) -> &P {
      &self.payload
    }
  }
}

/// イベント封筒の `manifest()` の名前を `manifest_name()` に変えた。
pub mod renamed_manifest {
  use chrono::{DateTime, Utc};

  pub use super::baseline::{AggregateId, EventStore, EventStoreError, SeqNr, SnapshotEnvelope, SnapshotRead};

  pub struct EventEnvelope<AID, P> {
    aggregate_id: AID,
    seq_nr: SeqNr,
    occurred_at: DateTime<Utc>,
    manifest: String,
    payload: P,
  }

  impl<AID, P> EventEnvelope<AID, P> {
    pub fn new(aggregate_id: AID, seq_nr: SeqNr, occurred_at: DateTime<Utc>, payload: P) -> Self {
      Self {
        aggregate_id,
        seq_nr,
        occurred_at,
        manifest: String::new(),
        payload,
      }
    }

    pub fn with_manifest(mut self, manifest: impl Into<String>) -> Self {
      self.manifest = manifest.into();
      self
    }

    pub fn aggregate_id(&self) -> &AID {
      &self.aggregate_id
    }

    pub fn seq_nr(&self) -> SeqNr {
      self.seq_nr
    }

    pub fn occurred_at(&self) -> &DateTime<Utc> {
      &self.occurred_at
    }

    pub fn manifest_name(&self) -> &str {
      &self.manifest
    }

    pub fn payload(&self) -> &P {
      &self.payload
    }
  }
}

/// イベント封筒の `seq_nr()` の戻り値を `SeqNr` ではなく `u32` にした。
pub mod seq_nr_returns_u32 {
  use chrono::{DateTime, Utc};

  pub use super::baseline::{AggregateId, EventStore, EventStoreError, SeqNr, SnapshotEnvelope, SnapshotRead};

  pub struct EventEnvelope<AID, P> {
    aggregate_id: AID,
    seq_nr: u32,
    occurred_at: DateTime<Utc>,
    manifest: String,
    payload: P,
  }

  impl<AID, P> EventEnvelope<AID, P> {
    pub fn new(aggregate_id: AID, seq_nr: SeqNr, occurred_at: DateTime<Utc>, payload: P) -> Self {
      Self {
        aggregate_id,
        seq_nr: u32::try_from(seq_nr).unwrap_or(u32::MAX),
        occurred_at,
        manifest: String::new(),
        payload,
      }
    }

    pub fn with_manifest(mut self, manifest: impl Into<String>) -> Self {
      self.manifest = manifest.into();
      self
    }

    pub fn aggregate_id(&self) -> &AID {
      &self.aggregate_id
    }

    pub fn seq_nr(&self) -> u32 {
      self.seq_nr
    }

    pub fn occurred_at(&self) -> &DateTime<Utc> {
      &self.occurred_at
    }

    pub fn manifest(&self) -> &str {
      &self.manifest
    }

    pub fn payload(&self) -> &P {
      &self.payload
    }
  }
}

/// イベント封筒の `new` から、必須の `seq_nr` の引数を消した。
pub mod constructor_missing_seq_nr {
  use chrono::{DateTime, Utc};

  pub use super::baseline::{AggregateId, EventStore, EventStoreError, SeqNr, SnapshotEnvelope, SnapshotRead};

  pub struct EventEnvelope<AID, P> {
    aggregate_id: AID,
    seq_nr: SeqNr,
    occurred_at: DateTime<Utc>,
    manifest: String,
    payload: P,
  }

  impl<AID, P> EventEnvelope<AID, P> {
    pub fn new(aggregate_id: AID, occurred_at: DateTime<Utc>, payload: P) -> Self {
      Self {
        aggregate_id,
        seq_nr: 0,
        occurred_at,
        manifest: String::new(),
        payload,
      }
    }

    pub fn with_manifest(mut self, manifest: impl Into<String>) -> Self {
      self.manifest = manifest.into();
      self
    }

    pub fn aggregate_id(&self) -> &AID {
      &self.aggregate_id
    }

    pub fn seq_nr(&self) -> SeqNr {
      self.seq_nr
    }

    pub fn occurred_at(&self) -> &DateTime<Utc> {
      &self.occurred_at
    }

    pub fn manifest(&self) -> &str {
      &self.manifest
    }

    pub fn payload(&self) -> &P {
      &self.payload
    }
  }
}

/// `get_events_by_id_since_seq_nr` が、イベント封筒の列ではなく `Vec<Self::P>` を返す。
pub mod events_return_payloads {
  use async_trait::async_trait;

  pub use super::baseline::{AggregateId, EventEnvelope, EventStoreError, SeqNr, SnapshotEnvelope, SnapshotRead};

  #[async_trait]
  pub trait EventStore: Send + Sync + 'static {
    type AID: AggregateId;
    type A: Send + Sync + 'static;
    type P: Send + Sync + 'static;

    async fn persist_event(&self, event: EventEnvelope<Self::AID, Self::P>) -> Result<(), EventStoreError>;

    async fn persist_event_and_snapshot(
      &self,
      event: EventEnvelope<Self::AID, Self::P>,
      snapshot: SnapshotEnvelope<Self::A>,
    ) -> Result<(), EventStoreError>;

    async fn get_latest_snapshot_by_id(
      &self,
      aid: &Self::AID,
    ) -> Result<Option<SnapshotRead<Self::A>>, EventStoreError>;

    async fn get_events_by_id_since_seq_nr(
      &self,
      aid: &Self::AID,
      seq_nr: SeqNr,
    ) -> Result<Vec<Self::P>, EventStoreError>;
  }
}

/// `EventStoreError` から `Storage` を消した。
pub mod missing_storage_error {
  pub use super::baseline::{AggregateId, EventEnvelope, EventStore, SeqNr, SnapshotEnvelope, SnapshotRead};

  #[derive(Debug)]
  pub enum EventStoreError {
    OptimisticLock,
    ContractViolation,
    Serialization,
    Configuration,
  }
}

/// `EventStore` の関連型 `P` に、許していない制約 `Clone` を足した。
pub mod store_payload_clone_bound {
  use async_trait::async_trait;

  pub use super::baseline::{AggregateId, EventEnvelope, EventStoreError, SeqNr, SnapshotEnvelope, SnapshotRead};

  #[async_trait]
  pub trait EventStore: Send + Sync + 'static {
    type AID: AggregateId;
    type A: Send + Sync + 'static;
    type P: Clone + Send + Sync + 'static;

    async fn persist_event(&self, event: EventEnvelope<Self::AID, Self::P>) -> Result<(), EventStoreError>;

    async fn persist_event_and_snapshot(
      &self,
      event: EventEnvelope<Self::AID, Self::P>,
      snapshot: SnapshotEnvelope<Self::A>,
    ) -> Result<(), EventStoreError>;

    async fn get_latest_snapshot_by_id(
      &self,
      aid: &Self::AID,
    ) -> Result<Option<SnapshotRead<Self::A>>, EventStoreError>;

    async fn get_events_by_id_since_seq_nr(
      &self,
      aid: &Self::AID,
      seq_nr: SeqNr,
    ) -> Result<Vec<EventEnvelope<Self::AID, Self::P>>, EventStoreError>;
  }
}

/// `SnapshotRead` の型の引数 `A` に、許していない制約 `Clone` を足した。
pub mod snapshot_read_clone_bound {
  pub use super::baseline::{AggregateId, EventEnvelope, EventStore, EventStoreError, SeqNr, SnapshotEnvelope};

  pub struct SnapshotRead<A: Clone> {
    snapshot: Option<SnapshotEnvelope<A>>,
    head_seq_nr: SeqNr,
  }

  impl<A: Clone> SnapshotRead<A> {
    pub fn snapshot(&self) -> Option<&SnapshotEnvelope<A>> {
      self.snapshot.as_ref()
    }

    pub fn head_seq_nr(&self) -> SeqNr {
      self.head_seq_nr
    }
  }
}

/// `EventStore` を `#[async_trait(?Send)]` にした。操作の戻り値の `Future` が `Send` を持たない。
pub mod store_future_not_send {
  use async_trait::async_trait;

  pub use super::baseline::{AggregateId, EventEnvelope, EventStoreError, SeqNr, SnapshotEnvelope, SnapshotRead};

  #[async_trait(?Send)]
  pub trait EventStore: Send + Sync + 'static {
    type AID: AggregateId;
    type A: Send + Sync + 'static;
    type P: Send + Sync + 'static;

    async fn persist_event(&self, event: EventEnvelope<Self::AID, Self::P>) -> Result<(), EventStoreError>;

    async fn persist_event_and_snapshot(
      &self,
      event: EventEnvelope<Self::AID, Self::P>,
      snapshot: SnapshotEnvelope<Self::A>,
    ) -> Result<(), EventStoreError>;

    async fn get_latest_snapshot_by_id(
      &self,
      aid: &Self::AID,
    ) -> Result<Option<SnapshotRead<Self::A>>, EventStoreError>;

    async fn get_events_by_id_since_seq_nr(
      &self,
      aid: &Self::AID,
      seq_nr: SeqNr,
    ) -> Result<Vec<EventEnvelope<Self::AID, Self::P>>, EventStoreError>;
  }
}
