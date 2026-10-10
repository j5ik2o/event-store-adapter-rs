use super::storage::{MemoryStorage, Records, StoredEvent, StoredSnapshot};
use crate::{
  aggregate_id::{AggregateId, AidString},
  error::{ContractRule, EventStoreError, RetentionFailure, StorageOperation},
  event_envelope::{EventEnvelope, SnapshotEnvelope, SnapshotRead},
  retention::select_expired_history_after_append,
  seq_nr::SeqNr,
  serializer::{EventSerializer, SnapshotSerializer},
  storage_backend::{AppendReceipt, AppendRequest, StorageBackend},
};
use async_trait::async_trait;
use std::sync::Arc;
pub(super) struct MemoryBackend<A, P> {
  pub storage: MemoryStorage,
  pub events: Arc<dyn EventSerializer<P>>,
  pub snapshots: Arc<dyn SnapshotSerializer<A>>,
}
impl<A, P> Clone for MemoryBackend<A, P> {
  fn clone(&self) -> Self {
    Self {
      storage: self.storage.clone(),
      events: self.events.clone(),
      snapshots: self.snapshots.clone(),
    }
  }
}
impl<A, P> std::fmt::Debug for MemoryBackend<A, P> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("MemoryBackend").field("storage", &self.storage).finish()
  }
}
#[async_trait]
impl<AID: AggregateId, A: Send + Sync + 'static, P: Send + Sync + 'static> StorageBackend<AID, A, P>
  for MemoryBackend<A, P>
{
  async fn append(&self, request: AppendRequest<'_, AID, A, P>) -> Result<AppendReceipt, EventStoreError> {
    let event = StoredEvent {
      seq_nr: request.event.seq_nr(),
      occurred_at: *request.event.occurred_at(),
      manifest: request.event.manifest().to_owned(),
      payload: self.events.serialize(request.event.payload())?,
    };
    let snapshot = request
      .snapshot
      .map(|s| {
        self.snapshots.serialize(s.aggregate()).map(|aggregate| StoredSnapshot {
          seq_nr: s.seq_nr(),
          manifest: s.manifest().to_owned(),
          aggregate,
        })
      })
      .transpose()?;
    let mut records = self.storage.lock(StorageOperation::Append)?;
    let head = records
      .get(request.aid)
      .and_then(|r| r.events.last_key_value().map(|(seq, _)| *seq));
    if head.is_some_and(|h| event.seq_nr <= h) {
      return Err(EventStoreError::OptimisticLock {
        aid: request.aid.as_str().to_owned(),
        seq_nr: event.seq_nr,
        head_seq_nr: head,
      });
    }
    if event.seq_nr != head.unwrap_or(0) + 1 {
      return Err(EventStoreError::ContractViolation {
        rule: ContractRule::W8Gap,
        seq_nr: Some(event.seq_nr),
        snapshot_seq_nr: None,
      });
    }
    #[cfg(feature = "test-hooks")]
    if let Some(hooks) = &self.storage.inner.hooks {
      hooks.before_commit(request.aid, event.seq_nr)?;
    }
    let record = records.entry(request.aid.clone()).or_default();
    let seq_nr = event.seq_nr;
    let just_written = snapshot.as_ref().map(|s| s.seq_nr);
    record.events.insert(event.seq_nr, event);
    if let Some(snapshot) = snapshot {
      if self.storage.inner.retention.keep_snapshot_count().is_some() {
        record.history.insert(snapshot.seq_nr, snapshot.clone());
      }
      record.snapshot = Some(snapshot);
    }
    Ok(AppendReceipt {
      retention_failure: self.retain_history(request.aid, seq_nr, just_written, record),
    })
  }

  async fn load_snapshot(&self, aid: &AidString) -> Result<Option<SnapshotRead<A>>, EventStoreError> {
    let copied = {
      let records = self.storage.lock(StorageOperation::LoadSnapshot)?;
      #[cfg(feature = "test-hooks")]
      if let Some(hooks) = &self.storage.inner.hooks {
        hooks.read_snapshot(aid)?;
      }
      records
        .get(aid)
        .and_then(|r| r.events.last_key_value().map(|(head, _)| (*head, r.snapshot.clone())))
    };
    copied
      .map(|(head, snapshot)| {
        let snapshot = snapshot
          .map(|s| {
            self
              .snapshots
              .deserialize(&s.aggregate)
              .map(|a| SnapshotEnvelope::new(a, s.seq_nr).with_manifest(s.manifest))
          })
          .transpose()?;
        Ok(SnapshotRead::new(snapshot, head))
      })
      .transpose()
  }

  async fn load_events(
    &self,
    aggregate_id: &AID,
    aid: &AidString,
    seq_nr: SeqNr,
  ) -> Result<Vec<EventEnvelope<AID, P>>, EventStoreError> {
    let copied: Vec<StoredEvent> = {
      let records = self.storage.lock(StorageOperation::LoadEvents)?;
      #[cfg(feature = "test-hooks")]
      if let Some(hooks) = &self.storage.inner.hooks {
        hooks.read_events(aid)?;
      }
      records
        .get(aid)
        .map(|r| r.events.range(seq_nr..).map(|(_, e)| e.clone()).collect())
        .unwrap_or_default()
    };
    copied
      .into_iter()
      .map(|e| {
        self.events.deserialize(&e.payload).map(|payload| {
          EventEnvelope::new(aggregate_id.clone(), e.seq_nr, e.occurred_at, payload).with_manifest(e.manifest)
        })
      })
      .collect()
  }
}

impl<A, P> MemoryBackend<A, P> {
  fn retain_history(
    &self,
    aid: &AidString,
    seq_nr: SeqNr,
    just_written: Option<SeqNr>,
    record: &mut Records,
  ) -> Option<RetentionFailure> {
    #[cfg(not(feature = "test-hooks"))]
    let _ = (aid, seq_nr);
    let keep = self.storage.inner.retention.keep_snapshot_count()?;
    let visible: Vec<_> = record.history.keys().copied().collect();
    #[cfg(feature = "test-hooks")]
    let visible = if let Some(hooks) = &self.storage.inner.hooks {
      match hooks.retention_visible_history(aid, &visible, just_written) {
        Ok(visible) => visible,
        Err(error) => return Some(retention_failure(aid, seq_nr, "retention-query", error)),
      }
    } else {
      visible
    };
    let expired = select_expired_history_after_append(&visible, just_written, keep);
    if expired.is_empty() {
      return None;
    }
    #[cfg(feature = "test-hooks")]
    if let Some(hooks) = &self.storage.inner.hooks {
      if let Err(error) = hooks.retention_delete(aid, &expired) {
        return Some(retention_failure(aid, seq_nr, "retention-delete", error));
      }
    }
    for seq_nr in expired {
      record.history.remove(&seq_nr);
    }
    None
  }
}

#[cfg(feature = "test-hooks")]
fn retention_failure(aid: &AidString, seq_nr: SeqNr, phase: &str, error: EventStoreError) -> RetentionFailure {
  RetentionFailure {
    aid: aid.as_str().to_owned(),
    seq_nr,
    phase: phase.to_owned(),
    error: error.to_string(),
  }
}
