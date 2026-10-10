use crate::user_account::{UserAccount, UserAccountError, UserAccountEvent, UserAccountId};
use event_store_adapter_rs::{EventEnvelope, EventStoreError, EventStoreForDynamoDB, SeqNr, SnapshotEnvelope};

pub struct UserAccountRepository {
  event_store: EventStoreForDynamoDB<UserAccountId, UserAccount, UserAccountEvent>,
}

#[derive(Debug, thiserror::Error)]
pub enum RepositoryError {
  #[error(transparent)]
  Store(#[from] EventStoreError),
  #[error(transparent)]
  Domain(#[from] UserAccountError),
  #[error("event replay reached {last_seq_nr}, but the head is {head_seq_nr}")]
  StaleRead { last_seq_nr: SeqNr, head_seq_nr: SeqNr },
  #[error("account {0} does not exist")]
  NotFound(String),
}

/// Read result restored from the latest snapshot plus differential replay.
/// The next write uses the last replayed sequence number plus one.
#[derive(Debug)]
pub struct ReplayedUserAccount {
  pub state: UserAccount,
  pub seq_nr: SeqNr,
}

impl UserAccountRepository {
  pub fn new(event_store: EventStoreForDynamoDB<UserAccountId, UserAccount, UserAccountEvent>) -> Self {
    Self { event_store }
  }

  pub async fn store_event(
    &self,
    event: EventEnvelope<UserAccountId, UserAccountEvent>,
  ) -> Result<(), RepositoryError> {
    self.event_store.persist_event(event).await?;
    Ok(())
  }

  pub async fn store_event_and_snapshot(
    &self,
    event: EventEnvelope<UserAccountId, UserAccountEvent>,
    snapshot: SnapshotEnvelope<UserAccount>,
  ) -> Result<(), RepositoryError> {
    self.event_store.persist_event_and_snapshot(event, snapshot).await?;
    Ok(())
  }

  pub async fn find_by_id(&self, id: &UserAccountId) -> Result<Option<ReplayedUserAccount>, RepositoryError> {
    let read = match self.event_store.get_latest_snapshot_by_id(id).await? {
      Some(read) => read,
      None => return Ok(None),
    };
    let (snapshot, head_seq_nr) = read.into_parts();
    let snapshot_seq_nr = snapshot.as_ref().map_or(0, SnapshotEnvelope::seq_nr);
    let events = if snapshot_seq_nr < head_seq_nr {
      self
        .event_store
        .get_events_by_id_since_seq_nr(id, snapshot_seq_nr + 1)
        .await?
    } else {
      Vec::new()
    };
    let last_seq_nr = events.last().map_or(snapshot_seq_nr, EventEnvelope::seq_nr);
    if last_seq_nr < head_seq_nr {
      return Err(RepositoryError::StaleRead {
        last_seq_nr,
        head_seq_nr,
      });
    }
    let state = UserAccount::replay_from(
      id,
      snapshot.map(SnapshotEnvelope::into_aggregate),
      events.into_iter().map(EventEnvelope::into_payload),
    )?;
    Ok(Some(ReplayedUserAccount {
      state,
      seq_nr: last_seq_nr,
    }))
  }
}
