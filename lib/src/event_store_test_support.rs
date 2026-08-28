#![cfg(test)]

use std::error::Error as StdError;
use std::fmt::{Display, Formatter};

use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::event_envelope::EventEnvelope;
use crate::types::{AggregateId, EventStore, EventStoreWriteError};

// FR7.1 / C3: 4 バックエンド共通の契約対称性ハーネス（v3 再設計版）。
// 全バックエンドが本シナリオを無改変で green にすることが受入条件となる。
// 旧シナリオの `user_account.seq_nr` / `user_account.version` への直接 assert
// （集約フィールド依存）は封筒アクセサ経由に置き換えた（BR2.5 — set_version 廃止の帰結）。

/// 手順 1（作成）で封筒に載せる manifest 値。
pub const CREATED_MANIFEST: &str = "user-account-created/v1";
/// 手順 3（リネーム）で封筒に載せる manifest 値。
pub const RENAMED_MANIFEST: &str = "user-account-renamed/v1";

#[derive(Debug)]
pub enum UserAccountError {
  AlreadyRenamed(#[allow(dead_code)] String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserAccountId {
  value: String,
}

impl UserAccountId {
  pub fn new(value: String) -> Self {
    Self { value }
  }
}

impl Display for UserAccountId {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.value)
  }
}

impl AggregateId for UserAccountId {
  fn type_name(&self) -> String {
    "UserAccount".to_string()
  }

  fn value(&self) -> String {
    self.value.clone()
  }
}

/// イベント payload（純ドメイン内容 — メタデータは封筒が運搬する。BR2.4）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UserAccountEvent {
  Created { name: String },
  Renamed { name: String },
}

/// 集約 payload（純ドメイン状態 — seq_nr / version / last_updated_at を持たない。FR3.2）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserAccount {
  pub id: UserAccountId,
  pub name: String,
}

impl UserAccount {
  pub fn new(id: UserAccountId, name: String) -> (Self, UserAccountEvent) {
    let my_self = Self { id, name: name.clone() };
    (my_self, UserAccountEvent::Created { name })
  }

  pub fn replay(events: impl IntoIterator<Item = UserAccountEvent>, snapshot: UserAccount) -> Self {
    events.into_iter().fold(snapshot, |mut result, event| {
      result.apply_event(&event);
      result
    })
  }

  fn apply_event(&mut self, event: &UserAccountEvent) {
    if let UserAccountEvent::Renamed { name } = event {
      self.name = name.clone();
    }
  }

  pub fn rename(&mut self, name: &str) -> Result<UserAccountEvent, UserAccountError> {
    if self.name == name {
      return Err(UserAccountError::AlreadyRenamed(name.to_string()));
    }
    self.name = name.to_string();
    Ok(UserAccountEvent::Renamed { name: name.to_string() })
  }
}

/// スナップショット + 差分リプレイ（W4）で復元した読取結果。
///
/// seq_nr / version は封筒（列由来）の値であり、集約フィールドへの書き戻しに依存しない
/// （AC3.1.2）。次のイベントの採番は `seq_nr + 1`、次回書込の expected_version は `version`。
pub struct ReplayedUserAccount {
  pub state: UserAccount,
  pub seq_nr: usize,
  pub version: usize,
}

/// スナップショット封筒の seq_nr をリプレイ開始点として最新状態を復元する（W4）。
pub async fn find_by_id<T>(
  event_store: &T,
  id: &UserAccountId,
) -> Result<Option<ReplayedUserAccount>, Box<dyn StdError + Send + Sync>>
where
  T: EventStore<AID = UserAccountId, A = UserAccount, P = UserAccountEvent>, {
  let snapshot = match event_store.get_latest_snapshot_by_id(id).await? {
    Some(snapshot) => snapshot,
    None => return Ok(None),
  };
  let snapshot_seq_nr = snapshot.seq_nr();
  let version = snapshot.version();
  let events = event_store
    .get_events_by_id_since_seq_nr(id, snapshot_seq_nr + 1)
    .await?;
  let seq_nr = events.last().map(|event| event.seq_nr()).unwrap_or(snapshot_seq_nr);
  let state = UserAccount::replay(
    events.into_iter().map(EventEnvelope::into_payload),
    snapshot.into_aggregate(),
  );
  Ok(Some(ReplayedUserAccount { state, seq_nr, version }))
}

/// 楽観ロックメッセージが許可キー（aid / expected_version / actual_version）のみで
/// 構成されていることを検証する（NFR3.2 — エラー契約の書式固定）。
pub fn assert_optimistic_lock_message_format(message: &str) {
  let mut parts = message.split(", ");
  assert_eq!(parts.next(), Some("optimistic lock failed"));
  let allowed_keys = ["aid", "expected_version", "actual_version"];
  for part in parts {
    let key = part.split('=').next().unwrap();
    assert!(allowed_keys.contains(&key), "unexpected field in message: {}", part);
  }
  assert!(!message.contains("://"), "message must not contain connection strings");
}

/// 共有シナリオ（v3 — 8 手順の assert 系列）。
///
/// C3: 全バックエンドが無改変で green にすべき受入契約。封筒メタデータ
/// （manifest / occurred_at / aggregate_id / seq_nr）の往復 assert を含む。
pub async fn exercise_user_account_flow<T>(
  event_store: &mut T,
  id: &UserAccountId,
) -> Result<(), Box<dyn StdError + Send + Sync>>
where
  T: EventStore<AID = UserAccountId, A = UserAccount, P = UserAccountEvent>, {
  // 手順 1 — 作成: seq_nr=1 封筒（manifest 指定あり）+ 初期集約を expected_version=0 で永続化
  let (user_account, created) = UserAccount::new(id.clone(), "test".to_string());
  let occurred_at_1 = Utc::now();
  let envelope_1 = EventEnvelope::new(id.clone(), 1, occurred_at_1, created).with_manifest(CREATED_MANIFEST);
  event_store
    .persist_event_and_snapshot(envelope_1, user_account.clone(), 0)
    .await?;

  // 手順 2 — 読取 1: snapshot 封筒の version() == 1 / seq_nr() == 1、aggregate が初期状態と一致
  let snapshot = event_store
    .get_latest_snapshot_by_id(id)
    .await?
    .expect("snapshot must exist after creation");
  assert_eq!(snapshot.version(), 1);
  assert_eq!(snapshot.seq_nr(), 1);
  assert_eq!(snapshot.aggregate(), &user_account);

  // 手順 3 — 更新（リネーム）: seq_nr=2 封筒を expected_version=1 で永続化（スナップショット付き）
  let mut state = snapshot.into_aggregate();
  let renamed = state.rename("test2").unwrap();
  let occurred_at_2 = Utc::now();
  let envelope_2 = EventEnvelope::new(id.clone(), 2, occurred_at_2, renamed).with_manifest(RENAMED_MANIFEST);
  event_store
    .persist_event_and_snapshot(envelope_2, state.clone(), 1)
    .await?;

  // 手順 4 — 競合: 同じ expected_version=1 で再度永続化 → OptimisticLockError（書式は許可キーのみ）
  let mut conflicting_state = state.clone();
  let conflicting = conflicting_state.rename("conflict").unwrap();
  let conflict_envelope = EventEnvelope::new(id.clone(), 2, Utc::now(), conflicting).with_manifest(RENAMED_MANIFEST);
  let conflict_result = event_store
    .persist_event_and_snapshot(conflict_envelope, conflicting_state, 1)
    .await;
  match conflict_result {
    Err(EventStoreWriteError::OptimisticLockError(message)) => {
      assert!(
        message.starts_with(&format!("optimistic lock failed, aid={}, expected_version=1", id)),
        "unexpected message: {}",
        message
      );
      assert_optimistic_lock_message_format(&message);
    }
    other => panic!("expected OptimisticLockError, got {:?}", other),
  }

  // 手順 5 — 読取 2: snapshot 封筒の version() == 2 / seq_nr() == 2
  let snapshot = event_store
    .get_latest_snapshot_by_id(id)
    .await?
    .expect("snapshot must exist after update");
  assert_eq!(snapshot.version(), 2);
  assert_eq!(snapshot.seq_nr(), 2);
  assert_eq!(snapshot.aggregate(), &state);

  // 手順 6 — イベントのみ更新（リネーム 2 回目）: seq_nr=3 を expected_version=2 で persist_event
  let mut state = snapshot.into_aggregate();
  let renamed_again = state.rename("test3").unwrap();
  let occurred_at_3 = Utc::now();
  // manifest 省略（既定の空文字列が往復することの検証を兼ねる — FR1.2）
  let envelope_3 = EventEnvelope::new(id.clone(), 3, occurred_at_3, renamed_again);
  event_store.persist_event(envelope_3, 2).await?;

  // 手順 7 — ストリーム読取: 封筒 3 件が seq_nr 昇順で返り、各封筒のメタデータが書込時と同値
  // （封筒メタデータ往復 assert — C3 の充足条件）
  let events = event_store.get_events_by_id_since_seq_nr(id, 1).await?;
  assert_eq!(events.len(), 3);

  assert_eq!(events[0].aggregate_id(), id);
  assert_eq!(events[0].seq_nr(), 1);
  assert_eq!(events[0].occurred_at(), &occurred_at_1);
  assert_eq!(events[0].manifest(), CREATED_MANIFEST);
  assert_eq!(
    events[0].payload(),
    &UserAccountEvent::Created {
      name: "test".to_string()
    }
  );

  assert_eq!(events[1].aggregate_id(), id);
  assert_eq!(events[1].seq_nr(), 2);
  assert_eq!(events[1].occurred_at(), &occurred_at_2);
  assert_eq!(events[1].manifest(), RENAMED_MANIFEST);
  assert_eq!(
    events[1].payload(),
    &UserAccountEvent::Renamed {
      name: "test2".to_string()
    }
  );

  assert_eq!(events[2].aggregate_id(), id);
  assert_eq!(events[2].seq_nr(), 3);
  assert_eq!(events[2].occurred_at(), &occurred_at_3);
  assert_eq!(events[2].manifest(), "");
  assert_eq!(
    events[2].payload(),
    &UserAccountEvent::Renamed {
      name: "test3".to_string()
    }
  );

  // 手順 8 — リプレイ: snapshot（seq_nr=2）以降のイベント（seq_nr=3）を適用した結果が最新状態と一致
  let snapshot = event_store
    .get_latest_snapshot_by_id(id)
    .await?
    .expect("snapshot must exist after event-only update");
  assert_eq!(snapshot.version(), 3);
  assert_eq!(snapshot.seq_nr(), 2);
  let replay_events = event_store
    .get_events_by_id_since_seq_nr(id, snapshot.seq_nr() + 1)
    .await?;
  assert_eq!(replay_events.len(), 1);
  assert_eq!(replay_events[0].seq_nr(), 3);
  let replayed = UserAccount::replay(
    replay_events.into_iter().map(EventEnvelope::into_payload),
    snapshot.into_aggregate(),
  );
  assert_eq!(replayed, state);
  assert_eq!(replayed.name, "test3");

  Ok(())
}

pub fn init_tracing() {
  let _ = tracing_subscriber::fmt()
    .with_max_level(tracing::Level::INFO)
    .with_target(false)
    .with_ansi(false)
    .without_time()
    .try_init();
}
