use std::error::Error as StdError;
use std::fmt::{Display, Formatter};

use chrono::{DateTime, Utc};
use event_store_adapter_test_utils_rs::docker::bigtable_emulator;
use event_store_adapter_test_utils_rs::id_generator::id_generate;
use googleapis_tonic_google_bigtable_admin_v2::google::bigtable::admin::v2::bigtable_table_admin_client::BigtableTableAdminClient;
use googleapis_tonic_google_bigtable_admin_v2::google::bigtable::admin::v2::gc_rule;
use googleapis_tonic_google_bigtable_admin_v2::google::bigtable::admin::v2::table;
use googleapis_tonic_google_bigtable_admin_v2::google::bigtable::admin::v2::{
  ColumnFamily, CreateTableRequest, GcRule, Table,
};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::bigtable_client::BigtableClient;
use serde::{Deserialize, Serialize};
use tonic::transport::Channel;
use ulid_generator_rs::ULID;

use crate::event_store_for_bigtable::EventStoreForBigtable;
use crate::types::{Aggregate, AggregateId, Event, EventStore};

#[derive(Debug)]
pub enum UserAccountError {
  AlreadyRenamed(#[allow(dead_code)] String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserAccountId {
  value: String,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UserAccountEvent {
  Created {
    id: ULID,
    aggregate_id: UserAccountId,
    seq_nr: usize,
    name: String,
    occurred_at: chrono::DateTime<chrono::Utc>,
  },
  Renamed {
    id: ULID,
    aggregate_id: UserAccountId,
    seq_nr: usize,
    name: String,
    occurred_at: chrono::DateTime<chrono::Utc>,
  },
}

impl Event for UserAccountEvent {
  type AggregateID = UserAccountId;
  type ID = ULID;

  fn id(&self) -> &Self::ID {
    match self {
      UserAccountEvent::Created { id, .. } => id,
      UserAccountEvent::Renamed { id, .. } => id,
    }
  }

  fn aggregate_id(&self) -> &Self::AggregateID {
    match self {
      UserAccountEvent::Created { aggregate_id, .. } => aggregate_id,
      UserAccountEvent::Renamed { aggregate_id, .. } => aggregate_id,
    }
  }

  fn seq_nr(&self) -> usize {
    match self {
      UserAccountEvent::Created { seq_nr, .. } => *seq_nr,
      UserAccountEvent::Renamed { seq_nr, .. } => *seq_nr,
    }
  }

  fn occurred_at(&self) -> &DateTime<Utc> {
    match self {
      UserAccountEvent::Created { occurred_at, .. } => occurred_at,
      UserAccountEvent::Renamed { occurred_at, .. } => occurred_at,
    }
  }

  fn is_created(&self) -> bool {
    matches!(self, UserAccountEvent::Created { .. })
  }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserAccount {
  id: UserAccountId,
  name: String,
  seq_nr: usize,
  version: usize,
  last_updated_at: DateTime<Utc>,
}

impl UserAccount {
  fn new(id: UserAccountId, name: String) -> (Self, UserAccountEvent) {
    let mut my_self = Self {
      id: id.clone(),
      name,
      seq_nr: 0,
      version: 1,
      last_updated_at: chrono::Utc::now(),
    };
    my_self.seq_nr += 1;
    let event = UserAccountEvent::Created {
      id: id_generate(),
      aggregate_id: id,
      seq_nr: my_self.seq_nr,
      name: my_self.name.clone(),
      occurred_at: chrono::Utc::now(),
    };
    (my_self, event)
  }

  fn replay(events: impl IntoIterator<Item = UserAccountEvent>, snapshot: UserAccount) -> Self {
    events.into_iter().fold(snapshot, |mut result, event| {
      result.apply_event(event.clone());
      result
    })
  }

  fn apply_event(&mut self, event: UserAccountEvent) {
    if let UserAccountEvent::Renamed { name, .. } = event {
      self.rename(&name).unwrap();
    }
  }

  pub fn rename(&mut self, name: &str) -> Result<UserAccountEvent, UserAccountError> {
    if self.name == name {
      return Err(UserAccountError::AlreadyRenamed(name.to_string()));
    }
    self.name = name.to_string();
    self.seq_nr += 1;
    let event = UserAccountEvent::Renamed {
      id: id_generate(),
      aggregate_id: self.id.clone(),
      seq_nr: self.seq_nr,
      name: name.to_string(),
      occurred_at: chrono::Utc::now(),
    };
    Ok(event)
  }
}

impl Aggregate for UserAccount {
  type ID = UserAccountId;

  fn id(&self) -> &Self::ID {
    &self.id
  }

  fn seq_nr(&self) -> usize {
    self.seq_nr
  }

  fn version(&self) -> usize {
    self.version
  }

  fn set_version(&mut self, version: usize) {
    self.version = version
  }

  fn last_updated_at(&self) -> &DateTime<Utc> {
    &self.last_updated_at
  }
}

async fn find_by_id<T: EventStore<AID = UserAccountId, AG = UserAccount, EV = UserAccountEvent>>(
  event_store: &mut T,
  id: &UserAccountId,
) -> Result<Option<UserAccount>, Box<dyn StdError + Send + Sync>> {
  let snapshot = event_store.get_latest_snapshot_by_id(id).await?;
  match snapshot {
    Some(snapshot) => {
      let events = event_store
        .get_events_by_id_since_seq_nr(id, snapshot.seq_nr() + 1)
        .await?;
      let user_account = UserAccount::replay(events, snapshot);
      Ok(Some(user_account))
    }
    None => Ok(None),
  }
}

fn init_tracing() {
  let _ = tracing_subscriber::fmt()
    .with_max_level(tracing::Level::INFO)
    .with_target(false)
    .with_ansi(false)
    .without_time()
    .try_init();
}

#[tokio::test]
async fn test_event_store_on_bigtable() {
  init_tracing();

  let node = bigtable_emulator().await;
  let port = node
    .get_host_port_ipv4(8086)
    .await
    .expect("Failed to get Bigtable port");

  // エミュレータがテーブル作成を受け付けるまでわずかな待機を入れる
  tokio::time::sleep(std::time::Duration::from_millis(200)).await;

  let endpoint = format!("http://127.0.0.1:{}", port);
  let channel = Channel::from_shared(endpoint.clone())
    .expect("invalid endpoint")
    .connect()
    .await
    .expect("failed to connect to emulator");

  let project = "test-project";
  let instance = "test-instance";
  let parent = format!("projects/{}/instances/{}", project, instance);

  let mut table_admin = BigtableTableAdminClient::new(channel.clone());
  create_table(&mut table_admin, &parent, "journal", &["event"]).await;
  create_table(&mut table_admin, &parent, "snapshot", &["snapshot"]).await;

  // 少し待ってからデータ面へアクセス
  tokio::time::sleep(std::time::Duration::from_millis(200)).await;

  let client = BigtableClient::new(channel.clone());

  let mut event_store = EventStoreForBigtable::new(
    client,
    project.to_string(),
    instance.to_string(),
    "journal".to_string(),
    "snapshot".to_string(),
    64,
  );

  let id_value = id_generate();
  let id = UserAccountId {
    value: id_value.to_string(),
  };

  let (user_account, event) = UserAccount::new(id.clone(), "test".to_string());
  event_store
    .persist_event_and_snapshot(&event, &user_account)
    .await
    .unwrap();

  let mut user_account = find_by_id(&mut event_store, &id).await.unwrap().unwrap();
  assert_eq!(user_account.name, "test");
  assert_eq!(user_account.seq_nr, 1);
  assert_eq!(user_account.version, 1);

  let event = user_account.rename("test2").unwrap();
  event_store.persist_event(&event, user_account.version()).await.unwrap();

  let mut user_account = find_by_id(&mut event_store, &id).await.unwrap().unwrap();
  assert_eq!(user_account.name, "test2");
  assert_eq!(user_account.seq_nr, 2);
  assert_eq!(user_account.version, 2);

  let event = user_account.rename("test3").unwrap();
  event_store
    .persist_event_and_snapshot(&event, &user_account)
    .await
    .unwrap();

  let user_account = find_by_id(&mut event_store, &id).await.unwrap().unwrap();
  assert_eq!(user_account.name, "test3");
  assert_eq!(user_account.seq_nr, 3);
  assert_eq!(user_account.version, 3);
}

async fn create_table(client: &mut BigtableTableAdminClient<Channel>, parent: &str, table_id: &str, families: &[&str]) {
  let mut column_families = std::collections::HashMap::new();
  for &family in families {
    column_families.insert(
      family.to_string(),
      ColumnFamily {
        gc_rule: Some(GcRule {
          rule: Some(gc_rule::Rule::MaxNumVersions(1)),
        }),
        ..Default::default()
      },
    );
  }

  let table = Table {
    column_families,
    granularity: table::TimestampGranularity::Millis as i32,
    ..Default::default()
  };

  client
    .create_table(CreateTableRequest {
      parent: parent.to_string(),
      table_id: table_id.to_string(),
      table: Some(table),
      initial_splits: vec![],
    })
    .await
    .expect("failed to create table");
}
