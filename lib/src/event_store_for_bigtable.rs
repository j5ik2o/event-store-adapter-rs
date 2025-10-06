use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::bigtable_client::BigtableClient;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::read_rows_response::cell_chunk::RowStatus;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::read_rows_response::CellChunk;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
  mutation, row_filter,
  row_range::{EndKey, StartKey},
  MutateRowRequest, Mutation, ReadRowsRequest, RowFilter, RowRange, RowSet,
};
use tonic::transport::Channel;
use tonic::Status;

use crate::key_resolver::{DefaultKeyResolver, KeyResolver};
use crate::serializer::{EventSerializer, SnapshotSerializer};
use crate::types::{
  Aggregate, AggregateId, Event, EventStore, EventStoreReadError, EventStoreWriteError,
  TransactionCanceledExceptionWrapper,
};

const EVENT_FAMILY: &str = "event";
const SNAPSHOT_FAMILY: &str = "snapshot";

#[derive(Clone)]
pub struct EventStoreForBigtable<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>, {
  client: BigtableClient<Channel>,
  project_id: String,
  instance_id: String,
  journal_table_name: String,
  snapshot_table_name: String,
  shard_count: u64,
  keep_snapshot_count: Option<usize>,
  key_resolver: Arc<dyn KeyResolver<ID = AID>>,
  event_serializer: Arc<dyn EventSerializer<E>>,
  snapshot_serializer: Arc<dyn SnapshotSerializer<A>>,
}

impl<AID, A, E> fmt::Debug for EventStoreForBigtable<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>,
{
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("EventStoreForBigtable")
      .field("project_id", &self.project_id)
      .field("instance_id", &self.instance_id)
      .field("journal_table_name", &self.journal_table_name)
      .field("snapshot_table_name", &self.snapshot_table_name)
      .field("shard_count", &self.shard_count)
      .finish_non_exhaustive()
  }
}

unsafe impl<AID, A, E> Sync for EventStoreForBigtable<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>,
{
}

unsafe impl<AID, A, E> Send for EventStoreForBigtable<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>,
{
}

impl<AID, A, E> EventStoreForBigtable<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>,
{
  pub fn new(
    client: BigtableClient<Channel>,
    project_id: String,
    instance_id: String,
    journal_table_name: String,
    snapshot_table_name: String,
    shard_count: u64,
  ) -> Self {
    Self {
      client,
      project_id,
      instance_id,
      journal_table_name,
      snapshot_table_name,
      shard_count,
      keep_snapshot_count: None,
      key_resolver: Arc::new(DefaultKeyResolver::default()),
      event_serializer: Arc::new(crate::serializer::JsonEventSerializer::default()),
      snapshot_serializer: Arc::new(crate::serializer::JsonSnapshotSerializer::default()),
    }
  }

  pub fn with_keep_snapshot_count(mut self, count: Option<usize>) -> Self {
    self.keep_snapshot_count = count;
    self
  }

  pub fn with_key_resolver(mut self, resolver: Arc<dyn KeyResolver<ID = AID>>) -> Self {
    self.key_resolver = resolver;
    self
  }

  pub fn with_event_serializer(mut self, serializer: Arc<dyn EventSerializer<E>>) -> Self {
    self.event_serializer = serializer;
    self
  }

  pub fn with_snapshot_serializer(mut self, serializer: Arc<dyn SnapshotSerializer<A>>) -> Self {
    self.snapshot_serializer = serializer;
    self
  }

  fn table_path(&self, table: &str) -> String {
    format!(
      "projects/{}/instances/{}/tables/{}",
      self.project_id, self.instance_id, table
    )
  }

  fn snapshot_row_key(&self, aid: &AID) -> Vec<u8> {
    format!(
      "{}#{}#{}",
      self.key_resolver.resolve_partition_key(aid, self.shard_count),
      aid.type_name(),
      aid.value()
    )
    .into_bytes()
  }

  fn event_row_prefix(&self, aid: &AID) -> Vec<u8> {
    format!(
      "{}#{}#{}",
      self.key_resolver.resolve_partition_key(aid, self.shard_count),
      aid.type_name(),
      aid.value()
    )
    .into_bytes()
  }

  fn event_row_key(&self, aid: &AID, seq_nr: usize) -> Vec<u8> {
    let mut key = self.event_row_prefix(aid);
    key.push(b'#');
    key.extend_from_slice(format!("{:020}", seq_nr).as_bytes());
    key
  }

  async fn read_snapshot_row(&self, aid: &AID) -> Result<Option<SnapshotRow<A>>, EventStoreReadError> {
    let row_key = self.snapshot_row_key(aid);
    let request = ReadRowsRequest {
      table_name: self.table_path(&self.snapshot_table_name),
      rows: Some(RowSet {
        row_keys: vec![row_key],
        row_ranges: vec![],
      }),
      filter: Some(RowFilter {
        filter: Some(row_filter::Filter::FamilyNameRegexFilter(SNAPSHOT_FAMILY.to_string())),
      }),
      rows_limit: 1,
      ..Default::default()
    };
    let mut rows = self.fetch_rows(request).await?;
    if let Some(row) = rows.pop() {
      let payload = row
        .cells
        .get(&(SNAPSHOT_FAMILY.to_string(), b"payload".to_vec()))
        .cloned()
        .ok_or_else(|| {
          let available = row
            .cells
            .keys()
            .map(|(family, qualifier)| format!("{}:{}", family, String::from_utf8_lossy(qualifier)))
            .collect::<Vec<_>>()
            .join(", ");
          EventStoreReadError::OtherError(format!(
            "snapshot payload is missing (available columns: {})",
            available
          ))
        })?;
      let version_bytes = row
        .cells
        .get(&(SNAPSHOT_FAMILY.to_string(), b"version".to_vec()))
        .ok_or_else(|| EventStoreReadError::OtherError("snapshot version is missing".to_string()))?;
      let version = parse_usize(version_bytes)?;
      let seq_bytes = row
        .cells
        .get(&(SNAPSHOT_FAMILY.to_string(), b"seq_nr".to_vec()))
        .cloned();
      let seq_nr = match seq_bytes {
        Some(bytes) => parse_usize(&bytes)?,
        None => 0,
      };
      let mut aggregate = *self.snapshot_serializer.deserialize(&payload)?;
      aggregate.set_version(version);
      Ok(Some(SnapshotRow {
        aggregate,
        version,
        seq_nr,
      }))
    } else {
      Ok(None)
    }
  }

  async fn fetch_rows(&self, request: ReadRowsRequest) -> Result<Vec<RowData>, EventStoreReadError> {
    let mut client = self.client.clone();
    let mut stream = client
      .read_rows(request)
      .await
      .map_err(status_to_read_error)?
      .into_inner();
    let mut rows = Vec::new();
    let mut acc = RowAccumulator::default();
    while let Some(response) = stream.message().await.map_err(status_to_read_error)? {
      for chunk in response.chunks {
        Self::process_chunk(&mut acc, chunk, &mut rows)?;
      }
    }
    if let Some(row) = acc.finish_row() {
      rows.push(row);
    }
    Ok(rows)
  }

  fn process_chunk(
    acc: &mut RowAccumulator,
    chunk: CellChunk,
    rows: &mut Vec<RowData>,
  ) -> Result<(), EventStoreReadError> {
    if matches!(chunk.row_status, Some(RowStatus::ResetRow(true))) {
      acc.reset();
      return Ok(());
    }

    if !chunk.row_key.is_empty() {
      if acc.key != chunk.row_key {
        if let Some(row) = acc.start_row(chunk.row_key.clone()) {
          rows.push(row);
        }
      }
    }

    if chunk.family_name.is_some() || chunk.qualifier.is_some() {
      acc.start_cell();
    }

    if let Some(family) = chunk.family_name {
      acc.current_family = Some(family);
    }
    if let Some(qualifier) = chunk.qualifier {
      acc.current_qualifier = Some(qualifier);
    }

    if !chunk.value.is_empty() {
      acc.current_value.extend_from_slice(&chunk.value);
    }

    if matches!(chunk.row_status, Some(RowStatus::CommitRow(true))) {
      if let Some(row) = acc.finish_row() {
        rows.push(row);
      }
    }

    Ok(())
  }

  async fn write_event(&self, event: &E) -> Result<(), EventStoreWriteError> {
    let row_key = self.event_row_key(event.aggregate_id(), event.seq_nr());
    let payload = self.event_serializer.serialize(event)?;
    let mutations = vec![
      set_cell(EVENT_FAMILY, b"payload", payload),
      set_cell(EVENT_FAMILY, b"seq_nr", event.seq_nr().to_string().into_bytes()),
      set_cell(EVENT_FAMILY, b"aggregate_id", event.aggregate_id().value().into_bytes()),
      set_cell(
        EVENT_FAMILY,
        b"occurred_at",
        event.occurred_at().timestamp_millis().to_string().into_bytes(),
      ),
    ];
    self
      .mutate_row(self.table_path(&self.journal_table_name), row_key, mutations)
      .await
  }

  async fn mutate_row(
    &self,
    table_name: String,
    row_key: Vec<u8>,
    mutations: Vec<Mutation>,
  ) -> Result<(), EventStoreWriteError> {
    let mut client = self.client.clone();
    client
      .mutate_row(MutateRowRequest {
        table_name,
        row_key,
        mutations,
        ..Default::default()
      })
      .await
      .map_err(status_to_write_error)?;
    Ok(())
  }
}

#[async_trait]
impl<AID, A, E> EventStore for EventStoreForBigtable<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>,
{
  type AG = A;
  type AID = AID;
  type EV = E;

  async fn persist_event(&mut self, event: &Self::EV, version: usize) -> Result<(), EventStoreWriteError> {
    if event.is_created() {
      return Err(EventStoreWriteError::OtherError(
        "persist_event cannot accept a creation event".to_string(),
      ));
    }
    let snapshot = self
      .read_snapshot_row(event.aggregate_id())
      .await
      .map_err(|err| EventStoreWriteError::IOError(Box::new(err)))?;
    let snapshot =
      snapshot.ok_or_else(|| EventStoreWriteError::OtherError("snapshot not found for aggregate".to_string()))?;

    if snapshot.version != version {
      return Err(EventStoreWriteError::OptimisticLockError(
        TransactionCanceledExceptionWrapper(None),
      ));
    }

    self.write_event(event).await?;

    let mutations = vec![
      set_cell(SNAPSHOT_FAMILY, b"version", (version + 1).to_string().into_bytes()),
      set_cell(
        SNAPSHOT_FAMILY,
        b"last_updated_at",
        event.occurred_at().timestamp_millis().to_string().into_bytes(),
      ),
    ];

    self
      .mutate_row(
        self.table_path(&self.snapshot_table_name),
        self.snapshot_row_key(event.aggregate_id()),
        mutations,
      )
      .await?;

    self.try_purge_excess_snapshots(event.aggregate_id()).await
  }

  async fn persist_event_and_snapshot(
    &mut self,
    event: &Self::EV,
    aggregate: &Self::AG,
  ) -> Result<(), EventStoreWriteError> {
    let (current_version, seq_nr) = if event.is_created() {
      (aggregate.version(), aggregate.seq_nr())
    } else {
      let snapshot = self
        .read_snapshot_row(event.aggregate_id())
        .await
        .map_err(|err| EventStoreWriteError::IOError(Box::new(err)))?;
      let snapshot =
        snapshot.ok_or_else(|| EventStoreWriteError::OtherError("snapshot not found for aggregate".to_string()))?;
      if snapshot.version != aggregate.version() {
        return Err(EventStoreWriteError::OptimisticLockError(
          TransactionCanceledExceptionWrapper(None),
        ));
      }
      (aggregate.version() + 1, aggregate.seq_nr())
    };

    let mut snapshot_payload = aggregate.clone();
    snapshot_payload.set_version(current_version);
    let payload = self.snapshot_serializer.serialize(&snapshot_payload)?;

    let mutations = vec![
      set_cell(SNAPSHOT_FAMILY, b"payload", payload),
      set_cell(SNAPSHOT_FAMILY, b"version", current_version.to_string().into_bytes()),
      set_cell(SNAPSHOT_FAMILY, b"seq_nr", seq_nr.to_string().into_bytes()),
      set_cell(
        SNAPSHOT_FAMILY,
        b"last_updated_at",
        event.occurred_at().timestamp_millis().to_string().into_bytes(),
      ),
    ];

    self
      .mutate_row(
        self.table_path(&self.snapshot_table_name),
        self.snapshot_row_key(event.aggregate_id()),
        mutations,
      )
      .await?;

    self.write_event(event).await?;
    self.try_purge_excess_snapshots(event.aggregate_id()).await
  }

  async fn get_latest_snapshot_by_id(&self, aid: &Self::AID) -> Result<Option<Self::AG>, EventStoreReadError> {
    Ok(self.read_snapshot_row(aid).await?.map(|row| row.aggregate))
  }

  async fn get_events_by_id_since_seq_nr(
    &self,
    aid: &Self::AID,
    seq_nr: usize,
  ) -> Result<Vec<Self::EV>, EventStoreReadError> {
    let prefix = self.event_row_prefix(aid);
    let mut end_key = prefix.clone();
    end_key.push(0xFF);
    let start_key = self.event_row_key(aid, seq_nr);

    let request = ReadRowsRequest {
      table_name: self.table_path(&self.journal_table_name),
      rows: Some(RowSet {
        row_keys: vec![],
        row_ranges: vec![RowRange {
          start_key: Some(StartKey::StartKeyClosed(start_key)),
          end_key: Some(EndKey::EndKeyOpen(end_key)),
        }],
      }),
      filter: Some(RowFilter {
        filter: Some(row_filter::Filter::FamilyNameRegexFilter(EVENT_FAMILY.to_string())),
      }),
      ..Default::default()
    };

    let rows = self.fetch_rows(request).await?;
    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
      if let Some(payload) = row.cells.get(&(EVENT_FAMILY.to_string(), b"payload".to_vec())) {
        let event = *self.event_serializer.deserialize(payload)?;
        events.push(event);
      }
    }
    Ok(events)
  }
}

impl<AID, A, E> EventStoreForBigtable<AID, A, E>
where
  AID: AggregateId,
  A: Aggregate<ID = AID>,
  E: Event<AggregateID = AID>,
{
  async fn try_purge_excess_snapshots(&self, _aid: &AID) -> Result<(), EventStoreWriteError> {
    // Bigtable 版では未対応。将来的に Change Stream や TTL 連携を導入する際に実装する。
    Ok(())
  }
}

fn set_cell(family: &str, qualifier: &[u8], value: Vec<u8>) -> Mutation {
  Mutation {
    mutation: Some(mutation::Mutation::SetCell(mutation::SetCell {
      family_name: family.to_string(),
      column_qualifier: qualifier.to_vec(),
      timestamp_micros: -1,
      value,
    })),
  }
}

fn status_to_read_error(status: Status) -> EventStoreReadError {
  EventStoreReadError::IOError(Box::new(status))
}

fn status_to_write_error(status: Status) -> EventStoreWriteError {
  EventStoreWriteError::IOError(Box::new(status))
}

fn parse_usize(bytes: &[u8]) -> Result<usize, EventStoreReadError> {
  let s = std::str::from_utf8(bytes).map_err(|err| EventStoreReadError::OtherError(err.to_string()))?;
  s.parse::<usize>()
    .map_err(|err| EventStoreReadError::OtherError(err.to_string()))
}

#[derive(Default)]
struct RowAccumulator {
  key: Vec<u8>,
  current_family: Option<String>,
  current_qualifier: Option<Vec<u8>>,
  current_value: Vec<u8>,
  cells: HashMap<(String, Vec<u8>), Vec<u8>>,
}

impl RowAccumulator {
  fn start_row(&mut self, key: Vec<u8>) -> Option<RowData> {
    let row = self.finish_row();
    self.key = key;
    row
  }

  fn start_cell(&mut self) {
    if let (Some(family), Some(qualifier)) = (&self.current_family, &self.current_qualifier) {
      let value = std::mem::take(&mut self.current_value);
      let key = (family.clone(), qualifier.clone());
      self.cells.entry(key).or_insert(value);
    } else {
      self.current_value.clear();
    }
    self.current_family = None;
    self.current_qualifier = None;
  }

  fn finish_row(&mut self) -> Option<RowData> {
    if self.key.is_empty() {
      return None;
    }
    self.start_cell();
    if self.cells.is_empty() {
      self.key.clear();
      return None;
    }
    let key = std::mem::take(&mut self.key);
    let cells = std::mem::take(&mut self.cells);
    Some(RowData { key, cells })
  }

  fn reset(&mut self) {
    self.key.clear();
    self.cells.clear();
    self.current_family = None;
    self.current_qualifier = None;
    self.current_value.clear();
  }
}

struct RowData {
  key: Vec<u8>,
  cells: HashMap<(String, Vec<u8>), Vec<u8>>,
}

struct SnapshotRow<A> {
  aggregate: A,
  version: usize,
  seq_nr: usize,
}
