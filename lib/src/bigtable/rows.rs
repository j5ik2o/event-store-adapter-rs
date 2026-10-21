//! Bigtable の要求の部品（列・フィルター・変更）と、ReadRows の行の組み立て・途中終了からの再開（BT-6・BT-10）。
//!
//! 全ての読み取りは、列ごとに最新の 1 セルに限る（BT-2）。書き込みのセル時刻は常に 0 で、置き換える列は
//! 全版を削除してから書く（BT-6）。

use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::bigtable_client::BigtableClient;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::mutation::{
  DeleteFromColumn, Mutation as MutationKind, SetCell,
};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::read_rows_response::cell_chunk::RowStatus;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_filter::{Chain, Filter};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_range::{EndKey, StartKey};
#[cfg(feature = "test-hooks")]
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::MutateRowRequest;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
  column_range, value_range, CheckAndMutateRowRequest, ColumnRange, Mutation, ReadRowsRequest, ReadRowsResponse,
  RowFilter, RowRange, RowSet, ValueRange,
};
use tonic::transport::Channel;

use super::auth::Authorizer;
#[cfg(feature = "test-hooks")]
use super::BigtableRequestKind;
use crate::error::{EventStoreError, StorageOperation};

/// 受信するメッセージの大きさの上限。封筒の上限の最大（8MiB）を持つ行を、head の行（イベントとスナップショット）として読める大きさ。
const MAX_DECODING_MESSAGE_SIZE: usize = 256 * 1024 * 1024;

// ---------------------------------------------------------------------------
// 列ファミリと列修飾子（4 章）
// ---------------------------------------------------------------------------

pub(super) const FAMILY_EVENT: &str = "event";
pub(super) const FAMILY_HISTORY: &str = "history";
pub(crate) const FAMILY_HEAD: &str = "head";
pub(crate) const FAMILY_SNAPSHOT: &str = "snapshot";
pub(super) const FAMILY_CONFIG: &str = "config";

pub(super) const EVENT_ENVELOPE: &str = "envelope";
pub(super) const EVENT_HAS_SNAPSHOT: &str = "has_snapshot";
pub(super) const HISTORY_STATE: &str = "state";
pub(super) const HISTORY_ENVELOPE: &str = "envelope";
pub(crate) const HEAD_SEQ_NR: &str = "seq_nr";
pub(crate) const HEAD_LAST_EVENT: &str = "last_event";
pub(crate) const HEAD_HAS_SNAPSHOT: &str = "has_snapshot";
pub(crate) const SNAPSHOT_CURRENT: &str = "current";
pub(super) const CONFIG_METADATA: &str = "metadata";

pub(super) const STATE_ACTIVE: &[u8] = b"active";
pub(super) const STATE_RETIRED: &[u8] = b"retired";
pub(super) const FLAG_SET: &[u8] = b"1";
pub(super) const FLAG_UNSET: &[u8] = b"0";

// ---------------------------------------------------------------------------
// フィルター
// ---------------------------------------------------------------------------

fn filter_of(filter: Filter) -> RowFilter {
  RowFilter { filter: Some(filter) }
}

/// 列ファミリと列修飾子が完全一致する列のセルだけを通す（両端を閉じた範囲）。
pub(super) fn column(family: &str, qualifier: &str) -> RowFilter {
  filter_of(Filter::ColumnRangeFilter(ColumnRange {
    family_name: family.to_string(),
    start_qualifier: Some(column_range::StartQualifier::StartQualifierClosed(
      qualifier.as_bytes().to_vec(),
    )),
    end_qualifier: Some(column_range::EndQualifier::EndQualifierClosed(
      qualifier.as_bytes().to_vec(),
    )),
  }))
}

/// 列ごとに最新の 1 セルだけを通す（BT-2・BT-6）。
pub(crate) fn latest_cell() -> RowFilter {
  filter_of(Filter::CellsPerColumnLimitFilter(1))
}

/// 値が完全一致するセルだけを通す（両端を閉じた範囲）。
pub(super) fn value_equals(value: &[u8]) -> RowFilter {
  filter_of(Filter::ValueRangeFilter(ValueRange {
    start_value: Some(value_range::StartValue::StartValueClosed(value.to_vec())),
    end_value: Some(value_range::EndValue::EndValueClosed(value.to_vec())),
  }))
}

/// 前から順にフィルターを適用する。
pub(super) fn chain(filters: Vec<RowFilter>) -> RowFilter {
  filter_of(Filter::Chain(Chain { filters }))
}

/// 列の最新 1 セルが `expected` のときだけ、セルを通すフィルター（条件付き更新の条件）。
/// 列を最新 1 セルに限ってから値を照合するので、過去の版には一致しない（BT-6）。
pub(super) fn latest_cell_equals(family: &str, qualifier: &str, expected: &[u8]) -> RowFilter {
  chain(vec![column(family, qualifier), latest_cell(), value_equals(expected)])
}

/// 列にセルがあるときだけ、セルを通すフィルター（条件付き更新の条件）。
pub(crate) fn latest_cell_exists(family: &str, qualifier: &str) -> RowFilter {
  chain(vec![column(family, qualifier), latest_cell()])
}

// ---------------------------------------------------------------------------
// 変更
// ---------------------------------------------------------------------------

fn mutation_of(mutation: MutationKind) -> Mutation {
  Mutation {
    mutation: Some(mutation),
  }
}

/// セルを書く。セル時刻は常に 0µs にする（BT-6）。
pub(crate) fn set_cell(family: &str, qualifier: &str, value: &[u8]) -> Mutation {
  mutation_of(MutationKind::SetCell(SetCell {
    family_name: family.to_string(),
    column_qualifier: qualifier.as_bytes().to_vec(),
    timestamp_micros: 0,
    value: value.to_vec(),
  }))
}

/// 列の全ての版を削除する。
pub(super) fn delete_column(family: &str, qualifier: &str) -> Mutation {
  mutation_of(MutationKind::DeleteFromColumn(DeleteFromColumn {
    family_name: family.to_string(),
    column_qualifier: qualifier.as_bytes().to_vec(),
    time_range: None,
  }))
}

/// 列の全ての版を削除してから、新しい値を書く。同じ原子的な操作の中に並べて使う（BT-6）。
pub(super) fn replace_cell(family: &str, qualifier: &str, value: &[u8]) -> [Mutation; 2] {
  [delete_column(family, qualifier), set_cell(family, qualifier, value)]
}

// ---------------------------------------------------------------------------
// 行の組み立て（BT-10）
// ---------------------------------------------------------------------------

/// 完成した行のセル 1 つ。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AssembledCell {
  pub family: String,
  pub qualifier: Vec<u8>,
  pub timestamp_micros: i64,
  pub value: Vec<u8>,
}

/// 完成した行。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AssembledRow {
  pub row_key: Vec<u8>,
  pub cells: Vec<AssembledCell>,
}

impl AssembledRow {
  /// 列の最初のセルの値を返す。読み取りは最新 1 セルに限るので、列のセルは高々 1 つである。
  pub(crate) fn value(&self, family: &str, qualifier: &str) -> Option<&[u8]> {
    self
      .cells
      .iter()
      .find(|cell| cell.family == family && cell.qualifier == qualifier.as_bytes())
      .map(|cell| cell.value.as_slice())
  }
}

fn invalid(operation: StorageOperation, message: &'static str) -> EventStoreError {
  EventStoreError::Storage {
    operation,
    source: Box::new(std::io::Error::other(message)),
  }
}

/// ReadRows の応答（チャンク）から行を組み立てる。完成した行（`CommitRow`）だけを結果に加える。
///
/// エミュレーターは、同じ行の 2 つ目以降のチャンクにも行キーを付ける。行キーが現在の行と同じなら、新しい行を
/// 始めず、同じ行の続きとして扱う。
#[derive(Debug)]
pub(crate) struct RowCollector {
  operation: StorageOperation,
  rows: Vec<AssembledRow>,
  current: Option<AssembledRow>,
  family: String,
  qualifier: Vec<u8>,
  pending: Option<AssembledCell>,
  last_completed_key: Option<Vec<u8>>,
  completed_since_resume: bool,
}

impl RowCollector {
  pub(crate) fn new() -> Self {
    Self {
      operation: StorageOperation::LoadEvents,
      rows: Vec::new(),
      current: None,
      family: String::new(),
      qualifier: Vec::new(),
      pending: None,
      last_completed_key: None,
      completed_since_resume: false,
    }
  }

  /// 誤りに付ける保存先の操作を設定する。
  pub(crate) fn with_operation(mut self, operation: StorageOperation) -> Self {
    self.operation = operation;
    self
  }

  /// 応答 1 つ分のチャンクを順に取り込む。
  pub(crate) fn feed(&mut self, response: ReadRowsResponse) -> Result<(), EventStoreError> {
    for chunk in response.chunks {
      if matches!(chunk.row_status, Some(RowStatus::ResetRow(true))) {
        // サーバーが途中の行を取り消した。後で同じ行が読み直される。
        self.current = None;
        self.pending = None;
        continue;
      }
      self.accept_row_key(&chunk.row_key)?;
      // セルの値の続きでなく、列ファミリ・列修飾子・値・時刻のどれも持たないチャンクは、行の完了だけを伝える。
      let carries_cell_data = self.pending.is_some()
        || chunk.family_name.is_some()
        || chunk.qualifier.is_some()
        || !chunk.value.is_empty()
        || chunk.value_size != 0
        || chunk.timestamp_micros != 0;
      if let Some(family) = chunk.family_name {
        self.family = family;
      }
      if let Some(qualifier) = chunk.qualifier {
        self.qualifier = qualifier;
      }
      if carries_cell_data {
        self.accept_cell_data(&chunk.value, chunk.value_size, chunk.timestamp_micros)?;
      }
      if matches!(chunk.row_status, Some(RowStatus::CommitRow(true))) {
        self.commit_row()?;
      }
    }
    Ok(())
  }

  fn accept_row_key(&mut self, row_key: &[u8]) -> Result<(), EventStoreError> {
    match (&self.current, row_key.is_empty()) {
      (Some(_), true) => Ok(()),
      (Some(row), false) if row.row_key == row_key => Ok(()),
      (Some(_), false) => Err(invalid(
        self.operation,
        "a new row started before the previous row was completed",
      )),
      (None, true) => Err(invalid(self.operation, "a chunk arrived without a row key")),
      (None, false) => {
        self.current = Some(AssembledRow {
          row_key: row_key.to_vec(),
          cells: Vec::new(),
        });
        self.family.clear();
        self.qualifier.clear();
        self.pending = None;
        Ok(())
      }
    }
  }

  fn accept_cell_data(&mut self, value: &[u8], value_size: i32, timestamp_micros: i64) -> Result<(), EventStoreError> {
    if self.pending.is_none() {
      if self.family.is_empty() {
        return Err(invalid(self.operation, "a cell arrived before its column family"));
      }
      self.pending = Some(AssembledCell {
        family: self.family.clone(),
        qualifier: self.qualifier.clone(),
        timestamp_micros,
        value: Vec::new(),
      });
    }
    if let Some(cell) = self.pending.as_mut() {
      cell.value.extend_from_slice(value);
    }
    // 値の続きがあるあいだは `value_size` が 0 でない。最後のチャンクで 0 になる。
    if value_size == 0 {
      if let (Some(cell), Some(row)) = (self.pending.take(), self.current.as_mut()) {
        row.cells.push(cell);
      }
    }
    Ok(())
  }

  fn commit_row(&mut self) -> Result<(), EventStoreError> {
    if self.pending.is_some() {
      return Err(invalid(self.operation, "a row was committed in the middle of a cell"));
    }
    let Some(row) = self.current.take() else {
      return Err(invalid(self.operation, "a row was committed without any chunk"));
    };
    self.last_completed_key = Some(row.row_key.clone());
    self.completed_since_resume = true;
    self.rows.push(row);
    Ok(())
  }

  /// 流れが途中で終わったときに呼ぶ。途中の行を捨て、最後に完成した行のキーを返す。再開はその次の行から始める。
  ///
  /// 前回の再開から 1 行も完成していなければ、同じ要求を繰り返さず、保存先エラーにする。
  pub(super) fn interrupt(&mut self) -> Result<Vec<u8>, EventStoreError> {
    self.current = None;
    self.pending = None;
    let progressed = std::mem::take(&mut self.completed_since_resume);
    match (&self.last_completed_key, progressed) {
      (Some(row_key), true) => Ok(row_key.clone()),
      _ => Err(invalid(
        self.operation,
        "the read was interrupted without completing any row",
      )),
    }
  }

  /// 流れが正常に終わったときに呼ぶ。未完成の行が残っていれば、結果に入れず保存先エラーにする。
  pub(crate) fn finish(self) -> Result<Vec<AssembledRow>, EventStoreError> {
    if self.current.is_some() || self.pending.is_some() {
      return Err(invalid(self.operation, "the read finished in the middle of a row"));
    }
    Ok(self.rows)
  }
}

// ---------------------------------------------------------------------------
// 1 つのテーブルへの要求
// ---------------------------------------------------------------------------

/// データ API の対象の 1 テーブル。全ての要求に app profile を付ける（BT-1）。
/// トークンの取得元があれば、全ての要求の直前に取得元を呼び、`authorization` も付ける。
#[derive(Clone)]
pub(crate) struct TableTarget {
  pub(crate) client: BigtableClient<Channel>,
  pub(crate) table_name: String,
  pub(crate) app_profile_id: String,
  pub(crate) authorizer: Authorizer,
}

impl std::fmt::Debug for TableTarget {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    formatter
      .debug_struct("TableTarget")
      .field("table_name", &self.table_name)
      .field("app_profile_id", &self.app_profile_id)
      .finish()
  }
}

fn storage(operation: StorageOperation, status: tonic::Status) -> EventStoreError {
  EventStoreError::Storage {
    operation,
    source: Box::new(status),
  }
}

impl TableTarget {
  pub(crate) fn new(data: Channel, table_name: String, app_profile_id: &str, authorizer: Authorizer) -> Self {
    Self {
      client: BigtableClient::new(data).max_decoding_message_size(MAX_DECODING_MESSAGE_SIZE),
      table_name,
      app_profile_id: app_profile_id.to_string(),
      authorizer,
    }
  }

  /// 1 行を読む。行がない、または `filter` を通ったセルが 1 つもなければ `None`。
  pub(crate) async fn read_row(
    &self,
    row_key: Vec<u8>,
    filter: RowFilter,
    operation: StorageOperation,
  ) -> Result<Option<AssembledRow>, EventStoreError> {
    let request = ReadRowsRequest {
      table_name: self.table_name.clone(),
      app_profile_id: self.app_profile_id.clone(),
      rows: Some(RowSet {
        row_keys: vec![row_key],
        row_ranges: vec![],
      }),
      filter: Some(filter),
      ..Default::default()
    };
    let request = self.authorizer.authorize(request, operation).await?;
    #[cfg(feature = "test-hooks")]
    self.authorizer.observe(BigtableRequestKind::ReadRows, &request);
    let mut stream = self
      .client
      .clone()
      .read_rows(request)
      .await
      .map_err(|status| storage(operation, status))?
      .into_inner();
    let mut collector = RowCollector::new().with_operation(operation);
    while let Some(response) = stream.message().await.map_err(|status| storage(operation, status))? {
      collector.feed(response)?;
    }
    Ok(collector.finish()?.into_iter().next())
  }

  /// 範囲 `[start, end)` の行を、行キーの順に全て読む。流れが途中で終わったら、最後に完成した行の次から再開する。
  pub(super) async fn read_range(
    &self,
    start: Vec<u8>,
    end: Vec<u8>,
    filter: RowFilter,
    operation: StorageOperation,
  ) -> Result<Vec<AssembledRow>, EventStoreError> {
    let mut collector = RowCollector::new().with_operation(operation);
    let mut start_key = StartKey::StartKeyClosed(start);
    loop {
      let request = ReadRowsRequest {
        table_name: self.table_name.clone(),
        app_profile_id: self.app_profile_id.clone(),
        rows: Some(RowSet {
          row_keys: vec![],
          row_ranges: vec![RowRange {
            start_key: Some(start_key.clone()),
            end_key: Some(EndKey::EndKeyOpen(end.clone())),
          }],
        }),
        filter: Some(filter.clone()),
        ..Default::default()
      };
      let request = self.authorizer.authorize(request, operation).await?;
      #[cfg(feature = "test-hooks")]
      self.authorizer.observe(BigtableRequestKind::ReadRows, &request);
      let mut stream = self
        .client
        .clone()
        .read_rows(request)
        .await
        .map_err(|status| storage(operation, status))?
        .into_inner();
      loop {
        match stream.message().await {
          Ok(Some(response)) => collector.feed(response)?,
          Ok(None) => return collector.finish(),
          Err(status) => {
            // 進んだ行がなければ、同じ要求を繰り返さず、流れを止めた原因を返す。
            let resume_after = collector.interrupt().map_err(|_| storage(operation, status))?;
            start_key = StartKey::StartKeyOpen(resume_after);
            break;
          }
        }
      }
    }
  }

  /// 条件付き更新（CheckAndMutateRow）を 1 回だけ送る。条件を満たしたかを返す。再送しない。
  pub(crate) async fn check_and_mutate(
    &self,
    row_key: Vec<u8>,
    predicate: RowFilter,
    true_mutations: Vec<Mutation>,
    false_mutations: Vec<Mutation>,
    operation: StorageOperation,
  ) -> Result<bool, EventStoreError> {
    let request = CheckAndMutateRowRequest {
      table_name: self.table_name.clone(),
      app_profile_id: self.app_profile_id.clone(),
      row_key,
      predicate_filter: Some(predicate),
      true_mutations,
      false_mutations,
      ..Default::default()
    };
    let request = self.authorizer.authorize(request, operation).await?;
    #[cfg(feature = "test-hooks")]
    self
      .authorizer
      .observe(BigtableRequestKind::CheckAndMutateRow, &request);
    let response = self
      .client
      .clone()
      .check_and_mutate_row(request)
      .await
      .map_err(|status| storage(operation, status))?;
    Ok(response.into_inner().predicate_matched)
  }

  /// 1 行への変更（MutateRow）を 1 回だけ送る。
  #[cfg(feature = "test-hooks")]
  pub(super) async fn mutate(
    &self,
    row_key: Vec<u8>,
    mutations: Vec<Mutation>,
    operation: StorageOperation,
  ) -> Result<(), EventStoreError> {
    let request = MutateRowRequest {
      table_name: self.table_name.clone(),
      app_profile_id: self.app_profile_id.clone(),
      row_key,
      mutations,
      ..Default::default()
    };
    let request = self.authorizer.authorize(request, operation).await?;
    self.authorizer.observe(BigtableRequestKind::MutateRow, &request);
    self
      .client
      .clone()
      .mutate_row(request)
      .await
      .map_err(|status| storage(operation, status))?;
    Ok(())
  }
}

#[cfg(test)]
#[path = "rows_test.rs"]
mod tests;
