//! 旧テーブルの読み取り。送る要求は ReadRows だけで、テーブルの内容は変えない。
//!
//! v5 の `TableTarget::read_range` は終了キーが必須なので、終了キーなしでテーブルの末尾まで読む口をここに置く。
//! 1 回の要求は `PAGE_ROWS` 行までで、続きは最後に受け取った行キーの直後から読む。

use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::row_range::StartKey;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{ReadRowsRequest, RowRange, RowSet};

use crate::bigtable::keys::CONFIG_ROW_KEY;
use crate::bigtable::rows::{latest_cell, AssembledRow, RowCollector, TableTarget};
use crate::error::{EventStoreError, StorageOperation};

/// 1 回の ReadRows が返す行の上限。
pub(super) const PAGE_ROWS: i64 = 1000;

fn storage(operation: StorageOperation, status: tonic::Status) -> EventStoreError {
  EventStoreError::Storage {
    operation,
    source: Box::new(status),
  }
}

/// `after` の直後（排他的）から、行キーの順に `rows_limit` 行までを読む。`after` が `None` なら先頭から読む。
///
/// 列ごとに最新の 1 セルだけを読む。v3 の読み取りと同じ意味である。流れが途中で止まったときはやり直さず、誤りを返す。
async fn read_page(
  target: &TableTarget,
  after: Option<&[u8]>,
  rows_limit: i64,
  operation: StorageOperation,
) -> Result<Vec<AssembledRow>, EventStoreError> {
  let request = ReadRowsRequest {
    table_name: target.table_name.clone(),
    app_profile_id: target.app_profile_id.clone(),
    rows: Some(RowSet {
      row_keys: vec![],
      row_ranges: vec![RowRange {
        start_key: after.map(|key| StartKey::StartKeyOpen(key.to_vec())),
        end_key: None,
      }],
    }),
    filter: Some(latest_cell()),
    rows_limit,
    ..Default::default()
  };
  let request = target.authorizer.authorize(request, operation).await?;
  let mut stream = target
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
  collector.finish()
}

/// テーブルを行キーの順に `PAGE_ROWS` 行ずつ読む。
pub(super) struct Pages<'a> {
  target: &'a TableTarget,
  operation: StorageOperation,
  after: Option<Vec<u8>>,
  finished: bool,
}

impl<'a> Pages<'a> {
  /// テーブルの先頭から読み始める。
  pub(super) fn new(target: &'a TableTarget, operation: StorageOperation) -> Self {
    Self {
      target,
      operation,
      after: None,
      finished: false,
    }
  }

  /// 次の行の組を返す。最後まで読んだら `None` を返す。
  pub(super) async fn next(&mut self) -> Result<Option<Vec<AssembledRow>>, EventStoreError> {
    if self.finished {
      return Ok(None);
    }
    let rows = read_page(self.target, self.after.as_deref(), PAGE_ROWS, self.operation).await?;
    // 要求した行数に満たなければ、テーブルの末尾まで読み終えている。
    if i64::try_from(rows.len()).is_ok_and(|length| length < PAGE_ROWS) {
      self.finished = true;
    }
    let Some(last) = rows.last() else {
      return Ok(None);
    };
    self.after = Some(last.row_key.clone());
    Ok(Some(rows))
  }
}

/// 設定行（行キー `0x00`）より後ろに、データの行が 1 つでもあるか。
pub(super) async fn first_data_row_exists(
  target: &TableTarget,
  operation: StorageOperation,
) -> Result<bool, EventStoreError> {
  Ok(!read_page(target, Some(CONFIG_ROW_KEY), 1, operation).await?.is_empty())
}
