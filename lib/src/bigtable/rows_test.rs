//! ReadRows の行の組み立てと途中終了からの再開（BT-10）の試験。
//! `rows.rs` の末尾から `#[cfg(test)] #[path = "rows_test.rs"] mod tests;` で結ぶ。
//! 応答の分割は、実際の ReadRows の応答（チャンク）の規則に合わせて組み立てる。

use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::read_rows_response::cell_chunk::RowStatus;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::read_rows_response::CellChunk;
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::ReadRowsResponse;

use super::*;
use crate::error::EventStoreError;

fn response(chunks: Vec<CellChunk>) -> ReadRowsResponse {
  ReadRowsResponse {
    chunks,
    ..Default::default()
  }
}

/// 行の最初のチャンク（行キー・列ファミリ・列修飾子を持つ）。`value_size > 0` なら、値がこの後のチャンクに続く。
fn first_chunk(row_key: &[u8], family: &str, qualifier: &[u8], value: &[u8], value_size: i32) -> CellChunk {
  CellChunk {
    row_key: row_key.to_vec(),
    family_name: Some(family.to_string()),
    qualifier: Some(qualifier.to_vec()),
    value: value.to_vec(),
    value_size,
    ..Default::default()
  }
}

/// 同じ行の同じ列ファミリの次のセル（列ファミリは前のチャンクから引き継ぐ）。
fn next_cell_chunk(qualifier: &[u8], value: &[u8]) -> CellChunk {
  CellChunk {
    qualifier: Some(qualifier.to_vec()),
    value: value.to_vec(),
    ..Default::default()
  }
}

/// 分割された値の続き。最後のチャンクだけ `value_size` を 0 にする。
fn continuation_chunk(value: &[u8], value_size: i32) -> CellChunk {
  CellChunk {
    value: value.to_vec(),
    value_size,
    ..Default::default()
  }
}

fn committed(mut chunk: CellChunk) -> CellChunk {
  chunk.row_status = Some(RowStatus::CommitRow(true));
  chunk
}

fn reset(mut chunk: CellChunk) -> CellChunk {
  chunk.row_status = Some(RowStatus::ResetRow(true));
  chunk
}

/// 1 つのセルを持つ、完成した行のチャンク。
fn complete_row(row_key: &[u8], value: &[u8]) -> Vec<CellChunk> {
  vec![committed(first_chunk(row_key, "event", b"envelope", value, 0))]
}

fn cells_of(row: &AssembledRow) -> Vec<(String, Vec<u8>, Vec<u8>)> {
  row
    .cells
    .iter()
    .map(|cell| (cell.family.clone(), cell.qualifier.clone(), cell.value.clone()))
    .collect()
}

// BT-10: 複数のチャンクと複数の応答に分割されたセルの値を連結し、列ファミリを引き継いで、行の完了後に結果へ加える。
#[test]
fn should_collector_assemble_a_cell_split_across_chunks_and_responses_into_a_row() {
  let mut collector = RowCollector::new();

  collector
    .feed(response(vec![
      first_chunk(b"row-1", "event", b"envelope", b"ab", 6),
      continuation_chunk(b"cd", 6),
    ]))
    .expect("途中の応答を受け付ける");
  collector
    .feed(response(vec![
      continuation_chunk(b"ef", 0),
      committed(next_cell_chunk(b"has_snapshot", b"0")),
    ]))
    .expect("続きの応答を受け付ける");
  collector
    .feed(response(complete_row(b"row-2", b"x")))
    .expect("次の行を受け付ける");
  let rows = collector.finish().expect("全ての行が完成している");

  assert_eq!(rows.len(), 2);
  assert_eq!(rows[0].row_key, b"row-1");
  assert_eq!(
    cells_of(&rows[0]),
    vec![
      ("event".to_string(), b"envelope".to_vec(), b"abcdef".to_vec()),
      ("event".to_string(), b"has_snapshot".to_vec(), b"0".to_vec()),
    ],
    "分割された値は連結され、2 つ目のセルは列ファミリを引き継ぐ"
  );
  assert_eq!(rows[1].row_key, b"row-2");
  assert_eq!(
    cells_of(&rows[1]),
    vec![("event".to_string(), b"envelope".to_vec(), b"x".to_vec())]
  );
}

// BT-10: エミュレーターは、同じ行の 2 つ目以降のチャンクにも行キーを付ける。行キーが同じなら、同じ行の続きとして扱う。
#[test]
fn should_collector_treat_a_repeated_row_key_inside_a_row_as_a_continuation_of_that_row() {
  let mut collector = RowCollector::new();
  let mut second = committed(next_cell_chunk(b"has_snapshot", b"0"));
  second.row_key = b"row-1".to_vec();
  second.family_name = Some("event".to_string());

  collector
    .feed(response(vec![
      first_chunk(b"row-1", "event", b"envelope", b"a", 0),
      second,
    ]))
    .expect("同じ行のチャンクを受け付ける");
  let rows = collector.finish().expect("行は完成している");

  assert_eq!(rows.len(), 1, "行キーが同じなら新しい行を始めない");
  assert_eq!(
    cells_of(&rows[0]),
    vec![
      ("event".to_string(), b"envelope".to_vec(), b"a".to_vec()),
      ("event".to_string(), b"has_snapshot".to_vec(), b"0".to_vec()),
    ]
  );
}

// BT-10: サーバーが ResetRow で取り消した途中の行は捨て、同じ行の読み直しだけを結果に入れる。
#[test]
fn should_collector_discard_a_row_that_the_server_reset() {
  let mut collector = RowCollector::new();

  collector
    .feed(response(vec![
      first_chunk(b"row-1", "event", b"envelope", b"stale", 0),
      reset(CellChunk::default()),
    ]))
    .expect("取り消しを受け付ける");
  collector
    .feed(response(complete_row(b"row-1", b"fresh")))
    .expect("読み直した行を受け付ける");
  let rows = collector.finish().expect("全ての行が完成している");

  assert_eq!(rows.len(), 1, "取り消した行を結果に入れない");
  assert_eq!(
    cells_of(&rows[0]),
    vec![("event".to_string(), b"envelope".to_vec(), b"fresh".to_vec())]
  );
}

// BT-10: 行の途中で流れが終わったら、途中の行を捨て、最後に完成した行の次から再開する。再開した要求の結果が続く。
#[test]
fn should_collector_resume_after_the_last_completed_row_when_the_stream_ends_in_the_middle_of_a_row() {
  let mut collector = RowCollector::new();
  collector.feed(response(complete_row(b"row-1", b"a"))).unwrap();
  collector
    .feed(response(vec![first_chunk(b"row-2", "event", b"envelope", b"par", 6)]))
    .unwrap();

  let resume_after = collector.interrupt().expect("完成した行があるので再開できる");

  assert_eq!(resume_after, b"row-1", "最後に完成した行の次から再開する");
  collector.feed(response(complete_row(b"row-2", b"bb"))).unwrap();
  collector.feed(response(complete_row(b"row-3", b"c"))).unwrap();
  let rows = collector.finish().expect("再開した要求で全ての行が完成した");
  assert_eq!(
    rows.iter().map(|row| row.row_key.clone()).collect::<Vec<_>>(),
    vec![b"row-1".to_vec(), b"row-2".to_vec(), b"row-3".to_vec()]
  );
  assert_eq!(
    cells_of(&rows[1]),
    vec![("event".to_string(), b"envelope".to_vec(), b"bb".to_vec())],
    "捨てた途中の行の値（par）が混ざらない"
  );
}

// BT-10: 流れが正常に終わったのに未完成の行が残っていたら、その行を結果に入れず保存先エラーにする。
#[test]
fn should_collector_report_a_storage_error_when_the_stream_finishes_with_an_incomplete_row() {
  let mut collector = RowCollector::new();
  collector.feed(response(complete_row(b"row-1", b"a"))).unwrap();
  collector
    .feed(response(vec![first_chunk(b"row-2", "event", b"envelope", b"par", 6)]))
    .unwrap();

  let result = collector.finish();

  assert!(
    matches!(result, Err(EventStoreError::Storage { .. })),
    "未完成の行を結果に入れない: {:?}",
    result.as_ref().err()
  );
}

// BT-10: 完成した行が 1 つもないまま流れが途中で終わったら、再開せずに保存先エラーにする（進まない再開を繰り返さない）。
#[test]
fn should_collector_report_a_storage_error_when_a_request_is_interrupted_without_completing_any_row() {
  let mut collector = RowCollector::new();
  collector
    .feed(response(vec![first_chunk(b"row-1", "event", b"envelope", b"par", 6)]))
    .unwrap();

  let result = collector.interrupt();

  assert!(matches!(result, Err(EventStoreError::Storage { .. })), "{result:?}");
}
