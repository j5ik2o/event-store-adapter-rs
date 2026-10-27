//! ExecuteStreamingSql の応答（`PartialResultSet`）の結合（SPN-10・R-5）の試験。
//! `values.rs` の末尾から `#[cfg(test)] #[path = "values_test.rs"] mod tests;` で結ぶ。
//! 通信を行わない純粋な検査だけを扱う。エミュレーターが実際に分割して返す値の往復は `lib/tests/spanner_test.rs` に置く。
//!
//! 前提にしている関数: `merge_partial_result_sets(responses: Vec<PartialResultSet>) -> Result<Vec<Vec<prost_types::Value>>, E>`
//! （`E` は `Debug`）。応答の列を、先頭の応答の `metadata.row_type.fields` の列数ごとの行に組み立てる。
//! `chunked_value` が真の応答の最後の値は未完で、次の応答の最初の値と文字列として連結する。
//! 最後の応答が `chunked_value` のまま終わるとき、または最後の行が列数に満たないときは、結果を返さず誤りにする。

use googleapis_tonic_google_spanner_v1::google::spanner::v1::struct_type::Field;
use googleapis_tonic_google_spanner_v1::google::spanner::v1::{
  PartialResultSet, ResultSetMetadata, StructType, Type, TypeCode,
};
use prost_types::value::Kind;
use prost_types::Value;

use super::*;

fn text(content: &str) -> Value {
  Value {
    kind: Some(Kind::StringValue(content.to_string())),
  }
}

fn number(content: f64) -> Value {
  Value {
    kind: Some(Kind::NumberValue(content)),
  }
}

/// 先頭の応答だけが持つメタデータ。`columns` 個の STRING 列を宣言する。
fn metadata_of(columns: usize) -> ResultSetMetadata {
  ResultSetMetadata {
    row_type: Some(StructType {
      fields: (0..columns)
        .map(|index| Field {
          name: format!("c{index}"),
          r#type: Some(Type {
            code: TypeCode::String as i32,
            ..Default::default()
          }),
        })
        .collect(),
    }),
    ..Default::default()
  }
}

fn first(columns: usize, values: Vec<Value>, chunked_value: bool) -> PartialResultSet {
  PartialResultSet {
    metadata: Some(metadata_of(columns)),
    values,
    chunked_value,
    ..Default::default()
  }
}

fn next(values: Vec<Value>, chunked_value: bool) -> PartialResultSet {
  PartialResultSet {
    values,
    chunked_value,
    ..Default::default()
  }
}

fn strings_of(rows: &[Vec<Value>]) -> Vec<Vec<String>> {
  rows
    .iter()
    .map(|row| {
      row
        .iter()
        .map(|value| match &value.kind {
          Some(Kind::StringValue(content)) => content.clone(),
          other => panic!("文字列の値のはず: {other:?}"),
        })
        .collect()
    })
    .collect()
}

#[test]
fn should_merge_join_a_value_that_the_service_split_across_two_responses() {
  let responses = vec![
    first(1, vec![text("row-1"), text("AAAA")], true),
    next(vec![text("BBBB"), text("row-3")], false),
  ];

  let rows = merge_partial_result_sets(responses).expect("結合できる");

  assert_eq!(strings_of(&rows), vec![vec!["row-1"], vec!["AAAABBBB"], vec!["row-3"]]);
}

#[test]
fn should_merge_join_a_value_that_the_service_split_into_three_parts() {
  let responses = vec![
    first(1, vec![text("AAAA")], true),
    next(vec![text("BBBB")], true),
    next(vec![text("CCCC")], false),
  ];

  let rows = merge_partial_result_sets(responses).expect("結合できる");

  assert_eq!(strings_of(&rows), vec![vec!["AAAABBBBCCCC"]]);
}

#[test]
fn should_merge_keep_adjacent_values_of_different_responses_apart_when_the_value_is_not_chunked() {
  let responses = vec![first(1, vec![text("AAAA")], false), next(vec![text("BBBB")], false)];

  let rows = merge_partial_result_sets(responses).expect("結合できる");

  assert_eq!(strings_of(&rows), vec![vec!["AAAA"], vec!["BBBB"]]);
}

#[test]
fn should_merge_assemble_a_row_whose_cells_arrive_in_different_responses() {
  let responses = vec![
    first(2, vec![text("a1"), text("b1"), text("a2")], false),
    next(vec![text("b2")], false),
  ];

  let rows = merge_partial_result_sets(responses).expect("結合できる");

  assert_eq!(strings_of(&rows), vec![vec!["a1", "b1"], vec!["a2", "b2"]]);
}

#[test]
fn should_merge_join_a_chunked_cell_of_a_row_that_continues_in_the_next_response() {
  let responses = vec![
    first(2, vec![text("a1"), text("XX")], true),
    next(vec![text("YY"), text("a2"), text("b2")], false),
  ];

  let rows = merge_partial_result_sets(responses).expect("結合できる");

  assert_eq!(strings_of(&rows), vec![vec!["a1", "XXYY"], vec!["a2", "b2"]]);
}

#[test]
fn should_merge_return_no_rows_for_a_stream_that_has_only_the_metadata() {
  let responses = vec![first(2, vec![], false)];

  let rows = merge_partial_result_sets(responses).expect("結合できる");

  assert!(rows.is_empty(), "{rows:?}");
}

#[test]
fn should_merge_report_an_error_when_the_stream_ends_with_a_value_that_is_still_chunked() {
  let responses = vec![first(1, vec![text("row-1"), text("AAAA")], true)];

  let result = merge_partial_result_sets(responses);

  assert!(result.is_err(), "分割の途中で終わった結果は返さない: {result:?}");
}

#[test]
fn should_merge_report_an_error_when_the_stream_ends_in_the_middle_of_a_row() {
  let responses = vec![first(2, vec![text("a1"), text("b1"), text("a2")], false)];

  let result = merge_partial_result_sets(responses);

  assert!(
    result.is_err(),
    "列数に満たない行を、完成した行として返さない: {result:?}"
  );
}

#[test]
fn should_merge_report_an_error_when_a_split_value_is_not_a_string() {
  let responses = vec![first(1, vec![number(1.0)], true), next(vec![number(2.0)], false)];

  let result = merge_partial_result_sets(responses);

  assert!(result.is_err(), "連結できるのは文字列の値だけ: {result:?}");
}
