//! 要求の検査（`observe.requests`）の条件の語の収集の試験。

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use event_store_adapter_conformance_rs::data::load;
use event_store_adapter_conformance_rs::observe::constraint_words;
use serde_json::json;

fn conformance_dir() -> PathBuf {
  Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance")
}

fn words(list: &[&str]) -> BTreeSet<String> {
  list.iter().map(|word| word.to_string()).collect()
}

#[test]
fn should_constraint_words_collects_keys_from_initialization_and_every_step() {
  let case = json!({
    "initialization": {"observe": {"requests": [
      {"api": "BatchGetItem", "phase": "configuration-read", "constraints": {"keys": [], "same_store_id": true}}
    ]}},
    "steps": [
      {"op": "a", "observe": {"requests": [
        {"api": "Query", "phase": "retention-query", "constraints": {"index": "history", "scan_index_forward": false}}
      ]}},
      {"op": "b"},
      {"op": "c", "observe": {"requests": [
        {"api": "Query", "phase": "retention-query", "constraints": {"index": "history"}}
      ]}}
    ]
  });

  assert_eq!(
    constraint_words(&case),
    words(&["index", "keys", "same_store_id", "scan_index_forward"])
  );
}

#[test]
fn should_constraint_words_ignores_same_names_outside_request_constraints() {
  let case = json!({
    "store": {"retention_count": null, "retention_mode": "delete", "layout_version": 1},
    "steps": [{
      "op": "getLatestSnapshotById",
      "observe": {"items": [{"table": "journal", "attributes": {"aid": "S"}}]}
    }],
    "faults": [{
      "operation": 1,
      "phase": "read-snapshot",
      "kind": "sdk-response",
      "injection": "replace-response",
      "repeat": {"mode": "count", "count": 1},
      "details": {"unprocessed_keys": ["journal:__config__:0"], "keys": []}
    }]
  });

  assert!(constraint_words(&case).is_empty());
}

#[test]
fn should_constraint_words_of_real_configuration_case_are_top_level_constraint_keys_only() {
  let data = load(&conformance_dir()).expect("実データを読める");
  let body = &data
    .cases
    .iter()
    .find(|case| case.id == "dynamodb-config-new")
    .expect("dynamodb-config-new がある")
    .body;

  // `condition` の中の `attribute_not_exists` は、条件の語ではない。
  assert_eq!(
    constraint_words(body),
    words(&[
      "condition",
      "consistent_read_all_tables",
      "keys",
      "layout_version",
      "put_tables",
      "same_store_id"
    ])
  );
}
