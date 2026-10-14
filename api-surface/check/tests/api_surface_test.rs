//! 公開 API の形の照合の試験。
//!
//! - 肯定: ライブラリ (`event-store-adapter-rs`) と、同じ形の `baseline` が、報告 0 件で通る。
//! - 否定: 形を 1 か所だけずらした定義が、期待する 1 か所の場所だけで、分かりやすいメッセージで報告される。
//! - 対応表: `rs.json` にない共通の項目は、飛ばさずに報告される。
//!
//! rustdoc の JSON は、固定日の nightly (`TOOLCHAIN`) で 1 回だけ作る。

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use event_store_adapter_api_surface_rs::{check_api, document_library, Mapping, Problem, Surface};
use rustdoc_types::Crate;

const LIBRARY_PACKAGE: &str = "event-store-adapter-rs";
const FIXTURES_PACKAGE: &str = "event-store-adapter-api-surface-fixtures-rs";

fn workspace_root() -> PathBuf {
  Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn api_surface_file(name: &str) -> String {
  let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join(name);
  std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("{} を読める: {error}", path.display()))
}

fn surface() -> Surface {
  Surface::from_json(&api_surface_file("surface.json")).expect("surface.json を読み込める")
}

fn mapping() -> Mapping {
  Mapping::from_json(&api_surface_file("rs.json")).expect("rs.json を読み込める")
}

fn documented(cell: &'static OnceLock<Result<Crate, String>>, package: &str, directory: &str) -> &'static Crate {
  cell
    .get_or_init(|| {
      let target_dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(directory);
      document_library(&workspace_root(), package, &target_dir)
    })
    .as_ref()
    .unwrap_or_else(|message| panic!("{package} の rustdoc の JSON を作れる: {message}"))
}

fn library() -> &'static Crate {
  static LIBRARY: OnceLock<Result<Crate, String>> = OnceLock::new();
  documented(&LIBRARY, LIBRARY_PACKAGE, "rustdoc-library")
}

fn fixtures() -> &'static Crate {
  static FIXTURES: OnceLock<Result<Crate, String>> = OnceLock::new();
  documented(&FIXTURES, FIXTURES_PACKAGE, "rustdoc-fixtures")
}

fn describe(problems: &[Problem]) -> String {
  if problems.is_empty() {
    return "(報告なし)".to_string();
  }
  problems.iter().map(ToString::to_string).collect::<Vec<_>>().join("\n")
}

/// 報告が空でなく、すべての報告が `path` の 1 か所で、`message` を含む報告がある。
fn assert_reported_only_at(problems: &[Problem], path: &str, message: &str) {
  assert!(!problems.is_empty(), "ずれを報告する: {path}");
  assert!(
    problems.iter().all(|problem| problem.path == path),
    "報告の場所は {path} だけ。実際の報告:\n{}",
    describe(problems)
  );
  assert!(
    problems.iter().any(|problem| problem.message.contains(message)),
    "メッセージに {message:?} を含む。実際の報告:\n{}",
    describe(problems)
  );
}

fn check_fixture(module: &str) -> Vec<Problem> {
  check_api(fixtures(), &[module], &surface(), &mapping())
}

// ---- 肯定 ----

#[test]
fn should_library_match_the_surface() {
  let problems = check_api(library(), &[], &surface(), &mapping());

  assert!(
    problems.is_empty(),
    "公開 API が形に合う。実際の報告:\n{}",
    describe(&problems)
  );
}

#[test]
fn should_baseline_fixture_match_the_surface() {
  let problems = check_fixture("baseline");

  assert!(
    problems.is_empty(),
    "正しい形の定義は報告 0 件。実際の報告:\n{}",
    describe(&problems)
  );
}

// ---- 否定 (形を 1 か所だけずらす) ----

#[test]
fn should_report_event_envelope_without_occurred_at() {
  let problems = check_fixture("missing_occurred_at");

  assert_reported_only_at(
    &problems,
    "event_envelope.occurred_at",
    "EventEnvelope has no method occurred_at (want fn occurred_at(&self) -> &chrono::DateTime<chrono::Utc>)",
  );
}

#[test]
fn should_report_renamed_manifest_getter() {
  let problems = check_fixture("renamed_manifest");

  assert_reported_only_at(
    &problems,
    "event_envelope.manifest",
    "EventEnvelope has no method manifest (want fn manifest(&self) -> &str)",
  );
}

#[test]
fn should_report_seq_nr_getter_returning_u32() {
  let problems = check_fixture("seq_nr_returns_u32");

  assert_reported_only_at(
    &problems,
    "event_envelope.seq_nr",
    "EventEnvelope::seq_nr returns u32, want SeqNr",
  );
}

#[test]
fn should_report_constructor_without_required_seq_nr() {
  let problems = check_fixture("constructor_missing_seq_nr");

  assert_reported_only_at(
    &problems,
    "event_envelope.constructor",
    "required element not accepted: seq_nr (SeqNr)",
  );
}

#[test]
fn should_report_events_operation_returning_payloads() {
  let problems = check_fixture("events_return_payloads");

  assert_reported_only_at(
    &problems,
    "store.get_events_by_id_since_seq_nr",
    "EventStore::get_events_by_id_since_seq_nr returns Result<Vec<Self::P>, EventStoreError>, \
     want Result<Vec<EventEnvelope<Self::AID, Self::P>>, EventStoreError>",
  );
}

#[test]
fn should_report_error_enum_without_storage() {
  let problems = check_fixture("missing_storage_error");

  assert_reported_only_at(
    &problems,
    "error_categories.storage",
    "EventStoreError has no variant Storage",
  );
}

#[test]
fn should_report_payload_bound_that_the_surface_does_not_allow() {
  let problems = check_fixture("store_payload_clone_bound");

  assert_reported_only_at(
    &problems,
    "store.type_parameters.P",
    "EventStore::P has bound Clone; allowed: Send, Sync, 'static",
  );
}

#[test]
fn should_report_data_type_parameter_bound_that_the_surface_does_not_allow() {
  let problems = check_fixture("snapshot_read_clone_bound");

  assert_reported_only_at(
    &problems,
    "snapshot_read.type_parameters.A",
    "SnapshotRead::A has bound Clone; allowed: Send, Sync, 'static",
  );
}

// ---- 対応表にない項目は飛ばさずに報告する ----

#[test]
fn should_report_surface_type_that_rs_json_does_not_map() {
  let mut mapping = mapping();
  mapping.types.remove("snapshot_read");

  let problems = check_api(fixtures(), &["baseline"], &surface(), &mapping);

  assert!(
    problems.iter().any(|problem| problem.path == "snapshot_read"
      && problem
        .message
        .contains("rs.json has no mapping for type snapshot_read")),
    "対応表にない型を報告する。実際の報告:\n{}",
    describe(&problems)
  );
}

#[test]
fn should_report_surface_operation_that_rs_json_does_not_map() {
  let mut mapping = mapping();
  mapping.store.operations.remove("persist_event");

  let problems = check_api(fixtures(), &["baseline"], &surface(), &mapping);

  assert_reported_only_at(
    &problems,
    "store.persist_event",
    "rs.json has no mapping for operation persist_event",
  );
}

#[test]
fn should_report_error_category_that_rs_json_does_not_map() {
  let mut mapping = mapping();
  mapping.error_categories.variants.remove("configuration");

  let problems = check_api(fixtures(), &["baseline"], &surface(), &mapping);

  assert_reported_only_at(
    &problems,
    "error_categories.configuration",
    "rs.json has no mapping for error category configuration",
  );
}

#[test]
fn should_report_value_type_that_rs_json_does_not_map() {
  let mut mapping = mapping();
  mapping.value_types.remove("timestamp");

  let problems = check_api(fixtures(), &["baseline"], &surface(), &mapping);

  assert!(
    problems.iter().any(|problem| problem.path == "value_types.timestamp"
      && problem
        .message
        .contains("rs.json has no mapping for value type timestamp")),
    "対応表にない値の種類を報告する。実際の報告:\n{}",
    describe(&problems)
  );
}
