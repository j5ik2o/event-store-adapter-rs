//! フックと直列化への障害の差し込み（`HookFaults`）と、保存先ごとの対応範囲の判定の試験。
//! `hook_faults.rs` の末尾から `#[cfg(test)] #[path = "hook_faults_test.rs"] mod tests;` で結ぶ。

use event_store_adapter_rs::error::{EventStoreError, StorageOperation};
use serde_json::{json, Value};

use super::*;

fn fault(operation: u32, phase: &str, injection: &str) -> Value {
  json!({
    "operation": operation,
    "phase": phase,
    "kind": "storage-error",
    "injection": injection,
    "repeat": {"mode": "count", "count": 1},
    "details": {"message": "INJECTED"}
  })
}

/// `bigtable` だけを対象にする、2 手順の場面の障害の計画を登録する。
fn bigtable_plan(faults: Vec<Value>) -> FaultPlan {
  FaultPlan::register(&json!({"backends": ["bigtable"], "steps": [{}, {}], "faults": faults})).expect("宣言は正しい")
}

fn hook_faults_of(plan: &FaultPlan, operation: u32) -> HookFaults {
  HookFaults::new(plan.begin_operation(operation))
}

fn is_append_storage_error(result: &Result<(), EventStoreError>) -> bool {
  matches!(
    result,
    Err(EventStoreError::Storage {
      operation: StorageOperation::Append,
      ..
    })
  )
}

// Bigtable の書き出しの失敗（BT-7）: 段階 `journal-write-*` の storage-error（replace-request）と、
// `commit` の storage-error（replace-response）は、フックで表せる。
#[test]
fn should_supports_all_bigtable_accept_journal_write_failures_and_a_commit_response_loss() {
  let plan = bigtable_plan(vec![
    fault(1, "journal-write-previous", "replace-request"),
    fault(1, "journal-write-tail", "replace-request"),
    fault(2, "commit", "replace-response"),
    fault(2, "commit", "replace-request"),
  ]);

  assert!(supports_all_bigtable(&plan));
}

// 共通の場面の障害（直列化・復元・確定・読み取り・保持）は、Bigtable でもメモリ・SQLite と同じようにフックで表せる。
#[test]
fn should_supports_all_bigtable_accept_every_fault_that_the_common_hooks_can_express() {
  let mut serialization = fault(1, "serialize-event", "replace-request");
  serialization["kind"] = json!("serialization-error");
  let mut response_plan = fault(2, "retention-query", "replace-response");
  response_plan["kind"] = json!("sdk-response");
  response_plan["details"] = json!({"history_pages": [[3, 2, 1]]});
  let plan = bigtable_plan(vec![
    serialization,
    fault(1, "commit", "replace-request"),
    fault(1, "read-events", "replace-request"),
    fault(1, "read-snapshot", "replace-request"),
    fault(2, "retention-query", "replace-request"),
    response_plan,
    fault(2, "retention-delete", "replace-request"),
  ]);

  assert!(
    supports_all(&plan),
    "試験の前提: メモリと SQLite がフックで表せる障害だけを並べた"
  );
  assert!(supports_all_bigtable(&plan));
}

// 応答の消失（replace-response）と書き出しの段階は Bigtable だけが表せる。メモリ・SQLite の判定は変えない。
#[test]
fn should_supports_all_keep_rejecting_a_commit_response_loss_that_only_bigtable_can_express() {
  let plan = bigtable_plan(vec![fault(1, "commit", "replace-response")]);

  assert!(!supports_all(&plan), "メモリと SQLite は応答の消失を表せない");
}

#[test]
fn should_supports_all_bigtable_reject_a_fault_that_the_hooks_cannot_express() {
  let wrong_injection = bigtable_plan(vec![fault(1, "journal-write-tail", "replace-response")]);
  let mut serialization = fault(1, "journal-write-previous", "replace-request");
  serialization["kind"] = json!("serialization-error");
  let wrong_kind = bigtable_plan(vec![serialization]);
  let store_creation = bigtable_plan(vec![fault(0, "commit", "replace-request")]);

  assert!(!supports_all_bigtable(&wrong_injection));
  assert!(!supports_all_bigtable(&wrong_kind));
  assert!(
    !supports_all_bigtable(&store_creation),
    "ストアの生成（操作 0）の障害は表せない"
  );
}

// 応答の消失を replace-request として扱わない: 要求の差し込み（before_commit）では消費されず、応答の差し込みだけが適用する。
#[test]
fn should_inject_leave_a_replace_response_fault_unconsumed_and_inject_response_apply_it_once() {
  let plan = bigtable_plan(vec![fault(1, "commit", "replace-response")]);
  let hooks = hook_faults_of(&plan, 1);

  let before_commit = hooks.inject(Phase::Commit);
  let first_response = hooks.inject_response(Phase::Commit);
  let second_response = hooks.inject_response(Phase::Commit);

  assert!(
    before_commit.is_ok(),
    "要求の差し込みは応答の消失を適用しない: {before_commit:?}"
  );
  assert!(is_append_storage_error(&first_response), "{first_response:?}");
  assert!(
    second_response.is_ok(),
    "宣言した回数（1 回）で尽きる: {second_response:?}"
  );
}

#[test]
fn should_inject_response_leave_a_replace_request_fault_unconsumed() {
  let plan = bigtable_plan(vec![fault(1, "commit", "replace-request")]);
  let hooks = hook_faults_of(&plan, 1);

  let response = hooks.inject_response(Phase::Commit);
  let request = hooks.inject(Phase::Commit);

  assert!(
    response.is_ok(),
    "応答の差し込みは要求の置き換えを適用しない: {response:?}"
  );
  assert!(is_append_storage_error(&request), "{request:?}");
}

// 書き出しの段階の誤りは、追記の保存先エラーに対応付ける（対応しない段階の panic に入らない）。
#[test]
fn should_inject_map_journal_write_phases_to_append_storage_errors() {
  let plan = bigtable_plan(vec![
    fault(1, "journal-write-previous", "replace-request"),
    fault(1, "journal-write-tail", "replace-request"),
  ]);
  let hooks = hook_faults_of(&plan, 1);

  let previous = hooks.inject(Phase::JournalWritePrevious);
  let tail = hooks.inject(Phase::JournalWriteTail);

  assert!(is_append_storage_error(&previous), "{previous:?}");
  assert!(is_append_storage_error(&tail), "{tail:?}");
}

// ---------------------------------------------------------------------------
// Spanner の障害: コミットの中断（SPN-7・transaction-aborted）と、コミットの応答の消失（SPN-8）
// ---------------------------------------------------------------------------

fn aborted_fault(operation: u32, times: u32) -> Value {
  json!({
    "operation": operation,
    "phase": "commit",
    "kind": "transaction-aborted",
    "injection": "replace-request",
    "repeat": {"mode": "count", "count": times},
    "details": {"message": "STORAGE_SENTINEL"}
  })
}

/// `spanner` だけを対象にする、2 手順の場面の障害の計画を登録する。
fn spanner_plan(faults: Vec<Value>) -> FaultPlan {
  FaultPlan::register(&json!({"backends": ["spanner"], "steps": [{}, {}], "faults": faults})).expect("宣言は正しい")
}

// Spanner が表せる障害: コミットの中断と、コミットの応答の消失は、共通の障害に加えて表せる。
#[test]
fn should_supports_all_spanner_accept_aborted_commits_and_a_commit_response_loss() {
  let plan = spanner_plan(vec![
    aborted_fault(2, 2),
    fault(1, "commit", "replace-response"),
    fault(1, "commit", "replace-request"),
    fault(1, "read-events", "replace-request"),
  ]);

  assert!(supports_all_spanner(&plan));
}

// コミットの中断は Spanner だけが表せる。メモリ・SQLite・Bigtable の判定は変えない。
#[test]
fn should_supports_all_keep_rejecting_aborted_commits_that_only_spanner_can_express() {
  let plan = spanner_plan(vec![aborted_fault(2, 1)]);

  assert!(!supports_all(&plan), "メモリと SQLite はコミットの中断を表せない");
  assert!(!supports_all_bigtable(&plan), "Bigtable はコミットの中断を表せない");
}

#[test]
fn should_supports_all_spanner_reject_a_fault_in_store_creation() {
  let plan = spanner_plan(vec![aborted_fault(0, 1)]);

  assert!(!supports_all_spanner(&plan), "ストアの生成（操作 0）の障害は表せない");
}

// コミットの中断は、コミットを送る前の差し込み（要求の置き換え）で、宣言した回数だけ中断として返す。
#[test]
fn should_inject_commit_request_report_an_abort_for_the_declared_number_of_times_and_then_proceed() {
  let plan = spanner_plan(vec![aborted_fault(2, 2)]);
  let hooks = hook_faults_of(&plan, 2);

  let first = hooks.inject_commit_request();
  let second = hooks.inject_commit_request();
  let third = hooks.inject_commit_request();

  assert!(matches!(first, CommitInjection::Aborted), "{first:?}");
  assert!(matches!(second, CommitInjection::Aborted), "{second:?}");
  assert!(
    matches!(third, CommitInjection::Proceed),
    "宣言した回数（2 回）で尽きる: {third:?}"
  );
}

#[test]
fn should_inject_commit_request_proceed_when_no_fault_is_declared_for_the_operation() {
  let plan = spanner_plan(vec![aborted_fault(2, 1)]);
  let hooks = hook_faults_of(&plan, 1);

  let injection = hooks.inject_commit_request();

  assert!(matches!(injection, CommitInjection::Proceed), "{injection:?}");
}

// 中断ではない storage-error の要求の置き換えは、再試行の対象にならない失敗として返す。
#[test]
fn should_inject_commit_request_report_a_storage_error_as_a_failure_and_not_as_an_abort() {
  let plan = spanner_plan(vec![fault(1, "commit", "replace-request")]);
  let hooks = hook_faults_of(&plan, 1);

  let injection = hooks.inject_commit_request();

  assert!(
    matches!(
      &injection,
      CommitInjection::Failed(EventStoreError::Storage {
        operation: StorageOperation::Append,
        ..
      })
    ),
    "{injection:?}"
  );
}

// 応答の消失（replace-response）と中断は互いの差し込みで消費されない。
#[test]
fn should_inject_commit_request_leave_a_replace_response_fault_for_inject_response() {
  let plan = spanner_plan(vec![fault(1, "commit", "replace-response")]);
  let hooks = hook_faults_of(&plan, 1);

  let request = hooks.inject_commit_request();
  let response = hooks.inject_response(Phase::Commit);

  assert!(matches!(request, CommitInjection::Proceed), "{request:?}");
  assert!(is_append_storage_error(&response), "{response:?}");
}

#[test]
fn should_inject_response_leave_an_aborted_commit_for_inject_commit_request() {
  let plan = spanner_plan(vec![aborted_fault(1, 1)]);
  let hooks = hook_faults_of(&plan, 1);

  let response = hooks.inject_response(Phase::Commit);
  let request = hooks.inject_commit_request();

  assert!(response.is_ok(), "応答の差し込みは中断を適用しない: {response:?}");
  assert!(matches!(request, CommitInjection::Aborted), "{request:?}");
}
