use super::OperationNotifications;
use tracing::Instrument;

fn notification() {
  tracing::warn!(target: "event_store_adapter::retention", category = "retention-failure", "test notification");
}

#[test]
fn should_collect_only_retention_warn_notifications_in_the_operation() {
  let operation = OperationNotifications::begin("case", 1).unwrap();
  notification();
  {
    let span = operation.span();
    let _entered = span.enter();
    tracing::info!(target: "event_store_adapter::retention", category = "retention-failure", "wrong level");
    tracing::warn!(target: "other", category = "retention-failure", "wrong target");
    tracing::warn!(target: "event_store_adapter::retention", category = "other", "wrong category");
    notification();
    notification();
  }
  assert_eq!(operation.finish(), vec!["retention-failure"]);
  assert!(OperationNotifications::begin("case", 2).unwrap().finish().is_empty());
}

#[test]
fn should_separate_repeated_parallel_cases_and_collect_future_on_another_thread() {
  let first = OperationNotifications::begin("same-case", 1).unwrap();
  let second = OperationNotifications::begin("same-case", 1).unwrap();
  let other = OperationNotifications::begin("other-case", 1).unwrap();
  let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
  let worker_barrier = barrier.clone();
  let future = async move {
    worker_barrier.wait();
    notification();
  }
  .instrument(first.span());
  let first_worker = std::thread::spawn(move || {
    tokio::runtime::Builder::new_current_thread()
      .build()
      .unwrap()
      .block_on(future);
  });
  let second_future = async move {
    barrier.wait();
  }
  .instrument(second.span());
  let second_worker = std::thread::spawn(move || {
    tokio::runtime::Builder::new_current_thread()
      .build()
      .unwrap()
      .block_on(second_future);
  });
  first_worker.join().unwrap();
  second_worker.join().unwrap();
  assert_eq!(first.finish(), vec!["retention-failure"]);
  assert!(second.finish().is_empty());
  assert!(other.finish().is_empty());
}

#[test]
fn should_remove_collection_when_operation_is_cancelled() {
  let cancelled = OperationNotifications::begin("cancelled", 1).unwrap();
  let collected = cancelled.collected.clone();
  let collection_id = cancelled.collection_id;
  let span = cancelled.span();
  drop(cancelled);
  assert!(!collected.lock().unwrap().contains_key(&collection_id));
  let next = OperationNotifications::begin("cancelled", 1).unwrap();
  {
    let _entered = span.enter();
    notification();
  }
  assert!(next.finish().is_empty());
}

fn journal_notification() {
  tracing::warn!(
    target: "event_store_adapter::journal",
    category = "journal-write-failure",
    "test journal notification"
  );
}

#[test]
fn should_collect_journal_write_failure_warn_notifications_together_with_retention_failures() {
  let operation = OperationNotifications::begin("case", 1).unwrap();
  {
    let span = operation.span();
    let _entered = span.enter();
    journal_notification();
    journal_notification();
    notification();
  }
  assert_eq!(operation.finish(), vec!["journal-write-failure", "retention-failure"]);
}

#[test]
fn should_not_collect_a_notification_whose_target_level_and_category_do_not_form_a_known_pair() {
  let operation = OperationNotifications::begin("case", 1).unwrap();
  {
    let span = operation.span();
    let _entered = span.enter();
    tracing::info!(target: "event_store_adapter::journal", category = "journal-write-failure", "wrong level");
    tracing::warn!(target: "other", category = "journal-write-failure", "wrong target");
    tracing::warn!(target: "event_store_adapter::journal", category = "retention-failure", "wrong pair");
    tracing::warn!(target: "event_store_adapter::retention", category = "journal-write-failure", "wrong pair");
    tracing::warn!(target: "event_store_adapter::journal", category = "other", "wrong category");
  }
  assert!(operation.finish().is_empty());
}
