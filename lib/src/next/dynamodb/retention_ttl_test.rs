use std::sync::atomic::{AtomicU64, Ordering};

use super::*;
use crate::next::dynamodb::clock::Clock;

#[derive(Debug)]
struct AdvancingClock(AtomicU64);

impl Clock for AdvancingClock {
  fn now_epoch_seconds(&self) -> u64 {
    self.0.fetch_add(1, Ordering::SeqCst)
  }
}

#[tokio::test]
async fn should_mark_selected_history_after_all_pages_using_each_mark_time() {
  for written_visible in [false, true] {
    let mut first = page(if written_visible { &[6, 5, 5] } else { &[5, 5] }).1;
    first["LastEvaluatedKey"] = history(5);
    let continuation = first["LastEvaluatedKey"].clone();
    let (mut store, script, _) = setup(
      DynamoDbOptions {
        retention: RetentionSettings::keep_latest(2).with_mode(RetentionMode::Ttl { grace_seconds: 60 }),
        ..Default::default()
      },
      vec![(200, first), page(&[4, 3]), (200, json!({})), (200, json!({}))],
    );
    store.clock = Arc::new(AdvancingClock(AtomicU64::new(4_102_444_800)));

    let receipt = store.retain_history_after_append(&aid(), 6).await;

    assert!(receipt.retention_failure.is_none());
    let inputs = script.inputs.lock().unwrap();
    assert_eq!(inputs.len(), 4);
    assert_eq!(inputs[1]["ExclusiveStartKey"], continuation);
    for query in &inputs[..2] {
      assert_eq!(query["IndexName"], "history");
      assert_eq!(query["ScanIndexForward"], false);
      assert_eq!(query["ConsistentRead"], false);
    }
    for (update, (number, expires)) in inputs[2..].iter().zip([(4, "4102444860"), (3, "4102444861")]) {
      assert_eq!(update["TableName"], "second");
      assert_eq!(
        update["Key"],
        json!({"aid": {"S": aid().as_str()}, "skey": {"N": number.to_string()}})
      );
      assert_eq!(
        update["UpdateExpression"],
        "SET #ttl = :expires REMOVE active_history_seq_nr"
      );
      assert_eq!(update["ConditionExpression"], "attribute_exists(active_history_seq_nr)");
      assert_eq!(update["ExpressionAttributeNames"], json!({"#ttl": "ttl"}));
      assert_eq!(update["ExpressionAttributeValues"], json!({":expires": {"N": expires}}));
    }
  }
}

#[tokio::test]
async fn should_send_zero_and_maximum_grace_as_overflow_free_decimal_numbers() {
  for (now, grace, expected) in [
    (4_102_444_800, 0, "4102444800"),
    (4_102_444_800, u64::MAX, "18446744077811996415"),
    (u64::MAX, u64::MAX, "36893488147419103230"),
  ] {
    let (mut store, script, _) = setup(
      DynamoDbOptions {
        retention: RetentionSettings::keep_latest(1).with_mode(RetentionMode::Ttl { grace_seconds: grace }),
        ..Default::default()
      },
      vec![page(&[1]), (200, json!({}))],
    );
    store.clock = Arc::new(AdvancingClock(AtomicU64::new(now)));

    let receipt = store.retain_history_after_append(&aid(), 2).await;

    assert!(receipt.retention_failure.is_none());
    assert_eq!(
      script.inputs.lock().unwrap()[1]["ExpressionAttributeValues"][":expires"],
      json!({"N": expected})
    );
  }
}

#[tokio::test]
async fn should_continue_after_typed_condition_failure_without_retrying_the_mark() {
  let (store, script, sleep) = setup(
    DynamoDbOptions {
      retention: RetentionSettings::keep_latest(1).with_mode(RetentionMode::Ttl { grace_seconds: 60 }),
      ..Default::default()
    },
    vec![
      page(&[3, 2, 1]),
      (
        400,
        json!({"__type": "ConditionalCheckFailedException", "Message": "already marked"}),
      ),
      (200, json!({})),
    ],
  );

  let receipt = store.retain_history_after_append(&aid(), 3).await;

  assert!(receipt.retention_failure.is_none());
  let inputs = script.inputs.lock().unwrap();
  assert_eq!(inputs.len(), 3);
  assert_eq!(inputs[1]["Key"]["skey"]["N"], "2");
  assert_eq!(inputs[2]["Key"]["skey"]["N"], "1");
  assert!(sleep.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn should_keep_mark_failure_phase_and_cause_in_the_receipt_and_stop() {
  let (store, script, _) = setup(
    DynamoDbOptions {
      retention: RetentionSettings::keep_latest(1).with_mode(RetentionMode::Ttl { grace_seconds: 60 }),
      ..Default::default()
    },
    vec![
      page(&[4, 3, 2, 1]),
      (200, json!({})),
      (
        400,
        json!({"__type": "ProvisionedThroughputExceededException", "Message": "RETENTION_ORIGINAL_CAUSE"}),
      ),
    ],
  );

  let failure = store
    .retain_history_after_append(&aid(), 4)
    .await
    .retention_failure
    .unwrap();

  assert_eq!(failure.aid, aid().as_str());
  assert_eq!(failure.seq_nr, 4);
  assert_eq!(failure.phase, "retention-mark");
  assert!(failure.error.contains("ProvisionedThroughputExceededException"));
  assert!(failure.error.contains("RETENTION_ORIGINAL_CAUSE"));
  assert_eq!(script.inputs.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn should_not_read_the_clock_or_send_a_mark_when_no_history_is_expired() {
  let clock = Arc::new(AdvancingClock(AtomicU64::new(0)));
  let (mut store, script, _) = setup(
    DynamoDbOptions {
      retention: RetentionSettings::keep_latest(2).with_mode(RetentionMode::Ttl { grace_seconds: 60 }),
      ..Default::default()
    },
    vec![page(&[1])],
  );
  store.clock = clock.clone();

  let receipt = store.retain_history_after_append(&aid(), 2).await;

  assert!(receipt.retention_failure.is_none());
  assert_eq!(script.inputs.lock().unwrap().len(), 1);
  assert_eq!(clock.0.load(Ordering::SeqCst), 0);
}

#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn should_share_the_injected_clock_with_store_clones() {
  let clock = Arc::new(AdvancingClock(AtomicU64::new(4_102_444_800)));
  let (store, script, _) = setup(
    DynamoDbOptions {
      retention: RetentionSettings::keep_latest(1).with_mode(RetentionMode::Ttl { grace_seconds: 60 }),
      ..Default::default()
    },
    vec![page(&[1]), (200, json!({})), page(&[2]), (200, json!({}))],
  );
  let store = store.with_clock_for_test(clock.clone());
  let cloned = store.clone();
  assert!(store
    .retain_history_after_append(&aid(), 2)
    .await
    .retention_failure
    .is_none());
  clock.0.store(4_102_445_800, Ordering::SeqCst);
  assert!(cloned
    .retain_history_after_append(&aid(), 3)
    .await
    .retention_failure
    .is_none());
  let inputs = script.inputs.lock().unwrap();
  assert_eq!(inputs[1]["ExpressionAttributeValues"][":expires"]["N"], "4102444860");
  assert_eq!(inputs[3]["ExpressionAttributeValues"][":expires"]["N"], "4102445860");
}
