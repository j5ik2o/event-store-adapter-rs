use super::*;
use crate::case::operation_context;
use crate::fault::FaultPlan;

#[tokio::test]
async fn should_record_mutable_clock_waits_and_native_serializer_faults() {
  let clock = MutableClock(AtomicU64::new(10));
  assert_eq!(clock.now_epoch_seconds(), 10);
  clock.0.store(20, Ordering::SeqCst);
  assert_eq!(clock.now_epoch_seconds(), 20);
  let waits = Waits::default();
  waits.sleep(Duration::from_millis(50)).await;
  assert_eq!(*waits.0.lock().unwrap(), vec![50]);
  let transport = FaultTransport::new(RequestLayout::new("j", "s", "h", "i").unwrap());
  let plan=FaultPlan::register(&json!({"steps":[{}],"faults":[{"operation":1,"phase":"serialize-event","kind":"serialization-error","injection":"replace-request","repeat":{"mode":"count","count":1},"details":{"message":"failure"}}]})).unwrap();
  let guard = transport.begin_operation(&plan, 1).unwrap();
  let serializer = FaultSerializer(transport);
  let error = EventSerializer::serialize(&serializer, &json!({})).unwrap_err();
  assert_eq!(case::error_value(&error)["category"], "serialization");
  assert_eq!(case::error_value(&error)["phase"], "serialize-event");
  let bytes = EventSerializer::serialize(&serializer, &json!({"x":1})).unwrap();
  assert_eq!(
    EventSerializer::deserialize(&serializer, &bytes).unwrap(),
    json!({"x":1})
  );
  let bytes = SnapshotSerializer::serialize(&serializer, &json!([1])).unwrap();
  assert_eq!(
    SnapshotSerializer::deserialize(&serializer, &bytes).unwrap(),
    json!([1])
  );
  let report = guard.finish();
  assert!(report.unfired.is_empty());
  assert_eq!(report.applications[0].applied, 1);
  assert!(report.requests.is_empty());
}

#[test]
fn should_bind_observers_to_actual_operation_arguments_and_preserve_unfired_errors() {
  let body = json!({"fixtures":{"events":{"e4":{"aggregate_id":{"type_name":"Order","value":"9"},"seq_nr":4}}}});
  let (id, seq) = operation_context(&body, &json!({"arguments":{"event":"e4"}})).unwrap();
  assert_eq!(id.unwrap().value, "9");
  assert_eq!(seq, Some(4));
  let (id, seq) = operation_context(
    &body,
    &json!({"arguments":{"aggregate_id":{"type_name":"Order","value":"9"},"seq_nr":0}}),
  )
  .unwrap();
  assert_eq!(id.unwrap().type_name, "Order");
  assert_eq!(seq, Some(0));
  assert!(operation_context(&body, &json!({"arguments":{"event":"missing"}})).is_err());
  let transport = FaultTransport::new(RequestLayout::new("j", "s", "h", "i").unwrap());
  let plan=FaultPlan::register(&json!({"steps":[{}],"faults":[{"operation":1,"phase":"commit","kind":"storage-error","injection":"replace-request","repeat":{"mode":"count","count":2},"details":{}}]})).unwrap();
  let report = transport.begin_operation(&plan, 1).unwrap().finish();
  let outcome = unfired(1, &report, json!({"result":"success"}));
  assert!(matches!(outcome,CaseOutcome::Failed {unfired_faults,..} if unfired_faults.len()==1));
}
