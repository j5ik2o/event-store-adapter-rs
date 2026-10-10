use std::sync::{Arc, Mutex};

use aws_sdk_dynamodb::config::{AsyncSleep, BehaviorVersion, Credentials, Region, Sleep};

use super::*;

fn tables() -> DynamoDbTables {
  DynamoDbTables {
    journal_table_name: "first-table".into(),
    snapshot_table_name: "second-table".into(),
    head_table_name: "third-table".into(),
    snapshot_history_index_name: "history-index".into(),
  }
}

fn configurations(tables: &DynamoDbTables) -> HashMap<String, Item> {
  initial_request(tables)
    .unwrap()
    .into_iter()
    .map(|(table, attributes)| {
      let mut item = attributes.keys()[0].clone();
      item.insert("store_id".into(), AttributeValue::S("store-123".into()));
      item.insert("layout_version".into(), AttributeValue::N("1".into()));
      (table, item)
    })
    .collect()
}

fn assert_storage(error: EventStoreError) {
  assert!(matches!(
    error,
    EventStoreError::Storage {
      operation: StorageOperation::ReadConfiguration,
      ..
    }
  ));
}

#[test]
fn should_define_the_specified_defaults_and_three_strong_keys() {
  let options = DynamoDbOptions::default();
  assert_eq!(options.retention, crate::retention::RetentionSettings::current_only());
  assert_eq!(options.unprocessed_retry_limit, 10);
  assert_eq!(options.unprocessed_retry_initial_delay, Duration::from_millis(50));
  assert_eq!(options.unprocessed_retry_max_delay, Duration::from_secs(2));
  let tables = tables();
  let request = initial_request(&tables).unwrap();
  assert_eq!(request.len(), 3);
  for (table, sort) in [
    (&tables.journal_table_name, Some("seq_nr")),
    (&tables.snapshot_table_name, Some("skey")),
    (&tables.head_table_name, None),
  ] {
    let attributes = &request[table];
    assert_eq!(attributes.consistent_read(), Some(true));
    assert_eq!(attributes.keys().len(), 1);
    let key = &attributes.keys()[0];
    assert_eq!(key["aid"], AttributeValue::S("__config__".into()));
    assert_eq!(key.len(), if sort.is_some() { 2 } else { 1 });
    if let Some(sort) = sort {
      assert_eq!(key[sort], AttributeValue::N("0".into()));
    }
  }
}

#[test]
fn should_compare_numerical_zero_and_one_without_rounding() {
  for number in ["1", "1.0", "10e-1", ".01e2", "+1.000", "0.100000e1"] {
    assert!(number_is(&AttributeValue::N(number.into()), 1).unwrap(), "{number}");
  }
  for number in [
    "2",
    "-1",
    "0",
    "1.0000000000000000000000000000000000001",
    "0.9999999999999999999999999999999999999",
    "1e125",
  ] {
    assert!(!number_is(&AttributeValue::N(number.into()), 1).unwrap(), "{number}");
  }
  for number in ["0", "-0.00e2", "0e-20"] {
    assert!(number_is(&AttributeValue::N(number.into()), 0).unwrap());
  }
  assert!(!number_is(&AttributeValue::N("1".into()), 0).unwrap());
  for number in ["", "-", "1e", "1e0e0", "1.2.3", "NaN"] {
    assert_storage(number_is(&AttributeValue::N(number.into()), 1).unwrap_err());
  }
  assert_storage(number_is(&AttributeValue::S("1".into()), 1).unwrap_err());
}

#[test]
fn should_accumulate_responses_and_normalize_only_pending_keys_to_strong_reads() {
  let tables = tables();
  let request = initial_request(&tables).unwrap();
  let items = configurations(&tables);
  let mut accumulated = HashMap::new();
  let output = BatchGetItemOutput::builder()
    .responses(
      &tables.journal_table_name,
      vec![items[&tables.journal_table_name].clone()],
    )
    .unprocessed_keys(
      &tables.head_table_name,
      KeysAndAttributes::builder()
        .set_keys(Some(request[&tables.head_table_name].keys().to_vec()))
        .consistent_read(false)
        .build()
        .unwrap(),
    )
    .build();
  let pending = accumulate(&mut accumulated, &request, output).unwrap();
  assert_eq!(accumulated.len(), 1);
  assert_eq!(pending.len(), 1);
  assert_eq!(pending[&tables.head_table_name].consistent_read(), Some(true));
  assert_eq!(
    pending[&tables.head_table_name].keys(),
    request[&tables.head_table_name].keys()
  );
  let next = BatchGetItemOutput::builder()
    .responses(&tables.head_table_name, vec![items[&tables.head_table_name].clone()])
    .build();
  assert!(accumulate(&mut accumulated, &pending, next).unwrap().is_empty());
  assert_eq!(accumulated.len(), 2);
}

#[test]
fn should_reject_unrequested_duplicate_or_conflicting_response_keys() {
  let tables = tables();
  let request = initial_request(&tables).unwrap();
  let item = configurations(&tables)[&tables.journal_table_name].clone();
  let mut wrong_key = item.clone();
  wrong_key.insert("seq_nr".into(), AttributeValue::N("1".into()));
  let mut missing_key = item.clone();
  missing_key.remove("aid");
  for output in [
    BatchGetItemOutput::builder()
      .responses("unknown", vec![item.clone()])
      .build(),
    BatchGetItemOutput::builder()
      .responses(&tables.journal_table_name, vec![item.clone(), item.clone()])
      .build(),
    BatchGetItemOutput::builder()
      .responses(&tables.journal_table_name, vec![wrong_key])
      .build(),
    BatchGetItemOutput::builder()
      .responses(&tables.journal_table_name, vec![missing_key])
      .build(),
    BatchGetItemOutput::builder()
      .responses(&tables.journal_table_name, vec![item.clone()])
      .unprocessed_keys(&tables.journal_table_name, request[&tables.journal_table_name].clone())
      .build(),
    BatchGetItemOutput::builder()
      .unprocessed_keys("unknown", request[&tables.head_table_name].clone())
      .build(),
    BatchGetItemOutput::builder()
      .unprocessed_keys(
        &tables.head_table_name,
        KeysAndAttributes::builder()
          .keys(HashMap::from([("aid".into(), AttributeValue::S("other".into()))]))
          .build()
          .unwrap(),
      )
      .build(),
    BatchGetItemOutput::builder()
      .unprocessed_keys(
        &tables.head_table_name,
        KeysAndAttributes::builder()
          .keys(request[&tables.head_table_name].keys()[0].clone())
          .keys(request[&tables.head_table_name].keys()[0].clone())
          .build()
          .unwrap(),
      )
      .build(),
  ] {
    assert_storage(accumulate(&mut HashMap::new(), &request, output).unwrap_err());
  }
}

#[test]
fn should_reconcile_empty_matching_and_each_partial_configuration() {
  let tables = tables();
  assert_eq!(
    reconcile(&tables, &HashMap::new()).unwrap(),
    ConfigurationRead::CreationRequired
  );
  let items = configurations(&tables);
  assert_eq!(
    reconcile(&tables, &items).unwrap(),
    ConfigurationRead::Matched {
      store_id: "store-123".into()
    }
  );
  let names = [
    &tables.journal_table_name,
    &tables.snapshot_table_name,
    &tables.head_table_name,
  ];
  for mask in 1..7 {
    let partial = names
      .iter()
      .enumerate()
      .filter(|(index, _)| mask & (1 << index) != 0)
      .map(|(_, name)| ((*name).clone(), items[*name].clone()))
      .collect();
    assert!(matches!(
      reconcile(&tables, &partial),
      Err(EventStoreError::Configuration {
        reason: ConfigurationReason::PartialDynamoDbConfiguration
      })
    ));
  }
  let malformed_partial = HashMap::from([(tables.journal_table_name.clone(), Item::new())]);
  assert!(matches!(
    reconcile(&tables, &malformed_partial),
    Err(EventStoreError::Configuration {
      reason: ConfigurationReason::PartialDynamoDbConfiguration
    })
  ));
}

#[test]
fn should_reject_mismatched_and_malformed_attributes_for_each_table() {
  let tables = tables();
  for table in [
    &tables.journal_table_name,
    &tables.snapshot_table_name,
    &tables.head_table_name,
  ] {
    for (attribute, value, reason) in [
      (
        "store_id",
        AttributeValue::S("different".into()),
        ConfigurationReason::DynamoDbStoreIdMismatch,
      ),
      (
        "layout_version",
        AttributeValue::N("2".into()),
        ConfigurationReason::UnsupportedDynamoDbLayoutVersion,
      ),
    ] {
      let mut items = configurations(&tables);
      items.get_mut(table).unwrap().insert(attribute.into(), value);
      assert!(
        matches!(reconcile(&tables, &items), Err(EventStoreError::Configuration { reason: actual }) if actual == reason)
      );
    }
    for attribute in ["store_id", "layout_version"] {
      let mut items = configurations(&tables);
      items.get_mut(table).unwrap().remove(attribute);
      assert_storage(reconcile(&tables, &items).unwrap_err());
      items
        .get_mut(table)
        .unwrap()
        .insert(attribute.into(), AttributeValue::Bool(true));
      assert_storage(reconcile(&tables, &items).unwrap_err());
    }
  }
}

#[derive(Debug, Clone, Default)]
struct SdkSleeper(Arc<Mutex<Vec<Duration>>>);

impl AsyncSleep for SdkSleeper {
  fn sleep(&self, delay: Duration) -> Sleep {
    self.0.lock().unwrap().push(delay);
    Sleep::new(async {})
  }
}

#[tokio::test]
async fn should_forward_waits_to_the_sdk_sleep_implementation() {
  let sleeper = SdkSleeper::default();
  RetryWait::Sdk(SharedAsyncSleep::new(sleeper.clone()))
    .sleep(Duration::from_millis(50))
    .await;
  assert_eq!(*sleeper.0.lock().unwrap(), vec![Duration::from_millis(50)]);
}

#[tokio::test]
async fn should_reject_each_duplicate_table_configuration_before_transfer() {
  let sleeper = SdkSleeper::default();
  let client = Client::from_conf(
    aws_sdk_dynamodb::Config::builder()
      .behavior_version(BehaviorVersion::latest())
      .region(Region::new("us-east-1"))
      .credentials_provider(Credentials::new("x", "x", None, None, "test"))
      .endpoint_url("http://127.0.0.1:1")
      .sleep_impl(sleeper.clone())
      .build(),
  );
  let valid = tables();
  for duplicated in [
    DynamoDbTables {
      journal_table_name: valid.head_table_name.clone(),
      snapshot_table_name: valid.head_table_name.clone(),
      ..valid.clone()
    },
    DynamoDbTables {
      snapshot_table_name: valid.journal_table_name.clone(),
      ..valid.clone()
    },
    DynamoDbTables {
      head_table_name: valid.journal_table_name.clone(),
      ..valid.clone()
    },
    DynamoDbTables {
      head_table_name: valid.snapshot_table_name.clone(),
      ..valid.clone()
    },
  ] {
    let result = read_configuration(&client, &duplicated, &DynamoDbOptions::default()).await;
    assert!(
      matches!(
        &result,
        Err(EventStoreError::Configuration {
          reason: ConfigurationReason::DuplicateDynamoDbTableNames
        })
      ),
      "{duplicated:?}: {result:?}"
    );
    assert!(sleeper.0.lock().unwrap().is_empty());
  }
}

#[tokio::test]
async fn should_reject_missing_stored_sleeper_and_invalid_retention_before_transfer() {
  let client = Client::from_conf(
    aws_sdk_dynamodb::Config::builder()
      .behavior_version(BehaviorVersion::latest())
      .region(Region::new("us-east-1"))
      .credentials_provider(Credentials::new("x", "x", None, None, "test"))
      .endpoint_url("http://127.0.0.1:1")
      .build(),
  );
  assert!(client.config().sleep_impl().is_none(), "保存済みSDK設定を実観測");
  assert!(matches!(
    read_configuration(&client, &tables(), &DynamoDbOptions::default()).await,
    Err(EventStoreError::Configuration {
      reason: ConfigurationReason::MissingRetrySleeper
    })
  ));
  let options = DynamoDbOptions {
    retention: crate::retention::RetentionSettings::keep_latest(0),
    ..Default::default()
  };
  assert!(matches!(
    read_with_wait(
      &client,
      &tables(),
      &options,
      RetryWait::Sdk(SharedAsyncSleep::new(SdkSleeper::default()))
    )
    .await,
    Err(EventStoreError::Configuration {
      reason: ConfigurationReason::KeepSnapshotCountZero
    })
  ));
}
