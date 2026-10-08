//! 内部設定読取だけを、型付きSDKと固定DynamoDB Local 3.3.1の境界で検証する。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aws_sdk_dynamodb::config::{
  AsyncSleep, BehaviorVersion, Credentials, Region, Sleep, StalledStreamProtectionConfig,
};
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client;
use aws_smithy_runtime_api::client::http::{
  http_client_fn, HttpClient, HttpConnector, HttpConnectorFuture, SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::HttpRequest;
use aws_smithy_types::timeout::TimeoutConfig;
use event_store_adapter_conformance_rs::fault::{FaultPlan, Phase};
use event_store_adapter_conformance_rs::target_dynamodb::{FaultTransport, OperationReport, RequestLayout};
use event_store_adapter_rs::next::dynamodb::{
  read_configuration_for_test, ConfigurationRead, DynamoDbOptions, DynamoDbTables, RetrySleeper,
};
use event_store_adapter_rs::next::error::{ConfigurationReason, EventStoreError, StorageOperation};
use event_store_adapter_rs::next::retention::{RetentionMode, RetentionSettings};
use event_store_adapter_test_utils_rs::{docker, dynamodb};
use serde_json::{json, Value};
use testcontainers::{ContainerAsync, GenericImage};

type Item = HashMap<String, AttributeValue>;

#[derive(Debug, Clone, Default)]
struct Waits(Arc<Mutex<Vec<Duration>>>);

#[async_trait::async_trait]
impl RetrySleeper for Waits {
  async fn sleep(&self, delay: Duration) {
    self.0.lock().unwrap().push(delay);
  }
}

impl AsyncSleep for Waits {
  fn sleep(&self, delay: Duration) -> Sleep {
    let waits = self.clone();
    Sleep::new(async move {
      waits.0.lock().unwrap().push(delay);
    })
  }
}

#[derive(Debug, Clone)]
struct Transfer {
  uri: String,
  body: Value,
  succeeded: bool,
}

#[derive(Debug, Clone)]
struct ObservedConnector {
  upstream: SharedHttpConnector,
  transfers: Arc<Mutex<Vec<Transfer>>>,
}

impl HttpConnector for ObservedConnector {
  fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
    let uri = request.uri().to_string();
    let body = serde_json::from_slice(request.body().bytes().unwrap()).unwrap();
    let upstream = self.upstream.clone();
    let transfers = self.transfers.clone();
    HttpConnectorFuture::new(async move {
      let response = upstream.call(request).await;
      transfers.lock().unwrap().push(Transfer {
        uri,
        body,
        succeeded: response.as_ref().is_ok_and(|response| response.status().is_success()),
      });
      response
    })
  }
}

struct Fixture {
  container: ContainerAsync<GenericImage>,
  raw: Client,
  client: Client,
  transport: FaultTransport,
  tables: DynamoDbTables,
  endpoint: String,
  transfers: Arc<Mutex<Vec<Transfer>>>,
  sdk_waits: Waits,
  hook_waits: Waits,
}

fn client_builder(endpoint: &str) -> aws_sdk_dynamodb::config::Builder {
  aws_sdk_dynamodb::Config::builder()
    .behavior_version(BehaviorVersion::latest())
    .region(Region::new("us-west-1"))
    .credentials_provider(Credentials::new("x", "x", None, None, "local-test"))
    .endpoint_url(endpoint)
    .timeout_config(TimeoutConfig::disabled())
    // SDKの通信監視も同じsleep_implを使う。設定再要求の待ちだけを直接記録する。
    .stalled_stream_protection(StalledStreamProtectionConfig::disabled())
}

impl Fixture {
  async fn new(stored_sleeper: bool) -> Self {
    let container = docker::dynamodb_local().await.unwrap();
    let port = container.get_host_port_ipv4(docker::DYNAMODB_LOCAL_PORT).await.unwrap();
    let endpoint = format!("http://127.0.0.1:{port}");
    let raw = dynamodb::create_dynamodb_local_client(port);
    let names = dynamodb::TableNames {
      journal: "config-first".into(),
      snapshot: "config-second".into(),
      head: "config-third".into(),
      snapshot_history_index: "config-history".into(),
    };
    dynamodb::create_tables(&raw, &names, false).await.unwrap();
    let tables = DynamoDbTables {
      journal_table_name: names.journal,
      snapshot_table_name: names.snapshot,
      head_table_name: names.head,
      snapshot_history_index_name: names.snapshot_history_index,
    };
    let transport = transport(&tables);
    let transfers = Arc::new(Mutex::new(Vec::new()));
    let sdk_waits = Waits::default();
    let mut builder = client_builder(&endpoint);
    if stored_sleeper {
      builder = builder.sleep_impl(sdk_waits.clone());
    }
    let client = transport.client(builder, observed_http(transfers.clone()));
    assert_eq!(client.config().sleep_impl().is_some(), stored_sleeper);
    Self {
      container,
      raw,
      client,
      transport,
      tables,
      endpoint,
      transfers,
      sdk_waits,
      hook_waits: Waits::default(),
    }
  }

  fn names(&self) -> [&str; 3] {
    [
      &self.tables.journal_table_name,
      &self.tables.snapshot_table_name,
      &self.tables.head_table_name,
    ]
  }

  async fn seed(&self, items: [Option<Item>; 3]) {
    for (index, (table, item)) in self.names().into_iter().zip(items).enumerate() {
      self
        .raw
        .delete_item()
        .table_name(table)
        .set_key(Some(config_key(index)))
        .send()
        .await
        .unwrap();
      if let Some(item) = item {
        self
          .raw
          .put_item()
          .table_name(table)
          .set_item(Some(item))
          .send()
          .await
          .unwrap();
      }
    }
  }

  async fn read(
    &self,
    options: &DynamoDbOptions,
    faults: Vec<Value>,
    hook: bool,
  ) -> (Result<ConfigurationRead, EventStoreError>, OperationReport) {
    self.read_with_tables(&self.tables, options, faults, hook).await
  }

  async fn read_with_tables(
    &self,
    tables: &DynamoDbTables,
    options: &DynamoDbOptions,
    faults: Vec<Value>,
    hook: bool,
  ) -> (Result<ConfigurationRead, EventStoreError>, OperationReport) {
    self.transfers.lock().unwrap().clear();
    self.sdk_waits.0.lock().unwrap().clear();
    self.hook_waits.0.lock().unwrap().clear();
    let plan = FaultPlan::register(&json!({"steps": [{}], "faults": faults})).unwrap();
    let operation = self.transport.begin_operation(&plan, 0).unwrap();
    let sleeper = hook.then(|| Arc::new(self.hook_waits.clone()) as Arc<dyn RetrySleeper>);
    let result = read_configuration_for_test(&self.client, tables, options, sleeper).await;
    (result, operation.finish())
  }

  fn assert_requests(&self, report: &OperationReport, keys: &[&[usize]]) {
    assert!(report.unfired.is_empty(), "{report:?}");
    assert_eq!(report.requests.len(), keys.len());
    let transfers = self.transfers.lock().unwrap();
    assert_eq!(transfers.len(), keys.len(), "各要求を実Localへ転送");
    for ((request, transfer), indices) in report.requests.iter().zip(transfers.iter()).zip(keys) {
      assert_eq!(request.api, "BatchGetItem");
      assert_eq!(request.phase, Some(Phase::ConfigurationRead));
      assert_eq!(request.body, transfer.body);
      assert!(transfer.uri.starts_with(&self.endpoint));
      assert!(transfer.succeeded);
      let sent = request.body["RequestItems"].as_object().unwrap();
      assert_eq!(sent.len(), indices.len());
      for index in *indices {
        let attributes = &sent[self.names()[*index]];
        assert_eq!(attributes["ConsistentRead"], true);
        assert_eq!(attributes["Keys"], json!([json_key(*index)]));
      }
    }
  }

  async fn close(self) {
    for table in self.names() {
      self.raw.delete_table().table_name(table).send().await.unwrap();
    }
    self.container.rm().await.unwrap();
  }
}

fn observed_http(transfers: Arc<Mutex<Vec<Transfer>>>) -> aws_smithy_runtime_api::client::http::SharedHttpClient {
  let upstream = aws_smithy_http_client::Builder::new().build_http();
  http_client_fn(move |settings, components| {
    SharedHttpConnector::new(ObservedConnector {
      upstream: upstream.http_connector(settings, components),
      transfers: transfers.clone(),
    })
  })
}

fn transport(tables: &DynamoDbTables) -> FaultTransport {
  FaultTransport::new(
    RequestLayout::new(
      &tables.journal_table_name,
      &tables.snapshot_table_name,
      &tables.head_table_name,
      &tables.snapshot_history_index_name,
    )
    .unwrap(),
  )
}

fn config_key(index: usize) -> Item {
  let mut key = HashMap::from([("aid".into(), AttributeValue::S("__config__".into()))]);
  if index < 2 {
    key.insert(
      if index == 0 { "seq_nr" } else { "skey" }.into(),
      AttributeValue::N("0".into()),
    );
  }
  key
}

fn json_key(index: usize) -> Value {
  let mut key = json!({"aid": {"S": "__config__"}});
  if index < 2 {
    key[if index == 0 { "seq_nr" } else { "skey" }] = json!({"N": "0"});
  }
  key
}

fn config_item(index: usize) -> Item {
  let mut item = config_key(index);
  item.insert("store_id".into(), AttributeValue::S("real-local-store".into()));
  item.insert("layout_version".into(), AttributeValue::N("1".into()));
  item
}

fn all_items() -> [Option<Item>; 3] {
  std::array::from_fn(|index| Some(config_item(index)))
}

fn partial_response(responses: &[usize], unprocessed: &[usize], count: u32) -> Value {
  let symbols = ["journal", "snapshot", "head"];
  let selectors = ["journal:__config__:0", "snapshot:__config__:0", "head:__config__"];
  let responses: serde_json::Map<String, Value> = responses
    .iter()
    .map(|index| (symbols[*index].into(), json!("seed-config")))
    .collect();
  json!({
    "operation": 0, "phase": "configuration-read", "kind": "sdk-response", "injection": "replace-response",
    "repeat": {"mode": "count", "count": count},
    "details": {"responses": responses, "unprocessed_keys": unprocessed.iter().map(|index| selectors[*index]).collect::<Vec<_>>()}
  })
}

fn assert_matched(result: Result<ConfigurationRead, EventStoreError>) {
  assert_eq!(
    result.unwrap(),
    ConfigurationRead::Matched {
      store_id: "real-local-store".into()
    }
  );
}

fn assert_configuration(result: Result<ConfigurationRead, EventStoreError>, expected: ConfigurationReason) {
  assert!(matches!(result, Err(EventStoreError::Configuration { reason }) if reason == expected));
}

fn assert_storage(result: Result<ConfigurationRead, EventStoreError>) {
  assert!(matches!(
    result,
    Err(EventStoreError::Storage {
      operation: StorageOperation::ReadConfiguration,
      ..
    })
  ));
}

#[tokio::test]
async fn should_read_and_classify_real_configuration_in_three_independent_tables() {
  let fixture = Fixture::new(true).await;
  let options = DynamoDbOptions::default();
  fixture.seed(all_items()).await;
  let (result, report) = fixture.read(&options, vec![], false).await;
  assert_matched(result);
  fixture.assert_requests(&report, &[&[0, 1, 2]]);
  assert!(fixture.sdk_waits.0.lock().unwrap().is_empty());

  fixture.seed([None, None, None]).await;
  let (result, report) = fixture.read(&options, vec![], false).await;
  assert_eq!(result.unwrap(), ConfigurationRead::CreationRequired);
  fixture.assert_requests(&report, &[&[0, 1, 2]]);
  for table in fixture.names() {
    assert!(
      fixture
        .raw
        .scan()
        .table_name(table)
        .send()
        .await
        .unwrap()
        .items()
        .is_empty(),
      "読み取りは設定を作らない"
    );
  }
  for mask in 1..7 {
    fixture
      .seed(std::array::from_fn(|index| {
        (mask & (1 << index) != 0).then(|| config_item(index))
      }))
      .await;
    let (result, report) = fixture.read(&options, vec![], false).await;
    assert_configuration(result, ConfigurationReason::PartialDynamoDbConfiguration);
    fixture.assert_requests(&report, &[&[0, 1, 2]]);
  }
  for index in 0..3 {
    for (attribute, value, expected) in [
      (
        "store_id",
        AttributeValue::S("other-store".into()),
        ConfigurationReason::DynamoDbStoreIdMismatch,
      ),
      (
        "layout_version",
        AttributeValue::N("2".into()),
        ConfigurationReason::UnsupportedDynamoDbLayoutVersion,
      ),
    ] {
      let mut items = all_items();
      items[index].as_mut().unwrap().insert(attribute.into(), value);
      fixture.seed(items).await;
      let (result, report) = fixture.read(&options, vec![], false).await;
      assert_configuration(result, expected);
      fixture.assert_requests(&report, &[&[0, 1, 2]]);
    }
    for attribute in ["store_id", "layout_version"] {
      for wrong_type in [false, true] {
        let mut items = all_items();
        let item = items[index].as_mut().unwrap();
        item.remove(attribute);
        if wrong_type {
          item.insert(attribute.into(), AttributeValue::Bool(true));
        }
        fixture.seed(items).await;
        let (result, report) = fixture.read(&options, vec![], false).await;
        assert_storage(result);
        fixture.assert_requests(&report, &[&[0, 1, 2]]);
      }
    }
  }
  let mut unsupported = all_items();
  for item in &mut unsupported {
    item
      .as_mut()
      .unwrap()
      .insert("layout_version".into(), AttributeValue::N("2".into()));
  }
  fixture.seed(unsupported).await;
  let (result, _) = fixture.read(&options, vec![], false).await;
  assert_configuration(result, ConfigurationReason::UnsupportedDynamoDbLayoutVersion);
  fixture.close().await;
}

#[tokio::test]
async fn should_accumulate_real_partial_responses_and_retry_only_unprocessed_keys() {
  let fixture = Fixture::new(true).await;
  fixture.seed(all_items()).await;
  let options = DynamoDbOptions::default();
  for order in [[0, 1, 2], [1, 2, 0], [2, 0, 1]] {
    let (result, report) = fixture
      .read(
        &options,
        vec![
          partial_response(&[order[0]], &[order[1], order[2]], 1),
          partial_response(&[order[1]], &[order[2]], 1),
        ],
        true,
      )
      .await;
    assert_matched(result);
    fixture.assert_requests(&report, &[&[0, 1, 2], &order[1..], &order[2..]]);
    assert_eq!(
      *fixture.hook_waits.0.lock().unwrap(),
      vec![Duration::from_millis(50), Duration::from_millis(100)]
    );
    assert!(fixture.sdk_waits.0.lock().unwrap().is_empty());
  }
  // 先に得た識別子を捨てると、この実seedの不一致を検出できない。
  let mut items = all_items();
  items[0]
    .as_mut()
    .unwrap()
    .insert("store_id".into(), AttributeValue::S("first-response-store".into()));
  fixture.seed(items).await;
  let (result, report) = fixture
    .read(
      &options,
      vec![partial_response(&[0], &[1, 2], 1), partial_response(&[1], &[2], 1)],
      true,
    )
    .await;
  assert_configuration(result, ConfigurationReason::DynamoDbStoreIdMismatch);
  fixture.assert_requests(&report, &[&[0, 1, 2], &[1, 2], &[2]]);
  fixture.close().await;
}

#[tokio::test]
async fn should_bound_unprocessed_retries_and_record_capped_exponential_waits() {
  let fixture = Fixture::new(true).await;
  fixture.seed([None, None, None]).await;
  let default_waits = [50, 100, 200, 400, 800, 1600, 2000, 2000, 2000, 2000].map(Duration::from_millis);
  for limit in [0, 1, 10] {
    let options = DynamoDbOptions {
      unprocessed_retry_limit: limit,
      ..Default::default()
    };
    let (result, report) = fixture
      .read(&options, vec![partial_response(&[], &[0, 1, 2], limit + 1)], true)
      .await;
    assert_storage(result);
    fixture.assert_requests(&report, &vec![&[0, 1, 2][..]; limit as usize + 1]);
    assert_eq!(*fixture.hook_waits.0.lock().unwrap(), default_waits[..limit as usize]);
  }
  // 上限ちょうどの再要求で読み切れば、空の実３表を不存在と判定できる。
  let (result, report) = fixture
    .read(
      &DynamoDbOptions::default(),
      vec![partial_response(&[], &[0, 1, 2], 10)],
      true,
    )
    .await;
  assert_eq!(result.unwrap(), ConfigurationRead::CreationRequired);
  fixture.assert_requests(&report, &[&[0, 1, 2][..]; 11]);
  assert_eq!(*fixture.hook_waits.0.lock().unwrap(), default_waits);
  for (initial, maximum, expected) in [
    (Duration::from_secs(9), Duration::from_secs(2), Duration::from_secs(2)),
    (Duration::MAX, Duration::MAX, Duration::MAX),
    (Duration::ZERO, Duration::ZERO, Duration::ZERO),
  ] {
    let options = DynamoDbOptions {
      unprocessed_retry_limit: 2,
      unprocessed_retry_initial_delay: initial,
      unprocessed_retry_max_delay: maximum,
      ..Default::default()
    };
    let (result, report) = fixture
      .read(&options, vec![partial_response(&[], &[0, 1, 2], 3)], true)
      .await;
    assert_storage(result);
    fixture.assert_requests(&report, &[&[0, 1, 2], &[0, 1, 2], &[0, 1, 2]]);
    assert_eq!(*fixture.hook_waits.0.lock().unwrap(), vec![expected, expected]);
  }
  fixture.close().await;
}

#[tokio::test]
async fn should_use_the_stored_sdk_sleeper_or_the_explicit_retry_hook() {
  let fixture = Fixture::new(true).await;
  fixture.seed(all_items()).await;
  let (result, report) = fixture
    .read(
      &DynamoDbOptions::default(),
      vec![partial_response(&[0], &[1, 2], 1)],
      false,
    )
    .await;
  assert_matched(result);
  fixture.assert_requests(&report, &[&[0, 1, 2], &[1, 2]]);
  assert_eq!(*fixture.sdk_waits.0.lock().unwrap(), vec![Duration::from_millis(50)]);
  assert!(fixture.hook_waits.0.lock().unwrap().is_empty());
  fixture.close().await;

  let fixture = Fixture::new(false).await;
  fixture.seed(all_items()).await;
  let (result, report) = fixture.read(&DynamoDbOptions::default(), vec![], false).await;
  assert_configuration(result, ConfigurationReason::MissingRetrySleeper);
  fixture.assert_requests(&report, &[]);
  let (result, report) = fixture
    .read(
      &DynamoDbOptions::default(),
      vec![partial_response(&[0], &[1, 2], 1)],
      true,
    )
    .await;
  assert_matched(result);
  fixture.assert_requests(&report, &[&[0, 1, 2], &[1, 2]]);
  assert_eq!(*fixture.hook_waits.0.lock().unwrap(), vec![Duration::from_millis(50)]);
  let invalid = DynamoDbOptions {
    retention: RetentionSettings::keep_latest(0),
    ..Default::default()
  };
  let (result, report) = fixture.read(&invalid, vec![], true).await;
  assert_configuration(result, ConfigurationReason::KeepSnapshotCountZero);
  fixture.assert_requests(&report, &[]);
  let ttl = DynamoDbOptions {
    retention: RetentionSettings::keep_latest(2).with_mode(RetentionMode::Ttl { grace_seconds: 60 }),
    ..Default::default()
  };
  let (result, report) = fixture.read(&ttl, vec![], true).await;
  assert_matched(result);
  fixture.assert_requests(&report, &[&[0, 1, 2]]);

  let fault = json!({"operation": 0, "phase": "configuration-read", "kind": "storage-error", "injection": "replace-response", "repeat": {"mode": "count", "count": 1}, "details": {}});
  let (result, report) = fixture.read(&DynamoDbOptions::default(), vec![fault], true).await;
  assert_storage(result);
  fixture.assert_requests(&report, &[&[0, 1, 2]]);
  assert!(fixture.hook_waits.0.lock().unwrap().is_empty());
  fixture.close().await;
}

#[tokio::test]
async fn should_reject_duplicate_table_names_before_transfer_through_both_wait_paths() {
  let fixture = Fixture::new(true).await;
  let options = DynamoDbOptions::default();
  let valid = &fixture.tables;
  let duplicated = [
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
  ];
  for present in [true, false] {
    fixture
      .seed(if present { all_items() } else { [None, None, None] })
      .await;
    for tables in &duplicated {
      for hook in [false, true] {
        let (result, report) = fixture.read_with_tables(tables, &options, vec![], hook).await;
        assert!(
          matches!(
            &result,
            Err(EventStoreError::Configuration {
              reason: ConfigurationReason::DuplicateDynamoDbTableNames
            })
          ),
          "{tables:?}, hook={hook}, present={present}: {result:?}"
        );
        fixture.assert_requests(&report, &[]);
        assert!(fixture.sdk_waits.0.lock().unwrap().is_empty());
        assert!(fixture.hook_waits.0.lock().unwrap().is_empty());
      }
    }
    for hook in [false, true] {
      let (result, report) = fixture.read(&options, vec![], hook).await;
      if present {
        assert_matched(result);
      } else {
        assert_eq!(result.unwrap(), ConfigurationRead::CreationRequired);
      }
      fixture.assert_requests(&report, &[&[0, 1, 2]]);
      assert!(fixture.sdk_waits.0.lock().unwrap().is_empty());
      assert!(fixture.hook_waits.0.lock().unwrap().is_empty());
    }
  }
  fixture.close().await;
}

#[tokio::test]
async fn should_classify_a_real_sdk_connection_failure_as_storage() {
  let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
  let endpoint = format!("http://{}", listener.local_addr().unwrap());
  drop(listener);
  let tables = DynamoDbTables {
    journal_table_name: "offline-first".into(),
    snapshot_table_name: "offline-second".into(),
    head_table_name: "offline-third".into(),
    snapshot_history_index_name: "offline-index".into(),
  };
  let transport = transport(&tables);
  let transfers = Arc::new(Mutex::new(Vec::new()));
  let client = transport.client(client_builder(&endpoint), observed_http(transfers.clone()));
  let plan = FaultPlan::register(&json!({"steps": [{}], "faults": []})).unwrap();
  let operation = transport.begin_operation(&plan, 0).unwrap();
  let waits = Waits::default();
  let result = read_configuration_for_test(
    &client,
    &tables,
    &DynamoDbOptions::default(),
    Some(Arc::new(waits.clone())),
  )
  .await;
  assert_storage(result);
  let report = operation.finish();
  assert_eq!(report.requests.len(), 1);
  assert!(report.unfired.is_empty());
  assert_eq!(transfers.lock().unwrap().len(), 1);
  assert!(!transfers.lock().unwrap()[0].succeeded);
  assert!(waits.0.lock().unwrap().is_empty());
}
