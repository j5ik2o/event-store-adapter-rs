use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use aws_sdk_dynamodb::config::{AsyncSleep, Credentials, Region, Sleep};
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::AttributeValue as A;
use aws_sdk_dynamodb::Client;
use aws_smithy_runtime_api::client::http::{
  http_client_fn, HttpClient, HttpConnector, HttpConnectorFuture, SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::HttpRequest;
use aws_smithy_types::{body::SdkBody, byte_stream::ByteStream, retry::RetryConfig};
use event_store_adapter_rs::migration::LegacyDynamoDbTables;
use event_store_adapter_rs::next::aggregate_id::AggregateId;
use event_store_adapter_rs::next::dynamodb::{DynamoDbOptions, DynamoDbTables, EventStoreForDynamoDB};
use event_store_adapter_rs::next::error::EventStoreError;
use event_store_adapter_rs::next::serializer::{EventSerializer, SnapshotSerializer};
use event_store_adapter_test_utils_rs::{docker, dynamodb};
use serde_json::{json, Value};
use testcontainers::{ContainerAsync, GenericImage};

pub type Item = HashMap<String, A>;
pub type Traces = Arc<Mutex<Vec<Value>>>;
pub type BytesStore = EventStoreForDynamoDB<Id, Vec<u8>, Vec<u8>>;

#[derive(Debug)]
struct LocalSleep;
impl AsyncSleep for LocalSleep {
  fn sleep(&self, duration: std::time::Duration) -> Sleep {
    Sleep::new(tokio::time::sleep(duration))
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Id(pub String, pub String);
impl AggregateId for Id {
  fn type_name(&self) -> String {
    self.0.clone()
  }

  fn value(&self) -> String {
    self.1.clone()
  }
}

#[derive(Debug)]
struct BytesSerializer;
impl EventSerializer<Vec<u8>> for BytesSerializer {
  fn serialize(&self, _: &Vec<u8>) -> Result<Vec<u8>, EventStoreError> {
    panic!("公開readの検証でserializeを呼ばない")
  }

  fn deserialize(&self, data: &[u8]) -> Result<Vec<u8>, EventStoreError> {
    Ok(data.to_vec())
  }
}
impl SnapshotSerializer<Vec<u8>> for BytesSerializer {
  fn serialize(&self, _: &Vec<u8>) -> Result<Vec<u8>, EventStoreError> {
    panic!("公開readの検証でserializeを呼ばない")
  }

  fn deserialize(&self, data: &[u8]) -> Result<Vec<u8>, EventStoreError> {
    Ok(data.to_vec())
  }
}

#[derive(Debug, Clone)]
pub struct Install {
  pub table: String,
  pub item: Item,
}

#[derive(Debug)]
struct Connector {
  upstream: SharedHttpConnector,
  raw: Client,
  traces: Traces,
  install: Arc<Mutex<Option<Install>>>,
}
impl HttpConnector for Connector {
  fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
    let api = request
      .headers()
      .get("x-amz-target")
      .unwrap()
      .rsplit('.')
      .next()
      .unwrap()
      .to_string();
    let input: Value = serde_json::from_slice(request.body().bytes().unwrap()).unwrap();
    let install = {
      let mut pending = self.install.lock().unwrap();
      if api == "PutItem"
        && pending
          .as_ref()
          .is_some_and(|install| input["TableName"] == install.table)
      {
        pending.take()
      } else {
        None
      }
    };
    let index = {
      let mut records = self.traces.lock().unwrap();
      let index = records.len();
      records.push(json!({"api":api,"input":input}));
      index
    };
    let upstream = self.upstream.clone();
    let raw = self.raw.clone();
    let traces = self.traces.clone();
    HttpConnectorFuture::new(async move {
      if let Some(install) = install {
        raw
          .put_item()
          .table_name(install.table)
          .set_item(Some(install.item))
          .send()
          .await
          .unwrap();
      }
      let mut response = upstream.call(request).await?;
      let bytes = ByteStream::new(std::mem::replace(response.body_mut(), SdkBody::taken()))
        .collect()
        .await
        .unwrap()
        .into_bytes();
      traces.lock().unwrap()[index]["status"] = json!(response.status().as_u16());
      traces.lock().unwrap()[index]["output"] = serde_json::from_slice(&bytes).unwrap();
      *response.body_mut() = SdkBody::from(bytes);
      Ok(response)
    })
  }
}

pub struct Observed {
  pub client: Client,
  pub traces: Traces,
  pub install: Arc<Mutex<Option<Install>>>,
}
impl Observed {
  pub fn new(endpoint: &str, raw: &Client) -> Self {
    let traces = Arc::new(Mutex::new(Vec::new()));
    let install = Arc::new(Mutex::new(None));
    let records = traces.clone();
    let control = install.clone();
    let raw = raw.clone();
    let upstream = aws_smithy_http_client::Builder::new().build_http();
    let http = http_client_fn(move |settings, components| {
      SharedHttpConnector::new(Connector {
        upstream: upstream.http_connector(settings, components),
        raw: raw.clone(),
        traces: records.clone(),
        install: control.clone(),
      })
    });
    let config = aws_sdk_dynamodb::Config::builder()
      .behavior_version_latest()
      .region(Region::new("us-west-1"))
      .credentials_provider(Credentials::new("x", "x", None, None, "migration-local"))
      .endpoint_url(endpoint)
      .http_client(http)
      .sleep_impl(LocalSleep)
      .retry_config(RetryConfig::disabled())
      .build();
    Self {
      client: Client::from_conf(config),
      traces,
      install,
    }
  }
}

pub struct Fixture {
  pub _container: ContainerAsync<GenericImage>,
  pub raw: Client,
  pub endpoint: String,
  pub tables: DynamoDbTables,
  pub legacy: LegacyDynamoDbTables,
}
impl Fixture {
  pub async fn new() -> Self {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let container = docker::dynamodb_local().await.unwrap();
    let port = container.get_host_port_ipv4(docker::DYNAMODB_LOCAL_PORT).await.unwrap();
    let endpoint = format!("http://127.0.0.1:{port}");
    let raw = Client::from_conf(
      dynamodb::create_dynamodb_local_client(port)
        .config()
        .to_builder()
        .sleep_impl(LocalSleep)
        .build(),
    );
    let prefix = format!(
      "migration-{}-{}",
      std::process::id(),
      NEXT.fetch_add(1, Ordering::SeqCst)
    );
    let names = dynamodb::TableNames {
      journal: format!("{prefix}-new-journal"),
      snapshot: format!("{prefix}-new-snapshot"),
      head: format!("{prefix}-new-head"),
      snapshot_history_index: format!("{prefix}-new-history"),
    };
    dynamodb::create_tables(&raw, &names, true).await.unwrap();
    let legacy = LegacyDynamoDbTables {
      journal_table_name: format!("{prefix}-old-journal"),
      snapshot_table_name: format!("{prefix}-old-snapshot"),
    };
    dynamodb::create_journal_table(&raw, &legacy.journal_table_name, &format!("{prefix}-old-events-index"))
      .await
      .unwrap();
    dynamodb::create_snapshot_table(&raw, &legacy.snapshot_table_name, &format!("{prefix}-old-state-index"))
      .await
      .unwrap();
    Self {
      _container: container,
      raw,
      endpoint,
      legacy,
      tables: DynamoDbTables {
        journal_table_name: names.journal,
        snapshot_table_name: names.snapshot,
        head_table_name: names.head,
        snapshot_history_index_name: names.snapshot_history_index,
      },
    }
  }

  pub async fn seed(&self, events: &[Item], snapshots: &[Item]) {
    for (table, items) in [
      (&self.legacy.journal_table_name, events),
      (&self.legacy.snapshot_table_name, snapshots),
    ] {
      for item in items {
        self
          .raw
          .put_item()
          .table_name(table)
          .set_item(Some(item.clone()))
          .send()
          .await
          .unwrap();
      }
    }
  }

  pub async fn old_items(&self) -> (Vec<Value>, Vec<Value>) {
    (
      scan(&self.raw, &self.legacy.journal_table_name).await,
      scan(&self.raw, &self.legacy.snapshot_table_name).await,
    )
  }

  pub async fn open(&self) -> BytesStore {
    BytesStore::open_with_serializers(
      self.raw.clone(),
      self.tables.clone(),
      DynamoDbOptions::default(),
      Arc::new(BytesSerializer),
      Arc::new(BytesSerializer),
    )
    .await
    .unwrap()
  }
}

// 入力は旧属性で独立して指定する。新版期待値と共有属性生成器は参照しない。
pub fn raw_event(pkey: &str, skey: &str, seq: &str, bytes: Vec<u8>, manifest: Option<&str>) -> Item {
  let mut item = Item::from([
    ("pkey".into(), A::S(pkey.into())),
    ("skey".into(), A::S(skey.into())),
    ("aid".into(), A::S("old caller display".into())),
    ("seq_nr".into(), A::N(seq.into())),
    ("occurred_at".into(), A::N("-876543211".into())),
    ("payload".into(), A::B(Blob::new(bytes))),
  ]);
  if let Some(manifest) = manifest {
    item.insert("manifest".into(), A::S(manifest.into()));
  }
  item
}

pub fn raw_snapshot(pkey: &str, skey: &str, seq: &str, bytes: Vec<u8>, ttl: &str) -> Item {
  Item::from([
    ("pkey".into(), A::S(pkey.into())),
    ("skey".into(), A::S(skey.into())),
    ("aid".into(), A::S("old caller display".into())),
    ("seq_nr".into(), A::N(seq.into())),
    ("version".into(), A::N("42".into())),
    ("payload".into(), A::B(Blob::new(bytes))),
    ("last_updated_at".into(), A::N("-877".into())),
    ("ttl".into(), A::N(ttl.into())),
  ])
}

pub async fn get(client: &Client, table: &str, aid: &str, sort: Option<(&str, &str)>) -> Item {
  let mut request = client
    .get_item()
    .table_name(table)
    .key("aid", A::S(aid.into()))
    .consistent_read(true);
  if let Some((name, value)) = sort {
    request = request.key(name, A::N(value.into()));
  }
  request.send().await.unwrap().item.unwrap()
}

pub async fn scan(client: &Client, table: &str) -> Vec<Value> {
  let mut items = Vec::new();
  let mut key = None;
  loop {
    let page = client
      .scan()
      .table_name(table)
      .consistent_read(true)
      .set_exclusive_start_key(key)
      .send()
      .await
      .unwrap();
    items.extend(page.items().iter().map(wire));
    key = page.last_evaluated_key.filter(|key| !key.is_empty());
    if key.is_none() {
      break;
    }
  }
  items.sort_by_key(Value::to_string);
  items
}

pub fn wire(item: &Item) -> Value {
  fn attribute(value: &A) -> Value {
    match value {
      A::S(value) => json!({"S":value}),
      A::N(value) => json!({"N":value}),
      A::B(value) => json!({"B":aws_smithy_types::base64::encode(value.as_ref())}),
      A::M(value) => json!({"M":wire(value)}),
      A::L(value) => json!({"L":value.iter().map(attribute).collect::<Vec<_>>()}),
      _ => panic!("unexpected fixture attribute"),
    }
  }
  Value::Object(
    item
      .iter()
      .map(|(name, value)| (name.clone(), attribute(value)))
      .collect(),
  )
}

pub fn evidence(name: &str, value: Value) {
  if let Some(dir) = std::env::var_os("MIGRATION_EVIDENCE_DIR") {
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
      std::path::Path::new(&dir).join(format!("{name}.json")),
      serde_json::to_vec_pretty(&value).unwrap(),
    )
    .unwrap();
  }
}

pub fn assert_shapes(item: &Item, expected: &[(&str, &str)]) {
  assert_eq!(item.len(), expected.len());
  for (name, kind) in expected {
    assert_eq!(wire(item)[*name].as_object().unwrap().keys().next().unwrap(), kind);
  }
}
