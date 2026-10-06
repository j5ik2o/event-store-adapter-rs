use aws_sdk_dynamodb::config::{Credentials, Region};
use aws_sdk_dynamodb::types::{
  KeySchemaElement, KeyType, ProjectionType, ScalarAttributeType, StreamViewType, TableDescription, TableStatus,
  TimeToLiveStatus,
};
use aws_sdk_dynamodb::Client;
use testcontainers::{ContainerAsync, GenericImage};

use crate::docker::{dynamodb_local, DYNAMODB_LOCAL_PORT};
use crate::dynamodb::{create_dynamodb_local_client, create_tables, TableNames};

/// DynamoDB Local を起動し、コンテナ・公開ポート・クライアントを返す。
/// コンテナは呼び出し側が保持する（drop で削除される）。
async fn connect() -> (ContainerAsync<GenericImage>, u16, Client) {
  let container = dynamodb_local().await.expect("failed to start DynamoDB Local");
  let port = container
    .get_host_port_ipv4(DYNAMODB_LOCAL_PORT)
    .await
    .expect("failed to resolve the mapped port");
  let client = create_dynamodb_local_client(port);
  (container, port, client)
}

/// `prefix` から journal・snapshot・head・履歴 GSI の名前を作る。
fn table_names(prefix: &str) -> TableNames {
  TableNames {
    journal: format!("{prefix}-journal"),
    snapshot: format!("{prefix}-snapshot"),
    head: format!("{prefix}-head"),
    snapshot_history_index: format!("{prefix}-history"),
  }
}

async fn describe(client: &Client, table_name: &str) -> TableDescription {
  client
    .describe_table()
    .table_name(table_name)
    .send()
    .await
    .unwrap_or_else(|e| panic!("DescribeTable({table_name}) failed: {e:?}"))
    .table()
    .cloned()
    .unwrap_or_else(|| panic!("DescribeTable({table_name}) returned no table"))
}

/// キー構成を `(属性名, キー種別)` の並び（Hash が先）で取り出す。
fn key_schema_of(key_schema: &[KeySchemaElement]) -> Vec<(String, KeyType)> {
  key_schema
    .iter()
    .map(|k| (k.attribute_name().to_string(), k.key_type().clone()))
    .collect()
}

/// テーブルが宣言している属性を `(属性名, 型)` の並び（属性名の昇順）で取り出す。
fn attribute_types_of(table: &TableDescription) -> Vec<(String, ScalarAttributeType)> {
  let mut attributes: Vec<(String, ScalarAttributeType)> = table
    .attribute_definitions()
    .iter()
    .map(|a| (a.attribute_name().to_string(), a.attribute_type().clone()))
    .collect();
  attributes.sort_by(|a, b| a.0.cmp(&b.0));
  attributes
}

fn expected_keys(hash: &str, range: Option<&str>) -> Vec<(String, KeyType)> {
  let mut keys = vec![(hash.to_string(), KeyType::Hash)];
  if let Some(range) = range {
    keys.push((range.to_string(), KeyType::Range));
  }
  keys
}

fn is_stream_enabled(table: &TableDescription) -> bool {
  table.stream_specification().is_some_and(|s| s.stream_enabled())
}

/// `DescribeTimeToLive` の `(状態, 属性名)` を返す。
async fn ttl_description_of(client: &Client, table_name: &str) -> (Option<TimeToLiveStatus>, Option<String>) {
  let output = client
    .describe_time_to_live()
    .table_name(table_name)
    .send()
    .await
    .unwrap_or_else(|e| panic!("DescribeTimeToLive({table_name}) failed: {e:?}"));
  let description = output.time_to_live_description();
  (
    description.and_then(|d| d.time_to_live_status().cloned()),
    description.and_then(|d| d.attribute_name().map(str::to_string)),
  )
}

fn is_ttl_on(status: Option<&TimeToLiveStatus>) -> bool {
  matches!(
    status,
    Some(TimeToLiveStatus::Enabled) | Some(TimeToLiveStatus::Enabling)
  )
}

#[tokio::test]
async fn test_dynamodb_local_creates_three_tables() {
  // Given: DynamoDB Local が起動している
  let (_container, _port, client) = connect().await;
  let names = table_names("layout");

  // When: 起動の直後に、TTL なしで 3 テーブルを作る
  create_tables(&client, &names, false)
    .await
    .expect("create_tables should succeed right after startup");

  // Then: journal は aid(S) HASH + seq_nr(N) RANGE。GSI なし、Streams 無効
  let journal = describe(&client, &names.journal).await;
  assert_eq!(journal.table_status(), Some(&TableStatus::Active));
  assert_eq!(
    key_schema_of(journal.key_schema()),
    expected_keys("aid", Some("seq_nr"))
  );
  assert_eq!(
    attribute_types_of(&journal),
    vec![
      ("aid".to_string(), ScalarAttributeType::S),
      ("seq_nr".to_string(), ScalarAttributeType::N),
    ]
  );
  assert!(journal.global_secondary_indexes().is_empty());
  assert!(!is_stream_enabled(&journal));

  // And: snapshot は aid(S) HASH + skey(N) RANGE。履歴 GSI が 1 つ（aid HASH + active_history_seq_nr RANGE、KEYS_ONLY）。Streams 無効
  let snapshot = describe(&client, &names.snapshot).await;
  assert_eq!(snapshot.table_status(), Some(&TableStatus::Active));
  assert_eq!(key_schema_of(snapshot.key_schema()), expected_keys("aid", Some("skey")));
  assert_eq!(
    attribute_types_of(&snapshot),
    vec![
      ("active_history_seq_nr".to_string(), ScalarAttributeType::N),
      ("aid".to_string(), ScalarAttributeType::S),
      ("skey".to_string(), ScalarAttributeType::N),
    ]
  );
  assert_eq!(snapshot.global_secondary_indexes().len(), 1);
  let history_index = &snapshot.global_secondary_indexes()[0];
  assert_eq!(history_index.index_name(), Some(names.snapshot_history_index.as_str()));
  assert_eq!(
    key_schema_of(history_index.key_schema()),
    expected_keys("aid", Some("active_history_seq_nr"))
  );
  assert_eq!(
    history_index.projection().and_then(|p| p.projection_type()),
    Some(&ProjectionType::KeysOnly)
  );
  assert!(!is_stream_enabled(&snapshot));

  // And: head は aid(S) HASH だけ。GSI なし。Streams 有効（NEW_IMAGE）。Streams の ARN は DynamoDB Local のもの（LocalStack ではない）
  let head = describe(&client, &names.head).await;
  assert_eq!(head.table_status(), Some(&TableStatus::Active));
  assert_eq!(key_schema_of(head.key_schema()), expected_keys("aid", None));
  assert_eq!(
    attribute_types_of(&head),
    vec![("aid".to_string(), ScalarAttributeType::S)]
  );
  assert!(head.global_secondary_indexes().is_empty());
  assert!(is_stream_enabled(&head));
  assert_eq!(
    head.stream_specification().and_then(|s| s.stream_view_type()),
    Some(&StreamViewType::NewImage)
  );
  let stream_arn = head.latest_stream_arn().expect("head should have a stream ARN");
  assert!(
    stream_arn.contains(":ddblocal:"),
    "stream ARN should come from DynamoDB Local: {stream_arn}"
  );
}

#[tokio::test]
async fn test_dynamodb_local_shares_tables_across_credentials_and_regions() {
  // Given: DynamoDB Local が起動し、既定のクライアント（us-west-1・x/x）で 3 テーブルを作ってある
  let (_container, port, client) = connect().await;
  let names = table_names("shared");
  create_tables(&client, &names, false)
    .await
    .expect("create_tables should succeed");

  // When: リージョンと資格情報の違うクライアントでテーブルの一覧を取る
  let other_client = Client::from_conf(
    aws_sdk_dynamodb::Config::builder()
      .region(Some(Region::new("ap-northeast-1")))
      .endpoint_url(format!("http://127.0.0.1:{port}"))
      .credentials_provider(Credentials::new("otherkey", "othersecret", None, None, "other"))
      .behavior_version_latest()
      .build(),
  );
  let table_names_seen = other_client
    .list_tables()
    .send()
    .await
    .expect("ListTables should succeed")
    .table_names()
    .to_vec();

  // Then: 3 テーブルとも見える（-sharedDb により、資格情報とリージョンで DB が分かれない）
  for expected in [&names.journal, &names.snapshot, &names.head] {
    assert!(
      table_names_seen.contains(expected),
      "{expected} should be visible from another credential/region: {table_names_seen:?}"
    );
  }
}

#[tokio::test]
async fn test_create_tables_enables_snapshot_ttl_only_when_requested() {
  // Given: 1 つのコンテナに plain-* の 3 テーブル（GSI plain-history）を TTL なしで作ってある
  let (_container, _port, client) = connect().await;
  let plain = table_names("plain");
  create_tables(&client, &plain, false)
    .await
    .expect("create_tables(plain) should succeed");

  // When: 名前の違う ttl-* の 3 テーブル（GSI ttl-history）を、snapshot の TTL を有効にして作る
  let ttl = table_names("ttl");
  create_tables(&client, &ttl, true)
    .await
    .expect("create_tables(ttl) should succeed");

  // Then: 6 テーブルがすべて ACTIVE になる
  for name in [
    &plain.journal,
    &plain.snapshot,
    &plain.head,
    &ttl.journal,
    &ttl.snapshot,
    &ttl.head,
  ] {
    let table = describe(&client, name).await;
    assert_eq!(
      table.table_status(),
      Some(&TableStatus::Active),
      "{name} should be ACTIVE"
    );
  }

  // And: TTL が有効なのは ttl-snapshot だけで、属性名は ttl
  let (status, attribute_name) = ttl_description_of(&client, &ttl.snapshot).await;
  assert!(
    is_ttl_on(status.as_ref()),
    "{} should have TTL enabled, but was {status:?}",
    ttl.snapshot
  );
  assert_eq!(attribute_name.as_deref(), Some("ttl"));

  // And: 求めなかった plain-snapshot と、求めても対象外の journal・head は TTL が有効でない
  for name in [&plain.snapshot, &plain.journal, &plain.head, &ttl.journal, &ttl.head] {
    let (status, _) = ttl_description_of(&client, name).await;
    assert!(
      !is_ttl_on(status.as_ref()),
      "{name} should not have TTL enabled, but was {status:?}"
    );
  }
}

#[tokio::test]
async fn test_create_tables_returns_error_when_tables_exist() {
  // Given: dup-* の 3 テーブル（GSI dup-history）がすでにある
  let (_container, _port, client) = connect().await;
  let names = table_names("dup");
  create_tables(&client, &names, false)
    .await
    .expect("the first create_tables should succeed");

  // When: 同じ 4 つの名前を渡して、もう一度 3 テーブルを作る
  let result = create_tables(&client, &names, false).await;

  // Then: 作成関数は Err を返し、成功を装わない
  assert!(
    result.is_err(),
    "create_tables should fail when the tables already exist"
  );
}
