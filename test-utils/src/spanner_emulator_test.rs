use googleapis_tonic_google_spanner_admin_database_v1::google::spanner::admin::database::v1::database_admin_client::DatabaseAdminClient;
use googleapis_tonic_google_spanner_admin_database_v1::google::spanner::admin::database::v1::{
  DatabaseDialect, GetDatabaseRequest,
};
use googleapis_tonic_google_spanner_v1::google::spanner::v1::spanner_client::SpannerClient;
use googleapis_tonic_google_spanner_v1::google::spanner::v1::transaction_options::{
  read_only::TimestampBound, Mode, ReadOnly,
};
use googleapis_tonic_google_spanner_v1::google::spanner::v1::{
  transaction_selector, BeginTransactionRequest, CreateSessionRequest, ExecuteSqlRequest, Session, TransactionOptions,
  TransactionSelector,
};
use testcontainers::{ContainerAsync, GenericImage, Image};
use tonic::transport::Channel;

use crate::docker::{spanner_emulator, SPANNER_EMULATOR_PORT};
use crate::spanner::{
  connect, create_database, database_path, drop_database, insert_configuration, store_ddl, Cell, Param, RawSql,
  SeedConfiguration,
};

/// Spanner エミュレーターを起動し、コンテナと接続を返す。コンテナは呼び出し側が保持する（drop で削除される）。
async fn start() -> (ContainerAsync<GenericImage>, Channel) {
  let container = spanner_emulator().await.expect("failed to start the Spanner emulator");
  let port = container
    .get_host_port_ipv4(SPANNER_EMULATOR_PORT)
    .await
    .expect("failed to resolve the mapped port");
  let channel = connect(port).await.expect("failed to connect to the Spanner emulator");
  (container, channel)
}

async fn insert_current(raw: &RawSql, seq: i64) -> anyhow::Result<i64> {
  raw
    .execute(
      "INSERT INTO esa_snapshot_current (hash_prefix, aid, seq_nr, manifest, payload) VALUES (@hp, @aid, @seq, '', @payload)",
      &[
        ("hp", Param::Bytes(vec![0; 8])),
        ("aid", Param::String(format!("Order-{seq}"))),
        ("seq", Param::Int64(seq)),
        ("payload", Param::Bytes(vec![])),
      ],
    )
    .await
}

// 起動したコンテナは、digest で固定した 1.5.58 のイメージから作られる。動くタグ（`latest` など）で起動しない。
#[tokio::test]
async fn test_spanner_emulator_starts_from_the_digest_pinned_image() {
  let (container, _channel) = start().await;

  let image = container.image();

  assert_eq!(image.name(), "gcr.io/cloud-spanner-emulator/emulator@sha256");
  assert_eq!(
    image.tag(),
    "c6f3402f2599684f295a0fdefb6fbbbfb18a0e43e309ff5456ccb452a4570a79"
  );
  assert_eq!(
    image.tag().len(),
    64,
    "参照の末尾は 64 桁の 16 進数の digest だけである"
  );
  assert!(image.tag().chars().all(|c| c.is_ascii_hexdigit()));
}

// プロファイル 3 章の 5 テーブルを作り、config の 1 行を書いて、列の型どおりに読み戻せる。INT64 は符号付き 64 ビットの全域を失わない。
#[tokio::test]
async fn test_spanner_emulator_creates_the_five_tables_and_round_trips_raw_rows() {
  let (_container, channel) = start().await;
  create_database(&channel, "db1", store_ddl("esa", None))
    .await
    .expect("failed to create the database");
  let raw = RawSql::new(channel, "db1");
  insert_configuration(
    &raw,
    "esa",
    &SeedConfiguration::current_only("5f0c8b8e-3c1a-4d7e-9a4b-2f6d1c0e7a11"),
  )
  .await
  .expect("failed to insert the configuration");

  let counts = raw
    .execute_all(&[
      (
        "INSERT INTO esa_journal (hash_prefix, aid, seq_nr, occurred_at_ns, manifest, payload) VALUES (@hp, @aid, @seq, @ns, '', @payload)",
        &[
          ("hp", Param::Bytes(vec![0, 1, 2, 3, 4, 5, 6, 255])),
          ("aid", Param::String("Order-1".to_string())),
          ("seq", Param::Int64(1)),
          ("ns", Param::Int64(i64::MIN)),
          ("payload", Param::Bytes(b"{\"number\":1}".to_vec())),
        ],
      ),
      (
        "INSERT INTO esa_journal (hash_prefix, aid, seq_nr, occurred_at_ns, manifest, payload) VALUES (@hp, @aid, @seq, @ns, '', @payload)",
        &[
          ("hp", Param::Bytes(vec![0, 1, 2, 3, 4, 5, 6, 255])),
          ("aid", Param::String("Order-1".to_string())),
          ("seq", Param::Int64(2)),
          ("ns", Param::Int64(i64::MAX)),
          ("payload", Param::Bytes(b"{\"number\":2}".to_vec())),
        ],
      ),
    ])
    .await
    .expect("failed to insert the journal rows");

  let rows = raw
    .query(
      "SELECT hash_prefix, aid, seq_nr, occurred_at_ns, manifest, payload FROM esa_journal ORDER BY seq_nr",
      &[],
    )
    .await
    .expect("failed to query the journal");
  let configuration = raw
    .query(
      "SELECT store_id, layout_version, keep_snapshot_count, retention_mode, ttl_grace_days, change_feed_enabled FROM esa_config WHERE singleton = 1",
      &[],
    )
    .await
    .expect("failed to query the configuration");

  assert_eq!(counts, vec![1, 1]);
  assert_eq!(rows.len(), 2);
  assert_eq!(rows[0][0].bytes(), &[0, 1, 2, 3, 4, 5, 6, 255]);
  assert_eq!(rows[0][1].string(), "Order-1");
  assert_eq!(rows[0][2].int64(), 1);
  assert_eq!(rows[0][3].int64(), i64::MIN);
  assert_eq!(rows[1][3].int64(), i64::MAX);
  assert_eq!(rows[1][5].bytes(), b"{\"number\":2}");
  assert_eq!(
    configuration,
    vec![vec![
      Cell::String("5f0c8b8e-3c1a-4d7e-9a4b-2f6d1c0e7a11".to_string()),
      Cell::Int64(1),
      Cell::Null,
      Cell::String("delete".to_string()),
      Cell::Null,
      Cell::Bool(false),
    ]]
  );
}

// SPN-2 の照合が頼る情報（情報スキーマの列・主キー・行削除ポリシーと、管理 API のデータベースの方言）を、エミュレーターが返す。
#[tokio::test]
async fn test_spanner_emulator_reports_the_layout_policy_and_dialect_that_the_open_check_reads() {
  let (_container, channel) = start().await;
  create_database(&channel, "db2", store_ddl("esa", Some(7)))
    .await
    .expect("failed to create the database");
  let raw = RawSql::new(channel.clone(), "db2");

  let policy = raw
    .query(
      "SELECT TABLE_NAME, ROW_DELETION_POLICY_EXPRESSION FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '' AND STARTS_WITH(TABLE_NAME, 'esa_') ORDER BY TABLE_NAME",
      &[],
    )
    .await
    .expect("failed to read the row deletion policies");
  let columns = raw
    .query(
      "SELECT COLUMN_NAME, SPANNER_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '' AND TABLE_NAME = 'esa_snapshot_history' ORDER BY ORDINAL_POSITION",
      &[],
    )
    .await
    .expect("failed to read the columns");
  let primary_key = raw
    .query(
      "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.INDEX_COLUMNS WHERE TABLE_SCHEMA = '' AND TABLE_NAME = 'esa_snapshot_history' AND INDEX_NAME = 'PRIMARY_KEY' ORDER BY ORDINAL_POSITION",
      &[],
    )
    .await
    .expect("failed to read the primary key");
  let database = DatabaseAdminClient::new(channel)
    .get_database(GetDatabaseRequest {
      name: database_path("db2"),
    })
    .await
    .expect("failed to get the database")
    .into_inner();

  let expression_of = |table: &str| -> Option<String> {
    policy
      .iter()
      .find(|row| row[0].string() == table)
      .and_then(|row| (!row[1].is_null()).then(|| row[1].string().to_string()))
  };
  let expression = expression_of("esa_snapshot_history").expect("snapshot_history has a row deletion policy");
  assert_eq!(
    expression.to_ascii_uppercase().replace(' ', ""),
    "OLDER_THAN(EXPIRED_AT,INTERVAL7DAY)",
    "the emulator returns the policy expression: {expression}"
  );
  for table in ["esa_journal", "esa_head", "esa_snapshot_current", "esa_config"] {
    assert_eq!(expression_of(table), None, "{table} has no row deletion policy");
  }
  let names: Vec<(&str, &str)> = columns.iter().map(|row| (row[0].string(), row[1].string())).collect();
  assert_eq!(
    names,
    vec![
      ("hash_prefix", "BYTES(8)"),
      ("aid", "STRING(1024)"),
      ("seq_nr", "INT64"),
      ("manifest", "STRING(MAX)"),
      ("payload", "BYTES(MAX)"),
      ("expired_at", "TIMESTAMP"),
    ]
  );
  let key: Vec<&str> = primary_key.iter().map(|row| row[0].string()).collect();
  assert_eq!(key, vec!["hash_prefix", "aid", "seq_nr"]);
  assert_eq!(database.database_dialect, DatabaseDialect::GoogleStandardSql as i32);
}

// 読み取り専用トランザクション（strong）を開いている間に、別の読み書きトランザクションがコミットできる。
// 開いた読み取り専用トランザクションの中の 2 回目の読み取りは、開いた時点の状態を返す（SPN-9 の同じ時点の読み取り）。
#[tokio::test]
async fn test_spanner_emulator_commits_a_writer_while_a_strong_read_only_transaction_is_open() {
  let (_container, channel) = start().await;
  create_database(&channel, "db3", store_ddl("esa", None))
    .await
    .expect("failed to create the database");
  let raw = RawSql::new(channel.clone(), "db3");
  insert_current(&raw, 1).await.expect("failed to insert the first row");

  let mut client = SpannerClient::new(channel.clone());
  let session = client
    .create_session(CreateSessionRequest {
      database: database_path("db3"),
      session: Some(Session::default()),
    })
    .await
    .expect("failed to create a session")
    .into_inner()
    .name;
  let read_only = client
    .begin_transaction(BeginTransactionRequest {
      session: session.clone(),
      options: Some(TransactionOptions {
        mode: Some(Mode::ReadOnly(ReadOnly {
          timestamp_bound: Some(TimestampBound::Strong(true)),
          ..Default::default()
        })),
        ..Default::default()
      }),
      ..Default::default()
    })
    .await
    .expect("failed to begin the read-only transaction")
    .into_inner();
  let count = |client: &mut SpannerClient<Channel>, session: String, id: Vec<u8>| {
    let mut client = client.clone();
    async move {
      client
        .execute_sql(ExecuteSqlRequest {
          session,
          transaction: Some(TransactionSelector {
            selector: Some(transaction_selector::Selector::Id(id)),
          }),
          sql: "SELECT COUNT(*) FROM esa_snapshot_current".to_string(),
          ..Default::default()
        })
        .await
        .expect("failed to count the rows")
        .into_inner()
        .rows[0]
        .values[0]
        .kind
        .clone()
    }
  };
  let before = count(&mut client, session.clone(), read_only.id.clone()).await;

  insert_current(&raw, 2)
    .await
    .expect("a writer must be able to commit while the read-only transaction is open");
  let after = count(&mut client, session.clone(), read_only.id.clone()).await;

  assert_eq!(before, Some(prost_types::value::Kind::StringValue("1".to_string())));
  assert_eq!(
    after, before,
    "the second read in the same read-only transaction must not see the later commit"
  );
  let latest = raw
    .query("SELECT COUNT(*) FROM esa_snapshot_current", &[])
    .await
    .expect("failed to count the rows");
  assert_eq!(latest[0][0].int64(), 2, "a new strong read sees both rows");
}

// PostgreSQL 方言のデータベースも作れる（ライブラリが GoogleSQL 以外の方言を拒否することの試験に使う）。
#[tokio::test]
async fn test_spanner_emulator_creates_a_postgresql_dialect_database() {
  let (_container, channel) = start().await;
  crate::spanner::create_database_with_dialect(&channel, "pgdb", DatabaseDialect::Postgresql, vec![])
    .await
    .expect("failed to create the PostgreSQL dialect database");

  let database = DatabaseAdminClient::new(channel)
    .get_database(GetDatabaseRequest {
      name: database_path("pgdb"),
    })
    .await
    .expect("failed to get the database")
    .into_inner();

  assert_eq!(database.database_dialect, DatabaseDialect::Postgresql as i32);
}

// 使い終えたデータベースは削除でき、削除後は取得できない（場面ごとに新しいデータベースを作る呼び出し側が、数の上限に達しない）。
#[tokio::test]
async fn test_spanner_emulator_drops_a_database_so_that_it_cannot_be_read_afterwards() {
  let (_container, channel) = start().await;
  create_database(&channel, "dropped", store_ddl("esa", None))
    .await
    .expect("failed to create the database");

  drop_database(&channel, "dropped")
    .await
    .expect("failed to drop the database");

  let status = DatabaseAdminClient::new(channel)
    .get_database(GetDatabaseRequest {
      name: database_path("dropped"),
    })
    .await
    .expect_err("a dropped database cannot be read");
  assert_eq!(status.code(), tonic::Code::NotFound);
}

// 1 MiB の BYTES は、ExecuteStreamingSql が `chunked_value` つきの複数の応答に分けて返す。
// ライブラリの結合試験の大きな payload が、分割された値の結合を実際に通ることの前提である。
// 非ストリーミングの ExecuteSql は 3 MiB（base64 で約 4.2 MiB）で tonic の既定の受信上限（4 MiB）を超える。
#[tokio::test]
async fn test_spanner_emulator_streams_a_one_mebibyte_value_in_chunks() {
  let (_container, channel) = start().await;
  create_database(&channel, "db4", store_ddl("esa", None))
    .await
    .expect("failed to create the database");
  let raw = RawSql::new(channel.clone(), "db4");
  raw
    .execute(
      "INSERT INTO esa_journal (hash_prefix, aid, seq_nr, occurred_at_ns, manifest, payload) VALUES (@hp, 'Order-1', 1, 0, '', @payload)",
      &[("hp", Param::Bytes(vec![0; 8])), ("payload", Param::Bytes(vec![b'x'; 1024 * 1024]))],
    )
    .await
    .expect("failed to insert the one mebibyte value");
  let mut client = SpannerClient::new(channel.clone());
  let session = client
    .create_session(CreateSessionRequest {
      database: database_path("db4"),
      session: Some(Session::default()),
    })
    .await
    .expect("failed to create a session")
    .into_inner()
    .name;

  let mut stream = client
    .execute_streaming_sql(ExecuteSqlRequest {
      session,
      transaction: Some(TransactionSelector {
        selector: Some(transaction_selector::Selector::SingleUse(TransactionOptions {
          mode: Some(Mode::ReadOnly(ReadOnly {
            timestamp_bound: Some(TimestampBound::Strong(true)),
            ..Default::default()
          })),
          ..Default::default()
        })),
      }),
      sql: "SELECT payload FROM esa_journal".to_string(),
      ..Default::default()
    })
    .await
    .expect("failed to start the streaming query")
    .into_inner();
  let (mut messages, mut chunked) = (0, 0);
  while let Some(message) = stream.message().await.expect("failed to receive a partial result set") {
    messages += 1;
    if message.chunked_value {
      chunked += 1;
    }
  }

  assert!(
    messages >= 2,
    "the value is split into several partial result sets: {messages}"
  );
  assert!(
    chunked >= 1,
    "a partial result set is marked with chunked_value: {chunked}"
  );
}
