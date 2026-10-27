//! Spanner エミュレーターを試験で使うための補助。
//!
//! - エミュレーターの起動（試験のプロセスの中で共有する）、インスタンスとデータベースの作成、
//!   プロファイル（`storage/spanner.md` 3 章）どおりの 5 テーブルの DDL。
//! - ライブラリを通さない生の SQL の実行（行の読み取り・DML）。期待値は、ライブラリとは独立にこの補助で確かめる。
//!
//! 実サービスには接続しない。接続先は、エミュレーターの公開ポートだけである。

use std::collections::HashMap;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Result};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use googleapis_tonic_google_spanner_admin_database_v1::google::spanner::admin::database::v1::database_admin_client::DatabaseAdminClient;
use googleapis_tonic_google_spanner_admin_database_v1::google::spanner::admin::database::v1::{
  database::State as DatabaseState, CreateDatabaseRequest, DatabaseDialect, DropDatabaseRequest, GetDatabaseRequest,
};
use googleapis_tonic_google_spanner_admin_instance_v1::google::spanner::admin::instance::v1::instance_admin_client::InstanceAdminClient;
use googleapis_tonic_google_spanner_admin_instance_v1::google::spanner::admin::instance::v1::{
  CreateInstanceRequest, Instance, ListInstanceConfigsRequest,
};
use googleapis_tonic_google_spanner_v1::google::spanner::v1::spanner_client::SpannerClient;
use googleapis_tonic_google_spanner_v1::google::spanner::v1::transaction_options::{
  read_only::TimestampBound, Mode, ReadOnly, ReadWrite,
};
use googleapis_tonic_google_spanner_v1::google::spanner::v1::{
  commit_request, result_set_stats, transaction_selector, BeginTransactionRequest, CommitRequest, CreateSessionRequest,
  DeleteSessionRequest, ExecuteSqlRequest, Session, TransactionOptions, TransactionSelector, Type, TypeCode,
};
use prost_types::value::Kind;
use prost_types::{Struct, Value};
use tonic::transport::{Channel, Endpoint};
use tonic::Code;

/// 全ての試験が使うプロジェクトの ID。
pub const PROJECT: &str = "test-project";
/// 全ての試験が使うインスタンスの ID。
pub const INSTANCE: &str = "test-instance";
/// エミュレーターが持つインスタンス構成。
pub const INSTANCE_CONFIG: &str = "emulator-config";

/// エミュレーターの公開ポートへ接続する。認証は使わない。
pub async fn connect(port: u16) -> Result<Channel> {
  let endpoint = Endpoint::from_shared(format!("http://127.0.0.1:{port}"))?;
  Ok(endpoint.connect().await?)
}

/// 待ち合わせの 1 回の試行（接続と `ListInstanceConfigs`）の上限。応答しないコンテナで 1 回の試行が終わらなくなるのを防ぐ。
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

/// インスタンス管理 API の `ListInstanceConfigs` が成功するまで待つ（250 ミリ秒ごと、上限 120 秒）。
/// 1 回の試行は 5 秒で打ち切り、時間切れはその試行の失敗として扱う。上限を超えたときは、最後の誤りを含めて `Err` を返す。
pub(crate) async fn wait_until_list_instance_configs_succeeds(port: u16) -> Result<()> {
  let started_at = Instant::now();
  loop {
    let attempt = tokio::time::timeout(ATTEMPT_TIMEOUT, async {
      let channel = connect(port).await?;
      InstanceAdminClient::new(channel)
        .list_instance_configs(ListInstanceConfigsRequest {
          parent: format!("projects/{PROJECT}"),
          ..Default::default()
        })
        .await?;
      anyhow::Ok(())
    })
    .await
    .unwrap_or_else(|_| {
      Err(anyhow!(
        "ListInstanceConfigs attempt did not finish within {} seconds",
        ATTEMPT_TIMEOUT.as_secs()
      ))
    });
    match attempt {
      Ok(()) => return Ok(()),
      Err(error) => {
        if started_at.elapsed() >= Duration::from_secs(120) {
          bail!("ListInstanceConfigs did not succeed within 120 seconds: {error:?}");
        }
      }
    }
    tokio::time::sleep(Duration::from_millis(250)).await;
  }
}

// ---------------------------------------------------------------------------
// エミュレーター（プロセスの中で共有する）
// ---------------------------------------------------------------------------

/// 専用のスレッドと実行基盤でコンテナを持つ。落とすとコンテナを削除する。
pub struct EmulatorHost {
  port: u16,
  stop: Option<Sender<()>>,
  thread: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for EmulatorHost {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    formatter
      .debug_struct("EmulatorHost")
      .field("port", &self.port)
      .finish()
  }
}

impl EmulatorHost {
  /// エミュレーターのコンテナを起動する。起動の失敗は、その説明を返す。
  pub fn start() -> std::result::Result<Self, String> {
    let (port_sender, port_receiver) = channel::<std::result::Result<u16, String>>();
    let (stop, stop_receiver) = channel::<()>();
    let thread = std::thread::spawn(move || {
      let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
      {
        Ok(runtime) => runtime,
        Err(error) => {
          let _ = port_sender.send(Err(format!("エミュレーター用の実行基盤を作れない: {error}")));
          return;
        }
      };
      runtime.block_on(async move {
        let container = match crate::docker::spanner_emulator().await {
          Ok(container) => container,
          Err(error) => {
            let _ = port_sender.send(Err(format!("エミュレーターを起動できない: {error:?}")));
            return;
          }
        };
        let port = match container.get_host_port_ipv4(crate::docker::SPANNER_EMULATOR_PORT).await {
          Ok(port) => port,
          Err(error) => {
            let _ = port_sender.send(Err(format!("公開ポートを得られない: {error:?}")));
            return;
          }
        };
        let _ = port_sender.send(Ok(port));
        // 使う側が全ていなくなる（送信側が落ちる）まで、コンテナを持ち続ける。
        let _ = tokio::task::spawn_blocking(move || stop_receiver.recv()).await;
        drop(container);
      });
    });
    let port = port_receiver
      .recv()
      .map_err(|error| format!("エミュレーターのポートを受け取れない: {error}"))??;
    Ok(Self {
      port,
      stop: Some(stop),
      thread: Some(thread),
    })
  }

  /// エミュレーターの gRPC の公開ポート。
  pub fn port(&self) -> u16 {
    self.port
  }
}

impl Drop for EmulatorHost {
  fn drop(&mut self) {
    drop(self.stop.take());
    if let Some(thread) = self.thread.take() {
      let _ = thread.join();
    }
  }
}

static SHARED_EMULATOR: Mutex<Weak<EmulatorHost>> = Mutex::new(Weak::new());

/// プロセスの中で共有するエミュレーターを返す。使っている側が 1 つもなくなったときにコンテナを削除する。
pub fn shared_emulator() -> std::result::Result<Arc<EmulatorHost>, String> {
  let mut slot = SHARED_EMULATOR
    .lock()
    .map_err(|_| "エミュレーターの共有のロックが壊れている".to_string())?;
  if let Some(host) = slot.upgrade() {
    return Ok(host);
  }
  let host = Arc::new(EmulatorHost::start()?);
  *slot = Arc::downgrade(&host);
  Ok(host)
}

// ---------------------------------------------------------------------------
// インスタンス・データベース・DDL
// ---------------------------------------------------------------------------

/// データベースのリソース名。
pub fn database_path(database_id: &str) -> String {
  format!("projects/{PROJECT}/instances/{INSTANCE}/databases/{database_id}")
}

/// プロファイル 3 章（SPN-D1b）の 5 テーブルを、接頭辞 `prefix` から導いた名前で作る DDL。
///
/// `row_deletion_policy_days` を指定すると、snapshot_history に `ROW DELETION POLICY (OLDER_THAN(expired_at, INTERVAL n DAY))`
/// を付ける（TTL 方式）。`None` なら付けない。
pub fn store_ddl(prefix: &str, row_deletion_policy_days: Option<i64>) -> Vec<String> {
  let policy = row_deletion_policy_days
    .map(|days| format!(", ROW DELETION POLICY (OLDER_THAN(expired_at, INTERVAL {days} DAY))"))
    .unwrap_or_default();
  let key = "hash_prefix BYTES(8) NOT NULL, aid STRING(1024) NOT NULL";
  vec![
    format!("CREATE TABLE {prefix}_journal ({key}, seq_nr INT64 NOT NULL, occurred_at_ns INT64 NOT NULL, manifest STRING(MAX) NOT NULL, payload BYTES(MAX) NOT NULL) PRIMARY KEY (hash_prefix, aid, seq_nr)"),
    format!("CREATE TABLE {prefix}_head ({key}, type_name STRING(1024) NOT NULL, seq_nr INT64 NOT NULL, event_occurred_at_ns INT64 NOT NULL, event_manifest STRING(MAX) NOT NULL, event_payload BYTES(MAX) NOT NULL) PRIMARY KEY (hash_prefix, aid)"),
    format!("CREATE TABLE {prefix}_snapshot_current ({key}, seq_nr INT64 NOT NULL, manifest STRING(MAX) NOT NULL, payload BYTES(MAX) NOT NULL) PRIMARY KEY (hash_prefix, aid)"),
    format!("CREATE TABLE {prefix}_snapshot_history ({key}, seq_nr INT64 NOT NULL, manifest STRING(MAX) NOT NULL, payload BYTES(MAX) NOT NULL, expired_at TIMESTAMP) PRIMARY KEY (hash_prefix, aid, seq_nr){policy}"),
    format!("CREATE TABLE {prefix}_config (singleton INT64 NOT NULL, store_id STRING(36) NOT NULL, layout_version INT64 NOT NULL, keep_snapshot_count INT64, retention_mode STRING(8) NOT NULL, ttl_grace_days INT64, change_feed_enabled BOOL NOT NULL) PRIMARY KEY (singleton)"),
  ]
}

/// 試験用のインスタンスを作る。既にあれば何もしない（同じエミュレーターを共有する試験が、どの順でも呼べる）。
pub async fn ensure_instance(channel: &Channel) -> Result<()> {
  let result = InstanceAdminClient::new(channel.clone())
    .create_instance(CreateInstanceRequest {
      parent: format!("projects/{PROJECT}"),
      instance_id: INSTANCE.to_string(),
      instance: Some(Instance {
        config: format!("projects/{PROJECT}/instanceConfigs/{INSTANCE_CONFIG}"),
        display_name: INSTANCE.to_string(),
        node_count: 1,
        ..Default::default()
      }),
    })
    .await;
  match result {
    Ok(_) => Ok(()),
    Err(status) if status.code() == Code::AlreadyExists => Ok(()),
    Err(status) => Err(anyhow!("CreateInstance failed: {status:?}")),
  }
}

/// データベースを作り、`statements`（DDL）を適用して、`READY` になるまで待つ。インスタンスがなければ先に作る。
/// 方言は GoogleSQL である。
pub async fn create_database(channel: &Channel, database_id: &str, statements: Vec<String>) -> Result<()> {
  create_database_with_dialect(channel, database_id, DatabaseDialect::GoogleStandardSql, statements).await
}

/// PostgreSQL 方言の、テーブルのないデータベースを作る。ライブラリが GoogleSQL 以外の方言を配置の不一致として拒否することの試験に使う。
pub async fn create_postgresql_database(channel: &Channel, database_id: &str) -> Result<()> {
  create_database_with_dialect(channel, database_id, DatabaseDialect::Postgresql, vec![]).await
}

/// 方言を指定してデータベースを作る。ライブラリが GoogleSQL 以外の方言を配置の不一致として拒否することの試験に使う。
pub async fn create_database_with_dialect(
  channel: &Channel,
  database_id: &str,
  dialect: DatabaseDialect,
  statements: Vec<String>,
) -> Result<()> {
  ensure_instance(channel).await?;
  let mut admin = DatabaseAdminClient::new(channel.clone());
  let create_statement = match dialect {
    DatabaseDialect::Postgresql => format!("CREATE DATABASE \"{database_id}\""),
    _ => format!("CREATE DATABASE `{database_id}`"),
  };
  admin
    .create_database(CreateDatabaseRequest {
      parent: format!("projects/{PROJECT}/instances/{INSTANCE}"),
      create_statement,
      extra_statements: statements,
      database_dialect: dialect as i32,
      ..Default::default()
    })
    .await
    .map_err(|status| anyhow!("CreateDatabase failed: {status:?}"))?;
  let started_at = Instant::now();
  loop {
    let database = admin
      .get_database(GetDatabaseRequest {
        name: database_path(database_id),
      })
      .await
      .map_err(|status| anyhow!("GetDatabase failed: {status:?}"))?
      .into_inner();
    if database.state == DatabaseState::Ready as i32 {
      return Ok(());
    }
    if started_at.elapsed() >= Duration::from_secs(60) {
      bail!("database {database_id} did not become READY within 60 seconds");
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
  }
}

/// データベースを削除する。エミュレーターはインスタンスごとのデータベース数に上限を持つので、場面ごとに新しいデータベースを
/// 作る呼び出し側が、使い終えたものを削除するのに使う。
pub async fn drop_database(channel: &Channel, database_id: &str) -> Result<()> {
  DatabaseAdminClient::new(channel.clone())
    .drop_database(DropDatabaseRequest {
      database: database_path(database_id),
    })
    .await
    .map(|_| ())
    .map_err(|status| anyhow!("DropDatabase failed: {status:?}"))
}

/// config の 1 行（プロファイル 2 章）。`insert_configuration` が、ライブラリを通さず生の SQL で書く。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeedConfiguration {
  pub store_id: String,
  pub layout_version: i64,
  pub keep_snapshot_count: Option<i64>,
  pub retention_mode: String,
  pub ttl_grace_days: Option<i64>,
  pub change_feed_enabled: bool,
}

impl SeedConfiguration {
  /// 保持件数なし・即時削除方式・変更フィードなしの、版 1 の設定。
  pub fn current_only(store_id: &str) -> Self {
    Self {
      store_id: store_id.to_string(),
      layout_version: 1,
      keep_snapshot_count: None,
      retention_mode: "delete".to_string(),
      ttl_grace_days: None,
      change_feed_enabled: false,
    }
  }
}

// ---------------------------------------------------------------------------
// 生の SQL（ライブラリを通さない）
// ---------------------------------------------------------------------------

/// SQL の名前付きパラメーターの値。型もここで決まる。
#[derive(Debug, Clone, PartialEq)]
pub enum Param {
  Int64(i64),
  NullInt64,
  String(String),
  Bytes(Vec<u8>),
  Bool(bool),
}

impl Param {
  fn type_code(&self) -> TypeCode {
    match self {
      Param::Int64(_) | Param::NullInt64 => TypeCode::Int64,
      Param::String(_) => TypeCode::String,
      Param::Bytes(_) => TypeCode::Bytes,
      Param::Bool(_) => TypeCode::Bool,
    }
  }

  fn value(&self) -> Value {
    let kind = match self {
      // INT64 は、number を経由せず、10 進の文字列で渡す（SPN-5）。
      Param::Int64(value) => Kind::StringValue(value.to_string()),
      Param::NullInt64 => Kind::NullValue(0),
      Param::String(value) => Kind::StringValue(value.clone()),
      Param::Bytes(value) => Kind::StringValue(STANDARD.encode(value)),
      Param::Bool(value) => Kind::BoolValue(*value),
    };
    Value { kind: Some(kind) }
  }
}

/// 結果の 1 つの値。列の型（`ResultSetMetadata.row_type`）に従って復号する。
#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
  Null,
  Bool(bool),
  Int64(i64),
  String(String),
  Bytes(Vec<u8>),
  /// TIMESTAMP は RFC 3339 の文字列のまま返す。
  Timestamp(String),
}

impl Cell {
  pub fn int64(&self) -> i64 {
    match self {
      Cell::Int64(value) => *value,
      other => panic!("INT64 の値を期待したが {other:?}"),
    }
  }

  pub fn string(&self) -> &str {
    match self {
      Cell::String(value) => value,
      other => panic!("STRING の値を期待したが {other:?}"),
    }
  }

  pub fn bytes(&self) -> &[u8] {
    match self {
      Cell::Bytes(value) => value,
      other => panic!("BYTES の値を期待したが {other:?}"),
    }
  }

  pub fn is_null(&self) -> bool {
    matches!(self, Cell::Null)
  }
}

/// 1 つのデータベースへ、セッションを使い捨てにして生の SQL を送る。
#[derive(Debug, Clone)]
pub struct RawSql {
  channel: Channel,
  database: String,
}

/// 読み書きトランザクションが ABORTED になったときにやり直す上限。エミュレーターは、重なる読み書きトランザクションを中断する。
const RAW_RETRY_LIMIT: usize = 20;

impl RawSql {
  pub fn new(channel: Channel, database_id: &str) -> Self {
    Self {
      channel,
      database: database_path(database_id),
    }
  }

  fn params_of(params: &[(&str, Param)]) -> (Option<Struct>, HashMap<String, Type>) {
    let mut fields = std::collections::BTreeMap::new();
    let mut types = HashMap::new();
    for (name, param) in params {
      fields.insert((*name).to_string(), param.value());
      types.insert(
        (*name).to_string(),
        Type {
          code: param.type_code() as i32,
          ..Default::default()
        },
      );
    }
    (Some(Struct { fields }), types)
  }

  async fn session(&self) -> Result<(SpannerClient<Channel>, String)> {
    let mut client = SpannerClient::new(self.channel.clone());
    let session = client
      .create_session(CreateSessionRequest {
        database: self.database.clone(),
        session: Some(Session::default()),
      })
      .await
      .map_err(|status| anyhow!("CreateSession failed: {status:?}"))?
      .into_inner();
    Ok((client, session.name))
  }

  /// 強整合の読み取り専用トランザクションで SQL を実行し、行を返す。
  pub async fn query(&self, sql: &str, params: &[(&str, Param)]) -> Result<Vec<Vec<Cell>>> {
    let (mut client, session) = self.session().await?;
    let (params, param_types) = Self::params_of(params);
    let result = client
      .execute_sql(ExecuteSqlRequest {
        session: session.clone(),
        transaction: Some(TransactionSelector {
          selector: Some(transaction_selector::Selector::SingleUse(TransactionOptions {
            mode: Some(Mode::ReadOnly(ReadOnly {
              timestamp_bound: Some(TimestampBound::Strong(true)),
              ..Default::default()
            })),
            ..Default::default()
          })),
        }),
        sql: sql.to_string(),
        params,
        param_types,
        ..Default::default()
      })
      .await;
    let _ = client.delete_session(DeleteSessionRequest { name: session }).await;
    let result_set = result
      .map_err(|status| anyhow!("ExecuteSql failed: {status:?}"))?
      .into_inner();
    let types: Vec<i32> = result_set
      .metadata
      .and_then(|metadata| metadata.row_type)
      .map(|row_type| {
        row_type
          .fields
          .iter()
          .map(|field| field.r#type.as_ref().map(|t| t.code).unwrap_or_default())
          .collect()
      })
      .unwrap_or_default();
    result_set
      .rows
      .into_iter()
      .map(|row| {
        row
          .values
          .into_iter()
          .zip(types.iter())
          .map(|(value, code)| decode_cell(value, *code))
          .collect()
      })
      .collect()
  }

  /// SERIALIZABLE の読み書きトランザクションで DML を 1 つ実行してコミットし、影響した行数を返す。
  /// ABORTED は、上限までやり直す。
  pub async fn execute(&self, sql: &str, params: &[(&str, Param)]) -> Result<i64> {
    self.execute_all(&[(sql, params)]).await.map(|counts| counts[0])
  }

  /// 複数の DML を 1 つの SERIALIZABLE の読み書きトランザクションで実行してコミットし、文ごとの影響した行数を返す。
  pub async fn execute_all(&self, statements: &[(&str, &[(&str, Param)])]) -> Result<Vec<i64>> {
    let mut last_error = None;
    for _ in 0..RAW_RETRY_LIMIT {
      match self.execute_all_once(statements).await {
        Ok(counts) => return Ok(counts),
        Err(RawFailure::Aborted(error)) => {
          last_error = Some(error);
          tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Err(RawFailure::Other(error)) => return Err(error),
      }
    }
    Err(last_error.unwrap_or_else(|| anyhow!("ABORTED が続いた")))
  }

  async fn execute_all_once(
    &self,
    statements: &[(&str, &[(&str, Param)])],
  ) -> std::result::Result<Vec<i64>, RawFailure> {
    let (mut client, session) = self.session().await.map_err(RawFailure::Other)?;
    let result = self.run_in_transaction(&mut client, &session, statements).await;
    let _ = client.delete_session(DeleteSessionRequest { name: session }).await;
    result
  }

  async fn run_in_transaction(
    &self,
    client: &mut SpannerClient<Channel>,
    session: &str,
    statements: &[(&str, &[(&str, Param)])],
  ) -> std::result::Result<Vec<i64>, RawFailure> {
    let classify = |status: tonic::Status| {
      if status.code() == Code::Aborted {
        RawFailure::Aborted(anyhow!("aborted: {status:?}"))
      } else {
        RawFailure::Other(anyhow!("{status:?}"))
      }
    };
    let transaction = client
      .begin_transaction(BeginTransactionRequest {
        session: session.to_string(),
        options: Some(TransactionOptions {
          mode: Some(Mode::ReadWrite(ReadWrite::default())),
          isolation_level:
            googleapis_tonic_google_spanner_v1::google::spanner::v1::transaction_options::IsolationLevel::Serializable
              as i32,
          ..Default::default()
        }),
        ..Default::default()
      })
      .await
      .map_err(classify)?
      .into_inner();
    let mut counts = vec![];
    for (index, (sql, params)) in statements.iter().enumerate() {
      let (params, param_types) = Self::params_of(params);
      let result_set = client
        .execute_sql(ExecuteSqlRequest {
          session: session.to_string(),
          transaction: Some(TransactionSelector {
            selector: Some(transaction_selector::Selector::Id(transaction.id.clone())),
          }),
          sql: (*sql).to_string(),
          params,
          param_types,
          seqno: index as i64 + 1,
          ..Default::default()
        })
        .await
        .map_err(classify)?
        .into_inner();
      counts.push(match result_set.stats.and_then(|stats| stats.row_count) {
        Some(result_set_stats::RowCount::RowCountExact(count)) => count,
        _ => 0,
      });
    }
    client
      .commit(CommitRequest {
        session: session.to_string(),
        transaction: Some(commit_request::Transaction::TransactionId(transaction.id)),
        ..Default::default()
      })
      .await
      .map_err(classify)?;
    Ok(counts)
  }
}

enum RawFailure {
  Aborted(anyhow::Error),
  Other(anyhow::Error),
}

fn decode_cell(value: Value, code: i32) -> Result<Cell> {
  match value.kind {
    None | Some(Kind::NullValue(_)) => Ok(Cell::Null),
    Some(Kind::BoolValue(value)) => Ok(Cell::Bool(value)),
    Some(Kind::StringValue(text)) => match code {
      c if c == TypeCode::Int64 as i32 => Ok(Cell::Int64(text.parse()?)),
      c if c == TypeCode::Bytes as i32 => Ok(Cell::Bytes(STANDARD.decode(text)?)),
      c if c == TypeCode::Timestamp as i32 => Ok(Cell::Timestamp(text)),
      _ => Ok(Cell::String(text)),
    },
    Some(other) => bail!("想定していない値の種類: {other:?}"),
  }
}

/// config の 1 行を、ライブラリを通さず生の DML で書く（初期化の手順を経ない状態を作る試験用）。
pub async fn insert_configuration(raw: &RawSql, prefix: &str, configuration: &SeedConfiguration) -> Result<()> {
  raw
    .execute(
      &format!(
        "INSERT INTO {prefix}_config (singleton, store_id, layout_version, keep_snapshot_count, retention_mode, ttl_grace_days, change_feed_enabled) VALUES (1, @store_id, @layout_version, @keep_snapshot_count, @retention_mode, @ttl_grace_days, @change_feed_enabled)"
      ),
      &[
        ("store_id", Param::String(configuration.store_id.clone())),
        ("layout_version", Param::Int64(configuration.layout_version)),
        (
          "keep_snapshot_count",
          configuration.keep_snapshot_count.map(Param::Int64).unwrap_or(Param::NullInt64),
        ),
        ("retention_mode", Param::String(configuration.retention_mode.clone())),
        (
          "ttl_grace_days",
          configuration.ttl_grace_days.map(Param::Int64).unwrap_or(Param::NullInt64),
        ),
        ("change_feed_enabled", Param::Bool(configuration.change_feed_enabled)),
      ],
    )
    .await?;
  Ok(())
}
