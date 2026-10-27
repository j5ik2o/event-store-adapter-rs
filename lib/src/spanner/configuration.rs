//! 生成時の照合と初期化（SPN-2・SPN-16・SPN-17・SPN-D1b・SPN-D5）。
//!
//! 生成（[`open_checked`]）は、方言・5 テーブルの列と主キー・snapshot_history の行削除ポリシー・config の行を
//! strong の読み取りで観測して、クライアントの設定と照合するだけで、DDL を実行せず、足りない行も作らない。
//! 初期化（[`initialize_spanner_store`]）は、config 以外を同じ手順で照合し、読み書きトランザクションで 5 テーブルが
//! 空であることを確かめてから config を 1 行書く。テーブルと行削除ポリシーは、利用者が先に DDL で用意する。

use std::collections::HashMap;

use googleapis_tonic_google_spanner_admin_database_v1::google::spanner::admin::database::v1::database_admin_client::DatabaseAdminClient;
use googleapis_tonic_google_spanner_admin_database_v1::google::spanner::admin::database::v1::{
  DatabaseDialect, GetDatabaseRequest,
};
use googleapis_tonic_google_spanner_v1::google::spanner::v1::{mutation, Mutation};
use prost_types::{ListValue, Value};

use super::client::{storage_error, tx_failure, wait_for_next_attempt, Rpc, TxError};
use super::values::{
  bool_value, cell, int64_value, null_value, read_bool, read_int64, read_required_int64, read_string, string_value,
  Param,
};
use super::{SpannerOptions, SpannerRetention, SpannerTables};
use crate::error::{ConfigurationReason, EventStoreError, StorageOperation};

/// 識別子の最大の長さ（Spanner の制限）。
const MAX_IDENTIFIER_LENGTH: usize = 128;
/// config の `layout_version`。
const LAYOUT_VERSION: i64 = 1;

pub(super) fn configuration(reason: ConfigurationReason) -> EventStoreError {
  EventStoreError::Configuration { reason }
}

/// 同じ接頭辞から導いた 5 テーブルの名前（SPN-D1b）。
#[derive(Debug, Clone)]
pub(crate) struct TableNames {
  pub(crate) journal: String,
  pub(crate) head: String,
  pub(crate) snapshot_current: String,
  pub(crate) snapshot_history: String,
  pub(crate) config: String,
}

fn is_identifier(name: &str) -> bool {
  let mut characters = name.chars();
  let starts_well = characters
    .next()
    .is_some_and(|first| first.is_ascii_alphabetic() || first == '_');
  starts_well
    && name
      .chars()
      .all(|character| character.is_ascii_alphanumeric() || character == '_')
    && name.len() <= MAX_IDENTIFIER_LENGTH
}

impl TableNames {
  /// 接頭辞から 5 テーブルの名前を導き、SQL の識別子として検査する。通信の前に行う。
  fn derive(prefix: &str) -> Result<Self, EventStoreError> {
    let names = Self {
      journal: format!("{prefix}_journal"),
      head: format!("{prefix}_head"),
      snapshot_current: format!("{prefix}_snapshot_current"),
      snapshot_history: format!("{prefix}_snapshot_history"),
      config: format!("{prefix}_config"),
    };
    if prefix.is_empty() || !names.all().iter().all(|name| is_identifier(name)) {
      return Err(configuration(ConfigurationReason::SpannerInvalidTablePrefix));
    }
    Ok(names)
  }

  fn all(&self) -> [&str; 5] {
    [
      &self.journal,
      &self.head,
      &self.snapshot_current,
      &self.snapshot_history,
      &self.config,
    ]
  }
}

/// 保持の方式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetentionStyle {
  /// 古い履歴を削除する（SPN-12）。
  Delete,
  /// 古い履歴に期限切れの印を付ける。猶予は日数（SPN-16）。
  Ttl { grace_days: i64 },
}

impl RetentionStyle {
  fn recorded_mode(self) -> &'static str {
    match self {
      RetentionStyle::Delete => "delete",
      RetentionStyle::Ttl { .. } => "ttl",
    }
  }

  fn recorded_grace_days(self) -> Option<i64> {
    match self {
      RetentionStyle::Delete => None,
      RetentionStyle::Ttl { grace_days } => Some(grace_days),
    }
  }
}

/// 通信の前に検査した設定。config に記録する値の形にそろえてある。
#[derive(Debug, Clone, Copy)]
pub(crate) struct ValidatedOptions {
  pub(crate) keep_snapshot_count: Option<i64>,
  pub(crate) style: RetentionStyle,
  pub(crate) transaction_retry_limit: u32,
}

fn recordable_count(count: usize) -> Result<i64, EventStoreError> {
  if count == 0 {
    return Err(configuration(ConfigurationReason::KeepSnapshotCountZero));
  }
  i64::try_from(count).map_err(|_| configuration(ConfigurationReason::SpannerKeepSnapshotCountOutOfRange))
}

fn recordable_grace_days(grace_days: u64) -> Result<i64, EventStoreError> {
  match i64::try_from(grace_days) {
    Ok(days) if days > 0 => Ok(days),
    _ => Err(configuration(ConfigurationReason::SpannerTtlGraceDaysOutOfRange)),
  }
}

/// 設定値を検査し、5 テーブルの名前を導く（S-1・SPN-16・SPN-17・SPN-D1b）。通信の前に行う。
///
/// 値は丸めず、config の列（INT64）に収まらない値と、変更フィードを求める設定は、設定エラーにする。
pub(crate) fn validate(
  tables: &SpannerTables,
  options: &SpannerOptions,
) -> Result<(TableNames, ValidatedOptions), EventStoreError> {
  let (keep_snapshot_count, style) = match options.retention {
    SpannerRetention::CurrentOnly => (None, RetentionStyle::Delete),
    SpannerRetention::KeepLatest { count } => (Some(recordable_count(count)?), RetentionStyle::Delete),
    SpannerRetention::KeepLatestWithTtl { count, grace_days } => {
      let count = recordable_count(count)?;
      let grace_days = recordable_grace_days(grace_days)?;
      (Some(count), RetentionStyle::Ttl { grace_days })
    }
  };
  if options.change_feed {
    return Err(configuration(ConfigurationReason::SpannerChangeFeedUnsupported));
  }
  let names = TableNames::derive(&tables.table_prefix)?;
  Ok((
    names,
    ValidatedOptions {
      keep_snapshot_count,
      style,
      transaction_retry_limit: options.transaction_retry_limit,
    },
  ))
}

// ---------------------------------------------------------------------------
// 期待する配置（プロファイル 3 章）
// ---------------------------------------------------------------------------

type Column = (&'static str, &'static str);

const KEY: [Column; 2] = [("hash_prefix", "BYTES(8)"), ("aid", "STRING(1024)")];
const SEQ_NR: Column = ("seq_nr", "INT64");
const MANIFEST: Column = ("manifest", "STRING(MAX)");
const PAYLOAD: Column = ("payload", "BYTES(MAX)");

struct ExpectedTable {
  columns: Vec<Column>,
  primary_key: Vec<&'static str>,
}

impl ExpectedTable {
  /// 観測した列（名前と `SPANNER_TYPE`）が、期待する全列を型まで一致して含むか。余分な列は許す。
  fn has_all_columns(&self, actual: &[(String, String)]) -> bool {
    self.columns.iter().all(|(name, spanner_type)| {
      actual
        .iter()
        .any(|(found, found_type)| found == name && found_type == spanner_type)
    })
  }
}

fn expected_tables(names: &TableNames) -> Vec<(String, ExpectedTable)> {
  let with_key = |rest: &[Column]| KEY.iter().chain(rest.iter()).copied().collect::<Vec<_>>();
  vec![
    (
      names.journal.clone(),
      ExpectedTable {
        columns: with_key(&[SEQ_NR, ("occurred_at_ns", "INT64"), MANIFEST, PAYLOAD]),
        primary_key: vec!["hash_prefix", "aid", "seq_nr"],
      },
    ),
    (
      names.head.clone(),
      ExpectedTable {
        columns: with_key(&[
          ("type_name", "STRING(1024)"),
          SEQ_NR,
          ("event_occurred_at_ns", "INT64"),
          ("event_manifest", "STRING(MAX)"),
          ("event_payload", "BYTES(MAX)"),
        ]),
        primary_key: vec!["hash_prefix", "aid"],
      },
    ),
    (
      names.snapshot_current.clone(),
      ExpectedTable {
        columns: with_key(&[SEQ_NR, MANIFEST, PAYLOAD]),
        primary_key: vec!["hash_prefix", "aid"],
      },
    ),
    (
      names.snapshot_history.clone(),
      ExpectedTable {
        columns: with_key(&[SEQ_NR, MANIFEST, PAYLOAD, ("expired_at", "TIMESTAMP")]),
        primary_key: vec!["hash_prefix", "aid", "seq_nr"],
      },
    ),
    (
      names.config.clone(),
      ExpectedTable {
        columns: vec![
          ("singleton", "INT64"),
          ("store_id", "STRING(36)"),
          ("layout_version", "INT64"),
          ("keep_snapshot_count", "INT64"),
          ("retention_mode", "STRING(8)"),
          ("ttl_grace_days", "INT64"),
          ("change_feed_enabled", "BOOL"),
        ],
        primary_key: vec!["singleton"],
      },
    ),
  ]
}

/// config の 1 行。
#[derive(Debug, Clone)]
pub(crate) struct ConfigRow {
  store_id: String,
  layout_version: i64,
  keep_snapshot_count: Option<i64>,
  retention_mode: String,
  ttl_grace_days: Option<i64>,
  change_feed_enabled: bool,
}

/// 観測した保存先の姿。
#[derive(Debug, Default)]
pub(crate) struct Observed {
  /// 方言が GoogleSQL か。
  pub(crate) google_sql: bool,
  /// テーブル名ごとの、列の名前と型（`SPANNER_TYPE`）の並び。
  pub(crate) columns: HashMap<String, Vec<(String, String)>>,
  /// テーブル名ごとの、主キーの列の並び。
  pub(crate) primary_keys: HashMap<String, Vec<String>>,
  /// snapshot_history の行削除ポリシーの式。なければ `None`。
  pub(crate) row_deletion_policy: Option<String>,
  /// config の行。なければ `None`。
  pub(crate) config: Option<ConfigRow>,
}

/// 行削除ポリシーの式から、`expired_at` 列の `DAY` 単位の日数を取り出す（SPN-16）。
///
/// 大文字小文字と空白の違いは同じ式として扱う。列・単位が違う式と、形が違う式は `None` を返す。
fn policy_days(expression: &str) -> Option<u64> {
  let mut tokens: Vec<String> = Vec::new();
  let mut word = String::new();
  for character in expression.chars() {
    if character.is_whitespace() || matches!(character, '(' | ')' | ',') {
      if !word.is_empty() {
        tokens.push(std::mem::take(&mut word));
      }
      if !character.is_whitespace() {
        tokens.push(character.to_string());
      }
    } else {
      word.push(character);
    }
  }
  if !word.is_empty() {
    tokens.push(word);
  }
  match tokens.as_slice() {
    [older_than, open, column, comma, interval, days, unit, close]
      if older_than.eq_ignore_ascii_case("OLDER_THAN")
        && open == "("
        && column.eq_ignore_ascii_case("expired_at")
        && comma == ","
        && interval.eq_ignore_ascii_case("INTERVAL")
        && unit.eq_ignore_ascii_case("DAY")
        && close == ")" =>
    {
      days
        .chars()
        .all(|character| character.is_ascii_digit())
        .then(|| days.parse().ok())
        .flatten()
    }
    _ => None,
  }
}

fn is_lowercase_uuid(text: &str) -> bool {
  let groups: Vec<&str> = text.split('-').collect();
  groups.len() == 5
    && groups.iter().zip([8_usize, 4, 4, 4, 12]).all(|(group, length)| {
      group.len() == length
        && group
          .chars()
          .all(|character| character.is_ascii_digit() || ('a'..='f').contains(&character))
    })
}

/// 観測した保存先と、クライアントの設定を照合する純粋関数（SPN-2・SPN-16・SPN-17）。
///
/// `require_config` が真なら config の行を要求して `Some(store_id)` を返す。偽なら config は照合せず `None` を返す（初期化）。
pub(crate) fn reconcile(
  observed: &Observed,
  names: &TableNames,
  options: &ValidatedOptions,
  require_config: bool,
) -> Result<Option<String>, EventStoreError> {
  let layout_mismatch = || configuration(ConfigurationReason::SpannerLayoutMismatch);
  let invalid = || configuration(ConfigurationReason::SpannerConfigurationInvalid);
  let mismatch = || configuration(ConfigurationReason::SpannerConfigurationMismatch);
  if !observed.google_sql {
    return Err(layout_mismatch());
  }
  for (table, expected) in expected_tables(names) {
    let actual = observed.columns.get(&table).map(Vec::as_slice).unwrap_or_default();
    let complete = expected.has_all_columns(actual);
    let key = observed.primary_keys.get(&table).map(Vec::as_slice).unwrap_or_default();
    if !complete || key != expected.primary_key.as_slice() {
      return Err(layout_mismatch());
    }
  }
  let policy = observed.row_deletion_policy.as_deref();
  let policy_matches = |grace_days: i64| {
    policy
      .and_then(policy_days)
      .is_some_and(|days| i64::try_from(days).is_ok_and(|days| days == grace_days))
  };
  let store_id = if require_config {
    let config = observed.config.as_ref().ok_or_else(invalid)?;
    if !is_lowercase_uuid(&config.store_id) {
      return Err(invalid());
    }
    if config.layout_version != LAYOUT_VERSION {
      return Err(mismatch());
    }
    if config.keep_snapshot_count.is_some_and(|count| count < 1) {
      return Err(invalid());
    }
    if config.keep_snapshot_count != options.keep_snapshot_count {
      return Err(mismatch());
    }
    if config.retention_mode != "delete" && config.retention_mode != "ttl" {
      return Err(invalid());
    }
    if config.ttl_grace_days.is_some_and(|days| days < 1) {
      return Err(invalid());
    }
    if config.change_feed_enabled {
      return Err(configuration(ConfigurationReason::SpannerChangeFeedUnsupported));
    }
    if config.retention_mode != options.style.recorded_mode() {
      return Err(mismatch());
    }
    if config.ttl_grace_days != options.style.recorded_grace_days() {
      return Err(match options.style {
        RetentionStyle::Delete => invalid(),
        RetentionStyle::Ttl { .. } => mismatch(),
      });
    }
    Some(config.store_id.clone())
  } else {
    None
  };
  match options.style {
    RetentionStyle::Ttl { grace_days } if !policy_matches(grace_days) => return Err(mismatch()),
    RetentionStyle::Delete if policy.is_some() => return Err(mismatch()),
    _ => {}
  }
  Ok(store_id)
}

// ---------------------------------------------------------------------------
// 観測（INFORMATION_SCHEMA と config の読み取り）
// ---------------------------------------------------------------------------

impl Rpc {
  /// データベースの方言が GoogleSQL か（管理 API の `GetDatabase`）。
  async fn is_google_sql(&self, operation: StorageOperation) -> Result<bool, EventStoreError> {
    let request = self
      .authorizer
      .authorize(
        GetDatabaseRequest {
          name: self.database.clone(),
        },
        &self.database,
        operation,
      )
      .await?;
    let database = DatabaseAdminClient::new(self.channel.clone())
      .get_database(request)
      .await
      .map_err(|status| storage_error(operation, status))?
      .into_inner();
    Ok(database.database_dialect == DatabaseDialect::GoogleStandardSql as i32)
  }
}

const COLUMNS_SQL: &str = "SELECT TABLE_NAME, COLUMN_NAME, SPANNER_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '' AND TABLE_NAME IN UNNEST(@tables) ORDER BY TABLE_NAME, ORDINAL_POSITION";
const PRIMARY_KEY_SQL: &str = "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.INDEX_COLUMNS WHERE INDEX_NAME = 'PRIMARY_KEY' AND TABLE_SCHEMA = '' AND TABLE_NAME IN UNNEST(@tables) ORDER BY TABLE_NAME, ORDINAL_POSITION";
const POLICY_SQL: &str = "SELECT ROW_DELETION_POLICY_EXPRESSION FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '' AND TABLE_NAME = @table";

fn malformed(
  error: impl Into<Box<dyn std::error::Error + Send + Sync>>,
  operation: StorageOperation,
) -> EventStoreError {
  storage_error(operation, error)
}

/// 方言・列・主キー・行削除ポリシー・config を、strong の読み取り専用トランザクションで観測する。
async fn observe(
  rpc: &Rpc,
  names: &TableNames,
  with_config: bool,
  operation: StorageOperation,
) -> Result<Observed, EventStoreError> {
  if !rpc.is_google_sql(operation).await? {
    return Ok(Observed::default());
  }
  let session = rpc.open_session(operation).await?;
  let observed = observe_in_session(&session, names, with_config, operation).await;
  session.close().await;
  observed
}

async fn observe_in_session(
  session: &super::client::SessionHandle,
  names: &TableNames,
  with_config: bool,
  operation: StorageOperation,
) -> Result<Observed, EventStoreError> {
  let transaction = session
    .begin_read_only()
    .await
    .map_err(|error| tx_failure(error, operation))?;
  let fail = |error| tx_failure(error, operation);
  let tables = Param::StringArray(names.all().iter().map(|name| (*name).to_owned()).collect());
  let mut observed = Observed {
    google_sql: true,
    ..Default::default()
  };
  for row in session
    .query(&transaction, COLUMNS_SQL, &[("tables", tables.clone())])
    .await
    .map_err(fail)?
  {
    let to_error = |error| malformed(error, operation);
    let table = read_string(cell(&row, 0).map_err(to_error)?).map_err(to_error)?;
    let column = read_string(cell(&row, 1).map_err(to_error)?).map_err(to_error)?;
    let spanner_type = read_string(cell(&row, 2).map_err(to_error)?).map_err(to_error)?;
    observed.columns.entry(table).or_default().push((column, spanner_type));
  }
  for row in session
    .query(&transaction, PRIMARY_KEY_SQL, &[("tables", tables)])
    .await
    .map_err(fail)?
  {
    let to_error = |error| malformed(error, operation);
    let table = read_string(cell(&row, 0).map_err(to_error)?).map_err(to_error)?;
    let column = read_string(cell(&row, 1).map_err(to_error)?).map_err(to_error)?;
    observed.primary_keys.entry(table).or_default().push(column);
  }
  let policy_rows = session
    .query(
      &transaction,
      POLICY_SQL,
      &[("table", Param::String(names.snapshot_history.clone()))],
    )
    .await
    .map_err(fail)?;
  if let Some(row) = policy_rows.first() {
    let value = cell(row, 0).map_err(|error| malformed(error, operation))?;
    if !matches!(value.kind, None | Some(prost_types::value::Kind::NullValue(_))) {
      observed.row_deletion_policy = Some(read_string(value).map_err(|error| malformed(error, operation))?);
    }
  }
  let config_complete = expected_tables(names)
    .into_iter()
    .find(|(table, _)| *table == names.config)
    .is_some_and(|(table, expected)| {
      let actual = observed.columns.get(&table).map(Vec::as_slice).unwrap_or_default();
      expected.has_all_columns(actual)
    });
  if with_config && config_complete {
    let sql = format!(
      "SELECT store_id, layout_version, keep_snapshot_count, retention_mode, ttl_grace_days, change_feed_enabled FROM `{}` WHERE singleton = 1",
      names.config
    );
    let rows = session.query(&transaction, &sql, &[]).await.map_err(fail)?;
    if let Some(row) = rows.first() {
      observed.config = parse_config_row(row);
    }
  }
  Ok(observed)
}

/// config の行を読む。形が壊れていれば `None`（config の行がないのと同じく、`SpannerConfigurationInvalid` になる）。
fn parse_config_row(row: &[Value]) -> Option<ConfigRow> {
  Some(ConfigRow {
    store_id: read_string(cell(row, 0).ok()?).ok()?,
    layout_version: read_required_int64(cell(row, 1).ok()?).ok()?,
    keep_snapshot_count: read_int64(cell(row, 2).ok()?).ok()?,
    retention_mode: read_string(cell(row, 3).ok()?).ok()?,
    ttl_grace_days: read_int64(cell(row, 4).ok()?).ok()?,
    change_feed_enabled: read_bool(cell(row, 5).ok()?).ok()?,
  })
}

/// 生成した結果。
pub(crate) struct Opened {
  pub(crate) names: TableNames,
  pub(crate) options: ValidatedOptions,
  pub(crate) store_id: String,
}

/// 設定を検査し、保存先を観測して照合する。DDL は実行せず、行も作らない（SPN-2・SPN-D5）。
pub(crate) async fn open_checked(
  rpc: &Rpc,
  tables: &SpannerTables,
  options: &SpannerOptions,
) -> Result<Opened, EventStoreError> {
  let (names, validated) = validate(tables, options)?;
  let observed = observe(rpc, &names, true, StorageOperation::ReadConfiguration).await?;
  let store_id = reconcile(&observed, &names, &validated, true)?
    .ok_or_else(|| configuration(ConfigurationReason::SpannerConfigurationInvalid))?;
  Ok(Opened {
    names,
    options: validated,
    store_id,
  })
}

fn mutation_insert(table: &str, columns: &[&str], values: Vec<Value>) -> Mutation {
  Mutation {
    operation: Some(mutation::Operation::Insert(mutation::Write {
      table: table.to_owned(),
      columns: columns.iter().map(|column| (*column).to_owned()).collect(),
      values: vec![ListValue { values }],
    })),
  }
}

/// 初期化する。config 以外を照合し、5 テーブルが空であることを読み書きトランザクションで確かめて、config を 1 行書く（SPN-D5）。
///
/// 5 テーブルと行削除ポリシーは、利用者が先に DDL で用意しておく。DDL は実行しない。
/// 既に config の行があるか、テーブルが空でなければ、`SpannerAlreadyInitialized` にする。
pub(crate) async fn initialize(
  rpc: &Rpc,
  tables: &SpannerTables,
  options: &SpannerOptions,
) -> Result<(), EventStoreError> {
  let operation = StorageOperation::CreateConfiguration;
  let (names, validated) = validate(tables, options)?;
  let observed = observe(rpc, &names, false, operation).await?;
  reconcile(&observed, &names, &validated, false)?;
  let session = rpc.open_session(operation).await?;
  let result = initialize_in_session(&session, &names, &validated, operation).await;
  session.close().await;
  result
}

async fn initialize_in_session(
  session: &super::client::SessionHandle,
  names: &TableNames,
  options: &ValidatedOptions,
  operation: StorageOperation,
) -> Result<(), EventStoreError> {
  let store_id = uuid::Uuid::new_v4().to_string();
  let mut attempt = 0;
  loop {
    match try_initialize(session, names, options, &store_id).await {
      Ok(()) => return Ok(()),
      Err(TxError::Aborted) => {
        attempt = wait_for_next_attempt(attempt, options.transaction_retry_limit, operation).await?;
      }
      Err(TxError::Failed(error)) => return Err(error),
    }
  }
}

async fn try_initialize(
  session: &super::client::SessionHandle,
  names: &TableNames,
  options: &ValidatedOptions,
  store_id: &str,
) -> Result<(), TxError> {
  let transaction = session.begin_read_write().await?;
  let outcome = async {
    for table in names.all() {
      let rows = session
        .query(&transaction, &format!("SELECT 1 FROM `{table}` LIMIT 1"), &[])
        .await?;
      if !rows.is_empty() {
        return Err(TxError::Failed(configuration(
          ConfigurationReason::SpannerAlreadyInitialized,
        )));
      }
    }
    let row = mutation_insert(
      &names.config,
      &[
        "singleton",
        "store_id",
        "layout_version",
        "keep_snapshot_count",
        "retention_mode",
        "ttl_grace_days",
        "change_feed_enabled",
      ],
      vec![
        int64_value(1),
        string_value(store_id),
        int64_value(LAYOUT_VERSION),
        options.keep_snapshot_count.map_or_else(null_value, int64_value),
        string_value(options.style.recorded_mode()),
        options.style.recorded_grace_days().map_or_else(null_value, int64_value),
        bool_value(false),
      ],
    );
    session.commit(&transaction, vec![row]).await
  }
  .await;
  if outcome.is_err() {
    session.rollback(&transaction).await;
  }
  outcome.map_err(|error| match error {
    // 同時に初期化した別の書き手が、先に config を書いた。
    TxError::Failed(EventStoreError::Storage { source, .. })
      if source
        .downcast_ref::<tonic::Status>()
        .is_some_and(|status| status.code() == tonic::Code::AlreadyExists) =>
    {
      TxError::Failed(configuration(ConfigurationReason::SpannerAlreadyInitialized))
    }
    other => other,
  })
}

#[cfg(test)]
#[path = "configuration_test.rs"]
mod tests;
