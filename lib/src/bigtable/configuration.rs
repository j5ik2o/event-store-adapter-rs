//! 設定値の検査と、列ファミリ・設定行（`config:metadata`）の照合・初期化（2 章・BT-2・BT-13・BT-16・BT-17）。
//!
//! 設定行のキーは `0x00` で、`config:metadata` に UTF-8 の JSON オブジェクトを保存する。
//! ストアの生成（[`open_checked`]）はテーブルも設定行も作らず、照合だけを行う（BT-D1）。

use std::collections::HashSet;
use std::fmt;

use googleapis_tonic_google_bigtable_admin_v2::google::bigtable::admin::v2::bigtable_table_admin_client::BigtableTableAdminClient;
use googleapis_tonic_google_bigtable_admin_v2::google::bigtable::admin::v2::{gc_rule, table, GetTableRequest};
use serde::de::{Deserializer, MapAccess, Visitor};
use serde::Deserialize;
use serde_json::{json, Value};
use tonic::transport::Channel;
use uuid::Uuid;

use super::keys::CONFIG_ROW_KEY;
#[cfg(feature = "test-hooks")]
use super::rows::replace_cell;
use super::rows::{
  latest_cell_exists, set_cell, TableTarget, CONFIG_METADATA, FAMILY_CONFIG, FAMILY_EVENT, FAMILY_HEAD, FAMILY_HISTORY,
  FAMILY_SNAPSHOT,
};
use super::{BigtableConnection, BigtableOptions, BigtableTables};
use crate::error::{ConfigurationReason, EventStoreError, StorageOperation};
use crate::retention::RetentionMode;

/// 封筒の上限の最小。最小の封筒（スナップショット封筒の固定部分）は 17 バイトである（BT-16）。
const MINIMUM_ENVELOPE_LIMIT: u64 = 17;
/// 封筒の上限の最大（8MiB）。
const MAXIMUM_ENVELOPE_LIMIT: u64 = 8_388_608;

pub(super) fn configuration(reason: ConfigurationReason) -> EventStoreError {
  EventStoreError::Configuration { reason }
}

fn invalid_configuration() -> EventStoreError {
  configuration(ConfigurationReason::BigtableConfigurationInvalid)
}

fn mismatched_configuration() -> EventStoreError {
  configuration(ConfigurationReason::BigtableConfigurationMismatch)
}

fn storage(operation: StorageOperation, status: tonic::Status) -> EventStoreError {
  EventStoreError::Storage {
    operation,
    source: Box::new(status),
  }
}

/// 設定値を検査し、設定行に記録する保持件数を返す（S-1・BT-13・BT-16・BT-17）。通信の前に行う。
///
/// 保持件数 0、期限切れ方式、変更フィード、範囲外の封筒の上限は設定エラーにする。
pub(super) fn validate_options(options: &BigtableOptions) -> Result<Option<u64>, EventStoreError> {
  options.retention.validate()?;
  if matches!(options.retention.mode(), RetentionMode::Ttl { .. }) {
    return Err(configuration(ConfigurationReason::BigtableTtlUnsupported));
  }
  if options.change_feed {
    return Err(configuration(ConfigurationReason::BigtableChangeFeedUnsupported));
  }
  if !(MINIMUM_ENVELOPE_LIMIT..=MAXIMUM_ENVELOPE_LIMIT).contains(&options.envelope_limit_bytes) {
    return Err(configuration(ConfigurationReason::BigtableEnvelopeLimitOutOfRange));
  }
  Ok(options.retention.keep_snapshot_count().map(|count| count as u64))
}

/// journal と head に別のテーブルが指定されていることを検査する（2 章）。通信の前に行う。
///
/// プロファイルは journal と head の 2 つのテーブルと、両方の設定行を前提にする。同じ ID を渡すと 1 つのテーブルが
/// 両方の役を兼ねてしまうため、設定エラーにする。
fn validate_tables(tables: &BigtableTables) -> Result<(), EventStoreError> {
  if tables.journal_table_id == tables.head_table_id {
    return Err(configuration(ConfigurationReason::BigtableDuplicateTableIds));
  }
  Ok(())
}

// ---------------------------------------------------------------------------
// 設定行の JSON
// ---------------------------------------------------------------------------

/// 設定行の項目。保持件数は、なしなら `None`、設定する場合は正の整数である。
#[derive(Debug, Clone, PartialEq, Eq)]
struct RecordedConfiguration {
  store_id: String,
  layout_version: i64,
  keep_snapshot_count: Option<u64>,
  retention_mode: String,
  envelope_limit_bytes: u64,
}

/// 最上位のキーが重複していない JSON オブジェクト。重複したキーは、同じ値でも読み取りの誤りにする。
struct UniqueKeyObject(Vec<(String, Value)>);

impl<'de> Deserialize<'de> for UniqueKeyObject {
  fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
    struct ObjectVisitor;

    impl<'de> Visitor<'de> for ObjectVisitor {
      type Value = UniqueKeyObject;

      fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON object without duplicate keys")
      }

      fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
        let mut seen = HashSet::new();
        let mut entries = Vec::new();
        while let Some(key) = map.next_key::<String>()? {
          if !seen.insert(key.clone()) {
            return Err(serde::de::Error::custom(format!("duplicate key `{key}`")));
          }
          entries.push((key, map.next_value::<Value>()?));
        }
        Ok(UniqueKeyObject(entries))
      }
    }

    deserializer.deserialize_map(ObjectVisitor)
  }
}

/// 保持件数の 10 進文字列を読む。正の整数で、指数表記や先頭のゼロ、符号を使わない。
fn parse_keep_snapshot_count(text: &str) -> Option<u64> {
  if text.is_empty() || text.starts_with('0') || !text.bytes().all(|byte| byte.is_ascii_digit()) {
    return None;
  }
  text.parse::<u64>().ok()
}

fn parse_configuration(bytes: &[u8]) -> Result<RecordedConfiguration, EventStoreError> {
  let UniqueKeyObject(entries) = serde_json::from_slice(bytes).map_err(|_| invalid_configuration())?;
  let field = |name: &str| entries.iter().find(|(key, _)| key == name).map(|(_, value)| value);
  let store_id = match field("store_id") {
    Some(Value::String(text)) => text.clone(),
    _ => return Err(invalid_configuration()),
  };
  let layout_version = match field("layout_version") {
    Some(Value::Number(number)) => number.as_i64().ok_or_else(invalid_configuration)?,
    _ => return Err(invalid_configuration()),
  };
  let keep_snapshot_count = match field("keep_snapshot_count") {
    Some(Value::Null) => None,
    Some(Value::String(text)) => Some(parse_keep_snapshot_count(text).ok_or_else(invalid_configuration)?),
    _ => return Err(invalid_configuration()),
  };
  let retention_mode = match field("retention_mode") {
    Some(Value::String(text)) => text.clone(),
    _ => return Err(invalid_configuration()),
  };
  let envelope_limit_bytes = match field("envelope_limit_bytes") {
    Some(Value::Number(number)) => number.as_u64().ok_or_else(invalid_configuration)?,
    _ => return Err(invalid_configuration()),
  };
  Ok(RecordedConfiguration {
    store_id,
    layout_version,
    keep_snapshot_count,
    retention_mode,
    envelope_limit_bytes,
  })
}

fn configuration_json(record: &RecordedConfiguration) -> Vec<u8> {
  json!({
    "store_id": record.store_id,
    "layout_version": record.layout_version,
    "keep_snapshot_count": record.keep_snapshot_count.map(|count| count.to_string()),
    "retention_mode": record.retention_mode,
    "envelope_limit_bytes": record.envelope_limit_bytes,
  })
  .to_string()
  .into_bytes()
}

/// 両テーブルの設定行を、クライアントの設定と照合する。一致すれば store_id を返す（2 章・BT-16）。
///
/// JSON のキーの順序と空白は比べない。必須フィールドの欠損、重複したキー、型や形の誤りは `BigtableConfigurationInvalid`、
/// 値の食い違いは `BigtableConfigurationMismatch` にする。
pub(super) fn verify_configuration(
  journal: &[u8],
  head: &[u8],
  keep_snapshot_count: Option<usize>,
  envelope_limit_bytes: u64,
) -> Result<String, EventStoreError> {
  let journal = parse_configuration(journal)?;
  let head = parse_configuration(head)?;
  if journal != head {
    return Err(mismatched_configuration());
  }
  if journal.layout_version != 1 || journal.retention_mode != "delete" {
    return Err(mismatched_configuration());
  }
  // store_id は小文字の UUID 文字列。大文字やハイフンのない形は食い違いとして扱う。
  match Uuid::parse_str(&journal.store_id) {
    Ok(parsed) if parsed.to_string() == journal.store_id => {}
    _ => return Err(mismatched_configuration()),
  }
  if journal.keep_snapshot_count != keep_snapshot_count.map(|count| count as u64)
    || journal.envelope_limit_bytes != envelope_limit_bytes
  {
    return Err(mismatched_configuration());
  }
  Ok(journal.store_id)
}

// ---------------------------------------------------------------------------
// 列ファミリの照合（BT-2）
// ---------------------------------------------------------------------------

const JOURNAL_FAMILIES: [&str; 3] = [FAMILY_EVENT, FAMILY_HISTORY, FAMILY_CONFIG];
const HEAD_FAMILIES: [&str; 3] = [FAMILY_HEAD, FAMILY_SNAPSHOT, FAMILY_CONFIG];

fn table_path(tables: &BigtableTables, table_id: &str) -> String {
  format!(
    "projects/{}/instances/{}/tables/{table_id}",
    tables.project_id, tables.instance_id
  )
}

/// 列ファミリが全て揃い、データの列ファミリの GC が「最大 1 版」だけであることを確かめる。
/// 設定の列ファミリは存在だけを見る。年齢による GC は許さない（BT-2）。
async fn verify_column_families(admin: Channel, table_name: String, families: &[&str]) -> Result<(), EventStoreError> {
  let operation = StorageOperation::ReadConfiguration;
  let mismatch = || configuration(ConfigurationReason::BigtableColumnFamilyMismatch);
  let response = BigtableTableAdminClient::new(admin)
    .get_table(GetTableRequest {
      name: table_name,
      view: table::View::SchemaView as i32,
    })
    .await;
  let table = match response {
    Ok(response) => response.into_inner(),
    Err(status) if status.code() == tonic::Code::NotFound => return Err(mismatch()),
    Err(status) => return Err(storage(operation, status)),
  };
  for family in families {
    let Some(column_family) = table.column_families.get(*family) else {
      return Err(mismatch());
    };
    if *family == FAMILY_CONFIG {
      continue;
    }
    let keeps_one_version = matches!(
      column_family.gc_rule.as_ref().and_then(|rule| rule.rule.as_ref()),
      Some(gc_rule::Rule::MaxNumVersions(1))
    );
    if !keeps_one_version {
      return Err(mismatch());
    }
  }
  Ok(())
}

// ---------------------------------------------------------------------------
// 生成と初期化
// ---------------------------------------------------------------------------

/// 列ファミリと設定行を照合した、開いたストアの接続を表す。
pub(super) struct OpenedStore {
  pub journal: TableTarget,
  pub head: TableTarget,
  pub store_id: String,
  pub keep_snapshot_count: Option<usize>,
  pub envelope_limit_bytes: u64,
}

fn targets(
  connection: &BigtableConnection,
  tables: &BigtableTables,
  options: &BigtableOptions,
) -> (TableTarget, TableTarget) {
  (
    TableTarget::new(
      connection.data.clone(),
      table_path(tables, &tables.journal_table_id),
      &options.app_profile_id,
    ),
    TableTarget::new(
      connection.data.clone(),
      table_path(tables, &tables.head_table_id),
      &options.app_profile_id,
    ),
  )
}

/// 設定行の `config:metadata` を読む。行がなければ `None`。
async fn read_configuration(
  target: &TableTarget,
  operation: StorageOperation,
) -> Result<Option<Vec<u8>>, EventStoreError> {
  let row = target
    .read_row(
      CONFIG_ROW_KEY.to_vec(),
      latest_cell_exists(FAMILY_CONFIG, CONFIG_METADATA),
      operation,
    )
    .await?;
  Ok(row.and_then(|row| row.value(FAMILY_CONFIG, CONFIG_METADATA).map(<[u8]>::to_vec)))
}

/// 初期化済みのストアを開き、列ファミリと両テーブルの設定行をクライアントの設定と照合する。
///
/// テーブルも設定行も作らず、欠けた定義も補わない（BT-D1）。設定値の誤りと journal・head の同じテーブルの指定は、
/// 通信の前に設定エラーにする。
pub(super) async fn open_checked(
  connection: &BigtableConnection,
  tables: &BigtableTables,
  options: &BigtableOptions,
) -> Result<OpenedStore, EventStoreError> {
  validate_options(options)?;
  validate_tables(tables)?;
  verify_column_families(
    connection.admin.clone(),
    table_path(tables, &tables.journal_table_id),
    &JOURNAL_FAMILIES,
  )
  .await?;
  verify_column_families(
    connection.admin.clone(),
    table_path(tables, &tables.head_table_id),
    &HEAD_FAMILIES,
  )
  .await?;
  let (journal, head) = targets(connection, tables, options);
  let operation = StorageOperation::ReadConfiguration;
  let journal_configuration = read_configuration(&journal, operation)
    .await?
    .ok_or_else(invalid_configuration)?;
  let head_configuration = read_configuration(&head, operation)
    .await?
    .ok_or_else(invalid_configuration)?;
  let keep_snapshot_count = options.retention.keep_snapshot_count();
  let store_id = verify_configuration(
    &journal_configuration,
    &head_configuration,
    keep_snapshot_count,
    options.envelope_limit_bytes,
  )?;
  Ok(OpenedStore {
    journal,
    head,
    store_id,
    keep_snapshot_count,
    envelope_limit_bytes: options.envelope_limit_bytes,
  })
}

/// 設定行がまだない `target` に、`config:metadata` を作る。既にあれば `BigtableAlreadyInitialized` にする。
async fn create_configuration_row(target: &TableTarget, json: &[u8]) -> Result<(), EventStoreError> {
  let operation = StorageOperation::CreateConfiguration;
  let already_exists = target
    .check_and_mutate(
      CONFIG_ROW_KEY.to_vec(),
      latest_cell_exists(FAMILY_CONFIG, CONFIG_METADATA),
      vec![],
      vec![set_cell(FAMILY_CONFIG, CONFIG_METADATA, json)],
      operation,
    )
    .await?;
  if already_exists {
    return Err(configuration(ConfigurationReason::BigtableAlreadyInitialized));
  }
  Ok(())
}

/// 用意済みの journal・head のテーブルに、同じ store_id の設定行を作って、ストアとして初期化する（BT-D1）。
///
/// テーブルと列ファミリは作らない。両テーブルのどちらかに設定行が既にあれば、何も書かず設定エラーにする。
/// journal と head に同じテーブルが指定されている場合も、何も読み書きせず設定エラーにする。
/// 通常のストアの生成（[`EventStoreForBigtable::open`](super::EventStoreForBigtable::open)）は初期化しない。
pub async fn initialize_bigtable_store(
  connection: &BigtableConnection,
  tables: &BigtableTables,
  options: &BigtableOptions,
) -> Result<(), EventStoreError> {
  let keep_snapshot_count = validate_options(options)?;
  validate_tables(tables)?;
  let (journal, head) = targets(connection, tables, options);
  let operation = StorageOperation::CreateConfiguration;
  for target in [&journal, &head] {
    if read_configuration(target, operation).await?.is_some() {
      return Err(configuration(ConfigurationReason::BigtableAlreadyInitialized));
    }
  }
  let json = configuration_json(&RecordedConfiguration {
    store_id: Uuid::new_v4().to_string(),
    layout_version: 1,
    keep_snapshot_count,
    retention_mode: "delete".to_string(),
    envelope_limit_bytes: options.envelope_limit_bytes,
  });
  create_configuration_row(&journal, &json).await?;
  create_configuration_row(&head, &json).await
}

/// 試験用に設定行へ書き込む値を表す。項目名はプロファイルの設定行のとおり。
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BigtableConfigurationRecord {
  pub store_id: String,
  pub layout_version: i64,
  pub keep_snapshot_count: Option<u64>,
  pub retention_mode: String,
  pub envelope_limit_bytes: u64,
}

/// 初期化の後、ストアを開く前に、記録済みの設定行を `journal`・`head` の値で書き換える（試験用）。
///
/// 適合テストデータの `seed.configuration` を、公開 API を増やさずに記録するための経路である。
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub async fn overwrite_bigtable_configuration_for_test(
  connection: &BigtableConnection,
  tables: &BigtableTables,
  options: &BigtableOptions,
  journal: &BigtableConfigurationRecord,
  head: &BigtableConfigurationRecord,
) -> Result<(), EventStoreError> {
  let (journal_target, head_target) = targets(connection, tables, options);
  let operation = StorageOperation::CreateConfiguration;
  for (target, record) in [(&journal_target, journal), (&head_target, head)] {
    let json = configuration_json(&RecordedConfiguration {
      store_id: record.store_id.clone(),
      layout_version: record.layout_version,
      keep_snapshot_count: record.keep_snapshot_count,
      retention_mode: record.retention_mode.clone(),
      envelope_limit_bytes: record.envelope_limit_bytes,
    });
    target
      .mutate(
        CONFIG_ROW_KEY.to_vec(),
        replace_cell(FAMILY_CONFIG, CONFIG_METADATA, &json).to_vec(),
        operation,
      )
      .await?;
  }
  Ok(())
}

#[cfg(test)]
#[path = "configuration_test.rs"]
mod tests;
