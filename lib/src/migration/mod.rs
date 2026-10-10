//! v3既定DynamoDB配置だけを対象とする移行。旧書込の停止と新表の作成は運用者が行う。
//!
//! 全件検査の成功後に旧両表を再Scanする。検査拒否では設定以外を送信せず、旧表は変更しない。
//! 書込中の失敗では部分移行が残るため、新3表を作り直してから再実行する。

mod inspect;
mod legacy;
mod scan;
#[cfg(test)]
mod test_support;
mod write;

use std::collections::HashMap;

use aws_sdk_dynamodb::Client;
use serde::{Deserialize, Serialize};

use crate::next::dynamodb::{DynamoDbOptions, DynamoDbTables, EventStoreForDynamoDB};

/// 移行元の既定配置の2表を指定する。
#[derive(Debug, Clone)]
pub struct LegacyDynamoDbTables {
  pub journal_table_name: String,
  pub snapshot_table_name: String,
}

/// 検査または条件付き書込で停止した対象と理由を返す。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationRejection {
  pub table: String,
  pub pkey: Option<String>,
  pub skey: Option<String>,
  pub aid: Option<String>,
  pub reason: String,
}

/// 成功応答を受けた書込の件数と停止理由を返す。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationReport {
  pub aggregates: u64,
  pub events: u64,
  pub snapshots: u64,
  pub reasons: Vec<MigrationRejection>,
}

/// 保存先の失敗と、その時点までに確認できた移行結果を保持する。
#[derive(Debug, thiserror::Error)]
#[error("DynamoDB migration failed during {operation}")]
pub struct MigrationError {
  pub report: MigrationReport,
  pub operation: String,
  #[source]
  pub source: Box<dyn std::error::Error + Send + Sync>,
}

impl MigrationError {
  fn new(
    report: &MigrationReport,
    operation: impl Into<String>,
    source: impl std::error::Error + Send + Sync + 'static,
  ) -> Self {
    Self {
      report: report.clone(),
      operation: operation.into(),
      source: Box::new(source),
    }
  }
}

/// 全件検査の後、v3既定配置の保存値を新版へ転写する（D-8・P-45）。
///
/// 呼出し前に旧2表への書込を停止し、新3表と履歴GSIを作成する。
/// 型名にhyphenを含む場合だけ`type_mapping`で置換する。旧Hasherとserializerは使用しない。
/// 検査拒否・条件競合は`reasons`で返し、通信等の失敗は部分件数を持つ`MigrationError`で返す。
pub async fn migrate_v3_dynamodb(
  client: &Client,
  legacy_tables: &LegacyDynamoDbTables,
  tables: &DynamoDbTables,
  type_mapping: &HashMap<String, String>,
) -> Result<MigrationReport, MigrationError> {
  let mut report = MigrationReport::default();
  if !distinct_tables(legacy_tables, tables) {
    report.reasons.push(MigrationRejection {
      table: String::new(),
      pkey: None,
      skey: None,
      aid: None,
      reason: "旧2表と新3表には互いに異なるテーブル名が必要です".into(),
    });
    return Ok(report);
  }
  inspect::check_empty(client, tables, &mut report).await?;
  if !report.reasons.is_empty() {
    return Ok(report);
  }
  // 設定生成・照合は公開openに委譲し、ストアの書込/読取は呼ばない。
  EventStoreForDynamoDB::<legacy::Id, (), ()>::open(client.clone(), tables.clone(), DynamoDbOptions::default())
    .await
    .map_err(|error| MigrationError::new(&report, "open", error))?;
  let aggregates = inspect::inspect(client, legacy_tables, type_mapping, &mut report).await?;
  if report.reasons.is_empty() {
    write::write(client, legacy_tables, tables, type_mapping, aggregates, &mut report).await?;
  }
  Ok(report)
}

fn distinct_tables(legacy: &LegacyDynamoDbTables, tables: &DynamoDbTables) -> bool {
  let names = [
    &legacy.journal_table_name,
    &legacy.snapshot_table_name,
    &tables.journal_table_name,
    &tables.snapshot_table_name,
    &tables.head_table_name,
  ];
  names
    .iter()
    .enumerate()
    .all(|(index, name)| !names[..index].contains(name))
}

#[cfg(test)]
#[path = "mod_test.rs"]
mod tests;
