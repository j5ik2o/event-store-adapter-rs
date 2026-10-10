use std::error::Error as StdError;
use std::fmt;

use crate::seq_nr::SeqNr;

/// 契約違反の規則。文字列表現は仕様の規則番号（E-3）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ContractRule {
  T9,
  T11,
  T12,
  T13,
  W6,
  W8Gap,
  W9,
  /// D-7（判断の番号であり、必須の規則番号ではない。設計 10 章 Q-3）。
  ItemSizeLimit,
}

impl fmt::Display for ContractRule {
  fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    let text = match self {
      ContractRule::T9 => "T-9",
      ContractRule::T11 => "T-11",
      ContractRule::T12 => "T-12",
      ContractRule::T13 => "T-13",
      ContractRule::W6 => "W-6",
      ContractRule::W8Gap => "W-8",
      ContractRule::W9 => "W-9",
      ContractRule::ItemSizeLimit => "D-7",
    };
    formatter.write_str(text)
  }
}

/// 直列化の失敗が起きた段階。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SerializationPhase {
  SerializeEvent,
  SerializeSnapshot,
  DeserializeEvent,
  DeserializeSnapshot,
}

impl fmt::Display for SerializationPhase {
  fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    let text = match self {
      SerializationPhase::SerializeEvent => "serialize-event",
      SerializationPhase::SerializeSnapshot => "serialize-snapshot",
      SerializationPhase::DeserializeEvent => "deserialize-event",
      SerializationPhase::DeserializeSnapshot => "deserialize-snapshot",
    };
    formatter.write_str(text)
  }
}

/// 保存先の失敗が起きた操作の種類。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum StorageOperation {
  ReadConfiguration,
  CreateConfiguration,
  Append,
  LoadSnapshot,
  LoadEvents,
}

impl fmt::Display for StorageOperation {
  fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    let text = match self {
      StorageOperation::ReadConfiguration => "read-configuration",
      StorageOperation::CreateConfiguration => "create-configuration",
      StorageOperation::Append => "append",
      StorageOperation::LoadSnapshot => "load-snapshot",
      StorageOperation::LoadEvents => "load-events",
    };
    formatter.write_str(text)
  }
}

/// 設定エラーの理由（設計 2.10 の表の「設定エラーになる値」）。
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConfigurationReason {
  /// 保持件数 0（S-1）。`Some(0)` は無効。
  KeepSnapshotCountZero,
  /// メモリで保持件数があるときの TTL 方式（MEM-3・MEM-12）。
  TtlWithKeepSnapshotCount,
  /// SDK Client の設定に待ちの仕組みがない（設計 2.12.1）。
  MissingRetrySleeper,
  /// ３表の役割に同じ表名が指定されている（DY-8・SP-1）。
  DuplicateDynamoDbTableNames,
  /// ３表の一部だけに設定項目がある（DY-8）。
  PartialDynamoDbConfiguration,
  /// ３設定項目の store_id が一致しない（DY-8）。
  DynamoDbStoreIdMismatch,
  /// 設定項目の layout_version が版1ではない（DY-8）。
  UnsupportedDynamoDbLayoutVersion,
}

impl fmt::Display for ConfigurationReason {
  fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      ConfigurationReason::KeepSnapshotCountZero => {
        formatter.write_str("keep_snapshot_count must be None or at least 1, keep_snapshot_count=0")
      }
      ConfigurationReason::TtlWithKeepSnapshotCount => {
        formatter.write_str("retention mode ttl cannot be combined with a snapshot history count on the memory backend")
      }
      ConfigurationReason::MissingRetrySleeper => formatter.write_str("DynamoDB client has no retry sleeper"),
      ConfigurationReason::DuplicateDynamoDbTableNames => {
        formatter.write_str("DynamoDB journal, snapshot, and head table names must be distinct")
      }
      ConfigurationReason::PartialDynamoDbConfiguration => {
        formatter.write_str("DynamoDB configuration exists in only some tables")
      }
      ConfigurationReason::DynamoDbStoreIdMismatch => formatter.write_str("DynamoDB configuration store_id mismatch"),
      ConfigurationReason::UnsupportedDynamoDbLayoutVersion => {
        formatter.write_str("DynamoDB configuration layout_version must be 1")
      }
    }
  }
}

/// 保持処理の失敗の記録（S-4、設計 2.11 の `tracing` の項目）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionFailure {
  pub aid: String,
  pub seq_nr: SeqNr,
  pub phase: String,
  pub error: String,
}

/// 5 つの分類を 1 つの型で表す（E-1）。呼び出し側は `match` で分類を区別する。
#[derive(Debug)]
#[non_exhaustive]
pub enum EventStoreError {
  /// 楽観ロック。既存集約への新規作成（W-3）、seq_nr の重複（W-7）、ヘッド以下での追記（W-8）。
  OptimisticLock {
    aid: String,
    seq_nr: SeqNr,
    head_seq_nr: Option<SeqNr>,
  },
  /// 契約違反。W-6・W-9・T-9・T-11・T-12・T-13 と、W-8 の飛び番。
  ContractViolation {
    rule: ContractRule,
    seq_nr: Option<SeqNr>,
    snapshot_seq_nr: Option<SeqNr>,
  },
  /// 直列化。payload の直列化・復元の失敗。
  Serialization {
    phase: SerializationPhase,
    source: Box<dyn StdError + Send + Sync>,
  },
  /// 設定。生成時の不正な設定値と、保存先に記録された設定との食い違い。
  Configuration { reason: ConfigurationReason },
  /// 保存先。通信・保存先の失敗と、読み取ったデータの欠損。
  Storage {
    operation: StorageOperation,
    source: Box<dyn StdError + Send + Sync>,
  },
}

fn head_seq_nr_suffix(head_seq_nr: Option<SeqNr>) -> String {
  head_seq_nr
    .map(|head| format!(", head_seq_nr={head}"))
    .unwrap_or_default()
}

fn contract_violation_suffix(seq_nr: Option<SeqNr>, snapshot_seq_nr: Option<SeqNr>) -> String {
  let mut suffix = String::new();
  if let Some(seq_nr) = seq_nr {
    suffix.push_str(&format!(", seq_nr={seq_nr}"));
  }
  if let Some(snapshot_seq_nr) = snapshot_seq_nr {
    suffix.push_str(&format!(", snapshot_seq_nr={snapshot_seq_nr}"));
  }
  suffix
}

impl fmt::Display for EventStoreError {
  fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      EventStoreError::OptimisticLock {
        aid,
        seq_nr,
        head_seq_nr,
      } => write!(
        formatter,
        "optimistic lock failed: aid={aid}, seq_nr={seq_nr}{}",
        head_seq_nr_suffix(*head_seq_nr)
      ),
      EventStoreError::ContractViolation {
        rule,
        seq_nr,
        snapshot_seq_nr,
      } => write!(
        formatter,
        "contract violation: rule={rule}{}",
        contract_violation_suffix(*seq_nr, *snapshot_seq_nr)
      ),
      EventStoreError::Serialization { phase, .. } => {
        write!(formatter, "serialization failed: phase={phase}")
      }
      EventStoreError::Configuration { reason } => write!(formatter, "configuration error: {reason}"),
      EventStoreError::Storage { operation, .. } => write!(formatter, "storage error: operation={operation}"),
    }
  }
}

impl StdError for EventStoreError {
  fn source(&self) -> Option<&(dyn StdError + 'static)> {
    match self {
      EventStoreError::Serialization { source, .. } => Some(source.as_ref()),
      EventStoreError::Storage { source, .. } => Some(source.as_ref()),
      _ => None,
    }
  }
}
