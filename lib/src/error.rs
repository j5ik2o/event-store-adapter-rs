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
  /// BT-16: メタデータを含む直列化後の 1 封筒が、ストア単位の上限を超える（Bigtable）。
  EnvelopeSizeLimit,
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
      ContractRule::EnvelopeSizeLimit => "BT-16",
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
  /// メモリで保持件数があるときの TTL 方式（MEM-3・MEM-12）。5.0.1 からは返さず、
  /// メモリの `Ttl` はすべて `MemoryTtlUnsupported` になる。既存の利用者のために残す。
  TtlWithKeepSnapshotCount,
  /// メモリの保存先は期限切れ方式（`RetentionMode::Ttl`）を提供しない（MEM-3・MEM-12）。
  /// 5.0.1 から、メモリで `Ttl` を指定したときはこの理由を返す。
  MemoryTtlUnsupported,
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
  /// SQLite の保存先は期限切れ方式（`RetentionMode::Ttl`）を提供しない（SQ-11）。
  SqliteTtlUnsupported,
  /// SQLite の保存先は最初の版で変更フィードを提供しない。フィードを求める設定と、
  /// 記録した `change_feed_enabled` が 1 のストアを開く操作は、この理由になる（SQ-D8・SQ-20）。
  SqliteChangeFeedUnsupported,
  /// 保持件数が SQLite の符号付き 64bit の整数に収まらない（2 章の config の形）。
  SqliteKeepSnapshotCountOutOfRange,
  /// busy の待ち時間が 32bit のミリ秒に収まらない（SQ-1）。
  SqliteLockWaitOutOfRange,
  /// 実際に使う SQLite 本体の版が 3.51.3 未満（SQ-15）。
  UnsupportedSqliteVersion,
  /// 接続の設定（`locking_mode`・`journal_mode`・`synchronous`・`read_uncommitted`）を要求の値にできない（SQ-1・SQ-14）。
  SqliteConnectionSettingMismatch,
  /// 初期化の対象が、アプリケーションのオブジェクトを持つか初期化済みのデータベースである（SQ-2・SQ-D6）。
  SqliteDatabaseNotEmpty,
  /// テーブルの定義・主キー・STRICT の宣言が、期待する配置と一致しない（SQ-2・SQ-16）。
  SqliteLayoutMismatch,
  /// config の layout_version が版1ではない（SQ-2）。
  UnsupportedSqliteLayoutVersion,
  /// 記録した config の store_id・保持件数・保持方式・変更フィードの有無が、期待と一致しない（SQ-2・SQ-19）。
  SqliteConfigurationMismatch,
  /// Bigtable の保存先は期限切れ方式（`RetentionMode::Ttl`）を提供しない（BT-13）。
  BigtableTtlUnsupported,
  /// Bigtable の保存先は最初の版で変更フィードを提供しない。フィードを求める設定は、この理由になる（BT-D8・BT-17）。
  BigtableChangeFeedUnsupported,
  /// 封筒の上限が、最小の封筒も収まらない値か、最大（8388608 バイト）を超える（BT-16）。
  BigtableEnvelopeLimitOutOfRange,
  /// 列ファミリが欠けているか、データの列ファミリの GC が「最大 1 版」だけでない（BT-2）。
  BigtableColumnFamilyMismatch,
  /// 設定行が欠けているか、JSON として読めない・重複したキーがある・必須フィールドが欠けている・型や形が違う（2 章）。
  BigtableConfigurationInvalid,
  /// 読み取れた設定が、両テーブルの間、またはクライアントの設定（保持件数・封筒の上限）と食い違う。
  /// 配置の版が 1 でない、保持方式が `delete` でない、store_id が小文字の UUID でない場合もこの理由になる（2 章）。
  BigtableConfigurationMismatch,
  /// 初期化の対象に、既に設定行がある（BT-D1）。
  BigtableAlreadyInitialized,
  /// journal と head に同じテーブルの ID が指定されている（2 章）。
  BigtableDuplicateTableIds,
  /// Spanner の保存先は最初の版で変更フィードを提供しない。フィードを求める設定と、記録した `change_feed_enabled` が true のストアを開く操作は、この理由になる（SPN-D8・SPN-17）。
  SpannerChangeFeedUnsupported,
  /// 期限切れ方式の猶予の日数が 0 か、config の INT64 に収まらない（SPN-16）。
  SpannerTtlGraceDaysOutOfRange,
  /// 保持件数が Spanner の符号付き 64bit の整数に収まらない（2 章の config の形）。
  SpannerKeepSnapshotCountOutOfRange,
  /// テーブル名の接頭辞から導いた名前が、SQL の識別子でないか 128 文字を超える（SPN-D1b）。
  SpannerInvalidTablePrefix,
  /// データベースの方言、テーブルの列の名前と型、主キーの順が、プロファイルの配置と一致しない（SPN-2）。
  SpannerLayoutMismatch,
  /// config の行が欠けているか、形が壊れている（store_id が小文字の UUID でないなど）（SPN-2）。
  SpannerConfigurationInvalid,
  /// 読み取れた設定が、クライアントの設定（保持件数・方式・猶予）や行削除ポリシーと食い違う。配置の版が 1 でない場合もこの理由になる（SPN-2・SPN-16）。
  SpannerConfigurationMismatch,
  /// 初期化の対象が、既に config の行を持つか、空でないテーブルを持つ（SPN-D5）。
  SpannerAlreadyInitialized,
}

impl fmt::Display for ConfigurationReason {
  fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      ConfigurationReason::KeepSnapshotCountZero => {
        formatter.write_str("keep_snapshot_count must be None or at least 1, keep_snapshot_count=0")
      }
      ConfigurationReason::MemoryTtlUnsupported => {
        formatter.write_str("the memory backend does not support retention mode ttl")
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
      ConfigurationReason::SqliteTtlUnsupported => {
        formatter.write_str("the sqlite backend does not support retention mode ttl")
      }
      ConfigurationReason::SqliteChangeFeedUnsupported => {
        formatter.write_str("the sqlite backend does not support the change feed")
      }
      ConfigurationReason::SqliteKeepSnapshotCountOutOfRange => {
        formatter.write_str("keep_snapshot_count does not fit in a signed 64-bit integer")
      }
      ConfigurationReason::SqliteLockWaitOutOfRange => {
        formatter.write_str("the sqlite lock wait does not fit in 32-bit milliseconds")
      }
      ConfigurationReason::UnsupportedSqliteVersion => formatter.write_str("SQLite 3.51.3 or later is required"),
      ConfigurationReason::SqliteConnectionSettingMismatch => {
        formatter.write_str("the sqlite connection could not apply the required settings")
      }
      ConfigurationReason::SqliteDatabaseNotEmpty => formatter.write_str("the sqlite database is not uninitialized"),
      ConfigurationReason::SqliteLayoutMismatch => formatter.write_str("the sqlite table layout does not match"),
      ConfigurationReason::UnsupportedSqliteLayoutVersion => {
        formatter.write_str("sqlite configuration layout_version must be 1")
      }
      ConfigurationReason::SqliteConfigurationMismatch => {
        formatter.write_str("the sqlite configuration does not match the client settings")
      }
      ConfigurationReason::BigtableTtlUnsupported => {
        formatter.write_str("the bigtable backend does not support retention mode ttl")
      }
      ConfigurationReason::BigtableChangeFeedUnsupported => {
        formatter.write_str("the bigtable backend does not support the change feed")
      }
      ConfigurationReason::BigtableEnvelopeLimitOutOfRange => {
        formatter.write_str("envelope_limit_bytes must be between 17 and 8388608")
      }
      ConfigurationReason::BigtableColumnFamilyMismatch => {
        formatter.write_str("the bigtable column families do not match the profile")
      }
      ConfigurationReason::BigtableConfigurationInvalid => {
        formatter.write_str("the bigtable configuration row is missing or malformed")
      }
      ConfigurationReason::BigtableConfigurationMismatch => {
        formatter.write_str("the bigtable configuration does not match between the tables or the client settings")
      }
      ConfigurationReason::BigtableAlreadyInitialized => {
        formatter.write_str("the bigtable tables already hold a configuration row")
      }
      ConfigurationReason::BigtableDuplicateTableIds => {
        formatter.write_str("the bigtable journal and head table ids must be distinct")
      }
      ConfigurationReason::SpannerChangeFeedUnsupported => {
        formatter.write_str("the spanner store does not provide a change feed in this version")
      }
      ConfigurationReason::SpannerTtlGraceDaysOutOfRange => {
        formatter.write_str("the spanner ttl grace days must be a positive integer that fits in INT64")
      }
      ConfigurationReason::SpannerKeepSnapshotCountOutOfRange => {
        formatter.write_str("the spanner snapshot retention count does not fit in INT64")
      }
      ConfigurationReason::SpannerInvalidTablePrefix => {
        formatter.write_str("the spanner table prefix does not derive valid table names")
      }
      ConfigurationReason::SpannerLayoutMismatch => {
        formatter.write_str("the spanner database layout does not match the profile")
      }
      ConfigurationReason::SpannerConfigurationInvalid => {
        formatter.write_str("the spanner configuration row is missing or malformed")
      }
      ConfigurationReason::SpannerConfigurationMismatch => {
        formatter.write_str("the spanner configuration does not match the client settings or the row deletion policy")
      }
      ConfigurationReason::SpannerAlreadyInitialized => {
        formatter.write_str("the spanner tables are already initialized or not empty")
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
