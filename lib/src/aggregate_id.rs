use std::fmt::Debug;

use crate::error::{ContractRule, EventStoreError};

/// 集約 ID。型名と値の 2 つの文字列を持つ。利用者の `Display` や `ToString` には依存しない（T-1）。
pub trait AggregateId: Debug + Clone + Send + Sync + 'static {
  /// 集約の種別名。`-` を含んではならない（T-11）。
  fn type_name(&self) -> String;
  /// 集約の値。`-` を含んでよい。
  fn value(&self) -> String;
}

/// T-12 の上限（UTF-8 バイト数）。
const AID_MAX_BYTES: usize = 1024;

/// ライブラリが組み立てた aid 文字列（T-1）。構築時に T-11・T-12 を検査する。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AidString(String);

impl AidString {
  /// `${型名}-${値}` を組み立てる。
  ///
  /// 型名に `-` を含めば契約違反（T-11）、UTF-8 で 1024 バイトを超えれば契約違反（T-12）。
  /// 長さは型名・区切り・値の UTF-8 バイト数の合計で判定する（文字数で数えない）。
  /// 空の型名・空の値は許す。
  pub fn from_aggregate_id<AID: AggregateId>(id: &AID) -> Result<Self, EventStoreError> {
    let type_name = id.type_name();
    let value = id.value();
    if type_name.contains('-') {
      return Err(EventStoreError::ContractViolation {
        rule: ContractRule::T11,
        seq_nr: None,
        snapshot_seq_nr: None,
      });
    }
    let aid_len = type_name.len().saturating_add(1).saturating_add(value.len());
    if aid_len > AID_MAX_BYTES {
      return Err(EventStoreError::ContractViolation {
        rule: ContractRule::T12,
        seq_nr: None,
        snapshot_seq_nr: None,
      });
    }
    Ok(AidString(format!("{type_name}-{value}")))
  }

  /// 組み立て済みの aid 文字列を返す。
  pub fn as_str(&self) -> &str {
    &self.0
  }
}
