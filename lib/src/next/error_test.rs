use crate::next::error::{ConfigurationReason, ContractRule, EventStoreError, SerializationPhase, StorageOperation};

fn contract_violation(rule: ContractRule, seq_nr: Option<u64>, snapshot_seq_nr: Option<u64>) -> EventStoreError {
  EventStoreError::ContractViolation {
    rule,
    seq_nr,
    snapshot_seq_nr,
  }
}

// E-2: OptimisticLock の表示は aid と seq_nr を含み、ヘッド番号は分かるときだけ含める。
#[test]
fn should_optimistic_lock_display_include_aid_and_seq_nr() {
  let error = EventStoreError::OptimisticLock {
    aid: "Order-9".to_string(),
    seq_nr: 3,
    head_seq_nr: None,
  };

  let text = error.to_string();

  assert!(text.contains("Order-9"));
  assert!(text.contains("seq_nr=3"));
  assert!(!text.contains("head_seq_nr"));
}

// E-2: ヘッド番号が分かれば表示に含める。
#[test]
fn should_optimistic_lock_display_include_head_seq_nr_when_known() {
  let error = EventStoreError::OptimisticLock {
    aid: "Order-9".to_string(),
    seq_nr: 3,
    head_seq_nr: Some(2),
  };

  assert!(error.to_string().contains("head_seq_nr=2"));
}

// E-2: 接続文字列・資格情報・SDK の生文を含めない。
#[test]
fn should_optimistic_lock_display_not_include_connection_strings() {
  let error = EventStoreError::OptimisticLock {
    aid: "Order-9".to_string(),
    seq_nr: 1,
    head_seq_nr: Some(1),
  };

  let text = error.to_string();

  assert!(!text.contains("://"));
  assert!(!text.contains("https"));
  assert!(!text.contains("AKIA"));
}

// E-3: ContractViolation は違反した規則番号と、関係する seq_nr を含める。
#[test]
fn should_contract_violation_display_include_rule_and_seq_nr() {
  let text = contract_violation(ContractRule::W6, Some(0), None).to_string();

  assert!(text.contains("W-6"));
  assert!(text.contains("seq_nr=0"));
}

// E-3: 関係する seq_nr がないとき（T-11・T-12）は seq_nr を含めない。
#[test]
fn should_contract_violation_display_omit_seq_nr_when_absent() {
  let text = contract_violation(ContractRule::T11, None, None).to_string();

  assert!(text.contains("T-11"));
  assert!(!text.contains("seq_nr="));
}

// E-3: W-9 は異なるスナップショット番号も含める。
#[test]
fn should_contract_violation_display_include_both_seq_nrs_for_w9() {
  let text = contract_violation(ContractRule::W9, Some(42), Some(41)).to_string();

  assert!(text.contains("W-9"));
  assert!(text.contains("seq_nr=42"));
  assert!(text.contains("snapshot_seq_nr=41"));
}

// E-3: ContractRule の表示は仕様の規則番号と一致する。
#[test]
fn should_contract_rule_display_matches_spec_number() {
  assert_eq!(ContractRule::T9.to_string(), "T-9");
  assert_eq!(ContractRule::T11.to_string(), "T-11");
  assert_eq!(ContractRule::T12.to_string(), "T-12");
  assert_eq!(ContractRule::T13.to_string(), "T-13");
  assert_eq!(ContractRule::W6.to_string(), "W-6");
  assert_eq!(ContractRule::W8Gap.to_string(), "W-8");
  assert_eq!(ContractRule::W9.to_string(), "W-9");
}

// E-3: D-7 は判断の番号であり、表示は `D-7`（設計 10 章 Q-3）。
#[test]
fn should_item_size_limit_rule_display_as_d_7() {
  assert_eq!(ContractRule::ItemSizeLimit.to_string(), "D-7");
}

// E-1: Serialization の表示は段階を含み、5 分類がマッチで区別できる。
#[test]
fn should_serialization_display_include_phase() {
  let error = EventStoreError::Serialization {
    phase: SerializationPhase::DeserializeSnapshot,
    source: Box::new(std::io::Error::other("boom")),
  };

  assert!(error.to_string().contains("deserialize-snapshot"));
  assert!(matches!(error, EventStoreError::Serialization { .. }));
}

// E-1: Configuration の表示は理由を含む。
#[test]
fn should_configuration_display_include_reason() {
  let error = EventStoreError::Configuration {
    reason: ConfigurationReason::KeepSnapshotCountZero,
  };

  assert!(error.to_string().contains("keep_snapshot_count=0"));
  assert!(matches!(error, EventStoreError::Configuration { .. }));
}

// E-1: Storage の表示は操作の種類を含み、SDK の生文を出さない。
#[test]
fn should_storage_display_include_operation_without_source_text() {
  for (operation, name) in [
    (StorageOperation::Append, "append"),
    (StorageOperation::CreateConfiguration, "create-configuration"),
  ] {
    let error = EventStoreError::Storage {
      operation,
      source: Box::new(std::io::Error::other("connection reset by peer")),
    };
    let text = error.to_string();
    assert!(text.contains(name));
    assert!(!text.contains("connection reset by peer"));
    assert_eq!(
      std::error::Error::source(&error).unwrap().to_string(),
      "connection reset by peer"
    );
  }
}

#[test]
fn should_configuration_read_errors_keep_the_existing_categories_and_safe_display() {
  for reason in [
    ConfigurationReason::MissingRetrySleeper,
    ConfigurationReason::DuplicateDynamoDbTableNames,
    ConfigurationReason::PartialDynamoDbConfiguration,
    ConfigurationReason::DynamoDbStoreIdMismatch,
    ConfigurationReason::UnsupportedDynamoDbLayoutVersion,
  ] {
    let reason_text = reason.to_string();
    let error = EventStoreError::Configuration { reason };
    assert!(error.to_string().contains(&reason_text));
    assert!(matches!(error, EventStoreError::Configuration { .. }));
  }
  let error = EventStoreError::Storage {
    operation: StorageOperation::ReadConfiguration,
    source: Box::new(std::io::Error::other("http://SECRET_ENDPOINT SDK_SENTINEL")),
  };
  assert!(error.to_string().contains("read-configuration"));
  assert!(!error.to_string().contains("SECRET_ENDPOINT"));
  assert!(!error.to_string().contains("SDK_SENTINEL"));
}
