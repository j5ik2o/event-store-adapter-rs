use crate::next::error::{ConfigurationReason, EventStoreError};
use crate::next::retention::{select_expired_history, ttl_expires_epoch_seconds, RetentionMode, RetentionSettings};

#[test]
fn should_select_expired_history_without_adding_event_only_append_number() {
  let expired = super::retention::select_expired_history_after_append(&[1, 2, 3], None, 2);
  assert_eq!(expired, vec![1]);
  assert!(super::retention::select_expired_history_after_append(&[], None, 1).is_empty());
}

// S-1: 保持件数 0（`Some(0)`）は設定エラー。
#[test]
fn should_retention_settings_reject_zero_keep_count() {
  let settings = RetentionSettings::keep_latest(0);

  let error = settings.validate().expect_err("0 は設定エラー");

  assert!(matches!(
    error,
    EventStoreError::Configuration {
      reason: ConfigurationReason::KeepSnapshotCountZero
    }
  ));
}

// MEM-3 / MEM-12: メモリでは、保持件数があるときの Ttl 方式は設定エラー。
#[test]
fn should_retention_settings_reject_ttl_with_keep_count_for_memory() {
  let settings = RetentionSettings::keep_latest(2).with_mode(RetentionMode::Ttl { grace_seconds: 30 });

  let error = settings
    .validate_for_memory()
    .expect_err("メモリで保持件数ありの Ttl は設定エラー");

  assert!(matches!(
    error,
    EventStoreError::Configuration {
      reason: ConfigurationReason::TtlWithKeepSnapshotCount
    }
  ));
}

// S-1: 保存先に依存しない検査は、保持件数と Ttl の併用を拒否しない（DynamoDB は併用を許す）。
#[test]
fn should_retention_settings_validate_accept_ttl_with_keep_count() {
  let settings = RetentionSettings::keep_latest(2).with_mode(RetentionMode::Ttl { grace_seconds: 60 });

  settings
    .validate()
    .expect("保存先に依存しない検査は、保持件数と Ttl を拒まない");
}

// 2.10: 保持件数がないときは方式を使わないので、Ttl でも設定エラーにしない。
#[test]
fn should_retention_settings_ignore_ttl_without_keep_count() {
  let settings = RetentionSettings::current_only().with_mode(RetentionMode::Ttl { grace_seconds: 0 });

  settings.validate().expect("保持件数がなければ Ttl を無視する");
  settings
    .validate_for_memory()
    .expect("保持件数がなければメモリでも Ttl を無視する");
}

// S-1: 保持件数あり + Delete は有効。現在だけ + Delete も有効。
#[test]
fn should_retention_settings_accept_valid_combinations() {
  RetentionSettings::keep_latest(1).validate().expect("Some(1) は有効");
  RetentionSettings::current_only().validate().expect("None は有効");
}

// S-2: 新しい n 件を残し、それより古いものを返す。
#[test]
fn should_select_expired_history_keep_newest_n() {
  let visible = [1, 2, 3, 4, 5];

  let expired = select_expired_history(&visible, 5, 2);

  assert_eq!(expired, vec![3, 2, 1]);
}

// S-2: 今書いた履歴を重ねずに加える（すでに見えていれば二重に数えない）。
#[test]
fn should_select_expired_history_include_just_written_once() {
  let visible = [1, 2, 3];

  let expired = select_expired_history(&visible, 3, 2);

  assert_eq!(expired, vec![1]);
}

#[test]
fn should_select_expired_history_keep_just_written_with_duplicate_visible_numbers() {
  assert!(select_expired_history(&[1, 1], 1, 1).is_empty());
}

#[test]
fn should_select_expired_history_deduplicate_before_keeping_one() {
  assert_eq!(select_expired_history(&[2, 2, 1], 2, 1), vec![1]);
}

#[test]
fn should_select_expired_history_deduplicate_unsorted_numbers_before_keeping_two() {
  assert_eq!(select_expired_history(&[3, 2, 3, 1, 2], 3, 2), vec![1]);
  assert_eq!(select_expired_history(&[4, 1, 3, 2, 3, 1], 4, 2), vec![2, 1]);
}

#[test]
fn should_select_expired_history_add_missing_just_written_and_deduplicate_visible_numbers() {
  assert_eq!(select_expired_history(&[1, 1], 2, 1), vec![1]);
}

#[test]
fn should_select_expired_history_deduplicate_on_event_only_append() {
  let expired = super::retention::select_expired_history_after_append(&[2, 2, 1], None, 1);
  assert_eq!(expired, vec![1]);
  assert!(super::retention::select_expired_history_after_append(&[], None, 1).is_empty());
}

// S-2: 今書いた履歴が見えていなければ加えてから選ぶ。
#[test]
fn should_select_expired_history_add_just_written_when_not_visible() {
  let visible = [1, 2, 3];

  let expired = select_expired_history(&visible, 4, 2);

  assert_eq!(expired, vec![2, 1]);
}

// S-2: 保持件数以内なら取り除く対象はない。
#[test]
fn should_select_expired_history_return_nothing_within_keep() {
  let visible = [1, 2, 3];

  let expired = select_expired_history(&visible, 4, 4);

  assert!(expired.is_empty());
}

// S-3: 猶予 0 のとき、期限は印を付ける時刻そのもの。
#[test]
fn should_ttl_expires_with_zero_grace_equals_marked_at() {
  let expires = ttl_expires_epoch_seconds(4_102_444_800, 0);

  assert_eq!(expires, 4_102_444_800u128);
}

// S-3: 猶予に上限がないので、`u64::MAX` の猶予でもあふれない（u128 で計算する）。
#[test]
fn should_ttl_expires_not_overflow_with_u64_max_grace() {
  let expires = ttl_expires_epoch_seconds(u64::MAX, u64::MAX);

  assert_eq!(expires, u128::from(u64::MAX) * 2);
  assert!(expires > u128::from(u64::MAX));
}
