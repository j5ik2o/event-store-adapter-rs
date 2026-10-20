//! 設定値の検査と、設定行（`config:metadata`）の解析・照合（BT-2・BT-13・BT-16・BT-17・2 章）の試験。
//! `configuration.rs` の末尾から `#[cfg(test)] #[path = "configuration_test.rs"] mod tests;` で結ぶ。
//! 通信を行わない純粋な検査だけを扱う。保存先を使う検査は `lib/tests/bigtable_test.rs` に置く。

use super::*;
use crate::bigtable::BigtableOptions;
use crate::error::{ConfigurationReason, EventStoreError};
use crate::retention::{RetentionMode, RetentionSettings};

const STORE_ID: &str = "5f0c8b8e-3c1a-4d7e-9a4b-2f6d1c0e7a11";

fn is_configuration_error(result: &Result<impl std::fmt::Debug, EventStoreError>) -> bool {
  matches!(result, Err(EventStoreError::Configuration { .. }))
}

fn options_with_limit(envelope_limit_bytes: u64) -> BigtableOptions {
  BigtableOptions {
    envelope_limit_bytes,
    ..BigtableOptions::default()
  }
}

/// 両テーブルが同じ値を持つ設定行の JSON を、キーの順序と空白を変えずに組み立てる。
fn config_json(keep_snapshot_count: &str, envelope_limit_bytes: &str) -> String {
  format!(
    r#"{{"store_id":"{STORE_ID}","layout_version":1,"keep_snapshot_count":{keep_snapshot_count},"retention_mode":"delete","envelope_limit_bytes":{envelope_limit_bytes}}}"#
  )
}

// ---------------------------------------------------------------------------
// 設定値の検査（BT-13・BT-17・BT-16）
// ---------------------------------------------------------------------------

// BT-13: 期限切れ方式（TTL）を求める設定は、通信の前に設定エラーにする。
#[test]
fn should_ttl_retention_mode_be_a_configuration_error_before_any_communication() {
  let options = BigtableOptions {
    retention: RetentionSettings::keep_latest(1).with_mode(RetentionMode::Ttl { grace_seconds: 60 }),
    ..BigtableOptions::default()
  };

  let result = validate_options(&options);

  assert!(
    matches!(
      result,
      Err(EventStoreError::Configuration {
        reason: ConfigurationReason::BigtableTtlUnsupported
      })
    ),
    "{result:?}"
  );
}

// BT-17: 変更フィードを求める設定は、通信の前に設定エラーにする。
#[test]
fn should_change_feed_request_be_a_configuration_error_before_any_communication() {
  let options = BigtableOptions {
    change_feed: true,
    ..BigtableOptions::default()
  };

  let result = validate_options(&options);

  assert!(
    matches!(
      result,
      Err(EventStoreError::Configuration {
        reason: ConfigurationReason::BigtableChangeFeedUnsupported
      })
    ),
    "{result:?}"
  );
}

// BT-16: 既定の上限は 1048576。最小の封筒（スナップショット封筒の 17 バイト）と最大の 8388608 は受け入れる。
#[test]
fn should_envelope_limit_accept_the_default_and_both_ends_of_the_documented_range() {
  assert_eq!(BigtableOptions::default().envelope_limit_bytes, 1_048_576);

  for limit in [1_048_576, 17, 8_388_608] {
    let result = validate_options(&options_with_limit(limit));

    assert!(result.is_ok(), "{limit}: {result:?}");
  }
}

// BT-16: 最小の封筒も収まらない値（16）と、最大を超える値（8388609）は設定エラーにする。
#[test]
fn should_envelope_limit_outside_the_documented_range_be_a_configuration_error() {
  for limit in [16, 8_388_609] {
    let result = validate_options(&options_with_limit(limit));

    assert!(
      matches!(
        result,
        Err(EventStoreError::Configuration {
          reason: ConfigurationReason::BigtableEnvelopeLimitOutOfRange
        })
      ),
      "{limit}: {result:?}"
    );
  }
}

// ---------------------------------------------------------------------------
// 設定行の照合（2 章・BT-D1）
// ---------------------------------------------------------------------------

// 2 章: JSON のキーの順序と空白は比較しない。両テーブルが同じ値を持てば生成できる。
#[test]
fn should_verify_configuration_ignore_key_order_and_whitespace() {
  let head = format!(
    r#"{{ "envelope_limit_bytes" : 1048576 , "retention_mode":"delete","keep_snapshot_count":null,"layout_version":1,"store_id":"{STORE_ID}" }}"#
  );
  let journal = format!(
    r#"{{"layout_version":1,"store_id":"{STORE_ID}","retention_mode":"delete","envelope_limit_bytes":1048576,"keep_snapshot_count":null}}"#
  );

  let result = verify_configuration(journal.as_bytes(), head.as_bytes(), None, 1_048_576);

  assert_eq!(result.expect("キーの順序と空白が違っても照合できる"), STORE_ID);
}

// 2 章: 重複した JSON キーは設定エラーにする（同じ値でも許さない）。
#[test]
fn should_verify_configuration_reject_a_duplicate_json_key() {
  let head = format!(
    r#"{{"store_id":"{STORE_ID}","store_id":"{STORE_ID}","layout_version":1,"keep_snapshot_count":null,"retention_mode":"delete","envelope_limit_bytes":1048576}}"#
  );
  let journal = config_json("null", "1048576");

  let result = verify_configuration(journal.as_bytes(), head.as_bytes(), None, 1_048_576);

  assert!(is_configuration_error(&result), "{result:?}");
}

// 2 章: 保持件数は指数表記や先頭のゼロを使わない 10 進文字列。"02" は設定エラーにする。
#[test]
fn should_verify_configuration_reject_a_leading_zero_keep_snapshot_count() {
  let json = config_json(r#""02""#, "1048576");

  let result = verify_configuration(json.as_bytes(), json.as_bytes(), Some(2), 1_048_576);

  assert!(is_configuration_error(&result), "{result:?}");
}

// 2 章: 封筒の上限は JSON の整数。1048576.0 は整数ではないので設定エラーにする。
#[test]
fn should_verify_configuration_reject_a_non_integer_envelope_limit() {
  let json = config_json("null", "1048576.0");

  let result = verify_configuration(json.as_bytes(), json.as_bytes(), None, 1_048_576);

  assert!(is_configuration_error(&result), "{result:?}");
}
