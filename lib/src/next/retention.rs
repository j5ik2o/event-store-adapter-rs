use crate::next::error::{ConfigurationReason, EventStoreError};
use crate::next::seq_nr::SeqNr;

/// スナップショット保持の設定。メモリと DynamoDB が共有する。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionSettings {
  keep_snapshot_count: Option<usize>,
  mode: RetentionMode,
}

/// 保持の方式（S-3）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetentionMode {
  /// 古い履歴を削除する（S-2）。
  Delete,
  /// 期限切れの印を付ける（S-3）。猶予は秒単位。印を付ける時刻にこの秒数を足した値が期限になる。
  Ttl { grace_seconds: u64 },
}

impl RetentionSettings {
  /// 履歴を持たない。現在のスナップショットだけを持つ（既定）。
  pub fn current_only() -> Self {
    Self {
      keep_snapshot_count: None,
      mode: RetentionMode::Delete,
    }
  }

  /// 新しい `count` 件の履歴を残す。0 の検査はストアの生成時に行う（S-1）。
  pub fn keep_latest(count: usize) -> Self {
    Self {
      keep_snapshot_count: Some(count),
      mode: RetentionMode::Delete,
    }
  }

  /// 保持の方式を設定した設定を返す。
  pub fn with_mode(mut self, mode: RetentionMode) -> Self {
    self.mode = mode;
    self
  }

  /// 保持件数を返す。
  pub fn keep_snapshot_count(&self) -> Option<usize> {
    self.keep_snapshot_count
  }

  /// 保持の方式を返す。
  pub fn mode(&self) -> &RetentionMode {
    &self.mode
  }

  /// S-1: 保存先に依存しない設定検査を行う。保持件数 0 は設定エラー。
  ///
  /// メモリと DynamoDB が共有する。保持件数と `Ttl` の併用はメモリ固有の規則（MEM-3・MEM-12）なので、
  /// ここでは検査しない（DynamoDB は併用を許す。`validate_for_memory`）。
  pub fn validate(&self) -> Result<(), EventStoreError> {
    if self.keep_snapshot_count == Some(0) {
      return Err(EventStoreError::Configuration {
        reason: ConfigurationReason::KeepSnapshotCountZero,
      });
    }
    Ok(())
  }

  /// MEM-3・MEM-12: メモリの保存先が行う設定検査。S-1 に加え、保持件数があるときの `Ttl` を
  /// 設定エラーにする。保持件数がないときは方式を使わないので、`Ttl` でも設定エラーにしない。
  ///
  /// メモリ固有の規則なので、`validate` には含めない。DynamoDB は保持件数と `Ttl` を併用できる。
  /// PR 8 の `MemoryStorage::new` が消費する。
  #[allow(dead_code)]
  pub(crate) fn validate_for_memory(&self) -> Result<(), EventStoreError> {
    self.validate()?;
    if self.keep_snapshot_count.is_some() && matches!(self.mode, RetentionMode::Ttl { .. }) {
      return Err(EventStoreError::Configuration {
        reason: ConfigurationReason::TtlWithKeepSnapshotCount,
      });
    }
    Ok(())
  }
}

/// S-2: 保持の対象（除くべき古い履歴）を選ぶ。メモリと DynamoDB で共有する。
///
/// 手順は「見える履歴を取る」「今書いた履歴を重ねずに加える」「降順に並べる」「新しい n 件を残し、
/// それより古いものを選ぶ」である。戻り値は、新しい順に並べた「取り除く対象」の `seq_nr`。
pub fn select_expired_history(visible: &[SeqNr], just_written: SeqNr, keep: usize) -> Vec<SeqNr> {
  let mut history: Vec<SeqNr> = visible.to_vec();
  if !history.contains(&just_written) {
    history.push(just_written);
  }
  history.sort_unstable_by(|left, right| right.cmp(left));
  history.into_iter().skip(keep).collect()
}

/// S-3: 印を付ける期限（エポック秒）を計算する。
///
/// 猶予に上限を認めないので、`u64` の加算はあふれる。あふれない幅（`u128`）で計算する。
pub fn ttl_expires_epoch_seconds(marked_at_epoch_seconds: u64, grace_seconds: u64) -> u128 {
  u128::from(marked_at_epoch_seconds) + u128::from(grace_seconds)
}
