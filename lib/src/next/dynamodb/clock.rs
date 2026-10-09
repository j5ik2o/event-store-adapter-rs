use std::time::{SystemTime, UNIX_EPOCH};

/// TTL の印付け時刻をエポック秒で返す。
#[doc(hidden)]
pub trait Clock: std::fmt::Debug + Send + Sync {
  fn now_epoch_seconds(&self) -> u64;
}

#[derive(Debug)]
pub(super) struct SystemClock;

impl Clock for SystemClock {
  fn now_epoch_seconds(&self) -> u64 {
    SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .expect("system clock is before the Unix epoch")
      .as_secs()
  }
}

#[cfg(test)]
#[path = "clock_test.rs"]
mod tests;
