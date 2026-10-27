//! プロセスの中で共有する Spanner エミュレーター。
//!
//! 適合ケースは全て同じエミュレーターを使い、場面ごとに新しいデータベースを作る。コンテナは試験補助が持ち、
//! 使うスレッドが全ていなくなったときに削除する。スレッドごとに共有を保持するので、同じスレッドで続けて実行する
//! ケースは起動し直さない。実行器のコマンドは、メインスレッドの共有を [`release_emulator`] で明示的に放す。

use std::cell::RefCell;
use std::sync::Arc;

use event_store_adapter_test_utils_rs::spanner::{shared_emulator, EmulatorHost};

thread_local! {
  /// このスレッドが保持する共有。起動に失敗したときは、その説明を保持して、同じスレッドでは起動し直さない。
  static HELD: RefCell<Option<Result<Arc<EmulatorHost>, String>>> = const { RefCell::new(None) };
}

/// エミュレーターの公開ポートを返す。まだなければ起動して待つ。起動に失敗したときは、その説明を返す。
pub(super) fn port() -> Result<u16, String> {
  HELD.with(|held| {
    held
      .borrow_mut()
      .get_or_insert_with(shared_emulator)
      .as_ref()
      .map(|host| host.port())
      .map_err(Clone::clone)
  })
}

/// 現在のスレッドが保持する共有を放す。使うスレッドが他にいなければ、コンテナを削除する。
pub fn release_emulator() {
  HELD.with(|held| held.borrow_mut().take());
}
