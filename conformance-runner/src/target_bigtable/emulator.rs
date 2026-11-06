//! プロセスの中で共有する Bigtable エミュレーター。
//!
//! 適合ケースは全て同じエミュレーターを使い、場面ごとに新しいテーブルを作る。コンテナは専用のスレッドが持ち、
//! 使うスレッドが全ていなくなったときに削除する。スレッドごとに共有を保持するので、同じスレッドで続けて実行するケースは
//! 起動し直さない。実行器のコマンドは、メインスレッドの共有を [`release_emulator`] で明示的に放す。

use std::cell::RefCell;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex, Weak};
use std::thread::JoinHandle;

use event_store_adapter_test_utils_rs::docker;

/// 専用のスレッドでコンテナを持つ。落とすとコンテナを削除する。
struct EmulatorHost {
  port: u16,
  stop: Option<Sender<()>>,
  thread: Option<JoinHandle<()>>,
}

impl EmulatorHost {
  fn start() -> Result<Self, String> {
    let (ready_sender, ready_receiver) = channel::<Result<u16, String>>();
    let (stop, stop_receiver) = channel::<()>();
    let thread = std::thread::Builder::new()
      .name("bigtable-emulator".to_owned())
      .spawn(move || {
        match docker::bigtable_emulator() {
          Ok(container) => {
            let _ = ready_sender.send(Ok(container.port()));
            // 使う側が全ていなくなる（送信側が落ちる）まで、コンテナを持ち続ける。
            let _ = stop_receiver.recv();
            drop(container);
          }
          Err(error) => {
            let _ = ready_sender.send(Err(format!("{error:#}")));
          }
        }
      })
      .map_err(|error| format!("エミュレーターのスレッドを起動できない: {error}"))?;
    match ready_receiver.recv() {
      Ok(Ok(port)) => Ok(Self {
        port,
        stop: Some(stop),
        thread: Some(thread),
      }),
      Ok(Err(error)) => {
        let _ = thread.join();
        Err(error)
      }
      Err(_) => {
        let _ = thread.join();
        Err("エミュレーターのスレッドが、起動の結果を返す前に終了した".to_owned())
      }
    }
  }
}

impl Drop for EmulatorHost {
  fn drop(&mut self) {
    drop(self.stop.take());
    if let Some(thread) = self.thread.take() {
      let _ = thread.join();
    }
  }
}

static SHARED: Mutex<Weak<EmulatorHost>> = Mutex::new(Weak::new());

thread_local! {
  /// このスレッドが保持する共有。起動に失敗したときは、その説明を保持して、同じスレッドでは起動し直さない。
  static HELD: RefCell<Option<Result<Arc<EmulatorHost>, String>>> = const { RefCell::new(None) };
}

fn shared() -> Result<Arc<EmulatorHost>, String> {
  let mut slot = SHARED.lock().expect("エミュレーターの共有のロック");
  if let Some(host) = slot.upgrade() {
    return Ok(host);
  }
  let host = Arc::new(EmulatorHost::start()?);
  *slot = Arc::downgrade(&host);
  Ok(host)
}

/// エミュレーターの公開ポートを返す。まだなければ起動して待つ。起動に失敗したときは、その説明を返す。
pub(super) fn port() -> Result<u16, String> {
  HELD.with(|held| {
    held
      .borrow_mut()
      .get_or_insert_with(shared)
      .as_ref()
      .map(|host| host.port)
      .map_err(Clone::clone)
  })
}

/// 現在のスレッドが保持する共有を放す。使うスレッドが他にいなければ、コンテナを削除する。
pub fn release_emulator() {
  HELD.with(|held| held.borrow_mut().take());
}
