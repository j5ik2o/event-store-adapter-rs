//! プロセスの中で共有する Bigtable エミュレーター。
//!
//! 適合ケースは全て同じエミュレーターを使い、場面ごとに新しいテーブルを作る。コンテナは専用のスレッドと実行基盤が持ち、
//! 使うスレッドが全ていなくなったときに削除する。スレッドごとに共有を保持するので、同じスレッドで続けて実行するケースは
//! 起動し直さない。実行器のコマンドは、メインスレッドの共有を [`release_emulator`] で明示的に放す。

use std::cell::RefCell;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex, Weak};
use std::thread::JoinHandle;

use event_store_adapter_test_utils_rs::docker;

/// 専用のスレッドと実行基盤でコンテナを持つ。落とすとコンテナを削除する。
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
        let runtime = match tokio::runtime::Builder::new_multi_thread()
          .worker_threads(2)
          .enable_all()
          .build()
        {
          Ok(runtime) => runtime,
          Err(error) => {
            let _ = ready_sender.send(Err(format!("エミュレーター用の実行基盤を作れない: {error}")));
            return;
          }
        };
        runtime.block_on(async move {
          let started = async {
            let container = docker::bigtable_emulator()
              .await
              .map_err(|error| format!("{error:#}"))?;
            let port = container
              .get_host_port_ipv4(docker::BIGTABLE_EMULATOR_PORT)
              .await
              .map_err(|error| error.to_string())?;
            Ok::<_, String>((container, port))
          }
          .await;
          match started {
            Ok((container, port)) => {
              let _ = ready_sender.send(Ok(port));
              // 使う側が全ていなくなる（送信側が落ちる）まで、コンテナを持ち続ける。
              let _ = tokio::task::spawn_blocking(move || stop_receiver.recv()).await;
              drop(container);
            }
            Err(error) => {
              let _ = ready_sender.send(Err(error));
            }
          }
        });
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
