//! 保存先の I/O が呼び出し元のランタイムのワーカーを塞がないこと（L-1）を確かめる補助。
//!
//! - [`SlowProxy`] は、保存先（エミュレーター）の応答を遅らせて中継する TCP の中継。中継は試験のランタイムとは
//!   別のスレッドのランタイムで動くので、試験のスレッドが塞がれても、中継は止まらない。
//! - [`measure_caller_stall`] は、current_thread のランタイムで操作を待つ間に、同じスレッドの別のタスクが
//!   予定より遅れて動いた最長の時間を測る。操作が同期で待てば、その間ほかのタスクは動けず、遅れが大きくなる。

#![allow(dead_code)]

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::runtime::{Handle, Runtime, RuntimeFlavor};

/// 遅らせるときの、保存先からの応答の 1 回の読み取りごとの遅れ。
pub const RESPONSE_DELAY: Duration = Duration::from_millis(300);
/// 塞いでいないとみなす、別のタスクの遅れの上限。
pub const STALL_LIMIT: Duration = Duration::from_millis(150);
/// 別のタスクが眠る間隔。
const TICK: Duration = Duration::from_millis(10);

/// 保存先からの応答を、読み取りごとに遅らせて中継する。遅れは [`SlowProxy::delay_responses`] で切り替える。
pub struct SlowProxy {
  port: u16,
  delay_millis: Arc<AtomicU64>,
  runtime: Option<Runtime>,
}

impl SlowProxy {
  /// `127.0.0.1:upstream_port` へ中継する。始めは遅らせない。
  pub fn start(upstream_port: u16) -> Self {
    let runtime = tokio::runtime::Builder::new_multi_thread()
      .worker_threads(2)
      .enable_all()
      .build()
      .expect("中継のランタイムを作れる");
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("中継の待ち受けを作れる");
    listener.set_nonblocking(true).unwrap();
    let port = listener.local_addr().unwrap().port();
    let delay_millis = Arc::new(AtomicU64::new(0));
    let delay = delay_millis.clone();
    runtime.spawn(async move {
      let listener = tokio::net::TcpListener::from_std(listener).unwrap();
      while let Ok((client, _)) = listener.accept().await {
        let delay = delay.clone();
        tokio::spawn(async move {
          let Ok(upstream) = tokio::net::TcpStream::connect(("127.0.0.1", upstream_port)).await else {
            return;
          };
          let (mut client_read, mut client_write) = client.into_split();
          let (mut upstream_read, mut upstream_write) = upstream.into_split();
          let requests = async move {
            let _ = tokio::io::copy(&mut client_read, &mut upstream_write).await;
            let _ = upstream_write.shutdown().await;
          };
          let responses = async move {
            let mut buffer = vec![0u8; 64 * 1024];
            while let Ok(read) = upstream_read.read(&mut buffer).await {
              if read == 0 {
                break;
              }
              let millis = delay.load(Ordering::SeqCst);
              if millis > 0 {
                tokio::time::sleep(Duration::from_millis(millis)).await;
              }
              if client_write.write_all(&buffer[..read]).await.is_err() {
                break;
              }
            }
            let _ = client_write.shutdown().await;
          };
          tokio::join!(requests, responses);
        });
      }
    });
    Self {
      port,
      delay_millis,
      runtime: Some(runtime),
    }
  }

  /// 中継の待ち受けのポート。保存先のクライアントは、エミュレーターの代わりにここへ接続する。
  pub fn port(&self) -> u16 {
    self.port
  }

  /// これ以降に保存先から読み取った応答を、読み取りごとに [`RESPONSE_DELAY`] 遅らせる。
  pub fn delay_responses(&self) {
    self
      .delay_millis
      .store(RESPONSE_DELAY.as_millis() as u64, Ordering::SeqCst);
  }
}

impl Drop for SlowProxy {
  fn drop(&mut self) {
    if let Some(runtime) = self.runtime.take() {
      runtime.shutdown_background();
    }
  }
}

/// `operation` を待った時間と、その間に別のタスクが予定より遅れた最長の時間。
#[derive(Debug)]
pub struct CallerStall {
  pub elapsed: Duration,
  pub longest_delay: Duration,
}

/// current_thread のランタイムで `operation` を待ち、その間に同じスレッドの別のタスクが遅れた最長の時間を測る。
///
/// 操作が終わったあとも少し待つ。操作の最後の部分が同期で待っても、その遅れを数えるためである。
pub async fn measure_caller_stall<F: Future>(operation: F) -> (F::Output, CallerStall) {
  assert_eq!(
    Handle::current().runtime_flavor(),
    RuntimeFlavor::CurrentThread,
    "試験の前提: スレッドが 1 つのランタイムで測る"
  );
  let longest = Arc::new(Mutex::new(Duration::ZERO));
  let ticker = tokio::spawn({
    let longest = longest.clone();
    async move {
      loop {
        let started = Instant::now();
        tokio::time::sleep(TICK).await;
        let late = started.elapsed().saturating_sub(TICK);
        let mut longest = longest.lock().unwrap();
        *longest = (*longest).max(late);
      }
    }
  });
  tokio::task::yield_now().await;
  let started = Instant::now();
  let output = operation.await;
  let elapsed = started.elapsed();
  tokio::time::sleep(TICK * 3).await;
  ticker.abort();
  let longest_delay = *longest.lock().unwrap();
  (output, CallerStall { elapsed, longest_delay })
}

/// 遅らせた応答を待つ間も、別のタスクが [`STALL_LIMIT`] を超えて遅れないことを確かめる。
pub fn assert_caller_not_blocked(operation: &str, stall: &CallerStall) {
  assert!(
    stall.elapsed >= RESPONSE_DELAY,
    "試験の前提: {operation} は遅らせた応答を待つ（{stall:?}）"
  );
  assert!(
    stall.longest_delay < STALL_LIMIT,
    "{operation} が保存先の応答を待つ間も、呼び出し元のスレッドは別のタスクを進める（{stall:?}）"
  );
}
