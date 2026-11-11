//! 同期の I/O しか持たない保存先の操作を、呼び出し元の実行文脈から外して実行する、上限付きの実行資源（L-1）。
//!
//! 標準ライブラリのスレッドと上限付きの channel だけで作り、特定の非同期ランタイムに依存しない（Q1=A の制約）。

use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::panic::{self, AssertUnwindSafe};
use std::pin::{pin, Pin};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};

use tracing::{Dispatch, Span};

use crate::error::{EventStoreError, StorageOperation};

type Job = Box<dyn FnOnce() + Send>;

/// 1 つの専用ワーカーと、上限付きの待ち行列を持つ実行資源。
///
/// [`EventStoreForSqlite::open_offloaded`](super::EventStoreForSqlite::open_offloaded) で開いたストアは、
/// 操作の全体（入口の検査、直列化、トランザクション、保持、通知）をこのワーカーで、受け付けた順に 1 つずつ実行する。
/// 呼び出し元のスレッドは、受け付けの短い記帳だけを行う。
///
/// 利用者が作って所有する。複製は同じワーカーを指す。同じ実行資源を借りるストアは、すべての操作が 1 列に並ぶ。
/// 使い終わったら [`close`](Self::close) で受け付けを閉じ、[`drained`](Self::drained) で受け付け済みの操作の終わりを待つ。
/// 最後の複製を破棄したときも、受け付けは閉じる。
///
/// ```no_run
/// # use std::num::NonZeroUsize;
/// # use event_store_adapter_rs::{AggregateId, BlockingExecution, EventStoreError, EventStoreForSqlite, SqliteOptions};
/// # #[derive(Debug, Clone)]
/// # struct AccountId;
/// # impl AggregateId for AccountId {
/// #   fn type_name(&self) -> String { "Account".into() }
/// #   fn value(&self) -> String { "1".into() }
/// # }
/// # async fn run() -> Result<(), EventStoreError> {
/// let execution = BlockingExecution::single_worker(NonZeroUsize::new(64).unwrap())
///   .expect("start the worker thread");
/// let store: EventStoreForSqlite<AccountId, u64, u64> =
///   EventStoreForSqlite::open_offloaded("accounts.sqlite3", SqliteOptions::default(), &execution)?;
/// // store の 4 つの操作は、execution のワーカーで動く。
/// execution.close();
/// execution.drained().await;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct BlockingExecution {
  lane: Arc<Lane>,
}

struct Lane {
  admission: Mutex<Option<SyncSender<Job>>>,
  drain: Arc<Mutex<DrainState>>,
}

#[derive(Default)]
struct DrainState {
  done: bool,
  wakers: Vec<Waker>,
}

/// 実行資源が操作を受け付けなかった理由。拒否した操作は実行していない。
///
/// 拒否は `EventStoreError::Storage` の `source` に入る。`downcast_ref::<ExecutionRejected>()` で区別できる。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ExecutionRejected {
  /// 実行中の 1 件に加えて、待ち行列が上限まで埋まっている。
  QueueFull,
  /// [`BlockingExecution::close`] のあとである。
  Closed,
}

impl fmt::Display for ExecutionRejected {
  fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      ExecutionRejected::QueueFull => formatter.write_str("the blocking execution queue is full"),
      ExecutionRejected::Closed => formatter.write_str("the blocking execution is closed"),
    }
  }
}

impl std::error::Error for ExecutionRejected {}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
  mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl BlockingExecution {
  /// 1 つのワーカーのスレッドを起動し、`queue_capacity` 件まで待たせる実行資源を作る。
  ///
  /// 同時に受け付ける操作は、実行中の 1 件と待ち行列の `queue_capacity` 件までである。
  /// スレッドを起動できないときは、その誤りを返す。
  pub fn single_worker(queue_capacity: NonZeroUsize) -> std::io::Result<Self> {
    let (sender, receiver) = mpsc::sync_channel::<Job>(queue_capacity.get());
    let drain = Arc::new(Mutex::new(DrainState::default()));
    let worker_drain = drain.clone();
    thread::Builder::new()
      .name("event-store-adapter-blocking".to_owned())
      .spawn(move || {
        // 送り手がすべて消えると、受け付け済みの操作を出し切ってから止まる。
        for job in receiver {
          // 結果の受け渡しで起きた panic でも、後ろに並んだ操作を取り残さない。
          let _ = panic::catch_unwind(AssertUnwindSafe(job));
        }
        let wakers = {
          let mut state = lock(&worker_drain);
          state.done = true;
          std::mem::take(&mut state.wakers)
        };
        wakers.into_iter().for_each(Waker::wake);
      })?;
    Ok(Self {
      lane: Arc::new(Lane {
        admission: Mutex::new(Some(sender)),
        drain,
      }),
    })
  }

  /// 受け付けを閉じる。待たずに戻る。受け付け済みの操作は、受け付けた順に最後まで実行する。何度呼んでもよい。
  pub fn close(&self) {
    let sender = lock(&self.lane.admission).take();
    drop(sender);
  }

  /// 受け付けが閉じ、受け付け済みの操作がすべて終わり、ワーカーが止まると完了する Future を返す。
  ///
  /// 閉じる前に待ち始めてもよい。その場合は、閉じたあとの終わりを待つ。この実行資源で動く操作の中から待ってはならない
  /// （終わらない）。
  pub fn drained(&self) -> Drained {
    Drained {
      state: self.lane.drain.clone(),
    }
  }

  /// 操作を待ち行列に入れ、その結果を待つ Future を返す。満杯か閉じているときは、操作を実行せずに拒否を返す。
  ///
  /// 操作は、受け付けたときの `tracing` の購読者と span の中で動く。保持の失敗の通知（S-4）は、呼び出した側の
  /// 文脈に届く。購読者が 1 度も設定されていないプロセスでは、何も設定せずに動かし、`tracing` の `log` への出力を
  /// 止めない（購読者の設定は、`Dispatch::none()` でもプロセス全体の印を立てるため）。操作の中の panic は、返した Future を poll した側で再び起こす。
  pub(crate) fn submit<T, F>(&self, operation: StorageOperation, job: F) -> Result<Reply<T>, EventStoreError>
  where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static, {
    let state = Arc::new(Mutex::new(ReplyState {
      outcome: None,
      waker: None,
    }));
    let completer = state.clone();
    let context =
      tracing::dispatcher::has_been_set().then(|| (tracing::dispatcher::get_default(Dispatch::clone), Span::current()));
    let job: Job = Box::new(move || {
      let run = || panic::catch_unwind(AssertUnwindSafe(job));
      let outcome = match context {
        Some((dispatch, span)) => tracing::dispatcher::with_default(&dispatch, || span.in_scope(run)),
        None => run(),
      };
      let waker = {
        let mut state = lock(&completer);
        state.outcome = Some(outcome);
        state.waker.take()
      };
      if let Some(waker) = waker {
        waker.wake();
      }
    });
    let refused = {
      let admission = lock(&self.lane.admission);
      match admission.as_ref() {
        None => Some((job, ExecutionRejected::Closed)),
        Some(sender) => match sender.try_send(job) {
          Ok(()) => None,
          Err(TrySendError::Full(job)) => Some((job, ExecutionRejected::QueueFull)),
          Err(TrySendError::Disconnected(job)) => Some((job, ExecutionRejected::Closed)),
        },
      }
    };
    match refused {
      None => Ok(Reply { state }),
      // 入力の破棄（利用者の Drop）を、受け付けのロックの外で行う。
      Some((job, reason)) => {
        drop(job);
        Err(EventStoreError::Storage {
          operation,
          source: Box::new(reason),
        })
      }
    }
  }
}

impl fmt::Debug for BlockingExecution {
  fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    formatter
      .debug_struct("BlockingExecution")
      .field("closed", &lock(&self.lane.admission).is_none())
      .finish()
  }
}

/// [`BlockingExecution::drained`] が返す Future。
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Drained {
  state: Arc<Mutex<DrainState>>,
}

impl fmt::Debug for DrainState {
  fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    formatter.debug_struct("DrainState").field("done", &self.done).finish()
  }
}

impl Future for Drained {
  type Output = ();

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
    let mut state = lock(&self.state);
    if state.done {
      return Poll::Ready(());
    }
    if !state.wakers.iter().any(|waker| waker.will_wake(cx.waker())) {
      state.wakers.push(cx.waker().clone());
    }
    Poll::Pending
  }
}

struct ReplyState<T> {
  outcome: Option<thread::Result<T>>,
  waker: Option<Waker>,
}

/// 受け付けた操作の結果を待つ Future。破棄しても、受け付けた操作は止まらない。
pub(crate) struct Reply<T> {
  state: Arc<Mutex<ReplyState<T>>>,
}

impl<T> Future for Reply<T> {
  type Output = T;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
    let mut state = lock(&self.state);
    match state.outcome.take() {
      Some(Ok(value)) => Poll::Ready(value),
      Some(Err(payload)) => {
        drop(state);
        panic::resume_unwind(payload)
      }
      None => {
        state.waker = Some(cx.waker().clone());
        Poll::Pending
      }
    }
  }
}

struct ThreadWaker(Thread);

impl Wake for ThreadWaker {
  fn wake(self: Arc<Self>) {
    self.0.unpark();
  }
}

/// ワーカーの上で、保存先の非同期の操作を最後まで進める。SQLite の操作は I/O を同期で行うので、待ちは起きない。
pub(crate) fn block_on<F: Future>(future: F) -> F::Output {
  let waker = Waker::from(Arc::new(ThreadWaker(thread::current())));
  let mut cx = Context::from_waker(&waker);
  let mut future = pin!(future);
  loop {
    match future.as_mut().poll(&mut cx) {
      Poll::Ready(output) => return output,
      Poll::Pending => thread::park(),
    }
  }
}
