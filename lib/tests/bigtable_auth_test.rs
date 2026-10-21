#![cfg(all(feature = "bigtable", feature = "test-hooks"))]
//! Bigtable の保存先の認証（`authorization: Bearer <トークン>`）の結合試験（エミュレーター）。
//!
//! エミュレーターは認証を見ないので、メタデータが付くことは、機能 `test-hooks` の `BigtableTestHooks::before_request`
//! （要求を送る直前に、その要求の種類と、付けた `authorization` の値を受け取る）で確かめる。取得元が失敗した要求や、
//! 使えないトークンの要求は送られないので、フックの記録にも現れない。

mod bigtable_support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bigtable_support::{
  event, journal_key, only_value, options_keeping, row_prefix, snapshot, Fixture, Id, Store, TableKind,
};
use event_store_adapter_rs::bigtable::{initialize_bigtable_store_with_hooks, BigtableRequestKind, BigtableTestHooks};
use event_store_adapter_rs::{
  initialize_bigtable_store_with_token_source, AidString, BigtableOptions, BigtableTokenSource, EventStore,
  EventStoreError, JsonEventSerializer, JsonSnapshotSerializer, SeqNr, StorageOperation,
};
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::Layer;
use BigtableRequestKind::{CheckAndMutateRow, GetTable, ReadRows};

// ---------------------------------------------------------------------------
// 試験の補助: 要求を記録するフック、呼び出しごとに値の変わる取得元
// ---------------------------------------------------------------------------

/// フックが受け取った 1 件分（要求の種類、付いていた `authorization` の値）。
type Recorded = Vec<(BigtableRequestKind, Option<String>)>;

/// 送る直前の要求を記録する。ほかのフックは何もしない。
#[derive(Default)]
struct Recorder {
  requests: Mutex<Recorded>,
}

impl std::fmt::Debug for Recorder {
  fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    formatter.debug_struct("Recorder").finish()
  }
}

impl Recorder {
  /// これまでの記録を取り出して、空にする。
  fn take(&self) -> Recorded {
    std::mem::take(&mut *self.requests.lock().unwrap())
  }
}

impl BigtableTestHooks for Recorder {
  fn before_commit(&self, _: &AidString, _: SeqNr) -> Result<(), EventStoreError> {
    Ok(())
  }

  fn after_commit(&self, _: &AidString, _: SeqNr) -> Result<(), EventStoreError> {
    Ok(())
  }

  fn journal_write_previous(&self, _: &AidString, _: SeqNr) -> Result<(), EventStoreError> {
    Ok(())
  }

  fn journal_write_tail(&self, _: &AidString, _: SeqNr) -> Result<(), EventStoreError> {
    Ok(())
  }

  fn read_events(&self, _: &AidString) -> Result<(), EventStoreError> {
    Ok(())
  }

  fn read_snapshot(&self, _: &AidString) -> Result<(), EventStoreError> {
    Ok(())
  }

  fn before_request(&self, kind: BigtableRequestKind, authorization: Option<&str>) {
    self
      .requests
      .lock()
      .unwrap()
      .push((kind, authorization.map(str::to_owned)));
  }
}

/// 取得元の振る舞い。`n` は取得元の呼び出しの通し番号（1 から数える）。
#[derive(Debug, Clone, Copy)]
enum Mode {
  /// いつも `token-<n>` を返す。
  Issue,
  /// `n` が `k` 以上の呼び出しが、いつも失敗する。
  FailFrom(usize),
  /// `n` が `k` に等しい呼び出しだけが失敗する。
  FailAt(usize),
  /// `n` が `k` 以上の呼び出しが、メタデータの値にできないトークン（`token-<n>` と改行）を返す。
  InvalidFrom(usize),
}

/// 呼び出しごとに値の変わる取得元。失敗した呼び出しと使えないトークンを返した呼び出しも、回数に数える。
struct Tokens {
  calls: AtomicUsize,
  mode: Mutex<Mode>,
}

impl Tokens {
  fn with_mode(mode: Mode) -> Arc<Self> {
    Arc::new(Self {
      calls: AtomicUsize::new(0),
      mode: Mutex::new(mode),
    })
  }

  fn issuing() -> Arc<Self> {
    Self::with_mode(Mode::Issue)
  }

  fn calls(&self) -> usize {
    self.calls.load(Ordering::SeqCst)
  }

  fn set_mode(&self, mode: Mode) {
    *self.mode.lock().unwrap() = mode;
  }
}

#[async_trait]
impl BigtableTokenSource for Tokens {
  async fn access_token(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
    let mode = *self.mode.lock().unwrap();
    match mode {
      Mode::FailFrom(k) if n >= k => Err(Box::new(std::io::Error::other("token unavailable"))),
      Mode::FailAt(k) if n == k => Err(Box::new(std::io::Error::other("token unavailable"))),
      Mode::InvalidFrom(k) if n >= k => Ok(format!("token-{n}\n")),
      _ => Ok(format!("token-{n}")),
    }
  }
}

/// 記録から、要求の種類の列だけを取り出す。
fn kinds_of(recorded: &Recorded) -> Vec<BigtableRequestKind> {
  recorded.iter().map(|(kind, _)| *kind).collect()
}

fn as_source(tokens: &Arc<Tokens>) -> Arc<dyn BigtableTokenSource> {
  tokens.clone()
}

/// `n` 回目の取得元の呼び出しが返す値が付いた `authorization`。
fn bearer(n: usize) -> Option<String> {
  Some(format!("Bearer token-{n}"))
}

async fn open_store(
  fixture: &Fixture,
  options: &BigtableOptions,
  tokens: &Arc<Tokens>,
  recorder: &Arc<Recorder>,
) -> Result<Store, EventStoreError> {
  Store::open_with_hooks_and_token_source(
    fixture.connection().await,
    fixture.tables.clone(),
    options.clone(),
    Arc::new(JsonEventSerializer::new()),
    Arc::new(JsonSnapshotSerializer::new()),
    as_source(tokens),
    recorder.clone(),
  )
  .await
}

// ---------------------------------------------------------------------------
// 全ての操作を 1 通り行い、操作ごとにフックの記録を集める
// ---------------------------------------------------------------------------

/// 初期化・生成・追記 3 回・読み取り 2 回の、操作ごとのフックの記録。
struct Scenario {
  initialize: Recorded,
  open: Recorded,
  append_event: Recorded,
  append_snapshot: Recorded,
  append_snapshot_retiring: Recorded,
  read_events: Recorded,
  read_snapshot: Recorded,
  /// 保持の削除の要求が実際に効いたことの確認（journal の seq_nr 2 の履歴の状態）。
  history_state_of_seq_nr_2: Option<Vec<u8>>,
}

impl Scenario {
  /// 操作の順に並べた記録。
  fn by_operation(&self) -> [&Recorded; 7] {
    [
      &self.initialize,
      &self.open,
      &self.append_event,
      &self.append_snapshot,
      &self.append_snapshot_retiring,
      &self.read_events,
      &self.read_snapshot,
    ]
  }

  fn all(&self) -> Recorded {
    self.by_operation().into_iter().flatten().cloned().collect()
  }

  fn kinds_by_operation(&self) -> Vec<Vec<BigtableRequestKind>> {
    self
      .by_operation()
      .into_iter()
      .map(|recorded| recorded.iter().map(|(kind, _)| *kind).collect())
      .collect()
  }
}

/// 操作ごとに送られるはずの要求の種類。今の保存先の実装から導いた。保持件数 1、集約は `Id::order()`。
///
/// - 初期化: 両テーブルの設定行を読み（`ReadRows` 2 回）、両テーブルに設定行を作る（`CheckAndMutateRow` 2 回）。
/// - 生成: 両テーブルの列ファミリを照合し（管理 API の `GetTable` 2 回）、両テーブルの設定行を読む（`ReadRows` 2 回）。
/// - イベントだけの最初の追記: head を読み、head を更新し、末尾の行を書く。
/// - 2 回目（スナップショット付き）: head を読み、直前の行を書き出して照合し（`CheckAndMutateRow`、`ReadRows`）、
///   head を更新し、末尾の行を書き、保持の走査で履歴を読む。削除する履歴はまだない。
/// - 3 回目（スナップショット付き）: 2 回目と同じに加え、最後に、保持の削除で履歴を 1 つ `retired` にする。
/// - イベントの読み取り: head を読み、journal の範囲を読む。
/// - 最新のスナップショットの読み取り: head を読む。
fn expected_kinds_by_operation() -> Vec<Vec<BigtableRequestKind>> {
  vec![
    vec![ReadRows, ReadRows, CheckAndMutateRow, CheckAndMutateRow],
    vec![GetTable, GetTable, ReadRows, ReadRows],
    vec![ReadRows, CheckAndMutateRow, CheckAndMutateRow],
    vec![
      ReadRows,
      CheckAndMutateRow,
      ReadRows,
      CheckAndMutateRow,
      CheckAndMutateRow,
      ReadRows,
    ],
    vec![
      ReadRows,
      CheckAndMutateRow,
      ReadRows,
      CheckAndMutateRow,
      CheckAndMutateRow,
      ReadRows,
      CheckAndMutateRow,
    ],
    vec![ReadRows, ReadRows],
    vec![ReadRows],
  ]
}

/// `token_source` があれば取得元を渡す関数で、なければ取得元を渡さない関数で、全ての操作を行う。
async fn run_scenario(token_source: Option<Arc<dyn BigtableTokenSource>>) -> Scenario {
  let options = options_keeping(1);
  let fixture = Fixture::new().await;
  let recorder = Arc::new(Recorder::default());
  initialize_bigtable_store_with_hooks(
    &fixture.connection().await,
    &fixture.tables,
    &options,
    token_source.clone(),
    recorder.clone(),
  )
  .await
  .expect("初期化できる");
  let initialize = recorder.take();

  let connection = fixture.connection().await;
  let opened = match token_source {
    Some(source) => {
      Store::open_with_hooks_and_token_source(
        connection,
        fixture.tables.clone(),
        options.clone(),
        Arc::new(JsonEventSerializer::new()),
        Arc::new(JsonSnapshotSerializer::new()),
        source,
        recorder.clone(),
      )
      .await
    }
    None => {
      Store::open_with_hooks(
        connection,
        fixture.tables.clone(),
        options.clone(),
        Arc::new(JsonEventSerializer::new()),
        Arc::new(JsonSnapshotSerializer::new()),
        recorder.clone(),
      )
      .await
    }
  };
  let store = opened.expect("開ける");
  let open = recorder.take();

  let id = Id::order();
  store
    .persist_event(event(&id, 1))
    .await
    .expect("イベントだけを追記できる");
  let append_event = recorder.take();
  store
    .persist_event_and_snapshot(event(&id, 2), snapshot(2))
    .await
    .expect("イベントとスナップショットを追記できる");
  let append_snapshot = recorder.take();
  store
    .persist_event_and_snapshot(event(&id, 3), snapshot(3))
    .await
    .expect("保持の削除を伴う追記ができる");
  let append_snapshot_retiring = recorder.take();
  store
    .get_events_by_id_since_seq_nr(&id, 1)
    .await
    .expect("イベントを読める");
  let read_events = recorder.take();
  store
    .get_latest_snapshot_by_id(&id)
    .await
    .expect("最新のスナップショットを読める");
  let read_snapshot = recorder.take();

  let row_2 = fixture
    .raw_row(TableKind::Journal, &journal_key(&row_prefix(&id.aid(), None), 2))
    .await;
  Scenario {
    initialize,
    open,
    append_event,
    append_snapshot,
    append_snapshot_retiring,
    read_events,
    read_snapshot,
    history_state_of_seq_nr_2: only_value(&row_2, "history", "state"),
  }
}

// ---------------------------------------------------------------------------
// tracing の通知の収集
// ---------------------------------------------------------------------------

/// 収集した tracing の事象 1 件（target と、フィールド `category` の値）。
type CapturedEvent = (String, Option<String>);

#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<CapturedEvent>>>);

impl Captured {
  fn categories_of(&self, target: &str) -> Vec<String> {
    self
      .0
      .lock()
      .unwrap()
      .iter()
      .filter(|(event_target, _)| event_target == target)
      .filter_map(|(_, category)| category.clone())
      .collect()
  }
}

#[derive(Default)]
struct CategoryVisitor(Option<String>);

impl Visit for CategoryVisitor {
  fn record_str(&mut self, field: &Field, value: &str) {
    if field.name() == "category" {
      self.0 = Some(value.to_string());
    }
  }

  fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
}

struct CaptureLayer(Captured);

impl<S: Subscriber> Layer<S> for CaptureLayer {
  fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
    let mut visitor = CategoryVisitor::default();
    event.record(&mut visitor);
    let captured = &self.0;
    captured
      .0
      .lock()
      .unwrap()
      .push((event.metadata().target().to_string(), visitor.0));
  }
}

/// 現在のスレッドの間だけ有効な収集を始める。試験は `current_thread` の実行基盤（`#[tokio::test]` の既定）で使う。
fn capture_events() -> (Captured, tracing::subscriber::DefaultGuard) {
  let captured = Captured::default();
  let subscriber = tracing_subscriber::registry().with(CaptureLayer(captured.clone()));
  (captured, tracing::subscriber::set_default(subscriber))
}

const JOURNAL_TARGET: &str = "event_store_adapter::journal";
const RETENTION_TARGET: &str = "event_store_adapter::retention";

// ---------------------------------------------------------------------------
// 取得元を渡すと、全ての要求に新しい Bearer トークンが付く
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_bigtable_attach_a_fresh_bearer_token_to_every_request() {
  let tokens = Tokens::issuing();

  let scenario = run_scenario(Some(as_source(&tokens))).await;

  let all = scenario.all();
  let values: Vec<Option<String>> = all.iter().map(|(_, authorization)| authorization.clone()).collect();
  let expected: Vec<Option<String>> = (1..=27).map(bearer).collect();
  assert_eq!(
    values, expected,
    "データ API と管理 API の全 27 要求に、送る直前に取得元が返した値が `Bearer <値>` で付き、毎回違う値になる"
  );
  assert_eq!(
    tokens.calls(),
    27,
    "取得元は、要求を送る直前に毎回呼ばれる（値を使い回さない）"
  );
  assert_eq!(
    scenario.open[..2],
    [(GetTable, bearer(5)), (GetTable, bearer(6))],
    "管理 API の GetTable（生成のときの列ファミリの照合）にも付く"
  );
  assert_eq!(
    scenario.history_state_of_seq_nr_2,
    Some(b"retired".to_vec()),
    "保持の削除の要求も実際に送られて効いている"
  );
}

#[tokio::test]
async fn should_bigtable_send_no_authorization_without_a_token_source() {
  let scenario = run_scenario(None).await;

  let all = scenario.all();
  assert_eq!(all.len(), 27, "全ての操作で要求が送られ、フックに記録される");
  assert!(
    all.iter().all(|(_, authorization)| authorization.is_none()),
    "取得元を渡さなければ authorization を付けない（空の値も付けない）: {all:?}"
  );
}

#[tokio::test]
async fn should_bigtable_send_the_same_requests_in_the_same_order_with_and_without_a_token_source() {
  let tokens = Tokens::issuing();

  let with_token_source = run_scenario(Some(as_source(&tokens))).await;
  let without_token_source = run_scenario(None).await;

  assert_eq!(
    with_token_source.kinds_by_operation(),
    expected_kinds_by_operation(),
    "取得元ありでも、送る要求の種類とその順番は変わらない"
  );
  assert_eq!(
    without_token_source.kinds_by_operation(),
    expected_kinds_by_operation(),
    "取得元なしの要求の種類とその順番"
  );
}

// ---------------------------------------------------------------------------
// 公開の生成と初期化の関数は、取得元を要求ごとに 1 回呼ぶ
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_bigtable_public_token_source_functions_call_the_source_once_per_request_for_initialize() {
  let options = options_keeping(1);
  let fixture = Fixture::new().await;
  let tokens = Tokens::issuing();

  initialize_bigtable_store_with_token_source(
    &fixture.connection().await,
    &fixture.tables,
    &options,
    as_source(&tokens),
  )
  .await
  .expect("取得元を渡して初期化できる");

  assert_eq!(
    tokens.calls(),
    4,
    "初期化の要求（設定行の読み取り 2 回、設定行の作成 2 回）ごとに 1 回"
  );
  assert!(
    fixture.raw_configuration(TableKind::Journal).await.is_some(),
    "journal の設定行ができている"
  );
  assert!(
    fixture.raw_configuration(TableKind::Head).await.is_some(),
    "head の設定行ができている"
  );
}

#[tokio::test]
async fn should_bigtable_public_token_source_functions_call_the_source_once_per_request_for_open_with_token_source() {
  let options = options_keeping(1);
  let fixture = Fixture::initialized(&options).await;
  let tokens = Tokens::issuing();

  let store = Store::open_with_token_source(
    fixture.connection().await,
    fixture.tables.clone(),
    options,
    as_source(&tokens),
  )
  .await
  .expect("取得元を渡して開ける");

  assert_eq!(
    tokens.calls(),
    4,
    "生成の要求（GetTable 2 回、設定行の読み取り 2 回）ごとに 1 回"
  );
  store
    .persist_event(event(&Id::order(), 1))
    .await
    .expect("開いたストアで追記できる");
  assert_eq!(tokens.calls(), 7, "開いた後の追記の要求（3 回）でも、取得元が呼ばれる");
}

#[tokio::test]
async fn should_bigtable_public_token_source_functions_call_the_source_once_per_request_for_open_with_serializers() {
  let options = options_keeping(1);
  let fixture = Fixture::initialized(&options).await;
  let tokens = Tokens::issuing();

  let store = Store::open_with_serializers_and_token_source(
    fixture.connection().await,
    fixture.tables.clone(),
    options,
    Arc::new(JsonEventSerializer::new()),
    Arc::new(JsonSnapshotSerializer::new()),
    as_source(&tokens),
  )
  .await
  .expect("直列化と取得元を渡して開ける");

  assert_eq!(
    tokens.calls(),
    4,
    "生成の要求（GetTable 2 回、設定行の読み取り 2 回）ごとに 1 回"
  );
  store
    .persist_event(event(&Id::order(), 1))
    .await
    .expect("開いたストアで追記できる");
  assert_eq!(tokens.calls(), 7, "開いた後の追記の要求（3 回）でも、取得元が呼ばれる");
}

// ---------------------------------------------------------------------------
// 取得元の型の性質: Arc で共有できる
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_bigtable_token_source_be_shareable_as_a_trait_object_across_tasks() {
  fn assert_send_sync<T: Send + Sync + ?Sized>() {}
  assert_send_sync::<dyn BigtableTokenSource>();
  let tokens = Tokens::issuing();
  let source = as_source(&tokens);

  let handles: Vec<_> = (0..2)
    .map(|_| {
      let source = source.clone();
      tokio::spawn(async move { source.access_token().await.expect("トークンを取得できる") })
    })
    .collect();
  let mut issued = Vec::new();
  for handle in handles {
    issued.push(handle.await.expect("タスクが終了する"));
  }

  issued.sort();
  assert_eq!(issued, vec!["token-1".to_string(), "token-2".to_string()]);
}

// ---------------------------------------------------------------------------
// 取得元が失敗すると、その要求を送らず、その操作の保存先エラーになる
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_bigtable_open_fail_with_storage_error_when_the_token_source_fails() {
  let options = options_keeping(1);
  let fixture = Fixture::initialized(&options).await;
  let tokens = Tokens::with_mode(Mode::FailFrom(1));
  let recorder = Arc::new(Recorder::default());

  let result = open_store(&fixture, &options, &tokens, &recorder).await;

  assert!(
    matches!(
      result.as_ref().err(),
      Some(EventStoreError::Storage {
        operation: StorageOperation::ReadConfiguration,
        ..
      })
    ),
    "生成は ReadConfiguration の保存先エラーになる: {:?}",
    result.err()
  );
  assert!(recorder.take().is_empty(), "失敗した要求も、その後の要求も送られない");
  assert_eq!(tokens.calls(), 1, "失敗した取得元を、ライブラリの中でやり直さない");
}

#[tokio::test]
async fn should_bigtable_initialize_fail_with_storage_error_when_the_token_source_fails() {
  let options = options_keeping(1);
  let fixture = Fixture::new().await;
  let tokens = Tokens::with_mode(Mode::FailFrom(1));
  let recorder = Arc::new(Recorder::default());

  let result = initialize_bigtable_store_with_hooks(
    &fixture.connection().await,
    &fixture.tables,
    &options,
    Some(as_source(&tokens)),
    recorder.clone(),
  )
  .await;

  assert!(
    matches!(
      &result,
      Err(EventStoreError::Storage {
        operation: StorageOperation::CreateConfiguration,
        ..
      })
    ),
    "初期化は CreateConfiguration の保存先エラーになる: {result:?}"
  );
  assert!(recorder.take().is_empty(), "失敗した要求も、その後の要求も送られない");
  assert_eq!(tokens.calls(), 1, "失敗した取得元を、ライブラリの中でやり直さない");
  assert_eq!(
    fixture.raw_configuration(TableKind::Journal).await,
    None,
    "journal に設定行が書かれていない"
  );
  assert_eq!(
    fixture.raw_configuration(TableKind::Head).await,
    None,
    "head に設定行が書かれていない"
  );
}

#[tokio::test]
async fn should_bigtable_load_events_fail_with_storage_error_when_the_token_source_fails() {
  let options = options_keeping(1);
  let fixture = Fixture::initialized(&options).await;
  let tokens = Tokens::issuing();
  let recorder = Arc::new(Recorder::default());
  let store = open_store(&fixture, &options, &tokens, &recorder)
    .await
    .expect("開ける");
  let id = Id::order();
  store.persist_event(event(&id, 1)).await.expect("追記できる");
  recorder.take();
  let before = tokens.calls();
  tokens.set_mode(Mode::FailFrom(before + 1));

  let result = store.get_events_by_id_since_seq_nr(&id, 1).await;

  assert!(
    matches!(
      result.as_ref().err(),
      Some(EventStoreError::Storage {
        operation: StorageOperation::LoadEvents,
        ..
      })
    ),
    "イベントの読み取りは LoadEvents の保存先エラーになる: {:?}",
    result.err()
  );
  assert!(recorder.take().is_empty(), "失敗した要求は送られない");
  assert_eq!(
    tokens.calls(),
    before + 1,
    "失敗した取得元を、ライブラリの中でやり直さない"
  );
}

#[tokio::test]
async fn should_bigtable_load_snapshot_fail_with_storage_error_when_the_token_source_fails() {
  let options = options_keeping(1);
  let fixture = Fixture::initialized(&options).await;
  let tokens = Tokens::issuing();
  let recorder = Arc::new(Recorder::default());
  let store = open_store(&fixture, &options, &tokens, &recorder)
    .await
    .expect("開ける");
  let id = Id::order();
  store
    .persist_event_and_snapshot(event(&id, 1), snapshot(1))
    .await
    .expect("追記できる");
  recorder.take();
  let before = tokens.calls();
  tokens.set_mode(Mode::FailFrom(before + 1));

  let result = store.get_latest_snapshot_by_id(&id).await;

  assert!(
    matches!(
      result.as_ref().err(),
      Some(EventStoreError::Storage {
        operation: StorageOperation::LoadSnapshot,
        ..
      })
    ),
    "最新のスナップショットの読み取りは LoadSnapshot の保存先エラーになる: {:?}",
    result.err()
  );
  assert!(recorder.take().is_empty(), "失敗した要求は送られない");
  assert_eq!(
    tokens.calls(),
    before + 1,
    "失敗した取得元を、ライブラリの中でやり直さない"
  );
}

// ---------------------------------------------------------------------------
// 追記: 確定の前の失敗は追記の失敗で何も書かれない。確定の後の失敗は追記の成功を変えない
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_bigtable_append_fail_without_writing_when_the_token_source_fails_before_the_commit() {
  let options = options_keeping(1);
  let fixture = Fixture::initialized(&options).await;
  let tokens = Tokens::issuing();
  let recorder = Arc::new(Recorder::default());
  let store = open_store(&fixture, &options, &tokens, &recorder)
    .await
    .expect("開ける");
  recorder.take();
  let before = tokens.calls();
  // 新しい集約への最初の追記は、head の読み取り（1 回目）、head の条件付き更新（2 回目）の順に要求を送る。
  tokens.set_mode(Mode::FailFrom(before + 2));
  let id = Id::order();

  let result = store.persist_event(event(&id, 1)).await;

  assert!(
    matches!(
      &result,
      Err(EventStoreError::Storage {
        operation: StorageOperation::Append,
        ..
      })
    ),
    "確定の前の失敗は、追記の保存先エラーになる: {result:?}"
  );
  assert_eq!(
    recorder.take(),
    vec![(ReadRows, bearer(before + 1))],
    "head の条件付き更新は送られない"
  );
  assert_eq!(
    tokens.calls(),
    before + 2,
    "失敗した取得元を、ライブラリの中でやり直さない"
  );
  let reader = fixture.open(options).await.expect("取得元なしで開ける");
  assert!(
    reader.get_events_by_id_since_seq_nr(&id, 1).await.unwrap().is_empty(),
    "何も書かれていない: イベントは空"
  );
  assert!(
    reader.get_latest_snapshot_by_id(&id).await.unwrap().is_none(),
    "何も書かれていない: スナップショットもない"
  );
}

/// 追記の確定の後に取得元が失敗した結果と、その後の観測に必要なもの。
struct AppendAfterCommit {
  fixture: Fixture,
  options: BigtableOptions,
  id: Id,
  recorder: Arc<Recorder>,
  captured: Captured,
  result: Result<(), EventStoreError>,
  _guard: tracing::subscriber::DefaultGuard,
}

/// 保持件数 1 でイベント 1・スナップショット 1 を追記した後、イベント 2・スナップショット 2 の追記の
/// `failing_call` 番目（1 から数える）の取得元の呼び出しだけを失敗させて、追記する。
///
/// この追記は次の順に要求を送る: head の読み取り（1）、直前の行の書き出し（2）とその照合の読み取り（3）、
/// head の条件付き更新（4、確定）、末尾の行の書き出し（5）、保持の走査（6）、保持の削除（7）。
async fn append_second_snapshot_failing_call(failing_call: usize) -> AppendAfterCommit {
  let (captured, guard) = capture_events();
  let options = options_keeping(1);
  let fixture = Fixture::initialized(&options).await;
  let tokens = Tokens::issuing();
  let recorder = Arc::new(Recorder::default());
  let store = open_store(&fixture, &options, &tokens, &recorder)
    .await
    .expect("開ける");
  let id = Id::order();
  store
    .persist_event_and_snapshot(event(&id, 1), snapshot(1))
    .await
    .expect("最初の追記ができる");
  recorder.take();
  tokens.set_mode(Mode::FailAt(tokens.calls() + failing_call));

  let result = store.persist_event_and_snapshot(event(&id, 2), snapshot(2)).await;

  AppendAfterCommit {
    fixture,
    options,
    id,
    recorder,
    captured,
    result,
    _guard: guard,
  }
}

#[tokio::test]
async fn should_bigtable_append_succeed_and_report_when_the_token_source_fails_after_the_commit_at_the_tail_write() {
  let appended = append_second_snapshot_failing_call(5).await;

  assert!(
    appended.result.is_ok(),
    "確定した追記を失敗として返さない: {:?}",
    appended.result
  );
  assert_eq!(
    appended.captured.categories_of(JOURNAL_TARGET),
    vec!["journal-write-failure".to_string()],
    "末尾の行の書き出しの失敗は、別の経路で知らせる"
  );
  assert!(
    appended.captured.categories_of(RETENTION_TARGET).is_empty(),
    "保持の処理は成功しているので、保持の失敗は知らせない"
  );
  assert_eq!(
    kinds_of(&appended.recorder.take()),
    vec![
      ReadRows,
      CheckAndMutateRow,
      ReadRows,
      CheckAndMutateRow,
      ReadRows,
      CheckAndMutateRow
    ],
    "末尾の行の書き出し（5 番目）だけが送られない"
  );
  let prefix = row_prefix(&appended.id.aid(), None);
  assert!(
    appended
      .fixture
      .raw_row(TableKind::Journal, &journal_key(&prefix, 2))
      .await
      .is_empty(),
    "末尾の行（seq_nr 2）は書かれていない"
  );
  let reader = appended
    .fixture
    .open(appended.options)
    .await
    .expect("取得元なしで開ける");
  assert_eq!(
    reader
      .get_events_by_id_since_seq_nr(&appended.id, 1)
      .await
      .unwrap()
      .iter()
      .map(|stored| stored.seq_nr())
      .collect::<Vec<_>>(),
    vec![1, 2],
    "確定した追記は、head の末尾のイベントとして読める"
  );
}

#[tokio::test]
async fn should_bigtable_append_succeed_and_report_when_the_token_source_fails_after_the_commit_at_the_retention_scan()
{
  let appended = append_second_snapshot_failing_call(6).await;

  assert!(
    appended.result.is_ok(),
    "確定した追記を失敗として返さない: {:?}",
    appended.result
  );
  assert_eq!(
    appended.captured.categories_of(RETENTION_TARGET),
    vec!["retention-failure".to_string()],
    "保持の走査の失敗は、別の経路で知らせる"
  );
  assert!(
    appended.captured.categories_of(JOURNAL_TARGET).is_empty(),
    "末尾の行は書けているので、journal の書き出しの失敗は知らせない"
  );
  assert_eq!(
    kinds_of(&appended.recorder.take()),
    vec![
      ReadRows,
      CheckAndMutateRow,
      ReadRows,
      CheckAndMutateRow,
      CheckAndMutateRow
    ],
    "保持の走査（6 番目）と、それに続く保持の削除は送られない"
  );
  let prefix = row_prefix(&appended.id.aid(), None);
  let row_1 = appended
    .fixture
    .raw_row(TableKind::Journal, &journal_key(&prefix, 1))
    .await;
  assert_eq!(
    only_value(&row_1, "history", "state"),
    Some(b"active".to_vec()),
    "保持の処理が進まなかったので、履歴は active のまま"
  );
  let reader = appended
    .fixture
    .open(appended.options)
    .await
    .expect("取得元なしで開ける");
  let latest = reader
    .get_latest_snapshot_by_id(&appended.id)
    .await
    .unwrap()
    .expect("スナップショットがある");
  assert_eq!(latest.head_seq_nr(), 2, "確定した追記が見える");
  assert_eq!(latest.snapshot().map(|stored| stored.seq_nr()), Some(2));
}

#[tokio::test]
async fn should_bigtable_append_succeed_and_report_when_the_token_source_fails_after_the_commit_at_the_retention_delete(
) {
  let appended = append_second_snapshot_failing_call(7).await;

  assert!(
    appended.result.is_ok(),
    "確定した追記を失敗として返さない: {:?}",
    appended.result
  );
  assert_eq!(
    appended.captured.categories_of(RETENTION_TARGET),
    vec!["retention-failure".to_string()],
    "保持の削除の失敗は、別の経路で知らせる"
  );
  assert!(
    appended.captured.categories_of(JOURNAL_TARGET).is_empty(),
    "末尾の行は書けているので、journal の書き出しの失敗は知らせない"
  );
  assert_eq!(
    kinds_of(&appended.recorder.take()),
    vec![
      ReadRows,
      CheckAndMutateRow,
      ReadRows,
      CheckAndMutateRow,
      CheckAndMutateRow,
      ReadRows
    ],
    "保持の削除（7 番目）だけが送られない"
  );
  let prefix = row_prefix(&appended.id.aid(), None);
  let row_1 = appended
    .fixture
    .raw_row(TableKind::Journal, &journal_key(&prefix, 1))
    .await;
  assert_eq!(
    only_value(&row_1, "history", "state"),
    Some(b"active".to_vec()),
    "削除の要求が送られなかったので、履歴は active のまま"
  );
  let row_2 = appended
    .fixture
    .raw_row(TableKind::Journal, &journal_key(&prefix, 2))
    .await;
  assert!(!row_2.is_empty(), "末尾の行（seq_nr 2）は書けている");
}

// ---------------------------------------------------------------------------
// メタデータの値にできないトークンは、送らずに、その操作の保存先エラーになる
// ---------------------------------------------------------------------------

#[tokio::test]
async fn should_bigtable_reject_a_token_that_cannot_be_metadata_without_sending_when_opening() {
  let options = options_keeping(1);
  let fixture = Fixture::initialized(&options).await;
  let tokens = Tokens::with_mode(Mode::InvalidFrom(1));
  let recorder = Arc::new(Recorder::default());

  let result = open_store(&fixture, &options, &tokens, &recorder).await;

  assert!(
    matches!(
      result.as_ref().err(),
      Some(EventStoreError::Storage {
        operation: StorageOperation::ReadConfiguration,
        ..
      })
    ),
    "使えない文字を含むトークンは、生成の ReadConfiguration の保存先エラーになる: {:?}",
    result.err()
  );
  assert!(recorder.take().is_empty(), "要求は送られない");
  assert_eq!(tokens.calls(), 1, "使えないトークンでも、取得元をやり直さない");
}

#[tokio::test]
async fn should_bigtable_reject_a_token_that_cannot_be_metadata_without_sending_when_appending() {
  let options = options_keeping(1);
  let fixture = Fixture::initialized(&options).await;
  let tokens = Tokens::issuing();
  let recorder = Arc::new(Recorder::default());
  let store = open_store(&fixture, &options, &tokens, &recorder)
    .await
    .expect("開ける");
  recorder.take();
  let before = tokens.calls();
  tokens.set_mode(Mode::InvalidFrom(before + 1));
  let id = Id::order();

  let result = store.persist_event(event(&id, 1)).await;

  assert!(
    matches!(
      &result,
      Err(EventStoreError::Storage {
        operation: StorageOperation::Append,
        ..
      })
    ),
    "使えない文字を含むトークンは、追記の保存先エラーになる: {result:?}"
  );
  assert!(recorder.take().is_empty(), "要求は送られない");
  assert_eq!(tokens.calls(), before + 1, "使えないトークンでも、取得元をやり直さない");
  let reader = fixture.open(options).await.expect("取得元なしで開ける");
  assert!(
    reader.get_events_by_id_since_seq_nr(&id, 1).await.unwrap().is_empty(),
    "何も書かれていない: イベントは空"
  );
  assert!(
    reader.get_latest_snapshot_by_id(&id).await.unwrap().is_none(),
    "何も書かれていない: スナップショットもない"
  );
}
