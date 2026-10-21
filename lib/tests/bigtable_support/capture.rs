//! tracing の通知の収集。試験のバイナリごとに、プロセス全体で 1 つの収集器を大域の既定として取り付ける。
//!
//! スレッドだけの既定の収集器を試験ごとに付けると、呼び出し箇所ごとの関心の記憶（プロセス全体で共有）が、
//! 並行して動く試験のあいだで食い違い、知らせが落ちることがある。そのため、収集器は 1 度だけ付け、
//! 記録は共有する。試験は、自分の集約の `aid` と target で絞り込んで確かめる。
//!
//! 記録するのは、target が `event_store_adapter::` で始まる事象だけ（通信層などの大量の事象を溜め込まないため）。

use std::sync::{Arc, Mutex, OnceLock};

use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::Layer;

/// 記録する事象の target の接頭辞。
const TARGET_PREFIX: &str = "event_store_adapter::";

/// 収集した tracing の事象 1 件（target と、フィールド `category`・`aid` の値）。
struct CapturedEvent {
  target: String,
  category: Option<String>,
  aid: Option<String>,
}

/// プロセス全体で共有する、収集した事象の記録。読んでも消さない。
#[derive(Clone, Default)]
pub struct Captured(Arc<Mutex<Vec<CapturedEvent>>>);

impl Captured {
  /// target と `aid` が一致する事象の `category` を、記録した順に返す。`category` のない事象は飛ばす。
  pub fn categories_of(&self, target: &str, aid: &str) -> Vec<String> {
    self
      .0
      .lock()
      .unwrap()
      .iter()
      .filter(|event| event.target == target && event.aid.as_deref() == Some(aid))
      .filter_map(|event| event.category.clone())
      .collect()
  }
}

#[derive(Default)]
struct FieldsVisitor {
  category: Option<String>,
  aid: Option<String>,
}

impl Visit for FieldsVisitor {
  fn record_str(&mut self, field: &Field, value: &str) {
    if field.name() == "category" {
      self.category = Some(value.to_string());
    }
  }

  /// `aid = %値` のように表示の形で渡された値は、`record_str` ではなくここへ届く。
  fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
    if field.name() == "aid" {
      self.aid = Some(format!("{value:?}"));
    }
  }
}

struct CaptureLayer(Captured);

impl<S: Subscriber> Layer<S> for CaptureLayer {
  fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
    let target = event.metadata().target();
    if !target.starts_with(TARGET_PREFIX) {
      return;
    }
    let mut fields = FieldsVisitor::default();
    event.record(&mut fields);
    let captured = &self.0;
    captured.0.lock().unwrap().push(CapturedEvent {
      target: target.to_string(),
      category: fields.category,
      aid: fields.aid,
    });
  }
}

/// プロセス全体で 1 つの収集を返す。初めて呼ばれたときに、収集器を大域の既定として取り付ける。
///
/// 通知を出しうる処理より前に、必ず呼ぶ。取り付けが終わるまで、ほかの呼び出しは待たされるので、
/// 通知の呼び出し箇所は、取り付けの後にしか初めて登録されない。
pub fn captured_events() -> &'static Captured {
  static CAPTURED: OnceLock<Captured> = OnceLock::new();
  CAPTURED.get_or_init(|| {
    let captured = Captured::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(CaptureLayer(captured.clone())))
      .expect("tracing の大域の既定の収集器を取り付けられる");
    captured
  })
}
