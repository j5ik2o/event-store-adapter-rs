use std::collections::{BTreeSet, HashMap};
use std::sync::{
  atomic::{AtomicU64, Ordering},
  Arc, Mutex, OnceLock,
};

use tracing::{
  field::{Field, Visit},
  span::{Attributes, Id},
  Event, Span, Subscriber,
};
use tracing_subscriber::{layer::Context, prelude::*, registry::LookupSpan, Layer};

#[cfg(test)]
#[path = "notifications_test.rs"]
mod tests;

type Collected = Arc<Mutex<HashMap<u64, BTreeSet<String>>>>;
static COLLECTOR: OnceLock<Result<Collected, String>> = OnceLock::new();
static NEXT_OPERATION: AtomicU64 = AtomicU64::new(1);

#[derive(Default)]
struct Fields {
  collection_id: Option<u64>,
  category: Option<String>,
}
impl Visit for Fields {
  fn record_u64(&mut self, field: &Field, value: u64) {
    if field.name() == "collection_id" {
      self.collection_id = Some(value);
    }
  }

  fn record_str(&mut self, field: &Field, value: &str) {
    if field.name() == "category" {
      self.category = Some(value.to_owned());
    }
  }

  fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
}

struct Collector(Collected);
struct CollectionId(u64);
impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Collector {
  fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
    if attrs.metadata().target() != "event_store_adapter_conformance::operation" {
      return;
    }
    let mut fields = Fields::default();
    attrs.record(&mut fields);
    if let (Some(collection_id), Some(span)) = (fields.collection_id, ctx.span(id)) {
      span.extensions_mut().insert(CollectionId(collection_id));
    }
  }

  fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
    if event.metadata().target() != "event_store_adapter::retention"
      || *event.metadata().level() != tracing::Level::WARN
    {
      return;
    }
    let mut fields = Fields::default();
    event.record(&mut fields);
    if fields.category.as_deref() != Some("retention-failure") {
      return;
    }
    let Some(mut scope) = ctx.event_scope(event) else {
      return;
    };
    let collection_id = scope.find_map(|span| span.extensions().get::<CollectionId>().map(|id| id.0));
    if let Some(collection_id) = collection_id {
      if let Some(notifications) = self.0.lock().expect("通知収集のロック").get_mut(&collection_id) {
        notifications.insert(fields.category.expect("分類を確認済み"));
      }
    }
  }
}

pub(crate) struct OperationNotifications {
  collection_id: u64,
  collected: Collected,
  span: Span,
}
impl OperationNotifications {
  pub(crate) fn begin(case_id: &str, operation: u32) -> Result<Self, String> {
    let collected = COLLECTOR
      .get_or_init(|| {
        let collected = Arc::new(Mutex::new(HashMap::new()));
        tracing::subscriber::set_global_default(tracing_subscriber::registry().with(Collector(collected.clone())))
          .map_err(|e| format!("通知収集の購読者を登録できない: {e}"))?;
        Ok(collected)
      })
      .as_ref()
      .map_err(Clone::clone)?
      .clone();
    let collection_id = NEXT_OPERATION.fetch_add(1, Ordering::Relaxed);
    collected
      .lock()
      .expect("通知収集のロック")
      .insert(collection_id, BTreeSet::new());
    let span = tracing::info_span!(target: "event_store_adapter_conformance::operation", "operation", case_id, operation, collection_id);
    Ok(Self {
      collection_id,
      collected,
      span,
    })
  }

  pub(crate) fn span(&self) -> Span {
    self.span.clone()
  }

  pub(crate) fn finish(self) -> Vec<String> {
    self
      .collected
      .lock()
      .expect("通知収集のロック")
      .remove(&self.collection_id)
      .expect("操作の収集状態")
      .into_iter()
      .collect()
  }
}
impl Drop for OperationNotifications {
  fn drop(&mut self) {
    self
      .collected
      .lock()
      .expect("通知収集のロック")
      .remove(&self.collection_id);
  }
}
