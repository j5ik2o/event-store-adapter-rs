//! 要求の検査（`observe.requests`）の、条件の語の収集。

use std::collections::BTreeSet;

use serde_json::Value;

/// 実装済みの条件の語。語は、その語を使うケースを通す変更で足す（設計 5.5.1 節の表）。
pub const IMPLEMENTED_CONSTRAINT_WORDS: &[&str] = &[];

fn collect_words(observe: Option<&Value>, words: &mut BTreeSet<String>) {
  let Some(requests) = observe
    .and_then(|observe| observe.get("requests"))
    .and_then(Value::as_array)
  else {
    return;
  };
  for request in requests {
    if let Some(constraints) = request.get("constraints").and_then(Value::as_object) {
      words.extend(constraints.keys().cloned());
    }
  }
}

/// ケースの要求の検査が使う条件の語を集める。
///
/// 集める対象は、`/initialization/observe/requests/*/constraints` と
/// `/steps/*/observe/requests/*/constraints` のキーだけ。要求の検査の外にある同じ名前は数えない。
pub fn constraint_words(case: &Value) -> BTreeSet<String> {
  let mut words = BTreeSet::new();
  collect_words(case.pointer("/initialization/observe"), &mut words);
  if let Some(steps) = case.get("steps").and_then(Value::as_array) {
    for step in steps {
      collect_words(step.get("observe"), &mut words);
    }
  }
  words
}

/// ケースが使う条件の語のうち、実行器が実装していないものを返す。
pub fn unimplemented_constraint_words(case: &Value) -> BTreeSet<String> {
  constraint_words(case)
    .into_iter()
    .filter(|word| !IMPLEMENTED_CONSTRAINT_WORDS.contains(&word.as_str()))
    .collect()
}
