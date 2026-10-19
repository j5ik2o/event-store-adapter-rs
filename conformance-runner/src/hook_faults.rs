//! メモリと SQLite が共有する、障害の差し込みと保持履歴・観測の処理。
//!
//! 両保存先は、同じ段階を同じ誤りの分類へ対応付け、`retention-query` の応答計画（`history_pages`）を
//! 論理的な履歴のフックへ対応付ける（`conformance/README.md`）。同じ契約を 2 か所に書かないよう、ここに置く。

use std::sync::Mutex;

use event_store_adapter_rs::{
  aggregate_id::AidString,
  error::{EventStoreError, SerializationPhase, StorageOperation},
};
use serde_json::{json, Value};

use crate::{
  case::{required, seq, CaseId, ComparisonFailure},
  compare::json_equal,
  fault::{FaultKind, FaultPlan, Injection, OperationFaults, Phase},
};

/// 宣言された障害がすべて、フックと直列化の差し込みで表せるかを返す。
///
/// ストアの生成（操作 0）の障害は表せない。表せない障害があるケースは、実行せずに未検証にする。
pub(crate) fn supports_all(plan: &FaultPlan) -> bool {
  plan.faults().iter().all(|fault| {
    let supported = match fault.phase {
      Phase::SerializeEvent | Phase::SerializeSnapshot | Phase::DeserializeEvent | Phase::DeserializeSnapshot => {
        fault.kind == FaultKind::SerializationError && fault.injection == Injection::ReplaceRequest
      }
      Phase::Commit | Phase::ReadEvents | Phase::ReadSnapshot | Phase::RetentionDelete => {
        fault.kind == FaultKind::StorageError && fault.injection == Injection::ReplaceRequest
      }
      Phase::RetentionQuery => {
        (fault.kind == FaultKind::StorageError && fault.injection == Injection::ReplaceRequest)
          || (fault.kind == FaultKind::SdkResponse
            && fault.injection == Injection::ReplaceResponse
            && fault.details.get("history_pages").is_some())
      }
      _ => false,
    };
    fault.operation != 0 && supported
  })
}

/// 操作ごとの障害の適用回数と、応答計画の誤りを持ち、フックと直列化から呼ばれる。
#[derive(Debug)]
pub(crate) struct HookFaults {
  faults: Mutex<OperationFaults>,
  invalid_response: Mutex<Option<String>>,
}

impl HookFaults {
  /// 最初の操作の障害の計数から作る。
  pub(crate) fn new(faults: OperationFaults) -> Self {
    Self {
      faults: Mutex::new(faults),
      invalid_response: Mutex::new(None),
    }
  }

  /// 操作の障害の計数を差し替え、それまでの計数を返す。
  pub(crate) fn replace(&self, faults: OperationFaults) -> OperationFaults {
    std::mem::replace(&mut *self.faults.lock().expect("障害消費のロック"), faults)
  }

  /// 応答計画が不正だったときの説明を取り出す。なければ `None` を返す。
  pub(crate) fn take_invalid_response(&self) -> Option<String> {
    self.invalid_response.lock().expect("応答検査のロック").take()
  }

  /// 段階 `phase` に適用できる障害があれば、その段階の誤りを返す。なければ成功を返す。
  pub(crate) fn inject(&self, phase: Phase) -> Result<(), EventStoreError> {
    let mut faults = self.faults.lock().expect("障害消費のロック");
    let Some(fault) = faults.start_application(phase) else {
      return Ok(());
    };
    let source = Box::new(std::io::Error::other(
      fault
        .details
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("injected fault")
        .to_owned(),
    ));
    Err(match phase {
      Phase::SerializeEvent => EventStoreError::Serialization {
        phase: SerializationPhase::SerializeEvent,
        source,
      },
      Phase::SerializeSnapshot => EventStoreError::Serialization {
        phase: SerializationPhase::SerializeSnapshot,
        source,
      },
      Phase::DeserializeEvent => EventStoreError::Serialization {
        phase: SerializationPhase::DeserializeEvent,
        source,
      },
      Phase::DeserializeSnapshot => EventStoreError::Serialization {
        phase: SerializationPhase::DeserializeSnapshot,
        source,
      },
      Phase::Commit | Phase::RetentionDelete => EventStoreError::Storage {
        operation: StorageOperation::Append,
        source,
      },
      Phase::ReadEvents => EventStoreError::Storage {
        operation: StorageOperation::LoadEvents,
        source,
      },
      Phase::ReadSnapshot => EventStoreError::Storage {
        operation: StorageOperation::LoadSnapshot,
        source,
      },
      _ => unreachable!("対応する段階は実行前に検査する"),
    })
  }

  /// 保持処理が見る履歴を返す。応答計画があれば、その `history_pages` の履歴に差し替える。
  ///
  /// 応答計画が不正なら、その説明を記録して保存先の誤りを返す。
  pub(crate) fn retention_visible_history(
    &self,
    history: &[u64],
    just_written: Option<u64>,
  ) -> Result<Vec<u64>, EventStoreError> {
    let fault = self
      .faults
      .lock()
      .expect("障害消費のロック")
      .start_application(Phase::RetentionQuery)
      .cloned();
    let Some(fault) = fault else {
      return Ok(history.to_vec());
    };
    let result = if fault.kind == FaultKind::SdkResponse {
      visible_history(&fault.details, history, just_written)
    } else {
      return Err(EventStoreError::Storage {
        operation: StorageOperation::Append,
        source: Box::new(std::io::Error::other(
          fault.details["message"].as_str().unwrap_or("injected fault").to_owned(),
        )),
      });
    };
    result.map_err(|message| {
      *self.invalid_response.lock().expect("応答検査のロック") = Some(message.clone());
      EventStoreError::Storage {
        operation: StorageOperation::Append,
        source: Box::new(std::io::Error::other(message)),
      }
    })
  }
}

fn visible_history(details: &Value, history: &[u64], just_written: Option<u64>) -> Result<Vec<u64>, String> {
  let mut visible = Vec::new();
  for page in required(details, "history_pages")?
    .as_array()
    .ok_or("history_pages が配列ではない")?
  {
    for value in page.as_array().ok_or("履歴ページが配列ではない")? {
      let seq_nr = seq(value)?;
      if !history.contains(&seq_nr) {
        return Err(format!("応答計画の履歴 {seq_nr} が対象集約の保存済み履歴にない"));
      }
      visible.push(seq_nr);
    }
  }
  if details.get("omit_just_written_history").and_then(Value::as_bool) == Some(true)
    && just_written.is_some_and(|seq_nr| visible.contains(&seq_nr))
  {
    return Err("省略指定の応答計画に今書いた履歴が含まれる".to_owned());
  }
  Ok(visible)
}

/// 操作の `observe`（`notifications`・`history`）を検査し、観測した値を返す。
///
/// `history_of` は、集約の保存済みの履歴の seq_nr を昇順で返す。印付き（`marked`）の履歴は、
/// メモリと SQLite の保存先にない。
pub(crate) fn observe(
  step: &Value,
  id: Option<&CaseId>,
  notifications: &[String],
  history_of: impl Fn(&AidString) -> Result<Vec<u64>, EventStoreError>,
) -> Result<Value, ComparisonFailure> {
  let mut observed = serde_json::Map::new();
  let Some(observe) = step.get("observe") else {
    return Ok(Value::Object(observed));
  };
  for (key, value) in observe.as_object().ok_or("observe がオブジェクトではない")? {
    if key == "notifications" {
      let actual = json!(notifications);
      observed.insert("notifications".into(), actual.clone());
      if !json_equal(value, &actual) {
        return Err(ComparisonFailure {
          detail: "通知が一致しない".to_owned(),
          expected: Some(value.clone()),
          actual: Some(actual),
        });
      }
      continue;
    }
    if key != "history" {
      return Err(format!("未対応の観測: {key}").into());
    }
    let aid = AidString::from_aggregate_id(id.ok_or("観測する集約 ID がない")?).map_err(|e| e.to_string())?;
    let history = history_of(&aid).map_err(|e| e.to_string())?;
    observed.insert("history".into(), json!({"active":history,"marked":[]}));
    let active = required(value, "active")?
      .as_array()
      .ok_or("active が配列ではない")?
      .iter()
      .map(seq)
      .collect::<Result<Vec<_>, _>>()?;
    let marked = required(value, "marked")?.as_array().ok_or("marked が配列ではない")?;
    let absent = required(value, "absent")?
      .as_array()
      .ok_or("absent が配列ではない")?
      .iter()
      .map(seq)
      .collect::<Result<Vec<_>, _>>()?;
    let mut expected = active;
    expected.sort_unstable();
    if history != expected || !marked.is_empty() || absent.iter().any(|n| history.contains(n)) {
      return Err(ComparisonFailure {
        detail: "履歴が一致しない".to_owned(),
        expected: Some(value.clone()),
        actual: Some(json!({"active": history, "marked": []})),
      });
    }
  }
  Ok(Value::Object(observed))
}
