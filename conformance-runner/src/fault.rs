//! 障害の宣言の解釈（登録）と、操作ごとの適用回数の数え方。
//!
//! 「1 回の適用」が何を指すかは、差し込みの種類と、呼ぶ側の保存先が決める。例えば `history_pages` は、
//! 最初の `Query` で適用が始まり、ページ列を返し終えるまでを 1 回の適用と数える。この数え方は、保存先に
//! つながる後続の変更で、呼ぶ側が `start_application` を呼ぶ単位として実装する。

use serde_json::Value;

use crate::number::to_integer;

/// 障害を差し込む段階を表す。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Phase {
  SerializeEvent,
  SerializeSnapshot,
  DeserializeEvent,
  DeserializeSnapshot,
  Commit,
  ReadEvents,
  ReadSnapshot,
  RetentionQuery,
  RetentionDelete,
  RetentionMark,
  ConfigurationRead,
  ConfigurationCreate,
}

impl Phase {
  /// データの文字列から段階を作る。知らない文字列は `None` を返す。
  pub fn parse(value: &str) -> Option<Phase> {
    match value {
      "serialize-event" => Some(Phase::SerializeEvent),
      "serialize-snapshot" => Some(Phase::SerializeSnapshot),
      "deserialize-event" => Some(Phase::DeserializeEvent),
      "deserialize-snapshot" => Some(Phase::DeserializeSnapshot),
      "commit" => Some(Phase::Commit),
      "read-events" => Some(Phase::ReadEvents),
      "read-snapshot" => Some(Phase::ReadSnapshot),
      "retention-query" => Some(Phase::RetentionQuery),
      "retention-delete" => Some(Phase::RetentionDelete),
      "retention-mark" => Some(Phase::RetentionMark),
      "configuration-read" => Some(Phase::ConfigurationRead),
      "configuration-create" => Some(Phase::ConfigurationCreate),
      _ => None,
    }
  }
}

/// 障害の種類を表す。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultKind {
  StorageError,
  SerializationError,
  SdkError,
  SdkResponse,
  ReadInterleave,
}

impl FaultKind {
  /// データの文字列から種類を作る。知らない文字列は `None` を返す。
  pub fn parse(value: &str) -> Option<FaultKind> {
    match value {
      "storage-error" => Some(FaultKind::StorageError),
      "serialization-error" => Some(FaultKind::SerializationError),
      "sdk-error" => Some(FaultKind::SdkError),
      "sdk-response" => Some(FaultKind::SdkResponse),
      "read-interleave" => Some(FaultKind::ReadInterleave),
      _ => None,
    }
  }
}

/// 差し込みの方式を表す。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Injection {
  ReplaceRequest,
  ReplaceResponse,
}

impl Injection {
  /// データの文字列から方式を作る。知らない文字列は `None` を返す。
  pub fn parse(value: &str) -> Option<Injection> {
    match value {
      "replace-request" => Some(Injection::ReplaceRequest),
      "replace-response" => Some(Injection::ReplaceResponse),
      _ => None,
    }
  }
}

/// 障害を適用する回数の宣言を表す。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "mode", rename_all = "kebab-case")]
pub enum Repeat {
  Count { count: u32 },
  UntilOperationFinishes,
}

/// 登録した障害 1 つを表す。`index` は、ケースの `faults` 配列の位置。
#[derive(Debug, Clone)]
pub struct Fault {
  pub index: usize,
  pub operation: u32,
  pub phase: Phase,
  pub kind: FaultKind,
  pub injection: Injection,
  pub repeat: Repeat,
  pub details: Value,
}

/// 障害の宣言の誤りを表す。
#[derive(Debug, thiserror::Error)]
#[error("faults[{index}]: {message}")]
pub struct FaultError {
  pub index: usize,
  pub message: String,
}

fn fault_error(index: usize, message: &str) -> FaultError {
  FaultError {
    index,
    message: message.to_string(),
  }
}

/// ケースの障害の宣言を、検査して登録した計画を表す。
pub struct FaultPlan {
  faults: Vec<Fault>,
}

impl FaultPlan {
  /// ケースの `faults` を検査して登録する。`faults` がなければ空の計画を返す。
  ///
  /// `operation` は 0（ストアの生成）以上で手順の数以下、`repeat` の `count` は 1 以上でなければならない。
  pub fn register(case: &Value) -> Result<FaultPlan, FaultError> {
    let Some(declared) = case.get("faults").and_then(Value::as_array) else {
      return Ok(FaultPlan { faults: Vec::new() });
    };
    let steps = case.get("steps").and_then(Value::as_array).map_or(0, Vec::len);
    let mut faults = Vec::with_capacity(declared.len());
    for (index, fault) in declared.iter().enumerate() {
      let operation = fault
        .get("operation")
        .and_then(to_integer)
        .and_then(|operation| u32::try_from(operation).ok())
        .filter(|operation| usize::try_from(*operation).is_ok_and(|operation| operation <= steps))
        .ok_or_else(|| fault_error(index, "operation が 0 以上で手順の数以下の整数ではない"))?;
      let phase = fault
        .get("phase")
        .and_then(Value::as_str)
        .and_then(Phase::parse)
        .ok_or_else(|| fault_error(index, "phase が知らない段階"))?;
      let kind = fault
        .get("kind")
        .and_then(Value::as_str)
        .and_then(FaultKind::parse)
        .ok_or_else(|| fault_error(index, "kind が知らない種類"))?;
      let injection = fault
        .get("injection")
        .and_then(Value::as_str)
        .and_then(Injection::parse)
        .ok_or_else(|| fault_error(index, "injection が知らない方式"))?;
      let repeat = match fault.pointer("/repeat/mode").and_then(Value::as_str) {
        Some("count") => {
          let count = fault
            .pointer("/repeat/count")
            .and_then(to_integer)
            .and_then(|count| u32::try_from(count).ok())
            .filter(|count| *count >= 1)
            .ok_or_else(|| fault_error(index, "repeat.count が 1 以上の整数ではない"))?;
          Repeat::Count { count }
        }
        Some("until-operation-finishes") => Repeat::UntilOperationFinishes,
        _ => {
          return Err(fault_error(
            index,
            "repeat.mode が count か until-operation-finishes ではない",
          ))
        }
      };
      let details = fault
        .get("details")
        .filter(|details| details.is_object())
        .ok_or_else(|| fault_error(index, "details がオブジェクトではない"))?
        .clone();
      faults.push(Fault {
        index,
        operation,
        phase,
        kind,
        injection,
        repeat,
        details,
      });
    }
    Ok(FaultPlan { faults })
  }

  /// 登録した障害を、宣言の順に返す。
  pub fn faults(&self) -> &[Fault] {
    &self.faults
  }

  /// 操作 `operation`（0 はストアの生成）の中で数える、障害と適用回数の組を作る。
  ///
  /// ほかの操作の障害は含めない。適用回数は、操作ごとに 0 から数え始める。
  pub fn begin_operation(&self, operation: u32) -> OperationFaults {
    OperationFaults {
      entries: self
        .faults
        .iter()
        .filter(|fault| fault.operation == operation)
        .map(|fault| (fault.clone(), 0))
        .collect(),
    }
  }
}

/// 1 つの操作の中の、障害ごとの適用回数を表す。操作が終わったら `finish` で消費する。
#[derive(Debug)]
pub struct OperationFaults {
  entries: Vec<(Fault, u32)>,
}

fn is_fired(fault: &Fault, applied: u32) -> bool {
  match fault.repeat {
    Repeat::Count { count } => applied >= count,
    Repeat::UntilOperationFinishes => applied >= 1,
  }
}

impl OperationFaults {
  /// 段階 `phase` への適用を 1 回始め、適用する障害を返す。
  ///
  /// 同じ段階の障害は、配列の順に、回数を使い切ってから次を使う。`until-operation-finishes` は使い切り
  /// にならない。適用できる障害がなければ `None` を返す。
  pub fn start_application(&mut self, phase: Phase) -> Option<&Fault> {
    let (fault, applied) = self.entries.iter_mut().find(|(fault, applied)| {
      fault.phase == phase
        && match fault.repeat {
          Repeat::Count { count } => *applied < count,
          Repeat::UntilOperationFinishes => true,
        }
    })?;
    *applied += 1;
    Some(fault)
  }

  /// 操作の終わりに、発火しなかった障害を返す。すべて発火していれば `Ok` を返す。
  ///
  /// `Count` は宣言した回数の適用がすべて始まったとき、`UntilOperationFinishes` は 1 回以上適用できたとき、
  /// 発火と数える。
  pub fn finish(self) -> Result<(), Vec<UnfiredFault>> {
    let unfired: Vec<UnfiredFault> = self
      .entries
      .into_iter()
      .filter(|(fault, applied)| !is_fired(fault, *applied))
      .map(|(fault, applied)| UnfiredFault {
        index: fault.index,
        operation: fault.operation,
        phase: fault.phase,
        declared: fault.repeat,
        applied,
      })
      .collect();
    if unfired.is_empty() {
      Ok(())
    } else {
      Err(unfired)
    }
  }
}

/// 発火しなかった障害と、宣言した回数・適用できた回数を表す。ケースは `failed` になる。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct UnfiredFault {
  pub index: usize,
  pub operation: u32,
  pub phase: Phase,
  pub declared: Repeat,
  pub applied: u32,
}
