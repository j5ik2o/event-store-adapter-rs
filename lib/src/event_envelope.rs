use chrono::{DateTime, Utc};

use crate::seq_nr::SeqNr;

/// 1 件のイベントを運ぶ封筒。ライブラリは manifest と payload を解釈しない（T-4）。
/// フィールドは非公開。要素の追加が既存の利用コードを壊さない（T-5）。
#[derive(Debug, Clone, PartialEq)]
pub struct EventEnvelope<AID, P> {
  aggregate_id: AID,
  seq_nr: SeqNr,
  occurred_at: DateTime<Utc>,
  manifest: String,
  payload: P,
}

impl<AID, P> EventEnvelope<AID, P> {
  /// aggregate_id・seq_nr・occurred_at・payload は必須（T-2）。manifest は空文字列になる。
  pub fn new(aggregate_id: AID, seq_nr: SeqNr, occurred_at: DateTime<Utc>, payload: P) -> Self {
    Self {
      aggregate_id,
      seq_nr,
      occurred_at,
      manifest: String::new(),
      payload,
    }
  }

  /// manifest を設定した封筒を返す。ライブラリは値を解釈しない。
  pub fn with_manifest(mut self, manifest: impl Into<String>) -> Self {
    self.manifest = manifest.into();
    self
  }

  /// 集約 ID を返す。
  pub fn aggregate_id(&self) -> &AID {
    &self.aggregate_id
  }

  /// 通し番号を返す。
  pub fn seq_nr(&self) -> SeqNr {
    self.seq_nr
  }

  /// 発生時刻を返す。
  pub fn occurred_at(&self) -> &DateTime<Utc> {
    &self.occurred_at
  }

  /// manifest を返す（未指定の場合は空文字列）。
  pub fn manifest(&self) -> &str {
    &self.manifest
  }

  /// payload への参照を返す。
  pub fn payload(&self) -> &P {
    &self.payload
  }

  /// 封筒を消費して payload の所有権を返す。
  pub fn into_payload(self) -> P {
    self.payload
  }
}

/// 1 件のスナップショットを運ぶ封筒。書き込みと読み取りで同じ型を使う。
/// ヘッドの seq_nr は含めない（ADR-0002）。version は持たない。
#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotEnvelope<A> {
  aggregate: A,
  seq_nr: SeqNr,
  manifest: String,
}

impl<A> SnapshotEnvelope<A> {
  /// aggregate と seq_nr は必須（T-10）。manifest は空文字列になる。
  pub fn new(aggregate: A, seq_nr: SeqNr) -> Self {
    Self {
      aggregate,
      seq_nr,
      manifest: String::new(),
    }
  }

  /// manifest を設定した封筒を返す。ライブラリは値を解釈しない。
  pub fn with_manifest(mut self, manifest: impl Into<String>) -> Self {
    self.manifest = manifest.into();
    self
  }

  /// 集約状態への参照を返す。
  pub fn aggregate(&self) -> &A {
    &self.aggregate
  }

  /// 反映済みの seq_nr を返す。
  pub fn seq_nr(&self) -> SeqNr {
    self.seq_nr
  }

  /// manifest を返す（未指定の場合は空文字列）。
  pub fn manifest(&self) -> &str {
    &self.manifest
  }

  /// 封筒を消費して集約状態の所有権を返す。
  pub fn into_aggregate(self) -> A {
    self.aggregate
  }
}

/// 最新スナップショットの読み取り結果。スナップショット封筒（なくてもよい）と、
/// 読み取り時点のヘッドの seq_nr の組（R-2）。
#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotRead<A> {
  snapshot: Option<SnapshotEnvelope<A>>,
  head_seq_nr: SeqNr,
}

impl<A> SnapshotRead<A> {
  /// スナップショット封筒（なくてもよい）と、読み取り時点のヘッドの seq_nr から作る。
  pub fn new(snapshot: Option<SnapshotEnvelope<A>>, head_seq_nr: SeqNr) -> Self {
    Self { snapshot, head_seq_nr }
  }

  /// スナップショット封筒への参照を返す。
  pub fn snapshot(&self) -> Option<&SnapshotEnvelope<A>> {
    self.snapshot.as_ref()
  }

  /// 読み取り時点のヘッドの seq_nr を返す。
  pub fn head_seq_nr(&self) -> SeqNr {
    self.head_seq_nr
  }

  /// 封筒とヘッドの seq_nr に分ける。
  pub fn into_parts(self) -> (Option<SnapshotEnvelope<A>>, SeqNr) {
    (self.snapshot, self.head_seq_nr)
  }
}
