use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// FR1.1-1.4 / FR2.1: journal / snapshot に対応する封筒型。メタデータと payload を分離して運搬する。
// ADR-009: 構造体定義に trait 境界を置かない（derive は条件付き実装のみを生成し、構築時の要求を増やさない）。

/// ジャーナル 1 行に対応するイベント封筒を表す。
///
/// メタデータ 4 点（aggregate_id / seq_nr / occurred_at / manifest）と純ドメイン内容の
/// payload を運搬する。ライブラリは封筒を透過運搬するだけで、値を解釈しない。
///
/// # seq_nr 契約（FR3.3 / FR3.4 / BR6.1）
///
/// - seq_nr は 1 始まりで、同一ストリーム内で連続していることを利用者（ドメイン側）が保証する
/// - 採番はドメイン側の責務であり、ストアは採番しない。封筒の seq_nr がそのまま保存される
/// - ライブラリは連続性を検証しない。重複は楽観ロック（CAS / 一意制約）が拒否し、
///   飛び番は検出されずそのまま書き込まれる（利用者責務）
/// - seq_nr == 0 の封筒は書込時に `EventStoreWriteError::ContractViolation` で拒否される（BR1.4）
///
/// # 拡張（FR1.3 / BR1.5）
///
/// フィールドは非公開で、読取はアクセサ、構築は [`EventEnvelope::new`] +
/// `with_*` ビルダーに限定する。将来のフィールド追加は既定値付きの `with_*` を
/// 増やす形で行えるため、既存利用コードを壊さない（非破壊拡張）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventEnvelope<AID, P> {
  aggregate_id: AID,
  seq_nr: usize,
  occurred_at: DateTime<Utc>,
  manifest: String,
  payload: P,
}

impl<AID, P> EventEnvelope<AID, P> {
  /// イベント封筒を構築する。
  ///
  /// BR1.1: 必須メタデータ（aggregate_id / seq_nr / occurred_at）と payload は
  /// 引数として強制され、欠落はコンパイル不能。manifest のみ省略可能で、
  /// 省略時は空文字列となる（FR1.2）。
  pub fn new(aggregate_id: AID, seq_nr: usize, occurred_at: DateTime<Utc>, payload: P) -> Self {
    Self {
      aggregate_id,
      seq_nr,
      occurred_at,
      manifest: String::new(),
      payload,
    }
  }

  /// manifest を設定した封筒を返す。
  ///
  /// BR1.2 / FR1.2: manifest は利用者供給・自由形式であり、ライブラリは値を解釈せず
  /// 運搬のみを行う（保存した値が読出しで同値のまま返る）。
  pub fn with_manifest(mut self, manifest: impl Into<String>) -> Self {
    self.manifest = manifest.into();
    self
  }

  /// 集約 ID を返す。
  pub fn aggregate_id(&self) -> &AID {
    &self.aggregate_id
  }

  /// シーケンス番号（1 始まり・ドメイン採番）を返す。
  pub fn seq_nr(&self) -> usize {
    self.seq_nr
  }

  /// ドメイン供給の発生時刻を返す。
  ///
  /// BR1.3 / FR1.4: この値はストア刻印に置換されず、保存・読出しの全経路で
  /// 供給値のまま維持される。
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
  ///
  /// payload が `Clone` を実装しない型でもリプレイ（W4）で畳み込めるようにする
  /// 所有権移動のアクセサ（BR1.6 の最小境界を崩さないための出口）。
  pub fn into_payload(self) -> P {
    self.payload
  }
}

/// スナップショット 1 件に対応する封筒を表す。
///
/// 集約の純ドメイン状態（aggregate）と、反映済みイベント位置（seq_nr）・
/// 楽観ロック版数（version）を運搬する。FR2.1: 旧 `event_store_backend` の
/// 内部型からの公開昇格。FR2.2 / BR3.1: 読取 API はこの封筒を返し、
/// seq_nr / version を境界で破棄しない。
///
/// version の正は常にストレージ列側にあり、読取時に payload から補正されない
/// （BR2.5 / FR4.3）。利用者は `seq_nr()` をリプレイ開始点、`version()` を
/// 次回書込の expected_version として使う（W4）。
#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotEnvelope<A> {
  aggregate: A,
  seq_nr: usize,
  version: usize,
}

impl<A> SnapshotEnvelope<A> {
  /// スナップショット封筒を構築する。
  ///
  /// BR1.5: フィールドは非公開のため、将来のフィールド追加は非破壊で行える。
  pub fn new(aggregate: A, seq_nr: usize, version: usize) -> Self {
    Self {
      aggregate,
      seq_nr,
      version,
    }
  }

  /// 集約の純ドメイン状態への参照を返す。
  pub fn aggregate(&self) -> &A {
    &self.aggregate
  }

  /// このスナップショットが反映済みのイベント封筒の seq_nr を返す（リプレイ開始点）。
  pub fn seq_nr(&self) -> usize {
    self.seq_nr
  }

  /// 楽観ロックの版数を返す（1 始まり。正は列側 — BR2.5）。
  pub fn version(&self) -> usize {
    self.version
  }

  /// 封筒を消費して集約状態の所有権を返す。
  ///
  /// 集約状態が `Clone` を実装しない型でもリプレイの初期値に使えるようにする
  /// 所有権移動のアクセサ。
  pub fn into_aggregate(self) -> A {
    self.aggregate
  }
}
