# 次のメジャー版（4.0.0）の設計

この文書は、`event-store-adapter-rs` の次のメジャー版の設計を定める。新しい共通契約に合わせて書き直す。今回はコードを書かない。この文書をコーディネーターがレビューした。判断が要る点は、9 章のとおり決めた（2026-10-06）。実装の作業には、この文書を使う。

## 0. この文書の読み方

### 0.1 規範と出典

- 規範は、ハブのリポジトリ（`j5ik2o/event-store-adapter`）の次の資料である。この文書は規則を変えず、足さない。
  - 共通契約（`docs/spec/core-contract.md`）
  - DynamoDB のプロファイル（`docs/spec/storage/dynamodb.md`）
  - メモリのプロファイル（`docs/spec/storage/memory.md`）
  - 実装計画（`docs/plan/implementation-plan.md`）
  - 決定記録（`docs/adr/`）
- 適合テストデータは、このリポジトリの `conformance/`（版 1.0.0）である。読み方は `conformance/README.md` にある。
- 規則番号は、上の資料の番号をそのまま使う。次の略記を使う。
  - `T-`・`H-`・`W-`・`R-`・`S-`・`E-`・`F-`・`SP-`: 共通契約
  - `D-`・`DY-`: DynamoDB のプロファイル
  - `MEM-`: メモリのプロファイル
  - `IP-`: 実装計画（`IP 3` は実装計画の 3 章）
  - `CR`: `conformance/README.md`
  - `P-`: 仕様の合意記録の番号。仕様の本文に出てくるものだけを引用する。
- 「現行」は、このリポジトリの 3.0.6（`lib/Cargo.toml`）を指す。

### 0.1.1 用語

初めて出る専門の語は、次のように読む。

| 語 | 意味 |
|:--|:--|
| aid（集約 ID の文字列） | 集約の識別子の文字列。`${型名}-${値}` の形で、ライブラリが組み立てる（T-1） |
| ヘッド | 集約ごとに 1 件ある記録。直近の `seq_nr` と、その `seq_nr` で確定したイベント封筒を持つ（共通契約 2 章） |
| 封筒 | イベントやスナップショットを、メタデータと中身（payload）に分けて運ぶ型 |
| payload | イベントや集約状態の中身。ドメインが決める |
| manifest | 利用者が付ける自由形式の文字列。ライブラリは解釈しない（T-4） |
| 楽観ロック | 書き込みの前提（ヘッドの `seq_nr`）が変わっていたら、書き込みを拒む照合の方式（W-8） |
| GSI（グローバルセカンダリインデックス） | DynamoDB の、主キーと別のキーで読むための索引。この文書では、履歴のスナップショットを読む疎な索引を指す（D-3） |
| TTL（期限切れ） | DynamoDB が、期限を過ぎた項目を後から取り除く仕組み（S-3） |
| Streams | DynamoDB が、テーブルの変更の記録を流す仕組み。変更フィードの供給源になる（DY-12） |
| SDK | AWS が配布する DynamoDB の Rust 用の部品。`aws-sdk-dynamodb` |
| IAM | AWS の権限の仕組み |
| MSRV（最小サポートの Rust の版） | ライブラリが動くことを保証する、最も古い Rust の版 |
| CI | main への変更ごとに自動で動く検査（`.github/workflows/ci.yml`） |
| PR（プルリクエスト） | main への変更の提案 |
| フック | 試験が、ストアの内部の処理に差し込む点。通常の利用者は使わない |

### 0.2 記載の種類

この文書には、次の 4 種類の記載がある。混ぜない。

| 種類 | 意味 | 見分け方 |
|:--|:--|:--|
| 規範 | 仕様が定めたこと | 規則番号を付ける |
| 確認した事実 | このリポジトリや資料を読んで確かめたこと | 「確認した」と書き、ファイル名を示す |
| 設計案 | この文書が提案する実現方法 | 「案」と書く |
| 9 章で決めた判断 | 設計段階で決め、コーディネーターがレビューした判断。判断が要るものだけを、オーナーに上げた | `(9 章 D-n)` と書く。決めた者・日付・理由を 9 章に書く。本文に移した決定は、その場で「決定（決めた者、2026-10-06）」と書く |

9 章の判断に関わる箇所は、決めた内容で書いてある。9 章に、決めていない項目はない。

### 0.3 確認していないこと

- 今回、Rust のコードもコンテナも動かしていない。API の形、SDK の使い方、試験の手順は、すべて設計案である。
- SDK の API は、ローカルに取得済みの `aws-sdk-dynamodb 1.130.0` のソースで存在を確かめた。実装時に、使う版で再確認する。
- 現行リポジトリには `Cargo.lock` がない。`aws-sdk-dynamodb` の解決済みの版は、確認していない。
- crates.io と Cargo の pre-release の扱い（公開した版は取り消せず `yank` だけできること、版の要求が pre-release を明示しない限り選ばれないこと、SemVer の pre-release の大小）は、一般的な知識である。今回、実機では確認していない（9 章 D-6）。

## 1. 目的と範囲

### 1.1 目的

共通契約と各プロファイルに適合する書き直しを行う。現行は version（スナップショットの版数）で楽観ロックを照合する。論理シャードのキーを使う。aid（集約 ID の文字列）は利用者の文字列化に頼る。仕様はこれらを変える（IP 2.1、ADR-0002・0004・0006）。実質は書き直しである。

### 1.2 次の版の番号

- 次のメジャーの正式版は **4.0.0** とする。現行は 3.0.6 である。
- main の Snapshot は、次のメジャーの pre-release として crates.io に公開する（IP 3）。公開は要件であり、決める対象ではない。
- 決定（コーディネーター、2026-10-06。9 章 D-6）: pre-release の公開は手動で行う。版は `4.0.0-alpha.N`（N は 1 から順に上げる）とし、公開の時機はオーナーが選ぶ。main への変更を自動で公開しない（IP 3 の結果）。準備の PR は、7 章の PR 1〜PR 3 である。これらが、版の番号と公開の条件を変える。

### 1.3 最初のメジャーに含めるもの

IP-D3・IP-D4・IP-D8 に従い、次を含める。

| 含めるもの | 内容 | 主な規則 |
|:--|:--|:--|
| 中核 | aid の組み立て、イベント封筒、スナップショット封筒、エラー分類、4 つの操作、保持の設定 | T-、H-、W-、R-、S-、E- |
| メモリ | 単一プロセスのメモリの保存先。適合を宣言する | MEM-1〜MEM-13 |
| DynamoDB | 3 テーブルと設定項目の配置 | DY-、D- |
| 適合テストデータの実行器 | 全ケースを DynamoDB（DynamoDB Local）とメモリで実行する | CR、IP 5 |
| rs v3 の DynamoDB からの移行ツール | 旧データを新しい配置へ移す | D-8、dynamodb.md の 11 章、IP-D8 |

### 1.4 最初のメジャーから外すもの

| 外すもの | 理由と扱い |
|:--|:--|
| SQLite の保存先 | IP-D3。段階 5 で、新契約で出し直す。新しいメジャーから削除する（決定。9 章 D-2）。旧メジャー（3.x）には残る |
| Bigtable の保存先 | 同上 |
| 変更フィードの提供 | メモリは MEM-13 で提供しない。DynamoDB の Streams の読み取りも提供しない。オーナーの決定（2026-10-06、全言語共通）により、ヘッド遷移を組み立てる関数も、再同期（DY-15）の補助も含めない。書き込みは仕様どおり head テーブルの Streams に記録を残すので、利用者は自分で読める。適合データに変更フィードの場面を足すときに、改めて決める |
| TTL 方式（期限切れ方式）のメモリでの提供 | MEM-12。保持件数があるときの要求は設定エラーにする |
| 旧配置の透過的な読み取り | 通常の API は旧配置を読まない。旧データの支援は移行ツールだけである（8 章） |
| 独自の `KeyResolver` を使う利用者の旧データの移行 | dynamodb.md の 11 章が範囲外とする |
| js・java・go など他言語の旧配置 | この文書の範囲外 |
| FNV-1a 64 のハッシュの実装 | DynamoDB とメモリはハッシュを使わない（ADR-0006）。適合データの 4 件は、対象外として報告する（オーナーの決定、2026-10-06。5.2.1 節・5.8 節）。成功にも失敗にも数えない。段階 5 でハッシュを使う保存先を出すときに実行する |

### 1.5 この文書で扱わないこと

- ソースコード、試験、CI の設定、既存の文書（README・DATABASE_SCHEMA・MIGRATION_GUIDE・CHANGELOG・AGENTS.md）の変更。これらは 7 章の PR で行う。
- aidlc・openspec・`.kiro/` の手順やツール。`aidlc/` ディレクトリは使わない。

## 2. 公開 API

### 2.1 設計の方針

次の方針で設計する。

- 現行の構造を土台にする。確認した現行の構造は次のとおり。
  - イベント封筒は、非公開のフィールドと、構築関数 `new`・`with_manifest` を持つ（`lib/src/event_envelope.rs`）。
  - シリアライザは payload だけを扱う（`lib/src/serializer.rs`）。
  - 保存先の差は `StorageBackend` が吸収し、共通の処理は `GenericEventStore` が持つ。どちらも非公開の `mod` である（`lib/src/lib.rs`、`lib/src/event_store_backend.rs`、`lib/src/generic_event_store.rs`）。
  - 保存先のファサード型は `EventStoreForMemory`・`EventStoreForDynamoDB` である。
- 現行コードを転記しない。ADR-0001 は、封筒のモデルを基準にする。
- 非同期の表現は、現行どおり `#[async_trait]` と `&self` を使い、Future は `Send` とする。決定（コーディネーター、2026-10-06）。理由: 現行の慣行を保ち、MEM-4（並行呼び出しの保護は保存先の責任）と整合する。
- 名前は Rust の慣習に従う。型は `UpperCamelCase`、関数は `snake_case` である。仕様の `persistEvent` は `persist_event` と書く。

以下のコードは、設計案のシグネチャである。実装時に型境界の細部が変わりうる。

### 2.2 モジュールの構成（案）

```text
lib/src/
  lib.rs                 公開する型の再エクスポート
  aggregate_id.rs        AggregateId、AidString             (T-1, T-11, T-12)
  seq_nr.rs              SeqNr、SEQ_NR_MAX                  (T-9, 1.5)
  event_envelope.rs      EventEnvelope、SnapshotEnvelope、SnapshotRead   (T-2, T-3, T-5, T-10, T-13, R-2)
  error.rs               EventStoreError と 5 分類           (E-1, E-2, E-3)
  serializer.rs          EventSerializer、SnapshotSerializer、Json 版   (T-6, T-7, T-8)
  retention.rs           RetentionSettings、保持対象の選択     (S-1, S-2, S-3, S-4)
  event_store.rs         EventStore trait（4 つの操作）        (3 章)
  generic_event_store.rs 共通の入力検査と保存先への委譲（非公開）
  storage_backend.rs     保存先の差を吸収する trait（非公開）
  memory/                メモリの保存先                       (MEM-)
  dynamodb/              DynamoDB の保存先                    (DY-, D-)
  migration/             rs v3 の DynamoDB からの移行の本体。feature `migration`  (9 章 D-1、8.2 節)
```

旧 API との共存中は、新しいコードを `next` モジュール（`#[doc(hidden)] pub mod next`）に置き、PR 20 で直下へ移す。決定（コーディネーター、2026-10-06）。理由: 各 PR を小さく保ち、main の CI を通し続けられる（7.3 節）。

### 2.3 集約 ID（T-1・T-11・T-12）

```rust
/// 集約 ID。型名と値の 2 つの文字列を持つ。
/// 利用者の `Display` や `ToString` には依存しない（T-1）。
pub trait AggregateId: Debug + Clone + Send + Sync + 'static {
  /// 集約の種別名。`-` を含んではならない（T-11）。
  fn type_name(&self) -> String;
  /// 集約の値。`-` を含んでよい。
  fn value(&self) -> String;
}

/// ライブラリが組み立てた aid 文字列（T-1）。構築時に T-11・T-12 を検査する。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AidString(String);

impl AidString {
  /// `${型名}-${値}` を組み立てる。
  /// 型名に `-` を含めば契約違反（T-11）、UTF-8 で 1024 バイトを超えれば契約違反（T-12）。
  pub fn from_aggregate_id<AID: AggregateId>(id: &AID) -> Result<Self, EventStoreError>;
  pub fn as_str(&self) -> &str;
}
```

| 要素 | 満たす規則 | 説明 |
|:--|:--|:--|
| `AggregateId` に `Display` を求めない | T-1 | 現行は `Display + Serialize + Deserialize` を求める。保存や比較に使う文字列を、利用者の文字列化から切り離す |
| `AidString::from_aggregate_id` | T-1 | 区切りは `-` の 1 文字。型名と値から組み立てる |
| 型名の `-` の検査 | T-11 | 組み立て時に検査する。違反は `ContractViolation`（規則 `T-11`） |
| 1024 バイトの検査 | T-12 | 型名・区切り・値の UTF-8 バイト数の合計で判定する。文字数で数えない（`aid-multibyte-1025`） |
| 検査の場所 | T-11・T-12 | `AidString` の組み立てに 1 か所だけ置く。メモリと DynamoDB は、この型を受け取る |

`AidString` は公開する。適合データの `buildAid` の検査が、この関数を直接呼ぶためである（5.2 節）。

### 2.4 整数と時刻の型（T-9・T-13・1.5）

```rust
/// イベントとスナップショットの通し番号（seq_nr）。
/// T-9: 0 以上 2^53 − 1 以下。上限を超える値を操作に渡すと契約違反になる。
pub type SeqNr = u64;

/// T-9 の上限（2^53 − 1 = 9007199254740991）。
pub const SEQ_NR_MAX: SeqNr = (1 << 53) - 1;

// 発生時刻には chrono の `DateTime<Utc>` を使う。ナノ秒を表せるので、T-3 の丸めは不要である。
```

| 要素 | 満たす規則 | 説明 |
|:--|:--|:--|
| `SeqNr = u64` | T-9 | 決定（コーディネーター、2026-10-06）。理由: T-9 が符号なしの型に検査の免除を認める。現行の `usize` からの変更が最小である。符号なしなので負の値を表せない。T-9 は「言語の型で範囲外を表せない場合、その検査は不要」と定める。上限 2^53 − 1 を超える値の検査は、操作の入口で行う |
| 0 の扱い | T-9・W-6 | 0 は T-9 では有効である。イベントでは契約違反（W-6）にする。読み取りの `seq_nr` 引数では 0 を許す（全件を返す） |
| `DateTime<Utc>` | T-3 | ナノ秒を表せる。ストア側の時刻で置き換えない。保存した値が読み取りで同じ値で返る |
| 範囲の検査 | T-13 | エポックからのナノ秒が符号付き 64 ビットに収まる範囲。`timestamp_nanos_opt()` が `None` なら契約違反。検査は書き込みの入口で行う |

適合データの `representation.signed_seq_nr = true` のケースは、`u64` では負数を表せない。対象外（理由: 表現不能）として報告する。成功に数えない（5.8 節）。表現不能にするのは、この印のあるケースだけである。ミリ秒の精度のケース（`representation.time_precision = milliseconds`）は、ナノ秒を表せる型なので実行しない。理由を記録し、成功に数えない。

### 2.5 イベント封筒（T-2・T-3・T-4・T-5・T-13）

```rust
/// 1 件のイベントを運ぶ封筒。ライブラリは manifest と payload を解釈しない（T-4）。
/// フィールドは非公開。要素の追加が既存の利用コードを壊さない（T-5）。
#[derive(Debug, Clone, PartialEq)]
pub struct EventEnvelope<AID, P> { /* aggregate_id, seq_nr, occurred_at, manifest, payload */ }

impl<AID, P> EventEnvelope<AID, P> {
  /// aggregate_id・seq_nr・occurred_at・payload は必須（T-2）。manifest は空文字列になる。
  pub fn new(aggregate_id: AID, seq_nr: SeqNr, occurred_at: DateTime<Utc>, payload: P) -> Self;
  pub fn with_manifest(self, manifest: impl Into<String>) -> Self;

  pub fn aggregate_id(&self) -> &AID;
  pub fn seq_nr(&self) -> SeqNr;
  pub fn occurred_at(&self) -> &DateTime<Utc>;
  pub fn manifest(&self) -> &str;
  pub fn payload(&self) -> &P;
  pub fn into_payload(self) -> P;
}
```

| 要素 | 満たす規則 | 説明 |
|:--|:--|:--|
| `new` の必須引数 | T-2 | 4 要素を引数で強制する。欠落はコンパイルできない。型で欠落を表せないので、欠落の検査は要らない（T-2・T-10、P-42） |
| manifest の省略 | T-2 | `new` は空文字列で作る。`with_manifest` で設定する |
| 非公開フィールドと `with_*` | T-5 | 現行の形を引き継ぐ。将来の要素は `with_*` で足せる |
| `occurred_at` の保存 | T-3 | メモリはそのまま持つ。DynamoDB はエポックナノ秒の数値で書く |
| 契約違反の検査 | T-9・T-13・W-6 | 構築では検査しない。操作の入口で検査する。理由は 2.8 節 |
| `Serialize`・`Deserialize` の派生を外す案 | T-7 | 直列化の対象は payload だけである。封筒全体を直列化する経路は使わない。外してよいかは、実装時に旧 Bigtable・SQLite 以外の利用箇所がないことを確かめる（確認した範囲では、現行の `EventEnvelope` に `derive(Serialize, Deserialize)` が付いている） |

### 2.6 スナップショット封筒と読み取りの結果（T-10・R-2・R-3）

```rust
/// 1 件のスナップショットを運ぶ封筒。書き込みと読み取りで同じ型を使う（1.4）。
/// ヘッドの seq_nr は含めない（ADR-0002）。version は持たない。
#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotEnvelope<A> { /* aggregate, seq_nr, manifest */ }

impl<A> SnapshotEnvelope<A> {
  /// aggregate と seq_nr は必須（T-10）。manifest は空文字列になる。
  pub fn new(aggregate: A, seq_nr: SeqNr) -> Self;
  pub fn with_manifest(self, manifest: impl Into<String>) -> Self;

  pub fn aggregate(&self) -> &A;
  pub fn seq_nr(&self) -> SeqNr;
  pub fn manifest(&self) -> &str;
  pub fn into_aggregate(self) -> A;
}

/// 最新スナップショットの読み取り結果。スナップショット封筒（なくてもよい）と、
/// 読み取り時点のヘッドの seq_nr の組（R-2）。
#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotRead<A> { /* snapshot: Option<SnapshotEnvelope<A>>, head_seq_nr: SeqNr */ }

impl<A> SnapshotRead<A> {
  pub fn snapshot(&self) -> Option<&SnapshotEnvelope<A>>;
  pub fn head_seq_nr(&self) -> SeqNr;
  pub fn into_parts(self) -> (Option<SnapshotEnvelope<A>>, SeqNr);
}
```

| 要素 | 満たす規則 | 説明 |
|:--|:--|:--|
| `SnapshotEnvelope::new(aggregate, seq_nr)` | T-10 | 現行の `version` 引数を外す。必須の 2 要素を引数で強制する |
| `with_manifest` | T-10 | manifest を足す。現行のスナップショット封筒に manifest はない |
| `SnapshotRead` | R-2・R-3 | スナップショットがなくても、ヘッドの seq_nr を返す |
| ヘッドの seq_nr を別に持つ | ADR-0002 | 封筒の seq_nr はヘッドを下回りうる。DynamoDB では上回ることもある（R-8） |

### 2.7 payload とシリアライザ（T-6・T-7・T-8）

```rust
/// イベント payload の直列化契約。
pub trait EventSerializer<P>: Debug + Send + Sync + 'static {
  fn serialize(&self, payload: &P) -> Result<Vec<u8>, EventStoreError>;     // 失敗は Serialization
  fn deserialize(&self, data: &[u8]) -> Result<P, EventStoreError>;          // 失敗は Serialization
}

/// 集約状態（スナップショットの aggregate）の直列化契約。
pub trait SnapshotSerializer<A>: Debug + Send + Sync + 'static {
  fn serialize(&self, aggregate: &A) -> Result<Vec<u8>, EventStoreError>;
  fn deserialize(&self, data: &[u8]) -> Result<A, EventStoreError>;
}

/// 既定のシリアライザ。JSON を使う（T-8）。
pub struct JsonEventSerializer<P>(/* PhantomData */);
pub struct JsonSnapshotSerializer<A>(/* PhantomData */);
```

| 要素 | 満たす規則 | 説明 |
|:--|:--|:--|
| 直列化の入力は payload だけ | T-7 | 引数に封筒のメタデータは現れない。現行の構造を引き継ぐ |
| `EventStore` の `P`・`A` に serde を求めない案 | T-6 | trait の境界は `Send + Sync + 'static` だけにする。`Serialize + DeserializeOwned` は、既定の JSON シリアライザを使う生成関数にだけ付ける |
| 任意のシリアライザ | T-6 | `with_serializers` で差し替える。serde を実装しない型でも使える |
| 既定は JSON | T-8 | `serde_json` を使う。ドメイン型は serde の derive だけでよい。ライブラリの trait を実装させない |

現行の `EventStore` trait は、`P`・`A` に `Serialize + DeserializeOwned` を求める。T-6 を字義どおり読むと、任意のシリアライザを使うときは serde を要求できない。この読み方で決めた（10 章 Q-2）。

### 2.8 4 つの操作と非同期の表現（共通契約 3 章）

```rust
#[async_trait]
pub trait EventStore: Debug + Clone + Send + Sync + 'static {
  type AID: AggregateId;
  type A: Send + Sync + 'static;   // 集約状態（スナップショットの payload）
  type P: Send + Sync + 'static;   // イベントの payload

  /// 3.1 persistEvent。イベントだけを追記する。
  async fn persist_event(
    &self,
    event: EventEnvelope<Self::AID, Self::P>,
  ) -> Result<(), EventStoreError>;

  /// 3.2 persistEventAndSnapshot。イベントを追記し、同時にスナップショットを書く。
  /// W-9: `snapshot.seq_nr() != event.seq_nr()` なら契約違反。
  async fn persist_event_and_snapshot(
    &self,
    event: EventEnvelope<Self::AID, Self::P>,
    snapshot: SnapshotEnvelope<Self::A>,
  ) -> Result<(), EventStoreError>;

  /// 3.4 getLatestSnapshotById。ヘッドがなければ `None`（R-1）。
  async fn get_latest_snapshot_by_id(
    &self,
    aid: &Self::AID,
  ) -> Result<Option<SnapshotRead<Self::A>>, EventStoreError>;

  /// 3.5 getEventsByIdSinceSeqNr。`seq_nr` 以上のイベント封筒を昇順で、すべて返す（R-4・R-5・R-6）。
  async fn get_events_by_id_since_seq_nr(
    &self,
    aid: &Self::AID,
    seq_nr: SeqNr,
  ) -> Result<Vec<EventEnvelope<Self::AID, Self::P>>, EventStoreError>;
}
```

| 要素 | 満たす規則 | 説明 |
|:--|:--|:--|
| 期待値の引数がない | 3 章、W-8 | 現行の `expected_version` を外す。照合の期待値は `event.seq_nr()` から決まる |
| `persist_event` の seq_nr が 1 でもよい | W-3、MEM-7 | 現行はイベントだけの seq_nr = 1 を拒む。新しい契約ではヘッドを作れる |
| `persist_event_and_snapshot` が封筒を受け取る | T-10、W-9 | 現行は集約状態の値を受け取る。封筒を受け取り、manifest も運ぶ |
| 戻り値に `SnapshotRead` | R-2・R-3 | 現行は `SnapshotEnvelope`（version つき）を返す |
| 1 回の書き込みは 1 イベント | H-3 | 引数の型が `EventEnvelope` 1 件である |
| `&self` | MEM-4 | 現行の書き込みは `&mut self`。排他制御は保存先が持つので、`&self` で並行呼び出しを受けられる（決定。2.1 節） |
| 非同期は `#[async_trait]` | 8 章 | 現行の慣行を引き継ぐ。`Send` な Future を返す（決定。2.1 節） |

操作の入口の検査（次の表の 1〜5）は、共通の層（`GenericEventStore`）が 1 か所で行う。6 の直列化は、検査の後に保存先が行う。順序は次のとおりである。

| 順 | 検査 | 規則 | 分類 |
|:--|:--|:--|:--|
| 1 | aid の組み立て（型名の `-`、1024 バイト） | T-1・T-11・T-12 | 契約違反 |
| 2 | `seq_nr` が 2^53 − 1 以下 | T-9 | 契約違反 |
| 3 | イベントの `seq_nr` が 0 でない | W-6 | 契約違反 |
| 4 | `occurred_at` がナノ秒の範囲内 | T-13 | 契約違反 |
| 5 | スナップショットと `seq_nr` が一致 | W-9 | 契約違反 |
| 6 | payload の直列化（保存先の実装が行う。メモリはロックの前、DynamoDB は送信の前。シリアライザはストアのインスタンスが持つ。`MemoryStorage` は持たない） | T-6・T-7 | 直列化 |

照合（W-3・W-4・W-7・W-8）は、保存先ごとに実現が違う。メモリは排他制御の中で、DynamoDB は条件つきの書き込みで行う。共通の層は照合しない。理由は、確定の仕組みが H-1 でプロファイルに委ねられているからである。

保存先の差を吸収する非公開の trait は、次の形の案とする。保持処理の失敗は、戻り値の型で追記の結果から分ける（S-4）。この trait を実装するのは、保存先のハンドル（メモリなら `MemoryStorage`、DynamoDB なら `Client`）とシリアライザを持つ、ストアのインスタンスである。シリアライザの持ち主は、ストアのインスタンスである（2.12 節）。

```rust
#[async_trait]
pub(crate) trait StorageBackend<AID, A, P>: Send + Sync + 'static {
  /// 追記を確定し、保存先ごとの時機で保持処理を行う。
  /// 追記が確定していれば Ok を返す。保持処理の失敗は `AppendReceipt` に載せ、Err にしない。
  async fn append(&self, request: AppendRequest<'_, AID, A, P>) -> Result<AppendReceipt, EventStoreError>;

  /// 最新スナップショットとヘッドの seq_nr を読む（R-1〜R-3）。スナップショットの封筒には aggregate_id がない。
  async fn load_snapshot(&self, aid: &AidString) -> Result<Option<SnapshotRead<A>>, EventStoreError>;

  /// `seq_nr` 以上のイベント封筒を昇順で、すべて返す（R-4〜R-6）。
  /// `aggregate_id` は、公開操作が受け取った元の値である。返す封筒の `aggregate_id` に複製して使う。
  /// 保存先は元の値を保存せず（ジャーナルの属性は aid 文字列だけ。4.2.1 節）、aid 文字列から元の型を復元できない（R-6）。
  async fn load_events(
    &self,
    aggregate_id: &AID,
    aid: &AidString,
    seq_nr: SeqNr,
  ) -> Result<Vec<EventEnvelope<AID, P>>, EventStoreError>;
}

/// 追記の入力。共通の層が検査を済ませた値を、参照で渡す。
pub(crate) struct AppendRequest<'a, AID, A, P> {
  pub aid: &'a AidString,                         // 検査済みの aid 文字列（T-1・T-11・T-12）。保存先のキーに使う
  pub event: &'a EventEnvelope<AID, P>,           // 元の aggregate_id を持つ。直列化は保存先が行う（MEM-6、4.4 節）
  pub snapshot: Option<&'a SnapshotEnvelope<A>>,  // `Some` のとき `seq_nr` は event と一致する（W-9 は検査済み）
}

pub(crate) struct AppendReceipt {
  pub retention_failure: Option<RetentionFailure>, // S-4: 別の経路で知らせる
}
```

`GenericEventStore` は、公開操作の `&AID` から `AidString` を組み立てた後、`&AID` と `&AidString` の両方を保存先へ渡す。`get_events_by_id_since_seq_nr` が返す封筒の `aggregate_id` は、呼び出し側が渡した値の複製であり、新しいストアのインスタンスで既存データを読む場合も同じである（R-6）。

`GenericEventStore` は、`append` が `Ok` を返した後に `retention_failure` があれば、2.11 節の経路で知らせる。そのうえで `Ok(())` を返す。通知の呼び出しは、メモリの排他制御を解いた後に行う（MEM-11）。

### 2.9 エラー分類（4 章・E-1・E-2・E-3）

```rust
/// 5 つの分類を 1 つの型で表す（E-1）。呼び出し側は `match` で分類を区別する。
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EventStoreError {
  /// 楽観ロック。既存集約への新規作成（W-3）、seq_nr の重複（W-7）、ヘッド以下での追記（W-8）。
  #[error("optimistic lock failed: aid={aid}, seq_nr={seq_nr}{}", head_seq_nr_suffix(.head_seq_nr))]
  OptimisticLock { aid: String, seq_nr: SeqNr, head_seq_nr: Option<SeqNr> },

  /// 契約違反。W-6・W-9・T-9・T-11・T-12・T-13 と、W-8 の飛び番。
  #[error("contract violation: rule={rule}{}", contract_violation_suffix(.seq_nr, .snapshot_seq_nr))]
  ContractViolation { rule: ContractRule, seq_nr: Option<SeqNr>, snapshot_seq_nr: Option<SeqNr> },

  /// 直列化。payload の直列化・復元の失敗。
  #[error("serialization failed: phase={phase}")]
  Serialization { phase: SerializationPhase, #[source] source: Box<dyn Error + Send + Sync> },

  /// 設定。生成時の不正な設定値と、保存先に記録された設定との食い違い。
  #[error("configuration error: {reason}")]
  Configuration { reason: ConfigurationReason },

  /// 保存先。通信・保存先の失敗と、読み取ったデータの欠損。
  #[error("storage error: operation={operation}")]
  Storage { operation: StorageOperation, #[source] source: Box<dyn Error + Send + Sync> },
}

/// 契約違反の規則。文字列表現は仕様の規則番号（E-3）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ContractRule { T9, T11, T12, T13, W6, W8Gap, W9, ItemSizeLimit /* D-7（判断の番号であり、必須の規則番号ではない。10 章 Q-3） */ }
```

| 要素 | 満たす規則 | 説明 |
|:--|:--|:--|
| 5 つの変種 | 4 章、E-1 | 楽観ロック・契約違反・直列化・設定・保存先。呼び出し側は型で区別する。メッセージの文字列から分類を推測しない |
| 読み取りと書き込みで 1 つの型 | E-1 | 現行は `EventStoreWriteError` と `EventStoreReadError` に分かれる。5 分類は読み取りにも現れる（復元の失敗、データの欠損）ので、1 つにまとめる案 |
| `OptimisticLock` の表示 | E-2 | 含めるのは aid 文字列、追記しようとした seq_nr、分かればヘッドの seq_nr だけ。接続文字列・資格情報・SDK の生のエラー文を含めない。`source` も持たない |
| `ContractViolation` の表示 | E-3 | 違反した規則番号（`W-9` など）と、関係する seq_nr を含める。W-9 では異なるスナップショット番号も含める |
| ヘッド番号は契約違反の必須値でない | E-3、CR | `ContractViolation` はヘッド番号を持たない |
| `Storage` の `source` | 4 章 | SDK のエラーは `Box<dyn Error>` に入れ、公開 API の型に SDK の型を出さない。表示に生のエラー文を含めない |
| `#[non_exhaustive]` | T-5 | 変種や項目を足しても、利用者の `match` を壊さない |

`ContractRule` の文字列表現は、`W-9` のように仕様の規則番号と一致させる。`Display` はこの表現を出す。適合データの `must_contain: ["W-9", ...]` を満たすためである。

上のシグネチャの案は、次の型・関数の中身を定めない。実装時に定める。

- `SerializationPhase`: 適合データの段階 `serialize-event`・`serialize-snapshot`・`deserialize-event`・`deserialize-snapshot` に対応させる案。
- `ConfigurationReason`: 設定エラーの理由。2.10 節の表の「設定エラーになる値」（保持件数 0、メモリでの `Ttl`、保存先に記録された設定との食い違い）に対応させる案。
- `StorageOperation`: 保存先エラーが起きた操作の種類。
- `head_seq_nr_suffix`・`contract_violation_suffix`: 表示用の補助関数。値があるときだけ、`head_seq_nr` や `seq_nr` を表示に足す。
- `RetentionFailure`（2.8 節）: 保持処理の失敗の記録。2.11 節の `tracing` のイベントの項目（`aid`・`seq_nr`・`phase`・`error`）を持つ案。

呼び出し側が分類を区別する例を示す。

```rust
match store.persist_event(event).await {
  Ok(()) => { /* 確定した */ }
  Err(EventStoreError::OptimisticLock { .. }) => { /* 読み直して再試行する */ }
  Err(EventStoreError::ContractViolation { rule, .. }) => { /* 呼び出しの誤り。再試行しない */ }
  Err(EventStoreError::Serialization { .. }) => { /* payload の直列化の失敗 */ }
  Err(EventStoreError::Configuration { .. }) => { /* 設定の誤り */ }
  Err(EventStoreError::Storage { .. }) => { /* 保存先の失敗。確定状態は呼び出しの種類で判断する */ }
  Err(_) => { /* 将来の変種 */ }
}
```

D-7 の項目サイズ超過は、分類を契約違反とする。ここでの D-7 は `dynamodb.md` の判断の番号であり、必須の規則番号ではない。適合データは分類だけを期待する（`error.rule` もメッセージ条件もない）。`ContractRule::ItemSizeLimit` の文字列表現は `D-7` とする（10 章 Q-3）。

### 2.10 設定（S-1・S-3・MEM-3・DY-8）

```rust
/// スナップショット保持の設定。メモリと DynamoDB が共有する。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionSettings { /* keep_snapshot_count: Option<usize>, mode: RetentionMode */ }

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetentionMode {
  /// 古い履歴を削除する（S-2）。
  Delete,
  /// 期限切れの印を付ける（S-3）。猶予は秒単位。印を付ける時刻にこの秒数を足した値が期限になる。
  Ttl { grace_seconds: u64 },
}

impl RetentionSettings {
  /// 履歴を持たない。現在のスナップショットだけを持つ（既定）。
  pub fn current_only() -> Self;
  /// 新しい `count` 件の履歴を残す。0 の検査はストアの生成時に行う（S-1）。
  pub fn keep_latest(count: usize) -> Self;
  pub fn with_mode(self, mode: RetentionMode) -> Self;
}
```

| 設定 | 型 | 規則 | 設定エラーになる値 |
|:--|:--|:--|:--|
| 保持件数 | `Option<usize>` | S-1・MEM-3 | `Some(0)` |
| 保持の方式 | `RetentionMode` | S-3、MEM-12 | 保持件数があり、メモリで `Ttl` を要求したとき（MEM-3・MEM-12）。保持件数がないときは、方式を使わない。`Ttl` でも設定エラーにしない |
| TTL の猶予 | `u64`（秒） | S-3 | 検査を足さない。`u64` なので負の値は表せない。0 は有効（印を付ける時刻がそのまま期限になる）。上限は定めない（指揮役の回答、2026-10-06。10 章 Q-5） |
| テーブル名 3 つ | `String` | DY-8、`dynamodb.md` 2 章 | 保存先に記録された設定との食い違い（DY-8 の手順 3）。名前の値の検査はない |
| 履歴のインデックス名 | `String` | D-3 | 同上 |
| 未処理の再要求の上限 | `u32` | DY-8 | なし。0 を許す。既定は 10（初回を数えない）。上限到達は設定エラーではなく保存先エラー |
| 再要求の初期の待ち | `Duration` | DY-8 | なし。既定は 50 ミリ秒 |
| 再要求の上限の待ち | `Duration` | DY-8 | なし。既定は 2 秒 |

- 保持件数がない（`keep_snapshot_count` が `None`）ときは、`mode` を使わない。`mode` が `Ttl` でも、エラーにせず方式を無視する。履歴を作らないので、方式を使う場面がない。メモリでも DynamoDB でも同じである。
- 再要求の方針（値・式・揺らぎ・待ちの短縮）は、2.12.1 節で定める。
- 設定の検査は、ストアの生成時に行い、`Result` で返す。型で 0 を表せなくする方法（`NonZeroUsize`）は採らない。理由は、適合データの `core-retention-zero` が、生成の失敗として設定エラーを期待するからである。型が 0 を弾くと、ライブラリの検査に届かない（CR の手順 5）。
- 現行の `with_keep_snapshot_count(Some(0))` は契約違反を返す。新しい版は設定エラーを返す（4 章、S-1）。
- 現行の `with_delete_ttl(Option<Duration>)` は、`keep_snapshot_count` と併用したときだけ効く。新しい版は `RetentionMode` で方式を明示する。
- 変更フィードを要求する設定項目は作らない。メモリにも DynamoDB にもない。したがって、要求を設定エラーにする対象が型にない（MEM-3・MEM-13、10 章 Q-7）。

### 2.11 保持処理の失敗を知らせる経路（S-4・MEM-11）

`tracing` の WARN のイベントで知らせる。メモリと DynamoDB で共通にし、必須とする。決定（コーディネーター、2026-10-06）。理由: S-4 と MEM-D7 のとおり、ログを必須にする。通知用のコールバックは、公開 API に足さない。必要になれば、壊さない形で後から足せる。

```rust
// 保持処理が失敗しても、追記が確定していれば書き込みは成功として返す（S-4）。
// 失敗は、次の固定の対象（target）と項目で WARN のイベントを出す。
tracing::warn!(
  target: "event_store_adapter::retention",
  category = "retention-failure",
  aid = %aid,
  seq_nr,                       // 追記した seq_nr
  phase = "retention-delete",   // retention-query / retention-delete / retention-mark
  error = %error,
  "snapshot retention failed; the append was committed"
);
```

| 要素 | 満たす規則 | 説明 |
|:--|:--|:--|
| 書き込みは成功を返す | S-4・MEM-11 | 保持の失敗は `AppendReceipt` に載り、`EventStoreError` にならない |
| 通知は別の経路 | S-4 | ログで知らせる。公開 API の通知用の型は固定しない（S-4） |
| メモリの通知は排他制御を解いた後 | MEM-11 | `GenericEventStore` が `append` の戻り後に出す |
| 次の追記後の保持処理で再試行 | S-4・MEM-11 | 取り残した履歴は、次の保持処理で選び直して片付ける。再試行の専用の仕組みは持たない |
| 通知の失敗は追記の結果を変えない | MEM-11 | ログの出力は結果を返さない |

適合実行器は、この `tracing` のイベントを集める層（`Layer`）で `observe.notifications` を検査する（5.5 節）。

通知は、公開操作の Future の中で出す。バックグラウンドのタスクを作らない。したがって、通知は、操作の Future が poll される間に出る。非同期の実行時が、その Future を別のスレッドで poll しても、同じ Future の中で出た通知として観測できる（5.5 節）。

### 2.12 ストアの生成の API（案）

```rust
// メモリ（3 章）
pub struct MemoryStorage { /* Arc<...> */ }           // 保存先。clone すると同じ保存先を共有する（MEM-2）。持つのは記録（バイト列とメタデータ）・設定・排他制御だけで、シリアライザは持たない
impl MemoryStorage {
  pub fn new(retention: RetentionSettings) -> Result<Self, EventStoreError>;  // MEM-1, MEM-3
}
impl<AID, A, P> EventStoreForMemory<AID, A, P>
where
  AID: AggregateId,
  A: Serialize + DeserializeOwned + Send + Sync + 'static,   // 既定の JSON シリアライザの境界（T-6・T-8）
  P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
  pub fn new(storage: MemoryStorage) -> Self;
}
impl<AID, A, P> EventStoreForMemory<AID, A, P>
where
  AID: AggregateId,
  A: Send + Sync + 'static,   // 任意のシリアライザを使うので、serde の境界は付けない（T-6）
  P: Send + Sync + 'static,
{
  pub fn with_serializers(
    storage: MemoryStorage,
    event_serializer: Arc<dyn EventSerializer<P>>,
    snapshot_serializer: Arc<dyn SnapshotSerializer<A>>,
  ) -> Self;
}

// DynamoDB（4 章）
pub struct DynamoDbTables {
  pub journal_table_name: String,
  pub snapshot_table_name: String,
  pub head_table_name: String,
  pub snapshot_history_index_name: String,
}
#[derive(Debug, Clone)]
pub struct DynamoDbOptions {
  pub retention: RetentionSettings,             // S-1・S-3。既定は `RetentionSettings::current_only()`
  pub unprocessed_retry_limit: u32,              // DY-8。未処理の再要求の上限（初回を数えない）。既定は 10。0 を許す
  pub unprocessed_retry_initial_delay: Duration, // DY-8。指数バックオフの待ちの初期値（std::time::Duration）。既定は 50 ミリ秒
  pub unprocessed_retry_max_delay: Duration,     // DY-8。待ちの上限。既定は 2 秒
}
impl Default for DynamoDbOptions { /* 上の既定値 */ }

impl<AID, A, P> EventStoreForDynamoDB<AID, A, P>
where
  AID: AggregateId,
  A: Serialize + DeserializeOwned + Send + Sync + 'static,   // 既定の JSON シリアライザの境界（T-6・T-8）
  P: Serialize + DeserializeOwned + Send + Sync + 'static,
{
  /// 設定項目を照合し、なければ作る（DY-8）。非同期で、失敗しうる。
  pub async fn open(
    client: aws_sdk_dynamodb::Client,
    tables: DynamoDbTables,
    options: DynamoDbOptions,
  ) -> Result<Self, EventStoreError>;
}

impl<AID, A, P> EventStoreForDynamoDB<AID, A, P>
where
  AID: AggregateId,
  A: Send + Sync + 'static,   // 任意のシリアライザを使うので、serde の境界は付けない（T-6）
  P: Send + Sync + 'static,
{
  /// `open` と同じ照合を行い、イベントとスナップショットのシリアライザを差し替える。
  pub async fn open_with_serializers(
    client: aws_sdk_dynamodb::Client,
    tables: DynamoDbTables,
    options: DynamoDbOptions,
    event_serializer: Arc<dyn EventSerializer<P>>,
    snapshot_serializer: Arc<dyn SnapshotSerializer<A>>,
  ) -> Result<Self, EventStoreError>;
}
```

- メモリの保存先を `MemoryStorage` として型に出す。決定（コーディネーター、2026-10-06）。理由: MEM-3 と MEM-D11 のとおり、設定を保存先が不変で持つ形を、型で表せる。
- シリアライザの持ち主は、ストアのインスタンスである。`MemoryStorage` は持たない。持たせると `MemoryStorage` に型引数 `A`・`P` が要り、`with_serializers` の引数と食い違う。1 つの `MemoryStorage` を、別のシリアライザを持つ複数のストアで共有してよい。MEM-2・MEM-3 が共有を求めるのは、記録・設定・排他制御であり、シリアライザを含まない。あるストアが書いたバイト列を、別のシリアライザを持つストアが復元できなければ、読み取りは `Serialization`（`deserialize-event`・`deserialize-snapshot`）を返す。
- 生成関数は、serde の境界を持つ組（`new`・`open`。既定の JSON シリアライザ）と、境界を持たない組（`with_serializers`・`open_with_serializers`。任意のシリアライザ）に分ける（T-6・T-8、2.7 節）。`open_with_serializers` の引数は、`open` の 3 つに、イベントとスナップショットのシリアライザの 2 つを足したものである。
- `EventStoreForMemory` と `EventStoreForDynamoDB` は、現行の名前を残す。移行の手間を減らすためである。
- `EventStoreForDynamoDB::open` が非同期で `Result` を返すのは、DY-8 が生成時に 3 テーブルを読み、なければ書くからである。
- `KeyResolver`・`DefaultKeyResolver`・シャード数・旧 journal の GSI 名は廃止する（DY-16、ADR-0006）。

#### 2.12.1 未処理の再要求の方針（DY-8・DY-9）

この設計で決めた値である。コーディネーターのレビューで確かめる。DY-8 の設定の読み取りは `retry_limit` を指定しないケース（`dynamodb-config-unprocessed-keys`）を持つ。このため、既定の上限は 1 以上でなければならない。

| 項目 | 決めた値 |
|:--|:--|
| 上限 | `unprocessed_retry_limit`。既定は 10。初回を数えない。0 も許す |
| 初期の待ち | `unprocessed_retry_initial_delay`。既定は 50 ミリ秒 |
| 上限の待ち | `unprocessed_retry_max_delay`。既定は 2 秒 |
| 待ちの式 | k 回目の再要求の前に、min(初期の待ち × 2^(k−1), 上限の待ち) 待つ。k は 1 から数える |
| 揺らぎ（ジッター） | 入れない。待ちの列が毎回変わると、実行器が `exponential_backoff` を検査できない |
| 待ちの合計 | 既定では最大で約 11 秒（50・100・200・400・800・1600 ミリ秒の後、2000 ミリ秒を 4 回） |
| 上限到達 | `Storage`。保持の再送では、保持の失敗にする |
| 使う場所 | DY-8（設定の読み取り）、DY-9（最新スナップショットの読み取り）、保持の `BatchWriteItem` の `UnprocessedItems` の再送。同じ方針を使う（10 章 Q-6） |

- 理由: 対象のキーは 2〜3 件と少ない。待ちが決まった値になるので、適合データの `exponential_backoff` を検査できる。
- 項目名を `unprocessed_retry_*` にしたのは、設定の読み取りだけでなく、読み取りと保持の再送にも使うからである。
- 待ちは、`RetrySleeper` で行う。`RetrySleeper` は、`test-hooks` feature のときだけある `#[doc(hidden)]` の trait で、`async fn sleep(&self, delay: Duration)` を持つ。`Clock`（TTL の印付けの時計。4.6 節）とは別である。
  - 渡し方: `#[doc(hidden)]` の生成関数の引数で渡す。`DynamoDbOptions` の形は、feature の有無で変わらない。
  - 渡さないときの既定: `Client` の `config().sleep_impl()` が返す SDK の待ちの仕組みを使う。`sleep_impl()` が `None` なら、`open` が `Configuration` を返す。
  - 確認した範囲: `aws-sdk-dynamodb 1.130.0` に `Client::config()` と `Config::sleep_impl()` がある。既定で作った `Client` が `sleep_impl` を持つかは、確認していない。
  - 実行器は、`RetrySleeper` に要求された待ちを記録し、実際には待たない。これで `exponential_backoff`（5.5.1 節）を検査する。

### 2.13 現行の公開 API との対応表

現行（3.0.6）の公開 API は、`lib/src/lib.rs`・`lib/src/types.rs`・`lib/src/event_envelope.rs`・`lib/src/key_resolver.rs`・`lib/src/serializer.rs` と、各保存先の型である。

| 現行 | 次のメジャー | 扱い |
|:--|:--|:--|
| `AggregateId`（`Display + Serialize + Deserialize` を要求） | `AggregateId`（`type_name`・`value` だけ） | 置き換える。`Display` と serde の要求を外す（T-1） |
| aid の組み立て（`to_string()` と `KeyResolver`） | `AidString::from_aggregate_id` | 置き換える。ライブラリが組み立てる（T-1・T-11・T-12） |
| `EventEnvelope<AID, P>`（`usize` の seq_nr） | `EventEnvelope<AID, P>`（`SeqNr`） | 形を引き継ぐ。型を変える（T-9） |
| `SnapshotEnvelope<A>::new(aggregate, seq_nr, version)` | `SnapshotEnvelope::new(aggregate, seq_nr)` + `with_manifest` | `version` を外し、`manifest` を足す（T-10、ADR-0004） |
| `SnapshotEnvelope::version()` | なし | 削除する |
| `EventStore::persist_event(&mut self, event, expected_version)` | `persist_event(&self, event)` | 期待値の引数を削除する（W-8） |
| `EventStore::persist_event_and_snapshot(&mut self, event, aggregate, expected_version)` | `persist_event_and_snapshot(&self, event, snapshot)` | 封筒を受け取る。期待値を削除する（T-10、W-9） |
| `get_latest_snapshot_by_id → Option<SnapshotEnvelope<A>>` | `→ Option<SnapshotRead<A>>` | ヘッドの seq_nr を別に返す（R-2） |
| `get_events_by_id_since_seq_nr(aid, usize)` | `(aid, SeqNr)` | 型を変える |
| `EventStoreWriteError`（5 変種）・`EventStoreReadError`（3 変種） | `EventStoreError`（5 変種） | 置き換える。分類は 5 つ（E-1） |
| `OptimisticLockError(String)`・`ContractViolation(String)` | 構造化した変種 | 置き換える。文字列を組み立てる関数 `format_optimistic_lock_message` は削除する（E-2・E-3） |
| `IOError`・`OtherError` | `Storage` | 統合する。読み取ったデータの欠損も `Storage` |
| `SerializationError`・`DeserializationError` | `Serialization { phase }` | 統合する |
| `EventSerializer`・`SnapshotSerializer`（`EventStoreWriteError`/`ReadError` を返す） | 同名（`EventStoreError` を返す） | 形を引き継ぐ。エラーの型を変える |
| `JsonEventSerializer`・`JsonSnapshotSerializer` | 同名 | 引き継ぐ（T-8） |
| `KeyResolver`・`DefaultKeyResolver` | なし | 削除する（DY-16） |
| `EventStoreForDynamoDB::new(client, journal, journal_aid_index, snapshot, snapshot_aid_index, shard_count)` | `EventStoreForDynamoDB::open(client, DynamoDbTables, DynamoDbOptions).await` | 置き換える。3 テーブル、シャード数なし |
| `with_key_resolver` | なし | 削除する |
| `with_keep_snapshot_count`・`with_delete_ttl` | `RetentionSettings` | 置き換える（S-1・S-3） |
| `with_event_serializer`・`with_snapshot_serializer` | `open_with_serializers`・`with_serializers` | 置き換える |
| `EventStoreForMemory::new()` | `MemoryStorage::new(..)?` + `EventStoreForMemory::new(storage)` | 置き換える。独立と共有を明示する（MEM-2） |
| メモリの `A`・`P` への `Clone` の要求 | なし | 削除する（MEM-6） |
| `EventStoreForBigtable`・`EventStoreForSqlite`、feature `bigtable`・`sqlite`・`sqlite-system` | なし | 新しいメジャーから削除する（IP-D3。決定: 9 章 D-2）。旧メジャー（3.x）には残る |
| `GenericEventStore`・`StorageBackend`・`SnapshotMaintenance`（非公開） | 作り直す（非公開） | 公開 API ではない。semver の対象外 |
| 各保存先の `maintenance()` | なし | 削除する。設定は `RetentionSettings` が持つ |

置き換えた旧い名前や関数は、残さない。`#[deprecated]` の別名も作らない。旧 API の利用者への案内は 8 章に書く。

## 3. メモリの実装方針

メモリのプロファイル（`memory.md`、2026-10-05 合意）を満たす。現行の `lib/src/event_store_for_memory.rs` との差は、プロファイルの 8 章が挙げている。確認した現行の状態は次のとおり。

- `Arc<Mutex<HashMap<String, ..>>>` の 1 つのロックを共有する。ロックの保持中に `.await` しない。
- 照合はスナップショット封筒の `version` で行う。ヘッドを別に持たない。
- イベントだけの `seq_nr = 1` を拒む。
- payload と集約状態に `Clone` を要求し、複製して保存・返却する。
- 保持処理（履歴の間引き）をしない。

### 3.1 規則ごとの実現方針

| 規則 | 方針 |
|:--|:--|
| MEM-1 | 保存先 `MemoryStorage` はプロセス内のメモリだけを持つ。ファイル・再起動後の回復・プロセス間共有の API を作らない。利用中の全消去の API も作らない。保存先の寿命（最後の参照が落ちるとき）で全記録を失う |
| MEM-2 | `MemoryStorage::new` は空の独立した保存先を作る。同じ保存先を共有するのは、`MemoryStorage` を `clone` して渡したストアだけである。`clone` は `Arc` の参照を増やすので、明示的な共有になる。名前や識別子の同一性では共有しない。現行の `EventStoreForMemory::new()` は暗黙に新しい保存先を作り、`clone` で共有する。共有の単位を型（`MemoryStorage`）に出す |
| MEM-3 | 生成時（`MemoryStorage::new`）に設定を検査する。保持件数 0 と、保持件数があるときの `RetentionMode::Ttl`（MEM-12）は設定エラー。保持件数がないときは方式を使わないので、`Ttl` でも設定エラーにしない（2.10 節）。設定は保存先が不変で持つ。共有するストアは保存先の設定を使う。ストアは設定を上書きする API を持たない |
| MEM-4 | 保存先がストア全体の排他制御（`std::sync::Mutex`）を 1 つ持つ。読み取り・書き込み・保持の全操作が、このロックの中で状態を読む・変える。複数スレッドから同時に呼べる。ロックの保持中に `.await` しない。適合データに同時実行の場面はないので、`lib` の単体試験が唯一の確認である。複数スレッドから同じ `seq_nr` を追記して 1 件だけが確定すること、飛び番が契約違反になること、別の集約の履歴が消えないことを確かめる（7.2 節の PR 8・PR 9） |
| MEM-5 | 記録の索引は `AidString` の完全一致（`HashMap<AidString, AggregateRecord>`）とする。ハッシュだけで識別せず、前方一致で選ばない。aid と seq_nr は別の値で、記録の中では seq_nr を別のキーにする。T-9・T-11・T-12・T-13 の検査は共通の層が済ませる |
| MEM-6 | 保存値は、メタデータ（`seq_nr`・`occurred_at`・`manifest`）と、直列化した payload のバイト列に分けて持つ。書き込み時にシリアライザで直列化し、取得時に復元する。利用者の入力や取得結果への変更が、保存値に波及しない。`A`・`P` に `Clone` を要求しない（T-6）。シリアライザはストアのインスタンスが持つ。`MemoryStorage` は持たない。別のシリアライザを持つストアが同じ `MemoryStorage` を共有してよく、復元できなければ `Serialization` を返す（2.12 節） |
| MEM-7 | 3.2 節 |
| MEM-8 | 3.3 節 |
| MEM-9 | 3.3 節 |
| MEM-10 | 3.4 節 |
| MEM-11 | 3.4 節 |
| MEM-12 | 保持件数があるときの `RetentionMode::Ttl` は `MemoryStorage::new` で設定エラーにする。保持件数がないときは、方式を使わないので無視する（エラーにしない）。TTL の印・猶予・時計を持たない |
| MEM-13 | 変更フィードを提供しない。変更フィードを要求する設定項目を作らない（10 章 Q-7、回答済み） |

### 3.2 書き込み（MEM-7・H-1・H-2・W-3〜W-9）

確定の方式は、「準備してから、失敗しない公開をまとめて行う」とする。

```text
共通の層
  1. aid の組み立て、seq_nr・時刻・W-6・W-9 の検査          （契約違反）
保存先（排他制御の前）
  2. payload とスナップショットの直列化                      （直列化。ここで失敗しても記録は変わらない）
保存先（排他制御の中）
  3. ロックを取る
  4. ヘッドを aid の完全一致で読む
  5. 照合する                                              （3.2.1）
  6. 新しい記録を、ローカルの値として準備する
  7. 確定前のフック点（commit。試験だけ）                   （失敗しても記録は変わらない）
  8. 準備した記録を、状態へまとめて公開する                 （ここが確定。この手順は失敗しない代入だけ）
  9. 保持処理を行う                                        （失敗しても確定を取り消さない。3.4 節）
 10. ロックを外す
共通の層
 11. 保持の失敗があればログで知らせる                        （MEM-11）
 12. 成功を返す
```

- 手順 6 までは、状態を変えない。手順 8 の公開は、ジャーナル・ヘッド・現在のスナップショット・履歴を 1 回のロックの中で更新する。別の操作から途中状態は見えない（H-1）。
- 手順 8 は、メモリの追加と置き換えだけで、失敗する処理を含まない。準備中の失敗は、ヘッド・ジャーナル・スナップショットのどれにも変更を残さない（MEM-7）。
- 照合はヘッドの seq_nr だけに対して行う（H-2）。スナップショットは使わない。

#### 3.2.1 照合（W-3・W-4・W-7・W-8）

ヘッドの seq_nr を `h` とする。ヘッドがなければ `h = 0` とみなす。

| 条件 | 結果 | 規則 |
|:--|:--|:--|
| `event.seq_nr == 1` かつヘッドがない | 新規作成。ジャーナルとヘッドを作る。スナップショットがあれば現在の項目も作る | W-3 |
| `event.seq_nr == 1` かつヘッドがある | 楽観ロック | W-3 |
| `event.seq_nr == h + 1`（`h ≥ 1`） | 更新。ヘッドを進める | W-4・W-8 |
| `event.seq_nr ≤ h`（`h ≥ 1`） | 楽観ロック（重複・古い番号） | W-7・W-8 |
| `event.seq_nr ≥ h + 2` | 契約違反（飛び番。ヘッドがない場合も含む） | W-8 |

- イベントだけの `seq_nr = 1` も新規作成できる（MEM-7）。スナップショットなしでヘッドを作る。現行はこれを拒む。
- 楽観ロックのエラーは、aid 文字列・追記しようとした seq_nr・分かればヘッドの seq_nr だけを持つ（E-2）。メモリはヘッドが分かるので、ヘッドの seq_nr を載せる。
- 飛び番の契約違反は、規則 `W-8` と `event.seq_nr` を持つ（E-3）。

### 3.3 読み取り（MEM-8・MEM-9・R-1〜R-8）

- `get_latest_snapshot_by_id`: ロックの中で、ヘッドと現在のスナップショットを同じ時点で読む（MEM-8）。ヘッドがなければ `None`（R-1）。ヘッドがあれば、封筒（なくてもよい）とヘッドの seq_nr の組を返す（R-2・R-3）。原子的なので、封筒の seq_nr がヘッドを上回ることはない（R-8、MEM-8）。
- `get_events_by_id_since_seq_nr`: ロックの中で、条件に合う封筒を全件、同じ時点で取り出す（MEM-9）。ロックを外してから復元する。復元の失敗は `Serialization`（R-5・R-6）。封筒の `aggregate_id` は、呼び出し側が渡した元の値の複製である（2.8 節）。記録に元の値を持たない。
- 取り出した値は、直列化したバイト列の複製である。ロックを外してから復元するので、復元の時間が他の操作を止めない。復元した値は呼び出し側のものであり、保存値に波及しない（MEM-6）。
- 2 つの公開操作を続けて呼んでも、同じ時点を読む保証はない。メモリのプロファイルもこれを認める。

### 3.4 保持処理（MEM-10・MEM-11・S-1〜S-4）

| 観点 | 方針 |
|:--|:--|
| 履歴を持つ条件 | 保持件数が `Some(n)` のとき、スナップショットを伴う書き込みで履歴を 1 件足す。`None` なら現在のスナップショットだけ |
| 時機 | 追記の確定の後、同じ排他制御の中で行う。イベントだけの追記でも行い、前回取り残した履歴を片付ける（MEM-10）。DynamoDB と時機が違う（D-9）。保存先の `append` の中で決めるので、共通の層に保持の時機を持たせない |
| 選択 | 「見える履歴を取る」「今書いた履歴を加える（重ねない）」「降順に並べる」「新しい n 件を残し、それより古いものを選ぶ」の 4 手順を、純粋な関数 `select_expired_history` にする。DynamoDB と共有する（S-2）。メモリでは見える履歴が常に正確だが、手順を同じにして適合データの `history_pages` を両方で再現する |
| 削除 | 選んだ履歴を、論理履歴から取り除く。ジャーナルとヘッドは取り除かない（MEM-10） |
| 失敗 | 保持処理が失敗しても、確定を取り消さない。`AppendReceipt` に失敗を載せる（S-4） |
| 通知 | ロックを外した後、共通の層が `tracing` のログで知らせる（MEM-11、2.11 節） |
| 再試行 | 専用の再試行は持たない。次の追記の後の保持処理が、現在の履歴を読み直して片付ける（MEM-11） |

通常のメモリの保持処理は、失敗する処理を含まない。保持の失敗は、試験で差し込んだときだけ起きる（5.6 節）。

### 3.5 試験のためのフック点（`test-hooks` feature）

試験が確定前の障害・保持の障害・履歴の観測を行うため、保存先に次の差し込み点を置く案とする。cargo の feature `test-hooks`（既定で無効、`#[doc(hidden)]`）で囲み、通常の利用者の API に出さない。

| フック点 | 呼ぶ場所 | 用途 |
|:--|:--|:--|
| `before_commit(aid, seq_nr)` | 3.2 節の手順 7 | `commit` の障害。失敗を返せば、記録は変わらない |
| `read_events(aid)` | 3.3 節の取り出しの前 | `read-events` の障害 |
| `read_snapshot(aid)` | 3.3 節の取り出しの前 | `read-snapshot` の障害 |
| `retention_visible_history(aid)` | 3.4 節の「見える履歴を取る」 | `retention-query` の障害と、`history_pages` による見える履歴の置き換え |
| `retention_delete(aid, seq_nrs)` | 3.4 節の削除の前 | `retention-delete` の障害、`final-retention-failure` |
| `history_view(aid)` | 試験の観測 | `observe.history`（印のない履歴の集合。メモリには印付き履歴がない） |

- フックを足す時機: `before_commit`・`read_events`・`read_snapshot`・`history_view` は PR 8、`retention_visible_history`・`retention_delete` は PR 9 で足す（7.2 節）。
- フックは、書き込みの成功・失敗を変えない。失敗を返す指示は、障害を差し込む場面だけが出す。
- `serialize-*`・`deserialize-*` の障害は、フックを使わない。実行器がシリアライザを包み、指定の回数目で失敗させる（`with_serializers`、5.6 節）。
- フックの有無で本番の処理順を変えない。フックが `None` のとき、各手順は同じ順序で動く。

### 3.6 現行との差の対応

| 現行（確認した事実） | 次のメジャー | 規則 |
|:--|:--|:--|
| `SnapshotEnvelope::version` で照合する | ヘッドの seq_nr で照合する | H-2・W-8 |
| ヘッドを持たない | ヘッド（seq_nr と最後のイベント封筒）を持つ | H-4 |
| 新規作成は `persist_event_and_snapshot` だけ | イベントだけでも新規作成できる | W-3・MEM-7 |
| 不在の集約への更新は楽観ロック | 契約違反（飛び番） | W-8 |
| `Clone` で複製して保存・返却する | 直列化して保存し、取得時に復元する | MEM-6・T-6 |
| 保持処理がない | 確定後の保持処理 | MEM-10 |
| ロックの失敗（ポイズニング）を文字列のエラーにする | `Storage` の分類で返す。panic しない | 4 章 |

## 4. DynamoDB の実装方針

DynamoDB のプロファイル（`dynamodb.md`）を満たす。確認した現行の状態（`lib/src/event_store_for_dynamodb.rs`）は次のとおり。

- 2 テーブル（journal・snapshot）と、`pkey`（シャード）・`skey`（文字列）のキーを使う。
- 楽観ロックは snapshot 項目の `version` 属性の条件つき更新である。
- イベントは旧 journal の GSI を `Query` で読む。ページ送りは `LastEvaluatedKey` で続ける。
- 保持処理は、件数を数えてから超過分を選ぶ方式である。書き込みの失敗として返す。
- 失敗は `TransactionCanceledException` に理由があれば、すべて `OptimisticLockError` にする。

書き直しの範囲は大きい。キー・属性・テーブル構成・照合・読み取り・保持のすべてが変わる。

### 4.1 使う SDK とその版

| 項目 | 内容 |
|:--|:--|
| SDK | `aws-sdk-dynamodb`（公式の AWS SDK for Rust）。`aws-config` は、生成関数が `Client` を受け取るので、ライブラリ本体の必須の依存にしない案（現行は feature `dynamodb` の依存） |
| 現行の指定 | ワークスペースの `aws-sdk-dynamodb = "1.23.0"`、`aws-config = "1.2.1"`（`Cargo.toml`）。確認した事実 |
| 設計が前提にする API | `aws-sdk-dynamodb 1.130.0` のソースで存在を確認した。`Put`・`Update` の `return_values_on_condition_check_failure`、`CancellationReason` の `code()`・`item()`、`Config::Builder` の `interceptor`・`http_client`・`retry_config`、`Intercept` trait の `read_before_transmit`・`modify_before_transmit`。確認は、ローカルに取得済みのソースを読んだだけである |
| 最小の版 | 上の API が 1.23.0 にあるかは、確認していない。実装時に、最小の版を確かめる。確かめられなければ、下限を 1.130.0 へ上げる |
| 解決済みの版 | `Cargo.lock` がないので、確認していない。この文書は、候補の版を解決済みの版として扱わない |
| 最小サポートの Rust の版（MSRV） | 現行の `rust-version = "1.94.1"`（`lib/Cargo.toml`）を変えない。AWS SDK が要求する版による（確認した事実） |
| 追加する依存 | ストアの識別子（`store_id`）を作る乱数。`uuid`（v4）を `dynamodb` feature の依存にする案。版は実装時に決める |
| Streams の読み取り | ライブラリは Streams を読まない。変更フィードの関数（ヘッド遷移の組み立て・再同期の補助）を含めない（1.4 節。オーナーの決定、2026-10-06）。`aws-sdk-dynamodbstreams` は依存にしない |

### 4.2 3 テーブルの配置（DY-2・DY-3・DY-16・DY-17・DY-18・DY-19）

テーブルの作成はライブラリの外で行う。名前は `DynamoDbTables` で与える。

| テーブル | パーティションキー | ソートキー | GSI | Streams | TTL |
|:--|:--|:--|:--|:--|:--|
| journal | `aid` (S) | `seq_nr` (N) | なし | 無効 | なし |
| snapshot | `aid` (S) | `skey` (N) | 履歴用（4.6 節）。キーは `(aid, active_history_seq_nr (N))`、射影は `KEYS_ONLY` | 無効 | 属性 `ttl`。TTL 方式のときだけ有効（DY-2） |
| head | `aid` (S) | なし | なし | 有効、`NEW_IMAGE`（DY-3・DY-12・D-4） | なし |

- パーティションキーは `AidString` の文字列そのものにする。論理シャードもハッシュも使わない（DY-16）。
- journal のソートキーは `seq_nr`。snapshot の `skey` は、現在が 0、履歴がその履歴の `seq_nr`（DY-17）。
- 3 テーブルは同じリージョンに置く。書き込みもそのリージョンからだけ行う（DY-18）。グローバルテーブルは範囲外である。ライブラリは、リージョンの違いを検出しない（検出する手段を確認していない）。3 テーブルが同じ `Client` を通ることを、API の形で保証する（10 章 Q-13）。
- 読み取りは強整合で行う。保持処理の GSI の読み取りだけが結果整合である（DY-18）。
- 使う API は、1 つの集約の aid に絞った `GetItem`・`BatchGetItem`・`Query`・`TransactWriteItems`・`BatchWriteItem`・`UpdateItem` だけである。`Scan` を使わない（DY-19）。`Scan` を使うのは、移行ツール（8 章）だけである。

#### 4.2.1 項目の属性（`dynamodb.md` の 5 章）

| 項目 | 属性（型） |
|:--|:--|
| ジャーナル | `aid` (S)、`seq_nr` (N)、`occurred_at` (N、エポックナノ秒)、`manifest` (S)、`payload` (B) |
| スナップショット（現在） | `aid`、`skey` (N、0)、`seq_nr` (N)、`manifest` (S)、`payload` (B)、`last_updated_at` (N、`occurred_at` のミリ秒) |
| スナップショット（履歴） | 現在と同じ属性（`skey` は履歴の `seq_nr`）。印がない間だけ `active_history_seq_nr` (N)。TTL の印を付けると `ttl` (N、エポック秒) を持ち、`active_history_seq_nr` を失う |
| ヘッド | `aid` (S)、`type_name` (S)、`seq_nr` (N)、`events` (L)。`events` の要素は M で、`seq_nr`・`occurred_at`・`manifest`・`payload` を持つ。要素数は 1（H-3） |
| 設定項目 | `aid` = `__config__`、`store_id` (S)、`layout_version` (N)。journal は `seq_nr` = 0、snapshot は `skey` = 0、head はソートキーなし |

- 現在のスナップショットは、`active_history_seq_nr` も `ttl` も持たない。期限のない項目は `ttl` 属性自体を持たない（現行は 0 を書く。D-3）。
- `occurred_at` は、`DateTime<Utc>` からエポックナノ秒（`timestamp_nanos_opt()`）を作り、10 進の数値（N）で書く。範囲外は書き込みの前に契約違反（T-13）である。
- `manifest` は常に書く。省略時は空文字列である（T-2・T-10）。
- 属性名と型は、`conformance/dynamodb/layout.json` と `item-shapes.json` の期待と一致させる（IP 5 の 2）。

### 4.3 設定項目と照合（DY-8）

`EventStoreForDynamoDB::open` が、次の手順を行う。

```text
1. 3 つの設定項目のキーを 1 回の BatchGetItem に入れる（各テーブル ConsistentRead = true）
     journal:  (__config__, seq_nr = 0)   snapshot: (__config__, skey = 0)   head: (__config__)
2. Responses を、テーブル名とキーごとに蓄積する
3. UnprocessedKeys があれば、そのキーだけを、指数バックオフで強整合のまま再要求する
     未処理がなくなるまで、「存在しない」と判定せず、次の手順に進まない
     再要求の回数が上限（unprocessed_retry_limit）に達したら、保存先エラー（設定エラーではない）
4. 3 つの結果で場合分けする
     (a) 3 つともない        → 新しい store_id を作る。5 へ
     (b) 3 つともあり、store_id が一致し、layout_version が自分の版（1）と同じ → 生成を続ける
     (c) それ以外（一部だけ、store_id の食い違い、layout_version の違い）    → 設定エラー
5. 1 つの TransactWriteItems で、3 つの設定項目を attribute_not_exists(aid) の条件つき Put で書く
     条件が成り立たず失敗したら、別の生成が先に書いた。応答を捨て、1 から 3 をやり直して 4 へ
     取り消し理由が TransactionConflict（別の生成が書いている最中）のときも、応答を捨て、1 から 3 をやり直して 4 へ。
     ただし、やり直した 4 が (a)（3 つともない）なら、作成を繰り返さずに保存先エラー（DY-8、P-44）
     成功したら生成を続ける
```

| 観点 | 設計 | 規則 |
|:--|:--|:--|
| 読み取り | 3 件を 1 回の `BatchGetItem` にする。`ConsistentRead = true`。再要求も強整合 | DY-8 |
| 部分応答 | `UnprocessedKeys` のキーだけを再要求する。応答にない項目を、未処理が残る間は「存在しない」と扱わない | DY-8 |
| 再要求の上限 | `DynamoDbOptions` の `unprocessed_retry_limit`（初回を数えない回数。既定は 10。0 を許す）。仕様は値を定めない。上限到達は `Storage`（`dynamodb-config-retry-exhausted`） | DY-8 |
| バックオフ | 指数。k 回目の前に min(初期の待ち × 2^(k−1), 上限の待ち)。初期の待ちは 50 ミリ秒、上限の待ちは 2 秒、揺らぎはない（2.12.1 節）。待ちは `RetrySleeper` で行い、実行器が短縮できる。`Clock` とは別である（5.5 節） | DY-8 |
| 作成 | 3 件を 1 つの `TransactWriteItems` で、条件つき `Put` だけで書く。独立した `ConditionCheck` を使わない | DY-8（P-19） |
| 競合 | 条件不成立なら、応答を捨てて 3 件を強整合で読み直す。その結果で、場合分けをやり直す | DY-8（P-19） |
| 不一致 | 一部のテーブルだけにある、`store_id` が食い違う、`layout_version` が違う場合は、`Configuration` を返して生成を止める | DY-8、4 章（P-40） |
| 権限 | 3 テーブルへの `dynamodb:BatchGetItem` と `dynamodb:PutItem` | DY-8 |
| `store_id` | 最初の生成でランダムに作る。3 項目に同じ値を書く | DY-8 |
| 設定項目の属性 | `store_id` と `layout_version` だけ。snapshot の設定項目は `active_history_seq_nr` を持たない | DY-8 |

- `open` は、ストアを作ったあとの処理で設定項目を読み直さない。設定の食い違いは、生成時にだけ検出する。
- 設定項目の `aid`（`__config__`）は `-` を含まないので、`AidString`（必ず `-` を含む）と衝突しない（T-11）。

### 4.4 書き込み（D-5・D-6・D-7・W-8・H-1）

1 回の書き込みは、1 つの `TransactWriteItems` である。トランザクションが成功した時点が確定である（H-1）。

#### 4.4.1 トランザクションの構成

| 順（案） | アクション | 対象 | 条件 | 含める場合 |
|:--|:--|:--|:--|:--|
| 1 | `Put` | journal | `attribute_not_exists(aid)` | 常に |
| 2 | `Put` | head | `attribute_not_exists(aid)` | `event.seq_nr == 1`（新規作成） |
| 2 | `Update` | head | `seq_nr = :prev`（`:prev` = `event.seq_nr − 1`）。`seq_nr` と `events` を上書き | `event.seq_nr > 1` |
| 3 | `Put` | snapshot（現在、`skey` = 0） | なし | スナップショットがある |
| 4 | `Put` | snapshot（履歴、`skey` = `seq_nr`） | なし | スナップショットがあり、保持件数が `Some(n)` |

- 最大 4 アクションで、`TransactWriteItems` の上限（100 アクション、合計 4 MB）に収まる。
- head の `Put` と `Update` に、`ReturnValuesOnConditionCheckFailure = ALL_OLD` を付ける（D-5）。
- 順は、ライブラリが要求を組み立てる順である。仕様は順を強制しない。取り消し理由は、要求内のアクションの位置で読む（4.4.2 節）。
- 現行の「現在のスナップショットを version の条件つき更新で書く」方式をやめる。現在のスナップショットは、条件なしの `Put` である。ヘッドの条件が直列化する（`dynamodb.md` の 6.1 章）。
- スナップショットの `seq_nr` は、`event.seq_nr` と等しい（W-9）。検査は共通の層が済ませる。

#### 4.4.2 失敗の分類（D-5・D-6・W-3・W-7・W-8）

`TransactionCanceledException` の `CancellationReasons` を、要求内のアクションの位置で読む。追加の読み取りをしない（D-5）。

| 理由の読み方 | 分類 | 規則 |
|:--|:--|:--|
| 新規作成で、head が `ConditionalCheckFailed` | 楽観ロック（ヘッド番号は不明） | W-3 |
| 更新で、head が `ConditionalCheckFailed` かつ旧項目の `seq_nr` を `h` とする。`event.seq_nr ≤ h` | 楽観ロック（`head_seq_nr = h`） | W-8 |
| 同上。`event.seq_nr ≥ h + 2` | 契約違反（規則 `W-8`、飛び番） | W-8 |
| 同上。旧項目が返らない | `h = 0` とみなす。更新の `seq_nr ≥ 2` なので契約違反（飛び番） | W-8（D-5） |
| journal が `ConditionalCheckFailed` | 楽観ロック | W-7 |
| いずれかの項目が `TransactionConflict` | 楽観ロック | D-6 |
| 上のどれにも当たらない取り消し（`ProvisionedThroughputExceeded`・`ThrottlingError` など）、通信の失敗、その他 | 保存先 | `dynamodb.md` の 6.2 章 |

- 取り消し理由が複数の項目に付いたときは、どれかの項目に `TransactionConflict` があれば楽観ロック（D-6）にする。なければ、head の `ConditionalCheckFailed`（W-3・W-8 の判別）、journal の `ConditionalCheckFailed`（W-7）、その他（保存先）の順に見て、最初に当たったもので分類する（`dynamodb.md` 6.2、P-43。2026-10-06 オーナー決定）。`conformance/README.md` の、いずれの項目の `TransactionConflict` も D-6 とする記述と同じ扱いである。適合データの取り消し理由は、`None` でない要素がどのケースも 1 つだけなので、この順は `lib` の単体試験で確かめる。
- 取り消し理由の `code` の文字列と `item` の取り出しは、`CancellationReason` の公開メソッドを使う。SDK のエラーの表示文字列からは判別しない。
- 更新で、旧ヘッドが `event.seq_nr − 1` と等しいのに条件が不成立になることは、起こらない。起きたら黙って成功や楽観ロックとせず、`Storage`（想定外の取り消し）で返す案。
- 楽観ロックのメッセージは、aid 文字列、追記しようとした `seq_nr`、分かればヘッドの `seq_nr` だけを含む（E-2）。SDK の生のエラー文を含めない。
- 契約違反のメッセージは、規則 `W-8` と `event.seq_nr` を含む（E-3）。

#### 4.4.3 項目サイズの検査（D-7）

- 書き込みの前に、送る項目の大きさを見積もる。journal 項目、head 項目、snapshot 項目（現在・履歴）のすべてが対象である。payload は journal と head の両方に載る。
- 上限は 409600 バイト（400 KB）。超える見積もりは、`ContractViolation`（`ContractRule::ItemSizeLimit`）で返す。送信しない（H-1）。
- 見積もりは、属性名と値の大きさの合計を、AWS が公開している項目サイズの数え方に従って計算する関数 1 つにする。境界ちょうどの正確さは求めない（`conformance/README.md`）。
- 数える属性に、manifest を含める。イベントの manifest（journal 項目と、head 項目の `events[0]`）と、スナップショットの manifest（現在・履歴の項目）である。manifest は利用者が決める文字列であり、大きさに上限がない。
- 見積もりは、実際の大きさ以上（上界）にする。数値の属性は、AWS が示す数値の大きさの上限で数える。リストとマップは、外枠の大きさを足す。境界の近くでは、実際には上限内に収まる項目を拒むことがある。
- `dynamodb-item-size-head-overhead` は、journal が上限内で head が上限を超えるケースである。head は、`type_name`・`events` の属性名と値も数える。
- 見積もりが上界なので、DynamoDB が項目サイズの検証エラーを返すことは起きない。起きたら、`Storage` で返す（10 章 Q-4）。

#### 4.4.4 書き込みの後（D-9・S-4）

- トランザクションが成功したら、履歴のスナップショットを書いたときだけ、保持処理を行う（D-9）。イベントだけの追記、保持件数がないスナップショットの書き込みでは、保持処理を行わない。
- 保持処理の失敗は、書き込みの結果を変えない（S-4）。4.6 節。

### 4.5 読み取り（DY-9・DY-10・DY-11・R-1〜R-8）

#### 4.5.1 最新のスナップショット

```text
1. 1 回の BatchGetItem で、head（aid）と snapshot（aid, skey = 0）を読む。両方 ConsistentRead = true
2. UnprocessedKeys があれば、読み切るまで再要求する（DY-9）
3. head がなければ None（R-1）。snapshot があっても None（dynamodb-no-head-with-snapshot）
4. head があれば、SnapshotRead { snapshot: 封筒または None, head_seq_nr } を返す（R-2・R-3）
```

- 2 項目の読み取りは原子的でない（R-8）。`TransactGetItems` は使わない（P-25）。2 項目の間に別の書き込みが確定すると、スナップショットの `seq_nr` がヘッドを上回ることも下回ることもある。どちらも整合性は崩れない。
- `UnprocessedKeys` の再要求の上限は、DY-9 に定めがない。DY-8 と同じ再要求の方針（2.12.1 節）を使い、上限到達を `Storage` にする（10 章 Q-6）。
- 段階的に入れる。PR 12 は、再要求を持たず、`UnprocessedKeys` が残ったら `Storage` を返す最小の読み取りである。PR 13 で、再要求を足す（7.2 節）。
- 属性の欠落・型の不一致は、`Storage`（データの欠損）で返す。黙って無視しない（4 章）。
- スナップショットの復元の失敗は `Serialization`（`deserialize-snapshot`）。

#### 4.5.2 イベント

```text
journal を Query する
  KeyConditionExpression: aid = :aid AND seq_nr >= :seq_nr
  ConsistentRead = true、昇順（ScanIndexForward = true）
LastEvaluatedKey が返る間は、ExclusiveStartKey に渡して読み続ける
読み切ってから、封筒の列として返す（DY-11・R-4・R-5・R-6）
```

- 旧 journal の GSI を使わない。journal 本体を強整合で読む（P-29）。読み取りを始めた時点で確定していたイベントがすべて含まれる（R-5）。
- 各項目の `seq_nr`・`occurred_at`・`manifest`・`payload` と、呼び出し側が渡した元の `aggregate_id`（`load_events` の `&AID` の複製。2.8 節）で封筒を作る。保存した `aid` は、要求の aid 文字列と一致することの確認にだけ使う。属性の欠落は `Storage`。`aid` が要求と一致しない項目も `Storage` で返す（現行の確認と同じ）。
- 1 MB を超える読み取りの適合ケースは、`dynamodb-events-over-one-megabyte` である。ページ送りで読み切る。

### 4.6 スナップショットの保持処理（`dynamodb.md` 8 章・D-3・D-9・P-18・P-24・S-1〜S-4）

保持件数 `n` が `Some(n)` のときだけ、履歴を書いた書き込みの確定後に行う（D-9）。

```text
1. 履歴用の GSI を Query する        aid = :aid、ScanIndexForward = false、KEYS_ONLY
   LastEvaluatedKey が返る間は読み続ける。印のない履歴の seq_nr の列を得る
2. 今書いた履歴の seq_nr を加える（すでに見えていれば重ねない）
3. 降順に並べ、先頭 n 件を残す。それより古いものを、取り除く対象にする（S-2）
4. 方式で分ける
   Delete: BatchWriteItem の DeleteRequest を 25 件ずつに分けて送る（P-18）
           UnprocessedItems は再送する
   Ttl:    対象を 1 件ずつ UpdateItem する
           SET #ttl = :expires REMOVE active_history_seq_nr
           条件 attribute_exists(active_history_seq_nr)
           #ttl は ExpressionAttributeNames の別名。:expires は、印を付ける時点のエポック秒 + ttl_grace_seconds
           条件不成立（ConditionalCheckFailed）は「印付け済み」として読み飛ばす
```

| 観点 | 設計 | 規則 |
|:--|:--|:--|
| 選択 | `select_expired_history`（3.4 節。メモリと共有）の純粋な関数。入力は「見える履歴」「今書いた履歴」「`n`」 | S-2 |
| 件数を数えて超過分を選ぶ方式 | 使わない。GSI は結果整合で、書いた直後の履歴を数えられない。処理が重なると、新しい `n` 件まで取り除きうる | P-24 |
| 印付き履歴 | GSI に載らないので、件数に数えない。次に印を付ける対象にも選ばない。期限を先送りしない | S-3 |
| 期限の計算 | 印を付ける時刻（時計）+ 猶予。イベントの `occurred_at` から計算しない | S-3 |
| 25 件の分割 | `BatchWriteItem` の上限に合わせる。未処理の項目を再送する。再送は 2.12.1 節の方針（`unprocessed_retry_*`）に従い、上限に達したら保持の失敗にする | P-18 |
| 失敗 | 3 つの段階（query・delete・mark）の失敗は、`AppendReceipt` の `retention_failure` に載せる。書き込みは成功を返す | S-4 |
| 取り残し | 次の保持処理で、現在の GSI の内容から選び直して片付ける。反映の遅れで見えなかった分は多めに残り、次の処理で片付く | S-4・P-24 |

- 現行は、件数を数えてから超過分を選び、失敗を書き込みの失敗として返す（確認した事実）。IP 2.2 は、これが新しい履歴を消す不具合の原因と記す。
- 時計は `Clock`（現在のエポック秒を返す）の trait とし、既定は実時間である。試験が差し替える（5.5 節、`test-hooks` feature）。

### 4.7 SDK の差し込みの仕組みの置き場所（IP 6）

| 場所 | 内容 | 置き場所 |
|:--|:--|:--|
| ライブラリ本体 | 差し込みの仕組みを持たない。`EventStoreForDynamoDB::open` は、呼び出し側が作った `aws_sdk_dynamodb::Client` を受け取る。したがって、`Client` の設定が唯一の差し込み点になる | `lib/src/dynamodb/` |
| 要求の観測 | `Config::builder().interceptor(...)`。`Intercept` trait の `read_before_transmit`（送信直前に、要求の API 名・本文を読む）を使う。観測は要求を変えない | 適合実行器（5.5 節） |
| 要求と応答の差し替え | `Config::builder().http_client(...)`。SDK が送る HTTP の要求を受け、実際の DynamoDB Local へ転送するか、作った応答を返す（`replace-request`・`replace-response`） | 適合実行器 |
| 自動再試行の無効化 | `Config::builder().retry_config(RetryConfig::disabled())`。`sdk-error` の障害が、再試行で消えない | 適合実行器 |
| 保持処理の要求だけの失敗 | HTTP の層で、要求の内容（`Query` で GSI を読む、`BatchWriteItem`、`UpdateItem` の `#ttl`）から保持処理の要求を判別して失敗させる | 適合実行器 |

- `Intercept` の `modify_before_transmit` は、`Err` を返して要求を止められる。ただし、止めた失敗は、型つきのサービスのエラー（`TransactionCanceledException` など）にならない。`TransactionCanceledException` と `CancellationReasons` は、HTTP の層で作った応答を SDK に解釈させる。理由は 5.6 節。
- ライブラリ本体は、`tracing` の span 以外の観測の仕組みを持たない。現行は `tracing::Instrument` を使う。これを引き継ぐ。

### 4.8 現行との差の対応

| 現行（確認した事実） | 次のメジャー | 規則 |
|:--|:--|:--|
| journal・snapshot の 2 テーブル | journal・snapshot・head の 3 テーブル | DY-16 |
| `pkey`（シャード）・`skey`（文字列） | `aid`・`seq_nr`/`skey`（数値） | DY-16・DY-17 |
| `KeyResolver`・シャード数 | なし | DY-16 |
| `version` の条件つき更新 | head の `seq_nr` の条件つき更新 | H-2・W-8 |
| すべての取り消しを楽観ロックにする | 理由で分類する（D-5・D-6） | W-3・W-7・W-8 |
| 項目サイズの検査がない | 書き込み前の見積もり（D-7） | D-7 |
| 設定項目がない | 3 テーブルの設定項目と照合 | DY-8 |
| 旧 journal の GSI で読む | journal 本体を強整合で読む | DY-11 |
| 現在のスナップショットの GetItem | head と現在の BatchGetItem | DY-9 |
| 保持は件数を数えて選ぶ。失敗は書き込みの失敗 | 疎な GSI で選ぶ。失敗は別経路 | D-3・D-9・P-24・S-4 |
| `ttl` に 0 を書く | `ttl` 属性自体を持たない | D-3 |

## 5. 適合テストデータの実行器

適合テストデータ（`conformance/`、版 1.0.0）を読み、メモリと DynamoDB（DynamoDB Local）で全ケースを実行する実行器を作る（IP 5 の 1）。要件は `conformance/README.md`（CR）である。実行器は規則を足さない。

確認した事実:

- 配布の内容は、値 30 件（aid 9・時刻 11・seq_nr 6・FNV-1a 4）、場面 85 件（共通 42・DynamoDB 43）、配置 1 件である。ファイルごとの件数を数えて、README の記載と一致することを確認した。
- 場面の `store` の設定名は、`retention_count`・`retention_mode`・`ttl_grace_seconds`・`retry_limit` の 4 つだけである。
- 障害の段階（`phase`）の列挙は 12 種類である（`conformance/schema/common.schema.json`。10 章 Q-1）。
- このリポジトリの `tools/conformance/` には `manifest.py` だけがある。`validate.py`・`data.py`（generators の参照実装 `materialize`）・`uv.lock` はない。配るのは `conformance/` と `manifest.py` だけである（10 章 Q-11）。
- `conformance/.gitattributes`（`* -text`）は、すでにある。`conformance/manifest.json` にも載っている。
- generators の展開は、README の定めのとおり、実行器が自前で作る（5.2 節）。

### 5.1 置き場所

新しい workspace member `conformance-runner/`（crate 名 `event-store-adapter-conformance-rs`、`publish = false`）を作る。決定（コーディネーター、2026-10-06）。理由: 報告の JSON を出す実行ファイルにできる。`lib` の dev-dependency に、AWS や Docker の重い依存を増やさず、`lib` の依存を軽く保てる。

```text
conformance-runner/
  src/
    main.rs            引数（--backend memory|dynamodb、--data conformance、--report <path>、--require-all）
    data.rs            JSON の読み込み、manifest の照合、generators の展開
    number.rs          任意精度の整数（i128）と時刻の変換
    compare.rs         JSON の比較（payload・集約状態）
    runner.rs          場面の実行手順
    target_memory.rs   メモリの生成・フック
    target_dynamodb.rs DynamoDB の生成・テーブル作成・SDK 差し込み
    observe.rs         履歴・通知・要求・属性・配置の観測
    report.rs          報告
```

- 依存は、`event-store-adapter-rs`（features `dynamodb`・`test-hooks`）、`event-store-adapter-test-utils-rs`（DynamoDB Local の起動）、`serde_json`、`sha2`、`tracing-subscriber` を使う案。`test-hooks` は PR 8 で足す（3.5 節）。PR 4 の骨格は、`test-hooks` を使わない。
- 保存先ごとの差は `target_*` に閉じる。場面を実行する部分（`runner.rs`）は、保存先を知らない。
- 現行の `lib/src/event_store_test_support.rs` の共有シナリオ（`exercise_user_account_flow`）は、適合データが置き換える部分と、ドメインの例として残す部分がある。扱いは 7 章の切替の PR で決める。
- 実行器は `lib` の中の `#[cfg(test)]` にしない。`pub(crate)` を超えるフックを使うためと、`cargo test` と別に、報告の JSON を出す実行ファイルが要るためである。

### 5.2 データの読み方

| 要件（CR） | 設計 |
|:--|:--|
| UTF-8 の JSON、最上位に `format`・`version` | 読み込み時に `format` と `version`（`1.0.0`）を確かめる。違えば読み込みを失敗にする |
| 重複キー・NaN・Infinity の禁止 | `serde_json` は NaN・Infinity を JSON として受け付けない。重複キーは、`Deserialize` を自作してキーの出現を数え、検出したら読み込みを失敗にする |
| 文字列値と属性名を書き換えない | 読み込んだ文字列を、そのまま使う。正規化・大文字小文字の変換をしない |
| 任意精度の整数 | JSON の数値を `serde_json::Number`（`arbitrary_precision`）で読み、`i128` に変換する。`seq_nr` の `-1` や `2^53` を、`u64` へ直接デコードしない。`u64` への変換は、操作を呼ぶ直前に試す。`representation.signed_seq_nr = true` の印があるケースだけ、変換できなければ対象外（理由: 表現不能）と報告する。印のないケースで変換に失敗したら、データの誤りか実行器の誤りなので、`failed` にする |
| `epoch_nanoseconds` | 10 進文字列を `i128` として読み、整数だけで計算する。浮動小数点を使わない |
| 封筒の `occurred_at` | 9 桁の小数秒を持つ UTC の ISO 8601 文字列を、`DateTime<Utc>` に変換する |
| 時刻の精度の方針 | `precision_policy = native-time-type` の成功では、入力を標準時刻型（`DateTime<Utc>`）へ変換した値を期待値にする。`expect.value` は丸め前の参照値である。実行器は、変換した値と実際の値を報告する。`DateTime<Utc>` はナノ秒を表せるので、変換した値は入力と一致する |
| `representation.time_precision` | `nanoseconds` のケースを実行する。`milliseconds` のケースは、ナノ秒を表せる型を使うので実行しない。「対象外」（理由: 標準時刻型の精度に合わない選択）と報告する |
| `representation.signed_seq_nr = true` | `SeqNr = u64` は負数を表せない。実行せずに「対象外」（理由: 表現不能）と報告する。成功に集計しない。表現不能にするのは、この印のあるケースだけである |
| generators | `target`（ケースを根とする JSON Pointer。`~0`・`~1` を復号）、`character`（Unicode の 1 文字）、`byte_length`（UTF-8 の総バイト数）。対象は fixtures 内の空文字列だけで、各ポインタは 1 回だけ。`byte_length` が文字の UTF-8 幅で割り切れなければデータの誤りとして失敗にする。展開前の JSON でスキーマを検査し、展開後の値で操作を実行する。スキーマ検査の道具が配布にないことは、この章の冒頭で述べた（10 章 Q-11） |
| 400 KB と 1 MB | 409600 バイトと 1048576 バイトとして扱う（D-7・DY-11） |
| payload と集約状態の比較 | 既定の JSON シリアライザで直列化・復元した JSON 値を比較する。キー順と空白を無視する。配列の順序、`null`、真偽値、文字列、数値の値を保つ。真偽値と数値を同一視しない。Unicode 正規化をしない。`1` と `1.0` の書式の違いは要求しない |
| 配布の検証 | 実行器が `manifest.json` の版と、列挙された全ファイルの SHA-256 とファイル集合を照合し、結果を報告に載せる。CI は別に `python3 tools/conformance/manifest.py verify` と `manifest.json` の SHA-256 を照合する（現行の `lint` ジョブにある）。manifest を作り直して差分を隠さない |

payload の型は `serde_json::Value`、集約状態の型も `serde_json::Value` とする案。既定の JSON シリアライザの往復を検査する。

#### 5.2.1 値の表の操作

操作 `buildAid`・`validateOccurredAt`・`validateSeqNr`・`fnv1a64` は、公開 API の同名の関数を要求しない。実行器が次のように対応付ける。

| 操作 | 対応 | 規則 |
|:--|:--|:--|
| `buildAid` | 実行器が `AggregateId` を実装した試験用の型を持つ。`type_name()`・`value()` は入力の型名・値を返し、`user_string` は別の表現（`Display`）として持つ。`AidString::from_aggregate_id` を呼び、`as_str()` を比較する。異なる `user_string` が結果に影響しないことを確かめる。バイト数は型名・値・区切りの合計（`aid-multibyte-1025`） | T-1・T-11・T-12 |
| `validateSeqNr`（`context = value`） | 実行する保存先（メモリ・DynamoDB）のストアの `get_events_by_id_since_seq_nr(aid, seq_nr)` を呼ぶ。範囲内なら成功、`2^53` 以上は契約違反（T-9）になることを確かめる。0 は有効（全件を返す） | T-9 |
| `validateSeqNr`（`context = event`） | 封筒を作り、`persist_event` を呼ぶ。`seq_nr = 1` は成功、`0` は契約違反（W-6）。封筒の構築と公開 API の入口の検査を使い、実行器が同じ式を計算するだけにしない | T-9・W-6 |
| `validateOccurredAt` | 型名 `ConformanceTime`、値がケース ID の集約で、1 番から `event_seq_nr − 1` 番を時刻 `1970-01-01T00:00:00.123000000Z` で `persist_event` し、次に入力の時刻で `event_seq_nr` 番を書く（payload は空のオブジェクト、manifest は空文字列）。成功のケースは 1 番を読み戻して比較する。範囲外のケースは 7 番で契約違反（T-13）を比較する | T-3・T-13 |
| `fnv1a64` | DynamoDB とメモリはハッシュを使わない（ADR-0006）。最初のメジャーに共有のハッシュ実装はない。「対象外」（理由: 最初のメジャーにハッシュを使う保存先がない。オーナーの決定、2026-10-06。実装計画 5 章の受け入れ条件 1）と報告する。成功にも失敗にも数えない。段階 5 でハッシュを使う保存先を出すときに実行する。実行器の中に FNV-1a 64 を実装して計算することはしない（ライブラリの検査にならない） | K-1 |

値の表の `validateSeqNr`・`validateOccurredAt` は、保存先に依存する（書き込みと読み戻しを使う）ので、メモリと DynamoDB の両方で実行する。

### 5.3 場面の実行手順と、ストアを場面ごとに分ける方法

各場面は、独立したストアで実行する。他のケースの項目を使い回さない（CR）。

#### 5.3.1 ストアの分離

| 保存先 | 分離の方法 |
|:--|:--|
| メモリ | 場面ごとに `MemoryStorage::new` で空の保存先を作る |
| DynamoDB | 1 つの DynamoDB Local のコンテナを、実行全体で共有する。場面ごとに、3 テーブルと履歴の GSI の名前を実行器が割り当てる。名前は `cf-{実行の識別子}-{場面の通し番号}-{journal\|snapshot\|head}`、GSI は `cf-{実行の識別子}-{場面の通し番号}-history` とする案。実行の識別子は、実行ごとに乱数で作る。3 テーブルは同じリージョン・同じ `Client` に作る（DY-18）。場面が終わったらテーブルを削除する（失敗しても場面の結果を変えない） |

- テーブル名と GSI 名は、ライブラリの設定へ渡す値として実行器が決める。ライブラリは名前の形を仮定しない。
- テーブルは、`layout.json` の期待（キー・GSI・Streams）で作る。snapshot の TTL は、`retention_mode = ttl` の場面だけ有効にする（`layout.json` の `enabled_when = retention-mode-ttl`）。
- 作成後、`DescribeTable` で `ACTIVE` を待つ。

#### 5.3.2 手順

README の手順 1〜7 を、次のように実行する。

| 手順 | 内容 | 設計 |
|:--|:--|:--|
| 1 | `backends` に、実行する保存先があるか | なければ「対象外」（理由: 保存先が対象外）。`requires = ["ttl"]` は TTL 方式を要求する。メモリは TTL を提供しない（MEM-12）ので、v1 の TTL 場面は DynamoDB だけが対象である |
| 2 | `store` の設定でストアを作る | `retention_count = null` → `RetentionSettings::current_only()`。`retention_mode` は使わない（`ttl` が指定されていても、エラーにせず方式を無視する。2.10 節）。`retention_count` が整数のとき、`retention_mode` の `delete`・`ttl` → `RetentionMode`。`ttl_grace_seconds` → `grace_seconds`。`retry_limit` → `unprocessed_retry_limit`（ないときは既定の 10）。共通名を、実装の設定 API へ対応付ける。`retention_count = 0` は、型で弾かずに `0` をそのまま渡し、ライブラリの設定エラーを観測する（S-1） |
| 3 | `seed.items` | ストアを作る前に、テスト用の権限（実行器の別の `Client`）で、3 テーブルへ入れる |
| 4 | `initialization` | 生成の結果を検査する。生成の失敗のケースは、操作列を持たない。生成の前の障害（`operation = 0`）は、先に登録する |
| 5 | `generators` の展開、`fixtures` の封筒の構築 | 各操作の直前に、参照された封筒を構築する。無効な入力の封筒の構築中の規則違反も、その操作の失敗として捕まえる。実行器の事前検査でライブラリの検査を代替しない |
| 6 | `steps` | 配列の順に、並行せずに実行する |
| 7 | `expect`・`observe` | 保持の失敗や遅延のある場面は、操作が完了してから観測する（5.4 節）。フックは書き込みの成功・失敗を変えない |

- `expect` は、`success`・`none`・`snapshot`（`head_seq_nr` と封筒の組）・`events`（順序も比較）・`error` である。返るイベントは封筒全体（`aggregate_id`・`seq_nr`・`occurred_at`・`manifest`・`payload`）で比較する。スナップショットは、封筒とヘッドの番号を別々に比較する。
- `error` は、`EventStoreError` の変種を、データ上の 5 分類（`optimistic-lock`・`contract-violation`・`serialization`・`configuration`・`storage`）へ対応付ける。メッセージの文字列から分類を推測しない（E-1）。`error.rule` は `ContractRule` の文字列表現と比較する。`message.must_contain`・`must_not_contain` は、`to_string()` の部分文字列で検査する。
- 同時実行はしない。並行の勝者を決める場面は、データにない（`coverage.json` の注）。

### 5.4 観測の対象と時機

保持処理は、追記の確定後に `persist_*` の呼び出しの中で行う（メモリは排他制御の中、DynamoDB は確定の直後）。バックグラウンドで動かない。したがって、`persist_*` が戻った時点で、保持処理は完了または失敗している。実行器は、操作が戻った後に履歴・通知・要求を観測する。DynamoDB の物理的な期限切れの削除は、待たず、検査しない（2100 年の時計のため。5.5 節）。

### 5.5 フックの置き場所

「ライブラリ」は `lib` の `test-hooks` feature（既定で無効、`#[doc(hidden)]`）、「実行器」は `conformance-runner` を指す。

| フック（CR） | メモリ | DynamoDB |
|:--|:--|:--|
| 保持の決定的実行（`delete`・`ttl`） | 追記の中で同期的に実行される。専用のフックは要らない | 同じ。追記の直後に同期的に実行される。要らない |
| 内部履歴（`observe.history`） | ライブラリの `history_view(aid)`（3.5 節）。印のない履歴の集合を返す。印付き履歴はメモリにない | 実行器が、別の `Client` で snapshot テーブルを `aid` で強整合に読む。`active_history_seq_nr` を持つ項目が `active`、`ttl` を持つ項目が `marked`（`seq_nr` と期限の組）。現在の項目（`skey` = 0）と設定項目は数えない。対象はその集約だけ |
| 失敗通知（`observe.notifications`） | `tracing` の `Layer` を、実行器が全体の購読者として一度だけ登録し、対象 `event_store_adapter::retention` のイベントを集める。分類 `retention-failure`。取りこぼさない範囲は、表の後の説明のとおり | 同じ |
| SDK 要求（`observe.requests`） | なし（SDK がない） | 実行器の `Intercept`（`read_before_transmit`）が、送信直前の要求を記録する（4.7 節） |
| 属性（`observe.items`・`seed.items`） | なし | 実行器の別の `Client` が `GetItem`（`ConsistentRead = true`）で読み、5.5.2 節の手順で、属性集合・型・値・入れ子・バイナリ・束縛を比較する。`seed.items` は同じ手順の逆で組み立てる |
| 時計（`clock`） | メモリは TTL を持たない。使わない | ライブラリの `Clock` trait（`now_epoch_seconds`）。`Clock` は TTL の印付けにだけ使う。実行器が固定の時計を渡す。`clock.epoch_seconds` で設定し、操作の `clock_epoch_seconds` の前に進める。v1 は 2100 年 |
| 待ち（指数バックオフ） | 待たない（再要求がない） | ライブラリの `RetrySleeper` trait（2.12.1 節）。`Clock` とは別である。実行器が、要求された待ちを記録し、実際には待たない |
| 配置（`layout.json`） | なし | 実行器が、実際の 3 テーブルを `DescribeTable` と `DescribeTimeToLive` で照合する。照合する内容は 5.5.3 節 |

- ライブラリに足すのは、メモリのフック点（3.5 節）、`Clock`、`RetrySleeper` だけである。SDK 要求の観測と障害の差し込みは、`Client` の設定（4.7 節）で実行器が行う。ライブラリに SDK の差し込みの仕組みを足さない。
- 失敗通知の観測では、同じ最終失敗のログが複数ある場合、1 つの失敗通知に正規化してよい（CR）。空の配列は、その操作で失敗通知がないことを意味する。
- 失敗通知を取りこぼさない範囲（非同期の実行時が、操作の Future を別のスレッドで poll する場合を含む）:
  - `Layer` は、全体の購読者（`tracing::subscriber::set_global_default`）として、実行全体で一度だけ登録する。`with_default` は、そのスレッドの購読者だけを置き換えるので、別のスレッドで出た通知を取りこぼす。使わない。
  - 実行器は、各操作の Future を、ケースの ID と操作番号を持つ span で包む（`tracing::Instrument`）。
  - Future がどのスレッドで poll されても、その span の中で出た対象 `event_store_adapter::retention` のイベントを、その操作の通知として数える。
  - 観測の範囲は、操作の Future の poll の中で出た通知である。ライブラリがバックグラウンドのタスクを作らないことが前提である（2.11 節）。タスクを作ると、その通知は span の外になり、取りこぼす。
  - 通知は、span のケースの ID と操作番号で振り分ける。別の場面の通知が混ざらない。

#### 5.5.1 SDK 要求の検査（`observe.requests`）

- 式（`update.set`・`update.remove`・`condition`・`key_condition`）は、文字列の一致で比べない。SDK の要求の `ExpressionAttributeNames`・`ExpressionAttributeValues` を使い、属性名と値の束縛を解いた構造に変換して比べる。空白・節の順序・`AND` の順序は比較しない。
- `key_condition.all` は、`eq`・`gte` と、`aggregate_id`・`seq_nr` への束縛で検査する。TTL の `#ttl` と `expression_attribute_names` の対応を確かめる。`expires` はエポック秒である。
- `requests` の要素は、実際の別々の要求へ、配列の順で対応付ける。同じ API・段階の要素を複数置いたとき、1 回の要求で複数の要素を満たしたと判断しない。
- ページ送り・未処理キーの再要求・削除バッチの分割は、その段階の要求列全体に対して検査する。`initial_batch_sizes` は、再送を除いた削除バッチの件数である。
- `no_requests_in_phases`・`request_count`・`minimum_request_count` は、段階ごとの要求の数で検査する。`classify-condition-failure-read` は、条件不成立の分類のためだけの追加読み取りを指す。ライブラリは行わない（D-5）。
- 要求の段階は、API 名・テーブル・インデックス・キーから決める（`BatchGetItem` で設定項目のキー → `configuration-read`、`TransactWriteItems` で journal への書き込み → `commit` など）。

要求の観測（`observe.requests[].constraints`）の条件の語は、`conformance/schema/common.schema.json` が定める 24 語である（`additionalProperties` は `false`。ほかの語は書けない）。実行器は、24 語すべてを実装する。語は、その語を使うケースを通す PR で実装する（表の「実装する PR」）。それより前の PR では、その語を使うケースは `unverified` である。

| 分類 | 語 | 意味 | 使うケース（確認した範囲） | 実装する PR |
|:--|:--|:--|:--|:--|
| 構造化した式（上の箇条書きの比べ方） | `update` | `UpdateItem` の式。`set`（属性と `value_binding`）と `remove`（取り除く属性） | `dynamodb-retention-ttl-once` | PR 15 |
| 同上 | `condition` | 属性の存在・不在の条件（`attribute_not_exists`・`attribute_exists`） | `dynamodb-config-new`・`dynamodb-config-create-race`（`attribute_not_exists: aid`）、`dynamodb-retention-ttl-once`（`attribute_exists: active_history_seq_nr`） | PR 11、PR 15 |
| 同上 | `key_condition` | `Query` の `KeyConditionExpression`。`all` に `eq`・`gte` の条件を並べる | `dynamodb-events-over-one-megabyte` | PR 12 |
| 同上 | `expression_attribute_names` | 予約語を避けた別名の対応（`#ttl` → `ttl`） | `dynamodb-retention-ttl-once` | PR 15 |
| 同上 | `expires` | 印を付ける期限（エポック秒）。ttl の `value_binding = expires` が参照する値 | `dynamodb-retention-ttl-once`（4102444860 と 4102445860） | PR 15 |
| 読み取りの要求の形 | `table` | `Query` の対象テーブル。値は `journal`。その場面で割り当てた名前へ束縛する（5.3.1 節） | `dynamodb-events-over-one-megabyte` | PR 12 |
| 同上 | `index` | `Query` の対象インデックス。値は `configured-history-index`。設定値の履歴インデックス名へ束縛する | `dynamodb-retention-delete-paginated-batch` | PR 14 |
| 同上 | `projection` | 履歴 GSI の射影。値は `KEYS_ONLY`。`index` が指す索引の射影を、5.5.3 節が `DescribeTable` で確かめた結果から引いて比べる案。`Query` が `ProjectionExpression` で射影を広げていないことも確かめる。比べ方は README に定めがなく、設計案である | `dynamodb-retention-delete-paginated-batch` | PR 14 |
| 同上 | `consistent_read` | `Query` が `ConsistentRead = true` | `dynamodb-events-over-one-megabyte` | PR 12 |
| 同上 | `consistent_read_all_tables` | `BatchGetItem` の全テーブルで `ConsistentRead = true` | 設定照合の 13 ケース（`dynamodb-config-retry-exhausted` を除く）、`dynamodb-latest-unprocessed`、`dynamodb-snapshot-ahead-of-head` | PR 11、PR 13 |
| 同上 | `keys` | `BatchGetItem` が読むキー。`テーブル:aid:ソートキー` の文字列。設定項目は `journal:__config__:0`・`snapshot:__config__:0`・`head:__config__` | 設定照合の 13 ケース（上と同じ） | PR 11 |
| 同上 | `scan_index_forward` | `Query` の `ScanIndexForward`。journal の読み取りは `true`、履歴 GSI の読み取りは `false` | `dynamodb-events-over-one-megabyte`（`true`）、`dynamodb-retention-delete-paginated-batch`（`false`） | PR 12、PR 14 |
| 同上 | `follow_last_evaluated_key` | `LastEvaluatedKey` が返る間、次の要求の `ExclusiveStartKey` に渡して読み続ける | 上の 2 ケース | PR 12、PR 14 |
| 同上 | `head_and_current_snapshot` | 1 回の `BatchGetItem` が、head と現在のスナップショットの 2 つのキーを持つ（DY-9） | `dynamodb-latest-unprocessed`、`dynamodb-snapshot-ahead-of-head` | PR 13 |
| 同上 | `only_unprocessed_keys` | 再要求に、直前の `UnprocessedKeys` のキーだけが入っている | `dynamodb-config-unprocessed-keys`、`dynamodb-latest-unprocessed` | PR 11、PR 13 |
| 書き込み・設定の作成 | `put_tables` | `TransactWriteItems` が、指定の 3 テーブルへの `Put` だけを持つ | `dynamodb-config-new` | PR 11 |
| 同上 | `same_store_id` | 3 つの `Put` の `store_id` が同じ値 | `dynamodb-config-new` | PR 11 |
| 同上 | `layout_version` | 3 つの `Put` の `layout_version` が 1 | `dynamodb-config-new` | PR 11 |
| 同上 | `head_return_values_on_condition_check_failure` | head の `Put` か `Update` に `ReturnValuesOnConditionCheckFailure = ALL_OLD` が付いている（値は文字列 `ALL_OLD`） | `dynamodb-condition-equal`・`dynamodb-condition-older`・`dynamodb-condition-gap`・`dynamodb-condition-no-head` | PR 12 |
| 再要求・保持 | `exponential_backoff` | 再要求の前に、2.12.1 節の式の待ちを `RetrySleeper` に求めた | `dynamodb-config-unprocessed-keys` | PR 11 |
| 同上 | `include_just_written_history` | 今書いた履歴が保持の候補に加わる。続く削除・印付けの対象と `observe.history` で確かめる | `dynamodb-retention-delete-paginated-batch` | PR 14 |
| 同上 | `retry_unprocessed_items` | `BatchWriteItem` の `UnprocessedItems` を再送した | `dynamodb-retention-delete-paginated-batch` | PR 14 |
| 同上 | `initial_batch_sizes` | 未処理項目の再送を除いた削除バッチの件数。値は `[25, 5]` | `dynamodb-retention-delete-paginated-batch` | PR 14 |
| 同上 | `target_seq_nrs` | その段階の `UpdateItem` が対象にした履歴の seq_nr の集合 | `dynamodb-retention-ttl-once` | PR 15 |

実行器が実装していない条件の語が、要求の検査にあれば、そのケースは `unverified` にし、成功に数えない。語を無視して、残りの条件だけで成功と判断しない。上の表の語はすべて、実装する PR が決まっている。

#### 5.5.2 項目の属性の検査（`observe.items`・`seed.items`）

期待は、`table`・`attributes`・`values`・`nested_attributes`・`binary_json`・`bindings` を持つ（`conformance/README.md` の「保持・SDK 要求・属性の観測」）。実行器は、次の順に比べる。比較の対象は、実際に読んだ項目の全体である。

| 順 | 手順 | 失敗にする条件 |
|:--|:--|:--|
| 1 | `table`（`journal`・`snapshot`・`head`）を、その場面で割り当てた実際のテーブル名へ対応付ける（5.3.1 節）。キーは `values` から作る。journal は `aid` と `seq_nr`、snapshot は `aid` と `skey`、head は `aid` だけ。別の `Client` で `GetItem`（`ConsistentRead = true`）を送る | 項目がない |
| 2 | 実際の項目の属性名の集合を、`attributes` のキーの集合と完全一致で比べる。型の記述子（`S`・`N`・`B`・`L`・`M`）も属性ごとに比べる | 属性の過不足、型の違い。`attributes` にない属性（現在のスナップショットの `ttl`、印付き履歴の `active_history_seq_nr`、設定項目の GSI 用属性など）は、存在すれば失敗にする |
| 3 | `L` の属性は、要素数を確かめる。`events` の要素数は 1（H-3）。要素が `M` であることを確かめ、その属性名の集合と型を、`nested_attributes`（例: `events[0]`）と完全一致で比べる | 件数の違い、入れ子の属性の過不足、型の違い |
| 4 | `binary_json` の各パスを、実際の項目から取り出す。パスは属性名（`payload`）または `events[0].payload` のように、リストの添字とマップの属性名をたどる。`B` のバイト列を JSON として復元し、期待の JSON の値と比べる | パスがない、`B` でない、JSON として復元できない、値の違い。バイト列の見た目（キーの順序・空白）は比べない |
| 5 | `bindings` の `generated-store-id` を解く。同じ `observe.items` の配列の中で、最初に現れた項目の対象の属性（`store_id`、`S`）の実際の値で束縛し、後続の項目は同じ値であることを比べる。具体値は期待しない | 3 つの項目（journal・snapshot・head の設定項目）の値が食い違う |
| 6 | 手順 4・5 で比べたパス・属性を、項目の写しから取り除く（`events[0].payload` はリストの要素のマップから取り除く）。残りの値を `values` と比べる。`S` は文字列の一致である。`N` は、10 進文字列を整数に変換して比べる。整数に変換できない値は一致しない。`L` の要素は、`values` の同じ位置のマップと、属性ごとに比べる | 値の違い。`values` にない属性が残っていれば手順 2 で失敗済み |

- 手順 2・3 は、手順 4〜6 でパスを取り除く前の、実際の項目の全体に対して行う。取り除いた後の写しに対して属性集合や件数を比べると、`payload` の欠落を見逃すからである。
- head の `values.events` は、`payload` のバイナリを書かない（README）。`payload` は手順 4 で比べる。
- 順は、失敗の報告を安定させるためである。最初に失敗した手順で止め、手順の番号と属性のパスを報告に載せる。
- `seed.items` は、同じ期待の逆で項目を組み立てる。`values` の `S`・`N` を `attributes` の型で書く。`binary_json` の各パスの JSON をバイト列へ直列化し、`B` として、パスの位置（リストの要素のマップの中を含む）へ入れる。`bindings` を持つ項目には、実行器が生成した 1 つの識別子を同じ束縛名のすべての項目へ入れる。組み立てた項目は、ストアを作る前に、テスト用の権限の `Client` が `PutItem` で書く（5.3.2 節の手順 3）。
- 確認した範囲では、v1 のデータで `bindings` を持つのは、設定項目 3 つの `observe.items`（`generated-store-id`）だけである。`seed.items` で `bindings` を使う場面は、確認していない。
- この手順は設計案である。属性の観測を実装する PR 11 で、DynamoDB Local の実際の項目を読んで、全ケースで動くことを確かめる。

#### 5.5.3 配置の照合（`dynamodb/layout.json`）

`conformance/README.md` のとおり、`layout.json` の `tables` と `items` を、実行器が用意した実際の 3 テーブルと、ライブラリが書いた項目に照合する。テーブル名と GSI 名の具体値は、設定値へ束縛する（5.3.1 節）。

| 観点 | 照合の方法 | 失敗にする条件 |
|:--|:--|:--|
| キーの構成と型 | `DescribeTable` の `KeySchema` と `AttributeDefinitions` を、`partition_key`・`sort_key`（名前と型）と比べる。head はソートキーなし | キーの名前・型・有無の違い |
| GSI | snapshot の GSI は 1 つ。名前は設定値（`snapshot_history_index_name`）に束縛する。キーは `(aid (S), active_history_seq_nr (N))`。射影は `KEYS_ONLY`。journal と head に GSI はない | 数・キー・射影の違い |
| GSI に載らない項目 | 現在のスナップショット（`skey` = 0）・印付きの履歴・設定項目が、GSI に載らないことを確かめる。GSI の載り方は疎な索引の仕組みで決まるので、これらの項目が `active_history_seq_nr` を持たないことを、項目の属性で確かめる（5.5.2 節の手順 2）。GSI を `Query` して確かめない。GSI の読み取りは結果整合であり、結果が毎回同じにならないからである | 載らないはずの項目が `active_history_seq_nr` を持つ |
| Streams | head だけ有効で、`NEW_IMAGE`。journal と snapshot は無効。`DescribeTable` の `StreamSpecification` で確かめる | 有効・無効、`StreamViewType` の違い |
| TTL | `DescribeTimeToLive` で確かめる。snapshot は、`retention_mode = ttl` の場面だけ `ENABLED`（属性は `ttl`）。ほかの場面では無効。journal と head は常に無効（`enabled_when = never`） | 状態・属性名の違い |
| `items` と `item-shapes` の突き合わせ | `layout.json` の `items` の各要素（8 種類の項目）を、`item-shapes` の操作列で、ライブラリが実際に書いた項目と突き合わせる。項目の種類（journal・現在のスナップショット・印のない履歴・印付き履歴・head）ごとに、実際の項目の属性名の集合と型を、`attributes`（と `nested_attributes`）と完全一致で比べる。値は比べない（値は `item-shapes` の `observe.items` が検査する） | 属性の過不足、型の違い |
| 設定項目 | 設定項目 3 種（journal・snapshot・head）は、`item-shapes` では実行器が種として入れた項目である。ライブラリが書いたものと比べるため、`dynamodb-config-new` でライブラリが書いた設定項目と比べる（10 章 Q-18） | 属性の過不足、型の違い |

- 配置の場面は、`item-shapes` が通る PR 15 まで `unverified` とする。それまでは、テーブルの作成と、キー・GSI・Streams・TTL の照合だけを PR 11 から行う。
- 配置の場面の結果は、`layout.json` のケース（`dynamodb-layout-v1`）の 1 件として報告する。規則は、そのケースの `rules`（DY-2・DY-3・DY-12・DY-16・DY-17・D-1〜D-4・T-7・H-3・H-4）に対応付ける。

### 5.6 障害の差し込み

データの `faults` は、`operation`（0 がストアの生成、1 以上が 1 始まりの操作番号）、`phase`、`kind`、`injection`、`repeat` を持つ。登録した障害が発火しなければ、場面は失敗である。差し込めなければ「未検証」で、成功に集計しない（CR）。発火したかどうかの数え方は、5.6.6 節で定める。

段階は 12 種類である（10 章 Q-1）。v1 のデータで使われている組み合わせを確認し、表にした。

| 段階 | v1 での種別・保存先 | DynamoDB での差し込み | メモリでの差し込み |
|:--|:--|:--|:--|
| `serialize-event` | `serialization-error`、メモリ・DynamoDB | 実行器が `EventSerializer` を包み、指定の回数目の `serialize` で失敗させる | 同じ |
| `serialize-snapshot` | 同上 | `SnapshotSerializer` を包む | 同じ |
| `deserialize-event` | 同上 | `deserialize` を失敗させる。読み取りで復元するときに発火する | 同じ |
| `deserialize-snapshot` | 同上 | 同上 | 同じ |
| `commit` | `storage-error`（メモリ・DynamoDB）、`sdk-error`（DynamoDB の取り消し理由 12 件） | `TransactWriteItems` の要求を HTTP の層で止め、作った応答を返す（`replace-request`）。何も確定しない。取り消し理由は 5.6.2 | ライブラリの `before_commit` フック。失敗を返せば、ヘッド・ジャーナル・スナップショットに変更が残らない（MEM-7） |
| `read-events` | `storage-error`、メモリ・DynamoDB | journal の `Query` を止めて失敗を返す | `read_events` フック |
| `read-snapshot` | `storage-error`（メモリ・DynamoDB）、`sdk-response`・`read-interleave`（DynamoDB） | `BatchGetItem` を止める。`sdk-response` は 5.6.3。`read-interleave` は 5.6.4 | `read_snapshot` フック。`read-interleave` は v1 の対象が DynamoDB だけ |
| `retention-query` | `storage-error`（メモリ・DynamoDB）、`sdk-response`（`history_pages`、メモリ・DynamoDB） | 履歴 GSI の `Query` を止めて失敗を返す。`history_pages` は 5.6.3 | `retention_visible_history` フック。メモリでは、`history_pages` を論理履歴のフックへ対応付ける（README、10 章 Q-15）。ページ列は連結して「見える履歴」として返す |
| `retention-delete` | `storage-error`（メモリ・DynamoDB）、`sdk-error`・`sdk-response`（DynamoDB） | `BatchWriteItem` を止めて失敗を返す。`unprocessed_first_n` は 5.6.3 | `retention_delete` フック。`final-retention-failure` は、候補の選択の後、削除の前に失敗させる |
| `retention-mark` | `sdk-error`、DynamoDB | `UpdateItem`（`#ttl`）を止めて失敗を返す | 該当なし。メモリは TTL を提供しない。TTL の要求が設定エラーになることを、`lib` の単体試験で確かめる（MEM-3・MEM-12） |
| `configuration-read` | `sdk-response`、DynamoDB | 設定の `BatchGetItem` の応答を差し替える（5.6.3） | 該当なし。保存先に設定の読み取りがない（MEM-3）。設定を保存先が不変で持つことを、`lib` の単体試験で確かめる |
| `configuration-create` | `sdk-error`、DynamoDB | 設定の `TransactWriteItems` を止め、別の実行器が先に書いた項目を確定させて、取り消し応答を返す（5.6.5） | 該当なし。独立して生成した保存先が混ざらないことを、`lib` の単体試験で確かめる（MEM-2） |

差し込めない段階のうち、v1 のデータが対象にしていないものは「保存先が対象外」である。代わりの確かめ方は、上の表の `lib` の単体試験（メモリの MEM-2・MEM-3・MEM-12）である。メモリの読み取りの原子性（R-8・MEM-8）は、v1 に場面がない。代わりに、複数スレッドで追記と読み取りを繰り返し、`snapshot.seq_nr ≤ head_seq_nr` が常に成り立つことを確かめる単体試験を足す案。勝者を問わない不変条件の検査であり、結果は決定的である。

DynamoDB は、v1 で使われている全段階に差し込める見込みである。ただし、HTTP の層の差し込みが実際に動くかは、今回確認していない。実装時に、DynamoDB Local で確かめる。確かめられない段階は、「未検証」と報告し、別の確かめ方を設計し直してコーディネーターのレビューを受ける（IP 5 の 1）。

#### 5.6.1 差し込みの層

- DynamoDB の要求は、`Config::builder().http_client(...)` で差し込む `HttpClient` が受ける。実際の DynamoDB Local へ転送するか、作った応答を返す。観測は `Intercept` で行う（4.7 節）。
- `replace-request`: 要求を DynamoDB Local へ送らず、差し込みの応答を返す。何も確定しない。v1 の書き込み失敗と保持失敗は、すべて `replace-request` である。
- `replace-response`: 要求を DynamoDB Local へ送った後、SDK へ渡す応答を差し替える。送った要求の副作用は残る。
- `sdk-error`: SDK の自動再試行を無効にし（`RetryConfig::disabled()`）、`details.code` と `cancellation_reasons` から、SDK が解釈できる応答を作る。
- `repeat`: `{"mode":"count","count":n}` は、対象の段階への差し込みの最初の n 回の適用を指す（`conformance/README.md` の「対象段階への適用回数」）。1 回の適用が何を指すかは、差し込みの種類で違う（5.6.6 節）。要求 1 回とは限らない。応答計画の `history_pages` は、1 回の適用が、ページ列を全部返すまで続く。`{"mode":"until-operation-finishes"}` は、指定の操作の中の対象要求を、再試行も含めて失敗させる。次の操作へ持ち越さない。発火の数え方は 5.6.6 節。
- 同じ段階の障害は、配列の順に消費する。異なる段階の障害は、すべて登録する。

#### 5.6.2 取り消し理由（`cancellation_reasons`）

- トランザクションの各項目に 1 要素を作る。書き込みの対象名は `journal`・`head`・`current-snapshot`・`history-snapshot`（存在するアクションだけ）、設定作成は `configuration:journal`・`configuration:snapshot`・`configuration:head` である。失敗していない項目の `code` は文字列 `None`。
- 実行器は、対象名を実際の要求内のアクション（テーブルと操作の種類）に照合し、SDK の `CancellationReasons` を実際のアクション順に並べ直す。この順をライブラリの要求順として強制しない。
- head の `ConditionalCheckFailed` は `old_head_seq_nr` を持つ。`null` は旧項目が返らないことを意味する。実行器は、旧項目を `Item` として応答に載せる（D-5）。
- 応答の JSON の形は、DynamoDB Local の実際の取り消し応答から写して合わせる（実装時に確認する）。

#### 5.6.3 応答計画（`sdk-response`）

| 計画 | 設計 |
|:--|:--|
| `responses`・`unprocessed_keys`（`BatchGetItem`） | 要求を転送し、返った応答の `Responses` と `UnprocessedKeys` を、計画のとおりに書き換える。指定した最初の応答だけを置き換え、以後は未処理キーに対する実際の応答を返す。送られていないキーを返さない。`seed-config`・`stored-head` は、その場面に保存されている項目である。1 回の適用が置き換えるのは、1 つの応答である。`count` が n なら、続く n 回の `BatchGetItem` の応答を、1 回ずつ置き換える（5.6.6 節） |
| `history_pages`（履歴 GSI の `Query`） | 履歴の seq_nr のページ列を、そのまま返す。今書いた履歴を自動で足さない。ページごとに `LastEvaluatedKey` と、次の要求の `ExclusiveStartKey` を対応付ける。その場面の保存済みの履歴の項目を使う。1 回の適用は、最初の `Query` で始まり、ページ列を全部返し終えるまで続く。`LastEvaluatedKey` で続く 2 ページ目以降の `Query` は、同じ適用の続きである。別の適用として数えない（5.6.6 節） |
| `omit_just_written_history` | 既定は false。true なら、`history_pages` に今書いた番号がないことを確かめる。false でも自動で足さない。候補への追加と重複の排除はライブラリが行う |
| `unprocessed_first_n`（`BatchWriteItem`） | 受けた削除要求の先頭 n 件を `UnprocessedItems` として返し、残りを実際に処理する。n 件を除いた要求を DynamoDB Local へ転送し、応答に n 件を足す（`replace-request`）。ライブラリは再送する。1 回の適用は、対象の `BatchWriteItem` の要求 1 回である（5.6.6 節） |

削除・TTL の更新・書き込みの物理的な結果は、実際に反映し、`observe.history` と `observe.items` で検査する。

#### 5.6.4 読み取りの重なり（`read-interleave`、R-8・DY-9）

1. `BatchGetItem` の送信の直前に、旧ヘッドを実行器が別の `Client` で読んで捕捉する。
2. `interleaved_operation` の追記を、試験対象のストアを通して確定する。
3. 元の `BatchGetItem` を DynamoDB Local へ送り、現在のスナップショットを取得する。
4. 応答の中のヘッドを、捕捉した旧ヘッドへ差し替える。

返る組は、旧ヘッドと新しいスナップショットである。物理的なヘッドには追記が反映されている。実時間の競争は使わない。ライブラリに、ヘッドとスナップショットの別々の要求を要求しない。

#### 5.6.5 設定の作成の競合（`install_items`）

`configuration-create` の障害に `install_items` があるとき、実行器は `TransactWriteItems` を止める。そのときに、別の実行器が先に書いた設定項目（`install_items`）を DynamoDB Local へ書いて確定させる。その後、取り消し応答（`configuration:journal` の `ConditionalCheckFailed` など）を返す。ライブラリは、応答を捨て、3 件を強整合で読み直す（DY-8）。

#### 5.6.6 発火の数え方

障害は、「適用」を単位に数える。`count` は、対象の段階への適用回数である（`conformance/README.md`）。1 回の適用が指すものは、差し込みの種類で違う。要求 1 回とは限らない。

| 差し込みの種類 | 1 回の適用 |
|:--|:--|
| 失敗を返す差し込み（`storage-error`・`sdk-error`。DynamoDB では `replace-request`） | 対象の段階の要求 1 回を、失敗の応答に差し替える。自動再試行を無効にしている（5.6.1 節）ので、API の 1 回の呼び出しが 1 つの要求になる |
| `sdk-response` の `responses`・`unprocessed_keys`（`BatchGetItem`） | 対象の段階の要求 1 回の応答を、計画のとおりに置き換える。以後の再要求の応答は、実際の応答である。次の適用は、次の要求から始まる |
| `sdk-response` の `history_pages`（履歴 GSI の `Query`） | 計画の開始から、ページ列を全部返し終えるまで。`LastEvaluatedKey` で続く複数の `Query` の要求が、1 回の適用である。適用の開始（最初の `Query`）で、発火したと数える。残りのページを読み切るかは、`follow_last_evaluated_key` など要求の観測で検査する（5.5.1 節） |
| `sdk-response` の `unprocessed_first_n`（`BatchWriteItem`） | 対象の `BatchWriteItem` の要求 1 回 |
| `read-interleave` | 対象の `BatchGetItem` の要求 1 回（5.6.4 節の手順 1〜4 の全体が 1 回の適用） |
| 直列化の障害（`serialization-error`） | 実行器が包んだシリアライザの `serialize`・`deserialize` の 1 回の呼び出し（読み取りで複数のイベントを復元するときは、復元の呼び出しが複数回ある） |
| メモリのフック | フック点（`before_commit`・`read_events`・`read_snapshot`・`retention_visible_history`・`retention_delete`、3.5 節）の 1 回の呼び出し。`history_pages` は、ページ列を連結して返す `retention_visible_history` の 1 回の呼び出しが、1 回の適用である |

| 場合 | 数え方 |
|:--|:--|
| 有効な範囲 | `operation` が n 番の障害は、n 番の操作の中の対象の段階にだけ適用する。操作が終わったら外す。次の操作へ持ち越さない |
| 操作 0 | ストアの生成の障害である。ストアを作る前に登録する（5.3.2 節の手順 4） |
| `count` が n | その操作の中で、対象の段階への適用を、最初の n 回行う。n 回の適用がすべて始まったとき、発火したと数える。例 1: `dynamodb-config-retry-exhausted` は `retry_limit` が 1 で、`count` が 2 である。初回の読み取りと再要求の 1 回の、2 回の `BatchGetItem` の応答を、1 回ずつ置き換える。適用は 2 回で、2 回とも発火する。例 2: `dynamodb-retention-delete-paginated-batch` は `count` が 1 で、`history_pages` が 2 ページである。最初の `Query` で適用が始まり、2 ページ目の `Query` も、同じ適用の続きとして計画のページを返す。適用は 1 回で、発火する。2 ページ目を 2 回目の要求と数えて適用の範囲外にすると、DynamoDB Local の実際の応答が返り、候補が 30 件にならない |
| `until-operation-finishes` | その操作の中の、対象の段階の要求のすべてに適用する。再試行を含む。要求 1 回の差し替えが 1 回の適用である。1 回以上適用できたとき、発火したと数える。操作が終わったら外す |
| 一部だけ消費された | `count` が n で、操作が終わるまでに適用できた回数が n に届かなかった場合は、発火しなかったと数え、場面を `failed` にする。報告に、宣言した回数と、適用できた回数を載せる |
| 1 回も適用されなかった | 場面を `failed` にする（登録した障害が発火しなかった） |
| 同じ段階の障害が複数 | 配列の順に消費する。先の障害の回数を使い切ってから、次の障害を使う。異なる段階の障害は、すべて登録しておく |
| 差し込めない | `unverified`。成功に数えない |

- メモリの `count` が 2 以上の障害は、v1 のデータにない。`count` が 2 以上なのは、DynamoDB だけが対象の `dynamodb-config-retry-exhausted` である。
- `until-operation-finishes` は、v1 では DynamoDB の保持の削除と印付け（`retention-delete`・`retention-mark`）だけが使う。

### 5.7 CI での実行

案は、`ci.yml` に `conformance` ジョブを足す。

```yaml
conformance:
  runs-on: ubuntu-latest
  strategy:
    matrix:
      backend: [memory, dynamodb]
  steps:
    - uses: actions/checkout@v7
    - uses: dtolnay/rust-toolchain@stable
    - run: python3 tools/conformance/manifest.py verify
    - run: cargo run -p event-store-adapter-conformance-rs --release -- --backend ${{ matrix.backend }} --data conformance --report conformance-report-${{ matrix.backend }}.json
    - uses: actions/upload-artifact@v4
      with: { name: conformance-${{ matrix.backend }}, path: conformance-report-${{ matrix.backend }}.json }
```

- DynamoDB のジョブは Docker を使う。コンテナの起動は、実行器が `test-utils` の関数で行う（6 章）。
- 結果は、`GITHUB_STEP_SUMMARY` に、規則番号ごとの集計を出す。
- `CI Success` の `needs` に、このジョブを足す（7 章）。
- 7 章の途中の PR では、未実装の保存先・規則のケースは「未検証」で報告し、`--require-all` を付けない。「未検証」と「対象外」は、成功に集計しない。全ケースが実行できる PR 15 の後の PR 16 で、`--require-all` を付ける。`--require-all` の意味は 5.8 節で定める。
- `--require-all` を付けない間は、実行器は「失敗」と、manifest の照合の失敗だけで、終了コードを失敗にする。「未検証」は、終了コードを失敗にしない。ただし、報告には必ず載せ、成功に数えない。

### 5.8 報告の形

実行器は、保存先ごとに 1 つの JSON を出す。形は案である。

```json
{
  "data": { "version": "1.0.0", "manifest": { "sha256": "61c26614…", "verification": "passed" } },
  "implementation": { "language": "rust", "crate": "event-store-adapter-rs", "version": "4.0.0-…", "revision": "<git の識別子>" },
  "backend": "dynamodb",
  "environment": { "dynamodb_local": "3.3.1", "image_digest": "sha256:ff89bd48…" },
  "cases": [
    { "id": "core-gap-event", "rules": ["W-4", "W-8", "E-1", "E-3", "H-1"], "status": "passed" },
    { "id": "core-seq-negative", "rules": ["T-9", "E-1", "E-3", "R-1"], "status": "not-applicable",
      "reason": { "kind": "unrepresentable", "detail": "SeqNr は u64。負数を表せない（signed_seq_nr）" } },
    { "id": "…", "rules": ["…"], "status": "failed", "failed_operation": 3, "expected": {}, "actual": {} }
  ],
  "rules": [ { "rule": "W-8", "passed": 0, "failed": 0, "not_applicable": 0, "unverified": 0 } ]
}
```

| 状態 | 意味 | 成功に集計するか |
|:--|:--|:--|
| `passed` | 全操作が期待どおり | する |
| `failed` | 途中で期待と異なった。失敗した操作番号、期待値、実際の値を載せる。登録した障害が発火しなかった場合、宣言した回数に届かなかった場合（5.6.6 節）、印のないケースで入力を型へ変換できなかった場合もここ | しない |
| `not-applicable`（対象外） | 理由つき。認める理由は 4 つだけである（下の一覧） | しない（対象外として別に数える） |
| `unverified`（未検証） | 実行できなかった。障害を差し込めなかった場合、実行器が実装していない条件の語（5.5.1 節）がある場合、必須のケースを飛ばした場合 | しない |

対象外として認める理由は、次の 4 つである。

| 理由 | 内容 |
|:--|:--|
| 保存先が対象外 | ケースの `backends` に、実行する保存先がない |
| 表現能力の違い | 表現不能（`representation.signed_seq_nr = true` のケースだけ。5.2 節）。精度の選択（`representation.time_precision = milliseconds` のケース） |
| FNV-1a 64 の決定 | 最初のメジャーにハッシュを使う保存先がない。値の表の `fnv1a64` の 4 件（K-1）。オーナーの決定（2026-10-06）。成功にも失敗にも数えない。段階 5 で実行する |
| `coverage.json` の規則の単位の除外 | `exclusions` に載る規則。`W-5`（削除済み）、`R-7`（呼び出し側の推奨）。任意の能力（変更フィード）のケースは、v1 にない |

`--require-all` の意味は、次のとおりである。

| 指定 | 終了コードを失敗にする条件 |
|:--|:--|
| 付けない | `failed` が 1 件でもある。manifest の照合に失敗した |
| 付ける | 上に加えて、`unverified` が 1 件でもある。上の 4 つの理由のない `not-applicable` が 1 件でもある |

- どちらの場合も、対象外は成功に数えず、報告に理由を載せる。
- `--require-all` を付けても、FNV-1a 64 の 4 件は失敗にしない。上の理由のある対象外だからである。

- 1 つのケースが複数の規則を持つ場合、結果をすべての規則へ対応付ける。途中で期待と異なる操作があれば、成功済みの操作だけから、ケース全体の成功と報告しない（IP 5）。
- 規則ごとの集計は、そのケースの状態を数える。失敗が 1 件でもある規則は、規則として失敗とする。
- 「表現不能」は、状態ではなく、対象外の理由の 1 つである。表現不能にするのは、`signed_seq_nr` の印があるケースだけである（5.2 節）。
- 対象外は、上の 4 つの理由だけである。「必須のケースを飛ばした結果」や、「障害を差し込めなかった結果」を対象外にしない（`unverified` にする）。

#### 5.8.1 受け入れ条件との対応（IP 5）

| 受け入れ条件 | 実行器の対応 |
|:--|:--|
| 1. 全ケースを DynamoDB（DynamoDB Local 3.3.1）とメモリで通す。障害の場面も両方で行う | 5.3〜5.6 節。メモリの対象でないケースは「対象外」。対象外（表現能力と FNV-1a 64）は成功に数えず、理由を記録する。差し込めない保存先は 5.6 節の代わりの確かめ方 |
| 2. 項目の形が `layout.json`・`item-shapes.json` と一致 | `observe.items`・配置の照合（5.5 節・5.5.3 節） |
| 3. main の `CI Success` が成功 | 5.7 節 |

## 6. 試験環境

### 6.1 DynamoDB Local 3.3.1

DynamoDB の試験の環境は、DynamoDB Local 3.3.1 に統一する（IP 4.1）。イメージは digest で固定する。

| 項目 | 値 |
|:--|:--|
| イメージ | `amazon/dynamodb-local` |
| 版 | 3.3.1 |
| digest | `sha256:ff89bd48ff32cd8d9be5fee8873b65b8854dc408f1afe881be6eb00247bc0dab` |
| 参照 | `amazon/dynamodb-local@sha256:ff89bd48ff32cd8d9be5fee8873b65b8854dc408f1afe881be6eb00247bc0dab` |
| 起動の引数 | `-jar DynamoDBLocal.jar -inMemory -sharedDb -disableTelemetry` |
| コンテナ内のポート | 8000 |

- 出典: ハブの比較記録（`tools/spikes/dynamodb-emulators/README.md` と `probe.py`）。`latest` と `3.3.1` のタグの digest が一致したこと、起動に上の引数を使ったこと、コンテナ内のポートが 8000 であることを、記録のソースで確認した。今回、イメージの取得やコンテナの起動はしていない。
- `latest` を使わない。digest で固定する。

起動の例（手元の確認用）。

```sh
docker run --rm -p 127.0.0.1:8000:8000 \
  amazon/dynamodb-local@sha256:ff89bd48ff32cd8d9be5fee8873b65b8854dc408f1afe881be6eb00247bc0dab \
  -jar DynamoDBLocal.jar -inMemory -sharedDb -disableTelemetry
```

- `-inMemory` は、永続化しない。コンテナを止めるとデータが消える。
- `-sharedDb` は、資格情報やリージョンに関わらず 1 つのデータベースを使う。
- `-disableTelemetry` は、利用状況の送信を止める。

試験からの起動は、`test-utils` の関数（`testcontainers`）で行う案。`GenericImage` の名前と tag を、上の参照に合わせる。tag に digest を含めて渡せるかは、`testcontainers 0.28.0`（`Cargo.toml` の指定）で、実装時に確かめる。

### 6.2 クライアントの設定

- DynamoDB のクライアントに、endpoint（`http://127.0.0.1:{公開したポート}`）を明示する。
- 最初のメジャーは Streams を読まない（1.4 節）。将来 DynamoDB Streams のクライアントを使う場合も、同じ endpoint を明示する。DynamoDB Local の Streams の ARN は、リージョンが `ddblocal` になる。ARN からリージョンを推測しない（IP 4.1）。
- リージョンと資格情報は、固定のダミーの値を明示する。環境変数や AWS の設定ファイルから探索させない。3 テーブルは同じリージョンに置く（DY-18）。
- 現行の `test-utils/src/dynamodb.rs` の `create_client` は、リージョン `us-west-1`、資格情報 `x`/`x` を明示している（確認した事実）。この方針を引き継ぐ。

### 6.3 起動の待ち方

- 起動の確認は、コンテナの標準出力の文字列ではなく、`ListTables` が成功するまで待つ案。現行の LocalStack は、標準出力の `Ready.` を待つ（確認した事実）。
- テーブルの作成後は、`DescribeTable` で `ACTIVE` を待つ。
- コンテナは、実行器の実行全体で 1 つにする。場面ごとに別々のテーブル名を使う（5.3 節）。`lib` の DynamoDB の試験も、1 つのコンテナを共有し、試験ごとに別々のテーブル名を使う案。現行は、1 試験につき 1 コンテナを起動する。

### 6.4 現行の試験（LocalStack）からの移し方

確認した現行の状態:

- `test-utils/src/docker.rs` の `dynamodb_local()` は、LocalStack `2.1.0` を起動する。関数名は DynamoDB Local だが、実体は LocalStack である。
- `test-utils/src/dynamodb.rs` は、旧配置（`pkey`・`skey`・旧 GSI）のテーブルを作る関数を持つ。
- `lib/src/event_store_for_dynamodb_test.rs` は、LocalStack に接続して旧配置のストアを試験する。
- `test-utils/src/docker.rs` の `bigtable_emulator()` は、Bigtable のエミュレータを起動する。

移し方は、次の 4 段階である。

| 段階 | 7 章の PR | 内容 |
|:--|:--|:--|
| 1 | PR 5 | `test-utils` に、DynamoDB Local を起動する関数と、3 テーブル（履歴 GSI・Streams・条件つき TTL）を作る関数を足す。LocalStack の関数は、名前を実体に合わせて変え（関数名の変更は旧試験の呼び出しも直す）、旧試験は動かし続ける |
| 2 | PR 11〜PR 15 | 新しい DynamoDB の試験（適合実行器と、`lib` の単体試験）は、DynamoDB Local だけを使う |
| 3 | PR 20 | 旧 API の試験と、LocalStack の起動関数、旧配置のテーブルを作る関数を削除する。Bigtable のエミュレータの関数は、Bigtable を外す PR 18 で削除する（9 章 D-2） |
| 4 | PR 21 | AGENTS.md・README の試験の説明を直す |

- 旧試験の `TEST_TIME_FACTOR`（待ち時間の倍率）は、旧試験とともになくす。新しい試験は、`DescribeTable` の待ち方を使う。
- LocalStack にトークンが要る件（IP 4.1）は、LocalStack を使わなくなることで解消する。

## 7. main への入れ方

### 7.1 進め方の前提

- main に、PR ごとに squash マージする。新契約の変更を少しずつ入れる（IP 3 の手順 3）。マージはコーディネーターが行う（IP 9）。
- 確認した事実:
  - 版上げの workflow `lib-bump-version.yml` は、手動の起動（`workflow_dispatch`）だけで動く。起動時に段階 `level`（auto・patch・minor・major）を選べる。定期実行は外してある（IP 3 の結果、[PR #244](https://github.com/j5ik2o/event-store-adapter-rs/pull/244)）。したがって、main への変更が自動で公開されることはない。
  - 公開は、タグ `v[0-9]+.[0-9]+.[0-9]+` の push で `lib-release.yml` が `cargo publish -p event-store-adapter-rs` を行う。`lib-release.yml` は、タグ名と `Cargo.toml` の版の一致を検査しない。
  - `ci.yml` には日次の `schedule` があるが、これは CI の実行であり、版上げではない。
- 次のメジャーの正式版（4.0.0）は、IP 5 の受け入れ条件を満たした後、オーナーの承認を得て、`level = major` で手動で出す（IP 3 の手順 4）。出した後に、定期実行を戻す（PR 23、7.5 節）。
- 1 つの PR は、1 つの規則群に対応する（IP 9）。同じ規則群の変更は、同じ PR に入れる。別の規則群の変更は、別の PR にする。
- 7.2 節の 22 番は PR ではない。正式リリース（`level = major` の手動の起動）である。本文では「正式リリース」と書く。

### 7.2 PR の列

「新しく通るケース」は、その PR で新しく `passed` になるはずのケースである。メモリと DynamoDB の別は、各行に書く。件数は、`conformance/` のファイルごとに数えた（共通 42 件、DynamoDB 43 件、配置 1 件、値 30 件）。

| 順 | PR の範囲 | 対応する規則・条件 | 新しく通るケース |
|:--|:--|:--|:--|
| 1 | 版上げの保護と版の計算。`lib-bump-version.yml` に、開発版の保護を足す。`next-semver.py` を開発版に対応させ、新しい試験を足す。`ci.yml` の `release-control` に `semver` を入れ、`-p` の指定を広げる | IP 3 | なし |
| 2 | 公開の条件。`lib-release.yml` のタグの条件を開発版まで広げ、タグ名と `Cargo.toml` の版の一致を検査する | IP 3 | なし |
| 3 | 版と README。`lib/Cargo.toml` の版を `4.0.0-alpha.0` にし、README に注意を足す | IP 3 | なし |
| 4 | 実行器の骨格。読み込み・manifest の照合・generators の展開・比較・報告、障害の登録と数え方（5.6.6 節）、未実装の条件の語を `unverified` にする。`test-hooks` は使わない | CR | なし。対象外の理由が決まるケース以外は、`unverified` |
| 5 | 試験環境。DynamoDB Local の起動と 3 テーブルの作成（`test-utils`）。LocalStack の関数の名前を実体に合わせる | IP 4.1 | なし |
| 6 | CI の `conformance` ジョブ（`--require-all` なし）。`CI Success` の `needs` に足す | IP 5 の 3 | なし |
| 7 | 中核 | T-1〜T-13、W-6、W-9、E-1〜E-3、S-1、S-2（選択の関数） | 値の表の `buildAid` 9 件 |
| 8 | メモリの書き込みと読み取り。`test-hooks`（`before_commit`・`read_events`・`read_snapshot`・`history_view`）。単体試験（並行追記で 1 件だけ確定、飛び番、MEM-2、MEM-3・MEM-12、MEM-6、R-8） | MEM-1〜MEM-9・MEM-12・MEM-13、H-1〜H-4、W-3・W-4・W-7・W-8、R-1〜R-6・R-8 | メモリで、共通の書き込みと読み取り（29 件のうち 24 件。5 件は表現能力による対象外）、`core-retention-zero`、`core-retention-default-current-only`、直列化と復元の 4 件、`core-storage-commit-failure`・`core-storage-read-events`・`core-storage-read-snapshot`。値の表の `validateSeqNr`（6 件のうち 5 件）と `validateOccurredAt`（11 件のうち 7 件） |
| 9 | メモリの保持と通知。`retention_visible_history`・`retention_delete` のフック。単体試験（別の集約の履歴が消えない、MEM-10、MEM-11） | MEM-10、MEM-11、S-2、S-4 | メモリで、`core-retention-delete-1`・`core-retention-delete-2`・`core-retention-failure-after-commit`・`core-retention-query-failure`。メモリの全ケースがそろう |
| 10 | 障害の差し込みの基盤（要求の層）。HTTP の層（`http_client`）、`Intercept`、自動再試行の無効化、段階の判定、取り消し応答の組み立て（5.6 節） | CR の「障害と時計の差し込み」 | なし。実行器の単体試験で確かめる |
| 11 | DynamoDB の配置と設定照合。`open`・再要求の方針・`RetrySleeper`・項目の属性の組み立て。実行器: テーブルの作成、配置の照合（キー・GSI・Streams・TTL）、属性の観測、`BatchGetItem` の応答計画、`install_items` | DY-2・DY-3・DY-8・DY-16〜DY-19、`dynamodb.md` の D-1〜D-4 | DynamoDB で、設定照合の 14 件（障害の差し込みが要る `dynamodb-config-unprocessed-keys`・`dynamodb-config-retry-exhausted`・`dynamodb-config-create-race` を含む）、`core-retention-zero` |
| 12 | DynamoDB の書き込みと最小の読み取り。トランザクション、D-5 の分類、D-6、D-7 の見積もり。DY-9 の 1 回の `BatchGetItem`（`UnprocessedKeys` が残ったら `Storage`）、journal の `Query`（DY-10・DY-11） | H-1〜H-4、W-3〜W-9、`dynamodb.md` の D-5〜D-7、DY-10、DY-11、R-1〜R-6 | DynamoDB で、`write-errors` の 16 件、`dynamodb-events-over-one-megabyte`、`dynamodb-no-head-with-snapshot`。8 番と同じ共通の書き込みと読み取り、`core-retention-default-current-only`、直列化と復元、`core-storage-*`、値の表 |
| 13 | DY-9 の再要求と `read-interleave` | DY-9、R-8 | DynamoDB で、`dynamodb-latest-unprocessed`、`dynamodb-snapshot-ahead-of-head` |
| 14 | 削除方式の保持。`history_pages`・`unprocessed_first_n` | `dynamodb.md` 8 章の手順 1〜3、D-3・D-9、P-18・P-24、S-1・S-2・S-4 | DynamoDB で、削除方式の 5 件（`dynamodb-retention-delete-paginated-batch`・`dynamodb-retention-gsi-missing-new`・`dynamodb-retention-gsi-deduplicate`・`dynamodb-retention-event-only`・`dynamodb-retention-failure-delete`）、共通の保持 4 件（9 番と同じ） |
| 15 | TTL 方式の保持と `Clock` | S-3、DY-2、`dynamodb.md` 8 章の手順 4、P-17 | DynamoDB で、`dynamodb-retention-ttl-once`・`dynamodb-retention-ttl-stale-marked`・`dynamodb-retention-failure-ttl`、`dynamodb-written-item-shapes`、配置（`dynamodb-layout-v1`）。DynamoDB の全ケースがそろう |
| 16 | `conformance` ジョブに `--require-all` を付ける（5.8 節） | IP 5 の 1・3 | 全ケース（成功か、理由のある対象外） |
| 17 | 移行ツール（9 章 D-1 の C）。CI の `feature-matrix` に `migration` を足す | `dynamodb.md` の D-8・11 章、IP-D8 | なし。DynamoDB Local での試験（適合データの対象外） |
| 18 | SQLite・Bigtable の除去（9 章 D-2 の A）。CI の組を外す | IP-D3 | なし |
| 19 | 利用例 `examples/user-account` を `next` の API へ移す | IP 5 の 5 | なし |
| 20 | 切り替えと旧 API の削除。`next` を直下へ移す。旧試験と LocalStack の関数を消す。利用例のパスを付け替える | IP 4.2 | なし |
| 21 | 文書の整備 | IP 5 の 5・6 | なし |
| 22 | 正式リリース（PR ではない） | IP 3 の手順 4、IP 5 | なし |
| 23 | `lib-bump-version.yml` の `schedule`（`cron: '0 0 * * *'`、[PR #244](https://github.com/j5ik2o/event-store-adapter-rs/pull/244) で外したもの）を戻す | IP 3 の手順 4 | なし |

並べ方の理由は、次のとおりである。

- 規則群ごとに 1 つの PR にする。版・公開の条件・開発版の保護（PR 1〜PR 3）、実行器（PR 4〜PR 6）、メモリ（PR 8・PR 9）、DynamoDB（PR 10〜PR 15）を分ける。
- `test-hooks` は、PR 8 で足す。PR 4 の骨格は、`test-hooks` を使わない。実行器の骨格が、メモリの実装より先にできるからである。
- 障害の差し込みの基盤（PR 10）は、DynamoDB の配置と設定照合（PR 11）より前に置く。設定照合の 14 件のうち 3 件（`dynamodb-config-unprocessed-keys`・`dynamodb-config-retry-exhausted`・`dynamodb-config-create-race`）が、障害の差し込みを要するからである。障害の登録と数え方（5.6.6 節）は、メモリの `core-serialize-*`・`core-storage-*` も使うので、PR 4 に入れる。
- `write-errors` の 16 件は、どれも操作列の最後に読み取りを呼ぶ。したがって、書き込みだけの PR では通らない。最小の読み取りを、書き込みと同じ PR 12 に入れる。
- `dynamodb-written-item-shapes`（`item-shapes`）は、PR 15 に置く。操作は `persistEventAndSnapshot` を 2 回呼ぶだけだが、`retention_mode = ttl`、2100 年に固定した時計（`clock.epoch_seconds = 4102444800`）、2 回目の `retention-query` の `history_pages`、履歴の `ttl` の観測を使う。TTL 方式の保持と `Clock` がなければ通らない。
- PR 16 の `--require-all` は、全ケースがそろった後（PR 15 の後）に付ける。

各 PR の詳細は次のとおりである。

#### PR 1: 版上げの保護と版の計算

- `lib-bump-version.yml` に、保護の手順を足す。`lib/Cargo.toml` の版が開発版（pre-release。版に `-` を含む）のとき、`level = major` 以外（`auto` を含む）で起動したら失敗にする。理由: 最初の開発版のタグを打つまで、最新のタグは `v3.0.6` のままである。この間に `patch` や `auto` で起動すると、`next-semver.py` が `3.0.7` を計算し、版が `4.0.0-alpha.N` から `3.0.7` に下がる。`level = major` は止めない。正式版は `level = major` で出すからである。
- `.github/next-semver.py` を、開発版に対応させる。確認した事実:
  - `next-semver.py` は、正規表現 `.*v?(\d+\.\d+\.\d+)` で版の数字 3 つだけを取り出し、`-alpha.1` などの部分を無視する。
  - `lib-bump-version.yml` は、`git describe --abbrev=0 --tags` の結果（最新のタグ）をこのスクリプトへ渡す。
  - したがって、最新のタグが `v4.0.0-alpha.1` のとき、スクリプトは `4.0.0` を取り出す（コードを読んで確認した。`semver` の Python モジュールがないので、実行はしていない）。`level = major` なら、正式版が `4.0.0` でなく `5.0.0` になる。
  - 規則（案）: 最新のタグが開発版のとき、`major` は、その開発版の核の版（`4.0.0`）を返す。他の `level` は失敗で終える。最新のタグが `v3.0.6` のときの `major` は、これまでどおり `4.0.0` を返す。
- 新しい試験のファイル（例: `test_next_semver.py`）を `.github/` に足し、上の規則を固定する。`ci.yml` の `release-control` は、`python3 -m unittest discover -s .github -p 'test_semver_level.py'` だけを実行する。試験は `next-semver.py` が `import semver` を使うので、`pip install semver` が要る。`release-control` のジョブに、`pip install semver`（`lib-bump-version.yml` と同じ）を足し、`-p` の指定を広げる。`release-control` は `CI Success` の `needs` に含まれる。
- Rust のソースの変更はしない。

#### PR 2: 公開の条件

- `lib-release.yml` のタグの条件を広げる。確認した事実: 条件 `v[0-9]+.[0-9]+.[0-9]+` は、`v4.0.0-alpha.1` のような版を含まない。開発版のタグ（例: `v[0-9]+.[0-9]+.[0-9]+-*`）も起動の対象にする。
- `cargo publish` の前に、タグ名と `lib/Cargo.toml` の版の一致を検査する手順を足す。一致しなければ、公開せずに失敗にする。理由: `lib-release.yml` は、タグ名と版の一致を検査せずに公開する。手動でタグを打つ運用では、版を上げ忘れたタグや、打ち間違えたタグで、意図しない版が公開される。公開した版は取り消せない。
- 正式版のタグ（`v4.0.0`）にも、同じ検査が効く。

#### PR 3: 版と README

- `lib/Cargo.toml` の `version` を、`4.0.0-alpha.0` に変える。main の Snapshot を pre-release として公開することは、IP 3 の要件である。公開の起動のしかたは、手動である（9 章 D-6）。
- README に、「main は 4.0.0 を開発中。安定版は 3.x」という注意を足す。
- 開発版の保護（PR 1）が先に入っているので、この PR の後に `lib-bump-version.yml` を `patch` や `auto` で起動しても、失敗する。

#### PR 4: 実行器の骨格

- `conformance-runner/`（新しい workspace member）の骨格。データの読み込み・manifest の照合・generators の展開・比較・報告。
- 障害の登録と数え方（5.6.6 節）、実装していない条件の語を `unverified` にする処理（5.5.1 節）。保存先に依存しない部分である。要求の段階の判定は、DynamoDB の SDK 要求に依存するので、PR 10 で作る。
- 保存先がまだないので、全ケースを「未検証」と報告する。`--require-all` は付けない。
- `test-hooks` は使わない。

#### PR 5: 試験環境

- `test-utils`: DynamoDB Local の起動、3 テーブルの作成（6 章）。LocalStack の関数は、名前を実体に合わせる。旧試験は動かし続ける。

#### PR 6: CI の `conformance` ジョブ

- `ci.yml` に `conformance` ジョブを足し、`CI Success` の `needs` に加える（5.7 節）。

#### PR 7: 中核

- `next` モジュールに、2 章の型・trait・エラー・シリアライザ・保持の選択の関数・共通の入口検査・保存先の trait を置く。
- `lib` の `#[cfg(test)]` の単体試験で、各検査（T-9・T-11・T-12・T-13・W-6・W-9・S-1）、エラーのメッセージ（E-2・E-3）、選択の関数（S-2）を確かめる。
- 実行器の `buildAid` を実装する。

#### PR 8: メモリの書き込みと読み取り

- `next::memory` に、`MemoryStorage`・`EventStoreForMemory`・書き込み・読み取り（3.2 節・3.3 節）。
- `test-hooks`（`before_commit`・`read_events`・`read_snapshot`・`history_view`。3.5 節）。
- 実行器のメモリの対象。
- `lib` の単体試験:
  - MEM-4: 複数のスレッドから、同じ集約に同じ `seq_nr` を追記する。1 件だけが確定し、他は `OptimisticLock` になる。確定した 1 件が、読み取りで 1 件だけ返る。適合データに同時実行の場面がないので、この単体試験が唯一の確認である。
  - W-8: 飛び番（ヘッドより 2 以上大きい `seq_nr`）が、契約違反（規則 `W-8`）になる。ヘッドがない集約への 2 以上の `seq_nr` も同じである。
  - MEM-2: 独立した保存先と、`clone` で共有する保存先。
  - MEM-3・MEM-12: 保持件数 0 と、保持件数があるときの `Ttl` は設定エラー。保持件数がなく `Ttl` が指定されただけのときは、エラーにしない。
  - MEM-6: 入力と取得結果への変更が、保存値に波及しない。別のシリアライザを持つストアが、同じ `MemoryStorage` を共有したときの扱い（復元できなければ `Serialization`）。
  - R-8: 複数のスレッドで追記と読み取りを繰り返し、`snapshot.seq_nr ≤ head_seq_nr` が常に成り立つ。

#### PR 9: メモリの保持と通知

- `retention_visible_history`・`retention_delete` のフックと、保持処理・通知（3.4 節、2.11 節）。
- `lib` の単体試験:
  - MEM-10: 別の集約の履歴が、保持処理で消えない。
  - MEM-10: イベントだけの追記でも、前回取り残した履歴を片付ける（10 章 Q-16）。
  - MEM-11: 通知は排他制御を解いた後に出る。保持の失敗は、確定した追記の結果を変えない。
- 実行器: 失敗通知の観測（5.5 節）。

#### PR 10: 障害の差し込みの基盤（要求の層）

- 実行器に、`Config::builder().http_client(...)` の差し込み、`Intercept`（`read_before_transmit`）、`RetryConfig::disabled()`、要求の段階の判定、取り消し応答の組み立て（5.6.1 節・5.6.2 節）を作る。
- ライブラリは変えない（4.7 節）。実行器の単体試験で、段階の判定と応答の形を確かめる。DynamoDB Local での動作の確認は、PR 11 で行う。

#### PR 11: DynamoDB の配置と設定照合

- `next::dynamodb` に、`DynamoDbTables`・`DynamoDbOptions`・`open`（DY-8）・未処理の再要求の方針と `RetrySleeper`（2.12.1 節）・項目の属性の組み立て（4.2 節）。
- 実行器の DynamoDB の対象: テーブルの作成、配置の照合（5.5.3 節。`item-shapes` を除く部分）、属性の観測（5.5.2 節）、要求の観測の比較（5.5.1 節の条件の語のうち、設定照合のケースが使うもの。PR 10 の `Intercept` が記録した要求と比べる）、`BatchGetItem` の応答計画、`install_items`。

#### PR 12: DynamoDB の書き込みと最小の読み取り

- トランザクション、D-5 の分類、D-6、D-7 の見積もり（4.4 節）。
- 最小の読み取り: 最新スナップショットの 1 回の `BatchGetItem`（`UnprocessedKeys` が残ったら `Storage`）、journal の `Query`（4.5 節）。`write-errors` の 16 件が、最後に読み取りを呼ぶためである。
- 実行器: PR 10 の要求の層で、`commit` の障害（取り消し理由つき）を登録し、`write-errors` の 16 件を実行する。取り消し応答の組み立ては、PR 10 で作ってある。

#### PR 13: DY-9 の再要求と `read-interleave`

- `UnprocessedKeys` の再要求（2.12.1 節の方針）。
- 実行器: `read-interleave`（5.6.4 節）。

#### PR 14: 削除方式の保持

- 履歴の GSI の `Query`、`select_expired_history`、`BatchWriteItem` の削除と再送（4.6 節）。
- 実行器: 応答計画の `history_pages`・`unprocessed_first_n`（5.6.3 節）。

#### PR 15: TTL 方式の保持と `Clock`

- 印付けの `UpdateItem`、`Clock`（4.6 節）。
- 実行器: 時計の差し込み、配置の照合の `items` の突き合わせ（5.5.3 節）。この PR で、DynamoDB の全ケースが「未検証」でなく実行される。

#### PR 16: `--require-all`

- `conformance` ジョブに `--require-all` を付ける。意味は 5.8 節で定める。

#### PR 17: 移行ツール

- 8 章の移行ツール（形は 9 章 D-1 の C）。

#### PR 18: SQLite・Bigtable の除去

- feature `sqlite`・`sqlite-system`・`bigtable`、該当のモジュール・再エクスポート・試験、`examples/user-account-sqlite`、`test-utils` の Bigtable のエミュレータの関数を削除する（9 章 D-2）。
- CI の `feature-matrix` と `clippy` の `sqlite`・`bigtable` の組を外す。

#### PR 19: 利用例の移行

- `examples/user-account` を、`next` の API へ移す。

#### PR 20: 切り替えと旧 API の削除

- `next` の中身を、クレートの直下へ移す（`next` モジュールをなくす）。旧 API を削除する。
- 削除するもの: `types.rs`（旧 `EventStore`・旧エラー）、旧 `event_envelope.rs`、`key_resolver.rs`、旧 `event_store_backend.rs`、旧 `generic_event_store.rs`、旧メモリ・旧 DynamoDB、旧試験、`event_store_test_support.rs` の旧版、LocalStack の起動関数。
- 利用例のパスを付け替える。CI を更新する（7.4 節）。

#### PR 21: 文書の整備

- README・README.ja.md・`docs/DATABASE_SCHEMA*.md`・移行ガイド（`docs/MIGRATION_GUIDE_v4*.md`）を新契約に合わせる（IP 5 の 5・6）。
- `CHANGELOG.md` と `AGENTS.md` の誤りを直す（IP 4.2）。
- 文書の PR は、コーディネーターのレビューと、独立したレビューで進める（IP 11）。

#### PR 23: 定期実行を戻す

- 正式リリースの後に行う（7.5 節）。

### 7.3 旧 API との共存と削除の時点

- **共存**: PR 7 から PR 19 までは、旧 API をクレートの直下に残し、動かし続ける。新しいコードは `next` モジュール（`#[doc(hidden)] pub mod next`）に置く。決定（コーディネーター、2026-10-06。理由: 各 PR を小さく保ち、main の CI を通し続けられる）。共存する理由は、旧 API を使う利用例・旧試験・旧 Bigtable・旧 SQLite が、切替の PR までコンパイルできる必要があるからである。
- **削除**: PR 20 で、旧 API・旧試験を削除する。旧保存先（SQLite・Bigtable）は PR 18 で削除する。切替の後は、旧い名前の別名・`#[deprecated]` の橋渡し・version を変換する互換の経路を残さない。旧データの支援は、移行ツール（PR 17）だけである（8 章）。
- 旧 API を使う利用者は、3.x に留まる。3.x の修正が必要になった場合は、最後のタグ（`v3.0.6`）から保守ブランチ（`release/3.x`）を切る（IP 3）。そのための CI の変更は、必要になった時点で行う。

### 7.4 各 PR で main の CI を通し続ける方法

確認した現行の CI（`.github/workflows/ci.yml`）のジョブごとに、方法を示す。

| ジョブ | 内容（確認した事実） | 各 PR での方法 |
|:--|:--|:--|
| `release-control` | 版上げの判定の単体試験と `actionlint`。`semver` の Python モジュールを入れていない | PR 1 で `pip install semver` を足し、`next-semver.py` の新しい試験のファイルを足して、`-p` の指定を広げる（7.2 節の PR 1）。PR 2 で `lib-release.yml` を変えるので、`actionlint` が通ることを確かめる |
| `lint` | 適合データの manifest の照合（`manifest.py verify` と SHA-256）と、nightly の `cargo fmt -- --check`（`rustfmt.toml`） | 新しいコードを `rustfmt.toml` で整形する。`conformance/` を変えない（版 1.0.0 の SHA-256 は変えない） |
| `test-lib` | `cargo test -p event-store-adapter-rs --all-features`（Docker を使う試験を含む） | 旧試験は PR 20 まで動かし続ける。新しい試験は、DynamoDB Local を使う。PR 5 の後、新しい試験はコンテナを共有する |
| `feature-matrix` | `--no-default-features`、`sqlite`、`sqlite-system`、`dynamodb`、`bigtable`、`all-features` の組。クラウド SDK の漏れ、`hashlink`、手書きの `unsafe impl` を検査する | PR 7 から PR 17 の間は、既存の組を変えない。`test-hooks` の組（`--features test-hooks`、`--features dynamodb,test-hooks`）を PR 8 で足し、`migration` の組を PR 17 で足す。PR 18 で `sqlite`・`bigtable` の組を外す。手書きの `unsafe impl` の禁止は、新しいコードにも効く。型の目印は `PhantomData<fn() -> T>` の形（現行の慣行）にして、`Send`・`Sync` を自動導出に任せる |
| `clippy` | 各 feature の組で `cargo clippy --workspace --all-targets -- -D warnings` | 新しいコードは警告なしにする。`conformance-runner` はワークスペースの一員なので、同じ組で検査される（feature の統合の注意は、`ci.yml` のコメントにある）。PR 18 で `sqlite`・`bigtable` の組を外す |
| `audit` | `cargo-deny check advisories licenses` | 新しい依存（`uuid`、`sha2` など）を足したら、PR の中で通す。`audit` は `CI Success` の `needs` に含まれない（確認した事実）。含めるかは、この文書の範囲外 |
| `conformance`（新規） | 適合実行器 | PR 6 で足す。PR 15 までは、「未検証」を許す。PR 16 で `--require-all` を付ける |
| `ci-success` | `lint`・`test-lib`・`feature-matrix`・`clippy`・`release-control` の結果を集める | PR 6 で `conformance` を `needs` と判定に足す |

- 1 つの PR で、旧 API の公開された形を壊さない（PR 20 まで）。したがって、利用例は PR 19 まで旧 API でコンパイルできる。PR 19 で `next` の API へ移し、PR 20 でパスを付け替える。
- 各 PR で、実装した規則群のケースが実行され、成功することを、`conformance` ジョブの報告で確認する。未接続のケースは「未検証」であり、CI の成功は、完全な適合の証明ではない。正式リリースの前に、全ケースの成功と `CI Success` を確認する（IP 5）。
- Renovate の依存更新が書き直しの PR と衝突する恐れがある（IP 11）。段階 2 の間のメジャー更新の取り込みを、書き直しの後に回す方法は、この文書の範囲外である。

### 7.5 リリース

決定（コーディネーター、2026-10-06。9 章 D-6）: pre-release の公開は手動である。自動では公開しない（IP 3 の結果）。

#### 7.5.1 開発版（pre-release）の出し方

1. 版を `4.0.0-alpha.N`（N は 1 から順に上げる）に上げる PR を入れる。`lib/Cargo.toml` の `version` を、`cargo set-version -p event-store-adapter-rs 4.0.0-alpha.N`（cargo-edit）で書き換える。PR の件名は Conventional Commits の形にする。`lib-bump-version.yml` は使わない。
2. マージした後、そのマージコミットに、注釈つきのタグ `v4.0.0-alpha.N` を、手動で作って push する。
3. タグの push が `lib-release.yml` を起動する。`lib-release.yml` は、タグ名と `lib/Cargo.toml` の版の一致を検査する（PR 2）。一致しなければ、公開しない。一致すれば、`cargo publish` で crates.io に公開する。
4. 公開の時機は、オーナーが選ぶ。動くまとまりごとに選ぶ（例: PR 9・PR 15・PR 20 の後）。
5. 公開した版は取り消せない。取り消しではなく `yank` だけできる（0.3 節の一般的な知識。今回、実機では確認していない）。Cargo は、版の要求が pre-release を明示しない限り、pre-release を選ばない。公開した版は、3 系の利用者に自動では届かない。

最初の開発版のタグ（`v4.0.0-alpha.1`）を打つまで、最新のタグは `v3.0.6` のままである。この間に `lib-bump-version.yml` を `patch` や `auto` で起動すると、`3.0.7` を計算して、版が下がる。この起動は、開発版の保護（PR 1）が失敗にする。

#### 7.5.2 正式版

- 受け入れ条件（IP 5）を満たした後、オーナーの承認を得て、`lib-bump-version.yml` を `level = major` で手動で起動する。次の版は、最新のタグから `next-semver.py` が計算する。最新のタグが `v3.0.6`（開発版のタグがない）でも、開発版のタグ（`v4.0.0-alpha.N`）でも、PR 1 で直した後は、`4.0.0` になる。版を書き、タグ `v4.0.0` を打つ。タグの push が `lib-release.yml` を起動する。
- pre-release の公開は、正式版 `4.0.0` の承認を兼ねない。

#### 7.5.3 正式版の後に、定期実行を戻す（PR 23）

IP 3 の手順 4 のとおり、メジャーを出した後に、定期実行を戻す。

1. `lib-bump-version.yml` の `on:` に、`schedule`（`cron: '0 0 * * *'`）を戻す。[PR #244](https://github.com/j5ik2o/event-store-adapter-rs/pull/244) が外したものである。
2. 確認した事実: 現行の `lib-bump-version.yml` は、`inputs.level` が空のとき（定期実行の起動）、段階の値を空のままにして、版上げの手順をすべて飛ばす。定期実行を戻すときに、空の入力を `auto` として扱うように直す。
3. 戻した後は、main に入った変更が、毎日 00:00 UTC に、Conventional Commits から判定した版で公開される（IP 3 の手順 1 以前の運用）。
4. 戻す時機は、正式版（`4.0.0`）を出した後である。正式版の前は、開発版の保護（PR 1）が、`auto` の起動を失敗にする。

## 8. 移行の案内

2 つに分ける。

- 現行メジャー（3.x）の利用者が、コードを 4.0 へ移す案内（8.1）
- 旧データを新しい配置へ移す手順と、移行ツール（8.2）

### 8.1 利用者向けの移行の案内

PR 21 で、`docs/MIGRATION_GUIDE_v4.md` と `docs/MIGRATION_GUIDE_v4.ja.md` を作る（現行は `docs/MIGRATION_GUIDE_v3.md` と `.ja.md`）。構成は次のとおりである。

| 節 | 内容 |
|:--|:--|
| 全体の変更点の一覧 | 2.13 節の対応表を、利用者の言葉で書き直す |
| 1. 依存と feature | バージョンを 4 に上げる。`bigtable`・`sqlite`・`sqlite-system` は 4.0 にない。使う利用者は、3.x に留まる（IP-D3）。DynamoDB は feature `dynamodb` |
| 2. 集約 ID | `Display`・serde の実装を求めなくなった。型名に `-` を含められない（T-11）。型名と値から、ライブラリが aid を組み立てる（T-1）。aid が 1024 バイトを超えると契約違反（T-12） |
| 3. 番号と時刻 | `seq_nr` は `u64`。上限は 2^53 − 1（T-9）。`occurred_at` は、エポックからのナノ秒が符号付き 64 ビットに収まる範囲（T-13） |
| 4. スナップショット | `SnapshotEnvelope::new(state, seq_nr)`。`version` は廃止。`manifest` を足せる（T-10） |
| 5. 書き込み | `expected_version` を渡さない。`persist_event(event)` と `persist_event_and_snapshot(event, snapshot)`。最初のイベント（`seq_nr = 1`）も `persist_event` でよい。`snapshot.seq_nr() == event.seq_nr()` が必要（W-9）。照合は、イベントの `seq_nr` がヘッドの `seq_nr + 1` かどうかで決まる（W-8） |
| 6. 読み取り | `get_latest_snapshot_by_id` は `SnapshotRead` を返す。スナップショットがなくても、ヘッドがあれば `Some` が返る（R-3）。その場合は `seq_nr = 1` から全イベントを読む。リプレイで得た最後の `seq_nr` が、受け取った `head_seq_nr` に届いているかを確かめる（R-7、推奨）。次のイベントの番号は、最後の `seq_nr + 1` |
| 7. エラー | `EventStoreError` の 5 つの変種。再試行してよいのは `OptimisticLock` だけ（読み直してから）。`ContractViolation` は呼び出しの誤り（W-8 の飛び番を含む） |
| 8. 設定 | `DynamoDbTables`・`DynamoDbOptions`・`RetentionSettings`。`KeyResolver`・シャード数は廃止。`with_keep_snapshot_count(Some(0))` は設定エラー |
| 9. 保持処理 | 保持の失敗は書き込みの失敗として返らない。`tracing` のログで知らせる。追記は確定しているので、同じイベントを再送しない。再送は、確定済みの `seq_nr` の重複として `OptimisticLock` になる（W-7・W-8）。取り残した履歴は、次の追記の後の保持処理が選び直して片付ける。メモリは次の追記（イベントだけでもよい）、DynamoDB は次のスナップショット付きの追記である（S-4、MEM-10、D-9） |
| 10. メモリ | `MemoryStorage` を作り、clone して共有する。`A`・`P` に `Clone` を要求しない |
| 11. 旧データ | DynamoDB の 3.x のデータは、4.0 で読めない。8.2 の移行ツールを使う |

利用者のコードの例として、`examples/user-account` の `find_by_id` を前後で示す案。

```rust
// 4.0: スナップショットなしでも復元できる。ヘッドの番号は別に返る。
pub async fn find_by_id(&self, id: &UserAccountId) -> Result<Option<ReplayedUserAccount>, RepositoryError> {
  let read = match self.event_store.get_latest_snapshot_by_id(id).await.map_err(Self::to_repository_error)? {
    None => return Ok(None), // ヘッドがない = 集約が存在しない（R-1）
    Some(read) => read,
  };
  let head_seq_nr = read.head_seq_nr();
  let (snapshot, _) = read.into_parts();
  // スナップショットがなければ 1 から、あれば seq_nr + 1 から読む（R-3・R-4）
  let (from_seq_nr, snapshot_seq_nr) = match &snapshot {
    Some(snapshot) => (snapshot.seq_nr() + 1, snapshot.seq_nr()),
    None => (1, 0),
  };
  // スナップショットがヘッドに届いていれば、読むイベントはない。番号が上限（SEQ_NR_MAX）のスナップショットで
  // seq_nr + 1 を渡すと T-9 の範囲外になるので、読まない
  let events = if snapshot_seq_nr < head_seq_nr {
    self
      .event_store
      .get_events_by_id_since_seq_nr(id, from_seq_nr)
      .await
      .map_err(Self::to_repository_error)?
  } else {
    Vec::new()
  };
  let last_seq_nr = events.last().map(|event| event.seq_nr()).unwrap_or(snapshot_seq_nr);
  if last_seq_nr < head_seq_nr {
    // 結果整合の遅れで、ヘッドに届いていない。呼び出し側が読み直す（R-7）
    return Err(RepositoryError::StaleRead { last_seq_nr, head_seq_nr });
  }
  let state = UserAccount::replay_from(snapshot.map(SnapshotEnvelope::into_aggregate), events);
  Ok(Some(ReplayedUserAccount { state, seq_nr: last_seq_nr }))
}
```

上の例は、移行の案内の構成を示す概略である。`RepositoryError::StaleRead`・`UserAccount::replay_from`・`to_repository_error` は、利用例側の関数で、ライブラリの API ではない。実際のガイドの例は、PR 19 で `next` の API へ移し、PR 20 でパスを付け替えた利用例からそのまま写し、コンパイルできる形にする。

### 8.2 旧データの扱いと移行ツール（IP-D8・D-8）

#### 8.2.1 範囲

| 移行元 | 提供するもの | 根拠 |
|:--|:--|:--|
| rs v3 の DynamoDB（既定のキーの配置） | 移行ツール（PR 17） | IP-D8、`dynamodb.md` の 11 章 |
| rs v3 の DynamoDB で、独自の `KeyResolver` を使っていたもの | なし。範囲外 | `dynamodb.md` の 11 章。手順の中で、旧 pkey・旧 skey が既定の形でない項目を見つけたら、移行を止める（P-22） |
| rs v3 の Bigtable・SQLite | なし。4.0 に保存先がない | IP-D3。保存先を出し直すときに、移行ツールを付ける（IP 7） |
| js・java・go の旧配置 | この文書の範囲外 | IP 7 |
| メモリ | なし。永続化しないので、旧データがない | MEM-1 |

- 通常の読み取りは、旧配置を読まない。旧配置を透過的に読む経路（fallback）を作らない。旧データの支援は、移行ツールだけである。
- 旧配置の `pkey` を計算し直す方式は使わない。ツールチェーンの `DefaultHasher` が、書き込み時と同じ値を返す保証がないからである（`dynamodb.md` の 11 章）。

#### 8.2.2 手順（`dynamodb.md` の 11 章）

確認した事実:

- v3.0.0 以降、journal 項目は `manifest` 属性を持つ（現行の `put_journal` と、`manifest` を追加したコミット `081ba72` が最初に含まれるタグが `v3.0.0` であることで確認した）。journal の属性は、キー以外は新しい配置と同じ形である。
- v3 の journal 項目は、`pkey`・`skey`・`aid`（利用者の `to_string()` の値）・`seq_nr`・`payload`・`occurred_at`・`manifest` を持つ。
- v3 の journal の `occurred_at` は、エポックからのナノ秒である（v3.0.0 以降の `format_occurred_at` が `timestamp_nanos_opt()` で書く）。新しい配置と同じ単位なので、変換せずに写す。ナノ秒が符号付き 64bit に収まる値だけが書かれているので、T-13 の範囲にも収まる。ミリ秒で書いているのは、snapshot の `last_updated_at` だけである。
- v3 の snapshot 項目は、`pkey`・`skey`・`payload`・`aid`・`seq_nr`・`version`・`ttl`（期限がないときは 0 を書く）・`last_updated_at` を持つ。`manifest` はない。現在の項目の `skey` の末尾の数値は 0、履歴は履歴の `seq_nr` である（`lib/src/event_store_for_dynamodb.rs` で確認した）。

手順は、`dynamodb.md` の 11 章の番号のとおりである。手順 3 の読み取りと集計、手順 4 の検査、手順 6 の読み取りを先に行い（検査の段）、その後に、手順 3・5・6 の書き込みを行う（書き込みの段）。この二段は、仕様の 11 章が認める（P-45、2026-10-06 オーナー決定。10 章 Q-12）。

| 手順 | 担当 | 内容 |
|:--|:--|:--|
| 1 | 運用者 | 新しい 3 テーブル（journal・snapshot・head）と履歴の GSI を作る。ライブラリはテーブルを作らない。ツールは、新しいストアを `open`（DY-8）して設定項目を書く |
| 2 | 運用者 | 旧テーブルへの書き込みを止める。ツールは強制できない。止めないと書き直しの間の書き込みを取りこぼす |
| 3 | ツール | 旧 journal を、強整合の `Scan`（`LastEvaluatedKey` が返らなくなるまで）で読む。各項目の旧 `pkey` の末尾のシャード番号と、旧 `skey` の末尾の `seq_nr` を取り除いて、型名と値を取り出す。旧 pkey・旧 skey が既定の形でなければ止める（P-22）。型名に `-` を含み、対応表にないものがあれば止める（P-23）。型名に `-` を含み、対応表にあるものは、新しい型名に置き換える。型名に `-` を含まないものは、そのまま使う。**新 journal の項目は、`aid`・`seq_nr`・`occurred_at`・`manifest`・`payload` だけにする。旧 `aid` 属性（利用者の文字列化）は、置き換え後の型名と値から、ライブラリが組み立てる新しい aid（T-1）に書き換える。`pkey`・`skey` は捨てる。** manifest のない古い項目は、空文字列にする（T-2。コーディネーターの決定、2026-10-06）。検査の段では、読みながら集約ごとに集計し、書き込みはしない。書き込みの段で、旧 journal をもう一度読み直して、新 journal へ書く |
| 4 | ツール | 検査の段で、集約ごとに、`seq_nr` が 1 から最大値まで欠けずに並ぶか（件数が最大値と等しいか）確かめる。欠けた集約があれば、その一覧を出して止める（P-36）。欠番は利用者が直してから、移行し直す。追加の検査は、手順の表の後に書く |
| 5 | ツール | 書き込みの段で、集約ごとに、検査の段で集計した最大の `seq_nr` でヘッド項目を作る。新 journal を読み直して最大値を求めない。`events` には、その `seq_nr` のイベントを、新 journal から強整合の `GetItem` で読んで入れる（P-37）。旧 current 項目の `seq_nr` は、スナップショットの位置なので、ヘッドの `seq_nr` に使わない |
| 6 | ツール | 旧 snapshot を、手順 3 と同じ読み方で `Scan` する。検査の段で読み、書き込みの段でもう一度読んで、現在の項目と履歴を新しいキーで書く（P-20）。`version` 属性は捨てる。旧 `aid` 属性は、手順 3 と同じく新しい aid に書き換える。`ttl` が 0 の項目は属性を持たせず、印のない履歴には `active_history_seq_nr` を付ける。`ttl` が正の値の項目（印付きの履歴）は、`ttl` を残し、`active_history_seq_nr` を付けない（コーディネーターの決定、2026-10-06。手順 6 の文から読み取れる）。`manifest` は空文字列を入れる（T-10、P-21） |
| 7 | 運用者 | 4.0 のライブラリで書き込みを再開する。旧テーブルは、運用者が確認の後に捨てる。ツールは旧テーブルを書き換えない・削除しない |

検査の段で止める条件は、次のとおりである。すべて一覧にして、新しいテーブルへデータの項目を何も書かずに止める（設定項目は手順 1 で書く）。旧データの精度や型の情報が失われているものを、推測で補わない（IP 7）。

| 条件 | 規則・根拠 |
|:--|:--|
| 旧 pkey・旧 skey が既定の形でない項目 | P-22 |
| 型名に `-` を含み、対応表にない型名 | P-23 |
| 欠番のある集約 | W-8、P-36 |
| 最大の `seq_nr` が 2^53 − 1 を超える集約（v3 の `seq_nr` は `usize` で、上限の検査がなかった） | T-9 |
| 型名を置き換えた後の aid が、UTF-8 で 1024 バイトを超える集約 | T-12 |
| 最大の `seq_nr` のイベントを head に載せると、見積もり（4.4.3 節の上界）が 409600 バイトを超える集約。旧 journal の payload が大きいと、head は payload を載せるので journal より大きくなる | D-7 |
| 孤立したスナップショット（旧 journal に集約のない、旧 snapshot の項目） | コーディネーターの決定（2026-10-06） |
| 旧 journal の最大の `seq_nr` を超える `seq_nr` を持つスナップショット（現在の項目・履歴のどちらも） | 同上 |

- 並列 `Scan` や、書き込みを止めた後の時点のエクスポートで代えてよい（DY-19）。ツールは、まず単純な `Scan` を使う案。
- 二段にした理由: 欠番や不正なキーを見つけたとき、新しいテーブルに書く前に止められる。
- 書き込みの段の 2 回目の読み取り: 旧 journal と旧 snapshot を、もう一度 `Scan` する。手順 2 で書き込みを止めているので、2 回とも同じ内容を読む。全イベントをメモリに持たない。メモリの大きさは、集約の数に比例し、イベントの総量に比例しない（集約ごとに、最大の `seq_nr`・件数・aid のバイト数・head の見積もりだけを持つ）。費用は、旧テーブルの読み取り容量が 2 倍になることである。
- 再実行の保護: 検査の段の最初に、新しい 3 テーブルを `Scan` し、設定項目（`aid` が `__config__`）以外の項目が 1 件でもあれば、移行を始めずに止める。途中で止まった移行は、運用者が新しい 3 テーブルを作り直して、やり直す（コーディネーターの決定、2026-10-06）。
- 書き込みの段の書き込みは、`attribute_not_exists(aid)` の条件つき `Put` にする案。条件が不成立ならその項目を一覧に載せて止める。理由: 上の保護の確認の後に別の書き込みが入っても、上書きしない。
- ツールの入力は、旧 journal・旧 snapshot の 2 テーブル名、新 journal・新 snapshot・新 head の 3 テーブル名と履歴の GSI 名、`Client`（認証・endpoint）、型名の対応表である。rs v3 に head のテーブルはない。新 head は、手順 5 で、検査の段で集計した最大の `seq_nr` から作る。旧 GSI は使わない（手順 3・6 は、テーブル本体を `Scan` する）。出力は、移行した集約・イベント・スナップショットの件数と、止めた理由の一覧である。
- ツールが使う IAM は、旧 2 テーブルへの `Scan`（と `DescribeTable`）、新 3 テーブルへの `Scan`（再実行の保護）・`GetItem`・`PutItem`・`BatchGetItem` である。仕様はツールの権限を定めない。実装時に確認する。

#### 8.2.3 ツールの形

決定（オーナー、2026-10-06。9 章 D-1 の C）: 移行の本体をライブラリの関数として作り、それを呼ぶ薄い実行ファイルも付ける。

- 本体は、`lib` の feature `migration`（`lib/src/migration/`）の関数である。新しい配置の項目を、ライブラリと同じ属性・同じ値の形で書く。形がずれて、移行したデータをライブラリが読めなくなることを防ぐ。
- 薄い実行ファイルは、workspace の中に置き、`publish = false` にする。引数を読み、本体の関数を呼ぶだけである。リポジトリを取得して `cargo run` で実行できる。
- どの形でも、上の手順は変わらない。

## 9. 判断が要る点

実装計画 10 章のとおり、この文書の判断は、設計段階で決め、コーディネーターがレビューした。判断が要るものだけを、オーナーに上げた。各項目に、選択肢・それぞれの利点と欠点・推奨・決めた者・日付・理由を書く。この章に、決めていない項目はない（2026-10-06）。

| 項目 | 題 | 状態 |
|:--|:--|:--|
| D-1 | 移行ツールの形 | 決定 C（オーナー、2026-10-06） |
| D-2 | SQLite・Bigtable を最初のメジャーから外す方法 | 決定 A（コーディネーター、2026-10-06。IP-D3 で「外す」は合意済み。旧メジャー（3.x）には残る） |
| D-6 | 開発版（pre-release）の番号と公開の起動のしかた | 決定 A（コーディネーター、2026-10-06） |

D-3〜D-5・D-7〜D-10 は、決まったので本文へ移した。番号は再利用しない。移した先と決定は、次のとおりである。

| 項目 | 題 | 決定 | 決めた者 | 移した先 |
|:--|:--|:--|:--|:--|
| D-3 | 変更フィードの扱い | ヘッド遷移を組み立てる関数も、再同期（DY-15）の補助も含めない | オーナー（2026-10-06、全言語共通） | 1.4 節 |
| D-4 | 非同期の境界（`&self`・`async_trait`・`Send`） | `#[async_trait]` と `&self`。Future は `Send` | コーディネーター（2026-10-06） | 2.1 節・2.8 節 |
| D-5 | `SeqNr` の型 | `pub type SeqNr = u64` | コーディネーター（2026-10-06） | 2.4 節 |
| D-7 | 旧 API との共存の方法 | `#[doc(hidden)] pub mod next` に置き、PR 20 で直下へ移す | コーディネーター（2026-10-06） | 2.2 節・7.3 節 |
| D-8 | 保持処理の失敗を知らせる経路 | `tracing` の WARN を必須とする。コールバックは公開 API に足さない | コーディネーター（2026-10-06） | 2.11 節 |
| D-9 | 適合実行器の置き場所 | workspace member `conformance-runner/` | コーディネーター（2026-10-06） | 5.1 節 |
| D-10 | メモリの保存先の共有の表現 | `MemoryStorage` を公開の型にする | コーディネーター（2026-10-06） | 2.12 節・3.1 節 |

### D-1 移行ツールの形

**背景**: IP-D8 は、rs v3 の DynamoDB の旧データを新しい配置へ移すツールを、新しいメジャーに付けると定める。形は、実装計画 10 章が設計段階の決定事項として挙げる。ツールは、新しい配置の項目を、ライブラリと同じ属性・同じ値の形で書く必要がある（`dynamodb.md` の 5 章・11 章）。

| 案 | 内容 | 利点 | 欠点 |
|:--|:--|:--|:--|
| A | workspace の実行ファイル（別の crate） | 利用者が Rust を書かずに実行できる。運用の手順（書き込み停止の確認など）を、コマンドの引数として分けられる。ライブラリの依存を増やさない | 項目の組み立てを共有するには、ライブラリが内部の関数を公開する必要がある。公開しなければ重複実装になり、形がずれる恐れがある。`cargo install` で配るには、別の crate として crates.io に公開する必要があり、`lib-release.yml`（`-p event-store-adapter-rs` だけを公開）の変更が要る |
| B | ライブラリの関数（feature で囲む。例: `migration` モジュール） | 項目の組み立てを、ライブラリの内部と共有できる。形がずれない。同じ crate なので版が揃う。DynamoDB Local の試験を同じ場所に書ける | 利用者が、呼び出す小さな `main` を自分で書く必要がある。一度だけ使う機能に、公開 API として semver の責任を負う |
| C | B の関数と、薄い実行ファイル（workspace 内。公開しない） | B の利点に加え、リポジトリを取得して `cargo run` で実行できる | 実行ファイルの保守が増える。`cargo install` では入らない |

**推奨**: C。理由は 3 つある。(1) 新しい配置の項目を書くコードを、ライブラリと共有することが最優先である。形がずれると、移行したデータをライブラリが読めない。(2) 運用者が Rust を書かずに実行できる。(3) 公開 API は feature に閉じ込められ、「移行専用」と明記できる。公開 API の責任が重いと判断するなら、A を選び、組み立てを重複して、適合データの `item-shapes` で形を検査する方法もある。

**決定**: C。決めた者: オーナー。日付: 2026-10-06。理由: 実装計画 10 章の「決めたこと（2026-10-06）」のとおり、移行の本体をライブラリの関数として作り、それを呼ぶ薄い実行ファイルも付ける。上の推奨の理由 3 つによる。具体は 8.2.3 節。

### D-2 SQLite・Bigtable を最初のメジャーから外す方法

**背景**: IP-D3 は、SQLite・Bigtable を最初のメジャーから外し、段階 5 で新契約により出し直すと定める。廃止ではない。確認した現行の状態: feature は `sqlite`・`sqlite-system`・`bigtable`。実装は旧い `EventStore`・`StorageBackend`（`version` の照合）に依存しており、新契約のままでは動かない。CI に `feature-matrix` と `clippy` の組がある。`test-utils/src/bigtable.rs`・`examples/user-account-sqlite`・`deny.toml`（`all-features` で検査）がある。

| 案 | 内容 | 利点 | 欠点 |
|:--|:--|:--|:--|
| A | PR 18 で、feature・モジュール・再エクスポート・試験・利用例・CI の組を、main から削除する。履歴は git に残る。段階 5 で新規に作る | main が単純になる。旧契約のコードが残らない。`rusqlite`（同梱の SQLite の C のソース）・`tonic`・`googleapis-*` の依存が消え、`audit` の対象も減る。feature を後で足すのは、非破壊の追加である | 段階 5 まで、4.x で `features = ["sqlite"]` を指定するとエラーになる。SQLite・Bigtable の利用者は、3.x に留まる必要がある |
| B | 別の crate（`event-store-adapter-sqlite-rs` など）に分ける | 段階 5 で追加するとき、`lib` の feature を増やさずに済む | 今は新契約で書き直せないので、中身がない crate か、動かない旧コードを持つことになる。crate 名の確保・公開・release workflow の変更が要る。IP-D3 は「同じメジャーのマイナー版で出し直す」と書くが、別の crate は同じ crate のマイナー版ではない |
| C | feature の名前だけ残し、有効にすると `compile_error!` で未提供を案内する | 利用者に理由が伝わる。段階 5 で feature を戻すだけで済む | 未提供の feature を宣言し続ける。`compile_error!` のコードが残る。CI の組の扱いが要る |

**推奨**: A。理由は、IP-D3 の「外す」に最も素直で、旧契約のコードを残さないからである。名前が消えることは、Cargo の明確なエラーで利用者に伝わる。feature を後から足すのは、非破壊の追加（マイナー版）で行える。SQLite・Bigtable の利用者への案内は、移行ガイド（8 章）に書く。

**決定**: A。決めた者: コーディネーター。日付: 2026-10-06。理由: IP-D3 で「外す」ことは合意済みである。実装計画 10 章の「決めたこと（2026-10-06）」のとおり、rs の SQLite・Bigtable は新しいメジャーから削除する。旧メジャー（3.x）には残る。旧契約のコードを残さない。

### D-6 開発版（pre-release）の番号と公開の起動のしかた

**背景**: 実装計画 3 章（IP 3）は、新契約の変更を main に PR ごとに squash マージし、「main の Snapshot は、次のメジャーの版（java の `2.0.0-SNAPSHOT` など）で公開されるようにする」と定める。公開は必須の要件である。この項で決めるのは、公開するかどうかではない。次の 3 点である。

- 開発版の番号の書き方。
- 公開の起動のしかた。
- 公開の時機と頻度。

確認した事実:

- crates.io に Snapshot という仕組みはない。Rust で対応するものは、次のメジャーの pre-release（例: `4.0.0-alpha.N`）である。
- `lib-release.yml` は、タグ `v[0-9]+.[0-9]+.[0-9]+` の push だけで `cargo publish` する。`v4.0.0-alpha.1` のような pre-release のタグは、この条件に合わない。タグ名と `Cargo.toml` の版の一致も検査しない。
- `.github/next-semver.py` は、版の数字 3 つだけを正規表現で取り出し、`-alpha.1` などの部分を無視する。`lib-bump-version.yml` は、最新のタグ（`git describe --abbrev=0 --tags`）をこのスクリプトへ渡す。最新のタグが `v4.0.0-alpha.1` のとき、`level = major` は `5.0.0` を返す（コードを読んで確認した。実行はしていない）。正式版 `4.0.0` を出す前に、直す必要がある（7.2 節の PR 1）。
- 最初の pre-release のタグまで、最新のタグは `v3.0.6` である。`lib-bump-version.yml` を `patch` や `auto` で起動すると、`3.0.7` を計算する。`4.0.0-alpha.N` の版から `3.0.7` に下がる。
- `lib-bump-version.yml` は手動の起動（`workflow_dispatch`）だけで動く。`level` の選択肢は auto・patch・minor・major で、pre-release の段階がない。IP 3 の結果（2026-10-05）は、「main に入れた変更が自動で公開されることはない」と記す。
- `ci.yml` の `release-control` は、`test_semver_level.py` だけを実行し、`semver` の Python モジュールを入れていない。`next-semver.py` の試験はない。

一般的な知識（今回、実機では確認していない。0.3 節）:

- crates.io に公開した版は、上書きも削除もできない。`yank` で新しい依存の解決から外せるだけである。
- Cargo は、版の要求が pre-release を明示しない限り、pre-release を選ばない。このため、公開した pre-release が、現行の `3` 系を使う利用者に自動では届かない。

main の Snapshot は、pre-release として crates.io に公開する。公開の起動は、手動である。main への変更を自動で公開しない（IP 3 の結果）。案の違いは、公開の時機である。

| 案 | 内容 | 利点 | 欠点 |
|:--|:--|:--|:--|
| A | 公開の時機を、動くまとまりごとに、オーナーが選ぶ。main の `version` を `4.0.0-alpha.0` にする（PR 3）。公開は、版を `4.0.0-alpha.N` に上げる PR を入れた後に、タグ `v4.0.0-alpha.N` を手動で push して起動する。PR 1・PR 2 で、版上げの保護・`next-semver.py`・`lib-release.yml` のタグの条件とタグと版の一致の検査を整える。時機の例は、PR 9・PR 15・PR 20 の後 | IP 3 の結果（main に入れた変更が自動で公開されることはない）と合う。公開を、動くまとまりごとに選べる。誤った公開が起きにくい | 公開のたびに、オーナーの手作業が要る。公開の間隔が空くと、公開された版が main に遅れる |
| B | 公開は A と同じ手動の起動にする。ただし、PR 6 の後など、公開の時機を早い段階に固定する。版の番号は `4.0.0-alpha.1` から順に上げる | A の利点に加え、公開の手順（タグの条件・タグと版の一致の検査・crates.io への反映）を、中身が小さいうちに通しで確かめられる | 初期の版は、中身がほとんど空である。早い公開は、利用者に空の版を見せる |

版の番号の書き方は、別の判断である。案の例は、`4.0.0-alpha.N`、`4.0.0-rc.N`、`4.0.0-dev.N` である。SemVer は、pre-release の識別子を、数字と文字の並びで大小を決める。`alpha` は `beta` や `rc` より前に並ぶ。ここでは `4.0.0-alpha.N` を例に書く。

**推奨**: A。理由は、IP 3 の結果が自動公開を外していて、手動の起動が現行の運用と合うからである。公開の要件は満たす。PR 1 で、版上げの保護と `next-semver.py` の開発版への対応を入れ、PR 2 で、`lib-release.yml` のタグの条件を pre-release に広げて、タグと版の一致を検査する。`lib-bump-version.yml` に pre-release の段階を足す変更は、必要になってからでよい。公開の時機は、動くまとまりごとにオーナーが選ぶ。

**決定**: A。決めた者: コーディネーター。日付: 2026-10-06。理由: 実装計画 3 章の結果のとおり、main に入れた変更を自動で公開しない。手動の起動は現行の運用と合い、公開の要件も満たす。手順は 7.5 節、準備の PR は 7.2 節の PR 1〜PR 3 である。公開の要件（IP 3）は、決める対象に含まない。

## 10. 未解決の疑問

仕様の読み方が分からない点と、仕様や資料の間で食い違うように見える点を書く。この文書は、仕様を解釈して埋めない。各項目に、根拠と、この設計が暫定でどう扱ったかを書く。10.1 節は、回答を得たものである。10.2 節は、回答を待つものである。暫定の扱いは、回答を得たら見直す。

### 10.1 回答を得た疑問

回答は、コーディネーターのレビュー（2026-10-06）と、同じ日の仕様の決定（P-42〜P-45）による。

| 番号 | 疑問 | 回答 | 反映した箇所 |
|:--|:--|:--|:--|
| Q-1 | 障害の段階は、指示書が 13 段階と書くが、README とスキーマが列挙するのは 12 種類である。13 番目はあるか | 12 段階である。指示書の 13 は、コーディネーターの誤りだった | 5 章の冒頭、5.6 節の表 |
| Q-2 | T-6 は、既定の JSON シリアライザを使う場合に serde の `Serialize`・`DeserializeOwned` を要求してよい、と読むか。任意のシリアライザを使う場合は、serde を要求してはならないと読むか | 暫定案どおり。後者の読み方で設計する。trait の境界は `Send + Sync + 'static` だけにし、serde の境界は既定の JSON を使う生成関数にだけ付ける | 2.7 節、2.12 節 |
| Q-3 | D-7（項目サイズの超過）は契約違反に分類されるが、E-3 は「違反した規則の番号」をメッセージに含めると定める。D-7 は判断の番号であり、規則番号ではない。メッセージに何を含めるか | 暫定案どおり。`ContractRule::ItemSizeLimit` を持ち、表示を `D-7` にする。必須としない | 2.9 節 |
| Q-4 | 項目サイズの見積もりが実際より小さく、DynamoDB が項目サイズの検証エラーを返した場合の分類は何か | 見積もりを実際以上（上界）にする。このため、項目サイズの検証エラーは起きない。起きたら `Storage` で返す | 4.4.3 節 |
| Q-6 | DY-9 の `UnprocessedKeys` の再要求に、上限と待ち方の定めはあるか | 暫定案どおり。DY-8 と同じ再要求の方針（指数バックオフ、上限あり、上限到達は保存先エラー）を使う。保持の `BatchWriteItem` の未処理の再送にも、同じ方針を使う | 2.12.1 節、4.5.1 節、4.6 節 |
| Q-7 | MEM-3 は、変更フィードを要求する設定を設定エラーにすると定める。メモリの API に、そのような設定項目は存在するのか | 暫定案どおり。設定項目を作らない。TTL 方式の要求だけを設定エラーにする（MEM-12） | 2.10 節、3.1 節 |
| Q-8 | 保持の失敗の通知をログで行う場合、ログのイベントの形（対象・項目名）に、仕様上の定めはあるか。実行器が観測するために、固定してよいか | 暫定案どおり。`tracing` の対象 `event_store_adapter::retention` と項目 `category = "retention-failure"` を固定する。仕様の規則ではなく、この実装の取り決めである | 2.11 節 |
| Q-9 | K-1（FNV-1a 64）の 4 件を、ハッシュを使う保存先がない最初のメジャーで、どう報告するか。IP 5 の 1「全ケースを通す」に含まれるか | オーナーの決定（2026-10-06、全言語共通）。最初のメジャーにはハッシュを使う保存先がないので、理由を付けて「対象外」と報告する。成功にも失敗にも数えない。段階 5 でハッシュを使う保存先を出すときに実行する。実装計画 5 章の受け入れ条件 1 にも書かれた | 1.4 節、5.2.1 節、5.8 節 |
| Q-10 | IP 5 の 1「全ケースを通す」は、表現能力による対象外（`milliseconds` の精度、`signed_seq_nr`）を除くと読むか | 表現能力による対象外は、理由を記録する。成功に数えない | 2.4 節、5.2 節、5.8 節 |
| Q-11 | `conformance/README.md` が実行を求める検証の道具が、このリポジトリにない。配布の範囲は、`tools/conformance/manifest.py` だけでよいか。`.gitattributes` は必要か | 配るのは `conformance/` と `manifest.py` だけである。`conformance/.gitattributes`（`* -text`）は、すでにあり、`conformance/manifest.json` にも載っている。generators の参照実装 `materialize` は、配布にないので、README の定めのとおり、実行器が自前で作る | 5 章の冒頭、5.2 節 |
| Q-13 | DY-18 は、3 テーブルが同じリージョンにあることを前提とする。SP-1 は、前提を外れる構成を、検出できるなら設定エラーで拒むと定める。ライブラリは、3 テーブルのリージョンの違いを検出できるか。検出すべきか | 暫定案どおり。検出の手段を確認していないので、検出を実装しない。3 テーブルが同じ `Client` を通ることを、API の形で保証する | 4.2 節 |
| Q-14 | 更新で head の条件が不成立になったとき、旧ヘッドの `seq_nr` が `event.seq_nr − 1` と等しい場合の分類は何か | 暫定案どおり。起こらないはずの状態として、保存先エラー（想定外の取り消し）で返す。黙って成功にも楽観ロックにもしない | 4.4.2 節 |
| Q-15 | 共通場面の `retention-query` の応答計画（`history_pages`）は、メモリでは「論理履歴のフック」へ対応付けるとされる。メモリにはページがない。ページ列を連結して「見える履歴」とみなしてよいか | `conformance/README.md` のとおり、論理履歴のフックへ対応付ける。ページ列を連結して「見える履歴」として返す。連結の順序に意味を持たせない | 5.6 節の表 |
| Q-16 | メモリの MEM-10（イベントだけの追記でも、取り残した履歴を片付ける）を確かめる適合ケースがない。データに足すか、実装側の試験で行うか | 暫定案どおり。保存先ごとに時機を変える。メモリの MEM-10 は、`lib` の単体試験で確かめる。データの追加は、ハブの判断に委ねる | 3.4 節、4.4.4 節、7.2 節の PR 9 |
| Q-5 | TTL 方式の猶予（`ttl_grace_seconds`）の許容域はどこまでか。0 は有効か。上限はあるか | 0 は有効（適合テストデータのスキーマの最小値は 0。印を付ける時刻がそのまま期限になる）。`u64` なので負の値は表せない（負を表せる言語は、共通契約 4 章の「生成時の不正な設定値」として設定エラーにする）。上限は定めない。指揮役の回答（2026-10-06） | 2.10 節 |
| Q-12 | `dynamodb.md` の 11 章は、手順 3 で新 journal へ書きながら集計し、手順 4 で連続性を検査する。「旧テーブルを最後まで読んで検査してから（検査の段）、旧テーブルをもう一度読み直して新 journal へ書く（書き込みの段）」二段に分けてよいか | 二段に分けてよい。`dynamodb.md` の 11 章に、書く前に旧 journal を最後まで読んで手順 4 の検査を済ませてよいと足した（P-45、2026-10-06 オーナー決定） | 8.2.2 節 |
| Q-17 | 取り消し理由が複数あるときの優先順（4.4.2 節）は、仕様にあるか | どれかの項目に `TransactionConflict` があれば楽観ロック。なければ head、journal の条件不成立、その他の順（`dynamodb.md` 6.2、P-43、2026-10-06 オーナー決定）。適合データの説明と同じ扱い | 4.4.2 節 |
| Q-18 | 配置の照合（5.5.3 節）は、2 点を確認したい。(a) `layout.json` の `items` の設定項目 3 種を、どの項目と比べるか。`item-shapes` の設定項目は、実行器が種として入れたものである。(b) 「現在・印付き履歴・設定項目が GSI に載らないこと」を、GSI を `Query` せずに、項目の属性で確かめてよいか | 暫定案どおり。(a) `dynamodb-config-new` でライブラリが書いた設定項目と比べる。(b) 疎な GSI に載るかはキー属性 `active_history_seq_nr` の有無で決まるので、項目の属性で確かめてよい。指揮役の回答（2026-10-06） | 5.5.3 節 |

### 10.2 残る疑問

残る疑問はない。10.2 節にあった Q-5・Q-12・Q-17・Q-18 は、2026-10-06 の仕様の決定（P-43・P-45）と指揮役の回答で解けたので、10.1 節に移した。
