# セキュリティ設計 — u1-backend-features (security-design)

セキュリティ要件（`../nfr-requirements/security-requirements.md` NFR-2.x/NFR-4.x）・技術スタック決定（`../nfr-requirements/tech-stack-decisions.md` D1〜D7）・機能仕様（`../functional-design/functional-spec.md` ワークフロー1〜3）・契約（`../../../inception/contract-design/contract-summary.md` C-1/C-3）を、U1（feature分割・エラー型中立化・Memory準拠化）の具体設計へ落とす。なお性能・スケーラビリティ・信頼性・可観測性の各要件書（performance-requirements / scalability-requirements / reliability-requirements / observability-requirements）はキンド適用によりU1では対象外であり、対応する設計書も作成しない（Q&A Q1確定 — nfr-requirements工程）。

## 依存供給網の隔離設計（NFR-2.1 / NFR-2.2）

- **feature境界**: 契約C-3のとおり `dynamodb` / `bigtable` / `sqlite`（器）を定義し `default = []`。クラウドSDK（aws-sdk-dynamodb / aws-config / tonic / googleapis-tonic-google-bigtable-v2）は `optional = true` とし、対応featureの `dep:` 列挙でのみ有効化する
- **ゲート位置**: モジュール宣言と再エクスポートの両方に `#[cfg(feature = ...)]` を付与（`lib.rs`）。`#[allow(dead_code)]` 抑制は同時に除去（BR1.5）
- **削除**: aws-http / prost / serial_test は依存宣言ごと削除（TD-02/TD-14）
- **効果**: feature未指定の利用者の依存グラフからクラウドSDKが消え、使わないSDKのCVE影響（RUSTSEC通知・監査ノイズ）を受けない

## panic排除とエラー写像設計（NFR-4.1 / NFR-4.4）

写像の正本は機能仕様の決定表（バージョン不一致→`OptimisticLockError`、serde→`SerializationError`、I/O→`IOError`、他→`OtherError`）。設計上の固定点:

- **共通制御でのErr返却**: 作成イベントが `persist_event` に渡された場合は `GenericEventStore` の共通制御が `Err` を返す（BR1.6 — Memoryの既存panic経路を除去し、バックエンド実装にpanic判断を持ち込ませない）
- **メッセージ書式（設計固定 — Q3=A）**: 楽観的ロック失敗の整形文字列は基本形 `optimistic lock failed, aid=<id>, expected_version=<n>` とし、実バージョンが判明するバックエンドのみ `, actual_version=<m>` を付加する
- **メッセージ衛生（NFR-4.4）**: 整形文字列に含めてよいのは集約ID・バージョン情報のみ。接続文字列・資格情報・下位SDKの生エラー文字列は含めない（下位詳細は `IOError` / `OtherError` のソースエラー側に保持し、`OptimisticLockError` へ混入させない）

## スレッド安全設計 — Memory準拠化（NFR-4.2）

同期プリミティブは `Arc<Mutex<HashMap<String, InMemoryStoreState>>>`（Q1=A。キー=aid文字列、値=entities.mdの単一集約レコード `InMemoryStoreState`（events / snapshots））。設計制約（ユーザー確定事項）: **`Arc<Mutex<T>>` は公開APIに一切露出させず、バックエンド内部に隠蔽する**。利用者がロックを管理する場面を作らない。

```rust
// 設計スケッチ（インターフェースレベル）
pub struct EventStoreForMemory<AID, A, E> { /* GenericEventStore<AID, A, E, InMemoryBackend<AID, A, E>> へ委譲 */ }

#[derive(Debug)]
struct InMemoryBackend<AID, A, E> {  // 非公開型 — StorageBackend<AID, A, E>（契約C-2）を実装
  state: Arc<Mutex<HashMap<String, InMemoryStoreState>>>, // キー=aid、値=集約ごとの記録。ロックは外に出ない
  _marker: PhantomData<fn() -> (AID, A, E)>,              // fnポインタ経由でSend/Sync自動導出を阻害しない
}
// Clone は手動実装（Arcの共有クローン）— deriveはAID/A/EにClone境界を要求してしまうため
```

- **隠蔽境界**: 公開面は `EventStoreForMemory::new()` と `EventStore` トレイトのみ（NFR-1.2 — 公開API維持）。ロック型・ガード・`Arc` は公開シグネチャに現れない
- **ロックスコープ**: 各 `StorageBackend` メソッド内で取得し同メソッド内で解放する。`std::sync::Mutex` を用い、**ガードを保持したまま `.await` しない**（クリティカルセクションは同期処理のみ — async対応Mutexを追加依存なしで回避する設計）
- **Clone意味論**: `Clone` は `Arc` の共有クローンとし、クローン間で同一ストアを共有する（現行実装のClone間状態分岐を解消 — 機能仕様ワークフロー3）
- **自動導出**: `Send + Sync` は内部構造（`Arc<Mutex<...>>` と `PhantomData<fn() -> ...>`）から自動導出で成立し、手書き `unsafe impl` は書かない（BR1.7）。既存3バックエンドの手書き `unsafe impl` もU1接触範囲で除去する

## 機密情報の取り扱い（NFR-4.3）

- U1は資格情報を扱う機能を追加しない。設計上の禁止事項として、テスト・examplesを含め認証情報・APIキー・秘密のリテラル埋め込みを行わない（構築フェーズ規範由来の追加項目）
- 検証はgrepベースの目視確認で行う（下記「検証手順」）

## 検証手順（U1時点 — 手動ローカル、Q2=A）

CI化はU3の責務。U3はこのコマンド列をそのままCIマトリクスへ昇格する。

```bash
cargo tree --no-default-features | grep -E "aws-|tonic|googleapis" && echo NG || echo OK  # NFR-2.1
cargo tree --features dynamodb | grep -E "tonic|googleapis" && echo NG || echo OK          # NFR-2.2（bigtable/sqlite単独も同様）
grep -rn "unsafe impl" lib/src/ && echo NG || echo OK                                      # NFR-4.2
grep -rniE "(api[_-]?key|secret|password)\s*=" lib/src/ | grep -v test && echo CHECK || echo OK  # NFR-4.3
# NFR-3.1: feature未指定／各feature単独／全feature のビルド確認（tech-stack-decisions.md NFR-3.1）
cargo build --no-default-features
cargo build --no-default-features --features dynamodb
cargo build --no-default-features --features bigtable
cargo build --no-default-features --features sqlite
cargo build --all-features
```

## Assumptions & Open Questions

None.

## Review

**Verdict:** READY
**Reviewer:** aidlc-architecture-reviewer-agent
**Date:** 2026-08-23T00:08:03Z
**Iteration:** 2

### Findings

| # | Severity | Location | Finding | Recommendation |
|---|---|---|---|---|
| 1 | Minor | security-design.md スレッド安全設計節 コードスケッチ（28〜32行目） | Major所見①の核心（`InMemoryBackend`が`EventStoreForMemory<AID, A, E>`と揃うジェネリクスを持たず`StorageBackend<AID, A, E>`を実装できない形、および散文とコードスケッチの型形状不一致）は解消された。ただし修正後のコードでも `state: Arc<Mutex<HashMap<String, InMemoryStoreState>>>` の `InMemoryStoreState` は依然として `<A, E>` 等のジェネリクスなしで参照されており、events（`Vec<E>`相当）・snapshots（`A`由来）を保持するには本来 `InMemoryStoreState<A, E>` のような形が必要になる。ただし `entities.md` はドキュメント表記であり文字通りのRust構文を要求しないこと、かつ直上の行で `InMemoryBackend<AID, A, E>` へジェネリクスを付与するのと同一パターンをそのまま1段適用すれば足りることから、実装者が独力で解決できる軽微な省略にとどまる。Major所見①がブロックしていた「マップ構造か単一レコードか」「トレイト境界を満たせるか」という本質的な矛盾はもはや存在しない。 | `struct InMemoryBackend<AID, A, E> { state: Arc<Mutex<HashMap<String, InMemoryStoreState<A, E>>>>, ... }` のように、値型にもジェネリクスを明記して仕上げる。 |

### Previous Findings — Resolution Check

| # | Severity | Iteration 1 Finding | Status |
|---|---|---|---|
| 1 | Major | スレッド安全設計コードスケッチの内部矛盾（散文の`HashMap<String, InMemoryRecords>`とコードの`Arc<Mutex<InMemoryStoreState>>`の不一致、`InMemoryStoreState`がentities.md上は単一集約レコードなのにマップなしで直接ラップされていた点、`InMemoryBackend`に`AID, A, E`のジェネリクスがなく契約C-2の`StorageBackend<AID, A, E>`を実装できない形だった点） | 解消済み（残存する軽微な点は本イテレーションの所見#1としてMinorで再記録） — 散文とコードが `Arc<Mutex<HashMap<String, InMemoryStoreState>>>` に統一され、`InMemoryBackend<AID, A, E>` へジェネリクスが付与され、`PhantomData<fn() -> (AID, A, E)>` によりSend/Sync自動導出も阻害されない設計になった。手動Clone実装への切替（derive境界回避）も技術的に妥当（AID/A/E は元々AggregateId/Aggregate/Eventのスーパートレイトで Clone を要求しているため実害はないが、手動実装自体は正しく機能する）。 |
| 2 | Major | NFR-3.1の検証手順が上流要件（各feature単独ビルド確認）を満たさず、traceability.jsonの「OK」表示が過大申告だった | 解消済み — 検証手順に `cargo build --no-default-features` に加え `--features dynamodb` / `bigtable` / `sqlite` 各単独、`--all-features` の計5コマンドが追加され、`tech-stack-decisions.md` NFR-3.1（未指定／各feature単独／全feature）を実際にカバーする内容になった。traceability.json NFR-3.1行のtarget記述も実コマンド内容に合わせて更新済み。 |
| 3 | Major | logical-components.mdのDynamoDbBackend/BigtableBackend行が「変更はエラー写像の更新のみ」と「unsafe除去（NFR-4.2）も適用」で自己矛盾していた | 解消済み — 両行の「U1での変更」列が「エラー写像の更新＋手書きunsafe impl除去」に修正され、「適用されるNFR設計」列（unsafe除去 NFR-4.2）との矛盾が解消された。 |
| 4 | Minor | BR1.8（トレイトシグネチャ不変更）の引用が、コンストラクタ互換性の主張（NFR-1.2寄り）の根拠として不正確だった | 解消済み — 「隠蔽境界」行の引用がNFR-1.2（公開API維持）に差し替えられ、主張内容と根拠規則が一致した。 |

### Validation Tool Results

| Tool/確認 | 結果 | 解釈 |
|---|---|---|
| iteration 1→2 差分の再読・突合 | PASS | security-design.md（20〜39行目、46〜61行目）とlogical-components.md（13〜14行目）、traceability.json（NFR-3.1行）の変更箇所を実際に再読し、依頼メッセージの4件の修正内容が実バイトに正しく反映されていることを確認した。 |
| `StorageBackend<AID, A, E>: Send + Sync + Clone + Debug + 'static`（契約C-2 / `lib/src/event_store_backend.rs:22`）との突合 | PASS | 修正後の`InMemoryBackend<AID, A, E>`はこのトレイト境界を満たす形状（Send/Sync自動導出・手動Clone・derive Debug）になっており、`GenericEventStore<AID, A, E, B>`（`lib/src/generic_event_store.rs`）への委譲が型として成立する。 |
| `AggregateId`/`Event`/`Aggregate`トレイト定義（`lib/src/types.rs`）とのClone/Debug境界確認 | PASS | 3トレイトいずれも Clone・Debug・Send・Sync をスーパートレイトとして要求済みのため、`InMemoryBackend`が手動Clone・derive Debugのどちらを選んでも実害はなく、設計コメントの懸念（derive境界要求）は技術的にやや過大だが選択自体は妥当。 |

### Summary

iteration 1で指摘したMajor 3件・Minor 1件はいずれも実質的に解消されました。中核のスレッド安全設計コードスケッチは、マップ/単一レコードの矛盾と`StorageBackend`トレイト境界を満たせない構造という本質的な欠陥を解消し、NFR-3.1の検証手順も上流要件を実際にカバーする内容へ更新され、logical-components.mdの変更一覧表の自己矛盾も解消されました。残る所見はコードスケッチの内側の型（`InMemoryStoreState`）へのジェネリクス明記漏れという軽微な仕上げ不足のみで、実装を妨げるものではないためMinorとしています。Critical 0件・Major 0件・Minor 1件のためREADYと判定します。
