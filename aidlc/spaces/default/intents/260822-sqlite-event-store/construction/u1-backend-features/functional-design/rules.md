# ビジネスルール — u1-backend-features (rules)

ユニット定義（`../../../inception/units-generation/unit-of-work.md` U1）・要件（`../../../inception/requirements-analysis/requirements.md`）・コンポーネントカタログ（`../../../inception/domain-design/components.md`）・契約（`../../../inception/contract-design/contract-summary.md`）・ストーリーAC（`../../../inception/units-generation/unit-of-work-story-map.md` 経由のUS2.1/US2.2/US3.1）から導出。

## Source of truth

```yaml
rules:
  - id: BR1.1
    statement: 共通エラー型はバックエンドSDK型への参照を一切含まない
    category: constraint
    applies_to: CoreTypes（EventStoreWriteError / EventStoreReadError）
    trigger: 型定義・エラー写像コードのコンパイル時
    logic: IF 共通型モジュールがバックエンド固有SDKの型を参照する THEN 設計違反（ビルド構成で不可能にする）
    violation: コンパイル不能または型検査（AC2.2.3）で検出
    source: FR-2.4
  - id: BR1.2
    statement: 楽観的ロック失敗は OptimisticLockError(String) で表現し、メッセージに集約ID・期待/実バージョン（判明分）を含める
    category: constraint
    applies_to: 全バックエンドのエラー写像
    trigger: バージョン不一致の更新失敗時
    logic: >-
      IF 条件付き更新がバージョン不一致で失敗 THEN OptimisticLockError に
      整形済み文字列（例は optimistic lock failed, aid=<id>, expected_version=<n> の形式）を格納して返す
    violation: エラー契約テスト（AC2.2.2）で検出
    source: FR-2.4 / Q&A Q1=A
  - id: BR1.3
    statement: feature未指定のビルドは Memory バックエンドのみを含み、クラウドSDK依存をビルド対象に含めない
    category: constraint
    applies_to: Cargo.toml features / lib.rs cfgゲート
    trigger: cargo ビルド解決時
    logic: IF --no-default-features THEN aws-sdk-dynamodb / aws-config / tonic / googleapis系 は依存グラフに現れない
    violation: cargo tree 検査（AC2.1.1/AC2.1.2）で検出
    source: FR-2.1 / FR-2.2
  - id: BR1.4
    statement: 各バックエンドfeatureは自身の依存のみを有効化し、他バックエンドの依存を混入させない
    category: constraint
    applies_to: Cargo.toml features
    trigger: 各feature単独ビルド時
    logic: IF --features dynamodb（単独） THEN bigtable/sqlite系依存は現れない（bigtable/sqlite単独も同様）
    violation: cargo tree 検査（AC2.1.3）で検出
    source: FR-2.1
  - id: BR1.5
    statement: 再エクスポートは #[cfg(feature)] でガードし、#[allow(dead_code)] 抑制と未使用依存（aws-http / prost宣言 / serial_test）を除去する
    category: constraint
    applies_to: lib.rs / Cargo.toml
    trigger: feature分割の実装時
    logic: IF feature分割完了 THEN グロブ再エクスポートはガード済みで dead_code 抑制は不在、未使用依存は削除済み
    violation: コード検査（AC2.1.4）で検出
    source: FR-2.3 / FR-3.2
  - id: BR1.6
    statement: Memoryバックエンドは StorageBackend + GenericEventStore 経由で EventStore を提供し、作成イベントの persist_event 渡しは panic せず Err を返す
    category: constraint
    applies_to: MemoryBackend
    trigger: persist_event に is_created()==true のイベントが渡されたとき
    logic: IF 作成イベントが persist_event に渡される THEN GenericEventStore の共通制御が Err を返す（panicしない）
    violation: 挙動契約テスト（AC3.1.1）で検出
    source: FR-3.1
  - id: BR1.7
    statement: 新規・改修コードに手書きの unsafe impl Send/Sync を書かない（自動導出に任せる）
    category: policy
    applies_to: U1で触れる全モジュール
    trigger: コードレビュー・grep検査時
    logic: IF 対象範囲に手書き unsafe impl が存在 THEN 違反
    violation: grep検査（AC3.1.3）で検出
    source: FR-3.3 / project.md Forbidden
  - id: BR1.8
    statement: 公開トレイト（EventStore / AggregateId / Event / Aggregate）のシグネチャは変更しない
    category: constraint
    applies_to: CoreTypes
    trigger: U1の全変更
    logic: IF トレイトメソッドのシグネチャ差分が生じる THEN 設計違反
    violation: 既存テストのコンパイル・実行で検出
    source: NFR-1
  - id: BR1.9
    statement: 既存のDynamoDB/Bigtable統合テストはU1完了時点で緑のまま
    category: constraint
    applies_to: DynamoDbBackend / BigtableBackend（エラー写像更新の回帰保護）
    trigger: U1完了時のテスト実行
    logic: IF 既存統合テストが失敗 THEN U1未完（エラー型変更の影響吸収漏れ）
    violation: cargo test で検出
    source: unit-of-work.md U1制約
```

## ルールサマリー

| ID | 区分 | 要旨 | 検出手段 |
|---|---|---|---|
| BR1.1 | constraint | 共通型にSDK型を含めない | 型検査 |
| BR1.2 | constraint | OptimisticLockError(String) の中身形式 | エラー契約テスト |
| BR1.3 | constraint | feature未指定=Memoryのみ | cargo tree |
| BR1.4 | constraint | feature間の依存混入禁止 | cargo tree |
| BR1.5 | constraint | cfgガード＋不要物除去 | コード検査 |
| BR1.6 | constraint | Memory準拠化・panic禁止 | 挙動テスト |
| BR1.7 | policy | unsafe impl 複製禁止 | grep |
| BR1.8 | constraint | トレイトシグネチャ不変更 | 既存テスト |
| BR1.9 | constraint | 既存統合テスト緑維持 | cargo test |

## Assumptions & Open Questions

None.
