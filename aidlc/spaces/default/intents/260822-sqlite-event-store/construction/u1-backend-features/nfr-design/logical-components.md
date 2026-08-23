# 論理コンポーネント — u1-backend-features (logical-components)

U1が触れる論理コンポーネントの棚卸しと、障害ドメイン・ブラスト半径・分離戦略。セキュリティ要件（`../nfr-requirements/security-requirements.md`）・技術スタック決定（`../nfr-requirements/tech-stack-decisions.md`）・機能仕様（`../functional-design/functional-spec.md`）・契約（`../../../inception/contract-design/contract-summary.md`）に基づく。本書はNFR設計の決定がクレート内のどの境界に効くかを示し、後続工程（U1はライブラリのためクラウドインフラは存在しない — コード生成とビルド検証がこの設計の適用先）への橋渡しとする。なお性能系4要件書（performance / scalability / reliability / observability requirements）はキンド適用によりU1対象外。

## コンポーネント一覧

| コンポーネント | 実体 | U1での変更 | 適用されるNFR設計 |
|---|---|---|---|
| CoreTypes | `lib/src/types.rs` | SDK型リーク除去・`OptimisticLockError(String)` 化 | エラー写像設計（NFR-4.1/4.4） |
| FeatureGate | `Cargo.toml [features]` + `lib.rs` cfgガード | feature定義・optional化・不要依存削除 | 依存隔離設計（NFR-2.1/2.2）・ビルド互換（NFR-3.1） |
| GenericEventStore | `lib/src/generic_event_store.rs`（共通制御） | 作成イベントのErr返却の共通化確認 | panic排除（NFR-4.1） |
| MemoryBackend | `lib/src/event_store_for_memory.rs` | StorageBackend準拠化・内部 `Arc<Mutex<...>>` 化・unsafe除去 | スレッド安全設計（NFR-4.2）・公開API維持（NFR-1.2） |
| DynamoDbBackend | `lib/src/event_store_for_dynamodb.rs` | エラー写像の更新＋手書きunsafe impl除去 | エラー写像設計・unsafe除去（NFR-4.2） |
| BigtableBackend | `lib/src/event_store_for_bigtable.rs` | エラー写像の更新＋手書きunsafe impl除去 | 同上 |
| TestSupport | `test-utils/`・`#[cfg(test)]` モジュール | featureガード追随 | 既存テスト緑維持（BR1.9） |

## 障害ドメインとブラスト半径

- **FeatureGate誤構成**: 影響は利用者のビルド（契約C-3 — 未定義feature指定はcargoが即エラー）。半径=全利用者のビルド。緩和: featureマトリクスのビルド検証（security-design.md の検証手順、U3でCI化）
- **エラー型変更（CoreTypes）**: 意図された破壊的変更（semverメジャー・CHANGELOG記載 — C-1）。半径=エラーをマッチする利用者コード。緩和: 全バックエンドで同一書式へ写像するエラー契約テスト（U2で追加）
- **Memory書換**: 半径=Memory利用のテスト・利用者のみ。公開API不変（NFR-1.2）のためコンパイル互換は維持。緩和: 挙動契約テスト（AC3.1.1）
- **既存バックエンドの写像更新**: 半径=DynamoDB/Bigtableの楽観的ロック経路のみ。緩和: 既存統合テストの緑維持（BR1.9）

## 共有資源と分離戦略

- **共有資源**: `GenericEventStore`（4バックエンド共通制御）・共通エラー型（CoreTypes）・workspaceの依存テーブル。これらへの変更は全バックエンドに波及するため、U1内で先に確定させる
- **分離境界**: featureがバックエンド間の隔離境界。Memoryのみfeatureなしで常時有効（C-3 `always_on`）。バックエンド追加（U2のSQLite）はこの境界の新しい区画として入り、既存区画に触れない
- **状態の分離**: MemoryBackendの共有状態はバックエンドインスタンス内部に閉じる（`Arc<Mutex<...>>` 非公開 — security-design.md 参照）。プロセスグローバル状態・環境変数変異は持たない

## Assumptions & Open Questions

None.
