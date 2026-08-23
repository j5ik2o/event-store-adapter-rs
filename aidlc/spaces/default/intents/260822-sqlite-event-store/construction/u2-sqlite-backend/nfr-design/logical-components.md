# 論理コンポーネント — u2-sqlite-backend (logical-components)

U2が追加・変更する論理コンポーネントの棚卸しと、障害ドメイン・ブラスト半径・分離戦略。セキュリティ要件（`../nfr-requirements/security-requirements.md`）・技術スタック決定（`../nfr-requirements/tech-stack-decisions.md`）・機能仕様（`../functional-design/functional-spec.md`）・契約（`../../../inception/contract-design/contract-summary.md`）に基づく。性能系4要件書はキンド適用でU2対象外。本書はNFR設計の決定がクレート内のどの境界に効くかを示し、コード生成とビルド検証への橋渡しとする（ライブラリのためクラウドインフラは存在しない）。

## コンポーネント一覧

| コンポーネント | 実体 | U2での変更 | 適用されるNFR設計 |
|---|---|---|---|
| SqliteBackend | `lib/src/event_store_for_sqlite.rs`（新規） | StorageBackend 5メソッドのSQLite実装＋スキーマ自動作成＋保持ポリシー | エラー写像・スレッド安全・サポート境界（NFR-4.5〜4.7） |
| EventStoreForSqlite | 同ファイル内の公開ファサード（新規） | `new(path)` / `new_in_memory()` ＋ `with_*` ビルダー（GenericEventStore委譲） | 公開面の追加的変更（NFR-1.3）・ロック型非露出 |
| FeatureGate | `Cargo.toml [features]` ＋ `lib.rs` cfgゲート | `sqlite` / `sqlite-system` の実体化（U1の器を実装で埋める） | 依存供給網設計（NFR-2.3/2.4）・ビルド互換（NFR-3.2） |
| TestSupport | `lib/src/event_store_for_sqlite_test.rs`（新規）＋共有シナリオ搭載 | 共有シナリオ・競合パス・エラー契約テストの追加 | テスト環境衛生（NFR-5.1） |
| GenericEventStore / CoreTypes / KeyResolver / Serialization | 既存 | **変更なし**（U2は利用のみ — pkey/skey解決・直列化・共通制御を共用） | — |

## 障害ドメインとブラスト半径

- **SqliteBackend内部の失敗**: 半径= `sqlite` 系feature利用者のみ（新規ファサードの追加であり、既存バックエンド・feature未指定利用者には無影響）。緩和: 全経路のエラー写像一元化（panicなし）
- **単一接続の直列化**: 同一ストアインスタンス内の操作はMutexで直列化される（設計どおり — イベントストア用途で許容、機能設計Q1確定）。半径=当該インスタンスのスループットのみ
- **サポート境界外の使用（同一ファイルDBの複数同時オープン）**: サポート外（ユーザー確定）。発生し得る `SQLITE_BUSY` は `IOError` として即時返却され、データ破壊はSQLite自体のロックが防ぐ。緩和: U4ドキュメントへの境界明記
- **bundled SQLite本体のCVE**: 半径= `sqlite` feature（バンドル）利用者。緩和: RUSTSEC照合（U3 CI日次）＋Renovate追随（NFR-2.4）

## 共有資源と分離戦略

- **共有資源**: GenericEventStore（共通制御）・CoreTypes（中立エラー型）・KeyResolver（pkey/skey書き込み分散 — ユーザー確定の設計不変条件）・Serialization（Vec<u8>直列化）。U2はいずれも**変更せず利用のみ**のため、既存バックエンドへの回帰リスクは接触ゼロで遮断される
- **分離境界**: `any(feature = "sqlite", feature = "sqlite-system")` のcfgゲートがSQLite実装の隔離境界。テストも同ゲートで追随
- **状態の分離**: 接続はストアインスタンス内部に閉じ（`Arc<Mutex<Connection>>` 非公開）、サポートされる共有単位はインスタンスとその `Clone` のみ（BR2.5）

## Assumptions & Open Questions

None.
