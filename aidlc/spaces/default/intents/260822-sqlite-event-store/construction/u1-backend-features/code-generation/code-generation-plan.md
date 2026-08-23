# コード生成計画 — u1-backend-features (code-generation-plan)

U1（バックエンドfeature分割と基盤整合）の実装計画。設計正本: 機能仕様（`../functional-design/functional-spec.md` ワークフロー1〜3）・ルール（`../functional-design/rules.md` BR1.1〜BR1.9）・エンティティ（`../functional-design/entities.md`）・セキュリティ設計（`../nfr-design/security-design.md`）・CI/CD設計（`../infrastructure-design/cicd-pipeline.md`）・契約（`../../../inception/contract-design/contract-summary.md` C-1〜C-3）。実装順序はストーリー対応（US2.2 → US2.1 → US3.1）に従う。

## 実装ステップ

### 基盤

- [x] **Step 1**: ベースライン確認 — `cargo build -p event-store-adapter-rs` と既存テストランナーの確認。ユニットスコープのテストコマンド（unit-test-instructions.md 記載）が実行可能なことを検証し、Docker可用性（既存DynamoDB/Bigtable統合テストはtestcontainers前提）を記録する。既存スイートの基線（緑/実行不能の別）を code-summary.md に記録（brownfield基線プロトコル）

### US2.2: エラー型中立化（BR1.1 / BR1.2 / AC2.2.1 / AC2.2.3）

- [x] **Step 2**: 実装 — `lib/src/types.rs` から `TransactionCanceledExceptionWrapper` を削除し、`EventStoreWriteError::OptimisticLockError(String)` へ変更。整形ヘルパー（集約ID・期待バージョン・実バージョン〔判明分〕→ `optimistic lock failed, aid=<id>, expected_version=<n>[, actual_version=<m>]` の1行）を追加。DynamoDB写像（SDKの TransactionCanceledException 検出→整形文字列）、Bigtable/Memory の楽観ロック失敗経路も同一形式へ更新。写像不能な下位エラーは既存どおり `IOError` / `OtherError`（panicしない）
- [x] **Step 3**: テスト（test-after） — 整形ヘルパーの単体テストを同居 `#[cfg(test)]` に追加し実行: 基本形の書式、`actual_version` 付加形、機密情報（接続文字列等）非混入（NFR-4.4）

### US2.1: feature分割（BR1.3 / BR1.4 / BR1.5 / AC2.1.1〜AC2.1.4）

- [x] **Step 4**: 実装 — `lib/Cargo.toml` に `[features]`（`dynamodb` / `bigtable` / `sqlite`（器のみ）、`default = []`）を定義し、クラウドSDK依存（aws-sdk-dynamodb / aws-config / tonic / googleapis-tonic-google-bigtable-v2）を `optional = true` 化して対応featureの `dep:` に束ねる。未使用依存 aws-http・宣言のみの prost・未使用dev依存 serial_test を削除。`lib.rs` のモジュール宣言・再エクスポートへ `#[cfg(feature = ...)]` を付与し `#[allow(dead_code)]` を除去。テスト・test-utils・examples のfeature追随（既存DynamoDB/Bigtableテストを対応featureでガード — BR1.9）
- [x] **Step 5**: 検証（test-after） — featureマトリクスビルド5本（`--no-default-features`／`--features dynamodb`単独／`--features bigtable`単独／`--features sqlite`単独／`--all-features`）＋ `cargo tree` 検査（feature未指定でクラウドSDK不在、feature単独で他バックエンド依存不混入 — security-design.md 検証手順）

### US3.1: Memory準拠化（BR1.6 / BR1.7 / BR1.8 / AC3.1.1〜AC3.1.3）

- [x] **Step 6**: 実装 — `lib/src/event_store_for_memory.rs` を `StorageBackend` 実装＋`GenericEventStore` 委譲へ書換。内部状態は `Arc<Mutex<HashMap<String, InMemoryStoreState>>>`（キー=aid）とし**公開APIへ一切露出させない**（ロック取得は各メソッド内で完結、ガード保持中の `.await` 禁止、手動Clone実装でArc共有クローン、`PhantomData<fn() -> (AID, A, E)>`）。公開API `EventStoreForMemory::new()` と `with_*` ビルダー互換を維持。既存3バックエンドの手書き `unsafe impl Send/Sync` を除去（自動導出）
- [x] **Step 7**: テスト（test-after） — `lib/src/event_store_for_memory_test.rs`（同居 `#[cfg(test)]` モジュール、既存命名パターン準拠）を新設し実行: (1) 作成イベントの `persist_event` 渡しが panic せず `Err` を返す（AC3.1.1）、(2) 共有シナリオ `exercise_user_account_flow` のMemory搭載（契約対称性）、(3) 同一バージョンへの並行/逐次の競合更新で `OptimisticLockError(String)` が返りBR1.2書式であるエラー契約（AC2.2.2のU1範囲）、(4) Clone間で状態が共有される（既知欠陥の解消確認）

### 基盤（仕上げ）

- [x] **Step 8**: `.github/workflows/ci.yml` の `test-lib` ジョブを `cargo test --verbose -p event-store-adapter-rs --all-features` へ最小修正（cicd-pipeline.md Q1=A確定。lintジョブ・トリガー・他ワークフローは不変更）
- [x] **Step 9**: 最終検証 — `cargo test -p event-store-adapter-rs --all-features`（Docker可用時は統合テスト込み、不能時はDocker不要テスト全緑＋その旨記録）、`grep -rn "unsafe impl" lib/src/` が0件、`cargo +nightly fmt -- --check` パス。変更を論理単位でコミット（Conventional Commits — 破壊的変更は `!` / `BREAKING CHANGE` を明記。例: `refactor(types)!: replace SDK-leaked error type with neutral OptimisticLockError(String)`、`feat(features)!: gate backends behind cargo features with empty default`、`refactor(memory): conform memory backend to StorageBackend + GenericEventStore`）

## ストーリー対応（トレーサビリティ）

| ステップ | ストーリー / ルール | 検証AC |
|---|---|---|
| Step 2-3 | US2.2 / BR1.1, BR1.2 | AC2.2.1, AC2.2.3（型検査）, AC2.2.2はStep 7(3)でU1範囲 |
| Step 4-5 | US2.1 / BR1.3〜BR1.5, BR1.9 | AC2.1.1〜AC2.1.4 |
| Step 6-7 | US3.1 / BR1.6〜BR1.8 | AC3.1.1〜AC3.1.3 |
| Step 8 | インフラ設計 Q1=A（NFR-1.1のCI維持） | ci.yml差分レビュー |

## Testing Contract

```json
{
  "version": 1,
  "methodology": "test-after",
  "source": "team",
  "ordering": "各バックエンド実装（例: StorageBackend の SQLite 実装）を書いた後に、",
  "scope": "library",
  "test_strategy": "standard",
  "project_type": "brownfield",
  "applicable_notes": [
    {
      "layer": "org",
      "text": "We treat tests as a first-class deliverable in every Bolt. The specific\nmethodology (TDD, BDD, ATDD, or classic test-after) is affirmed at\npractices-discovery and recorded in `team.md` under this heading with explicit\n`Methodology` and `Ordering` fields; Code Generation resolves those fields\nindependently from coverage, tooling, and scope notes.\n\nWhen no posture has been affirmed, our default per scope is:\n- **Methodology**: test-after\n- **Ordering**: implement each applicable testable layer, then write and run\n  that layer's tests.\n- `mvp`, `enterprise`, `feature`, `infra`, `classic` add an 80% line-coverage\n  floor and CI execution before merge.\n- `bugfix`, `security-patch` add a targeted regression for the specific\n  bug/vulnerability and require the existing suite to remain green.\n- `express` uses the Minimal strategy: requirement-driven unit tests (one per\n  requirement, with a happy-path floor per component); existing tests remain\n  green.\n- `poc`, `refactor`, `workshop` add no extra new-test floor and require the\n  existing suite to remain green.\n\nThe active `Test Strategy` still applies in every scope and determines test\nvolume/types. Scope floors are additive; they never reduce or replace the\nselected strategy.\n\nAffirm a stricter posture in `team.md` if the team commits to one."
    },
    {
      "layer": "team",
      "text": "- **Methodology**: test-after\n- **Ordering**: 各バックエンド実装（例: StorageBackend の SQLite 実装）を書いた後に、\n  同じレイヤの `#[cfg(test)]` モジュールとして統合テストを書き、\n  `cargo test -p event-store-adapter-rs` で実行して確認する。\n\n補足（エビデンス・検分・インタビューに基づく確定事項）:\n- テストは専用 `tests/` ディレクトリを持たず、実装ファイルと同居する\n  `#[cfg(test)]` モジュール方式（例: `event_store_for_dynamodb_test.rs`,\n  `event_store_for_bigtable_test.rs`）を一貫して使います。SQLiteでも\n  `event_store_for_sqlite.rs` / `event_store_for_sqlite_test.rs` の\n  ファイル命名パターンに機械的に従います。\n- 共有シナリオ `event_store_test_support.rs::exercise_user_account_flow`\n  （作成→リネーム×2→スナップショット/リプレイ検証）を各バックエンドで\n  再利用しています。SQLiteバックエンドでもこの共有シナリオをまず流し込み、\n  バックエンド間の契約対称性を検証します。ただしこのシナリオは\n  **ハッピーパスのみ**（quality検分の指摘）であり、以下は別途カバーします。\n- **SQLiteの統合テストは testcontainers を使いません**（インタビュー時点の\n  合意方針）。SQLiteはDocker不要でファイルベース／`:memory:` DBを使い、\n  決定的・高速・テスト独立性の高いテストにします（プロセスグローバルな\n  `env::set_var` のような変異は踏襲しません）。\n- **SQLiteバックエンドでは楽観的ロック競合パステストとエラー契約テストを\n  必須とします（Q4、A確定）**。具体的には (1) 同一versionへの並行書込みで\n  片方が `OptimisticLockError` を返すことを検証する競合パステスト、(2)\n  `persist_event` の呼び出し契約（バックエンド中立なエラー表現で返す\n  こと。他バックエンドのAWS型リークやMemoryのpanicパターンを模倣しない\n  こと）を固定する回帰テスト、の2種類です。SQLiteは同一プロセス内で\n  再現できるため、既存バックエンドでは書けなかったこれらのテストを\n  最も安価に書ける場所です。\n- **カバレッジは計測しません（Q3、A確定）**。数値目標は設けず、テストの質は\n  共有シナリオ＋楽観的ロック競合テスト＋エラー契約テストの充足で担保します。\n  なお org.md のスコープ別80%カバレッジ床の列挙に `library` スコープは\n  含まれないため、そもそも本ワークフローには数値床は適用されません。\n  Test Strategy（Standard）が要求するテスト種別・量はこの決定で減じません\n  （スコープ床は戦略に対して加算的であり、Bで無床を選んでも戦略要求は\n  そのまま有効）。\n- 既知の欠落（今回のSQLite Boltでは埋めない）: Memory バックエンド専用\n  テストなし、スナップショット保持動作の実assertなし（DynamoDBテストは\n  `with_keep_snapshot_count` を設定するだけで結果を検証していない）、\n  examplesのビルド検証がCIにない。これらはバックログ扱いとします。"
    }
  ],
  "obligations": {
    "strategy": "standard",
    "strategy_volume": [
      "Five to eight tests per component.",
      "Unit tests plus integration tests for key boundaries.",
      "Add E2E, performance, or security tests when requirements demand them."
    ],
    "scope_floor": [
      "Keep the existing test suite green.",
      "This scope adds no extra new-test floor beyond the selected test strategy."
    ],
    "combination_rule": "Apply every selected-strategy obligation and every scope-floor obligation; neither replaces the other, and a targeted scope regression may add the narrowest necessary test type beyond the strategy default."
  },
  "plan_profile": {
    "methodology": "test-after",
    "runner_step": "Verify the existing test runner/configuration and record the exact unit-scoped command.",
    "runner_ready_before_first_test": true,
    "testable_layers": [
      "Data model / database behavior",
      "Repository / data access",
      "Business logic",
      "API / endpoint",
      "Frontend behavior"
    ],
    "steps": [
      "Project structure and production configuration skeleton.",
      "Verify the existing test runner/configuration and record the exact unit-scoped command.",
      "Data model / database behavior - implement.",
      "Data model / database behavior - write and run its tests after implementation.",
      "Repository / data access - implement.",
      "Repository / data access - write and run its tests after implementation.",
      "Business logic - implement.",
      "Business logic - write and run its tests after implementation.",
      "API / endpoint - implement.",
      "API / endpoint - write and run its tests after implementation.",
      "Frontend behavior - implement.",
      "Frontend behavior - write and run its tests after implementation.",
      "Environment/build configuration.",
      "Documentation and traceability."
    ]
  },
  "input_sha256": "sha256:fe3c818311b76b307798ad517105a5105a9005e663f2e6e2613e79960291c61c",
  "contract_sha256": "sha256:b4781add043666e0e0ee187edaf082d7b7aa19273c5b76c0e284962c3b01fb1d"
}
```

契約の `plan_profile.steps` への適合: 本ユニットはRustライブラリのため、テスト可能レイヤを「エラー型/整形（データモデル相当 — Step 2-3）」「ビルド構成（feature — Step 4-5、検証はビルド/依存グラフ検査）」「Memoryバックエンド（リポジトリ/データアクセス相当 — Step 6-7）」に適合させ、API/endpoint・Frontend レイヤは該当なしとして省略する（メソドロジー test-after は不変）。ランナー準備（Step 1）は最初のテストステップより前に置く。

## Assumptions & Open Questions

None.
