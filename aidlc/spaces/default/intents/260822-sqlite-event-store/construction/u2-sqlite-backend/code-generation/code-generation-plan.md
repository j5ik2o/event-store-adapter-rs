# コード生成計画 — u2-sqlite-backend (code-generation-plan)

U2（SQLiteバックエンド本体）の実装計画。設計正本: 機能仕様（`../functional-design/functional-spec.md` ワークフロー1〜5・決定表）・ルール（`../functional-design/rules.md` BR2.1〜BR2.12）・エンティティ（`../functional-design/entities.md`）・セキュリティ設計（`../nfr-design/security-design.md` 依存宣言・写像決定表・サポート境界）・技術スタック決定（`../nfr-requirements/tech-stack-decisions.md` D1〜D7）・CI/CD設計（`../infrastructure-design/cicd-pipeline.md`）・契約（`../../../inception/contract-design/contract-summary.md` C-1〜C-4）。実装順序はストーリー対応（US1.1 → US1.2 → US1.3 → US2.3 → US3.2 → US1.4）に従う。**pkey/skey は書き込み分散キーとして実使用し（ユーザー確定 — 機能設計Q2）、aid/seq_nr は読み込み用キー**。

## 実装ステップ

### 基盤

- [x] **Step 1**: ベースライン確認 — `cargo test -p event-store-adapter-rs --all-features` が全緑（U1完了時点15テスト）であることを確認し、ユニットスコープのテストコマンド（unit-test-instructions.md）が実行可能なことを検証。結果を code-summary.md に記録
- [x] **Step 2**: 依存とfeatureの実体化 — ルート `Cargo.toml` の `[workspace.dependencies]` に **`rusqlite = { version = "0.40.2", default-features = false }`** を追加（`default-features = false` は必須 — D1、hashlink混入防止）。`lib/Cargo.toml` の `[features]` を `sqlite = ["dep:rusqlite", "rusqlite/bundled"]`／`sqlite-system = ["dep:rusqlite"]` に実体化し、`lib.rs` のsqlite系cfgゲートを `any(feature = "sqlite", feature = "sqlite-system")` へ更新。ビルド確認（`--features sqlite` 単独）

### US1.1: SQLiteへの永続化と復元（BR2.1/BR2.2/BR2.3/BR2.8/BR2.11）

- [x] **Step 3**: 実装 — `lib/src/event_store_for_sqlite.rs`（新規）:
  - `SqliteBackend<AID, A, E>`（非公開型・`StorageBackend` 実装）: 内部 `Arc<Mutex<Connection>>` 完全隠蔽・手動Clone（Arc共有）・`PhantomData<fn() -> (AID, A, E)>`・ガード越し `.await` なし
  - `EventStoreForSqlite<AID, A, E>` 公開ファサード: `new(path)` / `new_in_memory()` ＋ `with_keep_snapshot_count` / `with_delete_ttl` / `with_key_resolver` / `with_event_serializer` / `with_snapshot_serializer` / シャード数設定（`GenericEventStore` 委譲 — `EventStore` 直接実装禁止）
  - スキーマ自動作成（冪等 — BR2.1）: journal（pkey, skey, aid, seq_nr, payload BLOB, occurred_at INTEGER — PK (pkey, skey)・UNIQUE索引 (aid, seq_nr)）／snapshot（pkey, skey, aid, seq_nr, version INTEGER, payload BLOB, last_updated_at INTEGER — PK (pkey, skey)・索引 (aid, seq_nr)）
  - 書き込みは KeyResolver の (pkey, skey)（書き込み分散 — BR2.2）、読み出しは (aid, seq_nr) 索引。スナップショットはスロット0規約（skey=resolve_sort_key(aid, 0) が現行行・version保持 — DynamoDB参照実装と対称）
  - エラー写像ヘルパー: 一意制約違反→`OptimisticLockError`（BR1.2書式）、SQLITE_BUSY等→`IOError`、直列化→`SerializationError`、他→`OtherError`。panicなし（Mutexポイズン含む）。メッセージ衛生（DBパス・生エラーをOptimisticLockErrorへ混入させない）
  - `fetch_latest_snapshot` / `fetch_events_since` / `create_event_and_snapshot`（単一トランザクション: スロット0挿入＋journal＋保持設定時履歴行）を実装
- [x] **Step 4**: テスト（test-after） — `lib/src/event_store_for_sqlite_test.rs`（同居 `#[cfg(test)]`・`any(...)`ゲート追随）: 共有シナリオ `exercise_user_account_flow`（ファイルDB — AC3.2.1）、スキーマ自動作成（空ファイルからの成功 — AC1.1.1）、復元一致（AC1.1.2）、書き込み不能パスで panic せず中立エラー（AC1.1.4）

### US1.2: 楽観的ロック（BR2.4）

- [x] **Step 5**: 実装 — `update_event_and_snapshot`: 単一トランザクション内で スロット0行を `version = expected` 条件付き更新（+1、aggregateありならpayload/seq_nr/last_updated_atも）→影響行0なら実version読取→`optimistic lock failed, aid=<id>, expected_version=<n>, actual_version=<m>` 書式で `OptimisticLockError` を返しロールバック→journal挿入→保持設定時履歴行挿入→コミット
- [x] **Step 6**: テスト（test-after） — 競合パステスト: 同一DBを共有する2ハンドル（Cloneが基底接続共有）で同一バージョンから順次コミットし一方が `OptimisticLockError`（決定的 — AC1.2.1/AC3.2.3）、原子性（失敗時にjournal残骸なし — AC1.2.2）、エラー契約テスト（BR1.2書式完全一致・actual_version付加 — AC3.2.3）

### US1.3: `:memory:` とファイルの両対応（BR2.5）

- [x] **Step 7**: テスト（test-after） — `:memory:` で同一インスタンス＋Clone間のデータ共有・整合（AC1.3.1）、ファイルDBでストア再構築後の復元（AC1.3.2）。テストは一時ディレクトリ＋一意名・`:memory:`のみ（Docker不要 — AC3.2.2）

### US2.3: バンドル／システムリンク（BR2.9）

- [x] **Step 8**: 検証 — `cargo build -p event-store-adapter-rs --no-default-features --features sqlite`（バンドル自己完結 — AC2.3.1）／`--features sqlite-system`（システムSQLiteリンク — AC2.3.2。この環境で検証し、リンク不能ならビルド試行結果と理由を記録）。併用時bundled優先の帰結はU4文書引き渡し（D3）

### US1.4: スナップショット保持ポリシー（BR2.7 — Could・最後）

- [x] **Step 9**: 実装＋テスト（test-after） — `on_event_persisted`: keep_snapshot_count 超過の履歴行削除・delete_ttl 期限切れ履歴行削除（現行スロット行は対象外・サイレント無効なし）。テスト: keep_snapshot_count=1 で古い履歴が残らない（AC1.4.1）、TTL経過後の保守フックで期限切れ削除（AC1.4.2）

### 仕上げ

- [x] **Step 10**: 最終検証とコミット — featureマトリクス7ビルド（未指定/dynamodb/bigtable/sqlite/sqlite-system/全feature/既存5本の再確認）、`cargo tree -e normal --no-default-features --features sqlite` で hashlink・クラウドSDK不在、`grep -rn "unsafe impl" lib/src/` 0件、`cargo test -p event-store-adapter-rs --all-features` 全緑、`cargo +nightly fmt -- --check` パス。論理単位でコミット（Conventional Commits — 例: `feat(sqlite): add SQLite-backed event store behind sqlite feature`、`feat(sqlite): implement snapshot retention policy for sqlite backend`）。push はしない

## ストーリー対応（トレーサビリティ）

| ステップ | ストーリー / ルール | 検証AC |
|---|---|---|
| Step 2 | 基盤 / BR2.9, BR2.10, D1〜D3 | ビルド・cargo tree |
| Step 3-4 | US1.1 / BR2.1〜BR2.3, BR2.8, BR2.11 | AC1.1.1〜AC1.1.4, AC3.2.1 |
| Step 5-6 | US1.2 / BR2.4 | AC1.2.1, AC1.2.2, AC3.2.3 |
| Step 7 | US1.3 / BR2.5, BR2.12 | AC1.3.1, AC1.3.2, AC3.2.2 |
| Step 8 | US2.3 / BR2.9 | AC2.3.1, AC2.3.2 |
| Step 9 | US1.4 / BR2.7 | AC1.4.1, AC1.4.2 |

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
契約の `plan_profile.steps` への適合: 本ユニットはRustライブラリのため、テスト可能レイヤを「基盤/ビルド構成（Step 2 — ビルド検証）」「データモデル/DB挙動（Step 3-4 — スキーマ・永続化・復元）」「リポジトリ/データアクセス（Step 5-7 — CAS・:memory:/ファイル）」「保守ロジック（Step 9）」に適合させ、API/endpoint・Frontend レイヤは該当なしとして省略する（メソドロジー test-after は不変）。ランナー準備（Step 1）は最初のテストステップより前に置く。

## Assumptions & Open Questions

None.
