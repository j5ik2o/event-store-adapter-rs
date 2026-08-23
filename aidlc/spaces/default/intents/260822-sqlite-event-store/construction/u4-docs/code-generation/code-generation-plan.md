# コード生成計画 — u4-docs (code-generation-plan)

U4（ドキュメント）の実装計画。上流: unit-of-work U4境界（examples/・README英日・docs/DATABASE_SCHEMA英日・CHANGELOG）、FR-6.1〜6.4、nfr-design(u4) の記載割り当て表（Q1=A確定）、infrastructure-design(u4)（CI・配信設定に一切触れない）。U1/U2/U3の実装済みコード（feature構成・`EventStoreForSqlite`・スキーマDDL・CI）が転記元の正である。

## ストーリー・要件対応

| 計画ステップ | 実装する要件 |
|---|---|
| Step 1 | FR-6.2の一部（TD-10解消: crate description） |
| Step 2〜3 | FR-6.2（README英/日: feature構成・移行手順・サポート境界・bundled優先・最小利用例・TD-10解消） |
| Step 4 | FR-6.3（DATABASE_SCHEMA英/日: SQLiteスキーマ、情報提供の明記） |
| Step 5 | FR-6.4（CHANGELOG: 破壊的変更・新機能） |
| Step 6 | FR-6.1（examples: SQLite利用例） |
| Step 7 | テスト（test-after: exampleのビルド・実行、fmt/clippy、記載照合） |

## 実装ステップ

- [x] **Step 1: `lib/Cargo.toml` の description 更新（TD-10）** — `"crate to make DynamoDB an Event Store"` をマルチバックエンド（DynamoDB / Bigtable / SQLite / Memory）を反映した記述へ更新。`readme = "../README.md"` 等の他のパッケージメタデータは変更しない
- [x] **Step 2: `README.md`（英）更新** — (1) feature構成の説明（`dynamodb` / `bigtable` / `sqlite` / `sqlite-system`・デフォルトfeature廃止）、(2) 既存利用者向け移行手順（Cargo.toml before/after、エラー型新旧対応表: `OptimisticLockError` のAWS型内包→`String`化・Memoryのpanic→`Err`化）、(3) サポート境界（同一ファイルDBの複数同時オープンはサポート外 — 多重起動防止はアプリケーション責務）、(4) `sqlite`+`sqlite-system` 併用時はbundled優先の注記（cargo feature加算性の帰結）、(5) SQLiteの最小利用例（コンパイル可能な形 — Step 6のexampleと同一APIで裏付け）、(6) 既存コード例と現行APIの乖離（TD-10）の解消
- [x] **Step 3: `README.ja.md`（日）更新** — Step 2と同内容の日本語版。両READMEの構成・記載事項を対にする
- [x] **Step 4: `docs/DATABASE_SCHEMA.md`（英）/ `docs/DATABASE_SCHEMA.ja.md`（日）更新** — SQLiteの journal / snapshot テーブル（列・型・PK (pkey, skey)・(aid, seq_nr) インデックス・pkey/skey=書き込み分散キー・aid/seq_nr=読み込みキーの説明）を追記。転記元は `lib/src/event_store_for_sqlite.rs` のCREATE TABLE文（実装が正）。**スキーマ記載は情報提供であり、テーブル作成はライブラリの自動作成が担う**ことを明記（FR-6.3・project.md Mandated）
- [x] **Step 5: `CHANGELOG.md` 新規作成** — Keep a Changelog 形式。Unreleased（または次版）として: 破壊的変更（デフォルトfeature廃止 — 利用者は明示的にfeatureを指定する必要、`OptimisticLockError(String)` へのエラー型変更、Memoryバックエンドのpanic廃止）、新機能（`sqlite` / `sqlite-system` feature・`EventStoreForSqlite`・スキーマ自動作成・保持ポリシー）、内部変更（`unsafe impl Send/Sync` 除去・CI強化）。semverはメジャー相当（C-1）である旨を記載
- [x] **Step 6: `examples/user-account-sqlite/` 新規作成（FR-6.1）** — 既存 `examples/user-account` の構成（`main.rs` / `user_account.rs` / `user_account_repository.rs`）に倣った独立クレート `example-user-account-sqlite`（`publish = false`）。`event-store-adapter-rs = { path = "../../lib", features = ["sqlite"] }` のみで動作し、AWS依存・testcontainers・Docker不要。DBはファイル（例: 相対パスの一時ファイル）または `:memory:`。実在の個人環境パスや機密リテラルを含めない（NFR-4.12）。ワークスペースメンバー（`examples/*` グロブ）として自動的にfmt/clippy対象に入る
- [x] **Step 7: テスト（test-after — 実装後に検証を書く/実行する）** — (1) `cargo build -p example-user-account-sqlite` と `cargo run -p example-user-account-sqlite` でexampleのコンパイル・実行を確認（AC4.1.2: クラウド接続なし）、(2) `cargo +nightly fmt -- --check` と `cargo clippy --workspace --all-targets -- -D warnings` で新規exampleがクリーンであること、(3) README/DATABASE_SCHEMAの記載照合（`grep -n "sqlite" README.md docs/DATABASE_SCHEMA.md`、READMEのCargo.toml例のfeature名と `lib/Cargo.toml` の実feature名の突合、DATABASE_SCHEMAの列名と実装DDLの突合）、(4) 機密リテラルgrep（`(api[_-]?key|secret|password|token)\s*=`）が追記分にヒットしないこと、(5) 既存スイートが緑のまま（`cargo test -p event-store-adapter-rs --all-features`）

## テスト方針の適用（本ユニットにおける読み替え）

- 本ユニットは文書＋examplesのユニットであり、テスト対象レイヤは「examples（コンパイル・実行可能なコード）」のみ。ライブラリ本体のテストはU1/U2で実装済み（24テスト緑）であり、U4は既存スイートを緑のまま保つ（スコープ床）
- Standard戦略の「コンポーネントあたり5〜8テスト」は、新規コンポーネントを持たない本ユニットでは「exampleの実行成功＋記載照合チェック群」に読み替える（コンポーネント=exampleクレート1つ。exampleは実行そのものがend-to-end検証: 作成→リネーム→スナップショット/リプレイの流れを実DBで通す）
- 文書の正確性テストは自動化しない（examplesビルド検証のCI追加はバックログ — infrastructure-design Q1=A確定）。本ユニット内ではStep 7の手元実行で検証する

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

契約の適用注記: `plan_profile.steps` の各テスタブルレイヤのうち、本ユニットに実在するのは「examples（実行可能コード）」のみであり、Data model〜Frontend の各レイヤはU1/U2実装済みのため対象外（方法論 test-after は維持: Step 6 実装 → Step 7 検証の順）。runner readiness は既存ワークスペースの cargo が担い、ユニットスコープのコマンドは `unit-test-instructions.md` に記録済み。

## Assumptions & Open Questions

None.
