# コード生成計画 — u3-ci-quality (code-generation-plan)

U3（CI品質保証）の実装計画。設計正本: CI/CDパイプライン設計（`../infrastructure-design/cicd-pipeline.md` — 変更後のパイプライン・引き渡し対応表）・インフラ仕様（`../infrastructure-design/infrastructure-specification.md` — ジョブ構成表）・セキュリティ設計（`../nfr-design/security-design.md` — deny.toml設計）・技術スタック決定（`../nfr-requirements/tech-stack-decisions.md` D1〜D6）。対象は `.github/workflows/ci.yml` と `deny.toml`（新規）のみで、アプリケーションコードには触れない。

## 実装ステップ

### 基盤

- [x] **Step 1**: ベースライン確認 — `cargo test -p event-store-adapter-rs --all-features` が全緑（24テスト）であることを確認し、結果を code-summary.md に記録

### US3.3: CIによる品質保証（NFR-2.5/2.6/3.3/4.9〜4.11）

- [x] **Step 2**: `deny.toml`（リポジトリルート・新規） — `[advisories]`（RUSTSEC照合、ignoreは理由コメント付きのみ）と `[licenses]`（許可リスト方式 — `cargo deny check licenses` の失敗出力から現依存の実ライセンスを列挙。MIT/Apache-2.0/BSD系/Unicode系等の許容的ライセンスのみ、GPL系は列挙しない）。ローカルで `cargo deny check advisories licenses` を緑化（cargo-deny未導入なら `brew install cargo-deny` または `cargo install cargo-deny` で導入）
- [x] **Step 3**: `ci.yml` に3ジョブ追加（既存 `lint` / `test-lib` ジョブ・`on:` トリガー・ブランチ保護は不変更）:
  - `feature-matrix`: 6構成のマトリクス — **未指定／sqlite／sqlite-system = ビルド＋テスト実行（Docker不要）、dynamodb／bigtable／全feature = `cargo build` のみ**（Q1=A）。sqlite-system構成は `libsqlite3-dev` をapt導入。＋依存グラフ検査ステップ（feature未指定でクラウドSDK不在・sqlite構成でhashlink不在 — `cargo tree -e normal | grep` 判定）＋unsafe不在ステップ（`grep -rn "unsafe impl" lib/src/` 0件）
  - `clippy`: stable で `cargo clippy --workspace --all-targets -- -D warnings`
  - `audit`: `cargo deny check advisories licenses`（導入方式は EmbarkStudios/cargo-deny-action と `cargo install` を比較し実行時間の短い方 — D6）
- [x] **Step 4**: clippy既存負債1件（`lib/src/generic_event_store.rs` テスト内TestBackendのeager clone警告）の扱い確定 — 自明に直せるなら修正、そうでなければ理由コメント付き最小 `#[allow]`（新規モジュールのクリーン維持が最低線 — NFR-4.9）。ローカルで `cargo clippy --workspace --all-targets -- -D warnings` を緑化
- [x] **Step 5**: 検証（test-after） — ローカルで各ジョブ相当を実行: `cargo +nightly fmt -- --check`／clippy緑／`cargo deny check advisories licenses` 緑／マトリクス6構成（3構成build＋3構成build&test）／cargo tree検査2種／unsafe grep 0件。`ci.yml` のYAML構文妥当性確認（python yaml.safe_load 等）
- [x] **Step 6**: コミット — 論理単位でConventional Commits（例: `ci: add feature matrix, clippy and dependency audit jobs`、`chore: add cargo-deny configuration`）。push はしない

## ストーリー対応（トレーサビリティ）

| ステップ | ストーリー / 要件 | 検証AC |
|---|---|---|
| Step 2 | US3.3 / NFR-2.5, FR-5.3 | AC3.3.3 |
| Step 3 | US3.3 / NFR-2.6, NFR-3.3, NFR-4.10, FR-5.1 | AC3.3.1 |
| Step 3-4 | US3.3 / NFR-4.9, FR-5.2 | AC3.3.2 |
| Step 5 | NFR-4.11（失敗顕在化・シークレットなし） | ローカル検証＋CI初回実行 |

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
契約の `plan_profile.steps` への適合: 本ユニットはCI設定（packaging）のため、テスト可能レイヤを「監査設定（Step 2 — deny.toml成立の検証）」「CIジョブ定義（Step 3-5 — ローカルでのジョブ相当コマンド実行が事後テスト）」に適合させ、データモデル・API・Frontend レイヤは該当なしとして省略する（メソドロジー test-after は不変 — 設定を書いた後にその検証コマンドを実行する）。ランナー準備（Step 1）は最初の検証より前に置く。

## Assumptions & Open Questions

None.
