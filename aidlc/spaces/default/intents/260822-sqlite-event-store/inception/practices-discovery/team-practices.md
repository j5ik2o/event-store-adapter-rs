# Team Practices — event-store-adapter-rs

> practices-discovery ステージの確定版。ソロメンテナーOSS Rustクレートの
> Git履歴・CI設定・コードスタイル設定・リバースエンジニアリング成果物
> （`aidlc/spaces/default/codekb/sqlite/code-structure.md`,
> `technology-stack.md`, `dependencies.md`, `code-quality-assessment.md`,
> `architecture.md`, `business-overview.md`）から推定した実務慣行に、
> quality/developer/devsecops 3エージェントの検分と人間インタビュー
> （Q1〜Q8）の回答を統合した最終版。

## Way of Working

私たちは `main` を単一のトランクとするトランクベース開発を行っています。
`main` への push と PR で CI（`ci.yml`）が走り、リリースは `v[0-9]+.[0-9]+.[0-9]+`
形式のタグ push で自動的に crates.io へ publish されます（`lib-release.yml`）。

PRのマージは **マージコミット方式**（`Merge pull request #NNN from ...`）を
一貫して使っており、squash-mergeではありません。直近30コミットのマージコミット
はすべて `Merge pull request` 形式で、`git log --merges` にsquashの痕跡（1行に
複数コミットが圧縮された形跡）は見られません。Renovate による依存更新PRは
minor/patch/pin/digest 更新と devDependencies が自動マージ（`platformAutomerge: true`）
される設定です。

**インタビュー確認事項（Q1）**: このプロジェクトのPRマージ方式は**マージコミット
方式を今後も継続**します（org.md既定のsquash-mergeへの統一は採用しません）。
AI-DLC自体の Construction worktree のマージ（Bolt branch → main）は org.md 既定
どおり**マージコミット方式**で行います（org.mdの「squash-merge」既定は本プロジェクト
では上書きし、Bolt 1つ = 1マージコミットとして `main` に反映します。個々のBolt
ブランチ上の細粒度コミット履歴はBoltブランチ側に残り、`main` にはマージコミット
としてまとまった形で入ります）。worktreeのベースブランチ・マージターゲットは
org.md既定どおり `main` です。

短命フィーチャーブランチ（`feature/*`, `renovate/*`, `j5ik2o-patch-*` など）を
使い、リモートに残存する未マージブランチも複数見られます
（`feature/fix-cargo-toml` など）。

バージョンは Conventional Commits ベースで `lib-bump-version.yml` が
`main` マージ後に自動判定・自動コミット（`version up to vX.Y.Z`）・自動タグ
付けを行い、それをトリガーに `lib-release.yml` が crates.io へ publish します。
すなわち「mainへのマージ = 次リリース候補の自動生成」という運用です。この
自動バンプ・自動タグの仕組みは今回の作業でも変更しません（詳細は
## Deployment 参照）。

今回の作業（SQLiteバックエンド追加 + feature分割）でも、既存の
マージコミット方式・Conventional Commits・タグ駆動リリースを踏襲します。

## Walking Skeleton

**インタビュー確認事項（Q2）**: 私たちはウォーキングスケルトンを最初に作ります。
SQLite対応を薄いエンドツーエンド1本（最小のイベント永続化＋読出し）から着手し、
`StorageBackend` 実装〜`GenericEventStore` 委譲〜公開ファサードの土台が
つながることを実機能の前に証明します。Bolt 1はこのスケルトンとして実行され、
ユーザーの明示的な承認を経てから残りのBoltを進めます（org.md既定の
`skeleton: on` 相当の明示的スタンス）。

## Testing Posture

- **Methodology**: test-after
- **Ordering**: 各バックエンド実装（例: StorageBackend の SQLite 実装）を書いた後に、
  同じレイヤの `#[cfg(test)]` モジュールとして統合テストを書き、
  `cargo test -p event-store-adapter-rs` で実行して確認する。

補足（エビデンス・検分・インタビューに基づく確定事項）:
- テストは専用 `tests/` ディレクトリを持たず、実装ファイルと同居する
  `#[cfg(test)]` モジュール方式（例: `event_store_for_dynamodb_test.rs`,
  `event_store_for_bigtable_test.rs`）を一貫して使います。SQLiteでも
  `event_store_for_sqlite.rs` / `event_store_for_sqlite_test.rs` の
  ファイル命名パターンに機械的に従います。
- 共有シナリオ `event_store_test_support.rs::exercise_user_account_flow`
  （作成→リネーム×2→スナップショット/リプレイ検証）を各バックエンドで
  再利用しています。SQLiteバックエンドでもこの共有シナリオをまず流し込み、
  バックエンド間の契約対称性を検証します。ただしこのシナリオは
  **ハッピーパスのみ**（quality検分の指摘）であり、以下は別途カバーします。
- **SQLiteの統合テストは testcontainers を使いません**（インタビュー時点の
  合意方針）。SQLiteはDocker不要でファイルベース／`:memory:` DBを使い、
  決定的・高速・テスト独立性の高いテストにします（プロセスグローバルな
  `env::set_var` のような変異は踏襲しません）。
- **SQLiteバックエンドでは楽観的ロック競合パステストとエラー契約テストを
  必須とします（Q4、A確定）**。具体的には (1) 同一versionへの並行書込みで
  片方が `OptimisticLockError` を返すことを検証する競合パステスト、(2)
  `persist_event` の呼び出し契約（バックエンド中立なエラー表現で返す
  こと。他バックエンドのAWS型リークやMemoryのpanicパターンを模倣しない
  こと）を固定する回帰テスト、の2種類です。SQLiteは同一プロセス内で
  再現できるため、既存バックエンドでは書けなかったこれらのテストを
  最も安価に書ける場所です。
- **カバレッジは計測しません（Q3、A確定）**。数値目標は設けず、テストの質は
  共有シナリオ＋楽観的ロック競合テスト＋エラー契約テストの充足で担保します。
  なお org.md のスコープ別80%カバレッジ床の列挙に `library` スコープは
  含まれないため、そもそも本ワークフローには数値床は適用されません。
  Test Strategy（Standard）が要求するテスト種別・量はこの決定で減じません
  （スコープ床は戦略に対して加算的であり、Bで無床を選んでも戦略要求は
  そのまま有効）。
- 既知の欠落（今回のSQLite Boltでは埋めない）: Memory バックエンド専用
  テストなし、スナップショット保持動作の実assertなし（DynamoDBテストは
  `with_keep_snapshot_count` を設定するだけで結果を検証していない）、
  examplesのビルド検証がCIにない。これらはバックログ扱いとします。

## Deployment

「デプロイ」はクラウド環境へのデプロイではなく、**crates.io へのライブラリ
公開**を意味します。project.md の学習事項（クラウドインフラ不要のOSSクレート、
質問はライブラリ文脈で）に沿い、org.md の「マージでstagingデプロイ、本番は
手動承認」は以下のように読み替えます。

- 「マージ」= `main` への PR マージ。マージ後 `lib-bump-version.yml` が
  自動的にConventional Commitsからsemverレベルを判定し、バージョンを
  バンプしてタグを打ちます（自動判定・自動タグ）。
- 「本番デプロイ」= タグ push をトリガーとした `lib-release.yml` による
  `cargo publish`（crates.io公開）。

**インタビュー確認事項（Q5）**: 「マージ→自動バージョンアップ→自動タグ→
自動crates.io公開」という現行の**完全自動リリースフローを維持**します。
SQLite機能を含むリリースについても手動承認ステップは追加しません
（org.mdの「本番は手動承認」既定はこのプロジェクトでは上書きします）。

**インタビュー確認事項（Q8・依存監査）**: 依存監査（`cargo-audit` /
`cargo-deny` の advisories チェック）を**今回のスコープでCIに追加**します。
既存の日次cron（`lib-bump-version.yml` 系のワークフロー）に相乗りさせるのが
最小コストです。Renovateはminor/patch/pin/digestとdevDependenciesを
`platformAutomerge: true` で自動マージするため、RUSTSEC照合ゲート無しでの
automergeは（特にbundled SQLiteのC由来CVE面が加わる今回は）リスクが
高いという devsecops 検分の指摘に基づく判断です。

## Code Style

- フォーマッタ: rustfmt（`rustfmt.toml` を正とする。実測値の代表例:
  `max_width = 120`, `tab_spaces = 2`, `newline_style = "Unix"`,
  `brace_style = "PreferSameLine"`, `indent_style = "Block"`,
  `normalize_comments = true`, `reorder_imports = true`,
  `reorder_impl_items = true`, `reorder_modules = true`。全キーは
  `rustfmt.toml` を参照）。nightly toolchainの `cargo +nightly fmt` を使用
  （`Makefile.toml` の `fmt` タスク、CI の `lint` ジョブでも nightly rustfmt）。
  **AGENTS.mdの「4 space インデント」という記述は実測値
  （`tab_spaces = 2`）と矛盾しており、実測を正とします**（CIが強制する
  のは `rustfmt.toml` であり、AGENTS.mdの散文には強制力がないため）。
- リンタ: **clippy を今回のスコープでCIに導入します（Q6、A確定）**。
  `cargo clippy --workspace --all-targets -- -D warnings` を `ci.yml` の
  `lint` ジョブに追加します。feature分割後は featureマトリクス
  （`--no-default-features` / 各feature単独 / `--all-features`）でも
  clippyを回します。SQLiteバックエンドは新規コードのため、既存コードの
  clippy負債と切り離し「新規モジュールはclippyクリーン」を最低線とします。
- **依存監査**: `cargo-audit` または `cargo-deny check advisories licenses`
  を今回CIに追加します（詳細は ## Deployment 参照）。
- 命名規約: Rust標準（RFC 430）どおり「型・トレイト = UpperCamelCase、
  関数・モジュール = snake_case」を軸とします（AGENTS.mdの「公開APIは
  CamelCase」という記述は不正確 — 公開APIの関数も `persist_event` のように
  snake_caseであり、正しい軸は型/関数の軸）。テスト関数名は
  AGENTS.mdが `should_*` を推奨する一方、実在テストは `test_*` 形式
  （`test_event_store_on_dynamodb` 等）であり慣行と文書が乖離しています
  — 実測（`test_*`）を優先します。
- **SQLiteバックエンドの公開型名は `EventStoreForSqlite` とします
  （Q7、A確定）**。既存の `EventStoreForDynamoDB` はブランド表記優先の
  例外であり、SQLiteはRust API Guidelines準拠（頭字語を1語扱い）で
  統一します。
- モジュール構成: 責務ごとにファイルを分割し、`serializer.rs` /
  `key_resolver.rs` のような横断的関心事は `lib/` 直下に配置。
  **新バックエンドは `StorageBackend`（5メソッド）実装 + `GenericEventStore`
  への委譲で `EventStore` を提供し、`EventStore` を直接実装しません
  （Memory バックエンドは旧構造の既知のレガシー例外）**。SQLite実装は
  この経路に従います。
- **エラー処理**: 公開・内部契約は `Result<_, EventStoreWriteError |
  EventStoreReadError>`（`thiserror` の enum）とします。バックエンド内部の
  エラーは panic させず、上記のバックエンド中立な型へ写像します。既存の
  逸脱（`OptimisticLockError` への `aws_sdk_dynamodb` 型リーク、Memory
  バックエンドの panic 使用）は新規コード（SQLite）で複製しません。
- **feature分割**: feature名は `dynamodb` / `bigtable` / `memory` /
  `sqlite` の小文字とし、`lib.rs` のグロブ再エクスポートに
  `#[cfg(feature = ...)]` ガードを付けます。現状の
  `#[allow(dead_code)]`（dynamodb / bigtable モジュール宣言に付与）は
  feature化とともに除去します（feature ゲート不在の代償として付与されて
  いたもので、踏襲しません）。
- **踏襲しないパターン**: 3バックエンドすべてにある手書きの
  `unsafe impl Send/Sync` を新規コードに複製しません。自動導出に任せます。
- **踏襲する確定慣行**: `#[async_trait]` の全 async トレイトでの使用、
  型パラメータ規約 `<AID: AggregateId, A: Aggregate<ID = AID>, E:
  Event<AggregateID = AID>>`、`new(...)` + `with_*` の self 消費型
  ビルダー構成。doc コメントは三人称現在形で開始（AGENTS.md記載）。
- MSRV（最小サポートRustバージョン）は `rust-version` フィールド未設定で
  宣言なし、CIでも未検証（`technology-stack.md` TD-11）— 今回のスコープ
  では宣言・検証を追加しません（Q6のCI強化範囲はclippyに限定し、MSRV
  検証はバックログとします）。

## Security（devsecops検分より、参考情報）

- DAST は本プロジェクトでは非該当（稼働Webサービスを持たないライブラリ
  クレートのため）。
- SAST相当は clippy（`-D warnings`）＋依存監査＋unsafe監査の3点で足ります。
- SQLite統合実装ではSQLインジェクション対策（`rusqlite` の `params!`
  バインドパラメータを使用し、文字列連結でSQLを構築しない）を徹底します。
- `openai-review.yml` の `pull_request_target` + 可変タグ
  `coderabbitai/openai-pr-reviewer@latest` 参照はサプライチェーンリスク
  として認識していますが、今回のSQLite Boltスコープには含めません
  （別イニシアチブのバックログ）。
