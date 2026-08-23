# Project-Level Rules

> Project-specific specialisation and corrections. Loaded after `org.md` and
> `team.md` as strict-additive guidance; contradictions with broader policy
> are rejected. Populated by practices-discovery and the self-learning loop.
>
> Use sparingly: most teams don't need a project layer. Reach for it
> only when this specific project needs stable, durable guidance beyond the
> team practice (for example, package-specific release checks or an additional
> regression suite for a legacy component).

## Way of Working

<!-- Project-specific specialisation. Example: -->
<!-- This monorepo requires package-scoped branch names and a package owner -->
<!-- review in addition to the team's normal merge policy. -->

## Walking Skeleton

<!-- Project-specific specialisation. Example: -->
<!-- The walking skeleton must exercise the legacy service adapter as well -->
<!-- as the new service boundary. -->

## Testing Posture

<!-- Project-specific specialisation. -->

## Deployment

<!-- Project-specific specialisation. -->

## Code Style

<!-- Project-specific specialisation. -->

## Tech Stack

<!-- Technology choices locked for this project. -->

## Decided

<!-- Decisions made in earlier stages that should not be re-asked. -->
<!-- Format: DECIDED: [decision] (Stage [slug], [date]) -->

- SQLiteバックエンドのサポート共有単位は1ストアインスタンスとそのクローンのみ。同一DBファイルを複数ストアインスタンス・複数プロセスから同時に開くことはサポート外とし、PRAGMA調整は行わず SQLITE_BUSY は IOError として即時返却する。多重起動防止はアプリケーション側の責務（ユーザー裁定、nfr-design/u2） (learned 2026-08-23) <!-- cid:260822-sqlite-event-store:nfr-design:a6bf24616df9b216c26069163e57a81bd6f1dd6d2af795eac46e43751a112a47 -->
## Scope Overrides

<!-- Custom scope rules for this project. -->

## Forbidden

<!-- Populated by practices-discovery affirmation gate. -->
<!-- Format: NEVER [behavior] (affirmed [date]) -->
<!-- Example: NEVER throw exceptions across service layer boundaries (affirmed 2026-05-17) -->

- NEVER 本プロジェクトの検討・質問においてクラウドインフラ前提 (affirmed 2026-08-22)
（AWSアカウント確保など）を持ち込まない。依存クレート制約・ビルド方式・ (affirmed 2026-08-22)
MSRV・CIマトリクスといったライブラリ文脈に置き換える (affirmed 2026-08-22)
- NEVER ソロOSSである本プロジェクトに、予算確保・モブ配員・複数チーム (affirmed 2026-08-22)
調整など組織前提の確認事項を持ち込まない（project.md ## Corrections (affirmed 2026-08-22)
学習事項、2026-08-22）。 (affirmed 2026-08-22)
- NEVER 新規コードに手書きの `unsafe impl Send/Sync` を複製しない (affirmed 2026-08-22)
（自動導出に任せる）。既存3バックエンドすべてにある実装パターン (affirmed 2026-08-22)
（TD-06）であり、developer/devsecops双方の検分がSQLiteでの非踏襲を (affirmed 2026-08-22)
勧告し、team-practices.mdの確定慣行として反映済み。 (affirmed 2026-08-22)
- NEVER バックエンド内部のエラーを panic させない。 (affirmed 2026-08-22)
`EventStoreReadError` / `EventStoreWriteError` へ写像する (affirmed 2026-08-22)
（Memoryバックエンドのpanic使用（TD-05）を新規コードで模倣しない）。 (affirmed 2026-08-22)
## Mandated

<!-- Populated by practices-discovery affirmation gate. -->
<!-- Format: ALWAYS [behavior] (affirmed [date]) -->
<!-- Example: ALWAYS use Result<T,E> for fallible operations in service layer (affirmed 2026-05-17) -->

- ALWAYS `main` ブランチへの PR マージ前に CI の `cargo fmt -- --check` (affirmed 2026-08-22)
（nightly rustfmt、`rustfmt.toml` 設定）をパスさせる (affirmed 2026-08-22)
（`ci.yml` の `lint` ジョブが `test-lib` ジョブの前提条件）。 (affirmed 2026-08-22)
- ALWAYS コミットメッセージを Conventional Commits 形式 (affirmed 2026-08-22)
（`feat:`, `fix:`, `docs:`, `style:`, `refactor:`, `perf:`, `test:`, (affirmed 2026-08-22)
`revert:`, `chore:`, `BREAKING CHANGE`）に従わせる。 (affirmed 2026-08-22)
`lib-bump-version.yml` がこの正規表現でコミットを走査してsemver (affirmed 2026-08-22)
レベルを自動判定するため、この形式を外れたコミットはバージョン (affirmed 2026-08-22)
バンプ計算から除外される（`AGENTS.md` にも明記、CIワークフローでも (affirmed 2026-08-22)
実装として確認）。 (affirmed 2026-08-22)
- ALWAYS DBスキーマ（テーブル）はライブラリの自動作成機能が担い、 (affirmed 2026-08-22)
ドキュメント（`docs/DATABASE_SCHEMA.md` 等）は情報提供に留める (affirmed 2026-08-22)
（project.md ## Corrections 学習事項、2026-08-22）。 (affirmed 2026-08-22)
- ALWAYS 新バックエンドは `StorageBackend` + `GenericEventStore` 経由で (affirmed 2026-08-22)
実装する（`EventStore` を直接実装しない。Memory バックエンドは既知の (affirmed 2026-08-22)
レガシー例外）。2/3バックエンドの準拠実績と `architecture.md` の (affirmed 2026-08-22)
移行方針で裏付けられ、developer検分の指摘を受けてQ7サマリー確認を (affirmed 2026-08-22)
経て確定した、本イニシアチブで最も実装を拘束する慣行。 (affirmed 2026-08-22)
- ALWAYS SQLiteバックエンドには楽観的ロック競合パスとエラー契約のテストを (affirmed 2026-08-22)
含める（同時更新で `OptimisticLockError` が返ることの検証、および (affirmed 2026-08-22)
バックエンド中立なエラー表現での返却を固定する回帰テスト） (affirmed 2026-08-22)
（Q4回答: A. 含める — 確定）。 (affirmed 2026-08-22)
## Corrections

<!-- Project-specific corrections from human feedback. -->
<!-- Format: NEVER/ALWAYS [behavior] (learned [date]) -->
- 本プロジェクトの市場調査では競合比較を行わない（ユーザー確認済み）。competitive-analysis は競合比較表ではなくポジショニング方針・参考情報・テーブルステークス中心の構成とする (learned 2026-08-22) <!-- cid:260822-sqlite-event-store:market-research:e2616802d47498fff5353afea02e54b470e2a824237c14a0f8a908ae12832601 -->
- 本プロジェクトはクラウドインフラ不要のOSS Rustクレートである。各工程の質問はクラウド前提（AWSアカウント等）をライブラリ文脈（依存クレート制約・ビルド方式・MSRV・CIマトリクス）に置き換えて生成する (learned 2026-08-22) <!-- cid:260822-sqlite-event-store:feasibility:5c1a5a8eb5cdb30ddf46a198b147c0c8553af8c2c78421826321d6f205c70349 -->
- スキーマのドキュメント記載（DATABASE_SCHEMA.md等）は情報提供であり、DDL提供によるテーブル作成責務の委任とは区別して扱う。本プロジェクトのスキーマ作成はライブラリの自動テーブル作成が担う (learned 2026-08-22) <!-- cid:260822-sqlite-event-store:scope-definition:77f36d519770854c107b85cd2c8ae1692850b628e914f5104b365e445465daac -->
- 本プロジェクトはソロOSS。予算・モブ配員・複数チーム調整などの組織前提の確認項目は対象外とし、質問はリスク・スコープ・技術判断に絞る (learned 2026-08-22) <!-- cid:260822-sqlite-event-store:approval-handoff:1fd9c3f298b684ea4b598d4876db5dac92a0177fe8a55c8f9d43d216e59a7db0 -->
- 質問は上流工程で確定済みの事項を再確認せず、未確定の残余のみに絞って生成する（フェーズが進むほど質問は逓減させる） (learned 2026-08-22) <!-- cid:260822-sqlite-event-store:requirements-analysis:bc5ced87b872a5b805fe51709ce4e66a70f39ee2106888d2217031455448229e -->
- 本プロジェクト（OSSライブラリ）では公開APIを「ユーザー向け機能」として扱い、利用者=外部開発者の視点でストーリー化する（ユーザーストーリー工程はライブラリだからという理由でスキップしない） (learned 2026-08-22) <!-- cid:260822-sqlite-event-store:user-stories:3c6985d71c7366cae954e96eaba0bcf0c5105a591a8197b4941a19df51036d89 -->
