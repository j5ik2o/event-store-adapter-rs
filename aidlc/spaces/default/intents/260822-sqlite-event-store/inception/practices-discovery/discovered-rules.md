# Discovered Rules — event-store-adapter-rs

> 明確にエビデンスがある、または人間により既に明言済みの制約のみを記載する。
> 推測でルールを作らない方針のため、各項目はインタビュー回答（Q1〜Q8）または
> 静的エビデンスに直接トレースできるもののみとする。

## Mandated

- ALWAYS `main` ブランチへの PR マージ前に CI の `cargo fmt -- --check`
  （nightly rustfmt、`rustfmt.toml` 設定）をパスさせる
  （`ci.yml` の `lint` ジョブが `test-lib` ジョブの前提条件）。
- ALWAYS コミットメッセージを Conventional Commits 形式
  （`feat:`, `fix:`, `docs:`, `style:`, `refactor:`, `perf:`, `test:`,
  `revert:`, `chore:`, `BREAKING CHANGE`）に従わせる。
  `lib-bump-version.yml` がこの正規表現でコミットを走査してsemver
  レベルを自動判定するため、この形式を外れたコミットはバージョン
  バンプ計算から除外される（`AGENTS.md` にも明記、CIワークフローでも
  実装として確認）。
- ALWAYS DBスキーマ（テーブル）はライブラリの自動作成機能が担い、
  ドキュメント（`docs/DATABASE_SCHEMA.md` 等）は情報提供に留める
  （project.md ## Corrections 学習事項、2026-08-22）。
- ALWAYS 新バックエンドは `StorageBackend` + `GenericEventStore` 経由で
  実装する（`EventStore` を直接実装しない。Memory バックエンドは既知の
  レガシー例外）。2/3バックエンドの準拠実績と `architecture.md` の
  移行方針で裏付けられ、developer検分の指摘を受けてQ7サマリー確認を
  経て確定した、本イニシアチブで最も実装を拘束する慣行。
- ALWAYS SQLiteバックエンドには楽観的ロック競合パスとエラー契約のテストを
  含める（同時更新で `OptimisticLockError` が返ることの検証、および
  バックエンド中立なエラー表現での返却を固定する回帰テスト）
  （Q4回答: A. 含める — 確定）。

## Forbidden

- NEVER 本プロジェクトの検討・質問においてクラウドインフラ前提
  （AWSアカウント確保など）を持ち込まない。依存クレート制約・ビルド方式・
  MSRV・CIマトリクスといったライブラリ文脈に置き換える
  （project.md ## Corrections 学習事項、2026-08-22）。
- NEVER ソロOSSである本プロジェクトに、予算確保・モブ配員・複数チーム
  調整など組織前提の確認事項を持ち込まない（project.md ## Corrections
  学習事項、2026-08-22）。
- NEVER 新規コードに手書きの `unsafe impl Send/Sync` を複製しない
  （自動導出に任せる）。既存3バックエンドすべてにある実装パターン
  （TD-06）であり、developer/devsecops双方の検分がSQLiteでの非踏襲を
  勧告し、team-practices.mdの確定慣行として反映済み。
- NEVER バックエンド内部のエラーを panic させない。
  `EventStoreReadError` / `EventStoreWriteError` へ写像する
  （Memoryバックエンドのpanic使用（TD-05）を新規コードで模倣しない）。
