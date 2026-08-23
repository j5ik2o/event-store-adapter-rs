# Bolt計画 (bolt-plan)

「Bolt」は、仕事のひとまとまりを設計〜実装〜テストまで一気に通す1回のビルドパス（終わると動くものが残る単位）。本計画は、ユニット定義（`../units-generation/unit-of-work.md`）・依存DAG（`../units-generation/unit-of-work-dependency.md`）・ストーリー対応（`../units-generation/unit-of-work-story-map.md`）・契約（`../contract-design/contract-summary.md`）・要件（`../requirements-analysis/requirements.md`）・ストーリー（`../user-stories/stories.md`）・コンポーネント（`../domain-design/components.md`）・チームプラクティス（`../practices-discovery/team-practices.md`）に基づき、Q&A（`delivery-planning-questions.md`）で確定した3 Bolt・直列構成を記述する。

## Bolt順序（直列）

| # | Bolt | 含むユニット | スケルトン | 複雑度 |
|---|---|---|---|---|
| 1 | bolt-skeleton | U1（最小部分）＋U2（最小スライス） | ✅ ウォーキングスケルトン（ゲート付き） | S |
| 2 | bolt-full-implementation | U1（残り）＋U2（残り） | — | L |
| 3 | bolt-ci-and-docs | U3＋U4 | — | S |

適用プラクティス（`../practices-discovery/team-practices.md`）: ウォーキングスケルトンを最初に作る（Bolt 1は単独・ゲート付きで、ユーザー承認後に残りを実行）。Construction worktreeのベース/ターゲットは `main`、マージはマージコミット方式。

## Bolt 1: bolt-skeleton〔ウォーキングスケルトン〕

- **含む作業**: U1のうちスケルトンに必要な最小 — 中立エラー型への再設計（`OptimisticLockError` のSDK型除去）と `sqlite` feature の器・rusqlite依存の追加。U2のうち最小スライス — `EventStoreForSqlite::new(path)` によるファイルDBへの最小のイベント＋スナップショット永続化と読出し（スキーマ自動作成含む）。対応ストーリー: US2.2（中核部分）・US1.1（AC1.1.1/AC1.1.2）
- **ウォーキングスケルトンとして証明する範囲**: `CoreTypes`（中立エラー型）→ `StorageAbstraction`（StorageBackend実装・GenericEventStore委譲）→ `SqliteBackend`（rusqlite・spawn_blocking・自動テーブル作成）→ 公開ファサード、の全レイヤが1本につながって動くこと
- **Definition of Done**: 新規テスト（作成→読出し→復元の最小シナリオ）が緑／既存テストが緑のまま（エラー型変更の影響吸収込み）／`cargo build --no-default-features --features sqlite` が成功
- **確信仮説（このBoltが出荷されると何が分かるか)**: rusqlite＋spawn_blocking のアーキテクチャ選定（ADR-002）と中立エラー型（ADR-001）が既存の抽象と整合して動く — 最大の技術リスク（A1/A2前提）が解消される
- **期待デモ**: ファイルDBを指定してイベントを永続化し、プロセス再起動相当の再構築で状態が復元されるテスト実行ログ

## Bolt 2: bolt-full-implementation

- **含む作業**: U1の残り — `dynamodb` / `bigtable` featureの隔離完成・デフォルトなし化・`lib.rs` cfgガード・未使用依存削除・Memory準拠化（panic→Err・unsafe impl除去）。U2の残り — 楽観的ロックの原子的CAS（ADR-003）・`:memory:` 対応・スナップショット保持ポリシー・`sqlite-system` feature・共有シナリオ搭載・競合パス/エラー契約テスト。対応ストーリー: US2.1・US2.2（完成）・US3.1・US1.2・US1.3・US1.4・US2.3・US3.2
- **Definition of Done**: 全featureの組み合わせ（未指定/各単独/全部）でローカルビルド成功／共有シナリオ `exercise_user_account_flow` がSQLite・Memoryで緑／競合パステストが `OptimisticLockError` を決定的に検証／既存DynamoDB/Bigtable統合テスト緑
- **確信仮説**: feature分割が既存利用者の全バックエンド動作を保ちながら依存を隔離できる（cargo treeでSDK依存が消える）— 破壊的変更の技術面が完成
- **期待デモ**: `cargo tree --no-default-features --features sqlite` にAWS/gRPC系依存が現れない出力＋全テスト緑

## Bolt 3: bolt-ci-and-docs

- **含む作業**: U3 — CIへのfeatureマトリクス・clippy（-D warnings）・依存監査（cargo-audit/deny）の追加。U4 — examples追加・README（英/日）feature説明と移行手順・DATABASE_SCHEMA.md（英/日）SQLiteスキーマ・CHANGELOG破壊的変更記載。対応ストーリー: US3.3・US4.1・US4.2
- **Definition of Done**: CIが新ジョブ込みで緑／examplesがクラウド接続なしで動作／移行手順にCargo.toml before/afterとエラー型新旧対応を記載
- **確信仮説**: リリース可能状態 — 利用者が移行手順に従って更新でき、CIが今後のfeature退行を防ぐ
- **期待デモ**: CI実行結果（マトリクス全緑）とREADMEのfeatureセクション

## 構築工程の反復方式

本計画はスケルトン先行・ユニット一気通貫（1つの作業まとまりを設計→実装まで通してから次へ）を要求するため、構築工程は unit-major（ユニット単位の一気通貫）で実行する。

## Assumptions & Open Questions

None.
