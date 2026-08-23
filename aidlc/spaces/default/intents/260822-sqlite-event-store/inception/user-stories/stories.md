# ユーザーストーリー (stories)

要件定義書（`../requirements-analysis/requirements.md`）のFR/NFRを、ペルソナ「CLIツール開発者のケント」（`personas.md`）の視点で機能領域別にストーリー化した。優先度は要件のMoSCoWを継承。コンポーネント文脈はコンポーネント目録（`aidlc/spaces/default/codekb/sqlite/component-inventory.md`）、テスト方針はチームプラクティス（`../practices-discovery/team-practices.md`）に基づく。

## グループ1: SQLiteバックエンドの利用

### US1.1 SQLiteへのイベント永続化と復元（Must）

As a CLIツール開発者のケント, I want イベントとスナップショットをSQLiteファイルに永続化し、後で集約を復元したい, so that クラウドサービスなしでイベントソーシングを実現できる。

- AC1.1.1: Given 空のSQLiteデータベースファイルを指すパス When `EventStoreForSqlite` を構築して作成イベント＋スナップショットを永続化する Then 必要なテーブル（journal / snapshot）が自動作成され、操作が成功する（FR-1.4）
- AC1.1.2: Given 永続化済みのアグリゲート When スナップショット取得とseq_nr以降のイベント取得で復元する Then 元と同一の状態が再構成される（FR-1.1）
- AC1.1.3: Given SQLiteバックエンドの実装 When 内部構造を確認する Then `StorageBackend` 実装＋`GenericEventStore` 委譲で `EventStore` が提供されている（FR-1.2）〔検証手段: コードレビューとコンパイル検査（`EventStore` の直接implが存在しないことのgrep走査）〕
- AC1.1.4: Given 書き込み不能な接続先（存在しないディレクトリ配下のパス等） When 永続化を試みる Then panicせず `EventStoreWriteError` 系のバックエンド中立エラーが返る（NFR-4）
- 依存: US2.1 / US2.2（feature分割とエラー型中立化の完了後、`sqlite` feature配下に実装する — スコープ定義の「feature分割先行」に整合）
- INVEST: 小・検証可能。ウォーキングスケルトンBoltの本体（スケルトンはUS2.1/US2.2の基盤整備を含めた薄い1本として実行）

### US1.2 楽観的ロックによる並行更新の保護（Must）

As a ケント, I want 同じ集約への並行更新が起きたときに片方が明確なエラーで失敗してほしい, so that データの上書き破壊なしに再試行処理を書ける。

- AC1.2.1: Given 同一バージョンのアグリゲートに対する2つの並行更新 When 両方がコミットを試みる Then 一方が成功し、他方は `OptimisticLockError` を受け取る（FR-1.3 / FR-4.2）
- AC1.2.2: Given バージョン検証と書き込み When SQLite上で実行される Then 判定と書き込みが単一トランザクションで原子的に行われる（FR-1.3）
- 実装ノート: AC1.2.1は同一DBを共有する2つのストアハンドル（`Clone` が基底接続を共有する前提）で同一バージョンから順次コミットさせることで決定的に検証する（スリープ依存の競合再現はしない）
- 依存: US1.1、US2.2（バックエンド中立なエラー表現が前提）

### US1.3 `:memory:` モードでの利用（Must）

As a ケント, I want `:memory:` のSQLiteでもEventStoreを使いたい, so that 自分のアプリのテストをファイルを残さず高速に実行できる。

- AC1.3.1: Given `:memory:` を指す接続指定 When 同一の `EventStoreForSqlite` インスタンス（およびその `Clone`）で永続化と読み出しを行う Then 同一インスタンス系列の中でデータが共有され整合した結果が得られる（`:memory:` の共有範囲はストアインスタンス単位。別インスタンスとの共有は要求しない）（FR-1.5）
- AC1.3.2: Given ファイルパス指定のDB When 永続化後にストアを再構築して読み出す Then 内容がファイルから復元される（FR-1.5）
- 依存: US1.1

### US1.4 スナップショット保持ポリシー（Could）

As a ケント, I want 古いスナップショットの保持数制限とTTL削除をSQLiteでも使いたい, so that DBファイルが際限なく肥大化しない。

- AC1.4.1: Given `keep_snapshot_count=1` の設定 When 複数回スナップショットを永続化する Then 保持数を超えた古いスナップショットが残らない（FR-1.6）
- AC1.4.2: Given `delete_ttl` の設定 When TTLを超えたスナップショットが存在する状態でイベント永続化後の保守フック（`on_event_persisted`）が実行される Then 期限切れスナップショットが削除される（FR-1.6）
- 注記: 「設定が実際に効く（サイレント無効が存在しない）」ことはAC1.4.1/AC1.4.2そのものの合否で担保する（独立ACにすると常時パス化するため統合）
- 依存: US1.1

## グループ2: featureによるビルド制御

### US2.1 必要なバックエンドだけを依存に含める（Must）

As a ケント, I want cargo feature で `sqlite` だけを選んでビルドしたい, so that AWS SDKやgRPCスタックなど使わない依存がビルドから消え、ビルドが軽くなる。

- AC2.1.1: Given `--no-default-features --features sqlite` のビルド When `cargo tree` を確認する Then aws-sdk-dynamodb / aws-config / tonic / googleapis系の依存が現れない（FR-2.1 / FR-2.2）
- AC2.1.2: Given feature未指定（`--no-default-features`）のビルド When コンパイルする Then Memoryバックエンドのみが利用可能でビルドが成功する（FR-2.1 / FR-2.2）
- AC2.1.3: Given 各バックエンドfeature単独のビルド（dynamodb / bigtable / sqlite） When `cargo tree` を確認する Then 他バックエンドの依存が混入しない（FR-2.1）
- AC2.1.4: Given feature分割後のコードベース When `lib.rs` を確認する Then 再エクスポートが `#[cfg(feature)]` でガードされ、`#[allow(dead_code)]` 抑制と未使用依存（aws-http / prost宣言 / serial_test）が除去されている（FR-2.3 / FR-3.2）
- 依存: US2.2と同一Boltで一体実施（エラー型の中立化〔US2.2〕が完了しないと dynamodb feature の独立コンパイル〔AC2.1.1/AC2.1.2〕は成立しない）

### US2.2 バックエンド中立なエラー型（Must）

As a ケント, I want 楽観的ロック失敗などのエラーがバックエンド固有のSDK型に依存しない形で返ってほしい, so that sqliteだけ使う自分のコードがAWS SDKの型に触れずにエラー処理を書ける。

- AC2.2.1: Given `sqlite` featureのみのビルド When `OptimisticLockError` を含むエラー処理コードを書く Then AWS SDK型（TransactionCanceledException等）への参照が不要である（FR-2.4）
- AC2.2.2: Given `sqlite` / Memory バックエンド When 楽観的ロック失敗が発生する Then 両者が同一のバックエンド中立なエラー表現を返す（ランタイム検証。dynamodb / bigtable は統合テスト環境が必要なため、次項の型レベル検証で担保）（FR-2.4 / FR-3.1）
- AC2.2.3: Given 共通エラー型（`types.rs`）の定義 When 型を検査する Then `OptimisticLockError` バリアントがバックエンドSDK型（`TransactionCanceledException` 等）を含まない（型レベル検証・コンパイル検査）（FR-2.4）
- 依存: なし（US2.1と同一Boltで一体実施。エラー型再設計はfeature分割検証の前提）

### US2.3 バンドル／システムSQLiteの選択（Should）

As a ケント, I want SQLiteを同梱（バンドル）するかシステムのSQLiteにリンクするかをfeatureで選びたい, so that 配布形態やビルド環境に合わせて最適な方式を使える。

- AC2.3.1: Given バンドル方式のfeature When ビルドする Then システムSQLiteへのリンクなしで（同梱ソースのコンパイルにより）ビルドが成功する〔CI検証はバンドルfeatureでのビルド成功をもって確認。システムSQLite完全不在環境の再現はベストエフォート〕（FR-2.5）
- AC2.3.2: Given システム方式のfeature When システムSQLiteのある環境でビルドする Then 同梱コンパイルなしでビルドが成功する（FR-2.5）
- 依存: US1.1

## グループ3: 品質保証

### US3.1 4バックエンドの挙動契約統一（Must）

As a ケント, I want どのバックエンドでも同じ操作が同じ挙動（成功／エラー）になってほしい, so that バックエンドを差し替えてもアプリの動作が予測できる。

- AC3.1.1: Given Memoryバックエンド When 作成イベントを `persist_event` に渡す Then panicせず他バックエンドと同じ `Err` が返る（FR-3.1）
- AC3.1.2: Given Memoryバックエンドの実装 When 内部構造を確認する Then `StorageBackend`＋`GenericEventStore` 経由に統一されている（FR-3.1）〔検証手段: コードレビューとコンパイル検査〕
- AC3.1.3: Given 新規コード（SQLite・Memoryリファクタ） When 走査する Then 手書きの `unsafe impl Send/Sync` が存在しない（FR-3.3）〔検証手段: `grep -rn "unsafe impl" lib/src/` の対象範囲ゼロ件〕
- 依存: US2.1

### US3.2 既存バックエンドと同等のテスト（Must）

As a ケント, I want SQLiteバックエンドが既存バックエンドと同じシナリオでテストされていてほしい, so that 安心して本番のCLIツールに採用できる。

- AC3.2.1: Given 共有シナリオ `exercise_user_account_flow` When SQLiteバックエンドで実行する Then 既存バックエンドと同様に緑になる（FR-4.1）
- AC3.2.2: Given SQLiteのテストスイート When CI上で実行する Then testcontainers / Docker を使わずファイルまたは `:memory:` DBのみで完結する（FR-4.3）
- AC3.2.3: Given 楽観的ロック競合とエラー契約のテスト When 実行する Then 並行更新の競合パスとバックエンド中立エラー表現が検証されている（FR-4.2）
- 依存: US1.1〜US1.3、US2.2、US3.1

### US3.3 CIによる品質保証（Should）

As a ケント, I want featureの組み合わせ・静的解析・依存監査がCIで検証されていてほしい, so that どのfeature構成を選んでも壊れていないライブラリを使える。

- AC3.3.1: Given CI設定 When PRを出す Then featureマトリクス（未指定／各feature単独／全feature）のビルド・テストが実行される（FR-5.1）
- AC3.3.2: Given CI設定 When PRを出す Then `cargo clippy --workspace --all-targets -- -D warnings` が実行され、新規モジュールはclippyクリーンである（FR-5.2）
- AC3.3.3: Given CI設定 When 定期実行またはPR時 Then 依存監査（cargo-audit / cargo-deny advisories）が実行される（FR-5.3）
- 依存: US2.1

## グループ4: ドキュメント

### US4.1 使い方がわかるドキュメントと例（Must / examplesはShould）

As a ケント, I want READMEのfeature説明とSQLiteのサンプルコードを読みたい, so that 導入から最初の永続化まで迷わず進める。

- AC4.1.1: Given README（英/日） When feature構成のセクションを読む Then 各featureの選び方と、既存利用者向け移行手順の具体内容（Cargo.toml 記述の before/after、エラー型の新旧対応表）が記載されている（FR-6.2）
- AC4.1.2: Given examples/ When SQLite利用例を実行する Then クラウド接続なしで動作する（FR-6.1）
- 依存: US1.1〜US2.3

### US4.2 スキーマと変更内容の把握（Must）

As a ケント, I want SQLiteのスキーマ定義と破壊的変更の内容を文書で確認したい, so that 自前のバックアップやマイグレーション判断ができる。

- AC4.2.1: Given docs/DATABASE_SCHEMA.md（英/日） When SQLiteセクションを読む Then journal / snapshot テーブルのスキーマが記載されている（情報提供であり作成はライブラリの自動作成が担う）（FR-6.3）
- AC4.2.2: Given CHANGELOG When 該当リリースの項を読む Then デフォルトfeature廃止とエラー型変更が破壊的変更として記載されている（FR-6.4）
- 依存: US1.1、US2.2

## ストーリー間の依存関係（要約）

- US2.1＋US2.2（feature分割とエラー型中立化 — 同一Boltで一体実施）が基盤 → US1.1 / US3.1 / US3.3 が続く
- US1.1（スケルトン中核）→ US1.2 / US1.3 / US1.4 / US2.3 / US3.2
- US4.x は対応機能の完成後

## INVEST 準拠ノート

- 各ストーリーは独立に検証可能（AC単位でテスト導出可能）
- US1.4（保持ポリシー）はCouldとして切り離し可能な形で分離
- 移行体験（既存利用者）は計画決定（Q1=B）によりストーリー化せず、FR-6.2 / FR-6.4 の要件検証で担保

## Assumptions & Open Questions

None.

## Review

**Verdict:** READY

**Reviewer:** aidlc-product-lead-agent
**Date:** 2026-08-22T13:08:43Z
**Iteration:** 1

### Findings

| # | Severity | Location | Finding | Recommendation |
|---|---|---|---|---|
| 1 | Major | US1.1（依存）／US2.1・US2.2（依存）／INVEST準拠ノート | ウォーキングスケルトンのBolt順序が自己矛盾している。US1.1の「依存」欄は「US2.1 / US2.2（feature分割とエラー型中立化の完了後、`sqlite` feature配下に実装する）」と、US2.1/US2.2が**先に完了した後**にUS1.1へ着手する逐次順序を明記する。一方、同じUS1.1の「INVEST」欄は「スケルトンはUS2.1/US2.2の基盤整備を含めた薄い1本として実行」と、US1.1・US2.1・US2.2を**単一のBolt内に束ねる**ことを述べる。さらにUS2.1／US2.2自身の「依存」欄も「US2.1と同一Boltで一体実施」と互いに同一Bolt内実施を明記しており、3ストーリーの関係が「3つの逐次Bolt」なのか「1つの束ねられたBolt」なのか本文だけでは一意に読み取れない。team-practices.md（Walking Skeleton節）は「Bolt 1はこのスケルトンとして実行され、ユーザーの明示的な承認を経てから残りのBoltを進める」とし、org.md/team.mdの確定慣行はBolt 1が単独・ゲート付きであることを前提とする。US1.1がBolt 1の本体であるにもかかわらず、その前提としてUS2.1/US2.2の「完了」を要求する記述が残っていると、delivery-planning工程でBolt分割を誤り、Bolt 1が実質2〜3個のBoltに分解されてウォーキングスケルトンの「薄い1本を最初に承認してから残りを進める」という確定運用と食い違うおそれがある。 | US1.1の「依存」欄を、INVEST欄の記述と整合するよう書き換える（例:「US2.1/US2.2の成果物〔feature分割・エラー型中立化〕を同一Bolt内で先に構築したうえで実装する。3ストーリーはBolt 1として一括実施し、個別に完了を待つ依存関係ではない」）。delivery-planning工程に渡す前に、Bolt 1のスコープが「US1.1+US2.1+US2.2の3ストーリー一括」で確定していることを一箇所に明記する。 |
| 2 | Minor | traceability.json（NFR-4 target: US1.1, US3.1／NFR-5 target: US3.2） | traceability.jsonはNFR-4のカバレッジ対象に「US1.1, US3.1」の両方をOKとして記録するが、stories.md本文でNFR-4を明示的に引用（`（NFR-4）`）しているのはUS1.1のAC1.1.4のみで、US3.1の該当AC（AC3.1.1、panicせずErrを返す）はFR-3.1のみを引用しNFR-4を引用していない。同様にNFR-5のtarget「US3.2」もAC3.2.2はFR-4.3のみを引用し、NFR-5（Docker不要・決定的・並列安全）の明示引用がない。内容としては趣旨を満たしているが、引用タグと実施記録（traceability.json）の対応が本文上で追跡できない箇所がある。 | US3.1のAC3.1.1に`（NFR-4）`の引用を追加するか、traceability.jsonのNFR-4/NFR-5 target記載を「本文で明示引用されている箇所のみ」に揃える。 |
| 3 | Minor | US4.1（優先度: Must / examplesはShould） | 1つのストーリー内でFR-6.1（Should、examples）とFR-6.2（Must、README）という異なる優先度のFRを束ねており、タイトル行に「Must / examplesはShould」と注記して対応してはいるものの、ストーリー全体としてのMoSCoWラベルが1つに定まらない。delivery-planning工程でBolt/スプリントの取捨選択（Should項目を後回しにする等）を行う際、AC単位ではなくストーリー単位でスコープ判断をすると誤ってFR-6.2（Must）まで一緒に後回しにするリスクがある。 | US4.1をAC単位（AC4.1.1=Must、AC4.1.2=Should）で分割して2ストーリー化するか、少なくとも「AC4.1.2〔examples〕はShouldのためBolt優先順位付け時に独立して取捨選択可能」という一文を明記する。 |

### Summary

各ストーリーはActor/Action/Value（As a / I want / so that）を備え、Given/When/Then形式のACはFR-1.1〜FR-6.4・NFR-4/NFR-5のすべてに対応付けられており、traceability.jsonの逆引き（reverse）もstories.md本文の引用と大筋で一致する（FRの取りこぼし・範囲外ストーリーの混入は確認されなかった）。MoSCoW優先度も要件定義書のマッピングと概ね整合する。唯一のMajor指摘は、ウォーキングスケルトン（US1.1）とfeature分割・エラー型中立化（US2.1/US2.2）の関係が「逐次完了待ち」なのか「単一Bolt内での一括実施」なのか本文中で自己矛盾しており、delivery-planning工程でのBolt 1スコープ確定を誤らせるリスクがある点である。Critical該当なし、Major実質1件（複数箇所にまたがる同一論点として集約）のため、READY判定とする。ただしBolt 1の依存関係表現は次工程（delivery-planning）着手前に一箇所へ統一しておくことを推奨する。
