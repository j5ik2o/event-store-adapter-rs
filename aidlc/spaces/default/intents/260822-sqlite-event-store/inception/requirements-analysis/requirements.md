# 要件定義書: SQLite対応EventStoreとバックエンドfeature分割

## 出典と前提

本要件は以下の確定済み上流成果物から導出した:

- インテントステートメント（`../../ideation/intent-capture/intent-statement.md`、intent-statement）— 問題・成功指標・feature構成・リリース方針
- スコープ定義書（`../../ideation/scope-definition/scope-document.md`、scope-document）— IN/OUT境界・MoSCoW・進め方
- コード知識ベース: 事業概要（`aidlc/spaces/default/codekb/sqlite/business-overview.md`、business-overview）、アーキテクチャ（`aidlc/spaces/default/codekb/sqlite/architecture.md`、architecture）、コード構成（`aidlc/spaces/default/codekb/sqlite/code-structure.md`、code-structure）— 既存の `StorageBackend` / `GenericEventStore` 抽象と技術的負債（TD-01〜TD-14）
- チームプラクティス（`../practices-discovery/team-practices.md`、team-practices）— テスト方針・命名・レイヤ境界・CI強化の確定事項
- 本工程Q&A（`requirements-analysis-questions.md`）— Memory準拠化・依存整理・保持ポリシー実装の確定

## 機能要件（FR）

### FR-1: SQLiteバックエンド

- **FR-1.1** システムは `EventStoreForSqlite<AID, A, E>` 公開型を提供し、既存の `EventStore` トレイト契約（`persist_event` / `persist_event_and_snapshot` / `get_latest_snapshot_by_id` / `get_events_by_id_since_seq_nr`）を完全に実装しなければならない（出典: intent-statement 成功指標、team-practices 型名Q7）
  - AC1: Given 新規アグリゲート When 作成イベント＋スナップショットを永続化 Then 読み出しで同一状態が復元される
  - AC2: Given 既存アグリゲート When seq_nr以降のイベントを取得 Then スナップショット＋イベントリプレイで最新状態が再構成される
- **FR-1.2** SQLiteバックエンドは内部抽象 `StorageBackend`（5メソッド）を実装し、`GenericEventStore` への委譲で `EventStore` を提供しなければならない。`EventStore` の直接実装は禁止（出典: team-practices レイヤ境界規約 / project.md Mandated）
- **FR-1.3** 楽観的ロック: バージョン不一致の更新は失敗し、`OptimisticLockError` として呼び出し側に返らなければならない。判定と書き込みはSQLiteトランザクション内で原子的に行う（Bigtableの非原子的な read→write 方式〔TD-07〕は踏襲しない）（出典: intent-statement 成功指標、architecture）
  - AC1: Given 同一バージョンのアグリゲートに対する2つの並行更新 When 両方がコミットを試みる Then 一方は成功し他方は `OptimisticLockError` を受け取る
- **FR-1.4** スキーマ（journal / snapshot テーブル）は初回利用時にライブラリが自動作成しなければならない。DDLの利用者側適用は要求しない（出典: scope-document Q5 / project.md Mandated）
  - AC1: Given 空のSQLiteデータベースファイル When EventStoreForSqlite を構築して最初の永続化を行う Then 必要なテーブルが自動作成され操作が成功する
- **FR-1.5** 接続先として、ファイスパスによる単一ファイルDBおよび `:memory:` モードの両方をサポートしなければならない（出典: competitive-analysis テーブルステークス〔scope-document IN項目経由〕）
- **FR-1.6** スナップショット保持ポリシー（`keep_snapshot_count` による保持数制限、`delete_ttl` による期限削除）をSQLiteで実装しなければならない。設定を受け付けて無視するサイレント無効（TD-08）を作ってはならない（出典: 本工程Q3=A）
  - AC1: Given keep_snapshot_count=1 When 複数回スナップショットを永続化 Then 古いスナップショットが保持数を超えて残らない

### FR-2: バックエンドfeature分割

- **FR-2.1** cargo feature `dynamodb` / `bigtable` / `sqlite` を定義し、各バックエンドのコード・依存をそのfeature配下に隔離しなければならない。Memory バックエンドは常時有効（featureなし）とする（出典: intent-statement）
- **FR-2.2** デフォルトfeatureは定義しない。feature未指定のビルドでは Memory 以外のバックエンドが含まれず、対応するクラウドSDK依存（aws-sdk-dynamodb / aws-config / tonic / googleapis-tonic-google-bigtable-v2）がビルド対象から外れなければならない（出典: intent-statement Q3）
  - AC1: Given `--no-default-features` でのビルド When `cargo tree` を確認 Then AWS SDK / tonic系の依存が現れない
- **FR-2.3** `lib.rs` の再エクスポートは `#[cfg(feature = ...)]` でガードし、`#[allow(dead_code)]` 抑制（TD-03）は除去する（出典: team-practices feature分割規約）
- **FR-2.4** 共通エラー型（`types.rs`）からAWS SDK型への依存（`TransactionCanceledExceptionWrapper` 〔TD-01〕）を除去し、楽観的ロック失敗をバックエンド中立な表現にしなければならない。破壊的変更は許容済み（出典: intent-statement リリース方針、architecture TD-01、project.md Forbidden〔SDK型リーク禁止〕）
- **FR-2.5** SQLiteのリンク方式として、バンドル（SQLite同梱）とシステムライブラリ利用の両方をfeatureで選択できなければならない（出典: constraint-register C2〔scope-document 経由〕）

### FR-3: 既存コードの整合（今回スコープ内のリファクタリング）

- **FR-3.1** Memory バックエンドを `StorageBackend` + `GenericEventStore` 準拠にリファクタし、作成イベントの `persist_event` 渡し時の panic（TD-05）を `Err` 返却に修正して4バックエンドの挙動契約を統一しなければならない（出典: 本工程Q1=A）
  - AC1: Given Memory バックエンド When 作成イベントを persist_event に渡す Then panic せず Err が返る
- **FR-3.2** 未使用依存（`aws-http`）・宣言のみの依存（`prost`）・未使用のdev依存（`serial_test`）を削除しなければならない（出典: 本工程Q2=A、code-structure）
- **FR-3.3** 新規コードに手書きの `unsafe impl Send/Sync` を複製してはならない（既存3バックエンドのパターン〔TD-06〕の非踏襲。出典: project.md Forbidden）

### FR-4: テスト

- **FR-4.1** SQLiteバックエンドは共有シナリオ `exercise_user_account_flow` に乗せ、既存バックエンドと同等のテストが緑にならなければならない（出典: intent-statement 成功指標、team-practices）
- **FR-4.2** 楽観的ロック競合パステスト（並行更新で `OptimisticLockError` を検証）とエラー契約テスト（バックエンド中立なエラー表現の検証）を必須で含めなければならない（出典: team-practices Q4確定）
- **FR-4.3** SQLiteのテストは testcontainers / Docker を使用せず、ファイルまたは `:memory:` DBで完結しなければならない（出典: team-practices）

### FR-5: CI

- **FR-5.1** GitHub Actions に featureマトリクス（最低限: feature未指定 / 各バックエンドfeature単独 / 全feature）のビルド・テスト検証を追加しなければならない（出典: scope-document Should、team-practices）
- **FR-5.2** `cargo clippy --workspace --all-targets -- -D warnings` をCIに追加しなければならない。新規モジュール（SQLite関連）はclippyクリーンを必須とする（出典: team-practices Q6確定）
- **FR-5.3** 依存監査（cargo-audit または cargo-deny の advisories チェック）をCIに追加しなければならない（出典: team-practices Q8確定）

### FR-6: ドキュメント

- **FR-6.1** examples に SQLite 利用例を追加しなければならない（出典: scope-document Q1）
- **FR-6.2** README（英/日）に feature 構成の説明と既存利用者向け移行手順を追記しなければならない（出典: scope-document Q1）
- **FR-6.3** docs/DATABASE_SCHEMA.md（英/日）に SQLite スキーマを記載しなければならない（情報提供であり、テーブル作成の責務はFR-1.4の自動作成が担う）（出典: scope-document Q1/Q5）
- **FR-6.4** CHANGELOG に破壊的変更（デフォルトfeature廃止・エラー型変更）を記載しなければならない（出典: intent-statement リリース方針）

## 非機能要件（NFR）

- **NFR-1（互換性）**: 既存の公開トレイトシグネチャ（`EventStore` / `AggregateId` / `Event` / `Aggregate`）は変更しない。破壊的変更はfeature構成・エラー型・モジュール公開範囲に限る（出典: constraint-register C3、feasibility Q6）
- **NFR-2（依存最小）**: SQLiteドライバは1クレートのみ。ORM等の大型フレームワーク依存は導入しない（出典: constraint-register C1）
- **NFR-3（ビルド互換）**: 現行の安定版Rust（edition 2021）でビルド可能であること。MSRV宣言・検証は今回のスコープ外（出典: feasibility Q6、team-practices）
- **NFR-4（コード品質）**: rustfmt（`rustfmt.toml` 準拠）・clippy（-D warnings）をパスすること。バックエンド内部のエラーはpanicさせず `EventStoreWriteError` / `EventStoreReadError` へ写像すること（出典: team-practices / project.md Forbidden）
- **NFR-5（テスト実行環境）**: SQLite関連テストはDocker不要・決定的・並列安全であること（出典: team-practices）

## 制約（Constraints）

制約レジスタ（C1〜C11）を継承する。特に: ドライバ1クレートのみ（C1）／バンドル・システム両対応（C2）／公開APIシグネチャ不変更（C3）／自作実装（C6）／完全自動リリースフロー維持（team-practices Q5）。

## 優先度マッピング（MoSCoW）

| 優先度 | 要件 |
|---|---|
| Must | FR-1.1〜FR-1.5、FR-2.1〜FR-2.4、FR-3.1〜FR-3.3、FR-4.1〜FR-4.3、FR-6.2〜FR-6.4 |
| Should | FR-2.5、FR-5.1〜FR-5.3、FR-6.1 |
| Could | FR-1.6（保持ポリシー — 実装を既定とするが、着手順は最後） |

## トレーサビリティ

すべてのFR/NFRは上記「出典と前提」の構想フェーズ成果物・チームプラクティス・本工程Q&Aのいずれかに遡る。逆方向: intent-statement の成功指標はFR-1.1/FR-1.3/FR-4.1で、scope-document のIN項目はFR-1〜FR-6でカバーされ、対応のないIN項目はない。

## Assumptions & Open Questions

None.

## Review

**Verdict:** READY
**Reviewer:** aidlc-product-lead-agent
**Date:** 2026-08-22T12:50:51Z
**Iteration:** 1

### Findings

| # | Severity | Location | Finding | Recommendation |
|---|---|---|---|---|
| 1 | Major | FR-1.5, FR-2.3〜FR-2.5, FR-3.2〜FR-3.3, FR-6.1〜FR-6.4（うち一部はMust優先度） | 多数のFRに受け入れ基準（AC）が付与されていない。特にMust優先度のFR-1.5（接続方式2種のサポート）、FR-2.3、FR-2.4、FR-3.2、FR-3.3、FR-6.2〜FR-6.4はACなしで、QAが要件文だけから合否判定テストを一意に書き起こせない。例えばFR-1.5は「ファイルパスDB」と「`:memory:`モード」の双方対応を要求するが、`:memory:`が接続（Connection）単位で揮発するのかストア（EventStoreForSqlite）インスタンス単位で共有されるのか、複数コネクション間でのデータ共有要否が本文からは読み取れず、実装判断が割れうる。 | 少なくともMust優先度の各FRに Given/When/Then 形式のACを1つ以上追加する。FR-1.5は「Given ファイルパスDBを指定 When 永続化→再読込 Then 内容が復元される」「Given `:memory:`を指定し同一`EventStoreForSqlite`インスタンスで複数回操作 When 永続化→読出し Then 同一プロセス内で整合する」のように、`:memory:`の共有範囲（インスタンス単位かコネクションプール単位か）を明示したACにする。 |
| 2 | Minor | FR-2.2 / FR-5.1 | FR-2.2のAC1は `--no-default-features`（feature未指定）時にAWS SDK/tonic系依存が現れないことのみ検証しており、「各バックエンドfeature単独ビルド時に他バックエンドの依存が混入しない」という分離要件（FR-2.1「各バックエンドのコード・依存をそのfeature配下に隔離」およびFR-5.1のfeatureマトリクスCI）に対応するACが無い。 | FR-2.2または新設のFR-2.1配下に「Given `--features dynamodb`（bigtable/sqlite feature無効） When `cargo tree` を確認 Then bigtable/sqlite側の依存が現れない」旨のACを追加する。 |
| 3 | Minor | FR-1.5 | 「ファイスパス」は「ファイルパス」の誤字（タイポ）。実装への影響はないが、公開ドキュメントに転記される前に修正を推奨する。 | 誤字修正。 |
| 4 | Minor | 優先度マッピング（MoSCoW） | FR-6.2〜FR-6.4（README/DATABASE_SCHEMA.md/CHANGELOG）をMustに、FR-6.1（examples）をShouldに割り付けているが、この区分の根拠がscope-document側のMoSCoW表には明示されていない（scope-documentのIN表はドキュメント4項目を優先度分けせず横並びでIN登録している）。要件定義段階での優先度細分化自体は正当な作業だが、出典欄にその判断根拠（なぜexamplesだけShouldか）が一言記載されていると、後続のdelivery-planningでの取捨選択時に迷いが減る。 | FR-6.1の出典行に、Must/Should分割の根拠（例:「破壊的変更の告知に直結するREADME/CHANGELOG/スキーマ文書を優先し、利用例は次点」）を一言添える。 |

### Summary

出典タグに基づくトレーサビリティ（上流3成果物＋team-practices＋本工程Q&Aの確定事項）は全FR/NFRで一貫しており、scope-documentのIN項目・MoSCoW区分・OUT除外項目との整合も取れている（対応漏れのIN項目、範囲外項目の混入のいずれも確認されなかった）。主な改善点はテスト可能性で、Must優先度を含む複数のFRに受け入れ基準（AC）が欠けており、QAが要件文のみからテストケースを一意に導出できない箇所がある（Major 1件）。Critical該当なし、Major実質1件（複数箇所にまたがる同種の指摘として集約）のため、READY判定とする。ただしFR-1.5を含むMust項目へのAC追加は、実装着手前に埋めておくことを強く推奨する。
