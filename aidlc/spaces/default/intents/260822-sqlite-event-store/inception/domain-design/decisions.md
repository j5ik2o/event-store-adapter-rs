# アーキテクチャ決定記録（ADR）: SQLite対応EventStoreとバックエンドfeature分割

コンポーネントカタログ（`components.md`）の設計判断の記録。出典は要件定義書（`../requirements-analysis/requirements.md`）、ストーリー（`../user-stories/stories.md`）、コード知識ベース（`aidlc/spaces/default/codekb/sqlite/architecture.md` / `component-inventory.md`）、チームプラクティス（`../practices-discovery/team-practices.md`）、本工程Q&A。

## ADR-001: 楽観的ロックエラーを軽量コンテキスト付きの中立バリアントへ再設計する

- **Context** — 共通エラー型 `EventStoreWriteError::OptimisticLockError` が AWS SDK の `TransactionCanceledException` を直接ラップしており（TD-01）、feature分割（FR-2.1/2.2）の最大の障害。Memory / Bigtable までAWS型を返す歪みがある。破壊的変更は許容済み（リリース方針）。
- **Decision** — `OptimisticLockError` を集約ID・期待バージョン等の文字列コンテキストを保持するバックエンド中立バリアントに再設計する（Q1=B）。`TransactionCanceledExceptionWrapper` は公開型から除去する。
- **Consequences** — (+) 全バックエンドが同一のエラー契約を持ち、sqliteのみの利用者がAWS型に触れない（US2.2）。(+) デバッグ時に競合の文脈が得られる。(−) 公開enumの破壊的変更（CHANGELOG記載が必須 — FR-6.4）。(−) DynamoDBバックエンドはSDK例外から文字列コンテキストへの写像コードが必要。
- **Alternatives Rejected** — (a) ユニットバリアント化（情報なし）: 最も単純だがデバッグ情報が失われる。(b) AWS型をdynamodb featureゲート下に保持: 共通型がfeatureで形を変えることになりAPIの一貫性が壊れるため棄却。

## ADR-002: SQLiteドライバとして rusqlite を採用する

- **Context** — 制約はドライバ1クレートのみ・ORM不可（NFR-2）・バンドル/システム両対応（FR-2.5）。既存APIはasync（async_trait）で、SQLiteアクセスの非同期整合が必要（feasibility A1）。
- **Decision** — rusqlite を唯一の追加依存として採用する（Q2=A）。同期呼び出しは非同期文脈から分離実行（spawn_blocking 相当）で包む。バンドル/システムのリンク方式は rusqlite（libsqlite3-sys）のfeatureを本クレートのfeatureとして再エクスポートする形で提供する。
- **Consequences** — (+) 依存最小・成熟したbundled対応・制約C1/C2を直接充足。(+) SQLiteの単一ライタ特性と同期APIの相性が良い。(−) 呼び出しごとのブロッキング分離のオーバーヘッド（CLI用途では実用上問題にならない見込み — R1受容と整合)。(−) 純asyncドライバに比べ接続の並行制御を自前で設計する必要（機能設計で確定）。
- **Alternatives Rejected** — (a) sqlx: 純asyncだが依存ツリーが大きく依存最小制約と衝突。(b) 選定を後続工程に先送り: リスク先行方針（ドライバ検証を早期に）と矛盾するため棄却。

## ADR-003: SQLiteの楽観的ロックは単一トランザクションの原子的CASで実装する

- **Context** — 既存の参照実装はDynamoDBのTransactWriteItems（原子的CAS）。BigtableはRead→Writeの2段階で非原子（TD-07）というアンチ前例がある。FR-1.3は原子性を要求。
- **Decision** — バージョン検証・イベント追記・スナップショット更新を単一のSQLiteトランザクション内で行い、検証失敗時はロールバックして `OptimisticLockError` を返す。
- **Consequences** — (+) DynamoDB契約との対称性（US3.1）。(+) 競合パステスト（FR-4.2）が決定的に書ける。(−) トランザクション設計（IMMEDIATE等の開始モード選択）は機能設計で詰める必要がある。
- **Alternatives Rejected** — Bigtable式のRead→Write 2段階: レースウィンドウがあり、要件（FR-1.3）とチームプラクティス（アンチ前例の非踏襲）に反するため棄却。

## ADR-004: モジュール構成は既存のフラット構成を踏襲する

- **Context** — feature分割に伴いモジュール再編（backends/ディレクトリ化）も可能だった。チームプラクティスはファイル命名パターン（`event_store_for_<backend>.rs` / `_test.rs`）を確定済み。
- **Decision** — フラットな `lib/src/*.rs` を維持し、`event_store_for_sqlite.rs` / `event_store_for_sqlite_test.rs` を追加する（Q3=A）。feature ゲートはモジュール宣言と再エクスポートの `#[cfg(feature)]` で行う。
- **Consequences** — (+) 差分最小でレビューが容易、既存の命名パターンと完全整合。(−) バックエンドが増え続けた場合の将来の再編余地は残る（本イニシアチブでは対象外）。
- **Alternatives Rejected** — ディレクトリ再編: 整理は進むが今回の差分が不必要に膨らみ、破壊的変更の主眼（feature化・エラー型）から焦点がずれるため棄却。

## ADR-005: Memoryバックエンドを StorageBackend + GenericEventStore 準拠へリファクタする

- **Context** — Memory だけが内部抽象に乗らず `EventStore` を直接実装し、作成イベントで panic する非対称（TD-05）。要件Q&AでA（準拠化）が確定（FR-3.1）。
- **Decision** — Memory を StorageBackend 実装＋GenericEventStore 委譲に載せ替え、panic を Err 返却へ修正。公開ファサード `EventStoreForMemory::new()` のAPIは維持。手書きの `unsafe impl Send/Sync` は除去。
- **Consequences** — (+) 4バックエンドの挙動契約が統一され、共有テストシナリオに全バックエンドを乗せられる。(+) `:memory:` SQLiteとの役割分担が明確になる（Memoryは依存ゼロの常時有効、SQLiteは永続化あり）。(−) Memory の内部実装は全面書き換えとなる（公開挙動の互換テストで保護）。
- **Alternatives Rejected** — (a) panicのErr化のみ（最小修正）: 非対称が残り、共有シナリオへの統一搭載が困難。(b) 現状維持: 挙動契約の非対称が新規バックエンド追加後も残るため棄却。

## Assumptions & Open Questions

None.
