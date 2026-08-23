**Collaborator:** aidlc-developer-agent

## Contribution

開発者視点（命名・レイヤ境界・エラー処理・ファイル構成・コードスタイル）で一次ソース（`rustfmt.toml`、`AGENTS.md`、`lib/src/lib.rs`、`lib/src/types.rs`、`lib/src/event_store_backend.rs`、codekb の `code-structure.md` / `architecture.md`）をリード草案と照合した結果を示す。

### 1. レイヤ境界規約が Code Style に未記載（最重要の追記提案）

- エビデンス: `event_store_for_dynamodb.rs` / `event_store_for_bigtable.rs` は private な `StorageBackend`（5メソッド）実装と `GenericEventStore` への委譲で `EventStore` を提供し、直接実装は `event_store_for_memory.rs`（旧構造、codekb TD-05）のみ。codekb `architecture.md` も「SQLite 追加は StorageBackend 5メソッド実装 + GenericEventStore ラップ + 公開ファサード」を明記している。
- 提案: team-practices.md の Code Style（モジュール構成の項）に「新バックエンドは `StorageBackend` + `GenericEventStore` 経由で実装し、`EventStore` を直接実装しない（Memory は既知のレガシー例外）」を追記する。2/3 バックエンドの準拠と codekb の移行方針という明確なエビデンスがあるため、affirmation ゲートで `ALWAYS` ルール候補として提示する価値がある。本イニシアチブで最も実装を拘束する慣行であり、これが practices に無いと「既存慣行に従う」の解釈が Memory 方式（直接実装）にも開いてしまう。

### 2. エラー処理規約が未記載

- エビデンス: 公開・内部契約はすべて `Result<_, EventStoreWriteError | EventStoreReadError>`（`types.rs`、`thiserror` の enum）。既知の逸脱が2点ある。(a) `OptimisticLockError(#[from] TransactionCanceledExceptionWrapper)` が `aws_sdk_dynamodb` 型を公開契約にリークしている（TD-01、feature 分割の最重要障害）。(b) Memory バックエンドは Err ではなく panic! を使い契約が非対称（TD-05）。
- 提案: 「バックエンド内部のエラーは panic させず `EventStoreReadError` / `EventStoreWriteError` へ写像する」「SQLite の楽観ロック検出はバックエンド中立なエラー表現で返し、AWS 型ラップの形式を踏襲しない」を実務慣行として記載する。TD-01 自体の再設計は破壊的変更になり得るため設計ステージの決定事項だが、「新規コードで AWS 型リークのパターンを模倣しない」は practices として今から効かせられる。Construction フェーズのガードレール（統合境界のエラー処理必須）に対する判断材料がこのセクションに無いと code-generation で判断がぶれる。

### 3. 命名規約の記述精度と SQLite 固有の命名決定

- 草案の「公開APIは `CamelCase`、内部関数・テストは `snake_case`」は AGENTS.md の転記だが不正確。公開APIでも関数は snake_case（`persist_event`、`get_latest_snapshot_by_id`）であり、正しい軸は Rust 標準（RFC 430）どおり「型・トレイト = UpperCamelCase、関数・モジュール = snake_case」。公開/内部の軸ではなく型/関数の軸で書き直すことを推奨。
- 既存の型名は Rust API Guidelines（頭字語を1語扱い: `DynamoDb`）ではなくブランド表記を優先している（`EventStoreForDynamoDB`）。SQLite ではブランド準拠なら `EventStoreForSQLite`、ガイドライン準拠なら `EventStoreForSqlite`（codekb の記載はこちら）に分岐する。code-generation 前に裁定が必要な未確定項目としてインタビュー項目への追加を推奨。
- ファイル命名は `event_store_for_<backend>.rs` + `event_store_for_<backend>_test.rs`（`#[cfg(test)]` 同居）が確立パターンで、SQLite は `event_store_for_sqlite.rs` / `event_store_for_sqlite_test.rs` に機械的に決まる。これは確定慣行として記載してよい。

### 4. feature 分割に関する規約が不在

- 本イニシアチブの中核が cargo feature 分割であるにもかかわらず、feature 命名（`dynamodb` / `bigtable` / `memory` / `sqlite` の小文字）、`lib.rs` のグロブ再エクスポート（`pub use event_store_for_*::*;`）への `#[cfg(feature = ...)]` ガード方針、default features の扱いが practices に現れていない。現状 `lib.rs` の `#[allow(dead_code)]`（dynamodb / bigtable モジュール宣言に付与）は feature ゲート不在の代償であり、feature 化とともに除去すべき「踏襲しない慣行」として明示することを推奨。
- CI の feature マトリクス欠落は草案 Testing Posture の未確定項目と接続する。feature 分割後は `--no-default-features` / 各 feature 単独 / all-features のビルド検証が無いと「コンパイルが通らない feature 組合せ」を検出できないため、インタビュー項目（CI 拡充の要否）への追加を推奨。

### 5. 踏襲すべきでない既存パターンの明示

- 3バックエンドすべてに手書きの `unsafe impl Send/Sync` がある（TD-06、不要の可能性大）。「既存慣行に従う」と一括で書くと SQLite に複製されるリスクがあるため、「新規バックエンドでは自動導出に任せ、手書きの unsafe impl を追加しない」を明記することを推奨。
- 一方、`#[async_trait]` の全 async トレイトでの使用、型パラメータ規約 `<AID: AggregateId, A: Aggregate<ID = AID>, E: Event<AggregateID = AID>>`、`new(...)` + `with_*` の self 消費型ビルダー構成は踏襲対象の確定慣行として記載を推奨（現草案には現れていない）。

### 6. Code Style セクションの細部

- 草案の rustfmt 列挙は `indent_style = "Block"` と `normalize_comments = true` を欠く。部分列挙はドリフトの温床のため、「`rustfmt.toml`（全9キー）を正とする」と参照形式にし、代表値（`max_width = 120`、`tab_spaces = 2`）のみ例示する形を推奨。
- doc コメント言語: `types.rs` は日本語主体・一部英日併記。crates.io / docs.rs 公開クレートとしてどちらに寄せるかは SQLite 追加分の doc 執筆で即座に問題になるため、未確定項目（インタビュー）への追加を推奨。AGENTS.md の「doc comment は三人称現在形で開始」は記載済み慣行として拾ってよい。

## Positions
- AGREE: インデント矛盾の裁定（実測値 `tab_spaces = 2` を優先し、AGENTS.md「4 space」は未解消矛盾としてインタビュー送り） — CI が強制するのは `rustfmt.toml` であり AGENTS.md の散文には強制力が無いため。
- AGREE: test-after / `#[cfg(test)]` 同居 / 共有シナリオ `exercise_user_account_flow` 再利用の記録 — `code-structure.md` と実ファイル構成に完全に整合するため。
- AGREE: discovered-rules.md の「推測でルールを作らない」抑制方針 — fmt ゲートと Conventional Commits はワークフロー実装で裏付けられた高信頼ルールのみで妥当なため。
- OBJECT: レイヤ境界規約（`StorageBackend` + `GenericEventStore` 経由、`EventStore` 直接実装禁止）の欠落 — 本イニシアチブで最も実装を拘束する evidence-backed 慣行が Code Style に現れていないため。
- OBJECT: エラー処理規約の欠落 — thiserror enum への写像・panic 禁止・AWS 型リーク非踏襲という判断材料が無いと code-generation で契約非対称（TD-01/TD-05）を再生産しかねないため。
- OBJECT: feature 命名・`#[cfg(feature)]` ゲート方針の欠落 — feature 分割はスコープの中核であり、命名と default features の方針は practices レベルで固定可能なため。
- OBJECT: 命名規約の記述が不正確（「公開API = CamelCase」） — 正しくは型/関数の軸であり、加えて `EventStoreForSQLite` と `EventStoreForSqlite` の裁定をインタビュー項目に追加すべきため。
