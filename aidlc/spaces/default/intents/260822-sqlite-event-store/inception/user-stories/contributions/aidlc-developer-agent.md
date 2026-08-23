**Collaborator:** aidlc-developer-agent

## Contribution

実装可能性・ストーリーサイズ・依存関係の観点でドラフト（`personas.md` / `stories.md`）を検分した。結論: スライス自体は実装単位として妥当でサイズも全て1〜5日に収まるが、**依存関係の記述に実装順序と矛盾する箇所が3点**あり、AC 1件に検証手順の明確化が必要。

### 1. サイズ評価（全ストーリー 1〜5日に収まる）

| ストーリー | 見積り | 備考 |
|---|---|---|
| US1.1 | 3〜5日 | 最大。`StorageBackend` 5メソッド実装＋スキーマ自動作成＋ファサード＋ビルダー。同期ドライバ（rusqlite想定）をasyncトレイト（`#[async_trait]`）に載せるアダプタ作業（`spawn_blocking` 等）が含まれる点は見積りに織り込むべき隠れ作業 |
| US1.2 / US1.3 / US1.4 | 各1〜2日 | 適正 |
| US2.1 | 3〜4日 | `Cargo.toml` のoptional化＋`lib.rs` の `#[cfg(feature)]` ガード＋依存削除。現状 `lib/Cargo.toml` に `[features]` セクションは存在せず全依存が無条件（実測確認済み）。**隠れ作業**: `examples/user-account/Cargo.toml` は `event-store-adapter-rs = { path = "../../lib" }` とfeature指定なしで依存しており、デフォルトfeature廃止後は `features = ["dynamodb"]` 追随が必要（test-utilsも同様）。どのACにも現れないがUS2.1に含まれる作業として明記を推奨 |
| US2.2 / US2.3 / US3.1〜US3.3 / US4.x | 各1〜2日 | 適正 |

### 2. 依存関係の矛盾（要修正）

**(a) US1.1「依存: なし」とUS2.1「feature分割はSQLite実装に先行」が両立しない。** 両ストーリーがともに「依存: なし」を主張しているが、US2.1の注記どおりfeature分割が先行するなら、US1.1は `依存: US2.1` と書くべき（sqliteモジュールを最初からfeatureゲート下に置くため）。逆にウォーキングスケルトン（team.md: Bolt 1 = SQLite薄いE2E）を先に走らせるなら、US2.1のACのうちsqlite featureを参照する部分（後述(c)）が先に検証不能になる。順序をどちらかに確定し、依存欄と「ストーリー間の依存関係（要約）」を一致させること。

**(b) US1.2とUS3.2にUS2.2（エラー型中立化）への依存が欠落している。** 実測: `lib/src/types.rs:111` は現在 `OptimisticLockError(#[from] TransactionCanceledExceptionWrapper)` で、AWS SDK型（`TransactionCanceledException`）を直接抱えている。US2.2完了前にUS1.2（AC1.2.1: `OptimisticLockError` 返却）を実装すると、SQLiteバックエンドはBigtable/Memoryと同じく `TransactionCanceledExceptionWrapper(None)` を構築するしかなく、これはproject.md Forbidden（SDK型リークの新規複製禁止）に抵触する。US1.2の依存欄に `US2.2` を、US3.2（AC3.2.3 エラー契約テスト）の依存欄にも `US2.2` を追加すべき。

**(c) US2.2「依存: US2.1」は依存方向が逆（少なくとも一部）。** AC2.1.1（`--features sqlite` ビルドで `cargo tree` にaws依存が現れない）とAC2.1.2（`--no-default-features` でビルド成功）は、`types.rs` が無条件に `use aws_sdk_dynamodb::...` している現状ではエラー型再設計（US2.2）完了後に初めてパスできる。つまりUS2.2はUS2.1のACの前提条件であり、後続ではない。ドラフトの「feature分割の一部として実施」という注記は実態を捉えているので、依存欄を「US2.1と同一Boltで一体実施（AC2.1.1/AC2.1.2はUS2.2完了が検証前提）」と明示する修正で足りる。

### 3. AC個別の実装可能性

**AC1.2.1（並行更新テスト）— 検証可能だが手順の明確化を推奨。** `persist_event(&mut self, ...)` かつ `EventStore: Clone`（`types.rs:46,63` 実測）なので、「2つの並行更新」は2スレッドの実時間競走ではなく、**同一DBを共有する2つのストアハンドル（クローンまたは同一ファイルを指す2インスタンス）を同一versionで準備し、順次コミットして後発が `OptimisticLockError` を受け取る決定的テスト**として構成できる。これはNFR-5（決定的・並列安全）とも整合する。ただし前提が1つある: `EventStoreForSqlite` の `Clone` は基底接続を共有（`Arc` 等）しなければならない。Memoryバックエンドのように `Clone` で状態が分岐する実装だとこのテストは書けず、`:memory:` モード（AC1.3.1）でも別接続=別DBとなり破綻する。「クローン間で同一ストレージを共有する」ことをUS1.1またはUS1.2の実装ノートとして一言加えることを推奨（ACの修正までは不要）。

**AC1.4.2（TTL削除）— トリガーが未指定。** 「保守処理が動く」がいつ動くのか（既存設計では書込み成功後の `on_event_persisted` フック）が書かれていない。SQLiteにはDBネイティブTTLがないためライブラリ駆動の削除になる。決定的にテストするには `delete_ttl` を0または極小に設定する構成も併記すると良い。Couldなので軽微。

**AC2.3.1（システムSQLiteなし環境でのビルド）— 検証環境の注記が必要。** macOS/通常のLinuxにはシステムSQLiteが常在するため「なし」の環境はCIの最小コンテナでしか再現できない。代替として「バンドルfeatureビルドの成果物がシステムSQLiteに動的リンクしていないこと（`otool -L` / `ldd`）」の確認でも同値。検証手段を一言添えると実行可能性が上がる。

**AC2.2.2（4バックエンド同一エラー表現）— 検証可能。** 型レベル（単一enumからSDK型を除去）でコンパイル時に大半が保証され、DynamoDB/Bigtableの実行時検証は既存のtestcontainersスイートに乗る。問題なし。

**AC1.1.3 / AC3.1.2 / AC3.1.3（内部構造検査系AC）**: ユーザー可観測でないホワイトボックスACだが、レイヤ境界規約（Mandated）の固定化として妥当。実装レビュー＋grepで検証可能。

### 4. その他

- US3.1（Memory準拠化）の「依存: US2.1」は実装上は独立に着手可能（featureと無関係のリファクタ）。並行実施可能な旨を注記するとBolt計画の自由度が上がるが、現状のままでも害はない。
- feature名・型名（`sqlite` / `EventStoreForSqlite`）、`StorageBackend`+`GenericEventStore` 経由、unsafe impl非複製、panic禁止は、いずれもteam.md/project.mdの確定事項と一致しており矛盾なし。

## Positions

AGREE: 機能領域別のスライスとMoSCoW継承は実装単位として妥当。全ストーリーが1〜5日に収まり、Could（US1.4）の切り離しも適切。
AGREE: AC1.2.1は `&mut self` APIの下でも「同一DBを共有する2ハンドルの順次コミット」として決定的に検証可能であり、AC文面の変更までは不要（実装ノート追記のみ推奨）。
OBJECT: US1.1「依存: なし」とUS2.1注記「feature分割はSQLite実装に先行」が矛盾。順序を確定し依存欄・依存関係要約を一致させること（上記2(a)）。
OBJECT: US1.2とUS3.2の依存欄にUS2.2（エラー型中立化）が欠落。現行 `types.rs` のAWS型リークのため、US2.2前のUS1.2実装はproject.md Forbiddenに抵触する経路しかない（上記2(b)）。
OBJECT: US2.2「依存: US2.1」は検証順序として逆。AC2.1.1/AC2.1.2はUS2.2完了が前提のため、「同一Boltで一体実施」と明示すべき（上記2(c)）。
