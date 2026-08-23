# セキュリティ設計 — u2-sqlite-backend (security-design)

セキュリティ要件（`../nfr-requirements/security-requirements.md` NFR-2.3/2.4・NFR-4.5〜4.8・NFR-5.1）・技術スタック決定（`../nfr-requirements/tech-stack-decisions.md` D1〜D7）・機能仕様（`../functional-design/functional-spec.md` ワークフロー1〜5）・契約（`../../../inception/contract-design/contract-summary.md` C-2/C-3）を、U2（SQLiteバックエンド本体）の具体設計へ落とす。性能系4設計書はキンド適用で対象外（U1と同一の整理）。

## 依存供給網の設計（NFR-2.3 / NFR-2.4）

- **依存宣言**: `[workspace.dependencies]` に `rusqlite = { version = "0.40.2", default-features = false }`（D1 — デフォルトfeature `cache` の `hashlink` 混入を遮断し「rusqlite 1クレートのみ」を厳守）
- **feature実体化**: `sqlite = ["dep:rusqlite", "rusqlite/bundled"]`（バンドル既定・自己完結）／`sqlite-system = ["dep:rusqlite"]`（システムリンク — 単独指定が正規。併用時はbundled優先という帰結をU4文書へ明記）。cfgゲートは `any(feature = "sqlite", feature = "sqlite-system")`（D2/D3）
- **供給網の追随**: Renovate自動更新＋RUSTSEC照合（cargo-audit / cargo-deny — U3のCI日次）で bundled SQLite本体のCVEを検出し、rusqlite / libsqlite3-sys の更新で取り込む（Q2=A確定）
- **検証**: `cargo tree -p event-store-adapter-rs -e normal --features sqlite` で rusqlite / libsqlite3-sys 以外の新規ランタイム依存（hashlink等）が現れないこと

## panic排除とエラー写像設計（NFR-4.5 / NFR-4.7）

- **写像点の一元化**: rusqlite のエラーを中立エラー型へ写像する変換ヘルパーをバックエンド内に置き、全経路（接続確立・スキーマ作成・トランザクション・読み出し）で経由する。panicは書かない（Mutexポイズン含む — U1のMemory準拠化と同型のエラー写像）
- **写像決定表（U1決定表のSQLite具体化）**:
  - 現行スロット挿入の一意制約違反（SQLITE_CONSTRAINT系） → `OptimisticLockError`（BR1.2書式 — 作成の重複）
  - CAS更新の影響行0 → 実version読取のうえ `OptimisticLockError`（`, actual_version=<m>` 付加）
  - 直列化・復元失敗 → `SerializationError`
  - I/O・接続・ロック競合（SQLITE_BUSY等） → `IOError`（サポート境界の帰結 — 下記）
  - その他 → `OtherError`
- **メッセージ衛生（NFR-4.7）**: `OptimisticLockError` の文字列はBR1.2書式のみ。DBファイルパス・rusqlite生エラー文字列は `IOError` / `OtherError` のソースエラー側に保持し、`OptimisticLockError` へ混入させない

## スレッド安全設計とサポート境界（NFR-4.6 / Q1確定）

- **同期・隠蔽**: `Arc<Mutex<Connection>>` はバックエンド構造体の非公開フィールド（ロック型・ガード・Arcを公開シグネチャへ出さない — U1確立の隠蔽境界と同型）。各 `StorageBackend` メソッド内でロック取得→同期実行→解放し、ガード保持中に `.await` しない。`Send + Sync` は自動導出（手書き `unsafe impl` 禁止 — NFR-4.6、grep検証）
- **サポート境界（ユーザー確定 — Q1）**: **同一ファイルDBを複数のストアインスタンス／複数プロセスで同時に開く使い方はサポート外**。PRAGMA調整（busy_timeout / WAL）は行わず、ロック競合（SQLITE_BUSY）は既定どおり即時エラーとして `IOError` 系へ写像する。CLI多重起動の防止等はアプリケーション側の責務。サポートされる共有単位は「1つのストアインスタンスとその `Clone`」（基底接続共有 — BR2.5）のみ。この境界はU4のドキュメントに明記する
- **テスト環境衛生（NFR-5.1）**: テストは `:memory:` または一時ディレクトリ配下の一意名ファイルで完結（Docker不要・決定的・並列安全）。プロセスグローバル変異なし。ファイルDBテストは自テストで作成したファイルのみ削除する

## 機密情報の取り扱い（NFR-4.8）

- 実装・テスト・examplesに資格情報・APIキー・秘密のリテラルを置かない（grepで検証）。DBパスは機密ではないが、エラーメッセージ衛生（NFR-4.7）の範囲で扱う

## 検証手順（U2時点 — 手動ローカル。CI恒久化はU3）

```bash
cargo tree -p event-store-adapter-rs -e normal --no-default-features --features sqlite | grep -E "hashlink|aws-|tonic|googleapis" && echo NG || echo OK  # NFR-2.3
cargo build -p event-store-adapter-rs --no-default-features --features sqlite          # NFR-3.2 バンドル
cargo build -p event-store-adapter-rs --no-default-features --features sqlite-system   # NFR-3.2 システムリンク
grep -rn "unsafe impl" lib/src/ && echo NG || echo OK                                  # NFR-4.6
cargo test -p event-store-adapter-rs --no-default-features --features sqlite test_event_store_on_sqlite  # NFR-5.1（Docker不要で完結）
```

## Assumptions & Open Questions

None.

## Review

**Verdict:** READY
**Reviewer:** aidlc-architecture-reviewer-agent
**Date:** 2026-08-23T04:32:07Z
**Iteration:** 1

### Findings

新規Critical/Major/Minor所見なし。

### Validation Tool Results

| Tool/確認 | 結果 | 解釈 |
|---|---|---|
| Q1確定事項（同一ファイルDB複数同時オープンはサポート外・PRAGMA調整なし・SQLITE_BUSY→IOError・多重起動防止はアプリ責務・U4明記）とsecurity-design.md/logical-components.mdの反映突合 | PASS | 「スレッド安全設計とサポート境界」節（26行目）と「障害ドメインとブラスト半径」節（19行目）の両方に、ユーザーverbatim指示の趣旨（サポート外・PRAGMA非調整・IOError写像・アプリ責務・U4文書化）が過不足なく、かつ一貫して反映されている。 |
| `rusqlite::ErrorCode`の実際のバリアント確認（docs.rs `rusqlite/0.40.2/rusqlite/enum.ErrorCode.html`） | PASS | `ConstraintViolation`（"Abort due to constraint violation" — UNIQUE/PRIMARY KEY違反を含む）、`DatabaseBusy`、`DatabaseLocked`が実際に存在することを確認した。「現行スロット挿入の一意制約違反（SQLITE_CONSTRAINT系）→OptimisticLockError」「ロック競合（SQLITE_BUSY等）→IOError」という設計の写像決定表は、`rusqlite::Error::SqliteFailure`の`sqlite_error_code()`が返す実際のErrorCode体系と技術的に整合する。 |
| tech-stack-decisions.md D1〜D3（前段階iteration 2でREADY確定済み — `default-features = false`・feature設計）との整合確認 | PASS | security-design.mdの「依存供給網の設計」節は、D1の`rusqlite = { version = "0.40.2", default-features = false }`、D2/D3のfeature構成・bundled優先の帰結を過不足なく引き継いでいる。検証コマンド（36行目）の`grep`パターンにも前段階で問題となった`hashlink`が明示的に含まれている。 |
| U1側 `construction/u1-backend-features/nfr-design/security-design.md`（隠蔽境界・エラー写像決定表）との同型性確認 | PASS | 「写像点の一元化」「写像決定表」「メッセージ衛生」という節構成、および`Arc<Mutex<T>>`非公開・ガード保持中`.await`禁止・`Send+Sync`自動導出という設計パターンは、U1のMemoryバックエンド設計と構造・語彙レベルで同型であることを確認した。 |
| traceability.json の9 NFR ID列挙とnfr-requirements/security-requirements.md・tech-stack-decisions.mdとの突合 | PASS | NFR-1.3, 2.3, 2.4, 3.2, 4.5〜4.8, 5.1の9件が過不足なく列挙され、全件`status: OK`。target記述はいずれもsecurity-design.md/logical-components.md内の実在する節を正確に指している。 |
| `python3 -c "import json; json.load(...)"` によるtraceability.jsonのパース検証 | PASS | 構文エラーなし。 |
| rules.md BR2.x引用の実在性確認（BR2.5, BR2.3, BR2.4） | PASS | security-design.md/logical-components.mdが引用するBR2.5（Clone/インスタンス単位共有）・BR2.3（作成一意性）・BR2.4（CAS）はいずれも実在し内容が一致する。 |

### Summary

Q1（同一ファイルDB複数同時オープンのサポート境界）の反映は正確かつ一貫しており、エラー写像決定表の技術的正しさもdocs.rs上の実際のrusqlite ErrorCode体系との突合で確認できました。上流のtech-stack-decisions.md D1〜D3（`default-features = false`を含む、直前のiteration 2でREADY確定済み）との整合も取れており、前段階で問題となった`hashlink`の混入防止が検証コマンドにも正しく反映されています。U1のMemoryバックエンド設計との同型性も構造・語彙の両面で確認できました。Critical 0件・Major 0件・Minor 0件のためREADYと判定します。
