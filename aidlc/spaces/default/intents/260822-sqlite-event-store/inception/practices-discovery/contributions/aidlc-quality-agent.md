**Collaborator:** aidlc-quality-agent

## Contribution

### 1. テスト態勢（Testing Posture）の評価

草稿の `Methodology: test-after` / `#[cfg(test)]` 同居方式の踏襲は、エビデンス（`event_store_for_dynamodb_test.rs`, `event_store_for_bigtable_test.rs`, `event_store_test_support.rs`）と一致しており妥当。共有シナリオ `exercise_user_account_flow` のSQLiteバックエンドでの再利用も、バックエンド間の契約対称性を検証する最も安価な手段として支持する。

ただし共有シナリオの検証範囲を正確に記録しておくべき: `exercise_user_account_flow` は**ハッピーパスのみ**（作成→リネーム×2、スナップショット/リプレイ、seq_nr/version検証）を対象とし、以下を一切カバーしない。

- 楽観ロック競合（並行書込み→`OptimisticLockError`）— codekb既知欠落
- `persist_event` に作成イベントを渡した場合のエラー契約（TD-05: Memoryはpanic、他はErrで非対称）
- スナップショット保持動作の検証（DynamoDBテストは `with_keep_snapshot_count(Some(1))` / `with_delete_ttl` を**設定するだけで結果をassertしていない**。TD-08のBigtableサイレント無効が検出されなかったのはこのため）
- デシリアライズ失敗・境界値（`get_events_by_id_since_seq_nr` の境界）

construction フェーズガードレールは「テストファイルはハッピーパス＋最低2つのエラー/エッジケースをカバーする」ことを要求するため、**SQLiteテストは共有シナリオの再利用だけでは要件を満たせない**。草稿はこれらの欠落を「SQLite追加時に埋めるかは未確定」としているが、少なくとも次の2つはインタビュー結果に依らずSQLite Boltの受け入れ床に含めることを推奨する:

1. **楽観ロック競合テスト**（2つのストアハンドルから同一versionで書込み→片方が `OptimisticLockError`）。SQLiteはDocker不要・同一プロセス内で再現できるため、既存バックエンドで書けなかったこのテストを最も安価に書ける場所である。
2. **エラー契約テスト**。feature分割はTD-01（`OptimisticLockError` へのaws_sdk型リーク）の再設計を伴うため、新エラー型のバックエンド中立性を固定するテストが回帰防止として必須。

### 2. カバレッジ床の適用可否（草稿の前提修正）

草稿は「org.md既定の80%カバレッジ床は検証不能」と記載するが、org.md のスコープ別床の列挙（`mvp`, `enterprise`, `feature`, `infra`, `classic` に80%床）に **`library` スコープは含まれていない**。したがって本ワークフローでは「80%床を測定できるか」以前に「**80%床がそもそも適用されるか**」が未確定であり、インタビュー質問は次の3択に再構成すべき:

- A. 数値床を設ける（推奨ツール: `cargo-llvm-cov`。stableで動作しworkspace対応、tarpaulinより制約が少ない）
- B. 数値床なし、「共有シナリオ＋楽観ロック競合＋エラー契約」の主要パス網羅を受け入れ基準とする
- C. 今回は導入せず、既存スイートのグリーン維持のみ

なお Test Strategy は Standard であり、スコープ床は戦略に対して加算的（org.md）。B/C を選んでも Standard 戦略が要求するテスト種別・量は減らない点を確認事項に添えること。

### 3. CI品質ゲート — feature分割後の必須事項が質問リストから欠落

`ci.yml` の現状は `lint`（nightly rustfmt --check）→ `test-lib`（stable, `cargo test -p event-store-adapter-rs`）の2ジョブのみ。本イニシアチブの中核が**cargo feature分割**である以上、TD-11の「featureマトリクスCIは実質必須」は草稿・evidence.md の Open Questions に**明示的に載せるべき最重要ギャップ**である。理由:

- feature分割後、`cargo test`（デフォルトfeature）が何をビルド・実行するかはデフォルトfeature集合の決定に直結する。`#[cfg(test)]` 同居方式ではテストコード自体が feature ゲート下に入るため、**CIが素通しでグリーンでも非デフォルトfeature組合せがコンパイル不能**という故障モードが生まれる。
- Renovate が minor/patch を automerge する運用のため、CIが検証しないfeature組合せの破損は**自動マージで無警告に混入**する。現行の品質ゲート前提（CI green = マージ可）が崩れる。
- 最低限 `--no-default-features`、各feature単独、全feature有効の3点検証（`cargo hack --each-feature` の採用が定石）をCIに置くか、置かないなら根拠を記録すべき。

同様に、SQLiteは埋め込みDBのためテスト実行にDocker不要となり、**OSマトリクス（現状ubuntuのみ）の拡張が初めて現実的になる**。project.md の学習事項が質問対象として明示する「CIマトリクス」に該当するため、feature分割と合わせてインタビュー1問に束ねることを推奨する。

### 4. SQLiteテスト方式（testcontainers不要）の裏付け

草稿の「SQLiteはファイル/インメモリDBで軽量テスト化できる見込み」に同意し、品質観点の根拠を追加する: (a) 決定的・高速でテスト独立性を保証しやすい（テストごとに `:memory:` または一時ファイルDBを割り当てれば共有状態ゼロ）、(b) `TEST_TIME_FACTOR` のようなタイミング調整が不要、(c) 楽観ロック競合テストを同一プロセスで書ける。注意点として、既存テストにある `env::set_var("RUST_LOG", ...)` のようなプロセスグローバル変異は並列実行と相性が悪いため、SQLiteテストでは踏襲しないこと（テスト独立性の原則）。

### 5. エビデンス上の小さな矛盾（リード草稿が未記載）

- AGENTS.md はテスト関数名 `should_*` を推奨と明記するが、実在するテストは `test_event_store_on_dynamodb` / `test_event_store_on_bigtable` と `test_*` 形式であり**慣行と文書が乖離**している。SQLiteテストの命名はどちらに従うか、軽量確認項目として一覧に含めるとよい（tab幅の矛盾と同種の文書乖離）。
- AGENTS.md は `serial_test` による直列化をテスト方針として記載するが、TD-14 のとおり**宣言のみで未使用**。エビデンスとしてAGENTS.mdのテスト方針記述を引用する際は、この2点を実測優先で扱うべき。

### 6. clippy・スコープ境界の明確化

草稿はclippy未導入を事実として記載するが、「本イニシアチブでclippyを導入するか」を明示の判断項目にしていない。カバレッジ・clippy・examplesビルド検証・MSRV検証はいずれもTD-11に紐づく改善候補だが、全部を今回のスコープに抱き合わせるとスコープクリープになる。インタビューでは「今回導入するCI強化はどれか（featureマトリクスのみ最優先／+clippy／+カバレッジ／+MSRV検証）」を1問で択一させ、選外はバックログ化する形を推奨する。

## Positions
- AGREE: `Methodology: test-after` と `#[cfg(test)]` 同居方式・共有シナリオ再利用の踏襲 — エビデンスと完全に整合し、org.md既定とも矛盾しない。
- AGREE: SQLiteテストをtestcontainers不要のファイル/インメモリ方式とする方向性 — 決定性・速度・テスト独立性・楽観ロックテスト実現性のすべてで優位。
- AGREE: カバレッジ・楽観ロック・Memoryテスト欠落を推測ルール化せずインタビュー送りにした抑制 — discovered-rules.md の「推測でルールを作らない」方針は健全。
- OBJECT: 「org.md既定の80%カバレッジ床」という前提記載 — org.mdの床列挙に `library` スコープは含まれず、適用可否そのものが未確定。質問を「測定できるか」から「床を適用するか（A/B/C）」に再構成すべき。
- OBJECT: featureマトリクスCIの決定が Open Questions に不在 — feature分割＋Renovate automerge環境では非検証feature組合せの破損が無警告で混入するため、本イニシアチブ最重要のテスト系質問として明示が必要。
- OBJECT: SQLiteテストの受け入れ床が全面「未確定」扱い — constructionガードレール（ハッピーパス＋エラー/エッジ2件以上）とTD-01再設計の回帰防止から、楽観ロック競合テストとエラー契約テストはインタビュー結果に依らず床に含めるべき。
- OBJECT: AGENTS.mdのテスト方針記述（`should_*` 命名・`serial_test` 直列化）と実測の乖離が未記録 — tab幅矛盾と同様、実測優先で確認項目に載せるべき。
