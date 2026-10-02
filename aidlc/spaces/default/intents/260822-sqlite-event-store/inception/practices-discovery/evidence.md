# Evidence — practices-discovery

対象コミット: `e3a6ba8c2225ae27d2f3b1b204476e1182580942`（ブランチ `sqlite`）

## Sources

### Git履歴
- コマンド: `git log --oneline -30`, `git branch -a`, `git log --merges -10`,
  `git log --format='%H %P' -20`
- 観察: 直近30コミットは `Merge pull request #NNN from j5ik2o/...` 形式の
  マージコミットと `version up to vX.Y.Z` の自動コミット、
  `chore(deps): Update Rust crate ...` のRenovate自動コミットで構成される。
  マージコミットはすべて親2つ（`git log --format='%H %P'` で確認）— squash
  ではなく通常のマージコミット。リモートブランチに未マージのfeature/patch
  ブランチが複数残存（`feature/fix-cargo-toml`, `j5ik2o-patch-1〜3` 等）。
- 推論: マージ方式は「マージコミット」であり、org.md既定の「squash-merge」
  とは異なる実運用。人間インタビュー（Q1）で継続を確認済み。

### CI設定 `.github/workflows/ci.yml`
- 観察: `lint`（nightly rustfmt fmt --check）→ `test-lib`（stable、
  `cargo test --verbose -p event-store-adapter-rs`）の2ジョブ構成。
  push/PR（`main`向け）と毎日0時のcronで起動。clippy・カバレッジ・
  feature マトリクス・examplesビルド検証はいずれも存在しない。
- 推論: 品質ゲートはフォーマットチェック＋テスト実行のみ。clippy未導入は
  Q6でCI導入が確定した。

### `lib-release.yml` / `lib-bump-version.yml`
- 観察: `lib-release.yml` は `v[0-9]+.[0-9]+.[0-9]+` タグpushで
  `cargo publish` を実行。`lib-bump-version.yml` は毎日cron
  （および手動 `workflow_dispatch`）で走り、前回タグ以降の
  Conventional Commits を解析してsemverレベルを判定、
  `cargo set-version` でバージョンを書き換えてcommit・push・タグ付け・
  GitHub Release作成まで自動で行う。人間の承認ステップはワークフロー内に
  存在しない。
- 推論: 「マージ→リリース」は完全自動化されている。Q5で維持が確定した。

### `openai-review.yml` / `renovate.json`
- 観察: PRに対してLLM（coderabbitai/openai-pr-reviewer）自動レビューが
  動く（renovateラベル付きPRは除外）。Renovateはminor/patch/pin/digestと
  devDependenciesを自動マージする設定（`platformAutomerge: true`,
  `prConcurrentLimit: 5`）。
- 推論: 依存更新は高度に自動化されている。devsecops検分により
  RUSTSEC照合ゲートの不在が指摘され、Q8で依存監査追加が確定した。

### コードスタイル設定
- `rustfmt.toml`: `max_width=120`, `tab_spaces=2`, `newline_style=Unix`,
  `brace_style=PreferSameLine`, `indent_style=Block`,
  `normalize_comments=true`, `reorder_imports/impl_items/modules=true`。
- `Makefile.toml`: cargo-makeタスクは `fmt`（nightly rustfmt起動）のみ。
  lint/testタスクは定義されていない。
- `AGENTS.md`（日本語のリポジトリガイドライン）: モジュール構成
  （`lib/src` 配下の責務別ファイル分割）、命名規約（テスト関数
  `should_*` 推奨、公開API=CamelCaseと記載）、テスト方針
  （`serial_test`による直列化、`testcontainers`による外部サービス統合
  テスト、Docker前提）、コミット/PR方針（Conventional Commits互換、
  `Closes #123`形式でのIssueリンク）を明記。
- 推論: フォーマッタ・命名規約はAGENTS.mdとrustfmt.tomlで明文化されており
  信頼度が高い。ただし以下2点の文書/実測乖離を確認し、実測を優先した:
  (1) AGENTS.mdの「4 space インデント」記述は`rustfmt.toml`実測値の
  `tab_spaces=2`と矛盾。(2) AGENTS.mdの「公開APIはCamelCase」は不正確で、
  正しい軸は型/関数の軸（developer検分の指摘）。(3) AGENTS.mdの
  テスト関数名`should_*`推奨は実在テスト（`test_*`形式）と乖離
  （quality検分の指摘）。(4) AGENTS.mdの`serial_test`直列化記載は
  TD-14のとおり宣言のみで未使用。

### リバースエンジニアリングKB（consumed）
- `aidlc/spaces/default/codekb/sqlite/code-structure.md`:
  ワークスペース構成、`lib/src` 配下のファイル一覧、`#[cfg(test)]`
  同居方式のテストファイル構成を確認。
- `aidlc/spaces/default/codekb/sqlite/technology-stack.md`:
  edition 2021、MSRV宣言なし、clippy未導入、cargo-makeはfmtタスクのみ、
  依存はfeatureゲートなく無条件依存という現状を確認。
- `aidlc/spaces/default/codekb/sqlite/dependencies.md`:
  MSRV未宣言×Renovate automergeの組み合わせリスク、`Cargo.lock`未コミット
  という実運用を確認（devsecops検分がRUSTSEC照合ゲート不在と合わせて指摘）。
- `aidlc/spaces/default/codekb/sqlite/code-quality-assessment.md`:
  テスト構成の詳細（DynamoDB/Bigtable各1本の統合テスト、
  `event_store_test_support.rs`共有シナリオ）、カバレッジ計測なし、
  Memoryバックエンド専用テスト・楽観ロック競合パステストの欠落、
  clippy未導入、CI構成の一覧、TD-01（AWS型リーク）、TD-05（Memory panic）、
  TD-06（手書きunsafe impl）、TD-11（CI網羅不足）、TD-12
  （CODEOWNERSタイポ）、TD-14（serial_test未使用）を確認。
- `aidlc/spaces/default/codekb/sqlite/architecture.md`:
  「SQLite 追加は StorageBackend 5メソッド実装 + GenericEventStore
  ラップ + 公開ファサード」という移行方針を確認（developer検分が
  レイヤ境界規約の欠落を指摘する根拠）。
- `aidlc/spaces/default/codekb/sqlite/business-overview.md`:
  イベントストアという性質・crates.io公開ライブラリという文脈を確認
  （devsecopsの「N/A評価」やdeployment読み替えの前提）。
- 推論: これらは静的解析ベースの高信頼度エビデンスとして
  team-practices.mdのTesting Posture / Code Styleセクションに直接反映した。

### 既存の team.md
- 初回practices-discovery実行時点では空のテンプレートであり、上書きすべき
  既存の affirmed practices は存在しなかった。

## 支援エージェントの検分（contributions/）

### aidlc-quality-agent
- AGREE: `Methodology: test-after`・`#[cfg(test)]`同居・共有シナリオ再利用
  の踏襲、SQLiteのtestcontainers不要方針。
- OBJECT（採用）: 「org.md既定の80%カバレッジ床」という前提は誤り
  （`library`スコープは床の適用対象外）— Q3を「測定できるか」ではなく
  「床を適用するか」に再構成した上で提示し、Aで確定。
- OBJECT（採用）: 楽観的ロック競合テストとエラー契約テストは、
  constructionフェーズガードレール（ハッピーパス＋エラー/エッジ2件以上）
  とTD-01再設計の回帰防止の観点から、インタビュー結果に関わらず床に
  含めるべき — Q4として提示しAで確定。discovered-rules.mdにALWAYS化。
- OBJECT（未採用・理由あり）: featureマトリクスCIの決定をOpen Questions
  に明示すべきという指摘 — featureマトリクスCIは既に範囲定義
  （`scope-document.md`、優先度Should／IN）で確定済みのため、
  追加の質問は不要と判断した（今回のQ6はclippy導入の可否のみに絞った）。

### aidlc-developer-agent
- AGREE: インデント矛盾の裁定（`tab_spaces=2`優先）、test-after／
  共有シナリオ再利用の記録、discovered-rules.mdの推測抑制方針。
- OBJECT（採用）: レイヤ境界規約（`StorageBackend`+`GenericEventStore`
  経由、`EventStore`直接実装禁止）の欠落 — team-practices.md/
  discovered-rules.md双方にALWAYS化して反映。
- OBJECT（採用）: エラー処理規約（panic禁止、AWS型リーク非踏襲）の欠落 —
  team-practices.md Code Styleに反映、discovered-rules.mdにNEVER化。
- OBJECT（採用）: feature命名・`#[cfg(feature)]`ゲート方針の欠落 —
  team-practices.md Code Styleに反映。
- OBJECT（採用）: 命名規約の記述不正確（「公開API=CamelCase」）と
  `EventStoreForSQLite`/`EventStoreForSqlite`の裁定要否 — 前者は
  team-practices.mdで型/関数の軸に訂正、後者はQ7として提示しAで確定。

### aidlc-devsecops-agent
- AGREE: TD-11の未確定扱い、デプロイのcrates.io publish読み替え、
  DBスキーマ自動作成のMandatedルール。
- OBJECT（採用）: 「Renovate運用に乗る前提で問題ない」という評価は
  RUSTSEC照合ゲート不在を捨象している — Q8として提示しAで確定
  （依存監査を今回CIに追加）。
- OBJECT（一部採用）: `openai-review.yml`の`pull_request_target`+
  可変タグ`@latest`のサプライチェーンリスクが未評価 —
  team-practices.md「Security（参考情報）」に認識として記載したが、
  今回のSQLite Boltスコープには含めず別イニシアチブのバックログとした
  （インタビューでは択一の3項目提案のうち依存監査（Q8相当）のみを
  スコープに採用し、`openai-review.yml`ピン留めとcrates.io Trusted
  Publishing移行は今回の8問には含めなかった — 理由: ソロOSSのリスク/
  技術判断に絞るというproject.md学習事項に照らし、SQLite Bolt自体の
  受け入れ基準に直結する項目（テスト態勢・依存監査）を優先し、
  ワークフロー基盤の改修（publish認証方式・CI actionピン留め）は
  スコープクリープと判断したため）。
- OBJECT（採用）: 手書き`unsafe impl Send/Sync`はセキュリティ観点でも
  要注意 — discovered-rules.mdにNEVER化（developer検分と一致）。

## 8問インタビューの決定事項（確定・authoritative）

1. **Q1 マージ方式**: マージコミット方式を継続（squashにしない）。
2. **Q2 ウォーキングスケルトン**: 作る（薄い1本を最初に、Bolt 1として）。
3. **Q3 カバレッジ**: 目標・計測なし（`library`スコープは床適用対象外）。
4. **Q4 楽観的ロック競合パス＋エラー契約テスト**: SQLiteで必須。
5. **Q5 リリース**: 完全自動フローを維持（手動承認を追加しない）。
6. **Q6 clippy**: 今回のスコープでCIに導入（`-D warnings`）。
7. **Q7 型名**: `EventStoreForSqlite`（Rust API Guidelines準拠）。
8. **Q8 依存監査**: `cargo-audit`/`cargo-deny`を今回CIに追加。

なお **CI featureマトリクス**（`--no-default-features`/各feature単独/
`--all-features`）は承認済みの範囲定義書（`aidlc/spaces/default/intents/
260822-sqlite-event-store/ideation/scope-definition/scope-document.md`）で
既に優先度Should（IN）として確定済みのため、本インタビューの8問には
含めなかった。

全回答の統合サマリーは人間により "Looks correct" で確認済み
（`practices-discovery-questions.md` Consolidated Summary Confirmation）。

## 残存する未確定事項（今回のスコープ外・バックログ）

- MSRV（`rust-version`）の宣言要否とCI検証ジョブの追加。
- examplesビルド検証のCI追加。
- `openai-review.yml`のアクションSHAピン留め／代替方式検討。
- crates.io publishのTrusted Publishing（OIDC）移行、`PERSONAL_ACCESS_TOKEN`
  のスコープ最小化確認。
- `SECURITY.md`の追加、`.github/CODEONWERS`タイポ修正（TD-12）。
- Memoryバックエンド専用テスト、スナップショット保持動作の実assert追加。
- doc コメントの言語（日本語主体か英語統一か）方針。

これらはproject.mdの学習事項（ソロOSS、リスク/スコープ/技術判断に絞る）
に沿い、今回の8問には含めず将来のインタビュー・別イニシアチブに委ねる。
