**Collaborator:** aidlc-devsecops-agent

## Contribution

セキュリティ観点（lint/format・SAST/DAST・シークレット/依存スキャン・サプライチェーン統制）からのブラインドレビュー。対象エビデンス: `.github/workflows/`（4本）、`renovate.json`、`aidlc/spaces/default/codekb/sqlite/dependencies.md`、`code-quality-assessment.md`、およびリード草稿3点。ソロOSS Rustクレートという文脈（project.md ## Corrections）に比例した統制のみ提案する。

### 1. Lint / Format — 現状評価と推奨

- 現状: CIの品質ゲートは `cargo fmt -- --check`（nightly rustfmt）のみ。clippy未導入はリード草稿・TD-11 の認識どおりで正確。
- org.md ## Code Style は「リンタはCIで実行しマージ前に失敗をブロック」を既定とするため、clippy欠如は org 既定との乖離であり「未導入なら提案対象」に該当する。推奨: `ci.yml` の `lint` ジョブに `cargo clippy --workspace --all-targets -- -D warnings` を追加し、feature分割後は clippy も featureマトリクス（`--no-default-features` / 各feature単独 / `--all-features`）で回す。SQLiteバックエンドは新規コードのため、既存コードの clippy 負債と切り離して「新規モジュールは clippy クリーン」を最低線にできる。
- TD-03 の `#[allow(dead_code)]`（モジュール宣言レベル）は警告を実質封殺しており、リンタ導入効果を減衰させる。feature分割で `#[cfg(feature)]` ガードに置換する際に同時撤去するのが自然な返済経路。
- TD-06 の手書き `unsafe impl Send/Sync` はセキュリティ観点でも要注意（Sendでない内部状態を誤ってSend宣言するとデータ競合＝メモリ安全性侵害になり得る）。SQLite実装で踏襲しない方針に同意。既存分の削除検証後、`lib.rs` に `#![deny(unsafe_code)]`（または Cargo.toml の `[lints.rust] unsafe_code = "deny"`）を導入し、逸脱をCIで機械検出することを推奨。

### 2. SAST / DAST — ライブラリ文脈への読み替え

- **DAST は本プロジェクトでは適用対象外（N/A）**。稼働Webサービスを持たないライブラリクレートであり、project.md の学習事項（クラウド前提をライブラリ文脈へ置換）に従い、team-practices.md には「該当なし」と明記して将来の再質問を防ぐことを推奨（「欠落」ではなく「非該当」として記録する）。
- Rustライブラリにおける SAST 相当の実務は (a) clippy（`-D warnings`）、(b) `cargo audit` / `cargo-deny` による RUSTSEC 脆弱性照合、(c) unsafe 監査（上記 `deny(unsafe_code)`）の3点で足りる。ソロOSSに CodeGuru/SonarQube 級の導入は過剰であり提案しない。
- SQLite統合テストで SQL を組み立てる際の SQLインジェクション対策（バインドパラメータ必須、文字列連結によるSQL構築禁止）は、Construction フェーズの secure-coding 規範として discovered-rules.md 化する価値がある（`rusqlite` は `params!` バインドが標準なので低コスト）。

### 3. シークレット管理・スキャン

- ワークフロー内にハードコードされた秘密情報は無し（`secrets.*` 参照のみ）— org.md Mandated（秘密のハードコード禁止）に現状適合。
- **`lib-release.yml`**: `cargo publish --token secrets.CARGO_TOKEN` はCLI引数渡し。長期有効な crates.io トークンを GitHub Secrets に保持する現行方式に対し、crates.io の **Trusted Publishing（GitHub Actions OIDC連携）** へ移行すれば長期トークン自体を廃止できる。タグ駆動publishという現行運用と互換であり、ソロOSSでも移行コストは小さい。少なくとも環境変数 `CARGO_REGISTRY_TOKEN` 渡しへの変更とトークンのスコープ最小化（publish専用）を推奨。
- **`lib-bump-version.yml`**: `PERSONAL_ACCESS_TOKEN` + `persist-credentials: true` で main へ直接 push・タグ付け・Release作成を行う。PATは fine-grained（対象リポジトリ限定、contents: write のみ）であることを確認すべき。classic PAT だと漏洩時の爆発半径がアカウント全体に及ぶ。
- リポジトリ自体は「秘密を持たない設計」のライブラリなので、専用の secret-scanning ツール導入は不要。GitHub 標準の Secret scanning + Push protection（公開リポジトリは無料）の有効化確認で十分。

### 4. 依存スキャン — リード草稿への最重要指摘

- **RUSTSEC アドバイザリ照合が全ワークフローに存在しない**。一方で Renovate は minor/patch/pin/digest を `platformAutomerge: true` で自動マージする。つまり「依存は自動で入るが、既知脆弱性の検出網が無い」非対称があり、エビデンス.md の「Renovate運用に乗る前提で問題ない」はこの欠落を捨象している。推奨: `ci.yml`（毎日cronが既にあるため好適）に `cargo-deny check advisories licenses` もしくは最低限 `cargo audit` ジョブを追加。既存の日次cronは「最新依存でのビルド破壊検知」として既に機能しており、そこにアドバイザリ照合を相乗りさせるのが最小コスト。
- `Cargo.lock` は未コミット（`git ls-files` で確認）。ライブラリとしては妥当な選択だが、CIビルドの再現性は無い＝日次cronが暗黙の「浮動依存カナリア」になっている。この実運用は team-practices.md に明記する価値がある。
- MSRV未宣言 × automerge は「依存更新が最低サポートRustを黙って引き上げる」リスク（dependencies.md 指摘）を増幅する。リード草稿のMSRVインタビュー項目に賛成。宣言する場合は CI に MSRV ジョブ（`rust-version` のツールチェーンでビルド）を対にすること。

### 5. サプライチェーン統制 — GitHub Actions と bundled SQLite

- **[P1] `openai-review.yml` は `pull_request_target` トリガで `coderabbitai/openai-pr-reviewer@latest` という可変タグ参照を実行し、`GITHUB_TOKEN`（pull-requests: write）と `OPENAI_API_KEY` を渡している**。`@latest` は上流アクションの改竄・乗っ取りがそのまま secrets 窃取に直結する既知のサプライチェーン攻撃経路であり、リード草稿・evidence.md はこのワークフローを「LLMレビューが動く」としか評価していない。推奨: コミットSHAピン留めへ変更、または当該アクション（アーカイブ済みで更新停止中）の廃止・代替を検討。4ワークフロー中、これが唯一の即時対応価値がある指摘。
- **[P2] アクション参照のピン留めが不統一**: `baptiste0928/cargo-install` はSHAピン（良い実践）だが、`actions/checkout@v6` / `dtolnay/rust-toolchain` / `actions/create-release@v1` はタグ参照。特に `actions/create-release@v1` は**アーカイブ済み・保守終了**のアクション。方針として「サードパーティ製アクションはSHAピン、GitHub公式はメジャータグ可」程度の軽量ルールを discovered-rules.md 候補にできる。
- **[P2] `permissions:` ブロック欠如**: `ci.yml` / `lib-bump-version.yml` / `lib-release.yml` はトップレベル `permissions:` 未指定で、デフォルトトークン権限に依存。最小権限原則として `ci.yml` に `permissions: contents: read` の明示を推奨（1行の変更で済む）。
- **bundled SQLite の新規サプライチェーン面**: `rusqlite` の `bundled` feature は `libsqlite3-sys` 同梱の SQLite C アマルガメーションを `cc` でコンパイルする。これにより (a) C コード（SQLite本体のCVE）がクレートの脆弱性面に加わる、(b) その供給元は crates.io パッケージの同梱ソースになる。統制としては、RUSTSEC が `libsqlite3-sys` 同梱SQLiteのCVEを追跡しているため、**上記4の cargo-audit/deny ゲートが bundled 採用の実質的な前提条件**となる（アドバイザリ検出 → Renovate のpatch自動マージで修正が流れる、という閉ループが成立する）。ライセンス面は SQLite = パブリックドメインで問題なし。`cargo vet` / SBOM 生成まではソロOSSには過剰と判断し提案しない。
- **feature分割はそれ自体が攻撃面削減**: 現状は全利用者に AWS SDK + gRPC スタックが無条件リンクされる（dependencies.md）。分割後は利用者の依存グラフ＝脆弱性面が選択したバックエンド分に縮小する。これはセキュリティ上の便益として feature 分割の動機付けに追記する価値がある。
- 軽微: `SECURITY.md`（脆弱性報告窓口）が無い。イベントストアという性質上、報告経路の明示は1ファイルで済む低コスト統制。`.github/CODEONWERS` の typo（TD-12）はソロ運用では実害軽微だがリネームは1手。

### インタビュー項目への追加提案（ソロOSS・リスク/技術判断に限定）

1. `openai-review.yml` の `@latest` + `pull_request_target` を続けるか、SHAピン/廃止するか（P1）。
2. crates.io publish を長期トークン継続か Trusted Publishing (OIDC) へ移行するか。
3. `cargo-deny`（advisories + licenses）を日次cron CIに追加するか、`cargo audit` のみの最小構成にするか。

## Positions

- AGREE: TD-11（clippy・featureマトリクス・MSRV検証のCI欠如）の認識と未確定扱い — org.md Code Style の「リンタはCIでブロック」既定との乖離として正確に捕捉されており、feature分割後のマトリクスCI実質必須という評価にも同意。
- AGREE: デプロイを「crates.io publish」に読み替えた整理と、SQLiteリリース時の手動承認要否をインタビューに回す判断 — project.md のライブラリ文脈置換に忠実で、完全自動フローの実態記述もワークフロー実装と一致している。
- AGREE: DBスキーマはライブラリ自動作成が担うという Mandated ルール — SQLite実装でバインドパラメータ必須のDDL/DML統制を敷く前提とも整合する。
- OBJECT: evidence.md の「SQLite関連の新規依存追加時もこのRenovate運用に乗る前提で問題ない」— RUSTSEC照合ゲートが皆無のまま automerge に乗せる評価は、bundled SQLite で C コードのCVE面が加わる本イニシアチブでは楽観的すぎる。cargo-audit/deny 追加を前提条件として併記すべき。
- OBJECT: `openai-review.yml` の評価が「LLMレビューが動く」に留まっている — `pull_request_target` + 可変タグ `@latest` + secrets 露出という4ワークフロー中最大のサプライチェーンリスクが未評価。team-practices.md か未確定項目のいずれかに載せるべき。
- OBJECT: リリース自動化の記述にサプライチェーン統制の欠落（アクションのピン留め不統一、アーカイブ済み `actions/create-release@v1`、`permissions:` 未指定、長期 `CARGO_TOKEN`）が反映されていない — 「手動承認の要否」だけでなく「publish経路の資格情報強化」もインタビュー項目に含めるべき。
