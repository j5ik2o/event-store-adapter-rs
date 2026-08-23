# インフラ仕様 — u4-docs (infrastructure-specification)

U4（ドキュメント）のインフラ仕様。Q1=A確定（U4はCI・ワークフロー・配信設定に一切触れない）に基づき、本ユニットが**新規インフラを一切導入しない**こと、および成果物が乗る既存配信経路を記録する。

## 結論（Q1=A確定）

U4はインフラ・CI・配信設定への変更を行わない。成果物（README英/日・docs/DATABASE_SCHEMA英/日・CHANGELOG・examples）はすべて既存の配信経路に乗る静的ファイルであり、新しい実行環境・サービス・ワークフローを要しない。

## 既存配信経路の整理（変更なし・情報提供）

| 成果物 | 配信経路 | 配信タイミング |
|---|---|---|
| README.md / README.ja.md | GitHubリポジトリのMarkdownレンダリング／crates.io・docs.rsのREADME表示 | GitHubはmainマージと同時。crates.io/docs.rsは次回リリース（タグpush → `lib-release.yml` の `cargo publish`）時 |
| docs/DATABASE_SCHEMA.md / .ja.md | GitHubリポジトリのMarkdownレンダリング | mainマージと同時 |
| CHANGELOG.md | GitHubリポジトリのMarkdownレンダリング | mainマージと同時 |
| examples/ | リポジトリ内ソース（利用者がローカルで `cargo run --example` 実行） | mainマージと同時 |

- crates.ioに載るREADMEは `lib/Cargo.toml` の `readme` 指定に従う（パッケージング済み設定 — U4はこの設定自体を変更しない。`description` フィールドの文言更新はコード生成工程のTD-10解消として行うが、これはパッケージメタデータの文言変更であり配信経路・インフラの変更ではない）
- examplesの実行環境要件: SQLite exampleはクラウド接続なし・Docker不要（ファイルDBまたは `:memory:`）。既存のDynamoDB/Bigtable examplesの実行要件（ローカルエミュレータ等）は変更しない

## 明示的な非変更事項

- `.github/workflows/` 配下のワークフロー（`ci.yml`・`lib-bump-version.yml`・`lib-release.yml`）に変更なし
- `deny.toml`・Renovate設定に変更なし
- examplesのCIビルド検証は追加しない — 既知の欠落としてバックログ維持（team.md確定、U4 nfr-requirements traceability NFR-3 N/A と整合）

## Assumptions & Open Questions

None.

## Review

**Verdict:** READY
**Reviewer:** aidlc-architecture-reviewer-agent
**Date:** 2026-08-23T09:40:50Z
**Iteration:** 1

### Findings

| # | Severity | Location | Finding | Recommendation |
|---|---|---|---|---|
| 1 | Minor | traceability.json（NFR-4.12行） | `target`欄が「examplesのクラウド接続なし・Docker不要の実行要件」の出典を`cicd-pipeline.md`としているが、`grep -rn "クラウド接続\|Docker不要" .../infrastructure-design/*.md`で確認したところ、この文言は`infrastructure-specification.md`19行目にのみ存在し、`cicd-pipeline.md`には一切出現しない（0件）。`cicd-pipeline.md`が実際に述べているのは「依存追加なし（audit）」「fmt/clippy対象」のみで、実行環境要件（クラウド接続なし・Docker不要）の記述はない。実行要件の内容自体はinfrastructure-specification.mdに実在し正確だが、traceability上の出典ファイルの帰属が誤っている。 | `target`欄を「examplesのクラウド接続なし・Docker不要の実行要件（infrastructure-specification.md）／CI通過条件（cicd-pipeline.md — 依存追加なし・fmt/clippy対象）」のように出典を分割して修正する。 |
| 2 | Minor | cicd-pipeline.md 9-10行目 | 「`ci.yml`の既存ジョブがexamplesを明示的にビルドするステップは持たない — examplesビルド検証の欠落は既知のバックログ」という記述と、同じ文書内の「clippyジョブは`--workspace --all-targets`のため examples も検査対象に入る」という記述が緊張関係にある。ルートの`Cargo.toml`は`members = ["lib", "test-utils", "examples/*"]`のバーチャルワークスペースであり、`examples/user-account`は独立クレート（`examples/user-account/Cargo.toml`）としてワークスペースメンバーに含まれる。`cargo clippy --workspace --all-targets`はコンパイル（型検査）が成功しなければlintを実行できないため、examplesクレートに壊れたコードがあればclippyジョブは既に失敗する。一方`feature-matrix`/`test-lib`ジョブは`cargo build/test -p event-store-adapter-rs`と`-p`指定のみで、examplesクレートを対象にしない（この点自体は正確）。「ビルド検証が全くない」という含意は、実際には「専用のbuild/testステップはないが、clippyジョブによる事実上のコンパイル検証は既に存在する」という実態をやや過小に伝えている。team.md確定の「既知の欠落」という上位判断自体を覆すものではないため、記述の精度向上に留まる指摘。 | 「専用の`cargo build`/`cargo test`ステップはないが、clippyジョブ（`--workspace --all-targets`）が examples の事実上のコンパイル検証を代替している」旨を明記し、バックログの範囲を「実行時テスト（`cargo test -p example-user-account`相当）の欠如」に限定して記述する。 |

新規Critical所見なし。新規Major所見なし。

### Validation Tool Results

| 確認内容 | コマンド／方法 | 結果 | 解釈 |
|---|---|---|---|
| Q1=A確定の3成果物への反映一貫性 | infrastructure-specification.md / monitoring-design.md / cicd-pipeline.md / infrastructure-design-questions.mdの通読比較 | 一貫 | 4ファイルすべてで「U4はCI・ワークフロー・配信設定に一切触れない」「examplesビルド検証はバックログ維持」が矛盾なく反復されている。 |
| clippyジョブがexamplesを検査対象に含むという主張 | `cat .github/workflows/ci.yml`（clippyジョブ: `cargo clippy --workspace --all-targets -- -D warnings`）、`cat Cargo.toml`（`members = ["lib", "test-utils", "examples/*"]`） | 主張どおり | `--workspace`が`examples/*`配下のクレート（実測: `examples/user-account`）を含み、`--all-targets`がその中のbin等を含むため、examplesはclippy検査対象に入る。 |
| ci.ymlの既存ジョブがexamplesを明示的にビルドしないという主張 | `cat .github/workflows/ci.yml`のfeature-matrix/test-libジョブを確認 | 主張どおり | 両ジョブとも`cargo build/test -p event-store-adapter-rs <flags>`であり、`-p`指定によりexamples配下のクレートはビルド対象外。`--examples`や`--workspace`を使うステップは存在しない。 |
| lib-bump-version.ymlのトリガーがworkflow_dispatch＋日次cronという主張 | `cat .github/workflows/lib-bump-version.yml`の`on:`セクション | 主張どおり | `on: workflow_dispatch: / schedule: - cron: '0 0 * * *'`のみで、`push`トリガーは存在しない。mainマージ即時トリガーではないという記述は正確。 |
| lib/Cargo.tomlに`readme`指定があるという主張 | `cat lib/Cargo.toml` | 主張どおり | 11行目に`readme = "../README.md"`が実在する。捏造ではない。 |
| traceability.json（infra-design, u4-docs）のJSONパース | `python3 -c "import json; json.load(open('.../infrastructure-design/traceability.json'))"` | PASS | 構文エラーなし。 |
| traceability.jsonの3 ID（NFR-1.4/1.5/4.12）と上流nfr-requirements(u4)の突合 | nfr-requirements/security-requirements.mdの見出し一覧、nfr-requirements/traceability.jsonのcoverage欄と比較 | 過不足なく一致 | u4のnfr-requirementsが定義する派生ID（NFR-1.4・NFR-1.5・NFR-4.12）と完全一致。余剰IDも欠落IDもない。 |
| rustfmtチェックがexamplesを含むかの主張 | `cat Cargo.toml`（ルートがバーチャルワークスペース、`[package]`なし） | 主張と整合 | ルートManifestに`[package]`がないバーチャルワークスペースのため、`cargo fmt -- --check`（`-p`/`--all`指定なし）は全ワークスペースメンバーを対象とする（cargo-fmtの既知の挙動）。examplesも対象に入るという記述は妥当。 |

### Summary

Q1=A確定（U4はCI・ワークフロー・配信設定に一切触れない）は4成果物すべてで一貫して反映されており、矛盾は見つからなかった。cicd-pipeline.mdの実CI主張（clippyの`--workspace --all-targets`によるexamples検査対象化、feature-matrix/test-libの`-p`指定によるexamples除外、lib-bump-version.ymlのworkflow_dispatch＋日次cronトリガー）はすべて実ファイルと突合し正確であった。`lib/Cargo.toml`の`readme`指定についても捏造ではなく実在を確認した。traceability.jsonはJSONとして妥当であり、3つの派生NFR IDは上流nfr-requirements(u4)と過不足なく一致する。唯一の実質的な指摘は、traceability.jsonのNFR-4.12行が一部の記述内容（クラウド接続なし・Docker不要）の出典ファイルを誤って`cicd-pipeline.md`に帰属させている点（Minor #1）と、cicd-pipeline.md自身の「examplesビルド検証の欠落」という記述がclippyジョブによる事実上のコンパイル検証の存在とやや緊張関係にある点（Minor #2）である。いずれも実装を妨げる誤りではなく、記述精度の改善に留まるためCritical/Major所見はなし。Critical 0件・Major 0件・Minor 2件のためREADYと判定する。
