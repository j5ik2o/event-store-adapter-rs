# CI/CDパイプライン設計 — u1-backend-features (cicd-pipeline)

U1（feature分割・エラー型中立化・Memory準拠化）の配信パイプライン設計。セキュリティ設計（`../nfr-design/security-design.md` 検証手順）・論理コンポーネント（`../nfr-design/logical-components.md` FeatureGateブラスト半径）・コンポーネントカタログ（`../../../inception/domain-design/components.md`）・機能仕様（`../functional-design/functional-spec.md` ワークフロー2）・契約（`../../../inception/contract-design/contract-summary.md` C-3 feature軸）に基づく。本プロジェクトはOSS Rustクレートであり、「デプロイ」= crates.io への公開。クラウドインフラは存在しない。なお性能系の設計書（performance-design / scalability-design / reliability-design / observability-design）はキンド適用によりU1対象外で、infrastructure-specification.md / monitoring-design.md も同様に対象外（ライブラリユニット）。

## 現行パイプライン（変更前の正）

| 段階 | 実体 | 内容 |
|---|---|---|
| Lint | `ci.yml` `lint` ジョブ | nightly rustfmt で `cargo fmt -- --check`（PR・main push・日次cron） |
| Test | `ci.yml` `test-lib` ジョブ（lint後続） | stable で `cargo test --verbose -p event-store-adapter-rs` |
| バージョン確定 | `lib-bump-version.yml` | mainマージ後、Conventional Commitsからsemverレベルを自動判定しバンプコミット＋タグpush |
| 公開 | `lib-release.yml` | `v*` タグpushをトリガーに `cargo publish`（crates.io） |

## U1での変更（最小 — Q1=A確定）

- **`ci.yml` `test-lib` ジョブのみ**: `cargo test --verbose -p event-store-adapter-rs` → `cargo test --verbose -p event-store-adapter-rs --all-features` に修正する
  - 理由: U1で `default = []` となるため、feature指定なしのテスト実行ではDynamoDB/Bigtableのテストがコンパイル対象外となり、U3のマトリクス整備までCIから外れる。mainマージ＝自動リリースである以上、既存テストのCI実行（BR1.9の検証面）を切らさない
  - 変更しないもの: lintジョブ・トリガー・cron・`lib-bump-version.yml`・`lib-release.yml`（完全自動リリースフロー維持 — チーム確定事項Q5）
- **新しいシークレットは追加しない**: 公開用トークン（既存のGitHub Actionsシークレット）を含め、CI/CDの資格情報構成は不変。U1のコードにも資格情報は入らない（NFR-4.3）

## ステージ→ゲート対応

| ステージ | ゲート | U1での状態 |
|---|---|---|
| PR上の `cargo fmt -- --check` | マージ前提条件（既存） | 不変（project.md Mandated） |
| PR上の `cargo test --all-features` | マージ前提条件 | U1で `--all-features` 化（本設計） |
| featureマトリクス（未指定/各単独/全feature）ビルド・テスト | マージ前提条件（将来） | U3が整備（FR-5.1）。それまでは security-design.md の手動検証手順で代替 |
| clippy `-D warnings` / 依存監査 | マージ前提条件（将来） | U3が整備（FR-5.2/FR-5.3） |
| 本番公開 | なし（完全自動） | 不変 — 手動承認は追加しない（チーム確定事項Q5） |

## U3への引き渡し要求仕様

U1完了時点でU3が昇格すべき検証（security-design.md の検証手順コマンド列が正）:

1. featureマトリクス: `--no-default-features`／`--features dynamodb`単独／`--features bigtable`単独／`--features sqlite`単独／`--all-features` のビルド・テスト
2. 依存グラフ検査: feature未指定時にクラウドSDKが依存ツリーへ現れないこと（cargo tree）
3. clippy（`-D warnings`）・依存監査（cargo-audit または cargo-deny advisories）

## ロールバック手順

- **マージ前**: PRクローズのみ（既存フロー）
- **マージ後・公開前**: `main` 上で `git revert`（Conventional Commits形式のrevertコミット — 自動バンプが後続リリースとして扱う）
- **公開後**: 欠陥バージョンは `cargo yank --vers <x.y.z>` で取り下げ、revert後の新バージョンを自動フローで公開。破壊的変更の告知はCHANGELOG（FR-6.4）が担う

## Assumptions & Open Questions

None.

## Review

**Verdict:** READY
**Reviewer:** aidlc-architecture-reviewer-agent
**Date:** 2026-08-23T00:57:48Z
**Iteration:** 1

### Findings

| # | Severity | Location | Finding | Recommendation |
|---|---|---|---|---|
| 1 | Major | cicd-pipeline.md「現行パイプライン（変更前の正）」表 バージョン確定行（9〜12行目）、および「U1での変更」節の理由記載（17行目） | 「バージョン確定」行は `lib-bump-version.yml` を「mainマージ後、Conventional Commitsからsemverレベルを自動判定しバンプコミット＋タグpush」と記載しているが、実ファイル `.github/workflows/lib-bump-version.yml` の `on:` ブロックは `workflow_dispatch` と `schedule: cron: '0 0 * * *'`（日次cron）のみで、`push: branches: [main]` に相当するトリガーは存在しない。つまりマージ直後に即座にバージョンバンプ・タグ付けが走るのではなく、手動実行または最大24時間周期の日次cronでのみ起動する。この誤認は「U1での変更」節の理由（「mainマージ＝自動リリースである以上」）およびQ&Aファイル（Q1、「`main`へのマージは自動リリースに直結するため」）でも前提として繰り返されており、本書自身が「変更前の正」と明記するベースライン表の事実誤認が、中核設計判断の緊急性の根拠として2箇所で再利用されている。推奨する`--all-features`化の結論自体は妥当（トリガー頻度に関わらずCIでのテスト網羅維持は必要）だが、「マージ後・公開前」ウィンドウの実際の長さ（日次cron待ちのため最大24時間程度の余地がある）という、後段の「ロールバック手順」節の前提にも関わる事実がこのままでは正しく伝わらない。 | バージョン確定行を「`workflow_dispatch`（手動）または日次cron（`0 0 * * *`）で起動し、直近タグ以降のConventional Commitsからsemverレベルを自動判定してバンプコミット＋タグpushする（mainマージへの即時反応ではない）」に修正し、U1での変更の理由・Q&Aへの言及も「マージ後、最大24時間以内の次回cron起動時に自動リリース対象となる」等、正確な起動条件に合わせて調整する。 |
| 2 | Minor | cicd-pipeline.md「ステージ→ゲート対応」表（23〜29行目） | `cargo fmt -- --check`（lintジョブ）と`cargo test --all-features`（test-libジョブ）の両行を、いずれも独立した「マージ前提条件」として並列に記載している。しかし実際のGitHubブランチ保護設定（`gh api repos/j5ik2o/event-store-adapter-rs/branches/main/protection`で確認）では `required_status_checks.contexts` は `["test-lib"]` のみで、`lint`は必須ステータスチェックとして個別設定されていない。`lint`が実質的にマージを止めるのは`test-lib`が`needs: lint`でジョブ依存しているためであり（lintが失敗するとtest-libが走らずrequired checkが満たされない）、結果としての実効果は表の記載どおりだが、独立した2つのゲートであるかのような表現はGitHub側の実設定と一致しない。この文書はU3への引き渡し仕様を明記的に含むため、U3側がこの表を読んで「lintも個別のrequired status check」と誤認するリスクがある。 | 「PR上の`cargo fmt -- --check`」の行に、「独立した必須ステータスチェックではなく、`test-lib`がジョブ依存（`needs: lint`）することで間接的にマージを止める」旨の注記を追加する。 |

### Validation Tool Results

| Tool/確認 | 結果 | 解釈 |
|---|---|---|
| `.github/workflows/ci.yml` 実ファイルとの突合（lint/test-libジョブ） | PASS | トリガー（push main／pull_request main／daily cron）、lintジョブ（nightly toolchain、`cargo fmt -- --check`）、test-libジョブ（`needs: lint`、stable toolchain、`cargo test --verbose -p event-store-adapter-rs`）はいずれも「現行パイプライン」表の記載と完全に一致する。 |
| `.github/workflows/lib-release.yml` 実ファイルとの突合 | PASS | `on: push: tags: 'v[0-9]+.[0-9]+.[0-9]+'` と `cargo publish --token ... -p event-store-adapter-rs` が「公開」行の記載と一致する。 |
| `.github/workflows/lib-bump-version.yml` 実ファイルとの突合 | FAIL（所見#1） | トリガーが実際には`workflow_dispatch`＋日次cronのみで、「mainマージ後」という記載と食い違う。バンプ・タグ付けのロジック自体（semver自動判定・コミット・タグpush）は記載どおり。 |
| `gh api repos/j5ik2o/event-store-adapter-rs/branches/main/protection` | 実施 — `required_status_checks.contexts: ["test-lib"]` のみ | 所見#2の根拠。lintは個別のrequired status checkではなく、test-libへのジョブ依存を通じて間接的にゲートしている。 |
| `gh api repos/j5ik2o/event-store-adapter-rs/rules/branches/main`（rulesets） | 実施 — `[]`（空） | 追加のrulesetによる補完的な必須チェックは存在しないことを確認し、所見#2の結論を裏付けた。 |
| `cargo yank --vers <version>` コマンド構文の実機検証（`cargo 1.95.0`） | PASS | `cargo yank --help`には`--version`のみが表示されるが、`--vers`は下位互換のため実際には受理される（`--totally-invalid-flag`のような真に未知のフラグとは異なり、即座の引数解析エラーにならず認証エラーまで到達することをoffline実行で確認）。ロールバック手順の`cargo yank --vers <x.y.z>`は妥当なコマンドである。 |
| `unit-of-work.md` kind確認・`produces_kinds`突合 | PASS | U1のkindは`library`。infrastructure-specification/monitoring-designは`[service, ui, packaging]`でlibraryを含まず対象外、cicd-pipeline/traceabilityは`[..., library]`で必須という、本ユニットの成果物構成（2ファイル）はステージ定義の前提どおり正しい。 |
| FR-5.1〜5.3・FR-6.4の引用突合（`inception/requirements-analysis/requirements.md`） | PASS | 「U3への引き渡し要求仕様」節が引くFR-5.1（featureマトリクス）・FR-5.2（clippy）・FR-5.3（依存監査）、および「ロールバック手順」節が引くFR-6.4（CHANGELOG記載）は、いずれも実際の要件文と一致する。 |

### Summary

「現行パイプライン」表・「U3への引き渡し要求仕様」・ロールバック手順の大部分は実ワークフローファイルおよびGitHubの実設定と正確に一致しており、Q1確定（`--all-features`最小修正）の設計反映も妥当です。ただし、`lib-bump-version.yml`の起動条件（mainマージ即時ではなく、手動実行または日次cron）について本書の「現行パイプライン（変更前の正）」表と設計判断の理由づけの双方に誤認が残っており、これをMajorとして記録しました。この誤認は推奨される設計判断自体を覆すものではありませんが、後続のロールバック手順や運用理解に影響しうる事実誤りであるため修正を要します。あわせて、マージゲートの実効メカニズム（lintは独立チェックではなくジョブ依存経由）についてMinorの精度改善点を1件記録しました。Critical 0件・Major 1件・Minor 1件のためREADYと判定します。
