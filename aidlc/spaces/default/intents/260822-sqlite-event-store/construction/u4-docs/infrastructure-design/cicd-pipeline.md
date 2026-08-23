# CI/CDパイプライン — u4-docs (cicd-pipeline)

U4（ドキュメント）のCI/CD関与の記録。Q1=A確定: **U4はCI・ワークフロー・配信設定に一切触れない**。

## U4のCI通過条件（既存パイプラインをそのまま通過する）

U4のコミット（README英/日・DATABASE_SCHEMA英/日・CHANGELOG・examples）は、既存CI（U1/U3で確定済み）の対象としてそのまま検証される:

- **feature-matrix / test-lib**: 文書ファイルの変更はビルド・テストに影響しない。examplesの追加・変更は `cargo build` の対象になる範囲でコンパイル検証される（`ci.yml` の既存ジョブがexamplesを明示的にビルドするステップは持たない — examplesビルド検証の欠落は既知のバックログ）
- **lint（rustfmt）/ clippy**: examplesのRustコードは `cargo +nightly fmt -- --check` の対象。clippyジョブは `--workspace --all-targets` のため examples も検査対象に入る（新規exampleはclippyクリーンを最低線とする — team.mdのclippy方針に整合）
- **audit（cargo-deny）**: U4は依存を追加しないため、監査結果に影響しない

## 配信フロー（既存・変更なし）

1. `sqlite` ブランチ → `main` へPRマージ（マージコミット方式）
2. GitHub上の文書（README・DATABASE_SCHEMA・CHANGELOG・examples）は即時に最新化
3. `lib-bump-version.yml`（workflow_dispatch＋日次cron — mainマージ即時トリガーではない。U1インフラ設計レビューで確認済みの事実）がConventional Commitsからsemverレベルを判定し、バージョンバンプ＋タグpush
4. タグpush → `lib-release.yml` が `cargo publish` — この時点でcrates.io/docs.rsのREADME表示が更新される

## 明示的な非変更・バックログ

- examplesのCIビルド検証ステップ追加はバックログ（Q1=A確定・team.md既知の欠落）
- ワークフローYAML・deny.toml・Renovate設定への変更なし

## Assumptions & Open Questions

None.
