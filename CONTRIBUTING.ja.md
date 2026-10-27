> この文書は [CONTRIBUTING.md](CONTRIBUTING.md) の日本語版です。英語版が正本で、食い違うときは英語版に従います。

# event-store-adapter-rs の開発者向け案内

## 概要

この案内は、このリポジトリに変更を加える開発者のためのものです。ライブラリの使い方は [README](README.ja.md) を読んでください。

英語のファイル（`X.md`）が正本で、日本語のファイル（`X.ja.md`）は補助です。この案内の英語版は [CONTRIBUTING.md](CONTRIBUTING.md) です。

リポジトリは Cargo の workspace です。リリースの workflow が crates.io へ公開するのは `event-store-adapter-rs` だけで、ほかのパッケージはリポジトリの中で使います。

| ディレクトリ | パッケージ | 役割 |
|:--|:--|:--|
| `lib` | `event-store-adapter-rs` | ライブラリのクレート。公開するパッケージ |
| `test-utils` | `event-store-adapter-test-utils-rs` | テストのために、DynamoDB Local、Spanner エミュレーター、Bigtable エミュレーターをコンテナで起動する |
| `examples/user-account` | `example-user-account` | DynamoDB Local を使う利用例のアプリケーション |
| `conformance-runner` | `event-store-adapter-conformance-rs` | `conformance/` の適合テストデータを実行する |
| `migration-cli` | `event-store-adapter-migration-rs` | v3 のデータを移行するコマンドラインの道具 |
| `api-surface/check` | `event-store-adapter-api-surface-rs` | 公開 API が、共通契約の求める形をしているかを確かめる |
| `api-surface/fixtures` | `event-store-adapter-api-surface-fixtures-rs` | 照合の否定の試験で使う、形をわざとずらした定義 |

## 共通の契約と決定

すべての言語の実装が共有する仕様と設計判断は、ここではなくハブのリポジトリにあります。

- 仕様: <https://github.com/j5ik2o/event-store-adapter/tree/main/docs/spec>
- 設計判断（ADR）: <https://github.com/j5ik2o/event-store-adapter/tree/main/docs/adr>

このクレートは共通契約 v5 に従い、メジャーの番号 5 は契約の版と同じです。契約が定める振る舞いを変えるときは、先に仕様と判断を読んでください。

このリポジトリには、ハブから固定して写したファイルやデータがあります。CI は、それらの SHA-256 を `.github/workflows/ci.yml` の `lint` のジョブに書いた値と照合するので、手で変えると CI が失敗します。

| パス | 内容 | 規則 |
|:--|:--|:--|
| `conformance/` | 適合テストデータ。データの版は 1.3.0 | `python3 tools/conformance/manifest.py verify` で確かめる。CI は `conformance/manifest.json` の SHA-256 も照合する |
| `api-surface/surface.json` | 公開 API の形についてのハブの定義。版は 0.1.0 | 手で直さない。CI が SHA-256 を照合する |
| `user-docs/template.json` と `tools/user-docs/check.py` | 利用者向け文書についてのハブの型と、その照合の道具。版は 1.0.0 | 手で直さない。CI が SHA-256 を照合する |
| `api-surface/rs.json` と `user-docs/repo.json` | このリポジトリの公開 API との対応表と、照合の道具へのこのリポジトリの設定 | このリポジトリのために直してよい |

## 手元の準備

- Rust のツールチェーン: ツールチェーンを固定するファイルはありません。CI は stable でビルドと試験をします。最小サポートの Rust の版は 1.94.1 です（`lib/Cargo.toml` の `rust-version`）。
- 整形: nightly の rustfmt を使います。設定は `rustfmt.toml` にあります（`tab_spaces = 2`、`max_width = 120`）。nightly が入っていなければ入れてください。
- 公開 API の形の照合: `cargo test -p event-store-adapter-api-surface-rs` と `cargo test --workspace` には、日付を固定した nightly が要ります。照合が、その nightly で rustdoc の JSON を作るためです。
- Docker: DynamoDB・Spanner・Bigtable の保存先の試験、移行、利用例、DynamoDB・Spanner・Bigtable に対する適合試験は、testcontainers でコンテナを起動するので、Docker が起動している必要があります。使うのは、digest で固定した DynamoDB Local 3.3.1、Spanner エミュレーター 1.5.58（`gcr.io/cloud-spanner-emulator/emulator`）、Bigtable エミュレーター（google-cloud-cli 587.0.0-emulators）です。試験は、DynamoDB Local にはダミーの region と資格情報で、2 つのエミュレーターには資格情報なしで接続します。Spanner の試験は、試験ごとに新しいデータベースを作り、Spanner の適合試験の実行は、場面が終わるたびにその場面のデータベースを削除します。メモリと SQLite の試験に Docker は要りません。
- Python 3: `tools/user-docs/check.py` と `tools/conformance/manifest.py` は標準ライブラリだけを使います。`.github/test_*.py` の試験には `semver` のパッケージが要ります。CI は Python 3.15 を使います。 `tools/user-docs/check.py` は Python 3.10 以上が必要です。
- actionlint: CI は `go install`（Go 1.27）で入れ、workflow のファイルに実行します。
- cargo-deny: `deny.toml` の設定で依存を監査します。

```sh
rustup install nightly
rustup toolchain install nightly-2026-09-27 --profile minimal
pip install semver
go install github.com/rhysd/actionlint/cmd/actionlint@v1.7.12
```

## 検査

CI（`.github/workflows/ci.yml`）は、`main` への push、`main` への pull request、日次の定期実行で動きます。pull request を出す前に、変更に合う検査を実行してください。

| ジョブ | 確かめること | Docker |
|:--|:--|:--|
| `lint` | 適合テストデータ、固定したファイルの SHA-256、`tools/user-docs/check.py`、nightly の rustfmt | 要らない |
| `release-control` | `.github/` にあるリリース用スクリプトの試験と actionlint | 要らない |
| `test-lib` | すべての feature を有効にしたライブラリの全試験 | 要る |
| `test-conformance-migration` | すべての feature を有効にした、適合の実行器と移行 CLI の試験 | 要る |
| `api-surface` | 固定した nightly での、公開 API の形 | 要らない |
| `feature-matrix` | feature の組ごとのライブラリ（詳細は下） | 要らない |
| `clippy` | 同じ 8 つの feature の組での、警告を拒否した clippy | 要らない |
| `conformance` | 保存先ごとの適合テストデータ: `memory`、`dynamodb`、`sqlite`、`spanner`、`bigtable` | `dynamodb`、`spanner`、`bigtable` だけ要る |
| `audit` | `cargo deny check advisories licenses` | 要らない |
| `CI Success` | `lint`、`test-lib`、`test-conformance-migration`、`api-surface`、`feature-matrix`、`clippy`、`release-control`、`conformance` をまとめる。`audit` は含まない | 要らない |

CI をもとにした代表的なローカルコマンドを並べます。コマンドと feature の組み合わせの全体は [CI のワークフロー](.github/workflows/ci.yml) にあり、feature-matrix と clippy の構成は下にも説明しています。`cargo +nightly fmt --all -- --check` は、`lint` のジョブが nightly のツールチェーンで実行する `cargo fmt -- --check` を、`AGENTS.md` が示す形にしたものです。`audit` のジョブは、cargo-deny の action を通して `cargo deny check advisories licenses` を実行します。

```sh
cargo +nightly fmt --all -- --check
python3 tools/conformance/manifest.py verify
python3 tools/user-docs/check.py
cargo test -p event-store-adapter-rs --no-default-features
cargo test -p event-store-adapter-rs --no-default-features --features sqlite
cargo test -p event-store-adapter-rs --no-default-features --features sqlite-migration
cargo test -p event-store-adapter-rs --all-features
cargo test -p event-store-adapter-conformance-rs -p event-store-adapter-migration-rs --all-features
cargo test -p event-store-adapter-api-surface-rs
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo run -p event-store-adapter-conformance-rs --release -- --backend memory --data conformance --report conformance-report-memory.json --require-all
python3 -m unittest discover -s .github -p 'test_*.py' -v
actionlint -shellcheck=''
cargo deny check advisories licenses
```

`AGENTS.md` には、workspace 全体のコマンドとして次の 2 つもあります。CI はこれらを実行しません。試験には、Docker と日付を固定した nightly が要ります（「手元の準備」を参照）。

```sh
cargo build --workspace --all-targets --all-features
cargo test --workspace --all-features
```

- `feature-matrix` は、`--no-default-features`、`--features sqlite`、`--features sqlite-migration` では試験を実行します。`--features dynamodb`、`--features spanner`、`--features bigtable`、`--features bigtable-migration`、`--all-features` ではビルドだけを行います。
- `feature-matrix` は、`cargo tree` で依存グラフも確かめます。feature がないときは、クラウドの SDK も SQLite のクレートも入ってはいけません。`sqlite-migration` だけのときは、クラウドの SDK が入ってはいけません。`bigtable-migration` だけのときは、AWS・SQLite・Google の認証のクレートが入ってはいけません。`spanner` だけのときは、AWS・SQLite・Bigtable・Google の認証のクレートが入ってはいけません。`lib/src/` に手書きの `unsafe impl` がないことも確かめます。
- `clippy` は、8 つのフラグの組で `cargo clippy --workspace --all-targets <フラグ> -- -D warnings` を実行します。組は、`--no-default-features`、`--no-default-features` に `dynamodb`・`sqlite`・`sqlite-migration`・`spanner`・`bigtable`・`bigtable-migration` のそれぞれの `--features <feature>` を付けたもの、`--all-features` です。
- `conformance` の `dynamodb`・`sqlite`・`spanner`・`bigtable` の leg は、`--release` の後に `--features <保存先>` を付け、`--backend <保存先>` にします。`dynamodb`・`spanner`・`bigtable` の leg は、コンテナを自分で起動します。Spanner の leg を手元で実行するには、`cargo run -p event-store-adapter-conformance-rs --release --features spanner -- --backend spanner --data conformance --report conformance-report-spanner.json --require-all` を実行します。
- 単体試験は、コードの隣の `*_test.rs` に置き、`#[cfg(test)] #[path = "x_test.rs"] mod tests;` で結びます。結合試験は、各クレートの `tests/` のディレクトリに置きます。
- `tools/user-docs/check.py` は、README・保存先のスキーマの文書・移行ガイドを、英語と日本語の両方について照合します。見出しの組み立て、相対リンクの行き先が存在するか、日本語のファイルの 1 行目を見ます。文章の中身は見ません。

## 設計の経緯

- `docs/design/next-major.md`（日本語は `docs/design/next-major.ja.md`）は、v5 を作るときの設計の記録です。現在の使い方は、README と現在のコードで確認してください。
- `docs/design/spanner-backend.md`（日本語は `docs/design/spanner-backend.ja.md`）は、Spanner の保存先の設計の記録です。設計判断、適合テストデータでは表せない規則を確かめる試験、適合の結果、エミュレーターで確かめられないことを記しています。
- すべての実装が共有する判断は、ハブのリポジトリの ADR にあります（「共通の契約と決定」を参照）。
- `CHANGELOG.md` は 5.0.1 までのリリースの記録で、もう更新しません。以後の変更の記録は、[GitHub Releases](https://github.com/j5ik2o/event-store-adapter-rs/releases) のリリースノートです。
- `aidlc/` には、以前の作業で生成した成果物があります。歴史的な記録です。

## 変更とリリース

- コミットメッセージと pull request の題は、Conventional Commits の形で書きます。型は `build`、`ci`、`feat`、`fix`、`docs`、`style`、`refactor`、`perf`、`test`、`revert`、`chore` で、`BREAKING CHANGE` も認識されます。型の直後の `!`、または本文の `BREAKING CHANGE:` か `BREAKING-CHANGE:` は、major の版上げになります。`feat` と `revert` は minor、それ以外の型は patch の版上げになります。
- pull request は squash だけでマージするので、pull request の題が、リリースノートの 1 行になります。
- pull request には、概要、動機、試験結果、影響範囲を書き、関連する issue を `Closes #123` の形でリンクし、レビュアーが再現できる `Steps to verify` を明記します。
- 版上げ: `lib-bump-version` の workflow は、手動で起動したとき（`workflow_dispatch`）だけ動きます。入力 `level` は `auto`・`patch`・`minor`・`major` のどれかです。入力 `version` を指定しないときに、`lib/Cargo.toml` の版が pre-release なら、`major` だけを受け付けます。任意の入力 `version`（`X.Y.Z`）を指定すると、`level` とこの制限を使わずにその版へ上げます。その版は、既存の `v*` タグの最大より大きく、同じ名前のタグがないものに限ります。workflow は `cargo set-version` を実行し、`version up to vX.Y.Z` をコミットし、タグを push し、自動で作ったリリースノートの GitHub Release を作ります。
- リリース: `lib-release` の workflow は、`vX.Y.Z` または `vX.Y.Z-*` の形のタグが push されると起動します。タグと `lib/Cargo.toml` の版が一致することを確かめ、`rust-lang/crates-io-auth-action` で認証し、`cargo publish -p event-store-adapter-rs` を実行します。
- 依存の更新: Renovate は、題が `chore(deps):` で始まる pull request を作ります。minor・patch・pin・digest の更新と、devDependencies の更新は自動でマージします。`takt` と `rustdoc-types` は更新しません。
- `main` の CI が失敗すると、`main-ci-failure` の workflow が、ラベル `ci-failure-on-main` の issue を作るか、開いている issue にコメントを足します。のちの成功した実行で、その issue を閉じます。
