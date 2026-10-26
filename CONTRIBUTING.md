# Contributing to event-store-adapter-rs

## Overview

This guide is for developers who change this repository. To use the library, read the [README](README.md).

The English file (`X.md`) is the source of truth, and the Japanese file (`X.ja.md`) is a supplement. The Japanese file for this guide is [CONTRIBUTING.ja.md](CONTRIBUTING.ja.md).

The repository is a Cargo workspace. The release workflow publishes only `event-store-adapter-rs` to crates.io; the other packages are used inside the repository.

| Directory | Package | Role |
|:--|:--|:--|
| `lib` | `event-store-adapter-rs` | The library crate. The package that is published |
| `test-utils` | `event-store-adapter-test-utils-rs` | Starts DynamoDB Local and the Bigtable emulator in containers for tests |
| `examples/user-account` | `example-user-account` | An example application that uses DynamoDB Local |
| `conformance-runner` | `event-store-adapter-conformance-rs` | Runs the conformance test data in `conformance/` |
| `migration-cli` | `event-store-adapter-migration-rs` | The command line tool that migrates v3 data |
| `api-surface/check` | `event-store-adapter-api-surface-rs` | Checks that the public API has the shape the common contract requires |
| `api-surface/fixtures` | `event-store-adapter-api-surface-fixtures-rs` | Deliberately shifted definitions for the negative tests of the check |

## Shared contract and decisions

The specification and the design decisions shared by all language implementations are kept in the hub repository, not here:

- Specification: <https://github.com/j5ik2o/event-store-adapter/tree/main/docs/spec>
- Design decisions (ADRs): <https://github.com/j5ik2o/event-store-adapter/tree/main/docs/adr>

This crate follows common contract v5, and its major version 5 matches the contract revision. Read the specification and the decisions before you change behavior that the contract defines.

Some files in this repository are pinned copies or data from the hub. CI compares their SHA-256 values with the values written in the `lint` job of `.github/workflows/ci.yml`, so changing one of them by hand makes CI fail.

| Path | Content | Rule |
|:--|:--|:--|
| `conformance/` | Conformance test data, data version 1.3.0 | Check it with `python3 tools/conformance/manifest.py verify`. CI also compares the SHA-256 of `conformance/manifest.json` |
| `api-surface/surface.json` | The hub's shape definition for the public API, version 0.1.0 | Do not edit it by hand. CI compares its SHA-256 |
| `user-docs/template.json` and `tools/user-docs/check.py` | The hub's template for the user documentation and its checker, version 1.0.0 | Do not edit them by hand. CI compares their SHA-256 values |
| `api-surface/rs.json` and `user-docs/repo.json` | This repository's own mapping to the public API and its own settings for the checker | Edit them for this repository |

## Local setup

- Rust toolchain: no file pins the toolchain. CI builds and tests with stable. The minimum supported Rust version is 1.94.1 (`rust-version` in `lib/Cargo.toml`).
- Formatting: use nightly rustfmt. The settings are in `rustfmt.toml` (`tab_spaces = 2`, `max_width = 120`). Install nightly if you do not have it.
- API surface check: `cargo test -p event-store-adapter-api-surface-rs` and `cargo test --workspace` need the nightly of a fixed date, because the check builds rustdoc JSON with it.
- Docker: the tests of the DynamoDB and Bigtable storages, the migration, the example, and the conformance runs for DynamoDB and Bigtable start containers with testcontainers, so Docker must be running. They use DynamoDB Local 3.3.1 and the Bigtable emulator (google-cloud-cli 587.0.0-emulators), both pinned by digest. The tests connect to DynamoDB Local with a dummy region and dummy credentials, and to the emulator without credentials. The Memory and SQLite tests do not need Docker.
- Python 3: `tools/user-docs/check.py` and `tools/conformance/manifest.py` use only the standard library. The tests in `.github/test_*.py` need the `semver` package. CI uses Python 3.15. `tools/user-docs/check.py` requires Python 3.10 or later.
- actionlint: CI installs it with `go install` (Go 1.27) and runs it on the workflow files.
- cargo-deny: it audits the dependencies with the settings in `deny.toml`.

```sh
rustup install nightly
rustup toolchain install nightly-2026-09-27 --profile minimal
pip install semver
go install github.com/rhysd/actionlint/cmd/actionlint@v1.7.12
```

## Checks

CI (`.github/workflows/ci.yml`) runs on pushes to `main`, on pull requests to `main`, and on a daily schedule. Run the checks that match your change before you open a pull request.

| Job | What it checks | Docker |
|:--|:--|:--|
| `lint` | The conformance data, the SHA-256 values of the pinned files, `tools/user-docs/check.py`, and nightly rustfmt | No |
| `release-control` | The tests of the release scripts in `.github/` and actionlint | No |
| `test-lib` | All tests of the library with all features | Yes |
| `test-conformance-migration` | The tests of the conformance runner and the migration CLI with all features | Yes |
| `api-surface` | The shape of the public API, with the pinned nightly | No |
| `feature-matrix` | The library for each feature set (details below) | No |
| `clippy` | Clippy with warnings denied for the same seven feature sets | No |
| `conformance` | The conformance test data for each storage: `memory`, `dynamodb`, `sqlite`, and `bigtable` | `dynamodb` and `bigtable` only |
| `audit` | `cargo deny check advisories licenses` | No |
| `CI Success` | Collects `lint`, `test-lib`, `test-conformance-migration`, `api-surface`, `feature-matrix`, `clippy`, `release-control`, and `conformance`. It does not include `audit` | No |

Representative local commands based on CI are listed below. The complete commands and all feature combinations are in [the CI workflow](.github/workflows/ci.yml); the feature-matrix and clippy configurations are also described below. `cargo +nightly fmt --all -- --check` is the form that `AGENTS.md` gives for the `cargo fmt -- --check` that the `lint` job runs with the nightly toolchain. The `audit` job runs `cargo deny check advisories licenses` through the cargo-deny action.

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

`AGENTS.md` also lists the following commands for the whole workspace. CI does not run them. The test needs Docker and the nightly of a fixed date (see "Local setup"):

```sh
cargo build --workspace --all-targets --all-features
cargo test --workspace --all-features
```

- `feature-matrix` runs the tests for `--no-default-features`, `--features sqlite`, and `--features sqlite-migration`. It only builds `--features dynamodb`, `--features bigtable`, `--features bigtable-migration`, and `--all-features`.
- `feature-matrix` also checks the dependency graph with `cargo tree`. Without features, no cloud SDK and no SQLite crate may appear. With only `sqlite-migration`, no cloud SDK may appear. With only `bigtable-migration`, no AWS, SQLite, or Google authentication crate may appear. It also checks that `lib/src/` has no hand-written `unsafe impl`.
- `clippy` runs `cargo clippy --workspace --all-targets <flags> -- -D warnings` for seven sets of flags: `--no-default-features`; `--no-default-features` with `--features <feature>` for each of `dynamodb`, `sqlite`, `sqlite-migration`, `bigtable`, and `bigtable-migration`; and `--all-features`.
- The `dynamodb`, `sqlite`, and `bigtable` legs of `conformance` add `--features <storage>` after `--release` and set `--backend <storage>`. The `dynamodb` and `bigtable` legs start the container themselves.
- Unit tests live next to the code in `*_test.rs` files that are attached with `#[cfg(test)] #[path = "x_test.rs"] mod tests;`. Integration tests live in the `tests/` directory of each crate.
- `tools/user-docs/check.py` checks the README, the storage schema documents, and the migration guides, in English and Japanese. It checks the heading structure, whether the targets of relative links exist, and the first line of each Japanese file. It does not check the text itself.

## Design history

- `docs/design/next-major.md` (Japanese: `docs/design/next-major.ja.md`) is the design record from building v5. For current usage, read the README and the current code.
- The decisions shared by all implementations are in the ADRs of the hub repository (see "Shared contract and decisions").
- `CHANGELOG.md` records the releases up to 5.0.1 and is no longer updated. The record of later changes is the release notes on [GitHub Releases](https://github.com/j5ik2o/event-store-adapter-rs/releases).
- `aidlc/` holds generated artifacts of earlier work. They are historical records.

## Changes and releases

- Write the commit messages and the pull request titles in the Conventional Commits form. The types are `build`, `ci`, `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `revert`, and `chore`, and `BREAKING CHANGE` is also recognized. A `!` right after the type, or `BREAKING CHANGE:` or `BREAKING-CHANGE:` in the body, means a major bump. `feat` and `revert` mean a minor bump. The other types mean a patch bump.
- Pull requests are merged by squash only, so the title of a pull request becomes one line of the release notes.
- In a pull request, describe the summary, the motivation, the test results, and the scope of the impact, link the related issue as `Closes #123`, and add `Steps to verify` that a reviewer can reproduce.
- Version bump: the `lib-bump-version` workflow runs only when started by hand (`workflow_dispatch`). The input `level` is one of `auto`, `patch`, `minor`, and `major`. When the input `version` is not given and the version in `lib/Cargo.toml` is a pre-release, only `major` is accepted. The optional input `version` (`X.Y.Z`) bumps to that version without using `level` or this restriction; the version must be greater than the greatest existing `v*` tag, and no tag of the same name may exist. The workflow runs `cargo set-version`, commits `version up to vX.Y.Z`, pushes the tag, and creates a GitHub Release with generated release notes.
- Release: the `lib-release` workflow starts when a tag of the form `vX.Y.Z` or `vX.Y.Z-*` is pushed. It checks that the tag matches the version in `lib/Cargo.toml`, authenticates with `rust-lang/crates-io-auth-action`, and runs `cargo publish -p event-store-adapter-rs`.
- Dependency updates: Renovate opens pull requests whose titles start with `chore(deps):`. It merges minor, patch, pin, and digest updates and updates of devDependencies automatically. It does not update `takt` and `rustdoc-types`.
- When CI fails on `main`, the `main-ci-failure` workflow opens an issue with the label `ci-failure-on-main`, or adds a comment to the open one. A later successful run closes the issue.
