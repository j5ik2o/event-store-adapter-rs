# Release path model (Quint)

`release_path.qnt` models the release path: the version pull request, the merge, the release gate, the tag, the publish job, the publish target (for crates.io, accepted means published), the checks after publishing and the GitHub release. `release_path_test.qnt` runs fixed paths through the model.

The model was written for the release path design of the event-store-adapter repositories (the js shape of j5ik2o/event-store-adapter-js#1158 applied to java, kotlin, scala, rs and go). This repository is event-store-adapter-rs. The model is the one of j5ik2o/event-store-adapter-scala (`.github/quint/` at `48e4256`) with one constant and one invariant added for rs (R6):

- `RELEASE_AFTER_CONFIRM`: the GitHub release is created only after the check after publishing (the sparse index serves the version) is `Confirmed`. The other instances keep `false` (the Maven Central shape: the release follows `PUBLISHED`), so their behaviour does not change.
- I17 `releaseOnlyAfterConfirmed`: in `safety` under `RELEASE_AFTER_CONFIRM`. The instance `rsReleaseNotGated` drops the constant and breaks it.

The rs instances have the shape of `lib-release.yml`:

- `INIT_VER = 1, INIT_PRE = false`: `lib/Cargo.toml` holds `5.2.0`, which is on crates.io (`LEGACY = Set()`, `REQUIRED_PUBLISHED = Set()`).
- `ASYNC_PORTAL = false`: crates.io publishes what it accepts; there is no deployment that is processed later.
- `PRECHECK_SEES_DEPLOYMENTS = true`, `RECORD_VIEW = false`: the precheck reads the crates.io API, which returns an accepted version at once. It does not read records of earlier runs.
- `CONFIRM_ATTEMPTS = 8 > MAX_DELAY = 6`: the 30-minute wait for the sparse index covers its delay.

How the jobs map to the model: the `package` and `publish` jobs together are one `publishJob` action. Its decision (`publishGate`) is the gate and the precheck of the `publish` job, right before the authentication; the gate and the precheck of the `package` job filter earlier (a failure there is `publishFailed`, and its recovery is `rerunPublishJob`). The concurrency group of the `publish` job is `cancelPendingPublish`. The success of the `publish` job is `jobOk`, which the model sets at the first check after publishing (the Maven Central shape, where the wait is inside the job); since the release needs `Confirmed` (I17), the difference does not affect safety. Outside the model: the version position in `lib/Cargo.toml` (P4r, P5), the checksum of the verified crate (I19), the OIDC scope (I20) and `trustpub_data` (I21), which the tests in `.github/tests/` check.

`rs`, `rsNoManual` and `rsReleasesOnly` are expected to have no violation. `rsNoPrecheck` (the precheck reads only the delayed sparse index), `rsNoLatestTagCheck`, `rsShortConfirm` (a wait shorter than the index delay, as in js#1161) and `rsReleaseNotGated` are expected to violate an invariant. `rsPathsTest` runs the fixed rs paths: the release waits for the index, an index timeout is recovered by re-running only the check, and a publish job that fails after crates.io accepted the crate does not publish twice.

Outside the model: a tag made by hand on a commit before R6 (`v5.2.0` at `35b83b6` or earlier). Such a commit runs its old `lib-release.yml` (tags `vX.Y.Z` and `vX.Y.Z-*`, no gate). CONTRIBUTING forbids such tags; tag rulesets would close it structurally.

The instances of the other languages (java, kotlin, scala, go) and the migration and failure instances are kept unchanged, so that the model stays the same in every repository until it moves to a shared source.

## Running

```sh
bash .github/quint/run-quint.sh ci          # what CI runs: typecheck, every quint test, 3 rs and 5 negative instances with 500 samples
bash .github/quint/run-quint.sh full 3000   # every instance; run it locally when the model changes and put the result in the pull request
```

The script uses `quint` from `PATH`, or the command in `QUINT` (CI uses `npx --yes @informalsystems/quint@0.32.0`).

When `release-gate.py` or `crates-io.py` changes what it decides, change the model first, then the code.
