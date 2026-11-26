#!/usr/bin/env bash
# release_path.qnt の検査を流す。
# 使い方:
#   bash .github/quint/run-quint.sh ci          CI 用：typecheck、quint test の全件（決まった手順。rsPathsTest を含む）、
#                                               違反なしを期待する rs の3つと、違反を期待する5つ（rs の4つと cancelledLeftAlone）の
#                                               quint run（500 サンプル）。ほかの言語のインスタンスの乱数の試行は full だけ
#   bash .github/quint/run-quint.sh full [N]    全 instance を N サンプル（既定 3000）で流す。モデルを変えた PR で手元で流し、結果を PR に書く
# 環境変数 QUINT：quint のコマンド（既定 quint）。CI は npx で版を固定して使う。
set -euo pipefail
cd "$(dirname "$0")"
# 結果の行（[ok]・[violation]）を先頭から読むので、色の付いた出力にしない（FORCE_COLOR=1 の環境でも）
export FORCE_COLOR=0 NO_COLOR=1
read -r -a Q <<< "${QUINT:-quint}"
mode="${1:-ci}"
SEED=7
STEPS=30
POSITIVE_ALL=(java javaReleasesOnly kotlin kotlinNoManual kotlinReleasesOnly scala scalaNoManual scalaReleasesOnly rs rsNoManual rsReleasesOnly go javaR2aOnly scalaBeforeR5 javaR2b javaR2bReleasesOnly)
# 前提を外すと破れることを示す instance と、破れる不変条件
NEGATIVE_ALL=(kotlinNoRequired:legacyBeforeNewer scalaNoRequired:legacyBeforeNewer goManualTags:publishedOnlyFromReleasePR
  jsBefore1161:neverTimesOutAfterPublished wholeRerun:noDuplicatePublish noDeploymentCheck:noDuplicatePublish
  javaR2aPushesTags:noDuplicatePublish scalaOldBumpLive:legacyBeforeNewer noOtherDeploymentCheck:publishedMonotonic
  stepRecordDropped:noDuplicatePublish unknownIgnored:noDuplicatePublish noneSkipsQuery:noDuplicatePublish
  noneSkipsQuery:publishedMonotonic noLatestTagCheck:publishedMonotonic
  cancelledLeftAlone:stopsAreRecoverable cancelledLeftAlone:untaggedReleaseRecoverable
  rsNoPrecheck:noDuplicatePublish rsNoLatestTagCheck:publishedMonotonic rsShortConfirm:neverTimesOutAfterPublished rsReleaseNotGated:releaseOnlyAfterConfirmed)
POSITIVE_CI=(rs rsNoManual rsReleasesOnly)
NEGATIVE_CI=(rsNoPrecheck:noDuplicatePublish rsNoLatestTagCheck:publishedMonotonic rsShortConfirm:neverTimesOutAfterPublished
  rsReleaseNotGated:releaseOnlyAfterConfirmed cancelledLeftAlone:stopsAreRecoverable)
TESTS=(rsPathsTest javaPathsTest kotlinPathsTest scalaPathsTest javaR2aOnlyTest javaR2bPathsTest confirmBoundaryTest tagRecoveryTest)

if [[ "$mode" == ci ]]; then
  samples=500
  positive=("${POSITIVE_CI[@]}")
  negative=("${NEGATIVE_CI[@]}")
elif [[ "$mode" == full ]]; then
  samples="${2:-3000}"
  positive=("${POSITIVE_ALL[@]}")
  negative=("${NEGATIVE_ALL[@]}")
else
  echo "usage: run-quint.sh ci|full [samples]" >&2
  exit 2
fi

failed=0
log="$(mktemp)"
trap 'rm -f "$log"' EXIT
"${Q[@]}" typecheck release_path.qnt
"${Q[@]}" typecheck release_path_test.qnt
echo "typecheck: ok"
for main in "${TESTS[@]}"; do
  echo "## quint test ${main}"
  "${Q[@]}" test release_path_test.qnt --main="$main" || failed=1
done
for main in "${positive[@]}"; do
  echo "## ${main}  (expect: no violation of safety)"
  if "${Q[@]}" run release_path.qnt --main="$main" --invariant=safety --max-samples="$samples" \
    --max-steps="$STEPS" --seed="$SEED" > "$log" 2>&1 && grep -qE '^\[ok\]' "$log"; then
    grep -E '^\[ok\]' "$log"
  else
    tail -n 40 "$log"
    failed=1
  fi
done
for item in "${negative[@]}"; do
  main="${item%%:*}"
  invariant="${item#*:}"
  echo "## ${main}  invariant=${invariant}  (expect: violation)"
  # 反例は、非ゼロの終了と [violation] の両方。実行時の誤り（[error]）は反例と数えず、診断を出して失敗にする
  if ! "${Q[@]}" run release_path.qnt --main="$main" --invariant="$invariant" --max-samples="$samples" \
    --max-steps="$STEPS" --seed="$SEED" > "$log" 2>&1 && grep -qE '^\[violation\]' "$log"; then
    grep -E '^\[violation\]' "$log"
  else
    echo "expected a violation of ${invariant} in ${main}, found none"
    tail -n 40 "$log"
    failed=1
  fi
done
if [[ "$mode" == full ]]; then
  echo "## java  invariant=neverTimesOutAfterPublished  (expect: no violation)"
  if "${Q[@]}" run release_path.qnt --main=java --invariant=neverTimesOutAfterPublished --max-samples="$samples" \
    --max-steps="$STEPS" --seed="$SEED" > "$log" 2>&1 && grep -qE '^\[ok\]' "$log"; then
    grep -E '^\[ok\]' "$log"
  else
    tail -n 40 "$log"
    failed=1
  fi
fi
exit "$failed"
