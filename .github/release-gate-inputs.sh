#!/usr/bin/env bash
# 関門 (release-gate.py check) にかける事実を、指定のディレクトリに書く。
# event-store-adapter-scala の .github/release-gate-inputs.sh（48e4256）を写し、版のファイルを lib/Cargo.toml に替え、
# 公開先に必要な版の照会を除き、関連する pull request を1つずつ読み直すようにしたもの（設計の追記 design-r6.md §6・§17）。
# 使い方: release-gate-inputs.sh <コミット> <出力先>
# 環境変数: GH_TOKEN、GITHUB_REPOSITORY、PULLS_RETRY_SECONDS（既定 10。PR の照会を再試行する間隔）
# 親（またはコミット）に lib/Cargo.toml が無いときは、parent-version（または version）を書かない。関門はそれを「無い」と読む。
set -euo pipefail

sha="$1"
out="$2"
version_file=lib/Cargo.toml
mkdir -p "$out"

git fetch --no-tags origin main
on_main=false
if git merge-base --is-ancestor "$sha" FETCH_HEAD; then
  on_main=true
fi
echo "$on_main" > "${out}/on-main"
# 親の数。通常のマージ（親が2つ）は関門が拒否する
git rev-list --parents -n 1 "$sha" | awk '{print NF - 1}' > "${out}/parents"
if git cat-file -e "${sha}^:${version_file}" 2> /dev/null; then
  git cat-file blob "${sha}^:${version_file}" > "${out}/parent-version"
fi
if git cat-file -e "${sha}:${version_file}" 2> /dev/null; then
  git cat-file blob "${sha}:${version_file}" > "${out}/version"
fi
git diff --name-only "${sha}^" "$sha" > "${out}/changed-files"
git ls-remote --tags origin > "${out}/remote-tags"

# マージの直後は、コミットと PR の関連付けがまだ返らないことがある。一時的な失敗と空の応答だけを数回やり直す。
attempts=5
read_ok=false
for attempt in $(seq 1 "$attempts"); do
  if gh api "repos/${GITHUB_REPOSITORY}/commits/${sha}/pulls" > "${out}/pulls.json"; then
    read_ok=true
    if [[ "$(tr -d '[:space:]' < "${out}/pulls.json")" != "[]" ]]; then
      break
    fi
    echo "no pull request is associated with ${sha} yet (${attempt}/${attempts})"
  else
    read_ok=false
    echo "could not read the pull requests of ${sha} (${attempt}/${attempts})"
  fi
  if [[ "$attempt" -lt "$attempts" ]]; then
    sleep "${PULLS_RETRY_SECONDS:-10}"
  fi
done
if [[ "$read_ok" != true ]]; then
  echo "::error::could not read the pull requests of ${sha}; re-run the job"
  exit 1
fi
# 一覧の項目には changed_files・additions・deletions が無い。関門が pull request の全体を確かめられるよう、1つずつ読み直す
numbers="$(python3 -c 'import json, sys; print(" ".join(str(p["number"]) for p in json.load(open(sys.argv[1]))))' \
  "${out}/pulls.json")"
details=()
for number in $numbers; do
  gh api "repos/${GITHUB_REPOSITORY}/pulls/${number}" > "${out}/pull-${number}.json"
  details+=("${out}/pull-${number}.json")
done
python3 -c 'import json, sys; json.dump([json.load(open(p)) for p in sys.argv[2:]], open(sys.argv[1], "w"))' \
  "${out}/pulls.json" ${details[@]+"${details[@]}"}
