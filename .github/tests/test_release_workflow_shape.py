"""ワークフローの形のテスト（設計の追記 design-r6.md §3・§5・§7、再レビュー N1〜N3）。"""
import ast
import re
import sys
import unittest

from hypothesis import given, settings
from hypothesis import strategies as st

from release_test_support import BUMP, GITHUB, RELEASE, job, job_names, steps
from release_workflow_canonical import CANONICAL, release_has_gate
from workflow_runner import compare, job_steps

CI = (GITHUB / 'workflows' / 'ci.yml').read_text()
PUBLISH = job(RELEASE, 'publish')


def step_by_id(workflow, job_name, step_id):
    return next(item for item in job_steps(workflow, job_name) if item['id'] == step_id)


def all_runs(workflow, job_name=None):
    names = [job_name] if job_name else job_names(workflow)
    return [item for name in names for item in job_steps(workflow, name) if item['run']]


class ReleaseWorkflowShapeTest(unittest.TestCase):
    def test_only_release_tags_start_the_release(self):
        self.assertIn("on:\n  push:\n    tags:\n      - 'v[0-9]+.[0-9]+.[0-9]+'\nenv:", RELEASE)
        self.assertNotIn('workflow_dispatch', RELEASE)
        self.assertNotIn('-*', RELEASE.split('jobs:')[0])

    def test_the_file_and_the_name_are_kept_for_trusted_publishing(self):
        self.assertTrue((GITHUB / 'workflows' / 'lib-release.yml').is_file())
        self.assertTrue(RELEASE.startswith('name: lib-release\n'))
        self.assertNotIn('environment:', RELEASE)

    def test_the_jobs_and_their_order(self):
        self.assertEqual(job_names(RELEASE), ['package', 'publish', 'confirm', 'github-release'])
        self.assertIn('\n    needs: package\n', PUBLISH)
        self.assertIn("\n    if: needs.package.outputs.action == 'publish'\n", PUBLISH)
        confirm = job(RELEASE, 'confirm')
        self.assertIn('\n    needs: [package, publish]\n', confirm)
        self.assertIn("\n    if: always() && needs.package.result == 'success' && (needs.package.outputs.action == "
                      "'published' || needs.publish.result == 'success')\n", confirm)
        self.assertIn('\n    timeout-minutes: 40\n', confirm)
        self.assertIn("\n    needs: confirm\n    if: always() && needs.confirm.result == 'success'\n",
                      job(RELEASE, 'github-release'))

    def test_only_the_publish_job_can_request_an_oidc_token(self):
        """I20：id-token: write は publish ジョブだけ。ほかのジョブにもワークフローにも無い。"""
        self.assertEqual(RELEASE.count('id-token:'), 1)
        self.assertIn('\n    permissions:\n      contents: read\n      pull-requests: read\n      id-token: write\n',
                      PUBLISH)
        self.assertNotIn('\npermissions:', RELEASE.split('\njobs:\n')[0])

    def test_the_publish_job_builds_nothing(self):
        """I20：publish ジョブの cargo は --version・package --no-verify・publish --no-verify だけ。"""
        commands = []
        for item in all_runs(RELEASE, 'publish'):
            commands += re.findall(r'(?:^|\$\()cargo ([^\n]*)', item['run'], re.MULTILINE)
        self.assertEqual(sorted(commands), sorted([
            '--version)', 'package --no-verify --locked -p event-store-adapter-rs',
            'publish --no-verify --locked -p event-store-adapter-rs']))
        for item in all_runs(RELEASE, 'publish'):
            self.assertNotRegex(item['run'], r'\b(pip|pip3|npm|cargo (build|test|check|run|install|clippy|doc))\b')

    def test_the_publish_job_uses_only_the_standard_library(self):
        """再レビュー N3：publish ジョブは pip install をせず、スクリプトは標準ライブラリだけを import する。"""
        self.assertNotRegex(PUBLISH, r'\bpip3?\b|requirements')
        for script in ('crates-io.py', 'release-gate.py'):
            tree = ast.parse((GITHUB / script).read_text())
            modules = {alias.name.split('.')[0] for node in ast.walk(tree) if isinstance(node, ast.Import)
                       for alias in node.names}
            modules |= {node.module.split('.')[0] for node in ast.walk(tree)
                        if isinstance(node, ast.ImportFrom) and node.module}
            self.assertLessEqual(modules, set(sys.stdlib_module_names), script)
        self.assertNotRegex((GITHUB / 'release-gate-inputs.sh').read_text(), r'\bpip3?\b')

    def test_the_steps_before_the_authentication(self):
        # 関門と事前の確認は認証の直前（#375 の Devin の指摘：判定から公開までの間を短くする）
        ids = [item['id'] for item in job_steps(RELEASE, 'publish')]
        self.assertEqual(ids, [None, None, 'rust', 'download_verified', 'crate_check', 'release_gate_again',
                               'precheck_again', 'auth', 'publish'])
        for step_id in ids[:7]:
            self.assertIsNone(job_steps(RELEASE, 'publish')[ids.index(step_id)]['if'], step_id)
        for step_id in ('auth', 'publish'):
            self.assertEqual(step_by_id(RELEASE, 'publish', step_id)['if'],
                             "steps.precheck_again.outputs.action == 'publish'", step_id)
        package_ids = [item['id'] for item in job_steps(RELEASE, 'package')]
        self.assertEqual(package_ids, [None, None, 'release_gate', 'rust', 'build_version', 'precheck', 'lock',
                                       'verify', 'upload_verified'])
        self.assertNotIn('always()', '\n'.join(steps(RELEASE, 'package') + steps(RELEASE, 'publish')))

    def test_the_hashes_are_recorded_before_any_build_code_runs(self):
        """#375 の Devin の指摘：Cargo.lock と .crate の SHA-256 は、検証のビルド（verify）の前の手順の出力。"""
        lock = step_by_id(RELEASE, 'package', 'lock')['run'].split('\n')
        self.assertEqual(lock[:3], ['set -o pipefail', 'cargo generate-lockfile',
                                    'cargo package --no-verify --locked -p event-store-adapter-rs'])
        self.assertEqual(step_by_id(RELEASE, 'package', 'verify')['run'],
                         'cargo package --locked -p event-store-adapter-rs')
        package = job(RELEASE, 'package')
        for name in ('artifact', 'sha256', 'lock_sha256'):
            self.assertIn(f'      {name}: ${{{{ steps.lock.outputs.{name} }}}}\n', package)
        self.assertNotRegex(package, r'steps\.verify\.outputs')

    def test_the_token_reaches_only_cargo_publish(self):
        self.assertEqual(RELEASE.count('steps.auth.outputs.token'), 1)
        self.assertEqual(step_by_id(RELEASE, 'publish', 'publish')['env'],
                         {'CARGO_REGISTRY_TOKEN': '${{ steps.auth.outputs.token }}'})
        self.assertEqual(RELEASE.count('CARGO_REGISTRY_TOKEN'), 1)
        self.assertNotIn('secrets.', RELEASE)

    def test_every_action_is_pinned_by_sha(self):
        uses = re.findall(r'^ *(?:- )?uses: ([^\n]*)$', RELEASE, re.MULTILINE)
        self.assertTrue(uses)
        for value in uses:
            self.assertRegex(value, r'^[a-z0-9-]+/[a-z0-9-]+@[0-9a-f]{40} # v[0-9.]+$')
        self.assertNotIn('dtolnay/rust-toolchain', RELEASE)

    def test_the_artifact_is_uploaded_once_per_attempt(self):
        """再レビュー N2：成果物の名前は試みごと。overwrite を使わない。"""
        verified = step_by_id(RELEASE, 'package', 'lock')
        self.assertIn('  echo "artifact=verified-crate-${RUN_ATTEMPT}"\n} >> "$GITHUB_OUTPUT"', verified['run'])
        self.assertEqual(verified['env']['RUN_ATTEMPT'], '${{ github.run_attempt }}')
        upload = step_by_id(RELEASE, 'package', 'upload_verified')
        self.assertEqual(upload['with'], {'name': '${{ steps.lock.outputs.artifact }}',
                                          'path': '${{ runner.temp }}/verified-crate', 'retention-days': '7',
                                          'if-no-files-found': 'error'})
        self.assertIn('      artifact: ${{ steps.lock.outputs.artifact }}\n', job(RELEASE, 'package'))
        self.assertNotIn('overwrite', RELEASE)

    def test_the_artifact_is_downloaded_by_name_outside_the_workspace(self):
        """再レビュー N1・(b)：name: で取り寄せ（無ければ失敗）、RUNNER_TEMP の新しいディレクトリへ。"""
        download = step_by_id(RELEASE, 'publish', 'download_verified')
        self.assertEqual(download['with'], {'name': '${{ needs.package.outputs.artifact }}',
                                            'path': '${{ runner.temp }}/verified-crate'})
        self.assertNotIn('pattern:', RELEASE)
        self.assertNotIn('merge-multiple', RELEASE)

    def test_only_a_regular_cargo_lock_is_taken_from_the_artifact(self):
        """再レビュー N1：成果物から使うのは Cargo.lock だけ。copy-lock が通常のファイルかを確かめる。"""
        check = step_by_id(RELEASE, 'publish', 'crate_check')['run']
        lines = [line.strip() for line in check.split('\n') if line.strip()]
        self.assertEqual(lines[0], 'python3 "${GITHUB_WORKSPACE}"/.github/crates-io.py copy-lock --from '
                                   '"${RUNNER_TEMP}/verified-crate" \\')
        self.assertEqual(lines[1], '--to "${GITHUB_WORKSPACE}/Cargo.lock" --sha256 "$VERIFIED_LOCK_SHA256"')
        self.assertEqual(step_by_id(RELEASE, 'publish', 'crate_check')['env']['VERIFIED_LOCK_SHA256'],
                         '${{ needs.package.outputs.lock_sha256 }}')
        self.assertEqual(lines[2], 'cargo package --no-verify --locked -p event-store-adapter-rs')
        self.assertIn('check-crate', lines[3])
        # publish ジョブで取り寄せたディレクトリに触れるのは、取り寄せ（path:）と copy-lock だけ
        self.assertEqual(PUBLISH.count('/verified-crate'), 2)

    def test_the_github_release_job_is_serialized_across_versions(self):
        """#375 の CodeRabbit の指摘：Latest を決めてから作るまでに、ほかの版のリリースが作られない。"""
        self.assertIn('\n    concurrency:\n      group: github-release\n      cancel-in-progress: false\n',
                      job(RELEASE, 'github-release'))
        self.assertEqual(RELEASE.count('group: github-release'), 1)
        run = next(item for item in job_steps(RELEASE, 'github-release') if item['id'] == 'github_release')['run']
        # リリースの一覧の読み直しとリモートのタグは、Latest を決める直前（ノートを書いたあと）
        self.assertLess(run.index('printf'), run.rindex('gh api --paginate'))
        self.assertLess(run.rindex('gh api --paginate'), run.index('git ls-remote --tags origin'))
        self.assertLess(run.index('git ls-remote --tags origin'), run.index('gh release create'))

    def test_the_publish_job_is_serialized(self):
        self.assertIn('\n    concurrency:\n      group: release-publish\n      cancel-in-progress: false\n', PUBLISH)
        self.assertNotIn('concurrency', job(RELEASE, 'package'))

    def test_checkouts_do_not_keep_credentials(self):
        for workflow in (RELEASE, BUMP):
            for text in re.findall(r'uses: actions/checkout@[^\n]*\n(?:        [^\n]*\n)*', workflow):
                self.assertIn('persist-credentials: false', text)

    def test_runs_with_a_pipe_set_pipefail(self):
        """パイプを持つ手順は、最初の行で set -o pipefail（GitHub の既定の bash -e には pipefail が無い）。"""
        for workflow in (RELEASE, BUMP):
            for name in job_names(workflow):
                for item in all_runs(workflow, name):
                    if re.search(r'(?<!\|)\|(?!\|)', item['run']):
                        self.assertEqual(item['run'].split('\n')[0], 'set -o pipefail',
                                         f'{name}/{item["id"] or item["name"]}')

    def test_the_canonical_copy_matches(self):
        # 正本との一致は release_has_gate の条件（uses: の版と注釈だけは違ってよい。#375 の Devin の指摘）
        self.assertTrue(release_has_gate(RELEASE))
        self.assertTrue(release_has_gate(CANONICAL))


class CanonicalProperties(unittest.TestCase):
    @settings(max_examples=200, deadline=None, database=None)
    @given(st.data())
    def test_any_line_change_other_than_an_action_version_loses_the_gate(self, data):
        lines = RELEASE.split('\n')
        index = data.draw(st.integers(0, len(lines) - 1))
        replacement = data.draw(st.sampled_from(['', '#', lines[index] + ' ', lines[index].replace('a', 'b'),
                                                 '      - run: true']))
        if replacement == lines[index]:
            return
        changed = '\n'.join(lines[:index] + [replacement] + lines[index + 1:])
        self.assertFalse(release_has_gate(changed))

    def test_an_action_version_may_change(self):
        # 今の uses: の行から選ぶ（固定の SHA だと、更新のあとに置き換えが空振りする。#375 の CodeRabbit の指摘）
        pins = sorted(set(re.findall(r'@[0-9a-f]{40} # v[0-9.]+', RELEASE)))
        self.assertTrue(pins)
        for pin in pins:
            with self.subTest(pin):
                changed = RELEASE.replace(pin, '@' + 'f' * 40 + ' # v99.0.0')
                self.assertNotEqual(changed, RELEASE)
                self.assertTrue(release_has_gate(changed))


class ExpressionComparisonTest(unittest.TestCase):
    def test_mixed_types_compare_as_numbers_like_github_actions(self):
        """#375 の CodeRabbit の指摘。https://docs.github.com/en/actions/reference/workflows-and-actions/expressions"""
        cases = [
            (False, '', True), (False, '0', True), (True, '1', True), (True, 'true', False), (None, '', True),
            (None, False, True), (1, '1', True), (1, '1.0', True), (0, 'x', False), ('x', 'X', True),
            ('true', 'false', False), (None, None, True), (True, True, True), (float('nan'), float('nan'), False),
        ]
        for left, right, equal in cases:
            with self.subTest(left=left, right=right):
                self.assertEqual(compare(left, '==', right), equal)
                self.assertEqual(compare(left, '!=', right), not equal)
                self.assertEqual(compare(right, '==', left), equal)


class BumpWorkflowShapeTest(unittest.TestCase):
    def test_nothing_is_pushed_to_main(self):
        self.assertNotRegex(BUMP, r'git push origin main|HEAD:main|:refs/heads/main')
        pushes = re.findall(r'git push origin "([^"]*)"', BUMP)
        self.assertEqual(pushes, ['HEAD:refs/heads/${BRANCH}', 'refs/tags/${RELEASE_TAG}'])

    def test_the_old_release_steps_are_gone(self):
        for removed in ('actions/create-release', 'cargo set-version', 'cargo-edit', 'cargo-install'):
            self.assertNotIn(removed, BUMP)

    def test_the_version_branch_changes_only_lib_cargo_toml(self):
        commit = next(item for item in job_steps(BUMP, 'bump-version') if item['id'] == 'version_commit')
        self.assertIn('python3 "${GITHUB_WORKSPACE}"/.github/release-gate.py set-version "$NEXT_VERSION"', commit['run'])
        self.assertIn('git add lib/Cargo.toml\n', commit['run'])
        self.assertNotIn('git add .', commit['run'])

    def test_the_tag_job_runs_on_main_pushes_of_lib_cargo_toml(self):
        self.assertIn('on:\n  push:\n    branches:\n      - main\n    paths:\n      - lib/Cargo.toml\n', BUMP)
        tag = job(BUMP, 'tag-release')
        self.assertIn('\n    concurrency:\n      group: release-tag\n      cancel-in-progress: false\n', tag)
        self.assertIn("github.repository == 'j5ik2o/event-store-adapter-rs'", tag)
        self.assertEqual(re.findall(r'secrets\.([A-Z_]+)', tag), ['PERSONAL_ACCESS_TOKEN'])

    def test_tag_pushing_is_on_only_with_the_gated_release_workflow(self):
        # 'false'（公開を止める pull request）はいつでもよい。'true' は関門があるときだけ（#375 の CodeRabbit の指摘）
        value = re.findall(r"\nenv:\n  PUSH_RELEASE_TAG: '([^']*)'\n", BUMP)
        self.assertIn(value, (['true'], ['false']))
        if value == ['true']:
            self.assertTrue(release_has_gate(RELEASE))

    def test_the_pat_is_used_only_to_push_and_to_open_the_pull_request(self):
        users = [item['id'] for name in job_names(BUMP) for item in job_steps(BUMP, name)
                 if 'secrets.PERSONAL_ACCESS_TOKEN' in item['text']]
        self.assertEqual(users, ['version_commit', 'version_pr', 'release_tag'])

    def test_the_review_thread_check_is_started_for_the_pull_request(self):
        self.assertIn('gh workflow run review-thread-resolution.yml --repo "$GITHUB_REPOSITORY" --ref main '
                      '-f pr_number="$PR_NUMBER"', BUMP)
        self.assertIn('\n    permissions:\n      contents: read\n      actions: write\n', job(BUMP, 'bump-version'))


class CiShapeTest(unittest.TestCase):
    def test_release_control_runs_the_release_tests_and_actionlint(self):
        control = job(CI, 'release-control')
        self.assertIn('pip install -r .github/tests/requirements.txt', control)
        self.assertIn("python3 -m unittest discover -s .github/tests -p 'test_*.py' -v", control)
        self.assertIn('actionlint', control)

    def test_ci_success_waits_for_the_release_model(self):
        model = job(CI, 'release-model')
        self.assertIn('bash .github/quint/run-quint.sh ci', model)
        # #375：quint が評価器を認証なしで取り寄せないよう、認証つきで先に置き、QUINT_HOME で指す
        fetch = model.index('bash .github/quint/fetch-evaluator.sh "${RUNNER_TEMP}/quint-home"')
        self.assertLess(fetch, model.index('bash .github/quint/run-quint.sh ci'))
        self.assertIn('GH_TOKEN: ${{ github.token }}', model[:fetch])
        self.assertIn('QUINT_HOME: ${{ runner.temp }}/quint-home', model[fetch:])
        needs = re.search(r'\n    needs: \[([^\]]*)\]', job(CI, 'ci-success')).group(1)
        self.assertIn('release-model', needs)
        self.assertIn('RELEASE_MODEL_RESULT', job(CI, 'ci-success'))


if __name__ == '__main__':
    unittest.main()
