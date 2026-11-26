"""lib-bump-version.yml の2つのジョブを、手元の git の履歴と偽の gh で流す（設計の追記 design-r6.md §5）。

版上げのジョブ：枝 release/vX.Y.Z に lib/Cargo.toml の版だけのコミットを作って push し、PR を開く。main へは push しない。
タグのジョブ：版上げの PR の squash だけにタグを打つ。
"""
import json
import os
import re
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

from release_test_support import BUMP, FAKE_PAT, REPOSITORY, VERSION_FILE, History, cargo_toml, fake_bin, git, pull, \
    read_log
from workflow_runner import Runner, default_environment


def bump_workflow(push_tag):
    """lib-bump-version.yml の PUSH_RELEASE_TAG を push_tag（'true' か 'false'）にしたもの。
    実物の値は公開を止める pull request で 'false' になりうるので、試験は値を明示する（#375 の CodeRabbit の指摘）。"""
    workflow, count = re.subn(r"\nenv:\n  PUSH_RELEASE_TAG: '(?:true|false)'\n",
                              f"\nenv:\n  PUSH_RELEASE_TAG: '{push_tag}'\n", BUMP)
    assert count == 1
    return workflow


class BumpFixture:
    def __init__(self, push_tag='true'):
        self.workflow = bump_workflow(push_tag)
        self.base = Path(tempfile.mkdtemp())
        self.history = History(self.base)
        self.bin = fake_bin(self.base / 'bin')
        self.log = self.base / 'log.jsonl'
        self.pulls = self.base / 'pulls.json'
        self.pulls.write_text('[]')
        self.count = 0

    def close(self):
        shutil.rmtree(self.base, ignore_errors=True)

    def run_job(self, name, inputs=None, ref='main'):
        self.count += 1
        workspace = self.base / f'job-{self.count}'
        git(self.base, 'clone', '-q', str(self.history.origin), str(workspace))
        git(workspace, 'checkout', '-q', ref)
        temp = self.base / f'temp-{self.count}'
        temp.mkdir()
        home = self.base / f'home-{self.count}'
        home.mkdir()
        path = os.pathsep.join([str(self.bin), str(Path(sys.executable).parent), os.environ['PATH']])
        environment = default_environment(workspace, temp, REPOSITORY, 7, 1, path, home)
        environment.update(FAKE_LOG=str(self.log), FAKE_PULLS=str(self.pulls), PULLS_RETRY_SECONDS='0',
                           FAKE_RELEASES=str(self.base / 'none'))
        sha = git(workspace, 'rev-parse', 'HEAD')
        context = {'token': 'fake-github-token', 'server_url': 'https://github.com', 'sha': sha,
                   'repository': REPOSITORY, 'ref': 'refs/heads/main', 'event_name': 'workflow_dispatch'}
        inputs = {'level': 'auto', 'version': '', 'rehearsal': False, **(inputs or {})}
        runner = Runner(self.workflow, workspace, temp, context, environment,
                        secrets={'PERSONAL_ACCESS_TOKEN': FAKE_PAT}, inputs=inputs)
        return runner.run(name)

    def remote_branches(self):
        return git(self.history.origin, 'for-each-ref', '--format=%(refname)', 'refs/heads/').split()

    def gh(self, *prefix):
        return [entry['args'] for entry in read_log(self.log)
                if entry['tool'] == 'gh' and entry['args'][:len(prefix)] == list(prefix)]


class BumpVersionTest(unittest.TestCase):
    def setUp(self):
        self.fixture = BumpFixture()
        self.addCleanup(self.fixture.close)

    def assert_version_branch(self, version):
        origin = self.fixture.history.origin
        branch = f'refs/heads/release/v{version}'
        self.assertIn(branch, self.fixture.remote_branches())
        self.assertEqual(git(origin, 'diff', '--name-only', 'refs/heads/main', branch), VERSION_FILE)
        main_text = git(origin, 'show', f'refs/heads/main:{VERSION_FILE}')
        branch_text = git(origin, 'show', f'{branch}:{VERSION_FILE}')
        old = main_text.split('version = "', 1)[1].split('"', 1)[0]
        self.assertEqual(branch_text, main_text.replace(f'version = "{old}"', f'version = "{version}"', 1))
        self.assertEqual(git(origin, 'log', '-1', '--format=%s', branch), f'version up to v{version}')
        self.assertEqual(git(origin, 'rev-parse', f'{branch}^'), git(origin, 'rev-parse', 'refs/heads/main'))

    def test_a_feature_opens_a_minor_version_pull_request_and_main_is_not_pushed(self):
        before = git(self.fixture.history.origin, 'rev-parse', 'refs/heads/main')
        self.fixture.history.commit('feat: add a thing', files={'lib/src/lib.rs': 'pub fn g() {}\n'})
        main = git(self.fixture.history.origin, 'rev-parse', 'refs/heads/main')
        self.assertNotEqual(before, main)
        result = self.fixture.run_job('bump-version')
        self.assertTrue(result.succeeded, [s.stderr for s in result.steps])
        self.assert_version_branch('5.3.0')
        self.assertEqual(git(self.fixture.history.origin, 'rev-parse', 'refs/heads/main'), main)
        [create] = self.fixture.gh('pr', 'create')
        self.assertEqual(create[create.index('--head') + 1], 'release/v5.3.0')
        self.assertEqual(create[create.index('--title') + 1], 'version up to v5.3.0')
        self.assertNotIn('--draft', create)
        [dispatch] = self.fixture.gh('workflow', 'run')
        self.assertEqual(dispatch[-1], 'pr_number=7')
        body = Path(create[create.index('--body-file') + 1]).read_text()
        self.assertIn('add a thing', body)
        self.assertEqual(self.fixture.history.origin.joinpath('refs', 'tags', 'v5.3.0').exists(), False)

    def test_breaking_and_fixes_follow_the_rs_rules(self):
        cases = {('feat!: replace', ''): '6.0.0', ('fix: a', 'BREAKING CHANGE: x'): '6.0.0', ('fix: a', ''): '5.2.1',
                 ('revert: a', ''): '5.3.0'}
        for (subject, body), version in cases.items():
            with self.subTest(subject=subject, body=body):
                fixture = BumpFixture()
                self.addCleanup(fixture.close)
                fixture.history.commit(subject + ('\n\n' + body if body else ''))
                result = fixture.run_job('bump-version')
                self.assertTrue(result.succeeded, [s.stderr for s in result.steps])
                self.assertIn(f'refs/heads/release/v{version}', fixture.remote_branches())

    def test_nothing_to_release_with_level_auto(self):
        self.fixture.history.commit('Update README')
        result = self.fixture.run_job('bump-version')
        self.assertTrue(result.succeeded)
        self.assertEqual(result.outcome('version_branch'), 'skipped')
        self.assertEqual(self.fixture.remote_branches(), ['refs/heads/main'])
        self.assertFalse(self.fixture.gh('pr', 'create'))

    def test_an_explicit_level_bumps_without_matching_commits(self):
        self.fixture.history.commit('Update README')
        result = self.fixture.run_job('bump-version', {'level': 'patch'})
        self.assertTrue(result.succeeded, [s.stderr for s in result.steps])
        self.assert_version_branch('5.2.1')

    def test_an_explicit_version(self):
        result = self.fixture.run_job('bump-version', {'version': '5.4.0'})
        self.assertTrue(result.succeeded, [s.stderr for s in result.steps])
        self.assert_version_branch('5.4.0')
        for version in ('5.2.0', '5.1.0', '5.4.0-rc.1', 'v5.4.0'):
            with self.subTest(version):
                refused = self.fixture.run_job('bump-version', {'version': version})
                self.assertEqual(refused.outcome('requested_version'), 'failure')

    def test_a_merged_version_without_a_tag_is_not_lowered(self):
        self.fixture.history.commit('version up to v5.3.0', version='5.3.0')
        result = self.fixture.run_job('bump-version', {'level': 'patch'})
        self.assertEqual(result.outcome('versions'), 'failure')
        self.assertIn('must be greater than the version 5.3.0', result.step('versions').stderr)
        self.assertEqual(self.fixture.remote_branches(), ['refs/heads/main'])

    def test_an_existing_version_branch_stops_before_anything_is_pushed(self):
        git(self.fixture.history.work, 'push', '-q', 'origin', 'HEAD:refs/heads/release/v5.2.1')
        result = self.fixture.run_job('bump-version', {'level': 'patch'})
        self.assertEqual(result.outcome('version_branch'), 'failure')
        self.assertFalse(self.fixture.gh('pr', 'create'))

    def test_a_rehearsal_opens_a_draft(self):
        result = self.fixture.run_job('bump-version', {'level': 'patch', 'rehearsal': True})
        self.assertTrue(result.succeeded, [s.stderr for s in result.steps])
        [create] = self.fixture.gh('pr', 'create')
        self.assertIn('--draft', create)
        self.assertEqual(create[create.index('--title') + 1], 'version up to v5.2.1 (rehearsal, do not merge)')

    def test_a_two_digit_major_is_bumped_from_its_own_digits(self):
        self.fixture.history.commit('version up to v10.2.0', version='10.2.0')
        self.fixture.history.tag('v10.2.0')
        self.fixture.history.commit('feat: x')
        result = self.fixture.run_job('bump-version')
        self.assertTrue(result.succeeded, [s.stderr for s in result.steps])
        self.assert_version_branch('10.3.0')

    def test_a_major_bump_of_a_pre_release_finalizes_it(self):
        """#375 の CodeRabbit の指摘：基点は最新の正式のタグではなく、今の pre-release の版。"""
        cases = {'6.1.0-rc.1': '6.1.0', '5.2.1-rc.1': '5.2.1'}
        for current, version in cases.items():
            with self.subTest(current):
                fixture = BumpFixture()
                self.addCleanup(fixture.close)
                fixture.history.commit('chore: pre', version=current)
                result = fixture.run_job('bump-version', {'level': 'major'})
                self.assertTrue(result.succeeded, [s.stderr for s in result.steps])
                self.assertIn(f'refs/heads/release/v{version}', fixture.remote_branches())

    def test_a_non_major_bump_of_a_pre_release_is_refused(self):
        self.fixture.history.commit('chore: pre', version='6.0.0-rc.1')
        result = self.fixture.run_job('bump-version', {'level': 'minor'})
        self.assertEqual(result.outcome('prerelease'), 'failure')


class TagReleaseTest(unittest.TestCase):
    def setUp(self):
        self.fixture = BumpFixture()
        self.addCleanup(self.fixture.close)

    def test_the_squash_of_a_version_pull_request_is_tagged(self):
        sha = self.fixture.history.commit('version up to v5.2.1 (#9)', version='5.2.1')
        head = self.fixture.history.pull_head(sha)
        self.fixture.pulls.write_text(json.dumps([pull(sha, head_sha=head, whole_diff=None)]))
        result = self.fixture.run_job('tag-release')
        self.assertTrue(result.succeeded, [s.stderr for s in result.steps])
        self.assertEqual(git(self.fixture.history.origin, 'rev-parse', 'refs/tags/v5.2.1^{commit}'), sha)
        again = self.fixture.run_job('tag-release')
        self.assertTrue(again.succeeded)
        self.assertEqual(again.outcome('release_tag'), 'skipped')

    def test_with_tag_pushing_off_the_gate_runs_but_no_tag_is_pushed(self):
        fixture = BumpFixture(push_tag='false')
        self.addCleanup(fixture.close)
        sha = fixture.history.commit('version up to v5.2.1 (#9)', version='5.2.1')
        head = fixture.history.pull_head(sha)
        fixture.pulls.write_text(json.dumps([pull(sha, head_sha=head, whole_diff=None)]))
        result = fixture.run_job('tag-release')
        self.assertTrue(result.succeeded, [s.stderr for s in result.steps])
        self.assertEqual(result.outcome('tag_summary'), 'success')
        self.assertEqual(result.outcome('release_tag'), 'skipped')
        self.assertEqual(git(fixture.history.origin, 'tag', '-l', 'v5.2.1'), '')

    def test_a_rebase_that_lowers_the_version_and_changes_another_file_is_not_tagged(self):
        """#375 の CodeRabbit の反例。main は 5.2.1（タグなし）。pull request は、版を 5.2.0 に下げてほかのファイルの
        1行を変え、最後に 5.2.1 に戻す。rebase のマージで main に積まれ、最後のコミットは版だけを上げる。
        全体の件数は (1, 1, 1) だが、全体の差分は lib/src/lib.rs だけなので拒否する。"""
        history = self.fixture.history
        base = history.commit('version up to v5.2.1', version='5.2.1')
        lowered = history.commit('chore: lower', version='5.2.0', files={'lib/src/lib.rs': 'pub fn f() { g() }\n'})
        last = history.commit('version up to v5.2.1', version='5.2.1')
        # rebase の前の pull request のコミット（同じ木で、SHA が違う）
        first = git(history.work, 'commit-tree', git(history.work, 'rev-parse', f'{lowered}^{{tree}}'), '-p', base,
                    '-m', 'chore: lower')
        head = git(history.work, 'commit-tree', git(history.work, 'rev-parse', f'{last}^{{tree}}'), '-p', first,
                   '-m', 'version up to v5.2.1')
        git(history.work, 'push', '-q', 'origin', f'{head}:refs/pull/1/head')
        self.assertEqual(git(history.origin, 'diff', '--name-only', base, head), 'lib/src/lib.rs')
        self.fixture.pulls.write_text(json.dumps([pull(last, head_sha=head, whole_diff=None)]))
        result = self.fixture.run_job('tag-release')
        self.assertEqual(result.outcome('release_tag'), 'skipped')
        self.assertEqual(git(history.origin, 'tag', '-l', 'v5.2.1'), '')
        self.assertIn('as a whole must change only the version', result.step('tag_summary').stderr
                      + result.step('tag_summary').stdout + ''.join(s.stderr for s in result.steps))

    def test_other_changes_of_lib_cargo_toml_are_not_tagged(self):
        cases = {
            'a dependency change': ('chore(deps): bump', cargo_toml('5.2.0').replace('dep:uuid"]', 'dep:uuid", "x"]', 1),
                                    'release/v5.2.0'),
            'a version change from another branch': ('chore: bump', cargo_toml('5.2.1'), 'feature/x'),
            'a version change with a comment change': ('version up to v5.2.1',
                                                       cargo_toml('5.2.1').replace('# FR8.3', '# FR8.4'),
                                                       'release/v5.2.1'),
        }
        for label, (message, text, ref) in cases.items():
            with self.subTest(label):
                fixture = BumpFixture()
                self.addCleanup(fixture.close)
                sha = fixture.history.commit(message, files={VERSION_FILE: text})
                fixture.pulls.write_text(json.dumps([pull(sha, ref=ref, head_sha=fixture.history.pull_head(sha),
                                                          whole_diff=None)]))
                result = fixture.run_job('tag-release')
                self.assertEqual(result.outcome('release_tag'), 'skipped', label)
                self.assertEqual(git(fixture.history.origin, 'tag', '-l', 'v5.2.1'), '')


if __name__ == '__main__':
    unittest.main()
