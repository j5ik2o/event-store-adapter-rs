"""lib-release.yml の4つのジョブを、手元の git の履歴・crates.io のスタブ・偽の cargo で流す（設計の追記 design-r6.md §3）。

確かめること：公開の経路が正しく進むこと、止まるべきときに認証（crates-io-auth-action）より前で止まること、
OIDC と token が publish ジョブだけに届き、そこで cargo のビルドが動かないこと（I20）、package ジョブの成果物からは
通常のファイルの Cargo.lock だけを使うこと（再レビュー N1）。
"""
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

from release_test_support import (
    FAKE_CRATES_TOKEN, RELEASE, REPOSITORY, CratesStub, History, fake_bin, git, pull, read_log, sha256,
)
from workflow_runner import Runner, default_environment, job_condition, job_if, job_needs

JOBS = ('package', 'publish', 'confirm', 'github-release')


class ReleaseFixture:
    def __init__(self):
        self.base = Path(tempfile.mkdtemp())
        self.history = History(self.base)
        self.stub = CratesStub()
        self.bin = fake_bin(self.base / 'bin')
        self.log = self.base / 'log.jsonl'
        self.pulls = self.base / 'pulls.json'
        self.pulls.write_text('[]')
        self.releases = self.base / 'releases.txt'
        self.releases.write_text('v5.2.0\n')
        self.artifacts = {}
        self.count = 0
        self.tag_sha = None

    def close(self):
        self.stub.close()
        shutil.rmtree(self.base, ignore_errors=True)

    def release_commit(self, version='5.2.1', files=None):
        sha = self.history.commit(f'version up to v{version}', version=version, files=files)
        self.pulls.write_text(json.dumps([pull(sha, ref=f'release/v{version}')]))
        self.history.tag(f'v{version}')
        self.tag_sha = sha
        return sha

    def run_job(self, name, needs, tag='v5.2.1', attempt=1, step_env=None):
        self.count += 1
        workspace = self.base / f'job-{self.count}-{name}'
        git(self.base, 'clone', '-q', str(self.history.origin), str(workspace))
        git(workspace, 'checkout', '-q', '--detach', tag)
        temp = self.base / f'temp-{self.count}'
        temp.mkdir()
        home = self.base / f'home-{self.count}'
        home.mkdir()
        path = os.pathsep.join([str(self.bin), str(Path(sys.executable).parent), os.environ['PATH']])
        environment = default_environment(workspace, temp, REPOSITORY, 4242, attempt, path, home)
        environment.update(
            FAKE_LOG=str(self.log), FAKE_PULLS=str(self.pulls), FAKE_RELEASES=str(self.releases),
            FAKE_PUBLISH_URL=self.stub.url + '/_publish', FAKE_TAG_SHA=git(workspace, 'rev-parse', f'{tag}^{{commit}}'),
            CRATES_IO_API_URL=self.stub.url, CRATES_INDEX_URL=self.stub.url + '/index', PULLS_RETRY_SECONDS='0',
        )
        context = {'token': 'fake-github-token', 'ref_name': tag, 'run_attempt': str(attempt),
                   'server_url': 'https://github.com', 'sha': environment['FAKE_TAG_SHA'], 'repository': REPOSITORY}
        runner = Runner(RELEASE, workspace, temp, context, environment, needs=needs, artifacts=self.artifacts,
                        step_env=step_env)
        result = runner.run(name)
        result.workspace = workspace
        return result

    def release(self, tag='v5.2.1', attempt=1, step_env=None, start=None, needs=None):
        """start から順に、needs の条件を満たすジョブだけを流す。結果は {ジョブ名: JobResult か None}。"""
        results, needs = {}, dict(needs or {})
        names = JOBS[JOBS.index(start):] if start else JOBS
        for name in names:
            deps = {dep: needs.get(dep, {'result': 'skipped', 'outputs': {}}) for dep in job_needs(RELEASE, name)}
            if not job_condition(job_if(RELEASE, name), deps):
                results[name] = None
                needs[name] = {'result': 'skipped', 'outputs': {}}
                continue
            result = self.run_job(name, deps, tag=tag, attempt=attempt, step_env=(step_env or {}).get(name))
            results[name] = result
            needs[name] = {'result': 'success' if result.succeeded else 'failure', 'outputs': result.outputs}
        results['_needs'] = needs
        return results

    def cargo_calls(self):
        return [entry for entry in read_log(self.log) if entry['tool'] == 'cargo']

    def gh_calls(self, *prefix):
        return [entry['args'] for entry in read_log(self.log)
                if entry['tool'] == 'gh' and entry['args'][:len(prefix)] == list(prefix)]


class ReleaseWorkflowRunTest(unittest.TestCase):
    def setUp(self):
        self.fixture = ReleaseFixture()
        self.addCleanup(self.fixture.close)

    def assert_not_authenticated(self, results):
        publish = results.get('publish')
        self.assertFalse(publish and publish.auth_used, 'crates-io-auth-action must not run')
        self.assertEqual(self.fixture.stub.published, [])
        self.assertTrue(all(not call['token'] for call in self.fixture.cargo_calls()))

    def test_a_version_pull_request_merge_is_published_confirmed_and_released(self):
        self.fixture.release_commit()
        results = self.fixture.release()
        package, publish = results['package'], results['publish']
        self.assertTrue(package.succeeded, [s.stderr for s in package.steps])
        self.assertEqual(package.outputs['action'], 'publish')
        self.assertEqual(package.outputs['artifact'], 'verified-crate-1')
        self.assertTrue(publish.succeeded, [s.stderr for s in publish.steps])
        self.assertTrue(publish.auth_used)
        self.assertEqual(publish.outputs['uploaded'], 'true')
        [published] = self.fixture.stub.published
        self.assertEqual((published['version'], published['checksum'], published['sha']),
                         ('5.2.1', package.outputs['sha256'], self.fixture.tag_sha))
        self.assertTrue(results['confirm'].succeeded, [s.stdout + s.stderr for s in results['confirm'].steps])
        self.assertIn('with the verified checksum', results['confirm'].step('confirm').stdout)
        self.assertTrue(results['github-release'].succeeded)
        [create] = self.fixture.gh_calls('release', 'create')
        self.assertEqual(create[2], 'v5.2.1')
        self.assertIn('--latest', create)
        # OIDC と token は publish ジョブの cargo publish だけ。publish ジョブの cargo はビルドしない（I20）
        calls = self.fixture.cargo_calls()
        with_token = [call['args'] for call in calls if call['token']]
        self.assertEqual(with_token, [['publish', '--no-verify', '--locked', '-p', 'event-store-adapter-rs']])
        self.assertTrue(all(call['token'] == FAKE_CRATES_TOKEN for call in calls if call['token']))
        with_oidc = [call['args'] for call in calls if call['oidc']]
        self.assertEqual(with_oidc, [['--version'], ['package', '--no-verify', '--locked', '-p', 'event-store-adapter-rs'],
                                     ['publish', '--no-verify', '--locked', '-p', 'event-store-adapter-rs']])
        self.assertIn(['package', '-p', 'event-store-adapter-rs'], [call['args'] for call in calls if not call['oidc']])

    def test_a_version_already_published_from_the_tagged_commit_is_not_published_again(self):
        sha = self.fixture.release_commit()
        self.fixture.stub.add('5.2.1', sha=sha, checksum='1' * 64)
        results = self.fixture.release()
        self.assertEqual(results['package'].outputs['action'], 'published')
        self.assertIsNone(results['publish'])
        self.assertTrue(results['confirm'].succeeded)
        self.assertNotIn('checksum', results['confirm'].step('confirm').stdout)
        self.assertTrue(results['github-release'].succeeded)
        self.assert_not_authenticated(results)

    def test_a_version_published_by_hand_or_from_another_commit_stops_before_anything_else(self):
        for kwargs in ({'trust': False}, {'sha': 'b' * 40}, {'repository': 'someone/r'}, {'yanked': True}):
            with self.subTest(**kwargs):
                fixture = ReleaseFixture()
                self.addCleanup(fixture.close)
                sha = fixture.release_commit()
                fixture.stub.add('5.2.1', **{'sha': sha, **kwargs})
                results = fixture.release()
                self.assertEqual(results['package'].outcome('precheck'), 'failure')
                self.assertEqual([results[name] for name in ('publish', 'confirm', 'github-release')], [None] * 3)
                self.assertFalse(fixture.gh_calls('release', 'create'))

    def test_the_gate_refuses_a_tag_on_a_commit_that_changes_more_than_the_version(self):
        self.fixture.release_commit(files={'lib/src/lib.rs': 'pub fn g() {}\n'})
        results = self.fixture.release()
        self.assertEqual(results['package'].outcome('release_gate'), 'failure')
        self.assertEqual(results['package'].ran()[-1], 'release_gate')
        self.assertIsNone(results['publish'])
        self.assert_not_authenticated(results)

    def test_a_larger_version_on_crates_io_stops_the_precheck(self):
        self.fixture.release_commit()
        self.fixture.stub.add('5.3.0')
        results = self.fixture.release()
        self.assertEqual(results['package'].outcome('precheck'), 'failure')
        self.assert_not_authenticated(results)

    def test_a_tampered_artifact_stops_before_the_authentication(self):
        cases = {
            'symlinked Cargo.lock': lambda stored: (os.remove(stored / 'Cargo.lock'),
                                                    os.symlink('/etc/hosts', stored / 'Cargo.lock')),
            'changed Cargo.lock': lambda stored: (stored / 'Cargo.lock').write_text('# another lock\n'),
            'missing Cargo.lock': lambda stored: os.remove(stored / 'Cargo.lock'),
        }
        for label, tamper in cases.items():
            with self.subTest(label):
                fixture = ReleaseFixture()
                self.addCleanup(fixture.close)
                fixture.release_commit()
                results = fixture.release()
                self.assertTrue(results['publish'].auth_used)
                fixture.stub.published.clear()
                del fixture.stub.versions['5.2.1']
                [stored] = fixture.artifacts.values()
                tamper(stored)
                rerun = fixture.release(start='publish', attempt=2, needs={'package': results['_needs']['package']})
                self.assertEqual(rerun['publish'].outcome('crate_check'), 'failure', label)
                self.assertFalse(rerun['publish'].auth_used)
                self.assertEqual(fixture.stub.published, [])

    def test_only_cargo_lock_is_taken_from_the_artifact(self):
        self.fixture.release_commit()
        package = self.fixture.release()['package']
        self.fixture.stub.published.clear()
        del self.fixture.stub.versions['5.2.1']
        [stored] = self.fixture.artifacts.values()
        (stored / '.cargo').mkdir()
        (stored / '.cargo' / 'config.toml').write_text('[registry]\nglobal-credential-providers = ["evil"]\n')
        (stored / 'lib').mkdir()
        (stored / 'lib' / 'Cargo.toml').write_text('[package]\n')
        rerun = self.fixture.release(start='publish', attempt=2,
                                     needs={'package': {'result': 'success', 'outputs': package.outputs}})
        publish = rerun['publish']
        self.assertTrue(publish.succeeded, [s.stderr for s in publish.steps])
        self.assertFalse((publish.workspace / '.cargo').exists())
        self.assertIn('version = "5.2.1"', (publish.workspace / 'lib' / 'Cargo.toml').read_text())

    def test_a_missing_artifact_stops_before_the_authentication(self):
        self.fixture.release_commit()
        package = self.fixture.release()['package']
        self.fixture.stub.published.clear()
        del self.fixture.stub.versions['5.2.1']
        self.fixture.artifacts.clear()
        rerun = self.fixture.release(start='publish', attempt=2,
                                     needs={'package': {'result': 'success', 'outputs': package.outputs}})
        self.assertEqual(rerun['publish'].outcome('download_verified'), 'failure')
        self.assertFalse(rerun['publish'].auth_used)
        self.assertEqual(self.fixture.stub.published, [])

    def test_another_cargo_in_the_publish_job_stops_before_the_authentication(self):
        self.fixture.release_commit()
        results = self.fixture.release(step_env={'publish': {'rust': {'FAKE_CARGO_VERSION': 'cargo 1.101.0 (fake)'}}})
        self.assertEqual(results['publish'].outcome('rust'), 'failure')
        self.assert_not_authenticated(results)

    def test_a_newer_tag_before_the_publish_job_stops_it(self):
        self.fixture.release_commit()
        package = self.fixture.release()['package']
        self.fixture.stub.published.clear()
        del self.fixture.stub.versions['5.2.1']
        self.fixture.history.commit('chore: later')
        self.fixture.history.tag('v5.2.2')
        rerun = self.fixture.release(start='publish', needs={'package': {'result': 'success', 'outputs': package.outputs}})
        self.assertEqual(rerun['publish'].outcome('release_gate_again'), 'failure')
        self.assertFalse(rerun['publish'].auth_used)
        self.assertEqual(self.fixture.stub.published, [])

    def test_a_publish_job_that_failed_after_the_upload_is_resumed_without_a_second_upload(self):
        self.fixture.release_commit()
        first = self.fixture.release(step_env={'publish': {'publish': {'FAKE_PUBLISH_FAILS_AFTER': '1'}}})
        self.assertEqual(first['publish'].outcome('publish'), 'failure')
        self.assertIsNone(first['confirm'])
        self.assertEqual(len(self.fixture.stub.published), 1)
        rerun = self.fixture.release(start='publish', attempt=2, needs={'package': first['_needs']['package']})
        self.assertTrue(rerun['publish'].succeeded)
        self.assertEqual(rerun['publish'].outcome('rust'), 'skipped')
        self.assertFalse(rerun['publish'].auth_used)
        self.assertEqual(rerun['publish'].outputs['uploaded'], '')
        self.assertTrue(rerun['confirm'].succeeded)
        self.assertTrue(rerun['github-release'].succeeded)
        self.assertEqual(len(self.fixture.stub.published), 1)

    def test_a_checksum_that_differs_from_the_verified_crate_stops_the_release(self):
        self.fixture.release_commit()
        self.fixture.stub.tamper_checksum = True
        results = self.fixture.release()
        self.assertTrue(results['publish'].succeeded)
        self.assertEqual(results['confirm'].outcome('confirm'), 'failure')
        self.assertIsNone(results['github-release'])
        self.assertFalse(self.fixture.gh_calls('release', 'create'))

    def test_an_older_release_is_not_marked_latest_and_notes_start_at_a_released_tag(self):
        sha = self.fixture.release_commit()
        self.fixture.history.commit('feat: later')
        self.fixture.history.tag('v5.3.0')
        self.fixture.stub.add('5.2.1', sha=sha)
        results = self.fixture.release(start='github-release', needs={'confirm': {'result': 'success', 'outputs': {}}})
        self.assertTrue(results['github-release'].succeeded, [s.stderr for s in results['github-release'].steps])
        [create] = self.fixture.gh_calls('release', 'create')
        self.assertIn('--latest=false', create)
        self.assertIn('release notes from v5.2.0', results['github-release'].step('github_release').stdout)

    def test_an_existing_release_is_left_alone_and_a_failed_listing_stops(self):
        self.fixture.release_commit()
        self.fixture.releases.write_text('v5.2.0\nv5.2.1\n')
        results = self.fixture.release(start='github-release', needs={'confirm': {'result': 'success', 'outputs': {}}})
        self.assertTrue(results['github-release'].succeeded)
        self.assertFalse(self.fixture.gh_calls('release', 'create'))
        failing = self.fixture.release(start='github-release', needs={'confirm': {'result': 'success', 'outputs': {}}},
                                       step_env={'github-release': {'github_release': {'FAKE_RELEASES_FAIL': '1'}}})
        self.assertEqual(failing['github-release'].outcome('github_release'), 'failure')
        self.assertFalse(self.fixture.gh_calls('release', 'create'))


if __name__ == '__main__':
    unittest.main()
