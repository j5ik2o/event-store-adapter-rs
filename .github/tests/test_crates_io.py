"""crates-io.py の決まった例（設計の追記 design-r6.md §4、再レビュー N1）。手元の HTTP のスタブを叩く。"""
import hashlib
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock

from release_test_support import GITHUB, LOOPBACK, REPOSITORY, CratesStub, load_crates_io

crates = load_crates_io()
SHA = 'a' * 40


class StubTest(unittest.TestCase):
    def setUp(self):
        self.stub = CratesStub()
        self.addCleanup(self.stub.close)
        self.directory = Path(tempfile.mkdtemp())
        self.output = self.directory / 'output'
        self.output.write_text('')
        self.remote = self.directory / 'remote-tags'
        self.remote.write_text(f'{"1" * 40}\trefs/tags/v5.2.0\n{"2" * 40}\trefs/tags/v5.2.1\n')

    def run_script(self, *args):
        env = {**os.environ, 'CRATES_IO_API_URL': self.stub.url, 'CRATES_INDEX_URL': self.stub.url + '/index',
               'GITHUB_REPOSITORY': REPOSITORY, 'GITHUB_OUTPUT': str(self.output), 'no_proxy': LOOPBACK,
               'NO_PROXY': LOOPBACK}
        return subprocess.run([sys.executable, str(GITHUB / 'crates-io.py'), *args], env=env, capture_output=True,
                              text=True)

    def precheck(self, version='5.2.1'):
        with mock.patch.dict(os.environ, {'CRATES_IO_API_URL': self.stub.url, 'GITHUB_REPOSITORY': REPOSITORY,
                                          'GITHUB_OUTPUT': str(self.output), 'no_proxy': LOOPBACK}):
            return crates.main(['crates-io.py', 'precheck', '--version', version, '--sha', SHA],
                               git=lambda remote, deadline: self.remote.read_text())


class PrecheckTest(StubTest):
    def test_a_new_version_is_published(self):
        self.stub.add('5.2.0')
        self.assertEqual(self.precheck(), 0)
        self.assertEqual(self.output.read_text(), 'action=publish\n')

    def test_a_version_from_the_tagged_commit_is_published_already(self):
        self.stub.add('5.2.1', sha=SHA)
        self.assertEqual(self.precheck(), 0)
        self.assertEqual(self.output.read_text(), 'action=published\n')

    def test_stops(self):
        cases = {
            'yanked': lambda: self.stub.add('5.2.1', sha=SHA, yanked=True),
            'by hand': lambda: self.stub.add('5.2.1', trust=False),
            'another commit': lambda: self.stub.add('5.2.1', sha='b' * 40),
            'another repository': lambda: self.stub.add('5.2.1', sha=SHA, repository='someone/r'),
            'a larger version': lambda: self.stub.add('5.3.0'),
            'a larger pre-release': lambda: self.stub.add('6.0.0-rc.1'),
            'not the latest tag': lambda: self.remote.write_text(f'{"3" * 40}\trefs/tags/v5.3.0\n'),
            'a server error': lambda: self.stub.fail.update({'/api/v1/crates/': 503}),
            'a cut body': lambda: self.stub.fail.update({'/api/v1/crates/': 'drop'}),
            'forbidden': lambda: self.stub.fail.update({'/api/v1/crates/': 403}),
            'versions fail': lambda: self.stub.fail.update({f'/api/v1/crates/{crates.CRATE}/versions': 500}),
        }
        for label, arrange in cases.items():
            with self.subTest(label):
                self.stub.versions.clear()
                self.stub.fail.clear()
                self.remote.write_text(f'{"1" * 40}\trefs/tags/v5.2.0\n{"2" * 40}\trefs/tags/v5.2.1\n')
                self.output.write_text('')
                arrange()
                self.assertEqual(self.precheck(), 1)
                self.assertEqual(self.output.read_text(), '')

    def test_requests_carry_the_user_agent(self):
        self.precheck()
        self.assertTrue(self.stub.requests)
        for _, _, headers in self.stub.requests:
            self.assertEqual(headers.get('User-Agent'), crates.USER_AGENT)

    def test_the_command_line_reads_the_remote_tags_with_git(self):
        with tempfile.TemporaryDirectory() as directory:
            subprocess.run(['git', 'init', '-q', directory], check=True)
            result = subprocess.run([sys.executable, str(GITHUB / 'crates-io.py'), 'precheck', '--version', '5.2.1',
                                     '--sha', SHA, '--remote', directory],
                                    env={**os.environ, 'CRATES_IO_API_URL': self.stub.url,
                                         'GITHUB_REPOSITORY': REPOSITORY, 'no_proxy': LOOPBACK},
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 1)
            self.assertIn('is not the latest release tag (the latest is None)', result.stderr)


class HttpTest(StubTest):
    def test_no_request_is_sent_after_the_deadline(self):
        clock = [0.0]
        deadline = crates.Deadline(10, lambda: clock[0])
        clock[0] = 10
        with mock.patch.dict(os.environ, {'no_proxy': LOOPBACK}):
            self.assertEqual(crates.http(self.stub.url + '/index/ev/en/x', deadline), crates.Response(None))
        self.assertEqual(self.stub.requests, [])

    def test_the_timeout_is_cut_to_the_remaining_time(self):
        seen = []

        def urlopen(request, timeout):
            seen.append(timeout)
            raise TimeoutError()
        clock = [0.0]
        deadline = crates.Deadline(100, lambda: clock[0])
        clock[0] = 97.5
        with mock.patch.object(crates.urllib.request, 'urlopen', urlopen):
            self.assertEqual(crates.http('http://127.0.0.1:9/x', deadline), crates.Response(None))
        self.assertEqual(seen, [2.5])


class SlowBodyTest(unittest.TestCase):
    """#375 の Devin と CodeRabbit の指摘：urlopen の timeout は通信の1回ごと。本文を少しずつ読み、締め切りで打ち切る。"""

    def trickle(self, clock, pieces, step, status=200):
        """1回の read ごとに clock を step 進め、pieces を1つずつ返す（尽きたら b''）。読んだ回数は reads に入る。"""
        reads = []

        class Slow:
            closed = False

            def __init__(self):
                self.status = status

            def close(self):
                self.closed = True

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self, amount=-1):
                reads.append(amount)
                clock[0] += step
                return pieces.pop(0) if pieces else b''
        return Slow(), reads

    def test_a_body_that_finishes_after_the_deadline_is_a_failed_query(self):
        clock = [0.0]
        slow, _ = self.trickle(clock, [b'{"vers": ', b'"5.2.1"}'], 6.0)
        deadline = crates.Deadline(10, lambda: clock[0])
        with mock.patch.object(crates.urllib.request, 'urlopen', lambda request, timeout: slow):
            self.assertEqual(crates.http('http://127.0.0.1:9/x', deadline), crates.Response(None))
        clock[0] = 0.0
        slow, reads = self.trickle(clock, [b'{"vers": ', b'"5.2.1"}'], 1.0)
        deadline = crates.Deadline(100, lambda: clock[0])
        with mock.patch.object(crates.urllib.request, 'urlopen', lambda request, timeout: slow):
            self.assertEqual(crates.http('http://127.0.0.1:9/x', deadline),
                             crates.Response(200, b'{"vers": "5.2.1"}'))
        self.assertTrue(all(amount == crates.BODY_CHUNK for amount in reads))

    def test_a_body_that_never_ends_is_cut_at_the_deadline(self):
        clock = [0.0]
        slow, reads = self.trickle(clock, [b'x'] * 1000, 1.0)
        deadline = crates.Deadline(10, lambda: clock[0])
        with mock.patch.object(crates.urllib.request, 'urlopen', lambda request, timeout: slow):
            self.assertEqual(crates.http('http://127.0.0.1:9/x', deadline), crates.Response(None))
        self.assertEqual(len(reads), 10)

    def test_the_body_of_an_error_response_is_cut_at_the_deadline_too(self):
        clock = [0.0]
        slow, reads = self.trickle(clock, [b'x'] * 1000, 1.0, status=404)

        def raising(request, timeout):
            raise crates.urllib.error.HTTPError('http://127.0.0.1:9/x', 404, 'Not Found', {}, slow)
        deadline = crates.Deadline(10, lambda: clock[0])
        with mock.patch.object(crates.urllib.request, 'urlopen', raising):
            self.assertEqual(crates.http('http://127.0.0.1:9/x', deadline), crates.Response(None))
        self.assertEqual(len(reads), 10)


class TrickleServer:
    """少しずつ送り続ける本物の HTTP サーバー（127.0.0.1 のスレッド）。mode は
    'length'（200、Content-Length つきの本文を 1 バイトずつ）、'error'（404 で同じ）、
    'chunked'（200、chunked の長さの行を 1 文字ずつ。1回の read1 の中で何度も受信させる）。"""

    def __init__(self, mode, every=0.05, total=8.0):
        stop = self.stop = threading.Event()

        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def log_message(self, *args):
                pass

            def do_GET(self):
                count = int(total / every)
                self.send_response(404 if mode == 'error' else 200)
                if mode == 'chunked':
                    self.send_header('Transfer-Encoding', 'chunked')
                else:
                    self.send_header('Content-Length', str(count))
                self.end_headers()
                self.wfile.flush()
                try:
                    if mode == 'chunked':
                        self.wfile.write(b'1;')
                    for _ in range(count):
                        if stop.wait(every):
                            return
                        self.wfile.write(b'x')
                        self.wfile.flush()
                except OSError:
                    pass

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.server.daemon_threads = True
        self.url = f'http://127.0.0.1:{self.server.server_address[1]}/x'
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.stop.set()
        self.server.shutdown()
        self.server.server_close()


class TrickleTest(unittest.TestCase):
    """#375 の CodeRabbit の再指摘：read(65536) は1回の呼び出しの中で何度も受信するので、mock では待ちを再現できない。
    本物の HTTP サーバーが少しずつ送り続けても、http は締め切り（1 秒）で打ち切る。"""

    def setUp(self):
        self.no_proxy = mock.patch.dict(os.environ, {'no_proxy': LOOPBACK, 'NO_PROXY': LOOPBACK})
        self.no_proxy.start()
        self.addCleanup(self.no_proxy.stop)

    def test_a_trickling_body_is_cut_at_the_deadline(self):
        for mode in ('length', 'error', 'chunked'):
            with self.subTest(mode):
                server = TrickleServer(mode)
                self.addCleanup(server.close)
                started = time.monotonic()
                result = crates.http(server.url, crates.Deadline(1.0))
                elapsed = time.monotonic() - started
                self.assertEqual(result, crates.Response(None))
                self.assertLess(elapsed, 2.5, mode)

    def test_a_body_that_arrives_in_time_is_read(self):
        server = TrickleServer('length', every=0.01, total=0.2)
        self.addCleanup(server.close)
        self.assertEqual(crates.http(server.url, crates.Deadline(10)), crates.Response(200, b'x' * 20))


class WatchdogTest(unittest.TestCase):
    def test_a_socket_that_cannot_be_shut_down_fails_the_query(self):
        """締め切りに socket を閉じられなかったら、握りつぶさずにその照会を時間切れにする。"""
        class Broken:
            def fileno(self):
                return 3

            def shutdown(self, how):
                raise OSError(107, 'Transport endpoint is not connected')
        watchdog = crates.Watchdog(Broken(), 60)
        watchdog.cancel()
        watchdog.fire()
        with self.assertRaises(TimeoutError):
            watchdog.raise_failure()

    def test_a_closed_socket_is_left_alone(self):
        class Closed:
            def fileno(self):
                return -1

            def shutdown(self, how):
                raise AssertionError('a closed socket must not be shut down')
        watchdog = crates.Watchdog(Closed(), 60)
        watchdog.cancel()
        watchdog.fire()
        watchdog.raise_failure()


class ConfirmTest(StubTest):
    def test_the_index_serves_the_version(self):
        self.stub.add('5.2.1', checksum='c' * 64)
        result = self.run_script('confirm', '--version', '5.2.1', '--deadline', '5', '--interval', '0.1')
        self.assertEqual(result.returncode, 0, result.stderr)
        index = [headers for method, path, headers in self.stub.requests if path.startswith('/index/')]
        self.assertEqual(index[0].get('Cache-Control'), 'no-cache')

    def test_the_checksums_must_match_the_verified_crate(self):
        self.stub.add('5.2.1', checksum='c' * 64)
        ok = self.run_script('confirm', '--version', '5.2.1', '--sha256', 'c' * 64, '--deadline', '5')
        self.assertEqual(ok.returncode, 0, ok.stderr)
        wrong = self.run_script('confirm', '--version', '5.2.1', '--sha256', 'd' * 64, '--deadline', '5')
        self.assertEqual(wrong.returncode, 1)
        self.assertIn('do not create the GitHub release', wrong.stderr)
        bad = self.run_script('confirm', '--version', '5.2.1', '--sha256', 'C' * 64, '--deadline', '5')
        self.assertEqual(bad.returncode, 1)

    def test_a_version_that_does_not_appear_times_out_without_publishing(self):
        self.stub.add('5.2.0')
        result = self.run_script('confirm', '--version', '5.2.1', '--deadline', '0.5', '--interval', '0.1')
        self.assertEqual(result.returncode, 1)
        self.assertIn('re-run only the confirm job', result.stderr)
        self.assertEqual([method for method, _, _ in self.stub.requests], ['GET'] * len(self.stub.requests))

    def test_a_late_index_is_waited_for(self):
        self.stub.add('5.2.1', visible=False)
        clock = [0.0]

        def sleep(seconds):
            clock[0] += seconds
            if clock[0] >= 40:
                self.stub.visible.add('5.2.1')
        with mock.patch.dict(os.environ, {'CRATES_INDEX_URL': self.stub.url + '/index', 'no_proxy': LOOPBACK}):
            self.assertEqual(crates.main(['crates-io.py', 'confirm', '--version', '5.2.1'], sleep=sleep,
                                         clock=lambda: clock[0]), 0)
        self.assertGreaterEqual(clock[0], 40)


class CrateFilesTest(unittest.TestCase):
    def setUp(self):
        self.directory = Path(tempfile.mkdtemp())

    def run_main(self, *args):
        return crates.main(['crates-io.py', *args])

    def test_check_crate(self):
        crate = self.directory / 'x.crate'
        crate.write_bytes(b'abc')
        digest = hashlib.sha256(b'abc').hexdigest()
        self.assertEqual(self.run_main('check-crate', '--crate', str(crate), '--sha256', digest), 0)
        self.assertEqual(self.run_main('check-crate', '--crate', str(crate), '--sha256', '0' * 64), 1)
        self.assertEqual(self.run_main('check-crate', '--crate', str(crate), '--sha256', ''), 1)

    def test_copy_lock_takes_only_a_regular_cargo_lock(self):
        source = self.directory / 'artifact'
        source.mkdir()
        (source / 'Cargo.lock').write_text('# lock\n')
        (source / '.cargo').mkdir()
        (source / '.cargo' / 'config.toml').write_text('[registry]\n')
        workspace = self.directory / 'workspace'
        workspace.mkdir()
        self.assertEqual(self.run_main('copy-lock', '--from', str(source), '--to', str(workspace / 'Cargo.lock'),
                                       '--sha256', hashlib.sha256(b'# lock\n').hexdigest()), 0)
        self.assertEqual((workspace / 'Cargo.lock').read_text(), '# lock\n')
        self.assertEqual(sorted(p.name for p in workspace.iterdir()), ['Cargo.lock'])

    def test_copy_lock_refuses_links_and_missing_files(self):
        secret = self.directory / 'secret'
        secret.write_text('x')
        cases = {}
        missing = self.directory / 'missing'
        missing.mkdir()
        cases['missing'] = missing
        linked = self.directory / 'linked'
        linked.mkdir()
        os.symlink(secret, linked / 'Cargo.lock')
        cases['symlink'] = linked
        directory_lock = self.directory / 'dir-lock'
        (directory_lock / 'Cargo.lock').mkdir(parents=True)
        cases['directory'] = directory_lock
        real = self.directory / 'real-artifact'
        real.mkdir()
        (real / 'Cargo.lock').write_text('# lock\n')
        linked_directory = self.directory / 'linked-directory'
        os.symlink(real, linked_directory)
        cases['linked artifact directory'] = linked_directory
        for label, source in cases.items():
            with self.subTest(label):
                target = self.directory / f'out-{label}'
                self.assertEqual(self.run_main('copy-lock', '--from', str(source), '--to', str(target),
                                               '--sha256', hashlib.sha256(b'# lock\n').hexdigest()), 1)
                self.assertFalse(target.exists())
        target = self.directory / 'link-target'
        os.symlink(secret, target)
        good = self.directory / 'good'
        good.mkdir()
        (good / 'Cargo.lock').write_text('# lock\n')
        self.assertEqual(self.run_main('copy-lock', '--from', str(good), '--to', str(target), '--sha256', hashlib.sha256(b'# lock\n').hexdigest()), 1)
        self.assertEqual(secret.read_text(), 'x')

    def test_copy_lock_refuses_a_cargo_lock_that_differs_from_the_recorded_one(self):
        source = self.directory / 'artifact'
        source.mkdir()
        (source / 'Cargo.lock').write_text('# another lock\n')
        target = self.directory / 'Cargo.lock'
        self.assertEqual(self.run_main('copy-lock', '--from', str(source), '--to', str(target), '--sha256', hashlib.sha256(b'# lock\n').hexdigest()), 1)
        self.assertFalse(target.exists())


if __name__ == '__main__':
    unittest.main()
