"""crates-io.py の性質を Hypothesis で確かめる（設計の追記 design-r6.md §7 の P7・P8d・P19・P20・P21。除外は付けない）。"""
import argparse
import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import semver
from hypothesis import given, settings
from hypothesis import strategies as st

from release_test_support import REPOSITORY, load_crates_io

crates = load_crates_io()
SHA = 'a' * 40
SETTINGS = settings(max_examples=300, deadline=None, database=None)

release_versions = st.builds(lambda *parts: '.'.join(map(str, parts)),
                             st.integers(0, 12), st.integers(0, 12), st.integers(0, 12))
any_versions = st.one_of(release_versions, st.builds(lambda v, p: f'{v}-{p}', release_versions,
                                                      st.sampled_from(['rc.1', 'alpha', '0'])))
trusts = st.one_of(
    st.none(),
    st.fixed_dictionaries({'provider': st.sampled_from(['github', 'gitlab']),
                           'repository': st.sampled_from([REPOSITORY, 'someone/r']),
                           'sha': st.sampled_from([SHA, 'b' * 40]), 'run_id': st.just('1')}),
    st.just('not a dict'))
responses = st.one_of(
    st.builds(crates.Response, st.sampled_from([None, 500, 502, 503, 403, 429, 301]), st.just(b'')),
    st.builds(crates.Response, st.just(200), st.sampled_from([b'', b'{', b'[]', b'{"version": 1}', b'\xff'])))


class PrecheckProperties(unittest.TestCase):
    @SETTINGS
    @given(release_versions, st.one_of(st.none(), st.fixed_dictionaries({
        'yanked': st.sampled_from([False, True, None]), 'trustpub_data': trusts})),
        st.one_of(st.none(), any_versions), st.one_of(st.none(), release_versions.map(lambda v: 'v' + v)))
    def test_p19_the_decision_matches_the_independent_conditions(self, version, existing, max_version, latest):
        if existing is not None:
            existing = {'num': version, **existing}
        trust = existing and existing['trustpub_data']
        if existing is not None:
            expected = 'published' if (existing['yanked'] is False and isinstance(trust, dict)
                                       and trust['provider'] == 'github' and trust['repository'] == REPOSITORY
                                       and trust['sha'] == SHA) else 'stop'
        else:
            lower = max_version is None or semver.Version.parse(max_version) < semver.Version.parse(version)
            expected = 'publish' if lower and latest == f'v{version}' else 'stop'
        try:
            actual = crates.decide_precheck(version, SHA, REPOSITORY, existing, max_version, latest)
        except crates.Stop:
            actual = 'stop'
        self.assertEqual(actual, expected)

    @SETTINGS
    @given(responses)
    def test_p7_only_a_404_reads_as_absent(self, response):
        """照会の失敗（接続、5xx、壊れた本文）を「無い」と見ない。"""
        with self.assertRaises(crates.Stop):
            crates.read_version(response)
        self.assertIsNone(crates.read_version(crates.Response(404, b'')))

    @SETTINGS
    @given(responses)
    def test_p7_a_failed_versions_listing_stops(self, response):
        with self.assertRaises(crates.Stop):
            crates.read_max_version(response)


class FakeNetwork:
    """照会のたびに時計を進める偽の http。script は (応答の種類, 所要時間) の列。"""

    def __init__(self, script, version, checksum):
        self.script, self.version, self.checksum = list(script), version, checksum
        self.now = 0.0
        self.calls = 0
        self.seen = []

    def clock(self):
        return self.now

    def sleep(self, seconds):
        assert seconds >= 0
        self.now += seconds

    def http(self, url, deadline, timeout=60, headers=None):
        left = deadline.remaining()
        if left <= 0:
            return crates.Response(None)
        kind, duration = self.script[self.calls % len(self.script)] if self.script else ('absent', 1.0)
        self.calls += 1
        self.now += min(duration, timeout, left)
        if duration > min(timeout, left):
            return crates.Response(None)   # 時間切れ
        self.seen.append(kind)
        if '/index/' in url or url.endswith('/' + crates.CRATE):
            if kind == 'visible':
                return crates.Response(200, json.dumps({'vers': self.version, 'cksum': self.checksum}).encode())
            if kind == 'other':
                return crates.Response(200, json.dumps({'vers': '0.0.1', 'cksum': 'x'}).encode() + b'\nnot json')
            return crates.Response({'absent': 404, 'error': 503, 'cut': None}.get(kind, 404))
        self.seen[-1] = 'api'   # API の照会が締め切りの中で返った
        return crates.Response(200, json.dumps({'version': {'num': self.version, 'checksum': self.checksum}}).encode())


class ConfirmProperties(unittest.TestCase):
    @SETTINGS
    @given(st.lists(st.tuples(st.sampled_from(['absent', 'error', 'cut', 'other', 'visible']),
                              st.floats(0, 200, allow_nan=False)), max_size=12),
           st.floats(1, 600, allow_nan=False), st.floats(0.1, 120, allow_nan=False),
           st.sampled_from([None, 'match', 'mismatch']))
    def test_p7_p8d_p20_confirm_stays_within_the_deadline_and_succeeds_only_when_it_saw_the_version(
            self, script, limit, interval, checksum):
        verified = 'c' * 64
        network = FakeNetwork(script, '5.2.1', verified if checksum != 'mismatch' else 'd' * 64)
        args = argparse.Namespace(version='5.2.1', sha256=None if checksum is None else verified, interval=interval)
        with mock.patch.object(crates, 'http', network.http):
            try:
                crates.confirm(args, crates.Deadline(limit, network.clock), network.sleep)
                result = 'ok'
            except crates.Stop:
                result = 'stop'
        self.assertLessEqual(network.now, limit + 1e-6)
        # 確かめ終わる条件：index が版を返した（--sha256 があれば、そのあと API の照会も締め切りの中で返った）
        done = 'visible' in network.seen if checksum is None else 'api' in network.seen
        if result == 'ok':
            self.assertTrue(done)
            self.assertNotEqual(checksum, 'mismatch')
        elif checksum != 'mismatch':
            self.assertFalse(done, 'the version was served with the verified checksum, but confirm stopped')

    @SETTINGS
    @given(st.one_of(st.none(), st.text(max_size=70)), st.one_of(st.none(), st.text(max_size=70)))
    def test_p20_checksums_pass_only_when_both_equal_the_verified_one(self, index, api):
        verified = 'c' * 64
        try:
            crates.check_checksums('5.2.1', verified, index, api)
            passed = True
        except crates.Stop:
            passed = False
        self.assertEqual(passed, index == verified and api == verified)


class CrateProperties(unittest.TestCase):
    @SETTINGS
    @given(st.binary(max_size=200), st.binary(max_size=200))
    def test_p21_a_crate_passes_only_when_its_sha256_equals_the_recorded_one(self, data, other):
        with tempfile.TemporaryDirectory() as directory:
            crate = Path(directory) / 'x.crate'
            crate.write_bytes(data)
            recorded = hashlib.sha256(other).hexdigest()
            args = argparse.Namespace(crate=str(crate), sha256=recorded)
            try:
                crates.check_crate(args)
                passed = True
            except crates.Stop:
                passed = False
            self.assertEqual(passed, data == other)


if __name__ == '__main__':
    unittest.main()
