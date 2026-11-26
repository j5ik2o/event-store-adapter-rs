"""関門の性質を Hypothesis で確かめる（設計の追記 design-r6.md §7 の P1〜P3・P4r・P5・P6。除外は付けない）。

照合の相手は関門の関数ではなく、この中で別に書いた条件と、semver・tomllib のライブラリにする。
"""
import tomllib
import unittest

import semver
from hypothesis import example, given, settings
from hypothesis import strategies as st

from release_test_support import REAL_CARGO_TOML, REPOSITORY, VERSION_FILE, cargo_toml, load_gate, pull

gate = load_gate()
SHA = 'a' * 40
SETTINGS = settings(max_examples=400, deadline=None, database=None)

identifiers = st.one_of(st.integers(0, 30).map(str), st.sampled_from(['rc', 'alpha', '0a', 'a-b', '01', '']))
generated_versions = st.builds(
    lambda major, minor, patch, pre, build: f'{major}.{minor}.{patch}' + (pre and '-' + pre) + (build and '+' + build),
    st.integers(0, 12), st.integers(0, 12), st.integers(0, 12),
    st.one_of(st.just(''), st.lists(identifiers, min_size=1, max_size=3).map('.'.join)),
    st.sampled_from(['', '', '', 'build.1']))
versions = st.one_of(generated_versions,
                     st.sampled_from(['05.2.0', '5.2', '5.2.0.0', 'v5.2.0', '５.２.０', '5.٢.0', '5.2.0-', '']),
                     st.text(alphabet='0123456789.-+aZ', max_size=12))
release_versions = st.builds(lambda *parts: '.'.join(map(str, parts)),
                             st.integers(0, 12), st.integers(0, 12), st.integers(0, 12))


def accepted(version):
    """semver として正しく、build metadata が無く、数字が ASCII だけ（関門と別に書いた条件）。"""
    return version.isascii() and semver.Version.is_valid(version) and '+' not in version


# ---------------------------------------------------------------- 生成する lib/Cargo.toml

keys = st.sampled_from(['name = "x"', 'rust-version = "1.94.1"', 'edition = "2021"', 'readme = "../README.md"',
                        'description = "a version = \\"1\\" here"', '# version = "9.9.9"', '', '# ', 'keywords = ["a"]',
                        'versions = "1"', 'version_x = "1"', 'license = "MIT"'])
other_tables = st.sampled_from(['', '[package.metadata.docs.rs]\nall-features = true\n',
                                '[package.metadata.x]\nversion = "1.0.0"\n', '[dependencies.foo]\nversion = "1"\n',
                                '[dependencies]\nfoo = { version = "1" }\n', '[features]\ndefault = []\n',
                                'x = """\n[package]\nversion = "9.9.9"\n"""\n'])


@st.composite
def manifests(draw, version=None):
    """通る形の lib/Cargo.toml と、その版。"""
    version = version or draw(st.one_of(release_versions, st.sampled_from(['6.0.0-rc.1', '1.0.0-alpha.0'])))
    chosen = draw(st.lists(keys, max_size=6, unique=True))
    cut = draw(st.integers(0, len(chosen)))
    before, after = chosen[:cut], chosen[cut:]
    head = draw(st.sampled_from(['', '# head\n', '\n']))
    tail = draw(other_tables)
    text = head + '[package]\n' + ''.join(line + '\n' for line in before) + f'version = "{version}"\n' \
        + ''.join(line + '\n' for line in after) + tail
    return text, version


class LocateVersionProperties(unittest.TestCase):
    @SETTINGS
    @given(manifests(), release_versions)
    @example((REAL_CARGO_TOML, '5.2.0'), '5.2.1')
    def test_p4r_set_version_changes_only_the_version_and_reads_back(self, manifest, new):
        text, version = manifest
        located = gate.locate_version(text)
        self.assertEqual(located.version, version)
        self.assertEqual(tomllib.loads(text)['package']['version'], version)   # 関門の方が緩いことは無い
        written = gate.set_version(text, new)
        self.assertEqual(written[:located.start], text[:located.start])
        self.assertEqual(written[located.start + len(new):], text[located.end:])
        self.assertEqual(gate.locate_version(written).version, new)

    @SETTINGS
    @given(st.text(alphabet=st.sampled_from(list('[]package version="5.2.0"\n\r\t#\'﻿')), max_size=80))
    def test_p4r_whatever_the_gate_reads_is_what_toml_reads(self, text):
        try:
            located = gate.locate_version(text)
        except gate.GateError:
            return
        self.assertEqual(tomllib.loads(text)['package']['version'], located.version)
        self.assertTrue(accepted(located.version))

    @SETTINGS
    @given(manifests(version='5.2.0'), st.data())
    @example((REAL_CARGO_TOML, '5.2.0'), None)
    def test_p5_any_byte_change_outside_the_version_is_refused(self, manifest, data):
        parent, _ = manifest
        current = gate.set_version(parent, '5.2.1')
        located = gate.locate_version(current)
        if data is None:
            position, change = 0, 'insert'
        else:
            position = data.draw(st.integers(0, len(current)))
            change = data.draw(st.sampled_from(['insert', 'delete', 'replace']))
        if located.start <= position <= located.end:
            position = 0 if located.start > 0 else len(current)
        byte = 'Z' if data is None else data.draw(st.sampled_from(list(' #\n\rax"=﻿')))
        if change == 'insert':
            mutated = current[:position] + byte + current[position:]
        elif change == 'delete' and position < len(current) and not located.start <= position < located.end:
            mutated = current[:position] + current[position + 1:]
        elif position < len(current) and not located.start <= position < located.end:
            mutated = current[:position] + (byte if current[position] != byte else 'Q') + current[position + 1:]
        else:
            mutated = current + byte
        candidate = gate.Candidate(REPOSITORY, SHA, parent, mutated, (VERSION_FILE,),
                                   (pull(SHA, ref='release/v5.2.1'),), {'v5.2.0': 'b' * 40}, True)
        for mode in ('tag', 'publish'):
            with self.assertRaises(gate.GateError):
                result = gate.check_candidate(mode, candidate, 'v5.2.1')
                if result is None:   # tag のモードで「リリースなし」と読めた（版の行が壊れて同じ版に見えた）は、関門を通っていない
                    raise gate.GateError('no release')


class OrderProperties(unittest.TestCase):
    @SETTINGS
    @given(versions, versions)
    def test_p3_the_order_matches_semver(self, left, right):
        a, b = gate.parse(left, release_only=False), gate.parse(right, release_only=False)
        self.assertEqual(a is not None, accepted(left))
        self.assertEqual(b is not None, accepted(right))
        if a is not None and b is not None:
            expected = semver.Version.parse(left).compare(semver.Version.parse(right))
            self.assertEqual((a > b) - (a < b), expected)

    @SETTINGS
    @given(st.lists(st.one_of(versions.map(lambda v: 'v' + v), versions), max_size=8))
    def test_latest_tag_is_the_greatest_release_tag(self, names):
        releases = [name for name in names if name.startswith('v') and accepted(name[1:]) and '-' not in name]
        expected = max(releases, key=lambda name: semver.Version.parse(name[1:])) if releases else None
        latest = gate.latest_tag(names)
        if expected is None:
            self.assertIsNone(latest)
        else:
            self.assertEqual(semver.Version.parse(latest[1:]), semver.Version.parse(expected[1:]))


def oracle(candidate, mode, requested):
    """関門と別に書いた、通る条件（P1・P2）。通るならタグ名、何もしないなら None、拒否なら 'refuse'。"""
    try:
        before, after = tomllib.loads(candidate.parent)['package']['version'], \
            tomllib.loads(candidate.current)['package']['version']
    except Exception:
        return 'refuse'
    tag = f'v{after}'
    remote = candidate.tags.get(tag)
    if mode == 'tag' and (not accepted(after) or '-' in after or before == after or remote == SHA):
        return None if accepted(after) and accepted(before) else 'refuse'
    others = [semver.Version.parse(n[1:]) for n in candidate.tags
              if n != tag and n.startswith('v') and accepted(n[1:]) and '-' not in n]
    ok = (accepted(after) and '-' not in after and accepted(before)
          and semver.Version.parse(after) > semver.Version.parse(before)
          and candidate.on_main and candidate.parents == 1 and list(candidate.changed_files) == [VERSION_FILE]
          and candidate.parent.replace(f'version = "{before}"', f'version = "{after}"', 1) == candidate.current
          and len([p for p in candidate.pulls if p['merged_at'] and p['merge_commit_sha'] == SHA
                   and p['base']['ref'] == 'main' and p['head']['ref'] == f'release/v{after}'
                   and p['head']['repo'] and p['head']['repo']['full_name'] == REPOSITORY]) == 1
          and all((p.get('changed_files'), p.get('additions'), p.get('deletions')) == (1, 1, 1)
                  for p in candidate.pulls if p['merge_commit_sha'] == SHA and p['merged_at']
                  and p['head']['ref'] == f'release/v{after}')
          and all(semver.Version.parse(after) > other for other in others))
    if not ok:
        return 'refuse'
    if mode == 'publish':
        return tag if requested == tag and remote == SHA else 'refuse'
    return tag if remote is None else 'refuse'


@st.composite
def candidates(draw):
    """通る事実を作り、そこから 0〜3 か所を崩す。"""
    major, minor, patch = draw(st.integers(1, 12)), draw(st.integers(0, 9)), draw(st.integers(0, 9))
    target = f'{major}.{minor}.{patch}'
    before = draw(st.sampled_from([f'{major - 1}.9.9', f'{target}-rc.1', f'{major}.{minor}.{patch + 1}']))
    parent = cargo_toml(before)
    current = cargo_toml(target)
    changed = [VERSION_FILE]
    pulls = [pull(SHA, ref=f'release/v{target}')]
    tags = {f'v{major - 1}.0.0': 'b' * 40, 'v0.1.0': 'c' * 40}
    on_main, parents = True, 1
    for change in draw(st.lists(st.integers(0, 12), max_size=3)):
        if change == 0:
            on_main = False
        elif change == 1:
            parents = draw(st.sampled_from([0, 2]))
        elif change == 2:
            changed = draw(st.permutations([VERSION_FILE, 'lib/src/lib.rs']))
        elif change == 3:
            current = current.replace('edition = "2021"', 'edition = "2024"')
        elif change == 4:
            pulls = pulls + [pull(SHA, ref=f'release/v{target}')]
        elif change == 5:
            pulls = [pull(SHA, ref=f'release/v{target}', repo='someone/r')]
        elif change == 6:
            pulls = [pull(SHA, ref=draw(st.sampled_from([f'release/V{target}', 'feature/x'])))]
        elif change == 7:
            pulls = [pull(SHA, ref=f'release/v{target}', merged=False)]
        elif change == 8:
            tags[draw(st.sampled_from([f'v{major + 1}.0.0', f'v{major}.{minor}.{patch + 1}', f'v{target}-rc.1',
                                       f'v{major}0.0.0']))] = 'd' * 40
        elif change == 9:
            tags[f'v{target}'] = draw(st.sampled_from([SHA, 'e' * 40]))
        elif change == 10:
            current = cargo_toml(draw(st.sampled_from([before, f'{target}-rc.2', f'0.0.{patch}'])))
        elif change == 11:
            pulls = []
        else:
            # rebase のマージ：最後のコミットは版だけでも、pull request の全体はほかも変える
            pulls = [pull(SHA, ref=f'release/v{target}',
                          size=draw(st.sampled_from([(2, 2, 1), (1, 2, 2), (1, 1, 0), (0, 0, 0)])))]
    candidate = gate.Candidate(REPOSITORY, SHA, parent, current, tuple(changed), tuple(pulls), tags, on_main, parents)
    requested = draw(st.sampled_from([f'v{target}', f'v{target}', 'v0.0.1']))
    return candidate, requested


class CheckProperties(unittest.TestCase):
    @SETTINGS
    @given(candidates())
    def test_p1_p2_the_gate_matches_the_independent_conditions(self, generated):
        candidate, requested = generated
        for mode in ('tag', 'publish'):
            try:
                result = gate.check_candidate(mode, candidate, requested)
            except gate.GateError:
                result = 'refuse'
            self.assertEqual(result, oracle(candidate, mode, requested), mode)

    @SETTINGS
    @given(candidates())
    def test_p2_publish_passes_only_for_the_tag_that_tag_mode_returns(self, generated):
        candidate, requested = generated
        try:
            published = gate.check_candidate('publish', candidate, requested)
        except gate.GateError:
            return
        untagged = gate.Candidate(candidate.repository, candidate.sha, candidate.parent, candidate.current,
                                  candidate.changed_files, candidate.pulls,
                                  {k: v for k, v in candidate.tags.items() if k != published}, candidate.on_main,
                                  candidate.parents)
        self.assertEqual(gate.check_candidate('tag', untagged), published)
        self.assertEqual(candidate.tags[published], SHA)

    @SETTINGS
    @given(candidates())
    def test_p6_a_tag_on_the_same_commit_is_idempotent(self, generated):
        candidate, _ = generated
        try:
            tag = gate.check_candidate('tag', candidate)
        except gate.GateError:
            return
        if tag is None:
            return
        tagged = gate.Candidate(candidate.repository, candidate.sha, candidate.parent, candidate.current,
                                candidate.changed_files, candidate.pulls, {**candidate.tags, tag: SHA},
                                candidate.on_main, candidate.parents)
        self.assertIsNone(gate.check_candidate('tag', tagged))
        moved = gate.Candidate(candidate.repository, candidate.sha, candidate.parent, candidate.current,
                               candidate.changed_files, candidate.pulls, {**candidate.tags, tag: 'f' * 40},
                               candidate.on_main, candidate.parents)
        with self.assertRaises(gate.GateError):
            gate.check_candidate('tag', moved)


if __name__ == '__main__':
    unittest.main()
