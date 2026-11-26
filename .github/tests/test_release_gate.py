"""関門（release-gate.py）の決まった例（設計の追記 design-r6.md §2・§9）。性質は test_release_gate_properties.py。"""
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from release_test_support import (GITHUB, ON_DISK_CARGO_TOML, REAL_CARGO_TOML, REPOSITORY, VERSION_FILE, cargo_toml,
                                  fixture_manifest, load_gate, pull)

gate = load_gate()
SHA = 'a' * 40


def candidate(parent=None, current=None, changed=(VERSION_FILE,), pulls=None, tags=None, on_main=True, parents=1):
    parent = cargo_toml('5.2.0') if parent is None else parent
    current = cargo_toml('5.2.1') if current is None else current
    return gate.Candidate(REPOSITORY, SHA, parent, current, tuple(changed),
                          tuple([pull(SHA)] if pulls is None else pulls),
                          {'v5.2.0': 'b' * 40} if tags is None else tags, on_main, parents)


class LocateVersionTest(unittest.TestCase):
    def test_the_lib_cargo_toml_on_disk_is_accepted(self):
        """作業ツリーの版がいくつでも（版上げの pull request の CI でも）通る。"""
        located = gate.locate_version(ON_DISK_CARGO_TOML)
        self.assertEqual(ON_DISK_CARGO_TOML[located.start:located.end], located.version)
        self.assertEqual(fixture_manifest(ON_DISK_CARGO_TOML, located.version), ON_DISK_CARGO_TOML)

    def test_the_fixture_does_not_depend_on_the_version_on_disk(self):
        """#375 の CodeRabbit の指摘：版上げのあとも、試験の親と子は求めた版になる。"""
        current = gate.locate_version(ON_DISK_CARGO_TOML).version
        for on_disk in [v for v in ('5.2.1', '6.0.0', '6.1.0-rc.1', '99.0.0') if v != current]:
            bumped = ON_DISK_CARGO_TOML.replace(f'version = "{current}"', f'version = "{on_disk}"', 1)
            self.assertNotEqual(bumped, ON_DISK_CARGO_TOML)
            self.assertEqual(fixture_manifest(bumped), REAL_CARGO_TOML)
            self.assertEqual(gate.locate_version(fixture_manifest(bumped, '7.0.0')).version, '7.0.0')
            self.assertNotEqual(fixture_manifest(bumped, '5.2.0'), fixture_manifest(bumped, '5.2.1'))

    def test_the_real_lib_cargo_toml_is_accepted(self):
        located = gate.locate_version(REAL_CARGO_TOML)
        self.assertEqual(located.version, '5.2.0')
        self.assertEqual(REAL_CARGO_TOML[located.start:located.end], '5.2.0')
        self.assertIn('rust-version = "1.94.1"', REAL_CARGO_TOML)
        self.assertIn('[package.metadata.docs.rs]', REAL_CARGO_TOML)

    def test_set_version_changes_only_the_version(self):
        text = gate.set_version(REAL_CARGO_TOML, '5.2.1')
        self.assertEqual(text, REAL_CARGO_TOML.replace('version = "5.2.0"', 'version = "5.2.1"', 1))
        self.assertEqual(gate.locate_version(text).version, '5.2.1')

    def test_refused_shapes(self):
        base = '[package]\nname = "x"\nversion = "5.2.0"\n'
        cases = {
            'workspace version': '[package]\nname = "x"\nversion.workspace = true\n',
            'no spaces': '[package]\nname = "x"\nversion="5.2.0"\n',
            'two spaces': '[package]\nname = "x"\nversion  = "5.2.0"\n',
            'quoted key': '[package]\nname = "x"\n"version" = "5.2.0"\n',
            'single-quoted key': "[package]\nname = \"x\"\n'version' = \"5.2.0\"\n",
            'single-quoted value': "[package]\nname = \"x\"\nversion = '5.2.0'\n",
            'trailing comment': '[package]\nname = "x"\nversion = "5.2.0" # now\n',
            'crlf': '[package]\r\nname = "x"\r\nversion = "5.2.0"\r\n',
            'bom': '﻿' + base,
            'table before package': '[lib]\nname = "y"\n[package]\nname = "x"\nversion = "5.2.0"\n',
            'two versions': base + 'version = "5.2.1"\n',
            'version in a multi-line string': '[package]\nname = "x"\ndescription = """\nversion = "9.9.9"\n"""\n'
                                              'version = "5.2.0"\n',
            'fake package in a string': 'x = """\n[package]\nversion = "9.9.9"\n"""\n[package]\nversion = "5.2.0"\n',
            'non-ascii digits': '[package]\nname = "x"\nversion = "５.2.0"\n',
            'arabic-indic digit': '[package]\nname = "x"\nversion = "5.٢.0"\n',
            'leading zero': '[package]\nname = "x"\nversion = "05.2.0"\n',
            'build metadata': '[package]\nname = "x"\nversion = "5.2.0+b"\n',
            'no version': '[package]\nname = "x"\n',
            'invalid toml': '[package]\nname = "x\nversion = "5.2.0"\n',
            'indented version': '[package]\nname = "x"\n  version = "5.2.0"\n',
        }
        for label, text in cases.items():
            with self.subTest(label):
                with self.assertRaises(gate.GateError):
                    gate.locate_version(text)

    def test_accepted_shapes(self):
        cases = {
            'rust-version before': ('[package]\nrust-version = "1.94.1"\nversion = "5.2.0"\n', '5.2.0'),
            'metadata table after': ('[package]\nversion = "5.2.0"\n[package.metadata.x]\nversion = "1.0.0"\n', '5.2.0'),
            'dependency version after': ('[package]\nversion = "5.2.0"\n[dependencies.foo]\nversion = "1"\n', '5.2.0'),
            'inline dependency': ('[package]\nversion = "5.2.0"\n[dependencies]\nfoo = { version = "1" }\n', '5.2.0'),
            'comment before': ('# head\n\n[package]\nversion = "5.2.0"\n', '5.2.0'),
            'prerelease': ('[package]\nversion = "6.0.0-rc.1"\n', '6.0.0-rc.1'),
            'no final newline': ('[package]\nversion = "5.2.0"', '5.2.0'),
        }
        for label, (text, version) in cases.items():
            with self.subTest(label):
                self.assertEqual(gate.locate_version(text).version, version)


class CheckCandidateTest(unittest.TestCase):
    def test_a_version_pull_request_squash_is_tagged_and_published(self):
        self.assertEqual(gate.check_candidate('tag', candidate()), 'v5.2.1')
        published = candidate(tags={'v5.2.0': 'b' * 40, 'v5.2.1': SHA})
        self.assertEqual(gate.check_candidate('publish', published, 'v5.2.1'), 'v5.2.1')

    def test_the_tag_mode_does_nothing_without_a_release(self):
        cases = {
            'unchanged': candidate(current=cargo_toml('5.2.0')),
            'prerelease': candidate(current=cargo_toml('6.0.0-rc.1')),
            'already tagged here': candidate(tags={'v5.2.1': SHA}),
            'no parent file': gate.Candidate(REPOSITORY, SHA, None, cargo_toml('5.2.1'), (VERSION_FILE,), (), {}, True),
        }
        for label, value in cases.items():
            with self.subTest(label):
                self.assertIsNone(gate.check_candidate('tag', value))

    def test_refusals(self):
        cases = {
            'comment change': candidate(current=cargo_toml('5.2.1').replace('# FR8.3', '# FR8.4')),
            'dependency change': candidate(current=cargo_toml('5.2.1').replace('dep:uuid"]', 'dep:uuid", "x"]', 1)),
            'another file': candidate(changed=(VERSION_FILE, 'lib/src/lib.rs')),
            'lower version': candidate(current=cargo_toml('5.1.9')),
            'not on main': candidate(on_main=False),
            'merge commit': candidate(parents=2),
            'no pull request': candidate(pulls=[]),
            'fork': candidate(pulls=[pull(SHA, repo='someone/r')]),
            'other branch': candidate(pulls=[pull(SHA, ref='feature/x')]),
            'unmerged': candidate(pulls=[pull(SHA, merged=False)]),
            'other base': candidate(pulls=[pull(SHA, base='develop')]),
            'newer tag': candidate(tags={'v5.2.0': 'b' * 40, 'v5.3.0': 'c' * 40}),
            'two-digit newer tag': candidate(tags={'v10.0.0': 'c' * 40}),
            'tag on another commit': candidate(tags={'v5.2.1': 'c' * 40}),
            'rust-version change': candidate(current=cargo_toml('5.2.1').replace('1.94.1', '1.95.0')),
            # #375 の CodeRabbit の指摘：rebase のマージでは、最後のコミットだけを見ても前のコミットの変更が分からない
            'rebase with a source change': candidate(pulls=[pull(SHA, size=(2, 2, 1))]),
            'rebase with a manifest change': candidate(pulls=[pull(SHA, size=(1, 2, 2))]),
            'only the list item': candidate(pulls=[{k: v for k, v in pull(SHA).items()
                                                    if k not in ('changed_files', 'additions', 'deletions')}]),
        }
        for label, value in cases.items():
            with self.subTest(label):
                with self.assertRaises(gate.GateError):
                    gate.check_candidate('tag', value)
        with self.assertRaises(gate.GateError):
            gate.check_candidate('publish', candidate(tags={'v5.2.1': SHA}), 'v5.2.2')
        with self.assertRaises(gate.GateError):
            gate.check_candidate('publish', candidate(), 'v5.2.1')   # リモートにタグが無い

    def test_check_next(self):
        gate.check_next(cargo_toml('5.2.0'), '5.2.1')
        for version in ('5.2.0', '5.1.9', '5.2.1-rc.1', 'v5.2.1'):
            with self.subTest(version):
                with self.assertRaises(gate.GateError):
                    gate.check_next(cargo_toml('5.2.0'), version)
        with self.assertRaises(gate.GateError):
            gate.check_next(cargo_toml('5.3.0'), '5.2.1')   # マージ済みでタグの無い版上げがある


class CommandLineTest(unittest.TestCase):
    def run_gate(self, *args, stdin=''):
        return subprocess.run([sys.executable, str(GITHUB / 'release-gate.py'), *args], input=stdin,
                              capture_output=True, text=True)

    def test_check_reads_the_inputs_that_release_gate_inputs_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            d = Path(directory)
            (d / 'parent-version').write_text(cargo_toml('5.2.0'))
            (d / 'version').write_text(cargo_toml('5.2.1'))
            (d / 'changed-files').write_text(VERSION_FILE + '\n')
            (d / 'pulls.json').write_text(json.dumps([pull(SHA)]))
            (d / 'remote-tags').write_text(f'{"1" * 40}\trefs/tags/v5.2.0\n{"b" * 40}\trefs/tags/v5.2.0^{{}}\n')
            (d / 'parents').write_text('1\n')
            args = ['check', '--mode', 'tag', '--repository', REPOSITORY, '--sha', SHA, '--on-main', 'true',
                    '--parents', str(d / 'parents'), '--parent-version', str(d / 'parent-version'),
                    '--version-file', str(d / 'version'), '--changed-files', str(d / 'changed-files'),
                    '--pulls', str(d / 'pulls.json'), '--remote-tags', str(d / 'remote-tags')]
            result = self.run_gate(*args)
            self.assertEqual((result.returncode, result.stdout), (0, 'v5.2.1\n'), result.stderr)
            (d / 'parent-version').unlink()
            result = self.run_gate(*args)
            self.assertEqual((result.returncode, result.stdout), (0, ''))
            publish = self.run_gate(*[a if a != 'tag' else 'publish' for a in args], '--tag', 'v5.2.1')
            self.assertEqual(publish.returncode, 1)
            self.assertIn('has no lib/Cargo.toml', publish.stderr)

    def test_latest_tag_reads_two_digit_majors(self):
        result = self.run_gate('latest-tag', stdin='v5.2.0\nv10.0.0\nv9.9.9\nv11.0.0-rc.1\nV12.0.0\nv013.0.0\n')
        self.assertEqual(result.stdout, 'v10.0.0\n')

    def test_set_version_and_read_version(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'Cargo.toml'
            path.write_text(REAL_CARGO_TOML)
            self.assertEqual(self.run_gate('set-version', '5.3.0', '--file', str(path)).returncode, 0)
            self.assertEqual(self.run_gate('read-version', '--file', str(path)).stdout, '5.3.0\n')
            self.assertEqual(path.read_text(), cargo_toml('5.3.0'))
            refused = self.run_gate('set-version', '5.3.0-rc.1', '--file', str(path))
            self.assertEqual(refused.returncode, 1)
            self.assertEqual(path.read_text(), cargo_toml('5.3.0'))


if __name__ == '__main__':
    unittest.main()
