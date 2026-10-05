import importlib.util
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("semver-level.py")
spec = importlib.util.spec_from_file_location("semver_level", SCRIPT)
semver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(semver)


def commit(subject, body=""):
    return f"{subject}\x1f{body}\x1e\n"


class SemverLevelTest(unittest.TestCase):
    def test_breaking_subjects(self):
        for subject in ("feat!: replace API", "fix(scope)!: replace API", "perf!: replace API"):
            with self.subTest(subject=subject):
                self.assertEqual(semver.semver_level(commit(subject)), "major")

    def test_breaking_body(self):
        for marker in ("BREAKING CHANGE:", "BREAKING-CHANGE:"):
            with self.subTest(marker=marker):
                body = f"Description with\ttabs.\n\n{marker} replace API\nMore details.\n"
                self.assertEqual(semver.semver_level(commit("fix: update", body)), "major")

    def test_minor_subjects(self):
        for kind in ("feat", "revert"):
            with self.subTest(kind=kind):
                self.assertEqual(semver.semver_level(commit(f"{kind}(scope): update")), "minor")

    def test_patch_subjects(self):
        for kind in ("perf", "fix", "build", "ci", "docs", "style", "refactor", "test", "chore", "custom"):
            with self.subTest(kind=kind):
                self.assertEqual(semver.semver_level(commit(f"{kind}: update")), "patch")

    def test_highest_level_wins_in_either_order(self):
        cases = (
            ([commit("perf: update"), commit("feat: update")], "minor"),
            ([commit("feat: update"), commit("fix!: update")], "major"),
            ([commit("fix: update"), commit("chore: update", "BREAKING CHANGE: update")], "major"),
        )
        for records, expected in cases:
            for ordered in (records, records[::-1]):
                with self.subTest(records=ordered):
                    self.assertEqual(semver.semver_level("".join(ordered)), expected)

    def test_markers_must_be_at_start_of_body_line(self):
        self.assertEqual(semver.semver_level(commit("fix: update!", "Mention BREAKING CHANGE: here")), "patch")

    def test_multiline_body_does_not_become_another_subject(self):
        self.assertEqual(semver.semver_level(commit("fix: update", "feat!: quoted\ntext\twith tabs\n")), "patch")

    def test_empty_and_nonconventional_input(self):
        for log in ("", "\n", commit("Update README")):
            with self.subTest(log=log):
                self.assertIsNone(semver.semver_level(log))

    def test_cli(self):
        for log, code, output in ((commit("perf: update"), 0, "patch\n"), ("", 1, "")):
            with self.subTest(log=log):
                result = subprocess.run([sys.executable, str(SCRIPT)], input=log, text=True, capture_output=True)
                self.assertEqual((result.returncode, result.stdout, result.stderr), (code, output, ""))

    def test_real_git_log_preserves_body_separators(self):
        with tempfile.TemporaryDirectory() as directory:
            def git(*args):
                return subprocess.run(["git", "-C", directory, *args], check=True, text=True, capture_output=True).stdout

            git("init", "-q")
            git("-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "--allow-empty", "-q",
                "-m", "perf: improve speed", "-m", "Details with\ttabs.\n\nBREAKING-CHANGE: replace API\nMore details.")
            workflow = SCRIPT.with_name("workflows") / "lib-bump-version.yml"
            filters = re.findall(r"--grep='([^']+)'", workflow.read_text())[:2]
            for pattern in filters:
                for subject, body in (
                    ("feat!: replace API", ""),
                    ("fix(scope)!: replace API", ""),
                    ("Update API", "Details with\ttabs.\n\nBREAKING CHANGE: replace API"),
                    ("Update API", "Details with\ttabs.\n\nBREAKING-CHANGE: replace API"),
                ):
                    with self.subTest(subject=subject, body=body, pattern=pattern):
                        git("-c", "user.name=Test", "-c", "user.email=test@example.com", "commit",
                            "--allow-empty", "-q", "-m", subject, "-m", body)
                        log = git("log", "HEAD^..HEAD", "--pretty=format:%s%x1f%b%x1e", "--no-merges",
                                  "-P", f"--grep={pattern}")
                        self.assertTrue(log)
                        self.assertEqual(semver.semver_level(log), "major")


if __name__ == "__main__":
    unittest.main()
