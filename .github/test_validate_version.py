import importlib.util
from pathlib import Path
import subprocess
import sys
import unittest

SCRIPT = Path(__file__).with_name("validate-version.py")
spec = importlib.util.spec_from_file_location("validate_version", SCRIPT)
validate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(validate)

RELEASED_TAGS = ("v1.4.0", "v2.0.0", "v3.0.6")

INVALID_FORMATS = (
    "v5.0.0",
    "5.0.0-alpha.1",
    "5.0.0+build.1",
    "5.0",
    "5",
    "5.0.0.0",
    "",
    " 5.0.0",
    "5.0.0 ",
    "5.0.0\n",
    "05.0.0",
    "５.0.0",
    "a.b.c",
)


def tag_list(tags):
    return "".join(f"{tag}\n" for tag in tags)


def validate_version(version, tags):
    return subprocess.run([sys.executable, str(SCRIPT), version], input=tag_list(tags), text=True, capture_output=True)


class ValidateVersionTest(unittest.TestCase):
    def assert_rejected(self, result):
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")
        self.assertNotEqual(result.stderr, "")

    def test_greater_release_version_is_accepted(self):
        for version in ("5.0.0", "3.0.7", "3.1.0"):
            with self.subTest(version=version):
                self.assertEqual(validate.validate_version(version, tag_list(RELEASED_TAGS)), version)

    def test_cli_prints_accepted_version(self):
        for version in ("5.0.0", "3.0.7", "3.1.0"):
            with self.subTest(version=version):
                result = validate_version(version, RELEASED_TAGS)
                self.assertEqual((result.returncode, result.stdout, result.stderr), (0, f"{version}\n", ""))

    def test_invalid_format_is_rejected(self):
        for version in INVALID_FORMATS:
            with self.subTest(version=version):
                with self.assertRaises(ValueError):
                    validate.validate_version(version, tag_list(RELEASED_TAGS))

    def test_cli_rejects_invalid_format_without_output(self):
        for version in INVALID_FORMATS:
            with self.subTest(version=version):
                self.assert_rejected(validate_version(version, RELEASED_TAGS))

    def test_version_not_greater_than_greatest_tag_is_rejected(self):
        tags = ("v3.0.4", "v3.0.6")
        for version in ("3.0.5", "3.0.6", "2.9.9"):
            with self.subTest(version=version):
                self.assert_rejected(validate_version(version, tags))

    def test_versions_are_compared_as_numbers(self):
        self.assert_rejected(validate_version("9.0.0", ("v2.0.0", "v10.0.0")))

    def test_existing_tag_of_the_same_name_is_rejected_even_when_a_pre_release_tag_sorts_last(self):
        # `sort -V` lists v5.0.0-alpha.1 after v5.0.0, so the greatest-tag check alone would accept 5.0.0.
        self.assert_rejected(validate_version("5.0.0", ("v4.0.0", "v5.0.0", "v5.0.0-alpha.1")))

    def test_missing_or_unreadable_greatest_tag_is_rejected(self):
        for label, tags in (("no tags", ()), ("not a version", ("v3.0.6", "vfoo"))):
            with self.subTest(label=label):
                self.assert_rejected(validate_version("5.0.0", tags))

    def test_cli_without_a_version_argument_fails(self):
        result = subprocess.run([sys.executable, str(SCRIPT)], input=tag_list(RELEASED_TAGS), text=True, capture_output=True)
        self.assert_rejected(result)


if __name__ == "__main__":
    unittest.main()
