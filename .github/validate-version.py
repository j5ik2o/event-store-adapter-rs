#!/usr/bin/env python3
"""Validate a requested release version against the existing v* tags.

Usage: git tag -l 'v*' | sort -V | validate-version.py X.Y.Z

The tags must be listed in `sort -V` order, so the last one is the greatest.
Prints the requested version when it is acceptable; otherwise exits non-zero
without writing to stdout.
"""

import re
import sys

import semver

# [0-9] instead of \d (which also matches non-ASCII digits) and fullmatch instead of
# match + $ (which accepts a trailing newline). Leading zeros are rejected because
# neither semver nor Cargo accepts them.
RELEASE_VERSION = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")


def validate_version(requested, sorted_tags):
    if not RELEASE_VERSION.fullmatch(requested):
        raise ValueError(f"the requested version {requested!r} must be X.Y.Z (digits only, no v prefix, pre-release or build metadata)")
    tags = [line for line in sorted_tags.splitlines() if line]
    if not tags:
        raise ValueError("no v* tag exists, so the requested version cannot be compared with the greatest tag")
    if f"v{requested}" in tags:
        raise ValueError(f"the tag v{requested} already exists")
    latest = tags[-1]
    try:
        if not latest.startswith("v"):
            raise ValueError("it does not start with v")
        latest_version = semver.VersionInfo.parse(latest[1:])
    except ValueError as e:
        raise ValueError(f"the greatest tag {latest!r} is not a semantic version ({e})") from e
    if semver.VersionInfo.parse(requested) <= latest_version:
        raise ValueError(f"the requested version {requested} must be greater than the greatest tag {latest}")
    return requested


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(f"usage: {sys.argv[0]} X.Y.Z (the sorted v* tags are read from stdin)")
    try:
        print(validate_version(sys.argv[1], sys.stdin.read()))
    except ValueError as e:
        sys.exit(str(e))
