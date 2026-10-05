#!/usr/bin/env python3
"""Determine the highest version bump from git log subjects and bodies."""

import re
import sys

SUBJECT = re.compile(r"^([a-z]+)(?:\([^\r\n]*\))?(!)?:")
BREAKING = re.compile(r"^(?:BREAKING CHANGE|BREAKING-CHANGE):", re.MULTILINE)


def semver_level(log):
    level = None
    for record in log.split("\x1e"):
        record = record.lstrip("\n")
        if not record:
            continue
        subject, body = record.split("\x1f", 1)
        match = SUBJECT.match(subject)
        if BREAKING.search(body) or (match and match.group(2)):
            return "major"
        if match:
            if match.group(1) in {"feat", "revert"}:
                level = "minor"
            elif level is None:
                level = "patch"
    return level


if __name__ == "__main__":
    level = semver_level(sys.stdin.read())
    if level is None:
        sys.exit(1)
    print(level)
