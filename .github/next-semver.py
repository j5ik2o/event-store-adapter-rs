#! /usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import semver
import re

args = sys.argv

for s in sys.stdin:
    # The latest tag is a pre-release: only a major bump may finalize it to its core version.
    # The prefix is lazy so that the core version keeps all of its leading digits (v14.0.0-alpha.1 -> 14.0.0).
    pre = re.match(r'.*?v?(\d+\.\d+\.\d+)(-[0-9A-Za-z.-]+)', s)
    if pre:
        if args[1] != "major":
            sys.exit(f"the latest tag is the pre-release {pre.group(1)}{pre.group(2)}; only level=major may bump it (requested: {args[1]})")
        print(semver.VersionInfo.parse(pre.group(1)))
        continue
    r = re.match(r'.*v?(\d+\.\d+\.\d+)', s)
    if r:
        cur_ver = semver.VersionInfo.parse(r.group(1))
        if args[1] == "major":
            next_ver = cur_ver.bump_major()
        elif args[1] == "minor":
            next_ver = cur_ver.bump_minor()
        else:
            next_ver = cur_ver.bump_patch()
        print(next_ver)