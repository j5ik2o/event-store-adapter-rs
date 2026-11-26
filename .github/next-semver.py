#! /usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import semver

args = sys.argv

for s in sys.stdin:
    # 先頭の v だけを外し、残り全体を SemVer として読む（`.*v?(\d+…)` で前を読み飛ばすと v10.2.0 が 0.2.0 になる。
    # event-store-adapter-scala#934 の指摘。設計の追記 design-r6.md §5 の1）。読めないタグは失敗する
    s = s.strip()
    if not s:
        continue
    cur_ver = semver.Version.parse(s[1:] if s.startswith('v') else s)
    if cur_ver.prerelease:
        # The latest tag is a pre-release: only a major bump may finalize it to its core version.
        if args[1] != "major":
            sys.exit(f"the latest tag is the pre-release {cur_ver}; only level=major may bump it (requested: {args[1]})")
        print(cur_ver.finalize_version())
        continue
    if args[1] == "major":
        next_ver = cur_ver.bump_major()
    elif args[1] == "minor":
        next_ver = cur_ver.bump_minor()
    else:
        next_ver = cur_ver.bump_patch()
    print(next_ver)
