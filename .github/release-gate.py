#!/usr/bin/env python3
"""リリースの関門。タグを打つ前と、crates.io へ公開する前に、同じ条件で確かめる。

event-store-adapter-scala の .github/release-gate.py（48e4256）を写し、版のファイルを lib/Cargo.toml の
[package].version に替えたもの（設計の追記 design-r6.md §2）。公開先に必要な版（REQUIRED_PUBLISHED）は持たない（§6）。

latest-tag                   標準入力のタグ名から、最大の正式版のタグ (vX.Y.Z) を書く（無ければ何も書かない）。
read-version                 lib/Cargo.toml の版を書く（版の位置の判定 locate_version を通ったときだけ）。
set-version <X.Y.Z>          lib/Cargo.toml の版の位置だけを書き換える。
check-next <X.Y.Z>           次の版が lib/Cargo.toml の今の版より大きいかを確かめる（版上げの PR を開く前。下げる PR を作らない）。
check --mode tag|publish ... コミットが、版上げの PR を squash したものかを確かめる。
  tag     : 親に版のファイルが無い、版が変わっていない、正式版 (X.Y.Z) でない、
            タグが既に同じコミットにある、のときは何も書かない。通るならタグ名を書く。
  publish : --tag のタグが関門を通るなら、タグ名を書く。
  どちらも、通らないときは理由を ::error:: で書いて終了コード 1 を返す。
"""
import argparse
import json
import os
import re
import sys
import tomllib
from dataclasses import dataclass
from typing import Optional

NUMERIC = r'0|[1-9][0-9]*'
IDENTIFIER = r'0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*'
# [0-9] と [A-Za-z] だけ（\d は ASCII 以外の数字にも合う）。build metadata（+…）は受け付けない
VERSION = re.compile(
    rf'({NUMERIC})\.({NUMERIC})\.({NUMERIC})(?:-((?:{IDENTIFIER})(?:\.(?:{IDENTIFIER}))*))?')
VERSION_FILE = 'lib/Cargo.toml'
PACKAGE_HEADER = '[package]'
# 版の行。値は二重引用の基本の文字列だけ。行末のコメント・空白・\r は受け付けない
VERSION_LINE = re.compile(r'version = "([^"\\]*)"')
# キーの字句がちょうど version の行（空白を除いた行頭が version で次が空白・=・.、または引用したキー）
VERSION_KEY = re.compile(r'[ \t]*(?:version(?=[ \t=.])|"version"|\'version\')')


class GateError(Exception):
    pass


@dataclass(frozen=True)
class Located:
    """lib/Cargo.toml の版の位置。text[start:end] が版の文字列。"""
    version: str
    start: int
    end: int


@dataclass(frozen=True)
class Candidate:
    """関門にかけるコミットと、その周りの事実。parent と current は版のファイルの中身（無ければ None）。"""
    repository: str
    sha: str
    parent: Optional[str]
    current: Optional[str]
    changed_files: tuple
    pulls: tuple
    tags: dict
    on_main: bool
    parents: int = 1


def parse(version, release_only=True):
    """semver の優先順位で比べられるキーを返す。版でない、または release_only でプレリリースなら None。"""
    match = VERSION.fullmatch(version or '')
    if not match or (release_only and match.group(4)):
        return None
    core = tuple(int(part) for part in match.groups()[:3])
    if match.group(4) is None:
        return core + (1, ())
    identifiers = tuple((0, int(part)) if part.isdigit() else (1, part) for part in match.group(4).split('.'))
    return core + (0, identifiers)


def _comment_or_blank(line):
    stripped = line.strip(' \t')
    return stripped == '' or stripped.startswith('#')


def locate_version(text):
    """lib/Cargo.toml の [package] の version の位置を求める（設計 §2）。受け付けない形は GateError。

    1. 最初の空でもコメントでもない行が、ちょうど [package]。
    2. [package] の表（次の [ で始まる行まで）に、ちょうど `version = "<版>"` の行が1つ。<版> は X.Y.Z[-pre]。
    3. 同じ表に、キーの字句がちょうど version のほかの行が無い（rust-version は当たらない）。
    4. tomllib で読んだ package.version が同じ版（複数行の文字列の中の偽の [package]・version に騙されない）。
    BOM と \\r\\n の行は 1.〜3. に合わないので拒否する。
    """
    lines = text.split('\n')
    offsets, position = [], 0
    for line in lines:
        offsets.append(position)
        position += len(line) + 1
    index = 0
    while index < len(lines) and _comment_or_blank(lines[index]):
        index += 1
    if index == len(lines) or lines[index] != PACKAGE_HEADER:
        raise GateError(f'{VERSION_FILE} must start with the table {PACKAGE_HEADER}')
    found = None
    for number in range(index + 1, len(lines)):
        line = lines[number]
        if line.lstrip(' \t').startswith('['):
            break
        if not VERSION_KEY.match(line):
            continue
        match = VERSION_LINE.fullmatch(line)
        if found is not None or not match:
            raise GateError(f'{VERSION_FILE} must hold exactly one line `version = "X.Y.Z"` in {PACKAGE_HEADER}, '
                            f'but holds {line!r}')
        if parse(match.group(1), release_only=False) is None:
            raise GateError(f'{VERSION_FILE} holds the version {match.group(1)!r}, which is not X.Y.Z[-prerelease]')
        start = offsets[number] + len('version = "')
        found = Located(match.group(1), start, start + len(match.group(1)))
    if found is None:
        raise GateError(f'{VERSION_FILE} has no line `version = "X.Y.Z"` in {PACKAGE_HEADER}')
    try:
        parsed = tomllib.loads(text)
    except tomllib.TOMLDecodeError as error:
        raise GateError(f'{VERSION_FILE} is not valid TOML: {error}') from error
    package = parsed.get('package')
    if not isinstance(package, dict) or package.get('version') != found.version:
        raise GateError(f'{VERSION_FILE}: the line `version = "{found.version}"` is not the version that TOML reads '
                        f'({package.get("version") if isinstance(package, dict) else None!r})')
    return found


def set_version(text, version):
    """版の位置だけを書き換えた中身を返す。ほかのバイトは元のまま。"""
    if parse(version) is None:
        raise GateError(f'{version!r} is not a release version (X.Y.Z)')
    located = locate_version(text)
    result = text[:located.start] + version + text[located.end:]
    if locate_version(result).version != version:
        raise GateError(f'{VERSION_FILE}: the version could not be written')
    return result


def stable_tags(names):
    return {name: parse(name[1:]) for name in names if name.startswith('v') and parse(name[1:])}


def latest_tag(names):
    candidates = [(key, name) for name, key in stable_tags(names).items()]
    return max(candidates)[1] if candidates else None


def remote_tags(ls_remote):
    """`git ls-remote --tags` の出力を {タグ名: コミット} にする（注釈つきタグは ^{} の先を使う）。"""
    tags = {}
    for line in ls_remote.splitlines():
        if not line.strip():
            continue
        sha, ref = line.split('\t', 1)
        name = ref.removeprefix('refs/tags/')
        if name.endswith('^{}'):
            tags[name[:-3]] = sha
        else:
            tags.setdefault(name, sha)
    return tags


def merged_from(pull, candidate, branch):
    head = pull['head']
    return (
        bool(pull['merged_at']) and pull['merge_commit_sha'] == candidate.sha
        and pull['base']['ref'] == 'main' and head['ref'] == branch
        and head['repo'] is not None and head['repo']['full_name'] == candidate.repository
    )


def whole_pull_is_the_version(pull, after):
    """pull request の全体の差分（release-gate-inputs.sh が git で求めた、マージの基点から head まで）が、
    lib/Cargo.toml の [package] の版を after に替えることだけか。

    rebase のマージでは、最後のコミットも親が1つで merge_commit_sha に一致し、前のコミットの変更は候補のコミットの
    差分に現れない。変更の件数だけでは、途中で版を下げてほかのファイルを変え、最後に戻す pull request を見分けられない
    （#375 の CodeRabbit の指摘）。全体の差分そのものを、候補のコミットと同じ規則（P5）で確かめる。"""
    diff = pull.get('whole_diff')
    if not isinstance(diff, dict) or diff.get('files') != [VERSION_FILE]:
        return False
    before_text, after_text = diff.get('before'), diff.get('after')
    if not isinstance(before_text, str) or not isinstance(after_text, str):
        return False
    try:
        return (locate_version(after_text).version == after and locate_version(before_text).version != after
                and set_version(before_text, after) == after_text)
    except GateError:
        return False


def approved_release(candidate, before, after):
    version = parse(after)
    if version is None:
        raise GateError(f'{after} is not a release version (X.Y.Z)')
    if not candidate.on_main:
        raise GateError(f'{candidate.sha} is not reachable from main')
    if candidate.parents != 1:
        # merge_commit_sha は通常のマージでも PR に結び付くので、リポジトリのマージ方式の設定に頼らずここで落とす
        raise GateError(f'{candidate.sha} has {candidate.parents} parents; only a squash merge is released')
    if list(candidate.changed_files) != [VERSION_FILE]:
        raise GateError(f'the commit must change only {VERSION_FILE}, but changes {list(candidate.changed_files)}')
    if set_version(candidate.parent, after) != candidate.current:
        raise GateError(f'the commit must change only the version in {VERSION_FILE} (P5)')
    if version <= parse(before, release_only=False):
        raise GateError(f'{after} must be greater than the previous version {before}')
    branch = f'release/v{after}'
    if not candidate.pulls:
        raise GateError(
            f'no pull request is associated with {candidate.sha} yet; '
            'if it is the merge of a version pull request, re-run the job')
    merged = [pull for pull in candidate.pulls if merged_from(pull, candidate, branch)]
    if len(merged) != 1:
        raise GateError(
            f'{candidate.sha} is not the squash merge of a pull request from {candidate.repository}:{branch} into main; '
            'restore the version with a pull request')
    if not whole_pull_is_the_version(merged[0], after):
        raise GateError(
            f'the pull request #{merged[0].get("number")} as a whole must change only the version in {VERSION_FILE} '
            f'to {after}; restore the version with a pull request')
    tag = f'v{after}'
    newest = latest_tag([name for name in candidate.tags if name != tag])
    if newest and version <= parse(newest[1:]):
        raise GateError(f'{after} must be greater than the latest tag {newest}')
    return tag


def check_candidate(mode, candidate, requested_tag=None):
    if candidate.parent is None:
        if mode == 'tag':
            print(f'no release: the parent of {candidate.sha} has no {VERSION_FILE}', file=sys.stderr)
            return None
        raise GateError(f'the parent of {candidate.sha} has no {VERSION_FILE}')
    if candidate.current is None:
        raise GateError(f'{candidate.sha} has no {VERSION_FILE}')
    after = locate_version(candidate.current).version
    if mode == 'tag' and parse(after) is None:
        print(f'no release: {after} is not a release version (X.Y.Z)', file=sys.stderr)
        return None
    before = locate_version(candidate.parent).version
    if mode == 'tag' and before == after:
        print(f'no release: version unchanged ({after})', file=sys.stderr)
        return None
    tag = f'v{after}'
    remote = candidate.tags.get(tag)
    if mode == 'tag' and remote == candidate.sha:
        # 打ったあとに新しいタグが増えても、同じコミットのジョブの再実行は何もしないで終わる
        print(f'no release: tag {tag} already points at {candidate.sha}', file=sys.stderr)
        return None
    approved_release(candidate, before, after)
    if mode == 'publish':
        if requested_tag != tag:
            raise GateError(f'tag {requested_tag} does not match the version {after}')
        if remote != candidate.sha:
            raise GateError(f'tag {tag} does not point at {candidate.sha} on the remote')
        return tag
    if remote is None:
        return tag
    raise GateError(f'tag {tag} already exists on another commit {remote}')


def decode(data, name):
    try:
        return data.decode('utf-8')
    except UnicodeDecodeError as error:
        raise GateError(f'{name} is not UTF-8: {error}') from error


def read_version_file(path):
    """版のファイルを読む。無ければ None（release-gate-inputs.sh は、無いファイルを書かない）。"""
    if not os.path.exists(path):
        return None
    with open(path, 'rb') as file:
        return decode(file.read(), path)


def check(args):
    def read(path):
        with open(path, encoding='utf-8') as file:
            return file.read()

    candidate = Candidate(
        repository=args.repository, sha=args.sha,
        parent=read_version_file(args.parent_version), current=read_version_file(args.version_file),
        changed_files=tuple(line for line in read(args.changed_files).splitlines() if line),
        pulls=tuple(json.loads(read(args.pulls))), tags=remote_tags(read(args.remote_tags)),
        on_main=args.on_main == 'true', parents=int(read(args.parents)),
    )
    return check_candidate(args.mode, candidate, args.tag)


def check_next(text, version):
    """次の版 version が、版のファイルの中身 text の版より大きいか。大きくなければ GateError。"""
    if parse(version) is None:
        raise GateError(f'{version!r} is not a release version (X.Y.Z)')
    current = locate_version(text).version
    if parse(version) <= parse(current, release_only=False):
        raise GateError(f'{version} must be greater than the version {current} in {VERSION_FILE} '
                        '(a version pull request may be merged but not tagged yet)')
    return version


def read_file(path):
    try:
        with open(path, 'rb') as file:
            return decode(file.read(), path)
    except OSError as error:
        raise GateError(f'could not read {path}: {error}') from error


def write_version(path, version):
    text = set_version(read_file(path), version)
    with open(path, 'wb') as file:
        file.write(text.encode('utf-8'))


def main(argv):
    parser = argparse.ArgumentParser(prog='release-gate.py')
    commands = parser.add_subparsers(dest='command', required=True)
    commands.add_parser('latest-tag')
    reader = commands.add_parser('read-version')
    reader.add_argument('--file', default=VERSION_FILE)
    setter = commands.add_parser('set-version')
    setter.add_argument('version')
    setter.add_argument('--file', default=VERSION_FILE)
    following = commands.add_parser('check-next')
    following.add_argument('version')
    following.add_argument('--file', default=VERSION_FILE)
    gate = commands.add_parser('check')
    gate.add_argument('--mode', choices=('tag', 'publish'), required=True)
    gate.add_argument('--tag')
    for name in ('repository', 'sha', 'parents', 'parent-version', 'version-file', 'changed-files', 'pulls',
                 'remote-tags'):
        gate.add_argument('--' + name, required=True)
    gate.add_argument('--on-main', choices=('true', 'false'), required=True)
    args = parser.parse_args(argv[1:])

    if args.command == 'latest-tag':
        tag = latest_tag([line.strip() for line in sys.stdin])
        if tag:
            print(tag)
        return 0
    try:
        if args.command == 'read-version':
            print(locate_version(read_file(args.file)).version)
            return 0
        if args.command == 'set-version':
            write_version(args.file, args.version)
            return 0
        if args.command == 'check-next':
            check_next(read_file(args.file), args.version)
            return 0
        tag = check(args)
    except GateError as error:
        print(f'::error::{error}', file=sys.stderr)
        return 1
    if tag:
        print(tag)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
