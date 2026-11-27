#!/usr/bin/env python3
"""crates.io への公開の、事前の確認・事後の確認・検証した .crate の照合（設計の追記 design-r6.md §3・§4）。

標準ライブラリだけを使う（publish ジョブで pip install をしない。形のテストが確かめる）。秘密の値は使わない。

precheck --version X.Y.Z --sha <タグのコミット> [--remote origin] [--deadline SECONDS]
    crates.io の API とリモートのタグを読んで決める。出力（$GITHUB_OUTPUT）：action=publish|published。
    publish  ：その版が crates.io に無く、crates.io の最大の版より大きく、最大の正式版のタグ。
    published：その版が crates.io にあり、取り下げでなく、trusted publishing で同じリポジトリのタグのコミットから出ている。
    ほかは止まる（終了コード 1）。照会の失敗（接続、時間切れ、本文の途中の切断、404 でない応答、壊れた JSON）は
    「無い」と見ずに止まる。
confirm --version X.Y.Z [--sha256 HEX] [--interval SECONDS] [--deadline SECONDS]
    sparse index（index.crates.io）がその版の行を返すまで待つ。--sha256 があれば、index の cksum と
    API の checksum がその値と同じことも確かめる（違えばすぐに失敗）。締め切りまでに返らなければ時間切れで失敗。
check-crate --crate <ファイル> --sha256 HEX
    .crate の SHA-256 が HEX と同じか。
copy-lock --from <取り寄せたディレクトリ> --to <ファイル> --sha256 HEX
    取り寄せたディレクトリの Cargo.lock だけを、通常のファイル（シンボリックリンクでない）で、SHA-256 が HEX
    （package ジョブが、第三者のビルドコードが動く前に記録した値）と同じことを確かめて写す。
    ほかのファイルは使わない（package ジョブの成果物は信用しない入力。設計の再レビュー N1、#375 の Devin の指摘）。

precheck と confirm は --deadline 秒の締め切りを単調時計で持ち、照会・待ち・git ls-remote の時間をすべて数える。
環境変数：GITHUB_REPOSITORY、CRATES_IO_API_URL・CRATES_INDEX_URL（テストで差し替える）、GITHUB_OUTPUT。
"""
import argparse
import hashlib
import http.client as http_client  # 関数 http と名前がぶつからないように
import json
import os
import re
import socket
import stat
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path

CRATE = 'event-store-adapter-rs'
USER_AGENT = 'event-store-adapter-rs-release (+https://github.com/j5ik2o/event-store-adapter-rs)'
BODY_CHUNK = 64 * 1024
RELEASE = re.compile(r'(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)')
SEMVER = re.compile(
    r'(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)'
    r'(?:-((?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*))?'
    r'(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?')
SHA256 = re.compile(r'[0-9a-f]{64}')


class Stop(Exception):
    """止まる（公開しない、または確認が通らない）。理由を ::error:: で書いて終了コード 1。"""


# ---------------------------------------------------------------- 版

def version_key(version):
    """semver の優先順位のキー（build metadata は無視）。版でなければ None。"""
    match = SEMVER.fullmatch(version or '')
    if not match:
        return None
    core = tuple(int(part) for part in match.groups()[:3])
    if match.group(4) is None:
        return core + (1, ())
    return core + (0, tuple((0, int(part)) if part.isdigit() else (1, part) for part in match.group(4).split('.')))


def latest_release_tag(ls_remote):
    """`git ls-remote --tags` の出力から、最大の正式版のタグ（vX.Y.Z）。無ければ None。"""
    best = None
    for line in ls_remote.splitlines():
        if '\t' not in line:
            continue
        name = line.split('\t', 1)[1].removeprefix('refs/tags/').removesuffix('^{}')
        if name.startswith('v') and RELEASE.fullmatch(name[1:]):
            key = version_key(name[1:])
            if best is None or key > best[0]:
                best = (key, name)
    return best[1] if best else None


# ---------------------------------------------------------------- 判定（純粋な関数。照会と分ける）

@dataclass(frozen=True)
class Response:
    """照会の結果。code が None なら照会の失敗（接続、時間切れ、本文の途中の切断）。"""
    code: object
    body: bytes = b''


def parse_json(response, what):
    try:
        return json.loads(response.body.decode('utf-8'))
    except (UnicodeDecodeError, ValueError) as error:
        raise Stop(f'could not read {what}: the body is not JSON ({error})') from error


def read_version(response):
    """`GET /api/v1/crates/<名前>/<版>` の結果。404 なら None、200 なら version の辞書、ほかは Stop。"""
    if response.code == 404:
        return None
    if response.code != 200:
        raise Stop(f'could not read the version from crates.io (status {response.code}); re-run the job')
    body = parse_json(response, 'the version from crates.io')
    version = body.get('version') if isinstance(body, dict) else None
    if not isinstance(version, dict) or not isinstance(version.get('num'), str):
        raise Stop('could not read the version from crates.io: the body has no version')
    return version


def read_max_version(response):
    """`GET /api/v1/crates/<名前>/versions?per_page=1&sort=semver` の先頭の版（取り下げを含む）。版が無ければ None。"""
    if response.code != 200:
        raise Stop(f'could not read the versions from crates.io (status {response.code}); re-run the job')
    body = parse_json(response, 'the versions from crates.io')
    versions = body.get('versions') if isinstance(body, dict) else None
    if not isinstance(versions, list):
        raise Stop('could not read the versions from crates.io: the body has no versions')
    if not versions:
        return None
    number = versions[0].get('num') if isinstance(versions[0], dict) else None
    if version_key(number) is None:
        raise Stop(f'could not read the versions from crates.io: {number!r} is not a version')
    return number


def decide_precheck(version, sha, repository, existing, max_version, latest_tag):
    """precheck の判定（P19）。existing は read_version の結果、max_version は read_max_version の結果。"""
    if not RELEASE.fullmatch(version):
        raise Stop(f'{version!r} is not a release version (X.Y.Z)')
    if existing is not None:
        if existing.get('num') != version:
            raise Stop(f'crates.io returned the version {existing.get("num")!r} for {version}')
        if existing.get('yanked') is not False:
            raise Stop(f'{CRATE} {version} is on crates.io but yanked; a person decides what to do')
        trust = existing.get('trustpub_data')
        if not (isinstance(trust, dict) and trust.get('provider') == 'github'
                and trust.get('repository') == repository and trust.get('sha') == sha):
            raise Stop(f'{CRATE} {version} is on crates.io, but not published by trusted publishing from '
                       f'{repository} at {sha} (trustpub_data: {trust!r}); a person decides what to do')
        return 'published'
    if max_version is not None and version_key(max_version) >= version_key(version):
        raise Stop(f'crates.io already has {CRATE} {max_version}, which is not lower than {version}')
    if latest_tag != f'v{version}':
        raise Stop(f'v{version} is not the latest release tag (the latest is {latest_tag})')
    return 'publish'


def index_versions(response):
    """sparse index の応答を {版: cksum} にする。200 でなければ None（まだ無い、または照会の失敗。待って読み直す）。"""
    if response.code != 200:
        return None
    found = {}
    for line in response.body.decode('utf-8', errors='replace').splitlines():
        try:
            entry = json.loads(line)
        except ValueError:
            continue
        if isinstance(entry, dict) and isinstance(entry.get('vers'), str):
            found[entry['vers']] = entry.get('cksum')
    return found


def check_checksums(version, sha256, index_cksum, api_checksum):
    """--sha256 の照合（P20、I19）。違えば Stop。"""
    if index_cksum != sha256:
        raise Stop(f'the sparse index has the checksum {index_cksum!r} for {version}, '
                   f'but the verified crate has {sha256}; do not create the GitHub release')
    if api_checksum != sha256:
        raise Stop(f'crates.io has the checksum {api_checksum!r} for {version}, '
                   f'but the verified crate has {sha256}; do not create the GitHub release')


# ---------------------------------------------------------------- 照会

class Deadline:
    """コマンド全体の締め切り（単調時計）。照会の時間も待ちに数える。"""

    def __init__(self, seconds, clock=time.monotonic):
        self.clock = clock
        self.end = clock() + seconds

    def remaining(self):
        return max(0.0, self.end - self.clock())


def response_socket(response):
    """応答の socket。CPython の http.client では、HTTPResponse.fp は socket.makefile('rb') の BufferedReader で、
    その raw（SocketIO）が socket を持つ。HTTPError は fp に HTTPResponse を持つ。取れなければ None。"""
    fp = getattr(response, 'fp', None)
    raw = getattr(fp, 'raw', None)
    sock = getattr(raw, '_sock', None)
    return sock if isinstance(sock, socket.socket) else None


def read_body(response, deadline, timeout):
    """本文を読み、締め切りで打ち切る（過ぎたら TimeoutError）。

    urlopen の timeout は通信の1回ごとにかかり、read(n) は n バイトそろうまで何度も受信するので、本文が少しずつ届くと
    締め切りを超えて待つ（#375 の Devin と CodeRabbit の指摘）。そこで、
    - 受信の前ごとに socket の timeout を締め切りの残りにし、read1（受信は1回）で読み、読むたびに締め切りを確かめる。
    - chunked の長さの行のように1回の read1 の中で何度も受信する場合に備え、締め切りに socket を閉じる（shutdown）。
      閉じたあとの受信は失敗し、照会の失敗になる。"""
    sock = response_socket(response)
    read = getattr(response, 'read1', None) or response.read
    watchdog = Watchdog(sock, deadline.remaining()) if sock is not None else None
    try:
        chunks = []
        while True:
            left = deadline.remaining()
            if left <= 0:
                raise TimeoutError('the deadline passed while reading the body')
            if sock is not None and sock.fileno() >= 0:   # 本文を読み終えると http.client が socket を閉じる
                sock.settimeout(min(timeout, left))
            chunk = read(BODY_CHUNK)
            if not chunk:
                break
            chunks.append(chunk)
        if deadline.remaining() <= 0:
            raise TimeoutError('the deadline passed while reading the body')
        return b''.join(chunks)
    finally:
        if watchdog is not None:
            watchdog.cancel()
            watchdog.raise_failure()


class Watchdog:
    """締め切りに socket を閉じ、受信を止める timer。閉じられなかったときは、その照会を時間切れとして失敗にする
    （raise_failure）。読み終えて閉じた socket には何もしない。"""

    def __init__(self, sock, seconds):
        self.sock = sock
        self.failure = None
        self.timer = threading.Timer(max(seconds, 0), self.fire)
        self.timer.daemon = True
        self.timer.start()

    def fire(self):
        if self.sock.fileno() < 0:
            return
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError as error:
            self.failure = error

    def cancel(self):
        self.timer.cancel()

    def raise_failure(self):
        if self.failure is not None:
            raise TimeoutError(f'could not shut the socket down at the deadline: {self.failure}') from self.failure


def http(url, deadline, timeout=60, headers=None):
    """GET して Response を返す。接続の失敗、時間切れ、本文の途中の切断（IncompleteRead）は code=None。
    締め切りの残りで照会を打ち切り、残りが無ければ照会しない。締め切りのあとに読み終えた応答は使わない。
    接続と見出し（headers）の受信は、1回の通信ごとに締め切りの残りまで（本文は read_body が締め切りで切る）。"""
    left = deadline.remaining()
    if left <= 0:
        return Response(None)
    request = urllib.request.Request(url, headers={'User-Agent': USER_AGENT, **(headers or {})})
    try:
        try:
            with urllib.request.urlopen(request, timeout=min(timeout, left)) as response:
                return Response(response.status, read_body(response, deadline, timeout))
        except urllib.error.HTTPError as error:
            with error:
                return Response(error.code, read_body(getattr(error, 'fp', None) or error, deadline, timeout))
    except (urllib.error.URLError, http_client.HTTPException, TimeoutError, OSError, ValueError):
        return Response(None)


def api_url(path):
    return os.environ.get('CRATES_IO_API_URL', 'https://crates.io').rstrip('/') + path


def index_url():
    base = os.environ.get('CRATES_INDEX_URL', 'https://index.crates.io').rstrip('/')
    return f'{base}/{CRATE[0:2]}/{CRATE[2:4]}/{CRATE}'


def ls_remote_tags(remote, deadline):
    left = deadline.remaining()
    if left <= 0:
        raise Stop('the deadline passed before reading the remote tags')
    try:
        process = subprocess.run(['git', 'ls-remote', '--tags', remote], capture_output=True, text=True,
                                 timeout=left, check=False)
    except subprocess.TimeoutExpired as error:
        raise Stop('reading the remote tags did not finish before the deadline') from error
    if process.returncode != 0:
        raise Stop(f'could not read the remote tags: {process.stderr.strip()}')
    return process.stdout


def write_output(**values):
    path = os.environ.get('GITHUB_OUTPUT')
    if path:
        with open(path, 'a', encoding='utf-8') as file:
            for key, value in values.items():
                file.write(f'{key}={value}\n')


def precheck(args, deadline, git=ls_remote_tags):
    repository = os.environ['GITHUB_REPOSITORY']
    existing = read_version(http(api_url(f'/api/v1/crates/{CRATE}/{args.version}'), deadline))
    max_version = latest = None
    if existing is None:
        max_version = read_max_version(http(api_url(f'/api/v1/crates/{CRATE}/versions?per_page=1&sort=semver'),
                                            deadline))
        latest = latest_release_tag(git(args.remote, deadline))
    action = decide_precheck(args.version, args.sha, repository, existing, max_version, latest)
    print(f'{CRATE} {args.version}: {action}')
    write_output(action=action)
    return 0


def confirm(args, deadline, sleep=time.sleep):
    if not args.interval > 0:
        raise Stop(f'--interval must be greater than 0, but is {args.interval}')
    if args.sha256 is not None and not SHA256.fullmatch(args.sha256):
        raise Stop(f'--sha256 must be 64 lowercase hexadecimal digits, but is {args.sha256!r}')
    attempt = 0
    while True:
        attempt += 1
        versions = index_versions(http(index_url(), deadline, headers={'Cache-Control': 'no-cache'}))
        if versions is not None and args.version in versions:
            if args.sha256 is None:
                print(f'the sparse index serves {CRATE} {args.version} (attempt {attempt})')
                return 0
            api = http(api_url(f'/api/v1/crates/{CRATE}/{args.version}'), deadline)
            if api.code == 200:
                check_checksums(args.version, args.sha256, versions[args.version],
                                read_version(api).get('checksum'))
                print(f'the sparse index and crates.io serve {CRATE} {args.version} with the verified checksum '
                      f'(attempt {attempt})')
                return 0
            # API の一時的な失敗は、締め切りまで読み直す（照合しないまま通さない）
            print(f'could not read {CRATE} {args.version} from the crates.io API (status {api.code}; attempt {attempt})')
        else:
            print(f'the sparse index does not serve {CRATE} {args.version} yet (attempt {attempt})')
        if deadline.remaining() <= 0:
            raise Stop(f'the sparse index did not serve {CRATE} {args.version} before the deadline; '
                       're-run only the confirm job (do not publish again)')
        sleep(min(args.interval, deadline.remaining()))


def check_crate(args):
    digest = hashlib.sha256(Path(args.crate).read_bytes()).hexdigest()
    if digest != args.sha256:
        raise Stop(f'{args.crate} has the SHA-256 {digest}, but the verified crate has {args.sha256}')
    print(f'{args.crate} matches the verified crate ({digest})')
    return 0


def copy_lock(args):
    source = os.path.join(args.source, 'Cargo.lock')
    for path in (args.source, source):
        if os.path.islink(path):
            raise Stop(f'{path} is a symbolic link; use only a regular Cargo.lock from the artifact')
    try:
        descriptor = os.open(source, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as error:
        raise Stop(f'the artifact has no regular Cargo.lock: {error}') from error
    if not stat.S_ISREG(os.fstat(descriptor).st_mode):
        os.close(descriptor)
        raise Stop(f'{source} is not a regular file')
    with os.fdopen(descriptor, 'rb') as file:
        data = file.read()
    digest = hashlib.sha256(data).hexdigest()
    if digest != args.sha256:
        raise Stop(f'{source} has the SHA-256 {digest}, but the Cargo.lock recorded before the build has {args.sha256}')
    if os.path.lexists(args.to):
        if os.path.islink(args.to) or not os.path.isfile(args.to):
            raise Stop(f'{args.to} exists and is not a regular file')
        os.remove(args.to)
    with open(args.to, 'xb') as file:
        file.write(data)
    print(f'copied Cargo.lock ({digest}) to {args.to}')
    return 0


def main(argv, sleep=time.sleep, clock=time.monotonic, git=ls_remote_tags):
    parser = argparse.ArgumentParser(prog='crates-io.py')
    commands = parser.add_subparsers(dest='command', required=True)
    pre = commands.add_parser('precheck')
    pre.add_argument('--version', required=True)
    pre.add_argument('--sha', required=True)
    pre.add_argument('--remote', default='origin')
    pre.add_argument('--deadline', type=float, default=600)
    con = commands.add_parser('confirm')
    con.add_argument('--version', required=True)
    con.add_argument('--sha256')
    con.add_argument('--interval', type=float, default=20)
    con.add_argument('--deadline', type=float, default=1800)
    crate = commands.add_parser('check-crate')
    crate.add_argument('--crate', required=True)
    crate.add_argument('--sha256', required=True)
    lock = commands.add_parser('copy-lock')
    lock.add_argument('--from', dest='source', required=True)
    lock.add_argument('--to', required=True)
    lock.add_argument('--sha256', required=True)
    args = parser.parse_args(argv[1:])
    try:
        if args.command == 'precheck':
            return precheck(args, Deadline(args.deadline, clock), git)
        if args.command == 'confirm':
            return confirm(args, Deadline(args.deadline, clock), sleep)
        if args.command == 'check-crate':
            return check_crate(args)
        return copy_lock(args)
    except Stop as error:
        print(f'::error::{error}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main(sys.argv))
