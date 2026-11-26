"""リリースの経路のテストが共有する読み込みと補助関数（設計の追記 design-r6.md）。"""
import hashlib
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

GITHUB = Path(__file__).resolve().parents[1]
ROOT = GITHUB.parent
WORKFLOWS = GITHUB / 'workflows'
BUMP = (WORKFLOWS / 'lib-bump-version.yml').read_text()
RELEASE = (WORKFLOWS / 'lib-release.yml').read_text()
REPOSITORY = 'o/r'
CRATE = 'event-store-adapter-rs'
VERSION_FILE = 'lib/Cargo.toml'
# 作業ツリーの lib/Cargo.toml（版上げの pull request のあとは 5.2.0 ではない）
ON_DISK_CARGO_TOML = (ROOT / VERSION_FILE).read_text()
FIXTURE_VERSION = '5.2.0'


def fixture_manifest(text, version=FIXTURE_VERSION):
    """lib/Cargo.toml の [package] の版（ファイルの最初の `version = "..."` の行）を version に替える。
    試験は実物の中身を使いつつ、作業ツリーの版に依存しない（#375 の CodeRabbit の指摘）。関門の locate_version は使わない。"""
    fixed, count = re.subn(r'(?m)^version = "[^"\n]*"$', f'version = "{version}"', text, count=1)
    if count != 1:
        raise AssertionError(f'no version line in {VERSION_FILE}')
    return fixed


# 実物の lib/Cargo.toml の版を 5.2.0 にしたもの
REAL_CARGO_TOML = fixture_manifest(ON_DISK_CARGO_TOML)
# 手元の HTTP のスタブ（127.0.0.1）を叩く子プロセスの no_proxy（環境のプロキシへ送らない）
LOOPBACK = '127.0.0.1,localhost'
# 試験の偽の秘密の値（本物ではない。作り物の印 Zq24Zq）
FAKE_PAT = 'fake-pat-Zq24Zq'
FAKE_CRATES_TOKEN = 'fake-crates-token-Zq24Zq'


def load_script(filename, name):
    spec = importlib.util.spec_from_file_location(name, GITHUB / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_gate():
    return load_script('release-gate.py', 'release_gate')


def load_crates_io():
    return load_script('crates-io.py', 'crates_io')


def job(workflow, name):
    start = workflow.index('\n  ' + name + ':\n')
    end = re.search(r'\n  [a-z-]+:\n', workflow[start + 1:])
    return workflow[start:start + 1 + end.start()] if end else workflow[start:]


def job_names(workflow):
    body = workflow.split('\njobs:\n', 1)[1]
    return re.findall(r'^  ([a-z-]+):$', body, re.MULTILINE)


def steps(workflow, job_name):
    """ジョブの手順を、書いた順に文字列で返す。"""
    body = job(workflow, job_name)
    body = body[body.index('\n    steps:\n'):]
    return ['\n      - ' + part for part in body.split('\n      - ')[1:]]


def outputs(path):
    return dict(line.split('=', 1) for line in path.read_text().splitlines() if line)


def pull(sha, ref='release/v5.2.1', repo=REPOSITORY, merged=True, base='main', number=1, size=(1, 1, 1)):
    """GET pulls/<番号> の応答の形。size は pull request の全体の (changed_files, additions, deletions)。"""
    changed_files, additions, deletions = size
    return {
        'number': number, 'merged_at': '2026-11-27T00:00:00Z' if merged else None,
        'merge_commit_sha': sha, 'base': {'ref': base}, 'head': {'ref': ref, 'repo': {'full_name': repo}},
        'changed_files': changed_files, 'additions': additions, 'deletions': deletions,
    }


def cargo_toml(version=FIXTURE_VERSION, extra=''):
    """実物の lib/Cargo.toml の版だけを替えたもの。"""
    return fixture_manifest(ON_DISK_CARGO_TOML, version) + extra


# ---------------------------------------------------------------- crates.io と sparse index のスタブ

class CratesStub:
    """crates.io の API（/api/v1/crates/...）と sparse index（/index/ev/en/<名前>）と、偽の cargo の公開（/_publish）。

    versions：{版: {'yanked', 'trustpub_data', 'checksum'}}。visible：index に出ている版。
    fail：{パスの先頭: 状態の番号 か 'drop'（本文の途中で切る）}。publish_visible：公開の直後に index に出すか。
    """

    def __init__(self):
        self.versions = {}
        self.visible = set()
        self.fail = {}
        self.publish_visible = True
        self.tamper_checksum = False
        self.published = []
        self.requests = []
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def reply(self, code, body, length=None):
                self.send_response(code)
                self.send_header('Content-Length', str(len(body) if length is None else length))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                stub.requests.append(('GET', self.path, dict(self.headers)))
                for prefix, failure in stub.fail.items():
                    if self.path.startswith(prefix):
                        if failure == 'drop':
                            return self.reply(200, b'{"version": {', length=1000)
                        return self.reply(failure, b'{}')
                code, body = stub.get(self.path)
                self.reply(code, body)

            def do_POST(self):
                length = int(self.headers.get('Content-Length', '0'))
                data = json.loads(self.rfile.read(length))
                stub.requests.append(('POST', self.path, dict(self.headers)))
                stub.publish(data)
                self.reply(200, b'{}')

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.url = f'http://127.0.0.1:{self.server.server_address[1]}'
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()

    def add(self, version, sha='a' * 40, repository=REPOSITORY, yanked=False, checksum='0' * 64, visible=True,
            trust=True):
        self.versions[version] = {
            'yanked': yanked, 'checksum': checksum,
            'trustpub_data': {'provider': 'github', 'repository': repository, 'run_id': '1', 'sha': sha} if trust
            else None,
        }
        if visible:
            self.visible.add(version)

    def publish(self, data):
        self.published.append(data)
        checksum = 'f' * 64 if self.tamper_checksum else data['checksum']
        self.add(data['version'], sha=data['sha'], repository=data['repository'], checksum=checksum,
                 visible=self.publish_visible)

    def get(self, path):
        gate = load_gate()
        if path.startswith('/api/v1/crates/'):
            rest = path[len('/api/v1/crates/'):]
            name, _, tail = rest.partition('/')
            if name != CRATE:
                return 404, b'{}'
            if tail.startswith('versions?'):
                ordered = sorted(self.versions, key=lambda v: gate.parse(v, release_only=False), reverse=True)
                return 200, json.dumps({'versions': [{'num': v} for v in ordered[:1]]}).encode()
            if tail in self.versions:
                return 200, json.dumps({'version': {'num': tail, **self.versions[tail]}}).encode()
            return 404, b'{"errors": [{"detail": "Not Found"}]}'
        if path == f'/index/ev/en/{CRATE}':
            lines = [json.dumps({'name': CRATE, 'vers': v, 'cksum': self.versions[v]['checksum']})
                     for v in self.versions if v in self.visible]
            return (200, ('\n'.join(lines) + '\n').encode()) if lines else (404, b'')
        return 404, b''


# ---------------------------------------------------------------- git の履歴

def git(cwd, *args, env=None):
    return subprocess.run(['git', *args], cwd=cwd, check=True, capture_output=True, text=True,
                          env={**os.environ, 'GIT_CONFIG_GLOBAL': os.devnull, 'GIT_CONFIG_SYSTEM': os.devnull,
                               'GIT_AUTHOR_NAME': 't', 'GIT_AUTHOR_EMAIL': 't@example.com',
                               'GIT_COMMITTER_NAME': 't', 'GIT_COMMITTER_EMAIL': 't@example.com',
                               'GIT_AUTHOR_DATE': '2026-11-27T00:00:00Z', 'GIT_COMMITTER_DATE': '2026-11-27T00:00:00Z',
                               **(env or {})}).stdout.strip()


class History:
    """origin（bare）と作業の clone。最初のコミットは .github・lib/Cargo.toml（5.2.0）・lib/src を持ち、v5.2.0 のタグ。"""

    def __init__(self, base):
        self.base = Path(base)
        self.origin = self.base / 'origin.git'
        self.work = self.base / 'work'
        git(self.base, 'init', '-q', '--bare', '-b', 'main', str(self.origin))
        git(self.base, 'init', '-q', '-b', 'main', str(self.work))
        shutil.copytree(GITHUB, self.work / '.github', ignore=shutil.ignore_patterns('__pycache__', '.hypothesis'))
        (self.work / 'lib' / 'src').mkdir(parents=True)
        (self.work / 'lib' / 'src' / 'lib.rs').write_text('pub fn f() {}\n')
        (self.work / VERSION_FILE).write_text(cargo_toml('5.2.0'))
        (self.work / '.gitignore').write_text('Cargo.lock\ntarget/\n')
        git(self.work, 'add', '-A')
        git(self.work, 'commit', '-q', '-m', 'chore: initial')
        git(self.work, 'tag', '-a', 'v5.2.0', '-m', 'v5.2.0')
        git(self.work, 'remote', 'add', 'origin', str(self.origin))
        git(self.work, 'push', '-q', 'origin', 'main', 'refs/tags/v5.2.0')

    def head(self):
        return git(self.work, 'rev-parse', 'HEAD')

    def commit(self, message, version=None, files=None):
        if version is not None:
            (self.work / VERSION_FILE).write_text(cargo_toml(version))
        for name, text in (files or {}).items():
            (self.work / name).parent.mkdir(parents=True, exist_ok=True)
            (self.work / name).write_text(text)
        git(self.work, 'add', '-A')
        git(self.work, 'commit', '-q', '--allow-empty', '-m', message)
        git(self.work, 'push', '-q', 'origin', 'main')
        return self.head()

    def tag(self, name, sha=None):
        git(self.work, 'tag', '-a', name, sha or self.head(), '-m', name)
        git(self.work, 'push', '-q', 'origin', f'refs/tags/{name}')

    def checkout(self, ref):
        git(self.work, 'checkout', '-q', '--detach', ref)


# ---------------------------------------------------------------- 偽の道具（PATH の先頭に置く）

FAKE_CARGO = r'''#!/usr/bin/env python3
"""偽の cargo。呼び出しを FAKE_LOG に書く。package は lib/ と Cargo.lock から決まった .crate を作る。

実物の cargo 1.100.0 と同じく、package はワークスペースの Cargo.lock を書かない（.crate の中にだけ作る）。
ワークスペースの Cargo.lock は generate-lockfile が作る。--locked で Cargo.lock が無ければ失敗する。
検証つきの package（--no-verify なし）は「ビルドスクリプト」を動かす：FAKE_BUILD_SCRIPT_WRITES のパスに書く。
"""
import hashlib, json, os, sys, tomllib, urllib.request
from pathlib import Path

args = sys.argv[1:]
with open(os.environ['FAKE_LOG'], 'a') as log:
    log.write(json.dumps({'tool': 'cargo', 'args': args,
                          'token': os.environ.get('CARGO_REGISTRY_TOKEN', ''),
                          'oidc': os.environ.get('ACTIONS_ID_TOKEN_REQUEST_TOKEN', '')}) + '\n')
root = Path.cwd()
version = tomllib.loads((root / 'lib/Cargo.toml').read_text())['package']['version']
if args[:1] == ['--version']:
    print(os.environ.get('FAKE_CARGO_VERSION', 'cargo 1.100.0 (fake)'))
elif args[:1] == ['metadata']:
    print(json.dumps({'packages': [{'name': 'event-store-adapter-rs', 'version': version}]}))
elif args[:1] == ['generate-lockfile']:
    (root / 'Cargo.lock').write_text(os.environ.get('FAKE_LOCK', '# lock 1\n'))
elif args[:1] in (['package'], ['publish']):
    lock = root / 'Cargo.lock'
    if lock.exists():
        lock_bytes = lock.read_bytes()
    elif '--locked' in args:
        sys.exit('error: the lock file needs to be updated but --locked was passed to prevent this')
    else:
        lock_bytes = os.environ.get('FAKE_LOCK', '# lock 1\n').encode()
    digest = hashlib.sha256()
    for path in sorted((root / 'lib').rglob('*')):
        if path.is_file():
            digest.update(str(path.relative_to(root)).encode() + b'\0' + path.read_bytes())
    digest.update(lock_bytes)
    data = b'crate\0' + digest.digest()
    out = root / 'target/package' / f'event-store-adapter-rs-{version}.crate'
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(data)
    if '--no-verify' not in args:
        for target in filter(None, os.environ.get('FAKE_BUILD_SCRIPT_WRITES', '').split(os.pathsep)):
            Path(target).write_text('# written by a build script\n')
    if args[0] == 'publish':
        if not os.environ.get('CARGO_REGISTRY_TOKEN'):
            sys.exit('error: no token')
        if os.environ.get('FAKE_PUBLISH_FAILS'):
            sys.exit('error: the registry failed')
        body = json.dumps({'version': version, 'checksum': hashlib.sha256(data).hexdigest(),
                           'sha': os.environ['FAKE_TAG_SHA'], 'repository': os.environ['GITHUB_REPOSITORY']}).encode()
        request = urllib.request.Request(os.environ['FAKE_PUBLISH_URL'], data=body, method='POST')
        urllib.request.urlopen(request).read()
        if os.environ.get('FAKE_PUBLISH_FAILS_AFTER'):
            sys.exit('error: the connection closed after the upload')
else:
    sys.exit(f'fake cargo: not modelled: {args}')
'''

FAKE_RUSTUP = r'''#!/usr/bin/env python3
import json, os, sys
with open(os.environ['FAKE_LOG'], 'a') as log:
    log.write(json.dumps({'tool': 'rustup', 'args': sys.argv[1:]}) + '\n')
'''

FAKE_GH = r'''#!/usr/bin/env python3
"""偽の gh。commits/<sha>/pulls は FAKE_PULLS のファイル、pulls/<番号> はその中の同じ番号の最初の項目、
releases は FAKE_RELEASES のファイル（1行に1つのタグ）。"""
import json, os, re, sys
args = sys.argv[1:]
with open(os.environ['FAKE_LOG'], 'a') as log:
    log.write(json.dumps({'tool': 'gh', 'args': args}) + '\n')
if args[:1] == ['api'] and args[-1].endswith('/pulls'):
    print(open(os.environ['FAKE_PULLS']).read())
elif args[:1] == ['api'] and re.search(r'/pulls/[0-9]+$', args[-1]):
    number = int(args[-1].rsplit('/', 1)[1])
    print(json.dumps(next(p for p in json.load(open(os.environ['FAKE_PULLS'])) if p['number'] == number)))
elif args[:1] == ['api'] and any(a.endswith('/releases') for a in args):
    if os.environ.get('FAKE_RELEASES_FAIL'):
        sys.exit('HTTP 502')
    print(open(os.environ['FAKE_RELEASES']).read(), end='')
elif args[:2] == ['release', 'create']:
    pass
elif args[:2] == ['pr', 'create']:
    print('https://github.com/o/r/pull/7')
elif args[:2] == ['workflow', 'run']:
    pass
else:
    sys.exit(f'fake gh: not modelled: {args}')
'''

FAKE_SHA256SUM = r'''#!/usr/bin/env python3
import hashlib, sys
for name in sys.argv[1:]:
    print(f'{hashlib.sha256(open(name, "rb").read()).hexdigest()}  {name}')
'''


def fake_bin(directory):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    for name, text in (('cargo', FAKE_CARGO), ('rustup', FAKE_RUSTUP), ('gh', FAKE_GH),
                       ('sha256sum', FAKE_SHA256SUM)):
        path = directory / name
        path.write_text(text.replace('#!/usr/bin/env python3', f'#!{sys.executable}', 1))
        path.chmod(0o755)
    return directory


def read_log(path):
    if not Path(path).exists():
        return []
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line]


def sha256(data):
    return hashlib.sha256(data).hexdigest()
