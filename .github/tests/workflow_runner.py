"""ワークフローの1つのジョブの手順を、書いた順に手元で流す小さな実行器。

event-store-adapter-scala の .github/tests/workflow_runner.py（48e4256）を写し、rs の lib-release.yml・
lib-bump-version.yml の形（ジョブの needs の出力、成果物の受け渡し、crates-io-auth-action）に広げたもの。

写す GitHub Actions の動き：
- 手順の if:：式を読んで評価する。状態の関数（always()・cancelled()）が無ければ success() && を付ける。
  読める形は、文字列・数・参照（steps・needs・inputs・env・github）、==・!=・>・<・&&・||・!、括弧だけ。
  知らない形は UnknownExpression。文字列の == と != は、大文字と小文字を区別しない。
- env: と with: の式：上の参照と secrets.<名前>、runner.temp。
- shell は bash -e {0}（pipefail は無い）。GITHUB_OUTPUT は手順ごとの新しいファイル。
- uses:：checkout と setup-python は飛ばす（作業の clone を前もって作る）。upload-artifact は成果物を名前で保存し、
  同じ名前があれば失敗する（overwrite は写さない）。download-artifact は name: の成果物を path に取り出し、
  無ければ失敗する（pattern: は写さない）。crates-io-auth-action は出力 token に偽の値を置き、使ったことを記録する。
  ほかの uses: は UnknownExpression。
- permissions に id-token: write があるジョブだけ、手順に ACTIONS_ID_TOKEN_REQUEST_* を渡す（OIDC の範囲の確かめ）。
"""
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

from release_test_support import FAKE_CRATES_TOKEN, LOOPBACK, job, outputs, steps

SKIPPED_ACTIONS = ('actions/checkout@', 'actions/setup-python@')
EXPRESSION = re.compile(r'\$\{\{ ([^}]*) \}\}')
TOKEN = re.compile(r"\s*(?:(?P<str>'(?:[^']|'')*')|(?P<num>[0-9]+)|(?P<op>==|!=|&&|\|\||>=|<=|>|<|!|\(|\))"
                   r"|(?P<ref>[A-Za-z_][A-Za-z0-9_.-]*(?:\(\))?))")
OIDC = {'ACTIONS_ID_TOKEN_REQUEST_URL': 'http://127.0.0.1:9/oidc', 'ACTIONS_ID_TOKEN_REQUEST_TOKEN': 'fake-oidc-Zq24Zq'}


class UnknownExpression(AssertionError):
    """実行器が写していない式・形。テストを失敗させる。"""


@dataclass
class StepResult:
    id: str
    name: str
    outcome: str              # success、failure、skipped
    stdout: str = ''
    stderr: str = ''
    secrets: frozenset = frozenset()
    oidc: bool = False


@dataclass
class JobResult:
    steps: list
    outputs: dict
    auth_used: bool = False

    def outcome(self, step_id):
        return next(step.outcome for step in self.steps if step.id == step_id)

    def step(self, step_id):
        return next(step for step in self.steps if step.id == step_id)

    def ran(self):
        return [step.id or step.name for step in self.steps if step.outcome != 'skipped']

    @property
    def succeeded(self):
        return all(step.outcome != 'failure' for step in self.steps)


def parse_step(text):
    """手順の文字列から、名前、id、if:、uses:、env、with、run の本文を読む。"""
    def scalar(key):
        match = re.search(r'\n(?:      - |        )' + key + r': ([^\n]*)', text)
        return match.group(1) if match else None

    def mapping(key):
        if f'\n        {key}:\n' not in text:
            return {}
        lines = []
        for line in text.split(f'\n        {key}:\n', 1)[1].split('\n'):
            if not line.startswith('          '):
                break
            lines.append(line)
        return dict(re.findall(r'^          ([A-Za-z0-9_-]+): ([^\n]*)$', '\n'.join(lines), re.MULTILINE))
    run = re.search(r'\n        run: \|\n((?:          [^\n]*\n?|\n)+)', text + '\n')
    single = scalar('run')
    body = ('\n'.join(line[10:] for line in run.group(1).rstrip('\n').split('\n')) if run
            else (single if single and single != '|' else None))
    return {'name': scalar('name'), 'id': scalar('id'), 'if': scalar('if'), 'uses': scalar('uses'),
            'env': mapping('env'), 'with': mapping('with'), 'run': body, 'text': text}


def job_steps(workflow, job_name):
    return [parse_step(text) for text in steps(workflow, job_name)]


def workflow_env(workflow):
    head = workflow.split('\njobs:\n', 1)[0]
    if '\nenv:\n' not in head:
        return {}
    block = head.split('\nenv:\n', 1)[1]
    return {key: value.strip("'") for key, value in re.findall(r'^  ([A-Z_]+): ([^\n]*)$', block, re.MULTILINE)}


class Evaluator:
    def __init__(self, lookup):
        self.lookup = lookup

    def tokens(self, text):
        position, result = 0, []
        while position < len(text):
            if text[position:].strip() == '':
                break
            match = TOKEN.match(text, position)
            if not match or match.end() == position:
                raise UnknownExpression(f'expression not modelled: {text}')
            kind = match.lastgroup
            result.append((kind, match.group(kind)))
            position = match.end()
        return result

    def evaluate(self, text):
        self.items, self.index = self.tokens(text), 0
        value = self.disjunction()
        if self.index != len(self.items):
            raise UnknownExpression(f'expression not modelled: {text}')
        return value

    def peek(self):
        return self.items[self.index][1] if self.index < len(self.items) else None

    def take(self):
        self.index += 1
        return self.items[self.index - 1]

    def disjunction(self):
        value = self.conjunction()
        while self.peek() == '||':
            self.take()
            right = self.conjunction()
            value = value if truthy(value) else right
        return value

    def conjunction(self):
        value = self.comparison()
        while self.peek() == '&&':
            self.take()
            right = self.comparison()
            value = right if truthy(value) else value
        return value

    def comparison(self):
        value = self.unary()
        if self.peek() in ('==', '!=', '>', '<', '>=', '<='):
            operator = self.take()[1]
            right = self.unary()
            return compare(value, operator, right)
        return value

    def unary(self):
        if self.peek() == '!':
            self.take()
            return not truthy(self.unary())
        if self.peek() == '(':
            self.take()
            value = self.disjunction()
            if self.take()[1] != ')':
                raise UnknownExpression('unbalanced parentheses')
            return value
        kind, text = self.take()
        if kind == 'str':
            return text[1:-1].replace("''", "'")
        if kind == 'num':
            return int(text)
        if kind == 'ref':
            return self.lookup(text)
        raise UnknownExpression(f'expression not modelled: {text}')


def truthy(value):
    return value not in ('', 0, False, None)


def number(value):
    """GitHub Actions の式の数値への変換（null は 0、true は 1、false は 0、空の文字列は 0、数でない文字列は NaN）。"""
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return value
    if value.strip() == '':
        return 0
    try:
        return float(value)
    except ValueError:
        return float('nan')


def compare(left, operator, right):
    if operator in ('==', '!='):
        if isinstance(left, str) and isinstance(right, str):
            equal = left.casefold() == right.casefold()
        elif type(left) is type(right) and not isinstance(left, (int, float)):
            equal = left == right
        else:
            # 型が違えば、両辺を数に変えて比べる（NaN はどれとも等しくない。#375 の CodeRabbit の指摘）
            equal = number(left) == number(right)
        return equal == (operator == '==')
    left, right = number(left), number(right)
    return {'>': left > right, '<': left < right, '>=': left >= right, '<=': left <= right}[operator]


class Runner:
    def __init__(self, workflow, workspace, temp, context, environment, secrets=None, inputs=None, needs=None,
                 artifacts=None, step_env=None):
        self.workflow = workflow
        self.workspace = Path(workspace)
        self.temp = Path(temp)
        self.context = context            # github.* の値（token、ref_name、run_attempt、sha、server_url など）
        self.environment = environment    # ランナーが渡す環境変数（PATH、GITHUB_* など）
        self.secrets = secrets or {}
        self.inputs = inputs or {}
        self.needs = needs or {}          # {ジョブ名: {'result': ..., 'outputs': {...}}}
        self.artifacts = artifacts if artifacts is not None else {}   # {名前: 保存先のディレクトリ}
        self.step_env = step_env or {}    # 試験のための、手順ごとの環境変数の差し替え
        self.step_outputs = {}
        self.env = {}

    def reference(self, name, failed):
        if name in ('always()', 'cancelled()', 'success()', 'failure()'):
            return {'always()': True, 'cancelled()': False, 'success()': not failed, 'failure()': failed}[name]
        parts = name.split('.')
        if parts[0] == 'steps' and len(parts) == 4 and parts[2] == 'outputs':
            return self.step_outputs.get(parts[1], {}).get(parts[3], '')
        if parts[0] == 'needs' and len(parts) == 3 and parts[2] == 'result':
            return self.needs.get(parts[1], {}).get('result', '')
        if parts[0] == 'needs' and len(parts) == 4 and parts[2] == 'outputs':
            return self.needs.get(parts[1], {}).get('outputs', {}).get(parts[3], '')
        if parts[0] == 'inputs' and len(parts) == 2:
            return self.inputs.get(parts[1], '')
        if parts[0] == 'env' and len(parts) == 2:
            return self.env.get(parts[1], '')
        if parts[0] == 'github' and len(parts) == 2 and parts[1] in self.context:
            return self.context[parts[1]]
        raise UnknownExpression(f'reference not modelled: {name}')

    def expand(self, value, used):
        def one(match):
            expression = match.group(1)
            if expression == 'runner.temp':
                return str(self.temp)
            if expression.startswith('secrets.'):
                used.add(expression[8:])
                return self.secrets[expression[8:]]
            result = Evaluator(lambda name: self.reference(name, False)).evaluate(expression)
            return 'true' if result is True else 'false' if result is False else str(result)
        if '${{' in EXPRESSION.sub('', value):
            raise UnknownExpression(f'expression not modelled: {value}')
        return EXPRESSION.sub(one, value)

    def condition(self, expression, failed):
        if expression is None:
            return not failed
        expression = expression.strip()
        if expression.startswith('${{') and expression.endswith('}}'):
            expression = expression[3:-2].strip()
        status = any(name in expression for name in ('always()', 'cancelled()', 'success()', 'failure()'))
        value = truthy(Evaluator(lambda name: self.reference(name, failed)).evaluate(expression))
        return value if status else (not failed and value)

    def uses(self, item, used, oidc):
        action = item['uses']
        settings = {key: self.expand(value, used) for key, value in item['with'].items()}
        if action.startswith(SKIPPED_ACTIONS):
            return None, {}
        if action.startswith('actions/upload-artifact@'):
            if 'overwrite' in settings:
                raise UnknownExpression('upload-artifact overwrite: is not modelled')
            path = Path(settings['path'])
            if not path.exists() or (path.is_dir() and not any(path.iterdir())):
                return (f'no file at {path}' if settings.get('if-no-files-found') == 'error' else None), {}
            if settings['name'] in self.artifacts:
                return f'an artifact named {settings["name"]} already exists (409)', {}
            stored = self.temp.parent / 'artifacts' / settings['name']
            if path.is_dir():
                shutil.copytree(path, stored, symlinks=True)
            else:
                stored.mkdir(parents=True)
                shutil.copy(path, stored)
            self.artifacts[settings['name']] = stored
            return None, {}
        if action.startswith('actions/download-artifact@'):
            if 'pattern' in settings or 'name' not in settings:
                raise UnknownExpression('download-artifact without name: is not modelled')
            stored = self.artifacts.get(settings['name'])
            if stored is None:
                return f'unable to download artifact {settings["name"]}: not found', {}
            shutil.copytree(stored, Path(settings.get('path', self.workspace)), symlinks=True, dirs_exist_ok=True)
            return None, {}
        if action.startswith('rust-lang/crates-io-auth-action@'):
            if not oidc:
                return 'the job has no id-token: write permission', {}
            self.auth_used = True
            return None, {'token': FAKE_CRATES_TOKEN}
        raise UnknownExpression(f'uses: not modelled: {action}')

    def run(self, job_name):
        body = job(self.workflow, job_name)
        oidc = re.search(r'\n    permissions:\n(?:      [^\n]*\n)*?      id-token: write\n', body + '\n') is not None
        self.env = workflow_env(self.workflow)
        self.auth_used = False
        results, failed = [], False
        for number, item in enumerate(job_steps(self.workflow, job_name)):
            step_id = item['id'] or ''
            used = set()
            if not self.condition(item['if'], failed):
                results.append(StepResult(step_id, item['name'], 'skipped'))
                continue
            if item['uses']:
                error, produced = self.uses(item, used, oidc)
                self.step_outputs[step_id] = produced
                results.append(StepResult(step_id, item['name'], 'failure' if error else 'success', stderr=error or '',
                                          secrets=frozenset(used), oidc=oidc))
                failed = failed or bool(error)
                continue
            env = {**self.environment, **self.env, **(OIDC if oidc else {}),
                   **{key: self.expand(value, used) for key, value in item['env'].items()}}
            output = self.temp / f'output-{job_name}-{number}'
            output.write_text('')
            script = self.temp / f'step-{job_name}-{number}.sh'
            script.write_text(item['run'] + '\n')
            env.update(GITHUB_OUTPUT=str(output), **self.step_env.get(step_id, {}))
            process = subprocess.run(['bash', '-e', str(script)],
                                     cwd=self.workspace, env=env, capture_output=True, text=True)
            self.step_outputs[step_id] = outputs(output)
            outcome = 'success' if process.returncode == 0 else 'failure'
            results.append(StepResult(step_id, item['name'], outcome, process.stdout, process.stderr, frozenset(used),
                                      oidc))
            failed = failed or outcome == 'failure'
        job_outputs = {}
        if '\n    outputs:\n' in body:
            block = body.split('\n    outputs:\n', 1)[1]
            for key, value in re.findall(r'^      ([a-z_0-9]+): ([^\n]*)$', block.split('\n    steps:', 1)[0],
                                         re.MULTILINE):
                job_outputs[key] = self.expand(value, set())
        return JobResult(results, job_outputs, self.auth_used)


def default_environment(workspace, temp, repository, run_id, attempt, path, home):
    """ランナーが手順に渡す環境変数（試験で要るものだけ）。"""
    return {
        'PATH': path, 'HOME': str(home), 'CI': 'true', 'GITHUB_ACTIONS': 'true',
        'GITHUB_WORKSPACE': str(workspace), 'RUNNER_TEMP': str(temp), 'GITHUB_REPOSITORY': repository,
        'GITHUB_RUN_ID': str(run_id), 'GITHUB_RUN_ATTEMPT': str(attempt),
        'GITHUB_STEP_SUMMARY': str(Path(temp) / 'summary.md'), 'LANG': os.environ.get('LANG', 'C.UTF-8'),
        'GIT_CONFIG_GLOBAL': str(Path(home) / '.gitconfig'), 'GIT_CONFIG_SYSTEM': os.devnull,
        'no_proxy': LOOPBACK, 'NO_PROXY': LOOPBACK,
    }


def job_if(workflow, job_name):
    match = re.search(r'^    if: ([^\n]*)$', job(workflow, job_name), re.MULTILINE)
    return match.group(1) if match else None


def job_needs(workflow, job_name):
    match = re.search(r'^    needs: ([^\n]*)$', job(workflow, job_name), re.MULTILINE)
    if not match:
        return []
    return [name.strip() for name in match.group(1).strip('[]').split(',')]


def job_condition(expression, needs):
    """ジョブの if:。状態の関数が無ければ、needs のジョブがすべて success のときだけ（success() の暗黙の &&）。"""
    def lookup(name):
        if name in ('always()', 'cancelled()'):
            return name == 'always()'
        if name == 'success()':
            return all(value.get('result') == 'success' for value in needs.values())
        parts = name.split('.')
        if parts[0] == 'needs' and len(parts) == 3 and parts[2] == 'result':
            return needs.get(parts[1], {}).get('result', '')
        if parts[0] == 'needs' and len(parts) == 4 and parts[2] == 'outputs':
            return needs.get(parts[1], {}).get('outputs', {}).get(parts[3], '')
        raise UnknownExpression(f'job if: reference not modelled: {name}')
    implicit = all(value.get('result') == 'success' for value in needs.values())
    if expression is None:
        return implicit
    value = truthy(Evaluator(lookup).evaluate(expression))
    if any(name in expression for name in ('always()', 'cancelled()', 'success()', 'failure()')):
        return value
    return implicit and value
