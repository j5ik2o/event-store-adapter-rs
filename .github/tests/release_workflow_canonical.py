"""lib-release.yml の正本との比べ方（形のテストの判定）。

event-store-adapter-scala の .github/tests/release_workflow_canonical.py（48e4256）を写したもの。
PUSH_RELEASE_TAG: 'true' を許すのは、lib-release.yml が正本 fixtures/release-workflow.yml と行ごとに一字一句同じときだけ。
違ってよいのは、`uses: <action>@<版>` の行の版と、その後ろの `# <版>` の注釈（Renovate が上げる）だけ。
lib-release.yml を変える PR は、正本も同じ PR で同じように変える。
"""
import re
from pathlib import Path

CANONICAL_PATH = Path(__file__).resolve().parent / 'fixtures' / 'release-workflow.yml'
CANONICAL = CANONICAL_PATH.read_text()
USES = re.compile(r'(\s*(?:- )?uses: [^@\s]+)@[A-Za-z0-9._-]+(?: # [A-Za-z0-9._-]+)?')


def release_has_gate(text):
    """text が正本と、uses: の版を除いて行ごとに一字一句同じか。"""
    lines, canonical = text.split('\n'), CANONICAL.split('\n')
    if len(lines) != len(canonical):
        return False
    for line, expected in zip(lines, canonical):
        if line == expected:
            continue
        found, wanted = USES.fullmatch(line), USES.fullmatch(expected)
        if not (found and wanted and found.group(1) == wanted.group(1)):
            return False
    return True
