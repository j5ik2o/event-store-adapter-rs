#!/bin/sh
# Combined statusline: personal claude-powerline (if installed) on top,
# the AI-DLC workflow statusline below. Not an aidlc-* framework file —
# this is a repo-local addition; framework upgrades will not touch it.
# Environments without claude-powerline fall back to the AI-DLC line only.
input=$(cat)
if command -v claude-powerline >/dev/null 2>&1; then
  printf '%s' "$input" | claude-powerline
fi
dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
if [ -f "$dir/aidlc-statusline.ts" ] && command -v bun >/dev/null 2>&1; then
  printf '%s' "$input" | bun "$dir/aidlc-statusline.ts" 2>/dev/null
fi
# Statusline is cosmetic; never propagate a renderer failure to the harness.
exit 0
