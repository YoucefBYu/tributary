#!/usr/bin/env bash
# TaskCompleted hook: fast quality gate on changed files.
# Exit 2 keeps the task open and feeds stderr back to the teammate.
# Exit 0 lets it complete.
#
# Notes:
# - Looks at the whole working tree's changes, so in a shared checkout a teammate can be
#   blocked by another teammate's broken work in progress. Commit early; if this bites,
#   run teammates in separate git worktrees (see docs/agent-team/README.md).
# - Does nothing until the repository has been bootstrapped (pyproject.toml exists).

cd "${CLAUDE_PROJECT_DIR:-.}" || exit 0

[ -f pyproject.toml ] || exit 0
if ! command -v uv >/dev/null 2>&1; then
  echo "uv is not installed; skipping Python quality gate." >&2
  exit 0
fi

changed=$( { git diff --name-only HEAD 2>/dev/null; git ls-files --others --exclude-standard 2>/dev/null; } | sort -u )
[ -z "$changed" ] && exit 0

fail=0

# Existing Python files that changed
py_changed=$(echo "$changed" | grep -E '\.py$' | while read -r f; do [ -f "$f" ] && echo "$f"; done)

if [ -n "$py_changed" ]; then
  echo "$py_changed" | xargs uv run ruff check >&2 || fail=1
  echo "$py_changed" | xargs uv run ruff format --check >&2 || fail=1

  pkgs=$(echo "$py_changed" | grep -oE '^packages/[^/]+' | sort -u)
  for p in $pkgs; do
    if [ -d "$p/tests" ]; then
      uv run pytest "$p/tests" -m "not integration and not chaos and not bench" -x -q >&2 || fail=1
    fi
    if [ -d "$p/src" ]; then
      uv run mypy "$p/src" >&2 || fail=1
    fi
  done
fi

# UI changes
if echo "$changed" | grep -qE '^ui/' && [ -f ui/package.json ] && command -v pnpm >/dev/null 2>&1; then
  ( cd ui && pnpm -s lint && pnpm -s typecheck && pnpm -s test --run ) >&2 || fail=1
fi

if [ "$fail" -ne 0 ]; then
  echo "" >&2
  echo "Quality gate failed. Fix the issues above, then mark the task complete again." >&2
  exit 2
fi

exit 0
