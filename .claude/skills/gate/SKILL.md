---
name: gate
description: "Lead's pre-push gate for Tributary. Use before every git push of a wave branch, before marking a wave or PR fix ready, or when asked to 'run the gate', 'check CI locally' or 'verify before push'. Runs lint, format, mypy, secret and licence scans, contract validation, integration-count pin, fast tests and integration tests on a clean detached worktree."
argument-hint: "[commit-sha]  (default: HEAD)"
---

Run the gate on a **clean detached worktree** of the commit you intend to push, never on the shared checkout (other agents' uncommitted work would pollute the result).

## Rules
- Capture the real exit code of every step: `<cmd>; echo "exit=$?"`. Never pipe pytest (or any gated command) through `tail`, `head` or `grep` when you need its exit code; if you must trim output, use `set -o pipefail` and still print `exit=`.
- Run each step as its own Bash call so one failure does not hide another. Report every step as PASS/FAIL with the exit code and the pytest summary line (real counts).
- A skipped step is a FAIL until it runs. Do not push on any FAIL.

## Steps
1. Worktree: `SHA=$(git rev-parse --short ${ARGUMENTS:-HEAD})`, then `git worktree add --detach /tmp/tb-gate-$SHA $SHA`. Run everything below as `cd /tmp/tb-gate-$SHA && <cmd>; echo "exit=$?"`. First sync: `uv sync --all-packages` (never a bare `uv sync`; see the `dev-env` skill).
2. `uv run ruff check .` and `uv run ruff format --check .`
3. `uv run mypy packages`
4. `uv run python .github/scripts/secret_scan.py`
5. `uv run python .github/scripts/licence_check.py`
6. `uv run --no-project --python 3.12 --with jsonschema --with openapi-spec-validator python contracts/validate.py --check-expectations`
7. `uv run python -m tests.support.integration_expected --check`
8. `uv run pytest -m "not integration and not chaos and not bench" -q -p no:cacheprovider`
9. Integration: `uv run pytest -m integration <affected package or tests dirs> -q -p no:cacheprovider`, or the full suite when the change is cross-cutting (runtime, sdk, contracts, compose, conftest).
10. Only when `deploy/helm/` or `contracts/topics/` changed: `python3 deploy/helm/gen_acls.py --check`, then helm lint, template and helm_check exactly as in [reference.md](reference.md#helm).
11. Clean up: `git worktree remove /tmp/tb-gate-$SHA`.

## Push
Only after every step is PASS, as a **standalone command** (no `&&`, no pipes):

`git push origin <sha>:refs/heads/wave/1-3`

The remote wave branch is currently `wave/1-3`; confirm with `git rev-parse --abbrev-ref @{u}` if unsure. Never force-push, never push `+` refspecs.

For the helm block, extra CI-only checks and troubleshooting, see [reference.md](reference.md).
