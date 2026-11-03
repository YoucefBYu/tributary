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
The gate is `.github/scripts/ci_local.sh`, the same script every CI workflow calls (decision: docs/agent-team/decisions/ci-alternatives.md). It creates and removes its own clean detached worktree.

1. `SHA=$(git rev-parse --short ${ARGUMENTS:-HEAD})`
2. If another agent may be editing `ci_local.sh` in the shared checkout, run the committed copy: `git show $SHA:.github/scripts/ci_local.sh > $SCRATCH/ci_local.sh && chmod +x $SCRATCH/ci_local.sh`, and call that instead.
3. Full gate (about 45 min; run it in the background): `TB_BUILD_CA_BUNDLE=/root/.ccr/ca-bundle.crt .github/scripts/ci_local.sh all --sha $SHA; echo "exit=$?"`. The CA variable is needed only in this sandbox, whose egress proxy re-terminates TLS for image builds.
4. Report the `PASS`/`FAIL`/`SKIP` lines. `FAIL:` lines printed *inside* the helm job are its mutation controls, so judge the job by its own `PASS helm` or `FAIL helm` line.
5. Small deltas on top of a commit that already passed `all`: run the affected jobs only (`contracts`, `lint`, `python`, `integration`, `supply-chain`, `image-check`, `ui`, `helm`, or `fast`) and say which ones you skipped and why.
6. Optional, once the human has set `TB_STATUS_TOKEN_FILE`: add `--report` to post `local/<job>` commit statuses.

## Push
Only after every step is PASS, as a **standalone command** (no `&&`, no pipes):

`git push -u origin wave/1-3` (the branch head must be the commit you gated)

The remote wave branch is currently `wave/1-3`; confirm with `git rev-parse --abbrev-ref @{u}` if unsure. Never force-push, never push `+` refspecs. Pushing a partial refspec is not permitted in this session.

For the helm block, extra CI-only checks and troubleshooting, see [reference.md](reference.md).
