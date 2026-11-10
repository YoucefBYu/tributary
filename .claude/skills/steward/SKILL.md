---
name: steward
description: "PR-driving guidance for the Tributary lead: handling red CI, review comments and PR events on wave branches, regenerating generated files with tooling, and running the gate before every push. Use on any GitHub PR event, CI failure notification, or request to babysit, fix or drive a PR."
---

You are driving a Tributary pull request as the **lead**. You delegate fixes to owners; you do not implement.

## On every PR event
1. Identify what changed: new CI result, review comment, or new push. Act only on the newest state.
2. Use `gh api` (GET) or the GitHub MCP tools with minimal output. Fetch a failed job's log only for the failing job, and hand it to the `scout` subagent: "Summarize this CI failure: failing step, test ids, first error lines, likely owner. Max 20 lines."

## CI red
- Reproduce locally on a detached worktree of the PR head (see the `gate` skill) before assigning a fix. If it does not reproduce, compare tool versions and say so.
- Assign the fix to the owning role from `docs/agent-team/OWNERSHIP.md` with the exact failing command and output summary. A bug fix ships with a regression test.
- Flaky failure: rerun once. If it fails again, it is a real failure. Never skip, xfail or deselect a test to get green without the owner's and QA's agreement, and never lower the integration count pin.

## Generated files
Never hand-edit generated or pinned files. Regenerate them with their tooling and commit the result alongside the source change:
- `tests/support/integration_expected.json`: `uv run python -m tests.support.integration_expected --write`
- `deploy/helm/tributary/files/acls.json`: `python3 deploy/helm/gen_acls.py`
- `uv.lock`: `uv lock` (one agent per batch)
- `sbom.cdx.json`: produced by `.github/scripts/licence_check.py`
- UI API client: regenerated from OpenAPI by `ui-engineer`

## Review comments
- Route each comment to the owner. Security or PHI comments also go through `security-reviewer` (see `review-loop`).
- Reply on the PR only with what changed and the commit SHA, or with a reasoned disagreement. Never paste PHI, secrets or long logs.

## Before every push
Run the full `gate` skill on the exact commit. Push only on all-PASS, as a standalone command: `git push origin <sha>:refs/heads/wave/1-3`. Never force-push or rewrite shared history. Merging and releases are human decisions.

## Escalate to the human
Licensing, invariant changes, real patient data, credentials, cloud spend, publishing, or two rounds of agent disagreement.
