---
name: review-loop
description: "When and how to run code-reviewer and security-reviewer on Tributary changes, the Critical/High severity rule, and re-review after fixes. Use after a task completes, before marking work done, when a reviewer reports findings, or when deciding whether a security review is required."
---

## When
- **code-reviewer:** after every completed task, before it is marked done (CLAUDE.md, Definition of done 6).
- **security-reviewer as well** when the change touches any of: authn/authz, crypto or key handling, network listeners (MLLP, HTTP, ops ports, TLS), PHI handling (logs, errors, archive, search, UI display), secrets, containers or Helm security settings, or **dependencies** (pyproject, uv.lock, package.json, images).
- Both run as subagents, read-only. They never edit files.

## How to dispatch (lead)
Give the reviewer a short, specific prompt (no pasted diffs; it runs `git diff` itself):
```
Review task <id> by <owner>. Scope: <paths>. Base: <sha or "uncommitted in shared checkout">.
Focus: <invariants touched, e.g. 1 and 6>. Report in your standard format.
```
Run code-reviewer and security-reviewer in parallel when both apply.

## Severity rule
- **Critical** (data loss, PHI leak, invariant violation, security hole) and **High** must be fixed before the task is done. No exceptions for inconvenience.
- Medium/Low: the owner fixes or records them as follow-ups in the handoff's Risks line.
- A reviewer that could not verify something says so. Treat unverified Critical/High claims as open until checked.

## Loop
1. Send findings to the **owner** verbatim (file:line, issue, fix). The lead does not fix them.
2. Owner fixes, re-runs the verify command, sends a short delta handoff.
3. **Re-review** with the same reviewer, scoped to the fix: "Re-review task <id>: findings <ids> were addressed in <files>. Confirm resolved and check for regressions."
4. Repeat until no Critical/High remain. After two rounds of disagreement between agents, escalate to the human (CLAUDE.md).

## For reviewers
- Read only what the diff touches plus the directly related tests; grep before reading whole files.
- Keep the report under about 40 lines. Approve and stop if the change is good.
