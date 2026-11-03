---
name: handoff
description: "Tributary task handoff format (15 lines max). Use when finishing, pausing or reporting a task to the lead, replying to 'hand off', 'report back', 'what changed', or before marking a shared-task-list item complete."
---

Finish every task with one handoff message of **15 lines or fewer**. Plain text, no tables, no pasted logs.

```
Task: <id> <one-line outcome>
Changed: <what changed and why, 1-3 lines>
Files: <paths touched, comma-separated; group by dir if many>
Verify: <exact command(s)> -> <real result, e.g. "212 passed, 0 failed, exit=0">
Integration tests: <module: count> per module touched (or "none, reason")
Invariants/PHI: <which of 1-10 the change touches, and how they hold; "none">
Delivery/ordering: <any effect on ACK, retry, ordering, idempotency; "none">
Risks/open: <known gaps, flaky areas, follow-ups for other owners>
Needs: <reviews requested (code-reviewer, security-reviewer) or messages sent to owners>
```

Rules:
- Counts come from the actual run you just did. Never estimate, round up or reuse an earlier run. Write a failure as a failure.
- Name files and ids only. No PHI, payload contents or secrets (invariant 6).
- **Do not commit** unless the lead told you to for this task. When told to, commit only files you own, with a conventional-commit message.
- If something is still red, put it in Risks and say so in the first line; do not mark the task complete.
