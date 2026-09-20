---
name: phase-gate
description: "Verify the exit criteria of a Tributary wave with tests, reviews and measurements, and report pass or fail."
argument-hint: "<phase> [wave]"
disable-model-invocation: true
---

Verify that the wave named after the command meets its exit criteria. Do not fix problems yourself; report them and assign them.

Argument: $ARGUMENTS

## Steps

1. Read the wave's exit criteria in `docs/agent-team/phases/phase-<N>.md` and the current `docs/agent-team/STATUS.md`.
2. Run the repository checks that exist: lint, format check, type check, fast tests, integration tests, and the wave-specific commands named in the plan. Record the exact commands and results.
3. Have `qa-engineer` run any chaos or benchmark suites the exit criteria require and report numbers against thresholds.
4. Run `code-reviewer` on the wave's cumulative diff (`git diff <wave-start-ref>..HEAD`) and `security-reviewer` when the wave touched authn/authz, crypto, listeners, PHI handling, containers or dependencies.
5. Ask `architect` to check the invariants (`CLAUDE.md` 1-10) and contract conformance across the wave.
6. Produce a gate report with one row per exit criterion: **criterion, evidence, PASS or FAIL**. Put outstanding Critical and High findings at the top.
7. Save it as `docs/agent-team/gates/phase-<N>-<wave>.md` (ask `architect` to write it). Recommend GO, GO WITH CONDITIONS, or NO-GO, and list the tasks needed to get to GO.

Never mark a criterion as passed without evidence. A skipped check is a FAIL until it runs.
