---
name: run-phase
description: "Launch the agent team for a build phase or wave of Tributary from its phase plan."
argument-hint: "<phase> [wave]  e.g. 0, 1 wave-1, 2 wave-2"
disable-model-invocation: true
---

Launch the agent team for the phase and wave the user named after the command. If the user gave no argument, list the available plans in `docs/agent-team/phases/` and ask which to run.

Argument: $ARGUMENTS

## Steps

1. **Read the context.** Read `CLAUDE.md`, `docs/adr/ADR-001-kafka-centric-python-integration-engine.md`, `docs/agent-team/OWNERSHIP.md`, `docs/agent-team/STATUS.md` (if present) and the plan `docs/agent-team/phases/phase-<N>.md`. Identify the requested wave.
2. **Check preconditions.** Confirm the previous wave's exit criteria in `STATUS.md` are met. If not, say what is missing and stop. Confirm agent teams are enabled (`CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS=1`). If they are not, tell the user, and offer to run the wave with subagents or separate git worktrees instead.
3. **Create the task list.** Create every task in the wave's table in the shared task list with its ID, owner, deliverable, verify line and dependencies. Split any task that is not one deliverable.
4. **Spawn the team as agent teams.** Spawn one teammate per role in the wave's roster, using the subagent definition of the same name as the agent type, and give each the exact name from the roster. Each spawn prompt must contain: their task IDs, the owned paths from `OWNERSHIP.md`, the contracts they depend on, the wave's exit criteria, and the handoff format from `CLAUDE.md`. Do not paste the whole plan into every prompt.
5. **Delegate, do not implement.** You are the lead. Do not write code, tests or contracts yourself. If you catch yourself implementing, stop and assign the work. Wait for teammates to finish before proceeding.
6. **Coordinate.**
   - Route contract questions to `architect`.
   - When a teammate finishes a task, have `code-reviewer` (and `security-reviewer` where `CLAUDE.md` requires it) review it as a subagent, and send findings back to the owner.
   - Watch for two teammates editing the same files. Reassign or sequence the work.
   - If a teammate is stuck or stops early, message it, or spawn a replacement and re-assign the task.
7. **Gate.** When all tasks are complete, run the `/phase-gate` skill for this wave.
8. **Close out.** Ask `architect` to update `docs/agent-team/STATUS.md`. Shut down the teammates. Give the user a summary: what was delivered, what failed or slipped, open questions for the human, and the recommended next wave. Do not start the next wave without the user's go-ahead.

## Escalate to the user immediately
Anything listed under "Escalate to a human" in `CLAUDE.md`, and any spike result that contradicts an assumption in ADR-001.
