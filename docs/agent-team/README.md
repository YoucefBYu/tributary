# Agent team for building Tributary

This pack configures a team of Claude Code agents to build the engine described in `docs/adr/ADR-001-kafka-centric-python-integration-engine.md`. It uses two Claude Code features:

- **Subagent definitions** (`.claude/agents/*.md`): reusable roles with a system prompt, tool limits and a model.
- **Agent teams** (experimental): a lead session that spawns teammates, each in its own context window, coordinating through a shared task list and direct messages. Teammates can use the subagent definitions as roles.

You are the human in the loop. The lead delegates, the roles build and review, and you approve at the checkpoints below.

Official docs: https://code.claude.com/docs/en/sub-agents and https://code.claude.com/docs/en/agent-teams

## What is in the pack

| Path | Purpose |
|---|---|
| `CLAUDE.md` | Loaded by every agent: invariants, standards, protocol, definition of done |
| `.claude/settings.json` | Enables agent teams, pre-approves safe commands, denies risky ones, wires hooks |
| `.claude/agents/` | 12 roles plus the `scout` helper (below) |
| `.claude/skills/` | `/run-phase`, `/phase-gate`, `/new-adr`, plus on-demand procedures `gate`, `handoff`, `dev-env`, `review-loop`, `steward` (see [Agent tooling and token budget](#agent-tooling-and-token-budget)) |
| `.claude/hooks/` | `guard-content.sh` (blocks non-synthetic HL7 fixtures and secrets), `task-completed.sh` (lint, type and fast-test gate before a task can complete) |
| `docs/adr/ADR-001-...md` | The architecture the team implements |
| `docs/agent-team/` | This guide, `OWNERSHIP.md`, `STATUS.md`, and phase plans in `phases/` |
| `samples/mirth/` | Where you drop anonymized Mirth channel exports (git-ignored) |

## Roster

| Role | Model | Access | Owns |
|---|---|---|---|
| `architect` | opus | read, write, web | contracts, ADRs, status |
| `platform-engineer` | opus | full dev | SDK, Kafka runtime, delivery semantics |
| `connector-engineer` | sonnet | full dev | receivers and loaders |
| `healthcare-standards-engineer` | sonnet | full dev, web | HL7 v2, FHIR, mappings, synthetic fixtures |
| `control-plane-engineer` | sonnet | full dev | API, PostgreSQL, CLI, RBAC, reconciler |
| `archive-engineer` | sonnet | full dev | archiver, ClickHouse, search, replay |
| `ui-engineer` | sonnet | full dev | web console |
| `devops-engineer` | sonnet | full dev | CI, Compose, Helm, operator, observability, supply chain |
| `qa-engineer` | sonnet | full dev (tests only) | integration, e2e, chaos, benchmarks |
| `migration-engineer` | sonnet | full dev, web | Mirth importer, compat shim, porting |
| `security-reviewer` | opus | read-only + Bash | security and PHI review |
| `code-reviewer` | sonnet | read-only + Bash | correctness and invariants review |
| `scout` | haiku | read-only + Bash | log triage, test-failure summaries, locating code (20-line answers) |

Models use aliases (`opus`, `sonnet`) so they follow the current versions. Change them in each file's frontmatter.

## Agent tooling and token budget

Recurring procedures live in skills, so they are not re-explained in every spawn prompt and only load when used. A skill's `description` sits in context every turn; its body loads on invocation; sibling files load only when the body links to them and the agent opens them.

| Skill | Used by | What it holds |
|---|---|---|
| `gate` (+ `reference.md`) | lead | Pre-push gate on a clean `git worktree add --detach` tree: ruff, format, mypy, secret scan, licence check, contract validation, integration-count pin, fast tests, integration tests, helm checks when `deploy/helm` or `contracts/topics` change. Real exit codes; push as a standalone `git push origin <sha>:refs/heads/wave/1-3`. |
| `handoff` | every role | The 15-line handoff: what changed, files, verify output with real counts, integration tests per module, invariants and PHI, risks. No commits unless told. |
| `dev-env` | engineers, QA | Sandbox facts: dockerd restart, unique testcontainer names, `TB_OPS_PORT=0`, `TB_BUILD_CA_BUNDLE`, no bare `uv sync`, `gh` is api-only, one editor of `pyproject.toml`/`uv.lock` per batch. |
| `review-loop` | lead, reviewers | When code-reviewer and security-reviewer run, the Critical/High rule, re-review after fixes. |
| `steward` | lead on PR events | CI-red handling, regenerating generated files with tooling, the gate before every push. |

Wiring:

- Agent files preload skills with the `skills:` frontmatter field (engineers and QA get `handoff` and `dev-env`, reviewers get `review-loop`, the architect gets `handoff`). This applies when a role runs as a **subagent**. **Teammates ignore `skills:`**, so every role also lists `Skill` in `tools` and its body tells it to invoke the skills when they are not already loaded.
- `scout` (haiku, read-only, `maxTurns: 20`) is the lead's cheap helper for log triage, CI-failure summaries and "where is X". Implementer roles have no Agent tool, so they grep themselves.
- `subagentPromptCacheTtl: "1h"` in `.claude/settings.json` keeps teammate and subagent prompt caches warm across long integration runs (the default is 5 minutes). One-hour cache writes cost more than five-minute ones, so set it back to `"5m"` if waves are mostly short tasks.

Token rules (also in each agent body):

1. Grep first, then read line ranges; do not read whole large files.
2. Never read `uv.lock`, `packages/tributary-content-hl7-fhir/tests/fixtures/fhir-r4/fhir.schema.json`, `sbom.cdx.json` or caches. `.venv/`, `.mypy_cache/`, `.pytest_cache/`, `node_modules/` and `sbom.cdx.json` are `Read`-denied in settings. `uv.lock` and the FHIR schema stay readable because tests and tooling use them; grep them for one entry.
3. Run pytest with `-q` and report only the summary line and failing ids; do not paste large outputs.
4. Handoffs are 15 lines or fewer.
5. Lead: keep spawn prompts to task ids, owned paths, contracts and exit criteria; point at skills instead of restating procedures. Shut teammates down when their tasks are done.

Permissions added for fewer prompts: read-only `git -C <path>` forms (with matching `-C` denies for push --force, reset --hard, checkout, switch, stash, rebase, clean), `git fetch`, `git push origin <sha>:refs/heads/wave/*`, `gh api` (mutating flags `-X`, `--method`, `-f`, `-F`, `--field`, `--raw-field` and `--input` ask), `helm` (install, upgrade, uninstall, delete, rollback and push ask), `python3`. A bare `uv sync` now asks.

## Prerequisites

- A current Claude Code version. The agent-teams docs describe behavior as of v2.1.178, and several details below need later versions.
- `git`, `uv`, Python 3.12+, Docker with Compose, Node 20+ and `pnpm`, `jq` (used by the hooks).
- Optional: `tmux` or iTerm2 for split-pane teammates (`teammateMode` is `auto`).

## Quick start

```bash
mkdir tributary && cd tributary && git init
# copy the contents of this pack into the repo root, including the hidden .claude/ directory
git add -A && git commit -m "chore: agent team configuration"
git checkout -b wave/0
claude
```

Accept the folder trust prompt (needed for project hooks and agent files). Then:

```text
/run-phase 0
```

The lead reads the plan, creates the tasks, spawns the Phase 0 team, and coordinates. When it reports completion:

```text
/phase-gate 0
```

Review `docs/spikes/PHASE-0-DECISION.md`, decide, then continue with `/run-phase 1 wave-1` on a new branch (`git checkout -b wave/1-1`), and so on.

Watching and steering the team:

- The agent panel below the prompt lists teammates. Arrow keys select, **Enter** opens a teammate's transcript so you can message it directly, **Esc** returns to the prompt.
- `Ctrl+T` toggles the task list in in-process mode. Tell the lead to wait if it starts implementing itself: "Wait for your teammates to complete their tasks before proceeding."
- If Claude spawns subagents instead of a team, ask explicitly for an agent team.

## How the build is organized

| Phase | Waves | Focus |
|---|---|---|
| 0 | one team | Spikes that test ADR-001 assumptions and produce a go/no-go |
| 1 | 4 waves | Walking skeleton, pipelines and delivery, archive and control plane, UI and hardening |
| 2 | 3 waves | Operations, differentiators (shadow deploys, visual editor, Mirth migration), release |

Rules of the road:

- **Contract-first.** The architect publishes contracts before dependent work starts. This is how parallel teammates avoid integration surprises.
- **One owner per path.** See `OWNERSHIP.md`. Teammates message owners instead of editing their files.
- **3 to 6 teammates per wave, 5 to 6 tasks each.** The docs recommend starting with 3 to 5 teammates; more adds coordination overhead faster than throughput.
- **One wave per session.** A session has one team, and in-process teammates are not restored on `/resume`.

## Human checkpoints

Stop and decide at these points. The lead is instructed to escalate and not to proceed.

1. **After Phase 0:** review the decision memo. Failed thresholds can change the receiver language, the topic strategy or the MVP scope.
2. **Before each wave:** confirm the previous gate report and the wave plan.
3. **Any invariant change, license, governance, naming or trademark question.**
4. **Merging:** agents never push. You review the `wave/*` branch and integrate.
5. **Anything involving real patient data, credentials, cloud spend or a public release.**

## Cost and scaling guidance

- Agent teams use significantly more tokens than a single session: each teammate is a separate Claude Code instance. Start with Phase 0, whose tasks have clear boundaries and mostly produce measurements.
- To reduce cost: use fewer teammates, keep tasks small, and run reviews as subagents. To force every subagent and teammate onto one model, set `CLAUDE_CODE_SUBAGENT_MODEL` and `CLAUDE_CODE_SUBAGENT_MODEL_FORCE=1` (needs Claude Code v2.1.257 or later).
- Reviewer and architect roles are the best place to spend on a stronger model. Implementers on `sonnet` are usually sufficient.

## Quality gates and safety

- `TaskCompleted` hook: runs ruff, ruff format check, mypy and fast tests for the changed packages (and UI checks for `ui/`). A failure keeps the task open and feeds the error back. It looks at the whole working tree, so in a shared checkout a teammate can be blocked by another teammate's broken work; commit early, or use worktrees.
- `PreToolUse` guard: blocks non-synthetic HL7 fixtures (MSH-3 must be `TRIBUTARY-SYNTH`) and obvious secrets.
- Permissions: safe commands pre-approved so teammates do not stall on prompts (teammate prompts appear in the lead session). Denied: `git push`, `git reset --hard`, `git rebase`, `git checkout`, `git switch`, `git stash`, `rm -rf`, `sudo`, and pipe-to-shell.
- Reviewer roles have no Write or Edit tools by design.
- The pre-approved mode is `acceptEdits`. Consider auto mode or a container if you want fewer prompts; do not use `bypassPermissions` on a machine with real credentials.

## Fallbacks

- **Agent teams unstable or too costly:** set `CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS` to `0` in `.claude/settings.json` and drive roles as subagents: "Use the connector-engineer subagent to do task W1-5", then "Use the code-reviewer subagent to review it".
- **File conflicts in a shared checkout:** give each role a git worktree and run one Claude Code session per role, passing findings between them.

```bash
git worktree add ../tributary-connector -b agent/connector-engineer/W1-5
cd ../tributary-connector && claude --agent connector-engineer
```

## Known limitations of agent teams (from the docs)

- No session resumption for in-process teammates; the lead may message teammates that no longer exist. Tell it to spawn new ones.
- Task status can lag; a teammate may forget to mark a task complete. Nudge it.
- One team per session; no nested teams; the lead is fixed.
- Teammates start with the lead's permission mode; you cannot set it per teammate at spawn time.
- For a teammate spawned from a subagent definition, the documented applied parts are `tools`, `model` and the body. Other frontmatter such as `memory`, `effort` and `color` applies when the role runs as a subagent, not necessarily as a teammate. Teammates inherit the lead's effort level.
- While teams are enabled, a subagent that Claude names launches as a teammate, so reviewer runs may appear as teammates. This is harmless here.

## Things to verify on first run

I built this from the current subagent and agent-team docs but could not run it. Check these once:

1. `claude plugin validate .claude/agents` (v2.1.233 or later) confirms agent frontmatter parses.
2. Run `claude --debug` briefly and confirm the hooks in `.claude/settings.json` load; the exact hook and settings schema is in the Claude Code hooks and settings docs. The hooks read the tool input with `jq` and do not depend on task payload fields.
3. Confirm `/run-phase 0` receives its argument. If it does not, type the phase number in the same message.
4. Confirm the permission rules match your commands; adjust `allow` and `deny` to taste.

## Customizing

- Add or split roles by adding a file to `.claude/agents/`. Changes are picked up within seconds, except the first time a new `agents` directory is created.
- Keep each `description` short: subagent descriptions share a context budget.
- Adjust invariants only through an ADR that the human approves; every role's behavior derives from `CLAUDE.md`.
