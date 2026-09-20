# Ownership map

One owner per path. Parallel agents stay out of each other's way by editing only what they own and messaging owners for anything else.

## Target repository layout

```
tributary/
├── CLAUDE.md
├── pyproject.toml                    # uv workspace
├── contracts/                        # envelope, topics, events, channel spec, OpenAPI
├── packages/
│   ├── tributary-sdk/                # Message, @step, ctx interfaces, test helpers
│   ├── tributary-runtime/            # Kafka consumer loop, pipeline executor, retry/DLQ
│   ├── tributary-hl7v2/              # parser, serializer, accessors, ACK builder
│   ├── tributary-fhir/               # FHIR helpers, ctx.fhir, ctx.terminology
│   ├── tributary-content-hl7-fhir/   # reference HL7 v2 to FHIR mapping steps
│   ├── tributary-receivers/          # MLLP, HTTP/FHIR, SFTP, Kafka, later DICOM, CDC
│   ├── tributary-loaders/            # HTTP/FHIR, PostgreSQL, ClickHouse, S3, SFTP, MLLP, Kafka
│   ├── tributary-archiver/           # events and content to ClickHouse, query library, replay
│   ├── tributary-api/                # FastAPI control plane
│   ├── tributary-cli/                # Typer CLI
│   ├── tributary-operator/           # kopf operator (Phase 2)
│   └── tributary-mirth-import/       # importer, mirth_compat, porting assistant
├── ui/                               # React + TypeScript console
├── deploy/                           # compose, helm, grafana, alerts
├── tests/                            # integration, e2e, chaos, conformance, fixtures, support
├── benchmarks/
├── spikes/                           # throwaway Phase 0 code
├── samples/mirth/                    # anonymized Mirth exports (git-ignored)
├── docs/{adr,spikes,agent-team,migration,security,api}/
└── .claude/{agents,skills,hooks,settings.json}
```

## Owners

| Path | Owner | Notes |
|---|---|---|
| `contracts/` | `architect` | Others propose changes by message |
| `docs/adr/`, `docs/spikes/`, `docs/agent-team/STATUS.md`, `docs/agent-team/gates/` | `architect` | |
| `packages/tributary-sdk/` | `platform-engineer` | Public API changes go through `architect` |
| `packages/tributary-runtime/` | `platform-engineer` | |
| `packages/tributary-hl7v2/`, `tributary-fhir/`, `tributary-content-hl7-fhir/` | `healthcare-standards-engineer` | |
| `packages/tributary-receivers/`, `tributary-loaders/` | `connector-engineer` | |
| `packages/tributary-archiver/` | `archive-engineer` | |
| `packages/tributary-api/`, `packages/tributary-cli/` | `control-plane-engineer` | |
| `packages/tributary-operator/`, `deploy/`, `.github/`, root build config | `devops-engineer` | |
| `packages/tributary-mirth-import/`, `docs/migration/`, `samples/mirth/` | `migration-engineer` | |
| `ui/` | `ui-engineer` | |
| `tests/integration/`, `tests/e2e/`, `tests/chaos/`, `tests/conformance/`, `tests/support/`, `benchmarks/` | `qa-engineer` | |
| `tests/fixtures/hl7/`, `tests/fixtures/fhir/` | `healthcare-standards-engineer` | QA may add generated data under `tests/support/` |
| Unit tests inside a package (`packages/<pkg>/tests/`) | the package owner | |
| `docs/security/` | `architect` (files reports written by `security-reviewer`) | |
| `spikes/S<n>-*/` | the spike owner named in the Phase 0 plan | |
| `docs/api/` | `control-plane-engineer` | |

Read-only roles: `code-reviewer`, `security-reviewer`.

## Cross-owner changes

1. **Small change in someone else's file** (a missing parameter, a bug): message the owner with the exact change and the reason. The owner makes it or replies with a better one.
2. **Contract change:** message `architect` with the proposed diff and impact. Wait for approval. Version the contract. Notify dependents.
3. **Shared seam** (for example the SDK `ctx` interface): the owner publishes an interface first, consumers code against it with fakes, and QA adds a contract test.
4. **Two agents need the same file:** the lead sequences the work or splits the file. If that is not possible, use git worktrees.

## Branches and commits

- **Shared checkout (default):** all teammates work in one working directory, which has exactly one checked-out branch. The human creates `wave/<phase>-<n>` before the run. Agents commit to it and never switch branches, because that would change files under everyone.
- **Git worktrees (optional, see README):** each agent gets its own worktree and branch `agent/<role>/<task-id>`, for example `agent/connector-engineer/W1-5-mllp-receiver`.
- Commit small and often with conventional commits, only the files you own. Never push or rewrite history. A human integrates and decides on rebases and merges.
