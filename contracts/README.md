# Contracts

Owned by the `architect`. Populated in Phase 0 (v0) and Phase 1 wave 1 (v1).

Planned contents:

| Contract | Format | Consumers |
|---|---|---|
| Message envelope (`tb-*` headers) | Markdown spec plus JSON Schema for header values | receivers, runtime, loaders, archiver |
| Topic naming and retention defaults | Markdown spec plus a validator script | runtime, devops, control plane |
| Lifecycle event schema (`_tb.events`) | JSON Schema | runtime, archiver, API, UI |
| Channel spec | JSON Schema plus valid and invalid examples | control plane, runtime, CLI, UI, migration |
| Control-plane API | OpenAPI 3.1 | API, UI client generation, CLI, tests |

Rules:

- Every contract is versioned with a changelog. Breaking changes need an ADR.
- Every contract ships valid and invalid examples that tests load.
- Owners of other paths propose changes by message to the `architect`.
