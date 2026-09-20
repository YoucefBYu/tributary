# Tributary: instructions for Claude Code agents

Tributary (codename) is an open-source, Kafka-centric healthcare integration engine that competes with Mirth Connect and the Open Integration Engine (OIE). Python is the runtime for receivers, pipeline workers and loaders.

Read `docs/adr/ADR-001-kafka-centric-python-integration-engine.md` before any design work. It is the source of truth for architecture. Team setup and phase plans are in `docs/agent-team/`.

## Architecture invariants

These are non-negotiable. Changing one needs a new ADR approved by a human.

1. **ACK after commit.** Receivers acknowledge a sender only after the message is committed to Kafka (`acks=all`, idempotent producer). If Kafka is unavailable, refuse or NACK. Never ACK and drop.
2. **Kafka hops only at durability boundaries:** `raw`, per-destination `out`, `retry`, `dlq`, `reply`, `_tb.events`. Steps inside a pipeline run in-process.
3. **The data plane is independent.** Receivers, workers and loaders keep running when PostgreSQL, the API or ClickHouse is down. Channel specs come from the compacted `_tb.channels` topic.
4. **At-least-once delivery.** Every loader documents and tests its idempotency technique keyed by `tb-msg-id`. Exactly-once applies only to Kafka-to-Kafka stages via transactions.
5. **Ordering is explicit.** Per-key ordering comes from the channel spec. Retry policy per destination is `blocking` (preserves order) or `retry-topic` (does not). Never reorder silently.
6. **No PHI in logs, metric labels, trace attributes, exception messages or test output.** Log message ids only.
7. **Channels are declarative.** Python steps are functions of `(message, ctx)`. Side effects go through `ctx`. Every step must be unit-testable without Kafka.
8. **Envelope:** metadata in Kafka headers (`tb-*`), payload as opaque bytes. Payloads over 1 MB use claim-check.
9. **The archive is off the critical path.** It consumes `_tb.events`; nothing in message flow waits on it.
10. **Clean-room and licensing.** Core is Apache-2.0. Do not copy code from Mirth, OIE or any MPL/GPL source. Reading public documentation and file-format descriptions for interoperability is fine. Dependencies must be Apache-2.0, MIT, BSD, ISC or PSF. MPL only as an unmodified dependency approved by the architect. No GPL, AGPL, SSPL or BSL dependencies in the default distribution.

## Data handling

- **Synthetic data only.** Never put real patient data anywhere in the repo, in prompts, or in test output.
- HL7 fixtures must set MSH-3 (sending application) to `TRIBUTARY-SYNTH`. A hook blocks fixtures that do not.
- Anonymized Mirth channel exports go only in `samples/mirth/` (git-ignored). Treat them as sensitive.
- No secrets in the repo. Use `secretRef` in specs, environment variables or a secrets manager.

## Stack and standards

| Area | Choice |
|---|---|
| Language | Python 3.12+, `uv` workspace monorepo, type hints required, `mypy --strict` for `packages/*/src` |
| Lint and format | `ruff check`, `ruff format` |
| Kafka | Apache Kafka 4.x (KRaft), `confluent-kafka`; cooperative-sticky assignor and static membership |
| Runtime | `asyncio` with `uvloop`, `orjson`, pydantic v2, multi-process workers |
| Control plane | FastAPI, SQLAlchemy 2, psycopg 3, Alembic, PostgreSQL |
| Archive | ClickHouse (`clickhouse-connect`), optional S3/MinIO |
| Tests | `pytest`, `hypothesis`, `testcontainers`; markers `integration`, `chaos`, `bench` |
| UI | React, TypeScript, Vite, `pnpm`, API client generated from OpenAPI |
| Ops | Docker Compose (lite), Helm, Strimzi, KEDA, OpenTelemetry, Prometheus, Grafana |

Conventions:

- Small modules, no global mutable state, typed exceptions, structured JSON logs.
- Every change ships with tests. Bug fixes ship with a regression test.
- Conventional commits. Commit early and often, and only files you own.
- In the shared checkout, stay on the current `wave/*` branch and never switch branches. Only in your own git worktree use `agent/<role>/<task-id>`.
- Never `git push`, force-push or rewrite shared history. A human pushes.

## Commands

These are the target commands. If one does not exist yet, creating it is part of the walking-skeleton wave.

```bash
uv sync
uv run ruff check . && uv run ruff format --check .
uv run mypy packages
uv run pytest -m "not integration and not chaos and not bench"
uv run pytest -m integration          # testcontainers: Kafka, ClickHouse, PostgreSQL
docker compose -f deploy/compose/docker-compose.yml up -d
pnpm --dir ui lint && pnpm --dir ui typecheck && pnpm --dir ui test --run
```

## Repository map and ownership

Full table: `docs/agent-team/OWNERSHIP.md`. Short version:

- `contracts/` holds the envelope, topic naming, event schema, channel-spec JSON Schema and OpenAPI. Contract-first: the architect owns it.
- `packages/*` holds Python packages, each with one owning role.
- `ui/`, `deploy/`, `tests/`, `benchmarks/`, `docs/` each have one owner.
- Edit only files you own. If you need a change elsewhere, message the owner (or the architect for `contracts/`) with the exact change you need and why.

## Team protocol

- The lead delegates and does not implement. Teammates work from the shared task list and claim unblocked tasks.
- Size tasks to one deliverable (a module, a test suite, a doc). Include a **verify** line: the exact command or observable result that proves it works.
- **Handoff message** when finishing a task: what changed, files touched, how to verify, open risks. Keep it under 15 lines.
- Contract changes: propose to the architect; wait for approval; version the contract; then implement. Breaking changes need an ADR.
- If blocked for more than one round, message the lead with the blocker and what you tried. Do not idle silently.
- The reviewer roles are read-only and run as subagents after a task completes. Address findings marked **Critical** and **High** before marking the task done.

## Definition of done

1. Code, tests and docs are in the same change. Lint, format, type check and fast tests pass.
2. Integration tests exist for any behavior involving Kafka, ClickHouse or PostgreSQL.
3. Invariants 1-10 are respected. Anything touching delivery, ordering or PHI is called out in the handoff.
4. Metrics, logs and trace context are added for new stages, connectors or failure paths.
5. Public interfaces (SDK, spec, API) are documented and changed only through contracts.
6. The `code-reviewer` found no unresolved Critical or High issues. Changes to auth, crypto, network listeners, PHI handling or dependencies also pass `security-reviewer`.

## Escalate to a human

- Licensing, governance, naming and trademark questions.
- Any change to an invariant, or a dependency with a non-permissive license.
- Anything involving real patient data, credentials or cloud spend.
- Designing cryptography or key management beyond documented libraries and KMS features.
- Two rounds of disagreement between agents without a resolution.
- Publishing or releasing anything.
