# Phase 1: MVP

**Goal:** a usable engine: HL7 v2 and FHIR flows from MLLP/HTTP/SFTP/Kafka to HTTP/FHIR, SQL, ClickHouse and Kafka, with archive, message browser, reprocess and replay, a CLI, a basic UI, and a one-command lite stack.

**Prerequisite:** Phase 0 signed off. Apply any scope or threshold changes from `PHASE-0-DECISION.md` before starting.

**Working mode:** one wave per session, one team per wave (3 to 6 teammates). Create branch `wave/1-<n>` before each wave. Run `/run-phase 1 wave-<n>`, then `/phase-gate 1 wave-<n>`.

---

## Wave 1: Walking skeleton

**Purpose:** an end-to-end thread that proves the architecture and gives everyone a working repo: MLLP in, Kafka, Python pass-through step, HTTP out, archived events.

**Team (5):** `architect`, `platform-engineer`, `connector-engineer`, `devops-engineer`, `qa-engineer`.

| ID | Owner | Deliverable | Depends on | Verify |
|---|---|---|---|---|
| W1-1 | `architect` | Contracts v1: envelope headers, topic naming, event schema, channel-spec JSON Schema (source, steps, destinations, ordering, retry), each with valid and invalid examples | none | Schema validates examples; a script in `contracts/` runs the checks |
| W1-2 | `devops-engineer` | Monorepo bootstrap: uv workspace, package skeletons, ruff, mypy, pytest markers, pre-commit, CI running the commands in `CLAUDE.md` | none | `uv sync` and the lint, type and test commands succeed on a clean clone |
| W1-3 | `platform-engineer` | SDK v0: `Message`, `@step`, `Drop`, `ctx` interface with fakes, `run_step` test helper | W1-1, W1-2 | Unit tests; a step can be tested with no Kafka |
| W1-4 | `platform-engineer` | Runtime v0: consumer loop, pipeline executor, produce to `out.<dest>`, lifecycle events to `_tb.events`, blocking retry, graceful shutdown, spec load from `_tb.channels` | W1-3 | Integration test with testcontainers Kafka: message flows raw to out with events emitted; kill and restart preserves order |
| W1-5 | `connector-engineer` | MLLP receiver v0: framing, ACK-after-commit, TLS option, metrics | W1-1, W1-2 | Integration test: ACK returned only after commit; with Kafka stopped the sender gets no ACK or a NACK |
| W1-6 | `connector-engineer` | HTTP loader v0 with idempotency key, timeout, backoff, circuit breaker | W1-4 | Duplicate delivery is harmless; outage triggers retry then blocking behavior per spec |
| W1-7 | `devops-engineer` | Compose lite stack: Kafka (KRaft), ClickHouse, PostgreSQL, receiver, worker, loader; Dockerfiles | W1-5, W1-4, W1-6 | `docker compose up -d` gives a healthy stack in under two minutes |
| W1-8 | `qa-engineer` | E2E test: send synthetic ADT via MLLP, observe transformed output at a stub HTTP server; first chaos test: kill a broker mid-stream | W1-7 | `pytest -m integration tests/e2e` passes; chaos test shows no acknowledged-message loss |

**Exit criteria**
1. The E2E test passes in CI.
2. Invariants 1, 2, 3 and 5 are covered by automated tests.
3. Contracts v1 published and referenced by code and tests.
4. `code-reviewer` clean on the wave diff.

---

## Wave 2: Pipelines and delivery

**Purpose:** real HL7 and FHIR capability plus the full connector set for the MVP.

**Team (5):** `platform-engineer`, `connector-engineer`, `healthcare-standards-engineer`, `qa-engineer`, `architect` (contract changes and reviews).

| ID | Owner | Deliverable | Depends on | Verify |
|---|---|---|---|---|
| W2-1 | `healthcare-standards-engineer` | `tributary-hl7v2`: lazy parser, serializer, accessors, ACK builder, round-trip guarantee | W1-3 | Hypothesis round-trip tests; conformance corpus passes |
| W2-2 | `healthcare-standards-engineer` | `tributary-fhir` helpers and `ctx.fhir`, `ctx.terminology` interfaces | W2-1 | Unit tests; interfaces published through `architect` |
| W2-3 | `healthcare-standards-engineer` | Reference mappings ADT (A01, A03, A08) and ORU R01 to FHIR, with golden files | W2-1, W2-2 | Golden-file tests; rule comments reference the mapping source |
| W2-4 | `platform-engineer` | Retry-topic policy with delay tiers, DLQ, transactional Kafka-to-Kafka stage, key partitioning from spec expression | W1-4 | Fault-injection property tests; duplicate rate reported |
| W2-5 | `platform-engineer` | Filter, enrich, validate, split steps; per-step timing metrics and tracing | W2-1 | Tests plus metrics visible in Compose |
| W2-6 | `connector-engineer` | Loaders: FHIR REST, PostgreSQL, ClickHouse, SFTP, S3, MLLP client, Kafka; idempotency section and test for each | W1-6 | Duplicate delivery tests per loader |
| W2-7 | `connector-engineer` | Receivers: HTTP/FHIR, SFTP poller, Kafka source | W1-5 | Integration tests per receiver |
| W2-8 | `connector-engineer` with `platform-engineer` | Sync-reply mode (from spike S3) | W2-4 | Latency and timeout tests |
| W2-9 | `qa-engineer` | Conformance suites, chaos: worker kill, rebalance, destination outage; ordering assertions | W2-3, W2-4, W2-6 | `pytest -m chaos` passes with reported duplicate rates |

**Exit criteria**
1. An HL7 ADT flow to a FHIR server and to ClickHouse works end to end from a YAML channel.
2. Ordered channel preserves per-key order under chaos tests.
3. Every loader has a passing duplicate-delivery test.
4. Throughput: report msg/s per core and ACK p99 against the Phase 0 numbers.

---

## Wave 3: Archive, control plane, operations

**Purpose:** search, reprocess, replay, API, CLI, deployment and observability.

**Team (5):** `archive-engineer`, `control-plane-engineer`, `devops-engineer`, `qa-engineer`, `architect`.

| ID | Owner | Deliverable | Depends on | Verify |
|---|---|---|---|---|
| W3-1 | `architect` | Contracts v1.1: OpenAPI for channels, deploys, messages search and detail, reprocess, replay, stats, events (SSE) | W1-1 | OpenAPI lint passes; examples load in tests |
| W3-2 | `archive-engineer` | Archiver and ClickHouse schema; keyed-hash search attributes; content storage; retention TTL; backfill | W1-4 | Integration tests with real ClickHouse; archiver stopped does not affect flow |
| W3-3 | `archive-engineer` | Query library, reprocess and replay implementations | W3-2 | Reprocess sets `tb-parent-id`; replay by time range works |
| W3-4 | `control-plane-engineer` | API skeleton, PostgreSQL schema and migrations, channel CRUD, validate, deploy, rollback | W3-1 | Tests with real PostgreSQL; workers keep running with API stopped |
| W3-5 | `control-plane-engineer` | Reconciler to `_tb.channels`; `git-managed` vs `ui-managed` modes | W3-4 | Hot reload of a channel change without worker restart |
| W3-6 | `control-plane-engineer` | Search, message detail, reprocess, replay endpoints; audit events | W3-3, W3-4 | Contract tests against OpenAPI; audit emitted for content views |
| W3-7 | `control-plane-engineer` | CLI: `dev`, `test`, `deploy`, `channels`, `messages` | W3-4 | CLI smoke tests against the Compose stack |
| W3-8 | `devops-engineer` | Helm chart (Strimzi values, KEDA on lag), OTel collector, Prometheus rules, Grafana dashboards | W1-7 | `helm lint`, `helm template` pass; dashboards load against Compose |
| W3-9 | `qa-engineer` | E2E for search, reprocess and replay; chaos: stop ClickHouse and PostgreSQL during load; benchmark suite | W3-2, W3-4 | Invariant 3 and 9 tests pass; benchmark report committed |

**Exit criteria**
1. Message browser API returns per-stage content and destination statuses for a test run.
2. Workers process at full rate with ClickHouse, PostgreSQL and the API stopped.
3. Deploy and rollback of a channel version works through the API and CLI.
4. `security-reviewer` clean on authn, audit and PHI handling of the API.

---

## Wave 4: UI and hardening

**Purpose:** the console, security review, and the MVP release candidate.

**Team (5):** `ui-engineer`, `control-plane-engineer`, `qa-engineer`, `devops-engineer`, `architect`. `security-reviewer` and `code-reviewer` run as subagents.

| ID | Owner | Deliverable | Depends on | Verify |
|---|---|---|---|---|
| W4-1 | `ui-engineer` | UI scaffold, generated API client, auth flow | W3-1, W3-4 | `pnpm` lint, typecheck and test pass |
| W4-2 | `ui-engineer` | Dashboard: channel state, throughput, lag, errors (SSE) | W4-1 | Playwright test against Compose |
| W4-3 | `ui-engineer` | Message browser: search, stage content, diff, destination statuses, reprocess; PHI masked by default with audited reveal | W4-1, W3-6 | Playwright tests; audit event emitted on reveal |
| W4-4 | `ui-engineer` | Channel view (diagram from YAML), deploy and rollback screens | W4-1 | Playwright deploy flow |
| W4-5 | `control-plane-engineer` | OIDC login and role checks used by the UI; API gaps found by UI | W4-1 | Authorization tests per role |
| W4-6 | `qa-engineer` | Full chaos and performance runs on the reference cluster (3 brokers); published numbers | W3-9 | Report with methodology and raw results |
| W4-7 | `devops-engineer` | Release pipeline: SBOM, image signing, license scan gate, versioned images and chart | W3-8 | CI produces signed artifacts; scan fails on a forbidden license |
| W4-8 | `architect` | Docs: quickstart, concepts, channel spec reference, operations guide, ADR-004 (archive and PHI) and ADR-005 (bundles and trust model) drafts | W4-6 | A new user follows the quickstart to a running channel |

**Exit criteria (MVP)**
1. Time to first working channel on the lite stack is under 15 minutes for someone following the quickstart.
2. Chaos suite: zero acknowledged-message loss; ordering preserved on ordered channels; data plane unaffected by control-plane or archive outages.
3. Measured ACK p99, sustained msg/s and end-to-end latency reported against the ADR targets.
4. `security-reviewer` and `code-reviewer` have no unresolved Critical or High findings.
5. Docs let a new user install, build a channel, test it, deploy it and find a message in the browser.
6. Human sign-off and a decision on public release.

## Notes

- If a wave grows beyond 5 or 6 teammates, split it into two sessions. Coordination cost rises faster than throughput.
- `healthcare-standards-engineer` continues into Wave 3 and 4 as a subagent for mapping questions.
