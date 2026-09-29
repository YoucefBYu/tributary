# ADR-001: A Kafka-Centric, Python-Native Open-Source Integration Engine (Mirth Connect Competitor)

**Working codename:** Tributary (placeholder, check trademarks before adopting)
**Status:** Proposed — **updated 2026-09-20 with Phase 0 measurements** (sections 5.3, 6.3, 7.1, 7.3, 13, 14). Awaiting human sign-off at the Phase 0 gate.
**Date:** 2026-09-20
**Deciders:** Project founders / architecture review board (to be named)
**Phase 0 verdict:** GO WITH CHANGES — `docs/spikes/PHASE-0-DECISION.md`. No invariant changed. Two planning values were wrong and are corrected in place: the 12-partition default (5.3) and the 200-500 msg/s/core estimate (6.3).

---

## 0. Summary

Build an open-source healthcare integration engine that competes with Mirth Connect (and its community fork, Open Integration Engine) by changing the architecture rather than copying the feature list:

- **Kafka is the backbone.** Every durability point is a Kafka topic, so retry, replay, fan-out, shadow deployments and horizontal scaling come from the platform instead of being rebuilt in a database-backed message store.
- **Python is the runtime for transformations and loading.** Channel logic is ordinary Python packages: unit-testable, type-checked, code-reviewed in Git, and able to use the data/ML ecosystem directly.
- **Channels are code.** Git is the source of truth. The UI, CLI and Kubernetes operator are views over the same declarative spec.
- **The message browser is off the critical path.** An archiver streams events and content into ClickHouse, so search never slows down message flow.
- **Adoption path is explicit.** A Mirth/OIE channel importer, a compatibility shim for common transformer idioms, and a parity checklist.

Decision drivers: durability and replay, scale-out without a commercial clustering tier, developer experience, and a credible migration story.

---

## 1. Context

### 1.1 Market situation (verify before publishing)

- Mirth Connect was open source (MPL 2.0) from 2006. According to the Open Integration Engine (OIE) project, it moved to a proprietary model starting with version 4.6 in March 2025.
- The community responded with OIE, an MPL-2.0 fork of the Java code base under community governance. Its stated near-term goals are stabilizing the code base, documentation and community growth. Its GitHub discussions include a proposal to explore a modern, API-compatible runtime, which shows demand for a new architecture and a possible collaboration channel.
- Other competitors: Rhapsody, Cloverleaf, Corepoint, InterSystems IRIS for Health, Iguana (commercial); Apache Camel/NiFi + HAPI (general-purpose, no healthcare-specific operations UI); cloud services (Azure Health Data Services, AWS HealthLake/HealthOmics, Google Cloud Healthcare API), which are ingestion and storage services rather than full engines.

Sources to re-check: openintegrationengine.org and the OIE GitHub repository (license, governance, roadmap).

### 1.2 Pain points in the incumbent model that this design targets

| Pain point | Consequence |
|---|---|
| Channels stored as XML in an application database, edited in a desktop client | Hard to code-review, diff, promote across environments, or roll back |
| Transformers in embedded JavaScript (Rhino/E4X style) | Weak testing, debugging, and dependency management; limited ecosystem |
| Message store and queues in the relational database | Throughput ceiling, pruning pain, and the DB is on the critical path |
| Horizontal scaling and advanced clustering historically tied to a commercial subscription | Open-source users scale vertically |
| Limited native observability | Custom scripts for lag, latency, error-rate tracking |
| No replay/shadow capability | Testing a channel change against real traffic is manual |

### 1.3 Assumptions

- Core team skills: Python, Kafka, PostgreSQL, ClickHouse, HL7 v2 and FHIR domain expertise. The team is comfortable with Java, but the runtime is Python by choice.
- Initial focus is HL7 v2 and FHIR R4, with DICOM/DICOMweb, CDA and X12 following.
- Primary deployment is Kubernetes, with a single-host Docker Compose distribution for development and small sites.
- Numbers in this document are planning estimates that must be validated by the Phase 0 spikes (section 14).

---

## 2. Requirements

### 2.1 Functional

| # | Requirement | Priority |
|---|---|---|
| F1 | Receive over MLLP (plain and TLS), HTTP(S)/REST/FHIR, SFTP/file, Kafka, database (CDC via Debezium or poller), DICOM (DIMSE C-STORE, DICOMweb) | MVP: first four; rest v1 |
| F2 | Channel model: source, filters, transformers, routing, multiple destinations with independent queues and retry | MVP |
| F3 | HL7 v2 parse/serialize (v2.3 to 2.8), FHIR R4/R5 JSON, XML/CDA, JSON, CSV, X12 | MVP: HL7 v2, FHIR, JSON, CSV |
| F4 | Configurable ACK/NACK behavior, including synchronous response from a destination | MVP |
| F5 | Message browser: search, view content at each stage, per-destination status, export | MVP |
| F6 | Reprocess single messages, filtered sets, or time ranges; replay a channel from a point in time | MVP |
| F7 | Alerts on error rates, lag, connector down, latency | v1 |
| F8 | Users, roles, channel-level permissions, audit trail, SSO | v1 |
| F9 | Versioned deploy, rollback, promote across dev/test/prod | MVP (Git) |
| F10 | Shadow deploy: run a new channel version on live traffic and diff outputs without side effects | v1 |
| F11 | Import Mirth/OIE channels and code templates | v1 |
| F12 | Attachments/large payloads via claim-check | v1 |
| F13 | Data retention policies per channel (Kafka and archive) | MVP |

### 2.2 Non-functional (targets to validate)

| Dimension | Target |
|---|---|
| Durability | No acknowledged message is lost (RPO = 0 for ACKed messages). ACK only after Kafka commit with `acks=all`, RF=3, `min.insync.replicas=2` |
| Ingest latency | p99 receive-to-ACK under 50 ms at normal load |
| End-to-end latency | p99 under 1 s for a typical HL7-to-FHIR pipeline, excluding slow destinations |
| Throughput | 5k msg/s aggregate on a 3-broker reference cluster; single-node lite mode about 500 msg/s |
| Availability | 99.95% for ingest (multiple receivers behind an L4 load balancer); data plane keeps running if control plane or archive is down |
| Ordering | Per-key ordering guaranteed (for example per patient), configurable per channel and per destination |
| Delivery | At-least-once end to end, with idempotency support in loaders; exactly-once only for Kafka-to-Kafka stages |
| Security | TLS/mTLS everywhere, RBAC, immutable audit, encryption at rest, no PHI in logs |
| Operability | One-command local start; Helm chart for production; standard metrics/traces/logs |
| Extensibility | Third-party connectors and steps as Python packages via entry points |

### 2.3 Constraints

- Open-source dependencies only. **Permissive by default: Apache-2.0, MIT, BSD, ISC, PSF.** MPL-2.0 is
  **not** in the default allow-list: it is admissible only as an **unmodified** dependency approved by
  the architect, and CLAUDE.md escalates any non-permissive dependency to a human. No GPL, AGPL, SSPL or
  BSL in the default distribution. **Corrected at the Phase 0 gate** (`security-reviewer` LOW 9): the
  original wording listed MPL alongside the permissive licences, which is looser than CLAUDE.md's
  invariant 10 and contradicted the sentence immediately below. CLAUDE.md governs.
- Clean-room implementation: do not copy Mirth/OIE source. MPL 2.0 is file-level copyleft and would complicate an Apache-2.0 core — which is exactly why it is approval-gated rather than allowed.
- **MPL items in the tree, HUMAN-APPROVED 2026-09-22** as unmodified dependencies
  (`docs/agent-team/gates/phase-0-spikes.md` section 5.1): `orjson` (metadata declares
  `MPL-2.0 AND (Apache-2.0 OR MIT)`; a **shipping runtime** dependency), `hypothesis` (MPL-2.0,
  test-only, named in CLAUDE.md's stack table), `pathspec` (MPL-2.0, build-time via hatchling).
  **The approval covers these three packages unmodified; any new or modified MPL dependency needs fresh
  approval.**
- **`certifi` (MPL-2.0), HUMAN-APPROVED 2026-09-23** as a separate decision, **not** an extension of the
  approval above (`docs/agent-team/gates/phase-1-wave-1.md` 5.3 finding 1). Unmodified, **dev group
  only, never shipped**, and scoped specifically to its current transitive path:
  `testcontainers[kafka]` → `docker` → `requests` → `certifi`. Arriving by a different path, or into the
  runtime groups, is a different dependency and needs a fresh decision.
  **Why a separate approval rather than adding a fourth name to the list above:** that approval was
  recorded as covering *those three packages*, and a transitive arrival is exactly the case the scope
  note was written for. Extending it silently would have made the scope note decorative.
- **Checker defect, fixed 2026-09-24 (`f083c49`).** `licence_check.py`'s MPL branch (and the LGPL
  branch modelled on it) approved any expression containing "mpl" without checking the other terms,
  so `MPL-2.0 AND Proprietary` passed. The wave 1 "0 failed" result therefore never checked the
  non-MPL terms of `orjson`'s `MPL-2.0 AND (Apache-2.0 OR MIT)`. Those terms are permissive, so the
  approvals above are unaffected, but the CI evidence was weaker than it read. Regression tests were
  shown red against the old code. See `docs/agent-team/STATUS.md`. **Third instance of the pattern:**
  a licence claim is only as good as the mechanism that produced it, and a checker needs a failing
  case too.
- **`paramiko` (LGPL-2.1), HUMAN-APPROVED 2026-09-24** as its own decision. It is **not** an
  extension of the MPL approvals above, nor of `certifi`'s. This is the first LGPL dependency and the
  first non-permissive one in the **shipped runtime** other than `orjson`. The human chose it, when
  asked directly by the lead, over two alternatives: shelling out to OpenSSH, or descoping SFTP
  (`contracts/channel-spec/v0/README.md` 10.2).
  **Scope:** unmodified `paramiko`, as a dependency of `tributary-loaders` (the SFTP loader) and
  `tributary-receivers` (the SFTP poller), for SFTP only. Any other package depending on it, or a
  modified or vendored copy, needs a fresh decision.
  **Its dependency closure is not approved.** The option the human chose stated that paramiko's own
  dependencies are permissive; it approved paramiko alone. The wider "dependency closure" wording was
  the lead's relay, not the human's (confirmed by the lead, 2026-09-24). So each package paramiko pulls
  in must pass `licence_check.py` on its own licence, and any non-permissive one is a different
  dependency needing a fresh decision.
  **Obligations:** what distributing an LGPL-2.1 library inside a container image requires
  (notices, availability of the library's source) belongs to the W4-7 release pipeline and its SBOM,
  and to the legal review in 10.3. It is not settled here, and this is not legal advice.
- **`clickhouse-connect` NOT adopted for the `clickhouse` loader, HUMAN DECISION 2026-09-25.** It is
  Apache-2.0, but it unconditionally requires `certifi`, whose approval above is non-runtime only, so
  `licence_check.py` failed when it entered `tributary-loaders`' runtime closure (found by
  `connector-engineer`, W2-6, and reverted). Its defaults (`query_retries=2`, internal HTTP retries)
  also re-send silently on connection failure. The loader instead uses ClickHouse's HTTP interface via
  the standard library, on the HTTP loader's rules: redirects refused, no pooling, no hidden retries,
  auth headers from `secretRef`, stdlib TLS. **No new dependency; `certifi`'s scope is unchanged.**
  Rejected: approving `certifi` for the runtime; deferring the loader. This departs from the CLAUDE.md
  stack table (Archive: `clickhouse-connect`) **for the loader only**. The wave 3 archiver's client was
  left open here and was decided by the human on 2026-09-28 (next entry).
- **`certifi` (MPL-2.0) in the runtime, HUMAN-APPROVED 2026-09-28** as its own decision. It is **not**
  an extension of the 2026-09-23 dev-only `certifi` approval, nor of the 2026-09-25 loader decision.
  The human chose `clickhouse-connect` (Apache-2.0) as the wave 3 archiver's ClickHouse client when
  asked directly by the lead, over building the archiver on the loader's stdlib HTTP client.
  **Scope:** unmodified `certifi`, in the runtime, only as a dependency of `clickhouse-connect` in the
  archiver package. Any other package or path pulling it into the runtime, or a modified or vendored
  copy, needs a fresh decision. **The `clickhouse` loader is unchanged:** it keeps its stdlib HTTP
  client, and `tributary-loaders` must not gain `certifi` in its runtime closure.
  **Conditions:** (1) `clickhouse-connect`'s silent retries are turned off, both `query_retries` and
  its internal HTTP retries, so the runtime's retry policy is the only retry; (2) the archiver passes
  the same `code-reviewer` and `security-reviewer` review as the loaders, including the library-default
  checks of `wave-2.md` 6.3. **Follow-up (wave 3, when the archiver package is created):**
  `devops-engineer` extends `MPL_APPROVED` in `.github/scripts/licence_check.py` to cover `certifi`'s
  runtime scope. Until then the checker still treats `certifi` as non-runtime only, which is correct.
  The CLAUDE.md stack-table line was updated by the lead on `wave/1-2`.
- **How this one nearly passed unnoticed, which is the reusable part.** The root manifest annotates
  `"testcontainers[kafka]>=4.8",  # Apache-2.0`. That comment is *correct about testcontainers itself*
  and says nothing about its transitive closure, so it reads as a clearance while clearing nothing —
  the same shape as the `pathspec` miss below. **A licence claim is only as good as the resolution it
  came from, and a hand-written comment is not a resolution.** It was `licence_check.py` exiting 1 in
  CI, not review, that caught it.
- **The MPL inventory is known to be incomplete.** `pathspec` is in none of the spike lockfiles, because
  `[build-system] requires` resolves separately from the runtime and dev groups — so a licence inventory
  taken from lockfiles alone misses every build-backend dependency, and `pathspec` may not be the only
  one. The Phase 1 CI licence/SBOM check must cover build-system requirements as well as the locked
  runtime and dev groups.
- Python 3.12+; type hints required in the SDK.

---

## 3. Options Considered

### Option A: Modern Mirth-style monolith (single runtime, database-backed queues)

| Dimension | Assessment |
|---|---|
| Complexity | Low to medium |
| Cost | Low (PostgreSQL only) |
| Scalability | Medium: vertical first, horizontal needs custom coordination |
| Team familiarity | High |

**Pros:** easiest install and mental model for Mirth users; fewest moving parts; fastest MVP.
**Cons:** rebuilds queueing, retry, replay, and partitioning in the DB; the database sits on the critical path; scale-out is a bespoke project; no natural shadow/replay story.

### Option B: Kafka-centric streaming pipeline (selected)

| Dimension | Assessment |
|---|---|
| Complexity | Medium to high (operating Kafka), reduced by a lite distribution |
| Cost | Medium (Kafka + ClickHouse + PostgreSQL); low with managed Kafka or small clusters |
| Scalability | High: partitions and consumer groups give near-linear horizontal scale |
| Team familiarity | High for Kafka/Python/ClickHouse/PostgreSQL |

**Pros:** durable log gives retry, replay, fan-out and backpressure natively; shadow deployments and time-travel reprocessing become cheap; consumer groups replace bespoke clustering; decouples ingest from processing from delivery; archive and analytics off the critical path.
**Cons:** operational weight for small sites; per-channel topics multiply partitions; request/reply (synchronous response) needs an explicit pattern; ordering plus retry interplay needs careful design.

### Option C: Decoupled control/data plane in Go or Rust with Postgres-backed queues

| Dimension | Assessment |
|---|---|
| Complexity | High (new language for a Python/JVM-skilled team) |
| Cost | Low to medium |
| Scalability | High for the data plane |
| Team familiarity | Low to medium |

**Pros:** small memory footprint, top single-core performance, static binaries.
**Cons:** smaller healthcare library ecosystem (HL7/FHIR/DICOM), transformations would need an embedded scripting layer anyway, contributor pool for healthcare integrators is narrower.

---

## 4. Decision

Adopt **Option B**, with Python as the runtime for receivers, pipeline workers and loaders, and keep two good ideas from the other options:

1. **From C:** strict control-plane/data-plane separation. The data plane (receivers, workers, loaders) keeps processing if PostgreSQL, the API or the archive are down, because channel specs are cached from a compacted Kafka topic.
2. **From A:** a lite distribution (Docker Compose with single-node Kafka in KRaft mode, ClickHouse, PostgreSQL) so that trying the engine is as easy as starting Mirth.

Reference broker is Apache Kafka 4.x in KRaft mode (Apache 2.0, no ZooKeeper). Kafka-API-compatible brokers are supported where their license and feature parity fit (some, such as Redpanda, are source-available under BSL; check current terms before making them a default).

---

## 5. High-Level Design

### 5.1 Component diagram

```
 Senders: EHR · LIS · RIS · PACS · apps                     Downstream systems
     │  MLLP · HTTP/FHIR · SFTP · DICOM · CDC                   ▲ FHIR · SQL · SFTP · MLLP · Kafka
     ▼                                                          │
┌────────────────────┐                                ┌─────────┴──────────┐
│ 1. RECEIVERS       │                                │ 4. LOADERS         │
│ stateless asyncio  │                                │ Python sink plugins│
│ ACK after commit   │                                │ retry · breaker ·  │
└─────────┬──────────┘                                │ idempotency        │
          │ produce (acks=all)                        └─────────▲──────────┘
          ▼                                                     │ consume
┌────────────────────────────────────────────────────────────────┴───────────┐
│                        APACHE KAFKA (KRaft, RF=3)                          │
│  <ch>.raw ──────────► <ch>.out.<dest> ──► <ch>.retry / <ch>.dlq            │
│  <ch>.reply   _tb.channels (compacted)   _tb.events   _tb.audit            │
└──────┬───────────────────────────▲──────────────────────────┬──────────────┘
       │ consume                   │ produce                  │ consume
       ▼                           │                          ▼
┌──────────────────────────┐       │                ┌───────────────────────┐
│ 2. PIPELINE WORKERS (Py) │───────┘                │ 5. ARCHIVER           │
│ parse → filter → enrich  │                        │ events + content →    │
│ → transform → route      │                        │ ClickHouse / S3       │
└──────────────────────────┘                        └───────────┬───────────┘
                                                                │ queries
┌───────────────────────────── 6. CONTROL PLANE ────────────────▼───────────┐
│ API (FastAPI) · Web UI (React) · CLI · Reconciler · K8s Operator          │
│ PostgreSQL (config, RBAC, deployments) · OIDC · Git + CI (bundles)        │
└────────────────────────────────────────────────────────────────────────────┘
```

### 5.2 Components

| # | Component | Responsibility | Notes |
|---|---|---|---|
| 1 | **Receivers** | Accept connections/requests, do minimal parsing (MSH/envelope only), write raw bytes to `<ch>.raw`, then ACK | Stateless; horizontally scaled behind an L4 load balancer; Python asyncio with uvloop; wire contract is language-neutral so a hot path can be rewritten in Rust/Go later |
| 2 | **Pipeline workers** | Consume `raw`, run the in-process step DAG (parse, filter, enrich, validate, transform, route), produce to per-destination topics | One consumer group per channel; Kafka hops only at durability boundaries, not between steps |
| 3 | **Kafka** | Durable log, ordering, buffering, replay | Per-channel topics; compacted config topic; short retention (days) with the archive for long-term |
| 4 | **Loaders** | Deliver to destinations (FHIR/HTTP, SQL, ClickHouse, S3, SFTP, MLLP, Kafka) with retry, backoff, circuit breaker, idempotency | Python plugins; batching for analytical sinks |
| 5 | **Archiver** | Sinks events and content to ClickHouse (and optionally object storage) for the message browser, analytics, long-term retention | Off the critical path: if it is down, Kafka buffers |
| 6 | **Control plane** | API, UI, CLI, RBAC, deploy orchestration, alert rules | Reconciler publishes validated channel specs to a compacted Kafka topic; workers hot-reload from it |

### 5.3 Topic topology (per channel)

| Topic | Purpose | Key | Default retention |
|---|---|---|---|
| `<env>.<ch>.raw` | Messages as received (source of truth for replay) | ordering key | 7 days |
| `<env>.<ch>.out.<dest>` | Transformed message for one destination | ordering key | 7 days |
| `<env>.<ch>.retry` | Non-blocking retries for unordered destinations | ordering key | 3 days |
| `<env>.<ch>.dlq` | Messages that exhausted retries or failed a step | message id | 30 days |
| `<env>.<ch>.reply` | Short-lived replies for synchronous response mode | correlation id | 10 minutes |
| `_tb.channels` | Compacted: deployed channel specs | channel id | compacted |
| `_tb.events` | Lifecycle events for every message and stage (feeds archiver, metrics, alerts) | message id | 3 days |
| `_tb.audit` | Control-plane and access audit events | actor | 30 days (then archived) |

**Partition sizing — MEASURED, and the original planning value was wrong.** S4 tested exactly this assumption and it failed: at 12 partitions per topic the cluster was lost at about **425 to 450 channels**, short of the 500-channel target, at roughly 21,000 partitions (3 brokers, 1 GB heap each, 7.4 GiB Docker VM). At **3 partitions per topic, 1,000 channels (12,009 partitions) were healthy.**

Default is therefore **3 partitions per channel topic** (`contracts/topics/v0/topic-defaults.json`, ADR-002 D5), overridable per channel where throughput needs more. The "roughly 24k partitions, workable but needs testing" sentence above was the assumption; the test says 24k is past the cliff on a cluster of that size.

Topic sharing stays the mitigation, with its trigger expressed in **projected total partitions** rather than channels (`channels x 4 x partitions`): warn at 8,000, share new channels at 12,000 for a 3-broker 1 GB cluster (ADR-002 D4). 12,000 is measured; 8,000 is judgement; the fatal point is one failure at a computed count, and nothing was tested between 12,000 and 21,000.

`out.<dest>` MUST match its `raw` partition count on per-key-ordered channels, or the key-to-partition mapping differs and order breaks silently (invariant 5).

**Caveat:** these are lower bounds from one laptop-hosted cluster, not cluster limits. Re-measure on real broker memory before using them for sizing. See `docs/spikes/S4-topic-scale.md` and `docs/spikes/PHASE-0-DECISION.md` section 2.

### 5.4 Message envelope

Metadata travels in Kafka headers, payload stays opaque bytes, so the engine never forces a canonical model.

| Header | Meaning |
|---|---|
| `tb-msg-id` | UUIDv7, assigned at receive |
| `tb-channel`, `tb-channel-version` | Channel and spec version that produced/consumed |
| `tb-source` | Receiver instance and remote peer |
| `tb-received-at` | Receive timestamp (UTC) |
| `tb-content-type` | For example `application/hl7-v2+er7`, `application/fhir+json` |
| `tb-correlation-id` | Links request/reply and split/aggregate flows |
| `tb-parent-id` | Original message id for split or reprocessed messages |
| `tb-attempt` | Delivery attempt counter |
| `traceparent` | W3C trace context for OpenTelemetry |
| `tb-claim-check` | Object-store URI when the payload is stored externally (DICOM, large CDA) |

Message key is the result of the channel's `partitionKey` expression (for example patient identifier). Default max inline payload is 1 MB, larger payloads use claim-check.

### 5.5 Message lifecycle

```
RECEIVED ─► QUEUED (raw committed, ACK sent)
              │
              ├─► FILTERED (dropped by filter, terminal)
              ├─► ERROR ─► DLQ (transform failure after policy, terminal until reprocessed)
              └─► TRANSFORMED ─► ROUTED ─► per destination:
                                              PENDING ─► SENT (terminal)
                                                 │
                                                 └─► RETRYING ─► SENT | ERROR ─► DLQ
```

Every transition is an event on `_tb.events`, which drives the message browser, metrics and alerts.

---

## 6. Deep Dive

### 6.1 Channel-as-code

A **bundle** is a Git repository containing channel specs, Python steps, tests and a lockfile. CI builds it into an OCI image; the spec references the image digest.

```
adt-integration/
├── channels/adt-to-fhir.yaml
├── src/adt_flow/{__init__,filters,mapping}.py
├── tests/{fixtures/*.hl7, golden/*.json, test_mapping.py}
├── pyproject.toml + uv.lock
└── (Dockerfile generated by CI)
```

Illustrative channel spec:

```yaml
apiVersion: tributary.dev/v1
kind: Channel
metadata:
  name: adt-to-fhir
  labels: {domain: adt, owner: integration-team}
spec:
  ordering: per-key
  partitionKey: "hl7:PID.3.1"
  partitions: 12            # explicit override; the measured default is 3 (section 5.3, ADR-002 D5)
  retention: {raw: 7d, dlq: 30d}
  source:
    type: mllp
    listen: {port: 6661, tls: {secretRef: mllp-tls}}
    ack: {mode: after-commit, nack-on: [parse-error]}
  pipeline:
    bundle: ghcr.io/example/adt-integration@sha256:<digest>
    steps:
      - {name: parse,   uses: tributary.hl7v2.parse, with: {version: "2.5.1"}}
      - {name: filter,  ref: adt_flow.filters.only_admit_transfer_discharge}
      - {name: to-fhir, ref: adt_flow.mapping.to_fhir_bundle}
  destinations:
    - name: fhir-server
      type: http
      with: {url: "https://fhir.example/fhir", method: POST, auth: {secretRef: fhir-oauth}}
      retry: {policy: blocking, backoff: exponential, max: 10, on-exhaust: halt-partition}
    - name: dwh
      type: clickhouse
      with: {table: adt_events, batch: {rows: 5000, max-wait: 2s}}
      retry: {policy: retry-topic, max: 5, on-exhaust: dlq}
```

### 6.2 Python SDK (illustrative API)

```python
from tributary.sdk import step, Drop
from tributary.hl7v2 import HL7Message
from tributary.fhir import Bundle, Patient  # pydantic v2 models

ALLOWED = {"A01", "A02", "A03", "A08"}

@step
def only_admit_transfer_discharge(msg: HL7Message):
    if msg["MSH.9.2"] not in ALLOWED:
        return Drop(reason="unsupported trigger event")
    return msg

@step
def to_fhir_bundle(msg: HL7Message, ctx) -> Bundle:
    patient = Patient(
        id=ctx.ids.patient(msg["PID.3.1"], msg["PID.3.4"]),
        name=[{"family": msg["PID.5.1"], "given": [msg["PID.5.2"]]}],
    )
    return Bundle(type="transaction", entry=[ctx.fhir.upsert(patient)])
```

Test with plain pytest:

```python
def test_a01_produces_patient_upsert(hl7_fixture, run_step):
    out = run_step(to_fhir_bundle, hl7_fixture("adt_a01_basic.hl7"))
    assert out.entry[0].request.method == "PUT"
```

SDK design rules:

- Steps are pure functions of message plus context wherever possible; side effects go through `ctx` (lookups, terminology, ID mapping), so they can be faked in tests.
- Message-scoped variables live in `ctx.vars` (the analog of Mirth's channelMap/connectorMap).
- Long-lived shared state uses `ctx.kv` (versioned key-value in PostgreSQL with a local cache) and secrets use `ctx.secrets`.
- Connectors and steps register through Python entry points (`tributary.connectors`, `tributary.steps`).

### 6.3 Python runtime and performance strategy

- Kafka client: `confluent-kafka` (librdkafka) for throughput and transaction support.
- One worker process per group of partitions; parallelism scales with processes across pods, which sidesteps the GIL. I/O-bound steps use asyncio inside a worker.
- Hot-path libraries: `orjson`, pydantic v2 (Rust core), lazy HL7 segment parsing (parse only what a step touches).
- Escape hatches, in order: PyO3/Rust extension for the HL7 parser, then Rust/Go receivers, then free-threaded Python builds as they mature.
- ~~**Planning estimate:** 2 to 5 ms per message for parse plus transform, so roughly 200 to 500 msg/s per core.~~ **MEASURED (S1), and the estimate was pessimistic by one to two orders of magnitude.** Parse plus ADT-to-FHIR transform, single core, solo host: a **minimal lazy parser at 36,322 msg/s/core** (27 us p50) and **`python-hl7` at 2,446 msg/s/core** (355 us p50). `hl7apy` manages **100 msg/s/core** (8.6 ms p50) and **fails** the 200 msg/s/core threshold, so it must stay off the hot path; it remains useful for validation and profile checks where 12 ms and 250 KB per message are acceptable.
- **Consequence: the PyO3/Rust parser escape hatch is not needed for this target.** S1b was proposed if the threshold was missed; it was met with 12x margin on the conservative candidate. The escape-hatch ordering above stands for future needs, but nothing in Phase 0 justifies spending it now.
- **Memory per parsed message** matters as much as speed for claim-check-sized and batch messages: lazy 0.8 KB, `python-hl7` 62 KB, `hl7apy` 251 KB, against a 465 B raw message.
- **Caveats on the number.** Windows host, not Linux CI; a small ADT-shaped mapping, not a production one with terminology lookups; and the lazy parser is not spec-complete (no escape sequences or repetition handling), so 36k is a ceiling, not the figure for a finished parser. At the 200 msg/s/core threshold a message has a 5 ms budget, of which `python-hl7` already spends 0.5 ms. Re-measure on Linux and against a real mapping (`docs/spikes/S1-hl7-throughput.md`).

### 6.4 Delivery semantics, ordering and retries

**Semantics**

| Stage | Guarantee | Mechanism |
|---|---|---|
| Sender to `raw` | Durable before ACK | Idempotent producer, `acks=all`, ACK after commit |
| `raw` to `out.<dest>` | Exactly-once (optional) or at-least-once | Kafka transactions (read-process-write with offsets in the same transaction) |
| `out.<dest>` to external system | At-least-once | Offset commit after successful delivery; idempotency key = `tb-msg-id` |

**Idempotency per sink**

| Sink | Technique |
|---|---|
| FHIR server | Conditional create/update (`If-None-Exist`, PUT with business identifier) |
| PostgreSQL | `INSERT ... ON CONFLICT` keyed by message id |
| ClickHouse | ReplacingMergeTree or block-level insert deduplication |
| HTTP | `Idempotency-Key` header where the receiver supports it |
| SFTP/file | Deterministic file name from message id |

**Ordering and retry policy (per destination)**

- `blocking`: on failure, pause the partition and retry with backoff. Preserves order, which matters for ADT (A01 before A08) and result updates. Behavior on exhaustion is configurable: `halt-partition` (alert, wait for an operator) or `dlq-and-continue`.
- `retry-topic`: failed messages go to `retry` with delay tiers (for example 5 s, 1 min, 15 min), then `dlq`. Highest throughput, but ordering is not preserved for retried messages. Use only for order-insensitive sinks.

**Poison messages:** a step exception is retried in place a small configurable number of times, then routed to `dlq` with the stage, exception and input attached. Reprocessing a DLQ message re-injects it into `raw` with `tb-parent-id`.

**Duplicate suppression at the edge:** senders retransmit when they do not see an ACK. Optional dedupe on `(sending application, MSH-10)` within a time window, using a state store (Redis or embedded RocksDB).

### 6.5 Synchronous response mode

Some integrations need the sender to wait for a downstream result (query/response HL7, FHIR REST facades, ACK derived from a destination reply).

1. Receiver generates a correlation id, publishes to `raw`, and subscribes to `<ch>.reply` for that id (shared consumer per receiver instance, in-memory map of pending requests).
2. Worker/loader publishes the response to `<ch>.reply` with the same correlation id.
3. Receiver returns it to the sender, or times out (default 10 s) with a configurable NACK/error.

This costs latency and adds a failure mode, so it is opt-in per channel. Default is asynchronous ACK-after-commit.

**Refined 2026-09-28 (W2-8, ADR-003 D10):** step 1's "subscribes for that id" is implemented as **targeted** routing, not broadcast. Each receiver process holds reply partitions through the `replier` consumer group and names one in `tb-reply-partition`; the worker produces the reply to that partition. The answer semantics (`AA` / `AE` / `AR`) and the spec shape are in `contracts/channel-spec/v0` 3.2.6.

### 6.6 Storage and data model

**PostgreSQL (control plane):** tenants, users, roles, role bindings, channels, channel versions (spec plus bundle digest), deployments (environment, version, status), alert rules, code/KV entries, API tokens, secret references. Migrations with Alembic.

**Kafka:** transport, buffering, short-term replay. Not a query store.

**ClickHouse (archive and analytics), illustrative DDL:**

```sql
CREATE TABLE message_events (
  ts               DateTime64(3, 'UTC'),
  message_id       UUID,
  channel          LowCardinality(String),
  channel_version  UInt32,
  stage            LowCardinality(String),   -- received|filtered|transformed|sent|error
  destination      LowCardinality(String),
  status           LowCardinality(String),
  attempt          UInt8,
  content_type     LowCardinality(String),
  size_bytes       UInt32,
  error_code       LowCardinality(String),
  error_message    String,
  attrs            Map(String, String)       -- searchable metadata, no raw PHI by default
) ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (channel, ts, message_id);

CREATE TABLE message_content (
  message_id UUID,
  stage      LowCardinality(String),
  ts         DateTime64(3, 'UTC'),
  payload    String CODEC(ZSTD(3)),          -- encrypted if field-level encryption is on
  claim_ref  String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (message_id, stage)
TTL toDateTime(ts) + INTERVAL 90 DAY;
```

**Searching without storing PHI in clear text:** searchable identifiers (for example MRN) are stored as keyed hashes (HMAC) in `attrs`; the message browser hashes the search term with the same key. Content can be envelope-encrypted with a key from Vault Transit or a cloud KMS.

**Object storage (S3/MinIO):** claim-check payloads and optional long-term content archive.

### 6.7 API design

REST with OpenAPI (FastAPI) as the primary API, gRPC optional later for the data plane.

| Endpoint | Purpose |
|---|---|
| `GET/PUT /v1/channels/{id}` | Read/update a spec (draft) |
| `POST /v1/channels/{id}:deploy` / `:rollback` | Deploy or roll back a version to an environment |
| `POST /v1/messages:search` | Search archive (channel, time range, status, attributes, hashed identifiers) |
| `GET /v1/messages/{id}` | Message with content per stage and destination statuses |
| `POST /v1/messages/{id}:reprocess` | Re-inject one message |
| `POST /v1/channels/{id}:replay` | Replay from `raw` by time range or offsets, optionally into a shadow target |
| `GET /v1/channels/{id}/stats` | Throughput, latency, lag, errors |
| `GET /v1/events` (SSE) | Live event stream for the UI |

Later: an optional Mirth-REST-compatible shim so that existing CI and tooling that talks to Mirth's API can be pointed at the new engine.

### 6.8 Web UI

Mirth users expect a GUI, so it is a first-class requirement, not an afterthought:

- Dashboard with live channel state, throughput, lag, error counts.
- Message browser with per-stage content, diff between stages, reprocess and export.
- Visual pipeline view (source, steps, destinations) that round-trips to YAML.
- In-browser code editor (Monaco) with Python highlighting for small edits and fixtures.
- **Source-of-truth rule:** each environment is either `git-managed` (UI read-only for specs, only operational actions allowed) or `ui-managed` (UI edits create commits or PRs in the bundle repository). This prevents drift between UI and Git.

### 6.9 Testing, shadow deploys and replay

Kafka makes these features cheap and they are the headline differentiators:

- **Local dev loop:** `tributary dev` starts the lite stack with hot reload; `tributary test` runs fixtures and golden files.
- **Shadow deploy:** start the new channel version as a second consumer group on the same `raw` topic, writing to a shadow topic, then diff outputs against production output automatically. No side effects on real destinations.
- **Time-travel replay:** re-run last week's traffic through a new version before promoting it.
- **Canary by key range:** route a subset of keys to the new version.

### 6.10 Observability

- **Traces:** OpenTelemetry, with `traceparent` propagated across receiver, worker and loader; sampled.
- **Metrics (Prometheus):** `tb_messages_total{channel,stage,status}`, `tb_stage_duration_seconds`, `tb_ack_latency_seconds`, consumer lag via a Kafka exporter, receiver connection counts.
- **Logs:** structured JSON, message id only, never message content.
- **Alerts:** alert rules defined in the UI map to Alertmanager rules; shipped Grafana dashboards.

---

## 7. Scale and Reliability

### 7.1 Load estimation (illustrative)

| Scenario | Daily messages | Average | Peak (10x) | Cores at 300 msg/s/core |
|---|---|---|---|---|
| University hospital | 10 M | about 116 msg/s | about 1.2k msg/s | about 4 to 6 |
| Regional HIE | 100 M | about 1.2k msg/s | about 11.6k msg/s | about 40 |

**The core counts above are now conservative for transform, and say nothing about receive.** S1
measured 2,446 msg/s/core with `python-hl7`, eight times the 300 msg/s/core used here, so transform
is not the sizing constraint at these volumes. The binding constraint is the receive path: S2
saturated a **single receiver process below 5k msg/s** on its host, so the Regional HIE peak needs
multiple receiver processes and instances regardless of transform cost. Both figures are from one
laptop-hosted cluster; do not size a deployment from them (`docs/spikes/PHASE-0-DECISION.md` 2).

Storage for the 10 M/day case, assuming about 8 KB per message across raw and transformed topics and about 5x compression: about 16 GB/day, about 48 GB/day with RF=3, about 340 GB for 7-day retention cluster-wide. Archive in ClickHouse at about 8x compression: roughly 10 GB/day, about 3.6 TB/year. Python is not the bottleneck at these volumes; horizontal partitions are cheap.

### 7.2 Scaling

- Receivers scale by instance count (stateless); MLLP senders reconnect through the load balancer.
- Workers and loaders scale by consumer-group members up to the partition count; on Kubernetes, KEDA autoscaling on consumer lag.
- Hot single key or single-partition channels: phase-2 key-level parallelism inside a partition with a commit watermark.
- Consumer group tuning: cooperative sticky assignor and static membership to avoid rebalance storms with many channels.

### 7.3 Failure modes

Rows marked **measured** were exercised in Phase 0; the rest remain design intent.

| Failure | Behavior | Evidence |
|---|---|---|
| Receiver crashes mid-message | Sender sees no ACK and retransmits; optional edge dedupe | not measured |
| Kafka broker down (one of three) | RF=3 with `min.insync.replicas=2` keeps writing; below quorum, receivers NACK/refuse so senders retry (no silent loss) | **measured (S2):** no acknowledged loss observed in 20/20 leader-kill trials, 244,710 messages acknowledged. **New finding: ACKs stop for 7.5 to 12.7 s (median about 9.5 s) across *every* connection, not only the affected partition.** Cause inferred from code and timeline, not confirmed. Design consequence: per-connection head-of-line isolation is required in the product receiver |
| Kafka below `min.insync.replicas` (two of three down) | Receivers refuse or NACK; nothing is acknowledged | **measured (S2):** 0 ACKs inside the outage window in 6/6 trials; first ACK 25 to 35 s after the kill; 10.6k to 13.7k NACKs per trial. Invariant 1 held |
| Worker crash | Rebalance; uncommitted offsets redelivered (at-least-once) | **measured (S5):** 12 messages replayed after a `kill -9` at `commit_every=20`, **0 order violations, 0 loss**. Takeover is bounded below by `session.timeout.ms` (10.01 s at a 10 s setting; the 45 s default means a 45 s stop on an ordered partition) |
| Worker restart with the same `group.instance.id` | Static membership avoids the session-timeout wait | **measured (S5):** 3.21 s recovery for 2.0 s downtime |
| Adding a worker (scale-up) | Cooperative-sticky moves the minimum; unaffected partitions keep flowing | **measured (S5):** exactly 2 of 6 partitions moved, one from each founder; 0 duplicates, 0 losses, 0 reorderings |
| Poison message | Retry in place, then DLQ with context; ordered channels can be configured to halt instead | **measured (S5):** exactly 8 of 8 dead-lettered, partition kept moving; `halt-partition` verified as the ordered alternative |
| Transactional stage killed with a transaction open | Successor fences the dead producer, aborts the transaction, input offsets roll back | **measured (S5):** 50 records proven inside an open transaction at the kill; after restart, 2,000 records with 0 duplicates and 0 losses |
| Destination outage | Backoff or circuit breaker; lag grows and alerts fire; other destinations unaffected | **partly measured (S5):** 598 blocking pause/seek/resume cycles, 0 loss, 0 skips. Outages of seconds only; a multi-minute outage against `max.poll.interval.ms` is untested |
| Synchronous reply worker crash | Requests in flight time out; the receiver returns a NACK | **measured (S3, stand-in worker):** 5 to 6 s stall, 0 lost, no reply delivered to the wrong request. **The timed-out requests were already committed**, so a sender retry duplicates them downstream, and the timeout NACK means "committed, outcome unknown". The retry carries a **new** `tb-msg-id`, so loader idempotency on `tb-msg-id` does not absorb it; only a business-identifier sink or edge dedupe does (ADR-003 D9, amended 2026-09-28) |
| ClickHouse or archiver down | Processing unaffected; archiver catches up from Kafka within retention | not measured |
| PostgreSQL or API down | Data plane keeps running from compacted channel specs; deploys and UI edits unavailable | not measured |
| Bad deploy | Roll back to previous digest; replay affected range from `raw` | not measured |
| Broker loses quorum from partition-count pressure | — | **measured (S4):** at 12 partitions per topic the cluster died at about 425 to 450 channels, with metadata timeouts and raft election loss. This is a capacity failure mode the original table did not list |

### 7.4 Deployment topologies

1. **Lite:** Docker Compose with one Kafka (KRaft), ClickHouse, PostgreSQL, control plane, workers.
2. **Single-site production:** 3-broker Kafka, 2+ receivers, worker/loader pools, PostgreSQL HA (Patroni or CloudNativePG), ClickHouse with replication.
3. **Kubernetes:** Strimzi for Kafka, Helm chart plus operator (CRDs `Channel`, `Pipeline`), KEDA.
4. **Managed Kafka:** MSK, Confluent Cloud, or an Event Hubs Kafka endpoint. Verify support for transactions, compaction and ACLs before relying on them.

---

## 8. Security and Compliance

- **Transport:** TLS for all listeners (MLLP over TLS), mTLS or SASL/SCRAM plus ACLs on Kafka, per-channel topic ACLs. Every data-plane process reads its engine Kafka connection and credentials from one `engine-kafka/v0` document and refuses PLAINTEXT to any non-loopback broker, the lite Compose stack included (ADR-008, 2026-09-29). An mTLS listener can restrict which client certificates it accepts by SAN (ADR-007 D18).
- **AuthN/Z:** OIDC (Keycloak or similar); roles `admin`, `developer`, `operator`, `auditor`, `viewer`; channel-level scopes; API tokens with scopes and expiry.
- **Secrets:** Vault or Kubernetes external secrets; never in specs; only `secretRef`.
- **PHI protection:** field-level or payload envelope encryption in the archive; keyed-hash search index; log scrubbing; configurable masking in the message browser.
- **Audit:** every access and change to `_tb.audit`, archived to append-only storage; message-content views are audited.
- **Retention and erasure:** per-channel TTL in Kafka and archive; crypto-shredding option for erasure requests. Medical record retention duties often override erasure rights, so policies need per-jurisdiction review.
- **Code execution trust model:** Python steps are trusted code from reviewed Git repositories. Python has no reliable in-process sandbox, so multi-tenant or untrusted code requires container isolation (separate pods, gVisor/Kata, network policies), not `RestrictedPython`.
- **Supply chain:** locked dependencies, SBOM, signed images (Sigstore/cosign), dependency scanning.
- **Regulatory:** GDPR/DSGVO and HIPAA mapping documents; assess whether commercial distribution triggers medical-device software obligations in target markets. This is a legal question to confirm with counsel.

---

## 9. Mirth/OIE Parity and Migration

### 9.1 Concept mapping

| Mirth / OIE | Tributary |
|---|---|
| Channel | Channel spec (YAML or Kubernetes CRD) |
| Source connector | Receiver |
| Source filter / transformer | Pipeline `filter` and `transform` steps |
| Destination connector with queue | Loader with per-destination topic |
| Response transformer | `on_response` step / sync reply |
| channelMap / connectorMap / sourceMap / responseMap | `ctx.vars`, `ctx.source`, `ctx.responses` |
| globalMap / configurationMap | `ctx.kv`, `ctx.secrets` |
| Code template libraries | Python packages in the bundle |
| Deploy/undeploy/pre/post scripts | Lifecycle hooks (`on_start`, `on_stop`, `pre`, `post`) |
| Message browser, reprocess | Archive plus reprocess/replay API |
| Data pruner | Retention TTL policies (Kafka and archive) |
| Alerts | Alert rules on Alertmanager |
| Channel groups / tags | Labels and namespaces |
| Users and roles | OIDC plus RBAC |
| Clustering | Native via consumer groups |
| Plugins/extensions | Python entry points |
| Attachment handler | Claim-check store |

### 9.2 Migration tooling

- **Importer:** reads Mirth/OIE channel XML and emits a channel spec plus step skeletons. Connectors and settings translate mechanically; JavaScript transformer bodies do not.
- **`mirth_compat` shim:** a Python library exposing `msg["PID"]["PID.5"]`-style access and map objects so common idioms port with minimal edits.
- **Assisted porting:** optional LLM-assisted translation of JavaScript steps into Python, always emitting tests generated from real message samples and diffed against the original channel output through shadow mode. Treat generated code as a draft that a human reviews.
- **Parallel run:** point the same sender feed (or a mirrored copy) at both engines and compare outputs before cutover.

### 9.3 Where Mirth/OIE is ahead today

Installed base and community knowledge, mature GUI channel builder, breadth of connectors and plugins, simple single-process install, and proven production track record. A competitor has to reach a credible parity floor (section 10.2) and then win on operations and developer experience.

---

## 10. Competitive Strategy

### 10.1 Positioning

- **Segments:** (1) hospitals, HIEs and health-tech companies on Mirth/OIE that want modern operations and scale-out; (2) data and research platforms (for example university hospital data integration centers) that need HL7 v2 to FHIR to warehouse flows in Python; (3) startups building integration into their product.
- **Differentiators:** Git-native workflow; Python-native transformations; Kafka replay and shadow deploys; scale-out in the open-source core; OpenTelemetry-native observability; testability.
- **Complement, not only compete, with OIE:** OIE has the installed base and Java compatibility; this project offers a migration target and a different architecture. Consider engaging with the OIE community early rather than opposing it.

### 10.2 Credibility floor (must exist before calling it a Mirth alternative)

MLLP/HTTP/SFTP/DB/Kafka/DICOM connectors, ACK semantics including sync mode, message browser with reprocess, alerts, RBAC/SSO, importer, published benchmarks, security documentation, and a support offering.

### 10.3 Licensing and business model (options, not advice)

- **Core:** Apache 2.0 for the entire data plane, including scale-out (avoids the clustering-tier objection).
- **Revenue options:** support and consulting (as OIE proposes), managed cloud, certified content packs (HL7 v2 to FHIR mappings, national profiles such as German MII/ISiK), enterprise identity/audit add-ons if an open-core split is needed.
- **Content packs:** start from the HL7 v2-to-FHIR Implementation Guide mappings and extend them with tested, versioned Python steps.
- Get legal review on license choice, contributor agreements (CLA vs DCO) and trademark before public launch. This is not legal advice.

### 10.4 Success metrics

- Time to first working channel on the lite stack under 15 minutes.
- A typical Mirth channel migrated (with tests) in under one day.
- p99 ingest ACK under 50 ms; 5k msg/s on the reference cluster.
- Zero acknowledged-message loss in chaos tests (broker kill, network partition, worker kill).
- Published benchmark methodology reproducible by third parties.

---

## 11. Trade-off Analysis

| Decision | Gain | Cost | Why it is acceptable |
|---|---|---|---|
| Kafka as backbone | Durability, replay, shadow, scale-out | Operational weight, learning curve for small sites | Lite distribution and managed Kafka options; core differentiator |
| Python runtime | Ecosystem, testing, talent pool, faster development | Lower single-core throughput than JVM/Rust; GIL | Horizontal partitions, multiprocess workers, escape hatches; volumes fit (section 7.1) |
| Kafka hops only at durability points | Low latency, fewer topics | Less granular replay inside a pipeline | Replay from `raw` re-runs the whole pipeline deterministically |
| Per-channel topics | Isolated retention, ACLs, lag, easy replay | Partition count growth | Topic-sharing option for tiny channels |
| At-least-once plus idempotent loaders | Simplicity, works with external systems | Duplicates possible when a sink cannot dedupe | Idempotency matrix and edge dedupe |
| Archive off the critical path | Message flow never blocked by search storage | Browser is eventually consistent (seconds of lag) | Acceptable for operations; live view uses `_tb.events` stream |
| Git as source of truth | Review, diff, promotion, rollback | Friction for GUI-first users | UI creates commits/PRs; `ui-managed` mode for small sites |
| Trusted-code Python steps | Full language power | No in-process sandbox | Container isolation for multi-tenant; documented trust model |
| Clean-room implementation | Clean Apache-2.0 licensing | No code reuse from Mirth/OIE | Different architecture anyway |

---

## 12. Consequences

**Easier**
- Replay, shadow testing and safe rollout of channel changes.
- Scaling out in the open-source distribution.
- Unit and integration testing of transformations in CI.
- Feeding the same messages to analytics/warehouse (ClickHouse, Spark/Pathling) as first-class consumers.

**Harder**
- Small installations need Kafka (mitigated by the lite stack).
- Synchronous request/reply flows need explicit design.
- Ordering plus retry semantics must be explained clearly to integrators used to a simple queue.
- Building a GUI with the polish Mirth users expect.

**To revisit**
- Python receivers vs Rust/Go if profiling shows the hot path is the bottleneck.
- Free-threaded Python once the ecosystem catches up.
- Kafka share groups (queue semantics) for unordered workloads once mature.
- Tiered storage to extend Kafka retention and possibly reduce dependence on the archive for replay.
- Multi-region and disaster recovery (MirrorMaker 2 or cluster linking).
- Whether a compatibility runtime for legacy JavaScript steps is worth its maintenance cost.

---

## 13. Risks and Open Questions

Updated at the Phase 0 gate with the evidence column the spikes were meant to fill. Changes are in
bold; the full version with ratings is `docs/spikes/PHASE-0-DECISION.md` section 9.

| Risk | Likelihood | Impact | Measured evidence | Mitigation |
|---|---|---|---|---|
| Kafka is too heavy for target small sites | Medium | High | none — no spike addressed footprint | Lite distribution, managed offerings, sizing guide |
| Python hot-path throughput below estimate | ~~Medium~~ **Low** | Medium | **S1: 2,446 msg/s/core (`python-hl7`), 36,322 (lazy), against a 200 threshold** | **PyO3 parser not needed**; re-measure on Linux and a production mapping |
| Migration cost from JavaScript deters adopters | High | High | **none — S6 INCONCLUSIVE, no corpus available** | Importer, compat shim, assisted porting; **timebox Phase 1 effort rather than committing to an estimate** |
| Healthcare buyers want references, certification, support SLAs | High | High | none | Early design partners, security documentation, paid support |
| Topic/partition explosion with many channels | Medium | Medium | **S4: 12,009 partitions healthy, about 21,000 fatal at 1G heap; 500 channels FAIL at 12 partitions per topic, 1,000 PASS at 3** | **Partition default cut to 3; sharing trigger stated in projected partitions; re-measure on real broker memory** |
| Contributor fragmentation with OIE | Medium | Medium | none | Open dialogue, interoperability, importer targeting OIE |
| Security incident from arbitrary Python steps | Low to medium | High | none | Trust model, container isolation, signed bundles, review gates |
| **Receiver-wide stall on a single broker loss** | **Medium** | **Medium** | **S2: 7.5 to 12.7 s across all connections, 20/20 trials; cause inferred** | **Per-connection head-of-line isolation; broker session and election tuning** |
| **Sync-reply duplicates after a timeout NACK** | **High** | **Medium** | **S3: 1,026 of 9,000 requests timed out, all already committed** | **Document `AE` as "committed, a resend duplicates" (channel-spec 3.2.6); business-identifier idempotency at the sink; edge dedupe (ADR-003 D8, deferred). `tb-msg-id` idempotency covers worker replays only: a resend has a new id (ADR-003 D9 as amended 2026-09-28)** |
| **ACK latency target unverified on production-like hardware** | **High** | **Medium** | **S2: p99 33 to 125 ms at 1k msg/s against a 50 ms threshold; a bare producer with no MLLP had p99 131 ms on the same cluster** | **Linux re-run with a dedicated cluster before any sizing or marketing claim** |
| **Untested mechanisms carried forward** (`retry-topic` delay tiers, claim-check) | **Medium** | **Medium** | **none — neither was exercised in Phase 0** | **Spike both before implementing; claim-check is the largest single PHI store in the design** |

**Open questions**
1. Final name, trademark check, and governance (foundation, company, or community project).
2. Licensing and monetization: pure services or open-core?
3. First design partners and target segment for the MVP.
4. Whether to pursue collaboration or interoperability with OIE.
5. Which regulatory posture applies in the first target markets.

---

## 14. Roadmap and Action Items

Timeline assumes a team of 3 to 5 engineers and is a rough planning estimate.

**Phase 0: Spikes — complete 2026-09-20. Verdict GO WITH CHANGES (`docs/spikes/PHASE-0-DECISION.md`).**
1. [x] Benchmark Python HL7 v2 parse plus transform per core. **PASS** at 12x (`python-hl7`) and 182x (lazy); `hl7apy` FAILS. Rust-backed parser **not needed** — S1b dropped.
2. [x] MLLP receiver with ACK-after-commit. **Zero-loss and no-ACK-while-unavailable MET; p99 ACK latency NOT MET as measured, cause unresolved** (environment-dominated; needs a Linux re-run). New finding: a single broker loss stalls all connections for 7.5 to 12.7 s.
3. [x] Synchronous reply pattern prototype and timeout behavior. Established **against a stand-in worker**: +40 ms p50 at 100 msg/s, 4.6 KB per in-flight request, accurate timeouts, 0 loss across a worker restart. Timed-out requests are already committed, so sync mode needs loader idempotency.
4. [x] Topic-count stress test. **500 channels FAIL at 12 partitions per topic, 1,000 channels PASS at 3.** Partition default changed to 3.
5. [x] Blocking retry with partition pause/resume under rebalances. **PASS**, 0 order violations anywhere. `retry-topic` with delay tiers was **not implemented or measured** and still needs a spike.
6. [ ] Import 10 real (anonymized) Mirth/OIE channels. **INCONCLUSIVE — no corpus was available.** Blocked on the human supplying anonymized exports; the estimate stays a guess until then.

**Phase 0 follow-ups carried into Phase 1** (detail in `docs/spikes/PHASE-0-DECISION.md` section 8):
re-run S1 and S2 on Linux CI with a dedicated cluster; spike `retry-topic` delay tiers and
claim-check before implementing them; per-connection head-of-line isolation and multi-process
receivers; build sync reply on the batched-commit path with fail-fast plus DLQ.

**Phase 1: MVP (about 3 to 4 months)**
7. [ ] Runtime: receivers (MLLP, HTTP, SFTP, Kafka), pipeline workers, loaders (HTTP/FHIR, PostgreSQL, ClickHouse, S3, SFTP, MLLP, Kafka).
8. [ ] Channel spec, bundle build, CLI (`dev`, `test`, `deploy`).
9. [ ] Archiver to ClickHouse, message browser, reprocess and replay API.
10. [ ] Docker Compose lite distribution and Helm chart.
11. [ ] Basic UI: dashboard and message browser.
12. [ ] Metrics, traces, Grafana dashboards.

**Phase 2: v1.0**
13. [ ] Visual pipeline editor with YAML round-trip; RBAC and OIDC; alerts.
14. [ ] Shadow deploys and diffing; canary by key range.
15. [ ] Mirth/OIE importer and `mirth_compat` shim.
16. [ ] Kubernetes operator and KEDA autoscaling; DICOM and database CDC connectors.
17. [ ] Key-level parallelism; published benchmark report; security whitepaper.

**Phase 3: Ecosystem**
18. [ ] HL7 v2 to FHIR content packs and national profile packs.
19. [ ] X12 and additional standards; optional Mirth REST compatibility shim.
20. [ ] Multi-tenancy; managed cloud offering; connector marketplace.

**Follow-up ADRs to write**
- ADR-002: Topic topology and envelope specification
- ADR-003: Ordering, retry and DLQ policy
- ADR-004: Archive design, PHI handling and key management
- ADR-005: Bundle format, dependency isolation and trust model
- ADR-006: Licensing, governance and OIE relationship
