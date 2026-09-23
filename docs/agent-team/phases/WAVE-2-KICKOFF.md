# Wave 2 Kickoff: Pipelines and Delivery

**Date:** 2026-09-24 · **Branch:** `wave/1-2` · **Approved:** Architect + Lead ✅

---

## The goal

Build real HL7 and FHIR capability plus the full connector set for the MVP.

**Why it matters:** Wave 1 proved the architecture. Wave 2 proves the platform: healthcare flows, transformations, multiple connectors, chaos resilience.

---

## What we're shipping

| Category | Deliverables |
|---|---|
| **HL7/FHIR** | Lazy parser, FHIR helpers, ADT + ORU golden-file mappings |
| **Connectors** | 3 receivers (HTTP/FHIR, SFTP, Kafka) + 6 loaders (FHIR, PostgreSQL, ClickHouse, SFTP, S3, MLLP, Kafka) |
| **Pipeline** | Retry-topic with DLQ, transactional Kafka-to-Kafka, sync-reply mode, filter/enrich/validate/split steps |
| **Quality** | Chaos suite (worker kill, rebalance, outage), per-loader duplicate-delivery tests, conformance |

**By exit:** An ADT message flows from MLLP → Kafka → FHIR server and ClickHouse, in order, without loss.

---

## Timeline: 3–4 weeks

### Week 1 (Days 1–5): Foundations ← **YOU ARE HERE**
- **Track A (healthcare-standards-engineer):** W2-1 (HL7 parser), W2-2 design (FHIR)
- **Track B (platform-engineer):** W2-4 spike (retry-topic), W2-5 (steps)
- **Track C (connector-engineer):** W2-6/7 loaders and receivers (HTTP, PostgreSQL)
- **Track D (qa-engineer):** E2E framework ready; first ADT → ClickHouse test

**Checkpoint:** HL7 parser landing; FHIR design approved by architect

### Week 2 (Days 6–10): Integration
- **A:** W2-2 (FHIR), W2-3 (ADT/ORU mappings, golden files)
- **B:** W2-4 retry-topic implemented, W2-5 complete
- **C:** All loaders drafted; HTTP/FHIR receiver ready
- **D:** First golden-file test passing

**Checkpoint:** ClickHouse integration verified; no blockers

### Week 3 (Days 11–15): Completion
- **B:** W2-8 (sync-reply) prototype or implementation
- **C:** All receivers and loaders landed
- **D:** Chaos framework ready; broker-kill scenario passing

### Week 4 (Days 16–20): Gate
- All 9 tasks complete, tested, reviewed
- Exit criteria verified
- Gate report filed, approved

---

## Week 1 task assignments

### For `healthcare-standards-engineer` (4 + 3 days)

**W2-1: HL7 v2 lazy parser** (Days 1–4)
- Build on Phase 0 S1 measurements (2,446 msg/s/core with `python-hl7`)
- Implement: lazy parser, serializer, ACK builder
- **Deliverable:** `packages/tributary-hl7v2/` module
- **Tests:** Hypothesis round-trip tests; conformance corpus (10+ ADT samples)
- **Verify:** `pytest packages/tributary-hl7v2/tests` passes; `python-hl7` performance baseline measured

**W2-2: FHIR helpers kickoff** (Days 3–5, parallel with W2-1 end)
- Wait for architect's `ctx.fhir` skeleton
- Design `ctx.fhir` and `ctx.terminology` interfaces for W2-3
- Unit tests with fakes
- **Deliverable:** Interface signatures in `packages/tributary-fhir/`
- **Verify:** Tests pass; no calls to external APIs

### For `platform-engineer` (4 + 3 + 3 days)

**W2-4: Retry-topic spike + implementation kickoff** (Days 1–3, embedded in W2-4)
- **Spike S7:** measure delay precision, ordering, poison handling under chaos
  - Run fault-injection tests: broker kill, rebalance, consumer crash
  - Decide: single `retry.<dest>` topic or per-tier?
  - **Output:** spike report; ADR-003 update; go/no-go for tiers
- **Implementation:** (Days 4+) build retry processor, blocking policy, DLQ integration
- **Tests:** Fault-injection property tests; duplicate rate reported
- **Verify:** `pytest -m integration tests/retry` passes

**W2-5: Pipeline steps kickoff** (Days 3–5, parallel)
- Implement: filter, enrich, validate, split step functions
- Add: per-step timing metrics and tracing context
- **Tests:** Unit tests for each step; metrics visible in Compose stack
- **Verify:** `pytest packages/tributary-runtime/tests/test_steps.py` passes

### For `connector-engineer` (5 + 4 days)

**W2-6: Loaders (HTTP/FHIR, PostgreSQL)** (Days 1–3, then complete by day 5)
- Start with HTTP and PostgreSQL (foundation for later)
- **Each loader:**
  - Idempotency key documentation (how `tb-msg-id` is used)
  - Duplicate-delivery test (intentional replay; system handles harmlessly)
  - Backoff/retry/circuit-breaker logic
- **Tests:** Integration tests with testcontainers
- **Verify:** Both loaders pass idempotency tests; `pytest -m integration tests/loaders` passes

**W2-7: Receivers (HTTP/FHIR, SFTP)** (Days 1–3, then complete by day 5)
- HTTP/FHIR receiver (mirrors W1-5 MLLP pattern)
- SFTP poller (file-based ingest)
- **Tests:** Integration tests; ACK/error handling
- **Verify:** Receivers work with Compose stack; logs show no PHI

### For `qa-engineer` (5 days)

**W2-9 kickoff: E2E and chaos setup** (Days 1–5)
- **Week 1 goal:** test infrastructure ready, first happy-path test green
- Set up: synthetic ADT fixtures, ClickHouse schema, stub HTTP server
- Build: first conformance test (ADT A01 → FHIR Bundle)
- **First E2E:** send ADT via MLLP, observe ClickHouse row + FHIR POST
- **Chaos framework:** broker-kill scenario scaffolded (not yet passing)
- **Tests:** `pytest -m integration tests/e2e` running (may have TODOs)
- **Verify:** First golden-file test passes; no timeout failures; chaos faults defined

### For `architect` (as-needed)

- **Day 1:** Publish `ctx.fhir` skeleton if not already done
- **Days 1–3:** Review W2-2 FHIR interface design; approve before W2-3
- **Days 3–5:** Review W2-4 spike S7 design and early results
- **Ongoing:** Unblock contract questions; approve any schema changes

---

## Critical path and dependencies

```
HL7 parser (W2-1) ──→ FHIR design (W2-2) ──→ Mappings (W2-3)
                                            ↓
                                    Conformance tests (W2-9)
                                            ↑
Retry-topic spike (S7) ──→ W2-4 Retry-topic ──→ Sync-reply (W2-8)
                                            ↑
Loaders (W2-6) ──────────────────→ Conformance / Chaos (W2-9)
Receivers (W2-7) ────────────────→
```

**Week 1 blockers to watch:**
1. HL7 parser performance disappoints (S1 baseline: 2,446 msg/s/core)
2. FHIR interface design delayed (architect decision)
3. Retry-topic spike reveals complexity (if so, ship `blocking` only + defer tiers)

---

## Code quality standards (carried from Wave 1)

- `mypy --strict` on all new code
- `ruff check` and `ruff format` passing
- Every change ships with tests (bug fix → regression test)
- Unit tests inside packages; integration tests in `tests/{integration,e2e,chaos}`
- No PHI in logs, metrics, span attributes, exception messages
- Conventional commits; one file per owner per commit

---

## Review pattern (Phase 0 lesson)

Wave 1 found a message-loss bug in code that had passed clean code review, `mypy --strict`, and 701 tests. The pattern: **a control never applied to a path nobody thought about.**

**For Wave 2 reviews:**
- `code-reviewer` focuses on library defaults and transitive edges, not already-tested paths
- `security-reviewer` checks every place an identifier or header could leak into logs/metrics
- **Test requirement:** assert the fault fired (e.g., chaos tests must show broker actually killed)

---

## What's blocked on others

### Architect must ship before W2-2 starts

**`ctx.fhir` skeleton**
- Interface: `fhir_resource()`, `fhir_bundle()`, `terminology_lookup()`, etc.
- Fakes: test helpers that don't call external services
- Location: `packages/tributary-fhir/src/tributary/fhir/ctx.py`
- Due: today or day 1
- Owner: architect
- Downstream: healthcare-standards-engineer can't start W2-2 until this exists

### Lead must confirm before day 1

- [ ] ClickHouse is in Compose stack (from W1-7)
- [ ] All W1 code merged to `main` and CI passing
- [ ] Team members have Wave 2 planning docs (they have them; read them)

---

## Daily standup template

**Lead runs this at 9 AM (or equivalent):**

**Wave 2 standup – Day [N]**

```
## Status
- W2-1 (HL7): [owner] → [% done] | Blockers: [none / list]
- W2-2 (FHIR): [owner] → [% done] | Blockers: [none / list]
- W2-4 (Retry/Spike): [owner] → [% done] | Blockers: [none / list]
- W2-5 (Steps): [owner] → [% done] | Blockers: [none / list]
- W2-6 (Loaders): [owner] → [% done] | Blockers: [none / list]
- W2-7 (Receivers): [owner] → [% done] | Blockers: [none / list]
- W2-9 (E2E/Chaos): [owner] → [% done] | Blockers: [none / list]

## Red flags
- [Task]: [reason] → Action: [what, by when]

## Next 24 hours
1. [Task]
2. [Task]
3. [Task]
```

---

## Escalation

**Blocker found?** Message lead immediately.

**Architecture question?** Escalate to architect (24h SLA).

**Dependency blocked?** Lead coordinates (24h SLA).

---

## Reference documents

- **Overview:** [`wave-2-README.md`](wave-2-README.md)
- **Full plan:** [`wave-2-plan.md`](wave-2-plan.md)
- **Lead checklist:** [`wave-2-lead-checklist.md`](wave-2-lead-checklist.md)
- **Spikes S7/S8:** [`wave-2-spikes.md`](wave-2-spikes.md)

---

## Phase 0 carryover (important context)

**Phase 0 measurements backing Wave 2 design:**
- S1 HL7 parsing: 2,446 msg/s/core (`python-hl7`), 12.2x threshold
- S2 ACK latency: p99 33–125 ms (environment-dominated; Linux re-run pending)
- S4 partition count: 3 per topic (not 12) supports 1,000+ channels
- S5 ordering: 0 violations under chaos; retry-topic ordered by key
- S3 sync-reply: 1,026 timed-out requests were already committed → loader idempotency required

**What Wave 2 measures:**
- Actual HL7/FHIR transformation latency (real mappings, not trivial ADT)
- All loaders' duplicate rates under chaos
- Multi-step pipeline throughput
- E2E latency: MLLP in to FHIR POST to ClickHouse row (benchmark vs S1/S2)

---

## First week wins

**By end of day 5, these should be green:**

- ✅ W2-1 HL7 parser: hypothesis tests and conformance corpus pass
- ✅ W2-2 design: `ctx.fhir` interfaces approved, unit tests running
- ✅ W2-4 spike S7: retry-topic topology decided; report filed
- ✅ W2-5 steps: filter/enrich/validate scaffolded; tests written
- ✅ W2-6/7 start: HTTP loaders + receivers drafted; integration tests running
- ✅ W2-9 E2E: first ADT → ClickHouse happy-path test green (may have pending faults)

**If any of these is red, escalate to lead before day 6.**

