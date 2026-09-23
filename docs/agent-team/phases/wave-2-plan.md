# Wave 2 Plan: Pipelines and delivery

**Status:** Draft · **Date:** 2026-09-23 · **Lead:** (to be assigned)

**Purpose:** Real HL7 and FHIR capability plus the full connector set for the MVP.

**Team (5):** `platform-engineer`, `connector-engineer`, `healthcare-standards-engineer`, `qa-engineer`, `architect` (contract changes and reviews).

**Branch:** `wave/1-2` (to be created by lead)

**Duration estimate:** 3–4 weeks, one wave per session

---

## Pre-wave setup

### Human decisions required (blocking)
Wave 2 cannot start until the human approves these Phase 0 decisions (as of 2026-09-22, all are approved):

- ✅ **Partition defaults:** 3 per channel topic (down from 12)
- ✅ **Invariants 1–10 unchanged** 
- ✅ **Linux re-run of S1 and S2** approved as Phase 1 CI task (do before receiver hardening)
- ✅ **Revised Phase 1 scope** approved (partition, retry-topic, claim-check as spikes)

### Scope changes from Phase 0 DECISION.md section 8

1. **Measured defaults on day one:** runtime must read 3 partitions, explicit `session.timeout.ms`, `commit_every` from `contracts/topics/v0/topic-defaults.json`
2. **Spikes before implementation:**
   - `retry-topic` delay tiers (S5 follow-up 4) — 2–3 days
   - Claim-check for payloads over 1 MB — 2–3 days
3. **Receiver design requirements:**
   - Per-connection head-of-line isolation, verified by reproducing S2's `leader-kill`
   - Multi-process receivers (one process saturated below 5k msg/s)
4. **Sync-reply design:** built on batched-commit path, fail-fast + DLQ for reply worker
5. **Test pattern carryover:** assert the fault fired and keep mutation controls for headline claims

### Contract changes in-flight or pending

None known. Architect will validate before wave starts.

---

## Task breakdown

| ID | Owner | Deliverable | Depends on | Estimate | Verify |
|---|---|---|---|---|---|
| **W2-1** | `healthcare-standards-engineer` | `tributary-hl7v2`: lazy parser, serializer, accessors, ACK builder, round-trip guarantee | W1-3 | 4d | Hypothesis round-trip tests; conformance corpus passes |
| **W2-2** | `healthcare-standards-engineer` | `tributary-fhir` helpers and `ctx.fhir`, `ctx.terminology` interfaces | W2-1 | 3d | Unit tests; interfaces published through `architect` |
| **W2-3** | `healthcare-standards-engineer` | Reference mappings: ADT (A01, A03, A08) and ORU R01 to FHIR, golden files | W2-1, W2-2 | 4d | Golden-file tests; rule comments reference mapping source |
| **W2-4** | `platform-engineer` | Retry-topic policy with delay tiers, DLQ, transactional Kafka-to-Kafka, key partitioning from spec expression | W1-4 | 4d | Fault-injection property tests; duplicate rate reported; spike first (2–3d) |
| **W2-5** | `platform-engineer` | Filter, enrich, validate, split steps; per-step timing metrics and tracing | W2-1 | 3d | Tests plus metrics visible in Compose |
| **W2-6** | `connector-engineer` | Loaders: FHIR REST, PostgreSQL, ClickHouse, SFTP, S3, MLLP client, Kafka; idempotency tests for each | W1-6 | 5d | Duplicate delivery tests per loader |
| **W2-7** | `connector-engineer` | Receivers: HTTP/FHIR, SFTP poller, Kafka source | W1-5 | 4d | Integration tests per receiver |
| **W2-8** | `connector-engineer` + `platform-engineer` | Sync-reply mode (from spike S3) with batched-commit path, fail-fast + DLQ | W2-4 | 3d | Latency and timeout tests; AE semantics documented |
| **W2-9** | `qa-engineer` | Conformance suites, chaos tests: worker kill, rebalance, destination outage; ordering assertions | W2-3, W2-4, W2-6 | 5d | `pytest -m chaos` passes; duplicate rates reported |

**Total estimate:** 35 person-days ≈ 5 weeks for a team of 2–3 (realistically: 3–4 weeks with full team)

### Dependency graph

```
W1-3, W1-4, W1-5, W1-6 (Wave 1 outputs)
    |
    +-- W2-1 (HL7 parser) ─+── W2-2 (FHIR) ─+
    |                       |                  +── W2-3 (Mappings) ──+
    |                       |                                         |
    +-- W1-4 ───+── W2-4 (Retry/DLQ) ──+                              |
    |           |                      +── W2-8 (Sync-reply)         |
    |           +── W2-5 (Steps)                                      |
    |                                                                  |
    +-- W1-6 ───── W2-6 (Loaders) ─────────────────────────+          |
    |                                                       |          |
    +-- W1-5 ───── W2-7 (Receivers) ─────────────────────+  |          |
    |                                                   |    |          |
    +─────────────────────────────────────────────── W2-9 ──┘
                                                       ↑
                                            (depends on all above)
```

---

## Sequencing strategy

### Phase 1: HL7 parsing and FHIR (Week 1–2)

**Parallel tracks:**
- **Track A (healthcare-standards-engineer):**
  - W2-1: `tributary-hl7v2` lazy parser, serializer, ACK builder (4d)
  - W2-2: `tributary-fhir` and `ctx` interfaces (3d)
  - W2-3: Reference ADT and ORU mappings with golden files (4d)
  - **Checkpoint:** all three ready for W2-9 conformance suite by end of week 2

- **Track B (platform-engineer, connector-engineer, qa-engineer):**
  - W2-4: Spike `retry-topic` delay tiers (2–3d within this task)
  - W2-5: Filter, enrich, validate, split steps (3d)
  - W2-6: All loaders except ClickHouse (which may not have been spiked yet; start with HTTP/FHIR, PostgreSQL, S3, MLLP, Kafka)
  - W2-7: HTTP/FHIR, SFTP, Kafka receivers

### Phase 2: Integration and chaos (Week 3–4)

- **W2-8:** Sync-reply (depends on W2-4 batched-commit path)
- **W2-9:** Full conformance and chaos suite (depends on all above)

---

## Critical path and risks

### Critical dependencies
1. **W2-1 (HL7 parser)** blocks W2-2, W2-3, W2-5 (validation step)
2. **W2-4 (Retry/DLQ spike)** blocks W2-8 (sync-reply requires batched-commit)
3. **W2-3 (Mappings)** and **W2-6 (Loaders)** block W2-9 (conformance)

### Known risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| HL7 parser performance disappoints on real data | Medium | High | Phase 0 S1 showed 2,446 msg/s/core with `python-hl7`; watch for regressions. Run Linux re-run early (approved as Phase 1 CI task). |
| Retry-topic spike finds complexity, extends task | Medium | Medium | Timebox to 2–3 days. If mechanism is complex, defer tiers to Phase 2 and ship `blocking` default. Document as a follow-up spike. |
| Claim-check left for later; not a W2 task | N/A | Medium | Marked for Phase 2 or post-MVP. Loaders may start with a size limit and reject oversized payloads with a clear error. |
| FHIR helpers' design is not settled | Low | Medium | Architect approves `ctx.fhir` and `ctx.terminology` interface early (W2-2 kickoff). Consumers code against fakes; QA adds contract tests. |
| Sync-reply requires deeper batched-commit work than spike showed | Medium | Medium | Spike S3 used a stand-in worker; the real pipeline worker path may differ. Run a prototype on real worker first; S5's note to S3 has the design input. |
| Duplicate test coverage is incomplete | Low | High | **Every loader must have a passing test with a real duplicate (idempotency key collision or manual replay).** QA signs off on per-loader tests before W2-9. |

---

## Success criteria and gate

### Exit criteria for Wave 2

1. **An HL7 ADT flow to a FHIR server and to ClickHouse works end to end from a YAML channel.**
   - Verify: channel spec in `samples/channels/`, test sends synthetic ADT via MLLP, observes FHIR resource created and ClickHouse row inserted.

2. **Ordered channel preserves per-key order under chaos tests.**
   - Verify: `pytest -m chaos` tests worker kill, rebalance, and destination outage; ordering violations are zero.
   - **Fault injection:** kills must fire (use the S5 pattern: fail if fault does not fire).

3. **Every loader has a passing duplicate-delivery test.**
   - Verify: `tests/e2e/test_loader_idempotency.py` or similar covers all loaders: HTTP/FHIR, PostgreSQL, ClickHouse, SFTP, S3, MLLP, Kafka.
   - **Per-loader test:** intentionally deliver the same message twice; system handles it without duplication or error.

4. **Throughput: report msg/s per core and ACK p99 against Phase 0 numbers.**
   - Verify: `pytest -m bench` produces a report comparing against S1 and S2 baselines.
   - **Expectation:** `python-hl7` parser stays at ~2,400 msg/s/core; ACK p99 remains environment-dominated and needs Linux re-run to settle.

### Additional quality gates

- **`code-reviewer` on the wave diff:** expects zero Critical or High; Medium findings documented as known limitations.
- **`security-reviewer` on HL7 and FHIR handling:** PHI rules enforced; no identifiers in logs or metrics; claim-check size limits documented.
- **Contracts:** no breaking changes; any new headers or events must be approved by `architect`.
- **Type checking:** `mypy --strict` on all new code; `ruff check` and `ruff format` passing.
- **Integration tests:** all pass with testcontainers Kafka, PostgreSQL, ClickHouse; chaos suite runs without timeout.

---

## Resource allocation and ownership

### Lead (1)
- Assign tasks, unblock dependencies, coordinate between tracks
- Run gate report at week 4

### `healthcare-standards-engineer` (1–2)
- W2-1, W2-2, W2-3 in sequence
- If two, can parallelize mapping work (golden files) with FHIR design

### `platform-engineer` (1)
- W2-4: spike and implement retry-topic with delay tiers
- W2-5: filter, enrich, validate, split steps

### `connector-engineer` (1)
- W2-6: all loaders
- W2-7: all receivers
- W2-8: sync-reply with `platform-engineer`

### `qa-engineer` (1)
- W2-9: conformance and chaos suite
- Coordinate per-loader idempotency tests with `connector-engineer`
- Run benchmarks against Phase 0 baseline

### `architect` (as needed)
- Contract review and approval (W2-3 FHIR mappings may need envelope changes)
- Spike decisions (retry-topic, claim-check)

---

## Pre-wave checklist (lead)

- [ ] Human approves Phase 1 scope and Linux re-run (already done)
- [ ] Create branch `wave/1-2`
- [ ] Verify all W1 tasks merged and CI green on `main`
- [ ] Read Phase 0 DECISION.md and spike reports S1–S6
- [ ] Confirm team: assign 5 people or defer task until full team available
- [ ] Architect publishes `ctx.fhir` skeleton for consumer code (before W2-2 starts)
- [ ] ClickHouse instance available in Compose (from W1-7)
- [ ] Run `/run-phase 1 wave-2` (if skill exists)

---

## In-wave checkpoints

### End of Week 1
- W2-1 complete; W2-2 design approved by architect
- W2-4 spike done; retry-topic mechanism decided (implement or defer tiers)
- W2-6 and W2-7 HTTP loaders and receivers landing (foundation for later)

### End of Week 2
- W2-2 and W2-3 complete
- W2-4, W2-5, W2-6, W2-7 in flight; no blockers found
- Conformance suite scaffolded; first two flows tested (ADT → ClickHouse, ADT → FHIR)

### End of Week 3
- W2-8 prototype or spike landed
- All loaders and receivers drafted
- Chaos test scenarios defined; no flaky tests

### End of Week 4 (gate)
- All 9 tasks complete
- Exit criteria 1–4 verified locally
- Gate report filed; code and security review complete

---

## Notes on decisions and scope

### `retry-topic` spike
- **Current state:** S5 did not implement or measure delay tiers.
- **Decision point:** if spike shows tiers are complex (multitenancy, state machine, operator burden), ship W2-4 with `blocking` default only and defer tiers to W3 or post-MVP.
- **Owner:** `platform-engineer` during task W2-4.
- **Done when:** `pytest -m integration tests/retry` passes; duplicate rate is reported; architecture doc is written.

### Claim-check
- **Current state:** untested, largest PHI store in the design.
- **Decision point:** defer to Phase 2 (after MVP). Loaders may enforce a size limit (e.g., 100 KB) and reject over-limit with a clear error for now.
- **Owner:** decision by `architect` + `platform-engineer` before W2-6; implement as a follow-up spike.

### ClickHouse loader
- **Current state:** not yet built.
- **Decision point:** is ClickHouse available in the dev Compose stack from W1-7?
- **If yes:** W2-6 includes it; `qa-engineer` adds conformance test (ADT → FHIR → ClickHouse).
- **If no:** land HTTP/FHIR, PostgreSQL, S3 first; ClickHouse as a quick W2 follow-up after Compose is updated.

### FHIR interfaces
- **Current state:** in spike S1, used in W2-2 and W2-3.
- **Design point:** `ctx.fhir` and `ctx.terminology` are SDK public APIs; changes must go through `architect`.
- **Process:** `healthcare-standards-engineer` proposes, `architect` reviews and publishes skeleton, QA writes consumer tests.

---

## Integration with Wave 1 and Wave 3

### From Wave 1
- Consumes: SDK, runtime, MLLP receiver, HTTP loader, Compose stack, conformance test framework
- If W1 tasks were incomplete, wave 2 is blocked (e.g., if Compose lacks ClickHouse, W2-9 cannot test ClickHouse loader)

### To Wave 3
- Produces: full connector set, chaos suite, pipeline infrastructure
- Wave 3 depends on all W2 deliverables to build archive, control plane, and ops (W3-2, W3-4, W3-9)

### Test handoff
- W2-9 produces conformance and chaos suite
- W3-9 extends into archive-specific tests (reprocess, replay) and performance benchmarks

---

## Decision log

| Date | Decision | Status |
|---|---|---|
| 2026-09-23 | Wave 2 plan drafted | Pending lead review |
| 2026-09-20 | Partition default 3, retry-topic as spike approved | Adopted in PHASE-0-DECISION.md |

