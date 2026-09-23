# Wave 2 Lead Checklist

**Purpose:** Everything a lead needs to start and run Wave 2 successfully.

**Use this:** Before the wave kicks off and at each weekly checkpoint.

---

## Pre-wave (Week −1)

### Approvals and gates
- [ ] Wave 1 CI passes; gate report filed and approved
- [ ] Human approves Phase 1 scope changes (retry-topic and claim-check as spikes)
- [ ] Human approves Linux re-run of S1/S2 as Phase 1 CI task
- [ ] Architecture invariants 1–10 confirmed unchanged

### Preparation
- [ ] Create branch `wave/1-2` from `main`
- [ ] Assign 5 team members: `platform-engineer`, `connector-engineer`, `healthcare-standards-engineer`, `qa-engineer`, plus `architect` (as-needed)
- [ ] Schedule 4 weekly team syncs + 1 gate session
- [ ] Set up task tracking (TaskCreate or similar)

### Technical setup
- [ ] Wave 1 branch merged to `main` and CI green
- [ ] All `W1-*` deliverables deployed locally: SDK, runtime, MLLP receiver, HTTP loader, Compose stack
- [ ] Compose stack includes: Kafka, PostgreSQL, ClickHouse (verify ClickHouse was added in W1-7)
- [ ] ClickHouse version documented (must support `FHIR` library or we need workarounds)
- [ ] Test framework ready: `pytest -m integration` and `pytest -m chaos` run locally

### Architecture
- [ ] Read `docs/spikes/PHASE-0-DECISION.md` section 8 (Phase 1 scope changes)
- [ ] Read ADR-001, ADR-002, ADR-003 (latest versions with Phase 0 updates)
- [ ] Spike S7 (retry-topic) and S8 (claim-check) owners assigned
- [ ] `ctx.fhir` and `ctx.terminology` interface skeleton prepared by architect (for W2-2)

---

## Week 1 checkpoint (Days 1–5)

### HL7 and FHIR track
- [ ] W2-1 (HL7 parser): `healthcare-standards-engineer` has completed lazy parser, serializer, ACK builder
  - Verify: Hypothesis round-trip tests pass
  - Verify: conformance corpus has at least 10 ADT samples
- [ ] W2-2 design: `ctx.fhir` interface published by architect
  - Verify: skeleton is in `tributary-sdk` or `tributary-fhir`
  - Verify: `healthcare-standards-engineer` can start W2-2 immediately after W2-1

### Platform and connectors track
- [ ] S7 (Retry-topic) spike kick-off: `platform-engineer` starts fault-injection testing
  - Verify: spike plan understood; testbed set up
  - Checkpoint: single-topic design question answered by day 3
- [ ] W2-4: `platform-engineer` has architecture, design reviewed
- [ ] W2-5, W2-6, W2-7: owners have task breakdown and no blockers
  - Verify: no architecture questions blocking W2-5 (filter/enrich/validate steps)
  - Verify: HTTP and PostgreSQL loaders have skeleton and tests

### Quality
- [ ] No regressions in W1 tests
- [ ] Type checking, lint, format all pass on `wave/1-2`
- [ ] First E2E draft: synthetic ADT → ClickHouse (verify schema exists)

### Red flags
- [ ] ❌ Spike S7 blocked on design question
  - Action: escalate to architect; unblock by day 3
- [ ] ❌ W2-1 parser performance disappoints
  - Action: profile and decide: optimize or escalate to Linux re-run task
- [ ] ❌ ClickHouse not in Compose or version incompatible
  - Action: add as W2-0 or descope ClickHouse from MVP

---

## Week 2 checkpoint (Days 6–10)

### Deliverables
- [ ] W2-1 complete (HL7 parser)
- [ ] W2-2 in flight (FHIR interfaces)
- [ ] W2-3 started (ADT and ORU mappings)
  - Verify: golden files for at least 3 ADT subtypes (A01, A03, A08)
  - Verify: mapping rule comments reference source (mapping spec or standard)
- [ ] W2-4 design + S7 spike report ready
  - Verify: retry-topic topology decided (single or per-tier)
  - Verify: spike tested and no critical gaps found
- [ ] W2-5 filter/enrich/validate steps landed
- [ ] W2-6 HTTP/FHIR and PostgreSQL loaders drafted
- [ ] W2-7 HTTP/FHIR receiver drafted

### Integration checkpoint
- [ ] First golden-file test passing: ADT A01 → FHIR Bundle
  - Verify: in `tests/conformance/test_adt_a01_to_fhir.py` or similar
- [ ] ClickHouse integration: HL7 ADT row inserted
  - Verify: schema defined; first conformance test running

### Code quality
- [ ] `mypy --strict packages` passes
- [ ] `ruff check .` and `ruff format --check .` pass
- [ ] No new Critical or High findings from spot-check reviews

### Red flags
- [ ] ❌ W2-1 or W2-2 delayed by architecture questions
  - Action: architect convene with `healthcare-standards-engineer`; unblock by day 7
- [ ] ❌ S7 spike reveals complexity; tiers need more work
  - Action: decide: implement minimal tiers or defer to W3; document in ADR-003
- [ ] ❌ FHIR mapping rule comments missing
  - Action: require before W2-3 completion; enforces mapping traceability

---

## Week 3 checkpoint (Days 11–15)

### Deliverables
- [ ] W2-2 complete (FHIR interfaces)
- [ ] W2-3 complete (ADT and ORU mappings, golden files)
- [ ] W2-4 retry-topic implementation landed
  - Verify: `pytest -m integration tests/retry` passes
  - Verify: duplicate rate reported (expect ≤ 0.5% under fault)
- [ ] W2-5 complete (filter, enrich, validate, split steps)
- [ ] W2-6 all loaders drafted: FHIR, PostgreSQL, ClickHouse, SFTP, S3, MLLP, Kafka
  - Verify: idempotency key documented for each loader
  - Verify: duplicate-delivery test scaffolded for each
- [ ] W2-7 all receivers drafted: HTTP/FHIR, SFTP, Kafka
- [ ] W2-8 sync-reply prototype running (may be incomplete)

### Chaos readiness
- [ ] Chaos test framework ready: broker kill, partition rebalance, destination outage
  - Verify: `pytest -m chaos` runs and produces a report
- [ ] First chaos scenario: worker kill, verify order preserved

### Red flags
- [ ] ❌ W2-3 mappings incomplete or untested
  - Action: prioritize; gate requires full coverage (ADT A01, A03, A08, ORU R01)
- [ ] ❌ Loaders lack duplicate-delivery tests
  - Action: QA and owner pair for each loader; weekly sync on test coverage
- [ ] ❌ S8 claim-check spike blocked
  - Action: scope decision now: implement in Wave 2 or defer; update plan

---

## Week 4 (Final week: Days 16–20)

### Completeness
- [ ] All 9 W2 tasks (`W2-1` through `W2-9`) have code in `wave/1-2`
- [ ] All tasks have tests passing locally
- [ ] No `TODO` or `FIXME` comments that block exit criteria

### Exit criteria verification
- [ ] **Criterion 1:** ADT flow (MLLP → Kafka → HTTP/FHIR and ClickHouse) works end to end
  - Verify: channel spec in `samples/channels/adt_to_fhir_and_ch.yml` or similar
  - Verify: E2E test flows synthetic ADT and observes both outputs
  - Verify: no loss, correct FHIR resource created, ClickHouse row inserted

- [ ] **Criterion 2:** Ordered channel preserves order under chaos
  - Verify: `pytest -m chaos tests/chaos/test_ordering.py` passes, 0 order violations
  - Verify: chaos log shows faults fired (broker kill, rebalance)

- [ ] **Criterion 3:** Every loader has duplicate-delivery test
  - Verify: test for HTTP/FHIR, PostgreSQL, ClickHouse, SFTP, S3, MLLP, Kafka
  - Verify: each test intentionally delivers twice; system handles harmlessly

- [ ] **Criterion 4:** Throughput report vs. Phase 0 baseline
  - Verify: `pytest -m bench` produces report
  - Report: `python-hl7` parser msg/s/core, ACK p99, end-to-end latency
  - Comparison: vs. S1/S2 baselines; note Linux re-run pending

### Code review
- [ ] Run `code-reviewer` on cumulative `wave/1-2` diff vs. `main`
  - Gate: zero Critical, resolve all High, document Medium as known limitations
- [ ] Run `security-reviewer` on HL7/FHIR and PHI handling
  - Gate: zero Critical, resolve all High
  - Verify: no identifiers in logs/metrics; claim-check plan documented

### Integration tests
- [ ] `pytest -m integration` passes (all 6+ integration tests, no timeout)
- [ ] `pytest -m chaos` passes (no flaky tests; faults actually fire)
- [ ] `tests/conformance` passes (at least 15+ channel-spec examples)

### Documentation
- [ ] Mapping rules have source citations (not free-text)
- [ ] Idempotency section exists for every loader (how and where `tb-msg-id` is used)
- [ ] Retry-topic documentation updated in ADR-003
- [ ] If claim-check implemented: lifecycle and size limits documented

### Gate report
- [ ] File `docs/agent-team/gates/phase-1-wave-2.md`
  - Executive summary (deliverables, code/security review status)
  - Exit criteria verdict (all 4 PASS or explicit reason if not)
  - Risks carried into Wave 3 (if any)
  - Decisions made (retry-topic topology, claim-check scope, etc.)
- [ ] Architect approves gate report
- [ ] Recommendation: **GO** or **GO WITH CONDITIONS** (and what conditions)

---

## Escalation paths

### Blocker: Architecture decision
- **Escalate to:** architect
- **Examples:** W2-2 `ctx.fhir` design, retry-topic topology, claim-check implementation scope
- **SLA:** 24 hours

### Blocker: Contract change needed
- **Escalate to:** architect
- **Examples:** new headers for FHIR metadata, claim-check URIs, new topic-defaults
- **Process:** propose exact diff, wait for approval, version contract, notify dependents
- **SLA:** 48 hours

### Blocker: Test or performance regression
- **Escalate to:** lead + affected owner
- **Examples:** Linux re-run of S1/S2 needed early, chaos test timeout
- **Decision:** fix or descope from exit criteria
- **SLA:** 24 hours

### Blocker: Resource unavailable
- **Escalate to:** lead + human
- **Examples:** ClickHouse not in Compose, team member unavailable, CI runner time
- **Decision:** adjust scope or timeline
- **SLA:** 48 hours

---

## Weekly standup template

**Lead runs this every Friday (or mid-week):**

```
## Wave 2 standup – Week [N]

### On track
- W2-1 (HL7): [owner] → [% complete]
- W2-2 (FHIR): [owner] → [% complete]
- ...

### At risk (yellow)
- [Task]: [reason]
  → Action: [what changes, by when]

### Blocked (red)
- [Task]: [reason]
  → Escalation: [to whom, decision needed by when]

### Next week's priorities
1. [Task]
2. [Task]
3. [Task]

### Code/test stats
- Lines of code: +[N], −[M]
- Tests: [N] passing, [M] pending
- Type check: mypy --strict [N] files, [M] errors (if any)
- Integration test time: [Ns] (target: <300s)
```

---

## Post-wave (Gate week)

### Gate report
- [ ] Drafted by lead with input from all owners
- [ ] Submitted to architect for review
- [ ] All exit criteria explicitly verified (not assumed)
- [ ] Known limitations and follow-ups documented

### Decision handoff
- [ ] Architect approves: **GO** or **GO WITH CONDITIONS**
- [ ] If conditions, they are explicitly scoped (e.g., "Linux re-run of S1 before Wave 3")
- [ ] Wave 3 lead is assigned and briefed

### Cleanup
- [ ] `wave/1-2` is ready to merge (human push and PR review)
- [ ] No stray commits or unfinished branches
- [ ] Task list is closed or carry-over tasks documented

