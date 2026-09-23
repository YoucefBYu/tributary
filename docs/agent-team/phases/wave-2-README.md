# Wave 2: Pipelines and Delivery — Planning Summary

**Status:** Ready to start · **Last updated:** 2026-09-23

This directory contains all planning documents for Wave 2 of Phase 1. Start here for an overview; detailed planning is in the linked documents.

---

## Quick facts

| Attribute | Value |
|---|---|
| **Phase** | Phase 1 (MVP) |
| **Wave** | 2 |
| **Purpose** | Real HL7 and FHIR capability; full connector set (loaders + receivers) |
| **Team size** | 5 people |
| **Duration** | 3–4 weeks (1 wave per session) |
| **Branch** | `wave/1-2` (to be created by lead) |
| **Prerequisite** | Wave 1 complete and merged to `main`; Phase 0 human decisions approved |
| **Exit gate** | 4 criteria; code and security review; zero Critical/High findings |

---

## What we're building

### From Wave 1 to Wave 2

Wave 1 proved the architecture with an end-to-end MLLP → Kafka → HTTP thread. Wave 2 adds:

- **HL7 v2 parsing and transformation** (lazy parser, ACK builder, round-trip guarantee)
- **FHIR helpers and reference mappings** (ADT and ORU to FHIR, golden files)
- **Full connector set:**
  - **Receivers:** HTTP/FHIR, SFTP poller, Kafka source (plus MLLP from W1)
  - **Loaders:** FHIR REST, PostgreSQL, ClickHouse, SFTP, S3, MLLP client, Kafka (plus HTTP from W1)
  - **All with idempotency keys and duplicate-delivery tests**
- **Advanced runtime:** retry-topic with delay tiers, DLQ, transactional Kafka-to-Kafka, sync-reply mode
- **Pipeline steps:** filter, enrich, validate, split (with per-step metrics)
- **Chaos testing:** worker kill, rebalance, destination outage; order assertions

### Exit criteria

1. ✅ An HL7 ADT flow to FHIR server and ClickHouse works end to end
2. ✅ Ordered channel preserves per-key order under chaos tests (0 order violations)
3. ✅ Every loader has a passing duplicate-delivery test
4. ✅ Throughput report: msg/s/core and ACK p99 vs. Phase 0 baseline

---

## Key decisions from Phase 0

Wave 2 applies these scope changes from `PHASE-0-DECISION.md` section 8:

| Decision | Impact |
|---|---|
| **Partition default: 3 (not 12)** | Dramatically simplifies cluster; enables 1,000+ channels without topic explosion |
| **Retry-topic is a pre-wave spike** | Don't guess; measure delay precision and ordering before implementation (S7) |
| **Claim-check is a post-MVP spike** | Largest PHI store untested; defer to Wave 3; loaders reject oversized payloads for now (S8) |
| **Receiver per-connection isolation required** | S2 found 7.5–12.7s stall on broker loss; this is a design requirement, not optional |
| **Sync-reply on batched-commit path** | Fail-fast + DLQ for retry; document timeout as "committed, outcome unknown" |
| **Test pattern carryover** | Assert that faults fire; keep mutation controls for every headline claim |

---

## Documents in this directory

### For leads and planning
- **[wave-2-plan.md](wave-2-plan.md)** — Full task breakdown, dependencies, sequencing, risks, resource allocation, gate criteria
- **[wave-2-lead-checklist.md](wave-2-lead-checklist.md)** — Weekly checklist, escalation paths, standup template
- **[wave-2-spikes.md](wave-2-spikes.md)** — Design for S7 (retry-topic) and S8 (claim-check) pre-implementation spikes

### For team members
- See above documents for your specific task (e.g., W2-1, W2-6)
- Report blockers early; escalate to lead within 24 hours

---

## Timeline at a glance

### Week 1 (Days 1–5): Foundations
- **Healthcare team:** HL7 parser (W2-1), FHIR design kick-off (W2-2)
- **Platform:** Retry-topic spike S7, architecture review
- **Connectors:** HTTP/FHIR and PostgreSQL loaders drafted
- **QA:** Test framework ready; first E2E draft (ADT → ClickHouse)

### Week 2 (Days 6–10): Integration
- **Healthcare team:** FHIR helpers (W2-2), ADT/ORU mappings (W2-3)
- **Platform:** Retry-topic implemented (W2-4), filter/enrich/validate (W2-5)
- **Connectors:** All loaders drafted; HTTP/FHIR receiver drafted
- **Checkpoint:** First golden-file test passing; ClickHouse integration verified

### Week 3 (Days 11–15): Chaos and completion
- **All teams:** W2-6, W2-7, W2-8 landing; sync-reply prototype
- **QA:** Chaos framework ready; first broker-kill scenario
- **Red flags:** Duplicate-delivery tests must be complete by end of week

### Week 4 (Days 16–20): Gate
- **All:** Final verification of exit criteria
- **Code review:** cumulative `wave/1-2` diff reviewed
- **Gate report:** filed, architect approved
- **Recommendation:** GO or GO WITH CONDITIONS

---

## Risks and mitigations

### High impact
| Risk | Mitigation |
|---|---|
| HL7 parser performance disappoints | Phase 0 S1 showed 2,446 msg/s/core; watch for regressions. Linux re-run early (W3 pre-req). |
| Sync-reply requires deeper changes than S3 spike showed | Use real pipeline worker path, not stand-in. S5's note to S3 has the design input. |
| Duplicate-delivery test coverage incomplete | **Every loader must have a test**; QA sign-off by week 3. |

### Medium impact
| Risk | Mitigation |
|---|---|
| Retry-topic spike finds complexity | Timebox to 2–3 days; if tiers are complex, ship blocking-only + note for Wave 3. |
| FHIR interface design is not settled | Architect publishes skeleton early (pre-W2-2). Consumers code against fakes. |
| ClickHouse loader not ready | Scope decision early; add to W2-0 or descope from MVP. |

---

## What's *not* in Wave 2

- **Claim-check:** deferred to Wave 3 or post-MVP (S8 spike will validate design)
- **Mirth importer:** timeboxed exploratory effort (post-MVP or Wave 3, contingent on corpus)
- **Archive/message browser:** Wave 3 (W3-2, W3-3, W3-6)
- **API and CLI:** Wave 3 (W3-4, W3-7)
- **UI:** Wave 4 (W4-1 through W4-4)
- **OIE migration spike:** Phase 2 (may need updated scope based on Mirth corpus)

---

## Phase 0 follow-ups in Wave 2

| Phase 0 item | Wave 2 action |
|---|---|
| **S1 re-run on Linux** | Approved as Phase 1 CI task; run before receiver hardening (Wave 3 pre-req) |
| **S2 ACK p99 unmet (33–125 ms vs 50 ms)** | Carry as a known limitation; Linux re-run needed to determine if environment-dominated |
| **S4 heap scaling inconclusive** | Carry as a limitation; re-measure on real broker memory post-MVP |
| **S5 `retry-topic` untested** | S7 spike in Wave 2; implement based on findings |
| **S6 Mirth corpus missing** | Escalation item; no agent can decide; impacts migration estimate |

---

## Decisions awaiting human input (Phase 0, not Wave 2 specific)

These are already approved but listed for completeness:

- ✅ Audit retention policy (blocks `_tb.<env>.audit` contract; Phase 3+)
- ✅ Mirth corpus (blocks S6 estimate; product decision)
- ✅ Lite mode RF=1 documentation (Wave 1 already ships with startup warning)
- ✅ Keyed-hash mechanism (blocks per-patient message browser; ADR-004, Phase 2+)

---

## How to use this plan

### For the lead
1. **Read:** wave-2-plan.md (full context)
2. **Use:** wave-2-lead-checklist.md (weekly tracking)
3. **Escalate:** use checklist's escalation paths for blockers
4. **Gate:** use checklist's "Week 4 (Final week)" section to verify exit criteria

### For team members
1. **Find your task:** by role in wave-2-plan.md table
2. **Understand dependencies:** see dependency graph in wave-2-plan.md
3. **Blockers?** Message the lead immediately (24-hour SLA)
4. **Design questions?** Escalate to architect (48-hour SLA)

### For the architect
1. **Review:** wave-2-plan.md pre-wave setup section
2. **Publish:** `ctx.fhir` and `ctx.terminology` skeleton before W2-2 starts
3. **Spikes:** S7 (retry-topic) and S8 (claim-check) decisions by week 2
4. **Gate report:** review and approve by week 4

---

## Related documents

- **Phase plan:** `docs/agent-team/phases/phase-1.md`
- **Phase 0 decision:** `docs/spikes/PHASE-0-DECISION.md` (scope, measurements, risks)
- **ADRs:** `docs/adr/ADR-001.md` (architecture), `ADR-002.md` (topics), `ADR-003.md` (retry/DLQ)
- **Wave 1 gate:** `docs/agent-team/gates/phase-1-wave-1.md`

---

## Status tracking

- **Plan created:** 2026-09-23
- **Ready to kick off:** upon lead assignment and team assembly
- **Branch:** awaiting creation by lead
- **First standup:** 24 hours after wave starts

