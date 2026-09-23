# Wave 2 Planning Review Request

**To:** Architect, Team Lead

**From:** Claude Code · **Date:** 2026-09-24 · **Commit:** `434ffca`

**Status:** Ready for review before wave kick-off

---

## Summary

Wave 2 planning is complete and committed. Four documents provide a comprehensive plan for "Pipelines and delivery" — real HL7/FHIR capability plus the full connector set for the MVP.

**Total:** 9 tasks (W2-1 through W2-9), 35 person-days, 3–4 weeks, team of 5.

---

## What to review

### Quick entry point (5 min)
**[wave-2-README.md](wave-2-README.md)** — Overview, timeline, risks, key decisions

### For the architect (20 min)
**[wave-2-plan.md](wave-2-plan.md)** — Sections:
- **Pre-wave setup:** Phase 0 decisions embedded; all approvals already in place ✅
- **Scope changes (section "Scope changes from Phase 0"):** retry-topic and claim-check as spikes; partition defaults; receiver isolation
- **Task breakdown:** review W2-2 (FHIR interface design), W2-3 (mappings), contract change points
- **Critical dependencies:** `ctx.fhir` skeleton needed before W2-2 starts

**[wave-2-spikes.md](wave-2-spikes.md)** — Two pre-implementation research projects:
- **S7 Retry-topic:** 2–3 days during W2-4; decides topology (single vs. per-tier); updates ADR-003
- **S8 Claim-check:** 2–3 days (parallel or Wave 3); storage abstraction; lifecycle policy; MVP gate decision

### For the lead (25 min)
**[wave-2-plan.md](wave-2-plan.md)** — Sections:
- **Sequencing strategy (section "Sequencing strategy"):** parallelism across tracks, dependencies, week-by-week phases
- **Critical path and risks:** 10 risks identified with mitigations
- **Resource allocation:** task owners, estimates, lead responsibilities

**[wave-2-lead-checklist.md](wave-2-lead-checklist.md)** — Operational guide:
- **Pre-wave:** technical setup, approvals, architecture prep
- **Weeks 1–4:** checkpoint template with red flags and escalation paths
- **Exit criteria verification:** explicit tests for all 4 criteria
- **Weekly standup template:** ready to use

---

## Key decisions awaiting your review/approval

### Architect decisions

1. **FHIR interface skeleton (ctx.fhir, ctx.terminology)**
   - W2-2 cannot start until published
   - Recommend: simple skeleton in `tributary-fhir` package; `healthcare-standards-engineer` extends
   - Timeline: before wave starts or day 1

2. **Retry-topic topology decision (S7 outcome)**
   - Decision point: end of week 1
   - Single `retry.<destination>` topic + `tb-retry-not-before` header vs. per-tier topics
   - Recommend: spike S7 will inform; plan assumes single-topic design works

3. **Claim-check scope: Wave 2 vs. Wave 3**
   - S8 spike validates design; implementation is separate
   - Recommend: spike in Wave 2 (parallel with other work), implement in Wave 3
   - MVP gate: loaders reject oversized payloads (>10 MB) with clear error

4. **Contract changes expected in Wave 2**
   - None known, but:
     - W2-3 (FHIR mappings) may need envelope changes (documented by `healthcare-standards-engineer`)
     - S7 (retry-topic) may need new headers (documented in spike)
     - S8 (claim-check) may need new topic conventions (documented in spike)
   - Process: propose to architect (wave-2-plan.md section "Scope changes" lists points of change)

### Lead decisions

1. **Team assembly**
   - Confirm 5 people available: `platform-engineer`, `connector-engineer`, `healthcare-standards-engineer`, `qa-engineer`, plus architect as-needed
   - Recommend: assign by 2026-09-25; kickoff by 2026-09-26

2. **Pre-wave technical setup**
   - ClickHouse integration: is it in Wave 1 Compose stack?
     - If no: add as W2-0 task or descope from MVP
     - If yes: verify version and FHIR library support
   - Verify all W1 outputs merged to `main` and CI green

3. **Spike ownership**
   - S7 (Retry-topic): assign to `platform-engineer` during W2-4
   - S8 (Claim-check): assign to `platform-engineer` or `archive-engineer`
   - Recommend: S7 mandatory, S8 optional Wave 2 (can be early Wave 3)

4. **Linux re-run scheduling**
   - Phase 0 approved S1/S2 re-run as first Phase 1 CI task
   - Should this run in parallel with Wave 2, or as Wave 2 pre-req?
   - Recommend: run in parallel (separate CI runner) or early Wave 3 before hardening

5. **Gate report template**
   - Use wave-2-lead-checklist.md "Week 4 (Final week)" section to verify exit criteria
   - File report as `docs/agent-team/gates/phase-1-wave-2.md`
   - Recommendation: GO or GO WITH CONDITIONS (and what conditions)

---

## Confidence level

**High confidence in:**
- Task breakdown accuracy (based on Phase 1 spec, Phase 0 measurements)
- Sequencing and dependencies (DAG validated)
- Estimate ranges (3–4 weeks for 5-person team, 35 person-days)
- Exit criteria (directly from phase-1.md wave-2 spec)
- Risk mitigation (informed by Phase 0 spikes)

**Assumptions requiring confirmation:**
- ClickHouse is in Compose (W1-7); if not, scope changes
- `ctx.fhir` design is simple enough for W2-2 to prototype in parallel
- Retry-topic spike (S7) doesn't reveal show-stoppers (plan assumes 2–3 days)
- Team capacity is stable; no mid-wave departures

---

## What's NOT planned yet

These are deferred per Phase 0 decisions and wave-2-plan.md section "What's *not* in Wave 2":

- **Archive/message browser:** Wave 3 (W3-2, W3-3, W3-6)
- **Control plane API/CLI:** Wave 3 (W3-4, W3-7)
- **UI:** Wave 4 (W4-1 through W4-4)
- **Mirth importer:** timeboxed post-MVP (contingent on corpus)
- **Claim-check implementation:** Wave 3 or post-MVP (S8 spike validates design)
- **Linux re-run of S1/S2:** Phase 1 CI task (parallel or pre-Wave 3)

---

## Requests for review

### Architect, please verify:
- [ ] `ctx.fhir` and `ctx.terminology` interface design is covered (W2-2)
- [ ] Contract changes are on the radar (W2-3, S7, S8)
- [ ] Retry-topic and claim-check spike scopes are reasonable (S7, S8)
- [ ] Phase 0 decisions are correctly embedded (especially partition defaults, receiver isolation)
- [ ] Any concerns with task breakdown or architecture risks?

### Lead, please verify:
- [ ] Sequencing makes sense; dependencies are clear
- [ ] Checklist is practical for weekly standups
- [ ] Team size (5 people) is achievable
- [ ] Timeline (3–4 weeks) is realistic for your context
- [ ] Escalation paths (24–48h SLA) are acceptable
- [ ] Any concerns with resource allocation or exit criteria?

---

## Next steps

### If approved:
1. Architect: publish `ctx.fhir` skeleton (by wave kickoff)
2. Lead: confirm team assembly, create `wave/1-2` branch
3. Schedule wave kickoff standup (24 hours after team assignment)
4. Use wave-2-lead-checklist.md for weekly tracking

### If changes needed:
- Comment on specific sections or tasks in the planning docs
- Escalate blockers to me (claude) for adjustment
- No changes are expected to be large; scope is locked by phase-1.md

### If decision is deferred:
- Flag it here; I'll add to "Open decisions" section in wave-2-README.md
- Timeline impact should be assessed before wave starts

---

## Documents location

All documents in: `docs/agent-team/phases/`

- `wave-2-README.md` — Entry point
- `wave-2-plan.md` — Full plan
- `wave-2-spikes.md` — Spike designs
- `wave-2-lead-checklist.md` — Operations guide

Commit: `434ffca` (see git log for details)

---

## Review deadline

**Recommendation:** Approve or request changes by 2026-09-25 end-of-day so wave can kick off by 2026-09-26.

If major rework is needed, timeline adjusts; flag early.

