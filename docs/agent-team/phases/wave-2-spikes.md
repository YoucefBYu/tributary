# Wave 2 Spikes: Retry-topic and Claim-check

**Status:** Pre-wave design · **Date:** 2026-09-23 · **Lead:** (architect to coordinate)

**Scope:** Two small spikes during Wave 2 to validate mechanisms before full implementation. Both are blocking product decisions, not nice-to-have.

---

## Spike 1: Retry-topic with delay tiers

**Owner:** `platform-engineer` (during W2-4)

**Duration:** 2–3 days (embedded in W2-4 task)

**Background:** Phase 0 spike S5 left retry-topic and delay tiers unimplemented. ADR-003 D2 says "`retry-topic` may not be selected until it is spiked." The design is specified but not measured. W2-4 must prove it works before a destination may select it in a channel spec.

### Questions this spike answers

1. **Architecture:** does a single `retry.<destination>` topic work, or do we need per-tier topics?
   - Single topic, per-record `tb-retry-not-before` header: simpler, but what about consumer lag under load?
   - Per-tier topics: cleaner lag visibility, but more topic management and client complexity.

2. **Delay precision:** can we achieve the target delay (e.g., 5 s, 30 s, 5 min) with reasonable precision?
   - Mechanism: a retry worker polls `retry.<destination>`, checks `tb-retry-not-before`, sleeps if needed, then produces to the destination.
   - Question: does `poll_timeout_ms` dominate the delay? Phase 0 S5 found "155 ms of each 169 ms retry cycle was poll_timeout_s".

3. **Poison handling:** what happens to a message that retries 3 times and still fails?
   - Expected: move to DLQ with a `tb-error-code` of `POISON` or `MAX_RETRIES`.
   - Test: verify dead-letter and offset committed, partition keeps moving.

4. **Ordering:** does `retry-topic` preserve per-key ordering under rebalance?
   - Expected: yes, because the topic is partitioned by key (same as the destination topic).
   - Test: verify order violations are zero under a partition-move scenario.

5. **Duplicate rate:** what is the expected duplicate rate under rebalance or crash?
   - Phase 0 S5 reported 0.10% under `broker_kill` and 0% without a fault.
   - Question: does retry-topic change this? (Likely no, because invariant 4 requires downstream idempotency.)

### Spike design

**Harness:** Extend the S5 fault-injection model to include retry-topic or use the runtime's existing consumer loop with a simple retry processor.

**Test scenarios:**
1. **Baseline:** message fails delivery 3 times, each retry delayed by 5 s, then moves to DLQ.
2. **Rebalance:** partition rebalance mid-retry; verify order preserved and no loss.
3. **Consumer crash:** kill the retry worker mid-retry; verify duplicate rate and on-restart behavior.
4. **Poison loop:** a message that always fails; verify it stops retrying after N attempts and lands in DLQ.
5. **Delay accuracy:** measure actual retry delay vs. configured delay; acceptable variance is ±500 ms.

**Success criteria:**
- Order violations: 0 across 1,000+ test runs.
- Duplicate rate: ≤ 0.5% under broker failure.
- Delay accuracy: measured within ±500 ms for 5 s, 30 s, 5 min targets.
- Poison message reaches DLQ; partition keeps moving.
- Single-topic design decision: does single `retry.<dest>` or per-tier topology make sense?

**Output:**
- Spike report `S7-retry-topic.md` with methodology and results.
- ADR-003 update: decide on topology (single vs. per-tier).
- Prototype retry processor code or reference implementation in `spikes/S7-retry-topic/`.
- Updated `contracts/topics/v0` if new headers or topic conventions are needed.

### Integration into W2-4

- **Days 1–2:** spike runs in parallel with other W2-4 design work.
- **Day 3:** spike complete, decision made; full implementation follows the same schedule.
- **If spike reveals complexity:** defer tiers to W3; ship W2-4 with `blocking` default and a known limitation.

---

## Spike 2: Claim-check for payloads over 1 MB

**Owner:** `platform-engineer` or `archive-engineer` (decision: post-design phase)

**Duration:** 2–3 days (could be parallel with S7 or early W3)

**Background:** Phase 0 identified claim-check as untested and the largest PHI store in the design. Payloads over 1 MB are rare but possible in healthcare (e.g., clinical documents with images). The design is in ADR-001 6.1 and `contracts/envelope/v0`, but no spike has validated the mechanism.

**Current spec:**
- Payloads over 1 MB → claim-check: store in S3/MinIO, emit `tb-claim-check-uri` header instead of body.
- Loader fetches by URI on demand.
- Archive ingests the S3 object directly (bypassing Kafka).

### Questions this spike answers

1. **Storage:** can we use S3/MinIO? What about on-disk storage in lite mode?
   - Expectation: S3 API for production, local file path for dev/lite.
   - Question: is the abstraction clean enough for both?

2. **URI scheme:** should claim-check URIs be opaque object IDs or structured paths?
   - Option A: `s3://bucket/claims/<uuid>` (content-addressed, simple).
   - Option B: `s3://bucket/claims/<msg-id>` (journey-traceable, but msg-id must be in header).
   - Tradeoff: A is simpler; B is more PHI-audit-friendly.

3. **Lifecycle:** when should claim-check objects be deleted?
   - Options: after archive ingestion, after DLQ TTL, after audit TTL, never.
   - Constraint: must not delete while a reprocess/replay is in flight.
   - Question: does this need a separate garbage-collection task or is it part of archive/retention?

4. **Performance:** what is the latency impact of fetching claim-check on-demand?
   - Baseline: Kafka message with embedded 1 MB payload ~10 ms; S3 fetch ~100–500 ms (network-dependent).
   - Question: is the latency acceptable, or should we pre-fetch on ingest?

5. **Durability:** what happens if S3 is unavailable?
   - Expected: receiver blocks or NACKs; loader retries or DLQs.
   - Test: S3 outage during claim-check ingest; verify no message loss (invariant 1).

### Spike design

**Harness:** Compose stack with MinIO; MLLP receiver with a large HL7 payload (synthetic, ≥1 MB); HTTP loader.

**Test scenarios:**
1. **Baseline:** receive 1 MB payload, store claim-check, emit header, load confirms URI validity.
2. **Edge case:** exactly 1 MB payload; just-under 1 MB (no claim-check).
3. **Availability:** S3 unavailable during ingest; verify receiver handles gracefully (NACK or block per spec).
4. **Multiple claims:** same message with two large attachments; verify both claim-check URIs.
5. **Reprocess:** retrieve a message from archive via reprocess; claim-check available during fetch.

**Success criteria:**
- Claim-check correctly emitted for ≥1 MB payloads; omitted for smaller.
- Loader fetches URI and delivers to destination without error.
- S3 outage does not cause acknowledged-message loss (invariant 1 holds).
- Lifecycle plan is documented (when objects are eligible for deletion).
- URI scheme is decided and implemented in both receiver and loader.

**Output:**
- Spike report `S8-claim-check.md` with methodology and results.
- Storage abstraction (interface + MinIO/file implementations).
- Receiver and loader implementations (claim-check detection and fetch).
- Updated `contracts/envelope/v0` if new headers are needed.
- Architecture decision: lifecycle and garbage collection (defer or handle in W3/post-MVP).

### Integration into Wave 2 or Wave 3

**Option A (Wave 2, optional):** if time permits and spike is quick, implement full claim-check in W2-6 (loaders).
- Pros: closes a known risk; full MVP.
- Cons: adds scope to already-full wave.

**Option B (Wave 3 or post-MVP, recommended):** spike in Wave 2, implement after MVP.
- Pros: unblocks wave 2; MVP ships without it; loaders can reject oversized payloads with a clear error.
- Cons: defers a PHI-handling risk.

**Current recommendation:** Spike during Wave 2 (or early Wave 3); defer implementation to post-MVP. Loaders enforce a 10 MB limit and reject over-limit with error `PAYLOAD_TOO_LARGE`. Documented as a Phase 2 task.

---

## Schedule and resource plan

### Spike sequence

| Week | Sprint | Task | Owner | Status | Gate |
|---|---|---|---|---|---|
| W2.1 | Days 1–3 | **S7 Retry-topic** | `platform-engineer` | Parallel with W2-4 design | Spike report, ADR-003 update |
| W2.1–2 | Days 1–6 | **S8 Claim-check** | `platform-engineer` (or `archive-engineer` decision) | Parallel or sequential | Spike report, architecture decision |
| W2.3+ | Days 11+ | Implement based on spike decisions | (to be assigned) | Depends on spike outcome | Full tests, gate criteria |

### Output deliverables

1. **S7 Retry-topic**
   - Spike report: `docs/spikes/S7-retry-topic.md`
   - ADR-003 addendum: topology decision (single or per-tier)
   - Prototype code: `spikes/S7-retry-topic/`

2. **S8 Claim-check**
   - Spike report: `docs/spikes/S8-claim-check.md`
   - Storage abstraction: interface definition, MinIO + file implementations
   - Lifecycle decision: when objects are eligible for deletion
   - Gate decision: implement in Wave 2, Wave 3, or post-MVP

---

## Decision points for the lead

### Before W2-4 starts
- [ ] Assign spike owner(s)
- [ ] Schedule spike window (likely days 1–3 of wave)
- [ ] Confirm S3/MinIO or local file storage available for S8

### After S7 Retry-topic complete
- [ ] Is single-topic design approved? (expected: yes)
- [ ] Can W2-4 implement tiers, or defer to blocking-only + note for W3?
- [ ] Does ADR-003 need an update, or is spike report sufficient?

### After S8 Claim-check complete
- [ ] Implement in Wave 2 (adds ~1–2 days to W2-6) or defer?
- [ ] Lifecycle policy decided: archive-driven or separate GC task?
- [ ] Loaders' size-limit enforcement: 10 MB or negotiate?

---

## Contingency

### If S7 reveals unexpected complexity
- **Action:** defer tiers to W3; ship W2-4 with `blocking` default.
- **Communication:** note in spike report and ADR-003; document as a known limitation in release notes.
- **Wave 2 gate:** exit criteria do not require tiers (only `blocking` policy).

### If S8 reveals S3 unavailability risk
- **Action:** implement local file storage in lite mode; make S3 required for production Compose.
- **Test:** S8 includes both MinIO and file-backed tests.

### If either spike blocks W2-4 or W2-6
- **Lead escalates:** architect + `platform-engineer` convene; decide: extend wave or descope from MVP.

---

## Success criteria for both spikes

- **Spike report filed, approved by architect**
- **Testing methodology documented** (hypothesis, model, or integration test framework)
- **Decisions recorded in ADR-003 or architecture docs**
- **No test flakiness; all runs pass locally at least 3x**
- **Integration plan clear:** which task implements, by when

