# Phase 1, wave 2 — pipelines and delivery

**Owner of this document:** `architect`, consolidating six lead drafts (section 9).
**Authoritative scope:** [`phase-1.md`](phase-1.md), Wave 2 section, plus
[`PHASE-0-DECISION.md`](../../spikes/PHASE-0-DECISION.md) section 8. Where this document and those
disagree, they win and this document is wrong.
**Branch:** `wave/1-2`. **Started:** 2026-09-24, on the human's approval to start wave 2.
**Live status:** [`STATUS.md`](../STATUS.md). This file is the plan, not the tracker.

---

## 1. Starting position — what is actually true on 2026-09-24

| Item | State |
|---|---|
| Wave 1 | **Pushed; CI passed (human-confirmed 2026-09-24); gate conditions closed. NOT merged to `main`** — `main` is still at `2c25448`. Wave 2 builds on the wave 1 branch, not on `main` |
| Phase 0 human decisions | Seven decided 2026-09-21, MPL approval and gate sign-off 2026-09-22, `certifi` 2026-09-23. **Still open:** audit retention, the Mirth corpus, and three governance items (section 7) |
| Contracts | Set 1.0 frozen at the wave 1 gate. **Set 1.1 published 2026-09-24 for wave 2 — unverified until the conformance run** (section 4) |
| Linux re-run of S1/S2 | **Not done.** Approved 2026-09-21 as the first Phase 1 CI task, "before the receiver is built"; the receiver was built in wave 1. `.github/workflows/bench-linux.yml` is a stub that records the environment and does not measure. See 6.1 |
| Compose | Has Kafka (KRaft), ClickHouse and PostgreSQL since W1-7 (gate `phase-1-wave-1.md` section 6) |

---

## 2. Tasks

Exactly the nine in `phase-1.md`. The **Contract / notes** column is what set 1.1 and the wave 1 gate
add; nothing else is added.

| ID | Owner | Deliverable | Verify (from `phase-1.md`) | Contract / notes |
|---|---|---|---|---|
| W2-1 | `healthcare-standards-engineer` | `tributary-hl7v2`: lazy parser, serializer, accessors, ACK builder, round-trip guarantee | Hypothesis round-trip tests; conformance corpus passes | S1 baseline for a lazy parser is **36,322 msg/s/core** (solo host, Windows); `python-hl7` was 2,446. Fixtures set MSH-3 to `TRIBUTARY-SYNTH` |
| W2-2 | `healthcare-standards-engineer` | `tributary-fhir` helpers; `ctx.fhir`, `ctx.terminology` interfaces | Unit tests; interfaces published through `architect` | **The engineer proposes, the architect approves.** Review criteria were sent 2026-09-24 |
| W2-3 | `healthcare-standards-engineer` | ADT A01/A03/A08 and ORU R01 to FHIR, golden files | Golden-file tests; rules cite the mapping source | Must define the identifier system each resource type is conditional on, so every Bundle entry is idempotent (channel-spec README 3.4.2) |
| W2-4 | `platform-engineer` | Retry-topic with delay tiers, DLQ, transactional K2K stage, key partitioning from a spec expression | Fault-injection property tests; duplicate rate reported | Key semantics fixed in channel-spec README 4.3; **one shared evaluator**, receivers call it. `retry-topic` stays refused until the nine criteria in README 4.1.1 are met — "stays refused" is an acceptable outcome |
| W2-5 | `platform-engineer` | Filter, enrich, validate, split steps; per-step timing and tracing | Tests plus metrics visible in Compose | **Plus per-destination steps** (README 3.5, ADR-007 D17), which exit criterion 1 needs |
| W2-6 | `connector-engineer` | Loaders: FHIR REST, PostgreSQL, ClickHouse, SFTP, S3, MLLP client, Kafka — **seven** | Duplicate-delivery test per loader | Shapes in README 3.4.x; what the duplicate test must assert per sink in 5.1. **The SFTP licence is a human decision first** (7); PostgreSQL uses `asyncpg` |
| W2-7 | `connector-engineer` | Receivers: HTTP/FHIR, SFTP poller, Kafka source | Integration test per receiver | Shapes in README 3.2.x. Phase 0 requires **per-connection head-of-line isolation and multi-process receivers** (memo 8.3) |
| W2-8 | `connector-engineer` with `platform-engineer` | Sync-reply mode | Latency and timeout tests | Batched-commit path, fail-fast plus DLQ, `AE` = "committed, outcome unknown" (ADR-003 D9). The `reply` block is a contract proposal still to come |
| W2-9 | `qa-engineer` | Conformance suites; chaos: worker kill, rebalance, destination outage; ordering assertions | `pytest -m chaos` passes with duplicate rates reported | Every chaos check asserts that its fault fired, with a broken-implementation control (Phase 0 lesson) |

**Dependencies that are not in the phase table but are real:** W2-7's per-key receivers need W2-4's
evaluator; W2-6's `fhir` loader needs W2-3's idempotency identifier; exit criterion 1 needs W2-5's
destination steps.

**Estimates.** The lead drafts estimated 35 person-days over 3-4 weeks. Those are planning guesses and
are kept only as that; the phase plan's working unit is one wave per session.

---

## 3. Exit criteria

Verbatim from `phase-1.md`, with the operational reading this wave will be gated on.

| # | Criterion | What counts |
|---|---|---|
| 1 | An HL7 ADT flow to a FHIR server and to ClickHouse works end to end from a YAML channel | The shape is `channel-spec/v0/examples/valid/adt-mllp-to-fhir-clickhouse-mllp.json`: one channel, per-destination steps, `fhir` and `clickhouse` destinations. Observed at a real FHIR test server and a real ClickHouse table, not stubs |
| 2 | Ordered channel preserves per-key order under chaos tests | 0 order violations, **and the fault is shown to have fired** in every scenario |
| 3 | Every loader has a passing duplicate-delivery test | All seven plus HTTP, per the per-sink definitions in channel-spec README 5.1 — a genuine redelivery from an uncommitted offset; ClickHouse asserts a `FINAL` count |
| 4 | Throughput: msg/s per core and ACK p99 against the Phase 0 numbers | **Comparable only if the Linux re-run exists.** Against the Windows-laptop numbers alone the report can be written but proves little; see 6.1 |

---

## 4. Contracts for this wave

Contract set 1.1, published 2026-09-24 — `contracts/CHANGELOG.md` top entry, ADR-007 D10-D17:

- Source types `http`, `sftp`, `kafka` and destination types `fhir`, `postgresql`, `clickhouse`,
  `sftp`, `s3`, `mllp`, `kafka`, each with a params object, security posture and idempotency rules.
- `destinations[].steps`, and `partitionKey` semantics including `record:key`.
- `retry-topic` still refused, with written lift criteria.
- Consumer-group role `receiver` in `topics/v0` 3.6.

**Unverified** until `qa-engineer` runs `validate.py`, `--emit-expectations` and
`pytest tests/conformance`. Contract changes during the wave: message the architect with the exact
diff and the reason; breaking changes need an ADR.

**No other contract change is expected**, with one exception: W2-8's `reply` block.
Specifically, **no new envelope header is planned for W2-3 or the retry spike**: the retry headers
(`tb-retry-not-before`, `tb-attempt`, `tb-destination`) already exist.

---

## 5. Spikes

### 5.1 `retry-topic` — inside W2-4, `platform-engineer`

The mechanism choice is ADR-003 D2: (a) the existing **shared** `<env>.<channel>.retry` topic plus
`tb-retry-not-before`, or (b) a topic per tier. What the spike must show, and what changes when it
passes, are fixed in advance in channel-spec README 4.1.1. Two points the lead drafts had wrong:

- The retry topic is **one per channel, shared across destinations**, not `retry.<destination>`
  (`topics/v0` section 1).
- **`retry-topic` does not preserve order for retried messages.** That is ADR-003 D1 and invariant 5's
  own wording. The spike characterises the reordering; it does not test for its absence.

No pass thresholds were set in advance for delay accuracy or duplicate rate — the drafts' "±500 ms"
and "≤ 0.5%" had no basis. Both are reported as distributions, and lifting the refusal is a decision
made on those numbers.

### 5.2 Claim-check — **not scheduled in wave 2**

`phase-1.md` has no wave 2 task for it, and the Phase 0 memo (section 8.2) says it must be **spiked
before any implementation**. Scheduling it is a lead decision. Until it is built:

- **Every source refuses input over 1,000,000 bytes** (the envelope inline limit; channel-spec 3.2.2).
  There is no loader-side limit, because nothing over the inline limit can reach a loader. The drafts'
  "10 MB" and "100 KB" loader limits were invented and are withdrawn.
- The object-key rule is already decided, by the round-1 security fix: `<env>/<channel>/<yyyy>/<mm>/<dd>/<tb-msg-id>`,
  header `tb-claim-check` (`envelope/v0` section 5). Object lifecycle is ADR-004's, whose scope the
  human approved on 2026-09-21.
- SFTP and HTTP sources will hit the limit more often than MLLP did (documents, PDFs). That is the
  most likely reason this spike gets scheduled.

---

## 6. Carried in from Phase 0 and wave 1

### 6.1 The Linux re-run of S1/S2

Approved by the human on 2026-09-21 as **the first Phase 1 CI task, before the receiver is built**. It
has not been done. `bench-linux.yml` records the environment and locked versions and then prints "not
wired yet: qa-engineer to add the S1 (parser) and S2 (MLLP to Kafka) entry points". It is manual-trigger
only, and `benchmarks/results/` holds no Linux output.

It matters now for three reasons: exit criterion 4 compares against Phase 0 numbers that are all from
one Windows laptop; the 64 MiB receiver queue default is labelled "measure in the Linux S2 re-run";
and S2's 50 ms ACK p99 is "NOT MET as measured, cause unresolved". **A hosted CI runner is not the
"dedicated 3-broker cluster" the memo's recommendation priced.** Whether to pay for dedicated capacity
is a cloud-spend decision, so it goes to the human (section 7).

### 6.2 Wave 1 gate, section 8 items carried to this wave

| Item | Owner |
|---|---|
| Invariant 9 mutation control: a failing event emitter must not affect message flow | `platform-engineer` |
| The ordinary reject path (envelope 2.3.1), with a unit test per placeholder | `platform-engineer` |
| Deploy-time step resolution, with a timeout, keeping the previous spec on failure | `platform-engineer` |
| An alert on `partition_halted` before the MVP (ADR-003 D3) | **Unassigned — lead to assign.** W3-8's Prometheus rules are the natural home; named here so it is not dropped |
| Two log sites bypassing the guarded logger (`receivers/mllp/server.py:205,235`, `loaders/http/loader.py:235`) | `connector-engineer` |
| Type-check scope extended to `.github/scripts/` | `devops-engineer` |
| Invariant 3 chaos coverage | Wave 3, once PostgreSQL and the API exist |

### 6.3 The review lesson

The wave 1 message-loss High was in code that had passed code review, `mypy --strict` and 701 tests:
a library default (`urllib`'s redirect handler) nobody had looked at. Both Phase 0 Highs, both wave 1
Highs and three of four wave 1 contract defects share that shape. Wave 2 adds seven client libraries,
so reviews should spend their effort on **library defaults and transitive edges** — redirects, TLS
verification, retries built into drivers, and what a driver puts in its exception text.

---

## 7. Decisions that are open — not approved

The drafts marked the first three as approved. **They are not.**

| Decision | State | Blocks |
|---|---|---|
| Audit retention (30 d vs a possible six-year duty) | **Open — human** | The `_tb.<env>.audit` contract (wave 3) |
| Mirth corpus | **Open — human** | Any migration estimate (S6 INCONCLUSIVE) |
| Keyed-hash (`_hmac`) mechanism | **Deliberately unresolved by human decision (2026-09-21)** until before the archiver is built; no agent designs it | Per-patient search in the browser (wave 3/4) |
| **SFTP client licence** (new) | **Open — human.** Unverified: common Python SFTP libraries are LGPL or EPL/GPL | W2-6 `sftp`, W2-7 `sftp` |
| **`psycopg` 3 in the CLAUDE.md stack** (new) | **Open — human.** LGPL-3.0, denied by `licence_check.py`. The W2-6 loader is unaffected: it uses `asyncpg` (Apache-2.0) | Wave 3 control plane only |
| **Linux re-run capacity** (new) | **Open — human** if it needs paid dedicated infrastructure | Exit criterion 4 being meaningful |
| Project name, licence and governance (ADR-006), OIE engagement | Open — human | Release |

Lite mode RF=1 **is** decided (ADOPTED 2026-09-21, ADR-002 D14).

---

## 8. Risks for this wave

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| SFTP has no permissively licensed client | Medium | Medium | Human decision before any code; the options are in channel-spec README 10.2 |
| Set 1.1 has a contract defect found only by an implementer (wave 1 had four) | High | Medium | Conformance run first; implementers send diffs; the corpus test keeps `connector:` entries unresolved |
| A driver default silently breaks a guarantee (redirects, TLS, retries, error text) | Medium | High | Section 6.3; the loader rules in channel-spec README 3.4 |
| ClickHouse deduplication is eventual, so a naive duplicate test passes falsely | Medium | Medium | Assert on `FINAL` (README 5.1) |
| Per-destination steps are more executor work than W2-5 assumed | Medium | Medium | `platform-engineer` asked to counter-propose before building |
| Exit criterion 4 is reported against laptop numbers only | High | Medium | 6.1; say so in the report rather than compare as if comparable |
| Claim-check is unscheduled while file and HTTP sources raise the rate of refused large inputs | Medium | Medium | 5.2; the lead decides whether to schedule the spike |

---

## 9. What this document replaced, and what was removed

Six lead drafts (`wave-2-README.md`, `wave-2-plan.md`, `wave-2-spikes.md`, `wave-2-lead-checklist.md`,
`WAVE-2-REVIEW-REQUEST.md`, `WAVE-2-KICKOFF.md`, all 2026-09-23/24) are consolidated here. The
architect had not reviewed them. They are reduced to one-line pointers to this file; the lead can
`git rm` them. Removed or corrected:

| Claim in the drafts | Why it was removed |
|---|---|
| Audit retention, Mirth corpus, keyed-hash "✅ already approved" | False. Open, open, and deliberately unresolved (section 7) |
| Wave 1 "merged to `main`"; prerequisite "merged to `main`" | False. Pushed and CI green, not merged |
| "Approved: Architect + Lead ✅"; "architect and lead approved wave 2 planning" | The architect had not reviewed the drafts |
| Loaders "enforce a 10 MB limit … `PAYLOAD_TOO_LARGE`"; "e.g. 100 KB" | Invented. The envelope inline limit is 1,000,000 bytes and sources refuse above it (5.2) |
| "ClickHouse version must support the `FHIR` library" | No such dependency exists; nothing in the design needs it |
| "6 loaders" listing seven | There are seven, plus the wave 1 HTTP loader |
| Architect "publishes the `ctx.fhir` skeleton" in `packages/tributary-fhir/…/ctx.py` | The architect writes no package code; W2-2's engineer proposes and the architect approves |
| Retry topic `retry.<destination>` | Contradicts `topics/v0`: one shared `<env>.<channel>.retry` |
| "Retry-topic preserves per-key ordering — expected: yes"; "retry-topic ordered by key" | Contradicts ADR-003 D1 and invariant 5 |
| Spike pass thresholds "≤ 0.5% duplicates", "±500 ms", "1,000+ runs" | No basis; replaced by the criteria in README 4.1.1 |
| Claim-check "post-MVP spike", header `tb-claim-check-uri`, "archive ingests the S3 object directly, bypassing Kafka", key options A/B | The memo says spike before implementing; the header is `tb-claim-check`; the archive-bypass claim is not in any ADR; the key rule was decided in security round 1 |
| Linux re-run "Wave 3 pre-req", "run in parallel or early Wave 3" | The human approved it as the first Phase 1 CI task, before the receiver |
| S2 ACK p99 "environment-dominated" stated as fact; "expectation: remains environment-dominated" | The memo says cause unresolved, and "environment-dominated is an argument, not a measurement" |
| W2-1 "build on 2,446 msg/s/core" | That is `python-hl7`; the lazy parser W2-1 builds measured 36,322 |
| Daily/weekly standup templates, 24/48-hour SLAs, calendar-day checkpoints | Process detail with no basis in `phase-1.md`; the lead runs the wave |
