# Status

Maintained by the `architect`. The lead reads this before starting a wave and checks the previous
wave's exit criteria here.

## Current

- **Phase / wave:** **Phase 1, wave 2 — pipelines and delivery. STARTED 2026-09-24** on the human's
  approval to start wave 2 (relayed by the lead). Architect work this session: contract set 1.1 and
  the planning-record corrections below.
- **Branch:** `wave/1-2`. It builds on the wave 1 branch; **wave 1 is not merged to `main`**, which is
  still at `2c25448`.
- **Plan:** [`phases/wave-2.md`](phases/wave-2.md), which replaces six lead drafts the architect had not
  reviewed. They contained errors, including three open human decisions marked "approved" and a claim
  that the architect had approved the plan. `wave-2.md` section 9 lists every removal; the six files are
  now one-line pointers, for the lead to `git rm`. Authoritative scope remains
  [`phase-1.md`](phases/phase-1.md), Wave 2.
- **Exit criteria** (verbatim, `phase-1.md`): (1) an HL7 ADT flow to a FHIR server and to ClickHouse
  works end to end from a YAML channel; (2) an ordered channel preserves per-key order under chaos
  tests; (3) every loader has a passing duplicate-delivery test; (4) throughput — msg/s per core and ACK
  p99 against the Phase 0 numbers. Operational readings are in `wave-2.md` section 3.
- **Contracts: set 1.1 published and VERIFIED 2026-09-24** (`qa-engineer`, committed `ffc1580`):
  `validate.py` **122 / 0**; `--emit-expectations` **86 pairs, 0 mismatches**, all 16 set 1.1
  predictions included; `pytest tests/conformance` **127 passed**. Set 1.1 adds the wave 2 connector
  shapes (3 source types, 7 destination types), per-destination steps, `partitionKey` semantics, and
  written lift criteria for `retry-topic`, which stays refused. `contracts/CHANGELOG.md`, ADR-007
  D10-D17. **Later the same day**: README follow-ups from implementers, with no schema change, and one
  valid example's key changed to `hl7:PID.3[5:MR].1`. Re-verified by the lead: `validate.py`
  **122 / 0**, conformance **127 passed** (`e628891`).
- **Linux re-run of S1/S2: NOT DONE.** Approved 2026-09-21 as "the first Phase 1 CI task, before the
  receiver is built". The receiver was built in wave 1. `.github/workflows/bench-linux.yml` exists as a
  stub: it records the environment and locked versions, then prints "not wired yet: qa-engineer to add
  the S1 (parser) and S2 (MLLP to Kafka) entry points". It is manual-trigger only, and
  `benchmarks/results/` holds only the Phase 0 Windows output. **Confirmed independently by
  `qa-engineer`**, who owns wiring it and has not started (the chaos suite came first). It matters for
  exit criterion 4: without it, the throughput report compares against laptop numbers only.
- **Exit criterion 3 gap, found by `qa-engineer`:** the existing HTTP and PostgreSQL
  `test_duplicate_delivery_is_harmless` tests call `deliver()` twice in one process. Under channel-spec
  README 5.1 that does not count as a genuine redelivery. `qa-engineer` builds the redelivery harness
  (a real consumer loop with offset replay) in W2-9, coordinating the interface with
  `connector-engineer`.
- **Verification owed for the 2026-09-25 contract edits** (`qa-engineer`):
  - `uv run --no-project --python 3.12 --with jsonschema python contracts/validate.py`: expect
    **122 / 0**, unchanged;
  - `uv run --no-project --python 3.12 --with pytest --with jsonschema python -m pytest tests/conformance`:
    expect **127 passed**, unchanged;
  - `uv run pytest packages/tributary-runtime/tests/test_core_steps.py`: the manifest `requires`
    edits, where no `runnable: false` example may resolve completely;
  - one independent `hashlib` recomputation of the three child-id vectors in channel-spec README
    3.3.3.

  `--emit-expectations` is not needed, because no schema or invalid example changed.
- **Only one wave 2 item is blocked on a human: W2-2 item 4** (above). SFTP was unblocked on 2026-09-24: the human approved
  `paramiko` (see decisions log). The
  PostgreSQL *loader* is **not** blocked: it uses `asyncpg` (Apache-2.0 per the lead; `licence_check.py`
  confirms it when the dependency lands). `psycopg` matters only to the wave 3 control plane.
- **W2-5 (built-in steps, splitter, destination steps; `platform-engineer`): DELIVERED, IN REVIEW.**
  Commits `74c9853`, `290a553`, `2cabf4a`, `627f547`, `7849d1f`, `288903a`. `code-reviewer` and
  `security-reviewer` are both running, so it is not done until any Critical or High they find is
  closed. The architect ruled on its six contract proposals on 2026-09-25 (decisions log). That
  produced four change requests back to `platform-engineer`:
  - the executor emits `TRANSFORM_ERROR`, not `STEP_ERROR`;
  - eight reserved prefixes;
  - the registry cross-check test;
  - the `ids.py` docstring.

  **Open risk: a produce-time `EnvelopeError` crashes the worker.** Envelope 2.3 says a component
  rejects and never crashes, and an unhandled exception in the consumer loop is a crash loop on one
  record, which halts a partition. The reviewers are expected to rule on its severity. It touches
  delivery, so it must be closed or explicitly accepted before W2-5 is marked done.
- **W2-3 (HL7 v2 to FHIR content; `healthcare-standards-engineer`): Critical found, fix in
  progress.** `code-reviewer` found that `Encounter` was keyed on `tb-msg-id` instead of PV1-19 (visit
  number). One visit would then be a new Encounter per message, and conditional-update idempotency
  (channel-spec 3.4.2) would hold per message but not per encounter, which is the wrong resource
  identity. `security-reviewer` is running. Not done until both have cleared.
- **W2-2 item 4 is blocked, and needs a human decision** (open questions). Validating the golden
  files against the FHIR R4 JSON Schema could not be done: `hl7.org` is blocked by the proxy, and no
  installed package ships the schema. That validation was a condition of the W2-2 approval: it
  guards against the R4B models accepting R4-invalid output. So W2-2 stays approved with that
  condition open, not closed.
- **Contract edits 2026-09-25, not yet verified** (the architect has no shell). Commands and expected
  results are in the verification note below.
- **Waiting on teammates:** final W2-2 signatures after the conditions (`healthcare-standards-engineer`);
  the `retry-topic` spike report (`platform-engineer`). The first model result has pause-until-due at
  p50 lateness of about 515 s at production tiers, which is option (a)'s head-of-line cost made
  concrete. A delay-buffer variant is being measured.

### Wave 1 exit

- **State: pushed, CI green (human-confirmed 2026-09-24, relayed by the lead), every gate condition
  closed, NOT merged to `main`.** The gate report's one remaining condition was "a human pushes
  `wave/1-1` and the CI integration job goes green". Per the human, that is now met.
- **Gate report:** [`gates/phase-1-wave-1.md`](gates/phase-1-wave-1.md). **GO WITH ONE CONDITION**,
  and that condition is now met. All eight tasks were delivered.
- **Reviews:** `code-reviewer-8` APPROVE, zero findings at any severity. **`security-reviewer`, final:
  0 Critical, 0 High, 0 Medium, 3 Low**, so the gate passes from their side. Their first pass found 2
  High, and **both were fixed inside the wave and confirmed closed with the mechanism re-derived, not
  taken on trust:**
  (1) `certifi` (MPL-2.0) human-approved 2026-09-23 (the lead attests first-hand; the reviewer cannot
  verify a human decision) — dev group only, never shipped, scoped to its `testcontainers[kafka]` →
  `docker` → `requests` path, recorded as **its own approval, not an extension** of the three-package one
  (ADR-001 2.3); `MPL_APPROVED` landed at `a35528c`, `licence_check.py` passes 44 components / 0 failed.
  (2) The HTTP loader silently followed 3xx with the default `urllib` opener — a redirected POST dropped
  its body, the target's 200 read as success and **the offset committed for a message that was never
  delivered** — fixed at `bec8029`, with three extra gap-checks the reviewer added themselves.
- **Phase 0 is complete and signed** (2026-09-22).
- **Exit criterion 4 is PASS.** It is specifically "`code-reviewer` clean on the wave diff", and that
  reviewer found nothing. While the two Highs were open they blocked through the definition of done
  item 6 (`security-reviewer` on auth, PHI, listeners and dependencies), **not** through criterion 4 —
  the distinction determines who has work to do, and is worth keeping straight at future gates.
- **Worth carrying into wave 2:** that message-loss bug was in code that had already passed a clean code
  review, `mypy --strict` and 701 tests. **Reviews find what they look for.** Both Highs, both Phase 0
  Highs and three of the four contract defects share one shape — *a control never applied to a path
  nobody thought about* — so wave 2 should spend review effort on library defaults and transitive edges
  rather than on paths that already have tests.
- **Branch:** `wave/1-1`
- **Exit criteria:** 1 **PASS**. It was PASS locally and pending CI at the gate; the human confirmed on
  2026-09-24 that CI passed on the pushed branch. The architect has not seen the CI run itself. 2 **PASS for
  invariants 1, 2, 5; PARTIAL for 3**; 3 **PASS**;
  4 **PASS** (`code-reviewer-8`, zero findings). Engineering checks green as run by the lead: `uv sync`,
  `ruff`, `ruff format`, `mypy packages` (56 files), fast suite **701 passed**, `validate.py` **102 / 0**,
  `pytest tests/conformance` **101 passed**, `pytest -m integration` **6 passed in 227.91 s**.
- **Invariant 3 is now PARTIAL, not "not exercised".** The mechanism is implemented and tested —
  `runtime/channels.py` reads the compacted `_tb.<env>.channels` topic and imports no database driver,
  no HTTP client and calls no API, with 12 tests — but PostgreSQL, the API and ClickHouse do not exist
  yet, so "keeps running while they are down" is *vacuously* true rather than tested. The chaos coverage
  in gate `phase-0-spikes.md` 7.2 item 19 stays open and moves to wave 3.
- **The wave's most useful finding is a pattern, not a defect: four contract defects, all found by
  implementers, none by any checker** — each a document that was internally valid and named something
  that did not exist. Recorded in gate report 5.1 and in `contracts/README.md`. Consequence to carry:
  **a green conformance run means the contracts are self-consistent, not that they agree with the
  code**, and the two-checker split (`validate.py` for well-formedness, the corpus test in
  `tributary-runtime` for name resolution) must not be merged.
- **W1-1 contracts (architect): DONE and verified.**
  `tributary.channel-spec/v0` is published — schema, README, 23 examples (4 valid, 19 invalid) — plus
  `contracts/validate.py`, a standalone checker for all four contracts. Envelope gained section 2.3
  (every header parsed defensively: reject, never crash — Phase 1 requirement 13 generalised beyond
  the single header SR-10 hit) and 2.4 (Phase 0 spike drift not to reproduce). Topics gained 3.7
  (`topic-defaults.json` is read, never restated as a literal) and 3.8 (the lite RF=1 startup warning
  is mandatory and testable). `events/v0` needed no change, recorded with the reason. Decisions in
  [ADR-007](../adr/ADR-007-channel-spec-shape-and-refusals.md).
  **Verified by three first-run-green checks, none of them by the architect, who has no shell:**
  `contracts/validate.py` **96 / 0** (lead); `--emit-expectations` showed **all 21 new
  `(json_path, validator)` pairs matched** the measured run, so no manifest needed correcting
  (`qa-engineer`); `pytest tests/conformance` **105 passed** and `validate.py` **100 / 0** at
  `f5bc92d`, the latest run, covering the requirement-13 and quarantine additions (`qa-engineer`).
  `devops-engineer`
  wired a standalone `contracts` CI job at `f649665`, running before `uv sync` and separate from the
  pytest conformance job. The 21 pairs were written as predictions and labelled UNMEASURED
  everywhere until they were measured.
- **"Contracts v1" is contract set 1.0 — APPROVED by the lead 2026-09-22**, not escalated to the
  human: a documentation and versioning convention, not a licensing or invariant question. Set 1.0 is
  `envelope/v0` + `topics/v0` + `events/v0` + `channel-spec/v0`, frozen together at the wave-1 gate;
  there are deliberately **no `contracts/*/v1/` directories**. Mapping table and the rejected
  alternative are in `contracts/README.md`. The lead will name the choice in the wave summary so the
  human is not surprised by the absent directories.
- **Phase 0 branch:** `wave/0`
- **All four blocking gate conditions are met:** (a) the root `.gitignore` restored and the stray root
  `Dockerfile` removed — human commit `325cef1`, 31 lines back, lead-verified that `samples/mirth/*`,
  `.env`, `secrets/` and `benchmarks/results/` are ignored again; (b) the spike data plane bound to
  localhost — `e553fd7` (MLLP default `127.0.0.1`) and `71364c7` (all four published ports on
  `127.0.0.1`, `DYNAMIC_CONFIG_ENABLED` removed), after the lead stopped the cluster on 2026-09-21;
  (c) the three MPL-2.0 dependencies approved by the human on 2026-09-22; (d) the sign-off itself.
  **What remains are the Phase 1 requirements** in the gate report section 7.1 and three non-blocking
  open items: audit retention, the Mirth corpus, and the keyed-hash mechanism.
- **Last gate report:** [`docs/agent-team/gates/phase-0-spikes.md`](gates/phase-0-spikes.md) — the
  exit-criteria check (**5 of 5 PASS**), the invariant 1-10 audit and both reviewers' findings
  (no Critical; both Highs now fixed). **GO WITH CONDITIONS, signed 2026-09-22.**
  The technical decision memo behind it is `docs/spikes/PHASE-0-DECISION.md` — **GO WITH CHANGES**, no
  invariant changes. Two planning values were disproved and corrected: the 12-partition default (now 3)
  and the 200-500 msg/s/core estimate (measured 2,446 with `python-hl7`). Two thresholds were not met
  as written: S2's 50 ms ACK p99 (cause unresolved, environment-dominated) and S4's 500 channels at 12
  partitions per topic. **9 decisions wait on the human** — see below.
- **Contracts:** v0 Draft published for envelope, topics and events. Conformance passed 62 / 0 / 0 on
  2026-09-20, then `security-reviewer` raised 5 HIGH findings; all 5 accepted and fixed in place (still
  Draft, no version bump). The suite then passed 70 / 0 / 0 on those fixes at commit `ae4d3bd` — envelope 7 valid /
  16 invalid, events 8 / 14, topic names 10 / 12. (Reported by the lead; the architect has no shell
  and did not run it. The working tree has moved past `ae4d3bd`, so that result covers the committed
  state, not the current one.)
  **Round 2 (findings 6-14) is triaged: 7 fixed, 1 escalated, 1 referred to the human, none
  disputed.** Finding 6 (the `_hmac` mechanism) is answered as UNRESOLVED-by-design and escalated to
  ADR-004 plus a human; finding 12 (audit retention) is in the open questions below. The other seven
  are fixed in place — per-finding record in `contracts/CHANGELOG.md`, status table in
  `docs/security/contracts-v0-review.md`. **Conformance: 76 passed at `83a1fd2`** (was 74, then 70 at
  `ae4d3bd`, 62 on the first run). The jump to 76 is `qa-engineer`'s expected-failure assertions: every
  invalid example must now fail at the `expected_json_path` and `expected_validator` recorded in its
  manifest, so an example failing for the wrong reason goes red — `code-reviewer` Medium 2 from the gate.
  No schema bugs at any point. Run by the lead and `qa-engineer`; the architect has no shell.

### Spike reports filed

All six filed and synthesised. Full threshold table with evidence ratings quoted verbatim:
`docs/spikes/PHASE-0-DECISION.md` section 3.

| Spike | Owner | Report | Verdict |
|---|---|---|---|
| S1 HL7 parse plus transform throughput | `qa-engineer` | `S1-hl7-throughput.md` (**accepted** 2026-09-20) | **PASS.** Solo host: lazy 36,322 msg/s/core, `python-hl7` 2,446 (12.2x); `hl7apy` 100 = **FAIL**, off the hot path. S1b (Rust/PyO3) not needed |
| S2 MLLP ACK-after-commit | `connector-engineer` | `S2-mllp-kafka.md` | **Mixed.** Zero-loss (20/20 trials, "no loss observed") and no-ACK-while-unavailable (6/6) **MET**; ACK p99 **NOT MET as measured** (33-125 ms vs 50 ms, cause unresolved). New finding: one broker kill stalls *all* connections 7.5-12.7 s |
| S3 synchronous reply | `connector-engineer`, `platform-engineer` | `S3-sync-reply.md` | **Established against a stand-in worker.** +40 ms p50 at 100 msg/s, 4.6 KB per in-flight request, 0 lost across a restart. 1,026 timed-out requests were already committed → sync mode needs loader idempotency |
| S4 topic-count stress | `qa-engineer` | `S4-topic-scale.md` | **PASS at 3 partitions/topic** (1,000 channels healthy); **FAIL at the plan's 12** (cluster lost at ~425-450 channels). Rebalance 4.3 s; static removal 43.8 s is session-timeout-bound. 2G heap inconclusive |
| S5 ordering, blocking retry, DLQ | `platform-engineer` | `S5-ordering-retry.md` | **PASS.** 0 order violations everywhere; 12-message replay after a kill still ordered; cooperative rebalance moved exactly 2 of 6 partitions; txn exactly-once across a kill. **`retry-topic` not implemented or measured** |
| S6 Mirth import corpus | `migration-engineer` | `S6-mirth-import-corpus.md` | **INCONCLUSIVE** — no corpus. 43-60+ person-days and "5-20% importable" are guesses |
| Evidence review | `code-reviewer` | `reviews/phase-0-evidence-review.md` | *"Nothing UNSUPPORTED. No Critical findings."* Two residual `WEAKLY SUPPORTED` ratings (S4 heap as the cause; the 8k/20k trigger judgement) and finding L2 open |

## Sign-offs (human)

| Checkpoint | Decision | Date | Notes |
|---|---|---|---|
| Phase 0 go/no-go | **GO WITH CONDITIONS (signed by the human)** | 2026-09-22 | Instruction: *"Approve orjson, hypothesis and pathspec; record Phase 0 GO WITH CONDITIONS signed"*. All four blocking conditions were met first. **The conditions still standing are the non-blocking open items only:** audit retention (open, blocks the `_tb.<env>.audit` contract), the Mirth corpus (not decided), the keyed-hash mechanism (unresolved until before the archiver is built), and the Phase 1 requirements table in `gates/phase-0-spikes.md` 7.1. **Signing does NOT start Phase 1 — Phase 1 needs the human's separate go-ahead.** |
| Phase 1 wave 1 exit | **CI passed on the pushed branch; starting wave 2 approved** | 2026-09-24 | Recorded exactly as relayed by the lead: the human confirmed that CI passed and approved starting wave 2. That met the gate's one remaining condition. **Not recorded here, because it was not stated:** a merge to `main` (none has happened; `main` is at `2c25448`) and any separate signature on the gate report |
| Phase 1 wave 2 exit | pending | | |
| Phase 1 wave 3 exit | pending | | |
| MVP release decision | pending | | |
| Phase 2 v1.0 release decision | pending | | |

## Decisions log

| Date | Decision | Reference |
|---|---|---|
| 2026-09-20 | Contracts v0 (Draft) published: envelope, topic naming and defaults, lifecycle events, each with a JSON Schema and valid/invalid examples | `contracts/README.md`, `contracts/CHANGELOG.md` |
| 2026-09-20 | Four headers added beyond ADR-001 5.4: `tb-destination`, `tb-stage`, `tb-error-code`, `tb-retry-not-before`. Additive; needed for retry and DLQ triage | `contracts/CHANGELOG.md` |
| 2026-09-20 | Platform topics are environment-scoped (`_tb.<env>.events`). Deviates from ADR-001 5.3 prose; topic class unchanged. **Needs human confirmation at the gate** | ADR-002 D2 |
| 2026-09-20 | Events carry a `kind` discriminator (`message` / `partition`) and an RFC 3339 `ts`, differing from the S5 spike emitter | `contracts/events/v0/README.md` |
| 2026-09-20 | ADR-002 and ADR-003 drafted as skeletons with `TODO(Sn)` placeholders; no unmeasured number presented as a result | `docs/adr/README.md` |
| 2026-09-20 | 5 HIGH security findings against contracts v0 all accepted, none disputed. Fixed in place while Draft: PHI key removed from the validation projection, `attrs` PHI rules made enforceable, claim-check URIs constrained, required security profile added, and the durability floor pinned per topic (an invariant-1 bug) | `contracts/CHANGELOG.md`, ADR-002 D8/D9 |
| 2026-09-20 | Transport security and authorization belong in the topics contract, not deferred wholesale to ADR-004. Boundary: contract fixes mechanisms and ACL shape; ADR-004 plus a human own the role model, rotation and key management | ADR-002 D9 |
| 2026-09-20 | Security review round 2 (findings 6-14): 7 fixed, 1 escalated (6), 1 referred to the human (12), none disputed | `contracts/CHANGELOG.md`, `docs/security/contracts-v0-review.md` |
| 2026-09-20 | `tb-correlation-id` is a UUID minted by the engine, not free text. A sender-supplied token is mapped by the receiver in memory and never becomes the reply record key | ADR-002 D10, `contracts/envelope/v0/README.md` 3.2 |
| 2026-09-20 | `tb-usr-*` headers are **not** projected into `_tb.events` in v0. Chose this over defining a header-to-`attrs` mapping: an automatic projection makes every channel-defined header a silent archive column | ADR-002 D11, `contracts/envelope/v0/README.md` 2.2 |
| 2026-09-20 | Topic sharing must not span a tenant or trust boundary — a restriction enforced by the control plane, not a documented cost | `contracts/topics/v0/README.md` 1.3 |
| 2026-09-20 | Encryption at rest is required by the topics contract (broker disks, claim-check bucket, archive volumes, backups), stated with the threats it does not cover. Key custody stays with ADR-004 | `contracts/topics/v0/README.md` 3.5 |
| 2026-09-20 | **Phase 0 verdict: GO WITH CHANGES.** No invariant changes; ADR-001 5.3, 6.3, 7.1, 7.3, 13 and 14 updated with measurements | `docs/spikes/PHASE-0-DECISION.md` |
| 2026-09-20 | **Partition default cut from 12 to 3 per channel topic**, and the sharing trigger restated in projected partitions (warn 8k, share 12k, believed fatal ~20k). The 12 was an ADR-001 planning value that S4 disproved | ADR-002 D4/D5, `contracts/topics/v0` 2.2 |
| 2026-09-20 | `session.timeout.ms` set explicitly to 15 s (band 10-15 s): it is the recovery-time floor for an ordered channel, confirmed by two independent harnesses (S5, S4) | ADR-002 D5.1, `contracts/topics/v0` 3.3 |
| 2026-09-20 | Retry defaults: `blocking` is the default policy, `blocking` → `halt-partition`, `retry-topic` → `dlq-and-continue`; poison retried 3 times (judgement, unmeasured). **`retry-topic` may not be selected until it is spiked** | ADR-003 D1/D3/D4 |
| 2026-09-20 | Kafka transactions stay **opt-in**, not on by default: their cost against a plain at-least-once stage was never isolated, and a dead producer stalls `read_committed` consumers for up to `transaction.timeout.ms` | ADR-003 D6 |
| 2026-09-20 | The retry-delay mechanism (one `retry` topic plus `tb-retry-not-before` versus one topic per tier) is **deferred to a Phase 1 spike** rather than decided on no evidence | ADR-003 D2 |
| 2026-09-20 | Synchronous reply requires loader idempotency on `tb-msg-id`; the timeout NACK means "committed, outcome unknown"; the reply worker uses fail-fast plus DLQ and the batched-commit path | ADR-003 D9 |
| 2026-09-21 | **Human, "proceed with the recommendations of the decisions":** invariants 1-10 confirmed unchanged — **ADOPTED** | gate report section 8 |
| 2026-09-21 | **Human:** `_tb.<env>.*` platform topic naming (ADR-002 D2) confirmed as a refinement of ADR-001 5.3 prose, not an invariant-2 change — **ADOPTED** | ADR-002 D2, gate report section 8 |
| 2026-09-21 | **Human:** lite mode with RF=1 — **ADOPTED** as a documented profile with a startup warning and no RPO=0 promise. Recorded as an explicit, human-approved **profile exception to invariant 1's durability promise**; the invariant is not edited and the exception lives in ADR-002 D14, `contracts/topics/v0` and the docs | ADR-002 D14 |
| 2026-09-21 | **Human:** `_hmac` mechanism — recommendation **ADOPTED**: leave UNRESOLVED until before the archiver is built, no agent designs it, and per-patient message-browser search stays blocked on it | `contracts/events/v0/README.md` 3.1 |
| 2026-09-21 | **Human:** ADR-004 scope **APPROVED** — Kafka role/ACL model, credential rotation, key custody for encryption at rest and the `_hmac` key, claim-check object lifecycle. Needed before the archiver or production | gate report section 8 |
| 2026-09-21 | **Human:** Linux re-run of S1 and S2 **APPROVED** as the first Phase 1 CI task, before the receiver is built | `PHASE-0-DECISION.md` section 7 item 7 |
| 2026-09-21 | **Human:** revised Phase 1 scope (`PHASE-0-DECISION.md` section 8) **APPROVED** | `PHASE-0-DECISION.md` section 8 |
| 2026-09-21 | Lead stopped the running spike cluster (`docker compose down`) because of the all-interfaces exposure (`security-reviewer` High 2). Nothing lost; storage was ephemeral | gate report 1.1 |
| 2026-09-22 | **Human, "Approve orjson, hypothesis and pathspec":** the three MPL-2.0 dependencies are **APPROVED** as unmodified — `orjson` (shipping runtime, `MPL-2.0 AND (Apache-2.0 OR MIT)`), `hypothesis` (test-only), `pathspec` (build-time). The approval covers these three packages unmodified; **any new or modified MPL dependency needs fresh approval**, and the inventory is known to be incomplete because `[build-system] requires` resolves outside the lockfiles | gate report 5.1, ADR-001 2.3 |
| 2026-09-22 | **Human: Phase 0 gate signed as GO WITH CONDITIONS.** Does not start Phase 1 | gate report, sign-offs table |
| 2026-09-23 | **Human: `certifi` (MPL-2.0) APPROVED** as an unmodified, **dev-group-only, never-shipped** dependency, scoped to its current transitive path `testcontainers[kafka]` → `docker` → `requests`. Recorded as **its own approval, not an extension** of the `orjson`/`hypothesis`/`pathspec` decision, whose scope note covered *those three packages* — a transitive arrival is exactly the case that note anticipated. A different path, or a runtime group, is a different dependency needing a fresh decision | ADR-001 2.3, gate `phase-1-wave-1.md` 5.3 |
| 2026-09-23 | **A hand-written licence comment is not a licence clearance.** `certifi` was found by `licence_check.py` exiting 1 in CI, not by review: the root manifest annotates `testcontainers[kafka]  # Apache-2.0`, which is correct about testcontainers and silent about its transitive closure. Second near-miss of the same shape after Phase 0's `pathspec`, which a lockfile-only inventory missed because `[build-system] requires` resolves separately | ADR-001 2.3 |
| 2026-09-22 | **Channel spec v0 published** (W1-1): declarative spec referencing code in a digest-pinned bundle. Rejected: a Mirth-parity spec with inline source (breaks invariant 7 and puts arbitrary code on `_tb.channels`, which is RCE across the data plane) and a code-first channel with no document (the UI and control plane could not reason about a channel without executing tenant code) | ADR-007, `contracts/channel-spec/v0` |
| 2026-09-22 | **`retry-topic` is in the policy enum and rejected by the validator.** Vocabulary stays shared across specs, docs and code; selection is a validation failure until the spike. Chosen over a prose-only ban because both Phase 0 High findings were controls that were specified and then not applied | ADR-007 D5, ADR-003 D1/D2 |
| 2026-09-22 | **Security rules that a schema can hold are held by the schema**: TLS unless loopback, `tls.clientAuth` stated explicitly, plaintext HTTP only to loopback, no query string in a destination URL, credential-shaped property names rejected, `queue.maxBytes` required. Phase 1 requirements 5 and 8 become validation failures rather than review findings | ADR-007 D7 |
| 2026-09-22 | **`idempotency` is required on every destination**, with a written justification when `none`. Invariant 4 becomes declarative and per-channel reviewable instead of a sentence in a loader docstring. The key is always `tb-msg-id` and is not configurable | ADR-007 D4 |
| 2026-09-22 | **The `_tb.<env>.channels` record shape is specified for the first time**: key, value, tombstone, and the rule that the runtime rejects a published record without `metadata.version`. Invariant 3 made that topic load-bearing and nothing had defined it | `contracts/channel-spec/v0/README.md` 6 |
| 2026-09-22 | **"Contracts v1" = contract set 1.0**, not `v1/` directories — **lead-approved**, deliberately not escalated to the human as a versioning convention rather than a licensing or invariant question. Rejected: copying three contracts into new major directories — ~60 files moved, every path in `tests/conformance`, ADR-002, ADR-003 and this file broken, no schema changed. A new major directory is created by a breaking change *after* a freeze, not by the freeze | `contracts/README.md`, `contracts/CHANGELOG.md` |
| 2026-09-22 | **`.claude/agent-memory/` covered by a directory pattern in `.gitignore`** (lead, `49b8a91`, verified with `git check-ignore`), replacing the per-file list the human added in `5aa6c54`, which two new memory files had already outgrown in a single wave. Additive and in its own commit — the opposite of Phase 0 High 1, where a `.gitignore` change rode along in another commit and silently removed protection. Recorded because gate item 22 assigns `.gitignore` to `devops-engineer`, and this is Phase 1's first change to it | gate `phase-0-spikes.md` 7.2 item 22 |
| 2026-09-22 | **Phase 1 requirement 13 generalised**: every integer, timestamp and UUID header is parse-or-reject, with `reject` defined (dlq + `HEADER_PARSE_ERROR` + event + commit) and the failing value kept out of the error string. A missing required header is the same class of failure | `contracts/envelope/v0` 2.3 |
| 2026-09-22 | **`topic-defaults.json` is read, never restated as a literal**, in runtime, control plane, topic creation, Compose and Helm. Reason: `min.insync.replicas` is topic-level, so a copied-and-forgotten literal makes invariant 1 silently false — the HIGH finding's original mechanism | `contracts/topics/v0` 3.7 |
| 2026-09-22 | Both gate High findings closed: root `.gitignore` restored (human `325cef1`) and the spike data plane bound to localhost (`e553fd7`, `71364c7`). Conformance suite now asserts *why* each invalid example fails (`83a1fd2`, 76 passed), and the AA-withheld-until-publish-resolves unit test exists (`a5df2c7`) | gate report 1.1 |
| 2026-09-24 | **Human (relayed by the lead): wave 1 CI passed; start wave 2.** Wave 1 is not merged to `main` | sign-offs table |
| 2026-09-24 | **Contract set 1.1 published, additive, unverified**: wave 2 connector shapes, per-destination steps, `partitionKey` semantics, receiver consumer-group role | `contracts/CHANGELOG.md` |
| 2026-09-24 | **ADR-007 D6 amended (D10):** connector types enter the enum at the start of the wave that builds them. Rejected: shapes arriving with each connector, which would have ten connectors inventing the contract in parallel | ADR-007 5a |
| 2026-09-24 | **Kafka connectors always use their own explicit connection and credentials** (D11). Rejected: a "local cluster" mode on the engine's principal, which is an ACL-model change owned by ADR-004 | ADR-007 5a |
| 2026-09-24 | **The `fhir` destination is transaction-only** (D12), which keeps identifiers out of request lines without a no-log exception. Rejected: conditional create/update with `?identifier=` | ADR-007 5a |
| 2026-09-24 | **SQL sinks take a step-built row; no mapping language in the spec** (D13). Each destination type may declare only the idempotency techniques it can honour, and the SQL loaders verify theirs at startup (D14) | ADR-007 5a |
| 2026-09-24 | **`retry-topic` lift criteria fixed before the spike** (D15), including that retried messages' reordering is characterised, not denied, and that a per-key channel must say `acceptReordering: true` | channel-spec README 4.1.1 |
| 2026-09-24 | **A refused Kafka-source record halts the source partition** (D16). The first draft dead-lettered it; the receiver ACL has no `dlq` WRITE, so that rule was unimplementable, and it was caught by tracing it into `topics/v0` before publishing | ADR-007 5a |
| 2026-09-24 | **Per-destination steps** (D17), run in the pipeline worker with no new hop. Exit criterion 1 needs a FHIR Bundle and a table row from one channel | channel-spec README 3.5 |
| 2026-09-24 | **Wave 2 planning drafts consolidated into `phases/wave-2.md`**; six lead drafts reduced to pointers, and errors listed in its section 9 | `phases/wave-2.md` |
| 2026-09-24 | **W2-2 `ctx.fhir` / `ctx.terminology` APPROVED WITH CONDITIONS.** Local Bundle-entry construction with no network; terminology behind a Protocol with fakes; no `ctx.ids` (it would reach the human-reserved `_hmac` question). Conditions: `fhir.resources`/`fhir-core` pass `licence_check.py`; per-message cost measured; R4B models guarded by validating the golden files against the FHIR R4 JSON schema; conditional entries on exactly one unique identifier system; no unconditional create; deterministic fullUrls; runtime attachment optional, not a hard import (agreed with `platform-engineer`) | channel-spec README 3.4.2 (corrected the same day: a business-identifier conditional PUT qualifies) |
| 2026-09-24 | **Human: `paramiko` (LGPL-2.1) APPROVED** as an unmodified dependency of `tributary-loaders` and `tributary-receivers`, for SFTP only. The lead asked the human directly and the human chose option 1 of three (shell-out to OpenSSH and descoping SFTP were the others). Recorded as **its own approval, not an extension** of the MPL or `certifi` decisions. Any other package, or a modified copy, needs a fresh decision. paramiko's own dependencies must each still pass `licence_check.py`; confirming that reading with the human is with the lead. SFTP contract shapes unchanged, `hostKeys` stay SHA-256 fingerprints | ADR-001 2.3 |
| 2026-09-24 | **Contract set 1.1 verified**: 122 / 0, 86 pairs matched, conformance 127 passed (`qa-engineer`, `ffc1580`) | `contracts/CHANGELOG.md` |
| 2026-09-24 | **Key evaluator APPROVED** as `tributary.sdk.keys` (`platform-engineer`): pure, stdlib only, and returns `ResolvedKey \| UnresolvedKey(reason code)` rather than raising, so the unresolved path cannot carry the value. The repr is redacted | channel-spec README 4.3 |
| 2026-09-24 | **PID-3 repetition selector** `hl7:PID.3[5:MR].1` added now, not in v1. Evidence (`platform-engineer`): repetition order differs between senders, so a bare `PID.3.1` gives one patient two keys. The "normalise in a step" advice was withdrawn as impossible, because keys are evaluated before steps | channel-spec README 4.3 |
| 2026-09-24 | **`with` binds to keyword-only step parameters by name**, refused loudly at load if wrong. The SDK's `@step` changes so that a keyword-only parameter without a default means "required from `with`". Rejected: an untyped `ctx.params` | channel-spec README 3.3 |
| 2026-09-24 | **Reprocess of any `dlq` record carrying `tb-destination` must be destination-scoped**, and is refused until W3-3 builds it. This covers wave 1 loader-exhaustion records too. A destination-step `dlq` record carries the step's input | ADR-003 follow-up 10 |
| 2026-09-24 | **W2-5 public surface APPROVED WITH CHANGES.** filter needs an explicit `onUnresolved` and may not filter on identifier fields; the enrich allow-list is trimmed; `Reject(code)` is added to the SDK and may not use engine-reserved codes; split becomes a `@splitter` node with **deterministic child ids** and is refused on per-key channels. Metrics and tracing wait for `security-reviewer` on the `/metrics` listener. `contracts/observability/v0` is to be published by the architect, seeded with `platform-engineer`'s names | channel-spec README 3.3.2 |
| 2026-09-24 | **W2-2 implemented (`39a4006`); the interface is approved from the architect's side, with five items open.** W2-3 itself stays gated behind the W2-1 security fixes (the lead's gate). (1) ~~Licence of `fhir.resources`/`fhir-core` UNVERIFIED~~ **Closed at `f083c49`**: `licence_check.py` now resolves them, both BSD. (2) Confirm conditional-only upsert, create and delete on one required identifier. (3) Production fullUrls derived from `tb-msg-id`, not uuid4, so shadow and reprocess diffs stay comparable. (4) Golden files validated against the FHIR R4 JSON schema. (5) Optional runtime attachment, agreed with `platform-engineer`. Informal cost is about 313 µs per resource against the 5 ms budget | `packages/tributary-fhir` |
| 2026-09-24 | **`postgresql` loader BUILT** (`eca445a`, `connector-engineer`), not yet "Implemented": its duplicate tests repeat `deliver()` in one process, so the 5.1 redelivery test waits for the W2-9 harness. Adopted from the build: all authentication material under `auth.secretRef`; the uniqueness check reads the catalog, because a bare unique index is invisible in `information_schema` | channel-spec README 3.0, 3.4.3 |
| 2026-09-24 | **Manifest `runnable` checked against W2-1** (`9078982` registers `tributary.hl7v2.parse` only). No example flips: each one naming `parse` also names a `ref` step, and the fanout example needs the unregistered `tributary.hl7v2.to_json`. `requires` lists trimmed to what is still missing | `examples/manifest.json` |
| 2026-09-25 | **Human (asked directly by the lead): the `clickhouse` loader uses ClickHouse's HTTP interface through the Python standard library**, built like the HTTP loader: redirects refused, no pooling, no hidden retries, auth in headers from `secretRef`, stdlib TLS. No new dependency. Found by `connector-engineer` on W2-6: `clickhouse-connect` (Apache-2.0) unconditionally requires `certifi` (MPL-2.0), approved dev-only, so `licence_check.py` failed when it entered `tributary-loaders`' runtime closure; the engineer stopped and reverted. Its defaults (`query_retries=2`, internal HTTP retries) also re-send silently on connection failure (`wave-2.md` 6.3). **`certifi`'s scope is unchanged: non-runtime only.** Rejected: approving `certifi` for the runtime; deferring the loader. **Deviates from the CLAUDE.md stack table** (Archive names `clickhouse-connect`) for the loader only; the table is human-owned and the proposed one-line change went to the lead. The wave 3 archiver's client is **not decided** (open questions) | ADR-001 2.3 |
| 2026-09-25 | **W2-5 contract proposals ruled (six).** (1) A keyword-named `with` key binds to `key_`, and `**kwargs` is refused: **approved**. (2) Derived child-id encoding: **approved with the layout pinned exactly.** Bits 12-17 of the 80-bit digest prefix are discarded, and the "set version and variant bits" wording, which also reads as overwrite-in-place, is replaced. (3) Engine error-code registry: **approved with amendments.** The contract is the source and code is checked against it, not generated from it. Eight loader-family prefixes are added. **Drift found:** the executor emits `STEP_ERROR` where the contract says `TRANSFORM_ERROR`, and the contract wins. (4) Built-in `with` shapes: **approved.** The filter's JSON deny-list applies to every pointer token, which corrects my "ends in" wording. (5) Reprocess refuses `tb-parent-id` records until W3-3: **approved as refuse-only.** It over-refuses reprocessed records, on purpose. (6) Manifest: **approved.** It was three examples, not two. The `parse` `version` gap is added to two examples, and nothing flips to runnable. Rejected: generating the registry from `codes.py`, and "refuse or scope" for split children before a re-entry point exists | `contracts/CHANGELOG.md` 2026-09-25, channel-spec README 3.3-3.3.3, envelope README 2.3.2, ADR-003 follow-up 10 |

## Open questions for the human

**Seven of the nine Phase 0 gate items were decided on 2026-09-21, and the MPL approval plus the gate
sign-off followed on 2026-09-22** — see the decisions log above. What remains open is listed here. Items
struck through were decided and are kept for one revision so the change is visible.

**Nothing blocks the Phase 0 gate any more — it is signed.** Wave 1 is complete and wave 2 has started
on the human's approval (2026-09-24).

**Open, new in wave 2:**

- **FHIR R4 JSON Schema for W2-2 golden-file validation: vendor the file, or allow one-time network
  access?** (Raised 2026-09-25.) This is a condition of the W2-2 approval: the `fhir.resources` models
  are R4B, and validating the goldens against the R4 schema catches R4-invalid output they accept.
  `hl7.org` is blocked by the proxy, and no installed package ships the schema.
  - **Option 1, vendor `fhir.schema.json` into the repo.** Reproducible and offline. It is a
    licensing question, because it is third-party content in the repo. The architect's
    understanding is that HL7 publishes FHIR under CC0, but that is **unverified**, and CC0 is not
    on invariant 10's list.
  - **Option 2, allow one-time network access to fetch it.** No licensing change to the tree, but
    the check is not reproducible offline or in CI unless the fetched file is cached somewhere.

  Either way it is a human call (licensing, or environment access). Until then, W2-2 condition 4
  stays open and the golden files are unchecked against R4.

- ~~**SFTP client licence.**~~ **DECIDED 2026-09-24: `paramiko` approved**, unmodified and scoped
  (decisions log, ADR-001 2.3). Kept for one revision so the change is visible. The scope question is
  settled: the "dependency closure" wording was the lead's relay, not the human's. The human's choice
  approved paramiko alone, so each of its dependencies must pass `licence_check.py` on its own.
- **Resolved 2026-09-24: `licence_check.py` did short-circuit on combined SPDX expressions**, in the
  MPL branch from `b773158` and in the LGPL branch modelled on it. Found by `security-reviewer`; fixed
  by `devops-engineer` at `f083c49`. Any expression containing "mpl" was approved as a whole, without
  checking the other terms: `MPL-2.0 AND Proprietary` passed as approved.
  **Note on the wave 1 licence evidence** (gate `phase-1-wave-1.md` 5.3, "44 components / 0 failed"):
  - For `orjson`, whose resolved expression is `MPL-2.0 AND (Apache-2.0 OR MIT)`, that result did
    **not** check the non-MPL terms. The conclusion was still right, since Apache-2.0 and MIT are
    permissive, but it came from a checker that could not have failed on a bad extra term.
  - `certifi` is affected only if its resolved expression is compound; a plain `MPL-2.0` hits no
    short-circuit.
  - The human approvals stand either way: they rest on the licences, not on the checker.
  - **The evidence is now real.** The fixed checker passes on the current tree. Its regression tests
    (`test_mpl_compound_expression_with_an_unapproved_extra_term_is_rejected`, and four LGPL cases)
    were shown red against the pre-fix code before going green, and the suite is 31 / 31.
  - Same fix: `tributary-fhir`'s `fhir.resources` and `fhir-core` are now actually checked (BSD), which
    closes W2-2 open item 1.
- **`psycopg` 3 in the CLAUDE.md stack table — answer needed before wave 3.** CLAUDE.md names it for
  the control plane. It is LGPL-3.0, and `licence_check.py` denies it (reported by the lead and
  `connector-engineer`). Invariant 10 has no LGPL path. The options: approve it as an unmodified
  dependency, or amend the stack table to a permissive driver. The wave 2 loaders already use `asyncpg`
  (Apache-2.0), which shows one exists. Choosing the stack is the human's decision. Not a wave 2 blocker.
- **ClickHouse client for the wave 3 archiver — answer needed before the archiver is built.** On
  2026-09-25 the human decided that the `clickhouse` *loader* uses the HTTP interface via the standard
  library, because `clickhouse-connect` unconditionally pulls in `certifi` (MPL-2.0, approved dev-only)
  and retries silently by default (decisions log). That decision does **not** cover the archiver. If the
  archiver uses `clickhouse-connect`, as the CLAUDE.md stack table still says, the same question
  returns: approve `certifi` for the runtime, or build the archiver on the same stdlib HTTP client as
  the loader. Not decided, and not a wave 2 blocker.
- **Linux re-run capacity.** The approved re-run has not happened (see Current). The memo priced it as
  "CI runner time and a dedicated 3-broker cluster". A hosted runner is not a dedicated cluster.
  Whether to pay for dedicated capacity is cloud spend.

**Open, carried:**

- **Audit retention — still open, and it blocks the `_tb.<env>.audit` contract.** What retention duty
  applies to the control-plane and access audit trail? HIPAA §164.316(b)(2)(i) requires six-year
  retention of records of required actions and activities; whether that reaches this audit trail is a
  legal-interpretation question and **no legal review has been done, nor should an agent do one**. No
  number has been written: the current 30 days is marked `TODO(human)`. The likely answer is
  architectural (ClickHouse or object storage as the system of record, Kafka as a buffer), and it also
  puts `_tb.<env>.events` (3 d) and `raw` (7 d, the replay source) in scope.
- **Mirth corpus — not decided.** Supply anonymized exports in `samples/mirth/`, or accept that the
  migration estimate stays a guess. S6 is INCONCLUSIVE without them, and migration cost is the
  highest-likelihood, highest-impact risk in ADR-001 section 13. The ordering risk that used to attach to
  this — `samples/mirth/` not being git-ignored — is **closed**: the `.gitignore` was restored in
  `325cef1`, so exports can safely land on disk now.
- Project name and trademark check (codename Tributary). **Its cost now rises with time:**
  `tributary-fhir`'s identifier system `https://tributary.dev/fhir/identifier/tb-msg-id` is written into
  resources in customers' FHIR servers. Once real data carries it, renaming means a data migration.
- License and governance model (ADR-006).
- Whether to engage the Open Integration Engine community, and how.

**Decided 2026-09-21** (detail in the decisions log and gate report section 8):

- ~~Confirm ADR-002 D2 (environment-scoped platform topics) as a refinement rather than an invariant-2
  change.~~ **ADOPTED.**
- ~~Lite mode (RF=1) cannot satisfy RPO=0. Confirm that shipping it with a startup warning and explicit
  documentation is acceptable.~~ **ADOPTED**, and recorded as an explicit human-approved profile
  exception to invariant 1's durability promise (ADR-002 D14). The invariant itself is unchanged.
- ~~Confirm that no invariant changes.~~ **ADOPTED**: invariants 1-10 unchanged.
- ~~ADR-004 scope~~ **APPROVED**: the Kafka role/ACL model, credential rotation, key custody for
  encryption at rest and the `_hmac` key, and claim-check object lifecycle. `contracts/topics/v0` 3.4
  and 3.5 state the required mechanisms and ACL shape; the model and anything touching key management
  stay a human decision. Needed before the archiver or any production deployment.
- ~~Linux re-run of S1 and S2~~ **APPROVED** as the first Phase 1 CI task, before the receiver is built.
- ~~Revised Phase 1 scope~~ **APPROVED** (`PHASE-0-DECISION.md` section 8).
- ~~The `attrs` `_hmac` mechanism (security finding 6, HIGH).~~ **Recommendation ADOPTED: it stays
  deliberately UNRESOLVED** until before the archiver is built, no agent designs it, and per-patient
  search in the message browser stays blocked on it. The schema pins the 64-lowercase-hex *shape*, which
  is all a schema can do — an unkeyed `sha256(mrn)` has the same shape and is reversible by exhaustive
  search over a bounded identifier space. Construction, key scope, rotation and key custody are ADR-004
  (approved scope) plus a human. Interim contract position stands: producers SHOULD NOT emit
  identifier-derived `attrs`, and `_hmac` values are PHI in ClickHouse. One question still needs a human
  answer when ADR-004 is written: is a keyed hash of an MRN de-identified for your compliance purposes?
  (Under a Safe Harbor reading it is not.) Detail: `contracts/events/v0/README.md` 3.1.

## Risks (live)

From ADR-001 section 13, with the evidence column Phase 0 was meant to fill. Now filled — the full
version is `docs/spikes/PHASE-0-DECISION.md` section 9 and ADR-001 section 13.

| Risk | Likelihood | Impact | Measured evidence | Mitigation |
|---|---|---|---|---|
| Kafka too heavy for small sites | Medium | High | none — no spike addressed footprint | Lite distribution, managed offerings, sizing guide |
| Python hot-path throughput below estimate | ~~Medium~~ **Low** | Medium | S1 solo host: ~36.3k msg/s/core (lazy) and 2,446 (`python-hl7`) against a 200 threshold. `hl7apy` fails at 100 and is off the hot path | S1b (Rust/PyO3) **not needed**. Re-measure on Linux CI and on a production mapping with terminology lookups |
| Migration cost from JavaScript deters adopters | High | High | none — S6 INCONCLUSIVE, no corpus | Importer, compat shim, assisted porting; **timebox the Phase 1 effort rather than committing to 43-60 person-days** |
| Buyers want references, certification, support SLAs | High | High | none | Design partners, security docs, paid support |
| Topic/partition explosion with many channels | Medium, **now bounded** | Medium | **S4: 12,009 partitions healthy, ~21,000 fatal at 1G heap; 500 channels FAIL at 12 partitions/topic, 1,000 PASS at 3** | **Partition default 3; budget in projected partitions; re-measure on real broker memory** |
| Contributor fragmentation with OIE | Medium | Medium | none | Open dialogue, interoperability |
| Security incident from arbitrary Python steps | Low to medium | High | none | Trust model, container isolation, signed bundles |
| **Receiver-wide stall on one broker loss** | **Medium** | **Medium** | **S2: 7.5-12.7 s across all connections, 20/20 trials; cause inferred, not confirmed** | **Per-connection head-of-line isolation; broker session and election tuning** |
| **Sync-reply duplicates after a timeout NACK** | **High** | **Medium** | **S3: 1,026 of 9,000 requests timed out, all already committed** | **Loader idempotency on `tb-msg-id`; document `AE` as "committed, outcome unknown"** |
| **ACK latency target unverified on production-like hardware** | **High** | **Medium** | **S2: p99 33-125 ms at 1k msg/s against a 50 ms threshold; a bare producer with no MLLP had p99 131 ms on the same cluster** | **Linux re-run with a dedicated cluster before any sizing claim** |
| **`retry-topic` delay tiers carried forward untested** | **Medium** | **Medium** | **none — not implemented or measured in S5** | **Spike before any destination may select it (ADR-003 D2, follow-up 5)** |

### Risks added in Phase 0

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| ~~Contract schemas pass conformance, but the green run predates the round-2 security fixes~~ **Closed**: re-run at 76 (`83a1fd2`), then 106 at the wave 1 gate; the suite is a required CI job (`f649665`). Set 1.1 is the new unverified case, under wave 2 risks | — | — | — |
| **A green test whose fault never fired is indistinguishable from a passing system.** Five S5 claims were first rated `WEAKLY SUPPORTED` for exactly this: a kill at `commit_every=1` replays nothing, a joiner assigned nothing measures no rebalance, a `txn_crash` between transactions proves nothing | Medium | High | Fixed in S5 by making the run **fail** when the fault does not fire, plus mutation controls. Carry the pattern into Phase 1 test design: assert the fault happened, and keep a broken-implementation control for any check backing a headline claim |
| **Every Phase 0 number comes from one Windows laptop with Docker Desktop.** Latency figures are the least transferable; capacity figures are lower bounds | High | Medium | Linux re-run of S1 and S2 as the first Phase 1 CI task; no external publication or capacity planning from these numbers until then |
| A contract can look complete and still omit a security control (the v0 config contract specified `acks` and `linger.ms` but no TLS, auth or ACLs; the durability floor was set per profile but not per topic) | Medium | High | Both fixed. Lesson applied going forward: every contract that configures a component states its required security posture and its failure-safe defaults explicitly, and `security-reviewer` sees contracts, not only code |
| Claim-check (payloads over 1 MB) is untested by any Phase 0 spike | Medium | Medium | Call it out as an untested path in the Phase 1 plan; ADR-004 owns object lifecycle |
| The record key is often a patient identifier, so any log, metric label or span attribute built from it is a PHI leak | Medium | High | Stated in `contracts/envelope/v0` section 4; reviewers treat it as Critical |

### Risks added in wave 2

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| ~~No permissively licensed SFTP client~~ **Closed 2026-09-24**: `paramiko` approved. Residual risk: its auto-add host-key policy is the library default to watch | Medium | High | Fingerprint check before authentication, no auto-add fallback (channel-spec README 10.2) |
| **Contract set 1.1 has a defect only an implementer finds** (wave 1 had four, all found that way) | High | Medium | Conformance run first; implementers send diffs; the manifest's `connector:` entries keep unbuilt connectors out of "runnable" |
| **A client-library default silently breaks a guarantee** (redirects, TLS verification, driver retries, error text quoting payload). Wave 2 adds seven client libraries | Medium | High | The loader rules in channel-spec README 3.4; reviews target defaults and transitive edges (`wave-2.md` 6.3) |
| **Remote file names and object keys carry PHI** (new surfaces with SFTP and S3) | Medium | High | Names built from `tb-msg-id` only on output; inbound file names never logged (README 3.2.4, 3.4.4) |
| **ClickHouse duplicate test passes falsely**, because deduplication is eventual | Medium | Medium | Assert a `FINAL` count (README 5.1) |
| **Exit criterion 4 reported against laptop numbers only** | High | Medium | Run the Linux re-run, or say so in the report rather than compare as if the numbers were comparable |
| **Claim-check is unscheduled** while file and HTTP sources make large inputs more common | Medium | Medium | Every source refuses input over 1 MB; the lead decides whether to schedule the spike (`wave-2.md` 5.2) |
| **A produce-time `EnvelopeError` crashes the pipeline worker** (W2-5, open) | Unknown until review rules | High: a crash loop on one record halts the partition, which is the envelope 2.3 failure class | Handle it as a reject (dlq, event, commit), never an unhandled exception. `code-reviewer`/`security-reviewer` rule on it; must be closed or explicitly accepted before W2-5 is done |
| **Contract and code drift on names nobody checks** (latest: `STEP_ERROR` vs `TRANSFORM_ERROR`, found only while writing the registry) | Medium | Medium | Registry plus a two-way cross-check test in `tributary-sdk`. The same shape as the wave 1 finding: a green conformance run says the contracts are self-consistent, not that they agree with the code |
