# Status

Maintained by the `architect`. The lead reads this before starting a wave and checks the previous
wave's exit criteria here.

## Current

- **Phase / wave:** **Phase 0 complete and SIGNED.** The human signed the gate on 2026-09-22 as
  **GO WITH CONDITIONS**. **This does not start Phase 1 — Phase 1 needs the human's separate go-ahead.**
- **Branch:** `wave/0`
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
| Phase 1 wave 1 exit | pending | | |
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
| 2026-09-22 | Both gate High findings closed: root `.gitignore` restored (human `325cef1`) and the spike data plane bound to localhost (`e553fd7`, `71364c7`). Conformance suite now asserts *why* each invalid example fails (`83a1fd2`, 76 passed), and the AA-withheld-until-publish-resolves unit test exists (`a5df2c7`) | gate report 1.1 |

## Open questions for the human

**Seven of the nine Phase 0 gate items were decided on 2026-09-21, and the MPL approval plus the gate
sign-off followed on 2026-09-22** — see the decisions log above. What remains open is listed here. Items
struck through were decided and are kept for one revision so the change is visible.

**Nothing blocks the Phase 0 gate any more — it is signed.** The next human decision is whether to start
Phase 1, which the sign-off deliberately does not do.

**Open:**

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
- Project name and trademark check (codename Tributary).
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
| Contract schemas pass conformance, but the green run predates the round-2 security fixes; a re-run is outstanding (expected 74 tests) | Medium | Low | `qa-engineer` re-runs `tests/conformance`; keep it in CI so a contract edit cannot silently break an example |
| **A green test whose fault never fired is indistinguishable from a passing system.** Five S5 claims were first rated `WEAKLY SUPPORTED` for exactly this: a kill at `commit_every=1` replays nothing, a joiner assigned nothing measures no rebalance, a `txn_crash` between transactions proves nothing | Medium | High | Fixed in S5 by making the run **fail** when the fault does not fire, plus mutation controls. Carry the pattern into Phase 1 test design: assert the fault happened, and keep a broken-implementation control for any check backing a headline claim |
| **Every Phase 0 number comes from one Windows laptop with Docker Desktop.** Latency figures are the least transferable; capacity figures are lower bounds | High | Medium | Linux re-run of S1 and S2 as the first Phase 1 CI task; no external publication or capacity planning from these numbers until then |
| A contract can look complete and still omit a security control (the v0 config contract specified `acks` and `linger.ms` but no TLS, auth or ACLs; the durability floor was set per profile but not per topic) | Medium | High | Both fixed. Lesson applied going forward: every contract that configures a component states its required security posture and its failure-safe defaults explicitly, and `security-reviewer` sees contracts, not only code |
| Claim-check (payloads over 1 MB) is untested by any Phase 0 spike | Medium | Medium | Call it out as an untested path in the Phase 1 plan; ADR-004 owns object lifecycle |
| The record key is often a patient identifier, so any log, metric label or span attribute built from it is a PHI leak | Medium | High | Stated in `contracts/envelope/v0` section 4; reviewers treat it as Critical |
