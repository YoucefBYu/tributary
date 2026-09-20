# Status

Maintained by the `architect`. The lead reads this before starting a wave and checks the previous
wave's exit criteria here.

## Current

- **Phase / wave:** Phase 0 complete. **P0-3 synthesis filed 2026-09-20; awaiting human sign-off at the gate.**
- **Branch:** `wave/0`
- **Last gate report:** `docs/spikes/PHASE-0-DECISION.md` — **GO WITH CHANGES.** No invariant changes.
  Two planning values were disproved and corrected: the 12-partition default (now 3) and the
  200-500 msg/s/core estimate (measured 2,446 with `python-hl7`). Two thresholds were not met as
  written: S2's 50 ms ACK p99 (cause unresolved, environment-dominated) and S4's 500 channels at 12
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
  `docs/security/contracts-v0-review.md`. Four invalid examples added, so the expected conformance
  count is now **74** (envelope 7 valid / 18 invalid, events 8 / 16, topic names 10 / 12, 3 guard
  tests). **That run is outstanding** — the architect has no shell.

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
| Phase 0 go/no-go | **pending human** | | `docs/spikes/PHASE-0-DECISION.md` filed 2026-09-20: **GO WITH CHANGES**, 9 decisions for the human in its section 7 |
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

## Open questions for the human

**The Phase 0 gate list, with a recommendation for each, is `docs/spikes/PHASE-0-DECISION.md`
section 7 (9 items).** It includes the five marked **[gate]** below plus four more: confirm that no
invariant changes (the recommendation is that none does — nothing measured contradicted one), approve
a Linux re-run of S1 and S2 as the first Phase 1 CI task, approve the revised Phase 1 scope, and
supply a Mirth corpus or accept that the migration estimate stays a guess. The items below are the
standing list and are not superseded by it.

- Project name and trademark check (codename Tributary).
- License and governance model (ADR-006).
- Whether to engage the Open Integration Engine community, and how.
- Whether anonymized Mirth channel exports can be provided for `samples/mirth/` — S6 is
  INCONCLUSIVE without them, so the migration effort estimate stays a guess.
- Confirm ADR-002 D2 (environment-scoped platform topics) as a refinement rather than an invariant-2
  change.
- Lite mode (RF=1) cannot satisfy RPO=0. Confirm that shipping it with a startup warning and explicit
  documentation is acceptable.
- **ADR-004 scope, raised by the security review:** the Kafka role/ACL model, credential rotation and
  key management for archive encryption and the `attrs` HMAC. `contracts/topics/v0` 3.4 states the
  required mechanisms and ACL shape, but the model itself and anything touching key management is a
  human decision per CLAUDE.md. Needed before the archiver or any production deployment, not before the
  Phase 0 gate.
- **The `attrs` `_hmac` mechanism is UNRESOLVED (security finding 6, HIGH).** The events schema pins
  the *shape* (exactly 64 lowercase hex) but cannot pin the mechanism, and an unkeyed `sha256(mrn)`
  has the same shape while being reversible by exhaustive search over a bounded identifier space. No
  agent will choose the construction, the key scope, the rotation story or the key store — that is
  cryptography and key management, which CLAUDE.md escalates. Open questions are written out in
  `contracts/events/v0/README.md` 3.1 and belong to ADR-004. Interim position in the contract:
  producers SHOULD NOT emit identifier-derived `attrs` at all, and `_hmac` values must be treated as
  PHI in ClickHouse. Also needs a human answer: is a keyed hash of an MRN de-identified for your
  compliance purposes? (Under a Safe Harbor reading it is not.)
- **Audit retention: 30 days versus a possible six-year duty (security finding 12, MEDIUM).**
  `_tb.<env>.audit` retains 30 days, a number picked to match the DLQ, not a compliance decision.
  HIPAA §164.316(b)(2)(i) requires six-year retention of the records of required actions and
  activities; whether that reaches a control-plane and access audit trail is an interpretation
  question, and **no legal review has been done — none should be done by an agent.** If the duty does
  apply, 30 days is a deletion policy: the evidence is gone before the first annual audit. The likely
  answer is architectural rather than a bigger number — six years at RF=3 points at ClickHouse or
  object storage as the system of record with Kafka as a transport buffer. Three questions:
  (a) what duty applies, (b) what is the system of record, (c) does the answer also change
  `_tb.<env>.events` (3 d) and `raw` (7 d), which is also the replay source. Written up in
  `contracts/topics/v0/README.md` 2.1; the value is unchanged and marked `TODO(human)`. Not a Phase 0
  gate blocker, but it must be settled before the `_tb.<env>.audit` contract is written.

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
