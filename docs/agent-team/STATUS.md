# Status

Maintained by the `architect`. The lead reads this before starting a wave and checks the previous
wave's exit criteria here.

## Current

- **Phase / wave:** Phase 0 (spikes), in progress
- **Branch:** `wave/0`
- **Last gate report:** none. `docs/spikes/PHASE-0-DECISION.md` is not written; it waits on S1-S6.
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

| Spike | Owner | Report | Verdict |
|---|---|---|---|
| P0-1 corpus and harness | `qa-engineer` | not filed | |
| P0-2 spike Kafka cluster | `platform-engineer` | not filed | |
| S1 HL7 parse plus transform throughput | `qa-engineer` | `docs/spikes/S1-hl7-throughput.md` (**accepted** 2026-09-20) | PASS for a lazy parser (~27.7k msg/s/core) and `python-hl7` (~1,970); FAIL for `hl7apy` (~59). Threshold 200 msg/s/core. S1b (Rust/PyO3) not needed |
| S2 MLLP ACK-after-commit | `connector-engineer` | not filed | |
| S3 synchronous reply | `connector-engineer`, `platform-engineer` | not filed | |
| S4 topic-count stress | `qa-engineer` | not filed | |
| S5 ordering, blocking retry, DLQ | `platform-engineer` | not filed | |
| S6 Mirth import corpus | `migration-engineer` | `docs/spikes/S6-mirth-import-corpus.md` | INCONCLUSIVE — no corpus available, scripts unexecuted |

S1 is the only threshold with a measured result so far. ADR-001 has not yet been updated with
measurements — that happens in P0-3 synthesis, once the remaining reports are in.

## Sign-offs (human)

| Checkpoint | Decision | Date | Notes |
|---|---|---|---|
| Phase 0 go/no-go | pending | | Needs `PHASE-0-DECISION.md` |
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

## Open questions for the human

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

From ADR-001 section 13, with the evidence column that Phase 0 is meant to fill.

| Risk | Likelihood | Impact | Measured evidence | Mitigation |
|---|---|---|---|---|
| Kafka too heavy for small sites | Medium | High | none | Lite distribution, managed offerings, sizing guide |
| Python hot-path throughput below estimate | ~~Medium~~ **Low** | Medium | S1: parse plus ADT-to-FHIR transform at ~27.7k msg/s/core (lazy parser) and ~1,970 (`python-hl7`) against a 200 threshold, on a loaded Windows host. `hl7apy` fails at ~59 and is off the hot path | S1b (Rust/PyO3) **not needed** for this target. Re-measure on Linux CI and on a full production mapping with terminology lookups |
| Migration cost from JavaScript deters adopters | High | High | none — S6 inconclusive, no corpus | Importer, compat shim, assisted porting, shadow diff |
| Buyers want references, certification, support SLAs | High | High | none | Design partners, security docs, paid support |
| Topic/partition explosion with many channels | Medium | Medium | none — S4 not filed | Topic sharing, partition defaults, load tests |
| Contributor fragmentation with OIE | Medium | Medium | none | Open dialogue, interoperability |
| Security incident from arbitrary Python steps | Low to medium | High | none | Trust model, container isolation, signed bundles |

### Risks added in Phase 0

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Contract schemas pass conformance, but the green run predates the security fixes; a re-run is outstanding | Medium | Low | `qa-engineer` re-runs `tests/conformance`; keep it in CI so a contract edit cannot silently break an example |
| A contract can look complete and still omit a security control (the v0 config contract specified `acks` and `linger.ms` but no TLS, auth or ACLs; the durability floor was set per profile but not per topic) | Medium | High | Both fixed. Lesson applied going forward: every contract that configures a component states its required security posture and its failure-safe defaults explicitly, and `security-reviewer` sees contracts, not only code |
| Claim-check (payloads over 1 MB) is untested by any Phase 0 spike | Medium | Medium | Call it out as an untested path in the Phase 1 plan; ADR-004 owns object lifecycle |
| The record key is often a patient identifier, so any log, metric label or span attribute built from it is a PHI leak | Medium | High | Stated in `contracts/envelope/v0` section 4; reviewers treat it as Critical |
