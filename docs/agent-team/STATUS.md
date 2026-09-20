# Status

Maintained by the `architect`. The lead reads this before starting a wave and checks the previous
wave's exit criteria here.

## Current

- **Phase / wave:** Phase 0 (spikes), in progress
- **Branch:** `wave/0`
- **Last gate report:** none. `docs/spikes/PHASE-0-DECISION.md` is not written; it waits on S1-S6.
- **Contracts:** v0 Draft published for envelope, topics and events. Schemas reviewed by hand,
  **never executed against a validator** — the first conformance run is part of publishing v0.

### Spike reports filed

| Spike | Owner | Report | Verdict |
|---|---|---|---|
| P0-1 corpus and harness | `qa-engineer` | not filed | |
| P0-2 spike Kafka cluster | `platform-engineer` | not filed | |
| S1 HL7 parse plus transform throughput | `qa-engineer` | not filed | |
| S2 MLLP ACK-after-commit | `connector-engineer` | not filed | |
| S3 synchronous reply | `connector-engineer`, `platform-engineer` | not filed | |
| S4 topic-count stress | `qa-engineer` | not filed | |
| S5 ordering, blocking retry, DLQ | `platform-engineer` | not filed | |
| S6 Mirth import corpus | `migration-engineer` | `docs/spikes/S6-mirth-import-corpus.md` | INCONCLUSIVE — no corpus available, scripts unexecuted |

No threshold has a measured result yet. Nothing in ADR-001 has been updated with measurements.

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

## Risks (live)

From ADR-001 section 13, with the evidence column that Phase 0 is meant to fill.

| Risk | Likelihood | Impact | Measured evidence | Mitigation |
|---|---|---|---|---|
| Kafka too heavy for small sites | Medium | High | none | Lite distribution, managed offerings, sizing guide |
| Python hot-path throughput below estimate | Medium | Medium | none — S1 not filed | Benchmark, PyO3 parser, Rust receivers |
| Migration cost from JavaScript deters adopters | High | High | none — S6 inconclusive, no corpus | Importer, compat shim, assisted porting, shadow diff |
| Buyers want references, certification, support SLAs | High | High | none | Design partners, security docs, paid support |
| Topic/partition explosion with many channels | Medium | Medium | none — S4 not filed | Topic sharing, partition defaults, load tests |
| Contributor fragmentation with OIE | Medium | Medium | none | Open dialogue, interoperability |
| Security incident from arbitrary Python steps | Low to medium | High | none | Trust model, container isolation, signed bundles |

### Risks added in Phase 0

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Contract schemas have never been executed, so the v0 examples may not validate | Medium | Low | First conformance run reports failures to `architect` as contract bugs |
| Claim-check (payloads over 1 MB) is untested by any Phase 0 spike | Medium | Medium | Call it out as an untested path in the Phase 1 plan; ADR-004 owns object lifecycle |
| The record key is often a patient identifier, so any log, metric label or span attribute built from it is a PHI leak | Medium | High | Stated in `contracts/envelope/v0` section 4; reviewers treat it as Critical |
