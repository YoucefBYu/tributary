# Phase 0: Spikes

**Goal:** test the assumptions in ADR-001 with measurements before committing to the MVP. Output is evidence and a go/no-go recommendation, not product code.

**Duration estimate:** 4 to 6 weeks of human calendar time; the agent work itself is far shorter. Agent time is dominated by environment setup and measurement runs.

**Team (5 teammates):** `architect`, `platform-engineer`, `connector-engineer`, `qa-engineer`, `migration-engineer`.
`code-reviewer` runs as a subagent on spike code that will be reused.

**Ground rules**
- Spike code lives in `spikes/S<n>-<name>/` and is throwaway. Do not import it from `packages/`.
- Every spike ends with `docs/spikes/S<n>-<name>.md`: question, method, environment (hardware, versions), raw results, thresholds, verdict, follow-ups.
- Thresholds below are the plan's targets. Missing a threshold is a valid result and must be reported plainly.
- Use synthetic data only (`MSH-3 = TRIBUTARY-SYNTH`). `qa-engineer` provides the generator first (P0-1).

## Tasks

| ID | Owner | Deliverable | Depends on | Verify |
|---|---|---|---|---|
| P0-1 | `qa-engineer` | Synthetic HL7 generator (ADT A01/A03/A08, ORU R01, ORM O01), 10k-message corpus, reproducible seed; benchmark harness skeleton in `benchmarks/` | none | `uv run python -m benchmarks.gen --count 10000 --seed 1` is deterministic; a hook-approved fixture set exists |
| P0-2 | `platform-engineer` | Local 3-broker Kafka (KRaft) via Compose for spikes; shared `spikes/common/` helpers | none | `docker compose ps` healthy; produce and consume smoke test |
| S1 | `qa-engineer` | HL7 parse plus transform throughput: candidates `hl7apy`, `python-hl7`, a minimal lazy parser; with and without an ADT-to-FHIR Bundle transform via pydantic and `orjson`; single-core and multi-process | P0-1 | Report includes msgs/s/core, p50/p99, memory, per parser. **Threshold: at least 200 msg/s/core for parse plus transform.** If missed, propose S1b (Rust/PyO3 parser) with expected gain |
| S2 | `connector-engineer` | asyncio + uvloop MLLP server that publishes to Kafka (`acks=all`, idempotent) and ACKs after commit; TLS on and off | P0-2, P0-1 | Load at 100, 1k and 5k msg/s with 1, 10 and 100 connections. **Threshold: ACK p99 under 50 ms at 1k msg/s.** Broker-failure test: 20 trials killing the partition leader; **zero acknowledged-message loss**, and no ACK while Kafka is unavailable |
| S3 | `connector-engineer` (receiver side) with `platform-engineer` (worker side) | Synchronous reply pattern: correlation id, shared reply consumer per receiver, timeout and NACK | S2 | Added latency vs async ACK; behavior on timeout; pending-map memory at 10k in-flight requests; correctness under worker restart |
| S4 | `qa-engineer` | Topic-count stress: 500 and 1,000 channels × 4 topics × 12 partitions on a 3-node KRaft cluster; consumer group startup; rebalance times; broker and controller memory; topic-sharing fallback prototype | P0-2 | Report cluster limits and the point where operations degrade. **Threshold: 500 channels operate normally with rebalance under 30 s.** Recommend defaults |
| S5 | `platform-engineer` | Blocking retry with partition pause and resume, cooperative-sticky and static membership, transactional Kafka-to-Kafka stage, poison-message to DLQ | P0-2 | Property tests under fault injection: **per-key order preserved** across rebalances and consumer kills; duplicate rate reported; commit-after-delivery verified |
| S6 | `migration-engineer` | Corpus analysis of channel exports in `samples/mirth/` (if provided) or of channel XML structure from public documentation; effort model for importer and `mirth_compat`; importer prototype for one connector type | none | Statistics: connector types, script LOC, feature usage (E4X, Java calls, DB lookups, global maps). **Estimate the share of channels importable with no manual work** |
| P0-3 | `architect` | Synthesis: update ADR-001 with measured numbers; draft ADR-002 (topic topology and envelope) and ADR-003 (ordering, retry, DLQ); contracts v0 for envelope, topics, events; `docs/spikes/PHASE-0-DECISION.md` | S1-S6 | Decision memo states GO, GO WITH CHANGES, or NO-GO, with each threshold, the result, and any invariant that should change |

## Coordination notes

- `qa-engineer` delivers P0-1 first; S1 and S2 cannot start without the corpus.
- S2 and S3 share receiver code. `connector-engineer` owns it; `platform-engineer` only supplies the worker side of the reply path.
- `platform-engineer` and `qa-engineer` both need Kafka: agree on the Compose file in `spikes/common/` and do not start competing clusters.

## Exit criteria

1. All seven spike reports exist with methodology, raw data and verdicts.
2. Each threshold is reported as passed or failed with numbers. No claim without a measurement.
3. `PHASE-0-DECISION.md` has a recommendation, updated risks, and a revised Phase 1 scope if needed.
4. ADR-001 updated, ADR-002 and ADR-003 drafted, contracts v0 published.
5. Human sign-off recorded in `docs/agent-team/STATUS.md`.

## Human checkpoint

Review the decision memo before starting Phase 1. Failed thresholds may change the runtime language for receivers, the partition and topic strategy, or the scope of the MVP.
