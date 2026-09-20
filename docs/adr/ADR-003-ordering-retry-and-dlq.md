# ADR-003: Ordering, Retry and DLQ Policy

**Status:** Proposed — **SKELETON. Every number marked `TODO(Sn)` is a placeholder, not a result.**
**Date:** 2026-09-20 (drafted), to be completed at P0-3 synthesis
**Deciders:** architect, with human sign-off at the Phase 0 gate
**Relates to:** ADR-001 sections 5.5, 6.4, 7.3 · invariants 4, 5 · ADR-002
**Contracts:** `contracts/envelope/v0` (`tb-attempt`, `tb-stage`, `tb-error-code`, `tb-retry-not-before`), `contracts/events/v0`
**Evidence:** `docs/spikes/S5-*.md` (ordering, retry, transactions), `docs/spikes/S2-*.md` (loss on broker failure) — **none filed at the time of drafting**

---

## 1. Context

Ordering and retry pull against each other. Preserving per-key order under failure means stopping,
which costs throughput and turns one bad destination into visible lag. Maximising throughput means
reordering retried messages, which is unacceptable for ADT (an A08 update applied before the A01 it
updates) and for result corrections.

ADR-001 6.4 named the two policies — `blocking` and `retry-topic` — and left the mechanism, the
defaults, the exhaustion behaviour and the measured cost open. This ADR settles them.

Forces:

- **Invariant 4:** at-least-once end to end, so every loader is idempotent on `tb-msg-id`, and
  duplicates are normal, not incidents.
- **Invariant 5:** ordering is explicit and never silently changed. A policy that reorders must be
  opt-in per destination and visible in the spec.
- **Invariant 1:** nothing here may create a path where a message is acknowledged and then dropped.
- A paused partition is invisible unless it is an event. `contracts/events/v0` therefore has
  first-class `partition_paused` / `partition_resumed` / `partition_halted` events.

### 1.1 Measured evidence

| Question | Spike | Result |
|---|---|---|
| Per-key order preserved across rebalances and consumer kills | S5 | TODO(S5) — threshold: order preserved |
| Duplicate rate under fault injection | S5 | TODO(S5) — report, no threshold |
| Commit-after-delivery verified | S5 | TODO(S5) |
| Throughput cost of a transactional Kafka-to-Kafka stage vs at-least-once | S5 | TODO(S5) |
| Partition pause/resume latency and behaviour under cooperative-sticky rebalance | S5 | TODO(S5) |
| Acknowledged-message loss on partition-leader kill | S2 | TODO(S2) — threshold: zero over 20 trials |
| Parse plus transform per core (sets how much headroom a blocking pause consumes) | S1 | TODO(S1) — threshold: at least 200 msg/s/core |

---

## 2. Decisions

### D1. Retry policy is per destination, declared in the spec (decided)

`blocking` preserves order by pausing the partition; `retry-topic` maximises throughput and does not
preserve order for retried messages. No default that silently reorders: TODO — confirm the default is
`blocking`, since a healthcare integration engine that reorders by default is a patient-safety
problem, and the operator cost (visible lag) is the safer failure.

### D2. Retry delay mechanism (options, TODO(S5) to choose)

Kafka has no per-record delay, so delay tiers need a mechanism.

| Option | How | Pros | Cons |
|---|---|---|---|
| (a) One `retry` topic plus `tb-retry-not-before`, consumer pauses the partition until the timestamp | Read head record, if not due, pause and seek back | One topic per channel; exact delays; matches `contracts/envelope/v0` | Head-of-line blocking inside the retry topic: a 15-minute record blocks a 5-second one behind it |
| (b) One topic per tier (`retry.5s`, `retry.1m`, `retry.15m`) | Consumer per tier, each with a fixed sleep | No head-of-line blocking across tiers | Topic count × tiers, which S4 constrains; tiers become a schema |
| (c) External scheduler re-injects due records | Control-plane timer | Flexible policy | Puts the control plane on the retry path, violating invariant 3 |

(c) is rejected outright: invariant 3 requires the data plane to keep working when the control plane
is down. The choice between (a) and (b) turns on the measured head-of-line cost.
TODO(S5): choose, with the measurement.

### D3. Exhaustion behaviour (decided shape, TODO default)

`on-exhaust: halt-partition` (alert, wait for an operator) or `dlq-and-continue`.

**Trade-off:** `halt-partition` never delivers out of order and never abandons a message, at the cost
of growing lag and requiring a human. `dlq-and-continue` keeps the pipeline moving and moves the
problem to the DLQ, at the cost of a gap in that key's order — later messages for a patient are
delivered while an earlier one sits in the DLQ.
TODO: set the default per policy (proposal: `blocking` defaults to `halt-partition`,
`retry-topic` defaults to `dlq-and-continue`, since choosing `retry-topic` already accepts reordering).

### D4. Poison messages (decided)

A step exception is retried in place a small configurable number of times (TODO: default, proposal 3),
then routed to `dlq` with `tb-stage` and `tb-error-code` set and the original payload intact, so the
message can be reprocessed after a code fix.

The DLQ record carries a **code, never exception text**. The redacted human message goes to
`_tb.events.error_message`. Exception text from HL7 and FHIR parsers routinely quotes the offending
field value, which is PHI (invariant 6).

### D5. Reprocess and replay identity (decided)

Reprocessing a DLQ record re-injects it into `raw` with a **new** `tb-msg-id` and `tb-parent-id` set
to the original. A deliberate reprocess must not be deduplicated away by a destination that is
correctly idempotent on `tb-msg-id`.

**Give up:** the browser must present a parent/child tree rather than one row per message, and
"how many times was this message delivered" becomes a tree query.

### D6. Exactly-once only for Kafka-to-Kafka (decided, cost TODO(S5))

Transactions (read-process-write with offsets in the transaction) apply to `raw` → `out.<dest>`. No
external destination gets exactly-once, because no external system offers the required atomicity.
TODO(S5): report the throughput cost and state whether transactions are on by default or opt-in.

### D7. Ordering under rebalance (TODO(S5))

Cooperative-sticky assignment plus static membership is required (`contracts/topics/v0` 3.3). Open:
whether a paused partition's pause state survives a cooperative rebalance, or whether the new owner
re-discovers the failure by retrying once.
TODO(S5): state the observed behaviour. If pause state is lost, the first record after a rebalance is
re-delivered, which at-least-once permits and idempotent loaders absorb — say so explicitly rather
than implying order was broken.

### D8. Edge duplicate suppression (deferred)

Optional dedupe on (sending application, MSH-10) within a window needs a state store (Redis or
RocksDB) and is a new dependency and failure mode. Not in the MVP unless a design partner needs it.
Senders retransmitting on a missing ACK is the normal, correct behaviour that invariant 4 already
covers.

---

## 3. Consequences

**Easier:** an operator can tell why delivery stopped, from the partition events; reordering can only
happen where someone opted in; a DLQ record has enough context to triage without opening the payload.

**Harder:** `blocking` plus `halt-partition` needs an operator response, so alerting must be good
before the MVP ships; the browser needs a parent/child message tree; explaining "at-least-once with
duplicates is correct" to integrators used to a simple queue.

**To revisit:** key-level parallelism inside a partition (ADR-001 7.2, phase 2), which changes what a
partition pause means; Kafka share groups for unordered destinations.

---

## 4. Impact on invariants and contracts

| Item | Impact |
|---|---|
| Invariant 4 | Implemented: `tb-msg-id` is the idempotency key, duplicates are expected, reprocess mints a new id. |
| Invariant 5 | Implemented: policy per destination, no silent reordering, partition pause is an event. |
| Invariant 1 | Unaffected: nothing here acknowledges before a Kafka commit. |
| No invariant change is proposed by this ADR. | If S5 shows per-key order cannot be preserved under rebalance, that **would** touch invariant 5 — escalate to the human with the measurement; do not weaken the invariant here. |
| `contracts/envelope/v0` | D2 depends on `tb-retry-not-before`; if option (b) wins, the header becomes advisory and the contract needs a note (additive). |
| `contracts/events/v0` | Partition events are the observability contract for D3. |

---

## 5. Follow-ups

1. Fill every TODO(Sn) from S5 and S2 before the Phase 0 gate.
2. Loader idempotency matrix (ADR-001 6.4) becomes a tested table: one integration test per sink.
3. Alert rules for `partition_halted` and DLQ arrival rate ship with the MVP, not after.
4. Document the ordering and retry model for integrators in `docs/` — this is a migration blocker for
   Mirth users, who expect per-destination queues with automatic retry.
