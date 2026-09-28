# ADR-003: Ordering, Retry and DLQ Policy

**Status:** Proposed — **completed at P0-3 2026-09-20.** Every `TODO(Sn)` is filled or, where the spike did not measure it, replaced by an explicit "not measured" with the decision deferred (D2, the retry-delay mechanism).
**Date:** 2026-09-20 (drafted), completed at P0-3 synthesis
**Deciders:** architect, with human sign-off at the Phase 0 gate
**Relates to:** ADR-001 sections 5.5, 6.4, 6.5, 7.3 · invariants 4, 5 · ADR-002
**Contracts:** `contracts/envelope/v0` (`tb-attempt`, `tb-stage`, `tb-error-code`, `tb-retry-not-before`), `contracts/events/v0`
**Evidence:** `docs/spikes/S5-ordering-retry.md` (ordering, retry, transactions), `docs/spikes/S2-mllp-kafka.md` (loss on broker failure), `docs/spikes/S3-sync-reply.md` (sync reply), rated in `docs/spikes/reviews/phase-0-evidence-review.md`. **All measurements come from one Windows/Docker-Desktop laptop** — `docs/spikes/PHASE-0-DECISION.md` section 2.

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

Filled at P0-3. Ratings in parentheses are quoted from
`docs/spikes/reviews/phase-0-evidence-review.md`; every number is from one laptop-hosted cluster.

| Question | Spike | Result |
|---|---|---|
| Per-key order preserved across rebalances and consumer kills | S5 | **0 violations** in 2,000 model fault schedules, 12 cluster scenarios, 13 integration tests — including a run where a `kill -9` replayed 12 messages ("SUPPORTED") |
| Duplicate rate under fault injection | S5 | **0.60%** (`kill`, `static_restart` at `commit_every=20`), 0.10% (`broker_kill`), **0% without a fault**. Bounded by `commit_every` |
| Commit-after-delivery verified | S5 | **0 violations**; online in the model's commit hook, and sampled every 250 ms through the kill window on the cluster, 144 samples ("SUPPORTED") |
| Throughput cost of a transactional Kafka-to-Kafka stage vs at-least-once | S5 | **Not isolated.** `txn` ran 40 transactions of 50 with commit p50 under 1 ms and p99 78 ms, 0 aborts, but no side-by-side throughput comparison was run. The measured cost driver in the ordered path was `commit_every`, not transactions |
| Partition pause/resume latency and behaviour under cooperative-sticky rebalance | S5 | **598 pause/seek/resume cycles, about 169 ms each — of which about 155 ms is the loader's 0.2 s poll timeout, not backoff or Kafka.** A joiner against a stable pair moved **exactly 2 of 6 partitions**, 0 duplicates, 0 reorderings ("SUPPORTED (single run; journal checked by the reviewer)") |
| Acknowledged-message loss on partition-leader kill | S2 | **No loss observed, 20/20 trials**, 244,710 acknowledged ("SUPPORTED (small-sample)"). 20 clean trials bound per-trial loss only to about 14% at 95% confidence |
| Parse plus transform per core (sets how much headroom a blocking pause consumes) | S1 | **2,446 msg/s/core** (`python-hl7`) and **36,322** (lazy) against the 200 threshold, so transform headroom is not what a blocking pause competes with |
| `retry-topic` with delay tiers | S5 | **NOT IMPLEMENTED, NOT MEASURED.** The `retry` topic exists in the layout and is unused |

---

## 2. Decisions

### D1. Retry policy is per destination, declared in the spec (decided)

`blocking` preserves order by pausing the partition; `retry-topic` maximises throughput and does not
preserve order for retried messages.

**Default: `blocking`.** A healthcare integration engine that reorders by default is a patient-safety
problem (an A08 update applied before the A01 it updates), and the operator cost of the safe choice —
visible lag — is the failure you can see. S5 measured what `blocking` costs: 598 real pause/seek/
resume cycles at about 169 ms each, with **zero loss, zero reordering and zero offset skips**, and
about 155 ms of each cycle was the loader's own poll timeout rather than anything inherent.

**`retry-topic` may not be selected until it is spiked.** It was never implemented or measured in
Phase 0, so it is a named option with no evidence behind it (section 5, follow-up 5).

### D2. Retry delay mechanism (options, TODO(S5) to choose)

Kafka has no per-record delay, so delay tiers need a mechanism.

| Option | How | Pros | Cons |
|---|---|---|---|
| (a) One `retry` topic plus `tb-retry-not-before`, consumer pauses the partition until the timestamp | Read head record, if not due, pause and seek back | One topic per channel; exact delays; matches `contracts/envelope/v0` | Head-of-line blocking inside the retry topic: a 15-minute record blocks a 5-second one behind it |
| (b) One topic per tier (`retry.5s`, `retry.1m`, `retry.15m`) | Consumer per tier, each with a fixed sleep | No head-of-line blocking across tiers | Topic count × tiers, which S4 constrains; tiers become a schema |
| (c) External scheduler re-injects due records | Control-plane timer | Flexible policy | Puts the control plane on the retry path, violating invariant 3 |

(c) is rejected outright: invariant 3 requires the data plane to keep working when the control plane
is down.

**Not decided, because it was not measured.** S5 implemented and measured only `blocking` and
DLQ-on-exhaustion; the head-of-line cost that separates (a) from (b) has no number behind it. S4
supplies half the answer — option (b) multiplies topics per channel by the number of tiers, and at
the measured budget (warn at 8,000 projected partitions, share at 12,000) three tiers would turn a
1,000-channel estate into a 21,000-partition one, which is where S4's cluster died. That argues for
(a) but does not establish that (a)'s head-of-line blocking is tolerable.

**Decision deferred to a Phase 1 spike** covering both the delay mechanism and the head-of-line cost.
`tb-retry-not-before` stays in `contracts/envelope/v0` either way: under (a) it is load-bearing, and
under (b) it becomes advisory, which is an additive contract note rather than a break.

### D3. Exhaustion behaviour (decided shape, TODO default)

`on-exhaust: halt-partition` (alert, wait for an operator) or `dlq-and-continue`.

**Trade-off:** `halt-partition` never delivers out of order and never abandons a message, at the cost
of growing lag and requiring a human. `dlq-and-continue` keeps the pipeline moving and moves the
problem to the DLQ, at the cost of a gap in that key's order — later messages for a patient are
delivered while an earlier one sits in the DLQ.
**Defaults: `blocking` → `halt-partition`; `retry-topic` → `dlq-and-continue`**, since choosing
`retry-topic` already accepts reordering.

S5 measured both halves. `halt-partition` works and is observable: in the `halt` scenario three
partitions stopped and 443 messages were held behind them, with committed offsets well below the high
watermarks — visible as lag and as `partition_halted` events, which is the point. `dlq-and-continue`
also works: exactly 8 of 8 poison messages were dead-lettered and the partition kept moving.

**What this costs:** `halt-partition` needs an operator, so alerting on `partition_halted` must exist
before the MVP ships, not after. S5 did not measure a multi-minute outage — its pauses lasted seconds
— so the interaction between a long pause and `max.poll.interval.ms` is an untested edge
(follow-up 6).

### D4. Poison messages (decided)

A step exception is retried in place a small configurable number of times (**default 3**; not
measured — S5 exercised the DLQ path, not the retry count — so this is a judgement, chosen because a
step exception is usually deterministic and retrying it many times only delays the DLQ), then routed
to `dlq` with `tb-stage` and `tb-error-code` set and the original payload intact, so the message can
be reprocessed after a code fix.

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

**Measured (S5):** exactly-once held **across a kill with a transaction open** — 50 records were
proven to be inside an open transaction at the moment of the kill (the difference between a
`read_uncommitted` and a `read_committed` read of `out`), and after restart on the same
`transactional.id` the output held 2,000 records with 0 duplicates and 0 losses. The successor fenced
the dead producer, aborted the transaction, and its input offsets rolled back with it. Commit p50 was
under 1 ms, p99 78 ms, across 40 transactions of 50 records, with 0 aborts.

**Transactions stay opt-in, not on by default.** Two reasons, one measured and one not: the
throughput cost against a plain at-least-once stage **was not isolated** in S5, so switching it on by
default would be an unmeasured tax; and a `read_committed` consumer downstream stalls behind an open
transaction if a producer dies, for up to `transaction.timeout.ms`. S5 hit the second edge as a bug —
librdkafka refuses to construct a transactional producer whose `message.timeout.ms` exceeds
`transaction.timeout.ms`, which is why the runtime clamps the former rather than raising the latter.

**Give up:** channels that would benefit from exactly-once must ask for it. Measure the cost in
Phase 1 before revisiting the default.

### D7. Ordering under rebalance (measured, S5)

Cooperative-sticky assignment plus static membership is required (`contracts/topics/v0` 3.3).
**Observed behaviour, all with 0 order violations:**

| Event | Measured | Why it behaves that way |
|---|---|---|
| A member joins a stable group | **Exactly 2 of 6 partitions moved**, one from each founder; the joiner delivered 540 records; **0 duplicates** | A cooperative revoke is a planned handover: the loader commits before giving up the partition, so the new owner resumes exactly where the old one stopped. The other four partitions were never revoked and kept flowing — the difference from eager assignment, which stops the whole channel |
| A member is killed (`kill -9`) | **12 messages replayed** at `commit_every=20`; takeover **10.01 s** at a 10 s `session.timeout.ms` | The dying worker commits nothing, so the successor replays from the last commit. At-least-once permits this and idempotent loaders absorb it; **order was preserved through the replay** |
| A member is killed with no replacement | Survivors pick the partitions up after **11.32 s** | Same session-timeout bound |
| A member is restarted with the same `group.instance.id` | **3.21 s** for 2.0 s of downtime | Static membership skips the session-timeout wait for planned restarts |

So static membership makes planned restarts cheap and unplanned deaths cost a full session timeout.
**`session.timeout.ms` is the recovery-time floor for an ordered channel** and must be set explicitly
(ADR-002 D5.1, `contracts/topics/v0` 3.3): at the 45 s Kafka default, an OOM-killed worker stops that
partition for 45 s.

**Pause state does not need to survive a rebalance.** The new owner re-discovers a failing destination
by retrying once, which costs one redelivery — permitted by at-least-once, absorbed by an idempotent
loader, and **not** a reordering. Zero duplicates in the cooperative-rebalance run and 12 after a kill
are both the design working; they differ because one handover is planned and the other is not.

### D8. Edge duplicate suppression (deferred)

Optional dedupe on (sending application, MSH-10) within a window needs a state store (Redis or
RocksDB) and is a new dependency and failure mode. Not in the MVP unless a design partner needs it.
Senders retransmitting on a missing ACK is the normal, correct behaviour that invariant 4 already
covers.

### D9. Synchronous reply requires downstream idempotency (decided from S3)

**A timeout NACK in synchronous mode means "committed, outcome unknown" — never "not received".**

S3 measured it: across a worker restart, 9,000 requests produced 7,974 `AA` and **1,026 `AE`
timeouts, and every one of those 1,026 messages had already been committed to `raw`** before the wait
began (invariant 1 requires exactly that ordering). The replacement worker then processed them and
produced 1,026 late replies, which the receiver correctly dropped as unknown correlation ids. Loss
across the restart was zero.

The consequence is unavoidable rather than a defect: **a sender that retries after a timeout NACK
creates a duplicate downstream.**

**Amended 2026-09-28 (W2-8, `platform-engineer`'s finding, verified by the architect).** The original
text drew the wrong conclusion from that sentence: it said loader idempotency on `tb-msg-id` is the
mitigation. It is not. **A resent message is a new message with a new `tb-msg-id`**
(`channel-spec/v0` 3.2.2, "Duplicates at ingest": the receiver mints the id per received message), so
a loader that is idempotent on `tb-msg-id` delivers it again. `tb-msg-id` idempotency absorbs the
*other* duplicate source in sync mode — worker replays after a crash, which keep their id. The only
mitigations for a sender resend are sink-native idempotency on a **business identifier** (for example
the W2-3 conditional FHIR update on a visit or order identifier) and edge dedupe (D8, deferred). No
invariant changes: invariant 4 promises at-least-once with `tb-msg-id` idempotency, and it never
covered a sender resubmitting. Therefore:

1. Every loader on a synchronous channel MUST be idempotent on `tb-msg-id` — invariant 4 already
   requires this, and it absorbs worker-replay duplicates, **not** sender resends after `AE`.
2. The timeout NACK's meaning MUST be documented for integrators, who will otherwise read `AE` as
   "resend it, nothing happened": `AE` means committed, a resend is a duplicate that `tb-msg-id`
   idempotency does not absorb, and only a business-identifier sink or D8 would. The control plane
   warns on every sync channel (`contracts/channel-spec/v0` 3.2.6).
3. The reply worker uses **fail-fast plus DLQ, not `blocking` retry**: pausing a partition to retry
   one reply would stall every other sender's request on that partition, and the receiver's own
   timeout is the correct control (S5's note to S3).
4. The reply worker uses the **batched-commit path**. S5 measured `commit_every=1` at 4.9x the cost,
   and S3's latency was already dominated by two Kafka commits per request.

**Give up:** exactly-once semantics at the edge. No sender protocol offers it, and edge dedupe (D8)
is the only general mitigation — deferred, and a state store either way.

**Caveat:** S3's numbers come from a **stand-in echo worker**, not a real pipeline worker. The
mechanism and its failure semantics are established; the latency figures are not a product number.

### D10. Synchronous reply: routing and what each answer means (decided 2026-09-28, W2-8)

From `platform-engineer`'s proposal (`docs/agent-team/proposals/W2-8-sync-reply.md`), ruled by the
architect. Contract: `contracts/channel-spec/v0` 3.2.6, `envelope/v0` 3 and 2.3.3, `topics/v0` 3.3.1,
set 1.2.

**Routing.** Criteria: no coordinator (invariant 3), each reply reaches the process holding the socket,
cost that does not grow with process count, least PHI exposure.

| Option | Verdict |
|---|---|
| Plain consumer group, replies hashed by correlation id | **Rejected.** The reply lands on whichever member owns the hash, not the one holding the socket |
| Broadcast (ADR-001 6.5 as written): every process reads every partition and filters | **Rejected as the default.** Reads scale as replies × processes, and with `body: step` every process reads every other sender's PHI-bearing answer |
| **Targeted:** each process holds reply partitions through the `replier` group and advertises one in `tb-reply-partition`; the worker produces to it | **Chosen.** Kafka's assignment is the lease. S3: no slower (p50 43 vs 49 ms at 100 msg/s, 85 vs 136 ms at 1k; single run, one instance, inconclusive) |

**Give up:** a reply partition count that must track the receiver process count (a deploy warning and
a readiness failure rather than a silent degradation), and a rebalance turns in-flight requests on the
moved partition into `AE` / `REPLY_ROUTE_LOST`. Planned restarts avoid that with static membership
(S5: 3.21 s).

**Answers.** One rule across both MLLP modes: **`AR` = commit not confirmed (resend is safe), `AE` =
committed (resend duplicates), `AA` = committed and routed.** Neither `AA` nor `AE` is ever sent before
the receiver's own `raw` delivery callback succeeds. A pipeline `Reject` answers `AE`, not `AR`,
because it was committed; a whole-message `Drop` answers `AA`, because it was committed and filtered on
purpose. **Rejected:** `AR` for a `Reject`, the proposal's own alternative, which would tell the sender
a resend is safe when it is not. HL7 original mode reads `AE` as a content error anyway. **Give up:** a
sender configured to expect `AR` for a refused message sees `AE`, and any sender that treats `AE` as
"resend" duplicates — hence the D9 amendment and the integrator note.

**Refused in set 1.2:** sync reply on transactional channels (a reply inside the transaction is late
by up to a batch; one outside could `AA` aborted work) and on channels with any splitter (N outcomes,
no aggregator). `after: delivered` is reserved. The HTTP status mapping is decided with W2-7 under
the constraint that no committed outcome uses a status clients retry automatically.

**Invariants.** 1: holds, and the proposal was amended so that a revoke before the commit callback
cannot produce an `AE`. 5: order is fixed at the `raw` commit and sync mode changes neither
partitioning nor retry; a resend after `AE` that lands after later messages is **sender-induced**, not
an engine reordering, and cannot be detected without D8, so it is documented rather than prevented. 6:
the sender's MSH-10 lives only in the receiver's in-memory map, and the three new headers carry an
integer, a timestamp and an enum.

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
| No invariant change is proposed by this ADR. | **S5 has now reported: 0 order violations across 2,000 model fault schedules, 12 cluster scenarios and 13 integration tests, including a kill that replayed 12 messages. Invariant 5 stands as written and nothing needs to be escalated.** |
| Invariant 4 (again) | **Reinforced by measurement.** S5's ordering check judges the *first* delivery of each sequence number, so `1,2,3,1,3,2` passes — correct under at-least-once, and precisely why a loader that is not idempotent on `tb-msg-id` would experience a replay as a reordering. D9 makes the same point from the sync-reply side. |
| `contracts/envelope/v0` | D2 depends on `tb-retry-not-before`; if option (b) wins, the header becomes advisory and the contract needs a note (additive). |
| `contracts/events/v0` | Partition events are the observability contract for D3. |

---

## 5. Follow-ups

1. ~~Fill every TODO(Sn) from S5 and S2 before the Phase 0 gate.~~ **Done at P0-3**, except D2, which
   is deferred with its reason.
2. Loader idempotency matrix (ADR-001 6.4) becomes a tested table: one integration test per sink.
3. Alert rules for `partition_halted` and DLQ arrival rate ship with the MVP, not after. D3's default
   of `halt-partition` requires an operator, so the alert *is* the mitigation.
4. Document the ordering and retry model for integrators in `docs/` — this is a migration blocker for
   Mirth users, who expect per-destination queues with automatic retry. Include D9's `AE` semantics
   as amended (a resend after `AE` is a new `tb-msg-id`), D10's answer table, and the sender-induced
   reordering of a late resend (2026-09-28).
5. **Spike `retry-topic` with delay tiers** (D2) before any destination may select it: choose between
   one topic plus `tb-retry-not-before` and one topic per tier, and measure the head-of-line cost that
   separates them. It is the only policy in ADR-001 6.4 with no evidence behind it. **The criteria for
   lifting the refusal are fixed in advance** in `contracts/channel-spec/v0/README.md` 4.1.1 (ADR-007
   D15), 2026-09-24 — including that the spike characterises how retried messages reorder rather than
   claiming they do not. Owner: `platform-engineer`, inside W2-4.
6. **Decouple resume from the poll timeout** in `tributary-runtime`: about 155 ms of each 169 ms retry
   cycle was `poll_timeout_s`, not backoff (S5 follow-up 1).
7. **Test a multi-minute destination outage** against `max.poll.interval.ms`. S5's pauses lasted
   seconds; a 30 s backoff cap under a 300 s poll interval is safe by arithmetic, and that should be a
   test rather than an argument.
8. **Measure the transactional stage against a plain at-least-once stage** before revisiting D6's
   opt-in default.
9. Port `test_librdkafka_semantics.py` into the runtime's test suite. It is four small tests that pin
   the client behaviours the whole design assumes — including the one that proves pause-then-resume
   without seeking back **silently loses the failed message**.
10. **Reprocess must be destination-scoped for any `dlq` record carrying `tb-destination`**
    (2026-09-24, found by `platform-engineer` while designing per-destination steps). D5 re-injects
    into `raw` with a new `tb-msg-id`. For a record that failed at one destination, that re-delivers
    to every destination under an id no loader has seen, and loader idempotency cannot absorb it. The
    gap already exists for wave 1 loader-exhaustion records. W3-3 implements destination-scoped
    re-entry. Until then, reprocess refuses these records. `contracts/channel-spec/v0/README.md` 3.5.
    **Extended 2026-09-25 (W2-5, `platform-engineer`): the same applies to any `dlq` record carrying
    `tb-parent-id`.** A dead-lettered split child's payload is the child as the splitter produced it.
    Re-injecting that into `raw` re-runs the splitter on one child: a JSON element is refused as
    `SPLIT_INPUT_INVALID`, and an HL7 message becomes a new identity that no loader has seen. So until
    W3-3 builds a post-split re-entry point that keeps the child's derived `tb-msg-id`, reprocess
    refuses these records too. **This over-refuses on purpose.** D5 also sets `tb-parent-id` on
    reprocessed records, so a reprocessed message that fails again is refused as well, and the header
    alone cannot tell the two cases apart. W3-3 picks the discriminator: a derivation check (no
    contract change, but it fails once a child is itself reprocessed) or an additive origin header.
    `contracts/channel-spec/v0/README.md` 3.3.2 split rules 5-6 and section 9.
