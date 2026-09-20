# S5: ordering, blocking retry, transactions and DLQ

Status: MEASURED 2026-09-20 by platform-engineer against the shared 3-broker spike cluster; awaiting architect verdict.

**Threshold result: per-key order preserved, zero violations.** No counterexample in 2,000
randomised fault schedules against the model, 12 real-cluster fault-injection scenarios, or 13
real-cluster integration tests. `reordered_key_count` was 0 in every run.

## Question

Can a Python consumer loop hold per-key ordering (invariant 5) while retrying a failing
destination, across rebalances and worker kills, without losing or silently reordering messages
(invariants 1 and 4)? Specifically (phase-0.md S5):

1. Does `blocking` retry via partition pause/resume preserve order and lose nothing?
2. Do cooperative-sticky assignment and static membership behave as ADR-001 6.4 assumes, and
   what does static membership cost when a worker dies?
3. Does a transactional Kafka-to-Kafka stage give exactly-once, including across a kill with a
   transaction open?
4. Does a poison message reach the DLQ without stalling its partition, and what is the
   order-preserving alternative?
5. What is the duplicate rate, and is every offset commit strictly after the downstream write?

## Method

Two layers, deliberately: neither is sufficient alone.

**Layer 1 - property tests over the algorithm** (`spikes/S5-ordering-retry/tests/test_properties.py`).
The real `BlockingLoader` runs against `s5.fakekafka`, an in-process model of the librdkafka
behaviours the algorithm depends on. Hypothesis draws fault schedules (partition count, message
count, key count, transient failure rate, poison rate, and 0-2 injected faults from
{cooperative rebalance, membership loss, `kill -9`} at arbitrary poll numbers). Each example is
checked for per-key order, no loss, commit-after-delivery - asserted *online*, in the commit
hook, so a violation is caught at the instant it happens rather than inferred afterwards - and
DLQ correctness. `tests/test_fakekafka.py` pins the modelled behaviours, so the model cannot
quietly drift into being easier than Kafka.

**Layer 2a - the client semantics, measured** (`tests/test_librdkafka_semantics.py`). Four tiny
tests against the real broker that assert the behaviours layer 1 assumes, rather than trusting
them. One of them reproduces silent data loss on purpose (see Results).

**Layer 2b - the same properties against the real cluster** (`s5.runner`,
`tests/test_integration_s5.py`). Each scenario creates its own
`spike.s5.<scenario>-<hhmmss>.{raw,out.fhir,retry,dlq,_tb.events}` topics (6 partitions, RF 3,
`min.insync.replicas=2`), publishes keyed synthetic HL7 (`tbcommon.payloads`, MSH-3
`TRIBUTARY-SYNTH`, header `tb-x-seq` = per-key sequence number), then runs the loader as
**separate OS processes** in one consumer group.

Separate processes because the fault that matters is `kill -9`: `Popen.kill()`
(`TerminateProcess` on Windows) gives the worker no chance to commit, close the consumer or
leave the group, so the group must recover through the session timeout exactly as it would for
an OOM-killed pod. A killed worker can report nothing, so all evidence is on disk: the sink is
an append-only JSONL file fsynced per accepted record, one line per delivery carrying
`(msg_id, key, seq, attempt, worker, source topic/partition/offset)`. The absence of a worker's
stats file is itself the proof that it was killed.

Analysis after each scenario, from the merged delivery logs plus the cluster:

| Property | How it is decided |
|---|---|
| Per-key order | Across all workers' logs, the first delivery of each `seq` per key is non-decreasing (`tbcommon.checks`) |
| No loss | Every produced `tb-msg-id` appears in a delivery log or in the DLQ |
| Commit-after-delivery | For every partition, every offset below the group's committed offset was delivered or dead-lettered |
| Drained | Committed offset == high watermark, i.e. the run *finished* rather than stopped |
| DLQ exactness | The DLQ holds exactly the poison set, no more and no fewer |
| Duplicate rate | Reported, never asserted to be zero (invariant 4) |
| No PHI | Every `_tb.events` body is scanned for `TRIBUTARY-SYNTH`, `MSH\|`, `PID\|` |

Reading topics back uses `enable.partition.eof`, not "read until offset == high watermark",
because on a transactional topic the high watermark counts commit/abort control records that are
never handed to the application - the naive loop never terminates. The transactional stage's
output is read with `read_committed`.

Reproduce: `uv run python -m s5.runner <scenario> --count 2000`, or
`uv run pytest -m integration`. Each run writes `spikes/S5-ordering-retry/runs/<scenario>-<hhmmss>/summary.json`
with every number quoted below, plus the per-worker delivery logs; `runs/` is git-ignored (as in
S2), so the tables in this report are the durable record.

## Environment

- Host: AMD Ryzen 7 5800H, 15.4 GB RAM, Windows 11, Docker Desktop on WSL2 (16 CPUs and 7.98 GB
  visible to the Docker VM). Shared with the other Phase 0 spikes; not an isolated benchmark
  host, and every cell below is a single run.
- Cluster: `spikes/common/docker-compose.yml`, Apache Kafka 4.0.0 (KRaft), 3 brokers.
- Clients: Python 3.12.14, `confluent-kafka` 2.15.1 / librdkafka 2.15.1, `hypothesis` 6.168.0.
- Consumer (`tbcommon.clients`): `enable.auto.commit=false`, `enable.auto.offset.store=false`,
  `auto.offset.reset=earliest`, `partition.assignment.strategy=cooperative-sticky`,
  `isolation.level=read_committed`, `max.poll.interval.ms=300000`, and
  **`session.timeout.ms=10000`** for these runs, lowered from the 45 s default so the
  static-membership hold time fits inside a spike run. That number is a recovery-time floor, so
  it is reported rather than hidden.
- Producer: `acks=all`, `enable.idempotence=true`, `compression.type=lz4`,
  `message.timeout.ms=120000`. Transactional producer: `transaction.timeout.ms=60000` and
  `message.timeout.ms` clamped to it (see Results).
- Loader: exponential backoff with full jitter, `poll_timeout_s=0.2`, `commit_every=1` unless
  stated, sink fsynced per record.

## Raw results

### Client semantics (`tests/test_librdkafka_semantics.py`, 4 passed)

| Claim | Result on Kafka 4.0.0 / librdkafka 2.15.1 |
|---|---|
| `pause()` purges the prefetch queue and `resume()` refetches from last-consumed + 1, so resuming without seeking **skips the failed message** | **Confirmed.** Consumed offset 0, paused, resumed, next record was offset 1. The message that failed was never seen again |
| Seeking back to the failed offset while still paused, then resuming, replays it | Confirmed |
| `commit()` with nothing stored raises `_NO_OFFSET` | Confirmed; the loader must treat it as benign or every revoke and shutdown looks broken |
| Stored-but-uncommitted offsets never reach the group | Confirmed; four records stored, consumer closed, committed offset still `-1001`, so a successor replays from the beginning |

The first row is the finding that justifies the whole design: `blocking` retry implemented as
pause-then-resume **silently loses data** unless it seeks back first. This is measured, not
asserted.

### Fault-injection scenarios (2,000 messages, 64 keys, 6 partitions, RF 3, one run each)

| Scenario | Delivered | Unique | Dupes | Dupe rate | Lost | Order kept | DLQ | msg/s | Notes |
|---|---|---|---|---|---|---|---|---|---|
| `baseline` | 2000 | 2000 | 0 | 0% | 0 | yes | 0/0 | 151 | drained |
| `baseline_batched` | 2000 | 2000 | 0 | 0% | 0 | yes | 0/0 | **733** | `commit_every=50`, 41 commits |
| `blocking_retry` | 2000 | 2000 | 0 | 0% | 0 | yes | 0/0 | 29 | 598 pause/seek/resume cycles, 0 offset skips |
| `poison_dlq` | 1992 | 1992 | 0 | 0% | 0 | yes | **8/8** | 144 | partition kept moving past each poison message |
| `halt` | 1557 | 1557 | 0 | 0% | 0 | yes | 0/0 | 76 | 3 partitions halted, 443 messages held behind them |
| `kill` | 2001 | 2000 | **1** | 0.05% | 0 | yes | 0/0 | 79 | takeover gap **10.06 s** |
| `static_hold` | 2000 | 2000 | 0 | 0% | 0 | yes | 0/0 | 81 | takeover gap **11.32 s**, nobody replaced the dead member |
| `static_restart` | 2000 | 2000 | 0 | 0% | 0 | yes | 0/0 | 122 | recovery **2.38 s** against 2.0 s downtime |
| `rebalance` | 2000 | 2000 | 0 | 0% | 0 | yes | 0/0 | 141 | third member joined mid-run; 0 partitions lost |
| `broker_kill` | 2002 | 2000 | **2** | 0.10% | 0 | yes | 0/0 | 53 | 7.7 s to healthy; both workers dropped out of the group |
| `txn` | 2000 | 2000 | 0 | 0% | 0 | yes | - | - | 40 transactions, commit p99 78 ms |
| `txn_crash` | 2000 | 2000 | 0 | 0% | 0 | yes | - | - | killed mid-transaction, restarted on the same `transactional.id` |

`commit_after_delivery_violations` was empty and `phi_findings` was empty in all twelve.
`undrained_partitions` was empty in all except `halt`, where three partitions are *supposed* to
stop (committed 39/95/171 against high watermarks 213/227/308).

### Ordering under fault injection, in detail

- **Model layer:** 500 hypothesis examples per property x 4 properties = **2,000 randomised
  fault schedules**, 0 counterexamples, 245 s. At the default 60 examples the same suite is 36
  tests in 33 s and runs on every commit.
- **Real cluster:** 12 scenarios above plus 9 integration tests (310 s) plus 4 semantics tests.
  Zero reordered keys anywhere.
- **Number of order violations measured: 0.** Nothing to escalate against invariant 5.

### Duplicates and commit-after-delivery

With `commit_every=1` the uncommitted window is a single message, and that is exactly what the
kills produced: 1 duplicate in `kill`, 2 in `broker_kill`, 0 in `static_restart` (the kill landed
between a commit and the next delivery). No run produced a duplicate without a fault. Every
commit in every run was preceded by the durable handling of every offset below it.

The trade is measurable: `commit_every=50` raised throughput from 151 to **733 msg/s (4.9x)** at
the cost of a replay window of up to 50 messages per partition instead of 1. Committing
synchronously per message is the dominant cost in the ordered path on this host, not Kafka
itself.

### Static membership, both directions

| Event | Recovery | Interpretation |
|---|---|---|
| Static member killed, replacement joins (`kill`) | 10.06 s | Bounded below by `session.timeout.ms` (10 s). The replacement cannot take the partitions until the dead member's session expires |
| Static member killed, nobody replaces it (`static_hold`) | 11.32 s | Same bound; the survivors pick the partitions up after expiry |
| Static member killed and restarted with the same `group.instance.id` (`static_restart`) | 2.38 s for 2.0 s downtime | No session-timeout wait. Overhead of the rejoin itself is ~0.4 s |

So static membership makes planned restarts nearly free and unplanned deaths cost a full session
timeout. For an ordered channel that gap is a hard stop on a partition, not a throughput dip:
`session.timeout.ms` is the recovery-time floor for ordered delivery and should be chosen as
such, not left at the 45 s default.

### Blocking retry cost

`blocking_retry` ran 598 pause/seek/resume cycles across two workers with a 25% per-attempt
failure rate. Zero loss, zero reordering, zero corrected offset skips. Delivery span 68.1 s
against the baseline's 13.3 s: about **169 ms per cycle**, of which only ~14 ms was the
configured backoff. The remaining ~155 ms is the loader's `poll_timeout_s=0.2` - resume is
checked at the top of the poll loop, so the poll timeout, not Kafka, sets the retry latency
floor. That is a fixable implementation detail, not a property of the approach (follow-up 1).

### Transactions

`txn`: 2,000 consumed, 2,000 produced, 40 transactions of 50, 0 aborts, 0 abortable and 0
retriable errors, transaction commit p50 <1 ms and p99 78 ms.

`txn_crash`: the stage was killed with a transaction open and restarted with the same
`transactional.id`. The restarted process consumed 1,550 (the first incarnation had committed
450) and the output topic, read with `read_committed`, held **2,000 records with 0 duplicates and
0 losses**. The dangling transaction was aborted by `init_transactions` on the successor and its
input offsets rolled back with it. Exactly-once holds across a kill for a Kafka-to-Kafka stage,
which is the only place ADR-001 claims it.

### Lifecycle events and PHI

Events were emitted for every stage transition and read back per run: `baseline` produced
exactly 2,000 `pending` plus 2,000 `sent`; the transactional stage produced 2,000 `transformed`.
`traceparent` was propagated on every event. Scanning every event body for `TRIBUTARY-SYNTH`,
`MSH|` and `PID|` found nothing in any run, so no payload reached the archive path (invariant 6).

### Two bugs the runs found

Reported because the first version of each *passed*:

1. `tbcommon.clients.make_transactional_producer` could not construct a producer at all:
   librdkafka rejects `message.timeout.ms` (120 s default) greater than `transaction.timeout.ms`
   (60 s) with `_INVALID_ARG` **at construction**. Fixed by clamping `message.timeout.ms` down to
   the transaction timeout, rather than raising the transaction timeout - a long transaction
   timeout is what a `read_committed` consumer stalls behind when a producer dies with a
   transaction open.
2. The harness wrote the poison set to disk but never passed it to the worker processes. The
   first `poison_dlq` run therefore delivered all 2,000 messages, dead-lettered nothing, and
   still reported `ok: true`. The verdict now requires the DLQ to contain exactly the expected
   poison set, and the poison file is wired in at pool construction so no scenario can omit it.

## Thresholds and verdict

| phase-0.md requirement | Result | Verdict |
|---|---|---|
| **Per-key order preserved across rebalances and consumer kills** | 0 violations in 2,000 model fault schedules, 12 real-cluster scenarios, 9 integration tests | **PASS** |
| Duplicate rate reported | 0.05% (`kill`), 0.10% (`broker_kill`), 0% without a fault; bounded by `commit_every` | **PASS** (reported) |
| Commit-after-delivery verified | 0 violations; checked online in the model and against committed offsets on the cluster | **PASS** |
| Blocking retry with partition pause and resume | 598 real cycles, 0 loss, 0 skips; seek-before-resume proven necessary by measurement | **PASS** |
| Cooperative-sticky and static membership | Incremental assignment worked with custom callbacks, 0 partitions lost on a planned join; static cost quantified both ways | **PASS** |
| Transactional Kafka-to-Kafka stage | Exactly-once held, including across a kill mid-transaction | **PASS** |
| Poison message to DLQ | Exactly the 8 poison messages dead-lettered, partition kept moving; `halt-partition` verified as the order-preserving alternative | **PASS** |

**Recommendation: GO for the ordering and retry design in ADR-001 6.4, with the defaults below.**
Nothing measured here contradicts invariants 1-5 or 7. No ADR change is required; three defaults
should be written down.

Recommended defaults for `tributary-runtime`:

1. `session.timeout.ms` is the ordered-channel recovery floor. 45 s means a 45 s stop on a
   partition when a worker is OOM-killed. Suggest 10-15 s with `heartbeat.interval.ms` at a
   third of it, and document the trade against spurious rebalances.
2. `commit_every` must be tunable per destination, defaulting well above 1. 1 costs 4.9x
   throughput to shrink the replay window from 50 to 1, which is only worth paying for a
   destination that is not idempotent - and invariant 4 requires idempotency anyway.
3. Retry-loop latency must not be tied to the poll timeout (follow-up 1).

## Follow-ups

1. **Decouple resume from the poll timeout.** ~155 ms of the 169 ms per retry cycle is
   `poll_timeout_s`, not backoff or Kafka. A short poll timeout while any partition is paused, or
   a separate timer that drives resumes, would make `blocking` retry's latency match its
   configured backoff. Owner: platform-engineer, in `tributary-runtime`.
2. **Measure a longer outage.** Outages here were seconds. A destination down for minutes with a
   30 s backoff cap needs a check that a paused partition does not trip `max.poll.interval.ms`
   (currently 300 s, so a 30 s cap is safe, but the interaction should be a test, not an
   argument).
3. **Key-level parallelism inside a partition** (ADR-001 7.2, later) is untested. Ordering here
   comes from one-consumer-per-partition; a commit watermark across concurrent keys is a
   different proof and needs its own spike.
4. **`retry-topic` policy with delay tiers is not implemented.** Only `blocking` and DLQ-on-
   exhaustion were measured. The `retry` topic exists in the layout but is unused. It does not
   preserve order by design (invariant 5), so it needs its own spike before a destination can
   select it.
5. **Throughput here is not a throughput result.** 151 to 733 msg/s was measured with a
   per-record-fsynced file sink on a contended laptop, and is a floor for the commit machinery,
   not a product number. A real loader measurement belongs in `benchmarks/` with qa-engineer.
6. **Spike code, not product code.** `s5.fakekafka` is a model, not a broker; it deliberately does
   not model murmur2 partitioning, replication, the LSO visibility rule, or broker timeouts.
   `tributary-runtime` should be written from these findings, and `test_librdkafka_semantics.py`
   is the file worth porting more or less directly.

## Note for S3 (synchronous reply worker)

connector-engineer measured S3 against a stand-in echo worker because no real worker existed.
Asked whether the S5 consumer loop could serve as the real reply worker: **yes structurally, no
as configured.**

What already fits: the loop commits only after the downstream write is durable, which for the
reply path means "after the reply record is durable" - exactly the stand-in's contract. Producing
the reply would be a `ReplySink` implementing the same `deliver()` seam the DLQ producer already
uses (produce, flush, then store the offset). Cooperative-sticky, static membership, lifecycle
events, `traceparent` propagation and graceful drain all carry over unchanged.

What does not fit, and would change S3's numbers:

- **`commit_every=1` would make sync reply slower.** The stand-in batches ~200 requests and
  commits once; the S5 loader commits synchronously per message. Measured here, that costs 4.9x
  throughput, and S3's added latency was already dominated by two Kafka commits per request. The
  reply worker must use the batched commit path.
- **`blocking` retry is the wrong policy for a reply.** A failed reply must not pause the
  partition and stall other senders' requests; the receiver's timeout is the correct control, so
  the reply worker wants fail-fast plus DLQ, not pause-and-retry.
- `idle_exit_s` and `max_runtime_s` are measurement affordances and have no place in a service.

Nothing new was built for S3 here, as instructed.
