# S5: ordering, blocking retry, transactions and DLQ

Status: MEASURED 2026-09-20 by platform-engineer against the shared 3-broker spike cluster. **ACCEPTED 2026-09-21 by architect** at the P0-3 synthesis (`docs/spikes/PHASE-0-DECISION.md`): **PASS**, and the recommendation "GO for the ordering and retry design in ADR-001 6.4" is adopted. No invariant changes — invariant 5 stands on 0 measured violations. All three recommended defaults are written down (ADR-002 D5.1 for `session.timeout.ms`, ADR-003 D1/D3/D6/D7 for the retry policy, transactions and rebalance behaviour; `commit_every` in `contracts/topics/v0` 3.3). `retry-topic` is recorded as **not implemented and not measured**, and no destination may select it until it is spiked (ADR-003 D2).

**Threshold result: per-key order preserved, zero violations.** No counterexample in 2,000
randomised fault schedules against the model, 12 real-cluster fault-injection scenarios, or 13
real-cluster integration tests. `reordered_key_count` was 0 in every run, including the runs
where a kill replayed 12 messages and where partitions changed hands mid-flight.

### Which runs this report quotes

`code-reviewer` rated most S5 claims WEAKLY SUPPORTED on the first run set, because several
scenarios could pass without exercising the fault they are named after. That was correct. The
tables below therefore distinguish three run sets under `spikes/S5-ordering-retry/runs/`:

| Run set | What it is | Status |
|---|---|---|
| `*-14xxxx` (14:02-14:20) | First `s5.runner` run set, 2,000 messages | Superseded for `kill`, `static_restart`, `rebalance`; still the source for the other scenarios |
| `*-142xxx` (14:25-14:31) | The `pytest -m integration` run set, 600 messages | Correctness only; never quoted for numbers |
| `*-19xxxx` and `*-20xxxx` (19:29-20:20) | Re-run after the review, 2,000 messages | **The source for `kill`, `static_restart`, `rebalance`, `txn_crash`** |

Every number below names its run set where the two differ. The two High findings and what they
changed are in "What the first runs did not demonstrate".

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
| `kill` (re-run) | 2012 | 2000 | **12** | 0.60% | 0 | yes | 0/0 | 131 | `commit_every=20`, killed mid-window; takeover gap **10.01 s** |
| `static_hold` | 2000 | 2000 | 0 | 0% | 0 | yes | 0/0 | 81 | takeover gap **11.32 s**, nobody replaced the dead member |
| `static_restart` (re-run) | 2012 | 2000 | **12** | 0.60% | 0 | yes | 0/0 | 124 | recovery **3.21 s** against 2.0 s downtime |
| `rebalance` (re-run) | 2000 | 2000 | 0 | 0% | 0 | yes | 0/0 | 141 | joined a **stable** pair; exactly 2 of 6 partitions moved, joiner delivered **540** |
| `broker_kill` | 2002 | 2000 | **2** | 0.10% | 0 | yes | 0/0 | 53 | 7.7 s to healthy; both workers dropped out of the group |
| `txn` | 2000 | 2000 | 0 | 0% | 0 | yes | - | - | 40 transactions, commit p99 78 ms |
| `txn_crash` (re-run) | 2000 | 2000 | 0 | 0% | 0 | yes | - | - | **50 records proven inside an open transaction at the kill**; restarted on the same `transactional.id` |

`commit_after_delivery_violations` was empty and `phi_findings` was empty in all twelve.
`undrained_partitions` was empty in all except `halt`, where three partitions are *supposed* to
stop (committed 39/95/171 against high watermarks 213/227/308).

### Ordering under fault injection, in detail

- **Model layer:** 500 hypothesis examples per property x 4 properties = **2,000 randomised
  fault schedules**, 0 counterexamples, 250 s. At the default 60 examples the same suite runs in
  31 s on every commit.

  What those 2,000 examples actually exercised, measured rather than assumed (printed by the
  suite; a floor of 25% is asserted so the harness cannot silently regress to vacuity):

  | Coverage over 2,000 examples | Value |
  |---|---|
  | Scheduled faults that actually fired | **1,339 / 1,396 (95.9%)** |
  | Examples requesting a fault where at least one fired | **904 / 904 (100%)** |
  | Examples that paused a partition | 956 |
  | Examples that dead-lettered | 753 |
  | Examples where a crash caused a **replay** (duplicates) | **209** |

  The last row matters most and was 0 before this was measured. The property suite hard-coded
  `commit_interval_s=0.0`, which made the time-based rule commit after every single delivery and
  collapsed every commit window to one message, so no crash ever replayed anything and the
  ordering property was never tested against a replay. The commit window is now drawn by the
  strategy (1, 3 or 8); 209 examples replay, and order held in all of them.
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
| Static member killed, replacement joins (`kill`) | 10.01 s | Bounded below by `session.timeout.ms` (10 s). The replacement cannot take the partitions until the dead member's session expires |
| Static member killed, nobody replaces it (`static_hold`) | 11.32 s | Same bound; the survivors pick the partitions up after expiry |
| Static member killed and restarted with the same `group.instance.id` (`static_restart`) | 3.21 s for 2.0 s downtime | No session-timeout wait. Rejoin overhead ~1.2 s |

So static membership makes planned restarts cheap and unplanned deaths cost a full session
timeout. For an ordered channel that gap is a hard stop on a partition, not a throughput dip:
`session.timeout.ms` is the recovery-time floor for ordered delivery and should be chosen as
such, not left at the 45 s default.

**Independent confirmation from S4.** qa-engineer measured static-member removal bounded at
43.8 s against the 45 s default `session.timeout.ms`. That is the same relationship this spike
measured at 10 s (10.01 s and 11.32 s), from a different harness on a different workload. Two
independent measurements agreeing that removal tracks `session.timeout.ms` is the basis for
recommending the default be lowered for ordered channels.

### Blocking retry cost

`blocking_retry` ran 598 pause/seek/resume cycles across two workers with a 25% per-attempt
failure rate. Zero loss, zero reordering, zero corrected offset skips. Delivery span 68.1 s
against the baseline's 13.3 s: about **169 ms per cycle**, of which only ~14 ms was the
configured backoff. The remaining ~155 ms is the loader's `poll_timeout_s=0.2` - resume is
checked at the top of the poll loop, so the poll timeout, not Kafka, sets the retry latency
floor. That is a fixable implementation detail, not a property of the approach (follow-up 1).

### Cooperative rebalance (re-run, `rebalance-202000`)

A third member joined a **stable** pair, with `founders_stable_before_join: true` recorded by
waiting until both founders owned partitions before starting it. The journal attributes the
movement unambiguously:

```
402.595  worker-a  assign  [0,1,2,3,4,5]
405.690  worker-b  assign  []            <- b joins
405.696  worker-a  revoke  [0,1,2]
408.800  worker-b  assign  [0,1,2]       <- founders now stable: a=[3,4,5], b=[0,1,2]
411.901  worker-c  assign  []            <- c joins
411.908  worker-a  revoke  [3]           <- caused by c
411.909  worker-b  revoke  [0]           <- caused by c
414.993  worker-c  assign  [0,3]
```

| Evidence | Value |
|---|---|
| Founders stable before the joiner started | **yes** |
| Partitions moved, caused by the joiner | **2** (0 and 3) |
| Revokes while running, after the join | worker-a: [3]; worker-b: [0] |
| Records delivered by the joining member | **540** |
| Duplicates / losses / reorderings | 0 / 0 / 0 |

**Cooperative-sticky moved the minimum.** Six partitions across three members is two each, and
exactly two partitions moved -- one from each founder -- while the other four were never revoked
and kept flowing throughout. That is the difference from eager assignment, which would have
revoked all six and stopped the channel. For an ordered channel this is the property that
matters: adding capacity stalls only the partitions that actually change hands.

**Zero duplicates here is the correct result, not a weak one.** A cooperative revoke is a planned
handover: `_on_revoke` commits before the partitions are given up, so the new owner starts exactly
where the old one stopped and nothing is replayed. That is the contrast with `kill`, where the
dying worker commits nothing and 12 messages are replayed. Both are the design working.

The earlier run of this scenario (`rebalance-200152`) could not support this claim: worker-b's own
join caused the revoke three seconds before worker-c was assigned anything, so the movement could
not be attributed to worker-c. It is superseded, and the fix that made the difference --
`wait_for_stable_assignment` -- is why this run's attribution is sound.

### Transactions

`txn`: 2,000 consumed, 2,000 produced, 40 transactions of 50, 0 aborts, 0 abortable and 0
retriable errors, transaction commit p50 <1 ms and p99 78 ms.

`txn_crash` (re-run, `txn_crash-200240`): the stage was killed mid-flight and restarted with the
same `transactional.id`. Measured **between the kill and the restart**, which is the only window
in which an open transaction is observable:

| Read of `out` at the moment of the kill | Records |
|---|---|
| `read_uncommitted` | 500 |
| `read_committed` | 450 |
| **Inside the open transaction** | **50** |

So a transaction holding 50 records really was open when the process died: those records were
written but invisible to a `read_committed` reader. After the restart the output topic held
**2,000 records with 0 duplicates and 0 losses**, so `init_transactions` in the successor fenced
the dead producer and aborted that transaction, and its input offsets rolled back with it -- the
restarted process re-consumed 1,550 records (the first incarnation had committed 450) and produced
each exactly once. Exactly-once holds across a kill for a Kafka-to-Kafka stage, which is the only
place ADR-001 claims it.

Without the uncommitted-vs-committed read this scenario could have killed the stage between
transactions and still passed; the run now fails if no transaction was open.

### Lifecycle events and PHI

Events were emitted for every stage transition and read back per run: `baseline` produced
exactly 2,000 `pending` plus 2,000 `sent`; the transactional stage produced 2,000 `transformed`.
`traceparent` was propagated on every event. Scanning every event body for `TRIBUTARY-SYNTH`,
`MSH|` and `PID|` found nothing in any run, so no payload reached the archive path (invariant 6).

### What the first runs did not demonstrate

`code-reviewer` found that several scenarios could pass without exercising their fault. Recorded
here because the first version of this report quoted them as if they had.

**1. The kill scenarios replayed nothing (High).** `kill` and `static_restart` ran with
`commit_every=1`, so the uncommitted window was a single message and a kill landed between
commits. They reported 0 or 1 duplicates, which measured *takeover time* but not "replay from
the last commit preserves order". Fixed by committing every 20, and then by a second problem:
the first re-run still reported 0 duplicates because the victim happened to be killed at its
260th delivery, exactly 13x20, immediately after a commit. The kill now triggers on the victim's
own delivery count sitting mid-window. The re-run replays 12 messages and the verdict now
*requires* duplicates in these scenarios, so a run that replays nothing fails instead of passing.

**2. Per-worker `redeliveries` is 0 across a kill by construction.** Each worker has its own
`FileSink` with its own `_seen` set, so a successor cannot know that the dead worker already
delivered a record. Duplicates are only visible in the **merged** delivery log across all
workers. A reader looking at per-worker stats will always see `redeliveries: 0` after a kill,
and that is not evidence that no replay happened. The duplicate counts quoted in this report all
come from the merged log.

**3. The rebalance evidence was unfalsifiable (High).** The cluster scenario
(`rebalance-141321`, 2,000 messages) *did* move partitions: worker-c delivered 323 records. The
integration-test run (`rebalance-142637`, 600 messages) did **not**: worker-c delivered 0, joined
too late, and the test still passed because it asserted only that revoke counts were non-zero --
and every worker revokes its partitions on clean shutdown. So High 2 applies to the integration
test, not to the cluster scenario, but the evidence for both was worthless either way. Workers
now write a timestamped assign/revoke/lost journal and count `revokes_while_running`, so a
rebalance revoke is distinguishable from a shutdown revoke, and the joining member starts at a
tenth of the run. **Resolved in two steps.** The first re-run (`rebalance-200152`) showed
revoke-and-resume mid-run was safe but could not attribute the movement: the journal showed
worker-b's own join caused the revoke three seconds before worker-c was assigned anything. After
adding `wait_for_stable_assignment`, the second re-run (`rebalance-202000`) has the founders
stable before the joiner starts, and the journal attributes exactly 2 moved partitions to
worker-c's join.

**4. Commit-after-delivery on the cluster was a final-state check.** It compared the last
committed offset against the whole delivery log, which cannot catch a commit that ran ahead of
its delivery and was then caught up with. The online version exists in the model (the commit
hook). The re-run adds a sampler that polls committed offsets every 250 ms through the kill
window: 144 samples in `kill`, zero violations.

**5. `txn_crash` did not prove a transaction was open at the kill.** It could have killed the
stage between transactions and still passed. The re-run reads `out` with `read_uncommitted` and
`read_committed` between the kill and the restart; the difference is the open transaction, and
the run now fails if there was none. **Resolved by the re-run**: 50 records were inside an open
transaction at the kill (`txn_crash-200240`).

**6. Property tests never asserted a fault fired.** A fault scheduled for poll 30 never runs if
the loader finishes at poll 12, so small examples could be vacuous. Firing is now counted,
surfaced through `hypothesis.event()`, and asserted in a dedicated test.

**7. There was no mutation control.** Added: three deliberately broken loaders and a test
asserting the checks **catch** each one. A suite that cannot go red is decoration, and this is
the only evidence offered here that the checks have teeth.

| Mutant | Bug it reintroduces | Check that must fail |
|---|---|---|
| `_SkippingLoader` | Resumes without seeking back to the failed offset | no-loss |
| `_CommitBeforeDeliveryLoader` | Stores and commits before the downstream write | commit-after-delivery |
| `_ReorderingLoader` | Delivers each adjacent pair of offsets in the wrong order | **per-key ordering** |

The third was added after review: the first two produce loss and a premature commit, neither of
which is a reordering, so nothing previously demonstrated that the *ordering* check -- the
headline claim of this spike -- is capable of failing. All three are caught.

**8. The property runs never replayed anything.** Covered in the coverage table above: a
hard-coded `commit_interval_s=0.0` collapsed every commit window to one message, so no crash in
2,000 examples ever caused a redelivery. Fixed by drawing the commit window from the strategy;
209 of 2,000 examples now replay, with order preserved in all of them. This is the model-layer
twin of High 1 and was found by measuring fault coverage rather than by the review.

**Two limits that remain, by design:**

- **A redelivered sequence number is not counted as a reordering**
  (`spikes/common/src/tbcommon/checks.py`). Ordering is judged on the *first* delivery of each
  `seq` per key. This is correct under at-least-once -- a replay is a duplicate, not a reordering
  -- but it means the sequence 1, 2, 3, 1, 3, 2 passes the ordering check. A sink that is not
  idempotent by `tb-msg-id` would see that as reordered. Invariant 4 requires that idempotency.
- **The property tests exercise one loader at a time**, with successors run sequentially after it
  stops. They therefore say nothing about two workers delivering concurrently. Concurrent-worker
  ordering rests entirely on the cluster scenarios, which run 2-3 real worker processes at once.

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
| **Per-key order preserved across rebalances and consumer kills** | 0 violations in 2,000 model fault schedules, 13 cluster scenarios, 13 integration tests; now including a 12-message replay after a kill | **PASS** |
| Duplicate rate reported | 0.60% (`kill` and `static_restart`, `commit_every=20`), 0.10% (`broker_kill`), 0% without a fault; bounded by `commit_every` | **PASS** (reported) |
| Commit-after-delivery verified | 0 violations; online in the model (commit hook) and sampled every 250 ms through the kill window on the cluster (144 samples) | **PASS** |
| Blocking retry with partition pause and resume | 598 real cycles, 0 loss, 0 skips; seek-before-resume proven necessary by measurement, and a mutant that omits it is caught | **PASS** |
| Cooperative-sticky and static membership | Static cost quantified in both directions and corroborated by S4. A member joining a stable pair moved exactly 2 of 6 partitions, one from each founder, leaving the other 4 uninterrupted; the joiner delivered 540 records with 0 duplicates, 0 losses and 0 reorderings | **PASS** |
| Transactional Kafka-to-Kafka stage | Exactly-once held across a kill, with 50 records proven to be inside an open transaction at the moment of the kill | **PASS** |
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
