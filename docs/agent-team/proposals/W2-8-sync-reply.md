# W2-8 proposal: synchronous reply mode (contract, phase 1)

**Status:** Proposed 2026-09-28 by `platform-engineer`, for `connector-engineer` (co-owner) and
`architect` (contract approval). No implementation until approved.
**Sources:** ADR-001 6.5, ADR-003 D8/D9, `docs/spikes/S3-sync-reply.md`, Phase 0 memo 8.3,
`channel-spec/v0` 3.2.2 and 9, `topics/v0` 1/3.6, `envelope/v0` 3/3.2 and `error-codes.json`.
**Target:** contract set 1.2, additive only. Nothing valid under 1.1 becomes invalid.

## 1. The `reply` block

`spec.source.reply` sits beside `ack` on sources that have a synchronous sender. **The block's
presence switches sync mode on.** Without it, the channel keeps the async after-commit path.
`ack.mode` stays the one-member enum `after-commit`, because sync mode changes what the sender waits
for, not when the commit happens.

| Field | Req | Default | Validation | Meaning |
|---|---|---|---|---|
| `timeoutMs` | O | 10000 (ADR-001 6.5) | 1000-60000 | Wait for the reply, counted from the receiver's raw delivery callback. Expiry answers `AE` / `REPLY_TIMEOUT` |
| `after` | O | `routed` | one-member enum; `delivered` reserved | `routed`: every `out.<dest>` record is acknowledged by Kafka, or deliberately dropped. `delivered` (a loader's outcome) needs a loader WRITE on `reply` and a choice of destination. Deferred |
| `body` | O | `ack` | `ack` or `step` | `ack`: the receiver builds the protocol ACK from the reply status. `step`: a step supplies the response payload through `ctx.reply` (query/response, section 2.3). `step` may land after `ack` |
| `maxPending` | O | 10000 | 1-50000 | Cap on in-flight requests per receiver process. S3 measured about 4.6 KB each, so 10k is about 46 MB. Over the cap: `AR` / `REPLY_BUSY`, nothing published |
| `partitions` | O | 3 (`topics/v0` default) | 1-64 | Partition count of `<env>.<ch>.reply`. **Must be at least the number of receiver processes** (section 4). Counts against the `topics/v0` 2.2 budget |

`additionalProperties: false` and a bound on every value apply, as everywhere else in the spec.

**Which receivers support it.** The schema allows `reply` on `mllp` (first, since it is built) and
`http` (after W2-7 lands). It forbids `reply` on `sftp` and `kafka`, which have no waiting sender.
Until the `http` receiver implements sync mode, a spec that selects it is refused at receiver
startup, the same arrangement as 3.0 in the channel spec.

**Cross-field rules** (code rules, enforced by the control plane and at component startup):

1. `reply` with `pipeline.transactional: true` is refused. A reply produced inside the transaction
   stays invisible until the batch commits (up to `commit_every` = 50 records). A reply produced
   outside the transaction could say `AA` for work that is then aborted. The rule can be relaxed
   later once the transactional reply is measured.
2. `reply` with a split step is refused. One request would have N outcomes, and turning them into
   one answer is an aggregator's job, which does not exist yet.
3. `reply.partitions` below the deployed receiver replica count is a warning at deploy. At runtime,
   a receiver process that holds no reply partition is not ready (section 4).

## 2. End-to-end flow

1. **Receiver.** Per request it mints `tb-correlation-id` (UUID, envelope 3.2), sets
   `tb-reply-partition` to a reply partition it owns and sets `tb-reply-deadline`. It records
   `{correlation id -> tb-msg-id, sender control id, connection slot, deadline}` in its in-memory
   pending map. The sender's MSH-10 lives **only** in that map, never in a header. The receiver
   produces to `raw` (`acks=all`, idempotent, unchanged). The request payload is released once the
   raw record is committed. Only the fields needed to build the ACK stay in memory.
2. **Commit.** The raw delivery callback succeeds and the timeout clock starts. If it fails, the
   receiver answers the Kafka-down row of 2.2, which is the existing path.
3. **Worker.** It runs the pipeline and destination steps, produces `out.<dest>` and waits for their
   delivery callbacks (the current `handle()` contract), then produces one reply record to
   `<env>.<ch>.reply` on the partition named by `tb-reply-partition`, keyed by `tb-correlation-id`
   (envelope 4). **Whether to reply is decided by the record, not the spec.** The worker replies iff
   the raw record carries `tb-reply-partition`, so hot-reloading a channel between sync and async
   never strands a request.
4. **Receiver.** Its reply consumer looks up `tb-correlation-id` and drops the record unless it is in
   the map and `tb-msg-id` matches. A match resolves the entry. The per-connection writer answers
   the sender.
5. **AA requires both** the receiver's own raw delivery callback and an `accepted` reply. A reply can
   in principle be read before the receiver has served its delivery callback, so the reply alone is
   never enough.

### 2.1 Worker rules (ADR-003 D9, fail-fast plus DLQ, batched commit)

- **Batched commit.** The reply is produced per record, but offsets still commit every
  `commit_every` records or on idle. Reply latency does not wait for the offset commit. After a
  worker crash, up to `commit_every` records replay. Their replies arrive late and the receiver
  drops them as unknown ids, and their `out` duplicates keep the same `tb-msg-id`, which loaders
  absorb.
- **Fail-fast.** Producing a reply is never retried and never pauses a partition. If the reply
  delivery fails, the worker increments a metric and emits a `replied` event with `status: error`
  and code `PRODUCE_FAILED`. It still commits the offset: the message was routed, and only the
  answer is lost. The receiver then times out, and `AE` is the truthful answer.
- **DLQ, then reply.** A `Reject` or an exhausted step goes to `dlq` as today. After the `dlq`
  record is acknowledged, the worker produces the reply with `rejected` or `failed` and the code.
- **Status rule.** `accepted` only if every destination was produced or deliberately dropped. Any
  dead letter, including one destination's step failure, answers with the first failing code. A
  whole-message `Drop` is `accepted`: the message was committed and filtered on purpose. **Open
  point for review:** some channels may want a filtered message refused.
- **Expired replies are skipped.** If the time is past `tb-reply-deadline` plus 5 s of clock-skew
  allowance, the worker produces no reply and counts `replies_expired`. It still processes the
  message. After an outage, this keeps a backlog from flooding `reply` with answers nobody waits for
  (S3: 1,026 late replies after one restart).
- The reply producer is the worker's existing idempotent producer (`acks=all`). Losing a reply is
  safe, since it ends as `AE`, so a weaker `acks` setting could cut latency. It is not proposed
  without a measurement.

### 2.2 What the sender gets

**Rule for integrators: `AR` means the engine did not confirm a commit, so resending cannot lose
data. `AE` means committed, so resending creates a duplicate.** `AA` is never sent before the
commit. One exception to `AR`: when the publish wait times out, librdkafka may still complete the
write, so a resend after that `AR` can duplicate, though it can never lose data. The MLLP `MSA-2` is
the sender's MSH-10, taken from the map. `MSA-3` is `<CODE> <tb-msg-id>` and never payload or
exception text.

| Case | Committed | MLLP | HTTP (proposal for W2-7) |
|---|---|---|---|
| Success (`accepted`) | yes | `AA` (or the step body) | `200`, body = `tb-msg-id` or the step body |
| Pipeline refusal (`rejected`, a `Reject` code) | yes | `AE` + code | `422` + code + `tb-msg-id` |
| Pipeline failure (`failed`, e.g. `TRANSFORM_ERROR`) | yes | `AE` + code | `500` + code + `tb-msg-id` |
| Reply timeout | yes | `AE` `REPLY_TIMEOUT` | `202` + `tb-msg-id`: the async meaning, "committed, not known processed" |
| Reply partition revoked with the request in flight | yes | `AE` `REPLY_ROUTE_LOST`, immediately | `202` + `tb-msg-id` |
| Kafka down or publish failed (existing path) | no | `AR` `unavailable` | `503` + `Retry-After` |
| Pending map full / no reply partition held | no, not published | `AR` `REPLY_BUSY` / `REPLY_UNAVAILABLE` | `503` + `Retry-After` |

A pipeline `Reject` answers `AE`, not HL7's `AR`, because it was committed and a resend duplicates
it. The code tells the sender why. Only original-mode ACKs are in scope. Enhanced mode (`CA`) is the
async path.

### 2.3 `body: step` (SDK surface, needs architect approval)

`ctx.reply(body: bytes, *, content_type: str) -> None`. It can be called at most once per message,
and a second call raises. `FakeContext` records the call, so the step stays unit-testable without
Kafka (invariant 7). If `body: step` is set and no step called it, the reply is `failed` with
`REPLY_BODY_MISSING`. In `ack` mode the reply value is zero bytes with
`tb-content-type: application/octet-stream`, and the status header carries the outcome.

## 3. The finding for the architect: D9's mitigation does not cover sender retries

ADR-003 D9 says a sender retry after `AE` "creates a duplicate downstream, therefore every loader
MUST be idempotent on `tb-msg-id`". **The resent message gets a new `tb-msg-id`** (channel-spec
3.2.2, "Duplicates at ingest"), so `tb-msg-id` idempotency does not absorb it. It only absorbs the
worker-replay duplicates in 2.1. The real mitigations are sink-native idempotency on a business
identifier (the W2-3 conditional FHIR identifier) or edge dedupe (D8, deferred). Proposal: for sync
channels, the integrator doc states this plainly, and the control plane warns when every destination
relies only on `tb-msg-id`. **Wording change to D9 requested. No invariant is affected.**

## 4. Consuming `reply`: targeted per-process partitions, assigned by a group

Three options were considered.

| Option | Verdict |
|---|---|
| Plain consumer group, replies keyed by correlation id | **Rejected.** The reply lands on whichever member owns the key's partition, not the one holding the socket |
| Broadcast (ADR-001 6.5 as written): every process reads every partition and filters | **Rejected as the default.** Reads scale as replies x processes. Every process sees every other sender's reply, which carries PHI in `body: step`. Multi-process receivers are a Phase 0 requirement (memo 8.3), so the process count will grow |
| **Targeted**: each process owns reply partitions, puts one in `tb-reply-partition`, and the worker produces to it | **Proposed.** S3 measured it no slower: p50 43 vs 49 ms at 100 msg/s, 85 vs 136 ms at 1k (single run, one instance, called inconclusive by S3) |

**How a process gets a partition without a coordinator (invariant 3).** All receiver processes of
the channel join the consumer group `tb.<env>.<ch>.replier`, using the `replier` role already in
`topics/v0` 3.6. They use cooperative-sticky assignment with `group.instance.id` = the receiver
instance id. Kafka's assignment is the lease. A process advertises a partition only while it holds
it. The reply consumer **never commits** and uses `auto.offset.reset=latest`, since a reply older
than the process's map is useless. It reads `read_uncommitted` because the reply producer is not
transactional (rule 1).

**Readiness.** A process with no assigned partition, or whose reply consumer is not connected,
answers every sync request with `AR` / `REPLY_UNAVAILABLE` **before publishing** and fails its
readiness probe, so the load balancer drains it. That is why `reply.partitions` must be at least the
replica count.

**Rebalance.** On revoke of partition p, the pending entries advertised on p answer `AE` /
`REPLY_ROUTE_LOST` at once instead of waiting out the timeout. Their replies are unreachable, and
the answer is the same one, only sooner. With static membership a planned restart avoids a
rebalance (S5: 3.21 s for a static restart). A killed process has already lost its sockets, so the
15 s session timeout only delays reuse of its partition. It stalls no other process.

**Head-of-line isolation.** The reply consumer runs on its own thread per process. It only resolves
futures (`call_soon_threadsafe`) and never writes to a socket, so a slow or dead sender cannot stall
it. MLLP answers each connection in request order (the existing `_ack_writer` queue), so a slow reply
delays later ACKs on **that connection only**, bounded by `timeoutMs`. Other connections and other
processes are unaffected.

**Latency budget (S3, stand-in worker, Windows laptop, single runs; not a product number).**

| Component | S3 figure |
|---|---|
| Async baseline, raw commit to ACK | p50 8.4 / p99 21.8 ms at 100 msg/s; 36 / 152 ms at 1k |
| Sync targeted, total | p50 43 / p99 86 ms at 100 msg/s; 85 / 250 ms at 1k |
| Added by the reply hop | about +35-40 ms p50 at 100 msg/s, +50-100 ms at 1k |

**The real worker adds a third sequential Kafka commit** (`raw`, then `out`, then `reply`), because
AA means routed. S3's echo worker had two, so expect about one more commit latency on top of S3. The
consumer tuning (`fetch.wait.max.ms`, `linger.ms` on the reply produce) is measured in the test plan,
not guessed here.

## 5. Ordering on per-key channels

- **Order is fixed at the raw commit, not at the reply.** The receiver answers only after the commit,
  so a sender that waits for its ACK before sending the next message keeps its own sequence.
  Partitioning and `blocking` retry are unchanged.
- The worker handles a partition sequentially, so replies from one partition come out in order.
  Replies from different partitions do not, and the per-connection writer restores request order.
- **A slow or failing message delays the replies of every key behind it on that worker partition**,
  including other senders'. The timeout bounds the wait. The pipeline worker has no `blocking` retry
  except on an `out` produce failure (Kafka trouble). Loader-side `blocking` retry is downstream of
  the reply, because `after: routed` does not wait for loaders.
- **A resend after `AE` is the one ordering hazard, and it is sender-induced.** A sender that resends
  A01 at once gives A01, A01'(new id), A08: order kept, duplicate applied. A sender that moves on
  after `AE` and resends A01 later puts it **after** A08. The engine cannot detect this without edge
  dedupe (D8). It goes in the integrator doc with 2.2, as ADR-003 follow-up 4 requires.

## 6. Contract changes to register (set 1.2, all additive)

| Contract | Change |
|---|---|
| `envelope/v0` 3 | `tb-reply-partition` (raw C: set iff sync; decimal integer, not PHI; `–` on `out`/`retry`/`dlq`, where the worker strips it, so a reprocessed record never replies to a partition nobody watches). `tb-reply-deadline` (raw O; RFC 3339). `tb-reply-status` (reply R; `accepted` / `rejected` / `failed`). `tb-error-code` on `reply` changes from `–` to C: required iff status is not `accepted`. This relaxes the reply-class `not` in the schema. `tb-x-reply-partition` from S3 retires |
| `error-codes.json` | Reserve the prefix `REPLY_` for the sync-reply path. Exact codes: `REPLY_TIMEOUT`, `REPLY_ROUTE_LOST` (receiver, `AE`); `REPLY_BUSY`, `REPLY_UNAVAILABLE` (receiver, `AR`); `REPLY_BODY_MISSING`, `REPLY_PARTITION_INVALID` (worker, `tb-reply-partition` out of range: no reply, event only). Reply produce failure reuses `PRODUCE_FAILED` with stage `replied` |
| `channel-spec/v0` | `source.reply` on `mllp` and `http` (section 1), README 3.2.2 rows, 9 closed. Examples: 1 valid, and invalid pairs for `reply` on `sftp`, `timeoutMs: 500`, `after: delivered`, and an unknown field |
| `topics/v0` | ACLs: receiver + READ on `.reply` and on group `tb.<env>.<ch>.replier*`; pipeline_worker + WRITE on `.reply`. 3.6: `replier` = the receivers' reply-consumer group. `reply.partitions` added to the per-channel overrides. Reply consumer settings (no commits, `latest`) in 3.3 |
| `events/v0` | None. The worker emits `replied` (existing stage) with `ok` / `error`. The receiver has no `_tb.events` ACL, so a sender-side `AE` is a metric and a log line with `tb-msg-id` only. **Open:** does the browser need "sender got AE"? That would need a receiver ACL on `_tb.events` |
| SDK | `ctx.reply` (2.3), through the architect |

## 7. Test plan

Every fault test asserts that its fault fired and has a broken-implementation control that turns it
red (Phase 0 rule). Latency is reported as distributions. The only hard gates are correctness and
timeout accuracy.

**Unit, no Kafka.** Schema pairs pinned in the manifest. Pending map with a fake clock: deadline,
cap, late reply, `tb-msg-id` mismatch, and revoke answering `REPLY_ROUTE_LOST`. Worker with a fake
producer: the reply comes after every `out` callback and after the `dlq` callback; each status and
code; a reply-produce failure neither pauses nor blocks the commit; the deadline skip; the reply
decision comes from headers across a hot reload. **Hypothesis:** over random interleavings of raw
callback, reply, timeout, revoke and disconnect, every request gets exactly one answer, and `AA`
never precedes a successful raw callback. Control: a receiver that answers on the reply alone.

**Integration (testcontainers Kafka, `-m integration`).**

1. End to end MLLP to AA, with `MSA-2` matching MSH-10 for 10k messages. Zero wrong replies,
   checked by matching `MSA-2`.
2. **Timeout:** worker stopped, `timeoutMs` 3000. Every answer is `AE` `REPLY_TIMEOUT` within
   +50 ms p99 (S3: about 5 ms). Every AE'd `tb-msg-id` is in `raw`. The map drains to 0. Late
   replies after a worker start are dropped. Control: a receiver whose timer starts at receive.
3. Worker `kill -9` mid-run: 0 lost (every AA'd id in `raw`), 0 wrong replies, AE count and
   duplicate count reported.
4. Three receiver processes: one killed, then one added. The other processes' p99 is unchanged, and
   `REPLY_ROUTE_LOST` stays bounded to the moved partition.
5. Broker leader kill and full Kafka outage: `AR` before publish, never `AA`, 0 lost.
6. Cap: `maxPending` + 1 gives `AR` `REPLY_BUSY` with nothing in `raw`.

**Bench (`-m bench`, `benchmarks/sync_reply/`, with `qa-engineer`).** Rerun the S3 cells with the
real worker, async vs sync at 100 and 1k msg/s, with 1 and 3 receiver processes. Five runs per cell
on Linux. Report p50, p99 and p99.9, the added latency per hop, and RSS per 1k pending.

**Verify, once implemented:** `uv run pytest -m "integration" -k sync_reply` and
`uv run pytest -m bench -k sync_reply`, both green and with the report attached.

**Split of work.** `connector-engineer`: pending map, reply consumer group, MLLP mapping, then HTTP.
`platform-engineer`: worker reply producer, status rules, `ctx.reply`, fake context, the property
test.
