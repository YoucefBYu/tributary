# ADR-002: Topic Topology and Envelope Specification

**Status:** Proposed — **SKELETON. Every number marked `TODO(Sn)` is a placeholder, not a result.**
**Date:** 2026-09-20 (drafted), to be completed at P0-3 synthesis
**Deciders:** architect, with human sign-off at the Phase 0 gate
**Relates to:** ADR-001 sections 5.3, 5.4, 7.2 · invariants 1, 2, 5, 8
**Contracts:** `contracts/topics/v0`, `contracts/envelope/v0`
**Evidence:** `docs/spikes/S2-*.md` (ACK path), `docs/spikes/S4-*.md` (topic count), `docs/spikes/S5-*.md` (ordering) — **none filed at the time of drafting**

---

## 1. Context

ADR-001 sketched a per-channel topic set and a header-based envelope. It left four things open that
every team needs settled before they can write code that interoperates: the exact topic grammar, how
many partitions and topics a cluster tolerates, what metadata travels where, and which headers are
mandatory at which hop.

Forces:

- **Invariant 2** fixes *where* Kafka hops happen (`raw`, `out.<dest>`, `retry`, `dlq`, `reply`,
  `_tb.events`). It does not fix how those are named or sized.
- **Invariant 5** makes ordering explicit, and ordering is a property of partitioning, so partition
  counts are a correctness concern, not a tuning knob.
- **Invariant 8** puts metadata in headers and keeps the payload opaque, which forces every routing
  and retry decision to be expressible in headers.
- ADR-001 7.1 projects 500 channels × ~4 topics × 12 partitions ≈ 24k partitions and calls it
  "workable but needs testing". S4 is that test.

### 1.1 Measured evidence

| Question | Spike | Result |
|---|---|---|
| Partitions and topics a 3-node KRaft cluster tolerates before operations degrade | S4 | TODO(S4) |
| Consumer-group startup and rebalance time at 500 and 1,000 channels | S4 | TODO(S4) — threshold: 500 channels, rebalance under 30 s |
| Broker and controller memory at that topic count | S4 | TODO(S4) |
| p99 receive-to-ACK with `acks=all`, RF=3, `min.insync.replicas=2` | S2 | TODO(S2) — threshold: under 50 ms at 1k msg/s |
| Acknowledged-message loss on partition-leader kill | S2 | TODO(S2) — threshold: zero over 20 trials |
| Whether `_tb.events` at ~4 events per message keeps up | S1/S2 | TODO |

---

## 2. Decisions

### D1. Topic grammar (decided)

`<env>.<channel>.<kind>[.<destination>]` for the data plane, `_tb.<env>.<kind>` for platform topics,
with a restricted lowercase segment charset. Specified in `contracts/topics/v0`.

**Options:** (a) dot-separated with restricted segments — chosen; (b) a single flat name with an
opaque channel id (`tb-7f3a-raw`), stable under rename but unreadable in `kcat` and in ACLs;
(c) unrestricted names matching Kafka's own charset, which breaks `split(".")` parsing and prefix
ACLs the first time a channel is called `adt.to.fhir`.

**Give up:** channel renames change topic names, so a rename is a migration, not an edit. Accepted:
renames are rare and the operational readability of `prod.adt-to-fhir.raw` is worth a recreate.
Mirth imports need slugification (`migration-engineer` owns a reversible, collision-free mapping).

### D2. Platform topics are environment-scoped (decided, deviates from ADR-001 5.3)

`_tb.<env>.channels`, `_tb.<env>.events`, `_tb.<env>.audit` instead of `_tb.channels` and so on.

**Why:** the data-plane names carry `<env>`, which only makes sense if one cluster can host more than
one environment; unqualified platform topics then let a dev control plane publish a channel spec that
production workers consume (invariant 3 makes workers trust that topic completely).

**Give up:** longer names, and a deviation from the invariant-2 wording. The topic *class* is still
`_tb.events`. The alternative is mandating one cluster per environment: simpler names, more
infrastructure, and a silent failure the first time someone shares a cluster anyway.

**Flag for the human:** this is a refinement of ADR-001 prose, not an invariant change. Confirm at
the gate.

### D3. Per-destination `out` topics (decided)

**Options:** (a) one topic per destination — chosen; (b) one `out` topic with `tb-destination` and
consumer-side filtering; (c) no `out` topic, loaders consume `raw` and transform themselves.

(a) gives independent consumer lag, retention, ACLs and scaling per destination, and a slow FHIR
server cannot starve a ClickHouse batch loader. It costs topic and partition count, which is exactly
what S4 measures. (b) halves topic count but every loader reads every destination's traffic — wasted
network and CPU proportional to destination count. (c) breaks invariant 2's durability boundary: a
retransform on every retry makes delivery non-deterministic.

**Give up:** topic count. Mitigation if S4 says the ceiling is near: topic sharing (D4).

### D4. Topic sharing for small channels (conditional — TODO(S4))

Grammar needs no change: a share group takes the `channel` position and consumers filter on the
`tb-channel` header. Enabled or not, and the volume threshold, wait on S4.
TODO(S4): state the threshold and whether it ships in the MVP.

### D5. Partition defaults (TODO(S4))

Draft defaults in `contracts/topics/v0/topic-defaults.json`: 12 per channel topic, 3 for `dlq`, 24
for `_tb.events`. These are ADR-001 planning values, not measurements.
TODO(S4): confirm or revise, and state the per-cluster channel ceiling.

`out.<dest>` MUST match its `raw` partition count for per-key-ordered channels. Different counts
change the key-to-partition mapping, so two messages for one patient can be consumed by different
workers and delivered out of order — a silent invariant-5 violation. The runtime refuses to start on
a mismatch.

### D6. Envelope is text headers, payload opaque (decided)

**Options:** (a) `tb-*` text headers, payload untouched — chosen; (b) a structured wrapper (JSON or
Avro) containing metadata plus payload; (c) headers as packed binary.

(b) forces every producer and consumer, including third-party tooling, to understand the wrapper, and
makes the payload non-extractable without a decode step — it also breaks invariant 8. (c) saves bytes
nobody is short of and makes `kcat` useless during an incident.

**Give up:** a few hundred bytes per record and an integer parse per header.

### D7. Four headers added beyond ADR-001 5.4 (decided)

`tb-destination`, `tb-stage`, `tb-error-code`, `tb-retry-not-before`. Rationale per header is in
`contracts/CHANGELOG.md`; the driver is that `retry` and `dlq` are per channel, and ADR-003 cannot
implement delay tiers or DLQ triage without them. All additive.

Deliberately **not** added: any header carrying human error text or payload excerpts. Exception text
quotes payload, and headers reach logs, metrics and the archive (invariant 6).

### D8. Claim-check threshold (decided, unverified)

Inline payload ≤ 1,000,000 bytes; `message.max.bytes` 1,048,576; above that, object store plus
`tb-claim-check` and a null Kafka value.
TODO: no Phase 0 spike exercised claim-check. Flag as an untested path in the Phase 1 plan.

---

## 3. Consequences

**Easier:** prefix ACLs and quotas per channel and environment; per-destination lag and alerting;
shadow deploys as a second consumer group; an operator can read a topic name and know what it is.

**Harder:** channel rename is a migration; topic count grows with channels × destinations; lite mode
(RF=1) cannot satisfy RPO=0 and must say so loudly.

**To revisit:** topic sharing thresholds after S4; tiered storage to extend `raw` retention and make
replay the primary recovery path; Kafka share groups for unordered destinations.

---

## 4. Impact on invariants and contracts

| Item | Impact |
|---|---|
| Invariant 1 | Reinforced: `contracts/topics/v0` section 3 lists the broker and producer settings without which ACK-after-commit is a lie (`unclean.leader.election.enable=false` in particular). |
| Invariant 2 | Unchanged. D2 renames the physical platform topics, not the classes. |
| Invariant 5 | Reinforced: the `out`/`raw` partition-count rule closes a silent reordering path. |
| Invariant 8 | Implemented by `contracts/envelope/v0`. |
| No invariant change is proposed by this ADR. | |
| `contracts/envelope/v0`, `contracts/topics/v0` | Published as Draft; frozen at the Phase 1 wave 1 gate. |

---

## 5. Follow-ups

1. Fill every TODO(Sn) from the filed spike reports before the Phase 0 gate.
2. ~~Conformance suite over both contracts' example manifests.~~ Done 2026-09-20:
   `tests/conformance/test_contracts_v0.py`, 62 passed / 0 failed, no schema bugs. Keep it in CI.
3. `platform-engineer`: reconcile `spikes/common/src/tbcommon/envelope.py` with the contract.
4. `devops-engineer`: Helm and Compose read `topic-defaults.json` rather than hardcoding values.
5. ADR-004 owns claim-check object lifecycle and retention.
