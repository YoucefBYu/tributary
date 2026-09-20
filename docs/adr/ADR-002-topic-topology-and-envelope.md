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

### D8. Claim-check threshold and URI form (decided, unverified)

Inline payload ≤ 1,000,000 bytes; `message.max.bytes` 1,048,576; above that, object store plus
`tb-claim-check` and a null Kafka value.

The URI is an **opaque locator**: no query string, no fragment, no userinfo, and an object key derived
only from `<env>/<channel>/<yyyy>/<mm>/<dd>/<tb-msg-id>`. Signed URLs are minted at read time by the
API and never persisted. Added after a security review: a claim-check URI is copied into `_tb.events`,
ClickHouse and the browser, so a presigned URL there becomes a stored, replayable credential that
outlives its own expiry in backups, and an object name taken from the sender is a classic accidental
PHI channel.

TODO: no Phase 0 spike exercised claim-check. Flag as an untested path in the Phase 1 plan.

### D9. Transport security and authorization are part of the config contract (decided)

The topics contract states a required security profile (SASL_SSL or mTLS, deny-by-default authorizer,
least-privilege prefix ACLs) rather than deferring all of it to ADR-004.

**Why:** a contract that specifies `acks`, `linger.ms` and `min.insync.replicas` but is silent on
authentication reads as a complete configuration, and an implementer who follows it exactly gets a
plaintext, unauthenticated cluster carrying PHI. Silence in a config contract is a decision, and it was
the wrong one.

**Give up:** some overlap with ADR-004's scope. The boundary: this contract fixes the required
mechanisms and the ACL shape; ADR-004 owns the role model, credential rotation and key management,
which need a human decision (CLAUDE.md escalates cryptography and key management).

### D10. `tb-correlation-id` is an engine-minted UUID (decided)

**Options:** (a) free text up to 128 characters, as first drafted; (b) free text plus a written rule
"must not be derived from patient data"; (c) UUID only, with the receiver mapping any sender token in
memory.

**Chose (c).** The correlation id is the `reply` record key and is copied into `_tb.events`,
ClickHouse and the browser in clear text. Option (a) invites the obvious implementation — reuse
MSH-10 or the sender's request id — and senders in the field put visit and accession numbers there.
Option (b) is the same data with a paragraph asking people not to. A UUID cannot represent a
payload-derived value at all, which is the difference between a rule and a constraint. ADR-001 6.5
already has the receiver generating the id, so this ratifies the flow rather than changing it.

**Give up:** a sender cannot search the browser by its own token, and receivers must hold a
token-to-UUID map for the life of the pending request (they already hold the open connection there).

### D11. `tb-usr-*` does not reach `_tb.events` (decided)

**Options:** (a) define a normative header-to-`attrs` mapping; (b) state that `tb-usr-*` stays in
headers and is never projected; (c) leave it as it was, describing a projection that no field can
hold.

**Chose (b).** Option (c) was the bug: the contract promised a path the schema could not express, so
each implementer would have invented one. Option (a) turns every channel-defined header into a silent
new archive column, populated by channel code, in the one place invariant 6 is hardest to police —
the archive is where PHI becomes durable, searchable and widely readable.

**Give up:** a step that wants metadata searchable must emit it deliberately as an `attrs` short code
through `ctx`, so metadata is set in two places, and the browser cannot show `tb-usr-*` in a list
view. Revisit in v1 with a per-channel allow-list if a real channel needs it.

### D12. Topic sharing is bounded by the tenant, not by preference (decided)

A share group occupies the `channel` position, and ACLs are prefix-based on `<env>.<channel>.`, so
every consumer of a shared topic can read every record on it. `tb-channel` filtering is consumer-side
convention, not access control. Therefore a share group MUST NOT span a tenant or trust boundary, the
control plane refuses such a deployment, and the group inherits the union of its members'
sensitivity.

**Why this is not a trade-off:** the first draft listed "no per-channel ACL" as a cost of sharing. In
a multi-tenant deployment that cost is one tenant reading another tenant's PHI, which is a breach.
"Opt-in" describes who enables sharing, not who can read the data afterwards.

**Give up:** small channels in different tenants cannot be consolidated, so the topic-count pressure
S4 is measuring is only relieved within a tenant.

### D13. Encryption at rest is required, and scoped honestly (decided)

Required: encrypted broker data directories, bucket-default encryption on the claim-check store,
encrypted ClickHouse and PostgreSQL volumes, encrypted backups (topics contract 3.5).

**Why it belongs here:** the same reasoning as D9. Kafka has no built-in encryption at rest, so a
contract that configures durability and transport but says nothing about disks reads as though none
is needed.

**Scoped honestly:** volume encryption covers a lost or decommissioned disk and a mis-shared
snapshot. It does not cover a compromised broker process, an over-broad ACL, or an unaudited Kafka
UI — D9 and the envelope contract's section 4 own those. Encryption at rest is routinely treated as
the answer to all three.

**Give up / escalated:** application-level or per-topic payload encryption is not required and not
designed here, and key custody (cloud-managed versus customer-managed KMS) is ADR-004 plus a human.
The requirement that encryption be on is not deferred.

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
| Invariant 1 | Reinforced: `contracts/topics/v0` section 3 lists the broker and producer settings without which ACK-after-commit is a lie. A security review found the first draft pinned `min.insync.replicas` only per profile, not per topic, so a topic created from the class defaults would satisfy `acks=all` with one replica — fixed by pinning the durability floor in every class. |
| Invariant 3 | Reinforced by D9: no worker or loader gets `WRITE` on `_tb.<env>.channels`. Invariant 3 makes workers trust that topic completely, so write access to it is remote code execution across the data plane. |
| Invariant 6 | Reinforced repeatedly by the two security rounds: the envelope validation projection carries no key value; claim-check URIs cannot carry credentials or identifiers; `tb-correlation-id` is a UUID so it cannot carry payload-derived content (D10); `tb-usr-*` does not reach the archive (D11); the record-key prohibition now names URLs, browser storage and sender-facing errors, and any tool that can display a key is treated as a PHI viewer; `attrs` is bounded so it cannot hold a message. |
| Invariant 2 | Unchanged. D2 renames the physical platform topics, not the classes. |
| Invariant 5 | Reinforced: the `out`/`raw` partition-count rule closes a silent reordering path. |
| Invariant 8 | Implemented by `contracts/envelope/v0`. |
| No invariant change is proposed by this ADR. | |
| `contracts/envelope/v0`, `contracts/topics/v0` | Published as Draft; frozen at the Phase 1 wave 1 gate. |

---

## 5. Follow-ups

1. Fill every TODO(Sn) from the filed spike reports before the Phase 0 gate.
2. Conformance suite over both contracts' example manifests. First run 2026-09-20 passed 62 / 0 with no
   schema bugs; the same day's security fixes changed both schemas and added 6 examples, and the re-run
   passed 70 / 0 at commit `ae4d3bd`. The second security round added four invalid examples, so the
   expected count is 74 and **that run is outstanding**. Keep it in CI, which is the point: a contract
   edit that invalidates its own examples must fail a build, not wait for a reviewer.
3. `platform-engineer`: reconcile `spikes/common/src/tbcommon/envelope.py` with the contract. Binding
   for `packages/tributary-sdk` and `-runtime` in Phase 1, not retroactively for throwaway spike code.
4. **Phase 1, `devops-engineer`** (not on the Phase 0 team, so these are carried forward here rather
   than assigned now):
   - CI runs `tests/conformance` on every change to `contracts/**`. The value is catching a contract
     edit that silently invalidates its own examples; the run command is in `contracts/README.md`.
   - Helm and Compose read `topic-defaults.json` rather than hardcoding partition counts and
     retentions in a third and fourth place.
   - Lite mode logs the RF=1 / no-RPO=0 warning at startup (`contracts/topics/v0` section 3.1).
5. ADR-004 owns claim-check object lifecycle and retention, which no Phase 0 spike exercised.
6. **ADR-004 also owns the `attrs` `_hmac` mechanism, left UNRESOLVED on purpose** (security finding
   6, HIGH). The events schema pins the 64-lowercase-hex shape, which is all a schema can do: an
   unkeyed `sha256(mrn)` has the identical shape and is reversible by exhaustive search over a
   bounded identifier space. Construction, key scope, rotation, key store, and whether a keyed hash
   counts as de-identified are cryptography and key management, which CLAUDE.md escalates to a human.
   Open questions are enumerated in `contracts/events/v0/README.md` 3.1. Interim contract position:
   producers SHOULD NOT emit identifier-derived `attrs`, and `_hmac` values are treated as PHI in
   ClickHouse. This blocks the per-patient search feature in the message browser (ADR-001 6.6), not
   the Phase 0 gate.
7. **Audit retention is an open human decision** (security finding 12, MEDIUM):
   `contracts/topics/v0/README.md` 2.1. The 30-day default may be two orders of magnitude short of a
   six-year duty, and the fix is likely architectural (ClickHouse or object storage as the system of
   record, Kafka as a buffer) rather than a larger `retention.ms`. Settle it before the
   `_tb.<env>.audit` event contract is written; it also puts `_tb.<env>.events` (3 d) and `raw` (7 d)
   in scope.
