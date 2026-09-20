# Contract: topic naming, defaults and required configuration — v0 (draft)

**Contract id:** `tributary.topics/v0`
**Status:** Draft. Partition and retention defaults carry TODO markers until S4 reports.
**Owner:** `architect`.
**Source:** ADR-001 sections 5.3 and 6.4, refined by ADR-002.
**Schema:** [`topic-name.schema.json`](topic-name.schema.json) · **Defaults:** [`topic-defaults.json`](topic-defaults.json) · **Examples:** [`examples/topic-names.json`](examples/topic-names.json)

Realises invariant 2 (Kafka hops only at durability boundaries) and the broker side of invariant 1
(ACK only after a durable commit).

---

## 1. Grammar

```
data-plane      = env "." channel "." kind [ "." destination ]
platform        = "_tb." env "." platform-kind
env             = [a-z][a-z0-9-]{0,15}
channel         = [a-z][a-z0-9-]{0,63}
destination     = [a-z][a-z0-9-]{0,63}
kind            = "raw" | "out" | "retry" | "dlq" | "reply"     ; "out" requires destination
platform-kind   = "channels" | "events" | "audit"
```

Total length ≤ 249 (Kafka's limit). The grammar's worst case is 150, so there is room.

| Topic | Purpose | Key |
|---|---|---|
| `<env>.<ch>.raw` | Messages as received. Source of truth for replay. | ordering key |
| `<env>.<ch>.out.<dest>` | Transformed message for one destination. | ordering key |
| `<env>.<ch>.retry` | Non-blocking retries, shared across destinations. | ordering key |
| `<env>.<ch>.dlq` | Exhausted retries and step failures. | `tb-msg-id` |
| `<env>.<ch>.reply` | Short-lived synchronous replies. | `tb-correlation-id` |
| `_tb.<env>.channels` | Compacted deployed channel specs. | channel id |
| `_tb.<env>.events` | Lifecycle events for every message and stage. | `tb-msg-id` |
| `_tb.<env>.audit` | Control-plane and access audit events. | actor id |

### 1.1 Why `.` as the separator, and why segments are restricted

Kafka permits `[a-zA-Z0-9._-]` in topic names. Using `.` as the structural separator makes a topic
name parseable by `split(".")`, and makes ACLs and quotas expressible as prefix patterns
(`prod.adt-to-fhir.` covers every topic of one channel in one environment). That only works if no
segment contains a `.`, hence the restricted grammar for `env`, `channel` and `destination`.

Lowercase only: Kafka metrics collapse `.` to `_` and are case-sensitive, so `ADT.x` and `adt.x`
produce colliding, confusing metric names. Rejecting mixed case at the contract boundary is cheaper
than debugging a Grafana panel later. The cost is that Mirth channel names must be slugified on
import — `migration-engineer` owns that mapping and must keep it reversible and collision-free.

### 1.2 Platform topics carry the environment

ADR-001 5.3 writes the platform topics as `_tb.channels`, `_tb.events`, `_tb.audit`. v0 makes them
`_tb.<env>.channels` and so on.

**Why:** the data-plane names already carry `<env>`, which only makes sense if one cluster can host
more than one environment. If it can, then unqualified platform topics let a dev control plane
publish a channel spec that production workers consume. That is a correctness and a safety problem,
not a naming preference.

**Trade-off:** the invariant-2 text and ADR-001 5.3 name `_tb.events` without an environment. The
*class* is still `_tb.events`; only the physical name gains a segment. This is an ADR-002 refinement,
not an invariant change. If the human reviewer disagrees, the alternative is one cluster per
environment and unqualified platform topics — simpler names, higher infrastructure cost, and a
silent failure mode the first time someone shares a cluster.

### 1.3 Topic sharing for small channels

ADR-001 5.3 proposes topic sharing for very small channels. v0 needs no new grammar for it: a share
group occupies the `channel` position (for example `prod.shared-small-a.raw`), and consumers filter
on the `tb-channel` header. Costs: no per-channel retention, no per-channel lag metric, and a
consumer reads and discards other channels' records. The defaults wait on S4 measurements.

**Restriction, not a cost: a share group MUST NOT span a tenant or trust boundary.** Channels on a
shared topic have no per-channel ACL — the ACL grammar is prefix-based on `<env>.<channel>.`, and the
share group *is* the channel position — so every consumer of a shared topic can read every record on
it, including channels it does not own. `tb-channel` filtering is a convention in consumer code, not
an access control. In a multi-tenant deployment that means one tenant's credentials read another
tenant's PHI, which is a breach, not a trade-off.

Therefore:

1. All channels in a share group MUST belong to one tenant and one trust boundary, and the control
   plane MUST refuse to deploy a channel into a share group that violates this.
2. The share group inherits the union of its members' sensitivity: its ACL grants, retention and
   audit scope are those of the most sensitive member.
3. A channel that needs its own ACL needs its own topics. That is the cost of isolation, and it is
   the correct default — sharing is the exception, justified by topic count (S4), never by
   convenience.

Listing this as "opt-in only" in the first draft was not enough: opt-in describes who turns it on,
not who can read the data afterwards.

---

## 2. Defaults

Machine-readable in [`topic-defaults.json`](topic-defaults.json), so runtime, the control plane and
Helm read one source instead of hardcoding three.

| Class | Partitions | Retention | Cleanup |
|---|---|---|---|
| `raw` | 12 (TODO: confirm against S4) | 7 d | delete |
| `out.<dest>` | 12, matching `raw` | 7 d | delete |
| `retry` | 12, matching `raw` | 3 d | delete |
| `dlq` | 3 | 30 d | delete |
| `reply` | 12 | 10 min | delete |
| `_tb.<env>.channels` | 6 | compacted | compact |
| `_tb.<env>.events` | 24 (TODO: confirm against S1/S2 event rate) | 3 d | delete |
| `_tb.<env>.audit` | 6 | 30 d (**TODO(human)**, section 2.1) | delete |

`out.<dest>` MUST have the same partition count as its `raw` topic when the channel declares
per-key ordering. Different counts change the hash-to-partition mapping, so two messages for one
patient can land on partitions consumed by different workers and be delivered out of order —
a silent violation of invariant 5. The runtime MUST refuse to start a channel whose partition counts
disagree.

`dlq` gets fewer partitions because it is keyed by message id, low volume, and read by humans.

### 2.1 Audit retention is a human decision, not an engineering default

`_tb.<env>.audit` retains 30 days. That number was chosen to match the DLQ and keep a small cluster
small. It is an engineering default and it may be wrong by two orders of magnitude.

`security-reviewer` raised this (finding 12). HIPAA §164.316(b)(2)(i) requires that policies,
procedures and *the records of actions, activities and assessments* the rule requires be retained for
six years. Whether that duty reaches a control-plane and access audit trail like this one is an
interpretation question, and many covered entities and their contracts read it that way; some state
rules and customer agreements go further. **UNVERIFIED — no legal review has been done, and no agent
should perform one.** If a six-year duty does apply, 30 days on a Kafka topic is not a retention
policy, it is a deletion policy: the evidence is gone before the first annual audit.

The architect is not deciding this. It is a compliance and cost question, and it probably has an
architectural answer rather than a bigger number: six years at RF=3 is a storage and tiering decision,
and the durable system of record for audit is more likely ClickHouse or object storage, with Kafka
retention acting only as a transport buffer in front of it. That is a different design from raising
`retention.ms`, so the question must be answered before the audit contract is written rather than
after.

Three questions for the human, carried in `docs/agent-team/STATUS.md`:

1. What retention duty applies to the control-plane and access audit trail?
2. Is the system of record for it Kafka, the ClickHouse archive, or object storage?
3. Does the same answer change `_tb.<env>.events` (3 d) and `raw` (7 d)? Those are the other two
   places a "what happened to this message, and who touched it" question is answered, and `raw` is
   also the replay source. A retention duty that covers the audit trail may or may not cover them.

Until that is answered, the 30-day value stands and is marked `TODO(human)` in
[`topic-defaults.json`](topic-defaults.json). The `_tb.<env>.audit` event contract itself is unwritten
(see the events contract, section 6), so nothing depends on this yet.

---

## 3. Required configuration

These are not tuning knobs. Invariant 1 (never ACK and drop) is false without them.

### 3.1 Broker and topic

| Setting | Value | Why |
|---|---|---|
| `replication.factor` | 3 (production), 1 (lite) | Survive one broker loss. |
| `min.insync.replicas` | 2 (production), 1 (lite) | With `acks=all`, a write is durable on two brokers before the receiver ACKs. |
| `unclean.leader.election.enable` | `false` | A leader elected from a stale replica silently loses acknowledged records. |
| `max.message.bytes` | 1048576 | Matches the envelope's 1 MB inline limit plus header headroom. |
| `compression.type` | `producer` | Do not recompress on the broker. |

**`min.insync.replicas` and `unclean.leader.election.enable` are set per topic, in every class in
`topic-defaults.json`, not only cluster-wide.** Both are topic-level configs in Kafka: a topic created
without `min.insync.replicas` inherits the broker default of 1, and `acks=all` is then satisfied by a
single replica. The producer still gets a success, the receiver still ACKs the sender, and the next
broker loss drops an acknowledged message — invariant 1 violated with every setting apparently
correct. The per-class values carry the production floor so that a tool which applies class configs
and forgets the profile overrides fails safe; only the explicit `lite` profile lowers it.

Lite mode (RF=1) cannot satisfy RPO=0 and must say so in its documentation. It is for development and
small sites that have accepted the risk, and the distribution should warn at startup.

### 3.2 Receiver producer (the ACK-after-commit path)

| Setting | Value |
|---|---|
| `acks` | `all` |
| `enable.idempotence` | `true` |
| `max.in.flight.requests.per.connection` | ≤ 5 (required by idempotence) |
| `retries` | effectively unbounded, bounded by `delivery.timeout.ms` |
| `delivery.timeout.ms` | 120000 |
| `linger.ms` | 5 (TODO: set from the S2 ACK-latency-versus-throughput curve) |
| `compression.type` | `lz4` — **assumption, unmeasured**: cheaper CPU than zstd for small text records |

The receiver ACKs the sender only from the producer's delivery callback, after a successful
commit. On `delivery.timeout.ms` expiry, or when fewer than `min.insync.replicas` replicas are
available, it NACKs or refuses the connection. It never ACKs optimistically.

### 3.3 Worker and loader consumer

| Setting | Value | Why |
|---|---|---|
| `enable.auto.commit` | `false` | Commit after the side effect, never before (invariant 4). |
| `partition.assignment.strategy` | `cooperative-sticky` | Avoid stop-the-world rebalances with many channels. |
| `group.instance.id` | set (static membership) | A rolling restart must not trigger a full rebalance. |
| `isolation.level` | `read_committed` on topics written by a transactional stage | Otherwise a consumer sees aborted records. |
| `max.poll.interval.ms` | ≥ the slowest destination timeout plus backoff | Otherwise a slow sink looks like a dead consumer and causes a rebalance loop. |

### 3.4 Transport security and authorization (required)

A cluster built from sections 3.1 to 3.3 alone is plaintext, unauthenticated and unauthorized, and it
carries PHI. These settings are part of the contract, not a separate hardening guide. Machine-readable
in `topic-defaults.json` under `security`.

| Area | Requirement |
|---|---|
| Client listeners | `SASL_SSL`, or `SSL` with mTLS. `PLAINTEXT` only on a loopback-bound lite listener. |
| Inter-broker | A dedicated internal listener with `SASL_SSL` or mTLS. |
| Authentication | `SCRAM-SHA-512` or `OAUTHBEARER`. Never `PLAIN` on a listener without TLS. |
| Hostname verification | `ssl.endpoint.identification.algorithm=https`. Leaving this off is the most common way a TLS Kafka deployment silently accepts a man-in-the-middle. |
| Authorization | `StandardAuthorizer` enabled and **`allow.everyone.if.no.acl.found=false`** — deny by default. |
| `super.users` | The topic-creation job and the control plane only. Never a data-plane component. |
| Credentials | From a `secretRef` or a secrets manager. Never in a channel spec, a Compose file or an image. |

ACLs are least privilege on the topic prefix `<env>.<channel>.`, so compromising one channel's
credentials does not grant read access to another channel's traffic. Per-component grants are listed in
`topic-defaults.json` under `security.acls`. Two rules are load-bearing: no data-plane component gets
cluster-wide `WRITE`, `DELETE` or `ALTER`; and no worker or loader gets `WRITE` on
`_tb.<env>.channels`, because that topic is what invariant 3 makes workers trust completely — write
access to it is remote code execution across the whole data plane.

**Escalated, not decided here:** the complete role model, credential rotation and key management are
ADR-004 plus a human decision. This section fixes the minimum shape the configuration contract must
state; it does not design the authorization model or any cryptography.

### 3.5 Encryption at rest (required)

3.4 encrypts PHI in flight and then writes it to unencrypted disks. Kafka has no built-in
encryption at rest, which is exactly why a contract that omits the requirement reads as though none
is needed.

| Surface | Requirement |
|---|---|
| Broker data directories | Encrypted volumes (LUKS, cloud provider disk encryption, or an encrypted storage class in Kubernetes). |
| Claim-check object store | Bucket-default encryption on (SSE-KMS, SSE-S3, or the MinIO equivalent). Objects here are whole payloads, so this is the largest single PHI store in the system. |
| ClickHouse and PostgreSQL volumes | Encrypted volumes. Named here because the archive is fed from these topics; the archive contract owns the detail. |
| Backups and snapshots | Encrypted, with the same rules as the volume they came from. A backup of an encrypted disk written to an unencrypted bucket is the usual gap. |

Volume-level encryption protects a lost or decommissioned disk and a misconfigured snapshot share. It
does **not** protect against a compromised broker process, an over-broad ACL, or an unaudited Kafka
UI — those are 3.4 and the envelope contract's section 4. Stating which threat each control covers is
the point: encryption at rest is often treated as the answer to all of them, and it is the answer to
one.

Application-level or per-topic payload encryption is **not** required in v0 and is not designed here:
it means key management, which CLAUDE.md escalates. If a deployment needs it, ADR-004 plus a human
decides. Key custody for the options above (cloud-managed keys versus customer-managed KMS keys) is
likewise an ADR-004 question; the requirement that encryption be on is not.

### 3.6 Consumer group naming

```
tb.<env>.<channel>.<role>[.<destination>]     role = pipeline | loader | replier | archiver
```

For example `tb.prod.adt-to-fhir.loader.fhir-server`. A shadow deployment appends
`.shadow-<label>`, which is what makes shadow deploys free: a second group on the same `raw` topic.

---

## 4. Conformance

[`examples/topic-names.json`](examples/topic-names.json) holds valid and invalid names with the rule
each invalid one breaks. Tests validate each string against `topic-name.schema.json`.

## 5. Open items for v1

- Partition defaults and the topic-count ceiling are placeholders until S4 reports.
- Tiered storage would let `raw` retention grow past 7 days and make replay the primary recovery
  path. Not in v0.
- Section 3.4 states the required security profile and the ACL *shape*. The concrete ACL templates,
  credential rotation and key management are ADR-004 plus a human decision, and are not settled here.
- Section 3.5 requires encryption at rest. What is deferred is **key custody only** (cloud-managed
  versus customer-managed KMS keys) and application-level payload encryption, both ADR-004. The
  requirement itself is not deferred — a v0 deployment with unencrypted broker disks does not
  conform.
- Audit retention (section 2.1) is an open human decision, and the `_tb.<env>.audit` event contract
  is unwritten. Both must be settled together.
