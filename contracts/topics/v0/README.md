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
on the `tb-channel` header. Costs: no per-channel retention, no per-channel ACL, no per-channel lag
metric, and a consumer reads and discards other channels' records. Opt-in only, and the defaults wait
on S4 measurements.

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
| `_tb.<env>.audit` | 6 | 30 d | delete |

`out.<dest>` MUST have the same partition count as its `raw` topic when the channel declares
per-key ordering. Different counts change the hash-to-partition mapping, so two messages for one
patient can land on partitions consumed by different workers and be delivered out of order —
a silent violation of invariant 5. The runtime MUST refuse to start a channel whose partition counts
disagree.

`dlq` gets fewer partitions because it is keyed by message id, low volume, and read by humans.

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

### 3.5 Consumer group naming

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
