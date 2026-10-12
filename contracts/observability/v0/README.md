# Contract: observability — v0 (draft)

**Contract id:** `tributary.observability/v0`
**Status:** Draft, **new in contract set 1.8 (2026-10-05)**. Additive: no existing document changes
meaning. Section 2 (readiness) was written first because W3-17 waits on it.
**Owner:** `architect`. Propose changes by message with the exact diff and the reason.
**Source:** W2-5 ruling (2026-09-24) that metric export waits on this contract; `topics/v0` 3.3.1
rule 2; set 1.7 `--instance` rule; S2 first-ACK finding (`docs/spikes/wave-2-linux-rerun.md`);
ADR-001 6.10; the in-process counters that exist today (section 3.1).
**Machine-readable:** [`readiness.schema.json`](readiness.schema.json),
[`metrics.json`](metrics.json) (registry) with [`metrics.schema.json`](metrics.schema.json),
[`log-line.schema.json`](log-line.schema.json). **Examples:** [`examples/manifest.json`](examples/manifest.json).

Serves invariant 1 (a receiver that cannot commit is not routed to), invariant 3 (no probe depends on
PostgreSQL, the API or ClickHouse, and a channels-topic outage never takes a receiver out of service),
invariant 6 (every value on these surfaces is a fixed code or a spec-defined name) and invariant 9
(metrics and logs work while the archiver is down).

---

## 1. Scope

| Surface | Normative for | Section |
|---|---|---|
| `GET /livez`, `GET /readyz` on the ops listener | **receivers** (all four types); the control-plane API serves the same two paths on its own listener (`openapi/v0`) | 2 |
| `GET /metrics` on the ops listener | every data-plane process, the archiver, the API | 3 |
| Structured log line | every process | 4 |

Workers, loaders and the archiver expose `/livez` and `/metrics` in v0. Their `/readyz` is not
contracted yet: nothing routes traffic to them, and KEDA scales them on lag, not on readiness. Adding
it later is additive.

---

## 2. Readiness and liveness (normative, W3-12 first deliverable)

### 2.1 The ops listener

| Rule | Value |
|---|---|
| **O1. Separate listener.** | The ops endpoints are **never** served on an ingest port (MLLP, HTTP receiver). The HTTP receiver's port faces senders; `/metrics` names every channel on the process |
| **O2. Loopback by default.** | Bind `127.0.0.1`, port `9180`. Flags `--ops-bind`, `--ops-port`; environment `TB_OPS_BIND`, `TB_OPS_PORT`; the flag wins. `--ops-port 0` picks an ephemeral port (tests); the chosen port is logged in the `listening` event as `ops_port` |
| **O3. Non-loopback is explicit.** | A non-loopback `--ops-bind` (Kubernetes needs the pod IP, because the kubelet probes the pod address) is allowed only when set explicitly, and logs `ops_listener_non_loopback` at WARN at startup. Plaintext is permitted there because every body is PHI-free by this contract; Helm restricts the port with a NetworkPolicy (W3-8) |
| **O4. Fail closed on bind.** | If the ops listener cannot bind, the process refuses to start (`startup_refused` with `code: OPS_LISTENER_BIND_FAILED`, `error: OSError` and the numeric `errno`, exit 2). A receiver that runs without its probe is never ready to an orchestrator and looks healthy to a human |
| **O5. Minimal HTTP surface.** | `GET` only (others 405, empty body). Unknown path 404, empty body. Query strings ignored; request bodies never read; request line plus headers capped at 8 KiB; 1 s read timeout; at most 8 concurrent connections. **No response echoes the request path, a header, or a peer address**, and no ops request is logged per request |
| **O6. No I/O on the request path.** | `/readyz` serialises a state snapshot the components maintain. It never calls Kafka. Measured reason: a `list_topics` on a busy consumer took 470-490 ms (`runtime/channels.py` docstring), longer than a probe timeout |

The listener goes to `security-reviewer` (network listener, W3-17).

### 2.2 `GET /livez`

`200` with `{"v":0,"status":"alive","component":"<role>"}` while the process's event loop (or main
loop) serves the request. **Liveness never depends on Kafka, the spec, or any check in 2.3.** A
Kafka outage must not restart every receiver at once: restarting does not fix Kafka and throws away
the warm producer. A wedged loop fails liveness by timing out, because the endpoint is served by that
loop. A fatal client error is not a liveness concern: the process exits non-zero itself (set 1.7).

### 2.3 `GET /readyz` for a receiver

`200` when ready, `503` when not, body per [`readiness.schema.json`](readiness.schema.json). **Ready
exactly when every gating check is `ok`.** Probe consumers read only the status code.

| Check | `ok` when | `reason` when not ok |
|---|---|---|
| `spec` | A spec for this receiver's channel has been applied: present, not tombstoned, not refused | `no_spec` (none ever read), `spec_refused` (the only spec seen is refused), `undeployed` (tombstone) |
| `component` | The listener is bound, or the poller or consumer loop runs, and the process is not stopping | `starting`, `bind_failed`, `stopping`, `component_failed` |
| `raw_producer` | Warm-up (2.4) succeeded since start, and the producer is neither fatal nor in an all-brokers-down state | `not_warmed`, `all_brokers_down`, `fatal` |
| `reply_consumer` (**required when `sync_reply` is true**) | `topics/v0` 3.3.1: holds at least one reply partition **with an established fetch position** (rule 1), the consumer is connected, not fatal, and its principal is not being refused | `no_partition`, `not_established`, `disconnected`, `fatal`, `unauthorized` (set 1.8) |

Each check carries only the reasons in its own row; the schema binds them (set 1.8). A reason must be
**true of the state**; the finer cause goes in the log line's `error_code` (section 4, L6), from the
`log_only` list of `envelope/v0` `error-codes.json`.

- **`reply_consumer`: `unauthorized` (set 1.8, W3-17).** The broker refuses the reply consumer's
  principal: `GROUP_`, `TOPIC_` or `CLUSTER_AUTHORIZATION_FAILED`, `SASL_AUTHENTICATION_FAILED` or
  `_AUTHENTICATION`. Before set 1.8 this reported `no_partition`, which is false when partitions are
  still held. The first refusal of a streak logs `reply_consumer_unauthorized` with `error_code`
  `REPLY_CONSUMER_UNAUTHORIZED`. It clears when no refusal has recurred for a decay window chosen and
  documented by the implementation (30 s in W3-17), because librdkafka re-raises a standing refusal and
  sends no "fixed" signal. When more than one reason holds, report the first of: `fatal`,
  `disconnected`, `unauthorized`, `not_established`, `no_partition`.
- **`spec` has no `unauthorized` reason (set 1.8 ruling).** A refused channels-topic principal before
  the first spec is `no_spec`, which is true: no spec was ever read. The cause is in the
  `spec_unavailable` log line's `error_code`: `CHANNELS_UNAUTHORIZED`, `CHANNELS_TOPIC_MISSING`,
  `CHANNEL_NOT_DEPLOYED` (topic readable, no record for this channel; a tombstone is `undeployed`) or
  `CHANNEL_SPEC_UNAVAILABLE` (anything else, such as a catch-up timeout). After the first spec, R3
  applies: `degraded: spec_source_unproven`, never a gating reason. **Gain:** the probe body stays a
  state summary, not a mirror of the log taxonomy. **Give up:** an operator reading only `/readyz` sees
  `no_spec` and must read the log line to tell a missing ACL from a channel nobody deployed.

Rulings, with what each gives up:

- **R1. Kafka trouble makes a receiver not ready.** `all_brokers_down` (librdkafka `_ALL_BROKERS_DOWN`
  via `error_cb`) clears on the next successful delivery report **or** metadata round trip. While not
  ready the receiver gets no traffic, so it runs the same off-thread metadata probe as the channels
  cache (at most one in flight, 5 s interval, 2 s timeout) to find out it has recovered. The receiver
  still NACKs or refuses anything that does arrive (invariant 1). **Gain:** in a partial outage the
  load balancer moves senders to receivers that can commit. **Give up:** in a total outage every
  receiver is unready at once, so senders see connection refused instead of a NACK. Both are refusals
  under invariant 1, and senders retry either way.
- **R2. `fatal` is reported, then the process exits non-zero** (producer `_FATAL`, or the replier
  fenced, set 1.7). The 503 between the two is best effort.
- **R3. A channels-topic outage after the first spec is `degraded`, never not ready.** The receiver
  keeps serving its last good spec (`ChannelsUnavailableError`, `lifecycle.py`), and `/readyz` adds
  `spec_source_unproven` to `degraded` with status unchanged. Invariant 3 says the data plane keeps
  running from the compacted topic's last state. A readiness drop would turn a control-plane-side
  fault into an ingest outage. **Give up:** a receiver serving a spec the control plane has since
  changed keeps taking traffic. That is what invariant 3 chose.
  **For W3-18:** "a stuck spec reader makes the receiver refuse and report not ready" applies
  **before the first spec is applied** (`spec`: `no_spec`). After it, W3-18 asserts
  `spec_source_unproven` appears and clears, and that readiness does not drop.
- **R4. `lite_profile`** in `degraded` while the `lite` profile is active (`topics/v0` 3.8). Not gating.
- **R5. Undeploy (tombstone) makes the receiver not ready** (`spec`: `undeployed`), and `pause` stops
  new work at once (`lifecycle.py`). Work already committed keeps its acknowledgement.
- **R6. Shutdown:** on SIGTERM `component` goes `stopping` **before** the drain starts, so the load
  balancer stops routing while admitted work is acknowledged.

### 2.4 Warm the producer before ready, and before listening

S2 measured one 0.6-1.05 s ACK per connection's first message on a cold receiver
(`wave-2-linux-rerun.md`). A pre-warm message before the clock removed it in the benchmark. A
receiver cannot send a warm-up record: `raw` carries only sender messages.

- **W1.** Before binding the ingest listener (or starting the poller or source consumer), the receiver
  completes one metadata round trip for `<env>.<channel>.raw` on its **own `raw` producer**, off the
  event loop, and the reply must name a leader for every partition. Failure retries with the open
  backoff (1 s to 10 s), and `raw_producer` stays `not_warmed`. Only then is the listener bound.
- **W2.** The raw topic name comes from `--env` and the channel, not from the spec, so warm-up may run
  before the first spec arrives.
- **UNVERIFIED: whether a metadata round trip alone removes the first-ACK outlier.** The benchmark's
  `--prewarm` sent a full message. The cost may sit in the idempotent producer's `InitProducerId` or
  in per-leader connections, which metadata does not open. **W3-17 measures it:** S2 harness, readiness
  gated, without `--prewarm`, at least 3 runs. If the outlier remains, the architect rules on the next
  step (for example a `flush(0)` after a metadata request for every leader). The readiness contract
  does not change either way.

### 2.5 What W3-17 must show (restates its verify line against this section)

Per receiver type: `503` with `raw_producer: not_warmed` until the first `raw` metadata round trip;
`503` with `reply_consumer` failing while the replier is disconnected, holds no partition, is refused
by the broker (`unauthorized`, set 1.8) or is `_FATAL`; `200` otherwise; `stopping` before drain; an ops-port bind conflict exits 2. Each test proves
its fault fired (Phase 0 lesson). A broken-implementation control (readiness hard-wired to `ready`)
must go red.

---

## 3. Metrics

### 3.1 Registry

[`metrics.json`](metrics.json) lists every metric name, type, label set and emitter. It was seeded
from the counters in code today: `receivers/metrics.py` `Metrics`, runtime `StepStats`, `LoopStats`
and `ReplyStats`, and loaders' `LoaderMetrics`, plus the three ADR-001 6.10 names. **The registry is
the contract, not the `snapshot()` keys.** A process exports only registered names. A new metric or
label is a contract change.

Exposition: Prometheus text format on `GET /metrics`, on the ops listener (2.1). The client library is
the implementer's choice and passes `licence_check.py` (`prometheus-client` 0.26.0 declares
`Apache-2.0 AND BSD-2-Clause` and no required dependencies, per PyPI metadata read 2026-10-05; both
terms are permitted, but it has not been run through `licence_check.py`). OpenTelemetry metrics may be added later if they produce
the same names.

### 3.2 Labels: a closed list (invariant 6)

Allowed label names are the `labelName` enum in `metrics.schema.json`, and nothing else. Every value
is a spec-defined name (`channel`, `destination`, `step`), an enum from another contract (`stage`,
`status`, `receiver_type`, `destination_type`), a registered error code, or a per-metric closed list
(`outcome`, `reason`).

**Never a label:** a message, parent or correlation id; the record key or anything derived from it
(usually a patient identifier, `envelope/v0` 4); a partition number or offset (the event carries
them); a peer address, host, file name, object key or URL; a user or subject id; free text. Each is
unbounded cardinality, PHI, or both. `route` on API metrics is the **OpenAPI path template**, never the
concrete path, which contains channel names and message ids.

Buckets are named sets in `metrics.json` (`latency`, `delivery`), so every emitter of a histogram
buckets alike. `latency` includes 0.05 s, the S2 ACK threshold.

### 3.3 Mapping notes for implementers

- `tb_messages_total` is incremented **at event emit time**, with the event's `stage`, `status` and
  `destination` (empty when none). It works while the archiver is down (invariant 9).
- `tb_partitions_halted` is what W3-8's `partition_halted` alert reads (`> 0` for 1 m). It has no
  partition label; the operator reads the partition from the event.
- `tb_spec_version` is the observable for exit criterion 3 (deploy v2, roll back, no restart).
- Loader `by_reason` keys become `tb_deliveries_total{outcome, code}`. `success` maps to
  `outcome="success", code="NONE"`. `circuit-open` maps to the registered `CIRCUIT_OPEN`. A reason
  that is not a registered code is registered first (`envelope/v0` `error-codes.json`).

### 3.4 The `audit_log` tamper check (set 1.8, ADR-004 R7 rev 3)

`tb_audit_tamper_suspect_ids{environment}` and `tb_audit_tamper_check_timestamp_seconds{environment}`.

- **Who runs it: the API**, which already holds SELECT on the archive (ADR-004 A5). **Rejected:** the
  events archiver. It holds INSERT only, so it would need a new SELECT grant, and it is the writer
  whose rows are being checked: a compromised writer could forge a row and report 0. **Give up:** the
  check pauses while the API is down. The staleness alert below catches that.
- **Refresh:** a background task per environment, every `TB_AUDIT_TAMPER_CHECK_INTERVAL_S` (default
  900, range 60-86400; outside the range the API refuses to start). The scrape only reads the stored
  values (O6). Each replica runs its own check; alerts use `max by (environment)`.
- **Query** (readonly, under the query-library limits):
  `SELECT count() FROM (SELECT audit_id FROM audit_log GROUP BY audit_id HAVING
  uniqExact(cityHash64(<every record field>)) > 1)`. "Every record field" means the `audit/v0` fields,
  **not** `inserted_at`, `source_partition` or `source_offset`. A redelivery can differ in those, and
  it is not tampering.
- **On success:** set the count, then set the timestamp to the completion time. **On failure:** change
  neither, and log `audit_tamper_check_failed` with `error_type` (class name only). Both series are
  absent until the first check completes.
- **Labels:** `environment` only. Never an `audit_id`, an actor or a channel. The operator finds the ids
  with the same query in ClickHouse.
- **Alerts (W3-8, `devops-engineer`):** `max by (environment) (tb_audit_tamper_suspect_ids) > 0` pages.
  `time() - max by (environment) (tb_audit_tamper_check_timestamp_seconds) > 3 *
  interval` warns, and so does `absent(...)` once the API has run for 3 × interval.

---

## 4. Log line schema

One JSON object per line on stderr, as `tributary.runtime.logs.JsonLogger` writes it. Schema:
[`log-line.schema.json`](log-line.schema.json).

| Rule | Detail |
|---|---|
| L1 Required | `ts` (UTC, milliseconds, `Z`), `level` (`debug`, `info`, `warning`, `error`), `event` (snake_case code, at most 64), `component` |
| L2 Field names | `^[a-z][a-z0-9_]{0,31}$`; at most 32 fields |
| L3 Identifier deny-list | Byte-for-byte the `events/v0` attrs deny-list and `guards.IDENTIFIER_NAMES` |
| L4 Content deny-list (**new**) | `payload`, `body`, `content`, `raw`, `message`, `msg`, `text`, `segment`, `headers`, `traceback`, `stack`, `stack_trace`, `exc_info`, `exception`, `exc_text`, `query`, `query_string`, `sql` |
| L5 Values | Scalars only, at most 256 characters, no CR or LF (`guards.validate_log_field`) |
| L6 Well-known fields | `message_id`, `parent_id`, `correlation_id` (uuid shape); `channel`, `channel_version`, `destination`; `error_code` (registered code); `error` (an exception **class name**, never `str(exc)`); `ready`; `instance`; `trace_id`, `span_id` (W3C hex, for log-trace correlation) |
| L6a Two code fields (set 1.8) | `error_code` holds a code **registered** in `envelope/v0` `error-codes.json` (`codes` or `log_only`) or a minted member of a reserved prefix. `code` on a `startup_refused` line is a startup-refusal code (`OPS_LISTENER_BIND_FAILED`, `SECRET_UNRESOLVED`, `ENGINE_KAFKA_SECRET_UNRESOLVED`; **set 1.8, W3-29 item 3:** `INSTANCE_REQUIRED` and `ARGUMENTS_INVALID` (`topics/v0` 3.3 rules 1-2), `RETENTION_DECREASE_REFUSED` (ADR-004 R6); `API_TLS_REQUIRED` and `API_TLS_INVALID` (ADR-004 C1a, 2026-10-12: no TLS on a non-loopback bind; TLS files missing, unreadable or unusable); `AUDIT_HOME_ENV_REQUIRED` and `AUDIT_HOME_ENV_UNKNOWN` (`audit/v0` 2, 2026-10-12: unset with several environments; set to an environment the control plane does not manage)): an exit reason for the operator, never on a record, an event or a metric label, and **not registered in v0** (section 6) |
| L7 Message id spelling | **`message_id`** (as in `events/v0`). `tb_msg_id` and `msg_id` (receivers, wave 2) are accepted in v0 as deprecated aliases with the same uuid shape |

**What the schema cannot check:** that a permitted name like `reason` or `path` carries no PHI. Code
review and `security-reviewer` stay the control, as for `events/v0` attrs.

---

## 5. Conformance

`examples/manifest.json` uses **per-entry `schema`** (new in set 1.8): an entry with its own `schema`
key is checked against it, otherwise the manifest-level one (readiness). `validate.py` also
cross-checks `metrics.json`: every label a metric uses is defined, each `per_metric` label has
`label_values`, `label_values` names only that metric's labels, and each histogram's bucket set exists.

Ten examples are PHI guards (listed in the manifest `note`), proposed to `qa-engineer` as
`OBSERVABILITY_PHI_GUARDS`.

**Code conformance (change requests, CHANGELOG set 1.8):** a `tributary-runtime` test validates
`JsonLogger` output against `log-line.schema.json`, with a field named `payload` refused by the guard;
the content deny-list (L4) goes into `tributary.sdk.guards`; the receivers' `print(json.dumps(...))`
lines (`kafka_source/receiver.py`, `sftp/poller.py`, the `listening` events) lack `ts`, `level` and
`component` and move to `JsonLogger`.

## 6. Open items

- Worker, loader and archiver `/readyz` (section 1).
- Trace attribute rules (invariant 6 names trace attributes). The likely shape: the same closed
  vocabulary as the labels plus `message_id`. Not written yet; no code emits spans with attributes today.
- Whether metadata warm-up alone removes the first-ACK outlier (2.4, W3-17 measures).
- A registry for `startup_refused` `code` values (L6a). Today they are named where they are defined
  (O4 for `OPS_LISTENER_BIND_FAILED`, `topics/v0` 3.3 for `INSTANCE_REQUIRED`). Registering one family
  member alone would make the list look complete when it is not. L6a names six now; still not a registry.
