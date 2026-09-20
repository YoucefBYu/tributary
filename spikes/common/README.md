# `spikes/common` — shared Phase 0 infrastructure (task P0-2)

Owner: `platform-engineer`. Consumers: every Phase 0 spike.

**There is one spike cluster.** Do not start a competing one (phase-0.md
coordination note). If you need different broker settings, message
`platform-engineer` instead of forking this file.

## The cluster

3 brokers, Apache Kafka 4.0 in KRaft mode, combined `broker,controller` roles,
`apache/kafka:4.0.0` (Apache-2.0 — do **not** substitute `confluentinc/cp-*`,
which is Confluent Community License).

| Broker | Compose service | Container | Host bootstrap | Node id |
|---|---|---|---|---|
| 1 | `kafka1` | `tb-kafka1` | `localhost:19092` | 1 |
| 2 | `kafka2` | `tb-kafka2` | `localhost:29092` | 2 |
| 3 | `kafka3` | `tb-kafka3` | `localhost:39092` | 3 |

**Bootstrap string for every spike:**

```
localhost:19092,localhost:29092,localhost:39092
```

Override with `TB_BOOTSTRAP`. In Python use `tbcommon.config.bootstrap()` —
do not hardcode.

Cluster defaults match ADR-001 section 2.2: `RF=3`, `min.insync.replicas=2`,
`acks=all`, transaction state log RF=3 / min ISR=2, auto topic creation off,
12 default partitions, `group.initial.rebalance.delay.ms=0`.

## Run it

```bash
docker compose -f spikes/common/docker-compose.yml up -d
docker compose -f spikes/common/docker-compose.yml ps          # all three "healthy"
uv run --directory spikes/common python -m tbcommon.smoke      # prints {"ok": true, ...}
```

Optional Kafka UI on <http://localhost:8080>:

```bash
docker compose -f spikes/common/docker-compose.yml --profile tools up -d
```

Tear down (this **wipes cluster state**, which is the intended reset):

```bash
docker compose -f spikes/common/docker-compose.yml down
```

Storage is deliberately ephemeral — no named volumes. `restart` / `stop` /
`start` of a single broker preserve its log dirs, which is what the fault
injection needs; only `down` resets. This also sidesteps the root-owned empty
named volume problem on Windows/WSL2, where the broker runs as uid 1000.

There is **no Docker restart policy** on the brokers (`restart: "no"`). A
restart policy fires on `docker kill` — only `docker stop` marks a container as
manually stopped — so `unless-stopped` would bring a broker back up in the
middle of an injected outage and quietly invalidate the measurement. Bring
brokers back with `tbcommon.faults.start_broker()` or `docker compose start`.

For S4's topic-count stress, raise the broker heap:

```bash
TB_KAFKA_HEAP='-Xmx3G -Xms3G' docker compose -f spikes/common/docker-compose.yml up -d
```

### Measured on the reference dev box

Docker Desktop 28.3.2 (WSL2), Windows 11, `apache/kafka:4.0.0`, CPython 3.12.14,
`confluent-kafka` 2.15.1. Recorded 2026-09-20 so later runs have a baseline:

| Observation | Value |
|---|---|
| `up -d` (image cached) to all three `healthy` | 29 s |
| `kafka-metadata-quorum --status` voters | 3, no follower lag |
| `kill -s SIGKILL kafka2` → `start kafka2` → `healthy` | 12 s |
| Under-replicated partitions after that recovery | 0 |
| Smoke: 2000 msgs / 50 keys / 6 partitions, produce | 1.2 s |
| Smoke: same, consume + verify | 0.4 s |
| Smoke: loss / duplicates / reorderings | 0 / 0 / 0 |

### Windows notes

The spike venv is pinned to **3.12** (`.python-version`). `uv` would otherwise
pick the newest interpreter on the box; a 3.14 venv resolves fine here but
narrows binary-wheel availability for the packages S2 needs (`uvloop`).

In Git Bash, `docker exec` mangles absolute container paths into Windows paths
(`/opt/kafka/...` becomes `C:/Program Files/Git/opt/kafka/...`). Prefix with
`MSYS_NO_PATHCONV=1`:

```bash
MSYS_NO_PATHCONV=1 docker exec tb-kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

## Helper library `tbcommon`

Throwaway. **Nothing in `packages/` may import it.**

| Module | Use it for |
|---|---|
| `config` | bootstrap strings, per-broker bootstrap, topic prefix, RF/min-ISR |
| `envelope` | the ADR-001 5.4 `tb-*` headers, UUIDv7, `traceparent` propagation |
| `clients` | producer/consumer/transactional-producer factories with the mandated settings and cooperative-sticky-safe rebalance callbacks |
| `admin` | `TopicSpec`, `ensure_topics` (waits for leaders), `wait_for_cluster`, `partition_leaders` |
| `faults` | kill / stop / start / pause a broker; `leader_service_for(topic, partition)` |
| `checks` | per-key ordering, duplicate rate and loss analysis over `(key, seq, msg_id)` |
| `payloads` | **placeholder** synthetic ADT builder (MSH-3 = `TRIBUTARY-SYNTH`) until P0-1 lands |
| `smoke` | the P0-2 verification run |

Use `clients.make_consumer(...)` rather than building a `Consumer` by hand. It
encodes two things that are easy to get wrong:

* with `cooperative-sticky` and custom rebalance callbacks you must call
  `incremental_assign` / `incremental_unassign`; plain `assign()` raises
  `Local: Erroneous state`;
* `on_lost` must not commit — those partitions already belong to someone else.

Topics created by spikes are prefixed `spike.` (`TB_TOPIC_PREFIX`) so they can
never collide with the contract's `<env>.<ch>.*` names.

## Fast tests (no cluster needed)

```bash
uv run --directory spikes/common pytest -q
```
