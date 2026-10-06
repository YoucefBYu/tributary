# Tributary lite stack (Compose)

The walking-skeleton deployment for local development: one Kafka broker (KRaft), ClickHouse,
PostgreSQL, the MLLP receiver, the pipeline worker, the HTTP loader, and two one-shot jobs that seed
the topics and the channel spec. **Development only — see the warning block at the top of
`docker-compose.yml`.** Never point this stack at real patient data (CLAUDE.md: synthetic data only).

## Start

```bash
docker compose -f deploy/compose/docker-compose.yml up -d
docker compose -f deploy/compose/docker-compose.yml ps   # kafka/clickhouse/postgres/mllp-receiver: "healthy"
```

Reaches that state in 16–30 seconds, measured repeatedly, warm or cold (`--build`) — well under the
two-minute budget in `docs/agent-team/phases/phase-1.md` W1-7. `worker` and `loader` have no
healthcheck (see the table below for why) but show "Up" with `RestartCount: 0`.

## Kafka security (ADR-008 D5, engine-kafka/v0 README section 6)

Kafka runs on `SASL_SSL` exactly like production. There is no PLAINTEXT listener and no opt-out: a
container network is not loopback, and every data-plane process refuses PLAINTEXT to a non-loopback
broker.

- `kafka-tls-init` writes, at `up`, a dev CA (its key is deleted right after signing; the CA has no
  nameConstraints, acceptable only because nothing can use it afterwards), a broker certificate (SANs `kafka`, `localhost`, `127.0.0.1`) and one random
  SCRAM-SHA-512 user per component (`tb-lite-receiver`, `-worker`, `-loader`, `-channel-publish`,
  `-topic-bootstrap`, `-admin`) password, plus client certificates for the broker, the healthcheck and
  `kafka-users-init`, into the `kafka-secrets` volume. Nothing is committed, baked into an
  image or printed. It is idempotent: valid credentials are kept, and regenerated (and the broker's
  storage reformatted to match) when they have under 7 days left or the script changed. Reset
  everything with `docker compose down -v`.
- Each component mounts only its own `tb-lite-<component>` directory and the CA bundle
  (`tb-lite-kafka-ca`), read-only, by volume subpath under `/var/run/secrets/tributary`, and its own
  `engine-kafka/<component>.json` (no secrets in it) as `TB_ENGINE_KAFKA_FILE`.
- `StandardAuthorizer`, `allow.everyone.if.no.acl.found=false`. `topic-bootstrap` (a super user, the
  control-plane stand-in) creates the topics and applies each role's grants, parsed from
  `contracts/topics/v0/topic-defaults.json` `security.acls` (not restated). The receiver can write
  `raw` only; nothing but the control-plane stand-in can write `_tb.dev.channels`.
- ACLs are only ever added. Narrowing `security.acls` in the contract does not remove an old grant from
  a running broker. Broker storage is a tmpfs, so any stop or `down` starts the broker clean, and
  `down -v` also regenerates the credentials. (Regenerated credentials also force a reformat.)
- The broker's own identity is a client certificate, not a password: the controller, inter-broker and
  admin listeners are SSL with required client certificates, each on the broker's loopback. The SCRAM
  users are created after start by `kafka-users-init` (`topic-bootstrap/add_scram_users.py`, granted only ALTER on the cluster, not a super
  user), which
  shares the broker's network namespace to reach the loopback admin listener and reads each password
  from that component's own secret file. No password is ever a process argument (not in a container's
  `ps`, not in the host's), nor in a properties file, and the broker container never has them. The
  healthcheck is an ACL-less certificate principal. JMX is off (`-XX:+DisableAttachMechanism`).
- Clients pin `ECDHE+AESGCM:ECDHE+CHACHA20` and verify the hostname; the broker allows TLS 1.2/1.3
  AEAD suites only. The controller, inter-broker and admin links are SSL with required client certificates, on the broker's own loopback.

## What's running

| Service | Image | Reachable at | Notes |
|---|---|---|---|
| `kafka-tls-init` | `apache/kafka:4.0.0` (for openssl) | n/a (runs once, exits 0) | generates the dev CA, certificates and one SCRAM-SHA-512 password per component into a Compose volume; see "Kafka security" |
| `kafka-users-init` | built from `topic-bootstrap/` | n/a (runs once, exits 0) | creates those SCRAM users on the broker, from the password files |
| `kafka` | `apache/kafka:4.0.0` | `127.0.0.1:19092` (host, SASL_SSL), `kafka:9094` (containers, SASL_SSL) | KRaft, single node, **SASL_SSL only, deny-by-default ACLs**, **lite profile: RF=1, min.insync.replicas=1** |
| `clickhouse` | `clickhouse/clickhouse-server:24.8-alpine` | `127.0.0.1:8123` (HTTP), `127.0.0.1:9000` (native) | empty archive; nothing writes to it yet |
| `postgres` | `postgres:16-alpine` | `127.0.0.1:5432` (db/user `tributary`, no password: `trust` auth) | empty control-plane DB; nothing writes to it yet |
| `channel-spec-volume-init` | `python:3.12-slim` | n/a (runs once, exits 0) | chowns the `channel-spec-published` volume so a non-root job can write to it — see Design notes |
| `topic-bootstrap` | built from `topic-bootstrap/` | n/a (runs once, exits 0) | creates the `adt-passthrough` demo channel's topics from `contracts/topics/v0/topic-defaults.json` |
| `channel-publish` | built from `channel-publish/` | n/a (runs once, exits 0) | stamps `metadata.version` and produces the demo channel spec to `_tb.dev.channels`, the worker's only source of truth for a channel; also writes the same published document to a shared volume for the receiver and loader to read |
| `mllp-receiver` | built from `images/python-app.Dockerfile` | see below | `packages/tributary-receivers`; channel spec is the published document from `channel-publish` (file, `--channel-spec`), not the raw contract example |
| `worker` | built from `images/python-app.Dockerfile` | n/a (no listener) | `packages/tributary-runtime`, `python -m tributary.runtime.worker`; consumes `_tb.dev.channels` and `dev.adt-passthrough.raw` |
| `loader` | built from `images/python-app.Dockerfile` | n/a (no listener) | `packages/tributary-loaders`, `python -m tributary.loaders.http --channel=adt-passthrough`, destination `http-stub`; reads its channel spec live from `_tb.dev.channels` (`--channel`, not a file — see below), so it depends on `channel-publish` the same way `worker` does; see further below for why delivery never succeeds in this stack |

### This stack proves `raw` -> `out.<dest>`, not `out.<dest>` -> the destination

Verified end to end against a real send (not a test double): one MLLP message gets `MSA|AA`, the
worker logs `transformed` then `routed` to `_tb.dev.events`, and the record lands on
`dev.adt-passthrough.out.http-stub` with `tb-destination: http-stub`, `tb-attempt: 0`, `tb-stage:
routed` and an unchanged payload. That is the whole W1-4 delivery path this stack can demonstrate.

What it cannot demonstrate: the demo channel's `http-stub` destination is `http://127.0.0.1:8080/ingest`
— loopback, same reasoning as the receiver's bind below — and there is no HTTP server listening there
in this stack (W1-8, `qa-engineer`'s e2e test brings its own). Every delivery attempt gets "connection
refused"; `loader` retries per the destination's policy (`blocking`, `maxAttempts: 5`) — four
`partition_paused`/`partition_resumed` pairs, then `partition_halted` on the fifth, exactly that
arithmetic, observed in `_tb.dev.events` — and halts the partition rather than reordering or dropping
the message (ADR-003 D3: `halt-partition` never delivers out of order and never abandons a message, at
the cost of needing an operator; the alternative the spec could have chosen, `dlq-and-continue`, keeps
moving and leaves a gap in that key's order instead). That is correct, observable behaviour, not a bug
in this file — it is why `loader` has no healthcheck ("running but paused, waiting for a destination"
has no single up/down signal to probe). Point it at a real stub, or let a future e2e test bring its
own, before treating a successful delivery to the destination as demonstrated.

**A halted partition does not clear itself**, and there is no resume tooling yet (control plane or CLI,
not wave 1) — so running the demo twice against the same Kafka data reuses a still-halted partition.
`docker compose down -v` between runs (which also regenerates the Kafka credentials) avoids this; it is why every verification in this file's history
uses it.

## Reaching the MLLP receiver

The demo channel spec binds the MLLP listener to `127.0.0.1` (loopback) *inside the container*
(`contracts/channel-spec/v0/examples/valid/walking-skeleton-loopback.json` `source.bind`, accepted by
`require_safe_bind` without TLS). That is deliberate — the same rule that would refuse a plaintext
listener on `0.0.0.0` off-box also means Docker's port publishing cannot reach it, because a
container's own loopback is only reachable from inside that same network namespace, not through the
NAT that `ports:` sets up. So there is **no published port** for `mllp-receiver`, and the healthcheck
runs inside the container (`docker exec`, which shares its network namespace) rather than from the
host.

To send it an MLLP message yourself, join its network namespace instead of adding a `ports:` mapping:

```yaml
# one-off client, run with: docker compose -f deploy/compose/docker-compose.yml run --rm mllp-client
mllp-client:
  image: python:3.12-slim@sha256:2f17fc044b579bab302c2e8054d3a686e2cb9a83de48e70534b94cd8ebbe06a9
  network_mode: "service:mllp-receiver"
  profiles: ["tools"]
```

or run a client as `docker compose exec mllp-receiver python -c '...'` for anything simple enough to
not need its own image. This is the intended reachability model for the lite profile, not a
workaround: see `docker-compose.yml`'s `mllp-receiver` comment.

## The lite-profile durability warning

RF=1 / min.insync.replicas=1 is a human-approved, documented exception to invariant 1 (ADR-002 D14),
**barred from production**. Every one of `topic-bootstrap`, `mllp-receiver`, `worker` and `loader` logs
the mandatory startup warning (`topic-defaults.json` `profiles.lite.warning`, or `worker`'s own copy
gated on the contract's `startup_warning_required` flag rather than the literal string `lite`) at
`WARNING` level, unsuppressed, satisfying `contracts/topics/v0` README 3.8. All four default to `lite`
on their own, so nothing here would go quiet without it, but `TB_PROFILE: lite` is set explicitly on
each service's `environment:` block anyway: the day this block is copied toward a production
deployment, an explicit `TB_PROFILE: production` is a decision a reviewer can see, where an absent
variable defaulting to a warning is not.

## Design notes

- **No named volumes for data.** `stop`/`start` preserves a container's data (Kafka's is a tmpfs: it starts clean each time); `down` starts clean.
  This avoids the root-owned-volume permission trap on Windows/WSL2 (`spikes/common/docker-compose.yml`
  hit the same thing, and `channel-spec-volume-init` below exists because a *different* named volume
  hit it anyway) and keeps the "under two minutes" bar honest. Add your own volumes for a longer-lived
  local sandbox.
- **`restart: "no"` everywhere.** A future chaos test (`tests/chaos`, W1-8) kills a container
  mid-stream; any restart policy would resurrect it inside the outage window and invalidate the test,
  the same reasoning `spikes/common/docker-compose.yml` documents.
- **Every published port is bound to `127.0.0.1`.** Phase 0 gate finding High 2 / SR-8 was an
  unauthenticated cluster reachable from the whole local network because Docker's default bind is
  `0.0.0.0`. Do not remove the `127.0.0.1:` prefixes to "just" test from another machine — run the
  stack there instead.
- **ClickHouse `CLICKHOUSE_USER`/`CLICKHOUSE_PASSWORD` are both left unset.** Setting a username
  without a password makes the 24.8-alpine entrypoint disable network access for that user entirely
  (confirmed empirically); leaving both unset keeps the image's own default (`default`, no password,
  network access on), which is fine only because the port is loopback-bound.
- **`topic-bootstrap` reads `contracts/topics/v0/topic-defaults.json` at container start** (mounted
  read-only), not a copy baked into the image, so a contract change or the scheduled Linux
  re-measurement of `session.timeout.ms`/partition defaults reaches this stack without a rebuild —
  see the file's own `must_not_hardcode` block and `topic-bootstrap/bootstrap_topics.py`'s docstring.
  `_tb.<env>.audit` and `<channel>.reply` are deliberately never created here; both reasons are in
  that docstring. `worker`, `mllp-receiver` and `loader` mount the same file (via `TB_CONTRACTS_DIR`)
  for the same reason: `tributary-runtime`'s producer/consumer settings and
  `tributary-receivers`' Kafka producer config both read it rather than hardcode it.
- **`channel-publish`'s record shape is a contract with `ChannelCache`, not just an implementation
  detail.** `packages/tributary-runtime/src/tributary/runtime/channels.py` is the consumer: key =
  channel name (`ChannelCache._apply` decodes it as one), value = the published document with
  `metadata.version` stamped (channel-spec contract 6). If either changes here without a matching
  change there, `ChannelCache` does not error — by design, a bad record must not take a channel down
  (see that file's docstring) — it just keeps running whatever spec it applied last. From the outside
  that looks like "the worker ignored my deploy," not like a rejected record, so check
  `channel_spec_applied`/`channels_consumer_error` in the worker's logs first if `channel-publish`
  exits 0 but the worker's behaviour does not change.
- **`channel-spec-published` is the one named volume in this file.** `channel-publish` stamps
  `metadata.version` onto the raw contract example and writes the result there; `mllp-receiver` mounts
  it read-only and points `--channel-spec` at it instead of the raw file, so `tb-channel-version`
  matches what `channel-publish` actually produced to `_tb.dev.channels` and what `worker` applied.
  `loader` does **not** mount this volume: its CLI takes `--channel`, not `--channel-spec`, and reads
  the published document live from `_tb.<env>.channels` itself (`tributary.runtime.channels.
  open_spec_source`, the same hot-reload path `worker` uses) — one fewer place a stale copy could
  drift, and the reason `loader` still depends on `channel-publish` completing even without mounting
  its output. `channel-spec-volume-init` exists only because Docker creates a new named volume owned
  by `root:root`, and every long-running container here is non-root (Phase 1 requirement 7) — it
  chowns the volume once, as root, and is the only thing in this file that runs as root.

## RF=3 test cluster (W3-33a)

`docker-compose.rf3.yml` adds `kafka-2` and `kafka-3` to make three KRaft nodes with RF=3 and
`min.insync.replicas=2` (the `production` profile of `topic-defaults.json`), within about 3 GiB of broker memory:

    docker compose -f deploy/compose/docker-compose.yml -f deploy/compose/docker-compose.rf3.yml \
      up -d kafka kafka-2 kafka-3 kafka-users-init topic-bootstrap

Host ports 19092 to 19094 (loopback). It proves RF=3 correctness, not absolute latency. CI runs it weekly and on
demand (`.github/workflows/chaos-rf3.yml`).
