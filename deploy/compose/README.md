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

## What's running

| Service | Image | Reachable at | Notes |
|---|---|---|---|
| `kafka` | `apache/kafka:4.0.0` | `127.0.0.1:19092` (host), `kafka:9092` (containers) | KRaft, single node, **lite profile: RF=1, min.insync.replicas=1** |
| `clickhouse` | `clickhouse/clickhouse-server:24.8-alpine` | `127.0.0.1:8123` (HTTP), `127.0.0.1:9000` (native) | empty archive; nothing writes to it yet |
| `postgres` | `postgres:16-alpine` | `127.0.0.1:5432` (db/user `tributary`, no password: `trust` auth) | empty control-plane DB; nothing writes to it yet |
| `channel-spec-volume-init` | `python:3.12-slim` | n/a (runs once, exits 0) | chowns the `channel-spec-published` volume so a non-root job can write to it — see Design notes |
| `topic-bootstrap` | built from `topic-bootstrap/` | n/a (runs once, exits 0) | creates the `adt-passthrough` demo channel's topics from `contracts/topics/v0/topic-defaults.json` |
| `channel-publish` | built from `channel-publish/` | n/a (runs once, exits 0) | stamps `metadata.version` and produces the demo channel spec to `_tb.dev.channels`, the worker's only source of truth for a channel; also writes the same published document to a shared volume for the receiver and loader to read |
| `mllp-receiver` | built from `images/python-app.Dockerfile` | see below | `packages/tributary-receivers`; channel spec is the published document from `channel-publish` (file, `--channel-spec`), not the raw contract example |
| `worker` | built from `images/python-app.Dockerfile` | n/a (no listener) | `packages/tributary-runtime`, `python -m tributary.runtime.worker`; consumes `_tb.dev.channels` and `dev.adt-passthrough.raw` |
| `loader` | built from `images/python-app.Dockerfile` | n/a (no listener) | `packages/tributary-loaders`, `python -m tributary.loaders.http --channel=adt-passthrough`, destination `http-stub`; reads its channel spec live from `_tb.dev.channels` (`--channel`, not a file — see below), so it depends on `channel-publish` the same way `worker` does; see further below for why delivery never succeeds in this stack |

### The loader never successfully delivers here, and that's expected

The demo channel's `http-stub` destination is `http://127.0.0.1:8080/ingest` — loopback, same
reasoning as the receiver's bind below — and there is no HTTP server listening there in this stack
(W1-8, `qa-engineer`'s e2e test brings its own). Every delivery attempt gets "connection refused";
`loader` retries per the destination's policy (blocking, `maxAttempts: 5`) and then halts the
partition. That is the retry-then-halt behaviour invariant 5 depends on, observable and correct, not a
bug in this file — it is why `loader` has no healthcheck ("running but paused, waiting for a
destination" has no single up/down signal to probe). Point it at a real stub, or let a future e2e test
bring its own, before treating a successful delivery as demonstrated.

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

- **No named volumes for data.** `stop`/`start` preserves a container's data; `down` starts clean.
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
