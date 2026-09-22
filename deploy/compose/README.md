# Tributary lite stack (Compose)

The walking-skeleton deployment for local development: one Kafka broker (KRaft), ClickHouse,
PostgreSQL, the MLLP receiver, and a one-shot job that creates the demo topics. **Development only —
see the warning block at the top of `docker-compose.yml`.** Never point this stack at real patient
data (CLAUDE.md: synthetic data only).

## Start

```bash
docker compose -f deploy/compose/docker-compose.yml up -d
docker compose -f deploy/compose/docker-compose.yml ps   # everything should say "healthy"
```

Comes up healthy in under 20 seconds on a warm image cache (measured; the two-minute budget in
`docs/agent-team/phases/phase-1.md` W1-7 leaves headroom for a cold `--build`).

## What's running

| Service | Image | Reachable at | Notes |
|---|---|---|---|
| `kafka` | `apache/kafka:4.0.0` | `127.0.0.1:19092` (host), `kafka:9092` (containers) | KRaft, single node, **lite profile: RF=1, min.insync.replicas=1** |
| `clickhouse` | `clickhouse/clickhouse-server:24.8-alpine` | `127.0.0.1:8123` (HTTP), `127.0.0.1:9000` (native) | empty archive; nothing writes to it yet |
| `postgres` | `postgres:16-alpine` | `127.0.0.1:5432` (db/user `tributary`, no password: `trust` auth) | empty control-plane DB; nothing writes to it yet |
| `topic-bootstrap` | built from `topic-bootstrap/` | n/a (runs once, then exits 0) | creates the `adt-passthrough` demo channel's topics from `contracts/topics/v0/topic-defaults.json` |
| `mllp-receiver` | built from `images/python-app.Dockerfile` | see below | `packages/tributary-receivers`, channel `contracts/channel-spec/v0/examples/valid/walking-skeleton-loopback.json` |

`worker` and `loader` are **not defined yet**: `tributary-runtime` has no consumer entry point (W1-4)
and `tributary-loaders` has no HTTP loader (W1-6) as this file is written. Ask `devops-engineer` to
add them once both exist — see the comment at the end of `docker-compose.yml`.

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
**barred from production**. `topic-bootstrap` logs the mandatory startup warning
(`topic-defaults.json` `profiles.lite.warning`) at `WARNING` level on every run — it cannot be turned
off. Each data-plane component must also emit it once it exists (`contracts/topics/v0` README 3.8);
that is tracked against W1-4/5/6, not this stack.

## Design notes

- **No named volumes.** `stop`/`start` preserves a container's data; `down` starts clean. This avoids
  the root-owned-volume permission trap on Windows/WSL2 (`spikes/common/docker-compose.yml` hit the
  same thing) and keeps the "under two minutes" bar honest. Add your own volumes for a longer-lived
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
  that docstring.
