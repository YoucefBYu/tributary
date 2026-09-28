"""The MLLP `asyncio` server: framing in, publish to `<env>.<channel>.raw`, ACK after commit.

Invariant 1: `AA` is written only after the delivery report for that exact record says success. Any
publish failure, timeout or full producer queue returns `AR` — never a dropped-and-ACKed message.
If the peer disappears after the commit but before the `AA` reaches it, the sender resends: a
duplicate downstream, never a loss (invariant 4; dedupe is downstream, keyed by `tb-msg-id`).

Per connection, frames are published concurrently (a sender may pipeline) but ACKs are written back
in arrival order, matching what a pipelining MLLP sender expects. The receiver-wide `ByteBudget`
(`backpressure.py`) is the real backpressure: a full budget delays reading the next frame on every
connection until room frees up, and a wait that outlives `ack.timeoutMs` becomes a `queue-full` NACK
rather than an unbounded buffer (gate requirement 5).

A reservation is held for a frame's whole lifetime in this process, including the time spent
writing its ACK to the peer: `_ack_writer` releases it only after that write attempt finishes
(success or a detected-dead connection), never as soon as the publish outcome is known. Releasing
early would let a stalled peer's connection keep admitting and publishing new frames — each one
freeing its own budget the instant Kafka acks it — while a backlog of already-decided ACKs queues
up behind the one write that is blocked in `w.drain()`, an unbounded-item-count bypass of a
byte-count budget (code review, W1-5 second round). `conn_pipeline` bounds `q` itself for the same
reason, defense in depth: even if a future change reordered the release again, a full `q` still
backpressures the reader independently of `ByteBudget`.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import ssl
import time
from typing import Any

from ..backpressure import ByteBudget
from ..engine_kafka import engine_security_properties
from ..metrics import Metrics
from ..net import require_safe_bind
from ..secrets import DirSecretResolver, SecretResolver
from ..tls import build_ssl_context
from .ack import build_ack
from .config import MllpSettings, mllp_settings_from_channel_spec
from .framing import Fatal, FrameParser, Oversize, control_id, looks_like_hl7
from .kafka_publisher import RawPublisher, build_raw_envelope, topic_name

__all__ = ["MllpServer", "MllpSettings", "serve_from_channel_spec"]

READ_CHUNK = 65536

#: (control id, outcome, reserved byte count or None). Outcome is a completed `asyncio.Task[bool]`
#: for a message that reached the publisher, or a `str` NACK reason decided before publishing.
_QueueItem = tuple[str, "asyncio.Task[bool] | str", int | None]


class MllpServer:
    def __init__(
        self,
        cfg: MllpSettings,
        publisher: RawPublisher,
        *,
        metrics: Metrics | None = None,
        secret_resolver: SecretResolver | None = None,
    ) -> None:
        require_safe_bind(cfg.bind, tls_configured=cfg.tls is not None)
        self.cfg = cfg
        self.publisher = publisher
        self.metrics = metrics if metrics is not None else Metrics()
        self.budget = ByteBudget(cfg.queue_max_bytes, cfg.queue_max_messages)
        self._inflight = asyncio.Semaphore(cfg.max_inflight)
        self._ssl_context: ssl.SSLContext | None = None
        if cfg.tls is not None:
            self._ssl_context = build_ssl_context(cfg.tls, secret_resolver or DirSecretResolver())

    async def _publish(self, frame: bytes, remote: str, deadline: float) -> bool:
        try:
            async with self._inflight:
                timeout = max(deadline - time.monotonic(), 0.001)
                message = build_raw_envelope(
                    channel=self.cfg.channel,
                    channel_version=self.cfg.channel_version,
                    source=f"mllp:{self.cfg.instance}@{remote}",
                    payload=frame,
                    key=None,  # ordering: none is the only mode this wave supports (config.py)
                )
                t0 = time.monotonic()
                ok = await self.publisher.publish(message, timeout_s=timeout)
                self.metrics.record_ack_latency_ms((time.monotonic() - t0) * 1000)
                return ok
        except Exception:
            return False

    async def _ack_writer(self, q: asyncio.Queue[_QueueItem | None], w: asyncio.StreamWriter) -> None:
        dead = False
        while (item := await q.get()) is not None:
            cid, outcome, reserved = item
            if isinstance(outcome, str):
                out = build_ack("AR", cid, outcome)
            else:
                result = await outcome
                if result:
                    out = build_ack("AA", cid)
                    self.metrics.acked += 1
                else:
                    out = build_ack("AR", cid, "unavailable")
                    self.metrics.publish_failures += 1
                    self.metrics.nacked_unavailable += 1
            try:
                if not dead:  # keep draining so the reader never blocks on a dead peer
                    w.write(out)
                    await asyncio.wait_for(w.drain(), self.cfg.write_timeout_s)
            except (ConnectionError, OSError, TimeoutError):
                dead = True
            finally:
                # Released only now, once the write attempt (or the decision to skip it for a
                # dead peer) is over: while a stalled peer's drain() is pending, this frame's
                # bytes stay charged, so the budget cannot be bypassed by piling up written-but-
                # unflushed ACKs (code review, W1-5 second round).
                if reserved is not None:
                    await self.budget.release(reserved)

    async def handle(self, r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        peer = w.get_extra_info("peername")
        remote = f"{peer[0]}:{peer[1]}" if peer else "unknown"
        if self.metrics.connections_open >= self.cfg.max_conns:
            self.metrics.connections_refused += 1
            w.close()
            return
        self.metrics.connections_total += 1
        self.metrics.connections_open += 1
        parser = FrameParser(self.cfg.max_message_bytes)
        # Bounded independently of ByteBudget (defense in depth: see the module docstring). A full
        # q means the ack_writer has fallen behind flushing ACKs to this peer; q.put then
        # backpressures the reader directly, on top of whatever ByteBudget itself is doing.
        q: asyncio.Queue[_QueueItem | None] = asyncio.Queue(self.cfg.conn_pipeline)
        writer_task = asyncio.create_task(self._ack_writer(q, w))
        try:
            while not parser.fatal:
                chunk = await asyncio.wait_for(r.read(READ_CHUNK), self.cfg.idle_timeout_s)
                if not chunk:
                    break  # peer closed; any partial frame is discarded, never ACKed
                for frame in parser.feed(chunk):
                    if isinstance(frame, Fatal):
                        self.metrics.nacked_oversize += 1
                        await q.put(("", "oversize", None))
                        break  # unterminated/hostile oversize sender: drop the connection
                    if isinstance(frame, Oversize):
                        self.metrics.nacked_oversize += 1
                        await q.put((frame.cid, "oversize", None))
                        continue
                    self.metrics.messages_received += 1
                    cid = control_id(frame)
                    deadline = time.monotonic() + self.cfg.ack_timeout_s
                    if "parse-error" in self.cfg.nack_on and not looks_like_hl7(frame):
                        self.metrics.nacked_parse_error += 1
                        await q.put((cid, "parse-error", None))
                        continue
                    reserved = await self.budget.reserve(len(frame), max(deadline - time.monotonic(), 0))
                    if not reserved:
                        self.metrics.nacked_queue_full += 1
                        await q.put((cid, "queue-full", None))
                        continue
                    task = asyncio.ensure_future(self._publish(frame, remote, deadline))
                    await q.put((cid, task, len(frame)))
        except (TimeoutError, ConnectionError, OSError, ssl.SSLError):
            pass  # idle timeout, reset, half-open detected by the idle timer, or a TLS failure
        finally:
            try:
                await asyncio.wait_for(q.put(None), self.cfg.write_timeout_s)
                await asyncio.wait_for(writer_task, self.cfg.ack_timeout_s + self.cfg.write_timeout_s)
            except (TimeoutError, asyncio.CancelledError):
                writer_task.cancel()
            self.metrics.connections_open -= 1
            w.close()

    async def start(self) -> asyncio.base_events.Server:
        return await asyncio.start_server(
            self.handle, self.cfg.bind, self.cfg.port, ssl=self._ssl_context, backlog=1024
        )


async def serve_from_channel_spec(
    spec: dict[str, Any],
    *,
    env: str,
    bootstrap_servers: str,
    instance: str = "mllp-0",
    secret_resolver: SecretResolver | None = None,
    metrics_file: str | None = None,
    engine_security_file: str | None = None,
) -> None:
    """Load, bind and serve forever. The coroutine to run from `python -m tributary.receivers.mllp`
    and the one an integration test drives with a real Kafka cluster."""
    cfg = mllp_settings_from_channel_spec(spec, env=env, bootstrap_servers=bootstrap_servers, instance=instance)
    engine_security = engine_security_properties(
        cfg.bootstrap_servers, engine_security_file, secret_resolver or DirSecretResolver()
    )
    topic = topic_name(cfg.env, cfg.channel)
    publisher = RawPublisher(
        cfg.bootstrap_servers,
        topic,
        security=engine_security,
        linger_ms=cfg.linger_ms,
        queue_max_messages=cfg.max_inflight * 2,
    )
    publisher.start(asyncio.get_running_loop())
    metrics = Metrics()
    server = MllpServer(cfg, publisher, metrics=metrics, secret_resolver=secret_resolver)
    try:
        asyncio_server = await server.start()
        bound_port = asyncio_server.sockets[0].getsockname()[1]
        print(
            json.dumps(
                {
                    "event": "listening",
                    "channel": cfg.channel,
                    "topic": topic,
                    "port": bound_port,
                    "tls": cfg.tls is not None,
                }
            ),
            flush=True,
        )
        async with asyncio_server:
            while True:
                await asyncio.sleep(5)
                if metrics_file:
                    with open(metrics_file, "w") as f:
                        json.dump(metrics.snapshot(), f)
    finally:
        publisher.close()


def emit_lite_profile_warning(*, profile: str) -> None:
    """`contracts/topics/v0` README 3.8 / `topic-defaults.json` `profiles.lite`: RF=1
    (`min.insync.replicas: 1`) cannot guarantee RPO=0 for an acknowledged message. Every data-plane
    component must emit this, unsuppressed, at startup when running against a lite deployment —
    `deploy/compose/topic-bootstrap/bootstrap_topics.py` emits its own copy for the topic-creation
    job; this is the receiver's (`tributary-loaders.http.main` carries the loader's)."""
    if profile != "lite":
        return
    print(
        json.dumps(
            {
                "event": "lite_profile_active",
                "detail": "RF=1, min.insync.replicas=1: no RPO=0 guarantee for an acknowledged message "
                "(contracts/topics/v0/topic-defaults.json profiles.lite)",
            }
        ),
        flush=True,
    )


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--channel-spec", required=True, help="path to a contracts/channel-spec/v0 JSON document")
    p.add_argument("--env", required=True, help="the topic-name env segment, e.g. 'dev' or 'prod'")
    p.add_argument("--bootstrap", required=True, help="Kafka bootstrap.servers")
    p.add_argument("--instance", default="mllp-0", help="this process's identity, for tb-source")
    p.add_argument("--secrets-dir", help="overrides TB_SECRETS_DIR / the default secrets mount")
    p.add_argument("--metrics-file")
    p.add_argument(
        "--engine-security",
        help="JSON file (channel-spec $defs/kafkaSecurity shape) for the engine Kafka link; absent means "
        "plaintext, which is refused unless every --bootstrap host is loopback (PROVISIONAL, engine_kafka.py)",
    )
    p.add_argument(
        "--profile",
        default=os.environ.get("TB_PROFILE", "lite"),
        choices=["lite", "production"],
        help="topic-defaults.json profile in use; 'lite' emits the mandatory RF=1 startup warning",
    )
    a = p.parse_args()
    emit_lite_profile_warning(profile=a.profile)
    with open(a.channel_spec, encoding="utf-8") as f:
        spec = json.load(f)
    resolver = DirSecretResolver(a.secrets_dir) if a.secrets_dir else None
    coro = serve_from_channel_spec(
        spec,
        env=a.env,
        bootstrap_servers=a.bootstrap,
        instance=a.instance,
        secret_resolver=resolver,
        metrics_file=a.metrics_file,
        engine_security_file=a.engine_security,
    )
    try:
        import uvloop  # type: ignore[import-not-found]  # no Windows wheel; optional on this platform
    except ImportError:
        asyncio.run(coro)
    else:
        uvloop.run(coro)


if __name__ == "__main__":
    main()
