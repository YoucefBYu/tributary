"""S2 spike (throwaway): asyncio+uvloop MLLP server, publish to Kafka, ACK after commit.

Invariant 1: AA is written only after the producer delivery report for that message says
success (acks=all, idempotent producer). Delivery failure, delivery timeout or a full
producer queue returns AR (sender retries). Nothing is ACKed and dropped. If the peer
disappears after the commit but before the ACK, the sender will resend: duplicate, never
loss (invariant 4; dedupe is downstream).

Per connection, frames are published concurrently (pipelined) but ACKs are written in
arrival order, which is what MLLP senders that pipeline expect. All queues are bounded:
per-connection ACK queue, global in-flight semaphore, producer queue, connection count.
Never logs message content; counters and control ids only.

Usage:
  python server.py --bootstrap localhost:19092,localhost:29092,localhost:39092 \
      --topic spike.s2.raw --port 2575 [--tls-cert c.pem --tls-key k.pem]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import ssl
import threading
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass

from mllp import FATAL, FrameParser, Oversize, build_ack, control_id

MAX_MSG = 1_048_576  # 1 MB envelope inline limit (invariant 8)
READ_CHUNK = 65536

Publisher = Callable[[bytes, str], Awaitable[bool]]


@dataclass
class Settings:
    port: int = 2575
    host: str = "0.0.0.0"  # noqa: S104 - spike; product default must be explicit
    idle_timeout_s: float = 60.0
    write_timeout_s: float = 10.0
    ack_timeout_s: float = 8.0  # > kafka message.timeout.ms so Kafka's verdict wins
    max_inflight: int = 5000
    max_conns: int = 512
    conn_pipeline: int = 256
    max_msg: int = MAX_MSG
    tls_cert: str | None = None
    tls_key: str | None = None


@dataclass
class Stats:
    received: int = 0
    acked: int = 0
    rejected: int = 0
    oversize: int = 0
    conns: int = 0
    conns_refused: int = 0
    open_conns: int = 0


class KafkaPublisher:
    """Awaitable produce: resolves True only on a successful delivery report."""

    def __init__(self, bootstrap: str, topic: str, linger_ms: int, delivery_timeout_ms: int,
                 queue_max: int) -> None:
        from confluent_kafka import Producer  # lazy: unit tests need no broker client

        self.topic = topic
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "enable.idempotence": True,  # implies acks=all, retries, in-flight<=5
                "acks": "all",
                "linger.ms": linger_ms,
                "compression.type": "none",
                "message.timeout.ms": delivery_timeout_ms,
                "queue.buffering.max.messages": queue_max,
                "socket.keepalive.enable": True,
            }
        )
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop = threading.Event()
        self._poller = threading.Thread(target=self._poll, daemon=True)

    def start(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._poller.start()

    def _poll(self) -> None:
        while not self._stop.is_set():
            self.producer.poll(0.005)

    def close(self) -> None:
        self._stop.set()
        self.producer.flush(5)

    async def __call__(self, payload: bytes, cid: str) -> bool:
        return await self.publish(payload, cid, [("tb-msg-id", str(uuid.uuid4()).encode())])

    async def publish(self, payload: bytes, key: str, headers: list[tuple[str, bytes]],
                      partition: int = -1) -> bool:
        loop = self._loop
        assert loop is not None
        fut: asyncio.Future[bool] = loop.create_future()

        def cb(err: object, _msg: object) -> None:
            loop.call_soon_threadsafe(lambda: fut.done() or fut.set_result(err is None))

        try:
            self.producer.produce(self.topic, value=payload, key=key.encode(), headers=headers,
                                  on_delivery=cb)
        except BufferError:
            return False  # local queue full -> refuse rather than buffer unboundedly
        return await fut


class DryRunPublisher:
    """NOT invariant-1 compliant (ACKs without committing). Only for measuring the MLLP
    receive/ACK overhead floor with no Kafka. Results from it must never be reported as
    S2 verdict evidence."""

    async def __call__(self, payload: bytes, cid: str) -> bool:
        return True


class Server:
    def __init__(self, cfg: Settings, publisher: Publisher) -> None:
        self.cfg = cfg
        self.publish = publisher
        self.stats = Stats()
        self.sem = asyncio.Semaphore(cfg.max_inflight)

    async def _publish_guarded(self, frame: bytes, cid: str) -> bool:
        try:
            async with self.sem:
                return await asyncio.wait_for(self.publish(frame, cid), self.cfg.ack_timeout_s)
        except Exception:  # noqa: BLE001 - any failure means "not committed" -> AR
            return False

    def render(self, cid: str, result: object) -> tuple[bytes, bool]:
        """Wire bytes for a finished publish. Subclasses (S3 sync reply) override."""
        ok = result is True
        return (build_ack("AA", cid) if ok else build_ack("AR", cid, "unavailable")), ok

    async def _ack_writer(self, q: asyncio.Queue[tuple[str, object] | None],
                          w: asyncio.StreamWriter) -> None:
        dead = False
        while (item := await q.get()) is not None:
            cid, what = item
            if isinstance(what, str):
                out = build_ack("AR", cid, what)  # pre-decided rejection (e.g. oversize)
                self.stats.oversize += 1
            else:
                assert isinstance(what, asyncio.Task)
                result = await what  # ACK only after the commit verdict
                out, ok = self.render(cid, result)
                if ok:
                    self.stats.acked += 1
                else:
                    self.stats.rejected += 1
            if dead:
                continue  # keep draining so the reader never blocks on a dead peer
            try:
                w.write(out)
                await asyncio.wait_for(w.drain(), self.cfg.write_timeout_s)
            except (ConnectionError, OSError, TimeoutError):
                dead = True

    async def handle(self, r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        if self.stats.open_conns >= self.cfg.max_conns:
            self.stats.conns_refused += 1
            w.close()
            return
        self.stats.conns += 1
        self.stats.open_conns += 1
        parser = FrameParser(self.cfg.max_msg)
        q: asyncio.Queue[tuple[str, object] | None] = asyncio.Queue(self.cfg.conn_pipeline)
        wt = asyncio.create_task(self._ack_writer(q, w))
        try:
            while not parser.fatal:
                chunk = await asyncio.wait_for(r.read(READ_CHUNK), self.cfg.idle_timeout_s)
                if not chunk:
                    break  # peer closed; partial frame discarded, never ACKed
                for frame in parser.feed(chunk):
                    if frame is FATAL:
                        await q.put(("", "too large"))
                        break  # drop connection: unterminated/hostile oversize sender
                    if isinstance(frame, Oversize):
                        await q.put((frame.cid, "too large"))
                        continue
                    self.stats.received += 1
                    cid = control_id(frame)
                    # bounded queue -> backpressure to the sender via TCP
                    await q.put((cid, asyncio.ensure_future(self._publish_guarded(frame, cid))))
        except (TimeoutError, ConnectionError, OSError, ssl.SSLError):
            pass  # idle timeout, reset, half-open detected by idle timer, TLS failure
        finally:
            try:
                await asyncio.wait_for(q.put(None), self.cfg.write_timeout_s)
                await asyncio.wait_for(wt, self.cfg.ack_timeout_s + self.cfg.write_timeout_s)
            except (TimeoutError, asyncio.CancelledError):
                wt.cancel()
            self.stats.open_conns -= 1
            w.close()

    def ssl_context(self) -> ssl.SSLContext | None:
        if not self.cfg.tls_cert:
            return None
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.load_cert_chain(self.cfg.tls_cert, self.cfg.tls_key)
        return ctx

    async def start(self) -> asyncio.Server:
        return await asyncio.start_server(
            self.handle, self.cfg.host, self.cfg.port, ssl=self.ssl_context(), backlog=1024
        )


async def amain(a: argparse.Namespace) -> None:
    cfg = Settings(port=a.port, tls_cert=a.tls_cert, tls_key=a.tls_key, max_inflight=a.max_inflight)
    loop = asyncio.get_running_loop()
    pub: Publisher
    kp: KafkaPublisher | None = None
    if a.dry_run_no_kafka:
        print(json.dumps({"warning": "dry-run: ACKs are NOT after commit"}), flush=True)
        pub = DryRunPublisher()
    else:
        kp = KafkaPublisher(a.bootstrap, a.topic, a.linger_ms, a.delivery_timeout_ms,
                            a.max_inflight * 2)
        kp.start(loop)
        pub = kp
    srv = Server(cfg, pub)
    s = await srv.start()
    port = s.sockets[0].getsockname()[1]
    print(json.dumps({"event": "listening", "port": port, "tls": cfg.tls_cert is not None}),
          flush=True)
    try:
        async with s:
            while True:
                await asyncio.sleep(5)
                if a.metrics_file:
                    with open(a.metrics_file, "w") as f:
                        json.dump(asdict(srv.stats), f)
    finally:
        if kp:
            kp.close()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="localhost:19092,localhost:29092,localhost:39092")
    p.add_argument("--topic", default="spike.s2.raw")
    p.add_argument("--port", type=int, default=2575)
    p.add_argument("--tls-cert")
    p.add_argument("--tls-key")
    p.add_argument("--linger-ms", type=int, default=2)
    p.add_argument("--delivery-timeout-ms", type=int, default=5000)
    p.add_argument("--max-inflight", type=int, default=5000)
    p.add_argument("--metrics-file")
    p.add_argument("--dry-run-no-kafka", action="store_true")
    a = p.parse_args()
    try:
        import uvloop
    except ImportError:
        asyncio.run(amain(a))
    else:
        uvloop.run(amain(a))


if __name__ == "__main__":
    main()
