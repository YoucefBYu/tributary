"""Server-level tests over real TCP loopback sockets, with a fake `RawPublisher` standing in for
Kafka (no cluster needed). Exercises invariant 1 (ACK only after a successful publish) and the
protocol edge cases: partial frames, oversized messages, half-open/idle connections, peer resets,
ACK ordering under pipelining, connection caps, and the byte-budget `queue-full` NACK. All messages
use MSH-3 `TRIBUTARY-SYNTH` synthetic data.

Adapted from the Phase 0 S2 spike's `test_server.py`, reimplemented against this package's real
`MllpServer`/`MllpSettings`/`RawPublisher` shapes rather than imported (spikes stay out of packages).
"""

from __future__ import annotations

import asyncio
import socket as socket_mod
import struct
from collections.abc import Awaitable, Callable
from dataclasses import replace

import pytest
from tributary.receivers.mllp.config import MllpSettings
from tributary.receivers.mllp.server import MllpServer
from tributary.sdk import Message

SB, EB, CR = b"\x0b", b"\x1c", b"\x0d"


def frame(payload: bytes) -> bytes:
    return SB + payload + EB + CR


def adt(cid: str) -> bytes:
    return (
        f"MSH|^~\\&|TRIBUTARY-SYNTH|SPIKE|||20260101000000||ADT^A01|{cid}|P|2.5\r"
        "PID|1||SYNTH0001^^^SPIKE||TESTPATIENT^SYNTHETIC||19700101|U\r"
    ).encode()


def settings(**overrides: object) -> MllpSettings:
    base = MllpSettings(
        channel="adt-passthrough",
        channel_version=0,
        bind="127.0.0.1",
        port=0,
        tls=None,
        ack_timeout_s=5.0,
        nack_on=frozenset(),
        max_message_bytes=1_000_000,
        queue_max_bytes=64 * 1024 * 1024,
        queue_max_messages=None,
        env="dev",
        bootstrap_servers="unused:9092",
    )
    return replace(base, **overrides)  # type: ignore[arg-type]


class FakePublisher:
    """Records every `Message` published; resolves per a scripted outcome."""

    def __init__(self) -> None:
        self.calls: list[Message] = []

    async def publish(self, message: Message, *, timeout_s: float) -> bool:
        self.calls.append(message)
        return True


class AlwaysFail:
    async def publish(self, message: Message, *, timeout_s: float) -> bool:
        return False


class Delayed:
    """Delay is looked up from the message payload (the control id lives inside it)."""

    def __init__(self, delays: dict[bytes, float]) -> None:
        self.delays = delays

    async def publish(self, message: Message, *, timeout_s: float) -> bool:
        assert message.payload is not None
        for needle, delay in self.delays.items():
            if needle in message.payload:
                await asyncio.sleep(delay)
                break
        return True


class Gated:
    """Publish does not resolve until `release()` is called: proves the ACK writer really waits
    for the publish outcome (invariant 1) rather than writing `AA` speculatively."""

    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self._gate = asyncio.Event()

    def release(self) -> None:
        self._gate.set()

    async def publish(self, message: Message, *, timeout_s: float) -> bool:
        self.entered.set()
        await self._gate.wait()
        return True


async def _start(cfg: MllpSettings, publisher: object) -> tuple[MllpServer, asyncio.base_events.Server]:
    srv = MllpServer(cfg, publisher)  # type: ignore[arg-type]
    asyncio_srv = await srv.start()
    return srv, asyncio_srv


async def _read_frames(r: asyncio.StreamReader, n: int, timeout: float = 5.0) -> list[bytes]:
    buf = bytearray()
    out: list[bytes] = []
    while len(out) < n:
        chunk = await asyncio.wait_for(r.read(65536), timeout)
        if not chunk:
            break
        buf += chunk
        while (e := buf.find(EB + CR)) != -1:
            out.append(bytes(buf[1:e]))
            del buf[: e + 2]
    return out


def msa(frame_body: bytes) -> tuple[str, str]:
    parts = frame_body.split(b"\rMSA|")[1].split(b"|")
    return parts[0].decode(), parts[1].decode()


async def _run(cfg: MllpSettings, publisher: object, client_fn: Callable[[int], Awaitable[None]]) -> None:
    _srv, asyncio_srv = await _start(cfg, publisher)
    port = asyncio_srv.sockets[0].getsockname()[1]
    try:
        await client_fn(port)
    finally:
        asyncio_srv.close()
        await asyncio_srv.wait_closed()


def test_single_message_round_trip_gets_aa() -> None:
    async def client(port: int) -> None:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(frame(adt("C1")))
        await w.drain()
        [f] = await _read_frames(r, 1)
        assert msa(f) == ("AA", "C1")
        w.close()

    asyncio.run(_run(settings(), FakePublisher(), client))


def test_publish_failure_gets_ar_unavailable() -> None:
    async def client(port: int) -> None:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(frame(adt("C1")))
        await w.drain()
        [f] = await _read_frames(r, 1)
        assert msa(f) == ("AR", "C1")
        w.close()

    asyncio.run(_run(settings(), AlwaysFail(), client))


def test_ack_is_withheld_until_publish_resolves() -> None:
    """Invariant 1, asserted directly rather than inferred from timing (gate requirement 12's
    "unit test that AA is withheld until the publish future resolves", applied to this package)."""

    async def client(port: int, gate: Gated) -> None:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(frame(adt("C1")))
        await w.drain()
        await asyncio.wait_for(gate.entered.wait(), 2.0)
        # the publish is now in flight and deliberately not resolved: nothing must arrive yet
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(r.read(1), 0.2)
        gate.release()
        [f] = await _read_frames(r, 1)
        assert msa(f) == ("AA", "C1")
        w.close()

    gate = Gated()

    async def run() -> None:
        await _run(settings(), gate, lambda port: client(port, gate))

    asyncio.run(run())


def test_oversize_frame_gets_ar_and_connection_stays_open() -> None:
    async def client(port: int) -> None:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        big = adt("BIG") + b"X" * 100
        w.write(frame(big))
        w.write(frame(adt("SMALL")))
        await w.drain()
        f1, f2 = await _read_frames(r, 2)
        assert msa(f1) == ("AR", "BIG")
        assert msa(f2) == ("AA", "SMALL")
        w.close()

    cfg = settings(max_message_bytes=200)  # both adt() messages fit; the +100 padding does not
    asyncio.run(_run(cfg, FakePublisher(), client))


def test_partial_frame_never_acked_on_abrupt_close() -> None:
    async def client(port: int) -> None:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(SB + adt("PARTIAL")[:10])  # no terminator, ever
        await w.drain()
        w.close()
        data = await asyncio.wait_for(r.read(65536), 2.0)
        assert data == b""

    asyncio.run(_run(settings(), FakePublisher(), client))


def test_unterminated_oversize_growth_gets_ar_then_connection_closes() -> None:
    async def client(port: int) -> None:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(SB + b"x" * 500)  # grows past max_message_bytes without ever terminating
        await w.drain()
        [f] = await _read_frames(r, 1)
        assert msa(f) == ("AR", "")
        data = await asyncio.wait_for(r.read(65536), 5.0)
        assert data == b""  # connection dropped after the AR, no further traffic served
        w.close()

    cfg = settings(max_message_bytes=64)
    asyncio.run(_run(cfg, FakePublisher(), client))


def test_acks_returned_in_send_order_despite_publish_latency() -> None:
    async def client(port: int) -> None:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(frame(adt("SLOW")) + frame(adt("FAST")))
        await w.drain()
        f1, f2 = await _read_frames(r, 2)
        assert msa(f1) == ("AA", "SLOW")
        assert msa(f2) == ("AA", "FAST")
        w.close()

    pub = Delayed({b"SLOW": 0.3, b"FAST": 0.0})
    asyncio.run(_run(settings(), pub, client))


def test_idle_timeout_closes_connection() -> None:
    async def client(port: int) -> None:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        data = await asyncio.wait_for(r.read(65536), 2.0)
        assert data == b""
        w.close()

    cfg = settings(idle_timeout_s=0.2)
    asyncio.run(_run(cfg, FakePublisher(), client))


def test_peer_reset_does_not_crash_server() -> None:
    async def client(port: int) -> None:
        _r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(frame(adt("C1"))[:5])  # partial, then hard reset
        await w.drain()
        sock = w.get_extra_info("socket")
        if sock is not None:
            sock.setsockopt(socket_mod.SOL_SOCKET, socket_mod.SO_LINGER, struct.pack("ii", 1, 0))
        w.close()
        await asyncio.sleep(0.2)  # give the server a chance to observe the reset

    asyncio.run(_run(settings(), FakePublisher(), client))


def test_max_conns_refuses_extra_connection() -> None:
    async def client(port: int) -> None:
        _keep_r, keep_w = await asyncio.open_connection("127.0.0.1", port)
        await asyncio.sleep(0.1)
        r2, w2 = await asyncio.open_connection("127.0.0.1", port)
        data = await asyncio.wait_for(r2.read(1), 2.0)
        assert data == b""  # refused: closed immediately
        keep_w.close()
        w2.close()

    cfg = settings(max_conns=1, idle_timeout_s=5)
    asyncio.run(_run(cfg, FakePublisher(), client))


def test_duplicate_control_id_same_connection_gets_two_acks() -> None:
    """No default dedupe (that is the optional edge-dedupe feature): a resend produces a second
    AA and a second Kafka record, safe under at-least-once plus downstream idempotency."""

    async def client(port: int) -> None:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(frame(adt("DUP")) + frame(adt("DUP")))
        await w.drain()
        f1, f2 = await _read_frames(r, 2)
        assert msa(f1) == ("AA", "DUP")
        assert msa(f2) == ("AA", "DUP")
        w.close()

    pub = FakePublisher()
    asyncio.run(_run(settings(), pub, client))
    assert len(pub.calls) == 2


def test_queue_full_nacks_when_the_byte_budget_is_exhausted() -> None:
    """gate requirement 5: the byte budget is real backpressure, not just a config field. A budget
    smaller than one message's worth of headroom cannot admit a second in-flight message, so the
    second concurrent send gets `queue-full` while the first is still being "published"."""

    async def client(port: int, gate: Gated) -> None:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        # equal-length control ids: the budget below fits exactly one frame, not two, without
        # either frame alone exceeding the budget (that is a config error, tested separately).
        # ACKs are written back in arrival order (a pipelining sender expects that), so BBBBB's
        # queue-full AR cannot reach the wire before AAAAA's AA even though the queue-full
        # decision itself is fast; wait out ack_timeout_s before releasing AAAAA so that decision
        # has definitely already happened, then confirm both arrive, in order.
        w.write(frame(adt("AAAAA")) + frame(adt("BBBBB")))
        await w.drain()
        await asyncio.wait_for(gate.entered.wait(), 2.0)
        await asyncio.sleep(0.4)  # > ack_timeout_s below: BBBBB's reserve() has timed out by now
        gate.release()
        f1, f2 = await _read_frames(r, 2, timeout=3.0)
        assert msa(f1) == ("AA", "AAAAA")
        assert msa(f2) == ("AR", "BBBBB")
        w.close()

    gate = Gated()
    cfg = settings(queue_max_bytes=len(adt("AAAAA")), max_message_bytes=len(adt("AAAAA")), ack_timeout_s=0.2)

    async def run() -> None:
        await _run(cfg, gate, lambda port: client(port, gate))

    asyncio.run(run())


def test_parse_error_nack_only_when_declared_in_nack_on() -> None:
    async def client(port: int) -> None:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(frame(b"not hl7 at all"))
        await w.drain()
        [f] = await _read_frames(r, 1)
        assert msa(f) == ("AR", "")
        w.close()

    cfg = settings(nack_on=frozenset({"parse-error"}))
    asyncio.run(_run(cfg, FakePublisher(), client))


def test_garbage_forwarded_when_parse_error_not_declared() -> None:
    async def client(port: int) -> None:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(frame(b"not hl7 at all"))
        await w.drain()
        [f] = await _read_frames(r, 1)
        assert msa(f) == ("AA", "")
        w.close()

    pub = FakePublisher()
    asyncio.run(_run(settings(nack_on=frozenset()), pub, client))
    assert len(pub.calls) == 1


class StalledWriter:
    """A `StreamWriter`-shaped fake whose `drain()` never resolves until `gate` is set: models a
    peer that stopped reading, without depending on filling a real OS socket buffer."""

    def __init__(self) -> None:
        self.written: list[bytes] = []
        self.gate = asyncio.Event()

    def write(self, data: bytes) -> None:
        self.written.append(data)

    async def drain(self) -> None:
        await self.gate.wait()

    def get_extra_info(self, name: str) -> None:
        return None

    def close(self) -> None:
        pass


async def _resolved(value: bool) -> bool:
    return value


def test_budget_is_not_released_until_the_ack_write_completes() -> None:
    """Regression for the code-review High: releasing the byte budget as soon as the publish
    outcome resolves — rather than once the ACK has actually been written to the peer — lets a
    stalled connection's reader keep admitting and publishing new frames while a backlog of
    already-decided ACKs queues up behind one write stuck in `drain()`: an unbounded-item-count
    bypass of a byte-count budget."""

    async def go() -> None:
        cfg = settings(queue_max_bytes=100, max_message_bytes=100, write_timeout_s=1.0)
        srv = MllpServer(cfg, FakePublisher())
        await srv.budget.reserve(50, timeout=1.0)  # simulates what handle() does before queueing
        assert srv.budget.used_bytes == 50

        writer = StalledWriter()
        q: asyncio.Queue = asyncio.Queue()
        writer_task = asyncio.ensure_future(srv._ack_writer(q, writer))
        await q.put(("C1", asyncio.ensure_future(_resolved(True)), 50))

        await asyncio.sleep(0.05)  # the writer has written and is now blocked in drain()
        assert writer.written  # the AA bytes were handed to write() already
        assert srv.budget.used_bytes == 50  # but the reservation is NOT released yet

        writer.gate.set()  # let drain() finish
        await asyncio.sleep(0.05)
        assert srv.budget.used_bytes == 0  # released only now

        await q.put(None)
        await asyncio.wait_for(writer_task, 2)

    asyncio.run(go())


def test_conn_pipeline_bounds_the_ack_queue_independently_of_the_byte_budget() -> None:
    """`conn_pipeline` (code-review Medium: defined, documented, never read) backpressures the
    reader on its own, on top of whatever `ByteBudget` does — defense in depth against a future
    change to the release-timing fix above."""

    async def go() -> None:
        cfg = settings(conn_pipeline=1, queue_max_bytes=10_000_000, write_timeout_s=5.0)
        srv = MllpServer(cfg, FakePublisher())
        writer = StalledWriter()
        q: asyncio.Queue = asyncio.Queue(cfg.conn_pipeline)
        writer_task = asyncio.ensure_future(srv._ack_writer(q, writer))

        # C1 is dequeued by the writer almost immediately and then blocks in drain(), freeing a
        # queue slot even though C1 itself is not done; C2 then fills that one slot (conn_pipeline
        # = 1), so only the THIRD item has nowhere to go and blocks on put().
        await q.put(("C1", asyncio.ensure_future(_resolved(True)), None))
        await asyncio.sleep(0.05)
        await q.put(("C2", asyncio.ensure_future(_resolved(True)), None))
        put_third = asyncio.ensure_future(q.put(("C3", asyncio.ensure_future(_resolved(True)), None)))
        await asyncio.sleep(0.05)
        assert not put_third.done()  # the queue is full: a third put blocks

        writer.gate.set()
        await asyncio.wait_for(put_third, 2)
        await q.put(None)
        await asyncio.wait_for(writer_task, 2)

    asyncio.run(go())
