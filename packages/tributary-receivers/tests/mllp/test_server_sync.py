"""Synchronous reply mode in the MLLP server, over real TCP loopback sockets with a fake `RawPublisher` and a
fake partition source (no Kafka). The reply is delivered the way the reply feed delivers it:
`server.pending.deliver_reply(...)` on the event loop.

What is asserted here is what the sender sees (README 3.2.6): `AR` before any commit and nothing published when
the request is refused, `AE` / `AA` never ahead of the receiver's own raw callback, and the D10 answer table.
Synthetic data only (MSH-3 `TRIBUTARY-SYNTH`)."""

from __future__ import annotations

import asyncio
import socket
import struct
from collections.abc import Awaitable, Callable
from dataclasses import replace

import pytest
from tributary.receivers.errors import ConfigError
from tributary.receivers.mllp.config import MllpSettings
from tributary.receivers.mllp.server import MllpServer
from tributary.receivers.reply.feed import ReplyRecord
from tributary.receivers.reply.settings import ReplySettings
from tributary.sdk import Message
from tributary.sdk import headers as h
from tributary.sdk.codes import REPLY_BODY_OVER_BUDGET, REPLY_BUSY, REPLY_ROUTE_LOST, REPLY_TIMEOUT, REPLY_UNAVAILABLE

from ..reply.support import CONTROL_ID_MARKER, KEY_MARKER, FakePartitions
from .test_server import AlwaysFail, Gated, _read_frames, adt, frame, settings


def msa3(body: bytes) -> tuple[str, str, str]:
    """(MSA-1, MSA-2, MSA-3) of an ACK frame body."""
    parts = body.split(b"\rMSA|")[1].split(b"\r")[0].split(b"|")
    return parts[0].decode(), parts[1].decode(), parts[2].decode() if len(parts) > 2 else ""


class Recorder:
    """A `RawPublisher` that records every record and answers per a script."""

    def __init__(self, *, ok: bool = True, delay: float = 0.0) -> None:
        self.calls: list[Message] = []
        self.ok, self.delay = ok, delay
        self.seen = asyncio.Event()

    async def publish(self, message: Message, *, timeout_s: float) -> bool:
        self.calls.append(message)
        self.seen.set()
        if self.delay:
            await asyncio.sleep(self.delay)
        return self.ok

    async def nth(self, n: int, timeout: float = 5.0) -> Message:
        async with asyncio.timeout(timeout):
            while len(self.calls) < n:
                await asyncio.sleep(0.005)
        return self.calls[n - 1]


def sync_settings(**overrides: object) -> MllpSettings:
    reply = overrides.pop("reply", ReplySettings())
    return replace(settings(**overrides), reply=reply)  # type: ignore[arg-type]


def reply_for(
    message: Message, *, status: str | None = "accepted", code: str | None = None, body: bytes = b""
) -> ReplyRecord:
    return ReplyRecord(
        partition=int(message.require(h.TB_REPLY_PARTITION)),
        correlation_id=message.require(h.TB_CORRELATION_ID),
        msg_id=message.require(h.TB_MSG_ID),
        status=status,
        code=code,
        body=body,
    )


async def rig(
    cfg: MllpSettings,
    publisher: object,
    client: Callable[[MllpServer, int], Awaitable[None]],
    feed: FakePartitions | None = None,
) -> None:
    server = MllpServer(cfg, publisher)  # type: ignore[arg-type]
    server.use_reply_feed(feed if feed is not None else FakePartitions(0))  # type: ignore[arg-type]
    listener = await server.start()
    port = listener.sockets[0].getsockname()[1]
    try:
        await client(server, port)
    finally:
        listener.close()
        await listener.wait_closed()


def run(
    cfg: MllpSettings,
    publisher: object,
    client: Callable[[MllpServer, int], Awaitable[None]],
    feed: FakePartitions | None = None,
) -> None:
    asyncio.run(rig(cfg, publisher, client, feed))


async def send(port: int, *cids: str) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    r, w = await asyncio.open_connection("127.0.0.1", port)
    for cid in cids:
        w.write(frame(adt(cid)))
    await w.drain()
    return r, w


def deliver(server: MllpServer, record: ReplyRecord) -> None:
    assert server.pending is not None
    matched, _ = server.pending.deliver_reply(record)
    assert matched


async def one(server: MllpServer, port: int, publisher: Recorder, **reply: object) -> tuple[str, str, str]:
    r, w = await send(port, "C1")
    message = await publisher.nth(1)
    deliver(server, reply_for(message, **reply))  # type: ignore[arg-type]
    [f] = await _read_frames(r, 1)
    w.close()
    return msa3(f)


# -- the answer table -----------------------------------------------------------------------------------


def test_accepted_reply_gets_aa_with_the_senders_control_id_and_the_engines_msg_id() -> None:
    publisher = Recorder()

    async def client(server: MllpServer, port: int) -> None:
        code, control, text = await one(server, port, publisher)
        message = publisher.calls[0]
        assert (code, control) == ("AA", "C1") and text == message.require(h.TB_MSG_ID)
        assert server.metrics.reply_aa == 1 and server.metrics.acked == 1
        assert len(server.pending or []) == 0

    run(sync_settings(), publisher, client)


def test_the_raw_record_carries_the_reply_route_and_never_the_control_id_or_the_key() -> None:
    publisher = Recorder()

    async def client(server: MllpServer, port: int) -> None:
        await one(server, port, publisher)
        message = publisher.calls[0]
        assert message.require(h.TB_REPLY_PARTITION) == "0"
        assert h.is_well_formed(h.TB_CORRELATION_ID, message.require(h.TB_CORRELATION_ID))
        assert h.is_well_formed(h.TB_REPLY_DEADLINE, message.require(h.TB_REPLY_DEADLINE))
        everything = "".join(message.headers.values()) + (message.key or b"").decode()
        assert "C1" not in message.headers.values()
        assert CONTROL_ID_MARKER not in everything and KEY_MARKER not in everything

    run(sync_settings(), publisher, client)


def test_the_control_id_never_appears_in_a_header_even_when_it_is_distinctive() -> None:
    publisher = Recorder()

    async def client(server: MllpServer, port: int) -> None:
        r, w = await send(port, CONTROL_ID_MARKER)
        message = await publisher.nth(1)
        assert all(CONTROL_ID_MARKER not in value for value in message.headers.values())
        deliver(server, reply_for(message))
        [f] = await _read_frames(r, 1)
        assert msa3(f)[:2] == ("AA", CONTROL_ID_MARKER), "it is the sender's own value, echoed to the sender only"
        w.close()

    run(sync_settings(), publisher, client)


@pytest.mark.parametrize(
    ("status", "code", "expected_code", "text_prefix"),
    [
        ("rejected", "LAB_QUERY_UNSUPPORTED", "AE", "LAB_QUERY_UNSUPPORTED"),
        ("failed", "TRANSFORM_ERROR", "AE", "TRANSFORM_ERROR"),
        (None, None, "AE", ""),
        ("partially-accepted", None, "AE", ""),
        ("partially-accepted", "SOME_CODE", "AE", "SOME_CODE"),
    ],
)
def test_a_reject_or_a_missing_or_unknown_status_answers_ae_never_aa(
    status: str | None, code: str | None, expected_code: str, text_prefix: str
) -> None:
    publisher = Recorder()

    async def client(server: MllpServer, port: int) -> None:
        answer, control, text = await one(server, port, publisher, status=status, code=code)
        assert (answer, control) == (expected_code, "C1")
        assert text == f"{text_prefix} {publisher.calls[0].require(h.TB_MSG_ID)}".strip()
        assert server.metrics.reply_ae == 1 and server.metrics.reply_aa == 0

    run(sync_settings(), publisher, client)


def test_a_drop_is_an_accepted_reply_and_answers_aa() -> None:
    """A whole-message `Drop` is `accepted` (the worker says so); the receiver cannot tell it from success."""
    publisher = Recorder()

    async def client(server: MllpServer, port: int) -> None:
        assert (await one(server, port, publisher, status="accepted"))[0] == "AA"

    run(sync_settings(), publisher, client)


def test_a_reply_that_beats_the_raw_callback_is_held_and_nothing_is_written_early() -> None:
    class Capture(Gated):
        calls: list[Message]

        async def publish(self, message: Message, *, timeout_s: float) -> bool:
            self.calls.append(message)
            return await super().publish(message, timeout_s=timeout_s)

    capture = Capture()
    capture.calls = []

    async def client(server: MllpServer, port: int) -> None:
        r, w = await send(port, "C1")
        await asyncio.wait_for(capture.entered.wait(), 2.0)
        deliver(server, reply_for(capture.calls[0]))  # accepted, before the raw callback
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(r.read(1), 0.3)  # neither AA nor AE ahead of the commit (invariant 1)
        capture.release()
        [f] = await _read_frames(r, 1)
        assert msa3(f)[:2] == ("AA", "C1")
        w.close()

    run(sync_settings(), capture, client)


def test_a_reply_that_beats_a_failed_raw_callback_answers_ar_not_the_reply() -> None:
    class SlowFail:
        calls: list[Message]

        def __init__(self) -> None:
            self.calls = []
            self.release = asyncio.Event()

        async def publish(self, message: Message, *, timeout_s: float) -> bool:
            self.calls.append(message)
            await self.release.wait()
            return False

    publisher = SlowFail()

    async def client(server: MllpServer, port: int) -> None:
        r, w = await send(port, "C1")
        async with asyncio.timeout(2):
            while not publisher.calls:
                await asyncio.sleep(0.005)
        deliver(server, reply_for(publisher.calls[0]))
        publisher.release.set()
        [f] = await _read_frames(r, 1)
        assert msa3(f) == ("AR", "C1", "unavailable")
        w.close()

    run(sync_settings(), publisher, client)


def test_no_reply_by_the_deadline_answers_ae_reply_timeout_with_the_clock_from_the_callback() -> None:
    publisher = Recorder(delay=0.4)  # the raw callback takes 0.4 s: the 1 s reply clock must start after it

    async def client(server: MllpServer, port: int) -> None:
        r, w = await send(port, "C1")
        t0 = asyncio.get_running_loop().time()
        [f] = await _read_frames(r, 1, timeout=5.0)
        elapsed = asyncio.get_running_loop().time() - t0
        assert msa3(f)[:2] == ("AE", "C1") and msa3(f)[2].startswith(REPLY_TIMEOUT)
        assert 1.3 <= elapsed < 1.9, "timeoutMs counted from the raw callback, not from receipt"
        assert server.metrics.reply_timeouts == 1 and len(server.pending or []) == 0
        w.close()

    run(sync_settings(reply=ReplySettings(timeout_s=1.0)), publisher, client)


def test_a_failed_publish_answers_ar_unavailable_and_never_ae() -> None:
    async def client(server: MllpServer, port: int) -> None:
        r, w = await send(port, "C1")
        [f] = await _read_frames(r, 1)
        assert msa3(f) == ("AR", "C1", "unavailable")
        assert server.metrics.reply_ae == 0 and len(server.pending or []) == 0
        w.close()

    run(sync_settings(), AlwaysFail(), client)


def test_a_publish_that_outlives_ack_timeout_answers_ar() -> None:
    class Hang:
        async def publish(self, message: Message, *, timeout_s: float) -> bool:
            await asyncio.sleep(timeout_s)
            return False  # the real publisher returns False when timeout_s elapses first

    async def client(server: MllpServer, port: int) -> None:
        r, w = await send(port, "C1")
        [f] = await _read_frames(r, 1)
        assert msa3(f)[0] == "AR"
        w.close()

    run(sync_settings(ack_timeout_s=0.3), Hang(), client)


def test_a_revoke_after_the_callback_answers_ae_route_lost_at_once() -> None:
    publisher = Recorder()
    feed = FakePartitions(3)

    async def client(server: MllpServer, port: int) -> None:
        r, w = await send(port, "C1")
        message = await publisher.nth(1)
        assert message.require(h.TB_REPLY_PARTITION) == "3"
        await asyncio.sleep(0.1)  # the callback has been served
        assert server.pending is not None
        server.pending.partitions_revoked(frozenset({3}))
        [f] = await _read_frames(r, 1, timeout=1.0)
        assert msa3(f)[0] == "AE" and msa3(f)[2].startswith(REPLY_ROUTE_LOST)
        assert server.metrics.reply_route_lost == 1
        w.close()

    run(sync_settings(), publisher, client, feed)


# -- refusals, before anything is published ---------------------------------------------------------------


def test_a_process_with_no_reply_partition_refuses_ar_and_publishes_nothing() -> None:
    publisher = Recorder()

    async def client(server: MllpServer, port: int) -> None:
        r, w = await send(port, "C1")
        [f] = await _read_frames(r, 1)
        assert msa3(f) == ("AR", "C1", REPLY_UNAVAILABLE)
        assert publisher.calls == [] and server.metrics.nacked_reply_unavailable == 1
        assert server.budget.used_messages == 0
        w.close()

    run(sync_settings(), publisher, client, FakePartitions())


def test_max_pending_refuses_ar_reply_busy_and_publishes_nothing_for_the_extra_request() -> None:
    publisher = Recorder()

    async def ordered(server: MllpServer, port: int) -> None:
        r, w = await send(port, "C1", "C2")
        first = await publisher.nth(1)
        await asyncio.sleep(0.2)
        assert len(publisher.calls) == 1, "the second request never reached the publisher"
        deliver(server, reply_for(first))
        f1, f2 = await _read_frames(r, 2)
        assert msa3(f1)[:2] == ("AA", "C1")
        assert msa3(f2) == ("AR", "C2", REPLY_BUSY)
        assert server.metrics.nacked_reply_busy == 1
        w.close()

    run(sync_settings(reply=ReplySettings(max_pending=1)), publisher, ordered)


def test_start_refuses_a_synchronous_spec_without_a_reply_consumer() -> None:
    async def go() -> None:
        server = MllpServer(sync_settings(), Recorder())  # type: ignore[arg-type]
        with pytest.raises(ConfigError):
            await server.start()

    asyncio.run(go())


def test_a_failed_bind_leaves_no_sink_behind() -> None:
    async def go() -> None:
        busy = socket.socket()
        busy.bind(("127.0.0.1", 0))
        busy.listen()
        feed = FakePartitions(0)
        server = MllpServer(sync_settings(port=busy.getsockname()[1]), Recorder())  # type: ignore[arg-type]
        server.use_reply_feed(feed)  # type: ignore[arg-type]
        with pytest.raises(OSError):
            await server.start()
        assert server.pending is not None and server.pending.retired
        busy.close()

    asyncio.run(go())


# -- ordering, bodies, budgets, lifecycle ------------------------------------------------------------------


def test_answers_are_written_in_request_order_even_when_replies_arrive_out_of_order() -> None:
    publisher = Recorder()

    async def client(server: MllpServer, port: int) -> None:
        r, w = await send(port, "C1", "C2")
        second = await publisher.nth(2)
        deliver(server, reply_for(second))
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(r.read(1), 0.2)  # C2 is answered, but C1 is first in line
        deliver(server, reply_for(publisher.calls[0]))
        f1, f2 = await _read_frames(r, 2)
        assert [msa3(f1)[1], msa3(f2)[1]] == ["C1", "C2"]
        w.close()

    run(sync_settings(), publisher, client)


RESPONSE = b"MSH|^~\\&|ENGINE|LAB|TRIBUTARY-SYNTH|LAB|20260928||RSP^K11|RSP-1|P|2.5.1\rMSA|AA|C1\rQAK|Q1|OK\r"


def test_a_step_body_replaces_the_built_ack_on_aa_and_its_bytes_are_charged_then_returned() -> None:
    publisher = Recorder()

    async def client(server: MllpServer, port: int) -> None:
        r, w = await send(port, "C1", "C2")
        await publisher.nth(2)
        base = server.budget.used_bytes
        deliver(server, reply_for(publisher.calls[1], body=RESPONSE))  # C2's body waits behind C1
        assert server.budget.used_bytes == base + len(RESPONSE), "a waiting body counts against queue.maxBytes"
        deliver(server, reply_for(publisher.calls[0], body=RESPONSE))
        f1, f2 = await _read_frames(r, 2)
        assert f1 == RESPONSE and f2 == RESPONSE, "the step body is the answer, byte for byte"
        async with asyncio.timeout(2):
            while server.budget.used_bytes:
                await asyncio.sleep(0.01)
        assert server.budget.used_messages == 0
        w.close()

    run(sync_settings(reply=ReplySettings(body="step")), publisher, client)


def test_a_body_beyond_the_budget_answers_ae_reply_busy() -> None:
    publisher = Recorder()
    small = len(adt("C1")) + 10

    async def client(server: MllpServer, port: int) -> None:
        r, w = await send(port, "C1")
        message = await publisher.nth(1)
        deliver(server, reply_for(message, body=RESPONSE))
        [f] = await _read_frames(r, 1)
        assert msa3(f)[0] == "AE" and msa3(f)[2].startswith(REPLY_BODY_OVER_BUDGET)
        w.close()

    run(
        sync_settings(reply=ReplySettings(body="step"), queue_max_bytes=small, max_message_bytes=small),
        publisher,
        client,
    )


def test_a_sender_that_resets_leaves_nothing_behind() -> None:
    publisher = Recorder()

    async def client(server: MllpServer, port: int) -> None:
        _r, w = await send(port, "C1", "C2")
        await publisher.nth(2)
        sock = w.get_extra_info("socket")
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))  # close() sends RST
        w.transport.abort()
        async with asyncio.timeout(5):
            while len(server.pending or []) or server.budget.used_messages:
                await asyncio.sleep(0.02)
        assert server.budget.used_bytes == 0

    run(sync_settings(reply=ReplySettings(timeout_s=30.0)), publisher, client)


def test_a_half_closed_sender_still_gets_its_answer() -> None:
    """EOF on the read side is not a gone sender (a half-close can still read): the answer is written."""
    publisher = Recorder()

    async def client(server: MllpServer, port: int) -> None:
        r, w = await send(port, "C1")
        w.write_eof()
        message = await publisher.nth(1)
        await asyncio.sleep(0.1)
        deliver(server, reply_for(message))
        [f] = await _read_frames(r, 1)
        assert msa3(f)[:2] == ("AA", "C1")
        w.close()

    run(sync_settings(), publisher, client)


def test_a_sender_waiting_for_a_reply_is_not_idle() -> None:
    """`idleTimeout` is for connections with nothing in flight: a reply may take up to `timeoutMs`."""
    publisher = Recorder()

    async def client(server: MllpServer, port: int) -> None:
        r, w = await send(port, "C1")
        message = await publisher.nth(1)
        await asyncio.sleep(0.6)  # three idle periods
        deliver(server, reply_for(message))
        [f] = await _read_frames(r, 1)
        assert msa3(f)[:2] == ("AA", "C1")
        w.close()

    run(sync_settings(idle_timeout_s=0.2), publisher, client)


def test_a_committed_request_still_gets_its_answer_after_the_channel_is_undeployed() -> None:
    publisher = Recorder()

    async def client(server: MllpServer, port: int) -> None:
        r, w = await send(port, "C1")
        message = await publisher.nth(1)
        await asyncio.sleep(0.1)
        server.stop_accepting("channel-undeployed")
        deliver(server, reply_for(message))
        [f] = await _read_frames(r, 1)
        assert msa3(f)[:2] == ("AA", "C1")
        w.close()

    run(sync_settings(), publisher, client)


def test_drain_waits_for_waiting_replies_then_ends_what_is_left() -> None:
    publisher = Recorder()

    async def client(server: MllpServer, port: int) -> None:
        _r, w = await send(port, "C1")
        await publisher.nth(1)
        await asyncio.sleep(0.1)
        server.stop_accepting("shutting-down")
        assert await server.drain(0.3) is False, "no reply came: the budget did not drain in time"
        assert len(server.pending or []) == 0, "whatever outlived the drain is ended, not leaked"
        w.close()

    run(sync_settings(reply=ReplySettings(timeout_s=30.0)), publisher, client)


def test_a_live_update_changes_the_cap_and_a_presence_change_is_refused() -> None:
    publisher = Recorder()

    async def client(server: MllpServer, port: int) -> None:
        await server.apply_settings(replace(server.cfg, reply=ReplySettings(max_pending=7, timeout_s=2.0)))
        assert server.pending is not None and server.pending.settings.max_pending == 7
        with pytest.raises(ValueError, match="restart"):
            await server.apply_settings(replace(server.cfg, reply=None))

    run(sync_settings(), publisher, client)


def test_async_channels_are_untouched_by_a_reply_feed() -> None:
    """No `source.reply`: the existing after-commit path, AA on the raw callback alone, no reply headers."""
    publisher = Recorder()

    async def go() -> None:
        server = MllpServer(settings(), publisher)  # type: ignore[arg-type]
        listener = await server.start()
        port = listener.sockets[0].getsockname()[1]
        r, w = await send(port, "C1")
        [f] = await _read_frames(r, 1)
        assert msa3(f) == ("AA", "C1", "")
        assert h.TB_REPLY_PARTITION not in publisher.calls[0].headers
        assert h.TB_CORRELATION_ID not in publisher.calls[0].headers
        assert server.pending is None
        w.close()
        listener.close()

    asyncio.run(go())


def test_a_live_timeout_change_and_a_half_close_neither_leak_capacity_nor_drop_the_answer() -> None:
    """Regression (security review): the writer's wait was bounded by the *current* reply timeout. A request opened
    under 5 s, `timeoutMs` lowered to 1 s live, then a half-close: the handler gave up on the writer, cancelled it
    while it waited on the outcome, and the frame's reservation was never returned (`used_messages` stuck at 1)."""
    publisher = Recorder()

    async def client(server: MllpServer, port: int) -> None:
        r, w = await send(port, "C1")
        await publisher.nth(1)
        await asyncio.sleep(0.2)
        await server.apply_settings(replace(server.cfg, reply=ReplySettings(timeout_s=1.0)))
        w.write_eof()  # the sender still waits for its answer
        [f] = await _read_frames(r, 1, timeout=10.0)
        assert msa3(f)[:2] == ("AE", "C1") and REPLY_TIMEOUT in msa3(f)[2], "answered at the 5 s it was opened under"
        async with asyncio.timeout(5):
            while server.budget.used_messages or len(server.pending or []):
                await asyncio.sleep(0.02)
        assert server.budget.used_bytes == 0
        w.close()

    run(sync_settings(ack_timeout_s=1.0, write_timeout_s=0.5, reply=ReplySettings(timeout_s=5.0)), publisher, client)


def test_a_writer_cancelled_while_waiting_on_an_outcome_returns_its_reservation() -> None:
    async def go() -> None:
        server = MllpServer(sync_settings(), Recorder())  # type: ignore[arg-type]
        server.use_reply_feed(FakePartitions(0))  # type: ignore[arg-type]
        await server.start()
        assert server.pending is not None
        entry = server.pending.open(sender_control_id="C1", connection=object())
        assert not isinstance(entry, str)
        assert await server.budget.reserve(100, 1.0)
        q: asyncio.Queue = asyncio.Queue()  # type: ignore[type-arg]

        class Sink:
            def write(self, data: bytes) -> None: ...

            async def drain(self) -> None: ...

            def close(self) -> None: ...

        waiter = asyncio.ensure_future(entry.outcome)
        await q.put(("C1", waiter, 100))
        writer = asyncio.create_task(server._ack_writer(q, Sink()))  # type: ignore[arg-type]
        await asyncio.sleep(0.05)
        assert server.budget.used_messages == 1
        writer.cancel()
        with pytest.raises(asyncio.CancelledError):
            await writer
        assert server.budget.used_messages == 0 and server.budget.used_bytes == 0
        server.stop_accepting("test")

    asyncio.run(go())


def test_a_body_reply_arriving_after_the_writer_was_cancelled_does_not_stay_charged() -> None:
    """Regression (re-review): cancelling the writer at `await outcome` cancels the entry's future too. A step body
    that then arrives, before the handler abandons the connection, was charged and dropped with the outcome."""

    async def go() -> None:
        server = MllpServer(sync_settings(reply=ReplySettings(timeout_s=5.0, body="step")), Recorder())  # type: ignore[arg-type]
        server.use_reply_feed(FakePartitions(0))  # type: ignore[arg-type]
        await server.start()
        assert server.pending is not None
        conn = object()
        entry = server.pending.open(sender_control_id="C1", connection=conn)
        assert not isinstance(entry, str) and await server.budget.reserve(100, 1.0)
        publisher = server.publisher
        task = asyncio.ensure_future(
            server._publish_sync(adt("C1"), "r", asyncio.get_running_loop().time() + 1, server.cfg, None, entry)
        )  # type: ignore[arg-type]
        q: asyncio.Queue = asyncio.Queue()  # type: ignore[type-arg]

        class Sink:
            def write(self, data: bytes) -> None: ...

            async def drain(self) -> None: ...

            def close(self) -> None: ...

        await q.put(("C1", task, 100))
        writer = asyncio.create_task(server._ack_writer(q, Sink()))  # type: ignore[arg-type]
        await asyncio.sleep(0.1)
        writer.cancel()
        with pytest.raises(asyncio.CancelledError):
            await writer
        assert entry.outcome.cancelled()
        server.pending.deliver_reply(reply_for(publisher.calls[0], body=RESPONSE))  # type: ignore[attr-defined]
        server.pending.abandon(conn)
        await asyncio.sleep(0.05)
        assert server.budget.used_bytes == 0 and server.budget.used_messages == 0 and len(server.pending) == 0
        server.stop_accepting("test")

    asyncio.run(go())
