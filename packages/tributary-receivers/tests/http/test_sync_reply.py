"""Synchronous reply on the HTTP receiver (channel-spec README 3.2.6, "What the sender gets (HTTP)", set 1.8, W3-19):
the status mapping, the pending-map bounds and invariant 1, over real loopback sockets with a fake publisher and a
fake reply feed (no Kafka; the real-broker tests are `integration/test_http_sync_reply.py`). Synthetic data only."""

from __future__ import annotations

import asyncio
import contextlib
import json
import secrets
from collections.abc import Awaitable, Callable
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from tributary.receivers.http.server import HttpServer
from tributary.receivers.reply.feed import ReplyRecord
from tributary.receivers.reply.pending import PendingMap
from tributary.receivers.reply.settings import ReplySettings
from tributary.sdk import Message

from .test_server import BODY, MARKER, FakePublisher, eventually, post, read_response, request, settings

#: Statuses a client or proxy retries on its own: never on a committed path (README 3.2.6, the binding constraint).
AUTO_RETRIED = {408, 429, *range(500, 600)}


class FakeFeed:
    def __init__(self, partition: int | None = 0) -> None:
        self.partition = partition
        self.sink: PendingMap | None = None

    def choose_partition(self) -> int | None:
        return self.partition

    def attach(self, sink: PendingMap) -> None:
        self.sink = sink

    def reply(self, message: Message, status: str | None, code: str | None = None) -> None:
        assert self.sink is not None
        record = ReplyRecord(
            partition=0,
            correlation_id=message.headers["tb-correlation-id"],
            msg_id=message.headers["tb-msg-id"],
            status=status,
            code=code,
            body=b"",
        )
        self.sink.deliver_reply(record)


class Replying(FakePublisher):
    """Publishes, then (on the loop, after the callback) delivers one reply record, or none."""

    def __init__(self, feed: FakeFeed, status: str | None, code: str | None = None, *, reply: bool = True) -> None:
        super().__init__()
        self.feed, self.status, self.code, self.reply = feed, status, code, reply

    async def publish(self, message: Message, *, timeout_s: float) -> bool:
        self.calls.append(message)
        if self.reply:
            asyncio.get_running_loop().call_later(0.05, self.feed.reply, message, self.status, self.code)
        return self.outcome


async def serve(
    publisher: Any,
    feed: FakeFeed,
    client: Callable[[int, HttpServer], Awaitable[None]],
    *,
    reply: ReplySettings | None = None,
    resolver: Any = None,
    **overrides: Any,
) -> None:
    srv = HttpServer(
        settings(reply=reply or ReplySettings(timeout_s=1.0), **overrides), publisher, secret_resolver=resolver
    )
    srv.use_reply_feed(feed)  # type: ignore[arg-type]
    listener = await srv.start()
    port = listener.sockets[0].getsockname()[1]
    try:
        await client(port, srv)
    finally:
        listener.close()
        await listener.wait_closed()


def run(publisher: Any, feed: FakeFeed, client: Callable[[int, HttpServer], Awaitable[None]], **kw: Any) -> None:
    asyncio.run(serve(publisher, feed, client, **kw))


def assert_sync_shape(headers: dict[str, str], body: bytes, *, msg_id: str | None) -> dict[str, Any]:
    doc = json.loads(body)
    assert doc["v"] == 0 and "status" in doc
    assert headers["content-type"] == "application/json" and headers["cache-control"] == "no-store"
    assert doc.get("tb-msg-id") == msg_id
    if msg_id is not None:
        assert headers["tb-msg-id"] == msg_id
    assert doc.get("code") == headers.get("tb-reply-code"), "`code` is present exactly when Tb-Reply-Code is"
    assert MARKER not in body, "no payload value in an answer"
    return doc  # type: ignore[no-any-return]


def test_accepted_reply_is_200_and_only_after_the_commit_and_the_reply() -> None:
    feed = FakeFeed()
    pub = Replying(feed, "accepted")

    async def client(port: int, srv: HttpServer) -> None:
        status, headers, body = await post(port, request())
        msg = pub.calls[0]
        assert {"tb-correlation-id", "tb-reply-partition", "tb-reply-deadline"} <= set(msg.headers)
        doc = assert_sync_shape(headers, body, msg_id=msg.headers["tb-msg-id"])
        assert (status, doc["status"]) == (200, "accepted") and "tb-reply-code" not in headers
        assert srv.metrics.acked == 1 and srv.metrics.reply_aa == 1 and len(srv.pending or ()) == 0

    run(pub, feed, client)


@pytest.mark.parametrize(
    ("reply_status", "reply_code", "body_status", "header_code"),
    [
        ("rejected", "VALIDATE_SEGMENT_MISSING", "rejected", "VALIDATE_SEGMENT_MISSING"),
        ("failed", "TRANSFORM_ERROR", "failed", "TRANSFORM_ERROR"),
        ("surprising", "STEP_ERROR", "unknown", "STEP_ERROR"),  # unrecognised status: fail-safe, never 200
        (None, None, "unknown", None),
        ("rejected", "bad code\r\nX-Injected: 1", "rejected", None),  # untrusted code: shape-checked, never echoed
    ],
)
def test_every_ae_is_202_with_the_code_and_never_an_auto_retried_status(
    reply_status: str | None, reply_code: str | None, body_status: str, header_code: str | None
) -> None:
    feed = FakeFeed()
    pub = Replying(feed, reply_status, reply_code)

    async def client(port: int, srv: HttpServer) -> None:
        status, headers, body = await post(port, request())
        doc = assert_sync_shape(headers, body, msg_id=pub.calls[0].headers["tb-msg-id"])
        assert status == 202 and status not in AUTO_RETRIED
        assert doc["status"] == body_status and headers.get("tb-reply-code") == header_code
        assert "x-injected" not in headers
        assert srv.metrics.reply_ae == 1 and srv.metrics.acked == 0

    run(pub, feed, client)


def test_no_reply_by_the_deadline_is_202_reply_timeout() -> None:
    feed = FakeFeed()
    pub = Replying(feed, "accepted", reply=False)

    async def client(port: int, srv: HttpServer) -> None:
        loop = asyncio.get_running_loop()
        started = loop.time()
        status, headers, body = await post(port, request())
        assert 0.9 <= loop.time() - started < 3.0, "timeoutMs is counted from the raw callback"
        doc = assert_sync_shape(headers, body, msg_id=pub.calls[0].headers["tb-msg-id"])
        assert (status, doc["status"], headers["tb-reply-code"]) == (202, "unknown", "REPLY_TIMEOUT")
        assert srv.metrics.reply_timeouts == 1 and len(srv.pending or ()) == 0

    run(pub, feed, client)


class Timing(Replying):
    """A publisher that can tell a delivery timeout from a failure, like `RawPublisher.publish_result`."""

    def __init__(self, feed: FakeFeed, result: str) -> None:
        super().__init__(feed, "accepted", reply=False)
        self.result = result

    async def publish_result(self, message: Message, *, timeout_s: float) -> str:
        self.calls.append(message)
        return self.result


@pytest.mark.parametrize(("result", "code"), [("failed", "PRODUCE_FAILED"), ("timeout", "PRODUCE_TIMEOUT")])
def test_a_failed_raw_publish_is_503_with_retry_after_not_committed_and_names_the_message(
    result: str, code: str
) -> None:
    """`Tb-Msg-Id` and the body `tb-msg-id` appear because a publish was attempted. A delivery timeout is
    `PRODUCE_TIMEOUT`, the one `AR` where a resend can duplicate; any other failure is `PRODUCE_FAILED`."""
    feed = FakeFeed()
    pub = Timing(feed, result)

    async def client(port: int, srv: HttpServer) -> None:
        status, headers, body = await post(port, request())
        doc = assert_sync_shape(headers, body, msg_id=pub.calls[0].headers["tb-msg-id"])
        assert (status, doc["status"], headers["tb-reply-code"]) == (503, "not_committed", code)
        assert int(headers["retry-after"]) >= 1
        assert srv.metrics.publish_failures == 1 and len(srv.pending or ()) == 0
        assert srv.metrics.sync_answers == {("AR", code): 1}

    run(pub, feed, client)


def test_a_plain_bool_publisher_failure_is_produce_failed() -> None:
    feed = FakeFeed()
    pub = Replying(feed, "accepted", reply=False)
    pub.outcome = False

    async def client(port: int, srv: HttpServer) -> None:
        status, headers, _ = await post(port, request())
        assert (status, headers["tb-reply-code"]) == (503, "PRODUCE_FAILED")

    run(pub, feed, client)


def test_a_full_pending_map_is_503_reply_busy_and_nothing_is_published() -> None:
    feed = FakeFeed()
    pub = Replying(feed, "accepted", reply=False)

    async def client(port: int, srv: HttpServer) -> None:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(request())
        await w.drain()
        await eventually(lambda: len(srv.pending or ()) == 1)
        status, headers, body = await post(port, request())  # the map holds maxPending = 1
        doc = assert_sync_shape(headers, body, msg_id=None)
        assert (status, doc["status"], headers["tb-reply-code"]) == (503, "not_committed", "REPLY_BUSY")
        assert int(headers["retry-after"]) >= 1 and len(pub.calls) == 1, "nothing published for the refused one"
        first_status, first_headers, _ = await read_response(r, timeout=5.0)  # the held one times out as AE
        assert (first_status, first_headers["tb-reply-code"]) == (202, "REPLY_TIMEOUT")
        w.close()

    run(pub, feed, client, reply=ReplySettings(timeout_s=1.0, max_pending=1))


def test_a_process_without_a_reply_partition_is_503_reply_unavailable_before_publishing() -> None:
    feed = FakeFeed(partition=None)
    pub = Replying(feed, "accepted")

    async def client(port: int, srv: HttpServer) -> None:
        status, headers, body = await post(port, request())
        doc = assert_sync_shape(headers, body, msg_id=None)
        assert (status, doc["status"], headers["tb-reply-code"]) == (503, "not_committed", "REPLY_UNAVAILABLE")
        assert pub.calls == []

    run(pub, feed, client)


def test_a_sender_that_disconnects_frees_its_slot_and_the_message_is_still_processed() -> None:
    feed = FakeFeed()
    pub = Replying(feed, "accepted", reply=False)

    async def client(port: int, srv: HttpServer) -> None:
        _r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(request())
        await w.drain()
        await eventually(lambda: len(srv.pending or ()) == 1)
        w.close()
        await eventually(lambda: len(srv.pending or ()) == 0)
        assert len(pub.calls) == 1, "sync mode never cancels work"
        feed.reply(pub.calls[0], "accepted")  # a late reply finds no entry and is dropped
        await eventually(lambda: srv.open_connections == 0)

    run(pub, feed, client)


def test_idempotency_key_is_ignored_never_forwarded_echoed_or_logged(caplog: pytest.LogCaptureFixture) -> None:
    feed = FakeFeed()
    pub = Replying(feed, "accepted")
    key = "SYNTH-IDEM-KEY-7e41"

    async def client(port: int, srv: HttpServer) -> None:
        status, headers, body = await post(port, request(extra=[f"Idempotency-Key: {key}"]))
        assert status == 200
        wire = repr(headers) + body.decode()
        assert key not in wire
        message = pub.calls[0]
        assert key not in repr(message.headers) and "idempotency-key" not in {k.lower() for k in message.headers}

    run(pub, feed, client)
    assert key not in caplog.text


def test_stopping_with_a_committed_request_pending_answers_202_route_lost_not_a_dropped_socket() -> None:
    feed = FakeFeed()
    pub = Replying(feed, "accepted", reply=False)

    async def client(port: int, srv: HttpServer) -> None:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(request())
        await w.drain()
        await eventually(lambda: len(srv.pending or ()) == 1)
        srv.stop_accepting("stopping")
        assert await srv.drain(0.2) is True
        status, headers, _ = await read_response(r, timeout=5.0)
        assert (status, headers["tb-reply-code"]) == (202, "REPLY_ROUTE_LOST")
        w.close()

    run(pub, feed, client, reply=ReplySettings(timeout_s=30.0))


def test_a_spec_without_reply_still_answers_the_async_202_body() -> None:
    pub = FakePublisher()

    async def client(port: int, srv: HttpServer) -> None:
        status, headers, body = await post(port, request())
        assert status == 202 and set(json.loads(body)) == {"tb-msg-id"} and "tb-reply-code" not in headers
        assert BODY and srv.pending is None

    async def go() -> None:
        srv = HttpServer(settings(), pub)  # type: ignore[arg-type]
        listener = await srv.start()
        try:
            await client(listener.sockets[0].getsockname()[1], srv)
        finally:
            listener.close()
            await listener.wait_closed()

    asyncio.run(go())


# -- review batch: per-peer fairness, abandoned requests, the commit gate, live updates ------------------------------


class Gate(FakePublisher):
    """Publishes only when released, then reports `outcome`: the raw callback under test control."""

    def __init__(self) -> None:
        super().__init__()
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def publish(self, message: Message, *, timeout_s: float) -> bool:
        self.calls.append(message)
        self.entered.set()
        await self.release.wait()
        return self.outcome


def test_one_peer_cannot_take_every_slot_the_fairness_bound_is_per_peer() -> None:
    """HTTP has one request per connection, so a per-connection bound would never bite: it is per sender address.
    maxPending 8 gives a cap of 2 per peer (all test connections come from 127.0.0.1)."""
    feed = FakeFeed()
    pub = Replying(feed, "accepted", reply=False)

    async def client(port: int, srv: HttpServer) -> None:
        held = []
        for _ in range(2):
            r, w = await asyncio.open_connection("127.0.0.1", port)
            w.write(request())
            await w.drain()
            held.append((r, w))
        await eventually(lambda: len(srv.pending or ()) == 2)
        status, headers, body = await post(port, request())
        doc = assert_sync_shape(headers, body, msg_id=None)
        assert (status, doc["code"]) == (503, "REPLY_BUSY") and len(pub.calls) == 2
        assert srv.metrics.nacked_reply_connection_cap == 1 and srv.metrics.nacked_reply_busy == 0
        for r, w in held:
            assert (await read_response(r, timeout=5.0))[0] == 202
            w.close()

    run(pub, feed, client, reply=ReplySettings(timeout_s=1.0, max_pending=8))


def test_a_max_pending_above_max_conns_is_warned_at_start(capsys: pytest.CaptureFixture[str]) -> None:
    feed = FakeFeed()

    async def client(port: int, srv: HttpServer) -> None:
        return None

    run(Replying(feed, "accepted"), feed, client, reply=ReplySettings(timeout_s=1.0, max_pending=10_000))
    lines = [json.loads(x) for x in capsys.readouterr().err.splitlines() if "reply_max_pending_exceeds" in x]
    assert len(lines) == 1 and lines[0]["max_pending"] == 10_000 and lines[0]["max_conns"] == 512


def test_a_sender_that_floods_bytes_after_its_request_is_dropped_without_an_answer() -> None:
    """The linger bound: more than 64 KiB after the request ends the entry, writes nothing, frees the slot; the
    committed message is still processed. The access line says 499, ids only."""
    feed = FakeFeed()
    pub = Replying(feed, "accepted", reply=False)

    async def client(port: int, srv: HttpServer) -> None:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(request())
        await w.drain()
        await eventually(lambda: len(srv.pending or ()) == 1)
        w.write(b"x" * (70 * 1024))
        with contextlib.suppress(ConnectionError, OSError):
            await w.drain()
        await eventually(lambda: len(srv.pending or ()) == 0)
        assert await asyncio.wait_for(r.read(65536), 5.0) == b"", "nothing is written to a flooding sender"
        assert len(pub.calls) == 1, "sync mode never cancels work"
        w.close()

    run(pub, feed, client, reply=ReplySettings(timeout_s=30.0))


def test_an_abandoned_request_gets_an_access_line_with_ids_only(capsys: pytest.CaptureFixture[str]) -> None:
    feed = FakeFeed()
    pub = Replying(feed, "accepted", reply=False)

    async def client(port: int, srv: HttpServer) -> None:
        _r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(request())
        await w.drain()
        await eventually(lambda: len(srv.pending or ()) == 1)
        w.close()
        await eventually(lambda: srv.open_connections == 0)

    run(pub, feed, client, reply=ReplySettings(timeout_s=30.0))
    err = capsys.readouterr().err
    lines = [json.loads(x) for x in err.splitlines() if '"http_access"' in x]
    assert [line["status"] for line in lines] == [499]
    assert lines[0]["tb_msg_id"] == pub.calls[0].headers["tb-msg-id"] and MARKER.decode() not in err


def test_a_refusal_after_the_entry_is_opened_does_not_name_a_message_in_the_access_line(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Tombstoned while waiting for an in-flight slot: a plain 503, nothing published, no `tb_msg_id`."""
    feed = FakeFeed()
    pub = Replying(feed, "accepted")

    async def client(port: int, srv: HttpServer) -> None:
        srv._inflight = asyncio.Semaphore(0)  # the request waits for a slot after its entry and id exist
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(request())
        await w.drain()
        await eventually(lambda: len(srv.pending or ()) == 1)
        srv.stop_accepting("undeployed")
        srv._inflight.release()
        status, _headers, _ = await read_response(r, timeout=5.0)
        assert status == 503 and pub.calls == [] and len(srv.pending or ()) == 0
        w.close()

    run(pub, feed, client)
    lines = [json.loads(x) for x in capsys.readouterr().err.splitlines() if '"http_access"' in x]
    assert lines and all("tb_msg_id" not in line for line in lines)


@pytest.mark.parametrize("how", ["reply_first", "route_lost_first"])
def test_ae_and_route_lost_wait_for_the_raw_callback(how: str) -> None:
    """Invariant 1 at the HTTP layer: a reply (or a lost route) that arrives before the receiver's own callback is
    held; nothing is written until the callback succeeds, then the answer follows."""
    feed = FakeFeed()
    pub = Gate()

    async def client(port: int, srv: HttpServer) -> None:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(request())
        await w.drain()
        await asyncio.wait_for(pub.entered.wait(), 5.0)
        if how == "reply_first":
            feed.reply(pub.calls[0], "rejected", "VALIDATE_SEGMENT_MISSING")
        else:
            assert srv.pending is not None
            srv.pending.route_lost_all()
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(r.read(1), 0.4)  # nothing yet: the commit is not confirmed
        pub.release.set()
        status, headers, doc = await read_response(r, timeout=5.0)
        expected = "VALIDATE_SEGMENT_MISSING" if how == "reply_first" else "REPLY_ROUTE_LOST"
        assert (status, headers["tb-reply-code"]) == (202, expected) and json.loads(doc)["tb-msg-id"]
        w.close()

    run(pub, feed, client)


def test_a_reply_before_a_failed_callback_is_still_503_not_an_ae() -> None:
    feed = FakeFeed()
    pub = Gate()
    pub.outcome = False

    async def client(port: int, srv: HttpServer) -> None:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(request())
        await w.drain()
        await asyncio.wait_for(pub.entered.wait(), 5.0)
        feed.reply(pub.calls[0], "accepted")
        pub.release.set()
        status, headers, _ = await read_response(r, timeout=5.0)
        assert (status, headers["tb-reply-code"]) == (503, "PRODUCE_FAILED")
        w.close()

    run(pub, feed, client)


def test_a_live_update_applies_to_new_requests_and_entries_already_waiting_keep_their_deadline() -> None:
    """`maxPending` shrinks below the entries waiting (they are untouched; new requests are refused) and `timeoutMs`
    changes (the waiting entry keeps the one it was opened with, the next request gets the new one)."""
    feed = FakeFeed()
    pub = Replying(feed, "accepted", reply=False)

    async def client(port: int, srv: HttpServer) -> None:
        loop = asyncio.get_running_loop()
        r, w = await asyncio.open_connection("127.0.0.1", port)
        started = loop.time()
        w.write(request())
        await w.drain()
        await eventually(lambda: len(srv.pending or ()) == 1)
        await srv.apply_settings(replace(srv.cfg, reply=ReplySettings(timeout_s=2.0, max_pending=1)))
        status, headers, _ = await post(port, request())  # the map holds 1 >= the new maxPending
        assert (status, headers["tb-reply-code"]) == (503, "REPLY_BUSY")
        first = await read_response(r, timeout=5.0)
        assert (first[0], first[1]["tb-reply-code"]) == (202, "REPLY_TIMEOUT")
        assert 0.9 <= loop.time() - started < 1.9, "the waiting entry kept timeoutMs = 1"
        second_started = loop.time()
        status, headers, _ = await post(port, request())
        assert (status, headers["tb-reply-code"]) == (202, "REPLY_TIMEOUT")
        assert loop.time() - second_started >= 1.9, "a new request uses the new timeoutMs = 2"
        w.close()

    run(pub, feed, client, reply=ReplySettings(timeout_s=1.0, max_pending=10))


def test_cancelling_the_handler_mid_wait_leaves_no_entry() -> None:
    feed = FakeFeed()
    pub = Replying(feed, "accepted", reply=False)

    async def client(port: int, srv: HttpServer) -> None:
        from tributary.receivers.http.server import _Conn

        assert srv.pending is not None
        conn = _Conn(None)
        opened = srv.pending.open(sender_control_id="", connection=conn, peer="127.0.0.1")
        assert not isinstance(opened, str)
        srv.pending.commit(opened, ok=True)
        waiter = asyncio.ensure_future(srv._await_outcome(asyncio.StreamReader(), conn, opened))
        await asyncio.sleep(0.05)  # the waiter is inside its wait
        assert len(srv.pending) == 1
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert len(srv.pending) == 0 and srv.metrics.reply_pending == 0

    run(pub, feed, client, reply=ReplySettings(timeout_s=30.0))


def test_at_default_settings_one_sender_cannot_take_all_the_connections() -> None:
    """maxPending defaults to 10000 but every request holds one of `max_conns` (512) connections, so the cap is
    min(10000, 512) // 4 = 128 per sender, not 2500 (which would never bite)."""
    feed = FakeFeed()
    pub = Replying(feed, "accepted", reply=False)

    async def client(port: int, srv: HttpServer) -> None:
        assert srv.cfg.max_conns == 512 and srv.cfg.reply is not None and srv.cfg.reply.max_pending == 10_000
        assert srv.pending is not None and srv.pending.connection_cap == 128
        held = []
        for _ in range(128):
            r, w = await asyncio.open_connection("127.0.0.1", port)
            w.write(request())
            held.append((r, w))
        await eventually(lambda: len(srv.pending or ()) == 128, timeout_s=20.0)
        status, headers, _ = await post(port, request())
        assert (status, headers["tb-reply-code"]) == (503, "REPLY_BUSY")
        assert srv.metrics.nacked_reply_connection_cap == 1 and len(pub.calls) == 128
        for _r, w in held:
            w.close()

    run(pub, feed, client, reply=ReplySettings(timeout_s=30.0))


@pytest.fixture
def tokens() -> tuple[str, str]:
    """Two bearer tokens generated per run: no literal credential in the repo."""
    return secrets.token_urlsafe(24), secrets.token_urlsafe(24)


def _token_file(tmp_path: Path, *tokens: str) -> None:
    (tmp_path / "tok").mkdir(exist_ok=True)
    (tmp_path / "tok" / "token").write_text("\n".join(tokens) + "\n", encoding="utf-8")


def test_the_cap_is_per_bearer_credential_not_per_address(tmp_path: Path, tokens: tuple[str, str]) -> None:
    """Behind a proxy every sender shares an address: the fairness key is the credential that matched, so a sender
    holding its whole share does not throttle the other credential."""
    from tributary.receivers.http.config import AuthSpec
    from tributary.receivers.secrets import DirSecretResolver

    first, second = tokens
    _token_file(tmp_path, first, second)
    feed = FakeFeed()
    pub = Replying(feed, "accepted", reply=False)

    async def client(port: int, srv: HttpServer) -> None:
        held = []
        for _ in range(2):  # max_conns 8 gives a cap of 2 per credential
            r, w = await asyncio.open_connection("127.0.0.1", port)
            w.write(request(token=first))
            held.append((r, w))
        await eventually(lambda: len(srv.pending or ()) == 2)
        status, headers, _ = await post(port, request(token=first))
        assert (status, headers["tb-reply-code"]) == (503, "REPLY_BUSY"), "credential 1 is at its share"
        r2, w2 = await asyncio.open_connection("127.0.0.1", port)
        w2.write(request(token=second))
        await eventually(lambda: len(srv.pending or ()) == 3)  # credential 2, same address, is admitted
        for _r, w in [*held, (r2, w2)]:
            w.close()

    run(
        pub,
        feed,
        client,
        reply=ReplySettings(timeout_s=30.0),
        resolver=DirSecretResolver(tmp_path),
        auth=AuthSpec("bearer", "tok"),
        max_conns=8,
    )


def test_reordering_the_secret_file_keeps_a_senders_fairness_key(tmp_path: Path, tokens: tuple[str, str]) -> None:
    """The key is a digest of the matched token, not its position in the file: a rotation that reorders the file
    (applied live) must not give a sender a second share."""
    from tributary.receivers.http.config import AuthSpec
    from tributary.receivers.secrets import DirSecretResolver

    first, second = tokens
    _token_file(tmp_path, first, second)
    feed = FakeFeed()
    pub = Replying(feed, "accepted", reply=False)

    async def client(port: int, srv: HttpServer) -> None:
        held = []
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(request(token=first))
        held.append((r, w))
        await eventually(lambda: len(srv.pending or ()) == 1)
        _token_file(tmp_path, second, first)  # `first` moves from index 0 to 1
        await srv.apply_settings(srv.cfg)
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(request(token=first))
        held.append((r, w))
        await eventually(lambda: len(srv.pending or ()) == 2)
        status, headers, _ = await post(port, request(token=first))
        assert (status, headers["tb-reply-code"]) == (503, "REPLY_BUSY"), "one key before and after the reorder"
        for _r, w in held:
            w.close()

    run(
        pub,
        feed,
        client,
        reply=ReplySettings(timeout_s=30.0),
        resolver=DirSecretResolver(tmp_path),
        auth=AuthSpec("bearer", "tok"),
        max_conns=8,
    )


def test_the_fairness_key_prefers_the_client_certificate_then_the_credential_then_the_address() -> None:
    import hashlib

    from tributary.receivers.http.server import _Conn

    class Cert:
        def __init__(self, der: bytes | None) -> None:
            self.der = der

        def getpeercert(self, binary_form: bool = False) -> bytes | None:
            return self.der

    peer = HttpServer._fairness_peer
    one, two = _Conn(Cert(b"cert-one")), _Conn(Cert(b"cert-two"))
    assert peer(one, "bearer:0", "10.0.0.1:5000") == "cert:" + hashlib.sha256(b"cert-one").hexdigest()
    assert peer(one, None, "10.0.0.1:5000") != peer(two, None, "10.0.0.1:5000"), "same address, two certificates"
    assert peer(one, None, "10.0.0.1:5000") == peer(one, None, "10.0.0.2:6000"), "the key is not the address"
    assert peer(_Conn(None), "bearer:1", "10.0.0.1:5000") == "bearer:1"
    assert peer(_Conn(Cert(None)), None, "10.0.0.1:5000") == "addr:10.0.0.1"
    assert "token" not in peer(_Conn(None), "bearer:1", "x:1")
