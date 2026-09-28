# ruff: noqa: S105, S106  (synthetic bearer tokens for a loopback test server)
"""Server-level tests over real loopback sockets with a fake `RawPublisher` (no Kafka). Invariant 1
(2xx only after the publish outcome), the protocol edge cases (slow sender, half-open, oversize,
peer reset, malformed framing), auth, per-key routing, the byte budget, and PHI-free logging.
All payloads are synthetic (`TRIBUTARY-SYNTH`)."""

from __future__ import annotations

import asyncio
import json
import logging
import shutil
import ssl
import subprocess
from collections.abc import Awaitable, Callable
from dataclasses import replace
from pathlib import Path

import pytest
from tributary.receivers.http.config import AuthSpec, HttpSettings
from tributary.receivers.http.server import HttpServer
from tributary.receivers.metrics import Metrics
from tributary.receivers.secrets import DirSecretResolver
from tributary.receivers.tls import TlsSpec
from tributary.sdk import Message
from tributary.sdk.keys import parse_key_expression

MARKER = b"SYNTHMARKER-9f3a"
TOKEN = "synthetic-token-1"  # tb-secret-scan: allow
ROTATED = "synthetic-token-2"  # tb-secret-scan: allow
BODY = json.dumps({"resourceType": "Patient", "id": "SYNTH1", "note": MARKER.decode()}).encode()


def settings(**overrides: object) -> HttpSettings:
    base = HttpSettings(
        channel="fhir-in",
        channel_version=3,
        bind="127.0.0.1",
        port=0,
        tls=None,
        path="/ingest",
        accept=frozenset({"application/fhir+json", "application/json"}),
        auth=AuthSpec("none"),
        key_expression=None,
        nack_on=frozenset(),
        ack_timeout_s=5.0,
        max_message_bytes=1_000_000,
        queue_max_bytes=64 * 1024 * 1024,
        queue_max_messages=None,
        env="dev",
        bootstrap_servers="unused:9092",
        idle_timeout_s=2.0,
        request_timeout_s=3.0,
    )
    return replace(base, **overrides)  # type: ignore[arg-type]


class FakePublisher:
    def __init__(self, outcome: bool = True) -> None:
        self.calls: list[Message] = []
        self.outcome = outcome

    async def publish(self, message: Message, *, timeout_s: float) -> bool:
        self.calls.append(message)
        return self.outcome


class Raises:
    async def publish(self, message: Message, *, timeout_s: float) -> bool:
        raise RuntimeError(f"boom {MARKER.decode()}")


class Gated:
    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self._gate = asyncio.Event()

    def release(self) -> None:
        self._gate.set()

    async def publish(self, message: Message, *, timeout_s: float) -> bool:
        self.entered.set()
        await self._gate.wait()
        return True


def request(
    body: bytes = BODY,
    *,
    path: str = "/ingest",
    method: str = "POST",
    content_type: str | None = "application/fhir+json",
    token: str | None = None,
    extra: list[str] | None = None,
    length: int | None = None,
) -> bytes:
    lines = [f"{method} {path} HTTP/1.1", "Host: x"]
    if content_type is not None:
        lines.append(f"Content-Type: {content_type}")
    if token is not None:
        lines.append(f"Authorization: Bearer {token}")
    if length is not False:  # type: ignore[comparison-overlap]
        lines.append(f"Content-Length: {len(body) if length is None else length}")
    lines += extra or []
    return ("\r\n".join(lines) + "\r\n\r\n").encode() + body


async def read_response(r: asyncio.StreamReader, timeout: float = 5.0) -> tuple[int, dict[str, str], bytes]:
    raw = await asyncio.wait_for(r.read(65536), timeout)
    head, _, body = raw.partition(b"\r\n\r\n")
    lines = head.decode().split("\r\n")
    status = int(lines[0].split(" ")[1])
    headers = {k.lower(): v for k, v in (line.split(": ", 1) for line in lines[1:])}
    return status, headers, body


async def run(
    cfg: HttpSettings,
    publisher: object,
    client: Callable[[int, HttpServer], Awaitable[None]],
    *,
    resolver: DirSecretResolver | None = None,
) -> None:
    srv = HttpServer(cfg, publisher, secret_resolver=resolver)  # type: ignore[arg-type]
    server = await srv.start()
    port = server.sockets[0].getsockname()[1]
    try:
        await client(port, srv)
    finally:
        server.close()
        await server.wait_closed()


async def post(port: int, data: bytes) -> tuple[int, dict[str, str], bytes]:
    r, w = await asyncio.open_connection("127.0.0.1", port)
    w.write(data)
    await w.drain()
    try:
        return await read_response(r)
    finally:
        w.close()


def test_202_only_after_publish_and_body_carries_msg_id() -> None:
    pub = FakePublisher()

    async def client(port: int, srv: HttpServer) -> None:
        status, _, body = await post(port, request())
        assert status == 202
        msg = pub.calls[0]
        assert json.loads(body) == {"tb-msg-id": msg.headers["tb-msg-id"]}
        assert msg.payload == BODY
        assert msg.headers["tb-content-type"] == "application/fhir+json"
        assert msg.headers["tb-channel"] == "fhir-in"
        assert msg.headers["tb-source"].startswith("http:http-0@127.0.0.1:")
        assert msg.key is None
        assert srv.metrics.acked == 1

    asyncio.run(run(settings(), pub, client))


def test_response_is_withheld_until_publish_resolves() -> None:
    pub = Gated()

    async def client(port: int, srv: HttpServer) -> None:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(request())
        await w.drain()
        await asyncio.wait_for(pub.entered.wait(), 3)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(r.read(1), 0.3)  # nothing, not even a status line, before commit
        pub.release()
        assert (await read_response(r))[0] == 202
        w.close()

    asyncio.run(run(settings(), pub, client))


def test_publish_failure_is_503_with_retry_after() -> None:
    pub = FakePublisher(outcome=False)

    async def client(port: int, srv: HttpServer) -> None:
        status, headers, body = await post(port, request())
        assert status == 503
        assert int(headers["retry-after"]) >= 1
        assert b"tb-msg-id" not in body
        assert srv.metrics.acked == 0 and srv.metrics.nacked_unavailable == 1

    asyncio.run(run(settings(), pub, client))


def test_publisher_exception_is_503_and_text_never_reaches_the_peer() -> None:
    async def client(port: int, srv: HttpServer) -> None:
        status, _, body = await post(port, request())
        assert status == 503
        assert MARKER not in body and b"boom" not in body

    asyncio.run(run(settings(), Raises(), client))


def test_content_type_charset_is_kept_and_other_parameters_dropped() -> None:
    pub = FakePublisher()

    async def client(port: int, srv: HttpServer) -> None:
        status, _, _ = await post(port, request(content_type="Application/JSON; profile=secret-x; charset=UTF-8"))
        assert status == 202
        assert pub.calls[0].headers["tb-content-type"] == "application/json; charset=utf-8"

    asyncio.run(run(settings(), pub, client))


@pytest.mark.parametrize("content_type", [None, "text/plain", "application/xml", "garbage"])
def test_unlisted_content_type_is_415_and_not_published(content_type: str | None) -> None:
    pub = FakePublisher()

    async def client(port: int, srv: HttpServer) -> None:
        assert (await post(port, request(content_type=content_type)))[0] == 415
        assert pub.calls == [] and srv.metrics.rejected_content_type == 1

    asyncio.run(run(settings(), pub, client))


def test_oversize_is_413_from_the_header_without_reading_the_body() -> None:
    pub = FakePublisher()

    async def client(port: int, srv: HttpServer) -> None:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(request(b"", length=1_000_001))  # headers only: the body is never sent
        await w.drain()
        assert (await read_response(r))[0] == 413
        assert pub.calls == [] and srv.metrics.nacked_oversize == 1
        w.close()

    asyncio.run(run(settings(), pub, client))


def test_exactly_the_limit_is_admitted() -> None:
    pub = FakePublisher()
    body = b'{"a":"' + b"x" * 993 + b'"}'

    async def client(port: int, srv: HttpServer) -> None:
        assert (await post(port, request(body)))[0] == 202
        assert (await post(port, request(body + b" ")))[0] == 413

    asyncio.run(run(settings(max_message_bytes=len(body)), pub, client))


@pytest.mark.parametrize(
    "extra, length, expected",
    [
        (["Transfer-Encoding: chunked"], None, 400),
        (["Content-Length: 5"], None, 400),  # duplicate Content-Length: ambiguous
        ([], -1, 400),
        ([], False, 411),
        (["Expect: 100-continue"], None, 417),
    ],
)
def test_malformed_framing_is_refused(extra: list[str], length: int | None, expected: int) -> None:
    pub = FakePublisher()

    async def client(port: int, srv: HttpServer) -> None:
        assert (await post(port, request(extra=extra, length=length)))[0] == expected
        assert pub.calls == []

    asyncio.run(run(settings(), pub, client))


def test_non_numeric_and_signed_content_length_is_400() -> None:
    async def client(port: int, srv: HttpServer) -> None:
        for bad in (b"+5", b"5 5", b"0x10", b"1e3"):
            raw = b"POST /ingest HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: " + bad + b"\r\n\r\n"
            assert (await post(port, raw))[0] == 400

    asyncio.run(run(settings(), FakePublisher(), client))


def test_empty_body_is_400() -> None:
    async def client(port: int, srv: HttpServer) -> None:
        assert (await post(port, request(b"")))[0] == 400

    asyncio.run(run(settings(), FakePublisher(), client))


def test_wrong_path_is_404_wrong_method_is_405_and_query_is_ignored() -> None:
    pub = FakePublisher()

    async def client(port: int, srv: HttpServer) -> None:
        assert (await post(port, request(path="/other")))[0] == 404
        assert (await post(port, request(path="/ingest/x")))[0] == 404
        status, headers, _ = await post(port, request(b"", method="GET", content_type=None, length=False))
        assert status == 405 and headers["allow"] == "POST"
        assert (await post(port, request(path="/ingest?secret=1")))[0] == 202

    asyncio.run(run(settings(), pub, client))


def test_oversized_headers_are_431() -> None:
    async def client(port: int, srv: HttpServer) -> None:
        assert (await post(port, request(extra=["X-Pad: " + "a" * 20000])))[0] == 431

    asyncio.run(run(settings(), FakePublisher(), client))


def test_malformed_header_lines_are_400() -> None:
    async def client(port: int, srv: HttpServer) -> None:
        assert (await post(port, request(extra=["no-colon-here"])))[0] == 400
        assert (await post(port, request(extra=["Bad Name : v"])))[0] == 400
        assert (await post(port, b"GARBAGE\r\n\r\n"))[0] == 400

    asyncio.run(run(settings(), FakePublisher(), client))


@pytest.fixture
def token_dir(tmp_path: Path) -> DirSecretResolver:
    (tmp_path / "tok").mkdir()
    (tmp_path / "tok" / "token").write_text(f"{TOKEN}\n{ROTATED}\n", encoding="utf-8")
    return DirSecretResolver(tmp_path)


def test_bearer_auth_is_mandatory_and_checked_before_the_body(token_dir: DirSecretResolver) -> None:
    pub = FakePublisher()
    cfg = settings(auth=AuthSpec("bearer", "tok"))

    async def client(port: int, srv: HttpServer) -> None:
        for token in (None, "wrong", "", TOKEN + "x", TOKEN[:-1]):
            status, headers, body = await post(port, request(token=token))
            assert status == 401 and headers["www-authenticate"] == "Bearer"
            assert TOKEN.encode() not in body
        assert pub.calls == [] and srv.metrics.rejected_auth == 5
        # Rejected before its body is read or the budget touched.
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(request(b"", length=500, token="wrong"))
        await w.drain()
        assert (await read_response(r))[0] == 401
        w.close()
        assert (await post(port, request(token=TOKEN)))[0] == 202
        assert (await post(port, request(token=ROTATED)))[0] == 202
        raw = request().replace(b"Content-Type", b"Authorization: Basic abc\r\nContent-Type")
        assert (await post(port, raw))[0] == 401

    asyncio.run(run(cfg, pub, client, resolver=token_dir))


def test_two_authorization_headers_are_refused(token_dir: DirSecretResolver) -> None:
    async def client(port: int, srv: HttpServer) -> None:
        status, _, _ = await post(port, request(token=TOKEN, extra=[f"Authorization: Bearer {TOKEN}"]))
        assert status == 400

    asyncio.run(run(settings(auth=AuthSpec("bearer", "tok")), FakePublisher(), client, resolver=token_dir))


def test_empty_token_secret_refuses_to_start(tmp_path: Path) -> None:
    (tmp_path / "tok").mkdir()
    (tmp_path / "tok" / "token").write_text("\n", encoding="utf-8")
    with pytest.raises(Exception, match="empty"):
        HttpServer(
            settings(auth=AuthSpec("bearer", "tok")), FakePublisher(), secret_resolver=DirSecretResolver(tmp_path)
        )  # type: ignore[arg-type]


def test_missing_token_secret_refuses_to_start(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        HttpServer(
            settings(auth=AuthSpec("bearer", "nope")), FakePublisher(), secret_resolver=DirSecretResolver(tmp_path)
        )  # type: ignore[arg-type]


PER_KEY = parse_key_expression("json:/subject/reference")


def test_per_key_channel_uses_the_shared_evaluator() -> None:
    pub = FakePublisher()
    body = json.dumps({"subject": {"reference": "Patient/SYNTH7"}}).encode()

    async def client(port: int, srv: HttpServer) -> None:
        assert (await post(port, request(body)))[0] == 202
        assert pub.calls[0].key == b"Patient/SYNTH7"

    asyncio.run(run(settings(key_expression=PER_KEY), pub, client))


def test_per_key_unresolved_key_is_400_and_never_published_null_keyed() -> None:
    pub = FakePublisher()

    async def client(port: int, srv: HttpServer) -> None:
        status, _, body = await post(port, request(BODY))
        assert status == 400 and pub.calls == []
        assert MARKER not in body and b"KEY_" not in body
        assert srv.metrics.nacked_parse_error == 1

    asyncio.run(run(settings(key_expression=PER_KEY), pub, client))


def test_header_key_scheme_is_case_insensitive_and_ambiguity_is_refused() -> None:
    pub = FakePublisher()
    cfg = settings(key_expression=parse_key_expression("header:X-Patient"))

    async def client(port: int, srv: HttpServer) -> None:
        assert (await post(port, request(extra=["x-patient: SYNTH9"])))[0] == 202
        assert pub.calls[0].key == b"SYNTH9"
        assert (await post(port, request(extra=["X-Patient: a", "x-patient: b"])))[0] == 400
        assert len(pub.calls) == 1

    asyncio.run(run(cfg, pub, client))


def test_parse_error_declared_rejects_malformed_json_only_when_declared() -> None:
    pub = FakePublisher()

    async def declared(port: int, srv: HttpServer) -> None:
        assert (await post(port, request(b'{"broken":')))[0] == 400
        assert pub.calls == []

    async def undeclared(port: int, srv: HttpServer) -> None:
        assert (await post(port, request(b'{"broken":')))[0] == 202

    asyncio.run(run(settings(nack_on=frozenset({"parse-error"})), pub, declared))
    asyncio.run(run(settings(), pub, undeclared))


def test_slow_sender_is_cut_off_by_the_whole_request_deadline() -> None:
    pub = FakePublisher()

    async def client(port: int, srv: HttpServer) -> None:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        data = request()
        loop = asyncio.get_running_loop()
        start = loop.time()
        # One byte every 0.3 s: each read is within the idle timeout, the request as a whole is not.
        closed = False
        for i in range(len(data)):
            try:
                w.write(data[i : i + 1])
                await w.drain()
            except (ConnectionError, OSError):
                closed = True
                break
            await asyncio.sleep(0.3)
            if r.at_eof() or loop.time() - start > 6:
                closed = r.at_eof()
                break
        assert closed
        assert pub.calls == [] and loop.time() - start < 6

    asyncio.run(run(settings(idle_timeout_s=1.0, request_timeout_s=1.0), pub, client))


def test_half_open_connection_is_closed_by_the_idle_timeout() -> None:
    async def client(port: int, srv: HttpServer) -> None:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        assert await asyncio.wait_for(r.read(1), 3) == b""
        await asyncio.sleep(0.05)
        assert srv.metrics.connections_open == 0
        w.close()

    asyncio.run(run(settings(idle_timeout_s=0.3), FakePublisher(), client))


def test_body_cut_short_is_never_published_or_acked() -> None:
    pub = FakePublisher()

    async def client(port: int, srv: HttpServer) -> None:
        _r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(request(BODY[:10], length=len(BODY)))
        await w.drain()
        w.close()
        await asyncio.sleep(0.3)
        assert pub.calls == [] and srv.metrics.acked == 0 and srv.metrics.connections_open == 0

    asyncio.run(run(settings(), pub, client))


def test_peer_reset_mid_request_does_not_harm_the_next_request() -> None:
    pub = FakePublisher()

    async def client(port: int, srv: HttpServer) -> None:
        import socket
        import struct

        s = socket.create_connection(("127.0.0.1", port))
        s.sendall(request()[:30])
        s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        s.close()  # RST
        await asyncio.sleep(0.1)
        assert (await post(port, request()))[0] == 202
        assert srv.metrics.connections_open == 0

    asyncio.run(run(settings(), pub, client))


def test_peer_gone_before_the_response_is_written_is_a_duplicate_not_a_loss() -> None:
    pub = Gated()

    async def client(port: int, srv: HttpServer) -> None:
        _r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(request())
        await w.drain()
        await asyncio.wait_for(pub.entered.wait(), 3)
        w.close()  # sender gives up; the record is committed anyway, the sender will re-send
        pub.release()
        await asyncio.sleep(0.3)
        assert srv.metrics.connections_open == 0

    asyncio.run(run(settings(), pub, client))


def test_max_conns_refuses_extra_connection() -> None:
    async def client(port: int, srv: HttpServer) -> None:
        _r1, w1 = await asyncio.open_connection("127.0.0.1", port)
        await asyncio.sleep(0.1)
        r2, w2 = await asyncio.open_connection("127.0.0.1", port)
        assert await asyncio.wait_for(r2.read(1), 2) == b""
        assert srv.metrics.connections_refused == 1
        w1.close()
        w2.close()

    asyncio.run(run(settings(max_conns=1), FakePublisher(), client))


def test_a_stalled_connection_does_not_block_another() -> None:
    """Head-of-line isolation (memo 8.3): connection A holds its request half-sent; B completes."""
    pub = FakePublisher()

    async def client(port: int, srv: HttpServer) -> None:
        _, wa = await asyncio.open_connection("127.0.0.1", port)
        wa.write(request()[:20])
        await wa.drain()
        for _ in range(5):
            assert (await post(port, request()))[0] == 202
        wa.close()

    asyncio.run(run(settings(), pub, client))


def test_a_connection_blocked_in_publish_does_not_block_another() -> None:
    class SelectivelyGated:
        def __init__(self) -> None:
            self.gate = asyncio.Event()
            self.calls = 0

        async def publish(self, message: Message, *, timeout_s: float) -> bool:
            self.calls += 1
            if b"HOLD" in (message.payload or b""):
                await self.gate.wait()
            return True

    pub = SelectivelyGated()

    async def client(port: int, srv: HttpServer) -> None:
        held = asyncio.create_task(post(port, request(b'{"k":"HOLD"}')))
        await asyncio.sleep(0.2)
        assert (await post(port, request()))[0] == 202
        assert not held.done()
        pub.gate.set()
        assert (await held)[0] == 202

    asyncio.run(run(settings(), pub, client))


def test_queue_full_is_503_when_the_byte_budget_is_exhausted_and_frees_after() -> None:
    pub = Gated()
    body = b'{"k":"' + b"x" * 1000 + b'"}'
    cfg = settings(queue_max_bytes=1_048_576, max_message_bytes=len(body) + 0, ack_timeout_s=0.3)
    cfg = replace(cfg, queue_max_bytes=len(body) + 10)

    async def client(port: int, srv: HttpServer) -> None:
        first = asyncio.create_task(post(port, request(body)))
        await asyncio.wait_for(pub.entered.wait(), 3)
        status, headers, _ = await post(port, request(body))
        assert status == 503 and "retry-after" in headers
        assert srv.metrics.nacked_queue_full == 1
        pub.release()
        assert (await first)[0] == 202
        assert srv.budget.used_bytes == 0  # released on every path

    asyncio.run(run(cfg, pub, client))


def test_budget_is_reserved_before_the_body_is_read() -> None:
    """A sender that announces a body and then stalls holds its reservation, so N stalled
    connections cannot buffer N x maxMessageBytes outside the budget."""
    pub = FakePublisher()

    async def client(port: int, srv: HttpServer) -> None:
        _r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(request(b"", length=5000))
        await w.drain()
        await asyncio.sleep(0.2)
        assert srv.budget.used_bytes == 5000
        w.close()
        await asyncio.sleep(0.3)
        assert srv.budget.used_bytes == 0  # released when the peer went away

    asyncio.run(run(settings(max_message_bytes=5000, queue_max_bytes=6000), pub, client))


def test_logs_carry_msg_id_and_never_payload_headers_or_path(caplog: pytest.LogCaptureFixture) -> None:
    pub = FakePublisher()

    async def client(port: int, srv: HttpServer) -> None:
        await post(port, request(token=None, path="/ingest?patient=SYNTHQUERY", extra=["X-Leak: SYNTHHEADER"]))
        await post(port, request(path="/secret-path-SYNTH"))
        await post(port, request(content_type="text/plain"))

    with caplog.at_level(logging.DEBUG):
        asyncio.run(run(settings(), pub, client))
    text = caplog.text
    for forbidden in (MARKER.decode(), "SYNTHQUERY", "SYNTHHEADER", "secret-path-SYNTH", "boom"):
        assert forbidden not in text
    events = [json.loads(rec.getMessage()) for rec in caplog.records if rec.name.endswith("http.access")]
    assert [e["status"] for e in events] == [202, 404, 415]
    assert events[0]["tb_msg_id"] == pub.calls[0].headers["tb-msg-id"]
    assert events[0]["bytes"] == len(BODY)
    assert set(events[0]) == {"event", "channel", "method", "status", "bytes", "tb_msg_id"}


def test_metrics_snapshot_has_no_payload_text() -> None:
    m = Metrics()

    async def client(port: int, srv: HttpServer) -> None:
        await post(port, request())
        assert MARKER.decode() not in json.dumps(srv.metrics.snapshot())

    asyncio.run(run(settings(), FakePublisher(), client))
    assert m.snapshot()["acked"] == 0


@pytest.mark.skipif(shutil.which("openssl") is None, reason="openssl not on PATH")
def test_tls_with_mtls_required_rejects_a_client_without_a_certificate(tmp_path: Path) -> None:
    ref = tmp_path / "srv-tls"
    ref.mkdir()
    subprocess.run(  # noqa: S603 - fixed argv, test fixture only
        [shutil.which("openssl"), "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
         "-keyout", str(ref / "tls.key"), "-out", str(ref / "tls.crt"), "-subj", "/CN=localhost",
         "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1"],
        check=True, capture_output=True,
    )  # fmt: skip
    (ref / "ca.crt").write_bytes((ref / "tls.crt").read_bytes())
    pub = FakePublisher()
    cfg = settings(tls=TlsSpec("srv-tls", "required"), auth=AuthSpec("mtls"))

    async def client(port: int, srv: HttpServer) -> None:
        ctx = ssl.create_default_context(cafile=str(ref / "tls.crt"))
        # TLS 1.3 reports the missing client certificate after the handshake completes, so the
        # failure shows up as an alert on first use or as an empty read, never as a response.
        try:
            r, w = await asyncio.open_connection("127.0.0.1", port, ssl=ctx, server_hostname="localhost")
            w.write(request())
            await w.drain()
            assert await asyncio.wait_for(r.read(10), 3) == b""
        except (ssl.SSLError, ConnectionError, OSError):
            pass
        assert pub.calls == []
        # With a client certificate (here the same self-signed pair, which chains to ca.crt) it works.
        ctx.load_cert_chain(str(ref / "tls.crt"), str(ref / "tls.key"))
        r, w = await asyncio.open_connection("127.0.0.1", port, ssl=ctx, server_hostname="localhost")
        w.write(request())
        await w.drain()
        assert (await read_response(r))[0] == 202
        w.close()

    asyncio.run(run(cfg, pub, client, resolver=DirSecretResolver(tmp_path)))
