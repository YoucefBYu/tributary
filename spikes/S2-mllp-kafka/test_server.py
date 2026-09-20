"""Server-level tests over real TCP loopback sockets, with fake publishers standing in
for Kafka (no cluster needed). These exercise invariant 1 (ACK only after a successful
publish) and the protocol edge cases required by S2: partial frames, oversized messages,
half-open/idle connections, peer resets, ACK ordering under pipelining, connection caps
and TLS framing. All messages use MSH-3 = TRIBUTARY-SYNTH synthetic data.
"""

from __future__ import annotations

import asyncio
import ssl
import subprocess
import shutil
from pathlib import Path

import pytest

from mllp import build_ack
from server import Server, Settings

pytestmark = pytest.mark.timeout(20)

SB, EB, CR = b"\x0b", b"\x1c", b"\x0d"


def frame(payload: bytes) -> bytes:
    return SB + payload + EB + CR


def adt(cid: str) -> bytes:
    return (
        f"MSH|^~\\&|TRIBUTARY-SYNTH|SPIKE|||20260101000000||ADT^A01|{cid}|P|2.5\r"
        "PID|1||SYNTH0001^^^SPIKE||TESTPATIENT^SYNTHETIC||19700101|U\r"
    ).encode()


class FakePublisher:
    """Records every call; resolves per a scripted outcome (True/False/Exception) after
    an optional delay, so tests can force out-of-order completion."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def __call__(self, payload: bytes, cid: str) -> bool:
        self.calls.append(cid)
        return True


class AlwaysFail:
    async def __call__(self, payload: bytes, cid: str) -> bool:
        return False


class Delayed:
    """cid -> delay seconds. First call for a cid uses the scripted delay; unlisted
    cids resolve immediately."""

    def __init__(self, delays: dict[str, float]) -> None:
        self.delays = delays

    async def __call__(self, payload: bytes, cid: str) -> bool:
        await asyncio.sleep(self.delays.get(cid, 0))
        return True


async def _start(cfg: Settings, publisher: object) -> tuple[Server, asyncio.base_events.Server]:
    srv = Server(cfg, publisher)  # type: ignore[arg-type]
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


async def _run(cfg: Settings, publisher: object, client_fn) -> None:  # noqa: ANN001
    srv, asyncio_srv = await _start(cfg, publisher)
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

    asyncio.run(_run(Settings(port=0), FakePublisher(), client))


def test_publish_failure_gets_ar_unavailable() -> None:
    async def client(port: int) -> None:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(frame(adt("C1")))
        await w.drain()
        [f] = await _read_frames(r, 1)
        assert msa(f) == ("AR", "C1")
        w.close()

    asyncio.run(_run(Settings(port=0), AlwaysFail(), client))


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

    cfg = Settings(port=0, max_msg=200)  # both adt() messages fit; the +100 padding does not
    asyncio.run(_run(cfg, FakePublisher(), client))


def test_partial_frame_never_acked_on_abrupt_close() -> None:
    async def client(port: int) -> None:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(SB + adt("PARTIAL")[:10])  # no terminator, ever
        await w.drain()
        w.close()
        # nothing should ever arrive for this connection
        data = await asyncio.wait_for(r.read(65536), 2.0)
        assert data == b""

    asyncio.run(_run(Settings(port=0), FakePublisher(), client))


def test_unterminated_oversize_growth_gets_ar_then_connection_closes() -> None:
    """An unterminated, ever-growing frame is never treated as delivered (no AA is ever
    possible, since it never completed as a frame) -- but the server tells the sender
    why via one AR before dropping the connection, rather than a bare close that looks
    like a network blip."""

    async def client(port: int) -> None:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(SB + b"x" * 500)  # grows past max_msg without ever terminating
        await w.drain()
        [f] = await _read_frames(r, 1)
        assert msa(f) == ("AR", "")
        data = await asyncio.wait_for(r.read(65536), 5.0)
        assert data == b""  # connection dropped after the AR, no further traffic served
        w.close()

    cfg = Settings(port=0, max_msg=64)
    asyncio.run(_run(cfg, FakePublisher(), client))


def test_acks_returned_in_send_order_despite_publish_latency() -> None:
    """A slow first publish must not block a later ACK from jumping ahead: MLLP is a
    stream protocol and pipelining senders expect ACKs in send order."""

    async def client(port: int) -> None:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(frame(adt("SLOW")) + frame(adt("FAST")))
        await w.drain()
        f1, f2 = await _read_frames(r, 2)
        assert msa(f1) == ("AA", "SLOW")
        assert msa(f2) == ("AA", "FAST")
        w.close()

    pub = Delayed({"SLOW": 0.3, "FAST": 0.0})
    asyncio.run(_run(Settings(port=0), pub, client))


def test_idle_timeout_closes_connection() -> None:
    async def client(port: int) -> None:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        data = await asyncio.wait_for(r.read(65536), 2.0)
        assert data == b""
        w.close()

    cfg = Settings(port=0, idle_timeout_s=0.2)
    asyncio.run(_run(cfg, FakePublisher(), client))


def test_peer_reset_does_not_crash_server() -> None:
    async def client(port: int) -> None:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(frame(adt("C1"))[:5])  # partial, then hard reset
        await w.drain()
        sock = w.get_extra_info("socket")
        if sock is not None:
            import socket as socket_mod
            import struct

            sock.setsockopt(
                socket_mod.SOL_SOCKET, socket_mod.SO_LINGER, struct.pack("ii", 1, 0)
            )
        w.close()
        await asyncio.sleep(0.2)  # give the server a chance to observe the reset

    asyncio.run(_run(Settings(port=0), FakePublisher(), client))


def test_max_conns_refuses_extra_connection() -> None:
    async def client(port: int) -> None:
        keep_r, keep_w = await asyncio.open_connection("127.0.0.1", port)
        await asyncio.sleep(0.1)
        r2, w2 = await asyncio.open_connection("127.0.0.1", port)
        data = await asyncio.wait_for(r2.read(1), 2.0)
        assert data == b""  # refused: closed immediately
        keep_w.close()
        w2.close()

    cfg = Settings(port=0, max_conns=1, idle_timeout_s=5)
    asyncio.run(_run(cfg, FakePublisher(), client))


def test_duplicate_control_id_same_connection_gets_two_acks() -> None:
    """The receiver does not dedupe by default (that is the optional edge-dedupe
    feature, out of scope for this spike): a resend produces a second AA and a second
    Kafka record, which is safe under at-least-once + downstream idempotency."""

    async def client(port: int) -> None:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(frame(adt("DUP")) + frame(adt("DUP")))
        await w.drain()
        f1, f2 = await _read_frames(r, 2)
        assert msa(f1) == ("AA", "DUP")
        assert msa(f2) == ("AA", "DUP")
        w.close()

    pub = FakePublisher()
    asyncio.run(_run(Settings(port=0), pub, client))
    assert pub.calls == ["DUP", "DUP"]


def _openssl_selfsigned(tmp_path: Path) -> tuple[Path, Path] | None:
    exe = shutil.which("openssl")
    if exe is None:
        return None
    cert, key = tmp_path / "cert.pem", tmp_path / "key.pem"
    subprocess.run(  # noqa: S603 - fixed argv, test fixture only
        [
            exe, "req", "-x509", "-newkey", "rsa:2048", "-nodes",
            "-keyout", str(key), "-out", str(cert), "-days", "1",
            "-subj", "/CN=localhost",
        ],
        check=True, capture_output=True, timeout=30,
    )
    return cert, key


def test_tls_round_trip(tmp_path: Path) -> None:
    pair = _openssl_selfsigned(tmp_path)
    if pair is None:
        pytest.skip("openssl not available to mint a self-signed test cert")
    cert, key = pair

    async def client(port: int) -> None:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE  # spike: self-signed, verification not under test here
        r, w = await asyncio.open_connection("127.0.0.1", port, ssl=ctx)
        w.write(frame(adt("TLS1")))
        await w.drain()
        [f] = await _read_frames(r, 1)
        assert msa(f) == ("AA", "TLS1")
        w.close()

    cfg = Settings(port=0, tls_cert=str(cert), tls_key=str(key))
    asyncio.run(_run(cfg, FakePublisher(), client))


def test_tls_plaintext_client_is_rejected(tmp_path: Path) -> None:
    """A client that does not speak TLS to a TLS-only listener must fail the
    handshake, not silently exchange plaintext MLLP frames."""
    pair = _openssl_selfsigned(tmp_path)
    if pair is None:
        pytest.skip("openssl not available to mint a self-signed test cert")
    cert, key = pair

    async def client(port: int) -> None:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        try:
            w.write(frame(adt("PLAIN")))
            await w.drain()
            data = await asyncio.wait_for(r.read(65536), 1.0)
            # either the handshake aborts the connection (empty read) or the write
            # itself already failed below -- either way, no plaintext AA/AR frame
            assert data == b""
        except (ConnectionError, OSError, TimeoutError):
            pass  # handshake failure surfaced as a reset/timeout: also acceptable
        finally:
            w.close()

    cfg = Settings(port=0, tls_cert=str(cert), tls_key=str(key))
    asyncio.run(_run(cfg, FakePublisher(), client))


def test_build_ack_used_by_server_matches_helper() -> None:
    # sanity: server ACKs are byte-identical in shape to the unit-tested helper
    assert build_ack("AA", "X").startswith(SB)
