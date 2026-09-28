"""`MllpConnection`: one persistent socket per destination, the shared connect+send+ACK timeout budget,
and the TLS/plaintext split. Driven against a **real** local TCP/TLS listener (a small, hand-rolled fake
MLLP server, thread-per-connection) rather than a fake socket object, so a partial frame, a slow
sender, and a peer that resets or hangs up are exercised the same way a real MLLP receiver would
produce them.
"""

from __future__ import annotations

import contextlib
import shutil
import socket
import ssl
import subprocess
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from tributary.loaders.mllp.client import (
    MAX_ACK_SIZE,
    MllpConnection,
    NoAckError,
    TlsVerificationError,
    TransportError,
    connection_from_config,
)
from tributary.loaders.mllp.config import CircuitBreakerSpec, MllpDestinationConfig, MllpTlsSpec
from tributary.loaders.mllp.framing import SB, control_id, frame

SB_ = SB
EB_CR = b"\x1c\x0d"


class FakeMllpServer:
    """One TCP listener, one connection at a time, running `handle_connection` in a thread. `respond`
    decides what bytes go back for each frame this server reads; `responses` is a queue of one
    callable per expected message so a test can script a different reply (or none) per attempt."""

    def __init__(self, respond: Callable[[bytes], bytes | None]) -> None:
        self._respond = respond
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(1)
        self.port = self._sock.getsockname()[1]
        self._stop = threading.Event()
        self.connections = 0
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        self._sock.settimeout(0.2)
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except TimeoutError:
                continue
            self.connections += 1
            threading.Thread(target=self._handle_one_connection, args=(conn,), daemon=True).start()

    def _handle_one_connection(self, conn: socket.socket) -> None:
        conn.settimeout(5)
        buf = bytearray()
        try:
            while not self._stop.is_set():
                chunk = conn.recv(4096)
                if not chunk:
                    return
                buf += chunk
                start = buf.find(SB_)
                if start == -1:
                    continue
                end = buf.find(EB_CR, start + 1)
                if end == -1:
                    continue
                message = bytes(buf[start + 1 : end])
                buf = buf[end + len(EB_CR) :]
                reply = self._respond(message)
                if reply is not None:
                    conn.sendall(reply)
        except OSError:
            return
        finally:
            conn.close()

    def close(self) -> None:
        self._stop.set()
        self._thread.join(2)
        self._sock.close()


@pytest.fixture
def ack_server() -> Iterator[FakeMllpServer]:
    def respond(message: bytes) -> bytes:
        ctrl = control_id(message)
        return frame(f"MSH|^~\\&\rMSA|AA|{ctrl}".encode())

    server = FakeMllpServer(respond)
    try:
        yield server
    finally:
        server.close()


MSH = b"MSH|^~\\&|TRIBUTARY-SYNTH|FAC|RECV|FAC2|20260101000000||ADT^A01|CTRL-1|P|2.5"


def test_sends_a_framed_message_and_reads_the_ack(ack_server: FakeMllpServer) -> None:
    conn = MllpConnection("127.0.0.1", ack_server.port, ssl_context=None)
    ack = conn.send_and_receive_ack(MSH, timeout_s=2.0)
    assert b"MSA|AA|CTRL-1" in ack
    conn.close()


def test_reuses_one_connection_across_multiple_sends(ack_server: FakeMllpServer) -> None:
    conn = MllpConnection("127.0.0.1", ack_server.port, ssl_context=None)
    conn.send_and_receive_ack(MSH, timeout_s=2.0)
    conn.send_and_receive_ack(MSH, timeout_s=2.0)
    conn.send_and_receive_ack(MSH, timeout_s=2.0)
    assert ack_server.connections == 1
    conn.close()


def test_a_stale_byte_arriving_after_the_last_ack_forces_a_reconnect() -> None:
    """Security review, Medium: `_is_stale` must catch a byte the peer pushes onto the connection
    AFTER this loader stopped reading (it stops as soon as one complete ACK frame arrives) -- a real
    peer here answers normally, then writes one extra, unsolicited byte a moment later, standing in for
    a stale/duplicate reply or a chatty peer. The next `send_and_receive_ack` call must discard that
    connection and open a fresh one rather than try to read the stray byte as part of the next ACK."""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(2)
    port = listener.getsockname()[1]
    accepted = 0
    lock = threading.Lock()

    def handle(conn: socket.socket) -> None:
        nonlocal accepted
        with lock:
            accepted += 1
        conn.settimeout(5)
        buf = bytearray()
        while SB_ not in buf or EB_CR not in buf:
            buf += conn.recv(4096)
        start, end = buf.index(SB_), buf.index(EB_CR)
        ctrl = control_id(bytes(buf[start + 1 : end]))
        conn.sendall(frame(f"MSH|^~\\&\rMSA|AA|{ctrl}".encode()))
        time.sleep(0.1)
        with contextlib.suppress(OSError):
            conn.sendall(b"\x00")  # unsolicited: arrives after the client already stopped reading

    def server_thread() -> None:
        for _ in range(2):
            conn, _ = listener.accept()
            threading.Thread(target=handle, args=(conn,), daemon=True).start()

    thread = threading.Thread(target=server_thread, daemon=True)
    thread.start()
    try:
        conn = MllpConnection("127.0.0.1", port, ssl_context=None)
        ack1 = conn.send_and_receive_ack(MSH, timeout_s=2.0)
        assert b"MSA|AA" in ack1
        time.sleep(0.3)  # let the stray byte actually arrive before this loader reuses the connection
        ack2 = conn.send_and_receive_ack(MSH, timeout_s=2.0)
        assert b"MSA|AA" in ack2
        assert accepted == 2  # the stale connection was discarded; a fresh one was accepted
        conn.close()
    finally:
        thread.join(2)
        listener.close()


def test_connection_refused_is_a_transport_error() -> None:
    conn = MllpConnection("127.0.0.1", 1, ssl_context=None)
    with pytest.raises(TransportError):
        conn.send_and_receive_ack(MSH, timeout_s=1.0)


def test_partial_frame_then_completed_later() -> None:
    """A peer that writes the ACK in two slow pieces still produces one complete frame -- exercises
    `FrameParser.feed`'s accumulation against real, arbitrarily-chunked `recv()` calls."""

    def respond(message: bytes) -> None:
        return None  # handled manually below via a raw socket instead

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]

    def server_thread() -> None:
        conn, _ = listener.accept()
        conn.settimeout(5)
        buf = bytearray()
        while SB_ not in buf or EB_CR not in buf:
            buf += conn.recv(4096)
        reply = frame(b"MSH|^~\\&\rMSA|AA|CTRL-1")
        conn.sendall(reply[:5])
        time.sleep(0.05)
        conn.sendall(reply[5:])
        conn.close()

    thread = threading.Thread(target=server_thread, daemon=True)
    thread.start()
    try:
        conn = MllpConnection("127.0.0.1", port, ssl_context=None)
        ack = conn.send_and_receive_ack(MSH, timeout_s=2.0)
        assert b"MSA|AA|CTRL-1" in ack
        conn.close()
    finally:
        thread.join(2)
        listener.close()


def test_oversized_ack_is_a_no_ack_error() -> None:
    def respond(message: bytes) -> bytes:
        # No terminator ever arrives once the buffer exceeds MAX_ACK_SIZE.
        return SB_ + b"x" * (MAX_ACK_SIZE + 10)

    server = FakeMllpServer(respond)
    try:
        conn = MllpConnection("127.0.0.1", server.port, ssl_context=None)
        with pytest.raises(NoAckError):
            conn.send_and_receive_ack(MSH, timeout_s=2.0)
    finally:
        server.close()


def test_peer_closes_before_a_complete_ack_is_a_no_ack_error() -> None:
    def respond(message: bytes) -> bytes:
        return SB_ + b"MSA|AA|CTRL-1"  # never terminated; the server then returns None and closes

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]

    def server_thread() -> None:
        conn, _ = listener.accept()
        conn.settimeout(5)
        buf = bytearray()
        while SB_ not in buf or EB_CR not in buf:
            buf += conn.recv(4096)
        conn.sendall(SB_ + b"MSA|AA|CTRL-1")  # unterminated
        conn.close()  # half-open from the client's perspective: reply arrived, then EOF

    thread = threading.Thread(target=server_thread, daemon=True)
    thread.start()
    try:
        conn = MllpConnection("127.0.0.1", port, ssl_context=None)
        with pytest.raises(NoAckError):
            conn.send_and_receive_ack(MSH, timeout_s=2.0)
    finally:
        thread.join(2)
        listener.close()


def test_a_connection_that_failed_mid_protocol_is_closed_so_the_next_attempt_reconnects() -> None:
    def respond(message: bytes) -> bytes:
        return b""  # never actually sent (see below); server closes instead

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(2)
    port = listener.getsockname()[1]
    accepted = []

    def server_thread() -> None:
        for _ in range(2):
            conn, _ = listener.accept()
            accepted.append(conn)
            conn.settimeout(5)
            buf = bytearray()
            while SB_ not in buf or EB_CR not in buf:
                buf += conn.recv(4096)
            conn.close()  # reset before any ACK: forces NoAckError, closing the client side

    thread = threading.Thread(target=server_thread, daemon=True)
    thread.start()
    try:
        conn = MllpConnection("127.0.0.1", port, ssl_context=None)
        with pytest.raises(NoAckError):
            conn.send_and_receive_ack(MSH, timeout_s=2.0)
        assert not conn.is_connected()
        # The next attempt opens a fresh connection rather than reusing the dead one.
        with pytest.raises(NoAckError):
            conn.send_and_receive_ack(MSH, timeout_s=2.0)
    finally:
        thread.join(2)
        listener.close()
        for c in accepted:
            c.close()


def test_timeout_waiting_for_an_ack_closes_the_connection() -> None:
    def respond(message: bytes) -> None:
        time.sleep(2)
        return None

    server = FakeMllpServer(respond)
    try:
        conn = MllpConnection("127.0.0.1", server.port, ssl_context=None)
        with pytest.raises(TimeoutError):
            conn.send_and_receive_ack(MSH, timeout_s=0.2)
        assert not conn.is_connected()
    finally:
        server.close()


def _selfsigned(out_dir: Path, cn: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(  # noqa: S603 - fixed argv, test fixture only
        [
            shutil.which("openssl"),  # type: ignore[list-item]
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-keyout",
            str(out_dir / "tls.key"),
            "-out",
            str(out_dir / "tls.crt"),
            "-subj",
            f"/CN={cn}",
        ],
        check=True,
        capture_output=True,
    )


@pytest.mark.skipif(shutil.which("openssl") is None, reason="openssl not on PATH")
def test_tls_hostname_mismatch_is_a_tls_verification_error(tmp_path: Path) -> None:
    """Real handshake, not a fake context: `client.py` wraps a `socket.create_connection` in
    `ssl_context.wrap_socket`, which raises `ssl.SSLError` directly (unlike `urlopen`'s
    `URLError`-wrapping) -- proved against an actual mismatch here rather than assumed."""
    cert_dir = tmp_path / "server-cert"
    _selfsigned(cert_dir, "localhost")

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]
    server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_ctx.load_cert_chain(certfile=str(cert_dir / "tls.crt"), keyfile=str(cert_dir / "tls.key"))

    def server_thread() -> None:
        raw, _ = listener.accept()
        with contextlib.suppress(ssl.SSLError):
            server_ctx.wrap_socket(raw, server_side=True)

    thread = threading.Thread(target=server_thread, daemon=True)
    thread.start()
    try:
        client_ctx = ssl.create_default_context(cafile=str(cert_dir / "tls.crt"))
        conn = MllpConnection("127.0.0.1", port, ssl_context=client_ctx)
        with pytest.raises(TlsVerificationError):
            conn.send_and_receive_ack(MSH, timeout_s=3.0)
    finally:
        thread.join(2)
        listener.close()


def test_a_non_certificate_ssl_error_during_handshake_stays_retryable() -> None:
    """Security review, Low, the same rule `7ed8343` applied to the HTTP and ClickHouse loaders: only
    `ssl.SSLCertVerificationError` is `TlsVerificationError`/non-retryable. A peer that accepts the TCP
    connection and then closes it before completing any TLS handshake at all raises a non-certificate
    `ssl.SSLError` (`SSLEOFError`, in practice) -- transient, and must stay `TransportError`/retryable."""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]

    def server_thread() -> None:
        raw, _ = listener.accept()
        raw.close()  # no TLS handshake at all: the client's wrap_socket sees a bare connection reset

    thread = threading.Thread(target=server_thread, daemon=True)
    thread.start()
    try:
        client_ctx = ssl.create_default_context()
        conn = MllpConnection("127.0.0.1", port, ssl_context=client_ctx)
        with pytest.raises(TransportError):
            conn.send_and_receive_ack(MSH, timeout_s=3.0)
    finally:
        thread.join(2)
        listener.close()


def test_a_handshake_that_never_completes_is_a_timeout_error() -> None:
    """Security review, Low: a stalled TLS handshake shares `MLLP_TIMEOUT`'s budget the same way a
    stalled connect or a stalled ACK wait does (`client.py`'s "one shared deadline" design), not
    `CONNECT_FAILED`."""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]

    def server_thread() -> None:
        raw, _ = listener.accept()
        time.sleep(2)  # accepts the TCP connection but never sends a single TLS record
        with contextlib.suppress(OSError):
            raw.close()

    thread = threading.Thread(target=server_thread, daemon=True)
    thread.start()
    try:
        client_ctx = ssl.create_default_context()
        conn = MllpConnection("127.0.0.1", port, ssl_context=client_ctx)
        with pytest.raises(TimeoutError):
            conn.send_and_receive_ack(MSH, timeout_s=0.3)
    finally:
        thread.join(3)
        listener.close()


def _config(
    port: int, *, tls: MllpTlsSpec | None = None, breaker: CircuitBreakerSpec | None = None
) -> MllpDestinationConfig:
    return MllpDestinationConfig(
        channel="c",
        destination="downstream-lis",
        host="127.0.0.1",
        port=port,
        tls=tls,
        timeout_s=2.0,
        on_negative_ack="retry",
        circuit_breaker=breaker,
        idempotency_technique="none",
        backoff="fixed",
        initial_delay_ms=100,
        max_delay_ms=1000,
        max_attempts=3,
        on_exhaust="halt-partition",
    )


class FakeSecretResolver:
    def __init__(self, ca_path: Path) -> None:
        self._ca_path = ca_path

    def resolve_text(self, ref: str, key: str) -> str:
        raise FileNotFoundError

    def resolve_file(self, ref: str, key: str) -> Path:
        if key != "ca.crt":
            raise FileNotFoundError  # no client certificate under this ref (optional, config.py's docstring)
        return self._ca_path


def test_connection_from_config_with_no_tls() -> None:
    conn = connection_from_config(_config(2575), FakeSecretResolver(Path("/dev/null")))
    assert conn.ssl_context is None


@pytest.mark.skipif(shutil.which("openssl") is None, reason="openssl not on PATH")
def test_connection_from_config_builds_a_context_with_the_configured_min_version(tmp_path: Path) -> None:
    cert_dir = tmp_path / "ca"
    _selfsigned(cert_dir, "ca")
    resolver = FakeSecretResolver(cert_dir / "tls.crt")
    conn = connection_from_config(_config(2575, tls=MllpTlsSpec("some-ref", "1.3")), resolver)
    assert conn.ssl_context is not None
    assert conn.ssl_context.minimum_version == ssl.TLSVersion.TLSv1_3
