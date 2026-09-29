"""The TLS floor of the shared HTTPS client (`fhir` and `http` use it; security review, Medium): TLS 1.2 or
later and AEAD suites with forward secrecy only, proven with real handshakes, not by reading the context.

Skipped if `openssl` is not on `PATH` (the throwaway certificate)."""

from __future__ import annotations

import shutil
import socket
import ssl
import subprocess
import threading
import urllib.request
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from tributary.loaders.deadline import DeadlineHTTPSHandler
from tributary.loaders.http.client import Request, TlsVerificationError, TransportError, stdlib_send
from tributary.loaders.tls import CIPHERS, default_client_context, harden

pytestmark = pytest.mark.skipif(shutil.which("openssl") is None, reason="openssl not on PATH")

_OK = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok"


def _selfsigned(out_dir: Path) -> tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(  # noqa: S603 - fixed argv, test fixture only
        [
            shutil.which("openssl"),  # type: ignore[list-item]
            "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
            "-keyout", str(out_dir / "tls.key"), "-out", str(out_dir / "tls.crt"),
            "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost",
        ],
        check=True,
        capture_output=True,
    )  # fmt: skip
    return out_dir / "tls.crt", out_dir / "tls.key"


def _serve(context: ssl.SSLContext) -> tuple[int, Callable[[], None]]:
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(4)
    listener.settimeout(0.2)
    stop = threading.Event()

    def loop() -> None:
        while not stop.is_set():
            try:
                raw, _ = listener.accept()
            except (TimeoutError, OSError):
                continue
            try:
                raw.settimeout(3)
                with context.wrap_socket(raw, server_side=True) as conn:
                    data = b""
                    while b"\r\n\r\n" not in data:
                        data += conn.recv(4096)
                    conn.sendall(_OK)
            except (OSError, ssl.SSLError):
                raw.close()

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()

    def close() -> None:
        stop.set()
        thread.join(3)
        listener.close()

    return int(listener.getsockname()[1]), close


def _server_context(tmp_path: Path, ciphers: str | None, *, version: ssl.TLSVersion | None = None) -> ssl.SSLContext:
    cert, key = _selfsigned(tmp_path / "srv")
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(str(cert), str(key))
    if ciphers is not None:
        ctx.set_ciphers(ciphers)
    if version is not None:
        ctx.minimum_version = ctx.maximum_version = version
    return ctx


@pytest.fixture
def serve(tmp_path: Path) -> Iterator[Callable[..., int]]:
    closers: list[Callable[[], None]] = []

    def start(ciphers: str | None, *, version: ssl.TLSVersion | None = None) -> int:
        port, close = _serve(_server_context(tmp_path, ciphers, version=version))
        closers.append(close)
        return port

    yield start
    for close in closers:
        close()


def _send(port: int, opener: urllib.request.OpenerDirector | None = None) -> int:
    request = Request("POST", f"https://localhost:{port}/", headers={}, body=b"", timeout_s=3)
    return stdlib_send(request, opener=opener) if opener is not None else stdlib_send(request)


def test_the_context_has_the_floor() -> None:
    ctx = default_client_context()
    assert ctx.minimum_version == ssl.TLSVersion.TLSv1_2
    assert ctx.verify_mode == ssl.CERT_REQUIRED and ctx.check_hostname is True
    tls12 = [c["name"] for c in ctx.get_ciphers() if c["protocol"] == "TLSv1.2"]
    assert tls12, "expected some TLS 1.2 suites"
    assert all("GCM" in n or "CHACHA20" in n for n in tls12) and all(n.startswith("ECDHE") for n in tls12)
    assert CIPHERS == "ECDHE+AESGCM:ECDHE+CHACHA20"


def test_a_cbc_only_server_is_refused_at_the_handshake(serve: Callable[..., int]) -> None:
    port = serve("AES128-SHA:@SECLEVEL=0", version=ssl.TLSVersion.TLSv1_2)
    with pytest.raises(TransportError) as exc_info:
        _send(port)  # the production opener: a cipher mismatch fails before certificate verification
    assert not isinstance(exc_info.value, TlsVerificationError)


def test_a_tls_1_1_server_is_refused_at_the_handshake(serve: Callable[..., int]) -> None:
    try:
        port = serve("DEFAULT:@SECLEVEL=0", version=ssl.TLSVersion.TLSv1_1)
    except (ssl.SSLError, ValueError):
        pytest.skip("this OpenSSL cannot serve TLS 1.1")
    with pytest.raises(TransportError) as exc_info:
        _send(port)
    assert not isinstance(exc_info.value, TlsVerificationError)


def test_an_aead_server_is_accepted_when_its_certificate_is_trusted(tmp_path: Path, serve: Callable[..., int]) -> None:
    port = serve("ECDHE+AESGCM")
    ctx = harden(ssl.create_default_context(cafile=str(tmp_path / "srv" / "tls.crt")))
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), DeadlineHTTPSHandler(ctx))
    assert _send(port, opener).status_code == 200  # type: ignore[attr-defined]
