"""TLS failure classification against a **real** local TLS listener (security review, Low, found on
the clickhouse loader's identical pattern): `urlopen` wraps a handshake failure in
`URLError(reason=ssl.SSLError)` rather than letting the `ssl.SSLError` escape directly, so a fake `send`
that raises `ssl.SSLError` (or `TlsVerificationError`) itself proves nothing about whether `client.py`'s
real exception-handling order ever reaches that classification.

`stdlib_send` takes an injectable `opener=` (added for exactly this test) rather than reloading
`http/client.py`'s module-level `_OPENER`/`_SSL_CONTEXT` singletons: an `importlib.reload` mutates
`sys.modules` for the rest of the test session, and `http/loader.py` imported `TlsVerificationError`
from this module at ITS OWN import time — a reload creates a new class object that the exception
`loader.py` already bound no longer matches, silently breaking `except TlsVerificationError:` for every
later test in the same process. Building a second opener here, scoped to this test only, has none of
that blast radius.

Skipped if `openssl` is not on `PATH`.
"""

from __future__ import annotations

import http.server
import shutil
import ssl
import subprocess
import threading
import urllib.request
from collections.abc import Iterator
from pathlib import Path

import pytest
from tributary.loaders.http.client import Request, TlsVerificationError, TransportError, stdlib_send
from tributary.loaders.http.client import _NoRedirectHandler as NoRedirectHandler

pytestmark = pytest.mark.skipif(shutil.which("openssl") is None, reason="openssl not on PATH")


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


class _OkHandler(http.server.BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        self.send_response(200)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *args: object) -> None:  # quiet
        pass


@pytest.fixture
def tls_server(tmp_path: Path) -> Iterator[tuple[int, Path]]:
    cert_dir = tmp_path / "server-cert"
    _selfsigned(cert_dir, "localhost")
    server = http.server.HTTPServer(("127.0.0.1", 0), _OkHandler)
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_context.load_cert_chain(certfile=str(cert_dir / "tls.crt"), keyfile=str(cert_dir / "tls.key"))
    server.socket = ssl_context.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_port, cert_dir / "tls.crt"
    finally:
        server.shutdown()
        thread.join(2)


def _opener_trusting(ca_file: Path) -> urllib.request.OpenerDirector:
    ctx = ssl.create_default_context(cafile=str(ca_file))
    return urllib.request.build_opener(
        NoRedirectHandler, urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=ctx)
    )


def test_hostname_mismatch_is_a_tls_verification_error(tls_server: tuple[int, Path]) -> None:
    port, server_cert = tls_server
    # Trusts the server's own certificate (the CA chain is fine) but connects via the loopback IP,
    # which is not the "localhost" name the certificate was issued for.
    opener = _opener_trusting(server_cert)
    with pytest.raises(TlsVerificationError):
        stdlib_send(Request("POST", f"https://127.0.0.1:{port}/", headers={}, body=b"", timeout_s=3), opener=opener)


def test_unknown_ca_is_a_tls_verification_error(tmp_path: Path, tls_server: tuple[int, Path]) -> None:
    port, _server_cert = tls_server
    other_ca_dir = tmp_path / "unrelated-ca"
    _selfsigned(other_ca_dir, "unrelated")
    opener = _opener_trusting(other_ca_dir / "tls.crt")
    with pytest.raises(TlsVerificationError):
        stdlib_send(Request("POST", f"https://127.0.0.1:{port}/", headers={}, body=b"", timeout_s=3), opener=opener)


def test_other_ssl_errors_are_not_reclassified_as_tls_verification_error() -> None:
    """The other half of the split (security review): a mid-handshake reset or `SSLEOFError` is
    transient and must stay `TransportError`/retryable, not become `TlsVerificationError` just because
    it is some `ssl.SSLError`."""
    import urllib.error

    class RaisingOpener:
        def open(self, request: object, timeout: float) -> object:
            raise urllib.error.URLError(ssl.SSLEOFError("EOF occurred in violation of protocol"))

    with pytest.raises(TransportError):
        stdlib_send(
            Request("POST", "https://127.0.0.1:1/", headers={}, body=b"", timeout_s=1),
            opener=RaisingOpener(),  # type: ignore[arg-type]
        )
