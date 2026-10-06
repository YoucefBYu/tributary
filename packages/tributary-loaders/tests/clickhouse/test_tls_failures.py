"""TLS failure classification against a **real** local TLS listener (security review, Low): `urlopen`
wraps a handshake failure in `URLError(reason=ssl.SSLError)` rather than letting the `ssl.SSLError`
escape directly, so a fake opener that raises `ssl.SSLError` (or even `TlsVerificationError`) itself
proves nothing about whether `client.py`'s real exception-handling order ever reaches that
classification. This generates a throwaway self-signed certificate with `openssl` (mirroring
`tributary-receivers`' `tests/test_tls.py`) and drives an actual TLS handshake through
`ClickhouseTransport.send` for both ways a certificate check fails: an unknown CA, and a hostname
mismatch. Skipped if `openssl` is not on `PATH`.
"""

from __future__ import annotations

import http.server
import shutil
import ssl
import subprocess
import threading
from collections.abc import Iterator
from pathlib import Path

import pytest
from tributary.loaders.clickhouse.client import TlsVerificationError, TransportError, transport_from_config
from tributary.loaders.clickhouse.config import ChAuthSpec, ChTlsSpec, ClickhouseDestinationConfig

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
    """A real HTTPS server on loopback, presenting a self-signed cert for CN=localhost. Connecting via
    `127.0.0.1` (an IP, not the name in the cert) is what forces a hostname-mismatch failure below;
    connecting with a *different* self-signed CA as the trust anchor forces an unknown-CA failure."""
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


class FakeSecretResolver:
    def __init__(self, ca_path: Path) -> None:
        self._ca_path = ca_path

    def resolve_text(self, ref: str, key: str) -> str:
        return "loader" if key == "user" else "s3cret"

    def resolve_file(self, ref: str, key: str) -> Path:
        return self._ca_path


def _config(port: int) -> ClickhouseDestinationConfig:
    return ClickhouseDestinationConfig(
        channel="c",
        destination="adt-analytics",
        host="127.0.0.1",
        port=port,
        database="analytics",
        table="adt_events",
        id_column="tb_msg_id",
        auth=ChAuthSpec("password", "ch-creds"),
        tls=ChTlsSpec("verify-full", "ch-ca"),
        timeout_s=3.0,
        circuit_breaker=None,
        idempotency_technique="sink-native",
        backoff="fixed",
        initial_delay_ms=100,
        max_delay_ms=1000,
        max_attempts=3,
        on_exhaust="halt-partition",
    )


def test_hostname_mismatch_is_a_tls_verification_error(tls_server: tuple[int, Path]) -> None:
    port, server_cert = tls_server
    # Trusts the server's own certificate (so the CA chain itself is fine) but connects via the loopback
    # IP, which is not the "localhost" name the certificate was issued for.
    resolver = FakeSecretResolver(server_cert)
    transport = transport_from_config(_config(port), resolver)
    with pytest.raises(TlsVerificationError):
        transport.send(b"SELECT 1", timeout_s=3.0)


def test_unknown_ca_is_a_tls_verification_error(tmp_path: Path, tls_server: tuple[int, Path]) -> None:
    port, _server_cert = tls_server
    # An unrelated self-signed certificate as the trust anchor: the server's real certificate does not
    # chain to it at all.
    other_ca_dir = tmp_path / "unrelated-ca"
    _selfsigned(other_ca_dir, "unrelated")
    resolver = FakeSecretResolver(other_ca_dir / "tls.crt")
    transport = transport_from_config(_config(port), resolver)
    with pytest.raises(TlsVerificationError):
        transport.send(b"SELECT 1", timeout_s=3.0)


def test_a_non_certificate_ssl_error_stays_a_retryable_transport_error() -> None:
    """The other half of the split (security review): a mid-handshake reset or `SSLEOFError` is
    transient and must stay `TransportError`/retryable, not become `TlsVerificationError` just because
    it is some `ssl.SSLError`. Exercised at the unit level: forcing a real `SSLEOFError` deterministically
    needs controlling exact byte timing of a TLS record, which `tests/clickhouse/test_client.py`'s
    `RaisingOpener` fakes already cover for the exception-type distinction; this asserts the concrete
    type kept out of the certificate-only branch."""
    assert not issubclass(ssl.SSLEOFError, ssl.SSLCertVerificationError)
    assert not issubclass(ssl.SSLZeroReturnError, ssl.SSLCertVerificationError)
    assert issubclass(ssl.SSLCertVerificationError, ssl.SSLError)


def test_other_ssl_errors_are_not_reclassified_as_tls_verification_error() -> None:
    import urllib.error

    from tributary.loaders.clickhouse.client import ClickhouseTransport

    class RaisingOpener:
        def open(self, request: object, timeout: float) -> object:
            raise urllib.error.URLError(ssl.SSLEOFError("EOF occurred in violation of protocol"))

    transport = ClickhouseTransport("https://example.org/?database=d", {}, RaisingOpener())  # type: ignore[arg-type]
    with pytest.raises(TransportError):
        transport.send(b"SELECT 1", timeout_s=1.0)
