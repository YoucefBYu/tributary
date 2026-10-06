"""`build_client` against real local servers: no environment proxy, no environment endpoint override,
credentials resolved from a secretRef, the request as it goes on the wire, and TLS verification (a
self-signed certificate is `TLS_VERIFY_FAILED`, not a retry)."""

from __future__ import annotations

import http.server
import shutil
import ssl
import subprocess
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any, ClassVar

import pytest
from tributary.loaders.s3.client import build_client
from tributary.loaders.s3.loader import S3Loader

from .test_loader import make_config, make_message

CREDS_REF = "creds"  # a secretRef name
KEY_ID_MARKER = "SYNTHKEYID0001"
SIGN_MARKER = "synthetic-marker-value-0001"


class _Resolver:
    def resolve_text(self, ref: str, key: str) -> str:
        if key == "access_key_id":
            return KEY_ID_MARKER
        if key == "secret_access_key":
            return SIGN_MARKER
        raise FileNotFoundError(key)

    def resolve_file(self, ref: str, key: str) -> Path:  # pragma: no cover
        raise FileNotFoundError(key)


class _Handler(http.server.BaseHTTPRequestHandler):
    seen: ClassVar[list[dict[str, str]]] = []

    def log_message(self, *_args: object) -> None:
        pass

    def do_PUT(self) -> None:
        length = int(self.headers.get("Content-Length", 0))
        self.rfile.read(length)
        type(self).seen.append({k.lower(): v for k, v in self.headers.items()} | {"path": self.path})
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()


@pytest.fixture
def server() -> Iterator[tuple[str, list[dict[str, str]]]]:
    handler = type("H", (_Handler,), {"seen": []})
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    try:
        yield f"http://127.0.0.1:{httpd.server_address[1]}", handler.seen  # type: ignore[attr-defined]
    finally:
        httpd.shutdown()


def _config(endpoint: str, **kw: Any) -> Any:
    return make_config(endpoint=endpoint, auth_type="access-key", auth_secret_ref=CREDS_REF, timeout_s=5.0, **kw)


def test_the_request_is_signed_encrypted_conditional_and_path_style(
    server: tuple[str, list[dict[str, str]]],
) -> None:
    endpoint, seen = server
    config = _config(endpoint)
    outcome = S3Loader(config, build_client(config, _Resolver())).deliver(make_message())
    assert outcome.success, outcome
    (request,) = seen
    assert request["authorization"].startswith("AWS4-HMAC-SHA256 Credential=" + KEY_ID_MARKER)
    assert SIGN_MARKER not in "".join(request.values())
    assert request["x-amz-server-side-encryption"] == "AES256"
    assert request["if-none-match"] == "*"
    assert request["path"].startswith("/bkt/adt/2026/03/09/")  # path style: bucket in the path


def test_an_environment_proxy_and_endpoint_override_are_ignored(
    server: tuple[str, list[dict[str, str]]], monkeypatch: pytest.MonkeyPatch
) -> None:
    endpoint, seen = server
    for name in ("HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy", "ALL_PROXY"):
        monkeypatch.setenv(name, "http://127.0.0.1:1")  # a dead proxy: any use of it fails the call
    monkeypatch.delenv("NO_PROXY", raising=False)
    monkeypatch.delenv("no_proxy", raising=False)
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "http://127.0.0.1:2")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://127.0.0.1:2")
    config = _config(endpoint)
    outcome = S3Loader(config, build_client(config, _Resolver())).deliver(make_message())
    assert outcome.success, outcome
    assert len(seen) == 1  # it went straight to the configured endpoint


def test_a_configured_endpoint_url_cannot_redirect_the_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    """With no `endpoint` in the spec the client must talk to AWS for the region, whatever
    `AWS_ENDPOINT_URL[_S3]` says (no request is made: the resolved endpoint is what is checked)."""
    monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "http://127.0.0.1:2")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://127.0.0.1:2")
    client = build_client(make_config(auth_type="access-key", auth_secret_ref=CREDS_REF), _Resolver())
    assert client.meta.endpoint_url == "https://s3.eu-west-1.amazonaws.com"


def test_timeouts_come_from_the_destination_and_botocore_does_not_retry() -> None:
    config = _config("https://s3.example.org")
    client = build_client(config, _Resolver())
    assert client.meta.config.retries["total_max_attempts"] == 1
    assert client.meta.config.connect_timeout == config.timeout_s
    assert client.meta.config.read_timeout == config.timeout_s


def test_access_key_needs_its_secret_resolved_at_build_time() -> None:
    class Empty(_Resolver):
        def resolve_text(self, ref: str, key: str) -> str:
            raise FileNotFoundError(key)

    with pytest.raises(FileNotFoundError):
        build_client(_config("https://s3.example.org"), Empty())


@pytest.mark.skipif(shutil.which("openssl") is None, reason="openssl not on PATH")
def test_a_self_signed_endpoint_is_a_non_retryable_tls_failure(tmp_path: Path) -> None:
    subprocess.run(  # noqa: S603 - fixed argv, test fixture only
        [
            shutil.which("openssl") or "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
            "-keyout", str(tmp_path / "k.pem"), "-out", str(tmp_path / "c.pem"),
            "-subj", "/CN=localhost", "-addext", "subjectAltName=IP:127.0.0.1",
        ],
        check=True,
        capture_output=True,
    )  # fmt: skip
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(tmp_path / "c.pem", tmp_path / "k.pem")
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    httpd.socket = context.wrap_socket(httpd.socket, server_side=True)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    try:
        config = _config(f"https://127.0.0.1:{httpd.server_address[1]}")
        outcome = S3Loader(config, build_client(config, _Resolver())).deliver(make_message())
    finally:
        httpd.shutdown()
    assert (outcome.success, outcome.reason, outcome.retryable) == (False, "TLS_VERIFY_FAILED", False)
