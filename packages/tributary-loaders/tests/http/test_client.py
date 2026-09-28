"""`stdlib_send` against a real loopback HTTP server (`http.server`, standard library — no new
dependency for testing the thing that exists specifically to avoid a new dependency)."""

from __future__ import annotations

import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import ClassVar

import pytest
from tributary.loaders.http.client import Request, TransportError, stdlib_send


class Handler(BaseHTTPRequestHandler):
    #: (method, headers) for every request `/redirect-target` ever received, class-level because
    #: `ThreadingHTTPServer` builds a fresh `Handler` per request; the `server` fixture clears it.
    redirect_target_hits: ClassVar[list[tuple[str, dict[str, str]]]] = []

    def log_message(self, *_args: object) -> None:  # silence test output
        pass

    def _respond(self) -> None:
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length else b""
        if self.path == "/echo-headers":
            self.send_response(200)
            self.send_header("X-Received-Idempotency-Key", self.headers.get("Idempotency-Key", ""))
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path == "/fail":
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b"server error")
            return
        if self.path == "/bad-request":
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"bad request")
            return
        if self.path == "/redirect":
            self.send_response(302)
            self.send_header("Location", "/redirect-target")
            self.end_headers()
            return
        if self.path == "/redirect-target":
            # if this is ever hit, stdlib_send followed the redirect it must refuse to follow
            type(self).redirect_target_hits.append((self.command, dict(self.headers)))
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"followed")
            return
        if self.path == "/slow":
            import time

            time.sleep(2)
            self.send_response(200)
            self.end_headers()
            return
        self.send_response(200)
        self.end_headers()
        self.wfile.write(body)

    do_GET = _respond
    do_POST = _respond
    do_PUT = _respond
    do_PATCH = _respond


@pytest.fixture
def server() -> Iterator[str]:
    Handler.redirect_target_hits = []
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    port = httpd.server_address[1]
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        httpd.shutdown()
        thread.join(2)


def test_post_round_trips_body_and_status(server: str) -> None:
    resp = stdlib_send(Request("POST", f"{server}/echo", headers={}, body=b"payload", timeout_s=2))
    assert resp.status_code == 200
    assert resp.body == b"payload"


def test_idempotency_header_reaches_the_server(server: str) -> None:
    resp = stdlib_send(
        Request("POST", f"{server}/echo-headers", headers={"Idempotency-Key": "abc-123"}, body=b"", timeout_s=2)
    )
    assert resp.headers["X-Received-Idempotency-Key"] == "abc-123"


def test_5xx_is_a_response_not_an_exception(server: str) -> None:
    resp = stdlib_send(Request("POST", f"{server}/fail", headers={}, body=b"", timeout_s=2))
    assert resp.status_code == 500


def test_4xx_is_a_response_not_an_exception(server: str) -> None:
    resp = stdlib_send(Request("POST", f"{server}/bad-request", headers={}, body=b"", timeout_s=2))
    assert resp.status_code == 400


def test_timeout_raises_transport_error(server: str) -> None:
    with pytest.raises(TransportError):
        stdlib_send(Request("POST", f"{server}/slow", headers={}, body=b"", timeout_s=0.2))


def test_connection_refused_raises_transport_error() -> None:
    with pytest.raises(TransportError):
        stdlib_send(Request("POST", "http://127.0.0.1:1", headers={}, body=b"", timeout_s=1))


def test_ignores_an_environment_proxy() -> None:
    """Code review, High (found on the clickhouse loader's identical opener pattern): `build_opener`
    otherwise installs a `ProxyHandler` that reads `HTTP_PROXY`/`HTTPS_PROXY`, which would send this
    loader's credentialed, PHI-carrying requests through an operator's outbound-internet proxy instead
    of straight to the destination. `ProxyHandler({})` (an explicit empty mapping, not the `None` that
    would fall back to reading the environment) registers no proxy handler at all regardless of what the
    environment holds -- confirmed against `urllib.request.OpenerDirector.add_handler`, which only wires
    up a handler's `<scheme>_open` methods when its `proxies` mapping is non-empty -- so this checks the
    already-built singleton structurally rather than reloading the module under a mutated environment
    (which would mutate `sys.modules` for the rest of the test session; `TlsVerificationError`, defined
    in the same module and imported elsewhere by identity, is exactly the kind of thing a reload would
    silently break -- see `tests/http/test_tls_failures.py`'s docstring)."""
    from tributary.loaders.http.client import _OPENER

    assert not any(getattr(h, "proxies", None) for h in _OPENER.handlers)


@pytest.mark.parametrize("method", ["POST", "PUT", "PATCH"])
def test_redirect_is_refused_not_followed(server: str, method: str) -> None:
    """Security review, gate finding, High: a 3xx must never turn into a second request — not a
    bodyless GET at the target (invariant 4: the record would be committed as delivered without its
    payload ever arriving anywhere), and not a second request carrying `Authorization` to wherever
    `Location` points. Checked for every method this loader can use
    (`contracts/channel-spec/v0` httpParams.method)."""
    resp = stdlib_send(
        Request(
            method,
            f"{server}/redirect",
            headers={"Authorization": "Bearer secret-token"},
            body=b"payload",
            timeout_s=2,
        )
    )
    assert resp.status_code == 302
    assert Handler.redirect_target_hits == []  # the redirect target was never contacted at all
