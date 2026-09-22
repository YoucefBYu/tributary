"""`stdlib_send` against a real loopback HTTP server (`http.server`, standard library — no new
dependency for testing the thing that exists specifically to avoid a new dependency)."""

from __future__ import annotations

import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from tributary.loaders.http.client import Request, TransportError, stdlib_send


class Handler(BaseHTTPRequestHandler):
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
