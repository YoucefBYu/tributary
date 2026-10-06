"""`stdlib_send`'s response cap, overall read deadline and PHI-free transport errors."""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from tributary.loaders.http.client import Request, TransportError, TransportTimeout, stdlib_send


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *_args: object) -> None:
        pass

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", 0))
        self.rfile.read(length)
        if self.path == "/big":
            self.send_response(200)
            self.send_header("Content-Length", "5000")
            self.end_headers()
            self.wfile.write(b"x" * 5000)
        elif self.path == "/exact":
            self.send_response(200)
            self.send_header("Content-Length", "100")
            self.end_headers()
            self.wfile.write(b"y" * 100)
        elif self.path == "/big-error":
            self.send_response(500)
            self.send_header("Content-Length", "5000")
            self.end_headers()
            self.wfile.write(b"e" * 5000)
        elif self.path == "/trickle":
            self.send_response(200)
            self.send_header("Content-Length", "100")
            self.end_headers()
            try:
                for _ in range(100):
                    self.wfile.write(b"z")
                    self.wfile.flush()
                    time.sleep(0.1)
            except OSError:
                pass


@pytest.fixture
def server() -> Iterator[str]:
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    try:
        yield f"http://127.0.0.1:{httpd.server_address[1]}"
    finally:
        httpd.shutdown()


def send(url: str, *, cap: int, timeout_s: float = 3.0):  # type: ignore[no-untyped-def]
    return stdlib_send(Request("POST", url, headers={}, body=b"", timeout_s=timeout_s, max_response_bytes=cap))


def test_a_body_over_the_cap_is_truncated_and_flagged(server: str) -> None:
    resp = send(f"{server}/big", cap=1000)
    assert len(resp.body) == 1000
    assert resp.truncated is True


def test_a_body_exactly_at_the_cap_is_not_flagged(server: str) -> None:
    resp = send(f"{server}/exact", cap=100)
    assert len(resp.body) == 100
    assert resp.truncated is False


def test_an_error_body_over_the_cap_is_truncated_too(server: str) -> None:
    resp = send(f"{server}/big-error", cap=64)
    assert resp.status_code == 500
    assert len(resp.body) == 64
    assert resp.truncated is True


def test_a_trickling_body_is_cut_at_the_overall_deadline(server: str) -> None:
    started = time.monotonic()
    resp = send(f"{server}/trickle", cap=1000, timeout_s=0.5)
    assert time.monotonic() - started < 3.0
    assert resp.truncated is True
    assert len(resp.body) < 100


def test_transport_errors_name_no_peer_or_url() -> None:
    with pytest.raises(TransportError) as exc_info:
        send("http://127.0.0.1:1/secret-path", cap=10, timeout_s=1)
    text = str(exc_info.value)
    assert "127.0.0.1" not in text
    assert "secret-path" not in text


def test_a_read_timeout_is_a_transport_timeout(server: str) -> None:
    class Slow(BaseHTTPRequestHandler):
        pass

    httpd = ThreadingHTTPServer(
        ("127.0.0.1", 0), type("H", (Slow,), {"do_POST": lambda self: time.sleep(2), "log_message": lambda *a: None})
    )
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    try:
        with pytest.raises(TransportTimeout):
            send(f"http://127.0.0.1:{httpd.server_address[1]}/", cap=10, timeout_s=0.3)
    finally:
        httpd.shutdown()
