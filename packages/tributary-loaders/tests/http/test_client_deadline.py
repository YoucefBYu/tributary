"""`stdlib_send`'s overall deadline over a real socket: a peer that trickles the body, the headers or the
status line just under the per-socket timeout must still be cut at the deadline (security review, Low).
Also the typed, text-free errors for a malformed status line and a header value with a line break (security
review, Medium), and the `Request` repr never showing `Authorization`."""

from __future__ import annotations

import socket
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager

import pytest
from tributary.loaders.http.client import (
    InvalidRequestError,
    Request,
    TransportError,
    TransportTimeout,
    stdlib_send,
)

Script = Callable[[socket.socket], None]


@contextmanager
def raw_server(script: Script) -> Iterator[str]:
    """One-connection-at-a-time TCP server running `script(conn)` after reading the request head."""
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(4)
    listener.settimeout(0.2)
    stop = threading.Event()

    def serve() -> None:
        while not stop.is_set():
            try:
                conn, _ = listener.accept()
            except (TimeoutError, OSError):
                continue
            with conn:
                conn.settimeout(5)
                try:
                    data = b""
                    while b"\r\n\r\n" not in data:
                        data += conn.recv(4096)
                    script(conn)
                except OSError:
                    pass

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{listener.getsockname()[1]}/"
    finally:
        stop.set()
        thread.join(3)
        listener.close()


def trickle(conn: socket.socket, prefix: bytes, filler: bytes, every_s: float = 0.1, times: int = 60) -> None:
    conn.sendall(prefix)
    for _ in range(times):
        conn.sendall(filler)
        time.sleep(every_s)


def send(url: str, timeout_s: float = 0.6) -> object:
    return stdlib_send(Request("POST", url, headers={}, body=b"x", timeout_s=timeout_s))


def test_a_trickled_body_is_cut_at_the_deadline_over_a_real_socket() -> None:
    def script(conn: socket.socket) -> None:
        trickle(conn, b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n", b"z")

    with raw_server(script) as url:
        started = time.monotonic()
        resp = stdlib_send(Request("POST", url, headers={}, body=b"x", timeout_s=0.6))
        assert time.monotonic() - started < 2.5
    assert resp.truncated is True
    assert len(resp.body) < 100


def test_trickled_headers_are_cut_at_the_deadline() -> None:
    def script(conn: socket.socket) -> None:
        conn.sendall(b"HTTP/1.1 200 OK\r\n")
        for i in range(200):  # one header line every 100 ms, forever below the 600 ms socket timeout
            conn.sendall(f"X-H{i}: v\r\n".encode())
            time.sleep(0.1)

    with raw_server(script) as url:
        started = time.monotonic()
        with pytest.raises(TransportTimeout):
            send(url)
        assert time.monotonic() - started < 2.5


def test_a_trickled_status_line_is_cut_at_the_deadline() -> None:
    def script(conn: socket.socket) -> None:
        for byte in b"HTTP/1.1 200 OK" + b" " * 100:
            conn.sendall(bytes([byte]))
            time.sleep(0.1)

    with raw_server(script) as url:
        started = time.monotonic()
        with pytest.raises(TransportTimeout):
            send(url)
        assert time.monotonic() - started < 2.5


def test_a_malformed_status_line_is_a_typed_error_that_does_not_echo_it() -> None:
    def script(conn: socket.socket) -> None:
        conn.sendall(b"PEER-CONTROLLED-TEXT-7f3a not http\r\n\r\n")

    with raw_server(script) as url, pytest.raises(TransportError) as exc_info:
        send(url)
    assert "PEER-CONTROLLED" not in str(exc_info.value)
    assert "PEER-CONTROLLED" not in repr(exc_info.value)
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__suppress_context__ is True


def test_a_header_value_with_a_line_break_is_a_typed_error_that_does_not_echo_it() -> None:
    request = Request(
        "POST",
        "http://127.0.0.1:1/",
        headers={"Authorization": "Bearer s3cr3t-token\nX-Injected: 1"},
        body=b"",
        timeout_s=1,
    )
    with pytest.raises(InvalidRequestError) as exc_info:
        stdlib_send(request)
    assert "s3cr3t" not in str(exc_info.value)
    assert exc_info.value.__suppress_context__ is True
    assert isinstance(exc_info.value, TransportError)


def test_the_request_repr_never_shows_the_authorization_header() -> None:
    request = Request("POST", "http://h/", headers={"Authorization": "Bearer s3cr3t-token"}, body=b"", timeout_s=1)
    assert "s3cr3t" not in repr(request)
    assert "Authorization" not in repr(request)
