"""The ClickHouse transport's deadline over a **real socket** and a real `HTTPResponse` (code review, High).

The unit tests around `_read_capped` use fakes that only implement `read`, so the `read1` branch the
production path takes (`HTTPResponse.read1`, which returns after one socket read instead of waiting for
`n` bytes) was never exercised: a `read`-only fake proves nothing about whether a trickling peer can hold a
call open. These tests run `ClickhouseTransport.send` against a TCP server that trickles bytes, mirroring
`tests/http/test_client_deadline.py`. Also the typed, text-free error for a malformed status line."""

from __future__ import annotations

import http.client
import socket
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager

import pytest
from tributary.loaders.clickhouse.client import TransportError, TransportTimeout, transport_from_config
from tributary.loaders.clickhouse.config import ChAuthSpec, ChTlsSpec, ClickhouseDestinationConfig

Script = Callable[[socket.socket], None]


class _Resolver:
    def resolve_text(self, ref: str, key: str) -> str:
        return {"user": "loader", "password": "s3cret"}[key]

    def resolve_file(self, ref: str, key: str) -> object:
        raise FileNotFoundError


@contextmanager
def raw_server(script: Script) -> Iterator[int]:
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
        yield int(listener.getsockname()[1])
    finally:
        stop.set()
        thread.join(3)
        listener.close()


def transport(port: int):  # type: ignore[no-untyped-def]
    config = ClickhouseDestinationConfig(
        channel="c",
        destination="d",
        host="127.0.0.1",
        port=port,
        database="db",
        table="t",
        id_column="tb_msg_id",
        auth=ChAuthSpec("password", "ch"),
        tls=ChTlsSpec("disable", None),
        timeout_s=0.6,
        circuit_breaker=None,
        idempotency_technique="sink-native",
        backoff="fixed",
        initial_delay_ms=100,
        max_delay_ms=1000,
        max_attempts=3,
        on_exhaust="halt-partition",
    )
    return transport_from_config(config, _Resolver())  # type: ignore[arg-type]


def test_the_production_response_type_has_read1() -> None:
    """The precondition of the real-socket tests below: the response object `urllib` hands
    `_read_capped` has the `read1` the loop prefers over `read`."""
    assert hasattr(http.client.HTTPResponse, "read1")


def test_a_trickled_body_is_cut_at_the_deadline_over_a_real_socket() -> None:
    def script(conn: socket.socket) -> None:
        conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n")
        for _ in range(60):  # a byte every 100 ms: always inside the 600 ms socket timeout
            conn.sendall(b"z")
            time.sleep(0.1)

    with raw_server(script) as port:
        started = time.monotonic()
        with pytest.raises(TransportTimeout):
            transport(port).send(b"INSERT", timeout_s=0.6)
        assert time.monotonic() - started < 2.5


def test_a_trickled_error_body_is_cut_at_the_deadline_over_a_real_socket() -> None:
    def script(conn: socket.socket) -> None:
        conn.sendall(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 100\r\n\r\n")
        for _ in range(60):
            conn.sendall(b"z")
            time.sleep(0.1)

    with raw_server(script) as port:
        started = time.monotonic()
        with pytest.raises(TransportTimeout):
            transport(port).send(b"INSERT", timeout_s=0.6)
        assert time.monotonic() - started < 2.5


def test_trickled_headers_are_cut_at_the_deadline() -> None:
    def script(conn: socket.socket) -> None:
        conn.sendall(b"HTTP/1.1 200 OK\r\n")
        for i in range(200):
            conn.sendall(f"X-H{i}: v\r\n".encode())
            time.sleep(0.1)

    with raw_server(script) as port:
        started = time.monotonic()
        with pytest.raises(TransportTimeout):
            transport(port).send(b"INSERT", timeout_s=0.6)
        assert time.monotonic() - started < 2.5


def test_a_malformed_status_line_is_a_typed_error_that_does_not_echo_it() -> None:
    def script(conn: socket.socket) -> None:
        conn.sendall(b"PEER-CONTROLLED-TEXT-7f3a not http\r\n\r\n")

    with raw_server(script) as port, pytest.raises(TransportError) as exc_info:
        transport(port).send(b"INSERT", timeout_s=2.0)
    assert "PEER-CONTROLLED" not in str(exc_info.value)
    assert exc_info.value.__suppress_context__ is True


def test_a_prompt_complete_response_still_succeeds() -> None:
    def script(conn: socket.socket) -> None:
        conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")

    with raw_server(script) as port:
        assert transport(port).send(b"INSERT", timeout_s=2.0).status_code == 200
