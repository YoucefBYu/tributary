"""The ops listener: `GET /livez` and `GET /readyz` on a port of their own (`contracts/observability/v0` 2.1).

Rules this module implements, by their contract names:

* **O1** never served on an ingest port: this is a separate `asyncio` server.
* **O2/O3** loopback `127.0.0.1:9180` by default (`--ops-bind`, `--ops-port`, `TB_OPS_BIND`, `TB_OPS_PORT`;
  the flag wins). A non-loopback bind is accepted only when given explicitly, and only as an IP literal (a
  name is never resolved here, the same reason as `net.is_loopback`).
* **O4** the caller treats a bind failure as `startup_refused`, exit 2.
* **O5** `GET` only (other methods 405), unknown path 404, both with an empty body. The query string is ignored,
  a request body is never read, the request line plus headers are capped at 8 KiB, the read has a 1 s deadline,
  and at most 8 connections are served at once. **No response echoes the path, a header or a peer address, and
  nothing is logged per request.**
* **O6** `/readyz` serialises a snapshot the lifecycle holds; this module has no Kafka client.
"""

from __future__ import annotations

import asyncio
import contextlib
import ipaddress
import os
import socket
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from functools import partial

from .net import is_loopback
from .readiness import LIVE_BODY, Report

DEFAULT_BIND = "127.0.0.1"
DEFAULT_PORT = 9180
MAX_HEAD_BYTES = 8 * 1024
READ_TIMEOUT_S = 1.0
WRITE_TIMEOUT_S = 1.0
MAX_CONNECTIONS = 8

_REASONS = {
    200: "OK",
    400: "Bad Request",
    404: "Not Found",
    405: "Method Not Allowed",
    431: "Request Header Fields Too Large",
    503: "Service Unavailable",
}


class OpsConfigError(ValueError):
    """A bad `--ops-bind` or `--ops-port`. The message names the flag, never its value."""


@dataclass(frozen=True, slots=True)
class OpsSettings:
    bind: str = DEFAULT_BIND
    port: int = DEFAULT_PORT

    @property
    def explicit_non_loopback(self) -> bool:
        return not is_loopback(self.bind)


def resolve_ops_settings(
    *, bind: str | None, port: int | str | None, environ: Mapping[str, str] | None = None
) -> OpsSettings:
    """Flag over environment over default. Raises `OpsConfigError` for a bind that is not an IP literal (or
    `localhost`) and for a port outside 0..65535 (0 picks an ephemeral port, for tests)."""
    env = os.environ if environ is None else environ
    chosen_bind = bind if bind is not None else env.get("TB_OPS_BIND") or DEFAULT_BIND
    raw_port = port if port is not None else env.get("TB_OPS_PORT")
    if chosen_bind == "localhost":
        chosen_bind = "127.0.0.1"  # never resolved: a name could resolve to something else
    else:
        try:
            ipaddress.ip_address(chosen_bind)
        except ValueError:
            raise OpsConfigError("ops bind address must be an IP literal") from None
    if raw_port is None or raw_port == "":
        chosen_port = DEFAULT_PORT
    else:
        try:
            chosen_port = int(raw_port)
        except (TypeError, ValueError):
            raise OpsConfigError("ops port must be an integer") from None
    if not 0 <= chosen_port <= 65535:
        raise OpsConfigError("ops port must be 0..65535")
    return OpsSettings(bind=chosen_bind, port=chosen_port)


def _response(status: int, body: bytes = b"", *, allow: bool = False) -> bytes:
    head = [f"HTTP/1.1 {status} {_REASONS[status]}", "Connection: close", f"Content-Length: {len(body)}"]
    if body:
        head.append("Content-Type: application/json")
    head.append("Cache-Control: no-store")
    if allow:
        head.append("Allow: GET")
    return ("\r\n".join(head) + "\r\n\r\n").encode("ascii") + body


def _same(protocol: asyncio.StreamReaderProtocol) -> asyncio.StreamReaderProtocol:
    return protocol


class OpsServer:
    """Capacity rule (O5, at most 8 connections): a newcomer is always served by evicting the **oldest connection
    still reading its head** (an idle or slow client must not lock a probe out). When all 8 are past their head
    (writing a response, which takes microseconds) nothing is evictable, so accepting pauses until one finishes:
    the connections wait in the kernel backlog instead of costing the loop an accept each.

    The accept loop is ours (`loop.sock_accept`), because `asyncio.Server` cannot pause its listening socket."""

    def __init__(self, settings: OpsSettings, readiness: Callable[[], Report]) -> None:
        self._settings = settings
        self._readiness = readiness
        self._sock: socket.socket | None = None
        self._accept_task: asyncio.Task[None] | None = None
        #: Open connections, oldest first; the value is True while the head is still being read.
        self._active: dict[asyncio.StreamWriter, bool] = {}
        self._room = asyncio.Event()
        self._room.set()
        self._closing = False

    @property
    def port(self) -> int:
        if self._sock is None:
            raise RuntimeError("ops listener is not bound")
        return int(self._sock.getsockname()[1])

    @property
    def open_connections(self) -> int:
        return len(self._active)

    async def start(self) -> int:
        """Bind. Raises `OSError` when it cannot (the caller exits 2). Returns the bound port."""
        bind = self._settings.bind
        family = socket.AF_INET6 if ":" in bind else socket.AF_INET
        sock = socket.socket(family, socket.SOCK_STREAM)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((bind, self._settings.port))
            sock.listen(16)
            sock.setblocking(False)
        except OSError:
            sock.close()
            raise
        self._sock = sock
        self._accept_task = asyncio.create_task(self._accept_loop(sock))
        return self.port

    async def close(self) -> None:
        self._closing = True
        task, self._accept_task = self._accept_task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        sock, self._sock = self._sock, None
        if sock is not None:
            sock.close()
        for writer in list(self._active):
            writer.close()
        self._active.clear()

    def _update_room(self) -> None:
        if len(self._active) < MAX_CONNECTIONS or any(self._active.values()):
            self._room.set()
        else:
            self._room.clear()

    async def _accept_loop(self, sock: socket.socket) -> None:
        loop = asyncio.get_running_loop()
        while not self._closing:
            await self._room.wait()  # paused at capacity with nothing evictable
            try:
                conn, _addr = await loop.sock_accept(sock)
            except asyncio.CancelledError:
                raise
            except OSError:
                if self._closing:
                    return
                await asyncio.sleep(0.05)  # EMFILE and the like: back off, never spin
                continue
            conn.setblocking(False)
            reader = asyncio.StreamReader(limit=MAX_HEAD_BYTES)
            protocol = asyncio.StreamReaderProtocol(reader, self._handle)
            try:
                await loop.connect_accepted_socket(partial(_same, protocol), conn)
            except OSError:
                conn.close()
                continue
            await asyncio.sleep(0)  # let the handler register before the next capacity check
            await asyncio.sleep(0)

    def _route(self, method: str, path: str) -> bytes:
        if method != "GET":
            return _response(405, allow=True)
        if path == "/livez":
            return _response(200, LIVE_BODY)
        if path == "/readyz":
            try:
                report = self._readiness()
            except Exception:  # a probe must answer; a broken snapshot is "not ready", never a 500 with text
                return _response(503)
            return _response(report.http_status, report.body())
        return _response(404)

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if len(self._active) >= MAX_CONNECTIONS:
            oldest = next((w for w, reading in self._active.items() if reading), None)
            if oldest is None:  # unreachable while the accept loop pauses; refuse rather than exceed the cap
                writer.close()
                return
            del self._active[oldest]
            oldest.close()  # its handler sees the closed stream and ends
        self._active[writer] = True
        self._update_room()
        try:
            try:
                head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), READ_TIMEOUT_S)
            except (TimeoutError, ConnectionError, OSError, asyncio.IncompleteReadError):
                return
            except asyncio.LimitOverrunError:
                self._reading_done(writer)
                await self._send(writer, _response(431))
                return
            self._reading_done(writer)
            if len(head) > MAX_HEAD_BYTES:
                await self._send(writer, _response(431))
                return
            line = head.split(b"\r\n", 1)[0].split(b" ")
            if len(line) != 3 or not line[2].startswith(b"HTTP/1."):
                await self._send(writer, _response(400))
                return
            method = line[0].decode("ascii", errors="replace")
            path = line[1].split(b"?", 1)[0].decode("ascii", errors="replace")
            await self._send(writer, self._route(method, path))
        finally:
            self._active.pop(writer, None)
            self._update_room()
            writer.close()

    def _reading_done(self, writer: asyncio.StreamWriter) -> None:
        if writer in self._active:
            self._active[writer] = False
            self._update_room()

    @staticmethod
    async def _send(writer: asyncio.StreamWriter, data: bytes) -> None:
        with contextlib.suppress(TimeoutError, ConnectionError, OSError):
            writer.write(data)
            await asyncio.wait_for(writer.drain(), WRITE_TIMEOUT_S)
