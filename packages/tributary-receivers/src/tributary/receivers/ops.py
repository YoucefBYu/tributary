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
import errno
import ipaddress
import os
import socket
import time
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from functools import partial

from tributary.runtime.errors import safe_token
from tributary.sdk.context import Logger

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


#: A connection younger than this is never evicted: a client that connects and sends its request a few
#: milliseconds later (every real probe) must not be dropped by a newer arrival.
MIN_EVICT_AGE_S = 0.05
BACKLOG = 128
#: Resource exhaustion: back off, so the loop does not spin on an accept that cannot succeed.
_BACKOFF_ERRNOS = frozenset({errno.EMFILE, errno.ENFILE, errno.ENOBUFS, errno.ENOMEM})
#: A connection that died between the kernel's accept queue and ours: not an error of the listener, retry at once.
_RETRY_ERRNOS = frozenset({errno.ECONNABORTED, errno.EPROTO})


@dataclass(slots=True)
class _Conn:
    peer: str
    born: float
    reading: bool = True  # still waiting for the end of its head
    got_bytes: bool = False  # has sent at least one byte of it
    oversize: bool = False


def _same(protocol: asyncio.StreamReaderProtocol) -> asyncio.StreamReaderProtocol:
    return protocol


class OpsServer:
    """Capacity rule (O5, at most 8 connections). A newcomer evicts one connection only if that one is *evictable*:
    still reading its head, has sent no byte, and is at least `MIN_EVICT_AGE_S` old. Among those, it takes the
    oldest from the peer address that holds the most slots, so a single flooder pays for its own flood and a probe
    from another address is not the one dropped. A connection that has sent bytes is never evicted.

    When all 8 are protected (young, or past their head), accepting pauses until one is evictable or done: the
    waiting connections sit in the kernel backlog (128) instead of costing the loop an accept each.

    The accept loop is ours (`loop.sock_accept`), because `asyncio.Server` cannot pause its listening socket."""

    def __init__(self, settings: OpsSettings, readiness: Callable[[], Report], *, log: Logger | None = None) -> None:
        self._settings = settings
        self._readiness = readiness
        self._log = log
        self._sock: socket.socket | None = None
        self._accept_task: asyncio.Task[None] | None = None
        self._active: dict[asyncio.StreamWriter, _Conn] = {}
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
            sock.listen(BACKLOG)
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

    def _victim(self) -> asyncio.StreamWriter | None:
        now = time.monotonic()
        candidates = [
            (w, c) for w, c in self._active.items() if c.reading and not c.got_bytes and now - c.born >= MIN_EVICT_AGE_S
        ]
        if not candidates:
            return None
        held = Counter(c.peer for c in self._active.values())
        heaviest = max(held[c.peer] for _w, c in candidates)
        return next(w for w, c in candidates if held[c.peer] == heaviest)  # dict order: the oldest of them

    def _log_death(self, exc: BaseException) -> None:
        if self._log is not None:
            self._log.error("ops_accept_loop_died", error=safe_token(type(exc).__name__))

    async def _accept_loop(self, sock: socket.socket) -> None:
        try:
            await self._accept_forever(sock)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # the listener is gone: liveness will now fail, which is the honest answer
            self._log_death(exc)

    async def _accept_forever(self, sock: socket.socket) -> None:
        loop = asyncio.get_running_loop()
        while not self._closing:
            if len(self._active) >= MAX_CONNECTIONS and self._victim() is None:
                await asyncio.sleep(0.01)  # every slot is protected: leave the rest in the backlog
                continue
            try:
                conn, addr = await loop.sock_accept(sock)
            except OSError as exc:
                if self._closing:
                    return
                if exc.errno in _RETRY_ERRNOS:
                    await asyncio.sleep(0)
                    continue
                if exc.errno in _BACKOFF_ERRNOS:
                    await asyncio.sleep(0.05)
                    continue
                raise
            try:
                conn.setblocking(False)
                reader = asyncio.StreamReader(limit=MAX_HEAD_BYTES)
                protocol = asyncio.StreamReaderProtocol(reader, self._handle)
                await loop.connect_accepted_socket(partial(_same, protocol), conn)
            except BaseException:
                conn.close()  # never leak an accepted socket, whatever went wrong between accept and the transport
                raise
            del addr
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
        peername = writer.get_extra_info("peername")
        conn = _Conn(peer=str(peername[0]) if peername else "", born=time.monotonic())
        if len(self._active) >= MAX_CONNECTIONS:
            victim = self._victim()
            if victim is None:  # every slot is protected: refuse this one rather than exceed the cap
                writer.close()
                return
            del self._active[victim]
            victim.close()  # its handler sees the closed stream and ends
        self._active[writer] = conn
        try:
            head = await self._read_head(reader, conn)
            if head is None:
                return
            if conn.oversize:
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
            writer.close()

    @staticmethod
    async def _read_head(reader: asyncio.StreamReader, conn: _Conn) -> bytes | None:
        """The request head (to the blank line), an empty head with `conn.oversize` set past 8 KiB, `None` on a timeout or a closed peer. The 1 s
        deadline covers the whole head, so a trickle does not extend it. `conn` records the first byte."""
        deadline = time.monotonic() + READ_TIMEOUT_S
        buffer = b""
        try:
            while b"\r\n\r\n" not in buffer:
                if len(buffer) > MAX_HEAD_BYTES:
                    conn.reading = False
                    conn.oversize = True
                    return b""
                chunk = await asyncio.wait_for(reader.read(4096), max(0.0, deadline - time.monotonic()))
                if not chunk:
                    return None
                conn.got_bytes = True
                buffer += chunk
        except (TimeoutError, ConnectionError, OSError):
            return None
        conn.reading = False
        end = buffer.index(b"\r\n\r\n") + 4
        conn.oversize = end > MAX_HEAD_BYTES
        return buffer[:end]

    @staticmethod
    async def _send(writer: asyncio.StreamWriter, data: bytes) -> None:
        with contextlib.suppress(TimeoutError, ConnectionError, OSError):
            writer.write(data)
            await asyncio.wait_for(writer.drain(), WRITE_TIMEOUT_S)
