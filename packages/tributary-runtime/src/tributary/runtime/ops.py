"""The ops listener for every Tributary process: `GET /livez`, `GET /readyz`, `GET /metrics` on a port of their
own (`contracts/observability/v0` 2.1 and 3.1). One implementation, moved here from `tributary.receivers.ops`
(W3-25, ruling H1) with the hardening unchanged.

Rules this module implements, by their contract names:

* **O1** never served on an ingest port: this is a separate `asyncio` server.
* **O2/O3** loopback `127.0.0.1:9180` by default (`--ops-bind`, `--ops-port`, `TB_OPS_BIND`, `TB_OPS_PORT`;
  the flag wins). A non-loopback bind is accepted only when given explicitly, and only as an IP literal (a
  name is never resolved here).
* **O4** the caller treats a bind failure as `startup_refused` with `code: OPS_LISTENER_BIND_FAILED`, exit 2
  (`start_ops_or_exit` does that for a synchronous process).
* **O5** `GET` only (other methods 405), unknown path 404, both with an empty body. The query string is ignored,
  a request body is never read, the request line plus headers are capped at 8 KiB, the read has a 1 s deadline,
  and at most 8 connections are served at once (at most 4 per peer address, with the eviction rules in
  `OpsServer`). **No response echoes the path, a header or a peer address, and nothing is logged per request.**
* **O6** `/readyz` and `/metrics` serialise state the process already holds; this module has no Kafka client.

Which paths a process serves is decided by what it passes: `/readyz` only with a `readiness` callable (receivers
and the API; `observability/v0` 1 contracts no worker, loader or archiver `/readyz`, so they get 404), `/metrics`
only with a `metrics` callable. `/livez` is always served.

**Liveness on a thread.** A synchronous process (the worker) serves this listener from a background thread
(`OpsThread`), so the endpoint is no longer "served by the loop it vouches for" (2.2). It passes `liveness`, a
heartbeat check on its own main loop, so a wedged loop still fails `/livez` (503, empty body) instead of looking
alive for ever. Liveness still never depends on Kafka, the spec or readiness.
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import contextlib
import errno
import ipaddress
import os
import socket
import threading
import time
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from functools import partial
from typing import Final, Protocol

import orjson
from tributary.sdk.context import Logger

from .errors import safe_token
from .startup import refuse

DEFAULT_BIND = "127.0.0.1"
DEFAULT_PORT = 9180
MAX_HEAD_BYTES = 8 * 1024
READ_TIMEOUT_S = 1.0
WRITE_TIMEOUT_S = 1.0
MAX_CONNECTIONS = 8

#: `observability/v0` `metrics.json` `labels.component`: the process roles.
COMPONENTS: Final = frozenset({"receiver", "worker", "loader", "archiver", "api"})
#: Prometheus text exposition format 0.0.4.
METRICS_CONTENT_TYPE: Final = "text/plain; version=0.0.4; charset=utf-8"
JSON_CONTENT_TYPE: Final = "application/json"

_REASONS = {
    200: "OK",
    400: "Bad Request",
    404: "Not Found",
    405: "Method Not Allowed",
    431: "Request Header Fields Too Large",
    503: "Service Unavailable",
}

_LOOPBACK_WORDS = frozenset({"localhost"})


class OpsConfigError(ValueError):
    """A bad `--ops-bind` or `--ops-port`. The message names the flag, never its value."""


class ReadinessReport(Protocol):
    """What `/readyz` serialises: a status code and a PHI-free body (`readiness.schema.json`)."""

    @property
    def http_status(self) -> int: ...

    def body(self) -> bytes: ...


def is_loopback(bind: str) -> bool:
    """True for any `127.0.0.0/8` literal, `::1`, or the word `localhost`. A name is never resolved."""
    if bind in _LOOPBACK_WORDS:
        return True
    try:
        return ipaddress.ip_address(bind).is_loopback
    except ValueError:
        return False


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


def add_ops_arguments(parser: argparse.ArgumentParser) -> None:
    """`--ops-bind` and `--ops-port` (O2). Resolved by `ops_settings_or_exit`, so the flag wins over the env."""
    parser.add_argument("--ops-bind", default=None, help=f"ops listener address (default {DEFAULT_BIND}; TB_OPS_BIND)")
    parser.add_argument(
        "--ops-port", default=None, help=f"ops listener port, 0 = ephemeral (default {DEFAULT_PORT}; TB_OPS_PORT)"
    )


def ops_settings_or_exit(args: argparse.Namespace, log: Logger) -> OpsSettings:
    try:
        return resolve_ops_settings(bind=args.ops_bind, port=args.ops_port)
    except OpsConfigError:
        refuse(log, error="UsageError", code="ARGUMENTS_INVALID", reason="invalid --ops-bind or --ops-port")


def live_body(component: str) -> bytes:
    return orjson.dumps({"v": 0, "status": "alive", "component": component})


def _response(status: int, body: bytes = b"", *, allow: bool = False, content_type: str = JSON_CONTENT_TYPE) -> bytes:
    head = [f"HTTP/1.1 {status} {_REASONS[status]}", "Connection: close", f"Content-Length: {len(body)}"]
    if body:
        head.append(f"Content-Type: {content_type}")
    head.append("Cache-Control: no-store")
    if allow:
        head.append("Allow: GET")
    return ("\r\n".join(head) + "\r\n\r\n").encode("ascii") + body


#: A connection younger than this is never evicted: a client that connects and sends its request a few
#: milliseconds later (every real probe) must not be dropped by a newer arrival.
MIN_EVICT_AGE_S = 0.05
#: A client that has sent some bytes but not finished its head after this long is a trickler, and may be evicted by a
#: newcomer from a different address.
TRICKLE_EVICT_AGE_S = 0.2
#: Slots one peer address may hold, so one address cannot fill the 8 with tricklers.
MAX_PER_PEER = 4
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

    The accept loop is ours (`loop.sock_accept`), because `asyncio.Server` cannot pause its listening socket.

    `readiness`, `metrics` and `liveness` are called on this server's event loop and must not block: they read a
    snapshot the process maintains (O6). Any exception from one answers 503 with an empty body, never a 500 with
    text.
    """

    def __init__(
        self,
        settings: OpsSettings,
        readiness: Callable[[], ReadinessReport] | None = None,
        *,
        component: str,
        metrics: Callable[[], bytes] | None = None,
        liveness: Callable[[], bool] | None = None,
        log: Logger | None = None,
    ) -> None:
        if component not in COMPONENTS:
            raise ValueError("component must be one of the observability/v0 component roles")
        self._settings = settings
        self._readiness = readiness
        self._metrics = metrics
        self._liveness = liveness
        self._live_body = live_body(component)
        self._log = log
        self._sock: socket.socket | None = None
        self._accept_task: asyncio.Task[None] | None = None
        self._active: dict[asyncio.StreamWriter, _Conn] = {}
        self._closing = False

    @property
    def settings(self) -> OpsSettings:
        return self._settings

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

    def _victim(self, peer: str | None) -> asyncio.StreamWriter | None:
        """The connection a newcomer from `peer` (`None`: not yet known) may take the slot of, or `None`.

        Evictable means still reading its head and either silent for `MIN_EVICT_AGE_S`, or a trickler (some bytes,
        head unfinished) for `TRICKLE_EVICT_AGE_S`. A peer address holds at most `MAX_PER_PEER` slots: at that
        count it may only replace one of its own. Otherwise the newcomer takes the oldest evictable connection of
        the peer holding the most slots, and never a trickler of its own address (that is what the cap is for)."""
        now = time.monotonic()
        held = Counter(c.peer for c in self._active.values())

        def evictable(c: _Conn) -> bool:
            age = now - c.born
            return c.reading and age >= (TRICKLE_EVICT_AGE_S if c.got_bytes else MIN_EVICT_AGE_S)

        if peer is not None and held[peer] >= MAX_PER_PEER:
            return next((w for w, c in self._active.items() if c.peer == peer and evictable(c)), None)
        candidates = [(w, c) for w, c in self._active.items() if evictable(c) and not (c.got_bytes and c.peer == peer)]
        if not candidates:
            return None
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
        except Exception as exc:
            # Close the listener, so a probe is refused rather than left hanging on a socket nobody accepts from.
            sock.close()
            self._log_death(exc)

    async def _accept_forever(self, sock: socket.socket) -> None:
        loop = asyncio.get_running_loop()
        while not self._closing:
            if len(self._active) >= MAX_CONNECTIONS and self._victim(None) is None:
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
            except OSError:
                conn.close()  # this connection is lost (reset while being set up); the listener is fine
                continue
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
            if self._liveness is not None:
                try:
                    alive = self._liveness()
                except Exception:
                    alive = False
                if not alive:
                    return _response(503)
            return _response(200, self._live_body)
        if path == "/readyz" and self._readiness is not None:
            try:
                report = self._readiness()
                return _response(report.http_status, report.body())
            except Exception:  # a probe must answer; a broken snapshot is "not ready", never a 500 with text
                return _response(503)
        if path == "/metrics" and self._metrics is not None:
            try:
                return _response(200, self._metrics(), content_type=METRICS_CONTENT_TYPE)
            except Exception:  # never a traceback on the wire
                return _response(503)
        return _response(404)

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peername = writer.get_extra_info("peername")
        conn = _Conn(peer=str(peername[0]) if peername else "", born=time.monotonic())
        over_peer_cap = sum(1 for c in self._active.values() if c.peer == conn.peer) >= MAX_PER_PEER
        if over_peer_cap or len(self._active) >= MAX_CONNECTIONS:
            victim = self._victim(conn.peer)
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
        """The request head (to the blank line), an empty head with `conn.oversize` set past 8 KiB, `None` on a
        timeout or a closed peer. The 1 s deadline covers the whole head, so a trickle does not extend it. `conn`
        records the first byte."""
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


class OpsThread:
    """Runs an `OpsServer` on its own event loop in a daemon thread, for a synchronous process (the worker).

    `start()` returns once the socket is bound, or raises the bind's `OSError` in the caller's thread, so O4 is
    decided before the process builds anything. `close()` is idempotent and bounded.
    """

    def __init__(self, server: OpsServer) -> None:
        self._server = server
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop: asyncio.Event | None = None
        self._bound: concurrent.futures.Future[int] = concurrent.futures.Future()

    @property
    def server(self) -> OpsServer:
        return self._server

    def start(self, *, timeout_s: float = 10.0) -> int:
        if self._thread is not None:
            raise RuntimeError("ops thread already started")
        self._thread = threading.Thread(target=self._run, name="tb-ops", daemon=True)
        self._thread.start()
        return self._bound.result(timeout=timeout_s)  # re-raises the OSError of a failed bind

    def _run(self) -> None:
        asyncio.run(self._main())

    async def _main(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._stop = asyncio.Event()
        try:
            port = await self._server.start()
        except BaseException as exc:
            self._bound.set_exception(exc)
            return
        self._bound.set_result(port)
        try:
            await self._stop.wait()
        finally:
            await self._server.close()

    def close(self, *, timeout_s: float = 5.0) -> None:
        loop, stop, thread = self._loop, self._stop, self._thread
        if loop is not None and stop is not None and not loop.is_closed():
            with contextlib.suppress(RuntimeError):  # the loop finished between the check and the call
                loop.call_soon_threadsafe(stop.set)
        if thread is not None:
            thread.join(timeout_s)


def start_ops_or_exit(server: OpsServer, log: Logger) -> OpsThread:
    """Bind on a background thread, or one `startup_refused` line and exit 2 (O4). Logs `ops_listening` with the
    bound port, and `ops_listener_non_loopback` at WARN for an explicit non-loopback bind (O3)."""
    thread = OpsThread(server)
    try:
        port = thread.start()
    except OSError as exc:
        thread.close()
        log.error("startup_refused", error="OSError", code="OPS_LISTENER_BIND_FAILED", errno=exc.errno or 0)
        raise SystemExit(2) from None
    if server.settings.explicit_non_loopback:
        log.warning("ops_listener_non_loopback", ops_port=port)
    log.info("ops_listening", ops_port=port)
    return thread
