"""An overall wall-clock deadline for one `urllib` request, status line and headers included.

`urllib`'s `timeout=` is per socket operation. A peer that trickles the status line or the headers one
byte at a time, just under that timeout, would hold `opener.open()` for as long as it liked: the body
loops (`_read_capped`) only start once the headers are in. `Watchdog` closes that gap: it arms a timer
when the connection is established and, when the deadline passes, shuts the socket down from the timer
thread, which makes whatever `recv` the request thread is blocked in return at once. The caller then
sees the failure as an ordinary `OSError`/`HTTPException` and asks `Watchdog.fired` to tell a deadline
from any other failure.

The TCP connect and (for HTTPS) the TLS handshake happen before the socket is armed: each is bounded per
socket operation only, and a handshake is a small, finite exchange. The watchdog's job is the response.

Wiring: `DeadlineRequest` carries the watchdog, and `DeadlineHTTPHandler`/`DeadlineHTTPSHandler` build
their connections with it. Plain `urllib.request.Request` objects (a test-injected opener, for instance)
work unchanged with these handlers: no watchdog means the stock connection class.
"""

from __future__ import annotations

import contextlib
import http.client
import socket
import ssl
import threading
import time
import urllib.request
from collections.abc import Callable
from typing import Any

__all__ = ["DeadlineHTTPHandler", "DeadlineHTTPSHandler", "DeadlineRequest", "Watchdog"]


class Watchdog:
    """Shuts a socket down `timeout_s` after construction. One per request; call `cancel()` when done."""

    def __init__(self, timeout_s: float) -> None:
        self.deadline = time.monotonic() + timeout_s
        self.fired = False
        #: True if the deadline passed before `headers_complete()`: whatever `opener.open` then returned
        #: was assembled from a header block cut short (an EOF ends a header parse quietly), so it is
        #: not a response to trust.
        self.fired_in_headers = False
        self._headers_done = False
        self._sock: socket.socket | None = None
        self._timer: threading.Timer | None = None
        self._lock = threading.Lock()

    def arm(self, sock: socket.socket) -> None:
        with self._lock:
            self._sock = sock
            timer = threading.Timer(max(self.deadline - time.monotonic(), 0.0), self._fire)
            timer.daemon = True
            self._timer = timer
        timer.start()

    def _fire(self) -> None:
        with self._lock:
            self.fired = True
            self.fired_in_headers = not self._headers_done
            sock = self._sock
        if sock is not None:
            with contextlib.suppress(OSError):
                sock.shutdown(socket.SHUT_RDWR)

    def headers_complete(self) -> None:
        """The caller has a full status line and header block (`opener.open` returned)."""
        with self._lock:
            self._headers_done = True

    def cancel(self) -> None:
        with self._lock:
            timer, self._timer = self._timer, None
        if timer is not None:
            timer.cancel()


class DeadlineRequest(urllib.request.Request):
    def __init__(self, *args: Any, watchdog: Watchdog | None = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.watchdog = watchdog


class _HTTPConnection(http.client.HTTPConnection):
    def __init__(self, *args: Any, watchdog: Watchdog, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._watchdog = watchdog

    def connect(self) -> None:
        super().connect()
        if self.sock is not None:
            self._watchdog.arm(self.sock)


class _HTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, *args: Any, watchdog: Watchdog, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._watchdog = watchdog

    def connect(self) -> None:
        super().connect()
        if self.sock is not None:
            self._watchdog.arm(self.sock)


def _watchdog_of(req: urllib.request.Request) -> Watchdog | None:
    watchdog = getattr(req, "watchdog", None)
    return watchdog if isinstance(watchdog, Watchdog) else None


class DeadlineHTTPHandler(urllib.request.HTTPHandler):
    def http_open(self, req: urllib.request.Request) -> http.client.HTTPResponse:
        watchdog = _watchdog_of(req)
        if watchdog is None:
            return self.do_open(http.client.HTTPConnection, req)
        factory: Callable[..., http.client.HTTPConnection] = lambda host, **kw: _HTTPConnection(  # noqa: E731
            host, watchdog=watchdog, **kw
        )
        return self.do_open(factory, req)


class DeadlineHTTPSHandler(urllib.request.HTTPSHandler):
    def __init__(self, context: ssl.SSLContext) -> None:
        super().__init__(context=context)
        self._tb_context = context

    def https_open(self, req: urllib.request.Request) -> http.client.HTTPResponse:
        watchdog = _watchdog_of(req)
        if watchdog is None:
            return self.do_open(http.client.HTTPSConnection, req, context=self._tb_context)
        factory: Callable[..., http.client.HTTPSConnection] = lambda host, **kw: _HTTPSConnection(  # noqa: E731
            host, watchdog=watchdog, **kw
        )
        return self.do_open(factory, req, context=self._tb_context)
