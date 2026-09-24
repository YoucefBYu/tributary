"""A stub HTTP destination for e2e tests: records every request instead of doing anything with it.

`deploy/compose/docker-compose.yml`'s `loader` service points at `http://127.0.0.1:8080/ingest` with
nothing listening there on purpose — "point it at a real stub, or let a future e2e test bring its own"
(`deploy/compose/README.md`). This is that stub. It never runs outside a test process and never touches
real patient data (synthetic fixtures only, per CLAUDE.md).
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


@dataclass(frozen=True, slots=True)
class CapturedRequest:
    method: str
    path: str
    headers: dict[str, str]
    body: bytes
    #: `time.monotonic()` at capture, so a chaos scenario spanning two `StubHttpServer` instances (one
    #: stopped to simulate an outage, a second started once it is "restored") can merge both servers'
    #: `requests` and still recover one true chronological order.
    received_at: float


class _RecordingServer(ThreadingHTTPServer):
    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)  # type: ignore[arg-type]
        self._lock = threading.Lock()
        self._captured: list[CapturedRequest] = []

    def record(self, request: CapturedRequest) -> None:
        with self._lock:
            self._captured.append(request)

    def snapshot(self) -> list[CapturedRequest]:
        with self._lock:
            return list(self._captured)


class _Handler(BaseHTTPRequestHandler):
    server: _RecordingServer

    def _capture(self, method: str) -> None:
        length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(length) if length else b""
        self.server.record(
            CapturedRequest(
                method=method,
                path=self.path,
                headers=dict(self.headers.items()),
                body=body,
                received_at=time.monotonic(),
            )
        )
        self.send_response(200)
        self.end_headers()

    def do_POST(self) -> None:
        self._capture("POST")

    def do_PUT(self) -> None:
        self._capture("PUT")

    def do_PATCH(self) -> None:
        self._capture("PATCH")

    def log_message(self, log_format: str, *args: object) -> None:
        pass  # quiet: the test asserts on `requests`, not on stderr noise


class StubHttpServer:
    """`http://127.0.0.1:<port>` — loopback-bound, matching every listener in this repo (Phase 0 gate
    finding High 2 / SR-8). `port` defaults to one chosen by the OS (bind to 0) so parallel test runs
    never collide; a chaos scenario simulating a destination *outage* (stop this, then start a fresh one
    on the same port once recovery is being tested) passes the prior instance's `port` explicitly so the
    channel spec's URL -- fixed once published -- still points at something once the stub comes back."""

    def __init__(self, *, port: int = 0) -> None:
        self._httpd = _RecordingServer(("127.0.0.1", port), _Handler)
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True, name="http-stub")

    @property
    def port(self) -> int:
        return int(self._httpd.server_address[1])

    def url(self, path: str = "/ingest") -> str:
        return f"http://127.0.0.1:{self.port}{path}"

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=5)

    @property
    def requests(self) -> list[CapturedRequest]:
        return self._httpd.snapshot()
