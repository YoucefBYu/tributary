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
    def __init__(self, *args: object, fail_first_n: int = 0, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)  # type: ignore[arg-type]
        self._lock = threading.Lock()
        self._captured: list[CapturedRequest] = []
        #: A genuine redelivery test (`contracts/channel-spec/v0/README.md` 5.1: "a genuine redelivery
        #: from an uncommitted offset, not a second call in the same process") needs a destination that
        #: fails on its own, from a real network response, so `ConsumerLoop`'s blocking retry is what
        #: redelivers the record -- not the test calling a loader method twice.
        self._remaining_failures = fail_first_n

    def record(self, request: CapturedRequest) -> None:
        with self._lock:
            self._captured.append(request)

    def snapshot(self) -> list[CapturedRequest]:
        with self._lock:
            return list(self._captured)

    def next_status(self) -> int:
        """200, unless `fail_first_n` failures are still owed -- each call consumes one."""
        with self._lock:
            if self._remaining_failures > 0:
                self._remaining_failures -= 1
                return 503
            return 200


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
        self.send_response(self.server.next_status())
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
    channel spec's URL -- fixed once published -- still points at something once the stub comes back.

    `fail_first_n` makes the first N requests to *any* path fail with a real 503 before the rest
    succeed with 200 -- a real HTTP failure, not a test-side decision, so a caller driving a loader's
    real `ConsumerLoop` against this gets a genuine blocking-retry redelivery from an uncommitted Kafka
    offset (`contracts/channel-spec/v0/README.md` 5.1)."""

    def __init__(self, *, port: int = 0, fail_first_n: int = 0) -> None:
        self._httpd = _RecordingServer(("127.0.0.1", port), _Handler, fail_first_n=fail_first_n)
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True, name="http-stub")
        self._stopped = False

    @property
    def port(self) -> int:
        return int(self._httpd.server_address[1])

    def url(self, path: str = "/ingest") -> str:
        return f"http://127.0.0.1:{self.port}{path}"

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        """Idempotent: a chaos scenario's cleanup path (`finally`) and its own mid-test "trigger the
        outage now" call can both legitimately want to stop the same instance -- the second call is a
        no-op rather than a double `server_close()` on an already-closed socket."""
        if self._stopped:
            return
        self._stopped = True
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=5)

    @property
    def requests(self) -> list[CapturedRequest]:
        return self._httpd.snapshot()
