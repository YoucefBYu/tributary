"""The `audit_log` tamper check (ADR-004 R7 rev 3; `contracts/observability/v0` README 3.4).

The API holds SELECT on the archive; the events archiver, which writes `audit_log`, never grades itself. A
background task per configured environment runs the archive's tamper query every
`TB_AUDIT_TAMPER_CHECK_INTERVAL_S` and stores two gauges. A scrape only reads the stored values.

* Success: set `tb_audit_tamper_suspect_ids`, then `tb_audit_tamper_check_timestamp_seconds`.
* Failure: change neither, log `audit_tamper_check_failed` with the exception class name only, and carry on.
  The stale timestamp is the signal (the warning alert). The API never stops because the archive is down, and
  nothing in the data plane depends on this task (invariants 3 and 9).
* Both series are absent until the first check for that environment completes.

**The query is mirrored, not imported.** `tributary-archiver` is the only importer the licence gate allows for
`certifi` (MPL-2.0); depending on it from the API would widen that approval, which is a human decision.
`tamper_sql` is therefore a copy of `tributary.archiver.schema.audit_tamper_sql`, and a test asserts the two are
equal for the same database, so a change to the archive's column list fails here until the copy follows.

The ClickHouse client is the HTTP interface from the standard library (as the ClickHouse loader does): one
readonly query, bounded rows and time, TLS verified, https or loopback http only.
"""

from __future__ import annotations

import asyncio
import contextlib
import http.client
import ipaddress
import re
import socket
import ssl
import threading
import time
from collections.abc import Callable, Mapping
from typing import Final
from urllib.parse import urlencode, urlsplit

from tributary.runtime.metrics import MetricsContract, MetricsRegistry
from tributary.sdk.context import Logger

AUDIT_TABLE: Final = "audit_log"
#: The `audit/v0` record fields: not `inserted_at`, `source_partition` or `source_offset` (an honest redelivery
#: may differ in those). Mirror of `tributary.archiver.schema.AUDIT_CONTENT_COLUMNS`.
AUDIT_CONTENT_COLUMNS: Final = (
    "ts, environment, action, phase, actor, actor_roles, request_id, operation_id, message_id, deployment_id, "
    "channel, destination, target_topic, stage, error_code, version, previous_version, allow_reorder, "
    "range_start, range_end, `count`"
)
MAX_RESPONSE_BYTES: Final = 1024
QUERY_TIMEOUT_S: Final = 30.0


_DB_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
#: `/livez` says alive while the task is running and a heartbeat has ticked within this many seconds. The
#: heartbeat is its own small task, so a slow or black-holed archive (a check awaiting its thread) never makes
#: a healthy API look dead; only a stopped task or a stalled event loop does.
LIVENESS_STALE_S: Final = 30.0
HEARTBEAT_S: Final = 2.0


def tamper_sql(*, database: str) -> str:
    """Mirror of `tributary.archiver.schema.audit_tamper_sql(database=...)`; equality is tested. A database
    name that is not a plain identifier is refused (it is interpolated between backticks)."""
    if not _DB_NAME.fullmatch(database):
        raise ValueError("invalid database name")
    return (
        f"SELECT count() FROM (SELECT audit_id FROM `{database}`.{AUDIT_TABLE} GROUP BY audit_id "  # noqa: S608
        f"HAVING uniqExact(tuple({AUDIT_CONTENT_COLUMNS})) > 1)"
    )


class TamperQueryError(Exception):
    """The archive could not answer. Carries no response text."""


class ClickHouseHttp:
    """`query(sql) -> str` over ClickHouse's HTTP interface. Blocking: call it off the event loop.

    Built on `http.client`, not `urllib.request.urlopen`, so by construction it never follows a redirect (a 3xx is
    an error, and the credential header goes to the configured host only) and never reads `HTTP_PROXY` or
    `HTTPS_PROXY` (the password is never handed to a proxy). TLS verifies against the system trust store. The
    timeout is per socket operation; `deadline_s` is the overall wall clock for the whole exchange, enforced by
    shutting the socket down from a timer thread (a server trickling bytes cannot hold a check open)."""

    def __init__(
        self,
        url: str,
        user: str,
        password: str,
        *,
        timeout_s: float = QUERY_TIMEOUT_S,
        deadline_s: float = QUERY_TIMEOUT_S,
    ) -> None:
        check_url(url)
        parts = urlsplit(url)
        self._https = parts.scheme == "https"
        self._host = parts.hostname or ""
        self._port = parts.port or (443 if self._https else 80)
        self._path = (
            (parts.path.rstrip("/") or "")
            + "/?"
            + urlencode({"readonly": "1", "max_execution_time": str(int(timeout_s)), "max_result_rows": "10"})
        )
        self._headers = {"X-ClickHouse-User": user, "X-ClickHouse-Key": password}
        self._timeout_s = timeout_s
        self._deadline_s = deadline_s
        self._context = ssl.create_default_context()

    def query(self, sql: str) -> str:
        """The whole exchange, DNS and connect and TLS included, has a hard bound of `deadline_s`: it runs in a
        daemon thread that this call joins for at most that long. On expiry the socket (if there is one yet) is
        shut down and the call fails; an abandoned thread ends at its own per-operation timeouts."""
        held: dict[str, http.client.HTTPConnection] = {}
        outcome: list[str | None] = []
        worker = threading.Thread(target=self._exchange, args=(sql, held, outcome), daemon=True, name="tb-tamper-query")
        worker.start()
        worker.join(self._deadline_s)
        if worker.is_alive():
            conn = held.get("conn")
            if conn is not None and conn.sock is not None:
                _shutdown(conn.sock)
            raise TamperQueryError
        if not outcome or outcome[0] is None:
            raise TamperQueryError
        return outcome[0]

    def _exchange(self, sql: str, held: dict[str, http.client.HTTPConnection], outcome: list[str | None]) -> None:
        conn: http.client.HTTPConnection
        if self._https:
            conn = http.client.HTTPSConnection(self._host, self._port, timeout=self._timeout_s, context=self._context)
        else:
            conn = http.client.HTTPConnection(self._host, self._port, timeout=self._timeout_s)
        held["conn"] = conn
        try:
            conn.request("POST", self._path, body=sql.encode(), headers=self._headers)
            response = conn.getresponse()
            body = response.read(MAX_RESPONSE_BYTES)
            if response.status == 200:  # anything else, every 3xx included: a redirect is never followed
                outcome.append(body.decode("utf-8", "replace"))
        except (OSError, http.client.HTTPException, ValueError):
            pass  # library text can carry the host and headers: the caller raises a bare error
        finally:
            conn.close()


def _shutdown(sock: socket.socket) -> None:
    with contextlib.suppress(OSError):
        sock.shutdown(socket.SHUT_RDWR)


def check_url(url: str) -> None:
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname or parts.username or parts.password:
        raise ValueError("the ClickHouse URL must be http(s) without credentials")
    if parts.scheme == "http":
        try:
            loopback = parts.hostname == "localhost" or ipaddress.ip_address(parts.hostname).is_loopback
        except ValueError:
            loopback = False
        if not loopback:
            raise ValueError("plain http to ClickHouse is allowed on loopback only")


class AuditMetrics:
    """The two gauges, registered through the contract registry (so a name or label outside it is refused)."""

    def __init__(self, registry: MetricsRegistry | None = None) -> None:
        self.registry = registry or MetricsRegistry(MetricsContract.load(), component="api", strict=True)
        self.suspects = self.registry.gauge("tb_audit_tamper_suspect_ids")
        self.checked_at = self.registry.gauge("tb_audit_tamper_check_timestamp_seconds")

    def render(self) -> bytes:
        return self.registry.render()


class TamperChecker:
    def __init__(
        self,
        targets: Mapping[str, str],
        query: Callable[[str], str],
        metrics: AuditMetrics,
        log: Logger,
        *,
        interval_s: float,
        clock: Callable[[], float] = time.time,
        liveness_stale_s: float = LIVENESS_STALE_S,
        heartbeat_s: float = HEARTBEAT_S,
    ) -> None:
        self._targets = dict(targets)
        self._query = query
        self._metrics = metrics
        self._log = log
        self._interval_s = interval_s
        self._clock = clock
        self._stale_s = liveness_stale_s
        self._heartbeat_s = heartbeat_s
        self._beat: float | None = None  # monotonic time of the last heartbeat; None until the task starts
        self._running = False

    def alive(self) -> bool:
        """For `/livez`: True before the task starts; afterwards the task must be running and have ticked
        recently, which also shows the event loop is turning. Reads two attributes; never blocks."""
        if self._beat is None:
            return not self._running
        return self._running and time.monotonic() - self._beat < self._stale_s

    async def check_once(self) -> None:
        """One check per environment. Never raises (cancellation excepted)."""
        for environment, database in self._targets.items():
            try:
                raw = await asyncio.to_thread(self._query, tamper_sql(database=database))
                suspects = int(raw.strip())
                if suspects < 0:
                    raise ValueError
                self._metrics.suspects.set(float(suspects), environment=environment)
                self._metrics.checked_at.set(self._clock(), environment=environment)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # any failure: leave both series as they were
                self._log.error("audit_tamper_check_failed", environment=environment, error_type=type(exc).__name__)
                continue
            if suspects:
                self._log.error("audit_tamper_suspected", environment=environment, suspect_ids=suspects)

    async def heartbeat(self) -> None:
        while True:
            self._beat = time.monotonic()
            await asyncio.sleep(self._heartbeat_s)

    async def run_forever(self) -> None:
        self._running = True
        self._beat = time.monotonic()
        beat = asyncio.create_task(self.heartbeat())  # independent of how long a check takes
        try:
            while True:
                await self.check_once()
                await asyncio.sleep(self._interval_s)
        finally:
            self._running = False
            beat.cancel()
