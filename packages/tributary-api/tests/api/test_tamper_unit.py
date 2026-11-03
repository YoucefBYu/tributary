"""The `audit_log` tamper check (ADR-004 R7 rev 3; observability/v0 README 3.4): a fake ClickHouse, the mirrored
query, the stored-gauge behaviour on success and failure, configuration, and the registry mapping."""

from __future__ import annotations

import asyncio
import contextlib
import http.server
import json
import threading
from typing import Any

import pytest
from tributary.api import config
from tributary.api.errors import ConfigurationError
from tributary.api.tamper import (
    AuditMetrics,
    ClickHouseHttp,
    TamperChecker,
    TamperQueryError,
    check_url,
    tamper_sql,
)

SUSPECTS = "tb_audit_tamper_suspect_ids"
STAMP = "tb_audit_tamper_check_timestamp_seconds"
BASE = {
    "TB_DATABASE_HOST": "db.example",
    "TB_DATABASE_NAME": "tb",
    "TB_DATABASE_USER": "tbapi",
    "TB_DATABASE_SECRET_REF": "pgcred",
    "TB_OIDC_ISSUER": "https://idp.example/realms/tb",
    "TB_OIDC_AUDIENCE": "tb-api",
    "TB_OIDC_ROLE_MAP": '{"tb-admin": "admin"}',
}
ARCHIVE = {
    "TB_CLICKHOUSE_URL": "https://ch.example:8443",
    "TB_CLICKHOUSE_USER": "api_ro",
    "TB_CLICKHOUSE_SECRET_REF": "chcred",
    "TB_AUDIT_TAMPER_TARGETS": '{"dev": "tb_dev", "prod": "tb_prod"}',
}


class Log:
    def __init__(self) -> None:
        self.events: list[tuple[str, str, dict[str, object]]] = []

    def _add(self, level: str, event: str, **fields: object) -> None:
        self.events.append((level, event, fields))

    def debug(self, event: str, /, **f: object) -> None:
        self._add("debug", event, **f)

    def info(self, event: str, /, **f: object) -> None:
        self._add("info", event, **f)

    def warning(self, event: str, /, **f: object) -> None:
        self._add("warning", event, **f)

    def error(self, event: str, /, **f: object) -> None:
        self._add("error", event, **f)


class FakeClickHouse:
    """Answers by database; an Exception value is raised, a str is returned."""

    def __init__(self, **answers: str | Exception) -> None:
        self.answers = answers
        self.seen: list[str] = []

    def __call__(self, sql: str) -> str:
        self.seen.append(sql)
        for db, answer in self.answers.items():
            if f"`{db}`." in sql:
                if isinstance(answer, Exception):
                    raise answer
                return answer
        raise AssertionError("unexpected database")


def make(ch: FakeClickHouse, now: list[float] | None = None) -> tuple[TamperChecker, AuditMetrics, Log]:
    metrics, log, clock = AuditMetrics(), Log(), now if now is not None else [1000.0]
    return (
        TamperChecker({"dev": "tb_dev", "prod": "tb_prod"}, ch, metrics, log, interval_s=900, clock=lambda: clock[0]),
        metrics,
        log,
    )


def test_the_query_is_the_archives_query() -> None:
    from tributary.archiver.schema import audit_tamper_sql

    assert tamper_sql(database="tb_dev") == audit_tamper_sql(database="tb_dev")


def test_series_are_absent_until_the_first_check_then_hold_the_count_and_the_time() -> None:
    async def go() -> None:
        checker, metrics, log = make(FakeClickHouse(tb_dev="0\n", tb_prod="3\n"), [1234.5])
        before = metrics.render().decode()
        assert f'{SUSPECTS}{{environment="dev"}}' not in before
        await checker.check_once()
        text = metrics.render().decode()
        assert f'{SUSPECTS}{{environment="dev"}} 0' in text
        assert f'{SUSPECTS}{{environment="prod"}} 3' in text
        assert f'{STAMP}{{environment="prod"}} 1234.5' in text
        assert [e for e in log.events if e[1] == "audit_tamper_suspected"] == [
            ("error", "audit_tamper_suspected", {"environment": "prod", "suspect_ids": 3})
        ]

    asyncio.run(go())


@pytest.mark.parametrize("failure", [TamperQueryError(), "not a number", "-1", ""])
def test_a_failed_check_changes_neither_series_and_logs_the_error_class_only(failure: Exception | str) -> None:
    async def go() -> None:
        clock = [100.0]
        ch = FakeClickHouse(tb_dev="2", tb_prod="0")
        checker, metrics, log = make(ch, clock)
        await checker.check_once()
        clock[0] = 200.0
        ch.answers = {"tb_dev": failure, "tb_prod": "5"}
        await checker.check_once()  # must not raise
        text = metrics.render().decode()
        assert f'{SUSPECTS}{{environment="dev"}} 2' in text and f'{STAMP}{{environment="dev"}} 100' in text
        assert f'{SUSPECTS}{{environment="prod"}} 5' in text and f'{STAMP}{{environment="prod"}} 200' in text
        failed = [e for e in log.events if e[1] == "audit_tamper_check_failed"]
        assert len(failed) == 1 and set(failed[0][2]) == {"environment", "error_type"}  # nothing else
        assert failed[0][2]["environment"] == "dev"

    asyncio.run(go())


def test_the_loop_survives_failures_and_stops_on_cancel() -> None:
    async def go() -> None:
        ch = FakeClickHouse(tb_dev=TamperQueryError(), tb_prod=TamperQueryError())
        metrics, log = AuditMetrics(), Log()
        checker = TamperChecker({"dev": "tb_dev"}, ch, metrics, log, interval_s=0.01)
        task = asyncio.create_task(checker.run_forever())
        while len(ch.seen) < 3:
            await asyncio.sleep(0.01)
        assert not task.done()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(go())


def test_the_gauges_are_the_contracts_and_every_api_metric_is_accounted_for() -> None:
    from pathlib import Path

    doc = json.loads(
        (Path(__file__).resolve().parents[4] / "contracts/observability/v0/metrics.json").read_text(encoding="utf-8")
    )
    api = {m["name"] for m in doc["metrics"] if "api" in m["emitters"]}
    registered = set(AuditMetrics().registry.names)
    assert registered == {SUSPECTS, STAMP} <= api
    # Registry entries the API does not export yet. A new `api` metric fails this until it is handled.
    not_yet = {
        "tb_build_info",
        "tb_ready",
        "tb_ready_check",
        "tb_api_requests_total",
        "tb_api_request_duration_seconds",
    }
    assert api == registered | not_yet


# -- the HTTP client -------------------------------------------------------------------------------


def serve(handler: type[http.server.BaseHTTPRequestHandler]) -> tuple[http.server.HTTPServer, str]:
    server = http.server.HTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, f"http://127.0.0.1:{server.server_port}"


def test_the_client_sends_one_readonly_bounded_post_with_credentials_in_headers() -> None:
    seen: dict[str, Any] = {}

    class H(http.server.BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            seen.update(path=self.path, user=self.headers["X-ClickHouse-User"], key=self.headers["X-ClickHouse-Key"])
            seen["body"] = self.rfile.read(int(self.headers["Content-Length"])).decode()
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"4\n")

        def log_message(self, *_a: Any) -> None:
            return

    server, url = serve(H)
    try:
        assert ClickHouseHttp(url, "api_ro", "pw-not-real").query("SELECT 1") == "4\n"
    finally:
        server.shutdown()
        server.server_close()
    assert "readonly=1" in seen["path"] and "max_result_rows=10" in seen["path"] and "pw-not-real" not in seen["path"]
    assert (seen["user"], seen["key"], seen["body"]) == ("api_ro", "pw-not-real", "SELECT 1")


def test_a_server_error_or_a_closed_port_is_a_bare_error_without_the_password() -> None:
    class H(http.server.BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b"Code: 62. secret detail")

        def log_message(self, *_a: Any) -> None:
            return

    server, url = serve(H)
    try:
        with pytest.raises(TamperQueryError) as e:
            ClickHouseHttp(url, "u", "pw-not-real").query("SELECT 1")
    finally:
        server.shutdown()
        server.server_close()
    assert "secret detail" not in repr(e.value) and "pw-not-real" not in repr(e.value) and e.value.__cause__ is None
    with pytest.raises(TamperQueryError):
        ClickHouseHttp(url, "u", "p", timeout_s=2).query("SELECT 1")  # nothing listens now


def test_plain_http_is_loopback_only() -> None:
    check_url("https://ch.example")
    check_url("http://127.0.0.1:8123")
    check_url("http://localhost:8123")
    for bad in ("http://ch.example", "ftp://127.0.0.1", "https://u:p@ch.example", "https://"):
        with pytest.raises(ValueError):
            check_url(bad)


# -- configuration ---------------------------------------------------------------------------------


def refused(code: str, **env: str) -> None:
    with pytest.raises(ConfigurationError) as exc:
        config.from_env({**BASE, **ARCHIVE, **env})
    assert exc.value.code == code


def test_off_by_default_and_the_documented_interval() -> None:
    assert config.from_env(BASE).archive is None
    s = config.from_env({**BASE, **ARCHIVE})
    assert s.archive is not None and s.archive.interval_s == 900.0
    assert dict(s.archive.targets) == {"dev": "tb_dev", "prod": "tb_prod"}
    assert config.from_env({**BASE, **ARCHIVE, "TB_AUDIT_TAMPER_CHECK_INTERVAL_S": "60"}).archive.interval_s == 60.0  # type: ignore[union-attr]


@pytest.mark.parametrize("value", ["59", "86401", "0", "-5", "nan"])
def test_an_interval_outside_60_to_86400_refuses_to_start(value: str) -> None:
    with pytest.raises(ConfigurationError) as exc:
        config.from_env({**BASE, **ARCHIVE, "TB_AUDIT_TAMPER_CHECK_INTERVAL_S": value})
    assert exc.value.code == "TAMPER_INTERVAL_OUT_OF_RANGE"
    with pytest.raises(ConfigurationError):  # the interval is validated even when the check is otherwise off
        config.from_env({**BASE, "TB_AUDIT_TAMPER_CHECK_INTERVAL_S": value})


def test_half_a_configuration_and_bad_values_refuse() -> None:
    for missing in ARCHIVE:
        env = {k: v for k, v in ARCHIVE.items() if k != missing}
        with pytest.raises(ConfigurationError) as exc:
            config.from_env({**BASE, **env})
        assert exc.value.code == "AUDIT_TAMPER_CONFIG_INCOMPLETE"
    for bad in ("not json", "[]", "{}", '{"Dev": "x"}', '{"dev": "x; DROP"}', '{"dev": 1}'):
        refused("AUDIT_TAMPER_TARGETS_INVALID", TB_AUDIT_TAMPER_TARGETS=bad)
    refused("CLICKHOUSE_URL_INVALID", TB_CLICKHOUSE_URL="http://ch.example:8123")


# -- credential safety (security review: redirects, proxies, deadline) --------------------------------


class Counter:
    """A TCP listener that counts connections and answers nothing."""

    def __init__(self) -> None:
        import socket

        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(5)
        self.port: int = self.sock.getsockname()[1]
        self.connections = 0
        self.sock.settimeout(0.2)

    def drain(self, seconds: float) -> None:
        import time

        end = time.monotonic() + seconds
        while time.monotonic() < end:
            try:
                self.sock.accept()[0].close()
                self.connections += 1
            except OSError:
                pass


def test_a_redirect_is_an_error_and_the_credential_never_reaches_the_target() -> None:
    hits: list[dict[str, str | None]] = []

    class Target(http.server.BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            hits.append({"key": self.headers.get("X-ClickHouse-Key")})
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"0")

        def log_message(self, *_a: Any) -> None:
            return

    target, target_url = serve(Target)

    class Front(http.server.BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            self.send_response(307)  # keeps method and body: the dangerous one
            self.send_header("Location", f"{target_url}/steal")
            self.end_headers()

        def log_message(self, *_a: Any) -> None:
            return

    front, front_url = serve(Front)
    try:
        with pytest.raises(TamperQueryError):
            ClickHouseHttp(front_url, "u", "pw-not-real").query("SELECT 1")
    finally:
        for srv in (front, target):
            srv.shutdown()
            srv.server_close()
    assert hits == []


def test_proxy_environment_variables_are_ignored(monkeypatch: pytest.MonkeyPatch) -> None:
    proxy = Counter()
    for name in ("http_proxy", "HTTP_PROXY", "https_proxy", "HTTPS_PROXY", "all_proxy", "ALL_PROXY"):
        monkeypatch.setenv(name, f"http://127.0.0.1:{proxy.port}")
    monkeypatch.delenv("no_proxy", raising=False)
    monkeypatch.delenv("NO_PROXY", raising=False)

    class H(http.server.BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"0\n")

        def log_message(self, *_a: Any) -> None:
            return

    server, url = serve(H)
    try:
        assert ClickHouseHttp(url.replace("127.0.0.1", "localhost"), "u", "pw-not-real").query("SELECT 1") == "0\n"
    finally:
        server.shutdown()
        server.server_close()
    proxy.drain(0.5)
    assert proxy.connections == 0  # `localhost` is not on a no_proxy list here: a proxy-honouring client would use it


def test_a_server_trickling_bytes_cannot_hold_a_check_past_the_overall_deadline() -> None:
    import socket
    import time

    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    stop = threading.Event()

    def trickle() -> None:
        conn, _ = srv.accept()
        conn.recv(4096)
        for byte in b"HTTP/1.1 200 OK\r\nX-Slow: " + b"a" * 200:
            if stop.is_set():
                break
            try:
                conn.sendall(bytes([byte]))
            except OSError:
                break
            time.sleep(0.3)  # always inside the per-read timeout
        conn.close()

    t = threading.Thread(target=trickle, daemon=True)
    t.start()
    started = time.monotonic()
    try:
        with pytest.raises(TamperQueryError):
            ClickHouseHttp(f"http://127.0.0.1:{srv.getsockname()[1]}", "u", "p", timeout_s=5, deadline_s=1.0).query(
                "SELECT 1"
            )
        assert time.monotonic() - started < 4.0
    finally:
        stop.set()
        srv.close()


# -- shutdown, liveness, identifiers ---------------------------------------------------------------


def test_a_failing_background_task_does_not_skip_the_rest_of_shutdown(make_harness: Any) -> None:
    class Dying:
        async def run_forever(self) -> None:
            raise RuntimeError("boom")

    h = make_harness(tamper=Dying())
    stopped: list[bool] = []
    original = h.keys.stop

    async def stop() -> None:
        stopped.append(True)
        await original()

    h.keys.stop = stop  # type: ignore[method-assign]

    async def go() -> None:
        async with h.running():
            await asyncio.sleep(0.05)  # the task has died by now

    asyncio.run(go())
    assert stopped == [True]  # keys.stop() still ran


def test_liveness_reflects_the_task_and_its_ticks() -> None:
    async def go() -> None:
        checker, _, _ = make(FakeClickHouse(tb_dev="0", tb_prod="0"))
        assert checker.alive()  # not started yet
        task = asyncio.create_task(checker.run_forever())
        await asyncio.sleep(0.05)
        assert checker.alive()
        checker._beat = checker._beat - 1000  # type: ignore[operator]  # the loop stopped ticking
        assert not checker.alive()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not checker.alive()  # the task is gone

    asyncio.run(go())


def test_a_database_name_with_a_backtick_is_refused_in_config_and_in_the_query() -> None:
    for bad in ("a`b", "a`; DROP TABLE x; --", "a b", ""):
        with pytest.raises(ValueError):
            tamper_sql(database=bad)
    refused("AUDIT_TAMPER_TARGETS_INVALID", TB_AUDIT_TAMPER_TARGETS='{"dev": "a`b"}')


def test_a_hung_check_does_not_make_a_healthy_api_look_dead() -> None:
    """The archive black-holes: the query hangs far past the staleness window (here 0.4 s standing in for the
    production 30 s, hang 2 s) and `/livez` keeps saying alive, because liveness is a separate heartbeat and the
    checker is judged only by its task still running."""
    release = threading.Event()

    def hung(_sql: str) -> str:
        release.wait(5)
        return "0"

    async def go() -> None:
        checker = TamperChecker(
            {"dev": "tb_dev"}, hung, AuditMetrics(), Log(), interval_s=900, liveness_stale_s=0.4, heartbeat_s=0.05
        )
        task = asyncio.create_task(checker.run_forever())
        try:
            for _ in range(40):  # 2 s, five staleness windows, with the check hung throughout
                await asyncio.sleep(0.05)
                assert checker.alive() and not task.done()
        finally:
            release.set()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert not checker.alive()

    asyncio.run(go())


def test_a_stalled_event_loop_is_not_alive() -> None:
    async def go() -> None:
        checker = TamperChecker(
            {"dev": "tb_dev"},
            lambda _s: "0",
            AuditMetrics(),
            Log(),
            interval_s=900,
            liveness_stale_s=0.2,
            heartbeat_s=0.05,
        )
        task = asyncio.create_task(checker.run_forever())
        await asyncio.sleep(0.1)
        assert checker.alive()
        import time

        time.sleep(0.5)  # the loop cannot run the heartbeat
        assert not checker.alive()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(go())


def test_the_whole_query_is_bounded_even_when_connect_never_completes() -> None:
    import socket
    import time

    # A listener whose backlog is full never completes the TCP handshake for later clients; fill it first.
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(0)
    fillers = []
    try:
        for _ in range(4):
            c = socket.socket()
            c.setblocking(False)
            c.connect_ex(srv.getsockname())
            fillers.append(c)
        started = time.monotonic()
        with pytest.raises(TamperQueryError):
            ClickHouseHttp(f"http://127.0.0.1:{srv.getsockname()[1]}", "u", "p", timeout_s=20, deadline_s=1.0).query(
                "SELECT 1"
            )
        assert time.monotonic() - started < 4.0  # not the 20 s per-operation connect timeout
    finally:
        for c in fillers:
            c.close()
        srv.close()


def test_a_worker_that_connects_after_the_deadline_sends_nothing_and_a_stuck_environment_is_not_retried() -> None:
    import socket
    import time

    # A listener that accepts late: the client's connect completes only once we accept (backlog full first).
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(0)
    fillers = []
    received: list[bytes] = []
    try:
        for _ in range(4):
            c = socket.socket()
            c.setblocking(False)
            c.connect_ex(srv.getsockname())
            fillers.append(c)
        client = ClickHouseHttp(
            f"http://127.0.0.1:{srv.getsockname()[1]}", "u", "pw-not-real", timeout_s=20, deadline_s=0.5
        )
        with pytest.raises(TamperQueryError):
            client.query("SELECT 1")  # the deadline passes during connect
        stuck = client._workers["SELECT 1"]
        started = time.monotonic()
        with pytest.raises(TamperQueryError):
            client.query("SELECT 1")  # same environment, worker still alive: refused at once
        assert time.monotonic() - started < 0.3 and client._workers["SELECT 1"] is stuck  # no second thread
        for c in fillers:  # free the backlog so the abandoned connect completes
            c.close()
        srv.settimeout(5)
        conn, _ = srv.accept()
        conn.settimeout(1.5)
        with contextlib.suppress(TimeoutError):
            received.append(conn.recv(4096))
        conn.close()
        stuck.join(5)
        assert not stuck.is_alive()
        assert not any(b"pw-not-real" in r or b"SELECT" in r for r in received)  # nothing was ever sent
    finally:
        for c in fillers:
            c.close()
        srv.close()
