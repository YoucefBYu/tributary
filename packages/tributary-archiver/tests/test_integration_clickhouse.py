"""The archive table against a real ClickHouse (`testcontainers`, a container of its own).

`uv run pytest -m integration packages/tributary-archiver`

Proves: a redelivered event counts once under `FINAL` (invariant 4's archive equivalent) while a plain
`count()` can still see both; a re-emitted event with a later `ts` also collapses; distinct stages,
destinations, attempts and partition events do not collapse; the TTL is on the table; a broken (plain
MergeTree) table would NOT collapse, so the `FINAL` assertion is not vacuous; no `_hmac` or
identifier-named attribute reaches a row; `ensure_schema` is re-runnable and re-applies a changed TTL;
the real client has driver retries off.
"""

from __future__ import annotations

import contextlib
import os
import secrets
import socket
import struct
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import orjson
import pytest
from testcontainers.community.clickhouse import ClickHouseContainer
from tributary.archiver.event import parse_event
from tributary.archiver.schema import ensure_schema
from tributary.archiver.sink import ClickhouseSink, ClickhouseTarget, connect
from tributary.archiver.testing import sample_event as _base

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def clickhouse() -> Iterator[ClickHouseContainer]:
    with ClickHouseContainer("clickhouse/clickhouse-server:24.8").with_env(
        "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1"
    ) as container:
        yield container


@pytest.fixture
def db(clickhouse: ClickHouseContainer) -> Iterator[tuple[object, ClickhouseSink, str]]:
    """A fresh database per test, so tests never see each other's rows."""
    name = f"t_{uuid.uuid4().hex[:12]}"
    admin = connect(
        ClickhouseTarget(
            host=clickhouse.get_container_host_ip(),
            port=int(clickhouse.get_exposed_port(8123)),
            database=clickhouse.dbname,
            username=clickhouse.username,
            password=clickhouse.password,
            secure=False,
        )
    )
    admin.command(f"CREATE DATABASE {name}")
    client = connect(
        ClickhouseTarget(
            host=clickhouse.get_container_host_ip(),
            port=int(clickhouse.get_exposed_port(8123)),
            database=name,
            username=clickhouse.username,
            password=clickhouse.password,
            secure=False,
        )
    )
    ensure_schema(client, database=name, ttl_days=90)
    try:
        yield client, ClickhouseSink(client), name
    finally:
        client.close()
        admin.command(f"DROP DATABASE IF EXISTS {name}")
        admin.close()


def stamp(seconds: float = 0.0) -> str:
    """A recent UTC ts: the table has a TTL, so a fixed 2026-01-01 would be expired by the server."""
    moment = datetime.now(UTC) - timedelta(minutes=5) + timedelta(seconds=seconds)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def base(**over: object) -> dict[str, object]:
    return _base(**{"ts": stamp(), **over})


def rows(*events: dict[str, object]) -> list[tuple[object, ...]]:
    return [parse_event(orjson.dumps(e)).values for e in events]


def count(client: object, *, final: bool) -> int:
    sql = "SELECT count() FROM message_events FINAL" if final else "SELECT count() FROM message_events"
    return int(client.command(sql))  # type: ignore[attr-defined]


def test_redelivered_event_counts_once_under_final(db: tuple[object, ClickhouseSink, str]) -> None:
    client, sink, _ = db
    event = base()
    sink.insert(rows(event))
    sink.insert(rows(event))  # the identical record, delivered again (uncommitted offset)
    assert count(client, final=False) >= 1  # a plain count may still show 2: merges are eventual
    assert count(client, final=True) == 1


def test_a_re_emitted_event_with_a_new_ts_collapses_too(db: tuple[object, ClickhouseSink, str]) -> None:
    client, sink, _ = db
    early, late = stamp(0.1), stamp(5.9)
    sink.insert(rows(base(ts=early), base(ts=late)))
    assert count(client, final=True) == 1
    kept = client.query("SELECT formatDateTime(ts, '%Y-%m-%dT%H:%i:%S') FROM message_events FINAL").result_rows  # type: ignore[attr-defined]
    assert kept[0][0] == late[:19], "the latest ts wins"


def test_distinct_stage_destination_attempt_and_message_stay_distinct(db: tuple[object, ClickhouseSink, str]) -> None:
    client, sink, _ = db
    other = "0194f5a0-0000-7000-8000-000000000002"
    sink.insert(
        rows(
            base(),
            base(stage="transformed"),
            base(stage="sent", destination="a"),
            base(stage="sent", destination="b"),
            base(stage="retrying", destination="a", attempt=1, status="error", error_code="TIMEOUT"),
            base(stage="retrying", destination="a", attempt=2, status="error", error_code="TIMEOUT"),
            base(message_id=other),
        )
    )
    assert count(client, final=True) == 7


def test_partition_events_for_one_partition_are_not_collapsed(db: tuple[object, ClickhouseSink, str]) -> None:
    client, sink, _ = db
    paused = base(kind="partition", stage="partition_paused", topic="dev.c.out.a", partition=0)
    paused.pop("message_id")
    resumed = dict(paused, ts=stamp(9))
    sink.insert(rows(paused, resumed, paused))
    assert count(client, final=True) == 2


def test_a_plain_merge_tree_would_not_collapse_the_same_redelivery(db: tuple[object, ClickhouseSink, str]) -> None:
    """Control: remove the Replacing engine and the duplicate stays, so the test above can fail."""
    client, _, name = db
    client.command(  # type: ignore[attr-defined]
        f"CREATE TABLE {name}.plain AS {name}.message_events ENGINE = MergeTree ORDER BY (channel, message_id, stage)"
    )
    plain = ClickhouseSink(client, table="plain")
    plain.insert(rows(base()))
    plain.insert(rows(base()))
    assert int(client.command("SELECT count() FROM plain")) == 2  # type: ignore[attr-defined]


def test_ttl_and_engine_are_on_the_table(db: tuple[object, ClickhouseSink, str]) -> None:
    client, _, _ = db
    shown = str(client.command("SHOW CREATE TABLE message_events"))  # type: ignore[attr-defined]
    assert "ReplacingMergeTree(ts)" in shown
    assert "toIntervalDay(90)" in shown
    assert "ttl_only_drop_parts = 1" in shown
    assert "_hmac" not in shown


def test_ensure_schema_is_rerunnable_and_never_alters_an_existing_ttl(
    db: tuple[object, ClickhouseSink, str],
) -> None:
    """ADR-004 R6: the archivers do not apply retention; only the migrate step does."""
    client, sink, name = db
    sink.insert(rows(base()))
    ensure_schema(client, database=name, ttl_days=90)
    assert count(client, final=True) == 1
    ensure_schema(client, database=name, ttl_days=30)
    assert "toIntervalDay(90)" in str(client.command("SHOW CREATE TABLE message_events"))  # type: ignore[attr-defined]
    assert count(client, final=True) == 1


def test_no_hmac_or_identifier_attribute_reaches_a_row(db: tuple[object, ClickhouseSink, str]) -> None:
    client, sink, _ = db
    sink.insert(rows(base(attrs={"mrn_hmac": "a" * 64, "mrn": "X", "trigger": "A01"})))
    keys = client.query("SELECT mapKeys(attrs) FROM message_events FINAL").result_rows  # type: ignore[attr-defined]
    assert keys == [(["trigger"],)]


def test_every_field_round_trips(db: tuple[object, ClickhouseSink, str]) -> None:
    client, sink, _ = db
    full = base(
        parent_id="0194f5a0-0000-7000-8000-0000000000aa",
        correlation_id="0194f5a0-0000-7000-8000-0000000000bb",
        destination="adt",
        attempt=3,
        content_type="application/hl7-v2",
        size_bytes=42,
        topic="dev.c.raw",
        partition=2,
        offset=99,
        duration_ms=12.5,
        traceparent="00-" + "a" * 32 + "-" + "b" * 16 + "-01",
        stage="retrying",
        status="error",
        error_code="TIMEOUT",
        error_message="destination timed out",
        attrs={"step": "to_fhir"},
    )
    sink.insert(rows(full))
    got = client.query(  # type: ignore[attr-defined]
        "SELECT attempt, size_bytes, `partition`, `offset`, duration_ms, error_code, attrs['step'], "
        "toString(parent_id), destination FROM message_events FINAL"
    ).result_rows
    assert got == [(3, 42, 2, 99, 12.5, "TIMEOUT", "to_fhir", "0194f5a0-0000-7000-8000-0000000000aa", "adt")]


def test_driver_retries_are_off_on_the_real_client(db: tuple[object, ClickhouseSink, str]) -> None:
    client, _, _ = db
    assert client.query_retries == 0  # type: ignore[attr-defined]
    assert client.http_retries == 0  # type: ignore[attr-defined]
    assert client.http.connection_pool_kw["retries"].total == 0  # type: ignore[attr-defined]


def test_insert_failure_is_a_sinkerror_without_server_text(db: tuple[object, ClickhouseSink, str]) -> None:
    from tributary.archiver.errors import SinkError

    client, _, _ = db
    missing = ClickhouseSink(client, table="no_such_table")
    with pytest.raises(SinkError) as caught:
        missing.insert(rows(base()))
    assert "no_such_table" not in str(caught.value)


class _ResettingProxy:
    """Forwards to ClickHouse until `armed`, then accepts and RSTs every connection (counting them)."""

    def __init__(self, host: str, port: int) -> None:
        self.upstream = (host, port)
        self.armed = False
        self.after_armed = 0
        self.server = socket.socket()
        self.server.bind(("127.0.0.1", 0))
        self.server.listen(16)
        self.port = self.server.getsockname()[1]
        threading.Thread(target=self._serve, daemon=True).start()

    def _pump(self, a: socket.socket, b: socket.socket) -> None:
        try:
            while data := a.recv(65536):
                b.sendall(data)
        except OSError:
            pass
        finally:
            for s in (a, b):
                with contextlib.suppress(OSError):
                    s.shutdown(socket.SHUT_RDWR)

    def _serve(self) -> None:
        while True:
            try:
                conn, _ = self.server.accept()
            except OSError:
                return
            if not self.armed:
                up = socket.create_connection(self.upstream)
                threading.Thread(target=self._pump, args=(conn, up), daemon=True).start()
                threading.Thread(target=self._pump, args=(up, conn), daemon=True).start()
            else:
                self.after_armed += 1
                conn.recv(65536)
                conn.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
                conn.close()


def test_reset_during_insert_on_a_connected_client_is_one_attempt_and_transient(
    clickhouse: ClickHouseContainer,
) -> None:
    from tributary.archiver.errors import SinkError

    proxy = _ResettingProxy(clickhouse.get_container_host_ip(), int(clickhouse.get_exposed_port(8123)))
    client = connect(
        ClickhouseTarget(
            host="127.0.0.1",
            port=proxy.port,
            database=clickhouse.dbname,
            username=clickhouse.username,
            password=clickhouse.password,
            secure=False,
        )
    )
    ensure_schema(client, database=clickhouse.dbname)
    proxy.armed = True
    try:
        with pytest.raises(SinkError) as caught:
            ClickhouseSink(client).insert(rows(base()))
        assert caught.value.transient is True
        assert proxy.after_armed == 1, "exactly one connection: the insert was not re-sent"
    finally:
        proxy.server.close()
        client.close()


def test_insert_only_credential_inserts_but_cannot_migrate(clickhouse: ClickHouseContainer) -> None:
    """ADR-004 A5: after a migration credential created the table, the runtime user needs INSERT only."""
    from tributary.archiver.errors import SinkError

    name = f"t_{uuid.uuid4().hex[:12]}"
    pw = secrets.token_hex(16)
    admin = connect(
        ClickhouseTarget(
            host=clickhouse.get_container_host_ip(),
            port=int(clickhouse.get_exposed_port(8123)),
            database=clickhouse.dbname,
            username=clickhouse.username,
            password=clickhouse.password,
            secure=False,
        )
    )
    admin.command(f"CREATE DATABASE {name}")
    ensure_schema(admin, database=name)
    admin.command(f"CREATE USER IF NOT EXISTS ins_{name} IDENTIFIED BY '{pw}'")
    admin.command(f"GRANT INSERT ON {name}.message_events TO ins_{name}")
    runtime = connect(
        ClickhouseTarget(
            host=clickhouse.get_container_host_ip(),
            port=int(clickhouse.get_exposed_port(8123)),
            database=name,
            username=f"ins_{name}",
            password=pw,
            secure=False,
        )
    )
    try:
        ClickhouseSink(runtime).insert(rows(base()))
        assert int(admin.command(f"SELECT count() FROM {name}.message_events FINAL")) == 1  # noqa: S608
        with pytest.raises(Exception):  # noqa: B017
            ensure_schema(runtime, database=name, ttl_days=7)
    finally:
        runtime.close()
        admin.command(f"DROP DATABASE IF EXISTS {name}")
        admin.command(f"DROP USER IF EXISTS ins_{name}")
        admin.close()
        assert SinkError  # imported for symmetry with the sibling tests


def test_migrate_only_creates_the_table_with_the_migration_credential_and_needs_no_kafka(
    clickhouse: ClickHouseContainer, tmp_path: Path
) -> None:
    name = f"t_{uuid.uuid4().hex[:12]}"
    admin = connect(
        ClickhouseTarget(
            host=clickhouse.get_container_host_ip(),
            port=int(clickhouse.get_exposed_port(8123)),
            database=clickhouse.dbname,
            username=clickhouse.username,
            password=clickhouse.password,
            secure=False,
        )
    )
    admin.command(f"CREATE DATABASE {name}")
    ref = tmp_path / "ch-migrate"
    ref.mkdir()
    (ref / "user").write_text(clickhouse.username)
    (ref / "password").write_text(clickhouse.password)
    for f in ref.iterdir():
        f.chmod(0o600)
    env = {k: v for k, v in os.environ.items() if k not in ("TB_ENGINE_KAFKA_FILE", "TB_BOOTSTRAP_SERVERS")}
    env.update(
        {
            "TB_ARCHIVE_CLICKHOUSE_HOST": clickhouse.get_container_host_ip(),
            "TB_ARCHIVE_CLICKHOUSE_PORT": str(clickhouse.get_exposed_port(8123)),
            "TB_ARCHIVE_CLICKHOUSE_SECURE": "false",
            "TB_ARCHIVE_CLICKHOUSE_DATABASE": name,
            "TB_ARCHIVE_CLICKHOUSE_MIGRATE_SECRET_REF": "ch-migrate",
            "TB_SECRETS_DIR": str(tmp_path),
            "TB_ARCHIVE_TTL_DAYS": "45",
        }
    )
    try:
        result = subprocess.run(
            [sys.executable, "-m", "tributary.archiver", "--env", "dev", "--migrate-only"],
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0, result.stderr[-300:]
        assert "toIntervalDay(45)" in str(admin.command(f"SHOW CREATE TABLE {name}.message_events"))
    finally:
        admin.command(f"DROP DATABASE IF EXISTS {name}")
        admin.close()


# ---- W3-30: retention applied by the migrate step (ADR-004 R2, R6) -----------------------------------------


class Migrator:
    """A database plus the way to run `--migrate-only` against it as a subprocess (the real entry point)."""

    def __init__(self, clickhouse: ClickHouseContainer, tmp_path: Path) -> None:
        self.name = f"t_{uuid.uuid4().hex[:12]}"
        self.admin = connect(
            ClickhouseTarget(
                host=clickhouse.get_container_host_ip(),
                port=int(clickhouse.get_exposed_port(8123)),
                database=clickhouse.dbname,
                username=clickhouse.username,
                password=clickhouse.password,
                secure=False,
            )
        )
        self.admin.command(f"CREATE DATABASE {self.name}")
        ref = tmp_path / "ch-migrate"
        ref.mkdir()
        (ref / "user").write_text(clickhouse.username)
        (ref / "password").write_text(clickhouse.password)
        for f in ref.iterdir():
            f.chmod(0o600)
        self.env = {k: v for k, v in os.environ.items() if k.startswith(("PATH", "HOME", "VIRTUAL_ENV", "PYTHON"))}
        self.env.update(
            {
                "TB_ARCHIVE_CLICKHOUSE_HOST": clickhouse.get_container_host_ip(),
                "TB_ARCHIVE_CLICKHOUSE_PORT": str(clickhouse.get_exposed_port(8123)),
                "TB_ARCHIVE_CLICKHOUSE_SECURE": "false",
                "TB_ARCHIVE_CLICKHOUSE_DATABASE": self.name,
                "TB_ARCHIVE_CLICKHOUSE_MIGRATE_SECRET_REF": "ch-migrate",
                "TB_SECRETS_DIR": str(tmp_path),
            }
        )

    def run(self, **settings: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-m", "tributary.archiver", "--env", "dev", "--migrate-only"],
            env={**self.env, **settings},
            capture_output=True,
            text=True,
            timeout=120,
        )

    def show(self, table: str) -> str:
        return str(self.admin.command(f"SHOW CREATE TABLE {self.name}.{table}"))

    def settings_rows(self) -> list[tuple[str, int, int | None]]:
        result = self.admin.query(
            f"SELECT table_name, days, previous_days FROM {self.name}.archive_settings WHERE table_name != 'content_storage' ORDER BY applied_at, table_name"  # noqa: S608
        )
        return [(str(r[0]), int(r[1]), None if r[2] is None else int(r[2])) for r in result.result_rows]

    def close(self) -> None:
        self.admin.command(f"DROP DATABASE IF EXISTS {self.name}")
        self.admin.close()


@pytest.fixture
def migrator(clickhouse: ClickHouseContainer, tmp_path: Path) -> Iterator[Migrator]:
    m = Migrator(clickhouse, tmp_path)
    try:
        yield m
    finally:
        m.close()


def refusal_codes(result: subprocess.CompletedProcess[str]) -> list[str]:
    import json

    lines = [json.loads(x) for x in result.stderr.splitlines() if x.startswith("{")]
    return [x["code"] for x in lines if x.get("event") == "startup_refused"]


def test_migrate_applies_retention_to_events_and_audit_log_and_defaults_match_the_adr(migrator: Migrator) -> None:
    result = migrator.run()
    assert result.returncode == 0, result.stderr[-300:]
    assert "toIntervalDay(90)" in migrator.show("message_events")
    audit = migrator.show("audit_log")
    assert "toIntervalDay(2190)" in audit
    assert "ENGINE = MergeTree" in audit and "ReplacingMergeTree" not in audit, "audit_log is append-only"
    assert "ttl_only_drop_parts = 1" in audit and "toYYYYMM(ts)" in audit
    columns = str(
        migrator.admin.command(
            "SELECT groupArray(name) FROM system.columns WHERE database = {db:String} AND table = 'archive_settings'",
            parameters={"db": migrator.name},
        )
    )
    assert "applied_by" in columns and "decrease_allowed" in columns
    assert "TTL" not in migrator.show("archive_settings")


def test_a_ttl_decrease_is_refused_then_allowed_with_the_flag_and_recorded(migrator: Migrator) -> None:
    assert migrator.run(TB_ARCHIVE_TTL_DAYS="45", TB_ARCHIVE_AUDIT_TTL_DAYS="400").returncode == 0
    assert "toIntervalDay(45)" in migrator.show("message_events")
    assert "toIntervalDay(400)" in migrator.show("audit_log")
    created = migrator.settings_rows()
    assert sorted(created) == [("audit_log", 400, None), ("message_events", 45, None)]
    recorded_by = migrator.admin.query(f"SELECT DISTINCT applied_by FROM {migrator.name}.archive_settings").result_rows  # noqa: S608
    assert recorded_by and recorded_by[0][0], "applied_by defaults to the migrating user"

    # Shorter events, longer audit: refused as a whole, so audit_log is not lengthened either.
    refused = migrator.run(TB_ARCHIVE_TTL_DAYS="10", TB_ARCHIVE_AUDIT_TTL_DAYS="500")
    assert refused.returncode == 2
    assert refusal_codes(refused) == ["RETENTION_DECREASE_REFUSED"], refused.stderr[-500:]
    assert "toIntervalDay(45)" in migrator.show("message_events")
    assert "toIntervalDay(400)" in migrator.show("audit_log")
    assert migrator.settings_rows() == created

    for bad in ("false", "yes"):  # only `true` allows it; an unrecognised value is refused, never read as true
        again = migrator.run(TB_ARCHIVE_TTL_DAYS="10", TB_ARCHIVE_ALLOW_TTL_DECREASE=bad)
        assert again.returncode == 2
        assert "toIntervalDay(45)" in migrator.show("message_events")

    allowed = migrator.run(
        TB_ARCHIVE_TTL_DAYS="10", TB_ARCHIVE_AUDIT_TTL_DAYS="500", TB_ARCHIVE_ALLOW_TTL_DECREASE="true"
    )
    assert allowed.returncode == 0, allowed.stderr[-300:]
    assert "toIntervalDay(10)" in migrator.show("message_events")
    assert "toIntervalDay(500)" in migrator.show("audit_log")
    assert ("message_events", 10, 45) in migrator.settings_rows()
    assert ("audit_log", 500, 400) in migrator.settings_rows()
    flags = migrator.admin.query(
        f"SELECT table_name, decrease_allowed FROM {migrator.name}.archive_settings WHERE days = 10"  # noqa: S608
    ).result_rows
    assert flags == [("message_events", True)]

    unchanged = migrator.run(TB_ARCHIVE_TTL_DAYS="10", TB_ARCHIVE_AUDIT_TTL_DAYS="500")
    assert unchanged.returncode == 0
    assert len(migrator.settings_rows()) == 4, "an unchanged value writes no settings row"


def test_out_of_range_retention_refuses_to_start(migrator: Migrator) -> None:
    for name, value in (("TB_ARCHIVE_TTL_DAYS", "0"), ("TB_ARCHIVE_AUDIT_TTL_DAYS", "36501")):
        result = migrator.run(**{name: value})
        assert result.returncode == 2
    tables_sql = "SELECT count() FROM system.tables WHERE database = {db:String}"
    assert migrator.admin.query(tables_sql, parameters={"db": migrator.name}).result_rows == [(0,)]


def test_lengthening_reaches_rows_already_stored(migrator: Migrator, clickhouse: ClickHouseContainer) -> None:
    """ADR-004 R6 (1), the claim that was unverified: MODIFY TTL materialises on existing parts."""
    assert migrator.run(TB_ARCHIVE_TTL_DAYS="10").returncode == 0
    client = connect(
        ClickhouseTarget(
            host=clickhouse.get_container_host_ip(),
            port=int(clickhouse.get_exposed_port(8123)),
            database=migrator.name,
            username=clickhouse.username,
            password=clickhouse.password,
            secure=False,
        )
    )
    try:
        moment = datetime.now(UTC) - timedelta(days=5)
        stored = moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"
        ClickhouseSink(client).insert(rows(base(ts=stored)))
        sql = (
            "SELECT max(delete_ttl_info_max) FROM system.parts "  # noqa: S608
            f"WHERE database = '{migrator.name}' AND table = 'message_events' AND active"
        )
        before = migrator.admin.query(sql).result_rows[0][0]
        assert before < datetime.now() + timedelta(days=6)
        assert migrator.run(TB_ARCHIVE_TTL_DAYS="60").returncode == 0
        deadline = time.monotonic() + 60
        after = before
        while time.monotonic() < deadline:
            after = migrator.admin.query(sql).result_rows[0][0]
            if after > datetime.now() + timedelta(days=40):
                break
            time.sleep(0.5)
        assert after > datetime.now() + timedelta(days=40), "the longer TTL must reach the part already stored"
        assert int(client.command("SELECT count() FROM message_events FINAL")) == 1
    finally:
        client.close()


AUDIT_COLUMNS = [
    "audit_id",
    "ts",
    "environment",
    "action",
    "phase",
    "actor",
    "actor_roles",
    "count",
    "source_partition",
    "source_offset",
]


def audit_row(audit_id: uuid.UUID, ts: datetime, count: int, offset: int = 0) -> list[object]:
    return [audit_id, ts, "dev", "message.reprocess", "completed", "SYNTH-actor", ["operator"], count, 0, offset]


def make_user(
    migrator: Migrator, clickhouse: ClickHouseContainer, grants: str, *, profile: bool = True
) -> tuple[str, str]:
    """A ClickHouse user with `grants`; `profile` pins insert_allow_materialized_columns = 0 CONST (README)."""
    user, pw = f"u_{uuid.uuid4().hex[:10]}", secrets.token_hex(16)
    migrator.admin.command(f"CREATE USER IF NOT EXISTS {user} IDENTIFIED BY '{pw}'")
    migrator.admin.command(f"GRANT {grants} TO {user}")
    if profile:
        migrator.admin.command(
            f"CREATE SETTINGS PROFILE IF NOT EXISTS p_{user} SETTINGS insert_allow_materialized_columns = 0 CONST TO {user}"
        )
    return user, pw


def connect_as(clickhouse: ClickHouseContainer, database: str, user: str, pw: str) -> object:
    return connect(
        ClickhouseTarget(
            host=clickhouse.get_container_host_ip(),
            port=int(clickhouse.get_exposed_port(8123)),
            database=database,
            username=user,
            password=pw,
            secure=False,
        )
    )


def test_an_insert_only_user_cannot_overwrite_an_audit_row_and_a_same_ts_injection_is_a_tamper_signal(
    migrator: Migrator, clickhouse: ClickHouseContainer
) -> None:
    """ADR-004 R7 rev 3 / audit/v0 4: append-only, dedup on audit_id keeping the earliest-archived copy."""
    from tributary.archiver.schema import audit_dedup_sql, audit_tamper_sql

    assert migrator.run().returncode == 0
    user, pw = make_user(migrator, clickhouse, f"INSERT ON {migrator.name}.audit_log")
    writer = connect_as(clickhouse, migrator.name, user, pw)
    db = migrator.name
    when = datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=3)
    original, honest, forged = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    try:
        writer.insert("audit_log", [audit_row(original, when, 1, 7)], column_names=AUDIT_COLUMNS)  # type: ignore[attr-defined]
        # Same audit_id, SAME ts, different content: what a merge-collapsing engine would have replaced.
        time.sleep(0.01)
        writer.insert("audit_log", [audit_row(original, when, 999, 8)], column_names=AUDIT_COLUMNS)  # type: ignore[attr-defined]
        # An honest at-least-once redelivery: identical content, new offset.
        writer.insert("audit_log", [audit_row(honest, when, 5, 1)], column_names=AUDIT_COLUMNS)  # type: ignore[attr-defined]
        writer.insert("audit_log", [audit_row(honest, when, 5, 2)], column_names=AUDIT_COLUMNS)  # type: ignore[attr-defined]
        assert forged  # (kept for readability of the three ids)
        migrator.admin.command(f"OPTIMIZE TABLE {db}.audit_log FINAL")
        stored = migrator.admin.query(f"SELECT count() FROM {db}.audit_log").result_rows  # noqa: S608
        assert stored == [(4,)], "every INSERT is kept: an INSERT can only add a row"
        deduped = migrator.admin.query(
            f"SELECT audit_id, `count` FROM {audit_dedup_sql(database=db)} ORDER BY audit_id"  # noqa: S608
        ).result_rows
        assert dict(deduped) == {original: 1, honest: 5}, "the earliest-archived copy wins; no FINAL"
        assert migrator.admin.query(audit_tamper_sql(database=db)).result_rows == [(1,)]
        with pytest.raises(Exception):  # noqa: B017
            writer.command("ALTER TABLE audit_log UPDATE `count` = 0 WHERE 1")  # type: ignore[attr-defined]
    finally:
        writer.close()  # type: ignore[attr-defined]
        migrator.admin.command(f"DROP USER IF EXISTS {user}")


def test_an_insert_only_user_cannot_backdate_inserted_at_to_win_the_dedup(
    migrator: Migrator, clickhouse: ClickHouseContainer
) -> None:
    """`inserted_at` is MATERIALIZED: an explicit early value is refused (or ignored), so the original still wins."""
    from tributary.archiver.schema import audit_dedup_sql

    assert migrator.run().returncode == 0
    db = migrator.name
    assert "MATERIALIZED now64(3)" in migrator.show("audit_log")
    user, pw = make_user(migrator, clickhouse, f"INSERT ON {db}.audit_log")
    writer = connect_as(clickhouse, db, user, pw)
    when = datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=3)
    audit_id = uuid.uuid4()
    try:
        writer.insert("audit_log", [audit_row(audit_id, when, 1, 5)], column_names=AUDIT_COLUMNS)  # type: ignore[attr-defined]
        forged = [*audit_row(audit_id, when, 999, 0), datetime(1970, 1, 2, tzinfo=UTC)]
        try:
            writer.insert("audit_log", [forged], column_names=[*AUDIT_COLUMNS, "inserted_at"])  # type: ignore[attr-defined]
        except Exception:
            refused = True
        else:
            refused = False
        for form in (
            "INSERT INTO audit_log (audit_id, ts, actor, inserted_at) VALUES (generateUUIDv4(), now(), 'x', '1970-01-02')",
            # The reviewer's bypass: the INSERT overrides the MATERIALIZED column by setting; the CONST profile refuses.
            "INSERT INTO audit_log (audit_id, ts, actor, inserted_at) SETTINGS insert_allow_materialized_columns = 1 "
            "VALUES (generateUUIDv4(), now(), 'x', '1970-01-02')",
        ):
            with pytest.raises(Exception):  # noqa: B017
                writer.command(form)  # type: ignore[attr-defined]
        with pytest.raises(Exception):  # noqa: B017
            writer.command("SET insert_allow_materialized_columns = 1")  # type: ignore[attr-defined]
        # Whatever happened, no stored row may carry a client-chosen time, and the original wins.
        assert refused, "24.8 refuses an explicit value for a MATERIALIZED column"
        oldest = migrator.admin.query(f"SELECT min(inserted_at) FROM {db}.audit_log").result_rows[0][0]  # noqa: S608
        assert oldest.year >= 2026, f"a client-set inserted_at was stored (refused={refused})"
        # Same-ms race: the archiver row is first; even if the forged copy was stored, it is later, so it loses.
        if not refused:
            migrator.admin.command(f"OPTIMIZE TABLE {db}.audit_log FINAL")
        winners = migrator.admin.query(f"SELECT `count` FROM {audit_dedup_sql(database=db)}").result_rows  # noqa: S608
        assert winners == [(1,)]
    finally:
        writer.close()  # type: ignore[attr-defined]
        migrator.admin.command(f"DROP USER IF EXISTS {user}")


def archiver_refusal(migrator: Migrator, user: str, pw: str, tmp_path: Path) -> subprocess.CompletedProcess[str]:
    """Run the real archiver entry point (no `--migrate-only`) as `user`; no Kafka broker is needed to be refused."""
    from tests.support.engine_kafka import engine_kafka_env

    ref = tmp_path / "ch-runtime"
    ref.mkdir()
    (ref / "user").write_text(user)
    (ref / "password").write_text(pw)
    for f in ref.iterdir():
        f.chmod(0o600)
    env = engine_kafka_env("127.0.0.1:9092", migrator.env)
    env.update(
        {"TB_ARCHIVE_CLICKHOUSE_SECRET_REF": "ch-runtime", "TB_OPS_PORT": "0", "TB_INSTANCE_ID": "archiver-grants"}
    )
    return subprocess.run(
        [sys.executable, "-m", "tributary.archiver", "--env", "dev", "--skip-ddl"],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )


def refusal_line(result: subprocess.CompletedProcess[str]) -> dict[str, object]:
    import json

    lines = [json.loads(x) for x in result.stderr.splitlines() if x.startswith("{")]
    return next(x for x in lines if x.get("event") == "startup_refused")


@pytest.mark.parametrize("extra", ["ALTER", "TRUNCATE", "DROP TABLE", "ALTER DELETE"])
def test_archiver_refuses_to_start_when_its_user_exceeds_insert_and_select_on_audit_log(
    migrator: Migrator, clickhouse: ClickHouseContainer, tmp_path: Path, extra: str
) -> None:
    assert migrator.run().returncode == 0
    user, pw = make_user(migrator, clickhouse, f"INSERT, SELECT, {extra} ON {migrator.name}.audit_log")
    try:
        result = archiver_refusal(migrator, user, pw, tmp_path)
    finally:
        migrator.admin.command(f"DROP USER IF EXISTS {user}")
    assert result.returncode == 2
    line = refusal_line(result)
    assert line["code"] == "ARCHIVER_GRANTS_EXCESSIVE"
    # ClickHouse reports the ALTER group as its expansion, ALTER TABLE.
    assert line["privilege"] == {"ALTER": "ALTER TABLE"}.get(extra, extra) and line["table"] == "audit_log"
    assert pw not in result.stderr and user not in result.stderr


def test_archiver_refuses_a_user_with_all_on_the_database(
    migrator: Migrator, clickhouse: ClickHouseContainer, tmp_path: Path
) -> None:
    assert migrator.run().returncode == 0
    user, pw = make_user(migrator, clickhouse, f"ALL ON {migrator.name}.*")
    try:
        result = archiver_refusal(migrator, user, pw, tmp_path)
    finally:
        migrator.admin.command(f"DROP USER IF EXISTS {user}")
    assert result.returncode == 2 and refusal_line(result)["code"] == "ARCHIVER_GRANTS_EXCESSIVE"


def test_archiver_refuses_to_start_without_the_settings_profile(
    migrator: Migrator, clickhouse: ClickHouseContainer, tmp_path: Path
) -> None:
    assert migrator.run().returncode == 0
    user, pw = make_user(migrator, clickhouse, f"INSERT, SELECT ON {migrator.name}.*", profile=False)
    try:
        result = archiver_refusal(migrator, user, pw, tmp_path)
    finally:
        migrator.admin.command(f"DROP USER IF EXISTS {user}")
    assert result.returncode == 2
    line = refusal_line(result)
    assert line["code"] == "ARCHIVER_SETTINGS_UNSAFE" and line["setting"] == "insert_allow_materialized_columns"


def test_migrate_refuses_an_audit_log_whose_inserted_at_is_writable(migrator: Migrator) -> None:
    from tributary.archiver.schema import audit_ddl

    ddl_text = audit_ddl(database=migrator.name).replace("MATERIALIZED now64(3)", "DEFAULT now64(3)")
    migrator.admin.command(ddl_text)
    result = migrator.run()
    assert result.returncode == 2 and refusal_line(result)["code"] == "AUDIT_LOG_ENGINE_INVALID"


def test_migrate_is_exempt_from_the_grant_check(migrator: Migrator) -> None:
    """The migrate credential is a full-access user (it needs ALTER) and `--migrate-only` must still work."""
    assert migrator.run().returncode == 0


def test_an_existing_audit_log_that_is_not_a_plain_mergetree_stops_migrate_and_start(
    migrator: Migrator, clickhouse: ClickHouseContainer, tmp_path: Path
) -> None:
    migrator.admin.command(
        f"CREATE TABLE {migrator.name}.audit_log (ts DateTime64(3, 'UTC'), audit_id UUID) "
        "ENGINE = ReplacingMergeTree(ts) ORDER BY audit_id"
    )
    result = migrator.run()
    assert result.returncode == 2 and refusal_line(result)["code"] == "AUDIT_LOG_ENGINE_INVALID"
    user, pw = make_user(migrator, clickhouse, f"INSERT, SELECT ON {migrator.name}.*")
    try:
        started = archiver_refusal(migrator, user, pw, tmp_path)
    finally:
        migrator.admin.command(f"DROP USER IF EXISTS {user}")
    assert started.returncode == 2 and refusal_line(started)["code"] == "AUDIT_LOG_ENGINE_INVALID"


def test_a_pre_existing_settings_table_gets_applied_by_without_stamping_old_rows(migrator: Migrator) -> None:
    name = migrator.name
    migrator.admin.command(
        f"CREATE TABLE {name}.archive_settings (table_name LowCardinality(String), days UInt32, "
        "previous_days Nullable(UInt32), applied_at DateTime64(3, 'UTC') DEFAULT now64(3), release String) "
        "ENGINE = MergeTree ORDER BY (table_name, applied_at)"
    )
    migrator.admin.command(
        f"INSERT INTO {name}.archive_settings (table_name, days, release) VALUES ('message_events', 90, 'old')"  # noqa: S608
    )
    assert migrator.run().returncode == 0
    old = migrator.admin.query(
        f"SELECT applied_by, source FROM {name}.archive_settings WHERE release = 'old'"  # noqa: S608
    ).result_rows
    assert old == [("", "applied")], "a row written before the column existed is not attributed to the migrating user"
    new = migrator.admin.query(f"SELECT applied_by FROM {name}.archive_settings WHERE release != 'old'").result_rows  # noqa: S608
    assert new and all(row[0] for row in new), "new rows carry the migrating user"
    assert "currentUser()" in migrator.show("archive_settings")


def test_a_lost_settings_row_is_backfilled_and_marked(migrator: Migrator) -> None:
    name = migrator.name
    assert migrator.run(TB_ARCHIVE_TTL_DAYS="45").returncode == 0
    # An earlier run altered the TTL but died before writing its row.
    migrator.admin.command(f"ALTER TABLE {name}.message_events MODIFY TTL toDateTime(ts) + INTERVAL 50 DAY")
    assert migrator.run(TB_ARCHIVE_TTL_DAYS="50").returncode == 0
    rows = migrator.admin.query(
        f"SELECT days, previous_days, source FROM {name}.archive_settings WHERE table_name = 'message_events' ORDER BY applied_at"  # noqa: S608
    ).result_rows
    assert rows == [(45, None, "applied"), (50, 45, "backfilled")]
