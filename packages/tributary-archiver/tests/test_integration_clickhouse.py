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
import socket
import struct
import subprocess
import sys
import threading
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


def test_ensure_schema_is_rerunnable_and_applies_a_changed_ttl(db: tuple[object, ClickhouseSink, str]) -> None:
    client, sink, name = db
    sink.insert(rows(base()))
    ensure_schema(client, database=name, ttl_days=90)
    assert count(client, final=True) == 1
    ensure_schema(client, database=name, ttl_days=30)
    assert "toIntervalDay(30)" in str(client.command("SHOW CREATE TABLE message_events"))  # type: ignore[attr-defined]
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
    admin.command(f"CREATE USER IF NOT EXISTS ins_{name} IDENTIFIED BY 'synth-ins-pw'")
    admin.command(f"GRANT INSERT ON {name}.message_events TO ins_{name}")
    runtime = connect(
        ClickhouseTarget(
            host=clickhouse.get_container_host_ip(),
            port=int(clickhouse.get_exposed_port(8123)),
            database=name,
            username=f"ins_{name}",
            password="synth-ins-pw",  # noqa: S106
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
