# ruff: noqa: S603, S608
"""Content storage on real Kafka and ClickHouse (W3-2b, ADR-004 A2-A6). Synthetic data only (MSH-3 TRIBUTARY-SYNTH).

`uv run pytest -m integration packages/tributary-archiver/tests/test_integration_content.py`

Proves: the stored payload belongs to the stage that claims it (raw/out/dlq) and a header cannot change that; a
channel that is not enabled is never stored; claim-checked records keep the locator only; the content TTL is
applied by the migrate step with the decrease guard and an `archive_settings` row; content off means the process
refuses to start and nothing is stored; backfill from `raw` is idempotent (a second run changes nothing) and
honours `--since`; the runtime user holds INSERT and SELECT only.
"""

from __future__ import annotations

import json
import os
import secrets
import subprocess
import sys
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.community.clickhouse import ClickHouseContainer
from tributary.archiver.sink import ClickhouseTarget, connect
from tributary.sdk.ids import uuid7

from tests.support.engine_kafka import engine_kafka_env
from tests.support.processes import ManagedProcess

pytestmark = pytest.mark.integration

SYNTH_RAW = b"MSH|^~\\&|TRIBUTARY-SYNTH|FAC|DST|FAC|20260101000000||ADT^A01|RAW1|P|2.5\rPID|1||SYNTH-RAW\r"
SYNTH_OUT = b'{"resourceType":"Bundle","note":"TRIBUTARY-SYNTH routed"}'
SYNTH_DLQ = b"MSH|^~\\&|TRIBUTARY-SYNTH|FAC|DST|FAC|20260101000000||ADT^A01|DLQ1|P|2.5\r"
RUNTIME_REF = "ch-content"


@pytest.fixture(scope="module")
def clickhouse() -> Iterator[ClickHouseContainer]:
    with ClickHouseContainer("clickhouse/clickhouse-server:24.8").with_env(
        "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1"
    ) as container:
        yield container


def target(ch: ClickHouseContainer, database: str, user: str | None = None, pw: str | None = None) -> ClickhouseTarget:
    return ClickhouseTarget(
        host=ch.get_container_host_ip(),
        port=int(ch.get_exposed_port(8123)),
        database=database,
        username=user or ch.username,
        password=pw or ch.password,
        secure=False,
    )


def write_secret(directory: Path, ref: str, user: str, password: str) -> None:
    path = directory / ref
    path.mkdir()
    (path / "user").write_text(user)
    (path / "password").write_text(password)
    for f in path.iterdir():
        f.chmod(0o600)


class Stack:
    """A fresh Kafka env (unique topic names), ClickHouse database and the two credentials."""

    def __init__(self, kafka: str, ch: ClickHouseContainer, tmp_path: Path) -> None:
        self.kafka, self.ch = kafka, ch
        self.env = f"c{uuid.uuid4().hex[:8]}"
        self.database = f"t_{uuid.uuid4().hex[:12]}"
        self.admin = connect(target(ch, ch.dbname))
        self.admin.command(f"CREATE DATABASE {self.database}")
        self.secrets = tmp_path
        write_secret(tmp_path, "ch-migrate", ch.username, ch.password)
        self.user, pw = f"cont_{uuid.uuid4().hex[:10]}", secrets.token_hex(16)
        self.admin.command(f"CREATE USER {self.user} IDENTIFIED BY '{pw}'")
        self.admin.command(f"GRANT INSERT, SELECT ON {self.database}.* TO {self.user}")
        self.admin.command(
            f"CREATE SETTINGS PROFILE p_{self.user} SETTINGS insert_allow_materialized_columns = 0 CONST TO {self.user}"
        )
        write_secret(tmp_path, RUNTIME_REF, self.user, pw)
        self.pw = pw
        self.base = {k: v for k, v in os.environ.items() if k.startswith(("PATH", "HOME", "VIRTUAL_ENV", "PYTHON"))}
        self.base.update(
            {
                "TB_ARCHIVE_CLICKHOUSE_HOST": ch.get_container_host_ip(),
                "TB_ARCHIVE_CLICKHOUSE_PORT": str(ch.get_exposed_port(8123)),
                "TB_ARCHIVE_CLICKHOUSE_SECURE": "false",
                "TB_ARCHIVE_CLICKHOUSE_DATABASE": self.database,
                "TB_ARCHIVE_CLICKHOUSE_MIGRATE_SECRET_REF": "ch-migrate",
                "TB_ARCHIVE_CLICKHOUSE_SECRET_REF": RUNTIME_REF,
                "TB_SECRETS_DIR": str(tmp_path),
                "TB_OPS_PORT": "0",
            }
        )
        topics = [
            f"{self.env}.alpha.raw",
            f"{self.env}.alpha.out.sink",
            f"{self.env}.alpha.dlq",
            f"{self.env}.beta.raw",
        ]
        admin_kafka = AdminClient({"bootstrap.servers": kafka})
        for future in admin_kafka.create_topics([NewTopic(t, 1, 1) for t in topics]).values():
            future.result(timeout=30)
        self.extra_users: list[str] = []
        self.producer = Producer({"bootstrap.servers": kafka, "acks": "all", "enable.idempotence": True})

    def topic(self, name: str) -> str:
        return f"{self.env}.{name}"

    def send(
        self, topic: str, payload: bytes, msg_id: str, *, channel: str = "alpha", ts_ms: int | None = None, **extra: str
    ) -> None:
        headers = {"tb-msg-id": msg_id, "tb-channel": channel, "tb-content-type": "text/plain", **extra}
        kwargs: dict[str, Any] = {} if ts_ms is None else {"timestamp": ts_ms}
        self.producer.produce(
            self.topic(topic), value=payload, headers=[(k, v.encode()) for k, v in headers.items()], **kwargs
        )
        assert self.producer.flush(30) == 0

    def env_vars(self, **settings: str) -> dict[str, str]:
        return {**self.base, **settings}

    def migrate(self, **settings: str) -> subprocess.CompletedProcess[str]:
        env = self.env_vars(TB_ARCHIVE_CONTENT="true", TB_ARCHIVE_CONTENT_CHANNELS="alpha", **settings)
        return subprocess.run(
            [sys.executable, "-m", "tributary.archiver", "--env", self.env, "--migrate-only"],
            env=env, capture_output=True, text=True, timeout=120,
        )  # fmt: skip

    def content_cmd(self, *extra: str) -> list[str]:
        return [
            sys.executable, "-m", "tributary.archiver.content_archiver", "--env", self.env,
            "--instance-id", "content-1", "--flush-interval-s", "0.5", *extra,
        ]  # fmt: skip

    def content_env(self, **settings: str) -> dict[str, str]:
        base = {"TB_ARCHIVE_CONTENT": "true", "TB_ARCHIVE_CONTENT_CHANNELS": "alpha", **settings}
        return engine_kafka_env(self.kafka, self.env_vars(**base))

    def run_content(self, *extra: str, **settings: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            self.content_cmd(*extra), env=self.content_env(**settings), capture_output=True, text=True, timeout=180
        )

    def spawn_content(self) -> ManagedProcess:
        proc = ManagedProcess(self.content_cmd(), env=self.content_env())
        proc.wait_for_event("content_archiver_started", timeout_s=60.0)
        return proc

    def rows(self, sql: str) -> list[tuple[Any, ...]]:
        return list(self.admin.query(sql.replace("{t}", f"{self.database}.message_content")).result_rows)

    def wait_rows(self, at_least: int, timeout_s: float = 60.0) -> int:
        deadline = time.monotonic() + timeout_s
        count = 0
        while time.monotonic() < deadline:
            try:
                count = int(self.rows("SELECT count() FROM {t} FINAL")[0][0])
            except Exception:
                count = 0
            if count >= at_least:
                return count
            time.sleep(0.5)
        return count

    def wait_committed(self, topics: list[str], timeout_s: float = 60.0) -> None:
        """Until the content archiver's committed offsets reach each topic's high watermark (so every record was
        consumed, inserted and committed). Asks as the same group without joining it."""
        from confluent_kafka import Consumer, TopicPartition

        probe = Consumer({"bootstrap.servers": self.kafka, "group.id": f"tb.{self.env}.archiver.content"})
        try:
            deadline = time.monotonic() + timeout_s
            while time.monotonic() < deadline:
                done = True
                for name in topics:
                    topic = self.topic(name)
                    _, high = probe.get_watermark_offsets(TopicPartition(topic, 0), timeout=10)
                    (committed,) = probe.committed([TopicPartition(topic, 0)], timeout=10)
                    done = done and committed.offset >= high
                if done:
                    return
                time.sleep(0.5)
            raise AssertionError("the content archiver did not commit up to the high watermark in time")
        finally:
            probe.close()

    def limited_user(self, extra_grant: str) -> str:
        """A runtime secretRef for a user that holds INSERT, SELECT on the database plus `extra_grant`."""
        user, pw = f"x_{uuid.uuid4().hex[:10]}", secrets.token_hex(16)
        self.admin.command(f"CREATE USER {user} IDENTIFIED BY '{pw}'")
        self.admin.command(f"GRANT INSERT, SELECT ON {self.database}.* TO {user}")
        self.admin.command(f"GRANT {extra_grant} TO {user}")
        self.admin.command(
            f"CREATE SETTINGS PROFILE p_{user} SETTINGS insert_allow_materialized_columns = 0 CONST TO {user}"
        )
        write_secret(self.secrets, f"ref-{user}".replace("_", "-"), user, pw)
        self.extra_users.append(user)
        return f"ref-{user}".replace("_", "-")

    def close(self) -> None:
        for extra in self.extra_users:
            self.admin.command(f"DROP USER IF EXISTS {extra}")
            self.admin.command(f"DROP SETTINGS PROFILE IF EXISTS p_{extra}")
        self.admin.command(f"DROP DATABASE IF EXISTS {self.database}")
        self.admin.command(f"DROP USER IF EXISTS {self.user}")
        self.admin.command(f"DROP SETTINGS PROFILE IF EXISTS p_{self.user}")
        self.admin.close()


@pytest.fixture
def stack(kafka_bootstrap: str, clickhouse: ClickHouseContainer, tmp_path: Path) -> Iterator[Stack]:
    s = Stack(kafka_bootstrap, clickhouse, tmp_path)
    try:
        yield s
    finally:
        s.close()


def refusal(result: subprocess.CompletedProcess[str]) -> dict[str, Any]:
    lines = [json.loads(x) for x in result.stderr.splitlines() if x.startswith("{")]
    return next(x for x in lines if x.get("event") == "startup_refused")


def test_stored_content_matches_the_stage_it_claims_and_only_enabled_channels_are_stored(stack: Stack) -> None:
    assert stack.migrate().returncode == 0
    msg = uuid7()
    stack.send(
        "alpha.raw", SYNTH_RAW, msg, **{"tb-stage": "dlq", "tb-usr-note": "SYNTH-NOTE"}
    )  # a header cannot move it
    stack.send("alpha.out.sink", SYNTH_OUT, msg, **{"tb-destination": "sink", "tb-stage": "routed"})
    stack.send("alpha.dlq", SYNTH_DLQ, msg, **{"tb-destination": "sink", "tb-error-code": "DEST_FAILED"})
    claimed = uuid7()
    stack.send("alpha.raw", b"", claimed, **{"tb-claim-check": "s3://synth-bucket/objects/claimed-1"})
    stack.send("beta.raw", SYNTH_RAW, uuid7(), channel="beta")  # not enabled: never stored
    proc = stack.spawn_content()
    try:
        assert stack.wait_rows(4) == 4
        stack.wait_committed(["alpha.raw", "alpha.out.sink", "alpha.dlq"])  # everything enabled was processed
        found = {
            (r[0], r[1]): r
            for r in stack.rows(
                "SELECT stage, destination, payload, error_code, claim_ref, size_bytes, channel, topic "
                "FROM {t} FINAL WHERE message_id = '" + msg + "'"
            )
        }
        assert set(found) == {("received", ""), ("routed", "sink"), ("dlq", "sink")}
        assert found[("received", "")][2] == SYNTH_RAW.decode()
        assert found[("routed", "sink")][2] == SYNTH_OUT.decode()
        assert found[("dlq", "sink")][2] == SYNTH_DLQ.decode() and found[("dlq", "sink")][3] == "DEST_FAILED"
        for (stage, _), r in found.items():
            assert r[7].endswith({"received": ".raw", "routed": ".out.sink", "dlq": ".dlq"}[stage])
        (ref,) = stack.rows("SELECT claim_ref, payload, size_bytes FROM {t} FINAL WHERE message_id = '" + claimed + "'")
        assert ref == ("s3://synth-bucket/objects/claimed-1", "", 0)
        assert stack.rows("SELECT count() FROM {t} WHERE channel = 'beta'") == [(0,)]
        assert stack.rows("SELECT count() FROM {t} WHERE payload LIKE '%SYNTH-NOTE%'") == [(0,)]
    finally:
        proc.terminate_and_wait()


def test_redelivery_collapses_on_message_id_and_stage(stack: Stack) -> None:
    assert stack.migrate().returncode == 0
    msg = uuid7()
    stack.send("alpha.raw", SYNTH_RAW, msg)
    first = stack.spawn_content()
    try:
        assert stack.wait_rows(1) == 1
    finally:
        first.terminate_and_wait()
    # A re-read of the same record (new consumer group state is not needed: backfill reads it again).
    done = stack.run_content("--backfill", "alpha")
    assert done.returncode == 0, done.stderr[-300:]
    assert stack.rows("SELECT count() FROM {t} FINAL") == [(1,)]


def test_content_ttl_is_applied_guarded_and_recorded(stack: Stack) -> None:
    assert stack.migrate(TB_ARCHIVE_CONTENT_TTL_DAYS="14").returncode == 0
    shown = str(stack.admin.command(f"SHOW CREATE TABLE {stack.database}.message_content"))
    assert "toIntervalDay(14)" in shown and "ReplacingMergeTree(ts)" in shown and "ttl_only_drop_parts = 1" in shown
    refused = stack.migrate(TB_ARCHIVE_CONTENT_TTL_DAYS="7")
    assert refused.returncode == 2 and refusal(refused)["code"] == "RETENTION_DECREASE_REFUSED"
    assert "toIntervalDay(14)" in str(stack.admin.command(f"SHOW CREATE TABLE {stack.database}.message_content"))
    allowed = stack.migrate(TB_ARCHIVE_CONTENT_TTL_DAYS="7", TB_ARCHIVE_ALLOW_TTL_DECREASE="true")
    assert allowed.returncode == 0
    assert "toIntervalDay(7)" in str(stack.admin.command(f"SHOW CREATE TABLE {stack.database}.message_content"))
    history = stack.admin.query(
        f"SELECT days, previous_days FROM {stack.database}.archive_settings "
        "WHERE table_name = 'message_content' ORDER BY applied_at"
    ).result_rows
    assert history == [(14, None), (7, 14)]


def test_content_off_creates_no_table_and_the_process_refuses_to_start(stack: Stack) -> None:
    plain = subprocess.run(
        [sys.executable, "-m", "tributary.archiver", "--env", stack.env, "--migrate-only"],
        env=stack.env_vars(), capture_output=True, text=True, timeout=120,
    )  # fmt: skip
    assert plain.returncode == 0
    assert stack.admin.query(
        "SELECT count() FROM system.tables WHERE database = {d:String} AND name = 'message_content'",
        parameters={"d": stack.database},
    ).result_rows == [(0,)]
    stack.send("alpha.raw", SYNTH_RAW, uuid7())
    off = stack.run_content(TB_ARCHIVE_CONTENT="false")
    assert off.returncode == 2 and refusal(off)["code"] == "CONTENT_DISABLED"
    no_channels = stack.run_content(TB_ARCHIVE_CONTENT_CHANNELS="")
    assert no_channels.returncode == 2 and refusal(no_channels)["code"] == "CONTENT_CHANNELS_EMPTY"
    no_table = stack.run_content()
    assert no_table.returncode == 2 and refusal(no_table)["code"] == "CONTENT_SCHEMA_MISSING"
    assert stack.admin.query(
        "SELECT count() FROM system.tables WHERE database = {d:String} AND name = 'message_content'",
        parameters={"d": stack.database},
    ).result_rows == [(0,)], "the content archiver never runs DDL"


def test_backfill_from_raw_is_idempotent_and_honours_since(stack: Stack) -> None:
    assert stack.migrate().returncode == 0
    now_ms = int(datetime.now(UTC).timestamp() * 1000)
    old_ms = int((datetime.now(UTC) - timedelta(days=2)).timestamp() * 1000)
    old_ids, new_ids = [uuid7() for _ in range(2)], [uuid7() for _ in range(3)]
    for i in old_ids:
        stack.send("alpha.raw", SYNTH_RAW, i, ts_ms=old_ms)
    for i in new_ids:
        stack.send("alpha.raw", SYNTH_RAW, i, ts_ms=now_ms)

    def run_backfill(*extra: str) -> dict[str, Any]:
        done = stack.run_content("--backfill", "alpha", *extra)
        assert done.returncode == 0, done.stderr[-400:]
        lines = [json.loads(x) for x in (done.stdout + done.stderr).splitlines() if x.startswith("{")]
        return next(x for x in lines if x.get("event") == "content_backfilled")

    since = (datetime.now(UTC) - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    recent = run_backfill("--since", since)
    assert (recent["scanned"], recent["stored"]) == (3, 3)
    assert stack.rows("SELECT count() FROM {t} FINAL") == [(3,)]
    full = run_backfill()
    assert (full["scanned"], full["stored"]) == (5, 5)
    assert stack.rows("SELECT count() FROM {t} FINAL") == [(5,)]
    before = stack.rows("SELECT message_id, stage, ts FROM {t} FINAL ORDER BY message_id")
    again = run_backfill()
    assert (again["scanned"], again["stored"]) == (5, 5)
    assert stack.rows("SELECT message_id, stage, ts FROM {t} FINAL ORDER BY message_id") == before, (
        "a second run changes nothing"
    )
    assert stack.rows("SELECT uniqExact(message_id, stage) FROM {t}") == [(5,)]
    refused = stack.run_content("--backfill", "beta")
    assert refused.returncode == 2 and refusal(refused)["code"] == "CONTENT_CHANNEL_NOT_ENABLED"


def test_the_runtime_user_can_insert_and_select_but_not_alter_or_delete_content(stack: Stack) -> None:
    assert stack.migrate().returncode == 0
    runtime = connect(target(stack.ch, stack.database, stack.user, stack.pw))
    try:
        assert int(runtime.command("SELECT count() FROM message_content")) == 0
        for statement in (
            "ALTER TABLE message_content MODIFY TTL toDateTime(ts) + INTERVAL 1 DAY",
            "ALTER TABLE message_content DELETE WHERE 1",
            "TRUNCATE TABLE message_content",
            "CREATE TABLE x (a UInt8) ENGINE = Memory",
        ):
            with pytest.raises(Exception):  # noqa: B017
                runtime.command(statement)
    finally:
        runtime.close()


@pytest.mark.parametrize(
    ("grant", "tables"),
    [
        ("ALTER ON {db}.message_content", {"message_content"}),
        ("TRUNCATE ON {db}.message_content", {"message_content"}),
        ("ALTER DELETE ON {db}.message_content", {"message_content"}),
        ("ALTER ON {db}.*", {"audit_log", "message_content"}),  # a wildcard reaches both
        ("ALL ON {db}.*", {"audit_log", "message_content"}),
    ],
)
def test_content_archiver_refuses_a_user_that_can_alter_or_delete_message_content(
    stack: Stack, grant: str, tables: set[str]
) -> None:
    assert stack.migrate().returncode == 0
    ref = stack.limited_user(grant.format(db=stack.database))
    result = stack.run_content(TB_ARCHIVE_CLICKHOUSE_SECRET_REF=ref)
    assert result.returncode == 2
    line = refusal(result)
    assert line["code"] == "ARCHIVER_GRANTS_EXCESSIVE" and line["table"] in tables
    assert stack.rows("SELECT count() FROM {t}") == [(0,)]


def test_content_archiver_starts_with_insert_and_select_and_states_that_custody_is_the_operators(stack: Stack) -> None:
    assert stack.migrate().returncode == 0
    proc = stack.spawn_content()
    try:
        notice = proc.wait_for_event("content_custody_notice", timeout_s=30.0)
        assert "operator" in notice["reason"] and "PHI" in notice["reason"]
    finally:
        proc.terminate_and_wait()
