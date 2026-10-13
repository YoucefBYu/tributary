# ruff: noqa: S603, S608
"""The events archiver writes `_tb.<env>.audit` records to `audit_log` (W3-30 part 2; ADR-004 R7 rev 3).

`uv run pytest -m integration packages/tributary-archiver/tests/test_integration_audit.py`

Real Kafka and ClickHouse, the real archiver process as the INSERT/SELECT runtime user. Proves: a published audit
record lands in `audit_log`; a redelivery adds a physical copy but a read by `audit_id` shows it once and the
tamper count stays 0; malformed, content-carrying and wrong-environment records are skipped (counted, never
stored) and do not stall the records after them; a same-`audit_id` row with different content, written by an
INSERT-only principal, shows in the tamper count.
"""

from __future__ import annotations

import json
import os
import secrets
import subprocess
import sys
import time
import urllib.request
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.community.clickhouse import ClickHouseContainer
from tributary.archiver.schema import audit_dedup_sql, audit_tamper_sql
from tributary.archiver.sink import ClickhouseTarget, connect
from tributary.sdk.ids import uuid7

from tests.support.engine_kafka import engine_kafka_env
from tests.support.processes import ManagedProcess

pytestmark = pytest.mark.integration

EXAMPLES = Path(__file__).resolve().parents[3] / "contracts" / "audit" / "v0" / "examples" / "valid"
RUNTIME_REF = "ch-audit"


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


def secret(directory: Path, ref: str, user: str, password: str) -> None:
    path = directory / ref
    path.mkdir()
    (path / "user").write_text(user)
    (path / "password").write_text(password)
    for f in path.iterdir():
        f.chmod(0o600)


class Rig:
    def __init__(self, kafka: str, ch: ClickHouseContainer, tmp_path: Path) -> None:
        self.kafka, self.ch = kafka, ch
        self.env = f"a{uuid.uuid4().hex[:8]}"
        self.database = f"t_{uuid.uuid4().hex[:12]}"
        self.admin = connect(target(ch, ch.dbname))
        self.admin.command(f"CREATE DATABASE {self.database}")
        secret(tmp_path, "ch-migrate", ch.username, ch.password)
        self.user, self.pw = f"arch_{uuid.uuid4().hex[:10]}", secrets.token_hex(16)
        self.admin.command(f"CREATE USER {self.user} IDENTIFIED BY '{self.pw}'")
        self.admin.command(f"GRANT INSERT, SELECT ON {self.database}.* TO {self.user}")
        self.admin.command(
            f"CREATE SETTINGS PROFILE p_{self.user} SETTINGS insert_allow_materialized_columns = 0 CONST TO {self.user}"
        )
        secret(tmp_path, RUNTIME_REF, self.user, self.pw)
        base = {k: v for k, v in os.environ.items() if k.startswith(("PATH", "HOME", "VIRTUAL_ENV", "PYTHON"))}
        base.update(
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
        self.base = base
        self.audit_topic, self.events_topic = f"_tb.{self.env}.audit", f"_tb.{self.env}.events"
        admin_kafka = AdminClient({"bootstrap.servers": kafka})  # kept alive until the futures resolve
        for future in admin_kafka.create_topics(
            [NewTopic(self.audit_topic, 1, 1), NewTopic(self.events_topic, 1, 1)]
        ).values():
            future.result(timeout=30)
        self.producer = Producer({"bootstrap.servers": kafka, "acks": "all", "enable.idempotence": True})
        migrated = subprocess.run(
            [sys.executable, "-m", "tributary.archiver", "--env", self.env, "--migrate-only"],
            env=base, capture_output=True, text=True, timeout=120,
        )  # fmt: skip
        assert migrated.returncode == 0, migrated.stderr[-300:]

    def record(self, name: str = "content-view-requested.json", **over: Any) -> dict[str, Any]:
        record = json.loads((EXAMPLES / name).read_text())
        record.update({"environment": self.env, "audit_id": uuid7(), **over})
        return record  # type: ignore[no-any-return]

    def publish(self, value: bytes, key: bytes = b"synth-operator-0002") -> None:
        self.producer.produce(self.audit_topic, value=value, key=key)
        assert self.producer.flush(30) == 0

    def spawn(self) -> ManagedProcess:
        proc = ManagedProcess(
            [
                sys.executable,
                "-m",
                "tributary.archiver",
                "--env",
                self.env,
                "--instance-id",
                "audit-1",
                "--flush-interval-s",
                "0.5",
                "--skip-ddl",
            ],
            env=engine_kafka_env(self.kafka, self.base),
        )
        proc.wait_for_event("archiver_started", timeout_s=60.0)
        return proc

    def wait_committed(self, timeout_s: float = 60.0) -> None:
        probe = Consumer({"bootstrap.servers": self.kafka, "group.id": f"tb.{self.env}.archiver"})
        try:
            deadline = time.monotonic() + timeout_s
            while time.monotonic() < deadline:
                _, high = probe.get_watermark_offsets(TopicPartition(self.audit_topic, 0), timeout=10)
                (committed,) = probe.committed([TopicPartition(self.audit_topic, 0)], timeout=10)
                if committed.offset >= high:
                    return
                time.sleep(0.5)
            raise AssertionError("the archiver did not commit the audit topic in time")
        finally:
            probe.close()

    def rows(self, sql: str) -> list[tuple[Any, ...]]:
        return list(self.admin.query(sql).result_rows)

    def close(self) -> None:
        self.admin.command(f"DROP DATABASE IF EXISTS {self.database}")
        self.admin.command(f"DROP USER IF EXISTS {self.user}")
        self.admin.command(f"DROP SETTINGS PROFILE IF EXISTS p_{self.user}")
        self.admin.close()


@pytest.fixture
def rig(kafka_bootstrap: str, clickhouse: ClickHouseContainer, tmp_path: Path) -> Iterator[Rig]:
    r = Rig(kafka_bootstrap, clickhouse, tmp_path)
    try:
        yield r
    finally:
        r.close()


def scrape(proc: ManagedProcess) -> str:
    port = next(int(x["ops_port"]) for x in proc.lines if x.get("event") == "ops_listening")
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as response:
        return response.read().decode()  # type: ignore[no-any-return]


def test_a_published_record_lands_once_a_redelivery_does_not_duplicate_it_and_poison_is_skipped(rig: Rig) -> None:
    first = rig.record()
    first_bytes = json.dumps(first).encode()
    rig.publish(first_bytes)
    rig.publish(first_bytes)  # the producer's retry of the same send: same bytes, same audit_id
    rig.publish(b"{this is not json")
    rig.publish(json.dumps({**rig.record(), "payload": "SYNTH-CONTENT-MUST-NOT-LAND"}).encode())  # carries content
    rig.publish(json.dumps(rig.record(environment="other")).encode())  # another environment's record
    second = rig.record("reprocess-requested.json")
    rig.publish(json.dumps(second).encode())
    proc = rig.spawn()
    try:
        rig.wait_committed()
        a = f"toUUID('{first['audit_id']}')"
        # Landed: every contract field of the first record is in the row.
        (row,) = rig.rows(
            f"SELECT action, phase, actor, actor_roles, channel, message_id, stage, destination, environment "
            f"FROM {audit_dedup_sql(database=rig.database)} WHERE audit_id = {a}"
        )
        assert row == (
            "message.content.view", "requested", first["actor"], first["actor_roles"], first["channel"],
            uuid.UUID(first["message_id"]), first["stage"], first["destination"], rig.env,
        )  # fmt: skip
        # Redelivery: a second physical copy (append-only), one logical record, no tamper signal.
        assert rig.rows(f"SELECT count() FROM {rig.database}.audit_log WHERE audit_id = {a}") == [(2,)]
        assert rig.rows(f"SELECT count() FROM {audit_dedup_sql(database=rig.database)} WHERE audit_id = {a}") == [(1,)]
        assert rig.rows(audit_tamper_sql(database=rig.database)) == [(0,)]
        # Poison skipped and counted; the record after it landed; nothing of the rejected ones is stored.
        total = rig.rows(f"SELECT count(), uniqExact(audit_id) FROM {rig.database}.audit_log")
        assert total == [(3, 2)], "first (x2) + second; the three rejected records were not stored"
        assert rig.rows(f"SELECT count() FROM {rig.database}.audit_log WHERE environment != '{rig.env}'") == [(0,)]
        assert 'tb_archiver_events_total{outcome="invalid"} 3' in scrape(proc)
        (src,) = rig.rows(
            f"SELECT source_partition, source_offset FROM {rig.database}.audit_log WHERE audit_id = toUUID('{second['audit_id']}')"
        )
        assert src == (0, 5), "provenance: the second valid record sat at offset 5 of the audit topic"
        stamp = rig.rows(f"SELECT inserted_at FROM {rig.database}.audit_log LIMIT 1")[0][0]
        assert stamp.year >= datetime.now(UTC).year, "inserted_at is the server's time"
    finally:
        proc.terminate_and_wait()


def test_the_runtime_user_cannot_overwrite_a_row_and_a_forged_same_id_copy_raises_the_tamper_count(rig: Rig) -> None:
    original = rig.record()
    rig.publish(json.dumps(original).encode())
    proc = rig.spawn()
    try:
        rig.wait_committed()
    finally:
        proc.terminate_and_wait()
    runtime = connect(target(rig.ch, rig.database, rig.user, rig.pw))
    try:
        assert rig.rows(audit_tamper_sql(database=rig.database)) == [(0,)]
        # An INSERT-only principal re-inserts the same audit_id with different content.
        runtime.command(
            f"INSERT INTO audit_log (audit_id, ts, environment, action, phase, actor, actor_roles, source_partition, source_offset) "
            f"SELECT audit_id, ts, environment, action, phase, 'forged-actor', actor_roles, 0, 99 "
            f"FROM audit_log WHERE audit_id = toUUID('{original['audit_id']}') LIMIT 1"
        )
        assert rig.rows(audit_tamper_sql(database=rig.database)) == [(1,)]
        (shown,) = rig.rows(
            f"SELECT actor FROM {audit_dedup_sql(database=rig.database)} WHERE audit_id = toUUID('{original['audit_id']}')"
        )
        assert shown == (original["actor"],), "the earliest-archived copy wins the read"
        with pytest.raises(Exception):  # noqa: B017
            runtime.command("ALTER TABLE audit_log UPDATE actor = 'x' WHERE 1")
    finally:
        runtime.close()
