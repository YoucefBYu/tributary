"""The tamper check against a real ClickHouse: the real `audit_log` DDL (the archiver's), one forged duplicate,
one honest redelivery, and the API's own stdlib HTTP client with `readonly=1`."""

from __future__ import annotations

import asyncio
import base64
import urllib.request
import uuid
from collections.abc import Iterator

import pytest
from tributary.api.tamper import AuditMetrics, ClickHouseHttp, TamperChecker

pytestmark = pytest.mark.integration


class Admin:
    def __init__(self, url: str, user: str, password: str) -> None:
        self.url, self._auth = url, base64.b64encode(f"{user}:{password}".encode()).decode()

    def run(self, sql: str) -> str:
        req = urllib.request.Request(  # noqa: S310
            self.url, data=sql.encode(), method="POST", headers={"Authorization": f"Basic {self._auth}"}
        )
        with urllib.request.urlopen(req, timeout=60) as r:  # noqa: S310
            return str(r.read().decode())


@pytest.fixture(scope="module")
def clickhouse() -> Iterator[tuple[str, str, str]]:
    from testcontainers.community.clickhouse import ClickHouseContainer

    with ClickHouseContainer("clickhouse/clickhouse-server:24.8") as c:
        url = f"http://{c.get_container_host_ip()}:{c.get_exposed_port(8123)}"
        yield url, c.username, c.password


def row(audit_id: uuid.UUID, count: int, offset: int) -> str:
    return (
        f"('{audit_id}', '2026-10-06 10:00:00.000', 'dev', 'channel.deploy', 'requested', 'actor-1', ['admin'], "
        f"{count}, 0, {offset})"
    )


def test_a_forged_duplicate_is_counted_and_an_honest_redelivery_is_not(clickhouse: tuple[str, str, str]) -> None:
    from tributary.archiver.schema import audit_ddl

    url, user, password = clickhouse
    admin, db = Admin(url, user, password), f"t_{uuid.uuid4().hex[:10]}"
    admin.run(f"CREATE DATABASE {db}")
    admin.run(audit_ddl(database=db))
    forged, honest = uuid.uuid4(), uuid.uuid4()
    admin.run(
        f"INSERT INTO {db}.audit_log (audit_id, ts, environment, action, phase, actor, actor_roles, `count`, "  # noqa: S608
        f"source_partition, source_offset) VALUES {row(forged, 1, 1)}, {row(forged, 999, 2)}, "
        f"{row(honest, 5, 1)}, {row(honest, 5, 2)}"
    )

    async def go() -> AuditMetrics:
        metrics = AuditMetrics()

        class Log:
            def __getattr__(self, _n: str):  # type: ignore[no-untyped-def]
                return lambda *a, **k: None

        checker = TamperChecker(
            {"dev": db},
            ClickHouseHttp(url, user, password).query,
            metrics,
            Log(),
            interval_s=900,  # type: ignore[arg-type]
        )
        await checker.check_once()
        return metrics

    text = asyncio.run(go()).render().decode()
    assert 'tb_audit_tamper_suspect_ids{environment="dev"} 1' in text
    assert 'tb_audit_tamper_check_timestamp_seconds{environment="dev"}' in text


def test_the_client_cannot_write(clickhouse: tuple[str, str, str]) -> None:
    from tributary.api.tamper import TamperQueryError

    url, user, password = clickhouse
    with pytest.raises(TamperQueryError):  # readonly=1 refuses DDL and INSERT
        ClickHouseHttp(url, user, password).query("CREATE TABLE should_not_exist (x UInt8) ENGINE = Memory")
