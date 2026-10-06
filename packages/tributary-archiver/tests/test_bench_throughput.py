"""Archiver write throughput against a real ClickHouse: parse + batch insert, no Kafka.

`uv run pytest -m bench packages/tributary-archiver -s` prints rows/s and bytes/s. A measurement, not a gate.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime

import orjson
import pytest
from testcontainers.community.clickhouse import ClickHouseContainer
from tributary.archiver.event import parse_event
from tributary.archiver.schema import ensure_schema
from tributary.archiver.sink import ClickhouseSink, ClickhouseTarget, connect
from tributary.archiver.testing import sample_event

pytestmark = pytest.mark.bench

TOTAL = 200_000
STAGES = ("received", "queued", "transformed", "sent")


@pytest.fixture(scope="module")
def clickhouse() -> Iterator[ClickHouseContainer]:
    with ClickHouseContainer("clickhouse/clickhouse-server:24.8") as container:
        yield container


@pytest.mark.parametrize("batch", [1000, 5000, 20000])
def test_insert_throughput(clickhouse: ClickHouseContainer, batch: int) -> None:
    client = connect(
        ClickhouseTarget(
            host=clickhouse.get_container_host_ip(),
            port=int(clickhouse.get_exposed_port(8123)),
            database=clickhouse.dbname,
            username=clickhouse.username,
            password=clickhouse.password,
            secure=False,
        )
    )
    table = f"bench_{uuid.uuid4().hex[:8]}"
    ensure_schema(client, ttl_days=365)
    client.command(f"CREATE TABLE {table} AS message_events")
    sink = ClickhouseSink(client, table=table)
    now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    payloads = [
        orjson.dumps(
            sample_event(
                ts=now,
                message_id=str(uuid.uuid4()),
                stage=STAGES[i % 4],
                destination="adt-analytics",
                size_bytes=1500,
                attrs={"trigger": "A01", "step": "to_fhir"},
            )
        )
        for i in range(TOTAL)
    ]
    size = sum(len(p) for p in payloads)
    start = time.perf_counter()
    for i in range(0, TOTAL, batch):
        sink.insert([parse_event(p).values for p in payloads[i : i + batch]])
    elapsed = time.perf_counter() - start
    print(f"batch={batch}: {TOTAL / elapsed:,.0f} rows/s, {size / elapsed / 1e6:.1f} MB/s of event JSON")
    assert int(client.command(f"SELECT count() FROM {table}")) == TOTAL  # noqa: S608
    client.close()
