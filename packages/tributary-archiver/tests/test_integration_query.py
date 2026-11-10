"""The query library against a real ClickHouse (own container): dedup at read, filters, keyset paging, volume.

`uv run pytest -m integration packages/tributary-archiver/tests/test_integration_query.py`

All data is synthetic. Events are written through the archiver's own parse and insert path.
"""

from __future__ import annotations

import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import orjson
import pytest
from tributary.archiver.event import parse_event
from tributary.archiver.query import ArchiveQueries, SearchFilter
from tributary.archiver.schema import ensure_schema
from tributary.archiver.sink import ClickhouseSink, ClickhouseTarget, connect
from tributary.archiver.testing import sample_event
from tributary.sdk.ids import uuid7

pytestmark = pytest.mark.integration


def stamp(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def fresh(ch: Any) -> tuple[Any, ClickhouseSink, ArchiveQueries]:
    def target(database: str) -> ClickhouseTarget:
        return ClickhouseTarget(
            host=ch.get_container_host_ip(),
            port=int(ch.get_exposed_port(8123)),
            database=database,
            username=ch.username,
            password=ch.password,
            secure=False,
        )

    name = f"q_{uuid.uuid4().hex[:12]}"
    admin = connect(target(ch.dbname))
    admin.command(f"CREATE DATABASE {name}")
    admin.close()
    client = connect(target(name))
    ensure_schema(client, database=name, ttl_days=3650)
    return client, ClickhouseSink(client), ArchiveQueries(client, database=name)


def put(sink: ClickhouseSink, events: list[dict[str, Any]]) -> None:
    sink.insert([parse_event(orjson.dumps(e)).values for e in events])


def ev(ts: datetime, mid: str, stage: str = "received", **over: Any) -> dict[str, Any]:
    return sample_event(ts=stamp(ts), message_id=mid, stage=stage, channel=over.pop("channel", "adt"), **over)


NOW = datetime.now(UTC).replace(microsecond=0)


def test_timeline_is_ordered_deduplicated_and_crosses_a_month_boundary(ch_container: Any) -> None:
    client, sink, queries = fresh(ch_container)
    first_of_month = NOW.replace(day=1, hour=0, minute=0, second=0)
    t0 = first_of_month - timedelta(seconds=90)  # previous month
    mid = uuid7(unix_ts_ms=int(t0.timestamp() * 1000))
    events = [
        ev(t0, mid, "received"),
        ev(t0 + timedelta(seconds=10), mid, "queued"),
        ev(t0 + timedelta(seconds=20), mid, "sent", destination="d-one"),
        ev(t0 + timedelta(seconds=21), mid, "sent", destination="d-two", status="error", error_code="HTTP_503"),
        ev(t0 + timedelta(seconds=22), mid, "sent", destination="d-two", attempt=1),
        # the same `queued` event re-emitted after a worker restart, landing in the NEXT month partition
        ev(first_of_month + timedelta(seconds=5), mid, "queued"),
        ev(t0, str(uuid.uuid4()), "received"),  # an unrelated message
    ]
    put(sink, events)
    plain = int(client.command("SELECT count() FROM message_events WHERE message_id = {m:UUID}", parameters={"m": mid}))
    final = int(
        client.command("SELECT count() FROM message_events FINAL WHERE message_id = {m:UUID}", parameters={"m": mid})
    )
    assert plain == 6
    # FINAL (read-time collapse) merges across partitions by default, so it counts 5; a background merge does not.
    assert final == 5
    client.command("OPTIMIZE TABLE message_events FINAL")
    merged = int(
        client.command("SELECT count() FROM message_events WHERE message_id = {m:UUID}", parameters={"m": mid})
    )
    assert merged == 6, "a physical merge cannot collapse a re-emit that crossed a month partition (README 4.1)"

    rows = queries.timeline(mid)
    assert [(r.stage, r.destination, r.attempt) for r in rows] == [
        ("received", "", 0),
        ("queued", "", 0),
        ("sent", "d-one", 0),
        ("sent", "d-two", 0),
        ("sent", "d-two", 1),
    ]
    queued = rows[1]
    assert queued.ts == first_of_month + timedelta(seconds=5), "the newest copy is kept"
    assert [r.stage for r in queries.timeline(mid, channel="adt")] == [r.stage for r in rows]
    assert queries.timeline(mid, channel="other") == ()
    assert queries.timeline(uuid.uuid4(), channel="adt") == ()


def test_search_filters_each_dimension(ch_container: Any) -> None:
    _, sink, queries = fresh(ch_container)
    base = NOW - timedelta(hours=1)
    ids = [str(uuid.uuid4()) for _ in range(6)]
    put(
        sink,
        [
            ev(base, ids[0], "received"),
            ev(base + timedelta(minutes=1), ids[1], "sent", destination="d-one"),
            ev(base + timedelta(minutes=2), ids[2], "sent", destination="d-two", status="error", error_code="HTTP_503"),
            ev(base + timedelta(minutes=3), ids[3], "dlq", destination="d-two", status="error", error_code="HTTP_400"),
            ev(base + timedelta(minutes=4), ids[4], "received", attrs={"trigger": "A01"}),
            ev(base + timedelta(minutes=5), ids[5], "received", channel="oru"),
            # a retried copy of ids[2]'s event: same key, later ts, now OK. Dedup keeps it, and the status
            # filter applies AFTER dedup, so the stale `error` copy must not surface.
            ev(base + timedelta(minutes=6), ids[2], "sent", destination="d-two", status="ok"),
        ],
    )

    def found(**kw: Any) -> set[str]:
        flt = SearchFilter(channel=kw.pop("channel", "adt"), since=base - timedelta(minutes=1), **kw)
        return {str(r.message_id) for r in queries.search(flt).items}

    assert found() == set(ids[:5])
    assert found(stages=["sent"]) == {ids[1], ids[2]}
    assert found(destination="d-two") == {ids[2], ids[3]}
    assert found(status="error") == {ids[3]}, "stale pre-dedup copy of ids[2] must not match"
    assert found(status="ok", stages=["sent"]) == {ids[1], ids[2]}
    assert found(error_code="HTTP_400") == {ids[3]}
    assert found(error_code="HTTP_503") == set()
    assert found(attrs={"trigger": "A01"}) == {ids[4]}
    assert found(channel="oru") == {ids[5]}
    window = SearchFilter(channel="adt", since=base + timedelta(minutes=3), until=base + timedelta(minutes=5))
    assert {str(r.message_id) for r in queries.search(window).items} == {ids[3], ids[4]}


def test_keyset_pagination_is_complete_stable_and_tie_safe(ch_container: Any) -> None:
    _, sink, queries = fresh(ch_container)
    base = NOW - timedelta(hours=2)
    # 260 messages, 4 per second so ts ties are common, plus duplicates of every 10th event.
    events = []
    expected: set[str] = set()
    for i in range(260):
        mid = str(uuid.uuid4())
        expected.add(mid)
        events.append(ev(base + timedelta(seconds=i // 4), mid, "received"))
        if i % 10 == 0:
            events.append(ev(base + timedelta(seconds=i // 4, milliseconds=500), mid, "received"))
    put(sink, events)
    flt = SearchFilter(channel="adt", since=base - timedelta(minutes=1))

    seen: list[tuple[datetime, str]] = []
    cursor = None
    pages = 0
    while True:
        page = queries.search(flt, limit=37, cursor=cursor)
        pages += 1
        seen.extend((r.ts, str(r.message_id)) for r in page.items)
        if pages == 1:  # a newer event arriving between pages must not shift or repeat anything
            put(sink, [ev(NOW, str(uuid.uuid4()), "received")])
        cursor = page.next_cursor
        if cursor is None:
            break
    assert pages == 8  # 260 / 37, rounded up
    ids = [m for _, m in seen]
    assert len(ids) == len(set(ids)) == 260 and set(ids) == expected
    assert [t for t, _ in seen] == sorted((t for t, _ in seen), reverse=True)


def test_search_at_volume_reports_latency(ch_container: Any) -> None:
    client, sink, queries = fresh(ch_container)
    total, per_channel_messages = 300_000, 100_000
    base = NOW - timedelta(hours=3)
    stages = ("received", "queued", "transformed")
    start = time.perf_counter()
    batch: list[dict[str, Any]] = []
    for i in range(total):
        channel = f"chan-{i % 3}"
        batch.append(
            ev(
                base + timedelta(milliseconds=i * 30),
                str(uuid.UUID(int=(i // 3) * 3 + (i % 3) + 1)),
                stages[(i // 3) % 3],
                channel=channel,
                destination="",
            )
        )
        if len(batch) == 20_000:
            put(sink, batch)
            batch = []
    loaded = time.perf_counter() - start
    assert int(client.command("SELECT count() FROM message_events")) == total

    flt = SearchFilter(channel="chan-1", since=base - timedelta(minutes=1), stages=["queued"])
    t = time.perf_counter()
    first = queries.search(flt, limit=100)
    first_s = time.perf_counter() - t
    assert len(first.items) == 100 and first.next_cursor is not None
    t = time.perf_counter()
    deep = first
    for _ in range(9):
        assert deep.next_cursor
        deep = queries.search(flt, limit=100, cursor=deep.next_cursor)
    deep_s = (time.perf_counter() - t) / 9
    t = time.perf_counter()
    queries.timeline(uuid.UUID(int=4), channel="chan-0")
    timeline_s = time.perf_counter() - t
    print(
        f"\nquery volume: {total:,} rows ({per_channel_messages:,} per channel) loaded in {loaded:.1f}s; "
        f"search first page {first_s * 1000:.0f} ms, pages 2-10 avg {deep_s * 1000:.0f} ms, "
        f"timeline {timeline_s * 1000:.0f} ms"
    )
    assert first_s < 10 and deep_s < 10
