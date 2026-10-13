"""Review hardening for the archive endpoints (W3-6): audit bridge timeout, replay slot accounting, stream slots
and caps, rate-limited failure logs, the cap on abandoned ClickHouse workers, and readonly forced on the client."""

from __future__ import annotations

import asyncio
import json
import socket
import threading
import time
from types import SimpleNamespace
from typing import Any

import pytest
from api_kit import OPENAPI, TEST_ACTOR
from tributary.api import reingest
from tributary.api.chclient import ArchiveClient
from tributary.api.pools import Pools
from tributary.api.problems import ApiProblem
from tributary.api.stream import EventStreamer, SlotStreamingResponse, StreamHub
from tributary.api.tamper import ClickHouseHttp, TamperQueryError
from tributary.archiver.audit import AuditEvent
from tributary.sdk.ids import uuid7


class Log:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, object]]] = []

    def __getattr__(self, level: str) -> Any:
        return lambda event, /, **f: self.events.append((event, f))


def test_an_audit_record_that_takes_too_long_fails_closed_as_audit_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    """`future.result(60)` raised a bare TimeoutError that surfaced as INTERNAL: now the action does not run."""
    monkeypatch.setattr(reingest, "AUDIT_WAIT_S", 0.3)

    class Stuck:
        async def requested(self, **_k: Any) -> str:
            await asyncio.sleep(30)
            return "x"

    async def go() -> None:
        loop = asyncio.get_running_loop()
        bridge = reingest.LibraryAudit(loop, Stuck(), "dev", TEST_ACTOR)  # type: ignore[arg-type]
        event = AuditEvent("message.reprocess", "requested", "tester", "adt", str(uuid7()), request_id=uuid7())
        with pytest.raises(ApiProblem) as e:
            await loop.run_in_executor(None, bridge, event)
        assert (e.value.code, e.value.status) == ("AUDIT_UNAVAILABLE", 503)

    asyncio.run(go())


def make_reingest(store: Any, max_replays: int = 1) -> reingest.Reingest:
    control = SimpleNamespace(deployed_ordering=lambda env, ch: _done("none"))
    reader = SimpleNamespace(database=lambda env: "db")
    return reingest.Reingest(
        reader,
        control,
        None,
        store,
        None,
        None,
        Log(),
        pools=Pools(),
        max_replays=max_replays,  # type: ignore[arg-type]
    )


async def _done(value: Any) -> Any:
    return value


REQUEST = SimpleNamespace(
    replay_id=uuid7(), start="2026-10-01T00:00:00.000Z", end="2026-10-01T01:00:00.000Z", allow_reorder=False,
    max_records=10, deadline_s=60,
)  # fmt: skip


def test_the_replay_slot_is_taken_before_anything_is_stored_and_given_back_on_every_other_path() -> None:
    class Store:
        mode = "started"

        async def begin(self, *a: Any) -> tuple[dict[str, Any], bool]:
            if self.mode == "boom":
                raise ApiProblem("REQUEST_ID_REUSED")
            return {"status": "running"}, self.mode == "started"

    store, release = Store(), asyncio.Event()

    async def go() -> None:
        engine = make_reingest(store)

        async def parked(*_a: Any) -> None:
            await release.wait()
            engine._active -= 1

        engine._run_replay = parked  # type: ignore[method-assign]
        store.mode = "boom"
        with pytest.raises(ApiProblem):
            await engine.start_replay("dev", "adt", REQUEST, TEST_ACTOR)
        assert engine._active == 0  # a begin that failed gave its slot back
        store.mode = "existing"
        assert (await engine.start_replay("dev", "adt", REQUEST, TEST_ACTOR))[1] == 200
        assert engine._active == 0  # an identical replay already there takes no slot
        store.mode = "started"
        assert (await engine.start_replay("dev", "adt", REQUEST, TEST_ACTOR))[1] == 202 and engine._active == 1
        with pytest.raises(ApiProblem) as e:  # the cap is checked with no await between check and take
            await engine.start_replay("dev", "adt", REQUEST, TEST_ACTOR)
        assert e.value.code == "UNAVAILABLE" and engine._active == 1
        release.set()
        await asyncio.sleep(0.05)
        assert engine._active == 0

    asyncio.run(go())


def test_stream_slots_are_per_subject_and_in_total_and_release_is_idempotent() -> None:
    async def go() -> None:
        hub = StreamHub(per_subject=2, total=3)
        a1, a2 = hub.acquire("a"), hub.acquire("a")
        with pytest.raises(ApiProblem):
            hub.acquire("a")  # the subject's cap
        b1 = hub.acquire("b")
        with pytest.raises(ApiProblem):
            hub.acquire("c")  # the replica's cap
        a1()
        a1()  # twice: counted once
        assert hub.open_streams() == 2
        hub.acquire("c")
        for r in (a2, b1):
            r()

    asyncio.run(go())


def test_a_response_that_fails_before_streaming_still_gives_its_slot_back() -> None:
    async def go() -> None:
        hub = StreamHub()
        release = hub.acquire("a")

        async def never() -> Any:
            yield b""

        response = SlotStreamingResponse(never(), release)

        async def receive() -> dict[str, Any]:
            return {"type": "http.disconnect"}

        async def send(message: dict[str, Any]) -> None:
            raise OSError("client went away before the first byte")

        with pytest.raises(Exception):  # noqa: B017 - starlette re-raises the send failure as its own type
            await response({"type": "http", "asgi": {"spec_version": "2.4"}}, receive, send)
        assert hub.open_streams() == 0

    asyncio.run(go())


def test_an_archive_that_stays_down_is_logged_once_a_minute_not_once_a_poll() -> None:
    now = [1000.0]
    log = Log()
    streamer = EventStreamer(None, StreamHub(), log, Pools(), clock=lambda: now[0])  # type: ignore[arg-type]
    for _ in range(50):
        streamer._log_poll_failure(OSError())
    assert len(log.events) == 1
    now[0] += 61
    streamer._log_poll_failure(OSError())
    assert len(log.events) == 2


def test_the_client_forces_readonly_whatever_the_caller_passes() -> None:
    seen: list[dict[str, str]] = []

    class T:
        def request(self, sql: str, *, settings: dict[str, str], **_k: Any) -> bytes:
            seen.append(settings)
            return json.dumps({"meta": [], "data": []}).encode()

    ArchiveClient(T()).query("SELECT 1", {}, {"readonly": 0, "max_execution_time": 5})  # type: ignore[arg-type]
    ArchiveClient(T()).query("SELECT 1")  # type: ignore[arg-type]
    assert [s["readonly"] for s in seen] == ["1", "1"] and list(seen[0])[-1] == "readonly"


def test_an_endpoint_that_never_completes_a_connect_cannot_pile_up_workers() -> None:
    """A worker given up on is shut down at once if it has a socket; one stuck in DNS or connect cannot be, so only
    the global cap stands between a black hole and threads (single_flight is off for the archive client)."""
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(0)  # a full backlog: later connects never complete
    fillers = []
    for _ in range(4):
        c = socket.socket()
        c.setblocking(False)
        c.connect_ex(srv.getsockname())
        fillers.append(c)
    client = ClickHouseHttp(
        f"http://127.0.0.1:{srv.getsockname()[1]}", "u", "p", timeout_s=20, deadline_s=0.3, single_flight=False,
        max_abandoned=2,
    )  # fmt: skip
    try:
        for _ in range(2):
            with pytest.raises(TamperQueryError):
                client.request("SELECT 1")
        started = time.monotonic()
        with pytest.raises(TamperQueryError):
            client.request("SELECT 1")
        assert time.monotonic() - started < 0.2  # refused at once: two workers are still stuck in connect
        assert len(client._abandoned) == 2
    finally:
        for c in fillers:
            c.close()
        srv.close()


def test_stream_and_replay_caps_are_configurable_and_bounded() -> None:
    from test_tamper_unit import ARCHIVE, BASE
    from tributary.api import config
    from tributary.api.errors import ConfigurationError

    s = config.from_env({**BASE, **ARCHIVE})
    assert s.archive is not None and (s.archive.max_streams, s.archive.max_replays) == (64, 4)
    s = config.from_env({**BASE, **ARCHIVE, "TB_API_MAX_STREAMS": "10", "TB_API_MAX_REPLAYS": "2"})
    assert s.archive is not None and (s.archive.max_streams, s.archive.max_replays) == (10, 2)
    for name, value, code in (
        ("TB_API_MAX_STREAMS", "0", "API_MAX_STREAMS_OUT_OF_RANGE"),
        ("TB_API_MAX_STREAMS", "1001", "API_MAX_STREAMS_OUT_OF_RANGE"),
        ("TB_API_MAX_REPLAYS", "17", "API_MAX_REPLAYS_OUT_OF_RANGE"),
    ):
        with pytest.raises(ConfigurationError) as e:
            config.from_env({**BASE, **ARCHIVE, name: value})
        assert e.value.code == code


def test_a_replay_that_outlives_the_close_wait_is_logged_with_its_id_and_source_waits_fit_inside_it() -> None:
    assert reingest.SOURCE_TIMEOUT_S * 2 < reingest.CLOSE_TIMEOUT_S  # a read, then the audit follow-up, fits

    async def go() -> None:
        log = Log()
        engine = make_reingest(SimpleNamespace())
        engine._log = log  # type: ignore[assignment]
        stop = threading.Event()

        async def stubborn() -> None:
            await asyncio.sleep(5)

        task = asyncio.create_task(stubborn())
        engine._running[task] = (stop, "01a133e9-0e50-7000-8000-000000000001")
        await engine.close(timeout_s=0.2)
        assert stop.is_set()  # it was told to stop
        [(_event, fields)] = [e for e in log.events if e[0] == "replay_close_timeout"]
        assert fields["replay_ids"] == ["01a133e9-0e50-7000-8000-000000000001"] and fields["waited_s"] == 0.2
        task.cancel()

    asyncio.run(go())


def test_gates_refuse_when_full_and_slots_come_back_only_when_the_work_ends() -> None:
    from tributary.api.pools import Admission

    gate = Admission(2)
    first, second = gate.enter(), gate.enter()
    assert gate.try_enter() is None
    with pytest.raises(ApiProblem) as e:
        gate.enter()
    assert (e.value.code, e.value.status) == ("ARCHIVE_UNAVAILABLE", 503)
    first()
    first()  # twice: counted once
    third = gate.enter()
    assert gate.try_enter() is None
    second()
    third()
    assert gate.try_enter() is not None


def test_event_streams_use_their_own_client_so_stuck_connections_are_counted_apart() -> None:
    from tributary.api.archive import ArchiveReader

    reads, content, streams = object(), object(), object()
    reader = ArchiveReader(
        reads,
        {"dev": "db"},
        None,
        None,
        Log(),
        Pools(),
        content_client=content,
        stream_client=streams,  # type: ignore[arg-type]
    )
    assert reader.queries("dev")._client is reads  # type: ignore[attr-defined]
    assert reader.queries("dev", stream=True)._client is streams  # type: ignore[attr-defined]
    assert reader._content_client is content


# -- a cancelled queued job gives its gate slot back (reads, content, stream polls) -------------------------


def free_slots(gate: Any) -> int:
    """How many more units the gate admits right now (it is put back as it was)."""
    taken = []
    while (leave := gate.try_enter()) is not None:
        taken.append(leave)
    for leave in taken:
        leave()
    return len(taken)


def saturate(pool: Any, workers: int) -> threading.Event:
    """Occupy every thread of `pool` so that anything submitted next waits in its queue."""
    release = threading.Event()
    started = threading.Barrier(workers + 1)

    def block() -> None:
        started.wait(10)
        release.wait(30)

    for _ in range(workers):
        pool.submit(block)
    started.wait(10)
    return release


def make_reader(pools: Pools) -> Any:
    from tributary.api.archive import ArchiveReader

    return ArchiveReader(object(), {"dev": "db"}, None, None, Log(), pools)  # type: ignore[arg-type]


def test_a_read_cancelled_while_queued_gives_its_slot_back() -> None:
    async def go() -> None:
        pools = Pools()
        reader = make_reader(pools)
        release = saturate(pools.reads, 8)
        try:
            assert free_slots(pools.read_gate) == 16
            task = asyncio.create_task(reader._blocking(lambda: "never runs"))
            await asyncio.sleep(0.2)
            assert free_slots(pools.read_gate) == 15  # queued behind the blockers, holding its slot
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert free_slots(pools.read_gate) == 16  # the slot came back although no thread ever ran the job
        finally:
            release.set()
            pools.shutdown()

    asyncio.run(go())


def test_a_content_read_cancelled_while_queued_gives_its_slot_back() -> None:
    async def go() -> None:
        pools = Pools()
        reader = make_reader(pools)
        release = saturate(pools.content, 4)
        try:
            leave = pools.content_gate.enter()
            task = asyncio.create_task(reader._run_gated(pools.content, lambda: "never runs", leave))
            await asyncio.sleep(0.2)
            assert free_slots(pools.content_gate) == 7
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert free_slots(pools.content_gate) == 8
        finally:
            release.set()
            pools.shutdown()

    asyncio.run(go())


def test_a_running_job_keeps_its_slot_until_its_thread_ends_even_if_the_request_is_cancelled() -> None:
    async def go() -> None:
        pools = Pools()
        reader = make_reader(pools)
        started, release = threading.Event(), threading.Event()

        def slow() -> str:
            started.set()
            release.wait(10)
            return "done"

        try:
            task = asyncio.create_task(reader._blocking(slow))
            await asyncio.get_running_loop().run_in_executor(None, started.wait, 5)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert free_slots(pools.read_gate) == 15  # the thread is still running: the gate still counts it
            release.set()
            for _ in range(50):
                if free_slots(pools.read_gate) == 16:
                    break
                await asyncio.sleep(0.05)
            assert free_slots(pools.read_gate) == 16
        finally:
            release.set()
            pools.shutdown()

    asyncio.run(go())


def test_a_stream_poll_cancelled_while_queued_gives_its_slot_back() -> None:
    from types import SimpleNamespace

    async def go() -> None:
        pools = Pools()
        release = saturate(pools.streams, 8)
        hub = StreamHub()
        reader = SimpleNamespace()
        streamer = EventStreamer(reader, hub, Log(), pools, poll_s=0.05, heartbeat_s=30)  # type: ignore[arg-type]
        events = streamer._events(object(), ["adt"], 0.0, None)
        try:
            task = asyncio.create_task(events.__anext__())
            await asyncio.sleep(0.3)  # the poll is submitted and waits behind the blockers
            assert free_slots(pools.stream_gate) == 15
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await events.aclose()
            assert free_slots(pools.stream_gate) == 16
        finally:
            release.set()
            pools.shutdown()

    asyncio.run(go())


# -- content storage flag outcomes, the SSE reset frame, and what the audit record claims -------------------------

VIEW_ID = "01a133e9-0e50-7000-8000-0000000000aa"


class FakeQueries:
    """The three library calls the content path makes, scripted."""

    def __init__(self, enabled: Any, read: Any) -> None:
        self.enabled, self.read = enabled, read
        self.calls: list[str] = []

    def content_storage_enabled(self) -> Any:
        self.calls.append("flag")
        if isinstance(self.enabled, Exception):
            raise self.enabled
        return self.enabled

    def read_content(self, *a: Any) -> Any:
        self.calls.append("read")
        if isinstance(self.read, Exception):
            raise self.read
        return self.read


class RecordingAudit:
    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []

    async def requested(self, **fields: Any) -> str:
        self.records.append(fields)
        return "audit-id"


def content_reader(queries: FakeQueries, audit: RecordingAudit) -> Any:
    from tributary.api.archive import ArchiveReader

    async def require(env: str, channel: str) -> None:
        return None

    control = SimpleNamespace(require_channel=require)
    reader = ArchiveReader(object(), {"dev": "db"}, control, audit, Log(), Pools())  # type: ignore[arg-type]
    reader.content_queries = lambda env: queries  # type: ignore[method-assign]
    return reader


def view_content(reader: Any) -> Any:
    return asyncio.run(reader.content("dev", "adt", VIEW_ID, "received", None, TEST_ACTOR))


@pytest.mark.parametrize(
    ("enabled", "read", "want"),
    [
        (False, None, ("CONTENT_STORAGE_OFF", 409)),  # the flag says off
        (None, "table-missing", ("CONTENT_STORAGE_OFF", 409)),  # no flag ever written: a missing table reads as off
        (True, "table-missing", ("ARCHIVE_UNAVAILABLE", 503)),  # on, and the table is not there: a fault
        (True, "failed", ("ARCHIVE_UNAVAILABLE", 503)),  # any other query failure
        (None, "other", ("ARCHIVE_UNAVAILABLE", 503)),  # any QueryError code the API does not know falls through
        ("flag-failed", None, ("ARCHIVE_UNAVAILABLE", 503)),  # the flag itself cannot be read
    ],
)
def test_every_content_flag_outcome_has_its_problem_and_the_audit_record_claims_nothing_was_served(
    enabled: Any, read: Any, want: tuple[str, int]
) -> None:
    from tributary.archiver.errors import QueryError

    errors = {
        "table-missing": QueryError("CONTENT_TABLE_MISSING", "archive table is missing"),
        "failed": QueryError("QUERY_FAILED", "archive query failed"),
        "other": QueryError("SOMETHING_NEW", "an unlisted code"),
    }
    queries = FakeQueries(
        QueryError("QUERY_FAILED", "x") if enabled == "flag-failed" else enabled,
        errors.get(read, read),
    )
    audit = RecordingAudit()
    with pytest.raises(ApiProblem) as e:
        view_content(content_reader(queries, audit))
    assert (e.value.code, e.value.status) == want
    if enabled is False:
        assert "read" not in queries.calls  # storage off: the table is never read
    # L3: the audit record written before the refusal is the `requested` record and says nothing about an outcome.
    [record] = audit.records
    assert record["action"] == "message.content.view"
    assert set(record) == {"action", "environment", "actor", "channel", "message_id", "stage", "destination"}
    assert not {"outcome", "served", "count", "error_code", "phase"} & set(record)


def test_the_content_audit_schema_has_no_outcome_field_so_a_requested_record_cannot_claim_a_serve() -> None:
    from tributary.api.audit import _validator

    properties = set(_validator().schema["properties"])
    assert not {"outcome", "served", "result", "status"} & properties
    schema_phases = _validator().schema["properties"]["phase"]["enum"]
    assert schema_phases == ["requested", "completed", "failed"]  # a view is `requested` only (audit/v0 README 2)


def test_the_dense_window_reset_frame_carries_the_reason_the_contract_enumerates() -> None:
    """The reset for a window too dense to read in order, pinned against OpenAPI 0.2.3 `StreamNotice`."""
    enum = json.loads(OPENAPI.read_text())["components"]["schemas"]["StreamNotice"]["properties"]["reason"]["enum"]
    assert "backlog_too_dense" in enum and "cursor_expired" in enum

    class Endless:
        """A channel with more pages than any poll reads: every page is full and has a next cursor."""

        def search(self, flt: Any, *, limit: int, cursor: Any) -> Any:
            return SimpleNamespace(items=[object()] * limit, next_cursor="more")

    async def go() -> bytes:
        streamer = EventStreamer(SimpleNamespace(), StreamHub(), Log(), Pools(), poll_s=0.05, heartbeat_s=30)  # type: ignore[arg-type]
        events = streamer._events(Endless(), ["adt"], 0.0, None)
        try:
            return await asyncio.wait_for(events.__anext__(), 10)
        finally:
            await events.aclose()

    frame = asyncio.run(go()).decode()
    lines = frame.strip().split("\n")
    assert lines[0] == "event: reset"
    reason = json.loads(lines[1].removeprefix("data: "))["reason"]
    assert reason == "backlog_too_dense" and reason in enum
    assert reason != "cursor_expired"  # real expiry keeps its own reason (stream.py resume path)
