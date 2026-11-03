"""The ts window (both tables are month-partitioned) and stuck-insert handling in the archive loop."""

from __future__ import annotations

import importlib.util
import io
import json
import sys
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import orjson
import pytest
from tributary.archiver import archiver as loop
from tributary.archiver.archiver import Archiver, ArchiverSettings, parse_event_record
from tributary.archiver.audit_record import parse_audit_record
from tributary.archiver.errors import AuditRejected, EventRejected, SinkError
from tributary.archiver.event import parse_event
from tributary.archiver.tswindow import in_window, record_time
from tributary.runtime.logs import JsonLogger


def _fakes() -> Any:
    path = Path(__file__).with_name("test_archiver_loop.py")
    spec = importlib.util.spec_from_file_location("archiver_loop_fakes2", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["archiver_loop_fakes2"] = module
    spec.loader.exec_module(module)
    return module


_loop = _fakes()
FakeConsumer, FakeMessage, base = _loop.FakeConsumer, _loop.FakeMessage, _loop.base
EXAMPLES = Path(__file__).resolve().parents[3] / "contracts" / "audit" / "v0" / "examples" / "valid"
NOW = datetime(2026, 10, 14, 12, tzinfo=UTC)


def audit(**over: Any) -> bytes:
    record = json.loads((EXAMPLES / "content-view-requested.json").read_text())
    return orjson.dumps({**record, **over})


def audit_row(anchor: datetime | None = None, *, now: datetime | None = NOW, **over: Any) -> Any:
    return parse_audit_record(audit(**over), env="prod", partition=0, offset=0, anchor=anchor, now=now)


# ---- the window ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "ts",
    ["0001-01-01T00:00:00.000Z", "1999-12-31T23:59:59.999Z", "9999-12-31T23:59:59.999Z", "2299-01-01T00:00:00.000Z"],
)
def test_an_absurd_audit_ts_is_rejected_with_a_fixed_code(ts: str) -> None:
    with pytest.raises(AuditRejected) as raised:
        audit_row(ts=ts)
    assert raised.value.code == "AUDIT_TIMESTAMP_INVALID"


def test_the_audit_window_is_anchored_on_the_kafka_record_time() -> None:
    anchor = datetime(2026, 10, 6, 9, 35, tzinfo=UTC)  # the example's ts is 2026-10-06T09:35:00.120Z
    assert audit_row(anchor=anchor).ts.year == 2026
    with pytest.raises(AuditRejected):  # a producer cannot move the window past the clock: anchor = min(record, now)
        audit_row(anchor=datetime(9000, 1, 1, tzinfo=UTC), ts="9000-01-01T00:00:00.000Z")
    with pytest.raises(AuditRejected):
        audit_row(anchor=datetime(2299, 1, 1, tzinfo=UTC), ts="2298-06-01T00:00:00.000Z")
    with pytest.raises(AuditRejected):  # more than a day after the record was produced
        audit_row(anchor=anchor - timedelta(days=2))
    with pytest.raises(AuditRejected):  # more than 30 days before it
        audit_row(anchor=anchor + timedelta(days=31), now=anchor + timedelta(days=40))
    assert audit_row(anchor=anchor + timedelta(days=29), now=anchor + timedelta(days=40)).ts.year == 2026


@pytest.mark.parametrize("field", ["range_start", "range_end"])
@pytest.mark.parametrize("value", ["1850-01-01T00:00:00.000Z", "0001-01-01T00:00:00.000Z", "9999-01-01T00:00:00.000Z"])
def test_replay_range_bounds_outside_datetime64_are_rejected(field: str, value: str) -> None:
    with pytest.raises(AuditRejected) as raised:
        parse_audit_record(
            (EXAMPLES / "replay-completed-reorder.json").read_text().replace("2026-10-06T06:00:00.000Z", value)
            if field == "range_start"
            else (EXAMPLES / "replay-completed-reorder.json").read_text().replace("2026-10-06T07:00:00.000Z", value),
            env="prod",
            partition=0,
            offset=0,
            anchor=datetime(2026, 10, 6, 9, 34, tzinfo=UTC),
        )
    assert raised.value.code == "AUDIT_TIMESTAMP_INVALID"


@pytest.mark.parametrize("ts", ["0001-01-01T00:00:00.000Z", "1999-12-31T23:59:59.999Z", "9999-12-31T23:59:59.999Z"])
def test_the_events_path_has_the_same_window(ts: str) -> None:
    with pytest.raises(EventRejected) as raised:
        parse_event(orjson.dumps(base(ts=ts)))
    assert raised.value.code == "EVENT_TS_OUT_OF_RANGE"
    assert parse_event(orjson.dumps(base())).ts.year == 2026  # a normal event still parses


def test_the_events_window_has_no_lookback_but_follows_the_record_time_upward() -> None:
    old = orjson.dumps(base(ts="2026-01-01T00:00:00.123Z"))  # a replayed `received` event: tb-received-at is old
    assert parse_event(old, anchor=datetime(2026, 10, 1, tzinfo=UTC)).ts.year == 2026
    with pytest.raises(EventRejected):  # produced a day and more BEFORE the event claims to have happened
        parse_event(old, anchor=datetime(2025, 12, 1, tzinfo=UTC))


def test_in_window_and_record_time_helpers() -> None:
    assert in_window(NOW, now=NOW) and in_window(NOW + timedelta(hours=23), now=NOW)
    assert not in_window(NOW + timedelta(days=2), now=NOW)
    assert not in_window(datetime(1999, 1, 1, tzinfo=UTC), now=NOW)
    assert in_window(NOW - timedelta(days=400), now=NOW), "without a record time only 2000-01-01 bounds the past"
    assert in_window(NOW - timedelta(days=31), NOW), "no lookback by default"
    assert not in_window(NOW - timedelta(days=31), NOW, lookback=timedelta(days=30))

    class Msg:
        def __init__(self, value: Any) -> None:
            self._v = value

        def timestamp(self) -> Any:
            return self._v

    assert record_time(Msg((1, 1_780_000_000_000))) == datetime.fromtimestamp(1_780_000_000, UTC)
    for bad in ((1, 0), (1, -5), None, (1,), "x", (1, 10**30)):
        assert record_time(Msg(bad)) is None
    assert record_time(object()) is None


def test_the_loop_gives_the_parser_the_record_time_and_a_bad_ts_is_one_skipped_record() -> None:
    old = orjson.dumps(base(ts="3000-01-01T00:00:00.123Z"))  # absurdly far ahead of the record
    ok = orjson.dumps(base(ts=datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.123Z")))
    now_ms = int(datetime.now(UTC).timestamp() * 1000)

    class Stamped(FakeMessage):
        def timestamp(self) -> tuple[int, int]:
            return (1, now_ms)

    consumer = FakeConsumer([[Stamped(0, old), Stamped(1, ok)]])
    sink = _loop.FakeSink()
    archiver = Archiver(
        consumer,
        sink,
        log=JsonLogger("t", stream=io.StringIO()),
        settings=ArchiverSettings(),
        sleep=lambda _s: None,
        parser=parse_event_record,
    )
    archiver.run_once()
    assert archiver.metrics.events_rejected == 1 and len(sink.inserted) == 1 and consumer.commits[-1] == [(0, 2)]


# ---- insert blocks and the stuck-insert path ----------------------------------------------------------------


class Row:
    def __init__(self, tag: str, ts: datetime, table: str = "") -> None:
        self.values: tuple[Any, ...] = (tag, 0, 5)
        self.ts, self.table = ts, table
        self.attrs_dropped = self.attrs_dropped_hmac = self.fields_blanked = 0


class RecordingSink:
    def __init__(
        self,
        bad: set[str] = frozenset(),  # type: ignore[assignment]
        *,
        all_bad: bool = False,
        transient_times: int = 0,
        transient_on_call: int | None = None,
        bad_unless_alone: set[str] = frozenset(),  # type: ignore[assignment]
    ) -> None:
        self.bad, self.all_bad, self.transient_times = set(bad), all_bad, transient_times
        self.transient_on_call, self.bad_unless_alone = transient_on_call, set(bad_unless_alone)
        self.blocks: list[list[str]] = []
        self.calls = 0

    def insert(self, rows: Sequence[Sequence[Any]]) -> None:
        self.calls += 1
        if self.transient_on_call == self.calls:
            raise SinkError("connection reset", transient=True)
        if self.transient_times > 0:
            self.transient_times -= 1
            raise SinkError("down", transient=True)
        if len(rows) > 1 and any(r[0] in self.bad_unless_alone for r in rows):
            raise SinkError("refused", transient=False, code="SINK_ERROR")
        if self.all_bad or any(r[0] in self.bad for r in rows):
            raise SinkError("refused", transient=False, code="SINK_ERROR")
        self.blocks.append([r[0] for r in rows])

    def close(self) -> None: ...


def archiver_for(sink: Any, audit_sink: Any = None, stream: io.StringIO | None = None) -> Archiver:
    return Archiver(
        FakeConsumer([]),
        sink,
        log=JsonLogger("t", stream=stream or io.StringIO()),
        settings=ArchiverSettings(backoff_initial_s=0.01, backoff_max_s=0.02),
        sleep=lambda _s: None,
        side_sinks={"audit_log": audit_sink} if audit_sink else None,
    )


def test_a_batch_spread_over_many_months_is_cut_into_blocks_of_at_most_fifty_months() -> None:
    rows = [Row(f"r{i}", datetime(2000 + i // 12, i % 12 + 1, 1, tzinfo=UTC)) for i in range(120)]  # 120 months
    sink = RecordingSink()
    archiver = archiver_for(sink)
    assert archiver._write(rows)  # type: ignore[arg-type]
    assert len(sink.blocks) == 3 and sum(len(b) for b in sink.blocks) == 120
    for block in sink.blocks:
        months = {(rows[int(tag[1:])].ts.year, rows[int(tag[1:])].ts.month) for tag in block}
        assert len(months) <= loop.MAX_MONTHS_PER_BLOCK
    assert archiver.metrics.rows_written == 120


def test_one_bad_row_is_isolated_set_aside_logged_and_the_rest_stored() -> None:
    day = datetime(2026, 10, 1, tzinfo=UTC)
    rows = [Row(f"r{i}", day) for i in range(40)]
    sink, stream = RecordingSink({"r17"}), io.StringIO()
    archiver = archiver_for(sink, stream=stream)
    assert archiver._write(rows)  # type: ignore[arg-type]
    stored = sorted(tag for block in sink.blocks for tag in block)
    assert stored == sorted(f"r{i}" for i in range(40) if i != 17), "every other row landed, none twice"
    assert archiver.metrics.events_rejected == 1 and archiver.metrics.rows_written == 39
    assert archiver.metrics.batches_halted == 0
    aside = [json.loads(x) for x in stream.getvalue().splitlines() if '"row_set_aside"' in x]
    assert len(aside) == 1 and aside[0]["table"] == "events" and "r17" not in stream.getvalue()


def test_a_bad_audit_row_is_counted_invalid_and_logged_with_its_source_position() -> None:
    day = datetime(2026, 10, 1, tzinfo=UTC)
    rows = [Row(f"a{i}", day, "audit_log") for i in range(8)]
    rows[3].values = ("a3", 7, 99)  # partition 7, offset 99
    audit_sink, stream = RecordingSink({"a3"}), io.StringIO()
    archiver = archiver_for(RecordingSink(), audit_sink, stream)
    archiver._origin[id(rows[3])] = ("_tb.prod.audit", 7, 99)
    assert archiver._write(rows)  # type: ignore[arg-type]
    assert archiver.metrics.audit_invalid == 1 and archiver.metrics.audit_written == 7
    (aside,) = [json.loads(x) for x in stream.getvalue().splitlines() if '"row_set_aside"' in x]
    assert (aside["table"], aside["topic"], aside["partition"], aside["offset"]) == (
        "audit_log",
        "_tb.prod.audit",
        7,
        99,
    )


def test_when_every_row_is_refused_the_cause_is_systemic_and_the_batch_halts_instead_of_being_dropped() -> None:
    day = datetime(2026, 10, 1, tzinfo=UTC)
    rows = [Row(f"r{i}", day) for i in range(200)]
    sink = RecordingSink(all_bad=True)
    archiver = archiver_for(sink)

    archiver._sleep = lambda _s: archiver.stop()  # type: ignore[assignment]  # stop at the first backoff
    assert archiver._write(rows) is False  # type: ignore[arg-type]  # asked to stop, nothing committed
    assert archiver.metrics.events_rejected == 0 and archiver.metrics.rows_written in (0,)
    assert archiver.metrics.batches_halted == 1 and archiver.metrics.insert_failures_permanent >= 1
    assert sink.blocks == [], "nothing was dropped or half-stored"
    # The isolation attempt is bounded: it does not make one failing insert per row.
    assert sink.calls <= 1 + loop.isolation_budget(200) + 8, "bounded: not one failing insert per row"


def test_a_transient_failure_during_normal_insert_still_retries_the_whole_batch() -> None:
    rows = [Row(f"r{i}", datetime(2026, 10, 1, tzinfo=UTC)) for i in range(5)]
    sink = RecordingSink(transient_times=2)
    archiver = archiver_for(sink)
    assert archiver._write(rows)  # type: ignore[arg-type]
    assert (
        sorted(t for b in sink.blocks for t in b) == [f"r{i}" for i in range(5)]
        and archiver.metrics.events_rejected == 0
    )


def test_local_driver_conversion_errors_are_permanent_but_network_errors_are_not() -> None:
    import struct

    from tributary.archiver.sink import is_permanent

    for exc in (AttributeError("x"), TypeError("x"), ValueError("x"), OverflowError("x"), struct.error("x")):
        assert is_permanent(exc), type(exc).__name__
    for exc in (ConnectionResetError("x"), TimeoutError("x"), OSError("x")):
        assert not is_permanent(exc), type(exc).__name__


def flat(sink: RecordingSink) -> list[str]:
    return [tag for block in sink.blocks for tag in block]


def rows_of(count: int, table: str = "") -> list[Row]:
    day = datetime(2026, 10, 1, tzinfo=UTC)
    return [Row(f"r{i}", day, table) for i in range(count)]


def test_a_suspect_that_the_sink_accepts_on_a_second_try_is_stored_not_set_aside() -> None:
    """Code review M2: a transient fault in disguise must not cost a row."""
    rows = rows_of(40)
    sink = RecordingSink(bad_unless_alone={"r5"})  # refused inside any block, accepted alone
    archiver = archiver_for(sink)
    assert archiver._write(rows)  # type: ignore[arg-type]
    assert sorted(flat(sink)) == sorted(f"r{i}" for i in range(40))
    assert archiver.metrics.events_rejected == 0 and archiver.metrics.rows_written == 40


def test_a_transient_error_in_the_middle_of_the_bisect_does_not_store_any_row_twice() -> None:
    """Code review M1: the retry must remember which sub-blocks already landed."""
    rows = rows_of(40)
    for failing_call in range(2, 14):  # a connection reset at every depth of the search
        sink = RecordingSink({"r17"}, transient_on_call=failing_call)
        archiver = archiver_for(sink)
        assert archiver._write(rows)  # type: ignore[arg-type]
        stored = flat(sink)
        assert sorted(stored) == sorted(f"r{i}" for i in range(40) if i != 17), failing_call
        assert len(stored) == len(set(stored)), f"a row was inserted twice (reset at call {failing_call})"
        assert archiver.metrics.events_rejected == 1


def test_after_the_budget_is_spent_the_batch_halts_without_bisecting_again_or_growing_the_table() -> None:
    rows = rows_of(200)
    bad = {f"r{i}" for i in range(1, 199)}  # 198 of 200: far past what a row-level cause explains
    sink = RecordingSink(bad)
    archiver = archiver_for(sink)
    cycles = [0]

    def sleep(_s: float) -> None:
        cycles[0] += 1
        if cycles[0] >= 4:
            archiver.stop()

    archiver._sleep = sleep  # type: ignore[assignment]
    assert archiver._write(rows) is False  # type: ignore[arg-type]
    stored = flat(sink)
    assert len(stored) == len(set(stored)), "no row was stored twice across the backoff cycles"
    assert archiver.metrics.batches_halted == 1 and archiver.metrics.events_rejected == 0
    assert sink.calls <= loop.isolation_budget(200) + 2 + 3 * 3, "later cycles retry the remainder once, no new bisect"


def test_the_budget_scales_with_the_batch() -> None:
    assert loop.isolation_budget(1) == loop.ISOLATION_BASE
    small, big = loop.isolation_budget(40), loop.isolation_budget(10_000)
    assert small < big and big >= (10_000 // 20) * 14, "5% of a 10k batch may be bad before it is called systemic"
    sink = RecordingSink({f"r{i}" for i in range(0, 600, 20)})  # 30 bad rows in 600: a row-level cause
    archiver = archiver_for(sink)
    assert archiver._write(rows_of(600)) is True  # type: ignore[arg-type]
    assert archiver.metrics.events_rejected == 30 and archiver.metrics.rows_written == 570


def test_a_lone_refused_row_is_set_aside_once_the_sink_has_worked_and_halts_before() -> None:
    sink = RecordingSink({"bad"})
    archiver = archiver_for(sink)
    archiver._sleep = lambda _s: archiver.stop()  # type: ignore[assignment]
    assert archiver._write([Row("bad", datetime(2026, 10, 1, tzinfo=UTC))]) is False  # type: ignore[arg-type]
    assert archiver.metrics.events_rejected == 0, "a sink that never worked: this may be the table, not the row"
    assert archiver._write(rows_of(3)) is True  # type: ignore[arg-type]  # now the sink is proven
    assert archiver._write([Row("bad", datetime(2026, 10, 1, tzinfo=UTC))]) is True  # type: ignore[arg-type]
    assert archiver.metrics.events_rejected == 1 and flat(sink) == ["r0", "r1", "r2"]


def test_a_streak_of_lone_rows_set_aside_is_called_systemic() -> None:
    sink = RecordingSink()
    archiver = archiver_for(sink)
    assert archiver._write(rows_of(2)) is True  # type: ignore[arg-type]
    sink.bad = {"bad"}
    for _ in range(loop.ASIDE_STREAK_LIMIT):
        assert archiver._write([Row("bad", datetime(2026, 10, 1, tzinfo=UTC))]) is True  # type: ignore[arg-type]
    archiver._sleep = lambda _s: archiver.stop()  # type: ignore[assignment]
    assert archiver._write([Row("bad", datetime(2026, 10, 1, tzinfo=UTC))]) is False  # type: ignore[arg-type]
    assert archiver.metrics.events_rejected == loop.ASIDE_STREAK_LIMIT


def test_a_conversion_error_on_top_of_a_network_fault_is_transient() -> None:
    from tributary.archiver.sink import is_permanent

    def wrapped(inner: BaseException) -> BaseException:
        try:
            try:
                raise inner
            except OSError as io_error:
                raise AttributeError("'NoneType' object has no attribute 'x'") from io_error
        except AttributeError as outer:
            return outer

    assert not is_permanent(wrapped(ConnectionResetError("reset")))
    assert is_permanent(AttributeError("'str' object has no attribute 'bytes'"))


def test_the_set_aside_log_for_an_event_names_where_it_came_from() -> None:
    from tributary.archiver.event import parse_event

    class Refusing:
        def __init__(self, bad_id: uuid.UUID) -> None:
            self.bad_id, self.inserted = bad_id, 0

        def insert(self, rows: Sequence[Sequence[Any]]) -> None:
            if any(self.bad_id in r for r in rows):
                raise SinkError("refused", transient=False)
            self.inserted += len(rows)

        def close(self) -> None: ...

    now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.123Z")
    ids = [str(uuid.UUID(int=(7 << 76) | (8 << 62) | i)) for i in range(1, 6)]
    messages = [
        FakeMessage(40 + i, orjson.dumps(base(ts=now, message_id=message_id))) for i, message_id in enumerate(ids)
    ]
    sink, stream = Refusing(uuid.UUID(ids[2])), io.StringIO()
    consumer = FakeConsumer([messages])
    archiver = Archiver(
        consumer,
        sink,
        log=JsonLogger("t", stream=stream),
        settings=ArchiverSettings(),
        sleep=lambda _s: None,
        parser=lambda m: parse_event(m.value()),
    )
    archiver.run_once()
    (aside,) = [json.loads(x) for x in stream.getvalue().splitlines() if '"row_set_aside"' in x]
    assert (aside["table"], aside["topic"], aside["partition"], aside["offset"]) == ("events", "_tb.dev.events", 0, 42)
    assert sink.inserted == 4 and consumer.commits[-1] == [(0, 45)], "the rest landed and the batch committed"
    assert ids[2] not in stream.getvalue()


class AcceptsAfterSink(RecordingSink):
    """Refuses `late` until call number `after`, then takes it (a fault that clears)."""

    def __init__(self, bad: set[str], late: str, after: int, transient_on_call: int) -> None:
        super().__init__(bad, transient_on_call=transient_on_call)
        self.late, self.after = late, after

    def insert(self, rows: Sequence[Sequence[Any]]) -> None:
        if self.calls >= self.after:
            self.bad.discard(self.late)
        super().insert(rows)


def test_a_row_set_aside_by_an_interrupted_attempt_is_still_logged_and_counted() -> None:
    """Code review M: the retry's lone suspect was accepted and the early return skipped the earlier bad row."""
    rows = [Row(f"r{i}", datetime(2026, 10, 1, tzinfo=UTC)) for i in range(4)]
    # Calls: 1 block, 2 [r0 r1], 3 r0, 4 r1 (bad), 5 [r2 r3], 6 r2, 7 r3 (bad), 8 retry r1 (bad), 9 retry r3 ->
    # a connection reset. On the retried batch r1 is already known bad and r3 is the lone row, now accepted.
    sink, stream = AcceptsAfterSink({"r1", "r3"}, late="r3", after=9, transient_on_call=9), io.StringIO()
    archiver = archiver_for(sink, stream=stream)
    archiver._origin[id(rows[1])] = ("_tb.dev.events", 0, 41)
    assert archiver._write(rows)  # type: ignore[arg-type]
    assert sorted(flat(sink)) == ["r0", "r2", "r3"], "r3 landed on its second try, r1 did not"
    assert archiver.metrics.events_rejected == 1
    (aside,) = [json.loads(x) for x in stream.getvalue().splitlines() if '"row_set_aside"' in x]
    assert (aside["table"], aside["partition"], aside["offset"]) == ("events", 0, 41)


def test_the_streak_resets_when_the_bisect_stores_rows() -> None:
    sink = RecordingSink({"r3"})
    archiver = archiver_for(sink)
    archiver._aside_streak[""] = loop.ASIDE_STREAK_LIMIT - 1
    assert archiver._write(rows_of(10)) is True  # type: ignore[arg-type]
    assert archiver._aside_streak[""] == 1, "the nine stored rows broke the streak; only this block's bad row counts"
    # With the old behaviour the streak would now be at the limit and a lone bad row would halt the archiver.
    sink.bad.add("lone")
    assert archiver._write([Row("lone", datetime(2026, 10, 1, tzinfo=UTC))]) is True  # type: ignore[arg-type]
