"""The ts window (both tables are month-partitioned) and stuck-insert handling in the archive loop."""

from __future__ import annotations

import importlib.util
import io
import json
import sys
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


def audit_row(anchor: datetime | None = None, **over: Any) -> Any:
    return parse_audit_record(audit(**over), env="prod", partition=0, offset=0, anchor=anchor)


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
    with pytest.raises(AuditRejected):  # more than a day after the record was produced
        audit_row(anchor=anchor - timedelta(days=2))
    with pytest.raises(AuditRejected):  # more than 30 days before it
        audit_row(anchor=anchor + timedelta(days=31))
    assert audit_row(anchor=anchor + timedelta(days=29)).ts.year == 2026


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
    def __init__(self, bad: set[str] = frozenset(), *, all_bad: bool = False, transient_times: int = 0) -> None:  # type: ignore[assignment]
        self.bad, self.all_bad, self.transient_times = set(bad), all_bad, transient_times
        self.blocks: list[list[str]] = []
        self.calls = 0

    def insert(self, rows: Sequence[Sequence[Any]]) -> None:
        self.calls += 1
        if self.transient_times > 0:
            self.transient_times -= 1
            raise SinkError("down", transient=True)
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
    assert archiver._write(rows)  # type: ignore[arg-type]
    assert archiver.metrics.audit_invalid == 1 and archiver.metrics.audit_written == 7
    (aside,) = [json.loads(x) for x in stream.getvalue().splitlines() if '"row_set_aside"' in x]
    assert (aside["table"], aside["partition"], aside["offset"]) == ("audit_log", 7, 99)


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
    assert sink.calls <= 1 + loop.ISOLATION_FAILED_INSERTS + 8, "bounded: not one failing insert per row"


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
