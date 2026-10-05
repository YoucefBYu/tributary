"""The consume, insert, commit loop against fakes (no Kafka, no ClickHouse)."""

from __future__ import annotations

import io
import json
from collections.abc import Sequence
from typing import Any

import orjson
import pytest
from confluent_kafka import KafkaError, KafkaException, TopicPartition
from tributary.archiver.archiver import Archiver, ArchiverSettings
from tributary.archiver.errors import SinkError
from tributary.archiver.testing import sample_event as base
from tributary.runtime.logs import JsonLogger


class FakeKafkaError:
    def __init__(self, code: int, *, fatal: bool = False) -> None:
        self._code, self._fatal = code, fatal

    def code(self) -> int:
        return self._code

    def fatal(self) -> bool:
        return self._fatal

    def name(self) -> str:
        return "FAKE"


class FakeMessage:
    def __init__(self, offset: int, value: bytes | None, *, partition: int = 0, error: Any = None) -> None:
        self._o, self._v, self._p, self._e = offset, value, partition, error

    def error(self) -> Any:
        return self._e

    def topic(self) -> str:
        return "_tb.dev.events"

    def partition(self) -> int:
        return self._p

    def offset(self) -> int:
        return self._o

    def value(self) -> bytes | None:
        return self._v


class FakeConsumer:
    def __init__(self, batches: list[list[FakeMessage]]) -> None:
        self.batches = batches
        self.commits: list[list[tuple[int, int]]] = []
        self.paused: list[list[TopicPartition]] = []
        self.resumed: list[list[TopicPartition]] = []
        self.polls = 0
        self.assigned = [TopicPartition("_tb.dev.events", 0)]
        self.seeks: list[tuple[int, int]] = []
        self.calls: list[str] = []
        self.on_poll: Any = None
        self.commit_error: Exception | None = None

    def consume(self, num_messages: int = 1, timeout: float = 0) -> Sequence[Any]:
        self.calls.append("consume")
        return self.batches.pop(0) if self.batches else []

    def poll(self, timeout: float = 0) -> Any:
        self.polls += 1
        return self.on_poll(self) if self.on_poll else None

    def seek(self, partition: TopicPartition) -> None:
        self.seeks.append((partition.partition, partition.offset))

    def commit(self, *, offsets: list[TopicPartition], asynchronous: bool) -> Any:
        assert asynchronous is False
        if self.commit_error:
            raise self.commit_error
        self.calls.append("commit")
        self.commits.append(sorted((tp.partition, tp.offset) for tp in offsets))

    def assignment(self) -> list[TopicPartition]:
        return list(self.assigned)

    def pause(self, partitions: list[TopicPartition]) -> None:
        self.paused.append(partitions)

    def resume(self, partitions: list[TopicPartition]) -> None:
        self.resumed.append(partitions)

    def position(self, partitions: list[TopicPartition]) -> list[TopicPartition]:
        return [TopicPartition("_tb.dev.events", 0, 5)]

    def get_watermark_offsets(
        self, partition: TopicPartition, timeout: float = 0, cached: bool = False
    ) -> tuple[int, int]:
        return (0, 9)

    def close(self) -> None:
        pass


class FakeSink:
    def __init__(self, fail_times: int = 0, *, transient: bool = True) -> None:
        self.fail_times = fail_times
        self.transient = transient
        self.inserted: list[list[Any]] = []
        self.calls = 0

    def insert(self, rows: Sequence[Sequence[Any]]) -> None:
        self.calls += 1
        if self.calls <= self.fail_times:
            raise SinkError("clickhouse insert failed (X)", transient=self.transient)
        self.inserted.extend(list(r) for r in rows)

    def close(self) -> None:
        pass


def event(offset: int, **over: Any) -> FakeMessage:
    return FakeMessage(offset, orjson.dumps(base(**over)))


def make(consumer: FakeConsumer, sink: FakeSink, stream: io.StringIO | None = None) -> Archiver:
    return Archiver(
        consumer,
        sink,
        log=JsonLogger("archiver-test", stream=stream or io.StringIO()),
        settings=ArchiverSettings(backoff_initial_s=0.01, backoff_max_s=0.02),
        sleep=lambda _s: None,
    )


def test_commit_follows_a_successful_insert_with_next_offset() -> None:
    consumer, sink = FakeConsumer([[event(4), event(5), event(6)]]), FakeSink()
    archiver = make(consumer, sink)
    assert archiver.run_once() == 3
    assert len(sink.inserted) == 3
    assert consumer.commits == [[(0, 7)]]
    assert archiver.metrics.rows_written == 3 and archiver.metrics.batches_written == 1


def test_clickhouse_down_pauses_retries_the_same_batch_and_commits_only_after_success() -> None:
    consumer, sink = FakeConsumer([[event(0), event(1)]]), FakeSink(fail_times=3)
    archiver = make(consumer, sink)
    archiver.run_once()
    assert sink.calls == 4
    assert len(sink.inserted) == 2, "the same batch, once"
    assert consumer.commits == [[(0, 2)]]
    assert len(consumer.paused) == 1 and len(consumer.resumed) == 1
    assert consumer.polls > 0, "the group is serviced during the outage"
    assert archiver.metrics.insert_failures == 3


def test_stop_during_an_outage_commits_nothing() -> None:
    consumer, sink = FakeConsumer([[event(0)]]), FakeSink(fail_times=10**6)
    archiver = make(consumer, sink)
    original = consumer.poll

    def stopping(timeout: float = 0) -> Any:
        archiver.stop()
        return original(timeout)

    consumer.poll = stopping  # type: ignore[method-assign]
    archiver.run_once()
    assert consumer.commits == []
    assert sink.inserted == []


def test_poison_records_are_skipped_and_committed_not_retried() -> None:
    consumer = FakeConsumer([[FakeMessage(0, b"garbage"), event(1), FakeMessage(2, None)]])
    sink, stream = FakeSink(), io.StringIO()
    archiver = make(consumer, sink, stream)
    archiver.run_once()
    assert len(sink.inserted) == 1
    assert consumer.commits == [[(0, 3)]]
    assert archiver.metrics.events_rejected == 2
    lines = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert {line["code"] for line in lines if line["event"] == "event_rejected"} == {"EVENT_NOT_JSON", "EVENT_EMPTY"}


def test_all_poison_batch_still_commits() -> None:
    consumer = FakeConsumer([[FakeMessage(0, b"x")]])
    make(consumer, FakeSink()).run_once()
    assert consumer.commits == [[(0, 1)]]


def test_logs_carry_no_event_field_values() -> None:
    marker = "SYNTH-LOG-MARKER"
    stream = io.StringIO()
    consumer = FakeConsumer([[FakeMessage(0, orjson.dumps(base(message_id=marker))), event(1, channel=marker.lower())]])
    make(consumer, FakeSink(fail_times=1), stream).run_once()
    assert marker not in stream.getvalue() and marker.lower() not in stream.getvalue()


def test_commit_failure_is_counted_not_fatal() -> None:
    consumer = FakeConsumer([[event(0)]])
    consumer.commit_error = KafkaException(KafkaError(KafkaError._TRANSPORT))
    archiver = make(consumer, FakeSink())
    archiver.run_once()
    assert archiver.metrics.commit_failures == 1 and archiver.metrics.rows_written == 1


def test_partition_eof_is_ignored_and_fatal_raises() -> None:
    eof = FakeMessage(0, None, error=FakeKafkaError(-191))
    consumer = FakeConsumer([[eof]])
    assert make(consumer, FakeSink()).run_once() == 0
    fatal = FakeConsumer([[FakeMessage(0, None, error=FakeKafkaError(-1, fatal=True))]])
    with pytest.raises(KafkaException):
        make(fatal, FakeSink()).run_once()


def test_ingest_lag_is_reported_and_attrs_drops_are_counted() -> None:
    consumer = FakeConsumer([[event(0, ts="2020-01-01T00:00:00.000Z", attrs={"mrn_hmac": "a" * 64, "t": "A01"})]])
    archiver = make(consumer, FakeSink())
    archiver.run_once()
    assert archiver.metrics.ingest_lag_ms > 1000 * 3600 * 24
    assert archiver.metrics.attrs_dropped == 1


def test_kafka_lag_figure() -> None:
    archiver = make(FakeConsumer([]), FakeSink())
    assert archiver._kafka_lag() == 4


def test_partition_gained_mid_outage_is_paused_and_a_stray_record_is_rewound_not_lost() -> None:
    consumer, sink = FakeConsumer([[event(0)]]), FakeSink(fail_times=3)
    gained = TopicPartition("_tb.dev.events", 1)
    stray = FakeMessage(41, orjson.dumps(base()), partition=1)
    fired: list[bool] = []

    def on_poll(c: FakeConsumer) -> Any:
        if not fired:  # an incremental assign lands during the outage and delivers before it is paused
            fired.append(True)
            c.assigned.append(gained)
            return stray
        return None

    consumer.on_poll = on_poll
    archiver = make(consumer, sink)
    archiver.run_once()
    paused_partitions = {tp.partition for call in consumer.paused for tp in call}
    assert paused_partitions == {0, 1}, "the new partition is paused on the next backoff iteration"
    assert consumer.seeks == [(1, 41)], "the record that slipped out is read again, not dropped"
    assert consumer.commits == [[(0, 1)]], "the commit covers only the batch that was written"
    assert archiver.metrics.stray_records_rewound == 1
    assert {tp.partition for tp in consumer.resumed[-1]} == {0, 1}


def test_a_permanent_failure_halts_without_dropping_or_committing_and_recovers() -> None:
    consumer, sink, stream = FakeConsumer([[event(0)]]), FakeSink(fail_times=5, transient=False), io.StringIO()
    archiver = make(consumer, sink, stream)
    archiver.run_once()
    assert archiver.metrics.insert_failures_permanent == 5
    assert archiver.metrics.batches_halted == 1, "one halt per batch, not one per retry"
    assert len(sink.inserted) == 1 and consumer.commits == [[(0, 1)]], "kept the batch, committed after recovery"
    halted = [json.loads(x) for x in stream.getvalue().splitlines() if '"insert_halted"' in x]
    assert len(halted) == 1 and halted[0]["level"] == "error"


def test_a_permanent_failure_with_stop_commits_nothing() -> None:
    consumer, sink = FakeConsumer([[event(0)]]), FakeSink(fail_times=10**6, transient=False)
    archiver = make(consumer, sink)
    consumer.on_poll = lambda _c: archiver.stop()
    archiver.run_once()
    assert consumer.commits == [] and sink.inserted == []


def test_each_batch_is_committed_before_the_next_consume_so_a_revoke_never_strands_rows() -> None:
    """No rebalance callback exists; `run_once` is synchronous, so flush-on-revoke holds by construction."""
    consumer = FakeConsumer([[event(0)], [event(1)]])
    archiver = make(consumer, FakeSink())
    archiver.run_once()
    archiver.run_once()
    assert consumer.calls == ["consume", "commit", "consume", "commit"]


class _LeakyClient:
    """A driver client whose exception text quotes a row value, as ClickHouse server errors can."""

    def __init__(self, marker: str, *, fail: int) -> None:
        self.marker, self.fail = marker, fail

    def insert(self, *_a: Any, **_k: Any) -> None:
        if self.fail > 0:
            self.fail -= 1
            raise RuntimeError(f"Code: 53. Cannot parse value '{self.marker}' for column mrn")

    def close(self) -> None:
        pass


def test_driver_exception_text_never_reaches_the_log_and_every_line_matches_the_log_schema() -> None:
    from pathlib import Path

    import jsonschema
    from tributary.archiver.sink import ClickhouseSink

    schema = json.loads(
        (Path(__file__).resolve().parents[3] / "contracts/observability/v0/log-line.schema.json").read_text()
    )
    marker = "SYNTH-PHI-MARKER-4471"
    stream = io.StringIO()
    # Code 53 TYPE_MISMATCH is classified permanent, so this run exercises insert_halted too.
    consumer = FakeConsumer([[event(0), FakeMessage(1, b"junk")]])
    archiver = Archiver(
        consumer,
        ClickhouseSink(_LeakyClient(marker, fail=2)),
        log=JsonLogger("archiver", stream=stream),
        settings=ArchiverSettings(backoff_initial_s=0.01, backoff_max_s=0.02, stats_interval_s=0.0),
        sleep=lambda _s: None,
    )
    archiver.run_once()
    out = stream.getvalue()
    assert marker not in out and "Cannot parse" not in out
    lines = [json.loads(x) for x in out.splitlines()]
    events = {x["event"] for x in lines}
    assert {"insert_halted", "event_rejected", "archiver_stats"} <= events
    for line in lines:
        jsonschema.validate(line, schema)
