"""Backfill termination and batching against a fake consumer (no Kafka)."""

from __future__ import annotations

from typing import Any

from tributary.archiver import content_backfill as cb
from tributary.archiver.content import ContentRow

EOF = -191


class Err:
    def code(self) -> int:
        return EOF


class Msg:
    def __init__(self, partition: int, offset: int, value: bytes | None = b"x", eof: bool = False) -> None:
        self._p, self._o, self._v, self._eof = partition, offset, value, eof

    def error(self) -> Any:
        return Err() if self._eof else None

    def partition(self) -> int:
        return self._p

    def offset(self) -> int:
        return self._o

    def value(self) -> bytes | None:
        return self._v


class FakeConsumer:
    """Partition 0 holds data at offsets 0-1 and a non-data record at 2 (high watermark 3)."""

    def __init__(self, scripted: list[list[Msg]], high: int = 3) -> None:
        self.scripted, self.high = scripted, high
        self.paused: list[Any] = []
        self.calls = 0

    def list_topics(self, topic: str, timeout: float) -> Any:
        entry = type("E", (), {"error": None, "partitions": {0: None}})()
        return type("M", (), {"topics": {topic: entry}})()

    def get_watermark_offsets(self, tp: Any, timeout: float) -> tuple[int, int]:
        return 0, self.high

    def assign(self, tps: list[Any]) -> None: ...

    def pause(self, tps: list[Any]) -> None:
        self.paused.extend(tps)

    def consume(self, num_messages: int, timeout: float) -> list[Msg]:
        self.calls += 1
        return self.scripted.pop(0) if self.scripted else []


class Sink:
    def __init__(self) -> None:
        self.batches: list[int] = []

    def insert(self, rows: Any) -> None:
        self.batches.append(len(rows))


def build(message: Any) -> ContentRow:
    return ContentRow(values=(message.offset(),), ts=None)  # type: ignore[arg-type]


def test_a_partition_ending_in_a_non_data_record_finishes_on_eof_not_on_the_deadline() -> None:
    clock = iter(range(0, 10_000))
    consumer = FakeConsumer([[Msg(0, 0), Msg(0, 1)], [Msg(0, 0, eof=True)]])
    sink = Sink()
    result = cb.backfill_channel(
        consumer, sink, build, env="dev", channel="alpha", deadline_s=5.0, clock=lambda: float(next(clock))
    )  # type: ignore[arg-type]
    assert (result.scanned, result.stored) == (2, 2) and sink.batches == [2]
    assert consumer.paused, "the finished partition is paused"


def test_without_eof_the_deadline_still_stops_it() -> None:
    import pytest

    clock = iter(range(0, 10_000))
    consumer = FakeConsumer([[Msg(0, 0), Msg(0, 1)]])
    with pytest.raises(cb.BackfillError) as raised:
        cb.backfill_channel(
            consumer, Sink(), build, env="dev", channel="alpha", deadline_s=3.0, clock=lambda: float(next(clock))
        )  # type: ignore[arg-type]
    assert raised.value.code == "SOURCE_TIMEOUT"


def test_batches_are_flushed_by_bytes() -> None:
    big = b"x" * (cb.MAX_BATCH_BYTES // 2 + 1)
    consumer = FakeConsumer([[Msg(0, 0, big), Msg(0, 1, big)]], high=2)
    sink = Sink()
    cb.backfill_channel(consumer, sink, build, env="dev", channel="alpha")  # type: ignore[arg-type]
    assert sink.batches == [2], "flushed once the byte cap was reached, not only at the row cap"
