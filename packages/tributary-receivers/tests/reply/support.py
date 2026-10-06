"""Doubles for the synchronous-reply tests: a controllable timer, a fake partition source, a fake reply
consumer and records. Synthetic ids only; nothing here is a real patient value."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from tributary.receivers.backpressure import ByteBudget
from tributary.receivers.metrics import Metrics
from tributary.receivers.reply.feed import ReplyFeed, ReplyRecord, ReplySink
from tributary.receivers.reply.pending import Pending, PendingMap
from tributary.receivers.reply.settings import ReplySettings
from tributary.sdk import headers as h
from tributary.sdk.ids import uuid7

#: Stand-ins for PHI. Neither may appear in a header, a log line or a metric.
CONTROL_ID_MARKER = "SYNTH-CTRL-MARKER-77d1"
KEY_MARKER = "SYNTH-KEY-MARKER-31c9"


@dataclass
class FakeTimer:
    delay: float
    callback: Callable[[], None]
    cancelled: bool = False
    fired: bool = False

    def cancel(self) -> None:
        self.cancelled = True


@dataclass
class FakeClock:
    """`schedule` for a `PendingMap`: timers fire only when the test says so."""

    timers: list[FakeTimer] = field(default_factory=list)

    def schedule(self, delay: float, callback: Callable[[], None]) -> FakeTimer:
        timer = FakeTimer(delay, callback)
        self.timers.append(timer)
        return timer

    def live(self) -> list[FakeTimer]:
        return [t for t in self.timers if not t.cancelled and not t.fired]

    def fire_all(self) -> None:
        for timer in list(self.live()):
            timer.fired = True
            timer.callback()


class FakePartitions:
    """A `PartitionSource`: holds whichever partitions the test sets."""

    def __init__(self, *held: int) -> None:
        self.held = list(held)
        self.sinks: list[ReplySink] = []

    def choose_partition(self) -> int | None:
        return self.held[0] if self.held else None

    def attach(self, sink: ReplySink) -> None:
        self.sinks.append(sink)


def record(
    entry: Pending, *, status: str | None = "accepted", code: str | None = None, body: bytes = b""
) -> ReplyRecord:
    assert entry.msg_id is not None
    return ReplyRecord(
        partition=entry.partition,
        correlation_id=entry.correlation_id,
        msg_id=entry.msg_id,
        status=status,
        code=code,
        body=body,
    )


def new_map(
    loop: asyncio.AbstractEventLoop,
    *,
    settings: ReplySettings | None = None,
    partitions: FakePartitions | None = None,
    budget: ByteBudget | None = None,
    metrics: Metrics | None = None,
    clock: FakeClock | None = None,
    **kwargs: Any,
) -> tuple[PendingMap, FakeClock, Metrics, ByteBudget]:
    clock = clock if clock is not None else FakeClock()
    metrics = metrics if metrics is not None else Metrics()
    budget = budget if budget is not None else ByteBudget(1_000_000)
    pending = PendingMap(
        settings if settings is not None else ReplySettings(),
        feed=partitions if partitions is not None else FakePartitions(0),
        loop=loop,
        budget=budget,
        metrics=metrics,
        schedule=clock.schedule,
        **kwargs,
    )
    return pending, clock, metrics, budget


def open_entry(pending: PendingMap, *, connection: object | None = None, msg_id: str | None = None) -> Pending:
    entry = pending.open(sender_control_id=CONTROL_ID_MARKER, connection=connection if connection else object())
    assert isinstance(entry, Pending), entry
    entry.bind(msg_id if msg_id is not None else str(uuid7()))
    return entry


# -- a fake reply consumer -----------------------------------------------------------------------------


@dataclass
class FakeTopicPartition:
    topic: str
    partition: int
    offset: int = -1001


class FakeMessage:
    def __init__(
        self,
        *,
        partition: int = 0,
        offset: int = 0,
        headers: list[tuple[str, bytes | None]] | None = None,
        value: bytes | None = b"",
        error: Any = None,
        topic: str = "dev.lab.reply",
    ) -> None:
        self._partition, self._offset, self._headers = partition, offset, headers
        self._value, self._error, self._topic = value, error, topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def topic(self) -> str:
        return self._topic

    def headers(self) -> list[tuple[str, bytes | None]] | None:
        return self._headers

    def value(self) -> bytes | None:
        return self._value

    def error(self) -> Any:
        return self._error

    def key(self) -> bytes | None:  # never read by the feed; present so a read would not crash a test
        return KEY_MARKER.encode()


class FakeKafkaError:
    def __init__(self, name: str) -> None:
        self._name = name

    def name(self) -> str:
        return self._name


def reply_message(
    corr: str, msg_id: str, *, status: str | None = "accepted", code: str | None = None, **kw: Any
) -> FakeMessage:
    headers: list[tuple[str, bytes | None]] = [
        (h.TB_CORRELATION_ID, corr.encode()),
        (h.TB_MSG_ID, msg_id.encode()),
    ]
    if status is not None:
        headers.append((h.TB_REPLY_STATUS, status.encode()))
    if code is not None:
        headers.append((h.TB_ERROR_CODE, code.encode()))
    return FakeMessage(headers=headers, **kw)


class FakeConsumer:
    """The slice of `confluent_kafka.Consumer` a `ReplyFeed` uses. `rebalance_assign` and `rebalance_revoke`
    run the callbacks the way librdkafka does, from inside `poll`'s thread (here: the caller's)."""

    def __init__(self, *, watermarks: dict[int, int] | None = None) -> None:
        self.watermarks = watermarks if watermarks is not None else {}
        self.watermark_failures: set[int] = set()
        self.messages: list[Any] = []
        self.assigned: list[FakeTopicPartition] = []
        self.seeks: list[tuple[int, int]] = []
        self.closed = False
        self.list_topics_ok = True
        self.callbacks: dict[str, Any] = {}
        self.subscribed: list[str] = []

    def subscribe(self, topics: list[str], on_assign: Any = None, on_revoke: Any = None, on_lost: Any = None) -> None:
        self.subscribed = topics
        self.callbacks = {"assign": on_assign, "revoke": on_revoke, "lost": on_lost}

    def poll(self, timeout: float) -> Any:
        return self.messages.pop(0) if self.messages else None

    def incremental_assign(self, partitions: list[Any]) -> None:
        self.assigned.extend(partitions)

    def incremental_unassign(self, partitions: list[Any]) -> None:
        gone = {p.partition for p in partitions}
        self.assigned = [p for p in self.assigned if p.partition not in gone]

    def get_watermark_offsets(self, partition: Any, timeout: float, cached: bool) -> tuple[int, int]:
        if partition.partition in self.watermark_failures:
            raise RuntimeError("watermark unavailable")
        return 0, self.watermarks.get(partition.partition, 0)

    def seek(self, partition: Any) -> None:
        self.seeks.append((partition.partition, partition.offset))

    def list_topics(self, topic: str | None, timeout: float) -> Any:
        if not self.list_topics_ok:
            raise RuntimeError("no brokers")
        return object()

    def close(self) -> None:
        self.closed = True

    def rebalance_assign(self, *partitions: int) -> None:
        self.callbacks["assign"](self, [FakeTopicPartition("dev.lab.reply", p) for p in partitions])

    def rebalance_revoke(self, *partitions: int, lost: bool = False) -> None:
        self.callbacks["lost" if lost else "revoke"](self, [FakeTopicPartition("dev.lab.reply", p) for p in partitions])


class RecordingLog:
    """A `Logger` that keeps what was said, to assert on."""

    def __init__(self) -> None:
        self.lines: list[tuple[str, str, dict[str, object]]] = []

    def _add(self, level: str, event: str, fields: dict[str, object]) -> None:
        self.lines.append((level, event, fields))

    def debug(self, event: str, /, **fields: object) -> None:
        self._add("debug", event, fields)

    def info(self, event: str, /, **fields: object) -> None:
        self._add("info", event, fields)

    def warning(self, event: str, /, **fields: object) -> None:
        self._add("warning", event, fields)

    def error(self, event: str, /, **fields: object) -> None:
        self._add("error", event, fields)

    def text(self) -> str:
        return repr(self.lines)

    def events(self) -> list[str]:
        return [event for _level, event, _fields in self.lines]


def immediate_post(fn: Callable[..., None], *args: Any) -> None:
    fn(*args)


def new_feed(
    consumer: FakeConsumer, *, metrics: Metrics | None = None, log: RecordingLog | None = None, slots: int = 8
) -> tuple[ReplyFeed, Metrics, RecordingLog]:
    metrics = metrics if metrics is not None else Metrics()
    log = log if log is not None else RecordingLog()
    feed = ReplyFeed(
        consumer, "dev.lab.reply", log=log, metrics=metrics, slots=slots, post=immediate_post, poll_timeout_s=0.0
    )
    consumer.subscribe([feed._topic], on_assign=feed._on_assign, on_revoke=feed._on_revoke, on_lost=feed._on_revoke)
    return feed, metrics, log
