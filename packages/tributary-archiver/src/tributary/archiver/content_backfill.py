"""Backfill `message_content` from a channel's `raw` topic (ADR-004 A6).

Re-reads `<env>.<channel>.raw` with `assign` (no group membership, no commits, no rebalance) from the earliest
retained offset, or from the first record at or after `since`, up to the high watermark when the run starts, and
writes through the same row builder and idempotent key as the live content archiver. **A second run changes
nothing**: rows carry the Kafka record timestamp as their version, so the replacing table collapses them (count
with `FINAL`). Only the `received` stage can be rebuilt (it is the only one `raw` holds).

**What is lost if retention lapses first.** `raw` keeps 7 days by default (`topics/v0` 2). Records older than the
log start are gone from Kafka and, if content was not stored while they were live, from everywhere. The event
timeline stays; the payload does not. Backfill reads from Kafka only; it cannot recover them.
"""

from __future__ import annotations

import contextlib
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Final

from confluent_kafka import KafkaException, TopicPartition
from tributary.runtime.topics import raw_topic

from .content import ContentRow
from .errors import EventRejected, SinkError
from .sink import Sink

_PARTITION_EOF: Final = -191
BATCH: Final = 2000
CHUNK: Final = 50  # messages per consume call, so a batch of ~1 MiB records stays bounded
MAX_BATCH_BYTES: Final = 64 * 1024 * 1024

RowBuilder = Callable[[Any], ContentRow]


@dataclass(frozen=True, slots=True)
class BackfillResult:
    scanned: int
    stored: int
    rejected: int


class BackfillError(SinkError):
    """The raw topic could not be read in time. The code is fixed text; no record content."""


def backfill_channel(
    consumer: Any,
    sink: Sink,
    build: RowBuilder,
    *,
    env: str,
    channel: str,
    since_ms: int | None = None,
    deadline_s: float = 600.0,
    clock: Callable[[], float] = time.monotonic,
) -> BackfillResult:
    topic = raw_topic(env, channel)
    try:
        metadata = consumer.list_topics(topic, timeout=30)
        entry = metadata.topics.get(topic)
        if entry is None or entry.error is not None:
            raise BackfillError("raw topic not found", transient=False, code="TOPIC_NOT_FOUND")
        partitions = sorted(entry.partitions)
        starts: dict[int, int] = {}
        ends: dict[int, int] = {}
        for partition in partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=30)
            start = low
            if since_ms is not None:
                (found,) = consumer.offsets_for_times([TopicPartition(topic, partition, since_ms)], timeout=30)
                start = high if found.offset < 0 else max(found.offset, low)
            starts[partition], ends[partition] = start, high
    except KafkaException:
        raise BackfillError("raw topic metadata unavailable", code="SOURCE_ERROR") from None

    pending = {p for p in partitions if starts[p] < ends[p]}
    if not pending:
        return BackfillResult(0, 0, 0)
    consumer.assign([TopicPartition(topic, p, starts[p]) for p in pending])
    scanned = stored = rejected = 0
    rows: list[ContentRow] = []
    give_up = clock() + deadline_s
    held = 0

    def flush() -> None:
        nonlocal stored, held
        if rows:
            sink.insert([row.values for row in rows])
            stored += len(rows)
            rows.clear()
        held = 0

    def finish(partition: int) -> None:
        pending.discard(partition)
        with contextlib.suppress(KafkaException):
            consumer.pause([TopicPartition(topic, partition)])

    while pending:
        if clock() > give_up:
            raise BackfillError("backfill deadline exceeded", code="SOURCE_TIMEOUT")
        for message in consumer.consume(num_messages=CHUNK, timeout=1.0):
            error = message.error()
            if error is not None:
                if error.code() == _PARTITION_EOF:
                    # The log ends here. A partition whose last offsets are not data (a transaction marker, a
                    # gap) never reaches `end - 1` as a record, so EOF is what tells us it is done.
                    finish(message.partition())
                    continue
                raise BackfillError("kafka error while reading raw", code="SOURCE_ERROR")
            partition = message.partition()
            if partition not in pending:
                continue
            if message.offset() >= ends[partition]:
                finish(partition)
                continue
            scanned += 1
            try:
                rows.append(build(message))
                held += len(message.value() or b"")
            except EventRejected:
                rejected += 1
            if message.offset() + 1 >= ends[partition]:
                finish(partition)
            if len(rows) >= BATCH or held >= MAX_BATCH_BYTES:
                flush()
    flush()
    return BackfillResult(scanned, stored, rejected)
