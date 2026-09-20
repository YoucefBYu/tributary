"""Reading the cluster back after a run, for the assertions.

Deliberately assignment-based rather than subscription-based: ``assign`` every
partition at its low watermark and read to the end. A subscribing consumer
would join a group, trigger a rebalance and stop wherever the broker felt like
stopping; here "I have read the whole topic" is a decidable statement, which is
what a no-loss claim needs.

Two things that are easy to get wrong and would quietly invalidate the results:

* **Read to EOF, not to the high watermark.** On a transactional topic the high
  watermark counts commit/abort *control records*, which are never returned to
  the application. Looping until ``offset == high`` therefore never terminates
  on the output of `s5.txn_stage`. ``enable.partition.eof`` makes the broker say
  when a partition is exhausted, which is true regardless of control records.
* **``read_committed``** (the `tbcommon.clients` default) is required for that
  same topic: reading uncommitted would show aborted records and turn the
  exactly-once check into nonsense.

Headers only. `read_all` never decodes a payload (invariant 6); `read_events`
parses the event bodies, which `s5.events` writes and which hold no payload.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import orjson
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from tbcommon import admin, clients
from tbcommon.envelope import H_MSG_ID, H_SEQ, decode


@dataclass(frozen=True, slots=True)
class Readback:
    """One record, reduced to what the analysis may look at."""

    topic: str
    partition: int
    offset: int
    key: str
    headers: dict[str, str]
    size_bytes: int

    @property
    def msg_id(self) -> str:
        return self.headers.get(H_MSG_ID, "")

    @property
    def seq(self) -> int:
        return int(self.headers.get(H_SEQ, "-1"))


def partition_count(topic: str) -> int:
    md = admin.admin().list_topics(topic=topic, timeout=15.0)
    return len(md.topics[topic].partitions)


def watermarks(topic: str) -> dict[int, tuple[int, int]]:
    """partition -> (low, high). The high watermark is the no-loss denominator."""
    consumer = Consumer(
        clients.consumer_config(f"s5-watermark-{time.time_ns()}", read_committed=True)
    )
    try:
        return {
            p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=15.0, cached=False)
            for p in range(partition_count(topic))
        }
    finally:
        consumer.close()


def _eof_consumer(topic: str, name: str, *, read_committed: bool) -> tuple[Consumer, set[int]]:
    """Consumer assigned to every partition at its low watermark, EOF enabled."""
    consumer = Consumer(
        clients.consumer_config(
            name, read_committed=read_committed, **{"enable.partition.eof": True}
        )
    )
    ends = watermarks(topic)
    consumer.assign([TopicPartition(topic, p, low) for p, (low, _high) in sorted(ends.items())])
    return consumer, set(ends)


def _drain(
    consumer: Consumer, topic: str, parts: set[int], deadline: float
) -> Iterator[Any]:
    """Yield records until every assigned partition has reported EOF."""
    at_end: set[int] = set()
    while at_end != parts:
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"reading {topic} timed out; {sorted(parts - at_end)} never reached EOF"
            )
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        err = msg.error()
        if err is not None:
            if err.code() == KafkaError._PARTITION_EOF:  # noqa: SLF001 - librdkafka constant
                at_end.add(msg.partition())
                continue
            raise KafkaException(err)
        at_end.discard(msg.partition())
        yield msg


def read_all(topic: str, *, timeout_s: float = 60.0, read_committed: bool = True) -> list[Readback]:
    """Every record currently in ``topic``, in (partition, offset) order."""
    consumer, parts = _eof_consumer(topic, f"s5-readback-{time.time_ns()}", read_committed=read_committed)
    out: list[Readback] = []
    try:
        for msg in _drain(consumer, topic, parts, time.monotonic() + timeout_s):
            out.append(
                Readback(
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                    key=(msg.key() or b"").decode(errors="replace"),
                    headers=decode(msg.headers()),
                    size_bytes=len(msg.value() or b""),
                )
            )
    finally:
        consumer.close()
    out.sort(key=lambda r: (r.partition, r.offset))
    return out


def committed_offsets(group: str, topic: str, *, timeout: float = 30.0) -> dict[int, int]:
    """partition -> committed offset for ``group``.

    ``Consumer.committed`` does not join the group, so calling this after a run
    cannot itself provoke a rebalance and change what it is measuring.
    """
    consumer = Consumer(clients.consumer_config(group))
    try:
        tps = [TopicPartition(topic, p) for p in range(partition_count(topic))]
        return {tp.partition: tp.offset for tp in consumer.committed(tps, timeout=timeout)}
    finally:
        consumer.close()


def read_events(topic: str, *, timeout_s: float = 30.0) -> list[dict[str, object]]:
    """Lifecycle events, parsed.

    The only place in this module that decodes a record *value*, and it is safe
    because `s5.events` writes the body: ids, stages, sizes and a traceparent,
    never payload bytes. The PHI guard in the integration tests re-checks that
    from the raw bytes rather than trusting this.
    """
    consumer, parts = _eof_consumer(topic, f"s5-events-{time.time_ns()}", read_committed=True)
    out: list[dict[str, object]] = []
    try:
        for msg in _drain(consumer, topic, parts, time.monotonic() + timeout_s):
            try:
                out.append(orjson.loads(msg.value() or b"{}"))
            except orjson.JSONDecodeError:  # pragma: no cover - the producer is ours
                continue
    finally:
        consumer.close()
    return out


def stage_counts(events: list[dict[str, object]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for event in events:
        stage = str(event.get("stage", "?"))
        counts[stage] = counts.get(stage, 0) + 1
    return counts
