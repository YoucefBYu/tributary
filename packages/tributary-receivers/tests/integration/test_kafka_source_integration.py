"""Kafka source receiver against real Kafka: the source offset is committed only after the `raw` produce
is acknowledged; an unavailable `raw` cluster commits nothing; a refused record halts its partition.

The source connection is configured explicitly (ADR-007 D11) even though the test's source and engine
clusters are the same container: the receiver is given the source side through `source_bootstrap` and the
engine side through the `KafkaSettings` given to `RawPublisher`, and the unavailable-Kafka test points only the latter at a dead
port, which is what proves the two are separate connections.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import replace
from typing import Any

import pytest
from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import NewTopic
from tributary.receivers.kafka_source.config import KafkaSourceSettings, SourceSecurity
from tributary.receivers.kafka_source.receiver import KafkaSourceReceiver, build_consumer
from tributary.receivers.mllp.kafka_publisher import RawPublisher, topic_name
from tributary.receivers.secrets import DirSecretResolver
from tributary.runtime.clients import KafkaSettings
from tributary.sdk.keys import parse_key_expression

from tests.support.kafka_helpers import create_topics_with_retry, drain_topic, free_port, header_map

pytestmark = pytest.mark.integration

ENV = "dev"


def cfg_for(source: str, engine: str, channel: str, topic: str, **kw: object) -> KafkaSourceSettings:
    base = KafkaSourceSettings(
        channel=channel,
        channel_version=1,
        content_type="application/json",
        source_bootstrap=(source,),
        security=SourceSecurity("PLAINTEXT"),  # loopback only, as the contract requires
        topic=topic,
        start_from="earliest",
        nack_on=frozenset(),
        key_expression=parse_key_expression("record:key"),
        ordering="per-key",
        max_message_bytes=1_000,
        ack_timeout_s=kw.pop("ack_timeout_s", 15.0),  # type: ignore[arg-type]
        env=ENV,
        instance=f"kafka-{uuid.uuid4().hex[:6]}",
        commit_every=2,
        commit_flush_interval_s=0.5,
        poll_timeout_s=0.5,
        unavailable_backoff_initial_s=0.2,
        unavailable_backoff_max_s=0.5,
    )
    return replace(base, **kw)  # type: ignore[arg-type]


def produce(bootstrap: str, topic: str, records: list[tuple[bytes, bytes]]) -> None:
    p = Producer({"bootstrap.servers": bootstrap, "acks": "all"})
    for key, value in records:
        p.produce(topic, key=key, value=value, partition=0)
    assert p.flush(30) == 0


#: Group-coordinator races on a fresh broker or group: the call is fine to repeat once the coordinator settles.
#: Same rule as `kafka_call_with_retry` in packages/tributary-archiver/tests/conftest.py.
RETRY_CODES = frozenset({"NOT_COORDINATOR", "COORDINATOR_LOAD_IN_PROGRESS"})


def _retriable(exc: KafkaException) -> bool:
    error: Any = exc.args[0] if exc.args else None
    return error is not None and (
        bool(getattr(error, "retriable", lambda: False)()) or str(error.name()) in RETRY_CODES
    )


def committed(bootstrap: str, group: str, topic: str, *, timeout_s: float = 15.0, pause_s: float = 0.25) -> int:
    """The group's committed offset, retrying a retriable or coordinator error within `timeout_s` (the old single
    call's budget); anything else, or the error at the deadline, is re-raised."""
    deadline = time.monotonic() + timeout_s
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": group, "enable.auto.commit": False})
    try:
        while True:
            try:
                (tp,) = c.committed([TopicPartition(topic, 0)], timeout=max(deadline - time.monotonic(), 0.1))
                return int(tp.offset)  # negative (-1001) when nothing was ever committed
            except KafkaException as exc:
                if not _retriable(exc) or time.monotonic() + pause_s >= deadline:
                    raise
                time.sleep(pause_s)
    finally:
        c.close()


async def run_for(rx: KafkaSourceReceiver, seconds: float) -> None:
    stop = asyncio.Event()
    task = asyncio.create_task(rx.run(stop))
    await asyncio.sleep(seconds)
    stop.set()
    await asyncio.wait_for(task, 30)


def wait_until(predicate, timeout_s: float = 30.0) -> None:  # type: ignore[no-untyped-def]
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.2)
    raise AssertionError("condition not reached")


def test_source_offset_advances_only_for_records_committed_to_raw(kafka: str) -> None:
    src, channel = "ext.orders.v1", "ksrc-commit"
    create_topics_with_retry(kafka, [NewTopic(src, 1, 1), NewTopic(topic_name(ENV, channel), 1, 1)])
    produce(kafka, src, [(f"K{i}".encode(), f'{{"n": {i}}}'.encode()) for i in range(5)])
    cfg = cfg_for(kafka, kafka, channel, src)

    async def go() -> KafkaSourceReceiver:
        publisher = RawPublisher(
            KafkaSettings(kafka), topic_name(ENV, channel), ordering="per-key", client_id_prefix="ksrc-test"
        )
        publisher.start(asyncio.get_running_loop())
        rx = KafkaSourceReceiver(cfg, publisher, build_consumer(cfg, DirSecretResolver()))
        try:
            await run_for(rx, 8)
        finally:
            publisher.close()
        return rx

    rx = asyncio.run(go())
    records = drain_topic(kafka, topic_name(ENV, channel), partitions=1, expected=5, timeout_s=30)
    assert [r.key() for r in records] == [f"K{i}".encode() for i in range(5)]  # in order, keyed by record:key
    assert all(header_map(r)["tb-channel"] == channel for r in records)
    # Every source offset committed is covered by a record that is already in raw.
    assert committed(kafka, cfg.group_id, src) == 5 == len(records)
    assert rx.metrics.acked == 5 and rx.metrics.partitions_halted == 0


def test_unreachable_raw_cluster_commits_no_source_offset(kafka: str) -> None:
    src, channel = "ext.blocked.v1", "ksrc-down"
    create_topics_with_retry(kafka, [NewTopic(src, 1, 1)])
    produce(kafka, src, [(b"K", b'{"n": 1}'), (b"K", b'{"n": 2}')])
    dead = f"127.0.0.1:{free_port()}"
    cfg = cfg_for(kafka, dead, channel, src, ack_timeout_s=1.0)

    async def go() -> KafkaSourceReceiver:
        publisher = RawPublisher(
            KafkaSettings(dead), topic_name(ENV, channel), ordering="per-key", client_id_prefix="ksrc-test"
        )
        publisher.start(asyncio.get_running_loop())
        rx = KafkaSourceReceiver(cfg, publisher, build_consumer(cfg, DirSecretResolver()))
        try:
            await run_for(rx, 8)
        finally:
            publisher.close(timeout_s=0.5)
        return rx

    rx = asyncio.run(go())
    assert rx.metrics.acked == 0 and rx.metrics.nacked_unavailable >= 2  # retried after backoff, still refused
    assert committed(kafka, cfg.group_id, src) < 0  # nothing committed: the records will be re-read


def test_refused_record_halts_the_partition_and_later_records_are_not_committed_or_published(kafka: str) -> None:
    src, channel = "ext.halt.v1", "ksrc-halt"
    create_topics_with_retry(kafka, [NewTopic(src, 1, 1), NewTopic(topic_name(ENV, channel), 1, 1)])
    good = b'{"ok": true}'
    produce(kafka, src, [(b"A", good), (b"B", good), (b"C", b"x" * 2000), (b"D", good), (b"E", good)])
    cfg = cfg_for(kafka, kafka, channel, src)  # max_message_bytes=1000: record C is oversize

    async def go() -> tuple[KafkaSourceReceiver, dict[int, int]]:
        publisher = RawPublisher(
            KafkaSettings(kafka), topic_name(ENV, channel), ordering="per-key", client_id_prefix="ksrc-test"
        )
        publisher.start(asyncio.get_running_loop())
        rx = KafkaSourceReceiver(cfg, publisher, build_consumer(cfg, DirSecretResolver()))
        stop = asyncio.Event()
        task = asyncio.create_task(rx.run(stop))
        try:
            deadline = time.monotonic() + 30
            while not rx.halted and time.monotonic() < deadline:
                await asyncio.sleep(0.1)
            halted = dict(rx.halted)  # closing the consumer revokes the partition and clears this
            await asyncio.sleep(2)  # give it every chance to (wrongly) carry on past the halted record
        finally:
            stop.set()
            await asyncio.wait_for(task, 30)
            publisher.close()
        return rx, halted

    rx, halted = asyncio.run(go())
    assert halted == {0: 2} and rx.metrics.partitions_halted == 1
    records = drain_topic(kafka, topic_name(ENV, channel), partitions=1, expected=2, timeout_s=20)
    assert [r.key() for r in records] == [b"A", b"B"]  # nothing at or past the halted offset reached raw
    assert committed(kafka, cfg.group_id, src) == 2  # committed up to, never past, the refused record
