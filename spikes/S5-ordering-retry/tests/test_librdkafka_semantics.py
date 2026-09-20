"""Pin the client semantics the whole spike rests on, against a real broker.

`tests/test_fakekafka.py` asserts that `s5.fakekafka` models these behaviours.
This file asserts that the behaviours are real -- Apache Kafka 4.0.0 with
librdkafka 2.15.1. If any of these fails, every ordering property proved on top
of the model is worth nothing, so they are separate from the scenario tests and
deliberately tiny: one topic, one partition, four records.

The first two are the reason `BlockingLoader` seeks before resuming. Test one
reproduces *silent data loss* on purpose; if it ever starts failing, librdkafka
changed its prefetch behaviour and the loader's seek became unnecessary (but
still harmless).
"""

from __future__ import annotations

import time
from collections.abc import Iterator

import pytest
from confluent_kafka import Consumer, KafkaException, TopicPartition
from tbcommon import admin, clients, config
from tbcommon.envelope import Envelope

pytestmark = [pytest.mark.integration, pytest.mark.timeout(300)]

RECORDS = 4


@pytest.fixture
def topic() -> Iterator[str]:
    """A fresh single-partition topic holding four keyed records."""
    name = f"{config.topic_prefix()}.s5.semantics-{time.strftime('%H%M%S')}-{time.time_ns() % 1000}"
    admin.wait_for_cluster()
    admin.ensure_topics([admin.TopicSpec(name, partitions=1, replication_factor=3)])
    producer = clients.make_producer()
    for seq in range(RECORDS):
        producer.produce(
            name,
            key=b"SYNTH-MRN-000001",
            value=b"x" * 16,
            headers=Envelope(channel="s5", seq=seq).to_headers(),
        )
    clients.flush_or_raise(producer)
    try:
        yield name
    finally:
        admin.delete_topics([name])


def _assigned_consumer(topic: str, group: str) -> Consumer:
    consumer = clients.make_consumer(group, [topic])
    deadline = time.monotonic() + 60.0
    while time.monotonic() < deadline:
        consumer.poll(0.2)
        if consumer.assignment():
            return consumer
    consumer.close()
    raise TimeoutError(f"never got an assignment for {topic}")


def _poll_record(consumer: Consumer, timeout_s: float = 20.0) -> object | None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        msg = consumer.poll(0.5)
        if msg is None:
            continue
        if msg.error() is not None:
            continue
        return msg
    return None


def test_resume_without_seek_skips_the_failed_message(topic: str) -> None:
    """pause() purges the prefetch queue; resume() refetches last-consumed + 1.

    So a loader that pauses on a failed message and later resumes without
    seeking back never sees that message again. This test exists to prove the
    trap is real rather than folklore -- it is the single behaviour that would
    have made `blocking` retry silently lose data.
    """
    consumer = _assigned_consumer(topic, f"s5-sem-skip-{time.time_ns()}")
    try:
        first = _poll_record(consumer)
        assert first is not None and first.offset() == 0  # type: ignore[attr-defined]

        consumer.pause([TopicPartition(topic, 0)])
        assert _poll_record(consumer, timeout_s=3.0) is None, "a paused partition still delivered"

        consumer.resume([TopicPartition(topic, 0)])
        after = _poll_record(consumer)
        assert after is not None
        assert after.offset() == 1, (  # type: ignore[attr-defined]
            "librdkafka no longer skips on resume; the loader's seek is now redundant"
        )
    finally:
        consumer.close()


def test_seek_before_resume_replays_the_failed_message(topic: str) -> None:
    """The fix. Seek while still paused, then resume: the failed offset comes back."""
    consumer = _assigned_consumer(topic, f"s5-sem-seek-{time.time_ns()}")
    try:
        failed = _poll_record(consumer)
        assert failed is not None
        offset = failed.offset()  # type: ignore[attr-defined]

        consumer.pause([TopicPartition(topic, 0)])
        consumer.seek(TopicPartition(topic, 0, offset))
        consumer.resume([TopicPartition(topic, 0)])

        again = _poll_record(consumer)
        assert again is not None
        assert again.offset() == offset  # type: ignore[attr-defined]
    finally:
        consumer.close()


def test_commit_with_nothing_stored_raises_and_must_be_tolerated(topic: str) -> None:
    """`_NO_OFFSET` is benign, and the loader's commit path has to say so.

    With `enable.auto.offset.store=false`, a forced commit on a partition where
    nothing succeeded yet raises. Treating it as a failure would make every
    revoke and every shutdown look broken.
    """
    consumer = _assigned_consumer(topic, f"s5-sem-nooffset-{time.time_ns()}")
    try:
        with pytest.raises(KafkaException) as excinfo:
            consumer.commit(asynchronous=False)
        assert "No offset stored" in str(excinfo.value)
    finally:
        consumer.close()


def test_a_killed_consumers_stored_offsets_never_reach_the_group(topic: str) -> None:
    """Stored != committed. This is the entire at-least-once argument.

    Storing offsets for all four records and then closing without committing
    must leave the group with no committed offset, so a successor replays from
    the beginning rather than skipping the work the dead worker never finished.
    """
    group = f"s5-sem-stored-{time.time_ns()}"
    consumer = _assigned_consumer(topic, group)
    try:
        for _ in range(RECORDS):
            msg = _poll_record(consumer)
            assert msg is not None
            consumer.store_offsets(message=msg)
    finally:
        consumer.close()  # close() does NOT commit when auto-commit is off

    checker = Consumer(clients.consumer_config(group))
    try:
        committed = checker.committed([TopicPartition(topic, 0)], timeout=30)
    finally:
        checker.close()
    assert committed[0].offset < 0, (
        f"offsets were committed without an explicit commit (got {committed[0].offset})"
    )
