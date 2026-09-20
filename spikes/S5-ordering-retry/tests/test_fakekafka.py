"""Pin the librdkafka behaviours the fake claims to model.

If one of these tests is wrong, every ordering property proved on top of the
fake is worth nothing. The matching assertions against a real broker live in
`test_integration_s5.py` (marked `integration`); these are the cheap version
that runs on every commit.
"""

from __future__ import annotations

import pytest
from confluent_kafka import KafkaException, TopicPartition
from s5.fakekafka import FakeCluster, SimulatedCrash

TOPIC = "spike.fake.out"


def _cluster(partitions: int = 2, records: int = 6) -> FakeCluster:
    """Records spread evenly over the partitions, one key per partition.

    The partition is passed explicitly. Deriving it from the key would depend
    on crc32 happening to send ``k0`` to partition 0, which it does not --
    ``crc32(b"k0") % 2`` and ``crc32(b"k1") % 2`` are both 1, so every record
    would pile onto one partition and the interleaving test could never pass.
    Keys are still one-to-one with partitions, which is what the ordering
    assertions rely on.
    """
    cluster = FakeCluster(default_partitions=partitions)
    cluster.create_topic(TOPIC, partitions)
    for i in range(records):
        part = i % partitions
        cluster.append(TOPIC, key=f"k{part}".encode(), value=b"x", headers=[], partition=part)
    return cluster


def test_a_key_always_lands_on_one_partition() -> None:
    cluster = FakeCluster(default_partitions=8)
    cluster.create_topic(TOPIC, 8)
    seen = {cluster.append(TOPIC, b"SYNTH-MRN-000042", b"x").partition for _ in range(50)}
    assert len(seen) == 1


def test_poll_interleaves_partitions_round_robin() -> None:
    cluster = _cluster(partitions=2, records=6)
    consumer = cluster.consumer("g", [TOPIC])
    order = []
    while (msg := consumer.poll(0)) is not None:
        order.append(msg.partition())
    assert order[:4] == [0, 1, 0, 1]


def test_resume_without_seek_skips_the_failed_message() -> None:
    """The bug the loader exists to avoid, reproduced on purpose.

    pause() purges the prefetch queue; resume() refetches from last consumed
    + 1. A loader that pauses on failure and resumes without seeking back
    never sees the failed message again -- silent loss, and the message after
    it is delivered out of order relative to it.
    """
    cluster = _cluster(partitions=1, records=4)
    consumer = cluster.consumer("g", [TOPIC])
    first = consumer.poll(0)
    assert first is not None and first.offset() == 0

    consumer.pause([TopicPartition(TOPIC, 0)])
    assert consumer.poll(0) is None
    consumer.resume([TopicPartition(TOPIC, 0)])

    after = consumer.poll(0)
    assert after is not None
    assert after.offset() == 1, "model must reproduce the skip, not hide it"


def test_seek_before_resume_replays_the_failed_message() -> None:
    cluster = _cluster(partitions=1, records=4)
    consumer = cluster.consumer("g", [TOPIC])
    failed = consumer.poll(0)
    assert failed is not None

    consumer.pause([TopicPartition(TOPIC, 0)])
    consumer.seek(TopicPartition(TOPIC, 0, failed.offset()))
    consumer.resume([TopicPartition(TOPIC, 0)])

    again = consumer.poll(0)
    assert again is not None
    assert again.offset() == failed.offset()


def test_seek_on_an_unassigned_partition_raises() -> None:
    cluster = _cluster(partitions=1)
    consumer = cluster.consumer("g", [TOPIC])
    with pytest.raises(KafkaException):
        consumer.seek(TopicPartition("spike.not.mine", 0, 0))


def test_a_rebalanced_partition_comes_back_unpaused_and_at_the_last_commit() -> None:
    cluster = _cluster(partitions=1, records=5)
    consumer = cluster.consumer("g", [TOPIC])
    first = consumer.poll(0)
    assert first is not None
    consumer.store_offsets(message=first)
    consumer.commit()
    second = consumer.poll(0)
    assert second is not None and second.offset() == 1
    consumer.pause([TopicPartition(TOPIC, 0)])

    consumer.rebalance()
    replayed = consumer.poll(0)

    assert replayed is not None
    assert replayed.offset() == 1, "reading restarts at the committed offset"
    assert consumer.poll(0) is not None, "and the partition is no longer paused"


def test_commit_with_nothing_stored_raises_no_offset_stored() -> None:
    cluster = _cluster()
    consumer = cluster.consumer("g", [TOPIC])
    with pytest.raises(KafkaException) as excinfo:
        consumer.commit()
    assert "No offset stored" in str(excinfo.value)


def test_stored_but_uncommitted_offsets_are_lost_on_crash() -> None:
    cluster = _cluster(partitions=1, records=3)
    consumer = cluster.consumer("g", [TOPIC])
    for _ in range(3):
        msg = consumer.poll(0)
        assert msg is not None
        consumer.store_offsets(message=msg)

    consumer.crash()
    with pytest.raises(SimulatedCrash):
        consumer.poll(0)
    consumer.commit()  # a dead consumer's commit must not reach the group
    consumer.close()

    assert cluster.committed_offset("g", TOPIC, 0) < 0
    successor = cluster.consumer("g", [TOPIC])
    assert successor.poll(0).offset() == 0  # type: ignore[union-attr]


def test_revoke_callback_runs_before_the_partitions_go_away() -> None:
    cluster = _cluster(partitions=1, records=3)
    seen: list[str] = []

    def on_revoke(c, partitions) -> None:  # type: ignore[no-untyped-def]
        # Still ours: this is the only safe place to commit on a planned move.
        seen.append("revoke")
        assert c.assignment()
        c.commit()

    consumer = cluster.consumer("g", [TOPIC], on_revoke=on_revoke)
    msg = consumer.poll(0)
    assert msg is not None
    consumer.store_offsets(message=msg)
    consumer.revoke()
    consumer.poll(0)

    assert seen == ["revoke"]
    assert cluster.committed_offset("g", TOPIC, 0) == 1


def test_lost_callback_runs_after_the_partitions_are_gone() -> None:
    cluster = _cluster(partitions=1, records=3)
    assignments: list[int] = []

    def on_lost(c, partitions) -> None:  # type: ignore[no-untyped-def]
        assignments.append(len(c.assignment()))

    consumer = cluster.consumer("g", [TOPIC], on_lost=on_lost)
    consumer.poll(0)
    consumer.lose()
    consumer.poll(0)

    assert assignments == [0], "committing here would write to someone else's partition"


def test_close_revokes_so_a_graceful_shutdown_can_commit() -> None:
    cluster = _cluster(partitions=1, records=2)
    calls: list[str] = []
    consumer = cluster.consumer(
        "g", [TOPIC], on_revoke=lambda c, p: calls.append("revoke")  # type: ignore[misc]
    )
    msg = consumer.poll(0)
    assert msg is not None
    consumer.store_offsets(message=msg)
    consumer.commit()
    consumer.close()
    assert calls == ["revoke"]


def test_commit_hook_sees_every_commit_in_order() -> None:
    cluster = _cluster(partitions=1, records=3)
    events = []
    cluster.commit_hook = events.append
    consumer = cluster.consumer("g", [TOPIC])
    for _ in range(3):
        msg = consumer.poll(0)
        assert msg is not None
        consumer.store_offsets(message=msg)
        consumer.commit()
    assert [e.committed for e in events] == [1, 2, 3]
    assert [e.previous for e in events] == [0, 1, 2]
