"""The transactional commit path (ADR-003 D6) against fakes: windows, aborts, rewinds and exhaustion.

The claims, each with the case that would expose its absence:

* offsets go into the transaction and never through `consumer.commit`;
* a retryable failure aborts the window and rewinds **every** partition in it to the window start -
  a rewind of only the failed partition would silently skip the other partitions' aborted records;
* the failed partition's retry still exhausts, although the records before it succeed on every replay;
* `dlq-and-continue` exhaustion dead-letters inside the open transaction instead of aborting;
* an open transaction is committed on an idle poll, so read_committed readers are not stalled.
"""

from __future__ import annotations

import io
from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException, TopicPartition
from tributary.runtime.consumer import ConsumerLoop, LoopSettings
from tributary.runtime.errors import DeliveryError
from tributary.runtime.events import RecordingEmitter
from tributary.runtime.logs import JsonLogger
from tributary.runtime.records import ConsumedRecord
from tributary.runtime.spec import RetryPolicy
from tributary.runtime.transactions import (
    TransactionalCommitter,
    transactional_id,
    transactional_producer_config,
)

from .fakes import FakeConsumer
from .test_consumer_loop import TOPIC, Clock, RecordingDlq, record


class TxnConsumer(FakeConsumer):
    def consumer_group_metadata(self) -> str:
        return "group-metadata"

    def commit(self, offsets: Any = None, asynchronous: bool = True) -> Any:
        raise AssertionError("a transactional loop must never commit offsets outside the transaction")


class FakeTxnProducer:
    """Records the transaction protocol calls; can be told to fail the next commit."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.sent_offsets: list[list[tuple[str, int, int]]] = []
        self.fail_commit: KafkaError | None = None

    def init_transactions(self, timeout: float) -> None:
        self.calls.append("init")

    def begin_transaction(self) -> None:
        self.calls.append("begin")

    def send_offsets_to_transaction(self, positions: list[TopicPartition], group_metadata: Any, timeout: float) -> None:
        assert group_metadata == "group-metadata"
        self.calls.append("offsets")
        self.sent_offsets.append([(tp.topic, tp.partition, tp.offset) for tp in positions])

    def commit_transaction(self, timeout: float) -> None:
        if self.fail_commit is not None:
            error, self.fail_commit = self.fail_commit, None
            raise KafkaException(error)
        self.calls.append("commit")

    def abort_transaction(self, timeout: float) -> None:
        self.calls.append("abort")


class ScriptedFailures:
    """Fails `(partition, offset)` the given number of times, then succeeds."""

    def __init__(self, failures: dict[tuple[int, int], int]) -> None:
        self.failures = dict(failures)
        self.seen: list[tuple[int, int]] = []

    def handle(self, rec: ConsumedRecord) -> None:
        key = (rec.partition, rec.offset)
        self.seen.append(key)
        if self.failures.get(key, 0) > 0:
            self.failures[key] -= 1
            raise DeliveryError("HTTP_503", retryable=True)


def build(
    consumer: TxnConsumer,
    handler: Any,
    *,
    retry: RetryPolicy | None = None,
    commit_every: int = 100,
    clock: Clock | None = None,
    dlq: Any = None,
) -> tuple[ConsumerLoop, FakeTxnProducer, TransactionalCommitter]:
    producer = FakeTxnProducer()
    clock = clock or Clock()
    committer = TransactionalCommitter(
        producer, consumer, log=JsonLogger("test", stream=io.StringIO()), max_age_s=10.0, clock=clock
    )
    loop = ConsumerLoop(
        consumer,
        handler,
        topics=[TOPIC],
        retry=retry
        or RetryPolicy(
            policy="blocking", max_attempts=3, on_exhaust="halt-partition", initial_delay_ms=10, max_delay_ms=20
        ),
        settings=LoopSettings(commit_every=commit_every, poll_timeout_s=0.0),
        emitter=RecordingEmitter(),
        log=JsonLogger("test", stream=io.StringIO()),
        channel="adt-passthrough",
        channel_version=1,
        instance="worker:test@localhost",
        dlq=dlq,
        clock=clock,
        committer=committer,
    )
    return loop, producer, committer


def test_a_window_commits_its_offsets_inside_the_transaction() -> None:
    consumer = TxnConsumer()
    for offset in range(3):
        consumer.append(record(offset))
    loop, producer, _ = build(consumer, ScriptedFailures({}), commit_every=3)
    loop.run(max_iterations=3)
    assert producer.calls == ["begin", "offsets", "commit"]
    assert producer.sent_offsets == [[(TOPIC, 0, 3)]]


def test_an_idle_poll_commits_an_open_transaction() -> None:
    """A half-full window on a quiet channel would otherwise block every read_committed reader."""
    consumer = TxnConsumer()
    consumer.append(record(0))
    loop, producer, committer = build(consumer, ScriptedFailures({}), commit_every=50)
    loop.run(max_iterations=1)  # one record, then nothing: the loop's own idle check commits
    loop.run(max_iterations=1)
    assert "commit" in producer.calls
    assert not committer.open


def test_a_retryable_failure_aborts_and_rewinds_every_partition_in_the_window() -> None:
    consumer = TxnConsumer()
    consumer.append(record(0, partition=0))
    consumer.append(record(1, partition=0))
    consumer.append(record(0, partition=1))
    consumer.append(record(1, partition=1))
    clock = Clock()
    handler = ScriptedFailures({(1, 1): 1})
    loop, producer, _ = build(consumer, handler, clock=clock)

    loop.run(max_iterations=4)  # p0:0, p0:1, p1:0 succeed; p1:1 fails -> abort
    assert "abort" in producer.calls
    # Partition 0 did nothing wrong but its records were in the aborted transaction: rewound too.
    assert (TOPIC, 0, 0) in consumer.seek_calls
    assert (TOPIC, 1, 0) in consumer.seek_calls, "the failed partition rewinds to the WINDOW start"
    assert (TOPIC, 1) in consumer.paused

    clock.advance(1.0)
    loop.run(max_iterations=8)
    delivered_after_abort = handler.seen[4:]
    assert delivered_after_abort.count((0, 0)) == 1 and delivered_after_abort.count((0, 1)) == 1
    assert delivered_after_abort.count((1, 0)) == 1 and delivered_after_abort.count((1, 1)) == 1
    assert producer.sent_offsets[-1] and {(t, p): o for t, p, o in producer.sent_offsets[-1]} == {
        (TOPIC, 0): 2,
        (TOPIC, 1): 2,
    }


def test_a_rewind_of_only_the_failed_partition_would_lose_the_others() -> None:
    """Mutation control for the test above: without rewinding partition 0 its window is never redone."""
    consumer = TxnConsumer()
    consumer.append(record(0, partition=0))
    consumer.append(record(0, partition=1))
    clock = Clock()
    handler = ScriptedFailures({(1, 0): 1})
    loop, _producer, _ = build(consumer, handler, clock=clock)
    loop._rewind_window = lambda: loop._window_start.clear()  # type: ignore[method-assign]
    loop.run(max_iterations=2)
    clock.advance(1.0)
    loop.run(max_iterations=4)
    assert handler.seen.count((0, 0)) == 1, "partition 0's aborted record was never re-produced: lost"


def test_the_retry_still_exhausts_although_earlier_records_replay_successfully() -> None:
    consumer = TxnConsumer()
    consumer.append(record(0))
    consumer.append(record(1))
    clock = Clock()
    handler = ScriptedFailures({(0, 1): 99})
    loop, _producer, _ = build(consumer, handler, clock=clock)
    for _ in range(10):
        loop.run(max_iterations=3)
        clock.advance(1.0)
    assert loop.stats.halts == 1, "each replay of offset 0 must not reset offset 1's attempt count"
    assert handler.seen.count((0, 1)) == 3


def test_dlq_and_continue_exhaustion_dead_letters_inside_the_transaction() -> None:
    consumer = TxnConsumer()
    consumer.append(record(0))
    consumer.append(record(1))
    clock = Clock()
    dlq = RecordingDlq()
    policy = RetryPolicy(policy="blocking", max_attempts=1, on_exhaust="dlq-and-continue")
    loop, producer, _ = build(
        consumer, ScriptedFailures({(0, 0): 99}), retry=policy, clock=clock, dlq=dlq, commit_every=2
    )
    loop.run(max_iterations=2)
    assert dlq.dead == [("HTTP_503", "pending")]
    assert "abort" not in producer.calls
    assert producer.sent_offsets == [[(TOPIC, 0, 2)]]


def test_an_abortable_commit_failure_rewinds_the_window() -> None:
    consumer = TxnConsumer()
    consumer.append(record(0))
    consumer.append(record(1))
    handler = ScriptedFailures({})
    loop, producer, _ = build(consumer, handler, commit_every=2)
    producer.fail_commit = KafkaError(KafkaError._TIMED_OUT)
    loop.run(max_iterations=2)
    assert "abort" in producer.calls and loop.stats.aborts == 1
    assert (TOPIC, 0, 0) in consumer.seek_calls
    loop.run(max_iterations=3)
    assert handler.seen == [(0, 0), (0, 1), (0, 0), (0, 1)]
    assert producer.calls[-1] == "commit"


def test_a_fatal_commit_failure_raises() -> None:
    """A fenced producer cannot recover: the worker must stop, and a new incarnation fences it for good."""
    consumer = TxnConsumer()
    consumer.append(record(0))
    loop, producer, _ = build(consumer, ScriptedFailures({}), commit_every=1)
    producer.fail_commit = KafkaError(KafkaError._FENCED, fatal=True)
    with pytest.raises(KafkaException):
        loop.run(max_iterations=1)


def test_transactional_id_comes_from_the_contract_template() -> None:
    from tributary.runtime.config import load_topic_defaults

    template = load_topic_defaults().transactional_id_template
    assert transactional_id(template, env="dev", channel="adt", instance="w1") == "tb.dev.adt.pipeline.w1"


def test_message_timeout_is_clamped_to_the_transaction_timeout() -> None:
    """librdkafka refuses to construct a transactional producer otherwise (S5)."""
    config = transactional_producer_config(
        {"acks": "all", "delivery.timeout.ms": 120_000}, transactional_id_value="tb.dev.adt.pipeline.w1"
    )
    assert config["message.timeout.ms"] == 60_000 == config["transaction.timeout.ms"]
    assert "delivery.timeout.ms" not in config
