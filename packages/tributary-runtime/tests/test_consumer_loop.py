"""The consumer loop: commit-after-success, blocking retry, exhaustion, rebalance and shutdown.

These run against `tests/fakes.FakeConsumer`, which models the librdkafka behaviour that makes blocking
retry dangerous — `pause()` purges the prefetch queue, so a resume without a seek continues *after* the
failed record. The mutation control for the whole retry design is
`test_a_pause_without_a_seek_would_skip_the_failed_record`: it drives the same fake with the seek
removed and asserts the message is lost, so the real test is known to be checking something.
"""

from __future__ import annotations

import io
from typing import Any

import pytest
from tributary.runtime.consumer import PARTITION_EOF, ConsumerLoop, LoopSettings, backoff_seconds
from tributary.runtime.errors import DeliveryError
from tributary.runtime.events import RecordingEmitter
from tributary.runtime.executor import StepFailure
from tributary.runtime.logs import JsonLogger
from tributary.runtime.records import ConsumedRecord, to_kafka_headers
from tributary.runtime.spec import RetryPolicy
from tributary.sdk.ids import uuid7
from tributary.sdk.testing import make_message

from .fakes import FakeConsumer, FakeError, FakeRecord

TOPIC = "dev.adt-passthrough.raw"
BLOCKING = RetryPolicy(
    policy="blocking", max_attempts=3, on_exhaust="halt-partition", initial_delay_ms=10, max_delay_ms=20
)


class Clock:
    """A clock the test moves by hand, so a backoff costs no wall-clock time."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class ScriptedHandler:
    """Fails the first `failures` calls for a given offset, then succeeds."""

    def __init__(self, *, failures: int = 0, error: Exception | None = None) -> None:
        self.failures = failures
        self.error = error or DeliveryError("HTTP_503", retryable=True)
        self.seen: list[int] = []

    def handle(self, record: ConsumedRecord) -> None:
        self.seen.append(record.offset)
        if self.failures > 0:
            self.failures -= 1
            raise self.error


class PerPartitionHandler:
    """Fails every record from `failing_partition`, succeeds everywhere else."""

    def __init__(self, failing_partition: int) -> None:
        self.failing_partition = failing_partition
        self.seen: list[tuple[int, int]] = []

    def handle(self, record: ConsumedRecord) -> None:
        self.seen.append((record.partition, record.offset))
        if record.partition == self.failing_partition:
            raise DeliveryError("HTTP_503", retryable=True)


class RecordingDlq:
    def __init__(self) -> None:
        self.dead: list[tuple[str, str]] = []
        self.quarantined: list[tuple[str, int]] = []

    def dead_letter(self, record: ConsumedRecord, *, error_code: str, stage: str) -> None:
        self.dead.append((error_code, stage))

    def quarantine(self, raw: Any, *, channel: str, channel_version: int) -> str:
        self.quarantined.append((channel, channel_version))
        return uuid7()


def record(offset: int, *, partition: int = 0, headers: list[tuple[str, bytes]] | None = None) -> FakeRecord:
    message = make_message(b"synthetic", key=b"SYNTH-KEY-1")
    return FakeRecord(
        _topic=TOPIC,
        _partition=partition,
        _offset=offset,
        _key=b"SYNTH-KEY-1",
        _value=b"synthetic",
        _headers=headers if headers is not None else to_kafka_headers(message),
    )


def build(
    consumer: FakeConsumer,
    handler: Any,
    *,
    retry: RetryPolicy = BLOCKING,
    commit_every: int = 1,
    dlq: Any = None,
    clock: Clock | None = None,
    emitter: RecordingEmitter | None = None,
) -> tuple[ConsumerLoop, RecordingEmitter]:
    events = emitter or RecordingEmitter()
    loop = ConsumerLoop(
        consumer,
        handler,
        topics=[TOPIC],
        retry=retry,
        settings=LoopSettings(commit_every=commit_every, poll_timeout_s=0.0),
        emitter=events,
        log=JsonLogger("test", stream=io.StringIO()),
        channel="adt-passthrough",
        channel_version=1,
        instance="worker:test@localhost",
        dlq=dlq,
        clock=clock or Clock(),
    )
    return loop, events


# -- the happy path -------------------------------------------------------------------------------


def test_offsets_are_committed_only_after_the_handler_returns() -> None:
    consumer = FakeConsumer()
    for offset in range(3):
        consumer.append(record(offset))
    handler = ScriptedHandler()
    loop, _ = build(consumer, handler, commit_every=1)

    stats = loop.run(max_iterations=5)

    assert handler.seen == [0, 1, 2]
    assert stats.handled == 3
    # Committed position is the next offset to read, i.e. processed + 1 (invariants 1 and 4).
    assert consumer.committed[0] == [(TOPIC, 0, 1)]
    assert consumer.committed[-1] == [(TOPIC, 0, 3)]


def test_commit_every_batches_commits() -> None:
    consumer = FakeConsumer()
    for offset in range(10):
        consumer.append(record(offset))
    loop, _ = build(consumer, ScriptedHandler(), commit_every=5)

    loop.run(max_iterations=12)

    # 10 records at commit_every=5 is two batched commits plus the drain commit, not ten.
    assert len(consumer.committed) <= 3
    assert consumer.committed[0] == [(TOPIC, 0, 5)]


def test_a_handler_that_never_returns_a_success_commits_nothing() -> None:
    """Mutation control for the commit rule: no success, no commit."""
    consumer = FakeConsumer()
    consumer.append(record(0))
    loop, _ = build(consumer, ScriptedHandler(failures=99), commit_every=1)

    loop.run(max_iterations=2)

    assert consumer.committed == [] or all(entry == [] for entry in consumer.committed)


# -- blocking retry -------------------------------------------------------------------------------


def test_a_retryable_failure_pauses_seeks_back_and_redelivers_the_same_offset() -> None:
    consumer = FakeConsumer()
    consumer.append(record(0))
    consumer.append(record(1))
    clock = Clock()
    handler = ScriptedHandler(failures=1)
    loop, events = build(consumer, handler, clock=clock)

    loop.run(max_iterations=1)  # consume offset 0, fail, pause + seek
    assert consumer.pause_calls == [(TOPIC, 0)]
    assert consumer.seek_calls == [(TOPIC, 0, 0)], "the seek must return to the FAILED offset, not past it"
    assert (TOPIC, 0) in consumer.paused

    clock.advance(1.0)
    loop.run(max_iterations=3)  # resume, redeliver 0, then 1

    assert handler.seen == [0, 0, 1], "the failed record is redelivered, not skipped"
    assert events.stages()[:2] == ["partition_paused", "partition_resumed"]
    assert consumer.committed[-1] == [(TOPIC, 0, 2)]


def test_a_pause_without_a_seek_would_skip_the_failed_record() -> None:
    """The control for the control.

    This drives the same fake the way a loop that pauses and resumes *without* seeking would behave. It
    must lose the record — if this test ever passes without loss, the fake has stopped modelling the
    prefetch purge and the test above proves nothing.
    """
    from confluent_kafka import TopicPartition

    consumer = FakeConsumer()
    consumer.append(record(0))
    consumer.append(record(1))

    first = consumer.poll(0.0)
    assert first is not None and first.offset() == 0
    consumer.pause([TopicPartition(TOPIC, 0)])  # no seek back to offset 0
    consumer.resume([TopicPartition(TOPIC, 0)])

    second = consumer.poll(0.0)
    assert second is not None
    assert second.offset() == 1, "without the seek the failed record 0 is silently skipped"


def test_a_paused_partition_yields_nothing_until_the_backoff_elapses() -> None:
    consumer = FakeConsumer()
    consumer.append(record(0))
    consumer.append(record(1))
    clock = Clock()
    handler = ScriptedHandler(failures=1)
    loop, _ = build(consumer, handler, clock=clock)

    loop.run(max_iterations=1)
    loop.run(max_iterations=2)  # clock has not advanced: still paused

    assert handler.seen == [0], "a partition in backoff must not deliver anything, including later offsets"
    assert consumer.resume_calls == []


def test_a_paused_partition_does_not_stop_the_others() -> None:
    """The difference between blocking retry and stopping the channel (ADR-003 D7).

    A destination failure pauses **one** partition. The rest keep flowing, which is why a cooperative
    design can afford to preserve order by stopping: an ADT backlog for one patient does not become an
    outage for every other patient on the topic. Nothing tested this before — the retry tests were all
    single-partition, so a loop that paused the *consumer* rather than the partition would have passed
    every one of them.
    """
    consumer = FakeConsumer()
    for offset in range(2):
        consumer.append(record(offset, partition=0))
    for offset in range(3):
        consumer.append(record(offset, partition=1))
    clock = Clock()
    handler = PerPartitionHandler(failing_partition=0)
    loop, events = build(consumer, handler, commit_every=1, clock=clock)

    loop.run(max_iterations=6)

    # Partition 0 failed once and is in backoff; partition 1 was delivered and committed in the SAME run.
    assert (TOPIC, 0) in consumer.paused
    assert consumer.pause_calls == [(TOPIC, 0)], "only the failing partition is paused, never the consumer"
    delivered_from_one = [offset for partition, offset in handler.seen if partition == 1]
    assert delivered_from_one == [0, 1, 2], "the healthy partition drained while the other sat in backoff"
    committed_one = [entry for commit in consumer.committed for entry in commit if entry[1] == 1]
    assert committed_one, "and its offsets were committed, not held behind the paused partition"
    assert not [entry for commit in consumer.committed for entry in commit if entry[1] == 0], (
        "the paused partition commits nothing: its record has not been delivered"
    )
    assert events.stages().count("partition_paused") == 1


def test_the_paused_partition_resumes_and_catches_up_without_disturbing_the_other() -> None:
    """The other half of the same claim: pausing one partition is temporary and scoped."""
    consumer = FakeConsumer()
    consumer.append(record(0, partition=0))
    consumer.append(record(0, partition=1))
    clock = Clock()

    class FailsOnceOnPartitionZero:
        def __init__(self) -> None:
            self.seen: list[tuple[int, int]] = []
            self.failed = False

        def handle(self, record: ConsumedRecord) -> None:
            self.seen.append((record.partition, record.offset))
            if record.partition == 0 and not self.failed:
                self.failed = True
                raise DeliveryError("HTTP_503", retryable=True)

    handler = FailsOnceOnPartitionZero()
    loop, events = build(consumer, handler, commit_every=1, clock=clock)

    loop.run(max_iterations=3)
    assert (0, 0) in handler.seen and (1, 0) in handler.seen

    clock.advance(1.0)
    loop.run(max_iterations=3)

    assert handler.seen.count((0, 0)) == 2, "the failed record is redelivered after the backoff"
    assert handler.seen.count((1, 0)) == 1, "the healthy partition is not replayed by the other's retry"
    assert events.stages().count("partition_resumed") == 1
    assert consumer.paused == set()


def test_backoff_grows_exponentially_and_is_capped_and_jittered() -> None:
    policy = RetryPolicy(
        policy="blocking", max_attempts=10, on_exhaust="halt-partition", initial_delay_ms=100, max_delay_ms=1000
    )
    first = [backoff_seconds(policy, 1) for _ in range(50)]
    later = [backoff_seconds(policy, 5) for _ in range(50)]

    assert all(0.05 <= value <= 0.1 for value in first)
    assert all(0.5 <= value <= 1.0 for value in later), "capped at maxDelayMs"
    assert len(set(first)) > 1, "unjittered backoff makes every partition retry in the same millisecond"


def test_fixed_backoff_does_not_grow() -> None:
    policy = RetryPolicy(
        policy="blocking", max_attempts=10, on_exhaust="halt-partition", backoff="fixed", initial_delay_ms=200
    )
    assert all(0.1 <= backoff_seconds(policy, attempt) <= 0.2 for attempt in (1, 5, 9))


# -- exhaustion -----------------------------------------------------------------------------------


def test_halt_partition_stops_the_partition_and_emits_for_the_operator() -> None:
    consumer = FakeConsumer()
    consumer.append(record(0))
    consumer.append(record(1))
    clock = Clock()
    loop, events = build(consumer, ScriptedHandler(failures=99), clock=clock)

    for _ in range(6):
        loop.run(max_iterations=1)
        clock.advance(1.0)

    assert "partition_halted" in events.stages()
    assert (TOPIC, 0) in consumer.paused
    assert consumer.resume_calls.count((TOPIC, 0)) <= 2, "a halted partition is not resumed by the backoff timer"
    halted = next(event for event in events.events if event.stage == "partition_halted")
    assert halted.error_code == "HTTP_503"
    assert halted.status == "error"


def test_dlq_and_continue_dead_letters_and_keeps_the_partition_moving() -> None:
    consumer = FakeConsumer()
    consumer.append(record(0))
    consumer.append(record(1))
    clock = Clock()
    dlq = RecordingDlq()
    policy = RetryPolicy(
        policy="blocking", max_attempts=2, on_exhaust="dlq-and-continue", initial_delay_ms=10, max_delay_ms=10
    )
    handler = ScriptedHandler(failures=2)
    loop, events = build(consumer, handler, retry=policy, dlq=dlq, clock=clock)

    for _ in range(6):
        loop.run(max_iterations=1)
        clock.advance(1.0)

    assert dlq.dead == [("HTTP_503", "pending")]
    assert "dlq" in events.stages()
    assert 1 in handler.seen, "the partition keeps moving after the dead letter"


def test_a_non_retryable_failure_goes_straight_to_the_dlq() -> None:
    consumer = FakeConsumer()
    consumer.append(record(0))
    dlq = RecordingDlq()
    handler = ScriptedHandler(failures=1, error=DeliveryError("PAYLOAD_REJECTED", retryable=False))
    loop, _ = build(consumer, handler, dlq=dlq)

    loop.run(max_iterations=2)

    assert dlq.dead == [("PAYLOAD_REJECTED", "pending")]
    assert consumer.pause_calls == [], "a permanent failure must not pause the partition"


def test_a_step_failure_is_dead_lettered_without_retrying_the_partition() -> None:
    """ADR-003 D4: the executor already retried it in place; retrying the partition only delays the DLQ."""
    consumer = FakeConsumer()
    consumer.append(record(0))
    dlq = RecordingDlq()

    class Failing:
        def handle(self, record: ConsumedRecord) -> None:
            raise StepFailure("to-fhir", 3, "ValueError")

    loop, _ = build(consumer, Failing(), dlq=dlq)
    loop.run(max_iterations=2)

    assert dlq.dead == [("STEP_ERROR", "transformed")]
    assert consumer.pause_calls == []


def test_without_a_dlq_a_permanent_failure_stops_the_worker_rather_than_committing() -> None:
    consumer = FakeConsumer()
    consumer.append(record(0))
    handler = ScriptedHandler(failures=1, error=DeliveryError("PAYLOAD_REJECTED", retryable=False))
    loop, _ = build(consumer, handler, dlq=None)

    with pytest.raises(DeliveryError):
        loop.run(max_iterations=2)
    assert consumer.committed == []


# -- undecodable records --------------------------------------------------------------------------


def test_an_undecodable_record_is_quarantined_and_the_partition_continues() -> None:
    consumer = FakeConsumer()
    consumer.append(record(0, headers=[("tb-channel", b"\xff\xfe")]))
    consumer.append(record(1))
    dlq = RecordingDlq()
    handler = ScriptedHandler()
    loop, events = build(consumer, handler, dlq=dlq)

    loop.run(max_iterations=3)

    assert dlq.quarantined == [("adt-passthrough", 1)], (
        "the wrapper's channel comes from the subscription; its stage and code are fixed by contract"
    )
    assert handler.seen == [1], "the partition keeps moving past a record nothing can decode"
    assert consumer.committed[-1] == [(TOPIC, 0, 2)]

    quarantine = next(event for event in events.events if event.stage == "quarantined")
    assert quarantine.topic == TOPIC and quarantine.offset == 0
    assert quarantine.message_id, "the event carries the id minted for the wrapper, never one from the record"
    assert quarantine.error_code == "RECORD_UNDECODABLE"


def test_nothing_from_an_undecodable_record_reaches_the_log() -> None:
    """Envelope contract 5.2: a header NAME is attacker-supplied too, and under mis-framing it is payload.

    So the permitted set is coordinates, the minted id, the code and byte counts — and this test drives
    a record whose payload, key and header name are all distinctive markers to prove none of them lands
    in a log line.
    """
    marker = "PID|1|SYNTH-MARKER^JANE"
    consumer = FakeConsumer()
    consumer.append(
        FakeRecord(
            _topic=TOPIC,
            _partition=0,
            _offset=0,
            _key=b"SYNTH-KEY-MARKER",
            _value=marker.encode(),
            _headers=[(marker, b"\xff\xfe")],
        )
    )
    stream = io.StringIO()
    loop = ConsumerLoop(
        consumer,
        ScriptedHandler(),
        topics=[TOPIC],
        retry=BLOCKING,
        settings=LoopSettings(commit_every=1, poll_timeout_s=0.0),
        emitter=RecordingEmitter(),
        log=JsonLogger("test", stream=stream, level="debug"),
        channel="adt-passthrough",
        channel_version=1,
        instance="worker:test@localhost",
        dlq=RecordingDlq(),
        clock=Clock(),
    )
    loop.run(max_iterations=2)

    written = stream.getvalue()
    assert "SYNTH-MARKER" not in written
    assert "SYNTH-KEY-MARKER" not in written
    assert "PID|" not in written
    assert "record_quarantined" in written, "the failure is still reported, by coordinates and code"


# -- consumer errors, rebalance and shutdown --------------------------------------------------------


def test_partition_eof_is_not_an_error() -> None:
    consumer = FakeConsumer()
    consumer.append(
        FakeRecord(TOPIC, 0, 0, None, None, None, FakeError(PARTITION_EOF))  # type: ignore[arg-type]
    )
    loop, _ = build(consumer, ScriptedHandler())
    stats = loop.run(max_iterations=2)
    assert stats.handled == 0 and stats.quarantined == 0


def test_a_fatal_consumer_error_stops_the_loop() -> None:
    consumer = FakeConsumer()
    consumer.append(FakeRecord(TOPIC, 0, 0, None, None, None, FakeError(-1, True)))  # type: ignore[arg-type]
    loop, _ = build(consumer, ScriptedHandler())
    with pytest.raises(RuntimeError):
        loop.run(max_iterations=2)


def test_a_cooperative_revoke_commits_before_handing_the_partition_over() -> None:
    """Zero duplicates after a planned rebalance come from this ordering (ADR-003 D7)."""
    from confluent_kafka import TopicPartition

    consumer = FakeConsumer()
    consumer.append(record(0))
    loop, _ = build(consumer, ScriptedHandler(), commit_every=1000)
    loop.start()
    # _consume_one rather than run(): run() drains and commits on the way out, which would hide
    # whether the revoke itself committed.
    raw = consumer.poll(0.0)
    assert raw is not None
    loop._consume_one(raw)
    assert consumer.committed == []  # nothing committed yet: commit_every is large

    loop._on_revoke(consumer, [TopicPartition(TOPIC, 0)])

    assert consumer.committed[-1] == [(TOPIC, 0, 1)]
    assert consumer.unassigned == [(TOPIC, 0)]


def test_lost_partitions_are_never_committed() -> None:
    """They already belong to another member; committing would move someone else's offsets."""
    from confluent_kafka import TopicPartition

    consumer = FakeConsumer()
    consumer.append(record(0))
    loop, _ = build(consumer, ScriptedHandler(), commit_every=1000)
    raw = consumer.poll(0.0)
    assert raw is not None
    loop._consume_one(raw)

    loop._on_lost(consumer, [TopicPartition(TOPIC, 0)])

    assert consumer.committed == []
    assert consumer.unassigned == [(TOPIC, 0)]


def test_assignment_rebuilds_retry_state_because_partitions_come_back_unpaused() -> None:
    from confluent_kafka import TopicPartition

    consumer = FakeConsumer()
    consumer.append(record(0))
    clock = Clock()
    loop, _ = build(consumer, ScriptedHandler(failures=99), clock=clock)
    loop.run(max_iterations=1)
    assert loop._retry_state

    loop._on_assign(consumer, [TopicPartition(TOPIC, 0)])

    assert loop._retry_state == {}
    assert consumer.assigned == [(TOPIC, 0)]


def test_graceful_shutdown_commits_what_was_handled_and_closes() -> None:
    consumer = FakeConsumer()
    for offset in range(3):
        consumer.append(record(offset))
    loop, _ = build(consumer, ScriptedHandler(), commit_every=1000)

    stats = loop.run(max_iterations=3)

    assert stats.handled == 3
    assert consumer.committed[-1] == [(TOPIC, 0, 3)], "the drain commits before close, not inside the revoke"
    assert consumer.closed is True


def test_stop_ends_the_loop_and_still_drains() -> None:
    consumer = FakeConsumer()
    for offset in range(5):
        consumer.append(record(offset))

    class StopAfterTwo:
        def __init__(self) -> None:
            self.seen: list[int] = []
            self.loop: ConsumerLoop | None = None

        def handle(self, record: ConsumedRecord) -> None:
            self.seen.append(record.offset)
            if len(self.seen) == 2 and self.loop is not None:
                self.loop.stop()

    handler = StopAfterTwo()
    loop, _ = build(consumer, handler, commit_every=1000)
    handler.loop = loop

    loop.run(max_iterations=10)

    assert handler.seen == [0, 1]
    assert consumer.committed[-1] == [(TOPIC, 0, 2)]
    assert consumer.closed is True
