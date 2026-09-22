"""The consumer loop: at-least-once delivery, per-key ordering, and blocking retry.

This is the part of the runtime the invariants live in, so each rule is written next to the code that
keeps it.

**Commit after the downstream side effect, never before (invariants 1 and 4).** An offset is committed
only once the handler has returned, which for the pipeline worker means every `out.<dest>` record has a
delivery callback. `commit_every` batches those commits: S5 measured 4.9x throughput at 50 versus 1
(151 -> 733 msg/s) for a replay window of 50 instead of 1, and invariant 4 requires loader idempotency
on `tb-msg-id` anyway, so a window of 1 buys little.

**Blocking retry preserves order (invariant 5).** On a retryable failure the partition is paused, the
consumer seeks back to the failed offset, and the record is redelivered after a backoff. Pausing without
seeking would **silently skip the failed message**: `pause()` purges the partition's local prefetch
queue and `resume()` refetches from last-consumed + 1. That was measured in Phase 0 and it is the single
most dangerous detail in this file. The seek happens while the partition is still paused; the other
order races an in-flight fetch.

**Resume is not driven by the poll timeout.** S5 measured about 155 ms of each 169 ms retry cycle being
the loader's own poll timeout rather than backoff (ADR-003 follow-up 6), so the loop checks due
partitions at the top of every iteration and keeps the poll timeout short.

**Rebalance.** Cooperative-sticky with custom callbacks requires `incremental_assign`/
`incremental_unassign` — plain `assign()` raises `Local: Erroneous state`. A revoke commits before
handing over, which is what makes a planned handover produce zero duplicates; `on_lost` must NOT
commit, because those partitions already belong to another member. Pause state is per assignment: a
partition returns from a rebalance un-paused, so retry state is rebuilt on assign. That costs one
redelivery, which at-least-once permits and an idempotent loader absorbs (ADR-003 D7).

**A `TopicPartition` is never formatted into a log line.** Its `repr` crashes on Windows, and the
record it is logging beside is then silently dropped.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any, Protocol

from confluent_kafka import TopicPartition
from tributary.sdk.context import Logger

from .errors import DeliveryError
from .events import EventEmitter, MessageEvent, PartitionEvent, RecordEvent
from .executor import StepFailure
from .records import ConsumedRecord, RecordDecodeError, decode_record
from .spec import RetryPolicy

#: librdkafka's "reached the end of this partition" signal. Informational, not an error.
PARTITION_EOF = -191


class KafkaConsumerLike(Protocol):
    """The slice of `confluent_kafka.Consumer` the loop uses."""

    def subscribe(
        self, topics: list[str], on_assign: Any = None, on_revoke: Any = None, on_lost: Any = None
    ) -> None: ...

    def poll(self, timeout: float) -> Any: ...

    def commit(self, offsets: list[TopicPartition] | None = None, asynchronous: bool = True) -> Any: ...

    def pause(self, partitions: list[TopicPartition]) -> None: ...

    def resume(self, partitions: list[TopicPartition]) -> None: ...

    def seek(self, partition: TopicPartition) -> None: ...

    def incremental_assign(self, partitions: list[TopicPartition]) -> None: ...

    def incremental_unassign(self, partitions: list[TopicPartition]) -> None: ...

    def close(self) -> None: ...


class RecordHandler(Protocol):
    """Processes one record. Returns on success; raises to signal what the loop should do.

    `DeliveryError(retryable=True)` asks for blocking retry, `DeliveryError(retryable=False)` and
    `StepFailure` go straight to the DLQ (a step exception was already retried in place, ADR-003 D4).
    """

    def handle(self, record: ConsumedRecord) -> None: ...


class DlqWriter(Protocol):
    """Writes a record to `<env>.<channel>.dlq` and **waits for its delivery** before returning.

    The wait is not optional: the loop commits the input offset immediately afterwards, so a DLQ write
    that is still in a buffer would turn a dead letter into a lost message.
    """

    def dead_letter(self, record: ConsumedRecord, *, error_code: str, stage: str) -> None: ...

    def quarantine(self, raw: Any, *, error_code: str, channel: str, channel_version: int, stage: str) -> str:
        """Write the quarantine wrapper for an undecodable record and return the id it minted."""


Key = tuple[str, int]


@dataclass(slots=True)
class RetryState:
    """Per-partition blocking-retry state. Rebuilt on assignment, never persisted."""

    offset: int
    attempt: int = 0
    resume_at: float = 0.0
    halted: bool = False


@dataclass(frozen=True, slots=True)
class LoopSettings:
    """Loop tuning. `commit_every` comes from the topics contract, not from a constant here."""

    commit_every: int
    poll_timeout_s: float = 0.2
    flush_timeout_s: float = 30.0


@dataclass(slots=True)
class LoopStats:
    """What the loop did, for tests and for the worker's shutdown log line."""

    consumed: int = 0
    handled: int = 0
    retried: int = 0
    dead_lettered: int = 0
    quarantined: int = 0
    commits: int = 0
    pauses: int = 0
    resumes: int = 0
    halts: int = 0
    pending: dict[Key, int] = field(default_factory=dict)


def backoff_seconds(policy: RetryPolicy, attempt: int) -> float:
    """Exponential backoff with equal jitter, capped by the policy.

    Jitter matters because every partition of a channel fails at the same moment when a destination
    goes down, and unjittered backoff makes them all retry in the same millisecond forever.
    """
    if policy.backoff == "fixed":
        base = policy.initial_delay_ms
    else:
        base = min(policy.initial_delay_ms * (2 ** max(attempt - 1, 0)), policy.max_delay_ms)
    base = min(base, policy.max_delay_ms)
    half = base / 2.0
    return (half + random.uniform(0, half)) / 1000.0  # noqa: S311 - retry jitter, not cryptography


class ConsumerLoop:
    """Consume, hand each record to a handler, commit after success, retry by pausing."""

    def __init__(
        self,
        consumer: KafkaConsumerLike,
        handler: RecordHandler,
        *,
        topics: list[str],
        retry: RetryPolicy,
        settings: LoopSettings,
        emitter: EventEmitter,
        log: Logger,
        channel: str,
        channel_version: int,
        instance: str,
        dlq: DlqWriter | None = None,
        destination: str | None = None,
        read_stage: str = "queued",
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._consumer = consumer
        self._handler = handler
        self._topics = topics
        self._retry = retry
        self._settings = settings
        self._emitter = emitter
        self._log = log
        self._channel = channel
        self._channel_version = channel_version
        self._instance = instance
        self._dlq = dlq
        self._destination = destination
        #: The stage that read the record, stamped on a quarantine wrapper: `queued` for a pipeline
        #: worker reading `raw`, `pending` for a loader reading `out.<dest>`.
        self._read_stage = read_stage
        self._clock = clock
        self._retry_state: dict[Key, RetryState] = {}
        self._since_commit = 0
        self._running = False
        self.stats = LoopStats()

    # -- lifecycle -------------------------------------------------------------------------------

    def start(self) -> None:
        self._consumer.subscribe(
            self._topics,
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
            on_lost=self._on_lost,
        )

    def stop(self) -> None:
        """Ask the loop to finish the current iteration and drain. Safe to call from a signal handler."""
        self._running = False

    def run(self, *, max_iterations: int | None = None) -> LoopStats:
        """Poll until `stop()`. `max_iterations` exists so tests can run a bounded loop."""
        self._running = True
        iterations = 0
        try:
            while self._running and (max_iterations is None or iterations < max_iterations):
                iterations += 1
                self._resume_due_partitions()
                raw = self._consumer.poll(self._settings.poll_timeout_s)
                if raw is None:
                    continue
                self._consume_one(raw)
        finally:
            self._drain()
        return self.stats

    def _drain(self) -> None:
        """Graceful shutdown: commit what is safe to commit, then close.

        `close()` triggers a final revoke, so the offsets are committed here first — a commit inside a
        revoke callback during interpreter shutdown is where "it committed on my laptop" comes from.
        """
        try:
            self._commit_pending(force=True)
        finally:
            self._consumer.close()
            self._log.info(
                "worker_stopped",
                channel=self._channel,
                consumed=self.stats.consumed,
                handled=self.stats.handled,
                dead_lettered=self.stats.dead_lettered,
                commits=self.stats.commits,
            )

    # -- record handling -------------------------------------------------------------------------

    def _consume_one(self, raw: Any) -> None:
        error = raw.error()
        if error is not None:
            if error.code() == PARTITION_EOF:
                return
            self._log.warning("consumer_error", error_code=str(error.code()), fatal=bool(error.fatal()))
            if error.fatal():
                raise RuntimeError("fatal consumer error")
            return

        self.stats.consumed += 1
        topic, partition, offset = raw.topic(), raw.partition(), raw.offset()
        try:
            record = decode_record(
                topic=topic,
                partition=partition,
                offset=offset,
                key=raw.key(),
                value=raw.value(),
                headers=raw.headers(),
            )
        except RecordDecodeError as exc:
            self._quarantine(raw, topic, partition, offset, exc)
            return

        try:
            self._handler.handle(record)
        except DeliveryError as exc:
            if exc.retryable:
                self._begin_retry(record, error_code=exc.error_code)
            else:
                self._dead_letter(record, error_code=exc.error_code, stage="pending")
            return
        except StepFailure as exc:
            self._dead_letter(record, error_code=exc.error_code, stage="transformed")
            return

        self._succeed(record)

    def _succeed(self, record: ConsumedRecord) -> None:
        key = (record.topic, record.partition)
        self._retry_state.pop(key, None)
        self.stats.handled += 1
        self._mark(key, record.offset)

    def _mark(self, key: Key, offset: int) -> None:
        self.stats.pending[key] = offset
        self._since_commit += 1
        if self._since_commit >= self._settings.commit_every:
            self._commit_pending()

    def _commit_pending(self, *, force: bool = False) -> None:
        if not self.stats.pending or (not force and self._since_commit == 0):
            return
        offsets = [
            TopicPartition(topic, partition, offset + 1) for (topic, partition), offset in self.stats.pending.items()
        ]
        self._consumer.commit(offsets=offsets, asynchronous=False)
        self.stats.commits += 1
        self._since_commit = 0
        self.stats.pending.clear()

    # -- blocking retry --------------------------------------------------------------------------

    def _begin_retry(self, record: ConsumedRecord, *, error_code: str) -> None:
        key = (record.topic, record.partition)
        state = self._retry_state.get(key)
        if state is None or state.offset != record.offset:
            state = RetryState(offset=record.offset)
            self._retry_state[key] = state
        state.attempt += 1
        self.stats.retried += 1

        if state.attempt >= self._retry.max_attempts:
            self._exhausted(record, state, error_code=error_code)
            return

        delay = backoff_seconds(self._retry, state.attempt)
        state.resume_at = self._clock() + delay
        self._pause_and_rewind(record, state)
        self._emitter.emit(
            PartitionEvent(
                stage="partition_paused",
                status="error",
                channel=self._channel,
                channel_version=self._channel_version,
                instance=self._instance,
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                message_id=record.msg_id,
                destination=self._destination,
                attempt=state.attempt,
                error_code=error_code,
                error_message="delivery failed; partition paused for blocking retry",
                duration_ms=delay * 1000,
            )
        )
        self.stats.pauses += 1
        self._log.warning(
            "partition_paused",
            channel=self._channel,
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
            attempt=state.attempt,
            error_code=error_code,
            backoff_ms=round(delay * 1000, 1),
        )

    def _pause_and_rewind(self, record: ConsumedRecord, state: RetryState) -> None:
        """Pause, then seek back to the failed offset **while still paused**.

        Both halves are required. `pause()` purges the prefetch queue, so a later `resume()` would
        otherwise refetch from last-consumed + 1 and skip this record entirely — at-least-once turned
        into silent loss. Seeking before the resume also avoids racing an in-flight fetch.
        """
        partition = TopicPartition(record.topic, record.partition, state.offset)
        self._consumer.pause([partition])
        self._consumer.seek(partition)

    def _resume_due_partitions(self) -> None:
        now = self._clock()
        for key, state in list(self._retry_state.items()):
            if state.halted or state.resume_at == 0.0 or now < state.resume_at:
                continue
            topic, partition_number = key
            state.resume_at = 0.0
            self._consumer.resume([TopicPartition(topic, partition_number, state.offset)])
            self.stats.resumes += 1
            self._emitter.emit(
                PartitionEvent(
                    stage="partition_resumed",
                    status="ok",
                    channel=self._channel,
                    channel_version=self._channel_version,
                    instance=self._instance,
                    topic=topic,
                    partition=partition_number,
                    offset=state.offset,
                    destination=self._destination,
                    attempt=state.attempt,
                )
            )

    def _exhausted(self, record: ConsumedRecord, state: RetryState, *, error_code: str) -> None:
        """`maxAttempts` reached. ADR-003 D3 decides what happens, per destination, in the spec."""
        if self._retry.on_exhaust == "dlq-and-continue":
            self._dead_letter(record, error_code=error_code, stage="pending")
            self._retry_state.pop((record.topic, record.partition), None)
            return

        state.halted = True
        self._pause_and_rewind(record, state)
        self.stats.halts += 1
        self._emitter.emit(
            PartitionEvent(
                stage="partition_halted",
                status="error",
                channel=self._channel,
                channel_version=self._channel_version,
                instance=self._instance,
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                message_id=record.msg_id,
                destination=self._destination,
                attempt=state.attempt,
                error_code=error_code,
                error_message="retries exhausted; partition halted, operator action required",
            )
        )
        self._log.error(
            "partition_halted",
            channel=self._channel,
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
            attempt=state.attempt,
            error_code=error_code,
        )

    # -- dead letters ----------------------------------------------------------------------------

    def _dead_letter(self, record: ConsumedRecord, *, error_code: str, stage: str) -> None:
        if self._dlq is None:
            # Without a DLQ the only correct behaviour is to stop: committing would drop the message.
            raise DeliveryError(error_code, retryable=False, detail="no DLQ configured")
        self._dlq.dead_letter(record, error_code=error_code, stage=stage)
        self.stats.dead_lettered += 1
        self._emitter.emit(
            MessageEvent(
                stage="dlq",
                status="error",
                channel=self._channel,
                channel_version=self._channel_version,
                instance=self._instance,
                message_id=record.msg_id or "",
                destination=self._destination,
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                error_code=error_code,
                error_message="routed to the dead-letter queue",
                traceparent=record.message.traceparent,
            )
        )
        self._retry_state.pop((record.topic, record.partition), None)
        self._mark((record.topic, record.partition), record.offset)

    def _quarantine(self, raw: Any, topic: str, partition: int, offset: int, exc: RecordDecodeError) -> None:
        """A record we cannot decode at all.

        The engine writes a **new** wrapper record to the DLQ carrying the original bytes as an opaque
        payload, with a freshly minted `tb-msg-id` (envelope contract 5.2), and emits a
        `kind: "record"`, `stage: "quarantined"` event so the failure is visible in the browser rather
        than only in a log file — the record class most likely to mean an attack or a broken producer.

        **Nothing from the record is logged**: not a value, not a header name, not a `repr` or a slice.
        Under mis-framing a header name is payload, so a patient name can be sitting in a name slot.
        Coordinates, the minted id, the code and byte counts are the whole permitted set.
        """
        minted: str | None = None
        if self._dlq is not None:
            minted = self._dlq.quarantine(
                raw,
                error_code=exc.error_code,
                channel=self._channel,
                channel_version=self._channel_version,
                stage=self._read_stage,
            )
        self.stats.quarantined += 1
        value = raw.value()
        self._emitter.emit(
            RecordEvent(
                channel=self._channel,
                channel_version=self._channel_version,
                instance=self._instance,
                topic=topic,
                partition=partition,
                offset=offset,
                message_id=minted,
                size_bytes=len(value) if value is not None else 0,
                error_code=exc.error_code,
                error_message="record headers could not be decoded; quarantined verbatim to the DLQ",
            )
        )
        self._log.error(
            "record_quarantined",
            channel=self._channel,
            topic=topic,
            partition=partition,
            offset=offset,
            error_code=exc.error_code,
            message_id=minted,
        )
        self._mark((topic, partition), offset)

    # -- rebalance -------------------------------------------------------------------------------

    def _on_assign(self, consumer: KafkaConsumerLike, partitions: list[TopicPartition]) -> None:
        """Cooperative assignment. Retry state is rebuilt here: partitions come back un-paused."""
        consumer.incremental_assign(partitions)
        for partition in partitions:
            self._retry_state.pop((partition.topic, partition.partition), None)
        self._log.info("partitions_assigned", channel=self._channel, count=len(partitions))

    def _on_revoke(self, consumer: KafkaConsumerLike, partitions: list[TopicPartition]) -> None:
        """A planned handover: commit first, then give the partitions up. This is what gives 0 duplicates."""
        self._commit_pending(force=True)
        consumer.incremental_unassign(partitions)
        for partition in partitions:
            self._retry_state.pop((partition.topic, partition.partition), None)
        self._log.info("partitions_revoked", channel=self._channel, count=len(partitions))

    def _on_lost(self, consumer: KafkaConsumerLike, partitions: list[TopicPartition]) -> None:
        """The group moved on without us. **Do not commit** — another member owns these now."""
        lost: Iterable[TopicPartition] = partitions
        for partition in lost:
            self.stats.pending.pop((partition.topic, partition.partition), None)
            self._retry_state.pop((partition.topic, partition.partition), None)
        consumer.incremental_unassign(partitions)
        self._log.warning("partitions_lost", channel=self._channel, count=len(partitions))
