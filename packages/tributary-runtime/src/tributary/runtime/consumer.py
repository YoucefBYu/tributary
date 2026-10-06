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
the loader's own poll timeout rather than backoff (ADR-003 follow-up 6). Checking due partitions at the
top of every iteration is not enough on its own: a quiet consumer sits inside `poll()` for the whole
timeout while a partition's backoff expires. So the poll timeout is clamped to the time until the next
due resume (`poll_timeout`), and a resume happens within a scheduler tick of its due time.

**Rebalance.** Cooperative-sticky with custom callbacks requires `incremental_assign`/
`incremental_unassign` — plain `assign()` raises `Local: Erroneous state`. A revoke commits before
handing over, which is what makes a planned handover produce zero duplicates; `on_lost` must NOT
commit, because those partitions already belong to another member. Pause state is per assignment: a
partition returns from a rebalance un-paused, so retry state is rebuilt on assign. That costs one
redelivery, which at-least-once permits and an idempotent loader absorbs (ADR-003 D7).

**A commit can be refused because the group moved on.** A synchronous commit issued while a cooperative
rebalance is in flight fails with `ILLEGAL_GENERATION`, `REBALANCE_IN_PROGRESS` or `UNKNOWN_MEMBER_ID`
(measured in S7: a loader crashed on exactly this during a planned handover, then hung on exit). That
is not a failure of anything already delivered, so the offsets stay pending and the next commit - or
the revoke callback - carries them; the worst case is a replay, which at-least-once permits. Any other
commit error still raises.

**Transactions (ADR-003 D6, opt-in).** With a `TransactionalCommitter` the records between two commits
are one Kafka transaction: their `out` and DLQ records and their input offsets commit together. A
retryable failure that will be retried (or halts) aborts the whole window, so every partition in it is
rewound to where the window began - not only the failed one - and the failed partition pauses there.
A failure that exhausts into `dlq-and-continue` does not abort: the dead letter joins the open
transaction, which is the same outcome the non-transactional path produces. `transactions.py` has the
client-side rules.

**A `TopicPartition` is never formatted into a log line.** Its `repr` crashes on Windows, and the
record it is logging beside is then silently dropped.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any, Protocol

from confluent_kafka import KafkaError, KafkaException, TopicPartition
from tributary.sdk import Message
from tributary.sdk.codes import TB_DLQ_UNWRITABLE
from tributary.sdk.context import Logger
from tributary.sdk.errors import EnvelopeError

from .errors import DeliveryError
from .events import EventEmitter, MessageEvent, PartitionEvent, RecordEvent
from .executor import PipelineAbort, StepFailure
from .records import ConsumedRecord, DecodeContext, RecordDecodeError, RecordRejectError, decode_record
from .spec import RetryPolicy
from .transactions import Committer, OffsetCommitter, TransactionError

#: librdkafka's "reached the end of this partition" signal. Informational, not an error.
PARTITION_EOF = -191

#: Commit errors that mean "the group moved on" rather than "something is broken" (see the docstring).
REBALANCE_COMMIT_ERRORS: frozenset[int] = frozenset(
    {KafkaError.ILLEGAL_GENERATION, KafkaError.REBALANCE_IN_PROGRESS, KafkaError.UNKNOWN_MEMBER_ID}
)


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

    def quarantine(self, raw: Any, *, channel: str, channel_version: int) -> str:
        """Write the quarantine wrapper for an undecodable record and return the id it minted."""


Key = tuple[str, int]


class DeadLetterObserver(Protocol):
    """Told after a dead letter's delivery callback succeeded and the record was marked.

    The pipeline worker answers a synchronous request here: the reply must follow the `dlq` callback
    (channel-spec 3.2.6). `rejected` is True only for a channel's own `Reject`.
    """

    def __call__(self, record: ConsumedRecord, *, error_code: str, rejected: bool) -> None: ...


@dataclass(slots=True)
class RetryState:
    """Per-partition blocking-retry state. Rebuilt on assignment, never persisted."""

    offset: int
    attempt: int = 0
    resume_at: float = 0.0
    halted: bool = False
    #: Where the partition is sought back to: the failed offset, or the start of an aborted
    #: transactional window, whose earlier records were discarded along with it.
    resume_from: int = -1


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
    rejected: int = 0
    commits: int = 0
    commits_refused: int = 0
    #: A commit error that is not a rebalance refusal; the loop re-raises it (`tb_commits_total` `failed`).
    commits_failed: int = 0
    pauses: int = 0
    resumes: int = 0
    halts: int = 0
    aborts: int = 0
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
        committer: Committer | None = None,
        on_dead_lettered: DeadLetterObserver | None = None,
    ) -> None:
        self._consumer = consumer
        self._on_dead_lettered = on_dead_lettered
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
        #: The stage stamped on an ordinary reject: `queued` for a worker reading `raw`, `pending` for a
        #: loader reading `out.<dest>` (envelope 2.3.1). A **quarantine** wrapper ignores this and is
        #: always `quarantined`, because that record never reached any stage.
        self._read_stage = read_stage
        self._clock = clock
        self._committer: Committer = committer if committer is not None else OffsetCommitter(consumer)
        #: Transactional mode: the first offset consumed per partition in the open window - where an
        #: abort must rewind to.
        self._window_start: dict[Key, int] = {}
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
                raw = self._consumer.poll(self.poll_timeout())
                if raw is not None:
                    self._consume_one(raw)
                if self._committer.should_commit_now(idle=raw is None):
                    # An open transaction blocks every read_committed reader downstream: never leave one.
                    self._commit_pending(force=True)
        finally:
            self._drain()
        return self.stats

    def _drain(self) -> None:
        """Graceful shutdown: commit what is safe to commit, then close.

        `close()` triggers a final revoke, so the offsets are committed here first — a commit inside a
        revoke callback during interpreter shutdown is where "it committed on my laptop" comes from.
        """
        try:
            self._commit_pending(force=True, rewind=False)
        finally:
            self._committer.abort()  # a no-op unless nothing was pending and a transaction is open
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
            self._log.warning("consumer_error", kafka_error=_kafka_error_name(error), fatal=bool(error.fatal()))
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
                context=DecodeContext(
                    channel=self._channel,
                    channel_version=self._channel_version,
                    source=self._instance,
                ),
            )
        except RecordRejectError as exc:
            self._reject(exc)
            return
        except RecordDecodeError as exc:
            self._quarantine(raw, topic, partition, offset, exc)
            return

        if self._committer.transactional:
            self._window_start.setdefault((topic, partition), offset)
        self._committer.before_record()
        try:
            self._handler.handle(record)
        except DeliveryError as exc:
            if exc.retryable:
                resume_from = record.offset
                if self._committer.transactional and not self._exhausts_to_dlq(record):
                    resume_from = self._abort_window().get((topic, partition), record.offset)
                self._begin_retry(record, error_code=exc.error_code, resume_from=resume_from)
            else:
                self._dead_letter(record, error_code=exc.error_code, stage="pending")
            return
        except StepFailure as exc:
            rejected = exc.rejected and not isinstance(exc, PipelineAbort)
            self._dead_letter(record, error_code=exc.error_code, stage="transformed", rejected=rejected)
            return

        self._succeed(record)

    def _succeed(self, record: ConsumedRecord) -> None:
        key = (record.topic, record.partition)
        state = self._retry_state.get(key)
        # After a transactional abort the partition replays from the window start, so records before
        # the failed one succeed first. Only a success at or past the failed offset ends the retry;
        # otherwise the attempt count would reset every cycle and never exhaust.
        if state is not None and record.offset >= state.offset:
            del self._retry_state[key]
        self.stats.handled += 1
        self._mark(key, record.offset)

    def _mark(self, key: Key, offset: int) -> None:
        self.stats.pending[key] = offset
        self._since_commit += 1
        if self._since_commit >= self._settings.commit_every:
            self._commit_pending()

    def _commit_pending(self, *, force: bool = False, rewind: bool = True) -> None:
        if not self.stats.pending or (not force and self._since_commit == 0):
            return
        offsets = [
            TopicPartition(topic, partition, offset + 1) for (topic, partition), offset in self.stats.pending.items()
        ]
        try:
            self._committer.commit(offsets)
        except TransactionError as exc:
            # The committer has already aborted: nothing in the window is durable, so re-consume it all.
            self._log.warning("window_aborted", channel=self._channel, error_code=exc.error_code)
            self.stats.aborts += 1
            if rewind:
                self._rewind_window()
            else:
                self._window_start.clear()
                self.stats.pending.clear()
            self._since_commit = 0
            return
        except KafkaException as exc:
            error = exc.args[0] if exc.args else None
            code = error.code() if isinstance(error, KafkaError) else None
            if code not in REBALANCE_COMMIT_ERRORS:
                self.stats.commits_failed += 1
                raise
            # Nothing delivered is undone: keep the offsets pending for the next commit or the revoke.
            self.stats.commits_refused += 1
            self._log.warning("commit_refused", channel=self._channel, kafka_error=_kafka_error_name(error))
            return
        self.stats.commits += 1
        self._since_commit = 0
        self.stats.pending.clear()
        self._window_start.clear()

    def _exhausts_to_dlq(self, record: ConsumedRecord) -> bool:
        """True when this failure is the last attempt and the policy dead-letters and moves on."""
        state = self._retry_state.get((record.topic, record.partition))
        attempts = state.attempt if state is not None and state.offset == record.offset else 0
        return attempts + 1 >= self._retry.max_attempts and self._retry.on_exhaust == "dlq-and-continue"

    def _abort_window(self) -> dict[Key, int]:
        """Abort the open transaction and rewind every partition in it. Returns the window starts."""
        starts = dict(self._window_start)
        self._committer.abort()
        self.stats.aborts += 1
        self._rewind_window()
        return starts

    def _rewind_window(self) -> None:
        """Seek every partition of an aborted window back to where the window began."""
        for (topic, partition_number), start in self._window_start.items():
            state = self._retry_state.get((topic, partition_number))
            if state is not None and 0 <= start < state.resume_from:
                state.resume_from = start
            self._consumer.seek(TopicPartition(topic, partition_number, start))
        self._window_start.clear()
        self.stats.pending.clear()

    # -- blocking retry --------------------------------------------------------------------------

    def _begin_retry(self, record: ConsumedRecord, *, error_code: str, resume_from: int | None = None) -> None:
        key = (record.topic, record.partition)
        state = self._retry_state.get(key)
        if state is None or state.offset != record.offset:
            state = RetryState(offset=record.offset)
            self._retry_state[key] = state
        state.resume_from = record.offset if resume_from is None else min(resume_from, record.offset)
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
        # `resume_from`, not the failed offset: after a transactional abort the records before the failed
        # one in this window were discarded too, and seeking past them would skip them.
        target = state.resume_from if state.resume_from >= 0 else state.offset
        partition = TopicPartition(record.topic, record.partition, target)
        self._consumer.pause([partition])
        self._consumer.seek(partition)

    def poll_timeout(self) -> float:
        """The configured poll timeout, clamped so `poll()` returns by the next due resume."""
        due = [state.resume_at for state in self._retry_state.values() if not state.halted and state.resume_at]
        if not due:
            return self._settings.poll_timeout_s
        return max(0.0, min(self._settings.poll_timeout_s, min(due) - self._clock()))

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
            if self._dead_letter(record, error_code=error_code, stage="pending"):
                self._retry_state.pop((record.topic, record.partition), None)
            return
        self._halt(
            record, state, error_code=error_code, reason="retries exhausted; partition halted, operator action required"
        )

    def _halt(self, record: ConsumedRecord, state: RetryState, *, error_code: str, reason: str) -> None:
        """Pause the partition at this record until an operator acts. `reason` is static text."""
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
                error_message=reason,
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

    def _halt_unwritable(self, record: ConsumedRecord) -> None:
        """A dead letter that cannot be written halts its partition instead of crashing the process.

        Crashing replays the same record for ever, taking every other partition down with it; committing
        would drop the record. Halting keeps it on `raw` at this offset, lets the other partitions flow,
        and says so with a fixed code and no values (security review of W2-5).
        """
        key = (record.topic, record.partition)
        state = self._retry_state.get(key)
        if state is None or state.offset != record.offset:
            state = RetryState(offset=record.offset)
            self._retry_state[key] = state
        self._halt(
            record,
            state,
            error_code=TB_DLQ_UNWRITABLE,
            reason="dead letter exceeds the envelope; partition halted, operator action required",
        )

    # -- dead letters ----------------------------------------------------------------------------

    def _dead_letter(self, record: ConsumedRecord, *, error_code: str, stage: str, rejected: bool = False) -> bool:
        """Dead-letter and mark the record. False when the dead letter could not be written and the
        partition was halted instead."""
        if self._dlq is None:
            # Without a DLQ the only correct behaviour is to stop: committing would drop the message.
            raise DeliveryError(error_code, retryable=False, detail="no DLQ configured")
        try:
            self._dlq.dead_letter(record, error_code=error_code, stage=stage)
        except EnvelopeError:
            self._halt_unwritable(record)
            return False
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
        self._notify_dead_lettered(record, error_code=error_code, rejected=rejected)
        return True

    def _notify_dead_lettered(self, record: ConsumedRecord, *, error_code: str, rejected: bool) -> None:
        """After the mark, and guarded: an observer failure must not replay a record already dead-lettered."""
        if self._on_dead_lettered is None:
            return
        try:
            self._on_dead_lettered(record, error_code=error_code, rejected=rejected)
        except Exception as exc:
            self._log.error(
                "dead_letter_observer_failed",
                channel=self._channel,
                message_id=record.msg_id,
                error_type=type(exc).__name__,
            )

    def _reject(self, exc: RecordRejectError) -> None:
        """An ordinary reject: identity intact, one or more other registered headers malformed.

        The normalised record goes to the DLQ keeping its own `tb-msg-id` and the reading stage, and the
        bad values are not preserved — the original is still on the source topic at a known offset, so a
        reject needs the record findable rather than duplicated (envelope 2.3.1).

        **Normalising is silent, so the event is not.** A reject record with `tb-attempt: "0"` is
        indistinguishable from a genuine first attempt, so the event names the offending headers. Names
        are safe where values are not: only registered names from the closed set can trigger a reject,
        so the names are ours and the values are the sender's.
        """
        record = exc.record
        if self._dlq is None:
            raise DeliveryError(exc.error_code, retryable=False, detail="no DLQ configured")
        try:
            self._dlq.dead_letter(record, error_code=exc.error_code, stage=self._read_stage)
        except EnvelopeError:
            self._halt_unwritable(record)
            return
        self.stats.rejected += 1
        self.stats.dead_lettered += 1
        self._emitter.emit(
            MessageEvent(
                stage="error",
                status="error",
                channel=self._channel,
                channel_version=self._channel_version,
                instance=self._instance,
                message_id=record.msg_id or "",
                destination=self._destination,
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                error_code=exc.error_code,
                error_message=f"malformed headers normalised: {', '.join(exc.bad_headers)}"[:512],
                traceparent=record.message.traceparent,
                attrs={"bad_header": exc.bad_headers[0]},
            )
        )
        self._log.warning(
            "record_rejected",
            channel=self._channel,
            topic=record.topic,
            partition=record.partition,
            offset=record.offset,
            message_id=record.msg_id,
            error_code=exc.error_code,
            bad_headers=",".join(exc.bad_headers),
        )
        self._mark((record.topic, record.partition), record.offset)
        self._notify_dead_lettered(record, error_code=exc.error_code, rejected=False)

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
            try:
                minted = self._dlq.quarantine(raw, channel=self._channel, channel_version=self._channel_version)
            except EnvelopeError:
                # Coordinates only: nothing from the undecodable record goes into the halt.
                self._halt_unwritable(
                    ConsumedRecord(topic=topic, partition=partition, offset=offset, message=Message())
                )
                return
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
                error_message="record could not be identified as a Tributary message; quarantined to the DLQ",
            )
        )
        self._log.error(
            "record_quarantined",
            channel=self._channel,
            topic=topic,
            partition=partition,
            offset=offset,
            error_code=exc.error_code,
            reason_code=exc.reason_code,
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
        """A planned handover: commit first, then give the partitions up. This is what gives 0 duplicates.

        If that commit is refused, the revoked partitions' pending offsets are dropped rather than kept:
        a later commit must never name partitions this member no longer owns. The new owner replays
        from the last committed offset instead.
        """
        self._commit_pending(force=True)
        for partition in partitions:
            self.stats.pending.pop((partition.topic, partition.partition), None)
        consumer.incremental_unassign(partitions)
        for partition in partitions:
            self._retry_state.pop((partition.topic, partition.partition), None)
        self._log.info("partitions_revoked", channel=self._channel, count=len(partitions))

    def _on_lost(self, consumer: KafkaConsumerLike, partitions: list[TopicPartition]) -> None:
        """The group moved on without us. **Do not commit** — another member owns these now.

        A transaction open across these partitions cannot be committed either: its offsets would be for
        partitions this member no longer owns, and the broker would reject them. It is aborted, and the
        surviving partitions of the window are rewound.
        """
        if self._committer.transactional and self._window_start:
            for partition in partitions:
                self._window_start.pop((partition.topic, partition.partition), None)
            self._abort_window()
        lost: Iterable[TopicPartition] = partitions
        for partition in lost:
            self.stats.pending.pop((partition.topic, partition.partition), None)
            self._retry_state.pop((partition.topic, partition.partition), None)
        consumer.incremental_unassign(partitions)
        self._log.warning("partitions_lost", channel=self._channel, count=len(partitions))


def _kafka_error_name(error: Any) -> str:
    """A librdkafka error's symbolic name (`REBALANCE_IN_PROGRESS`), else its numeric code, else `UNKNOWN`.

    Logged as `kafka_error`, never as `error_code`: observability/v0 L6 reserves `error_code` for codes
    registered in `envelope/v0` `error-codes.json`, and librdkafka's internal names (`_TRANSPORT`) do not
    even fit its pattern. Never the error's text, which can quote broker-side detail.
    """
    if error is None:
        return "UNKNOWN"
    name = getattr(error, "name", None)
    if callable(name):
        value = name()
        if isinstance(value, str) and value:
            return value
    code = getattr(error, "code", None)
    return str(code()) if callable(code) else "UNKNOWN"
