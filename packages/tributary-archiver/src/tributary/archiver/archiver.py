"""The consume, batch, insert, commit loop.

At-least-once and restartable: offsets are committed only after ClickHouse accepted the batch, so a crash
or restart re-reads the uncommitted tail and the table collapses it (`schema.py`). Poison records are
counted and skipped (committed), never retried: one malformed record must not stall the archive.

**Invariant 9.** The archiver is a plain consumer of `_tb.<env>.events`. Nothing in message flow reads
its state, waits on its group or on ClickHouse. If ClickHouse is down the archiver pauses its own
partitions, keeps polling (so the group stays alive) and retries the same batch with capped exponential
backoff. Events keep accumulating in Kafka (3-day retention on `_tb.<env>.events`); what is lost if the
archive stays down past that is in the package README.

**Rebalances.** There are no rebalance callbacks, and none are needed: `run_once` is synchronous, so a
batch is consumed, inserted and committed before the next `consume`, which is the only place a revoke or
an incremental assign is processed. Flush-on-revoke therefore holds by construction. During an outage the
pause set is re-applied from `assignment()` on every backoff iteration, so a partition gained through a
cooperative-sticky incremental assign is paused before it can deliver, and a record that still slips out
of `poll` is seeked back, never dropped.

**Permanent insert failures** (`SinkError.transient is False`: unknown table, type mismatch, access or
authentication refused) cannot succeed on retry. The batch is never dropped (at-least-once) and nothing is
committed: the archiver logs `insert_halted` once (fixed reason), counts `insert_failures_permanent` and
`batches_halted` (alert on both), and keeps its partitions paused, retrying at the maximum backoff so it
recovers by itself once an operator fixes the table or credentials. Events wait in Kafka meanwhile.

**One sink loop, two tables (events archiver).** The events archiver also writes `audit_log` in this loop, so a
permanent error on EITHER table (a column mismatch, a revoked grant) halts the whole batch and therefore both
topics' commits until an operator fixes it. `insert_halted` and `insert_failed` carry `table` so the alert
names the culprit. Both topics wait in Kafka meanwhile (events 3 d, audit 30 d).

**PHI (invariant 6).** Log lines carry counts, stable codes, topic/partition/offset and exception CLASS
names. Never an event field, a record value or an exception message.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Final, Protocol

from confluent_kafka import KafkaException, TopicPartition
from tributary.sdk.context import Logger

from .errors import AuditRejected, EventRejected, SinkError, UnsupportedVersion
from .event import parse_event
from .metrics import ArchiverMetrics, now_ms
from .sink import Sink

KAFKA_ERROR_LOG_INTERVAL_S: Final = 60.0
_PARTITION_EOF: Final = -191  # confluent_kafka.KafkaError._PARTITION_EOF


class RowLike(Protocol):
    """What the loop needs from a parsed record: the values to insert and the figures it counts."""

    @property
    def values(self) -> tuple[Any, ...]: ...
    @property
    def ts(self) -> Any: ...
    @property
    def attrs_dropped(self) -> int: ...
    @property
    def attrs_dropped_hmac(self) -> int: ...
    @property
    def fields_blanked(self) -> int: ...


#: Turns one consumed Kafka message into a row, or raises `EventRejected`. The default parses an event from the value.
RecordParser = Callable[[Any], RowLike]


def parse_event_record(message: Any) -> RowLike:
    value = message.value()
    if value is None:
        raise EventRejected("EVENT_EMPTY")
    return parse_event(value)


class ConsumerLike(Protocol):
    def consume(self, num_messages: int = ..., timeout: float = ...) -> Sequence[Any]: ...
    def poll(self, timeout: float = ...) -> Any: ...
    def commit(self, *, offsets: list[TopicPartition], asynchronous: bool) -> Any: ...
    def assignment(self) -> list[TopicPartition]: ...
    def pause(self, partitions: list[TopicPartition]) -> None: ...
    def resume(self, partitions: list[TopicPartition]) -> None: ...
    def seek(self, partition: TopicPartition) -> None: ...
    def position(self, partitions: list[TopicPartition]) -> list[TopicPartition]: ...
    def get_watermark_offsets(
        self, partition: TopicPartition, timeout: float = ..., cached: bool = ...
    ) -> tuple[int, int]: ...
    def close(self) -> None: ...


class ArchiverSettings:
    """Batch shape and backoff. Defaults are the measured ones in the package README."""

    __slots__ = (
        "backoff_initial_s",
        "backoff_max_s",
        "consume_chunk",
        "flush_interval_s",
        "max_batch",
        "max_batch_bytes",
        "stats_interval_s",
    )

    def __init__(
        self,
        *,
        max_batch: int = 5000,
        flush_interval_s: float = 1.0,
        backoff_initial_s: float = 0.5,
        backoff_max_s: float = 30.0,
        stats_interval_s: float = 10.0,
        max_batch_bytes: int = 64 * 1024 * 1024,
        consume_chunk: int | None = None,
    ) -> None:
        if (
            max_batch < 1
            or max_batch_bytes < 1
            or (consume_chunk is not None and consume_chunk < 1)
            or flush_interval_s <= 0
            or backoff_initial_s <= 0
            or backoff_max_s < backoff_initial_s
        ):
            raise ValueError("invalid archiver settings")
        self.max_batch = max_batch
        self.flush_interval_s = flush_interval_s
        self.backoff_initial_s = backoff_initial_s
        self.backoff_max_s = backoff_max_s
        self.stats_interval_s = stats_interval_s
        #: A batch stops growing at this many payload bytes. Content records can be ~1 MiB each, so a count cap
        #: alone (`max_batch` x 1 MiB) is not a memory bound. Only enforced when `consume_chunk < max_batch`:
        #: the loop then consumes in chunks and stops once the bytes are reached (the events archiver does not).
        self.max_batch_bytes = max_batch_bytes
        self.consume_chunk = max_batch if consume_chunk is None else min(consume_chunk, max_batch)


class Archiver:
    def __init__(
        self,
        consumer: ConsumerLike,
        sink: Sink,
        *,
        log: Logger,
        settings: ArchiverSettings | None = None,
        sleep: Callable[[float], None] = time.sleep,
        metrics: ArchiverMetrics | None = None,
        parser: RecordParser = parse_event_record,
        side_sinks: Mapping[str, Sink] | None = None,
    ) -> None:
        self._parse = parser
        #: Rows whose `table` attribute names an entry here go to that sink (the audit rows of the events
        #: archiver); every other row goes to the main sink. Absent attribute means the main sink.
        self._side_sinks = dict(side_sinks or {})
        self._consumer = consumer
        self._sink = sink
        self._log = log
        self._settings = settings or ArchiverSettings()
        self._sleep = sleep
        self._stop = False
        self._last_stats = time.monotonic()
        self.metrics = metrics if metrics is not None else ArchiverMetrics()
        self._ticked = time.monotonic()
        self._kafka_error_seen: dict[str, tuple[float | None, int]] = {}

    def stop(self) -> None:
        """Safe from a signal handler: the loop notices at its next poll or backoff slice."""
        self._stop = True

    def seconds_since_tick(self) -> float:
        """Seconds since the loop last made progress (a consume, or a backoff slice during an outage). For
        `/livez`: a wedged loop stops ticking. A slow or down ClickHouse does not, because backoff ticks."""
        return time.monotonic() - self._ticked

    def run(self) -> None:
        try:
            while not self._stop:
                self.run_once()
        finally:
            self._log.info("archiver_stopped", **self.metrics.snapshot())

    def run_once(self) -> int:
        """Consume up to one batch, write it, commit it. Returns records consumed (0 when idle)."""
        self._ticked = time.monotonic()
        messages = self._consume_batch()
        handled = 0
        rows: list[RowLike] = []
        offsets: dict[tuple[str, int], int] = {}
        for message in messages:
            error = message.error()
            if error is not None:
                self._on_kafka_error(error)
                continue
            handled += 1
            key = (message.topic(), message.partition())
            offsets[key] = max(offsets.get(key, -1), message.offset())
            value = message.value()
            self.metrics.bytes_consumed += len(value) if value is not None else 0
            try:
                try:
                    row = self._parse(message)
                except EventRejected:
                    raise
                except Exception:  # a parser bug must not kill the loop: this record is skipped, never retried
                    raise EventRejected("RECORD_UNPARSEABLE") from None
                rows.append(row)
                unknown = getattr(row, "unknown_fields", 0)
                if unknown:  # a count and a position, never a field name or value
                    self._log.warning(
                        "audit_unknown_fields_ignored",
                        count=unknown,
                        topic=key[0],
                        partition=key[1],
                        offset=message.offset(),
                    )
            except EventRejected as rejected:
                if isinstance(rejected, UnsupportedVersion):
                    self.metrics.audit_unsupported_version += 1
                elif isinstance(rejected, AuditRejected):
                    self.metrics.audit_invalid += 1
                else:
                    self.metrics.events_rejected += 1
                self._log.warning(
                    "event_rejected",
                    code=rejected.code,
                    topic=key[0],
                    partition=key[1],
                    offset=message.offset(),
                )
        if rows and not self._write(rows):
            return handled  # stopping: nothing committed, the batch is redelivered
        if offsets:
            self._commit(offsets)
        self._maybe_stats()
        return handled

    def _consume_batch(self) -> Sequence[Any]:
        settings = self._settings
        if settings.consume_chunk >= settings.max_batch:
            return self._consumer.consume(num_messages=settings.max_batch, timeout=settings.flush_interval_s)
        deadline = time.monotonic() + settings.flush_interval_s
        batch: list[Any] = []
        total = 0
        while len(batch) < settings.max_batch and total < settings.max_batch_bytes:
            remaining = max(0.0, deadline - time.monotonic())
            got = self._consumer.consume(
                num_messages=min(settings.consume_chunk, settings.max_batch - len(batch)), timeout=remaining
            )
            if not got:
                break
            batch.extend(got)
            total += sum(len(m.value() or b"") for m in got if m.error() is None)
            if remaining <= 0.0:
                break
        return batch

    def _write(self, rows: list[RowLike]) -> bool:
        delay = self._settings.backoff_initial_s
        paused: dict[tuple[str, int], TopicPartition] = {}
        halted = False
        groups: dict[str, list[tuple[Any, ...]]] = {}
        for row in rows:
            table = getattr(row, "table", "")
            groups.setdefault(table if table in self._side_sinks else "", []).append(row.values)
        done: set[str] = set()  # a group already inserted is not inserted again on a retry of the batch
        failing = "events"  # the table of the insert that raised, for the log (a name, never a value)
        try:
            while True:
                started = time.monotonic()
                try:
                    for key, values in groups.items():
                        if key not in done:
                            failing = key or "events"
                            (self._side_sinks[key] if key else self._sink).insert(values)
                            done.add(key)
                except SinkError as exc:
                    self.metrics.insert_failures += 1
                    if not exc.transient:
                        self.metrics.insert_failures_permanent += 1
                        delay = self._settings.backoff_max_s
                        if not halted:
                            halted = True
                            self.metrics.batches_halted += 1
                            self._log.error(
                                "insert_halted",
                                error=type(exc).__name__,
                                error_code=exc.code,
                                rows=len(rows),
                                table=failing,
                            )
                    else:
                        self._log.warning(
                            "insert_failed",
                            table=failing,
                            error=type(exc).__name__,
                            error_code=exc.code,
                            retry_in_s=delay,
                            rows=len(rows),
                        )
                    self._pause(paused)
                    if not self._backoff(delay, paused):
                        return False
                    if exc.transient:
                        delay = min(delay * 2, self._settings.backoff_max_s)
                    continue
                elapsed = time.monotonic() - started
                self.metrics.last_insert_ms = elapsed * 1000.0
                if self.metrics.observe_insert is not None:
                    self.metrics.observe_insert(elapsed)
                self.metrics.rows_written += len(rows)
                self.metrics.audit_written += sum(1 for row in rows if getattr(row, "table", "") in self._side_sinks)
                self.metrics.batches_written += 1
                self.metrics.attrs_dropped += sum(row.attrs_dropped for row in rows)
                self.metrics.attrs_dropped_hmac += sum(row.attrs_dropped_hmac for row in rows)
                self.metrics.fields_blanked += sum(row.fields_blanked for row in rows)
                newest = max(row.ts for row in rows)
                self.metrics.ingest_lag_ms = max(0.0, now_ms() - newest.timestamp() * 1000.0)
                return True
        finally:
            if paused:
                try:
                    self._consumer.resume(list(paused.values()))
                except KafkaException:  # assignment changed during the outage; the new owner resumes itself
                    self._log.warning("resume_failed")

    def _pause(self, paused: dict[tuple[str, int], TopicPartition]) -> None:
        """Pause everything currently assigned. Idempotent, so it is re-run every backoff iteration."""
        current = [tp for tp in self._consumer.assignment() if (tp.topic, tp.partition) not in paused]
        if current:
            self._consumer.pause(current)
            for tp in current:
                paused[(tp.topic, tp.partition)] = tp

    def _backoff(self, seconds: float, paused: dict[tuple[str, int], TopicPartition]) -> bool:
        """Wait, polling so the group membership survives a long outage. False when asked to stop."""
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if self._stop:
                return False
            self._ticked = time.monotonic()
            self._pause(paused)  # a partition gained by an incremental assign is paused before the next poll
            stray = self._consumer.poll(0)
            if stray is not None and stray.error() is None:
                # A record from a partition that was not paused yet. It must not be lost: the commit that
                # follows this batch would skip past it. Rewind so it is read again after resume.
                self._consumer.seek(TopicPartition(stray.topic(), stray.partition(), stray.offset()))
                self.metrics.stray_records_rewound += 1
            self._sleep(min(0.25, max(0.0, deadline - time.monotonic())))
        return not self._stop

    def _commit(self, offsets: dict[tuple[str, int], int]) -> None:
        try:
            self._consumer.commit(
                offsets=[TopicPartition(t, p, o + 1) for (t, p), o in offsets.items()], asynchronous=False
            )
            self.metrics.commits_ok += 1
        except KafkaException as exc:
            # Not fatal: the batch is already stored, and a redelivery collapses (at-least-once).
            self.metrics.commit_failures += 1
            self._log.warning("commit_failed", error_type=type(exc).__name__)

    def _on_kafka_error(self, error: Any) -> None:
        if error.code() == _PARTITION_EOF:
            return
        if error.fatal():
            self._log.error("kafka_fatal", code=str(error.name()))
            raise KafkaException(error)
        # An absent or unauthorised topic (UNKNOWN_TOPIC_OR_PART, TOPIC_AUTHORIZATION_FAILED) repeats at every
        # metadata refresh: log each code at most once a minute and say how many were held back.
        code = str(error.name())
        now = time.monotonic()
        last, held = self._kafka_error_seen.get(code, (None, 0))
        if last is not None and now - last < KAFKA_ERROR_LOG_INTERVAL_S:
            self._kafka_error_seen[code] = (last, held + 1)
            return
        self._kafka_error_seen[code] = (now, 0)
        self._log.warning("kafka_error", code=code, suppressed=held)

    def _maybe_stats(self) -> None:
        if time.monotonic() - self._last_stats < self._settings.stats_interval_s:
            return
        self._last_stats = time.monotonic()
        self.metrics.kafka_lag_messages = self._kafka_lag()
        self._log.info("archiver_stats", **self.metrics.snapshot())

    def _kafka_lag(self) -> int:
        total = 0
        try:
            for partition in self._consumer.assignment():
                _low, high = self._consumer.get_watermark_offsets(partition, timeout=1.0, cached=True)
                position = self._consumer.position([partition])[0].offset
                if position >= 0:
                    total += max(0, high - position)
        except KafkaException:
            return self.metrics.kafka_lag_messages
        return total
